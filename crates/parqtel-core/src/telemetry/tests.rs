//! Tests for the `telemetry` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[test]
fn signal_count_matches_array_layout() {
    // `ContentionMetrics` sizes its arrays from SIGNAL_COUNT; a mismatch
    // would silently drop or overrun observations.
    assert_eq!(SIGNAL_COUNT, SignalType::ALL.len());
    for (i, signal) in SignalType::ALL.iter().enumerate() {
        assert_eq!(signal.index(), i, "{signal:?} must map to slot {i}");
    }
}

#[test]
fn histogram_records_into_correct_bucket() {
    let mut h = Histogram::new(vec![1.0, 5.0, 10.0]);
    h.record(0.5);
    h.record(3.0);
    h.record(7.0);
    h.record(100.0); // overflow slot
    let rendered = h.render("test");
    assert!(rendered.contains("test_bucket{le=\"1\"} 1"));
    assert!(rendered.contains("test_bucket{le=\"5\"} 2"));
    assert!(rendered.contains("test_bucket{le=\"10\"} 3"));
    assert!(rendered.contains("test_bucket{le=\"+Inf\"} 4"));
    assert!(rendered.contains("test_count 4"));
    assert_eq!(h.count(), 4);
    assert!((h.sum() - 110.5).abs() < f64::EPSILON);
}

#[test]
fn histogram_bucket_bounds_are_ascending() {
    let h = Histogram::seconds();
    // Rendering walks buckets in order and emits cumulative counts, so a
    // descending bound list would produce nonsense series.
    let rendered = h.render("x");
    let mut prev = f64::NEG_INFINITY;
    for line in rendered.lines().filter(|l| l.contains("_bucket{")) {
        let le = line
            .split("le=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        if le == "+Inf" {
            break;
        }
        let bound: f64 = le.parse().unwrap();
        assert!(bound > prev, "bucket {bound} must exceed {prev}");
        prev = bound;
    }
}

#[test]
fn flush_guard_tracks_inflight_rows_and_duration() {
    let m = ContentionMetrics::new();
    assert_eq!(m.flush_inflight(SignalType::Metrics), 0);

    {
        let guard = m.flush_started(SignalType::Metrics);
        assert_eq!(m.flush_inflight(SignalType::Metrics), 1);
        // Other signals must be unaffected.
        assert_eq!(m.flush_inflight(SignalType::Logs), 0);
        guard.count_rows(1234);
    }

    assert_eq!(m.flush_inflight(SignalType::Metrics), 0);
    assert_eq!(m.flush_rows(SignalType::Metrics), 1234);
    assert_eq!(m.flush_duration_count(SignalType::Metrics), 1);
    assert_eq!(m.flush_duration_count(SignalType::Logs), 0);
}

#[test]
fn flush_guard_records_duration_even_on_error_path() {
    // The whole point of the RAII guard: a flush that returns Err must
    // still release the in-flight gauge and record its wall time.
    fn failing_flush(m: &ContentionMetrics) -> Result<(), ()> {
        let _guard = m.flush_started(SignalType::Logs);
        Err(())
    }

    let m = ContentionMetrics::new();
    assert!(failing_flush(&m).is_err());
    assert_eq!(m.flush_inflight(SignalType::Logs), 0);
    assert_eq!(m.flush_duration_count(SignalType::Logs), 1);
}

#[test]
fn renders_every_signal_even_when_idle() {
    // Series must not appear and disappear: a dashboard query against a
    // per-signal metric needs all three labels to exist from boot.
    let m = ContentionMetrics::new();
    let out = m.render();
    for signal in SignalType::ALL {
        assert!(
            out.contains(&format!("signal=\"{}\"", signal.as_str())),
            "missing {signal:?} series"
        );
    }
    for metric in [
        "parqtel_ingest_lock_wait_seconds",
        "parqtel_flush_duration_seconds",
        "parqtel_flush_inflight",
        "parqtel_flush_rows_total",
        "parqtel_index_lock_wait_seconds",
    ] {
        assert!(out.contains(metric), "missing metric {metric}");
    }
    assert!(out.contains("parqtel_flush_rows_total{signal=\"metrics\"} 0"));
    assert!(out.contains("parqtel_ingest_lock_wait_seconds{signal=\"traces\"}_count 0"));
}

#[test]
fn lock_wait_counters_are_per_signal() {
    let m = ContentionMetrics::new();
    m.record_ingest_lock_wait(SignalType::Metrics, Duration::from_millis(5));
    m.record_ingest_lock_wait(SignalType::Metrics, Duration::from_millis(15));
    m.record_ingest_lock_wait(SignalType::Logs, Duration::from_millis(1));
    m.record_index_lock_wait(Duration::from_millis(2));

    assert_eq!(m.ingest_lock_wait_count(SignalType::Metrics), 2);
    assert_eq!(m.ingest_lock_wait_count(SignalType::Logs), 1);
    assert_eq!(m.ingest_lock_wait_count(SignalType::Traces), 0);
    assert!(m
        .render()
        .contains("parqtel_index_lock_wait_seconds_count 1"));
}

/// Regression guard for a malformed-output bug: an earlier version
/// emitted the metric name with a label set but no value, producing lines
/// like `foo{signal="metrics"} foo{signal="metrics"}_bucket{le="1"} 3`.
/// Prometheus silently drops such samples, so the whole family would look
/// empty to a dashboard. Every non-comment line must be
/// `<series> <value>` with a parseable value.
#[test]
fn rendered_output_is_well_formed_prometheus_text() {
    let m = ContentionMetrics::new();
    m.record_ingest_lock_wait(SignalType::Metrics, Duration::from_millis(5));
    m.record_index_lock_wait(Duration::from_millis(1));
    {
        let guard = m.flush_started(SignalType::Traces);
        guard.count_rows(7);
    }

    let out = m.render();
    let mut samples = 0;
    for line in out.lines() {
        if line.starts_with('#') {
            continue;
        }
        let (series, value) = line
            .rsplit_once(' ')
            .unwrap_or_else(|| panic!("sample line has no value: {line:?}"));
        assert!(
            !series.is_empty(),
            "sample line has no series name: {line:?}"
        );
        // The series name must not itself contain a second metric name
        // (which is what a duplicated prefix would look like).
        assert_eq!(
            series.matches("_bucket{").count(),
            if series.contains("_bucket{") { 1 } else { 0 },
            "line mixes two series: {line:?}"
        );
        assert!(
            value.parse::<f64>().is_ok(),
            "value must be numeric, got {value:?} in {line:?}"
        );
        samples += 1;
    }
    assert!(samples > 50, "expected a full body, got {samples} samples");

    // Bucket label sets must combine the signal label with the le label,
    // and only the le label may be duplicated.
    let bucket_line = out
        .lines()
        .find(|l| l.starts_with("parqtel_flush_duration_seconds{signal=\"traces\"}_bucket"))
        .expect("traces flush bucket series must exist");
    assert!(bucket_line.contains("le=\""));
    assert!(bucket_line.contains("signal=\"traces\""));
    assert!(!bucket_line.contains("signal=\"metrics\""));
}

/// The shard gauge must distinguish "not reported" (0) from a genuinely
/// configured single shard, or a dashboard will conclude an embedder is
/// unsharded when it simply never called the setter.
#[test]
fn shard_gauge_distinguishes_unset_from_single_shard() {
    let m = ContentionMetrics::new();
    assert_eq!(
        m.ingest_rotator_shards(),
        0,
        "unreported must read as 0, not as one shard"
    );
    m.set_ingest_rotator_shards(1);
    assert_eq!(m.ingest_rotator_shards(), 1);
    m.set_ingest_rotator_shards(4);
    assert_eq!(m.ingest_rotator_shards(), 4);

    let out = m.render();
    assert!(out.contains("parqtel_ingest_rotator_shards 4"));
}

#[test]
fn each_histogram_family_declares_help_and_type_once() {
    let out = ContentionMetrics::new().render();
    for metric in [
        "parqtel_ingest_lock_wait_seconds",
        "parqtel_flush_duration_seconds",
        "parqtel_flush_inflight",
        "parqtel_flush_rows_total",
        "parqtel_index_lock_wait_seconds",
    ] {
        assert_eq!(
            out.matches(&format!("# HELP {metric} ")).count(),
            1,
            "{metric} must declare HELP exactly once"
        );
        assert_eq!(
            out.matches(&format!("# TYPE {metric} ")).count(),
            1,
            "{metric} must declare TYPE exactly once"
        );
    }
}
