//! Cross-crate self-instrumentation primitives.
//!
//! These live in `parqtel-core` rather than in the server crate because the
//! code that needs to measure them is spread across the workspace:
//! `parqtel-ingest` holds the per-signal ingest mutex, `parqtel-core::storage`
//! holds the block index lock, and `parqtel-server` renders the Prometheus
//! text. Putting the counters here keeps the hot paths free of any dependency
//! on the HTTP layer and gives one histogram implementation instead of one
//! per crate.
//!
//! What is measured (and why):
//!
//! * **Ingest lock wait** — every ingest request serializes on one mutex per
//!   signal, and a block flush happens while that mutex is held. Lock wait is
//!   therefore the single best predictor of ingest p99, and until now it was
//!   only derivable from a profiler.
//! * **Flush duration / in-flight** — `parqtel-server`'s 5-second tick records
//!   flush latency, but capacity-triggered flushes inside a request were not
//!   observed at all, so the expensive case was invisible.
//! * **Index lock wait** — `BlockIndex` is read by every query handler and
//!   written by the index task, which also performs synchronous file I/O.
//!
//! All counters are lock-free apart from short critical sections around the
//! histogram vectors, and the recording path allocates nothing.

use crate::models::storage::SignalType;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Number of signals tracked by [`ContentionMetrics`].
///
/// Must match the length of [`SignalType::ALL`]; asserted by a unit test.
const SIGNAL_COUNT: usize = SignalType::ALL.len();

/// Bucket upper bounds (seconds) for the latency histograms. Chosen so the
/// interesting region for both lock waits and flushes — sub-millisecond when
/// healthy, seconds when blocked — is well resolved.
const SECONDS_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Fixed-bucket cumulative histogram with a running sum and count.
///
/// Deliberately simple: no allocation on the recording path, and a linear
/// scan over ~12 bucket bounds, which is cheaper than any concurrent
/// structure at these observation rates.
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Upper bounds, ascending.
    buckets: Vec<f64>,
    /// `counts[i]` holds observations `<= buckets[i]`, plus a trailing
    /// `+Inf` overflow slot.
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    /// Builds a histogram with the given ascending bucket upper bounds.
    pub fn new(buckets: Vec<f64>) -> Self {
        let n = buckets.len();
        Self {
            buckets,
            counts: vec![0; n + 1],
            sum: 0.0,
            count: 0,
        }
    }

    /// A histogram with the shared [`SECONDS_BUCKETS`] layout.
    pub fn seconds() -> Self {
        Self::new(SECONDS_BUCKETS.to_vec())
    }

    /// Records one observation.
    pub fn record(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        for (i, &bound) in self.buckets.iter().enumerate() {
            if value <= bound {
                self.counts[i] += 1;
                return;
            }
        }
        // Above every bucket bound -> the +Inf overflow slot.
        if let Some(last) = self.counts.last_mut() {
            *last += 1;
        }
    }

    /// Records one observation expressed as a [`Duration`], in seconds.
    pub fn record_duration(&mut self, elapsed: Duration) {
        self.record(elapsed.as_secs_f64());
    }

    /// Total observations recorded.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Sum of all observations.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Renders the histogram in Prometheus text format. Bucket counts are
    /// cumulative, as the format requires.
    ///
    /// `series_prefix` carries the label set for the metric family, e.g.
    /// `parqtel_flush_duration_seconds{signal="metrics"}`, so the emitted
    /// bucket/sum/count lines are correctly labelled series rather than a
    /// single unlabelled family.
    pub fn render(&self, series_prefix: &str) -> String {
        let mut out = String::new();
        let mut cumulative = 0;
        for (i, &b) in self.buckets.iter().enumerate() {
            cumulative += self.counts[i];
            out.push_str(&format!(
                "{series_prefix}_bucket{{le=\"{b}\"}} {cumulative}\n"
            ));
        }
        cumulative += self.counts.last().copied().unwrap_or(0);
        out.push_str(&format!(
            "{series_prefix}_bucket{{le=\"+Inf\"}} {cumulative}\n"
        ));
        out.push_str(&format!("{series_prefix}_sum {}\n", self.sum));
        out.push_str(&format!("{series_prefix}_count {}\n", self.count));
        out
    }
}

/// Series prefix for a metric family carrying a `signal` label.
fn series(metric: &str, signal: SignalType) -> String {
    format!("{metric}{{signal=\"{}\"}}", signal.as_str())
}

/// Emits the `# HELP` / `# TYPE` header for a metric family.
///
/// Prometheus declares a family once; the per-signal series follow.
fn render_header(out: &mut String, metric: &str, help: &str, kind: &str) {
    out.push_str(&format!("# HELP {metric} {help}\n"));
    out.push_str(&format!("# TYPE {metric} {kind}\n"));
}

/// Renders a histogram family with one labelled series per signal.
///
/// Signals are always emitted (zero-valued when idle) so that a dashboard
/// query does not have to cope with series appearing and disappearing.
fn render_histogram_family<'h, F>(out: &mut String, metric: &str, help: &str, f: F)
where
    F: Fn(SignalType) -> &'h Histogram,
{
    render_header(out, metric, help, "histogram");
    for signal in SignalType::ALL {
        out.push_str(&f(signal).render(&series(metric, signal)));
    }
}

/// Renders a gauge or counter family with one labelled sample per signal.
fn render_sample_family<F>(out: &mut String, metric: &str, help: &str, kind: &str, f: F)
where
    F: Fn(SignalType) -> String,
{
    render_header(out, metric, help, kind);
    for signal in SignalType::ALL {
        out.push_str(&format!("{} {}\n", series(metric, signal), f(signal)));
    }
}

/// Lock-wait and flush-shape counters for the ingest path and the block index.
///
/// Cheap to share (`Arc<ContentionMetrics>`) and safe to call from any thread
/// or task. A default instance is created by the server and handed to the
/// three ingestion services, the block-index tasks and the `/metrics`
/// renderer.
#[derive(Debug)]
pub struct ContentionMetrics {
    /// Time spent waiting for the per-signal ingest mutex.
    ingest_lock_wait: [Mutex<Histogram>; SIGNAL_COUNT],
    /// Wall time of each block flush that actually wrote rows.
    flush_duration: [Mutex<Histogram>; SIGNAL_COUNT],
    /// Flushes currently encoding/writing for a signal.
    flush_inflight: [AtomicU64; SIGNAL_COUNT],
    /// Rows written by flushes for a signal, since process start.
    flush_rows: [AtomicU64; SIGNAL_COUNT],
    /// Time spent waiting for the block index write lock.
    index_lock_wait: Mutex<Histogram>,
}

impl Default for ContentionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentionMetrics {
    /// A fresh set of counters with empty histograms.
    pub fn new() -> Self {
        Self {
            ingest_lock_wait: std::array::from_fn(|_| Mutex::new(Histogram::seconds())),
            flush_duration: std::array::from_fn(|_| Mutex::new(Histogram::seconds())),
            flush_inflight: std::array::from_fn(|_| AtomicU64::new(0)),
            flush_rows: std::array::from_fn(|_| AtomicU64::new(0)),
            index_lock_wait: Mutex::new(Histogram::seconds()),
        }
    }

    /// Records how long a request waited for the ingest mutex of `signal`.
    ///
    /// Call this *after* the guard is acquired, so the contended mutex is not
    /// held while recording.
    pub fn record_ingest_lock_wait(&self, signal: SignalType, waited: Duration) {
        let mut h = self.ingest_lock_wait[signal.index()]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        h.record_duration(waited);
    }

    /// Records how long a block-index write lock acquisition took.
    pub fn record_index_lock_wait(&self, waited: Duration) {
        let mut h = self
            .index_lock_wait
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        h.record_duration(waited);
    }

    /// Marks the start of a flush and returns a guard that records the
    /// duration and the row count when dropped.
    ///
    /// The guard must be created only for a flush that really writes rows —
    /// an idle no-op flush would otherwise dilute the histogram, the same
    /// rule the SLI layer already applies.
    pub fn flush_started(&self, signal: SignalType) -> FlushGuard<'_> {
        self.flush_inflight[signal.index()].fetch_add(1, Ordering::Relaxed);
        FlushGuard {
            metrics: self,
            signal,
            started: Instant::now(),
        }
    }

    /// Flushes currently in progress for `signal`.
    pub fn flush_inflight(&self, signal: SignalType) -> u64 {
        self.flush_inflight[signal.index()].load(Ordering::Relaxed)
    }

    /// Rows written to Parquet blocks for `signal` since process start.
    pub fn flush_rows(&self, signal: SignalType) -> u64 {
        self.flush_rows[signal.index()].load(Ordering::Relaxed)
    }

    /// Observations recorded for the ingest lock wait of `signal`.
    pub fn ingest_lock_wait_count(&self, signal: SignalType) -> u64 {
        self.ingest_lock_wait[signal.index()]
            .lock()
            .map(|h| h.count())
            .unwrap_or(0)
    }

    /// Observations recorded for flushes of `signal`.
    pub fn flush_duration_count(&self, signal: SignalType) -> u64 {
        self.flush_duration[signal.index()]
            .lock()
            .map(|h| h.count())
            .unwrap_or(0)
    }

    /// Renders every counter in Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::new();

        // The histogram locks are held briefly and never across an await, so
        // borrowing them for the render is safe and keeps the output
        // internally consistent.
        let ingest_lock_wait = self.lock_all(&self.ingest_lock_wait);
        render_histogram_family(
            &mut out,
            "parqtel_ingest_lock_wait_seconds",
            "Seconds an ingest request waited for the per-signal ingest mutex",
            |s| &ingest_lock_wait[s.index()],
        );
        drop(ingest_lock_wait);

        let flush_duration = self.lock_all(&self.flush_duration);
        render_histogram_family(
            &mut out,
            "parqtel_flush_duration_seconds",
            "Wall time of each block flush that wrote rows (encode, compress, fsync)",
            |s| &flush_duration[s.index()],
        );
        drop(flush_duration);

        render_sample_family(
            &mut out,
            "parqtel_flush_inflight",
            "Block flushes currently in progress",
            "gauge",
            |s| self.flush_inflight(s).to_string(),
        );

        render_sample_family(
            &mut out,
            "parqtel_flush_rows_total",
            "Rows written to Parquet blocks since process start",
            "counter",
            |s| self.flush_rows(s).to_string(),
        );

        render_header(
            &mut out,
            "parqtel_index_lock_wait_seconds",
            "Seconds the block-index writer waited for the write lock",
            "histogram",
        );
        if let Ok(h) = self.index_lock_wait.lock() {
            out.push_str(&h.render("parqtel_index_lock_wait_seconds"));
        }

        out
    }

    /// Locks every histogram in `slot` once, up front.
    ///
    /// A poisoned lock (only reachable if a recording thread panicked) yields
    /// an empty histogram rather than losing the whole metric family, so a
    /// panic cannot make `/metrics` fail to render.
    fn lock_all<'a>(
        &'a self,
        slot: &'a [Mutex<Histogram>; SIGNAL_COUNT],
    ) -> Vec<std::sync::MutexGuard<'a, Histogram>> {
        slot.iter()
            .map(|m| m.lock().unwrap_or_else(|e| e.into_inner()))
            .collect()
    }
}

/// RAII guard returned by [`ContentionMetrics::flush_started`].
///
/// Records wall time and row count on drop, so a flush cannot be silently
/// forgotten on an early-return or error path. Because the row count is only
/// known after the writer has written, pass it to [`FlushGuard::count_rows`]
/// before the guard goes out of scope.
#[derive(Debug)]
pub struct FlushGuard<'a> {
    metrics: &'a ContentionMetrics,
    signal: SignalType,
    started: Instant,
}

impl FlushGuard<'_> {
    /// Records how many rows this flush wrote.
    pub fn count_rows(&self, rows: u64) {
        self.metrics.flush_rows[self.signal.index()].fetch_add(rows, Ordering::Relaxed);
    }
}

impl Drop for FlushGuard<'_> {
    fn drop(&mut self) {
        self.metrics.flush_inflight[self.signal.index()].fetch_sub(1, Ordering::Relaxed);
        let mut h = self.metrics.flush_duration[self.signal.index()]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        h.record_duration(self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
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
}
