//! Series construction.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

/// Grouping must merge a series arriving through different label handles,
/// order series deterministically, sort samples within a series, count what
/// matched before applying the series cap, and keep histograms apart.
#[test]
fn test_build_series_data_merges_orders_and_limits() {
    let d = MetricValue::Double;
    let raw = vec![
        point(30, "a", d(3.0)),
        point(10, "b", d(1.0)),
        point(20, "a", d(2.0)),
        point(5, "a", d(0.5)),
        point(15, "b", MetricValue::Int(2)),
    ];
    let built = build_series_data(raw.clone(), 0, 10, 100, 100);
    assert_eq!(built.total_series, 2, "a and b, merged across handles");
    assert_eq!(built.volume.iter().sum::<u64>(), 5);
    assert_eq!(built.numeric.len(), 2);
    // Fingerprint order, whatever it is, is the same on every run.
    let again = build_series_data(raw.clone(), 0, 10, 100, 100);
    let hosts = |b: &BuiltSeries| {
        b.numeric
            .iter()
            .map(|(l, _)| l.get("host").unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(hosts(&built), hosts(&again));
    for (labels, pts) in &built.numeric {
        assert!(pts.windows(2).all(|w| w[0].0 <= w[1].0), "sorted by time");
        let want = if labels.get("host") == Some("a") {
            3
        } else {
            2
        };
        assert_eq!(pts.len(), want);
    }

    // The cap keeps one series but still counts both and both in volume.
    let capped = build_series_data(raw.clone(), 0, 10, 1, 100);
    assert_eq!(capped.total_series, 2);
    assert_eq!(capped.numeric.len(), 1);
    assert_eq!(capped.volume.iter().sum::<u64>(), 5);

    // The per-series sample cap keeps the OLDEST samples.
    let few = build_series_data(raw, 0, 10, 100, 2);
    let a = few
        .numeric
        .iter()
        .find(|(l, _)| l.get("host") == Some("a"))
        .unwrap();
    assert_eq!(
        a.1.iter().map(|p| p.0).collect::<Vec<_>>(),
        vec![5, 10 + 10]
    );
}

#[test]
fn test_build_series_data_separates_histograms() {
    let h = MetricValue::histogram(3, 6.0, None, None, vec![1.0], vec![1, 2]);
    let built = build_series_data(
        vec![point(1, "h", h), point(2, "n", MetricValue::Double(1.0))],
        0,
        0,
        100,
        100,
    );
    assert_eq!(built.total_series, 2);
    assert_eq!(built.histograms.len(), 1);
    assert_eq!(built.numeric.len(), 1);
    assert_eq!(built.histograms[0].1[0].count, 3);
    // No window: nothing is bucketed.
    assert_eq!(built.volume.iter().sum::<u64>(), 0);
}

#[test]
fn test_v_to_f64() {
    assert_eq!(v_to_f64(&parqtel_core::MetricValue::Double(3.15)), 3.15);
    assert_eq!(v_to_f64(&parqtel_core::MetricValue::Int(42)), 42.0);
    assert_eq!(
        v_to_f64(&parqtel_core::MetricValue::histogram(
            1,
            99.0,
            None,
            None,
            vec![],
            vec![]
        )),
        99.0
    );
    assert_eq!(
        v_to_f64(&parqtel_core::MetricValue::summary(1, 55.0, vec![])),
        55.0
    );
}
