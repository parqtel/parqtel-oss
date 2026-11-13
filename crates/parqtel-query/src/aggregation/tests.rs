//! Tests for the `aggregation` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn pts(vals: &[(i64, f64)]) -> Vec<(i64, MetricValue)> {
    vals.iter()
        .map(|&(t, v)| (t, MetricValue::Double(v)))
        .collect()
}

#[test]
fn test_avg_sum_min_max_count() {
    let p = pts(&[(100, 10.0), (200, 20.0), (300, 30.0)]);
    assert_eq!(
        aggregate(AggregationOp::Avg, &p, None, None, None),
        Some(20.0)
    );
    assert_eq!(
        aggregate(AggregationOp::Sum, &p, None, None, None),
        Some(60.0)
    );
    assert_eq!(
        aggregate(AggregationOp::Min, &p, None, None, None),
        Some(10.0)
    );
    assert_eq!(
        aggregate(AggregationOp::Max, &p, None, None, None),
        Some(30.0)
    );
    assert_eq!(
        aggregate(AggregationOp::Count, &p, None, None, None),
        Some(3.0)
    );
}

#[test]
fn test_stddev_stdvar() {
    let p = pts(&[
        (0, 2.0),
        (1, 4.0),
        (2, 4.0),
        (3, 4.0),
        (4, 5.0),
        (5, 5.0),
        (6, 7.0),
        (7, 9.0),
    ]);
    let std = aggregate(AggregationOp::Stddev, &p, None, None, None).unwrap();
    assert!((std - 2.0).abs() < 0.01, "stddev={std}");
    let var = aggregate(AggregationOp::Stdvar, &p, None, None, None).unwrap();
    assert!((var - 4.0).abs() < 0.01, "stdvar={var}");
}

#[test]
fn test_rate() {
    let p = pts(&[(1_000_000_000, 10.0), (2_000_000_000, 20.0)]);
    assert_eq!(
        aggregate(AggregationOp::Rate, &p, None, None, None),
        Some(10.0)
    );
}

#[test]
fn test_irate_uses_last_two() {
    let p = pts(&[(0, 0.0), (1_000_000_000, 5.0), (2_000_000_000, 15.0)]);
    // irate = (15-5)/(1s) = 10.0
    assert_eq!(
        aggregate(AggregationOp::Irate, &p, None, None, None),
        Some(10.0)
    );
}

#[test]
fn test_irate_counter_reset() {
    let p = pts(&[(0, 100.0), (1_000_000_000, 5.0)]);
    // reset: dv < 0 → use last.value = 5.0 / 1s
    assert_eq!(
        aggregate(AggregationOp::Irate, &p, None, None, None),
        Some(5.0)
    );
}

#[test]
fn test_increase_and_delta() {
    let p = pts(&[(0, 10.0), (1_000_000_000, 30.0)]);
    assert_eq!(
        aggregate(AggregationOp::Increase, &p, None, None, None),
        Some(20.0)
    );
    assert_eq!(
        aggregate(AggregationOp::Delta, &p, None, None, None),
        Some(20.0)
    );
}

#[test]
fn test_delta_negative() {
    let p = pts(&[(0, 30.0), (1_000_000_000, 10.0)]);
    assert_eq!(
        aggregate(AggregationOp::Delta, &p, None, None, None),
        Some(-20.0)
    );
}

#[test]
fn test_abs_ceil_floor() {
    let p = pts(&[(0, -3.7)]);
    assert_eq!(
        aggregate(AggregationOp::Abs, &p, None, None, None),
        Some(3.7)
    );
    assert_eq!(
        aggregate(AggregationOp::Ceil, &p, None, None, None),
        Some(-3.0)
    );
    assert_eq!(
        aggregate(AggregationOp::Floor, &p, None, None, None),
        Some(-4.0)
    );
}

#[test]
fn test_round() {
    let p = pts(&[(0, 7.3)]);
    assert_eq!(
        aggregate(AggregationOp::Round, &p, None, None, None),
        Some(7.0)
    );
    // round to nearest 0.5
    let p2 = pts(&[(0, 7.3)]);
    assert_eq!(
        aggregate(AggregationOp::Round, &p2, None, Some(0.5), None),
        Some(7.5)
    );
}

#[test]
fn test_clamp_min_max() {
    let p = pts(&[(0, 5.0)]);
    assert_eq!(
        aggregate(
            AggregationOp::ClampMin,
            &p,
            None,
            None,
            Some((Some(10.0), None))
        ),
        Some(10.0)
    );
    assert_eq!(
        aggregate(
            AggregationOp::ClampMax,
            &p,
            None,
            None,
            Some((None, Some(3.0)))
        ),
        Some(3.0)
    );
    assert_eq!(
        aggregate(
            AggregationOp::ClampMin,
            &p,
            None,
            None,
            Some((Some(2.0), None))
        ),
        Some(5.0)
    );
}

#[test]
fn test_downsampling() {
    let points = pts(&[(100, 10.0), (150, 20.0), (250, 30.0)]);
    let res = downsample(points, 100, 300, 100, AggregationOp::Sum, None, None, None);
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].value, 30.0);
    assert_eq!(res[1].value, 30.0);
}
