use parqtel_core::MetricValue;
use crate::models::Sample;
use crate::plan::AggregationOp;

/// Aggregates a window of data points into a single scalar value.
/// Returns `None` for empty windows or ops that require ≥2 points with insufficient data.
pub fn aggregate(
    op: AggregationOp,
    points: &[(i64, MetricValue)],
    quantile: Option<f64>,
    scalar_param: Option<f64>,
    clamp: Option<(Option<f64>, Option<f64>)>,
) -> Option<f64> {
    if points.is_empty() {
        return None;
    }

    match op {
        AggregationOp::Avg => {
            let sum: f64 = points.iter().map(|(_, v)| v_to_float(v)).sum();
            Some(sum / points.len() as f64)
        }
        AggregationOp::Sum => Some(points.iter().map(|(_, v)| v_to_float(v)).sum()),
        AggregationOp::Min => points
            .iter()
            .map(|(_, v)| v_to_float(v))
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
        AggregationOp::Max => points
            .iter()
            .map(|(_, v)| v_to_float(v))
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
        AggregationOp::Count => Some(points.len() as f64),

        AggregationOp::Stddev => {
            let n = points.len() as f64;
            if n < 2.0 { return None; }
            let mean: f64 = points.iter().map(|(_, v)| v_to_float(v)).sum::<f64>() / n;
            let var = points.iter().map(|(_, v)| (v_to_float(v) - mean).powi(2)).sum::<f64>() / n;
            Some(var.sqrt())
        }
        AggregationOp::Stdvar => {
            let n = points.len() as f64;
            if n < 2.0 { return None; }
            let mean: f64 = points.iter().map(|(_, v)| v_to_float(v)).sum::<f64>() / n;
            Some(points.iter().map(|(_, v)| (v_to_float(v) - mean).powi(2)).sum::<f64>() / n)
        }

        // rate: (last - first) / elapsed_seconds  — per-second rate of a counter
        AggregationOp::Rate => {
            if points.len() < 2 { return None; }
            let first = &points[0];
            let last = &points[points.len() - 1];
            let dt = (last.0 - first.0) as f64 / 1_000_000_000.0;
            if dt <= 0.0 { return None; }
            let dv = v_to_float(&last.1) - v_to_float(&first.1);
            Some(dv / dt)
        }

        // irate: instantaneous rate using only the last 2 samples
        AggregationOp::Irate => {
            if points.len() < 2 { return None; }
            let prev = &points[points.len() - 2];
            let last = &points[points.len() - 1];
            let dt = (last.0 - prev.0) as f64 / 1_000_000_000.0;
            if dt <= 0.0 { return None; }
            let dv = v_to_float(&last.1) - v_to_float(&prev.1);
            // Handle counter reset
            let dv = if dv < 0.0 { v_to_float(&last.1) } else { dv };
            Some(dv / dt)
        }

        // increase: total increase of a counter over the range (rate * duration)
        AggregationOp::Increase => {
            if points.len() < 2 { return None; }
            let first = &points[0];
            let last = &points[points.len() - 1];
            let dt = (last.0 - first.0) as f64 / 1_000_000_000.0;
            if dt <= 0.0 { return None; }
            let dv = v_to_float(&last.1) - v_to_float(&first.1);
            // Handle counter reset by using last value as increase
            Some(if dv < 0.0 { v_to_float(&last.1) } else { dv })
        }

        // delta: difference between last and first (for gauges, not counters)
        AggregationOp::Delta => {
            if points.len() < 2 { return None; }
            let first = v_to_float(&points[0].1);
            let last = v_to_float(&points[points.len() - 1].1);
            Some(last - first)
        }

        AggregationOp::HistogramQuantile => {
            let q = quantile?;
            if let (_, MetricValue::Histogram { boundaries, counts, .. }) = &points[points.len() - 1] {
                Some(estimate_quantile(boundaries, counts, q))
            } else {
                None
            }
        }

        // ── Instant transforms — applied to the last value in the window ──────
        AggregationOp::Abs => Some(v_to_float(&points[points.len() - 1].1).abs()),
        AggregationOp::Ceil => Some(v_to_float(&points[points.len() - 1].1).ceil()),
        AggregationOp::Floor => Some(v_to_float(&points[points.len() - 1].1).floor()),
        AggregationOp::Round => {
            let v = v_to_float(&points[points.len() - 1].1);
            if let Some(to_nearest) = scalar_param {
                if to_nearest != 0.0 {
                    return Some((v / to_nearest).round() * to_nearest);
                }
            }
            Some(v.round())
        }
        AggregationOp::ClampMin => {
            let v = v_to_float(&points[points.len() - 1].1);
            let min = clamp.and_then(|(lo, _)| lo).unwrap_or(f64::NEG_INFINITY);
            Some(v.max(min))
        }
        AggregationOp::ClampMax => {
            let v = v_to_float(&points[points.len() - 1].1);
            let max = clamp.and_then(|(_, hi)| hi).unwrap_or(f64::INFINITY);
            Some(v.min(max))
        }

        // TopK/BottomK ranking is done at the result-set level in executor.rs,
        // not per window. Fall back to last value so windows are still populated.
        AggregationOp::TopK | AggregationOp::BottomK => {
            Some(v_to_float(&points[points.len() - 1].1))
        }

        // LabelReplace is a label-manipulation pass; value pass-through.
        AggregationOp::LabelReplace => Some(v_to_float(&points[points.len() - 1].1)),
    }
}

fn v_to_float(v: &MetricValue) -> f64 {
    match v {
        MetricValue::Double(f) => *f,
        MetricValue::Int(i) => *i as f64,
        MetricValue::Histogram { sum, .. } => *sum,
        MetricValue::Summary { sum, .. } => *sum,
    }
}

fn estimate_quantile(boundaries: &[f64], counts: &[u64], q: f64) -> f64 {
    if counts.is_empty() { return 0.0; }
    let total: u64 = counts.iter().sum();
    if total == 0 { return 0.0; }
    let target = q * total as f64;
    let mut cumulative = 0.0;
    for (i, &count) in counts.iter().enumerate() {
        let prev = cumulative;
        cumulative += count as f64;
        if cumulative >= target {
            let lower = if i == 0 { 0.0 } else { boundaries[i - 1] };
            let upper = if i < boundaries.len() { boundaries[i] } else { lower * 2.0 };
            let frac = if cumulative == prev { 0.0 } else { (target - prev) / (cumulative - prev) };
            return lower + (upper - lower) * frac;
        }
    }
    *boundaries.last().unwrap_or(&0.0)
}

/// Divides a time range into equal-width step windows and applies an aggregation.
pub fn downsample(
    points: Vec<(i64, MetricValue)>,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    op: AggregationOp,
    quantile: Option<f64>,
    scalar_param: Option<f64>,
    clamp: Option<(Option<f64>, Option<f64>)>,
) -> Vec<Sample> {
    let mut samples = Vec::new();
    let mut current = start_ns;
    let mut idx = 0;

    while current < end_ns {
        let window_end = current + step_ns;
        let mut window: Vec<(i64, MetricValue)> = Vec::new();
        while idx < points.len() && points[idx].0 < window_end {
            if points[idx].0 >= current {
                window.push(points[idx].clone());
            }
            idx += 1;
        }
        if let Some(val) = aggregate(op, &window, quantile, scalar_param, clamp) {
            samples.push(Sample { timestamp_ns: current, value: val });
        }
        current = window_end;
    }
    samples
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn pts(vals: &[(i64, f64)]) -> Vec<(i64, MetricValue)> {
        vals.iter().map(|&(t, v)| (t, MetricValue::Double(v))).collect()
    }

    #[test]
    fn test_avg_sum_min_max_count() {
        let p = pts(&[(100, 10.0), (200, 20.0), (300, 30.0)]);
        assert_eq!(aggregate(AggregationOp::Avg, &p, None, None, None), Some(20.0));
        assert_eq!(aggregate(AggregationOp::Sum, &p, None, None, None), Some(60.0));
        assert_eq!(aggregate(AggregationOp::Min, &p, None, None, None), Some(10.0));
        assert_eq!(aggregate(AggregationOp::Max, &p, None, None, None), Some(30.0));
        assert_eq!(aggregate(AggregationOp::Count, &p, None, None, None), Some(3.0));
    }

    #[test]
    fn test_stddev_stdvar() {
        let p = pts(&[(0, 2.0), (1, 4.0), (2, 4.0), (3, 4.0), (4, 5.0), (5, 5.0), (6, 7.0), (7, 9.0)]);
        let std = aggregate(AggregationOp::Stddev, &p, None, None, None).unwrap();
        assert!((std - 2.0).abs() < 0.01, "stddev={std}");
        let var = aggregate(AggregationOp::Stdvar, &p, None, None, None).unwrap();
        assert!((var - 4.0).abs() < 0.01, "stdvar={var}");
    }

    #[test]
    fn test_rate() {
        let p = pts(&[(1_000_000_000, 10.0), (2_000_000_000, 20.0)]);
        assert_eq!(aggregate(AggregationOp::Rate, &p, None, None, None), Some(10.0));
    }

    #[test]
    fn test_irate_uses_last_two() {
        let p = pts(&[(0, 0.0), (1_000_000_000, 5.0), (2_000_000_000, 15.0)]);
        // irate = (15-5)/(1s) = 10.0
        assert_eq!(aggregate(AggregationOp::Irate, &p, None, None, None), Some(10.0));
    }

    #[test]
    fn test_irate_counter_reset() {
        let p = pts(&[(0, 100.0), (1_000_000_000, 5.0)]);
        // reset: dv < 0 → use last.value = 5.0 / 1s
        assert_eq!(aggregate(AggregationOp::Irate, &p, None, None, None), Some(5.0));
    }

    #[test]
    fn test_increase_and_delta() {
        let p = pts(&[(0, 10.0), (1_000_000_000, 30.0)]);
        assert_eq!(aggregate(AggregationOp::Increase, &p, None, None, None), Some(20.0));
        assert_eq!(aggregate(AggregationOp::Delta, &p, None, None, None), Some(20.0));
    }

    #[test]
    fn test_delta_negative() {
        let p = pts(&[(0, 30.0), (1_000_000_000, 10.0)]);
        assert_eq!(aggregate(AggregationOp::Delta, &p, None, None, None), Some(-20.0));
    }

    #[test]
    fn test_abs_ceil_floor() {
        let p = pts(&[(0, -3.7)]);
        assert_eq!(aggregate(AggregationOp::Abs, &p, None, None, None), Some(3.7));
        assert_eq!(aggregate(AggregationOp::Ceil, &p, None, None, None), Some(-3.0));
        assert_eq!(aggregate(AggregationOp::Floor, &p, None, None, None), Some(-4.0));
    }

    #[test]
    fn test_round() {
        let p = pts(&[(0, 7.3)]);
        assert_eq!(aggregate(AggregationOp::Round, &p, None, None, None), Some(7.0));
        // round to nearest 0.5
        let p2 = pts(&[(0, 7.3)]);
        assert_eq!(aggregate(AggregationOp::Round, &p2, None, Some(0.5), None), Some(7.5));
    }

    #[test]
    fn test_clamp_min_max() {
        let p = pts(&[(0, 5.0)]);
        assert_eq!(aggregate(AggregationOp::ClampMin, &p, None, None, Some((Some(10.0), None))), Some(10.0));
        assert_eq!(aggregate(AggregationOp::ClampMax, &p, None, None, Some((None, Some(3.0)))), Some(3.0));
        assert_eq!(aggregate(AggregationOp::ClampMin, &p, None, None, Some((Some(2.0), None))), Some(5.0));
    }

    #[test]
    fn test_downsampling() {
        let points = pts(&[(100, 10.0), (150, 20.0), (250, 30.0)]);
        let res = downsample(points, 100, 300, 100, AggregationOp::Sum, None, None, None);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].value, 30.0);
        assert_eq!(res[1].value, 30.0);
    }
}
