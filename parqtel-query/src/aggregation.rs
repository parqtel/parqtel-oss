use crate::models::Sample;
use crate::plan::AggregationOp;
use parqtel_core::MetricValue;

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
            if n < 2.0 {
                return None;
            }
            let mean: f64 = points.iter().map(|(_, v)| v_to_float(v)).sum::<f64>() / n;
            let var = points
                .iter()
                .map(|(_, v)| (v_to_float(v) - mean).powi(2))
                .sum::<f64>()
                / n;
            Some(var.sqrt())
        }
        AggregationOp::Stdvar => {
            let n = points.len() as f64;
            if n < 2.0 {
                return None;
            }
            let mean: f64 = points.iter().map(|(_, v)| v_to_float(v)).sum::<f64>() / n;
            Some(
                points
                    .iter()
                    .map(|(_, v)| (v_to_float(v) - mean).powi(2))
                    .sum::<f64>()
                    / n,
            )
        }

        // rate: (last - first) / elapsed_seconds  — per-second rate of a counter
        AggregationOp::Rate => {
            if points.len() < 2 {
                return None;
            }
            let first = &points[0];
            let last = &points[points.len() - 1];
            let dt = (last.0 - first.0) as f64 / 1_000_000_000.0;
            if dt <= 0.0 {
                return None;
            }
            let dv = v_to_float(&last.1) - v_to_float(&first.1);
            Some(dv / dt)
        }

        // irate: instantaneous rate using only the last 2 samples
        AggregationOp::Irate => {
            if points.len() < 2 {
                return None;
            }
            let prev = &points[points.len() - 2];
            let last = &points[points.len() - 1];
            let dt = (last.0 - prev.0) as f64 / 1_000_000_000.0;
            if dt <= 0.0 {
                return None;
            }
            let dv = v_to_float(&last.1) - v_to_float(&prev.1);
            // Handle counter reset
            let dv = if dv < 0.0 { v_to_float(&last.1) } else { dv };
            Some(dv / dt)
        }

        // increase: total increase of a counter over the range (rate * duration)
        AggregationOp::Increase => {
            if points.len() < 2 {
                return None;
            }
            let first = &points[0];
            let last = &points[points.len() - 1];
            let dt = (last.0 - first.0) as f64 / 1_000_000_000.0;
            if dt <= 0.0 {
                return None;
            }
            let dv = v_to_float(&last.1) - v_to_float(&first.1);
            // Handle counter reset by using last value as increase
            Some(if dv < 0.0 { v_to_float(&last.1) } else { dv })
        }

        // delta: difference between last and first (for gauges, not counters)
        AggregationOp::Delta => {
            if points.len() < 2 {
                return None;
            }
            let first = v_to_float(&points[0].1);
            let last = v_to_float(&points[points.len() - 1].1);
            Some(last - first)
        }

        AggregationOp::HistogramQuantile => {
            let q = quantile?;
            if let (
                _,
                MetricValue::Histogram {
                    boundaries, counts, ..
                },
            ) = &points[points.len() - 1]
            {
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
    if counts.is_empty() {
        return 0.0;
    }
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return 0.0;
    }
    let target = q * total as f64;
    let mut cumulative = 0.0;
    for (i, &count) in counts.iter().enumerate() {
        let prev = cumulative;
        cumulative += count as f64;
        if cumulative >= target {
            let lower = if i == 0 { 0.0 } else { boundaries[i - 1] };
            let upper = if i < boundaries.len() {
                boundaries[i]
            } else {
                lower * 2.0
            };
            let frac = if cumulative == prev {
                0.0
            } else {
                (target - prev) / (cumulative - prev)
            };
            return lower + (upper - lower) * frac;
        }
    }
    *boundaries.last().unwrap_or(&0.0)
}

/// Divides a time range into equal-width step windows and applies an aggregation.
#[allow(clippy::too_many_arguments)]
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
    downsample_impl(
        points,
        start_ns,
        end_ns,
        step_ns,
        None,
        op,
        quantile,
        scalar_param,
        clamp,
    )
}

/// Plan-aware dispatch: windowed ops (rate/increase/delta/irate) with a
/// parsed `[range]` use per-step Prometheus lookback; everything else uses
/// the in-step aggregation path.
#[allow(clippy::too_many_arguments)]
pub fn downsample_plan(
    points: Vec<(i64, MetricValue)>,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    range_ns: Option<i64>,
    op: AggregationOp,
    quantile: Option<f64>,
    scalar_param: Option<f64>,
    clamp: Option<(Option<f64>, Option<f64>)>,
) -> Vec<Sample> {
    use AggregationOp::{Delta, Increase, Irate, Rate};
    let windowed = matches!(op, Rate | Increase | Delta | Irate) && range_ns.is_some();
    if windowed {
        downsample_impl(
            points,
            start_ns,
            end_ns,
            step_ns,
            range_ns,
            op,
            quantile,
            scalar_param,
            clamp,
        )
    } else {
        downsample_impl(
            points,
            start_ns,
            end_ns,
            step_ns,
            None,
            op,
            quantile,
            scalar_param,
            clamp,
        )
    }
}

/// Prometheus-semantics windowed evaluation for the counter/range family:
/// at each step boundary `t`, evaluates the op over the lookback window
/// `[t - range_ns, t)`. `rate` extrapolates to the window like Prometheus
/// and handles counter resets; `increase` = rate × range; `irate` uses the
/// last two samples inside the window; `delta` is the raw difference.
///
/// This replaces the legacy behaviour where `[5m]` was discarded and the
/// window silently became the whole query range.
#[allow(clippy::too_many_arguments)]
pub fn downsample_windowed(
    points: Vec<(i64, MetricValue)>,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    range_ns: i64,
    op: AggregationOp,
    quantile: Option<f64>,
    scalar_param: Option<f64>,
    clamp: Option<(Option<f64>, Option<f64>)>,
) -> Vec<Sample> {
    downsample_impl(
        points,
        start_ns,
        end_ns,
        step_ns,
        Some(range_ns),
        op,
        quantile,
        scalar_param,
        clamp,
    )
}

#[allow(clippy::too_many_arguments)]
fn downsample_impl(
    points: Vec<(i64, MetricValue)>,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    range_ns: Option<i64>,
    op: AggregationOp,
    quantile: Option<f64>,
    scalar_param: Option<f64>,
    clamp: Option<(Option<f64>, Option<f64>)>,
) -> Vec<Sample> {
    let mut samples = Vec::new();
    let mut current = start_ns;

    match range_ns {
        // ── Legacy path: in-step windows (aggregations, transforms) ────────
        None => {
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
                    samples.push(Sample {
                        timestamp_ns: current,
                        value: val,
                    });
                }
                current = window_end;
            }
        }
        // ── Windowed path: per-step lookback of `range_ns` (Prometheus) ──
        Some(range) => {
            let range_f = range as f64;
            while current < end_ns {
                let window_start = current.saturating_sub(range);
                // Binary search: first point >= window_start
                let lo = points.partition_point(|p| p.0 < window_start);
                // First point >= current + step (exclusive end)
                let hi = points.partition_point(|p| p.0 < current + step_ns);
                let val = if matches!(op, AggregationOp::Irate) {
                    // irate: last two samples inside [window_start, current+step)
                    if hi - lo >= 2 {
                        let prev = &points[hi - 2];
                        let last = &points[hi - 1];
                        instant_rate(prev, last)
                    } else {
                        None
                    }
                } else if hi > lo {
                    let first = &points[lo];
                    let last = &points[hi - 1];
                    match op {
                        AggregationOp::Rate => windowed_rate(first, last, range_f),
                        AggregationOp::Increase => {
                            windowed_rate(first, last, range_f).map(|r| r * range_f / 1e9)
                        }
                        AggregationOp::Delta => {
                            let dv = v_to_float(&last.1) - v_to_float(&first.1);
                            Some(dv)
                        }
                        // Non-counter ops with a range: fall back to step
                        // semantics over the lookback window (rare; keeps
                        // e.g. `avg(x[5m])` meaningful rather than an error).
                        _ => {
                            let window = &points[lo..hi];
                            aggregate(op, window, quantile, scalar_param, clamp)
                        }
                    }
                } else {
                    None
                };
                if let Some(val) = val {
                    samples.push(Sample {
                        timestamp_ns: current,
                        value: val,
                    });
                }
                current += step_ns;
            }
        }
    }
    samples
}

/// Per-second rate across a window with counter-reset correction and
/// Prometheus-style extrapolation to the window edges.
fn windowed_rate(
    first: &(i64, MetricValue),
    last: &(i64, MetricValue),
    range_ns: f64,
) -> Option<f64> {
    let dt = (last.0 - first.0) as f64 / 1_000_000_000.0;
    if dt <= 0.0 {
        return None;
    }
    let mut dv = v_to_float(&last.1) - v_to_float(&first.1);
    // Counter reset: value decreased -> the counter restarted; treat the
    // post-reset value as the increase (single-reset approximation, same
    // as the legacy implementation).
    if dv < 0.0 {
        dv = v_to_float(&last.1);
    }
    let rate = dv / dt;
    // Extrapolate toward window edges, capped at 1.1x the observed span
    // on each side (Prometheus behaviour).
    let span = dt;
    let window_s = range_ns / 1_000_000_000.0;
    let extrapolated = if window_s > span {
        let slack = ((window_s - span) / 2.0).min(span * 0.1);
        rate * (span + 2.0 * slack) / span
    } else {
        rate
    };
    Some(extrapolated)
}

/// Instant rate between two adjacent samples (irate) with reset correction.
fn instant_rate(prev: &(i64, MetricValue), last: &(i64, MetricValue)) -> Option<f64> {
    let dt = (last.0 - prev.0) as f64 / 1_000_000_000.0;
    if dt <= 0.0 {
        return None;
    }
    let dv = v_to_float(&last.1) - v_to_float(&prev.1);
    let dv = if dv < 0.0 { v_to_float(&last.1) } else { dv };
    Some(dv / dt)
}

#[cfg(test)]
mod tests {
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
}

#[cfg(test)]
mod windowed_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use parqtel_core::MetricValue;

    const S: i64 = 1_000_000_000; // 1 second in ns
    const M: i64 = 60 * S;
    const H: i64 = 60 * M;

    fn pts(samples: &[(i64, f64)]) -> Vec<(i64, MetricValue)> {
        samples
            .iter()
            .map(|(t, v)| (*t, MetricValue::Double(*v)))
            .collect()
    }

    /// rate(x[1m]) vs rate(x[1h]) over the same data MUST differ — this is
    /// the regression test for the discarded-range bug.
    #[test]
    fn range_selector_changes_result() {
        // Counter rising 1/sec over 1 hour.
        let mut data = Vec::new();
        for t in 0..3600 {
            data.push((t * S, t as f64));
        }
        let start = 3500 * S;
        let end = 3600 * S;
        let step = 10 * S;

        let r_1m = downsample_windowed(
            pts(&data),
            start,
            end,
            step,
            M,
            AggregationOp::Rate,
            None,
            None,
            None,
        );
        let r_1h = downsample_windowed(
            pts(&data),
            start,
            end,
            step,
            H,
            AggregationOp::Rate,
            None,
            None,
            None,
        );
        // Both windows contain a 1/sec counter; rate must be ~1.0 for both.
        // (Uniform counter: same rate regardless of window.) The
        // differentiation test is below with a bursty counter.
        assert!(
            (r_1m[0].value - 1.0).abs() < 0.05,
            "1m rate {}",
            r_1m[0].value
        );
        assert!(
            (r_1h[0].value - 1.0).abs() < 0.05,
            "1h rate {}",
            r_1h[0].value
        );
    }

    #[test]
    fn bursty_counter_short_vs_long_window() {
        // Counter flat at 0 for 58 minutes, then bursts +10/sec in the last 2m.
        let mut data = Vec::new();
        for t in 0..58 {
            data.push((t * M, 0.0));
        }
        for i in 0..120 {
            data.push((58 * M + i * S, i as f64 * 10.0)); // +10/sec burst
        }
        // Evaluate at t=59m (step boundary), step 30s.
        let start = 59 * M;
        let end = 59 * M + 30 * S;

        let r_1m = downsample_windowed(
            pts(&data),
            start,
            end,
            30 * S,
            M,
            AggregationOp::Rate,
            None,
            None,
            None,
        );
        let r_1h = downsample_windowed(
            pts(&data),
            start,
            end,
            30 * S,
            H,
            AggregationOp::Rate,
            None,
            None,
            None,
        );
        // The 1m window sees only the burst: ~10/sec.
        assert!(
            r_1m[0].value > 8.0,
            "1m window must see burst, got {}",
            r_1m[0].value
        );
        // The 1h window averages burst + flat tail over the hour:
        // ~1200 events / 3600s ≈ 0.33/sec.
        assert!(
            (r_1h[0].value - 1200.0 / 3600.0).abs() < 0.1,
            "1h window averages burst, got {}",
            r_1h[0].value
        );
    }

    #[test]
    fn increase_scales_by_range() {
        let mut data = Vec::new();
        for t in 0..3600 {
            data.push((t * S, t as f64));
        }
        let inc_5m = downsample_windowed(
            pts(&data),
            3500 * S,
            3600 * S,
            10 * S,
            5 * M,
            AggregationOp::Increase,
            None,
            None,
            None,
        );
        // ~300 events in a 5m window (1/sec counter).
        assert!(
            (inc_5m[0].value - 300.0).abs() < 35.0,
            "increase(5m) ≈ 300, got {}",
            inc_5m[0].value
        );
    }

    #[test]
    fn counter_reset_corrected() {
        // Counter resets to 0 midway through the window.
        let data = [
            (0, 100.0),
            (30 * S, 200.0),
            (60 * S, 250.0), // reset happened: 500 -> 0 -> 250
            (90 * S, 300.0),
            (120 * S, 400.0),
        ];
        let d: Vec<(i64, MetricValue)> = data
            .iter()
            .map(|(t, v)| (*t, MetricValue::Double(*v)))
            .collect();
        let r = downsample_windowed(
            d,
            120 * S,
            180 * S,
            60 * S,
            2 * M,
            AggregationOp::Rate,
            None,
            None,
            None,
        );
        // Without reset handling this would be hugely negative.
        assert!(r[0].value >= 0.0, "reset corrected, got {}", r[0].value);
    }

    #[test]
    fn irate_uses_last_two_samples_in_window() {
        let data = [
            (0, 0.0),
            (30 * S, 30.0),  // 1/sec
            (60 * S, 90.0),  // 2/sec (last two)
            (90 * S, 100.0), // ~0.33/sec
        ];
        let d: Vec<(i64, MetricValue)> = data
            .iter()
            .map(|(t, v)| (*t, MetricValue::Double(*v)))
            .collect();
        let r = downsample_windowed(
            d,
            90 * S,
            150 * S,
            30 * S,
            2 * M,
            AggregationOp::Irate,
            None,
            None,
            None,
        );
        // At t=90s the last two samples are (60s,90) -> (90s,100): 10/30s ≈ 0.333
        assert!((r[0].value - 10.0 / 30.0).abs() < 1e-9);
    }

    #[test]
    fn sparse_window_yields_no_sample() {
        // No points inside a given window -> no sample for that step
        // (Prometheus leaves the gap empty).
        let data = [(0, 1.0)];
        let d: Vec<(i64, MetricValue)> = data
            .iter()
            .map(|(t, v)| (*t, MetricValue::Double(*v)))
            .collect();
        let r = downsample_windowed(
            d,
            10 * M,
            20 * M,
            M,
            M,
            AggregationOp::Rate,
            None,
            None,
            None,
        );
        assert!(r.is_empty(), "empty window must produce no samples");
    }

    #[test]
    fn non_windowed_aggregation_unaffected_by_range() {
        // sum with a range present (post-AST this becomes avg_over_time etc.)
        // falls back to step semantics — must not crash or mis-window.
        let data = [(0, 1.0), (M, 2.0), (2 * M, 3.0)];
        let d: Vec<(i64, MetricValue)> = data
            .iter()
            .map(|(t, v)| (*t, MetricValue::Double(*v)))
            .collect();
        let r = downsample_windowed(d, 0, 3 * M, M, 5 * M, AggregationOp::Sum, None, None, None);
        // Window at t covers [t-5m, t+1m): cumulative lookbehind sums.
        assert_eq!(r.len(), 3);
        assert!((r[0].value - 1.0).abs() < 1e-9); // only (0,1)
        assert!((r[1].value - 3.0).abs() < 1e-9); // 1+2
        assert!((r[2].value - 6.0).abs() < 1e-9); // 1+2+3
    }
}
