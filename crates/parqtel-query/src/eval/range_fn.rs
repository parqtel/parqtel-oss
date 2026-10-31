//! Range-vector functions: the rate family and the `_over_time` functions.

use super::functions::quantile_of;
use super::Evaluator;
use crate::ast::*;
use parqtel_core::{Error, LabelSet, Result};

/// Extracts (range_expr, fn_name) when a call's argument is a Range.
/// Most range functions take the range vector first — rate(x[5m]) —
/// while quantile_over_time-style functions take a scalar param first
/// and the range second.
pub(super) fn range_fn_args(call: &CallExpr) -> Option<(&RangeExpr, String)> {
    let range_arg = call.args.iter().find_map(|a| match a {
        Expr::Range(r) => Some(r),
        _ => None,
    })?;
    Some((range_arg, call.name.clone()))
}

/// Applies a range-window function over one series' samples.
fn apply_range_fn(
    name: &str,
    vals: &[(i64, f64)],
    _call: &CallExpr,
    _ctx: EvalContext,
) -> Result<Option<f64>> {
    if vals.is_empty() {
        return Ok(None);
    }
    let result = match name {
        "rate" => windowed_rate_val(vals),
        "irate" => {
            if vals.len() < 2 {
                return Ok(None);
            }
            let (pt, pv) = vals[vals.len() - 2];
            let (lt, lv) = vals[vals.len() - 1];
            let dt = (lt - pt) as f64 / 1e9;
            if dt <= 0.0 {
                return Ok(None);
            }
            let mut dv = lv - pv;
            if dv < 0.0 {
                dv = lv;
            }
            dv / dt
        }
        "increase" => {
            let r = windowed_rate_val(vals);
            let span =
                vals.last().map(|x| x.0).unwrap_or(0) - vals.first().map(|x| x.0).unwrap_or(0);
            r * span as f64 / 1e9
        }
        "delta" => {
            if vals.len() < 2 {
                return Ok(None);
            }
            vals.last().map(|x| x.1).unwrap_or(0.0) - vals.first().map(|x| x.1).unwrap_or(0.0)
        }
        "idelta" => {
            // Last sample minus the one before it (counter resets
            // corrected to the current value, like rate's per-segment fix).
            if vals.len() < 2 {
                return Ok(None);
            }
            let (pt, pv) = vals[vals.len() - 2];
            let (lt, lv) = vals[vals.len() - 1];
            let _ = pt;
            let _ = lt;
            if lv >= pv {
                lv - pv
            } else {
                lv
            }
        }
        "quantile_over_time" => {
            // φ is the FIRST arg, range second: quantile_over_time(φ, x[5m]).
            let q = match _call.args.first() {
                Some(crate::ast::Expr::Number(n)) => *n,
                _ => return Ok(None),
            };
            if !(0.0..=1.0).contains(&q) {
                return Ok(None);
            }
            let mut sorted: Vec<f64> = vals.iter().map(|(_, v)| *v).collect();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            quantile_of(q, &sorted)
        }
        "mad_over_time" => {
            // Median absolute deviation over the window.
            let mut sorted: Vec<f64> = vals.iter().map(|(_, v)| *v).collect();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let med = quantile_of(0.5, &sorted);
            let mut devs: Vec<f64> = sorted.iter().map(|v| (v - med).abs()).collect();
            devs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            quantile_of(0.5, &devs)
        }
        "avg_over_time" => vals.iter().map(|(_, v)| *v).sum::<f64>() / vals.len() as f64,
        "min_over_time" => vals.iter().map(|(_, v)| *v).fold(f64::INFINITY, f64::min),
        "max_over_time" => vals
            .iter()
            .map(|(_, v)| *v)
            .fold(f64::NEG_INFINITY, f64::max),
        "sum_over_time" => vals.iter().map(|(_, v)| v).sum(),
        "count_over_time" => vals.len() as f64,
        "last_over_time" => vals.last().map(|x| x.1).unwrap_or(0.0),
        "present_over_time" => 1.0,
        "stddev_over_time" => {
            let n = vals.len() as f64;
            let mean = vals.iter().map(|(_, v)| *v).sum::<f64>() / n;
            let var = vals.iter().map(|(_, v)| (*v - mean).powi(2)).sum::<f64>() / n;
            var.sqrt()
        }
        "stdvar_over_time" => {
            let n = vals.len() as f64;
            let mean = vals.iter().map(|(_, v)| *v).sum::<f64>() / n;
            vals.iter().map(|(_, v)| (*v - mean).powi(2)).sum::<f64>() / n
        }
        "changes" => {
            let mut n = 0.0;
            for w in vals.windows(2) {
                if w[0].1 != w[1].1 {
                    n += 1.0;
                }
            }
            n
        }
        "resets" => {
            let mut n = 0.0;
            for w in vals.windows(2) {
                if w[1].1 < w[0].1 {
                    n += 1.0;
                }
            }
            n
        }
        "deriv" => {
            if vals.len() < 2 {
                return Ok(None);
            }
            let (t0, v0) = vals.first().copied().unwrap_or((0, 0.0));
            let (t1, v1) = vals.last().copied().unwrap_or((0, 0.0));
            let dt = (t1 - t0) as f64 / 1e9;
            if dt <= 0.0 {
                return Ok(None);
            }
            (v1 - v0) / dt
        }
        "predict_linear" => {
            // Linear regression over the window, extrapolated to
            // (evaluation timestamp + horizon) — Prometheus semantics.
            if vals.len() < 2 {
                return Ok(None);
            }
            let (slope, intercept) = linear_regression(vals);
            let horizon = match _call.args.get(1) {
                Some(crate::ast::Expr::Number(n)) => *n,
                _ => 0.0,
            };
            // linear_regression() returns the intercept at the LAST
            // sample (value-at-window-end convention), so the prediction
            // point is (eval_ts + horizon - last_sample_ts).
            let t_last = vals.last().map(|v| v.0).unwrap_or(0) as f64 / 1e9;
            let eval_secs = _ctx.ts_ns as f64 / 1e9;
            let x = eval_secs + horizon - t_last;
            intercept + slope * x
        }
        "double_exponential_smoothing" | "holt_winters" => {
            // Prometheus's renamed holt_winters: trend-corrected double
            // exponential smoothing with clamped factors.
            if vals.len() < 2 {
                return Ok(None);
            }
            let sf = match _call.args.get(1) {
                Some(crate::ast::Expr::Number(n)) => (*n).clamp(0.0, 1.0),
                _ => 0.1,
            };
            let tf = match _call.args.get(2) {
                Some(crate::ast::Expr::Number(n)) => (*n).clamp(0.0, 1.0),
                _ => 0.3,
            };
            let mut s = vals[0].1;
            let mut b = vals[1].1 - vals[0].1;
            for w in vals.windows(2) {
                let cur = w[1].1;
                let prev_s = s;
                s = sf * cur + (1.0 - sf) * (prev_s + b);
                b = tf * (s - prev_s) + (1.0 - tf) * b;
            }
            s
        }
        other => {
            return Err(Error::Validation(format!(
                "unsupported range function {other:?}"
            )))
        }
    };
    if result.is_nan() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

/// rate with per-segment reset accumulation + extrapolation:
/// a window may contain MULTIPLE counter resets; sum the increase of each
/// monotonic segment instead of collapsing to a single-reset correction.
/// `range_ns` is the window width for edge extrapolation (0 = none).
fn windowed_rate_val(vals: &[(i64, f64)]) -> f64 {
    rate_over_samples(vals, 0.0)
}

/// Core: per-segment counter increase over samples.
fn segment_increase(vals: &[(i64, f64)]) -> f64 {
    let mut total = 0.0;
    for w in vals.windows(2) {
        let (t0, v0) = w[0];
        let (t1, v1) = w[1];
        let delta = if v1 >= v0 { v1 - v0 } else { v1 };
        let _ = t0;
        let _ = t1;
        total += delta;
    }
    total
}

/// Prometheus-style rate: segment increase over the observed span,
/// extrapolated toward the window edges (capped at 10% of span per side).
fn rate_over_samples(vals: &[(i64, f64)], window_ns: f64) -> f64 {
    if vals.len() < 2 {
        return f64::NAN;
    }
    let (t0, _) = vals.first().copied().unwrap_or((0, 0.0));
    let (t1, _) = vals.last().copied().unwrap_or((0, 0.0));
    let span = (t1 - t0) as f64 / 1e9;
    if span <= 0.0 {
        return f64::NAN;
    }
    let increase = segment_increase(vals);
    let rate = increase / span;
    // Extrapolate to window edges when the window exceeds the observed span.
    if window_ns > span && window_ns > 0.0 {
        let slack = ((window_ns - span) / 2.0).min(span * 0.1);
        rate * (span + 2.0 * slack) / span
    } else {
        rate
    }
}

/// Ordinary least squares over (t_seconds, value) for predict_linear.
fn linear_regression(vals: &[(i64, f64)]) -> (f64, f64) {
    let n = vals.len() as f64;
    let t0 = vals.first().map(|v| v.0).unwrap_or(0);
    let xs: Vec<f64> = vals.iter().map(|(t, _)| (*t - t0) as f64 / 1e9).collect();
    let ys: Vec<f64> = vals.iter().map(|(_, v)| *v).collect();
    let sx: f64 = xs.iter().sum();
    let sy: f64 = ys.iter().sum();
    let sxy: f64 = xs.iter().zip(&ys).map(|(a, b)| a * b).sum();
    let sxx: f64 = xs.iter().map(|a| a * a).sum();
    let denom = n * sxx - sx * sx;
    if denom.abs() < 1e-12 {
        return (0.0, sy / n);
    }
    let slope = (n * sxy - sx * sy) / denom;
    let intercept = (sy - slope * sx) / n;
    (
        slope,
        intercept + slope * ((vals.last().map(|v| v.0).unwrap_or(0) - t0) as f64 / 1e9),
    )
}

impl<'a> Evaluator<'a> {
    pub(super) fn eval_range_fn(
        &self,
        name: &str,
        range_expr: &RangeExpr,
        call: &CallExpr,
        ctx: EvalContext,
    ) -> Result<InstantVector> {
        let windows = self.eval_range_windows(range_expr, ctx)?;

        // absent_over_time: emits 1 (with the selector's equality labels)
        // when NO series had samples in the window — Prometheus semantics.
        if name == "absent_over_time" {
            if windows.is_empty() {
                if let crate::ast::Expr::Selector(sel) = &*range_expr.expr {
                    let mut labels = LabelSet::default();
                    for m in &sel.matchers {
                        if m.op == crate::matcher::MatchOp::Equal {
                            labels = labels.merge(
                                &LabelSet::try_from_iter(vec![(m.name.clone(), m.value.clone())])
                                    .unwrap_or_default(),
                            );
                        }
                    }
                    return Ok(InstantVector {
                        series: vec![(crate::ast::shared_labels(labels), 1.0)],
                    });
                }
                return Ok(InstantVector {
                    series: vec![(crate::ast::shared_labels(LabelSet::default()), 1.0)],
                });
            }
            return Ok(InstantVector::default());
        }

        let mut out = Vec::with_capacity(windows.len());
        for (labels, samples) in windows {
            let vals: Vec<(i64, f64)> = samples.iter().map(|s| (s.timestamp_ns, s.value)).collect();
            let val = apply_range_fn(name, &vals, call, ctx)?;
            if let Some(v) = val {
                out.push((labels, v));
            }
        }
        Ok(InstantVector { series: out })
    }
}
