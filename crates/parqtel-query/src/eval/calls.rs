//! Function-call evaluation, including the label and histogram families.

use super::functions::{date_component, map_values, math_fn, quantile_from_buckets};
use super::range_fn::range_fn_args;
use super::Evaluator;
use crate::ast::*;
use crate::matcher::evaluate_matchers;
use parqtel_core::{Error, LabelSet, Result};
use std::collections::BTreeMap;

impl<'a> Evaluator<'a> {
    /// Latest histogram sample for each series of `name` at ctx.ts_ns
    /// (same lookback rule as instant selectors).
    fn hist_selector_windows(
        &self,
        sel: &SelectorExpr,
        ctx: EvalContext,
    ) -> Result<Vec<(crate::ast::SharedLabels, crate::ast::HistSample)>> {
        let out = Vec::new();
        let Some(name) = &sel.metric_name else {
            return Ok(out);
        };
        let Some(hist) = self.hist_data else {
            return Ok(out);
        };
        let Some(series) = hist.get(name) else {
            return Ok(out);
        };
        let lookback = ctx.range_ns.max(self.lookback_ns);
        let mut found = Vec::new();
        let shifted = ctx.ts_ns - ctx.offset_ns;
        for (labels, samples) in series {
            if !evaluate_matchers(&sel.matchers, labels, name) {
                continue;
            }
            let idx = samples.partition_point(|s| s.timestamp_ns <= shifted);
            if idx == 0 {
                continue;
            }
            let s = &samples[idx - 1];
            if s.timestamp_ns < shifted - lookback {
                continue;
            }
            // `labels` is shared across every step; cloning the Arc is a
            // refcount bump where cloning the LabelSet was one allocation per
            // label per step.
            found.push((std::sync::Arc::clone(labels), s.clone()));
        }
        Ok(found)
    }

    pub(super) fn eval_call(&self, call: &CallExpr, ctx: EvalContext) -> Result<InstantVector> {
        // Range functions take a RangeExpr arg.
        if let Some((fn_range, fn_name)) = range_fn_args(call) {
            return self.eval_range_fn(&fn_name, fn_range, call, ctx);
        }
        match call.name.as_str() {
            // Instant transforms applied per-series.
            "abs" | "ceil" | "floor" | "sqrt" | "exp" | "ln" | "log2" | "log10" | "sgn" | "sin"
            | "cos" | "tan" | "asin" | "acos" | "atan" | "sinh" | "cosh" | "tanh" | "asinh"
            | "acosh" | "atanh" | "deg" | "rad" => {
                let v = self.eval(&call.args[0], ctx)?;
                Ok(map_values(v, |x| math_fn(&call.name, x)))
            }
            "pi" => Ok(InstantVector {
                series: vec![(
                    crate::ast::shared_labels(LabelSet::default()),
                    std::f64::consts::PI,
                )],
            }),
            "atan2" => {
                // atan2(y, x): y is the vector, x a scalar (PromQL flips the
                // usual argument order: atan2(v, s) = atan2(v_i, s)).
                let v = self.eval(&call.args[0], ctx)?;
                let x = self.eval_scalar(&call.args[1], ctx)?;
                Ok(map_values(v, |y| y.atan2(x)))
            }
            "round" => {
                let v = self.eval(&call.args[0], ctx)?;
                let to = call
                    .args
                    .get(1)
                    .map(|a| self.eval_scalar(a, ctx))
                    .transpose()?
                    .unwrap_or(1.0);
                Ok(map_values(v, |x| (x / to).round() * to))
            }
            "clamp" => {
                let v = self.eval(&call.args[0], ctx)?;
                let min = self.eval_scalar(&call.args[1], ctx)?;
                let max = self.eval_scalar(&call.args[2], ctx)?;
                Ok(map_values(v, |x| x.clamp(min, max)))
            }
            "clamp_min" | "clamp_max" => {
                let v = self.eval(&call.args[0], ctx)?;
                let bound = self.eval_scalar(&call.args[1], ctx)?;
                Ok(map_values(v, |x| {
                    if call.name == "clamp_min" {
                        x.max(bound)
                    } else {
                        x.min(bound)
                    }
                }))
            }
            "label_replace" => self.eval_label_replace(call, ctx),
            "label_join" => self.eval_label_join(call, ctx),
            "scalar" => {
                let v = self.eval(&call.args[0], ctx)?;
                if v.series.len() == 1 {
                    Ok(InstantVector {
                        series: vec![(
                            crate::ast::shared_labels(LabelSet::default()),
                            v.series[0].1,
                        )],
                    })
                } else {
                    Ok(InstantVector {
                        series: vec![(crate::ast::shared_labels(LabelSet::default()), f64::NAN)],
                    })
                }
            }
            "vector" => {
                let s = self.eval_scalar(&call.args[0], ctx)?;
                Ok(InstantVector {
                    series: vec![(crate::ast::shared_labels(LabelSet::default()), s)],
                })
            }
            "time" => Ok(InstantVector {
                series: vec![(
                    crate::ast::shared_labels(LabelSet::default()),
                    ctx.ts_ns as f64 / 1e9,
                )],
            }),
            "absent" => self.eval_absent(call, ctx),
            // Date helpers: operate on the evaluation timestamp.
            "minute" | "hour" | "day_of_week" | "day_of_month" | "day_of_month_iso"
            | "day_of_year" | "days_in_month" | "month" | "year" => {
                // No-arg form uses the evaluation timestamp; with a vector
                // arg the fn applies per series over that vector's values.
                if call.args.is_empty() {
                    let v = date_component(&call.name, ctx.ts_ns as f64 / 1e9);
                    Ok(InstantVector {
                        series: vec![(crate::ast::shared_labels(LabelSet::default()), v)],
                    })
                } else {
                    let v = self.eval(&call.args[0], ctx)?;
                    Ok(map_values(v, |x| date_component(&call.name, x)))
                }
            }
            "timestamp" => {
                // Per-series timestamp of the last sample (seconds since
                // epoch). InstantVector holds values only, so track the
                // sample time through eval_selector via a side map: the
                // selector evaluation stamps each series with its newest
                // sample time in `last_seen`.
                let v = self.eval(&call.args[0], ctx)?;
                let ts_secs = ctx.ts_ns as f64 / 1e9;
                // Without per-sample retention the best faithful value is
                // the evaluation timestamp for present series (they all had
                // a sample within the lookback window).
                Ok(map_values(v, |_| ts_secs))
            }
            "sort" | "sort_desc" => {
                let mut v = self.eval(&call.args[0], ctx)?;
                if call.name == "sort" {
                    v.series
                        .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    v.series
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                }
                Ok(v)
            }
            "sort_by_label" | "sort_by_label_desc" => {
                // Experimental in Prometheus; sorts by label values
                // (string order), remaining labels as tiebreakers.
                let mut v = self.eval(&call.args[0], ctx)?;
                let label = self.eval_string(&call.args[1], ctx)?;
                let key = |ls: &LabelSet| ls.get(&label).map(|s| s.to_string()).unwrap_or_default();
                if call.name == "sort_by_label" {
                    v.series.sort_by_key(|(ls, _)| key(ls));
                } else {
                    v.series.sort_by_key(|(ls, _)| std::cmp::Reverse(key(ls)));
                }
                Ok(v)
            }
            "label_del" => self.eval_label_del(call, ctx),
            // ── Native-histogram family ───────────────────────────────
            // These take a plain (instant) histogram selector; OTLP
            // explicit-bucket histograms map to Prometheus classic-bucket
            // semantics directly.
            "histogram_count" | "histogram_sum" | "histogram_avg" => {
                let Some(Expr::Selector(sel)) = call.args.first() else {
                    return Err(Error::Validation(format!(
                        "{}() requires a histogram selector argument",
                        call.name
                    )));
                };
                let wins = self.hist_selector_windows(sel, ctx)?;
                let out: Vec<(crate::ast::SharedLabels, f64)> = wins
                    .into_iter()
                    .map(|(labels, h)| {
                        let v = match call.name.as_str() {
                            "histogram_count" => h.count as f64,
                            "histogram_sum" => h.sum,
                            _ => {
                                if h.count > 0 {
                                    h.sum / h.count as f64
                                } else {
                                    f64::NAN
                                }
                            }
                        };
                        (labels, v)
                    })
                    .collect();
                Ok(InstantVector { series: out })
            }
            "histogram_stddev" | "histogram_stdvar" => {
                let Some(Expr::Selector(sel)) = call.args.first() else {
                    return Err(Error::Validation(format!(
                        "{}() requires a histogram selector argument",
                        call.name
                    )));
                };
                let wins = self.hist_selector_windows(sel, ctx)?;
                let out: Vec<(crate::ast::SharedLabels, f64)> = wins
                    .into_iter()
                    .map(|(labels, h)| {
                        let v = if call.name == "histogram_stddev" {
                            h.variance().sqrt()
                        } else {
                            h.variance()
                        };
                        (labels, v)
                    })
                    .collect();
                Ok(InstantVector { series: out })
            }
            "histogram_fraction" => {
                // histogram_fraction(lower, upper, selector)
                let lower = self.eval_scalar(&call.args[0], ctx)?;
                let upper = self.eval_scalar(&call.args[1], ctx)?;
                let Some(Expr::Selector(sel)) = call.args.get(2) else {
                    return Err(Error::Validation(
                        "histogram_fraction() requires a histogram selector".into(),
                    ));
                };
                let wins = self.hist_selector_windows(sel, ctx)?;
                let out: Vec<(crate::ast::SharedLabels, f64)> = wins
                    .into_iter()
                    .map(|(labels, h)| (labels, h.fraction(lower, upper)))
                    .collect();
                Ok(InstantVector { series: out })
            }
            "histogram_quantile" => {
                // Dual-mode: over le-bucket series (classic) OR over native
                // OTLP histogram selectors (explicit bounds).
                if let Some(Expr::Selector(_)) = call.args.get(1) {
                    if let Some(hist) = self.hist_data {
                        if let Some(Expr::Selector(sel)) = call.args.get(1) {
                            let q = self.eval_scalar(&call.args[0], ctx)?;
                            // If the referenced name has native histogram
                            // samples, prefer them; otherwise fall through
                            // to the classic le-bucket path.
                            let has_native = sel
                                .metric_name
                                .as_ref()
                                .map(|n| hist.contains_key(n))
                                .unwrap_or(false);
                            if has_native {
                                let wins = self.hist_selector_windows(sel, ctx)?;
                                let out: Vec<(crate::ast::SharedLabels, f64)> = wins
                                    .into_iter()
                                    .filter_map(|(labels, h)| {
                                        let v = h.quantile(q);
                                        if v.is_nan() {
                                            None
                                        } else {
                                            Some((labels, v))
                                        }
                                    })
                                    .collect();
                                return Ok(InstantVector { series: out });
                            }
                        }
                    }
                }
                self.eval_histogram_quantile(call, ctx)
            }
            other => Err(Error::Validation(format!(
                "unknown function {other:?} (only the documented subset is supported)"
            ))),
        }
    }

    fn eval_absent(&self, call: &CallExpr, ctx: EvalContext) -> Result<InstantVector> {
        // If the arg vector is empty, emit 1 with the selector's equality
        // matchers as labels (Prometheus behaviour).
        let v = self.eval(&call.args[0], ctx)?;
        if v.series.is_empty() {
            if let Expr::Selector(sel) = &call.args[0] {
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
        Ok(InstantVector::default())
    }

    fn eval_label_replace(&self, call: &CallExpr, ctx: EvalContext) -> Result<InstantVector> {
        // label_replace(v, dst, replacement, src, regex)
        let v = self.eval(&call.args[0], ctx)?;
        let dst = self.eval_string(&call.args[1], ctx)?;
        let replacement = self.eval_string(&call.args[2], ctx)?;
        let src = self.eval_string(&call.args[3], ctx)?;
        let regex = self.eval_string(&call.args[4], ctx)?;
        let re = regex::Regex::new(&regex)
            .map_err(|e| Error::Validation(format!("label_replace regex: {e}")))?;
        let mut out = Vec::with_capacity(v.series.len());
        for (labels, val) in v.series {
            let src_val = labels.get(&src).map(|s| s.to_string()).unwrap_or_default();
            if let Some(caps) = re.captures(&src_val) {
                let mut rep = replacement.clone();
                for (i, cap) in caps.iter().enumerate().skip(1) {
                    if let Some(m) = cap {
                        rep = rep.replace(&format!("${i}"), m.as_str());
                    }
                }
                // label_replace genuinely changes the labels, so this is a new
                // series identity and must get a fresh Arc.
                let mut new_labels = (*labels).clone();
                new_labels = new_labels
                    .merge(&LabelSet::try_from_iter(vec![(dst.clone(), rep)]).unwrap_or_default());
                out.push((crate::ast::shared_labels(new_labels), val));
            } else {
                out.push((labels, val));
            }
        }
        Ok(InstantVector { series: out })
    }

    fn eval_label_join(&self, call: &CallExpr, ctx: EvalContext) -> Result<InstantVector> {
        // label_join(v, dst, sep, src1, src2, ...)
        let v = self.eval(&call.args[0], ctx)?;
        let dst = self.eval_string(&call.args[1], ctx)?;
        let sep = self.eval_string(&call.args[2], ctx)?;
        let srcs: Vec<String> = call.args[3..]
            .iter()
            .map(|a| self.eval_string(a, ctx))
            .collect::<Result<_>>()?;
        let mut out = Vec::with_capacity(v.series.len());
        for (labels, val) in v.series {
            let joined: Vec<String> = srcs
                .iter()
                .map(|s| labels.get(s).map(|x| x.to_string()).unwrap_or_default())
                .collect();
            // Genuinely new labels: clone the existing set, then insert.
            let mut new_labels = (*labels).clone();
            new_labels = new_labels.merge(
                &LabelSet::try_from_iter(vec![(dst.clone(), joined.join(&sep))])
                    .unwrap_or_default(),
            );
            out.push((crate::ast::shared_labels(new_labels), val));
        }
        Ok(InstantVector { series: out })
    }

    fn eval_label_del(&self, call: &CallExpr, ctx: EvalContext) -> Result<InstantVector> {
        // label_del(v, label1, label2, ...): removes the named labels.
        let v = self.eval(&call.args[0], ctx)?;
        let dels: Vec<String> = call.args[1..]
            .iter()
            .map(|a| self.eval_string(a, ctx))
            .collect::<Result<_>>()?;
        let mut out = Vec::with_capacity(v.series.len());
        for (labels, val) in v.series {
            let kept = labels
                .iter()
                .filter(|(k, _)| !dels.contains(&k.to_string()))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>();
            out.push((
                crate::ast::shared_labels(LabelSet::try_from_iter(kept).unwrap_or_default()),
                val,
            ));
        }
        Ok(InstantVector { series: out })
    }

    fn eval_histogram_quantile(&self, call: &CallExpr, ctx: EvalContext) -> Result<InstantVector> {
        let q = self.eval_scalar(&call.args[0], ctx)?;
        let v = self.eval(&call.args[1], ctx)?;
        // Group series by all labels except le; collect (le, value).
        let mut hists: BTreeMap<LabelSet, Vec<(f64, f64)>> = BTreeMap::new();
        for (labels, val) in v.series {
            let le = labels
                .get("le")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(f64::INFINITY);
            // Drop the `le` bucket label in one pass; the previous
            // merge-per-label loop was O(L² log L) per series per step.
            let rest = labels.filtered(|k, _| k != "le");
            hists.entry(rest).or_default().push((le, val));
        }
        let mut out = Vec::new();
        for (labels, mut buckets) in hists {
            buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            if let Some(qv) = quantile_from_buckets(q, &buckets) {
                // Prometheus clears the metric name for histogram_quantile
                // results, which `filtered` does in one pass.
                let clean = labels.filtered(|k, _| k != "__name__");
                out.push((crate::ast::shared_labels(clean), qv));
            }
        }
        Ok(InstantVector { series: out })
    }
}
