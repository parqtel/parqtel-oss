//! Instant and range selector evaluation.

use super::Evaluator;
use crate::ast::*;
use crate::matcher::evaluate_matchers;
use parqtel_core::{Error, Result};

impl<'a> Evaluator<'a> {
    pub(super) fn eval_selector(
        &self,
        sel: &SelectorExpr,
        ctx: EvalContext,
    ) -> Result<InstantVector> {
        let mut out = Vec::new();
        let Some(name) = &sel.metric_name else {
            return Ok(InstantVector::default());
        };
        let Some(series) = self.data.get(name) else {
            return Ok(InstantVector::default());
        };
        let lookback = ctx.range_ns.max(self.lookback_ns);
        for (labels, points) in series {
            if !evaluate_matchers(&sel.matchers, labels, name) {
                continue;
            }
            // Instant selector: last sample within lookback ending at ts.
            let shifted = ctx.ts_ns - ctx.offset_ns;
            let idx = points.partition_point(|(t, _)| *t <= shifted);
            if idx == 0 {
                continue;
            }
            let (t, v) = points[idx - 1];
            if t < shifted - lookback {
                continue;
            }
            out.push((labels.clone(), v));
        }
        Ok(InstantVector { series: out })
    }

    /// Range evaluation result: per-series windows (used by _over_time and
    /// binary ops over ranges).
    pub(super) fn eval_range_windows(
        &self,
        r: &RangeExpr,
        ctx: EvalContext,
    ) -> Result<RangeVector> {
        let window_start = ctx.ts_ns - ctx.offset_ns - r.range_ns;
        let window_end = ctx.ts_ns - ctx.offset_ns;
        let mut out = Vec::new();
        match &*r.expr {
            Expr::Selector(sel) => {
                let Some(name) = &sel.metric_name else {
                    return Ok(out);
                };
                let Some(series) = self.data.get(name) else {
                    return Ok(out);
                };
                for (labels, points) in series {
                    if !evaluate_matchers(&sel.matchers, labels, name) {
                        continue;
                    }
                    let lo = points.partition_point(|(t, _)| *t < window_start);
                    let hi = points.partition_point(|(t, _)| *t <= window_end);
                    if hi > lo {
                        let samples: Vec<crate::models::Sample> = points[lo..hi]
                            .iter()
                            .map(|(t, v)| crate::models::Sample {
                                timestamp_ns: *t,
                                value: *v,
                            })
                            .collect();
                        out.push((labels.clone(), samples));
                    }
                }
                Ok(out)
            }
            // Subqueries: evaluate the inner expression at every sub-step
            // and collect the per-step instant vectors as synthetic samples.
            inner => {
                let step = r.step_ns.unwrap_or(60_000_000_000); // default 1m
                let mut sub_ctx = ctx;
                sub_ctx.subquery_step_ns = Some(step);
                // Cache of inner instant vectors per timestamp.
                // Keyed by fingerprint rather than by label set: an
                // `BTreeMap<LabelSet, _>` would compare whole label sets on
                // every lookup, and grouping by fingerprint keeps the first
                // occurrence's `Arc` so the window carries shared labels.
                let mut order: Vec<u64> = Vec::new();
                let mut series_windows: std::collections::HashMap<
                    u64,
                    (crate::ast::SharedLabels, Vec<crate::models::Sample>),
                > = std::collections::HashMap::new();
                let mut ts = window_start + step;
                while ts <= window_end {
                    sub_ctx.ts_ns = ts;
                    let iv = self.eval(inner, sub_ctx)?;
                    for (labels, v) in iv.series {
                        let fp = labels.fingerprint();
                        let slot = series_windows.entry(fp).or_insert_with(|| {
                            order.push(fp);
                            (labels, Vec::new())
                        });
                        slot.1.push(crate::models::Sample {
                            timestamp_ns: ts,
                            value: v,
                        });
                    }
                    ts += step;
                }
                for fp in order {
                    let (labels, samples) = series_windows.remove(&fp).unwrap_or_default();
                    if !samples.is_empty() {
                        out.push((labels, samples));
                    }
                }
                Ok(out)
            }
        }
    }

    /// A bare range at top level (e.g. `x[5m]`) is not an instant vector —
    /// Prometheus rejects it in instant queries. We support it inside
    /// functions only; at top level produce a clear error.
    pub(super) fn eval_range_top(
        &self,
        _r: &RangeExpr,
        _ctx: EvalContext,
    ) -> Result<InstantVector> {
        Err(Error::Validation(
            "range vector cannot be used directly; wrap in a function like rate() or avg_over_time()"
                .into(),
        ))
    }
}
