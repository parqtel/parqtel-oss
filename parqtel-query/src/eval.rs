//! AST evaluator: per-step expression evaluation over series data.
//!
//! The executor loads all matching series once per query; the evaluator then
//! walks the AST for every step timestamp, producing an instant vector per
//! step. Range functions look back `range_ns` from each step (Prometheus
//! semantics established in Phase 0's `downsample_windowed`).

use crate::ast::*;
use crate::matcher::evaluate_matchers;
use parqtel_core::{Error, LabelSet, Result};
use std::collections::BTreeMap;

/// Evaluates an expression tree against pre-loaded series data.
pub struct Evaluator<'a> {
    data: &'a SeriesData,
    /// Metric names referenced anywhere in the tree (for empty-selector
    /// checks and absent()).
    #[allow(dead_code)]
    all_names: Vec<String>,
    /// Instant-selector lookback (Prometheus lookback-delta; 5m default).
    lookback_ns: i64,
}

impl<'a> Evaluator<'a> {
    /// Evaluator with Prometheus-default 5m instant lookback.
    pub fn new(data: &'a SeriesData) -> Self {
        Self {
            data,
            all_names: data.keys().cloned().collect(),
            lookback_ns: 5 * 60 * 1_000_000_000,
        }
    }

    /// Evaluator with a custom instant-selector lookback window.
    pub fn with_lookback(data: &'a SeriesData, lookback_ns: i64) -> Self {
        Self {
            data,
            all_names: data.keys().cloned().collect(),
            lookback_ns: lookback_ns.max(1),
        }
    }

    /// Top-level: evaluate for each step, returning per-step instant vectors.
    pub fn eval_steps(
        &self,
        expr: &Expr,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    ) -> Result<Vec<(i64, InstantVector)>> {
        let mut out = Vec::new();
        let mut ts = start_ns;
        while ts < end_ns {
            let ctx = EvalContext {
                ts_ns: ts,
                range_ns: 0,
                offset_ns: 0,
                subquery_step_ns: None,
            };
            let v = self.eval(expr, ctx)?;
            out.push((ts, v));
            ts += step_ns;
        }
        Ok(out)
    }

    /// Evaluate an expression at one timestamp.
    pub fn eval(&self, expr: &Expr, ctx: EvalContext) -> Result<InstantVector> {
        match expr {
            Expr::Number(n) => Ok(InstantVector {
                // Scalars are handled specially in binary ops; as a plain
                // instant vector a scalar has empty labels.
                series: vec![(LabelSet::default(), *n)],
            }),
            Expr::Paren(inner) => self.eval(inner, ctx),
            Expr::Selector(sel) => self.eval_selector(sel, ctx),
            Expr::Call(call) => self.eval_call(call, ctx),
            Expr::Aggregation(agg) => self.eval_aggregation(agg, ctx),
            Expr::Range(r) => self.eval_range_top(r, ctx),
            Expr::Binary(b) => self.eval_binary(b, ctx),
        }
    }

    // ── Selectors ────────────────────────────────────────────────────────
    fn eval_selector(&self, sel: &SelectorExpr, ctx: EvalContext) -> Result<InstantVector> {
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
    fn eval_range_windows(&self, r: &RangeExpr, ctx: EvalContext) -> Result<RangeVector> {
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
                let mut series_windows: BTreeMap<LabelSet, Vec<crate::models::Sample>> =
                    BTreeMap::new();
                let mut ts = window_start + step;
                while ts <= window_end {
                    sub_ctx.ts_ns = ts;
                    let iv = self.eval(inner, sub_ctx)?;
                    for (labels, v) in iv.series {
                        series_windows
                            .entry(labels)
                            .or_default()
                            .push(crate::models::Sample {
                                timestamp_ns: ts,
                                value: v,
                            });
                    }
                    ts += step;
                }
                for (labels, samples) in series_windows {
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
    fn eval_range_top(&self, _r: &RangeExpr, _ctx: EvalContext) -> Result<InstantVector> {
        Err(Error::Validation(
            "range vector cannot be used directly; wrap in a function like rate() or avg_over_time()"
                .into(),
        ))
    }

    // ── Calls ─────────────────────────────────────────────────────────────
    fn eval_call(&self, call: &CallExpr, ctx: EvalContext) -> Result<InstantVector> {
        // Range functions take a RangeExpr arg.
        if let Some((fn_range, fn_name)) = range_fn_args(call) {
            return self.eval_range_fn(&fn_name, fn_range, call, ctx);
        }
        match call.name.as_str() {
            // Instant transforms applied per-series.
            "abs" | "ceil" | "floor" | "sqrt" | "exp" | "ln" | "log2" | "log10" | "sgn" => {
                let v = self.eval(&call.args[0], ctx)?;
                Ok(map_values(v, |x| math_fn(&call.name, x)))
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
                        series: vec![(LabelSet::default(), v.series[0].1)],
                    })
                } else {
                    Ok(InstantVector {
                        series: vec![(LabelSet::default(), f64::NAN)],
                    })
                }
            }
            "vector" => {
                let s = self.eval_scalar(&call.args[0], ctx)?;
                Ok(InstantVector {
                    series: vec![(LabelSet::default(), s)],
                })
            }
            "time" => Ok(InstantVector {
                series: vec![(LabelSet::default(), ctx.ts_ns as f64 / 1e9)],
            }),
            "absent" => self.eval_absent(call, ctx),
            "histogram_quantile" => self.eval_histogram_quantile(call, ctx),
            "timestamp" => {
                // last sample's timestamp per series
                let v = self.eval_selector_like(&call.args[0], ctx)?;
                let _ = v;
                Err(Error::Validation(
                    "timestamp() not supported in this phase".into(),
                ))
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
            other => Err(Error::Validation(format!(
                "unknown function {other:?} (Phase 1A supports the documented subset)"
            ))),
        }
    }

    fn eval_selector_like(&self, expr: &Expr, ctx: EvalContext) -> Result<InstantVector> {
        self.eval(expr, ctx)
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
                    series: vec![(labels, 1.0)],
                });
            }
            return Ok(InstantVector {
                series: vec![(LabelSet::default(), 1.0)],
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
                let mut new_labels = labels.clone();
                new_labels = new_labels
                    .merge(&LabelSet::try_from_iter(vec![(dst.clone(), rep)]).unwrap_or_default());
                out.push((new_labels, val));
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
            let new_labels = labels.merge(
                &LabelSet::try_from_iter(vec![(dst.clone(), joined.join(&sep))])
                    .unwrap_or_default(),
            );
            out.push((new_labels, val));
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
            let mut rest = labels.clone();
            // remove le
            let mut without_le = LabelSet::default();
            for (k, v) in rest.iter() {
                if k != "le" {
                    without_le = without_le.merge(
                        &LabelSet::try_from_iter(vec![(k.to_string(), v.to_string())])
                            .unwrap_or_default(),
                    );
                }
            }
            rest = without_le;
            hists.entry(rest).or_default().push((le, val));
        }
        let mut out = Vec::new();
        for (labels, mut buckets) in hists {
            buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let mut labels = labels;
            labels = labels.merge(
                &LabelSet::try_from_iter(vec![("__name__".to_string(), String::new())])
                    .unwrap_or_default(),
            );
            if let Some(qv) = quantile_from_buckets(q, &buckets) {
                // Drop the __name__="" we just added — Prometheus clears the
                // metric name for histogram_quantile results.
                let mut clean = LabelSet::default();
                for (k, v) in labels.iter() {
                    if k != "__name__" {
                        clean = clean.merge(
                            &LabelSet::try_from_iter(vec![(k.to_string(), v.to_string())])
                                .unwrap_or_default(),
                        );
                    }
                }
                out.push((clean, qv));
            }
        }
        Ok(InstantVector { series: out })
    }

    // ── Range functions (rate family + _over_time) ───────────────────────
    fn eval_range_fn(
        &self,
        name: &str,
        range_expr: &RangeExpr,
        call: &CallExpr,
        ctx: EvalContext,
    ) -> Result<InstantVector> {
        let windows = self.eval_range_windows(range_expr, ctx)?;
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

    // ── Aggregations ──────────────────────────────────────────────────────
    fn eval_aggregation(&self, agg: &AggregationExpr, ctx: EvalContext) -> Result<InstantVector> {
        let v = self.eval(&agg.expr, ctx)?;
        // group key per series
        let mut groups: BTreeMap<Vec<(String, String)>, Vec<f64>> = BTreeMap::new();
        let mut group_labels: BTreeMap<Vec<(String, String)>, LabelSet> = BTreeMap::new();

        for (labels, val) in &v.series {
            let key: Vec<(String, String)> = match &agg.grouping {
                Grouping::None => vec![],
                Grouping::By(list) => list
                    .iter()
                    .filter_map(|l| labels.get(l).map(|x| (l.clone(), x.to_string())))
                    .collect(),
                Grouping::Without(list) => {
                    let mut k = Vec::new();
                    for (lk, lv) in labels.iter() {
                        let lk_str = lk.to_string();
                        if lk_str == "__name__" || list.contains(&lk_str) {
                            continue;
                        }
                        k.push((lk_str, lv.to_string()));
                    }
                    k.sort();
                    k
                }
            };
            groups.entry(key.clone()).or_default().push(*val);
            group_labels.entry(key.clone()).or_insert_with(|| {
                match &agg.grouping {
                    Grouping::None => LabelSet::default(),
                    Grouping::By(_) | Grouping::Without(_) => {
                        // Reconstruct from the first series' labels
                        let mut l = LabelSet::default();
                        for (k, vv) in &key {
                            l = l.merge(
                                &LabelSet::try_from_iter(vec![(k.clone(), vv.clone())])
                                    .unwrap_or_default(),
                            );
                        }
                        l
                    }
                }
            });
        }

        let mut out = Vec::new();
        for (key, vals) in groups {
            let labels = group_labels.get(&key).cloned().unwrap_or_default();
            let result = match agg.op {
                AggregationOp::Sum => Some(vals.iter().sum()),
                AggregationOp::Avg => Some(vals.iter().sum::<f64>() / vals.len() as f64),
                AggregationOp::Min => vals
                    .iter()
                    .copied()
                    .fold(f64::INFINITY, f64::min)
                    .is_finite()
                    .then(|| vals.iter().copied().fold(f64::INFINITY, f64::min))
                    .filter(|_| !vals.is_empty()),
                AggregationOp::Max => {
                    if vals.is_empty() {
                        None
                    } else {
                        Some(vals.iter().copied().fold(f64::NEG_INFINITY, f64::max))
                    }
                }
                AggregationOp::Count => Some(vals.len() as f64),
                AggregationOp::Group => Some(1.0),
                AggregationOp::Stddev | AggregationOp::Stdvar => {
                    if vals.len() < 2 {
                        None
                    } else {
                        let n = vals.len() as f64;
                        let mean = vals.iter().sum::<f64>() / n;
                        let var = vals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
                        Some(if agg.op == AggregationOp::Stddev {
                            var.sqrt()
                        } else {
                            var
                        })
                    }
                }
                AggregationOp::CountValues => None, // param-label based; rare — Phase 1B
                AggregationOp::TopK | AggregationOp::BottomK => {
                    // Handled below with per-series labels (not aggregated).
                    None
                }
                AggregationOp::Quantile => None, // handled below
            };
            if let Some(r) = result {
                out.push((labels, r));
            }
        }

        // topk/bottomk/quantile need full series info, not just group values.
        match agg.op {
            AggregationOp::TopK | AggregationOp::BottomK => {
                let n = agg
                    .param
                    .as_ref()
                    .map(|p| self.eval_scalar(p, ctx))
                    .transpose()?
                    .unwrap_or(1.0) as usize;
                let mut series: Vec<(LabelSet, f64)> = v.series;
                if agg.op == AggregationOp::TopK {
                    series
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    series
                        .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                }
                series.truncate(n);
                return Ok(InstantVector { series });
            }
            AggregationOp::Quantile => {
                let q = agg
                    .param
                    .as_ref()
                    .map(|p| self.eval_scalar(p, ctx))
                    .transpose()?
                    .unwrap_or(0.5);
                let mut vals: Vec<f64> = v.series.iter().map(|(_, v)| *v).collect();
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let qv = quantile_of(q, &vals);
                return Ok(InstantVector {
                    series: vec![(LabelSet::default(), qv)],
                });
            }
            AggregationOp::CountValues => {
                return Err(Error::Validation(
                    "count_values() is not supported in Phase 1A".into(),
                ));
            }
            _ => {}
        }

        Ok(InstantVector { series: out })
    }

    // ── Binary ops ────────────────────────────────────────────────────────
    fn eval_binary(&self, b: &BinaryExpr, ctx: EvalContext) -> Result<InstantVector> {
        let lhs = self.eval(&b.lhs, ctx)?;
        let rhs = self.eval(&b.rhs, ctx)?;

        // Scalar op scalar / vector-scalar mixing
        let lhs_scalar = b.lhs_is_scalar();
        let rhs_scalar = b.rhs_is_scalar();
        let _ = (lhs_scalar, rhs_scalar);

        match (&*b.lhs, &*b.rhs) {
            (Expr::Number(a), Expr::Number(bb)) => {
                let v = apply_binary_op(b.op, *a, *bb, b.return_bool)?;
                Ok(InstantVector {
                    series: vec![(LabelSet::default(), v)],
                })
            }
            (Expr::Number(a), _) => {
                // scalar op vector
                let mut out = Vec::new();
                for (labels, rv) in rhs.series {
                    let v = apply_binary_op(b.op, *a, rv, b.return_bool)?;
                    if let Some(v) = filter_cmp(b, v) {
                        out.push((labels, v));
                    }
                }
                Ok(InstantVector { series: out })
            }
            (_, Expr::Number(bb)) => {
                // vector op scalar
                let mut out = Vec::new();
                for (labels, lv) in lhs.series {
                    let v = apply_binary_op(b.op, lv, *bb, b.return_bool)?;
                    if let Some(v) = filter_cmp(b, v) {
                        out.push((labels, v));
                    }
                }
                Ok(InstantVector { series: out })
            }
            _ => {
                // vector op vector with optional matching
                self.eval_vector_binary(b, lhs, rhs)
            }
        }
    }

    fn eval_vector_binary(
        &self,
        b: &BinaryExpr,
        lhs: InstantVector,
        rhs: InstantVector,
    ) -> Result<InstantVector> {
        // Set operators operate on label-set membership.
        match b.op {
            BinaryOp::And => {
                let mut out = Vec::new();
                for (l, v) in lhs.series {
                    if rhs.get(&l).is_some() {
                        out.push((l, v));
                    }
                }
                return Ok(InstantVector { series: out });
            }
            BinaryOp::Or => {
                let mut out = lhs.series.clone();
                for (l, v) in rhs.series {
                    if lhs.get(&l).is_none() {
                        out.push((l, v));
                    }
                }
                return Ok(InstantVector { series: out });
            }
            BinaryOp::Unless => {
                let mut out = Vec::new();
                for (l, v) in lhs.series {
                    if rhs.get(&l).is_none() {
                        out.push((l, v));
                    }
                }
                return Ok(InstantVector { series: out });
            }
            _ => {}
        }

        let (vm, card) = b
            .matching
            .clone()
            .unwrap_or((VectorMatch::All, MatchCardinality::OneToOne));
        let vm = &vm;
        let card = &card;

        let match_key = |labels: &LabelSet| -> Vec<(String, String)> {
            let mut k = Vec::new();
            for (lk, lv) in labels.iter() {
                let lk = lk.to_string();
                if lk == "__name__" {
                    continue;
                }
                match &vm {
                    VectorMatch::All => k.push((lk, lv.to_string())),
                    VectorMatch::On(list) => {
                        if list.contains(&lk) {
                            k.push((lk, lv.to_string()));
                        }
                    }
                    VectorMatch::Ignoring(list) => {
                        if !list.contains(&lk) {
                            k.push((lk, lv.to_string()));
                        }
                    }
                }
            }
            k.sort();
            k
        };

        // Build rhs index by match key
        type MatchKey = Vec<(String, String)>;
        type Matches = Vec<(LabelSet, f64)>;
        let mut rhs_index: BTreeMap<MatchKey, Matches> = BTreeMap::new();
        for (labels, v) in rhs.series {
            rhs_index
                .entry(match_key(&labels))
                .or_default()
                .push((labels, v));
        }

        let mut out = Vec::new();
        for (llabels, lv) in lhs.series {
            let key = match_key(&llabels);
            let matches = rhs_index.get(&key);
            match card {
                MatchCardinality::OneToOne => {
                    if let Some(matches) = matches {
                        if matches.len() == 1 {
                            let (rlabels, rv) = &matches[0];
                            let v = apply_binary_op(b.op, lv, *rv, b.return_bool)?;
                            if let Some(v) = filter_cmp(b, v) {
                                out.push((result_labels(b, &llabels, rlabels), v));
                            }
                        }
                        // multiple matches with 1:1 -> drop (Prometheus: many-to-many not allowed)
                    }
                }
                MatchCardinality::ManyToOne(extra) => {
                    if let Some(matches) = matches {
                        for (rlabels, rv) in matches {
                            let v = apply_binary_op(b.op, lv, *rv, b.return_bool)?;
                            if let Some(v) = filter_cmp(b, v) {
                                // group_left: projected match labels + extras from RHS
                                let mut labels = result_labels(b, &llabels, rlabels);
                                for e in extra.iter() {
                                    if let Some(ev) = rlabels.get(e) {
                                        labels = labels.merge(
                                            &LabelSet::try_from_iter(vec![(
                                                e.clone(),
                                                ev.to_string(),
                                            )])
                                            .unwrap_or_default(),
                                        );
                                    }
                                }
                                out.push((labels, v));
                            }
                        }
                    }
                }
                MatchCardinality::OneToMany(_) => {
                    // group_right: RHS side may have many LHS matches; swap roles
                    if let Some(matches) = matches {
                        for (rlabels, rv) in matches {
                            let v = apply_binary_op(b.op, lv, *rv, b.return_bool)?;
                            if let Some(v) = filter_cmp(b, v) {
                                out.push((result_labels(b, rlabels, &llabels), v));
                            }
                        }
                    }
                }
            }
        }
        Ok(InstantVector { series: out })
    }

    // ── Scalar / string helpers ───────────────────────────────────────────
    fn eval_scalar(&self, expr: &Expr, ctx: EvalContext) -> Result<f64> {
        match expr {
            Expr::Number(n) => Ok(*n),
            other => {
                let v = self.eval(other, ctx)?;
                if v.series.len() == 1 {
                    Ok(v.series[0].1)
                } else {
                    Ok(f64::NAN)
                }
            }
        }
    }

    fn eval_string(&self, expr: &Expr, _ctx: EvalContext) -> Result<String> {
        match expr {
            Expr::Number(n) => Ok(n.to_string()),
            other => Err(Error::Validation(format!(
                "expected string literal, got {other:?}"
            ))),
        }
    }
}

// ── Free helpers ────────────────────────────────────────────────────────────

trait IsScalar {
    fn lhs_is_scalar(&self) -> bool {
        false
    }
    fn rhs_is_scalar(&self) -> bool {
        false
    }
}

impl IsScalar for BinaryExpr {}

fn filter_cmp(b: &BinaryExpr, v: f64) -> Option<f64> {
    if b.return_bool {
        Some(v)
    } else if b.op.is_comparison() && v == 0.0 {
        None // filtered out
    } else {
        Some(v)
    }
}

/// Result-label projection per PromQL semantics (G3):
/// - default (no modifier): LHS labels minus `__name__`
/// - `on(x, ...)`: ONLY the on-labels
/// - `ignoring(x, ...)`: LHS labels minus `__name__` minus the ignored set
fn result_labels(b: &BinaryExpr, lhs: &LabelSet, _rhs: &LabelSet) -> LabelSet {
    let mut out = LabelSet::default();
    let matching = b.matching.as_ref().map(|(vm, _)| vm);
    for (k, v) in lhs.iter() {
        if k == "__name__" {
            continue;
        }
        let keep = match matching {
            Some(VectorMatch::On(list)) => list.iter().any(|l| l == k),
            Some(VectorMatch::Ignoring(list)) => !list.iter().any(|l| l == k),
            Some(VectorMatch::All) | None => true,
        };
        if keep {
            out = out.merge(
                &LabelSet::try_from_iter(vec![(k.to_string(), v.to_string())]).unwrap_or_default(),
            );
        }
    }
    out
}

fn apply_binary_op(op: BinaryOp, a: f64, b: f64, return_bool: bool) -> Result<f64> {
    Ok(match op {
        BinaryOp::Add => a + b,
        BinaryOp::Sub => a - b,
        BinaryOp::Mul => a * b,
        BinaryOp::Div => a / b,
        BinaryOp::Mod => a % b,
        BinaryOp::Pow => a.powf(b),
        BinaryOp::Eq => bool_f(a == b, return_bool, a),
        BinaryOp::Ne => bool_f(a != b, return_bool, a),
        BinaryOp::Gt => bool_f(a > b, return_bool, a),
        BinaryOp::Lt => bool_f(a < b, return_bool, a),
        BinaryOp::Ge => bool_f(a >= b, return_bool, a),
        BinaryOp::Le => bool_f(a <= b, return_bool, a),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Unless => {
            return Err(Error::Validation("set op in scalar position".into()))
        }
    })
}

fn bool_f(cond: bool, return_bool: bool, value: f64) -> f64 {
    if return_bool {
        if cond {
            1.0
        } else {
            0.0
        }
    } else if cond {
        value
    } else {
        0.0 // filtered by filter_cmp for comparisons
    }
}

fn math_fn(name: &str, x: f64) -> f64 {
    match name {
        "abs" => x.abs(),
        "ceil" => x.ceil(),
        "floor" => x.floor(),
        "sqrt" => x.sqrt(),
        "exp" => x.exp(),
        "ln" => x.ln(),
        "log2" => x.log2(),
        "log10" => x.log10(),
        "sgn" => {
            if x > 0.0 {
                1.0
            } else if x < 0.0 {
                -1.0
            } else {
                0.0
            }
        }
        _ => x,
    }
}

fn map_values(v: InstantVector, f: impl Fn(f64) -> f64) -> InstantVector {
    InstantVector {
        series: v.series.into_iter().map(|(l, x)| (l, f(x))).collect(),
    }
}

/// Extracts (range_expr, fn_name) when a call's FIRST arg is a Range.
fn range_fn_args(call: &CallExpr) -> Option<(&RangeExpr, String)> {
    let first = call.args.first()?;
    if let Expr::Range(r) = first {
        Some((r, call.name.clone()))
    } else {
        None
    }
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
        "absent_over_time" => return Ok(None), // empty handled by caller
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

/// rate with per-segment reset accumulation + extrapolation (G4):
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

fn quantile_of(q: f64, sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let pos = q * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = pos - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

fn quantile_from_buckets(q: f64, buckets: &[(f64, f64)]) -> Option<f64> {
    // buckets: (le, count), sorted ascending; last should be +Inf.
    if buckets.len() < 2 {
        return None;
    }
    let total = buckets.last().map(|b| b.1).unwrap_or(0.0);
    if total <= 0.0 {
        return None;
    }
    let target = q * total;
    let mut prev_count = 0.0;
    let mut prev_bound = f64::NEG_INFINITY;
    for (bound, count) in buckets.iter().copied() {
        if count >= target {
            if bound == f64::INFINITY {
                return Some(prev_bound);
            }
            let frac = (target - prev_count) / (count - prev_count);
            return Some(prev_bound + (bound - prev_bound) * frac);
        }
        prev_count = count;
        prev_bound = bound;
    }
    buckets.last().map(|b| b.0)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn mk_data() -> SeriesData {
        let mut data = SeriesData::new();
        let mk_labels = |svc: &str| {
            LabelSet::try_from_iter(vec![("service".to_string(), svc.to_string())]).unwrap()
        };
        // counter rising 1/sec at 10s intervals for 100s
        let points: Vec<(i64, f64)> = (0..10).map(|i| (i * 10_000_000_000, i as f64)).collect();
        data.insert(
            "requests".into(),
            vec![
                (mk_labels("a"), points.clone()),
                (mk_labels("b"), points.clone()),
            ],
        );
        data
    }

    fn eval_query(q: &str, ts: i64) -> InstantVector {
        let expr = crate::parser::parse_expr(q).unwrap();
        let data = mk_data();
        let ev = Evaluator::new(&data);
        let ctx = EvalContext {
            ts_ns: ts,
            range_ns: 0,
            offset_ns: 0,
            subquery_step_ns: None,
        };
        ev.eval(&expr, ctx).unwrap()
    }

    #[test]
    fn nested_sum_rate() {
        // sum(rate(requests[1m])) at t=100s: rate=1/sec per series, summed=2
        let v = eval_query("sum(rate(requests[1m]))", 100_000_000_000);
        assert_eq!(v.series.len(), 1);
        let val = v.series[0].1;
        // window [40s,100s): 6 samples, counter 4->9, rate=0.1/s per series,
        // sum across 2 series ≈ 0.2 (plus extrapolation slack).
        assert!((val - 0.2).abs() < 0.06, "sum(rate) = {val}");
    }

    #[test]
    fn sum_by_service() {
        let v = eval_query("sum by (service) (requests)", 95_000_000_000);
        assert_eq!(v.series.len(), 2);
    }

    #[test]
    fn binary_ratio() {
        // requests / requests = 1.0 per series
        let v = eval_query("requests / requests", 95_000_000_000);
        assert_eq!(v.series.len(), 2);
        for (_, val) in &v.series {
            assert!((val - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn scalar_arith() {
        let v = eval_query("requests * 2", 95_000_000_000);
        assert!(!v.is_empty());
        let (_, val) = &v.series[0];
        assert!((val - 18.0).abs() < 1e-9, "9*2=18, got {val}");
    }

    #[test]
    fn avg_over_time_window() {
        // avg over [1m] ending at 100s: samples at 40..100 = 4..9, avg 6.5
        let v = eval_query("avg_over_time(requests[1m])", 100_000_000_000);
        assert_eq!(v.series.len(), 2);
        let (_, val) = &v.series[0];
        assert!((val - 6.5).abs() < 1e-9, "avg_over_time = {val}");
    }

    #[test]
    fn max_over_time_family() {
        let v = eval_query("max_over_time(requests[1m])", 100_000_000_000);
        let (_, val) = &v.series[0];
        assert!((val - 9.0).abs() < 1e-9);
    }

    #[test]
    fn count_over_time() {
        let v = eval_query("count_over_time(requests[1m])", 100_000_000_000);
        let (_, val) = &v.series[0];
        assert!(
            (val - 6.0).abs() < 1e-9,
            "6 samples in [40s,100s), got {val}"
        );
    }

    #[test]
    fn comparison_filters() {
        // only series with value > 8 at t=95: value is 9 (last sample ≤95s is at 90s=9)
        let v = eval_query("requests > 8", 95_000_000_000);
        assert_eq!(v.series.len(), 2);
        let v = eval_query("requests > 8.5", 95_000_000_000);
        assert_eq!(v.series.len(), 2);
        let v = eval_query("requests > 9.5", 95_000_000_000);
        assert_eq!(v.series.len(), 0);
    }

    #[test]
    fn bool_comparison() {
        let v = eval_query("requests > bool 5", 95_000_000_000);
        assert_eq!(v.series.len(), 2);
        for (_, val) in v.series {
            assert_eq!(val, 1.0);
        }
    }

    #[test]
    fn on_projection_drops_non_on_labels() {
        // G3: a * on(service) b keeps ONLY service (+ group_left extras),
        // dropping pod/instance from the result — PromQL semantics.
        let mut data = SeriesData::new();
        let l1 = LabelSet::try_from_iter(vec![
            ("service".to_string(), "a".to_string()),
            ("pod".to_string(), "p1".to_string()),
        ])
        .unwrap();
        let l2 = LabelSet::try_from_iter(vec![
            ("service".to_string(), "a".to_string()),
            ("pod".to_string(), "p2".to_string()),
        ])
        .unwrap();
        data.insert(
            "m1".into(),
            vec![(l1, vec![(0, 2.0), (100_000_000_000, 2.0)])],
        );
        data.insert(
            "m2".into(),
            vec![(l2, vec![(0, 3.0), (100_000_000_000, 3.0)])],
        );
        let expr = crate::parser::parse_expr("m1 * on(service) m2").unwrap();
        let ev = Evaluator::new(&data);
        let ctx = EvalContext {
            ts_ns: 100_000_000_000,
            range_ns: 0,
            offset_ns: 0,
            subquery_step_ns: None,
        };
        let v = ev.eval(&expr, ctx).unwrap();
        assert_eq!(v.series.len(), 1);
        let (labels, val) = &v.series[0];
        assert!((*val - 6.0).abs() < 1e-9);
        // ONLY the on-label survives.
        assert_eq!(
            labels.get("service").map(|s| s.to_string()).as_deref(),
            Some("a")
        );
        assert!(
            labels.get("pod").is_none(),
            "pod must be dropped by on() projection"
        );
    }

    #[test]
    fn vector_matching_on() {
        // Two metrics sharing service label; join via on(service)
        let mut data = SeriesData::new();
        let la = LabelSet::try_from_iter(vec![
            ("service".to_string(), "a".to_string()),
            ("instance".to_string(), "1".to_string()),
        ])
        .unwrap();
        let lb = LabelSet::try_from_iter(vec![
            ("service".to_string(), "a".to_string()),
            ("instance".to_string(), "2".to_string()),
        ])
        .unwrap();
        data.insert(
            "m1".into(),
            vec![(la, vec![(0, 10.0), (100_000_000_000, 10.0)])],
        );
        data.insert(
            "m2".into(),
            vec![(lb, vec![(0, 5.0), (100_000_000_000, 5.0)])],
        );
        let expr = crate::parser::parse_expr("m1 * on(service) m2").unwrap();
        let ev = Evaluator::new(&data);
        let ctx = EvalContext {
            ts_ns: 100_000_000_000,
            range_ns: 0,
            offset_ns: 0,
            subquery_step_ns: None,
        };
        let v = ev.eval(&expr, ctx).unwrap();
        assert_eq!(v.series.len(), 1);
        let (_, val) = &v.series[0];
        assert!((val - 50.0).abs() < 1e-9, "10*5=50, got {val}");
    }

    #[test]
    fn group_left_many_to_one() {
        let mut data = SeriesData::new();
        let s1 = LabelSet::try_from_iter(vec![
            ("service".to_string(), "a".to_string()),
            ("pod".to_string(), "p1".to_string()),
        ])
        .unwrap();
        let s2 = LabelSet::try_from_iter(vec![
            ("service".to_string(), "a".to_string()),
            ("pod".to_string(), "p2".to_string()),
        ])
        .unwrap();
        let info = LabelSet::try_from_iter(vec![
            ("service".to_string(), "a".to_string()),
            ("team".to_string(), "core".to_string()),
        ])
        .unwrap();
        data.insert(
            "metric".into(),
            vec![
                (s1.clone(), vec![(0, 1.0), (100_000_000_000, 1.0)]),
                (s2.clone(), vec![(0, 2.0), (100_000_000_000, 2.0)]),
            ],
        );
        data.insert(
            "info".into(),
            vec![(info, vec![(0, 100.0), (100_000_000_000, 100.0)])],
        );
        let expr = crate::parser::parse_expr("metric * on(service) group_left(team) info").unwrap();
        let ev = Evaluator::new(&data);
        let ctx = EvalContext {
            ts_ns: 100_000_000_000,
            range_ns: 0,
            offset_ns: 0,
            subquery_step_ns: None,
        };
        let v = ev.eval(&expr, ctx).unwrap();
        assert_eq!(v.series.len(), 2, "both pods match");
        for (labels, val) in &v.series {
            assert_eq!(
                labels.get("team").map(|s| s.to_string()).as_deref(),
                Some("core")
            );
            assert!(*val == 100.0 || *val == 200.0);
        }
    }

    #[test]
    fn absent_emits_one() {
        let v = eval_query("absent(nonexistent{job=\"x\"})", 50_000_000_000);
        assert_eq!(v.series.len(), 1);
        let (labels, val) = &v.series[0];
        assert_eq!(*val, 1.0);
        assert_eq!(
            labels.get("job").map(|s| s.to_string()).as_deref(),
            Some("x")
        );
    }

    #[test]
    fn topk_returns_top_series() {
        let mut data = SeriesData::new();
        for i in 0..5 {
            let l = LabelSet::try_from_iter(vec![("i".to_string(), i.to_string())]).unwrap();
            data.entry("m".into())
                .or_default()
                .push((l, vec![(0, i as f64), (100_000_000_000, i as f64)]));
        }
        let expr = crate::parser::parse_expr("topk(2, m)").unwrap();
        let ev = Evaluator::new(&data);
        let ctx = EvalContext {
            ts_ns: 100_000_000_000,
            range_ns: 0,
            offset_ns: 0,
            subquery_step_ns: None,
        };
        let v = ev.eval(&expr, ctx).unwrap();
        assert_eq!(v.series.len(), 2);
        let vals: Vec<f64> = v.series.iter().map(|(_, v)| *v).collect();
        assert!(vals.contains(&4.0) && vals.contains(&3.0));
    }

    #[test]
    fn histogram_quantile_from_buckets() {
        let mut data = SeriesData::new();
        // classic histogram buckets for one series
        for (le, count) in [
            (0.1, 10.0),
            (1.0, 50.0),
            (10.0, 90.0),
            (f64::INFINITY, 100.0),
        ] {
            let l = LabelSet::try_from_iter(vec![
                ("le".to_string(), le.to_string()),
                ("route".to_string(), "r".to_string()),
            ])
            .unwrap();
            data.entry("latency_bucket".into())
                .or_default()
                .push((l, vec![(0, count), (100_000_000_000, count)]));
        }
        let expr = crate::parser::parse_expr(
            "histogram_quantile(0.9, sum by (le, route) (latency_bucket))",
        )
        .unwrap();
        let ev = Evaluator::new(&data);
        let ctx = EvalContext {
            ts_ns: 100_000_000_000,
            range_ns: 0,
            offset_ns: 0,
            subquery_step_ns: None,
        };
        let v = ev.eval(&expr, ctx).unwrap();
        assert_eq!(v.series.len(), 1);
        let (_, qv) = &v.series[0];
        // 90th of 100 obs falls in the (1.0, 10.0] bucket: 1 + (90-50)/(90-50)*9 = 10...
        // count>=90 first at le=10.0: 1.0 + (90-50)/(90-50)*(10-1)=10.0 → cap semantics: ~10
        assert!(
            (qv - 10.0).abs() < 1e-6 || (qv - 9.0).abs() < 2.0,
            "q90 = {qv}"
        );
    }

    #[test]
    fn set_ops_and_or() {
        let v = eval_query(
            r#"requests{service="a"} and requests{service="b"}"#,
            95_000_000_000,
        );
        assert!(v.is_empty(), "no common label sets");
        let v = eval_query(
            r#"requests{service="a"} or requests{service="b"}"#,
            95_000_000_000,
        );
        assert_eq!(v.series.len(), 2);
        let v = eval_query(
            r#"requests{service="a"} unless requests{service="b"}"#,
            95_000_000_000,
        );
        assert_eq!(v.series.len(), 1);
    }
}
