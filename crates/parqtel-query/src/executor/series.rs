//! Turning scanned points into series, and the helpers shared by the metric queries.

use crate::models::TimeSeries;
use parqtel_core::{LabelSet, MetricValue, Result};
use std::sync::Arc;

/// One metric's scanned points, grouped into series and ready to evaluate.
pub(super) struct BuiltSeries {
    pub(super) numeric: Vec<(crate::ast::SharedLabels, Vec<(i64, f64)>)>,
    pub(super) histograms: Vec<(crate::ast::SharedLabels, Vec<crate::ast::HistSample>)>,
    /// Series that matched, counted before any truncation.
    pub(super) total_series: usize,
    /// Points per volume bucket, over every matched series.
    pub(super) volume: [u64; 60],
}

/// Groups `raw` by series and applies the result limits.
///
/// Series come out in fingerprint order, so series ids are deterministic across
/// runs. Matchers were already applied by the scan.
pub(super) fn build_series_data(
    raw: Vec<parqtel_core::DataPoint>,
    start_ns: i64,
    window_ns: i64,
    max_series: usize,
    max_samples_per_series: usize,
) -> BuiltSeries {
    use ahash::AHashMap;

    // Points of one series share a label set, and every point of a block's
    // series shares the same handle, so the handle's address finds the series
    // without hashing its labels. A handle not seen before falls back to the
    // fingerprint, which also merges the same series coming from other blocks.
    struct Group {
        fp: u64,
        labels: parqtel_core::LabelSet,
        points: Vec<(i64, MetricValue)>,
    }
    let mut groups: Vec<Group> = Vec::new();
    let mut by_addr: AHashMap<usize, usize> = AHashMap::new();
    let mut by_fp: AHashMap<u64, usize> = AHashMap::new();
    for dp in raw {
        let addr = dp.labels.addr();
        let idx = match by_addr.get(&addr) {
            Some(i) => *i,
            None => {
                let fp = dp.labels.fingerprint();
                let i = *by_fp.entry(fp).or_insert_with(|| {
                    groups.push(Group {
                        fp,
                        labels: dp.labels.clone(),
                        points: Vec::new(),
                    });
                    groups.len() - 1
                });
                by_addr.insert(addr, i);
                i
            }
        };
        groups[idx].points.push((dp.timestamp_ns, dp.value));
    }
    groups.sort_by_key(|g| g.fp);

    let mut out = BuiltSeries {
        numeric: Vec::new(),
        histograms: Vec::new(),
        total_series: 0,
        volume: [0; 60],
    };
    // Series kept so far; a series counts once whether numeric or histogram.
    let mut kept = 0usize;
    for group in groups {
        // Counted as a matched series regardless of truncation, and before it,
        // so `total_series_count` reports what matched rather than what
        // survived.
        out.total_series += 1;
        // Count into the volume histogram regardless of series truncation.
        // Lookback samples timestamped before `start_ns` feed range-selector
        // windows, not the visible timeline, so they are excluded.
        if window_ns > 0 {
            for (ts, _) in &group.points {
                if *ts >= start_ns {
                    let bucket = ((*ts - start_ns) / window_ns).clamp(0, 59) as usize;
                    out.volume[bucket] += 1;
                }
            }
        }
        // The same result limits the plan path applies, so the two engines
        // cannot disagree about `total_series_count` or escape the configured
        // memory bound.
        if kept >= max_series {
            continue;
        }
        kept += 1;
        let mut pts = group.points;
        // Samples were appended in scan order (blocks, then buffer appends), so
        // keep only the oldest `max_samples_per_series` after sorting.
        pts.sort_by_key(|(t, _)| *t);
        if pts.len() > max_samples_per_series {
            pts.truncate(max_samples_per_series);
        }
        // Split histogram-valued samples into the structured channel; numeric
        // series keep the plain path.
        let mut hist_pts: Vec<crate::ast::HistSample> = Vec::new();
        let mut num_pts: Vec<(i64, f64)> = Vec::with_capacity(pts.len());
        for (t, v) in pts {
            match v {
                MetricValue::Histogram(h) => {
                    let h = *h;
                    hist_pts.push(crate::ast::HistSample {
                        timestamp_ns: t,
                        count: h.count,
                        sum: h.sum,
                        boundaries: h.boundaries,
                        counts: h.counts,
                    })
                }
                other => num_pts.push((t, v_to_f64(&other))),
            }
        }
        // One Arc per series, shared by every step that evaluates it: cloning it
        // per step is a refcount bump, where cloning the LabelSet was one
        // allocation per label.
        let labels = crate::ast::shared_labels(group.labels);
        if !hist_pts.is_empty() {
            out.histograms
                .push((std::sync::Arc::clone(&labels), hist_pts));
        }
        if !num_pts.is_empty() {
            out.numeric.push((labels, num_pts));
        }
    }
    out
}

/// Series filter for a metric's label matchers, or `None` when there are none.
///
/// Evaluated once per distinct series inside the scan (see
/// [`Scanner::scan_filtered`]), so rows of series that cannot match are never
/// turned into points.
pub(super) fn series_filter(
    matchers: &[crate::matcher::LabelMatcher],
    metric: &str,
) -> Option<parqtel_core::storage::scanner::SeriesFilter> {
    if matchers.is_empty() {
        return None;
    }
    let matchers = matchers.to_vec();
    let metric = metric.to_string();
    Some(Arc::new(move |labels: &parqtel_core::LabelSet| {
        crate::matcher::evaluate_matchers(&matchers, labels, &metric)
    }))
}

/// What a block's series table can use to find a selector's series without
/// reading rows: the equalities and positive regexes of the matchers.
///
/// Only conditions an *absent* label can never satisfy are handed over - an
/// empty-value equality or a regex that accepts "" would match series that lack
/// the label, which the table's postings cannot express. They are narrowing
/// hints only: the full matcher (`series_filter`) still decides every series.
pub(super) fn series_selector(
    matchers: &[crate::matcher::LabelMatcher],
    metric: &str,
) -> parqtel_core::storage::SeriesSelector {
    use crate::matcher::MatchOp;
    use parqtel_core::storage::{SeriesSelector, ValueTest};
    let mut selector = SeriesSelector::all().with_filter(series_filter(matchers, metric));
    // An empty `service.name` matches series that have none, so it cannot be a
    // posting lookup; it still prunes row groups in blocks without a table.
    selector.service = service_of(matchers).map(|s| s.to_string());
    if let Some(svc) = selector.service.clone().filter(|s| !s.is_empty()) {
        selector.equals.push(("service.name".to_string(), svc));
    }
    for m in matchers {
        // `__name__` is the metric, not a label of any series.
        if m.name == "__name__" {
            continue;
        }
        match (m.op, &m.regex) {
            (MatchOp::Equal, _) if !m.value.is_empty() && m.name != "service.name" => {
                selector.equals.push((m.name.clone(), m.value.clone()));
            }
            (MatchOp::RegexMatch, Some(re)) if !re.is_match("") => {
                let re = re.clone();
                selector.value_tests.push((
                    m.name.clone(),
                    Arc::new(move |v: &str| re.is_match(v)) as ValueTest,
                ));
            }
            _ => {}
        }
    }
    selector
}

pub(super) fn v_to_f64(v: &MetricValue) -> f64 {
    match v {
        MetricValue::Double(f) => *f,
        MetricValue::Int(i) => *i as f64,
        MetricValue::Histogram(h) => h.sum,
        MetricValue::Summary(s) => s.sum,
    }
}

/// Applies grouping (by/without), topk/bottomk ranking, and label_replace
/// to the per-series result set produced by the executor.
pub(super) fn apply_post_processing(
    mut series: Vec<crate::models::TimeSeries>,
    plan: &crate::plan::QueryPlan,
) -> Vec<crate::models::TimeSeries> {
    // ── by / without grouping ──────────────────────────────────────────────
    if !plan.group_by.is_empty() || !plan.group_without.is_empty() {
        use std::collections::HashMap;
        let mut grouped: HashMap<Vec<(String, String)>, crate::models::TimeSeries> = HashMap::new();
        for ts in series {
            let key: Vec<(String, String)> = if !plan.group_by.is_empty() {
                plan.group_by
                    .iter()
                    .filter_map(|l| ts.labels.get(l).map(|v| (l.clone(), v.to_string())))
                    .collect()
            } else {
                ts.labels
                    .iter()
                    .filter(|(k, _)| !plan.group_without.contains(&k.to_string()))
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            };
            let entry = grouped.entry(key.clone()).or_insert_with(|| {
                let new_labels = LabelSet::try_from_iter(key).unwrap_or_default();
                crate::models::TimeSeries {
                    labels: new_labels,
                    samples: Vec::new(),
                }
            });
            for s in ts.samples {
                if let Some(existing) = entry
                    .samples
                    .iter_mut()
                    .find(|e| e.timestamp_ns == s.timestamp_ns)
                {
                    existing.value += s.value;
                } else {
                    entry.samples.push(s);
                }
            }
        }
        series = grouped.into_values().collect();
        for ts in &mut series {
            ts.samples.sort_by_key(|s| s.timestamp_ns);
        }
    }

    // ── label_replace ──────────────────────────────────────────────────────
    if let Some((dst, repl, src, regex)) = &plan.label_replace {
        if let Ok(re) = regex::Regex::new(regex) {
            for ts in &mut series {
                let src_val = ts.labels.get(src).unwrap_or("").to_string();
                let new_val = re.replace(&src_val, repl.as_str()).to_string();
                let patch =
                    LabelSet::try_from_iter(std::iter::once((dst.as_str(), new_val.as_str())));
                if let Ok(patch) = patch {
                    ts.labels = ts.labels.merge(&patch);
                }
            }
        }
    }

    // ── topk / bottomk ────────────────────────────────────────────────────
    if let Some(n) = plan.topk_n {
        let last_val =
            |ts: &crate::models::TimeSeries| ts.samples.last().map(|s| s.value).unwrap_or(0.0);
        match plan.aggregation {
            Some(crate::plan::AggregationOp::TopK) => {
                series.sort_by(|a, b| {
                    last_val(b)
                        .partial_cmp(&last_val(a))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                series.truncate(n);
            }
            Some(crate::plan::AggregationOp::BottomK) => {
                series.sort_by(|a, b| {
                    last_val(a)
                        .partial_cmp(&last_val(b))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                series.truncate(n);
            }
            _ => {}
        }
    }

    series
}

/// Evaluates a parsed AST across every step and folds the per-step results
/// into output series.
///
/// Pure CPU: no filesystem, no locks, no await points. Called through
/// `spawn_blocking` so it cannot park an async worker — a wide range query is
/// tens to hundreds of milliseconds of per-step aggregation, and inline that
/// blocks every other request sharing the worker for the whole duration.
pub(super) fn evaluate_steps_to_series(
    expr: &crate::ast::Expr,
    data: &crate::ast::SeriesData,
    hist_data: &crate::ast::HistData,
    lookback_ns: i64,
    start_ns: i64,
    end_ns: i64,
    step: i64,
) -> Result<Vec<TimeSeries>> {
    let eval = crate::eval::Evaluator::with_lookback(data, lookback_ns).with_hist_data(hist_data);

    // Fold each step into the result as it is produced. Collecting every step
    // first kept one instant vector per step live until the last step
    // finished — 960 of them for a 4-hour panel at a 15 s step, ~160 MB for
    // 1 000 series, on top of the output itself. Streaming makes peak memory
    // one step's vector, so O(series) rather than O(steps x series).
    use std::collections::BTreeMap;
    let mut out_series: BTreeMap<u64, TimeSeries> = BTreeMap::new();
    eval.eval_steps_into(expr, start_ns, end_ns, step, |ts, iv| {
        for (labels, value) in &iv.series {
            // Hashed from the label *contents*, not cached by Arc address: the
            // step's vector is dropped at the end of this call, so a freed Arc's
            // address can be reused by the next step's allocation and a
            // pointer-keyed cache would return a stale fingerprint and merge
            // two distinct series.
            let fp = labels.fingerprint();
            let ts_entry = out_series.entry(fp).or_insert_with(|| TimeSeries {
                labels: strip_metric_name(labels),
                samples: Vec::new(),
            });
            ts_entry.samples.push(crate::models::Sample {
                timestamp_ns: ts,
                value: *value,
            });
        }
        // `iv` is dropped here, freeing this step's vector before the next
        // step is evaluated.
        Ok(())
    })?;
    Ok(out_series.into_values().collect())
}

/// The value of a `service.name` equality selector, when the query has one.
///
/// Passed to the scanner so it can prune row groups on service. Rows are
/// written grouped by (metric, resource), so `service_name` statistics are exact
/// per row group — and on a multi-tenant cluster the service is usually the
/// *selective* dimension, not the metric name.
///
/// Only an exact equality matcher on the real `service.name` label is used.
///
/// * `!=` and `=~` are ignored: they exclude some rows rather than identifying
///   one group, so there is nothing safe to prune from.
/// * The underscored `service_name` spelling is deliberately **not** an alias.
///   No series ever carries that label — the resource attribute is stored under
///   the dotted OTLP key — so such a matcher matches nothing at row level, and
///   honouring it here would prune the block on a value no row can hold.
fn service_of(matchers: &[crate::matcher::LabelMatcher]) -> Option<&str> {
    matchers
        .iter()
        .find(|m| m.op == crate::matcher::MatchOp::Equal && m.name == "service.name")
        .map(|m| m.value.as_str())
}

/// Collects (metric_name, matchers) for every selector in the tree.
pub(super) fn collect_metric_refs(
    expr: &crate::ast::Expr,
    out: &mut Vec<(String, Vec<crate::matcher::LabelMatcher>)>,
) {
    use crate::ast::Expr;
    match expr {
        Expr::Selector(s) => {
            if let Some(name) = &s.metric_name {
                out.push((name.clone(), s.matchers.clone()));
            }
        }
        Expr::Paren(e) | Expr::Range(crate::ast::RangeExpr { expr: e, .. }) => {
            collect_metric_refs(e, out)
        }
        Expr::Call(c) => {
            for a in &c.args {
                collect_metric_refs(a, out);
            }
        }
        Expr::Aggregation(a) => {
            if let Some(p) = &a.param {
                collect_metric_refs(p, out);
            }
            collect_metric_refs(&a.expr, out);
        }
        Expr::Binary(b) => {
            collect_metric_refs(&b.lhs, out);
            collect_metric_refs(&b.rhs, out);
        }
        Expr::Number(_) | Expr::Str(_) => {}
    }
}

/// Largest [range] duration in the tree (scan lookback padding).
pub(super) fn max_range_ns(expr: &crate::ast::Expr) -> i64 {
    use crate::ast::Expr;
    match expr {
        Expr::Range(r) => r.range_ns.max(max_range_ns(&r.expr)),
        Expr::Paren(e) => max_range_ns(e),
        Expr::Call(c) => c.args.iter().map(max_range_ns).max().unwrap_or(0),
        Expr::Aggregation(a) => {
            let from_param = a.param.as_ref().map(|p| max_range_ns(p)).unwrap_or(0);
            from_param.max(max_range_ns(&a.expr))
        }
        Expr::Binary(b) => max_range_ns(&b.lhs).max(max_range_ns(&b.rhs)),
        Expr::Number(_) | Expr::Str(_) | Expr::Selector(_) => 0,
    }
}

/// Drops the __name__ label (computed results are nameless, PromQL-style).
fn strip_metric_name(labels: &parqtel_core::LabelSet) -> parqtel_core::LabelSet {
    let mut out = parqtel_core::LabelSet::default();
    for (k, v) in labels.iter() {
        if k == "__name__" {
            continue;
        }
        out = out.merge(
            &parqtel_core::LabelSet::try_from_iter(vec![(k.to_string(), v.to_string())])
                .unwrap_or_default(),
        );
    }
    out
}
