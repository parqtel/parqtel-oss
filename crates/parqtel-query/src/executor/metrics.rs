//! Metric queries: PromQL ASTs and the legacy query plan.

use super::series::{
    apply_post_processing, build_limited_series_data, collect_metric_refs,
    evaluate_steps_to_series, max_range_ns, series_filter, series_selector, v_to_f64,
};
use super::QueryExecutor;
use crate::models::{QueryResult, Sample, TimeSeries};
use crate::plan::QueryPlan;
use parqtel_core::{LabelSet, MetricValue, Result, Scanner};
use std::collections::BTreeMap;
use std::time::Instant;

impl QueryExecutor {
    /// Executes a [QueryPlan] for metrics and returns a [QueryResult].
    /// Executes a parsed AST query. Loads every metric
    /// referenced in the tree once, then evaluates per step.
    pub async fn execute_ast(
        &self,
        expr: &crate::ast::Expr,
        start_ns: i64,
        end_ns: i64,
        step_ns: Option<i64>,
    ) -> Result<QueryResult> {
        let start_time = Instant::now();

        // Collect referenced metric names + their selectors' matchers.
        let mut refs: Vec<(String, Vec<crate::matcher::LabelMatcher>)> = Vec::new();
        collect_metric_refs(expr, &mut refs);
        // Selector-free expressions (time(), pi(), vector(1), minute(), …)
        // are still valid instant queries — evaluate them against an empty
        // SeriesData instead of short-circuiting to an empty result.
        let mut data: crate::ast::SeriesData = Default::default();
        // Native-histogram channel: OTLP explicit-bucket samples kept in
        // structured form for the histogram_* function family.
        let mut hist_data: crate::ast::HistData = Default::default();
        let mut points_scanned: u64 = 0;
        let mut total_series = 0usize;
        let mut scanned_blocks = 0usize;
        let mut truncated_blocks = 0usize;

        // Volume histogram: 60 buckets over the visible timeline
        // [start_ns, end_ns], counting EVERY matched point — not just the
        // samples that survive series truncation — so the UI volume bars
        // show complete event counts for any timeline, exactly like the
        // legacy plan path does.
        let window_ns = (end_ns - start_ns) / 60;
        let mut volume_summary = vec![0u64; 60];
        if !refs.is_empty() {
            // Extend the scan range so range selectors reaching before `start`
            // still see samples (window lookback).
            let max_range = max_range_ns(expr);
            let scan_start = start_ns.saturating_sub(max_range.max(60_000_000_000));
            for (name, matchers) in &refs {
                // Blocks
                let blocks = {
                    let idx = self.index.read().await;
                    idx.query(scan_start, end_ns, Some(name))
                };
                // Label matchers are evaluated once per distinct series inside
                // the scan, so rows of series that cannot match never become
                // points. The service matcher additionally prunes row groups.
                let filter = series_filter(matchers, name);
                // Buffer
                let (buffered, buffered_rows) = self
                    .buffer
                    .scan_metrics_filtered(name, scan_start, end_ns, filter.as_ref())
                    .await;
                let scan = Scanner::scan_filtered_limited(
                    blocks,
                    name.clone(),
                    scan_start,
                    end_ns,
                    series_selector(matchers, name),
                    self.max_series,
                    start_ns,
                    window_ns,
                    buffered,
                    buffered_rows,
                )
                .await?;
                points_scanned += scan.rows_considered;
                scanned_blocks += scan.scanned_blocks;
                truncated_blocks += scan.truncated_blocks;

                // Grouping and value conversion run on the blocking pool.
                let (max_samples, total, volume) =
                    (self.max_samples_per_series, scan.total_series, scan.volume);
                let built = tokio::task::spawn_blocking(move || {
                    build_limited_series_data(scan.points, max_samples, total, volume)
                })
                .await
                .map_err(|e| {
                    parqtel_core::Error::Internal(format!("series grouping task panicked: {e}"))
                })?;
                total_series += built.total_series;
                for (acc, b) in volume_summary.iter_mut().zip(built.volume) {
                    *acc += b;
                }
                data.entry(name.clone()).or_default().extend(built.numeric);
                hist_data
                    .entry(name.clone())
                    .or_default()
                    .extend(built.histograms);
            }
        } // refs non-empty

        // Evaluate per step, on the blocking pool. Everything above this line
        // is I/O (block scan, buffer scan); everything in
        // `evaluate_steps_to_series` is pure CPU with no await point, so
        // running it inline parks a tokio worker for its whole duration and
        // stalls every other request multiplexed onto it.
        let step = step_ns.unwrap_or((end_ns - start_ns).max(1));
        // The AST is cloned so the closure can be 'static. An expression is a
        // few dozen nodes, against a CPU cost orders of magnitude larger.
        let eval_expr = expr.clone();
        let lookback_ns = self.lookback_ns;
        let series_vec = tokio::task::spawn_blocking(move || {
            evaluate_steps_to_series(
                &eval_expr,
                &data,
                &hist_data,
                lookback_ns,
                start_ns,
                end_ns,
                step,
            )
        })
        .await
        .map_err(|e| {
            parqtel_core::Error::Internal(format!("query evaluation task panicked: {e}"))
        })??;

        tracing::debug!(
            metric = %format!("{:?}", expr),
            series = total_series,
            points_scanned,
            duration_ms = start_time.elapsed().as_millis(),
            "execute_ast query complete"
        );
        Ok(QueryResult {
            series: series_vec,
            execution_time: start_time.elapsed(),
            points_scanned,
            total_series_count: total_series,
            volume_summary,
            scanned_blocks,
            truncated_blocks,
        })
    }

    pub async fn execute(&self, plan: QueryPlan) -> Result<QueryResult> {
        let start_time = Instant::now();

        // Find candidate blocks.
        let blocks = {
            let idx = self.index.read().await;
            idx.query(plan.start_ns, plan.end_ns, Some(&plan.metric_name))
        };
        let blocks_count = blocks.len();
        let filter = series_filter(&plan.matchers, &plan.metric_name);
        let (buffered, buffered_rows) = self
            .buffer
            .scan_metrics_filtered(
                &plan.metric_name,
                plan.start_ns,
                plan.end_ns,
                filter.as_ref(),
            )
            .await;
        let window_ns = (plan.end_ns - plan.start_ns) / 60;
        let scan = Scanner::scan_filtered_limited(
            blocks,
            plan.metric_name.clone(),
            plan.start_ns,
            plan.end_ns,
            series_selector(&plan.matchers, &plan.metric_name),
            plan.max_series,
            plan.start_ns,
            window_ns,
            buffered,
            buffered_rows,
        )
        .await?;
        let points_scanned = scan.rows_considered;
        let total_series_count = scan.total_series;
        let volume_summary = scan.volume.to_vec();
        let scanned_blocks = scan.scanned_blocks;
        let truncated_blocks = scan.truncated_blocks;
        let mut series_map: BTreeMap<u64, (LabelSet, Vec<(i64, MetricValue)>)> = BTreeMap::new();
        for dp in scan.points {
            let fp = dp.labels.fingerprint();
            let entry = series_map
                .entry(fp)
                .or_insert_with(|| (dp.labels.clone(), Vec::new()));
            entry.1.push((dp.timestamp_ns, dp.value));
        }

        // Downsample and format results.
        let mut results = Vec::new();
        for (_, (mut labels, mut points)) in series_map {
            points.sort_by_key(|(timestamp, _)| *timestamp);
            points.truncate(plan.max_samples_per_series);
            labels = labels.merge(&LabelSet::try_from_iter(vec![(
                "__name__",
                plan.metric_name.clone(),
            )])?);

            let samples = if let Some(step) = plan.step_ns {
                if let Some(op) = plan.aggregation {
                    crate::aggregation::downsample_plan(
                        points,
                        plan.start_ns,
                        plan.end_ns,
                        step,
                        plan.range_ns,
                        op,
                        plan.quantile,
                        plan.scalar_param,
                        plan.clamp,
                    )
                } else {
                    points
                        .into_iter()
                        .map(|(t, v)| Sample {
                            timestamp_ns: t,
                            value: v_to_f64(&v),
                        })
                        .collect()
                }
            } else {
                points
                    .into_iter()
                    .map(|(t, v)| Sample {
                        timestamp_ns: t,
                        value: v_to_f64(&v),
                    })
                    .collect()
            };

            results.push(TimeSeries { labels, samples });
        }
        results = apply_post_processing(results, &plan);

        tracing::debug!(
            metric = %plan.metric_name,
            blocks = blocks_count,
            series = total_series_count,
            points_scanned,
            duration_ms = start_time.elapsed().as_millis(),
            "query execute complete"
        );
        Ok(QueryResult {
            series: results,
            execution_time: start_time.elapsed(),
            points_scanned,
            total_series_count,
            volume_summary,
            scanned_blocks,
            truncated_blocks,
        })
    }
}
