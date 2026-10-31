//! Metric queries: PromQL ASTs and the legacy query plan.

use super::series::{
    apply_post_processing, build_series_data, collect_metric_refs, evaluate_steps_to_series,
    max_range_ns, series_filter, series_selector, v_to_f64,
};
use super::QueryExecutor;
use crate::models::{QueryResult, Sample, TimeSeries};
use crate::plan::QueryPlan;
use parqtel_core::{LabelSet, MetricValue, Result, Scanner};
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

impl QueryExecutor {
    /// Executes a [QueryPlan] for metrics and returns a [QueryResult].
    /// Executes a parsed AST query (Phase 1A path). Loads every metric
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
                let mut raw = Vec::new();
                if !blocks.is_empty() {
                    let scan = Scanner::scan_filtered(
                        blocks,
                        name.clone(),
                        scan_start,
                        end_ns,
                        series_selector(matchers, name),
                    )
                    .await?;
                    points_scanned += scan.rows_considered;
                    raw = scan.points;
                }
                // Buffer
                let (buffered, buffered_rows) = self
                    .buffer
                    .scan_metrics_filtered(name, scan_start, end_ns, filter.as_ref())
                    .await;
                points_scanned += buffered_rows;
                raw.extend(buffered);

                // Grouping, limiting and value conversion are pure CPU over every
                // point of the metric, so they run on the blocking pool rather
                // than on the async worker that would otherwise be parked for
                // the length of a large scan.
                let (max_series, max_samples) = (self.max_series, self.max_samples_per_series);
                let built = tokio::task::spawn_blocking(move || {
                    build_series_data(raw, start_ns, window_ns, max_series, max_samples)
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
        })
    }

    pub async fn execute(&self, plan: QueryPlan) -> Result<QueryResult> {
        let start_time = Instant::now();

        // 1. Find candidate blocks
        let blocks = {
            let idx = self.index.read().await;
            idx.query(plan.start_ns, plan.end_ns, Some(&plan.metric_name))
        };

        if blocks.is_empty() {
            // Even with no disk blocks, check the in-memory buffer
            let filter = series_filter(&plan.matchers, &plan.metric_name);
            let (buffered, considered) = self
                .buffer
                .scan_metrics_filtered(
                    &plan.metric_name,
                    plan.start_ns,
                    plan.end_ns,
                    filter.as_ref(),
                )
                .await;
            if considered == 0 {
                return Ok(QueryResult {
                    series: Vec::new(),
                    execution_time: start_time.elapsed(),
                    points_scanned: 0,
                    total_series_count: 0,
                    volume_summary: vec![0; 60],
                });
            }
            // Process buffered data through the same pipeline below
            let raw_points = buffered;
            let points_scanned = considered;
            let mut series_map: BTreeMap<u64, (LabelSet, Vec<(i64, MetricValue)>)> =
                BTreeMap::new();
            let mut matched_series_fps = HashSet::new();
            let mut volume_summary = vec![0u64; 60];
            let window_ns = (plan.end_ns - plan.start_ns) / 60;
            for dp in raw_points {
                if window_ns > 0 {
                    let bucket =
                        ((dp.timestamp_ns - plan.start_ns) / window_ns).clamp(0, 59) as usize;
                    volume_summary[bucket] += 1;
                }
                let fp = dp.labels.fingerprint();
                matched_series_fps.insert(fp);
                if series_map.contains_key(&fp) || series_map.len() < plan.max_series {
                    let entry = series_map
                        .entry(fp)
                        .or_insert_with(|| (dp.labels.clone(), Vec::new()));
                    if entry.1.len() < plan.max_samples_per_series {
                        entry.1.push((dp.timestamp_ns, dp.value));
                    }
                }
            }
            let total_series_count = matched_series_fps.len();
            let mut results = Vec::new();
            for (_, (mut labels, points)) in series_map {
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
            return Ok(QueryResult {
                series: results,
                execution_time: start_time.elapsed(),
                points_scanned,
                total_series_count,
                volume_summary,
            });
        }

        // 2. Scan blocks concurrently
        let blocks_count = blocks.len();
        let filter = series_filter(&plan.matchers, &plan.metric_name);
        let scan = Scanner::scan_filtered(
            blocks,
            plan.metric_name.clone(),
            plan.start_ns,
            plan.end_ns,
            series_selector(&plan.matchers, &plan.metric_name),
        )
        .await?;
        let mut points_scanned = scan.rows_considered;
        // Block points in time order, as a series' samples must be (the oldest
        // `max_samples_per_series` are kept); the buffer's points follow.
        let mut raw_points = scan.points;
        raw_points.sort_by_key(|p| p.timestamp_ns);

        // 2b. Merge in-memory buffer data (not yet flushed to disk)
        let (buffered, buffered_rows) = self
            .buffer
            .scan_metrics_filtered(
                &plan.metric_name,
                plan.start_ns,
                plan.end_ns,
                filter.as_ref(),
            )
            .await;
        points_scanned += buffered_rows;
        raw_points.extend(buffered);

        // 3. Group by series + Calculate Volume
        let mut series_map: BTreeMap<u64, (LabelSet, Vec<(i64, MetricValue)>)> = BTreeMap::new();
        let mut matched_series_fps = HashSet::new();
        let mut volume_summary = vec![0u64; 60];
        let window_ns = (plan.end_ns - plan.start_ns) / 60;

        for dp in raw_points {
            // Update volume histogram regardless of truncation
            if window_ns > 0 {
                let bucket = ((dp.timestamp_ns - plan.start_ns) / window_ns).clamp(0, 59) as usize;
                volume_summary[bucket] += 1;
            }

            let fp = dp.labels.fingerprint();
            matched_series_fps.insert(fp);

            // Check if we already have this series or if we can add a new one
            if series_map.contains_key(&fp) || series_map.len() < plan.max_series {
                let entry = series_map
                    .entry(fp)
                    .or_insert_with(|| (dp.labels.clone(), Vec::new()));

                if entry.1.len() < plan.max_samples_per_series {
                    entry.1.push((dp.timestamp_ns, dp.value));
                }
            }
        }

        let total_series_count = matched_series_fps.len();

        // 4. Downsample and format results
        let mut results = Vec::new();
        for (_, (mut labels, points)) in series_map {
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
        })
    }
}
