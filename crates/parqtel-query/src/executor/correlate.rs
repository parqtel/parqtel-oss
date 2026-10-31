//! Cross-signal correlation by service, pod or namespace.

use super::QueryExecutor;
use crate::matcher::LabelMatcher;
use crate::models::{CorrelatedEvent, CorrelationResult};
use crate::plan::QueryPlan;
use parqtel_core::{LabelSet, Result};
use serde_json::json;
use std::collections::HashSet;
use std::time::Instant;

impl QueryExecutor {
    /// Performs a cross-signal correlation query.
    pub async fn correlate(
        &self,
        _anchor_signal: &str,
        anchor_timestamp_ns: i64,
        anchor_labels: LabelSet,
        target_signal: &str,
        window_ns: i64,
        limit: usize,
    ) -> Result<CorrelationResult> {
        let start_time = Instant::now();

        // 1. Identify strongest dimension
        let (dimension, weight, matchers) = if let Some(trace_id) = anchor_labels.get("trace_id") {
            (
                "trace_id",
                100,
                vec![LabelMatcher::equal("trace_id", trace_id)],
            )
        } else if let Some(pod_uid) = anchor_labels.get("k8s_pod_uid") {
            (
                "k8s_pod_uid",
                80,
                vec![LabelMatcher::equal("k8s_pod_uid", pod_uid)],
            )
        } else if let (Some(pod), Some(ns)) = (
            anchor_labels.get("k8s_pod_name"),
            anchor_labels.get("k8s_namespace"),
        ) {
            (
                "k8s_pod_name+namespace",
                60,
                vec![
                    LabelMatcher::equal("k8s_pod_name", pod),
                    LabelMatcher::equal("k8s_namespace", ns),
                ],
            )
        } else if let Some(svc) = anchor_labels.get("service_name") {
            (
                "service_name",
                40,
                vec![LabelMatcher::equal("service_name", svc)],
            )
        } else if let Some(ns) = anchor_labels.get("k8s_namespace") {
            (
                "k8s_namespace",
                20,
                vec![LabelMatcher::equal("k8s_namespace", ns)],
            )
        } else {
            return Ok(CorrelationResult {
                correlation_dimension_used: "none".into(),
                correlated: Vec::new(),
                execution_time: start_time.elapsed(),
            });
        };

        let start_ns = anchor_timestamp_ns - window_ns;
        let end_ns = anchor_timestamp_ns + window_ns;

        let mut results = Vec::new();

        if target_signal == "log" {
            let res = self
                .query_logs(start_ns, end_ns, matchers, limit, true, None, None)
                .await?;
            for log in res.logs {
                let time_delta = (log.timestamp_ns - anchor_timestamp_ns).abs();
                let time_score = (window_ns - time_delta).max(0) as f64 / window_ns as f64 * 100.0;
                results.push(CorrelatedEvent {
                    signal: "log".into(),
                    record: serde_json::to_value(&log).unwrap_or_default(),
                    score: weight as f64 * 1000.0 + time_score,
                    time_delta_ns: log.timestamp_ns - anchor_timestamp_ns,
                });
            }
        } else if target_signal == "metric" {
            // Find metrics matching the dimension
            let metric_names = {
                let idx = self.index.read().await;
                idx.query(start_ns, end_ns, None)
                    .into_iter()
                    .flat_map(|b| b.metric_names.clone())
                    .collect::<HashSet<_>>()
            };

            for metric_name in metric_names {
                let plan = QueryPlan::new(
                    metric_name,
                    matchers.clone(),
                    start_ns,
                    end_ns,
                    None,
                    limit,
                    100,
                    None,
                    None,
                )?;
                let res = self.execute(plan).await?;
                for series in res.series {
                    for sample in series.samples {
                        let time_delta = (sample.timestamp_ns - anchor_timestamp_ns).abs();
                        let time_score =
                            (window_ns - time_delta).max(0) as f64 / window_ns as f64 * 100.0;
                        results.push(CorrelatedEvent {
                            signal: "metric".into(),
                            record: json!({
                                "metric": series.labels,
                                "value": sample.value,
                                "timestamp_ns": sample.timestamp_ns,
                            }),
                            score: weight as f64 * 1000.0 + time_score,
                            time_delta_ns: sample.timestamp_ns - anchor_timestamp_ns,
                        });
                    }
                }
            }
        }

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);

        Ok(CorrelationResult {
            correlation_dimension_used: dimension.into(),
            correlated: results,
            execution_time: start_time.elapsed(),
        })
    }
}
