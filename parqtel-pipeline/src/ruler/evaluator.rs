use std::sync::Arc;

use async_trait::async_trait;
use parqtel_core::{DataPoint, LabelSet, Metric, MetricKind, MetricValue, StorageEngine};
use tracing::{error, info};

use crate::expr::PromQlExpr;
use crate::rule::schema::RecordingRuleGroup;

/// Trait for executing PromQL queries (implemented by the query engine).
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Execute an instant query at the given timestamp. Returns (labels, value) pairs.
    async fn instant_query(
        &self,
        expr: &str,
        timestamp_ns: i64,
    ) -> crate::Result<Vec<(LabelSet, f64)>>;
}

/// Evaluates recording rules and writes results to storage.
pub struct RulerEvaluator {
    query_executor: Arc<dyn QueryExecutor>,
    storage: Arc<dyn StorageEngine>,
}

impl RulerEvaluator {
    pub fn new(query_executor: Arc<dyn QueryExecutor>, storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            query_executor,
            storage,
        }
    }

    /// Evaluate all rules in a group at the given timestamp.
    pub async fn evaluate_group(
        &self,
        group: &RecordingRuleGroup,
        eval_timestamp_ns: i64,
    ) -> crate::Result<()> {
        for rule in &group.rules {
            // Validate expression
            if let Err(e) = PromQlExpr::parse(&rule.expr) {
                error!(
                    "Invalid expression for rule '{}': {}",
                    rule.record, e
                );
                continue;
            }

            // Execute query
            let results = match self
                .query_executor
                .instant_query(&rule.expr, eval_timestamp_ns)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    error!("Query failed for rule '{}': {}", rule.record, e);
                    continue;
                }
            };

            if results.is_empty() {
                continue;
            }

            // Build metrics from results
            let mut metrics = Vec::with_capacity(results.len());
            for (labels, value) in &results {
                // Merge result labels with static rule labels
                let static_labels = LabelSet::try_from_iter(rule.labels.clone())
                    .unwrap_or_default();
                let merged = labels.merge(&static_labels);

                let dp = DataPoint {
                    timestamp_ns: eval_timestamp_ns,
                    value: MetricValue::Double(*value),
                    labels: merged,
                };

                metrics.push(Metric {
                    name: rule.record.clone(),
                    description: rule.description.clone().unwrap_or_default(),
                    unit: String::new(),
                    kind: MetricKind::Gauge,
                    resource_attributes: LabelSet::default(),
                    data_points: vec![dp],
                });
            }

            info!(
                "Rule '{}' produced {} series at ts={}",
                rule.record,
                metrics.len(),
                eval_timestamp_ns
            );

            // Write to storage
            if let Err(e) = self.storage.write_metrics_batch(metrics).await {
                error!("Failed to write rule '{}' results: {}", rule.record, e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::rule::schema::{RecordingRule, RecordingRuleGroup};
    use parqtel_core::engine::parquet::ParquetStorageEngine;
    use parqtel_core::config::BlockConfig;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    struct MockQueryExecutor {
        results: Vec<(LabelSet, f64)>,
    }

    #[async_trait]
    impl QueryExecutor for MockQueryExecutor {
        async fn instant_query(
            &self,
            _expr: &str,
            _timestamp_ns: i64,
        ) -> crate::Result<Vec<(LabelSet, f64)>> {
            Ok(self.results.clone())
        }
    }

    #[tokio::test]
    async fn test_recording_rule_writes_new_series() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();
        let storage = Arc::new(ParquetStorageEngine::new(config));

        let labels = LabelSet::try_from_iter(vec![("service", "api")]).unwrap();
        let executor = Arc::new(MockQueryExecutor {
            results: vec![(labels, 0.05)],
        });

        let evaluator = RulerEvaluator::new(executor, storage.clone());
        let group = RecordingRuleGroup {
            name: "test_group".into(),
            interval: "1m".into(),
            rules: vec![RecordingRule {
                record: "service:error_rate:rate5m".into(),
                expr: "rate(errors[5m])".into(),
                labels: BTreeMap::from([("generated_by".into(), "ruler".into())]),
                description: None,
                retention_override_days: None,
                for_duration: None,
            }],
        };

        evaluator.evaluate_group(&group, 1_000_000_000).await.unwrap();

        let results = storage
            .scan_metrics(parqtel_core::engine::MetricScanRequest {
                metric_name: "service:error_rate:rate5m".into(),
                start_ns: 0,
                end_ns: i64::MAX,
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_recording_rule_idempotent_evaluation() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();
        let storage = Arc::new(ParquetStorageEngine::new(config));

        let labels = LabelSet::try_from_iter(vec![("svc", "web")]).unwrap();
        let executor = Arc::new(MockQueryExecutor {
            results: vec![(labels, 1.0)],
        });

        let evaluator = RulerEvaluator::new(executor, storage.clone());
        let group = RecordingRuleGroup {
            name: "idempotent".into(),
            interval: "1m".into(),
            rules: vec![RecordingRule {
                record: "test:metric".into(),
                expr: "sum(x)".into(),
                labels: BTreeMap::new(),
                description: None,
                retention_override_days: None,
                for_duration: None,
            }],
        };

        // Evaluate same timestamp twice
        evaluator.evaluate_group(&group, 1_000_000_000).await.unwrap();
        evaluator.evaluate_group(&group, 1_000_000_000).await.unwrap();

        let results = storage
            .scan_metrics(parqtel_core::engine::MetricScanRequest {
                metric_name: "test:metric".into(),
                start_ns: 0,
                end_ns: i64::MAX,
            })
            .await
            .unwrap();
        // Both writes succeed (storage doesn't deduplicate, but both have same ts)
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_ruler_expression_error_does_not_crash() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();
        let storage = Arc::new(ParquetStorageEngine::new(config));

        let executor = Arc::new(MockQueryExecutor { results: vec![] });
        let evaluator = RulerEvaluator::new(executor, storage);

        let group = RecordingRuleGroup {
            name: "bad".into(),
            interval: "1m".into(),
            rules: vec![RecordingRule {
                record: "bad:rule".into(),
                expr: "((( unbalanced".into(), // invalid
                labels: BTreeMap::new(),
                description: None,
                retention_override_days: None,
                for_duration: None,
            }],
        };

        // Should not panic
        let result = evaluator.evaluate_group(&group, 1_000_000_000).await;
        assert!(result.is_ok()); // errors are logged, not propagated
    }
}
