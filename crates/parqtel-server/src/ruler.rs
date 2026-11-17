//! Adapters that connect the pipeline ruler to the live server.
//!
//! `RulerEvaluator` deliberately depends on two narrow traits rather than the
//! storage engine: the server's live path is the ingestion service (WAL,
//! buffer, index), not `ParquetStorageEngine`, which has its own private index.

use async_trait::async_trait;
use parqtel_core::{LabelSet, Metric};
use parqtel_pipeline::ruler::evaluator::{MetricSink, QueryExecutor};
use parqtel_pipeline::Error;
use parqtel_query::{parse_query, QueryPlan};
use tokio::sync::mpsc;

use crate::state::AppState;

/// Evaluates recording-rule expressions against the same executor the query
/// API uses, over the configured instant lookback window.
pub struct ServerQueryExecutor {
    state: AppState,
}

impl ServerQueryExecutor {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl QueryExecutor for ServerQueryExecutor {
    async fn instant_query(
        &self,
        expr: &str,
        timestamp_ns: i64,
    ) -> parqtel_pipeline::Result<Vec<(LabelSet, f64)>> {
        // Composed expressions (binary ops, nested selectors, subqueries) must
        // use the AST engine; the legacy plan parser mis-handles a braced
        // selector inside `rate(...)`. Simple selector shapes keep the plan
        // path, matching the query API's choice.
        if parqtel_query::needs_ast(expr) {
            let ast = parqtel_query::parser::parse_expr(expr)
                .map_err(|e| Error::Expression(e.to_string()))?;
            let result = self
                .state
                .inner
                .query_executor
                .execute_ast(&ast, timestamp_ns, timestamp_ns + 1, None)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            return Ok(last_samples(result));
        }

        let (
            metric_name,
            matchers,
            aggregation,
            quantile,
            topk_n,
            group_by,
            group_without,
            label_replace,
            scalar_param,
            clamp,
            range_ns,
        ) = parse_query(expr).map_err(|e| Error::Expression(e.to_string()))?;

        let lookback = self.state.inner.config.query.lookback_delta_ns;
        let plan = QueryPlan::new_full(
            metric_name,
            matchers,
            timestamp_ns.saturating_sub(lookback),
            timestamp_ns,
            None,
            self.state.inner.config.query.max_series,
            self.state.inner.config.query.max_samples_per_series,
            aggregation,
            quantile,
            topk_n,
            group_by,
            group_without,
            label_replace,
            scalar_param,
            clamp,
            range_ns,
        )
        .map_err(|e| Error::Expression(e.to_string()))?;

        let result = self
            .state
            .inner
            .query_executor
            .execute(plan)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(last_samples(result))
    }
}

fn last_samples(result: parqtel_query::QueryResult) -> Vec<(LabelSet, f64)> {
    result
        .series
        .into_iter()
        .filter_map(|series| {
            series
                .samples
                .last()
                .map(|sample| (series.labels, sample.value))
        })
        .collect()
}

/// Forwards recording-rule results to the metrics ingestion service through a
/// bounded channel, keeping the ruler task off that service's lock.
pub struct ChannelMetricSink {
    tx: mpsc::Sender<Vec<Metric>>,
}

impl ChannelMetricSink {
    pub fn new(tx: mpsc::Sender<Vec<Metric>>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl MetricSink for ChannelMetricSink {
    async fn write_metrics(&self, metrics: Vec<Metric>) -> parqtel_pipeline::Result<()> {
        self.tx
            .try_send(metrics)
            .map_err(|e| Error::Storage(format!("metric sink queue unavailable: {e}")))
    }
}
