use axum::{
    extract::State,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use crate::state::AppState;
use parqtel_query::{QueryPlan, AggregationOp, parse_selector};

#[derive(Debug, Deserialize)]
pub struct SimpleJSONQuery {
    pub range: SimpleJSONRange,
    pub interval_ms: u64,
    pub targets: Vec<SimpleJSONTarget>,
    pub max_data_points: usize,
}

#[derive(Debug, Deserialize)]
pub struct SimpleJSONRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Deserialize)]
pub struct SimpleJSONTarget {
    pub target: String,
}

#[derive(Serialize)]
pub struct SimpleJSONQueryResult {
    pub target: String,
    pub datapoints: Vec<(f64, i64)>, // [value, timestamp_ms]
}

/// Handler for POST /search.
pub async fn search(
    State(state): State<AppState>,
) -> Json<Vec<String>> {
    let metrics = state.inner.query_executor.list_metrics().await;
    Json(metrics.into_iter().collect())
}

/// Handler for POST /query.
pub async fn query(
    State(state): State<AppState>,
    Json(params): Json<SimpleJSONQuery>,
) -> Response {
    let mut all_results = Vec::new();

    let start_ns = chrono::DateTime::parse_from_rfc3339(&params.range.from).map(|dt| dt.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0);
    let end_ns = chrono::DateTime::parse_from_rfc3339(&params.range.to).map(|dt| dt.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0);

    for target in params.targets {
        let (metric_name, matchers) = match parse_selector(&target.target) {
            Ok(res) => (res.0.unwrap_or_default(), res.1),
            Err(_) => continue,
        };

        let plan = match QueryPlan::new(
            metric_name.clone(),
            matchers,
            start_ns,
            end_ns,
            Some(params.interval_ms as i64 * 1_000_000),
            state.inner.config.query.max_series,
            params.max_data_points,
            Some(AggregationOp::Avg),
            None,
        ) {
            Ok(p) => p,
            Err(_) => continue,
        };

        if let Ok(res) = state.inner.query_executor.execute(plan).await {
            for ts in res.series {
                let mut datapoints = Vec::new();
                for s in ts.samples {
                    datapoints.push((s.value, s.timestamp_ns / 1_000_000));
                }
                all_results.push(SimpleJSONQueryResult {
                    target: format!("{}: {:?}", metric_name, ts.labels),
                    datapoints,
                });
            }
        }
    }

    Json(all_results).into_response()
}

/// Handler for POST /tag-keys.
pub async fn tag_keys(
    State(state): State<AppState>,
) -> Json<Vec<serde_json::Value>> {
    let labels = state.inner.query_executor.list_labels(None).await;
    let res = labels.into_iter().map(|l| serde_json::json!({"type": "string", "text": l})).collect();
    Json(res)
}

/// Handler for POST /tag-values.
pub async fn tag_values(
    State(_state): State<AppState>,
) -> Json<Vec<serde_json::Value>> {
    // Stub
    Json(vec![])
}

/// Handler for POST /annotations.
pub async fn annotations() -> Json<Vec<serde_json::Value>> {
    Json(vec![])
}
