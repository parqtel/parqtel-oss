//! Metric name, label name and label value listing endpoints.

use super::common::PrometheusResponse;
use crate::state::AppState;
use axum::{
    extract::{rejection::QueryRejection, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;

/// Handler for GET /api/v1/label/__name__/values.
pub async fn list_metric_names(State(state): State<AppState>) -> Response {
    let names = state.inner.query_executor.list_metrics().await;
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: names.into_iter().collect::<Vec<_>>(),
        }),
    )
        .into_response()
}

/// Handler for GET /api/v1/labels.
pub async fn list_label_names(State(state): State<AppState>) -> Response {
    let names = state.inner.query_executor.list_labels(None).await;
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: names.into_iter().collect::<Vec<_>>(),
        }),
    )
        .into_response()
}

/// Handler for GET /api/v1/label/{name}/values.
///
/// Query params (builder autocomplete):
/// - `limit`  — cap the number of values returned (default: all; clamped
///   server-side to 100 when combined with a prefix/match filter or used
///   alone by the UI, which always asks for a bounded top-N).
/// - `match`  — prefix filter applied server-side so high-cardinality
///   labels return only values the user is typing toward (e.g.
///   `match=us-` while the user types `user_id="us-…"`).
///
/// Without any params this remains the full Prometheus-compatible listing.
/// With `limit` it switches to the recency-bounded path (in-memory buffer
/// first, then the newest blocks' flush-time label-value index) — safe for
/// labels with tens of thousands of distinct values.
#[derive(Deserialize)]
pub struct LabelValuesQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default, rename = "match")]
    match_prefix: Option<String>,
}

pub async fn list_label_values(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    params: Result<Query<LabelValuesQuery>, QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": e.to_string()})),
            )
                .into_response()
        }
    };
    // Full listing unless the caller explicitly bounds it.
    let values: Vec<String> = match params.limit {
        Some(limit) => {
            state
                .inner
                .query_executor
                .recent_label_values(&name, params.match_prefix.as_deref(), limit, 5)
                .await
        }
        None => state
            .inner
            .query_executor
            .list_label_values(&name)
            .await
            .into_iter()
            .collect(),
    };
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: values.into_iter().collect::<Vec<_>>(),
        }),
    )
        .into_response()
}
