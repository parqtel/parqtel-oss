//! Trace search and cross-signal correlation endpoints.

use super::common::map_error;
use crate::state::AppState;
use axum::{
    extract::{rejection::QueryRejection, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct CorrelateQuery {
    pub anchor_signal: String,
    pub anchor_timestamp_ns: i64,
    pub anchor_labels: String, // JSON
    pub target_signal: String,
    pub window_ns: Option<i64>,
    pub limit: Option<usize>,
}

/// Handler for GET /v1/correlate.
pub async fn correlate(
    State(state): State<AppState>,
    params: Result<Query<CorrelateQuery>, QueryRejection>,
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

    let labels: parqtel_core::LabelSet = match serde_json::from_str(&params.anchor_labels) {
        Ok(l) => l,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": "Invalid anchor_labels JSON"})),
            )
                .into_response()
        }
    };

    match state
        .inner
        .query_executor
        .correlate(
            &params.anchor_signal,
            params.anchor_timestamp_ns,
            labels,
            &params.target_signal,
            params.window_ns.unwrap_or(5_000_000_000),
            params.limit.unwrap_or(50),
        )
        .await
    {
        Ok(res) => (
            StatusCode::OK,
            Json(json!({ "status": "success", "data": res })),
        )
            .into_response(),
        Err(e) => map_error(e),
    }
}

/// Handler for GET /v1/traces/search.
pub async fn search_traces(
    State(state): State<AppState>,
    params: Query<serde_json::Value>,
) -> Response {
    let trace_id = params
        .get("trace_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let start = params
        .get("start")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let end = params
        .get("end")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or_else(|| chrono::Utc::now().timestamp() as f64);

    let start_ns = (start * 1_000_000_000.0) as i64;
    let end_ns = (end * 1_000_000_000.0) as i64;
    // Configurable result cap (default 200). Traces search semantics are
    // per-span; the UI groups into traces client-side.
    let limit = params
        .get("limit")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(200)
        .min(10_000) as usize;
    let filter = if trace_id.is_empty() {
        None
    } else {
        Some(trace_id)
    };
    // ParqtelQL span predicates: pushed down INTO the scan
    // pipeline (before the result cap), so q-filters don't lose matches.
    let search = params
        .get("q")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(parqtel_query::logql::parse_search);

    match state
        .inner
        .query_executor
        .query_traces_filtered_counted(start_ns, end_ns, filter, limit, search.as_ref())
        .await
    {
        Ok(result) => {
            let spans = &result.spans;
            // Predicates already applied in the scan (push-down).
            let tid = if let Some(first) = spans.first() {
                hex::encode(first.trace_id)
            } else {
                trace_id.to_string()
            };
            let span_json: Vec<serde_json::Value> = spans.iter().map(|s| {
                let svc = s.attributes.get("service.name").unwrap_or("unknown").to_string();
                let parent = hex::encode(s.parent_span_id);
                let events: Vec<serde_json::Value> = s.events.iter().map(|e| {
                    json!({ "time_ns": e.time_ns, "name": e.name, "attributes": e.attributes })
                }).collect();
                let links: Vec<serde_json::Value> = s.links.iter().map(|l| {
                    json!({ "trace_id": hex::encode(l.trace_id), "span_id": hex::encode(l.span_id), "attributes": l.attributes })
                }).collect();
                let mut obj = json!({
                    "trace_id": hex::encode(s.trace_id),
                    "span_id": hex::encode(s.span_id),
                    "operation_name": s.name,
                    "service_name": svc,
                    "start_timestamp_ns": s.start_time_ns,
                    "end_timestamp_ns": s.end_time_ns,
                    "attributes": s.attributes,
                    "status": { "code": s.status.code, "message": s.status.message },
                    "kind": s.kind,
                    "events": events,
                    "links": links,
                });
                if s.parent_span_id != [0u8; 8] {
                    if let Some(map) = obj.as_object_mut() {
                        map.insert("parent_span_id".into(), json!(parent));
                    }
                }
                obj
            }).collect();

            (
                StatusCode::OK,
                Json(json!({
                    "status": "success",
                    "data": {
                        "trace_id": tid,
                        "spans": span_json,
                        "total_spans_in_range": result.total_spans_in_range,
                        "spans_matched": result.spans_matched,
                        "truncated": result.truncated,
                        "limit": limit,
                    }
                })),
            )
                .into_response()
        }
        Err(e) => map_error(e),
    }
}
