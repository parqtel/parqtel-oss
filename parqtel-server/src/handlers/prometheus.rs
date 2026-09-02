use crate::state::AppState;
use axum::{
    extract::{rejection::QueryRejection, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use parqtel_query::{parse_query, AggregationOp, QueryPlan};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::Ordering;

#[derive(Debug, Deserialize)]
pub struct InstantQuery {
    pub query: String,
    pub time: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct RangeQuery {
    pub query: String,
    pub start: f64,
    pub end: f64,
    pub step: String,
}

#[derive(Debug, Deserialize)]
pub struct LogQuery {
    pub query: String,
    pub start: f64,
    pub end: f64,
    pub limit: Option<usize>,
    pub order: Option<String>,
    pub severity_min: Option<i32>,
    pub search: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LogCountQuery {
    pub query: String,
    pub start: f64,
    pub end: f64,
}

#[derive(Debug, Deserialize)]
pub struct CorrelateQuery {
    pub anchor_signal: String,
    pub anchor_timestamp_ns: i64,
    pub anchor_labels: String, // JSON
    pub target_signal: String,
    pub window_ns: Option<i64>,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct LogFieldsData {
    pub dedicated_columns: Vec<String>,
    pub common_attributes: Vec<String>,
}

#[derive(Serialize)]
pub struct LogBucket {
    pub start_ns: i64,
    pub end_ns: i64,
    pub count: u64,
}

#[derive(Serialize)]
pub struct LogCountResponse {
    pub status: String,
    pub data: Vec<LogBucket>,
}

#[derive(Serialize)]
pub struct PrometheusResponse<T> {
    pub status: String,
    pub data: T,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrometheusData {
    pub result_type: String,
    pub result: Vec<PrometheusResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_series_count: Option<usize>,
    pub volume_summary: Vec<u64>,
}

#[derive(Serialize)]
pub struct PrometheusResult {
    pub metric: serde_json::Value,
    pub values: Vec<(f64, String)>,
}

/// Renders a QueryResult as a Prometheus instant-vector response.
fn render_vector_result(result: parqtel_query::QueryResult) -> Response {
    let mut res = Vec::new();
    for ts in result.series {
        let values = ts
            .samples
            .into_iter()
            .map(|s| (s.timestamp_ns as f64 / 1_000_000_000.0, s.value.to_string()))
            .collect();
        res.push(PrometheusResult {
            metric: serde_json::to_value(&ts.labels).unwrap_or_default(),
            values,
        });
    }
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: PrometheusData {
                result_type: "vector".into(),
                result: res,
                total_series_count: Some(result.total_series_count),
                volume_summary: result.volume_summary,
            },
        }),
    )
        .into_response()
}

/// Renders a QueryResult as a Prometheus matrix (range) response.
fn render_range_result(result: parqtel_query::QueryResult) -> Response {
    let mut res = Vec::new();
    for ts in result.series {
        let values = ts
            .samples
            .into_iter()
            .map(|s| (s.timestamp_ns as f64 / 1_000_000_000.0, s.value.to_string()))
            .collect();
        res.push(PrometheusResult {
            metric: serde_json::to_value(&ts.labels).unwrap_or_default(),
            values,
        });
    }
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: PrometheusData {
                result_type: "matrix".into(),
                result: res,
                total_series_count: Some(result.total_series_count),
                volume_summary: result.volume_summary,
            },
        }),
    )
        .into_response()
}

/// Handler for GET /api/v1/query (Instant).
pub async fn query_instant(
    State(state): State<AppState>,
    params: Result<Query<InstantQuery>, QueryRejection>,
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

    let now = params
        .time
        .unwrap_or_else(|| chrono::Utc::now().timestamp() as f64);

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
    ) = match parse_query(&params.query) {
        Ok(res) => res,
        Err(e) => return map_error(e),
    };

    // Phase 1A: composed queries (nesting, binary ops, subqueries) go
    // through the AST evaluator; simple shapes keep the legacy plan path.
    if parqtel_query::needs_ast(&params.query) {
        let expr = match parqtel_query::parser::parse_expr(&params.query) {
            Ok(e) => e,
            Err(e) => return map_error(e),
        };
        let end_ns = (now * 1_000_000_000.0) as i64;
        let query_start = std::time::Instant::now();
        let result = state
            .inner
            .query_executor
            .execute_ast(&expr, end_ns - 60_000_000_000, end_ns, None)
            .await;
        state
            .inner
            .metrics
            .queries_executed
            .fetch_add(1, Ordering::Relaxed);
        if let Ok(mut hist) = state.inner.metrics.query_duration_ms.lock() {
            hist.record(query_start.elapsed().as_millis() as f64);
        }
        match result {
            Ok(result) => return render_vector_result(result),
            Err(e) => {
                state
                    .inner
                    .metrics
                    .query_errors
                    .fetch_add(1, Ordering::Relaxed);
                return map_error(e);
            }
        }
    }

    let plan = match QueryPlan::new_full(
        metric_name,
        matchers,
        (now * 1_000_000_000.0) as i64 - 60_000_000_000, // 1 min window
        (now * 1_000_000_000.0) as i64,
        None,
        state.inner.config.query.max_series,
        state.inner.config.query.max_samples_per_series,
        aggregation.or(Some(AggregationOp::Avg)),
        quantile,
        topk_n,
        group_by,
        group_without,
        label_replace,
        scalar_param,
        clamp,
        range_ns,
    ) {
        Ok(p) => p,
        Err(e) => return map_error(e),
    };

    let query_start = std::time::Instant::now();
    let result = state.inner.query_executor.execute(plan).await;
    state
        .inner
        .metrics
        .queries_executed
        .fetch_add(1, Ordering::Relaxed);
    if let Ok(mut hist) = state.inner.metrics.query_duration_ms.lock() {
        hist.record(query_start.elapsed().as_millis() as f64);
    }
    match result {
        Ok(result) => {
            let mut res = Vec::new();
            for ts in result.series {
                let values = ts
                    .samples
                    .into_iter()
                    .map(|s| (s.timestamp_ns as f64 / 1_000_000_000.0, s.value.to_string()))
                    .collect();

                res.push(PrometheusResult {
                    metric: serde_json::to_value(&ts.labels).unwrap_or_default(),
                    values,
                });
            }

            (
                StatusCode::OK,
                Json(PrometheusResponse {
                    status: "success".into(),
                    data: PrometheusData {
                        result_type: "vector".into(),
                        result: res,
                        total_series_count: Some(result.total_series_count),
                        volume_summary: result.volume_summary,
                    },
                }),
            )
                .into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .query_errors
                .fetch_add(1, Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for GET /api/v1/query_range.
pub async fn query_range(
    State(state): State<AppState>,
    params: Result<Query<RangeQuery>, QueryRejection>,
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
    ) = match parse_query(&params.query) {
        Ok(res) => res,
        Err(e) => return map_error(e),
    };

    let start_ns = (params.start * 1_000_000_000.0) as i64;
    let end_ns = (params.end * 1_000_000_000.0) as i64;

    let step_ns = match parse_duration(&params.step) {
        Ok(d) => d,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": "Invalid step duration"})),
            )
                .into_response()
        }
    };

    // Phase 1A: composed queries via the AST evaluator.
    if parqtel_query::needs_ast(&params.query) {
        let expr = match parqtel_query::parser::parse_expr(&params.query) {
            Ok(e) => e,
            Err(e) => return map_error(e),
        };
        let query_start = std::time::Instant::now();
        let result = state
            .inner
            .query_executor
            .execute_ast(&expr, start_ns, end_ns, Some(step_ns))
            .await;
        state
            .inner
            .metrics
            .queries_executed
            .fetch_add(1, Ordering::Relaxed);
        if let Ok(mut hist) = state.inner.metrics.query_duration_ms.lock() {
            hist.record(query_start.elapsed().as_millis() as f64);
        }
        match result {
            Ok(result) => return render_range_result(result),
            Err(e) => {
                state
                    .inner
                    .metrics
                    .query_errors
                    .fetch_add(1, Ordering::Relaxed);
                return map_error(e);
            }
        }
    }

    let plan = match QueryPlan::new_full(
        metric_name,
        matchers,
        start_ns,
        end_ns,
        Some(step_ns),
        state.inner.config.query.max_series,
        state.inner.config.query.max_samples_per_series,
        aggregation.or(Some(AggregationOp::Avg)),
        quantile,
        topk_n,
        group_by,
        group_without,
        label_replace,
        scalar_param,
        clamp,
        range_ns,
    ) {
        Ok(p) => p,
        Err(e) => return map_error(e),
    };

    let query_start = std::time::Instant::now();
    let result = state.inner.query_executor.execute(plan).await;
    state
        .inner
        .metrics
        .queries_executed
        .fetch_add(1, Ordering::Relaxed);
    if let Ok(mut hist) = state.inner.metrics.query_duration_ms.lock() {
        hist.record(query_start.elapsed().as_millis() as f64);
    }
    match result {
        Ok(result) => {
            let mut res = Vec::new();
            for ts in result.series {
                let values = ts
                    .samples
                    .into_iter()
                    .map(|s| (s.timestamp_ns as f64 / 1_000_000_000.0, s.value.to_string()))
                    .collect();

                res.push(PrometheusResult {
                    metric: serde_json::to_value(&ts.labels).unwrap_or_default(),
                    values,
                });
            }

            (
                StatusCode::OK,
                Json(PrometheusResponse {
                    status: "success".into(),
                    data: PrometheusData {
                        result_type: "matrix".into(),
                        result: res,
                        total_series_count: Some(result.total_series_count),
                        volume_summary: result.volume_summary,
                    },
                }),
            )
                .into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .query_errors
                .fetch_add(1, Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for GET /api/v1/logs.
pub async fn query_logs(
    State(state): State<AppState>,
    params: Result<Query<LogQuery>, QueryRejection>,
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

    let (_, matchers) = match parqtel_query::parse_selector(&params.query) {
        Ok(res) => res,
        Err(e) => return map_error(e),
    };

    let start_ns = (params.start * 1_000_000_000.0) as i64;
    let end_ns = (params.end * 1_000_000_000.0) as i64;
    let limit = params.limit.unwrap_or(1000).min(10000);
    let order_desc = params.order.as_deref() != Some("asc");

    match state
        .inner
        .query_executor
        .query_logs(
            start_ns,
            end_ns,
            matchers,
            limit,
            order_desc,
            params.severity_min,
            params.search,
        )
        .await
    {
        Ok(result) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "data": {
                    "logs": result.logs,
                    "total_matched": result.total_logs_count,
                    "truncated": result.total_logs_count > limit,
                    "volume_summary": result.volume_summary,
                }
            })),
        )
            .into_response(),
        Err(e) => map_error(e),
    }
}

/// Handler for GET /v1/logs/count.
pub async fn query_logs_count(
    State(state): State<AppState>,
    params: Result<Query<LogCountQuery>, QueryRejection>,
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

    let (_, matchers) = match parqtel_query::parse_selector(&params.query) {
        Ok(res) => res,
        Err(e) => return map_error(e),
    };

    let start_ns = (params.start * 1_000_000_000.0) as i64;
    let end_ns = (params.end * 1_000_000_000.0) as i64;

    match state
        .inner
        .query_executor
        .query_logs(start_ns, end_ns, matchers, 0, true, None, None)
        .await
    {
        Ok(result) => {
            let win = (end_ns - start_ns) / 60;
            let buckets = result
                .volume_summary
                .into_iter()
                .enumerate()
                .map(|(i, c)| LogBucket {
                    start_ns: start_ns + (i as i64 * win),
                    end_ns: start_ns + ((i + 1) as i64 * win),
                    count: c,
                })
                .collect();
            (
                StatusCode::OK,
                Json(LogCountResponse {
                    status: "success".into(),
                    data: buckets,
                }),
            )
                .into_response()
        }
        Err(e) => map_error(e),
    }
}

/// Handler for GET /v1/logs/fields.
pub async fn list_log_fields(State(state): State<AppState>) -> Response {
    let (dedicated, common) = state.inner.query_executor.get_log_fields().await;
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: LogFieldsData {
                dedicated_columns: dedicated,
                common_attributes: common,
            },
        }),
    )
        .into_response()
}

/// Handler for GET /v1/logs/field_values.
pub async fn list_log_field_values(
    State(state): State<AppState>,
    params: Result<Query<serde_json::Value>, QueryRejection>,
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

    let field = params
        .get("field_name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;

    let values = state
        .inner
        .query_executor
        .get_log_field_values(field, limit)
        .await;
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: values,
        }),
    )
        .into_response()
}

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
pub async fn list_label_values(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let values = state.inner.query_executor.list_label_values(&name).await;
    (
        StatusCode::OK,
        Json(PrometheusResponse {
            status: "success".into(),
            data: values.into_iter().collect::<Vec<_>>(),
        }),
    )
        .into_response()
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
    let filter = if trace_id.is_empty() {
        None
    } else {
        Some(trace_id)
    };

    match state
        .inner
        .query_executor
        .query_traces(start_ns, end_ns, filter, 200)
        .await
    {
        Ok(spans) => {
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
                    "data": { "trace_id": tid, "spans": span_json }
                })),
            )
                .into_response()
        }
        Err(e) => map_error(e),
    }
}

fn map_error(e: parqtel_core::Error) -> Response {
    let status = match &e {
        parqtel_core::Error::Validation(_) => StatusCode::BAD_REQUEST,
        parqtel_core::Error::InvalidOtlp(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    (
        status,
        Json(json!({
            "status": "error",
            "errorType": "bad_data",
            "error": e.to_string()
        })),
    )
        .into_response()
}

fn parse_duration(s: &str) -> Result<i64, ()> {
    if s.is_empty() {
        return Err(());
    }
    let val: f64 = s[..s.len() - 1].parse().map_err(|_| ())?;
    let unit = &s[s.len() - 1..];
    let multiplier: i64 = match unit {
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3600 * 1_000_000_000,
        "d" => 24 * 3600 * 1_000_000_000,
        _ => return Err(()),
    };
    Ok((val * multiplier as f64) as i64)
}
