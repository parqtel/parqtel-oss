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
            .execute_ast(&expr, end_ns, end_ns + 1, None)
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

    let start_ns = (params.start * 1_000_000_000.0) as i64;
    let end_ns = (params.end * 1_000_000_000.0) as i64;
    let limit = params.limit.unwrap_or(1000).min(10000);
    let order_desc = params.order.as_deref() != Some("asc");

    // ParqtelQL dispatch: an explicit selector shape (`{...}` or
    // `key="value"` with quoted values) uses the legacy matcher path;
    // everything else — bare terms, field:value, severity>=, ranges —
    // parses leniently as a ParqtelQL search (bare words search bodies,
    // matching the ClickStack-style search-box contract).
    let q_trim = params.query.trim();
    let selector_shape = q_trim.starts_with('{')
        || (q_trim.contains("=\"") && !q_trim.contains(' '))
        || q_trim.is_empty();
    if !selector_shape || params.severity_min.is_some() || params.search.is_some() {
        // ParqtelQL path (severity_min/search params fold into clauses).
        let mut q = parqtel_query::logql::parse_search(&params.query);
        if let Some(min) = &params.severity_min {
            q.clauses.push(parqtel_query::logql::Clause::SeverityMin(
                severity_word(*min).to_string(),
            ));
        }
        if let Some(text) = &params.search {
            q.terms.push(parqtel_query::logql::SearchTerm {
                text: text.to_lowercase(),
                negate: false,
                phrase: false,
                wildcard: false,
            });
        }
        let result = state
            .inner
            .query_executor
            .search_logs(start_ns, end_ns, &q, limit, order_desc)
            .await;
        return match result {
            Ok(result) => (
                StatusCode::OK,
                Json(json!({
                    "status": "success",
                    "data": {
                        "logs": result.logs,
                        "total_matched": result.total_logs_count,
                        "truncated": result.total_logs_count > limit,
                        "volume_summary": result.volume_summary,
                        "language": "parqtelql",
                    }
                })),
            )
                .into_response(),
            Err(e) => map_error(e),
        };
    }

    let (_, matchers) = match parqtel_query::parse_selector(&params.query) {
        Ok(res) => res,
        Err(e) => return map_error(e),
    };

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

/// Maps a numeric severity_min (OTel number) back to a rank word for
/// ParqtelQL SeverityMin clauses.
fn severity_word(min: i32) -> &'static str {
    match min {
        i if i >= 17 => "ERROR",
        i if i >= 13 => "WARN",
        i if i >= 9 => "INFO",
        i if i >= 5 => "DEBUG",
        _ => "TRACE",
    }
}

/// Applies a ParqtelQL SearchQuery to a span (service/status/duration/
/// kind/name/attr.* predicates).
fn span_matches(sq: &parqtel_query::logql::SearchQuery, s: &parqtel_core::Span) -> bool {
    use parqtel_query::logql::Clause;

    for clause in &sq.clauses {
        let ok = match clause {
            Clause::Eq { field, value } => span_field(s, field)
                .map(|v| v.eq_ignore_ascii_case(value))
                .unwrap_or(false),
            Clause::Ne { field, value } => span_field(s, field)
                .map(|v| !v.eq_ignore_ascii_case(value))
                .unwrap_or(true),
            Clause::Re { field, regex } => {
                let re = regex::Regex::new(regex).ok();
                match (span_field(s, field), re) {
                    (Some(v), Some(re)) => re.is_match(&v),
                    _ => false,
                }
            }
            Clause::Cmp { field, op, value } => {
                // duration in ms; other numeric fields via attributes.
                let n = if field == "duration" || field == "duration_ms" {
                    Some(s.duration_ns() as f64 / 1_000_000.0)
                } else {
                    span_field(s, field).and_then(|v| v.parse::<f64>().ok())
                };
                match n {
                    Some(n) => match op {
                        parqtel_query::logql::CmpOp::Gt => n > *value,
                        parqtel_query::logql::CmpOp::Ge => n >= *value,
                        parqtel_query::logql::CmpOp::Lt => n < *value,
                        parqtel_query::logql::CmpOp::Le => n <= *value,
                    },
                    None => false,
                }
            }
            Clause::Range { field, min, max } => {
                if field == "duration" || field == "duration_ms" {
                    let d = s.duration_ns() as f64 / 1_000_000.0;
                    d >= *min && d <= *max
                } else {
                    false
                }
            }
            Clause::Exists { field } => span_field(s, field).is_some(),
            Clause::SeverityMin(_) => true, // n/a for spans
        };
        if !ok {
            return false;
        }
    }
    for term in &sq.terms {
        let name = s.name.to_lowercase();
        let matched = name.contains(&term.text)
            || s.attributes
                .iter()
                .any(|(_, v)| v.to_lowercase().contains(&term.text));
        if term.negate {
            if matched {
                return false;
            }
        } else if !matched {
            return false;
        }
    }
    true
}

/// Resolves a ParqtelQL field to a span value.
fn span_field(s: &parqtel_core::Span, field: &str) -> Option<String> {
    match field {
        "service" | "service.name" => s.attributes.get("service.name").map(|v| v.to_string()),
        "name" | "operation" | "operation_name" => Some(s.name.clone()),
        "status" => Some(
            match s.status.code {
                2 => "ERROR",
                1 => "OK",
                _ => "UNSET",
            }
            .to_string(),
        ),
        "kind" => Some(
            match s.kind {
                1 => "internal",
                2 => "server",
                3 => "client",
                4 => "producer",
                5 => "consumer",
                _ => "unspecified",
            }
            .to_string(),
        ),
        "trace_id" => Some(hex::encode(s.trace_id)),
        _ => {
            if let Some(key) = field.strip_prefix("attr.") {
                s.attributes.get(key).map(|v| v.to_string())
            } else {
                s.attributes.get(field).map(|v| v.to_string())
            }
        }
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
    // ParqtelQL span predicates (Phase 1B): service=, status=ERROR,
    // duration>500, kind=server, attr.* — applied post-scan.
    let search = params
        .get("q")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(parqtel_query::logql::parse_search);

    match state
        .inner
        .query_executor
        .query_traces(start_ns, end_ns, filter, 200)
        .await
    {
        Ok(spans) => {
            // Apply ParqtelQL span predicates when a `q` was provided.
            let spans: Vec<_> = match &search {
                Some(sq) => spans.into_iter().filter(|s| span_matches(sq, s)).collect(),
                None => spans,
            };
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

// ═══ Saved searches (Phase 1B) ═══

pub async fn list_saved_searches(State(state): State<AppState>) -> Response {
    let searches = state.inner.saved_searches.list().await;
    (
        StatusCode::OK,
        Json(json!({ "status": "success", "data": searches })),
    )
        .into_response()
}

pub async fn create_saved_search(
    State(state): State<AppState>,
    axum::Json(search): axum::Json<serde_json::Value>,
) -> Response {
    let parsed: Result<crate::saved_searches::SavedSearch, _> = serde_json::from_value(search);
    match parsed {
        Ok(s) => match state.inner.saved_searches.create(s).await {
            Ok(created) => (
                StatusCode::OK,
                Json(json!({ "status": "success", "data": created })),
            )
                .into_response(),
            Err(e) => map_error(e),
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn delete_saved_search(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    if state.inner.saved_searches.delete(&id).await {
        (StatusCode::OK, Json(json!({ "status": "success" }))).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "status": "error", "error": "saved search not found" })),
        )
            .into_response()
    }
}
