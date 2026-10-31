//! Log search, count and field-discovery endpoints.

use super::common::{map_error, PrometheusResponse};
use crate::state::AppState;
use axum::{
    extract::{rejection::QueryRejection, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
        // Boolean shapes (OR / NOT / grouping) take the predicate-tree path.
        match parqtel_query::logql::parse_predicate(params.query.trim()) {
            Ok(pred)
                if matches!(
                    &pred,
                    parqtel_query::logql::Predicate::Or(_)
                        | parqtel_query::logql::Predicate::Not(_)
                ) =>
            {
                let result = state
                    .inner
                    .query_executor
                    .search_logs_predicate(start_ns, end_ns, &pred, limit, order_desc)
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
            _ => {}
        }
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
