//! Prometheus-compatible instant and range query endpoints.

use super::common::{
    map_error, parse_step_duration, PrometheusData, PrometheusResponse, PrometheusResult,
};
use crate::state::AppState;
use axum::{
    extract::{rejection::QueryRejection, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use parqtel_query::{parse_query, AggregationOp, QueryPlan};
use serde::Deserialize;
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

/// Renders a QueryResult as a Prometheus instant-vector response.
fn render_vector_result(result: parqtel_query::QueryResult) -> Response {
    let mut res = Vec::new();
    for ts in result.series {
        let values: Vec<(f64, String)> = ts
            .samples
            .into_iter()
            .map(|s| (s.timestamp_ns as f64 / 1_000_000_000.0, s.value.to_string()))
            .collect();
        // Instant vector: expose the (single) evaluation-timestamp sample in
        // the spec-required `value` field. The AST path evaluates one step
        // per series, so the last sample is the value at query time.
        let value = values.last().cloned();
        res.push(PrometheusResult {
            metric: serde_json::to_value(&ts.labels).unwrap_or_default(),
            value,
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
            value: None,
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
/// Handler for GET /api/v1/query (instant).
pub async fn query_instant(
    State(state): State<AppState>,
    params: Result<Query<InstantQuery>, QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(q) => q,
        Err(e) => {
            tracing::debug!(error = %e, "query_instant: invalid params");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": e.to_string()})),
            )
                .into_response();
        }
    };

    tracing::debug!(query = %params.query, time = params.time, "query_instant received");

    let now = params
        .time
        .unwrap_or_else(|| chrono::Utc::now().timestamp() as f64);

    // Composed queries (nesting, binary ops, subqueries, ALL
    // aggregations) go through the AST evaluator; simple selector shapes
    // keep the legacy plan path. The AST parse is attempted FIRST: the
    // legacy parse_query() mis-handles nested braced selectors — e.g. for
    // sum(rate(x{a="b"}[5m])) its strip_fn() hands
    // `x{a="b"}[5m]` to parse_selector(), which rejects the `[5m]` with
    // "Selector missing closing brace" — so a legacy-first order would
    // return that error and never reach the AST engine that parses it fine.
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

    let plan = match QueryPlan::new_full(
        metric_name,
        matchers,
        (now * 1_000_000_000.0) as i64 - state.inner.config.query.lookback_delta_ns,
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
                let values: Vec<(f64, String)> = ts
                    .samples
                    .into_iter()
                    .map(|s| (s.timestamp_ns as f64 / 1_000_000_000.0, s.value.to_string()))
                    .collect();

                res.push(PrometheusResult {
                    metric: serde_json::to_value(&ts.labels).unwrap_or_default(),
                    value: values.last().cloned(),
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

    tracing::debug!(
        query = %params.query,
        start = params.start,
        end = params.end,
        step = params.step,
        "query_range received"
    );

    let start_ns = (params.start * 1_000_000_000.0) as i64;
    let end_ns = (params.end * 1_000_000_000.0) as i64;

    let step_ns = match parse_step_duration(&params.step) {
        Ok(d) => d,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": "Invalid step duration"})),
            )
                .into_response()
        }
    };

    // Composed queries via the AST evaluator — attempted FIRST,
    // before the legacy parse_query() (which rejects nested braced range
    // selectors like sum(rate(x{a="b"}[5m])) with "Selector missing closing
    // brace" even though the AST parser accepts them).
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
                    value: None,
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
