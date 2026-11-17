//! Dashboard CRUD and server-side panel rendering.

use super::query::common::map_error;
use crate::dashboards::{DashboardInput, DashboardPanel};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub async fn list_dashboards(State(state): State<AppState>) -> Response {
    Json(json!({
        "status": "success",
        "data": state.inner.dashboards.list().await,
    }))
    .into_response()
}

pub async fn create_dashboard(
    State(state): State<AppState>,
    Json(input): Json<DashboardInput>,
) -> Response {
    match state.inner.dashboards.create(input).await {
        Ok(dashboard) => (
            StatusCode::CREATED,
            Json(json!({"status": "success", "data": dashboard})),
        )
            .into_response(),
        Err(e) => map_error(e),
    }
}

pub async fn update_dashboard(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<DashboardInput>,
) -> Response {
    match state.inner.dashboards.update(&id, input).await {
        Some(dashboard) => Json(json!({"status": "success", "data": dashboard})).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"status": "error", "error": "dashboard not found"})),
        )
            .into_response(),
    }
}

pub async fn delete_dashboard(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if state.inner.dashboards.delete(&id).await {
        Json(json!({"status": "success"})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"status": "error", "error": "dashboard not found"})),
        )
            .into_response()
    }
}

/// `GET /api/v1/dashboards/:id/render?start=&end=`
///
/// Executes every panel against the live engine. One failing panel returns an
/// error for that panel only, so the rest of the dashboard still renders.
pub async fn render_dashboard(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<serde_json::Value>,
) -> Response {
    let Some(dashboard) = state.inner.dashboards.get(&id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"status": "error", "error": "dashboard not found"})),
        )
            .into_response();
    };
    let end = params
        .get("end")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or_else(|| chrono::Utc::now().timestamp() as f64);
    let start = params
        .get("start")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(end - 3_600.0);
    let (start_ns, end_ns) = (
        (start * 1_000_000_000.0) as i64,
        (end * 1_000_000_000.0) as i64,
    );

    let mut panels = Vec::with_capacity(dashboard.panels.len());
    for panel in &dashboard.panels {
        let value = match render_panel(&state, panel, start_ns, end_ns).await {
            Ok(v) => json!({"id": panel.id, "title": panel.title, "result": v}),
            Err(e) => {
                json!({"id": panel.id, "title": panel.title, "error": e.to_string()})
            }
        };
        panels.push(value);
    }

    Json(json!({
        "status": "success",
        "data": {
            "dashboard_id": dashboard.id,
            "start": start_ns,
            "end": end_ns,
            "panels": panels,
        }
    }))
    .into_response()
}

async fn render_panel(
    state: &AppState,
    panel: &DashboardPanel,
    start_ns: i64,
    end_ns: i64,
) -> parqtel_core::Result<serde_json::Value> {
    match panel.signal.as_str() {
        "metrics" => {
            let expr = parqtel_query::parser::parse_expr(&panel.query)?;
            let result = state
                .inner
                .query_executor
                .execute_ast(
                    &expr,
                    start_ns,
                    end_ns,
                    Some(panel.step_secs as i64 * 1_000_000_000),
                )
                .await?;
            Ok(serde_json::to_value(result).map_err(|e| {
                parqtel_core::Error::Internal(format!("serialising panel result: {e}"))
            })?)
        }
        "logs" => {
            let (_, matchers) = parqtel_query::parse_selector(&panel.query)?;
            let result = state
                .inner
                .query_executor
                .query_logs(start_ns, end_ns, matchers, panel.limit, true, None, None)
                .await?;
            Ok(serde_json::to_value(result).map_err(|e| {
                parqtel_core::Error::Internal(format!("serialising panel result: {e}"))
            })?)
        }
        other => Err(parqtel_core::Error::Validation(format!(
            "unsupported panel signal: {other}"
        ))),
    }
}
