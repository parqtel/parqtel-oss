use crate::state::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

fn runtime_updates_forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "status": "error",
            "error": "runtime updates are disabled; set alerts.allow_runtime_updates = true behind an authenticating proxy"
        })),
    )
        .into_response()
}

pub async fn list_recording_rules(State(state): State<AppState>) -> impl IntoResponse {
    let groups = state.inner.pipeline_registry.get_groups();
    Json(json!({"status": "success", "data": groups}))
}

pub async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
    let pipelines = state.inner.pipeline_registry.get_pipelines();
    Json(json!({"status": "success", "data": pipelines}))
}

pub async fn create_recording_rule(
    State(state): State<AppState>,
    Json(group): Json<parqtel_pipeline::rule::schema::RecordingRuleGroup>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(group = %group.name, rules = group.rules.len(), "creating recording rule");
    state.inner.pipeline_registry.add_group(group.clone());
    (
        StatusCode::CREATED,
        Json(json!({"status": "success", "data": group})),
    )
        .into_response()
}

pub async fn create_pipeline(
    State(state): State<AppState>,
    Json(pipeline): Json<parqtel_pipeline::rule::schema::PipelineDefinition>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(name = %pipeline.name, "creating pipeline");
    state.inner.pipeline_registry.add_pipeline(pipeline.clone());
    (
        StatusCode::CREATED,
        Json(json!({"status": "success", "data": pipeline})),
    )
        .into_response()
}

pub async fn delete_recording_rule(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(group = %name, "deleting recording rule");
    state.inner.pipeline_registry.remove_group(&name);
    Json(json!({"status": "success"})).into_response()
}

pub async fn delete_pipeline(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(name = %name, "deleting pipeline");
    state.inner.pipeline_registry.remove_pipeline(&name);
    Json(json!({"status": "success"})).into_response()
}
