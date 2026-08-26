use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

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
) -> impl IntoResponse {
    state.inner.pipeline_registry.add_group(group.clone());
    (
        StatusCode::CREATED,
        Json(json!({"status": "success", "data": group})),
    )
}

pub async fn create_pipeline(
    State(state): State<AppState>,
    Json(pipeline): Json<parqtel_pipeline::rule::schema::PipelineDefinition>,
) -> impl IntoResponse {
    state.inner.pipeline_registry.add_pipeline(pipeline.clone());
    (
        StatusCode::CREATED,
        Json(json!({"status": "success", "data": pipeline})),
    )
}

pub async fn delete_recording_rule(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    state.inner.pipeline_registry.remove_group(&name);
    Json(json!({"status": "success"}))
}

pub async fn delete_pipeline(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    state.inner.pipeline_registry.remove_pipeline(&name);
    Json(json!({"status": "success"}))
}
