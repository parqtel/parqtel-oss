use crate::auth::Actor;
use crate::state::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde_json::json;

fn actor_name(actor: &Option<Extension<Actor>>) -> &str {
    actor
        .as_ref()
        .map(|Extension(actor)| actor.0.as_str())
        .unwrap_or("anonymous")
}

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
    actor: Option<Extension<Actor>>,
    Json(group): Json<parqtel_pipeline::rule::schema::RecordingRuleGroup>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(group = %group.name, rules = group.rules.len(), "creating recording rule");
    match state.inner.pipeline_registry.add_group(group.clone()) {
        Ok(()) => {
            state.inner.audit.record(
                actor_name(&actor),
                "pipelines.recording_rule.create",
                &group.name,
                None,
                serde_json::to_value(&group).ok(),
            );
            (
                StatusCode::CREATED,
                Json(json!({"status": "success", "data": group})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn create_pipeline(
    State(state): State<AppState>,
    actor: Option<Extension<Actor>>,
    Json(pipeline): Json<parqtel_pipeline::rule::schema::PipelineDefinition>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(name = %pipeline.name, "creating pipeline");
    match state.inner.pipeline_registry.add_pipeline(pipeline.clone()) {
        Ok(()) => {
            state.inner.audit.record(
                actor_name(&actor),
                "pipelines.pipeline.create",
                &pipeline.name,
                None,
                serde_json::to_value(&pipeline).ok(),
            );
            (
                StatusCode::CREATED,
                Json(json!({"status": "success", "data": pipeline})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn delete_recording_rule(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    actor: Option<Extension<Actor>>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(group = %name, "deleting recording rule");
    state.inner.pipeline_registry.remove_group(&name);
    state.inner.audit.record(
        actor_name(&actor),
        "pipelines.recording_rule.delete",
        &name,
        None,
        None,
    );
    Json(json!({"status": "success"})).into_response()
}

pub async fn delete_pipeline(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    actor: Option<Extension<Actor>>,
) -> Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return runtime_updates_forbidden();
    }
    tracing::info!(name = %name, "deleting pipeline");
    state.inner.pipeline_registry.remove_pipeline(&name);
    state.inner.audit.record(
        actor_name(&actor),
        "pipelines.pipeline.delete",
        &name,
        None,
        None,
    );
    Json(json!({"status": "success"})).into_response()
}

/// GET /api/v1/rules/status — registry generation, definitions, load errors,
/// and the last persisted ruler evaluation timestamps.
pub async fn rules_status(State(state): State<AppState>) -> impl IntoResponse {
    let registry = &state.inner.pipeline_registry;
    let groups: Vec<serde_json::Value> = registry
        .get_groups()
        .into_iter()
        .map(|group| {
            json!({
                "name": group.name,
                "interval": group.interval,
                "rules": group.rules.iter().map(|rule| &rule.record).collect::<Vec<_>>(),
            })
        })
        .collect();
    let pipelines: Vec<serde_json::Value> = registry
        .get_pipelines()
        .into_iter()
        .map(|pipeline| {
            json!({
                "name": pipeline.name,
                "stages": pipeline.stages.iter().map(|stage| &stage.name).collect::<Vec<_>>(),
            })
        })
        .collect();
    let state_file = state.inner.config.storage.data_dir.join("ruler/state.json");
    let ruler_state = std::fs::read_to_string(state_file)
        .ok()
        .and_then(|content| serde_json::from_str::<serde_json::Value>(&content).ok())
        .unwrap_or_else(|| json!({}));

    Json(json!({
        "status": "success",
        "data": {
            "generation": registry.generation(),
            "recording_rules": groups,
            "pipelines": pipelines,
            "load_errors": registry.load_errors(),
            "ruler_enabled": state.inner.config.alerts.recording_rules_enabled,
            "ruler_state": ruler_state,
        }
    }))
}
