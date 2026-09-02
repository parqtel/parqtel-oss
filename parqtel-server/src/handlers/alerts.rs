use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use ulid::Ulid;

use crate::state::AppState;
use parqtel_alert::state::machine::{AlertStateMachine, TransitionEvent};

#[derive(Serialize)]
struct AlertResponse<T: Serialize> {
    status: &'static str,
    data: T,
}

fn ok_json<T: Serialize>(data: T) -> impl IntoResponse {
    Json(AlertResponse {
        status: "success",
        data,
    })
}

/// GET /api/v1/alerts
pub async fn list_alerts(State(state): State<AppState>) -> impl IntoResponse {
    let alerts = state.inner.alert_store.list_active().await;
    ok_json(alerts)
}

/// GET /api/v1/alerts/:id
pub async fn get_alert(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let Ok(ulid) = id.parse::<Ulid>() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid alert id"})),
        )
            .into_response();
    };
    match state.inner.alert_store.get_by_id(ulid).await {
        Some(alert) => ok_json(alert).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "alert not found"})),
        )
            .into_response(),
    }
}

/// POST /api/v1/alerts/:id/acknowledge
pub async fn acknowledge_alert(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let Ok(ulid) = id.parse::<Ulid>() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid alert id"})),
        )
            .into_response();
    };
    let Some(mut instance) = state.inner.alert_store.get_by_id(ulid).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "alert not found"})),
        )
            .into_response();
    };
    let by = body
        .get("by")
        .and_then(|v| v.as_str())
        .unwrap_or("anonymous")
        .to_string();
    if let Some((new_state, transition)) = AlertStateMachine::transition(
        instance.state,
        TransitionEvent::Acknowledged { by: by.clone() },
    ) {
        instance.state = new_state;
        instance.acknowledged_by = Some(by);
        instance.updated_at = chrono::Utc::now();
        instance.transition_log.push(transition);
        state.inner.alert_store.save(&instance).await;
        ok_json(instance).into_response()
    } else {
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "invalid state transition"})),
        )
            .into_response()
    }
}

/// POST /api/v1/alerts/:id/resolve
pub async fn resolve_alert(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let Ok(ulid) = id.parse::<Ulid>() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid alert id"})),
        )
            .into_response();
    };
    let Some(mut instance) = state.inner.alert_store.get_by_id(ulid).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "alert not found"})),
        )
            .into_response();
    };
    if let Some((new_state, transition)) =
        AlertStateMachine::transition(instance.state, TransitionEvent::ConditionCleared)
    {
        instance.state = new_state;
        instance.resolved_at = Some(chrono::Utc::now());
        instance.updated_at = chrono::Utc::now();
        instance.transition_log.push(transition);
        state.inner.alert_store.save(&instance).await;
        ok_json(instance).into_response()
    } else {
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "invalid state transition"})),
        )
            .into_response()
    }
}

/// GET /api/v1/rules
pub async fn list_rules(State(state): State<AppState>) -> impl IntoResponse {
    let rules = state.inner.alert_registry.list_all().await;
    ok_json(rules)
}

/// POST /api/v1/rules
pub async fn create_rule(State(state): State<AppState>, body: String) -> impl IntoResponse {
    match parqtel_alert::rule::yaml::parse_rule(&body) {
        Ok(rule) => {
            state.inner.alert_registry.insert(rule.clone()).await;
            (StatusCode::CREATED, ok_json(rule)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// PUT /api/v1/rules/:id
pub async fn update_rule(
    State(state): State<AppState>,
    Path(_id): Path<String>,
    body: String,
) -> impl IntoResponse {
    match parqtel_alert::rule::yaml::parse_rule(&body) {
        Ok(rule) => {
            if state.inner.alert_registry.update(rule.clone()).await {
                ok_json(rule).into_response()
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "rule not found"})),
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /api/v1/rules/:id
pub async fn delete_rule(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if state.inner.alert_registry.disable(&id).await {
        ok_json(serde_json::json!({"disabled": true})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "rule not found"})),
        )
            .into_response()
    }
}

// ═══ Alert routing (F10) ═══

/// List configured notification routes.
pub async fn list_routes(State(state): State<AppState>) -> impl IntoResponse {
    let config = state.inner.alert_router.config().await;
    axum::Json(serde_json::json!({ "status": "success", "data": config.routes }))
}

/// Replace the full routing table (routes are config-driven; this updates
/// the running router atomically).
pub async fn set_routes(
    State(state): State<AppState>,
    axum::Json(routes): axum::Json<Vec<parqtel_core::config::RouteConfig>>,
) -> impl IntoResponse {
    let mut config = state.inner.alert_router.config().await;
    config.routes = routes;
    state.inner.alert_router.set_config(config).await;
    axum::Json(serde_json::json!({ "status": "success" }))
}

/// List active (unexpired) silences.
pub async fn list_silences(State(state): State<AppState>) -> impl IntoResponse {
    let silences = state.inner.alert_router.silences().await;
    axum::Json(serde_json::json!({ "status": "success", "data": silences }))
}

/// Create or replace a silence by name.
pub async fn create_silence(
    State(state): State<AppState>,
    axum::Json(silence): axum::Json<parqtel_core::config::SilenceConfig>,
) -> axum::response::Response {
    if silence.name.trim().is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "status": "error", "error": "name is required" })),
        )
            .into_response();
    }
    if silence.ends_at <= silence.starts_at {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "status": "error", "error": "ends_at must be after starts_at" })),
        )
            .into_response();
    }
    state.inner.alert_router.add_silence(silence).await;
    axum::Json(serde_json::json!({ "status": "success" })).into_response()
}

/// Delete a silence by name.
pub async fn delete_silence(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    let removed = state.inner.alert_router.remove_silence(&name).await;
    if removed {
        axum::Json(serde_json::json!({ "status": "success" })).into_response()
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({ "status": "error", "error": "silence not found" })),
        )
            .into_response()
    }
}
