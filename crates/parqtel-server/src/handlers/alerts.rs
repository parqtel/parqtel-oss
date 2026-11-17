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
    tracing::debug!(count = alerts.len(), "listing active alerts");
    ok_json(alerts)
}

/// GET /api/v1/alerts/:id
pub async fn get_alert(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let Ok(ulid) = id.parse::<Ulid>() else {
        tracing::debug!(alert_id = %id, "get_alert: invalid ulid format");
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid alert id"})),
        )
            .into_response();
    };
    tracing::debug!(alert_id = %ulid.to_string(), "fetching alert by id");
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
        instance.acknowledged_by = Some(by.clone());
        instance.updated_at = chrono::Utc::now();
        instance.transition_log.push(transition);
        state.inner.alert_store.save(&instance).await;
        tracing::info!(
            alert_id = %ulid.to_string(),
            acknowledged_by = %by,
            "alert acknowledged"
        );
        ok_json(instance).into_response()
    } else {
        tracing::debug!(alert_id = %ulid.to_string(), "alert acknowledge: invalid state transition");
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

/// List configured notification routes (and Alertmanager routing tree with secrets redacted).
pub async fn list_routes(State(state): State<AppState>) -> impl IntoResponse {
    let config = state.inner.alert_router.config().await;
    let routing = state
        .inner
        .config
        .alerting
        .routing
        .as_ref()
        .map(|r| r.redacted());
    axum::Json(serde_json::json!({
        "status": "success",
        "data": config.routes,
        "routing": routing,
    }))
}

#[derive(Debug, serde::Deserialize)]
pub struct TestRouteRequest {
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
}

/// POST /api/v1/alerts/test_route — dry-run route, grouping, silence, and inhibition evaluation.
pub async fn test_route(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<TestRouteRequest>,
) -> axum::response::Response {
    let silences = state.inner.alert_router.silences().await;
    let active_alerts = state.inner.alert_store.list_active().await;
    let now_unix = chrono::Utc::now().timestamp();
    let fingerprint = parqtel_alert::AlertInstance::compute_fingerprint("test_route", &req.labels);

    let Some(ref routing_cfg) = state.inner.config.alerting.routing else {
        let silenced = silences.iter().any(|s| {
            s.ends_at > now_unix
                && s.starts_at <= now_unix
                && s.match_labels
                    .iter()
                    .all(|(k, v)| req.labels.get(k).map(String::as_str) == Some(v.as_str()))
        });
        return axum::Json(serde_json::json!({
            "status": "success",
            "data": {
                "silenced": silenced,
                "inhibited": false,
                "matched_routes": [],
            }
        }))
        .into_response();
    };

    let evaluator = match parqtel_alert::routing::RoutingEvaluator::compile(routing_cfg) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "status": "error", "error": e.to_string() })),
            )
                .into_response();
        }
    };

    let eval = evaluator.evaluate(
        &req.labels,
        fingerprint,
        &silences,
        &active_alerts,
        now_unix,
    );
    let routes_only = evaluator.match_routes_only(&req.labels);
    let dummy_alert = parqtel_alert::AlertInstance {
        id: ulid::Ulid::new(),
        rule_id: "test_route".into(),
        rule_name: "test_route".into(),
        fingerprint,
        labels: req.labels,
        annotations: std::collections::BTreeMap::new(),
        state: parqtel_alert::AlertState::Firing,
        severity: parqtel_alert::Severity::Warning,
        value: None,
        started_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        resolved_at: None,
        acknowledged_by: None,
        noise_score: 0.0,
        transition_log: Vec::new(),
        notification_sent: false,
        source_rule_type: parqtel_alert::RuleType::Static,
    };

    let matched_routes: Vec<serde_json::Value> = routes_only
        .into_iter()
        .map(|r| {
            let (group_key, group_labels) =
                parqtel_alert::dispatcher::group::compute_group_key(&r, &dummy_alert);
            serde_json::json!({
                "route_path_id": r.route_path_id,
                "receiver": r.receiver,
                "group_by": r.group_by,
                "disable_grouping": r.disable_grouping,
                "group_key_hash": group_key.group_key_hash,
                "group_labels": group_labels,
                "group_wait_secs": r.group_wait.as_secs(),
                "group_interval_secs": r.group_interval.as_secs(),
                "repeat_interval_secs": r.repeat_interval.as_secs(),
            })
        })
        .collect();

    axum::Json(serde_json::json!({
        "status": "success",
        "data": {
            "silenced": eval.silenced,
            "inhibited": eval.inhibited,
            "matched_routes": matched_routes,
        }
    }))
    .into_response()
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use axum::body::to_bytes;
    use parqtel_core::config::{
        AlertRouteConfig, AlertmanagerRoutingConfig, InhibitRuleConfig, ReceiverConfig,
        SilenceConfig, SlackReceiverConfig,
    };
    use parqtel_core::Config;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn test_list_routes_redacts_secrets_and_test_route_evaluates() {
        let mut config = Config::default();
        config.alerting.routing = Some(AlertmanagerRoutingConfig {
            receivers: vec![ReceiverConfig {
                name: "slack-ops".into(),
                slack_configs: vec![SlackReceiverConfig {
                    webhook_url: "https://hooks.slack.com/services/SECRET/TOKEN".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            route: Some(AlertRouteConfig {
                receiver: Some("slack-ops".into()),
                group_by: Some(vec!["service.name".into()]),
                ..Default::default()
            }),
            inhibit_rules: vec![InhibitRuleConfig {
                source_matchers: vec!["severity = critical".into()],
                target_matchers: vec!["severity = warning".into()],
                equal: vec!["service.name".into()],
            }],
            ..Default::default()
        });
        let state = AppState::default_for_tests_with_config(config).await;

        // 1. GET /api/v1/alerts/routes redacts secrets
        let resp = list_routes(State(state.clone())).await.into_response();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["routing"]["receivers"][0]["slack_configs"][0]["webhook_url"],
            "***REDACTED***"
        );

        // 2. POST /api/v1/alerts/test_route matches route and computes group_labels
        let resp = test_route(
            State(state.clone()),
            axum::Json(TestRouteRequest {
                labels: BTreeMap::from([
                    ("severity".into(), "warning".into()),
                    ("service.name".into(), "checkout".into()),
                ]),
            }),
        )
        .await;
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["silenced"], false);
        assert_eq!(json["data"]["inhibited"], false);
        assert_eq!(json["data"]["matched_routes"][0]["receiver"], "slack-ops");
        assert_eq!(
            json["data"]["matched_routes"][0]["group_labels"]["service.name"],
            "checkout"
        );

        // 3. Add a firing critical alert -> warning alert becomes inhibited
        state
            .inner
            .alert_store
            .save(&parqtel_alert::AlertInstance {
                id: ulid::Ulid::new(),
                rule_id: "crit-1".into(),
                rule_name: "crit".into(),
                fingerprint: 999,
                labels: BTreeMap::from([
                    ("severity".into(), "critical".into()),
                    ("service.name".into(), "checkout".into()),
                ]),
                annotations: BTreeMap::new(),
                state: parqtel_alert::AlertState::Firing,
                severity: parqtel_alert::Severity::Critical,
                value: Some(1.0),
                started_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                resolved_at: None,
                acknowledged_by: None,
                noise_score: 0.0,
                transition_log: vec![],
                notification_sent: false,
                source_rule_type: parqtel_alert::RuleType::Static,
            })
            .await;

        let resp = test_route(
            State(state.clone()),
            axum::Json(TestRouteRequest {
                labels: BTreeMap::from([
                    ("severity".into(), "warning".into()),
                    ("service.name".into(), "checkout".into()),
                ]),
            }),
        )
        .await;
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["inhibited"], true);

        // 4. Add an active silence -> silenced = true
        let now = chrono::Utc::now().timestamp();
        state
            .inner
            .alert_router
            .add_silence(SilenceConfig {
                name: "maint".into(),
                created_by: "test".into(),
                match_labels: BTreeMap::from([("service.name".into(), "checkout".into())]),
                starts_at: now - 60,
                ends_at: now + 60,
            })
            .await;
        let resp = test_route(
            State(state.clone()),
            axum::Json(TestRouteRequest {
                labels: BTreeMap::from([("service.name".into(), "checkout".into())]),
            }),
        )
        .await;
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["silenced"], true);
    }

    #[tokio::test]
    async fn test_route_without_routing_config_and_metrics_render() {
        let state = AppState::default_for_tests().await;
        let resp = test_route(
            State(state.clone()),
            axum::Json(TestRouteRequest {
                labels: BTreeMap::from([("service.name".into(), "api".into())]),
            }),
        )
        .await;
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["matched_routes"], serde_json::json!([]));

        state
            .inner
            .metrics
            .routing_metrics
            .inc_notification("slack-ops", "slack", "success");
        let rendered = state.inner.metrics.render(&state.inner.index).await;
        assert!(rendered.contains("parqtel_alert_notifications_total"));
        assert!(rendered.contains("parqtel_alert_active_groups"));
    }
}
