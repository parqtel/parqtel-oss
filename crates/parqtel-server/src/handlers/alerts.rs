use crate::auth::Actor;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
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

fn actor_name(actor: &Option<Extension<Actor>>) -> &str {
    actor
        .as_ref()
        .map(|Extension(actor)| actor.0.as_str())
        .unwrap_or("anonymous")
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
        if let Some(dispatcher) = state.inner.alert_dispatcher.as_ref() {
            dispatcher.sync_alert_state(&instance, false).await;
        }
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
        if let Some(dispatcher) = state.inner.alert_dispatcher.as_ref() {
            dispatcher.sync_alert_state(&instance, true).await;
        }
        ok_json(instance).into_response()
    } else {
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "invalid state transition"})),
        )
            .into_response()
    }
}

/// POST /api/v1/alerts/:id/noise — manually classify this alert as noise.
pub async fn mark_alert_noise(
    State(state): State<AppState>,
    Path(id): Path<String>,
    actor: Option<Extension<Actor>>,
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
    let Some((new_state, transition)) =
        AlertStateMachine::transition(instance.state, TransitionEvent::MarkedNoise)
    else {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "invalid state transition"})),
        )
            .into_response();
    };
    instance.state = new_state;
    instance.noise_score = 1.0;
    instance.updated_at = chrono::Utc::now();
    instance.transition_log.push(transition);
    state.inner.alert_store.save(&instance).await;
    if let Some(dispatcher) = state.inner.alert_dispatcher.as_ref() {
        dispatcher.sync_alert_state(&instance, false).await;
        dispatcher.metrics().inc_suppressed_noise();
    }
    state.inner.audit.record(
        actor_name(&actor),
        "alerts.noise",
        &ulid.to_string(),
        None,
        Some(serde_json::json!({"state": new_state})),
    );
    ok_json(instance).into_response()
}

/// POST /api/v1/alerts/:id/signal — undo a noise flag and restore firing.
pub async fn mark_alert_signal(
    State(state): State<AppState>,
    Path(id): Path<String>,
    actor: Option<Extension<Actor>>,
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
    let Some((new_state, transition)) =
        AlertStateMachine::transition(instance.state, TransitionEvent::NoiseScoreDropped)
    else {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "invalid state transition"})),
        )
            .into_response();
    };
    instance.state = new_state;
    instance.noise_score = 0.0;
    instance.updated_at = chrono::Utc::now();
    instance.transition_log.push(transition);
    state.inner.alert_store.save(&instance).await;
    if let Some(dispatcher) = state.inner.alert_dispatcher.as_ref() {
        dispatcher.sync_alert_state(&instance, false).await;
    }
    state.inner.audit.record(
        actor_name(&actor),
        "alerts.signal",
        &ulid.to_string(),
        None,
        Some(serde_json::json!({"state": new_state})),
    );
    ok_json(instance).into_response()
}

/// GET /api/v1/rules
pub async fn list_rules(State(state): State<AppState>) -> impl IntoResponse {
    let rules = state.inner.alert_registry.list_all().await;
    let since = chrono::Utc::now() - chrono::Duration::days(7);
    let mut out = Vec::with_capacity(rules.len());
    for rule in rules {
        let alerts = state.inner.alert_store.list_by_rule(&rule.id, since).await;
        let scored: Vec<f32> = alerts
            .iter()
            .filter(|alert| alert.noise_score > 0.0)
            .map(|alert| alert.noise_score)
            .collect();
        let noise_score = if scored.is_empty() {
            0.0
        } else {
            scored.iter().sum::<f32>() / scored.len() as f32
        };
        let mut value = serde_json::to_value(&rule).unwrap_or_default();
        value["noise_score"] = serde_json::json!(noise_score);
        out.push(value);
    }
    ok_json(out)
}

/// POST /api/v1/rules
pub async fn create_rule(
    State(state): State<AppState>,
    actor: Option<Extension<Actor>>,
    body: String,
) -> impl IntoResponse {
    match parqtel_alert::rule::yaml::parse_rule(&body) {
        Ok(rule) => {
            state.inner.alert_registry.insert(rule.clone()).await;
            state.inner.audit.record(
                actor_name(&actor),
                "alerts.rule.create",
                &rule.id,
                None,
                serde_json::to_value(&rule).ok(),
            );
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
    actor: Option<Extension<Actor>>,
    body: String,
) -> impl IntoResponse {
    match parqtel_alert::rule::yaml::parse_rule(&body) {
        Ok(rule) => {
            if state.inner.alert_registry.update(rule.clone()).await {
                state.inner.audit.record(
                    actor_name(&actor),
                    "alerts.rule.update",
                    &rule.id,
                    None,
                    serde_json::to_value(&rule).ok(),
                );
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
    actor: Option<Extension<Actor>>,
) -> impl IntoResponse {
    if state.inner.alert_registry.disable(&id).await {
        state.inner.audit.record(
            actor_name(&actor),
            "alerts.rule.disable",
            &id,
            None,
            Some(serde_json::json!({"disabled": true})),
        );
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

        evidence: None,

        rca: None,
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

fn route_audit_value(routes: &[parqtel_core::config::RouteConfig]) -> serde_json::Value {
    serde_json::Value::Array(
        routes
            .iter()
            .map(|route| {
                serde_json::json!({
                    "name": route.name,
                    "match_severity": route.match_severity,
                    "match_labels": route.match_labels,
                    // Never write a webhook URL (it often contains a secret).
                    "webhook_url": if route.webhook_url.is_empty() { "" } else { "***" },
                    "repeat_minutes": route.repeat_minutes,
                })
            })
            .collect(),
    )
}

/// Replace the full routing table (routes are config-driven; this updates
/// the running router atomically).
pub async fn set_routes(
    State(state): State<AppState>,
    actor: Option<Extension<Actor>>,
    axum::Json(routes): axum::Json<Vec<parqtel_core::config::RouteConfig>>,
) -> axum::response::Response {
    if !state.inner.config.alerts.allow_runtime_updates {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "status": "error",
                "error": "runtime updates are disabled; set alerts.allow_runtime_updates = true behind an authenticating proxy"
            })),
        )
            .into_response();
    }
    let mut config = state.inner.alert_router.config().await;
    let before = route_audit_value(&config.routes);
    let after = route_audit_value(&routes);
    state.inner.audit.record(
        actor_name(&actor),
        "alerts.routes.update",
        "routes",
        Some(before),
        Some(after),
    );
    config.routes = routes;
    state.inner.alert_router.set_config(config).await;
    axum::Json(serde_json::json!({ "status": "success" })).into_response()
}

/// List active (unexpired) silences.
pub async fn list_silences(State(state): State<AppState>) -> impl IntoResponse {
    let silences = state.inner.alert_router.silences().await;
    axum::Json(serde_json::json!({ "status": "success", "data": silences }))
}

/// Create or replace a silence by name.
pub async fn create_silence(
    State(state): State<AppState>,
    actor: Option<Extension<Actor>>,
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
    state.inner.audit.record(
        actor_name(&actor),
        "alerts.silence.create",
        &silence.name,
        None,
        serde_json::to_value(&silence).ok(),
    );
    state.inner.alert_router.add_silence(silence).await;
    axum::Json(serde_json::json!({ "status": "success" })).into_response()
}

/// Delete a silence by name.
pub async fn delete_silence(
    State(state): State<AppState>,
    Path(name): Path<String>,
    actor: Option<Extension<Actor>>,
) -> axum::response::Response {
    let removed = state.inner.alert_router.remove_silence(&name).await;
    if removed {
        state.inner.audit.record(
            actor_name(&actor),
            "alerts.silence.delete",
            &name,
            None,
            None,
        );
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

                evidence: None,

                rca: None,
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
    async fn set_routes_is_forbidden_by_default_and_persists_when_enabled() {
        let state = AppState::default_for_tests().await;
        let resp = set_routes(State(state.clone()), None, axum::Json(Vec::new())).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let mut config = parqtel_core::Config::default();
        config.alerts.allow_runtime_updates = true;
        let state = AppState::default_for_tests_with_config(config).await;
        let route = parqtel_core::config::RouteConfig {
            name: "persisted".into(),
            match_severity: "critical".into(),
            match_labels: BTreeMap::new(),
            webhook_url: "https://example.com/hook".into(),
            repeat_minutes: 240,
        };
        let resp = set_routes(State(state.clone()), None, axum::Json(vec![route])).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let routes = state.inner.alert_router.config().await.routes;
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].name, "persisted");
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

/// GET /api/v1/alerts/:id/evidence.json
pub async fn get_alert_evidence(
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
    let Some(instance) = state.inner.alert_store.get_by_id(ulid).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "alert not found"})),
        )
            .into_response();
    };
    ok_json(serde_json::json!({
        "id": ulid,
        "query": instance.evidence.as_ref().map(|e| e.query.clone()),
        "evidence": instance.evidence,
        "rca": instance.rca,
    }))
    .into_response()
}

/// GET /api/v1/alerts/:id/evidence.svg
pub async fn get_alert_evidence_svg(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let Ok(ulid) = id.parse::<Ulid>() else {
        return (StatusCode::BAD_REQUEST, "invalid alert id").into_response();
    };
    let Some(instance) = state.inner.alert_store.get_by_id(ulid).await else {
        return (StatusCode::NOT_FOUND, "alert not found").into_response();
    };
    let Some(evidence) = instance.evidence.as_ref() else {
        return (StatusCode::NOT_FOUND, "no evidence snapshot").into_response();
    };
    (
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("image/svg+xml"),
        )],
        render_evidence_svg(evidence),
    )
        .into_response()
}

fn render_evidence_svg(evidence: &parqtel_alert::AlertEvidence) -> String {
    const W: f64 = 720.0;
    const H: f64 = 240.0;
    const PAD: f64 = 24.0;
    let points = &evidence.points;
    if points.is_empty() {
        return format!(
            r##"<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}"><text x="{PAD}" y="{H}" fill="#888" font-family="monospace" font-size="12">no evidence points</text></svg>"##
        );
    }
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for point in points {
        min = min.min(point.value);
        max = max.max(point.value);
    }
    if let Some(threshold) = evidence.threshold {
        min = min.min(threshold);
        max = max.max(threshold);
    }
    if (max - min).abs() < f64::EPSILON {
        max = min + 1.0;
    }
    let first = points[0].timestamp_ns;
    let last = points[points.len() - 1].timestamp_ns;
    let span = (last - first).max(1) as f64;
    let x = |ts: i64| PAD + (ts - first) as f64 / span * (W - 2.0 * PAD);
    let y = |value: f64| H - PAD - (value - min) / (max - min) * (H - 2.0 * PAD);
    let path = points
        .iter()
        .enumerate()
        .map(|(index, point)| {
            format!(
                "{}{:.2},{:.2}",
                if index == 0 { "M" } else { " L" },
                x(point.timestamp_ns),
                y(point.value)
            )
        })
        .collect::<String>();
    let threshold = evidence.threshold.map_or_else(String::new, |value| {
        format!(
            r##"<line x1="{PAD:.1}" x2="{:.1}" y1="{:.2}" y2="{:.2}" stroke="#f59e0b" stroke-dasharray="5 4"/>"##,
            W - PAD,
            y(value),
            y(value)
        )
    });
    let observed = evidence.observed;
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
<rect width="100%" height="100%" fill="#111827"/>
<line x1="{PAD}" x2="{}" y1="{:.1}" y2="{:.1}" stroke="#374151"/>
{threshold}
<path d="{path}" fill="none" stroke="#22d3ee" stroke-width="2"/>
<text x="{PAD}" y="16" fill="#e5e7eb" font-family="monospace" font-size="11">{} · observed {observed:.6}</text>
<text x="{PAD}" y="{}" fill="#9ca3af" font-family="monospace" font-size="10">min {min:.4} · max {max:.4}</text>
</svg>"##,
        W - PAD,
        H - PAD,
        H - PAD,
        evidence.query.replace('&', "&amp;").replace('<', "&lt;"),
        H - 6.0
    )
}

#[cfg(test)]
mod evidence_svg_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn renders_points_threshold_and_query() {
        let evidence = parqtel_alert::AlertEvidence {
            query: "rate(errors[5m])".into(),
            signal: "metrics".into(),
            window_start_ns: 0,
            window_end_ns: 2,
            operator: Some(">".into()),
            threshold: Some(0.05),
            observed: 0.12,
            points: vec![
                parqtel_alert::EvidencePoint {
                    timestamp_ns: 1,
                    value: 0.01,
                },
                parqtel_alert::EvidencePoint {
                    timestamp_ns: 2,
                    value: 0.12,
                },
            ],
            series_count: 1,
        };
        let svg = render_evidence_svg(&evidence);
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("stroke-dasharray"));
        assert!(svg.contains("rate(errors[5m])"));
        assert!(svg.contains("observed 0.120000"));
    }

    #[test]
    fn empty_evidence_is_an_explicit_empty_graph() {
        let evidence = parqtel_alert::AlertEvidence {
            query: "up".into(),
            signal: "metrics".into(),
            window_start_ns: 0,
            window_end_ns: 1,
            operator: None,
            threshold: None,
            observed: 0.0,
            points: Vec::new(),
            series_count: 0,
        };
        assert!(render_evidence_svg(&evidence).contains("no evidence points"));
    }
}

#[cfg(test)]
mod alert_evidence_api_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use axum::body::to_bytes;
    use parqtel_core::Config;
    use std::collections::BTreeMap;

    fn sample_alert(state: parqtel_alert::AlertState) -> parqtel_alert::AlertInstance {
        parqtel_alert::AlertInstance {
            id: Ulid::new(),
            rule_id: "r1".into(),
            rule_name: "High errors".into(),
            fingerprint: 42,
            labels: BTreeMap::from([("service.name".into(), "api".into())]),
            annotations: BTreeMap::new(),
            state,
            severity: parqtel_alert::Severity::Critical,
            value: Some(0.2),
            started_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.8,
            transition_log: Vec::new(),
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
            evidence: Some(parqtel_alert::AlertEvidence {
                query: "rate(errors[5m])".into(),
                signal: "metrics".into(),
                window_start_ns: 0,
                window_end_ns: 2,
                operator: Some(">".into()),
                threshold: Some(0.1),
                observed: 0.2,
                points: vec![
                    parqtel_alert::EvidencePoint {
                        timestamp_ns: 1,
                        value: 0.05,
                    },
                    parqtel_alert::EvidencePoint {
                        timestamp_ns: 2,
                        value: 0.2,
                    },
                ],
                series_count: 1,
            }),
            rca: Some(parqtel_alert::RcaSummary::threshold(
                "High errors",
                0.2,
                ">",
                0.1,
                30,
                &[],
            )),
        }
    }

    #[tokio::test]
    async fn manual_noise_and_signal_change_state() {
        let state = AppState::default_for_tests_with_config(Config::default()).await;
        let alert = sample_alert(parqtel_alert::AlertState::Firing);
        let id = alert.id;
        state.inner.alert_store.save(&alert).await;

        let response = mark_alert_noise(State(state.clone()), Path(id.to_string()), None)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let saved = state.inner.alert_store.get_by_id(id).await.unwrap();
        assert_eq!(saved.state, parqtel_alert::AlertState::NoiseFlagged);
        assert_eq!(saved.noise_score, 1.0);

        let response = mark_alert_signal(State(state.clone()), Path(id.to_string()), None)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let saved = state.inner.alert_store.get_by_id(id).await.unwrap();
        assert_eq!(saved.state, parqtel_alert::AlertState::Firing);
        assert_eq!(saved.noise_score, 0.0);
    }

    #[tokio::test]
    async fn evidence_json_and_svg_are_served() {
        let state = AppState::default_for_tests_with_config(Config::default()).await;
        let alert = sample_alert(parqtel_alert::AlertState::Firing);
        let id = alert.id;
        state.inner.alert_store.save(&alert).await;

        let response = get_alert_evidence(State(state.clone()), Path(id.to_string()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["evidence"]["query"], "rate(errors[5m])");
        assert_eq!(json["data"]["rca"]["method"], "threshold");

        let response = get_alert_evidence_svg(State(state), Path(id.to_string()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "image/svg+xml"
        );
    }
}
