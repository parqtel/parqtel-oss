pub mod pagerduty;
pub mod slack;
pub mod teams;
pub mod webhook;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parqtel_core::config::{resolve_secret, AlertmanagerRoutingConfig};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use crate::error::NotificationError;
use crate::Severity;

use self::pagerduty::PagerDutyNotifier;
use self::slack::SlackNotifier;
use self::teams::TeamsNotifier;
use self::webhook::WebhookNotifier;

/// Individual alert entry inside a [`NotificationBatch`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertPayload {
    /// `"firing"` or `"resolved"`.
    pub status: String,
    pub rule_id: String,
    pub rule_name: String,
    pub severity: Severity,
    pub value: Option<f64>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub starts_at: DateTime<Utc>,
    pub ends_at: Option<DateTime<Utc>>,
    pub fingerprint: u64,
}

/// Grouped alert batch dispatched to a receiver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationBatch {
    pub receiver: String,
    pub group_key: String,
    /// `"firing"` if any alert is firing, else `"resolved"`.
    pub status: String,
    pub group_labels: BTreeMap<String, String>,
    pub alerts: Vec<AlertPayload>,
}

/// Outbound notification channel abstraction.
#[async_trait]
pub trait Notifier: Send + Sync {
    fn name(&self) -> &str;
    fn channel_type(&self) -> &'static str;
    fn send_resolved(&self) -> bool;
    async fn send(&self, batch: &NotificationBatch) -> Result<(), NotificationError>;
}

/// Classify an HTTP response into `Ok(())` or a retryable/permanent [`NotificationError`].
pub async fn classify_http_response(resp: reqwest::Response) -> Result<(), NotificationError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }

    let code = status.as_u16();
    let retry_after = if code == 429 {
        resp.headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
    } else {
        None
    };

    let body = resp.text().await.unwrap_or_default();
    let message = if body.is_empty() {
        status
            .canonical_reason()
            .unwrap_or("unexpected HTTP status")
            .to_string()
    } else {
        body
    };

    if code == 429 {
        Err(NotificationError::Transient {
            status: Some(429),
            retry_after,
            message,
        })
    } else if status.is_server_error() {
        Err(NotificationError::Transient {
            status: Some(code),
            retry_after: None,
            message,
        })
    } else if status.is_client_error() {
        Err(NotificationError::Permanent {
            status: code,
            message,
        })
    } else {
        Err(NotificationError::Transient {
            status: Some(code),
            retry_after: None,
            message,
        })
    }
}

/// Classify a transport/timeout [`reqwest::Error`] as a transient [`NotificationError`].
pub fn classify_reqwest_error(err: reqwest::Error) -> NotificationError {
    NotificationError::Transient {
        status: None,
        retry_after: None,
        message: err.to_string(),
    }
}

/// Build all configured notifiers keyed by receiver name, resolving `${ENV_VAR}` and `{env:ENV_VAR}` secrets.
pub fn build_notifiers(
    cfg: &AlertmanagerRoutingConfig,
    client: reqwest::Client,
) -> Result<HashMap<String, Vec<Arc<dyn Notifier>>>, NotificationError> {
    let mut map: HashMap<String, Vec<Arc<dyn Notifier>>> =
        HashMap::with_capacity(cfg.receivers.len());

    for receiver in &cfg.receivers {
        let mut notifiers: Vec<Arc<dyn Notifier>> = Vec::new();

        for slack_cfg in &receiver.slack_configs {
            let webhook_url =
                resolve_secret(&slack_cfg.webhook_url).map_err(NotificationError::Config)?;
            notifiers.push(Arc::new(SlackNotifier::new(
                receiver.name.clone(),
                webhook_url,
                slack_cfg.channel.clone(),
                slack_cfg.username.clone(),
                slack_cfg.icon_emoji.clone(),
                slack_cfg.send_resolved,
                client.clone(),
            )));
        }

        for pd_cfg in &receiver.pagerduty_configs {
            let routing_key =
                resolve_secret(&pd_cfg.routing_key).map_err(NotificationError::Config)?;
            let url = pd_cfg
                .url
                .as_deref()
                .map(resolve_secret)
                .transpose()
                .map_err(NotificationError::Config)?;
            notifiers.push(Arc::new(PagerDutyNotifier::new(
                receiver.name.clone(),
                routing_key,
                url,
                pd_cfg.severity.clone(),
                pd_cfg.send_resolved,
                client.clone(),
            )));
        }

        for teams_cfg in &receiver.teams_configs {
            let webhook_url =
                resolve_secret(&teams_cfg.webhook_url).map_err(NotificationError::Config)?;
            notifiers.push(Arc::new(TeamsNotifier::new(
                receiver.name.clone(),
                webhook_url,
                teams_cfg.title.clone(),
                teams_cfg.send_resolved,
                client.clone(),
            )));
        }

        for wh_cfg in &receiver.webhook_configs {
            let url = resolve_secret(&wh_cfg.url).map_err(NotificationError::Config)?;
            let mut headers = BTreeMap::new();
            for (k, v) in &wh_cfg.headers {
                let resolved_v = resolve_secret(v).map_err(NotificationError::Config)?;
                headers.insert(k.clone(), resolved_v);
            }
            notifiers.push(Arc::new(WebhookNotifier::new(
                receiver.name.clone(),
                url,
                wh_cfg.send_resolved,
                headers,
                client.clone(),
            )?));
        }

        map.insert(receiver.name.clone(), notifiers);
    }

    Ok(map)
}

pub(crate) fn format_batch_title(batch: &NotificationBatch) -> String {
    let summary = batch
        .alerts
        .first()
        .map(|a| a.rule_name.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(batch.group_key.as_str());

    if batch.status == "firing" {
        let firing_count = batch
            .alerts
            .iter()
            .filter(|a| a.status == "firing")
            .count()
            .max(1);
        format!("[FIRING:{firing_count}] {summary}")
    } else {
        format!("[RESOLVED] {summary}")
    }
}

pub(crate) fn format_labels(labels: &BTreeMap<String, String>) -> String {
    if labels.is_empty() {
        return "none".to_string();
    }
    labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn highest_severity(alerts: &[AlertPayload]) -> Severity {
    let mut best = Severity::Info;
    for alert in alerts {
        match (best, alert.severity) {
            (_, Severity::Critical) => return Severity::Critical,
            (Severity::Info, Severity::Warning) => best = Severity::Warning,
            _ => {}
        }
    }
    best
}

pub(crate) fn severity_str(sev: Severity) -> &'static str {
    match sev {
        Severity::Info => "info",
        Severity::Warning => "warning",
        Severity::Critical => "critical",
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    pub(crate) struct MockServerState {
        pub tx: mpsc::UnboundedSender<(HeaderMap, serde_json::Value)>,
        pub status: StatusCode,
        pub retry_after: Option<String>,
        pub body: String,
    }

    async fn handle_post(
        State(state): State<MockServerState>,
        headers: HeaderMap,
        Json(payload): Json<serde_json::Value>,
    ) -> (StatusCode, HeaderMap, String) {
        let _ = state.tx.send((headers, payload));
        let mut resp_headers = HeaderMap::new();
        if let Some(ra) = &state.retry_after {
            if let Ok(val) = ra.parse() {
                resp_headers.insert(axum::http::header::RETRY_AFTER, val);
            }
        }
        (state.status, resp_headers, state.body.clone())
    }

    pub(crate) async fn spawn_mock_server(
        status: StatusCode,
        retry_after: Option<&str>,
        body: &str,
    ) -> (
        String,
        mpsc::UnboundedReceiver<(HeaderMap, serde_json::Value)>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let state = MockServerState {
            tx,
            status,
            retry_after: retry_after.map(str::to_string),
            body: body.to_string(),
        };
        let app = Router::new()
            .route("/hook", post(handle_post))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Detached on purpose: tests never wait for the server to shut down.
        let served = tokio::spawn(std::future::IntoFuture::into_future(axum::serve(
            listener, app,
        )));
        // Observe the task handle's completion instead of discarding it with `let _`.
        tokio::spawn(async move { served.await.is_ok() });
        (format!("http://{addr}/hook"), rx)
    }

    pub(crate) fn sample_firing_batch() -> NotificationBatch {
        NotificationBatch {
            receiver: "ops-primary".to_string(),
            group_key: "alertgroup-1".to_string(),
            status: "firing".to_string(),
            group_labels: BTreeMap::from([
                ("env".to_string(), "prod".to_string()),
                ("service.name".to_string(), "checkout".to_string()),
            ]),
            alerts: vec![AlertPayload {
                status: "firing".to_string(),
                rule_id: "high-error-rate".to_string(),
                rule_name: "High Error Rate".to_string(),
                severity: Severity::Critical,
                value: Some(12.5),
                labels: BTreeMap::from([
                    ("env".to_string(), "prod".to_string()),
                    ("service.name".to_string(), "checkout".to_string()),
                ]),
                annotations: BTreeMap::from([(
                    "summary".to_string(),
                    "Error rate exceeded 5%".to_string(),
                )]),
                starts_at: DateTime::parse_from_rfc3339("2026-11-17T10:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                ends_at: None,
                fingerprint: 42,
            }],
        }
    }

    pub(crate) fn sample_resolved_batch() -> NotificationBatch {
        let mut batch = sample_firing_batch();
        batch.status = "resolved".to_string();
        batch.alerts[0].status = "resolved".to_string();
        batch.alerts[0].ends_at = Some(
            DateTime::parse_from_rfc3339("2026-11-17T10:05:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        batch
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::test_util::*;
    use super::*;
    use axum::http::StatusCode;
    use parqtel_core::config::{
        PagerDutyReceiverConfig, ReceiverConfig, SlackReceiverConfig, TeamsReceiverConfig,
        WebhookReceiverConfig,
    };

    #[tokio::test]
    async fn test_classify_http_response_2xx() {
        let (url, _rx) = spawn_mock_server(StatusCode::OK, None, "ok").await;
        let resp = reqwest::Client::new()
            .post(&url)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert!(classify_http_response(resp).await.is_ok());

        let (url_no_content, _rx) = spawn_mock_server(StatusCode::NO_CONTENT, None, "").await;
        let resp = reqwest::Client::new()
            .post(&url_no_content)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert!(classify_http_response(resp).await.is_ok());
    }

    #[tokio::test]
    async fn test_classify_http_response_429_with_and_without_retry_after() {
        let (url, _rx) =
            spawn_mock_server(StatusCode::TOO_MANY_REQUESTS, Some("15"), "slow down").await;
        let resp = reqwest::Client::new()
            .post(&url)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        let err = classify_http_response(resp).await.unwrap_err();
        assert!(err.is_retryable());
        assert_eq!(err.retry_after(), Some(Duration::from_secs(15)));
        assert!(matches!(
            err,
            NotificationError::Transient {
                status: Some(429),
                ..
            }
        ));

        let (url_no_ra, _rx) =
            spawn_mock_server(StatusCode::TOO_MANY_REQUESTS, Some("not-a-number"), "").await;
        let resp = reqwest::Client::new()
            .post(&url_no_ra)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        let err = classify_http_response(resp).await.unwrap_err();
        assert!(err.is_retryable());
        assert_eq!(err.retry_after(), None);
    }

    #[tokio::test]
    async fn test_classify_http_response_5xx_transient() {
        for status in [
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let (url, _rx) = spawn_mock_server(status, None, "upstream down").await;
            let resp = reqwest::Client::new()
                .post(&url)
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap();
            let err = classify_http_response(resp).await.unwrap_err();
            assert!(err.is_retryable());
            assert_eq!(err.retry_after(), None);
            assert!(matches!(
                err,
                NotificationError::Transient {
                    status: Some(code),
                    ..
                } if code == status.as_u16()
            ));
        }
    }

    #[tokio::test]
    async fn test_classify_http_response_4xx_permanent() {
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
        ] {
            let (url, _rx) = spawn_mock_server(status, None, "bad request").await;
            let resp = reqwest::Client::new()
                .post(&url)
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap();
            let err = classify_http_response(resp).await.unwrap_err();
            assert!(!err.is_retryable());
            assert!(matches!(
                err,
                NotificationError::Permanent { status: code, .. } if code == status.as_u16()
            ));
        }
    }

    #[tokio::test]
    async fn test_classify_reqwest_error_network_is_transient() {
        // Bind then drop listener to guarantee connection refused
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let err = reqwest::Client::new()
            .post(format!("http://{addr}/hook"))
            .send()
            .await
            .unwrap_err();
        let classified = classify_reqwest_error(err);
        assert!(classified.is_retryable());
        assert!(matches!(
            classified,
            NotificationError::Transient {
                status: None,
                retry_after: None,
                ..
            }
        ));
    }

    #[test]
    fn test_build_notifiers_all_channels_and_secret_resolution() {
        let mut cfg = AlertmanagerRoutingConfig::default();
        cfg.receivers.push(ReceiverConfig {
            name: "multi".to_string(),
            slack_configs: vec![SlackReceiverConfig {
                webhook_url: "https://hooks.slack.com/services/${PATH}".to_string(),
                channel: Some("#alerts".to_string()),
                username: Some("parqtel".to_string()),
                icon_emoji: Some(":fire:".to_string()),
                send_resolved: true,
            }],
            pagerduty_configs: vec![PagerDutyReceiverConfig {
                routing_key: "{env:PATH}".to_string(),
                url: None,
                severity: None,
                send_resolved: false,
            }],
            teams_configs: vec![TeamsReceiverConfig {
                webhook_url: "https://outlook.office.com/webhook/abc".to_string(),
                title: Some("Ops".to_string()),
                send_resolved: true,
            }],
            webhook_configs: vec![WebhookReceiverConfig {
                url: "https://example.com/hook".to_string(),
                send_resolved: true,
                headers: BTreeMap::from([("X-Token".to_string(), "${PATH}".to_string())]),
            }],
        });

        let built = build_notifiers(&cfg, reqwest::Client::new()).unwrap();
        let list = built.get("multi").unwrap();
        assert_eq!(list.len(), 4);
        assert_eq!(list[0].channel_type(), "slack");
        assert_eq!(list[0].name(), "multi");
        assert!(list[0].send_resolved());
        assert_eq!(list[1].channel_type(), "pagerduty");
        assert!(!list[1].send_resolved());
        assert_eq!(list[2].channel_type(), "teams");
        assert_eq!(list[3].channel_type(), "webhook");
    }

    #[tokio::test]
    async fn test_classify_http_status_outside_error_ranges_is_transient() {
        // 3xx is neither success nor client/server error -> transient with the code.
        let (url, _rx) = spawn_mock_server(StatusCode::MULTIPLE_CHOICES, None, "").await;
        let resp = reqwest::Client::new()
            .post(&url)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        let err = classify_http_response(resp).await.unwrap_err();
        assert!(matches!(
            err,
            NotificationError::Transient {
                status: Some(300),
                ..
            }
        ));
    }

    #[test]
    fn test_format_batch_title_and_labels_edge_cases() {
        let mut batch = sample_firing_batch();
        batch.group_labels.clear();
        assert_eq!(format_labels(&batch.group_labels), "none");
        batch.group_labels.insert("b".into(), "2".into());
        batch.group_labels.insert("a".into(), "1".into());
        assert_eq!(format_labels(&batch.group_labels), "a=1, b=2");

        // Empty rule name falls back to the group key.
        batch.alerts[0].rule_name.clear();
        assert_eq!(format_batch_title(&batch), "[FIRING:1] alertgroup-1");
        // No alerts falls back to the group key; firing count floors at 1.
        batch.alerts.clear();
        assert_eq!(format_batch_title(&batch), "[FIRING:1] alertgroup-1");
        batch.status = "resolved".into();
        assert_eq!(format_batch_title(&batch), "[RESOLVED] alertgroup-1");
    }

    #[test]
    fn test_highest_severity_and_severity_str() {
        let mut warning = sample_firing_batch();
        warning.alerts[0].severity = Severity::Warning;
        assert_eq!(highest_severity(&[]), Severity::Info);
        assert_eq!(highest_severity(&warning.alerts), Severity::Warning);

        let mut info = warning.clone();
        info.alerts[0].severity = Severity::Info;
        assert_eq!(highest_severity(&info.alerts), Severity::Info);

        assert_eq!(severity_str(Severity::Info), "info");
        assert_eq!(severity_str(Severity::Warning), "warning");
        assert_eq!(severity_str(Severity::Critical), "critical");
    }

    #[test]
    fn test_build_notifiers_config_errors_for_each_channel() {
        let build = |receiver: ReceiverConfig| {
            let mut cfg = AlertmanagerRoutingConfig::default();
            cfg.receivers.push(receiver);
            build_notifiers(&cfg, reqwest::Client::new()).err().unwrap()
        };
        let missing = "${PARQTEL_NONEXISTENT_SECRET_99999}";

        // pagerduty routing key
        assert!(matches!(
            build(ReceiverConfig {
                name: "pd".into(),
                pagerduty_configs: vec![PagerDutyReceiverConfig {
                    routing_key: missing.into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            NotificationError::Config(_)
        ));

        // pagerduty url
        assert!(matches!(
            build(ReceiverConfig {
                name: "pd".into(),
                pagerduty_configs: vec![PagerDutyReceiverConfig {
                    routing_key: "rk".into(),
                    url: Some(missing.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            NotificationError::Config(_)
        ));

        // teams webhook url
        assert!(matches!(
            build(ReceiverConfig {
                name: "teams".into(),
                teams_configs: vec![TeamsReceiverConfig {
                    webhook_url: missing.into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            NotificationError::Config(_)
        ));

        // webhook url
        assert!(matches!(
            build(ReceiverConfig {
                name: "wh".into(),
                webhook_configs: vec![WebhookReceiverConfig {
                    url: missing.into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            NotificationError::Config(_)
        ));

        // webhook header value
        assert!(matches!(
            build(ReceiverConfig {
                name: "wh".into(),
                webhook_configs: vec![WebhookReceiverConfig {
                    url: "http://127.0.0.1/hook".into(),
                    headers: BTreeMap::from([("X-Token".to_string(), missing.to_string())]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            NotificationError::Config(_)
        ));

        // webhook with a bad header name surfaces the notifier's Config error
        assert!(matches!(
            build(ReceiverConfig {
                name: "wh".into(),
                webhook_configs: vec![WebhookReceiverConfig {
                    url: "http://127.0.0.1/hook".into(),
                    headers: BTreeMap::from([("bad header".to_string(), "v".to_string())]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            NotificationError::Config(_)
        ));
    }

    #[test]
    fn test_build_notifiers_defaults_for_optional_fields() {
        let mut cfg = AlertmanagerRoutingConfig::default();
        cfg.receivers.push(ReceiverConfig {
            name: "bare".into(),
            pagerduty_configs: vec![PagerDutyReceiverConfig {
                routing_key: "rk".into(),
                url: None,
                severity: None,
                send_resolved: false,
            }],
            teams_configs: vec![TeamsReceiverConfig {
                webhook_url: "http://127.0.0.1/teams".into(),
                title: None,
                send_resolved: false,
            }],
            ..Default::default()
        });

        let built = build_notifiers(&cfg, reqwest::Client::new()).unwrap();
        let list = built.get("bare").unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].channel_type(), "pagerduty");
        assert!(!list[0].send_resolved());
        assert_eq!(list[1].channel_type(), "teams");
        assert!(!list[1].send_resolved());
    }

    #[test]
    fn test_build_notifiers_missing_secret_returns_config_error() {
        let mut cfg = AlertmanagerRoutingConfig::default();
        cfg.receivers.push(ReceiverConfig {
            name: "broken".to_string(),
            slack_configs: vec![SlackReceiverConfig {
                webhook_url: "${PARQTEL_NONEXISTENT_SECRET_99999}".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        });

        let err = build_notifiers(&cfg, reqwest::Client::new()).err().unwrap();
        assert!(matches!(err, NotificationError::Config(_)));
    }
}
