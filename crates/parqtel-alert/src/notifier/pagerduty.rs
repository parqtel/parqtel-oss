use async_trait::async_trait;
use chrono::Utc;
use serde::Serialize;
use std::collections::BTreeMap;

use crate::error::NotificationError;

use super::{
    classify_http_response, classify_reqwest_error, format_batch_title, highest_severity,
    severity_str, AlertPayload, NotificationBatch, Notifier,
};

pub const DEFAULT_PAGERDUTY_URL: &str = "https://events.pagerduty.com/v2/enqueue";

/// PagerDuty Events API v2 notifier.
#[derive(Debug, Clone)]
pub struct PagerDutyNotifier {
    name: String,
    routing_key: String,
    url: String,
    severity_override: Option<String>,
    send_resolved: bool,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct PagerDutyEvent<'a> {
    routing_key: &'a str,
    event_action: &'static str,
    dedup_key: &'a str,
    payload: PagerDutyPayload<'a>,
}

#[derive(Serialize)]
struct PagerDutyPayload<'a> {
    summary: String,
    source: String,
    severity: String,
    timestamp: String,
    custom_details: PagerDutyCustomDetails<'a>,
}

#[derive(Serialize)]
struct PagerDutyCustomDetails<'a> {
    group_labels: &'a BTreeMap<String, String>,
    alerts: &'a [AlertPayload],
}

impl PagerDutyNotifier {
    pub fn new(
        name: String,
        routing_key: String,
        url: Option<String>,
        severity_override: Option<String>,
        send_resolved: bool,
        client: reqwest::Client,
    ) -> Self {
        Self {
            name,
            routing_key,
            url: url
                .filter(|u| !u.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_PAGERDUTY_URL.to_string()),
            severity_override,
            send_resolved,
            client,
        }
    }

    fn build_event<'a>(&'a self, batch: &'a NotificationBatch) -> PagerDutyEvent<'a> {
        let event_action = if batch.status == "firing" {
            "trigger"
        } else {
            "resolve"
        };

        let source = batch
            .group_labels
            .get("service.name")
            .or_else(|| batch.group_labels.get("service"))
            .or_else(|| batch.group_labels.get("host"))
            .or_else(|| {
                batch.alerts.first().and_then(|a| {
                    a.labels
                        .get("service.name")
                        .or_else(|| a.labels.get("service"))
                        .or_else(|| a.labels.get("host"))
                })
            })
            .cloned()
            .unwrap_or_else(|| "parqtel".to_string());

        let severity = self
            .severity_override
            .clone()
            .unwrap_or_else(|| severity_str(highest_severity(&batch.alerts)).to_string());

        let timestamp = batch
            .alerts
            .iter()
            .map(|a| a.starts_at)
            .min()
            .unwrap_or_else(Utc::now)
            .to_rfc3339();

        PagerDutyEvent {
            routing_key: &self.routing_key,
            event_action,
            dedup_key: &batch.group_key,
            payload: PagerDutyPayload {
                summary: format_batch_title(batch),
                source,
                severity,
                timestamp,
                custom_details: PagerDutyCustomDetails {
                    group_labels: &batch.group_labels,
                    alerts: &batch.alerts,
                },
            },
        }
    }
}

#[async_trait]
impl Notifier for PagerDutyNotifier {
    fn name(&self) -> &str {
        &self.name
    }

    fn channel_type(&self) -> &'static str {
        "pagerduty"
    }

    fn send_resolved(&self) -> bool {
        self.send_resolved
    }

    async fn send(&self, batch: &NotificationBatch) -> Result<(), NotificationError> {
        let event = self.build_event(batch);
        let resp = self
            .client
            .post(&self.url)
            .json(&event)
            .send()
            .await
            .map_err(classify_reqwest_error)?;
        classify_http_response(resp).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::notifier::test_util::{
        sample_firing_batch, sample_resolved_batch, spawn_mock_server,
    };
    use axum::http::StatusCode;
    use std::time::Duration;

    #[tokio::test]
    async fn test_pagerduty_trigger_and_resolve_payloads() {
        let (url, mut rx) =
            spawn_mock_server(StatusCode::ACCEPTED, None, "{\"status\":\"success\"}").await;
        let notifier = PagerDutyNotifier::new(
            "pd-oncall".into(),
            "rk-secret-123".into(),
            Some(url),
            None,
            true,
            reqwest::Client::new(),
        );
        assert_eq!(notifier.name(), "pd-oncall");
        assert_eq!(notifier.channel_type(), "pagerduty");
        assert!(notifier.send_resolved());

        let firing = sample_firing_batch();
        notifier.send(&firing).await.unwrap();
        let (_headers, body) = rx.recv().await.unwrap();
        assert_eq!(body["routing_key"], "rk-secret-123");
        assert_eq!(body["event_action"], "trigger");
        assert_eq!(body["dedup_key"], "alertgroup-1");
        assert_eq!(body["payload"]["summary"], "[FIRING:1] High Error Rate");
        assert_eq!(body["payload"]["source"], "checkout");
        assert_eq!(body["payload"]["severity"], "critical");
        assert_eq!(body["payload"]["timestamp"], "2026-11-17T10:00:00+00:00");
        assert_eq!(
            body["payload"]["custom_details"]["group_labels"]["env"],
            "prod"
        );
        assert_eq!(
            body["payload"]["custom_details"]["alerts"][0]["rule_id"],
            "high-error-rate"
        );

        let resolved = sample_resolved_batch();
        notifier.send(&resolved).await.unwrap();
        let (_headers, body) = rx.recv().await.unwrap();
        assert_eq!(body["event_action"], "resolve");
        assert_eq!(body["dedup_key"], "alertgroup-1");
    }

    #[test]
    fn test_pagerduty_default_url_and_severity_override() {
        let notifier = PagerDutyNotifier::new(
            "pd".into(),
            "rk".into(),
            None,
            Some("error".into()),
            false,
            reqwest::Client::new(),
        );
        assert_eq!(notifier.url, DEFAULT_PAGERDUTY_URL);
        let batch = sample_firing_batch();
        let event = notifier.build_event(&batch);
        assert_eq!(event.payload.severity, "error");
    }

    #[tokio::test]
    async fn test_pagerduty_http_error_classification() {
        let (url_429, _rx) =
            spawn_mock_server(StatusCode::TOO_MANY_REQUESTS, Some("30"), "limit").await;
        let n429 = PagerDutyNotifier::new(
            "p".into(),
            "k".into(),
            Some(url_429),
            None,
            false,
            reqwest::Client::new(),
        );
        let err = n429.send(&sample_firing_batch()).await.unwrap_err();
        assert_eq!(err.retry_after(), Some(Duration::from_secs(30)));

        let (url_500, _rx) =
            spawn_mock_server(StatusCode::INTERNAL_SERVER_ERROR, None, "err").await;
        let n500 = PagerDutyNotifier::new(
            "p".into(),
            "k".into(),
            Some(url_500),
            None,
            false,
            reqwest::Client::new(),
        );
        assert!(n500
            .send(&sample_firing_batch())
            .await
            .unwrap_err()
            .is_retryable());

        let (url_401, _rx) = spawn_mock_server(StatusCode::UNAUTHORIZED, None, "bad key").await;
        let n401 = PagerDutyNotifier::new(
            "p".into(),
            "k".into(),
            Some(url_401),
            None,
            false,
            reqwest::Client::new(),
        );
        assert!(matches!(
            n401.send(&sample_firing_batch()).await.unwrap_err(),
            NotificationError::Permanent { status: 401, .. }
        ));
    }
}
