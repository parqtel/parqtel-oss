use async_trait::async_trait;
use serde::Serialize;

use crate::error::NotificationError;

use super::{
    classify_http_response, classify_reqwest_error, format_batch_title, format_labels,
    highest_severity, severity_str, NotificationBatch, Notifier,
};

/// Slack incoming-webhook notifier.
#[derive(Debug, Clone)]
pub struct SlackNotifier {
    name: String,
    webhook_url: String,
    channel: Option<String>,
    username: Option<String>,
    icon_emoji: Option<String>,
    send_resolved: bool,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct SlackPayload<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    icon_emoji: Option<&'a str>,
    attachments: Vec<SlackAttachment>,
}

#[derive(Serialize)]
struct SlackAttachment {
    color: &'static str,
    title: String,
    text: String,
    fields: Vec<SlackField>,
}

#[derive(Serialize)]
struct SlackField {
    title: &'static str,
    value: String,
    short: bool,
}

impl SlackNotifier {
    pub fn new(
        name: String,
        webhook_url: String,
        channel: Option<String>,
        username: Option<String>,
        icon_emoji: Option<String>,
        send_resolved: bool,
        client: reqwest::Client,
    ) -> Self {
        Self {
            name,
            webhook_url,
            channel,
            username,
            icon_emoji,
            send_resolved,
            client,
        }
    }

    fn build_payload<'a>(&'a self, batch: &NotificationBatch) -> SlackPayload<'a> {
        let is_firing = batch.status == "firing";
        let color = if is_firing { "#dc2626" } else { "#16a34a" };
        let title = format_batch_title(batch);

        let text = batch
            .alerts
            .iter()
            .filter_map(|a| {
                a.annotations
                    .get("summary")
                    .or_else(|| a.annotations.get("description"))
                    .map(|s| format!("{}: {s}", a.rule_name))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let text = if text.is_empty() {
            format!(
                "{} alert(s) in group {}",
                batch.alerts.len(),
                batch.group_key
            )
        } else {
            text
        };

        let severity = severity_str(highest_severity(&batch.alerts)).to_string();
        let service = batch
            .group_labels
            .get("service.name")
            .or_else(|| batch.group_labels.get("service"))
            .or_else(|| {
                batch.alerts.first().and_then(|a| {
                    a.labels
                        .get("service.name")
                        .or_else(|| a.labels.get("service"))
                })
            })
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let value = batch
            .alerts
            .first()
            .and_then(|a| a.value)
            .map(|v| {
                format!("{v:.4}")
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_string()
            })
            .unwrap_or_else(|| "n/a".to_string());
        let labels = format_labels(&batch.group_labels);

        let fields = vec![
            SlackField {
                title: "Severity",
                value: severity,
                short: true,
            },
            SlackField {
                title: "Service",
                value: service,
                short: true,
            },
            SlackField {
                title: "Value",
                value,
                short: true,
            },
            SlackField {
                title: "Labels",
                value: labels,
                short: false,
            },
        ];

        SlackPayload {
            channel: self.channel.as_deref(),
            username: self.username.as_deref(),
            icon_emoji: self.icon_emoji.as_deref(),
            attachments: vec![SlackAttachment {
                color,
                title,
                text,
                fields,
            }],
        }
    }
}

#[async_trait]
impl Notifier for SlackNotifier {
    fn name(&self) -> &str {
        &self.name
    }

    fn channel_type(&self) -> &'static str {
        "slack"
    }

    fn send_resolved(&self) -> bool {
        self.send_resolved
    }

    async fn send(&self, batch: &NotificationBatch) -> Result<(), NotificationError> {
        let payload = self.build_payload(batch);
        let resp = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
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
    async fn test_slack_send_firing_and_resolved_payloads() {
        let (url, mut rx) = spawn_mock_server(StatusCode::OK, None, "ok").await;
        let notifier = SlackNotifier::new(
            "slack-ops".into(),
            url,
            Some("#alerts".into()),
            Some("parqtel".into()),
            Some(":rotating_light:".into()),
            true,
            reqwest::Client::new(),
        );
        assert_eq!(notifier.name(), "slack-ops");
        assert_eq!(notifier.channel_type(), "slack");
        assert!(notifier.send_resolved());

        let firing = sample_firing_batch();
        notifier.send(&firing).await.unwrap();
        let (_headers, body) = rx.recv().await.unwrap();
        assert_eq!(body["channel"], "#alerts");
        assert_eq!(body["username"], "parqtel");
        assert_eq!(body["icon_emoji"], ":rotating_light:");
        let att = &body["attachments"][0];
        assert_eq!(att["color"], "#dc2626");
        assert_eq!(att["title"], "[FIRING:1] High Error Rate");
        assert!(att["text"]
            .as_str()
            .unwrap()
            .contains("Error rate exceeded 5%"));
        assert_eq!(att["fields"][0]["title"], "Severity");
        assert_eq!(att["fields"][0]["value"], "critical");
        assert_eq!(att["fields"][1]["title"], "Service");
        assert_eq!(att["fields"][1]["value"], "checkout");
        assert_eq!(att["fields"][2]["title"], "Value");
        assert_eq!(att["fields"][2]["value"], "12.5");
        assert_eq!(att["fields"][3]["title"], "Labels");

        let resolved = sample_resolved_batch();
        notifier.send(&resolved).await.unwrap();
        let (_headers, body) = rx.recv().await.unwrap();
        let att = &body["attachments"][0];
        assert_eq!(att["color"], "#16a34a");
        assert_eq!(att["title"], "[RESOLVED] High Error Rate");
    }

    #[tokio::test]
    async fn test_slack_http_error_classification() {
        let (url_429, _rx) =
            spawn_mock_server(StatusCode::TOO_MANY_REQUESTS, Some("7"), "rate").await;
        let n429 = SlackNotifier::new(
            "s".into(),
            url_429,
            None,
            None,
            None,
            false,
            reqwest::Client::new(),
        );
        let err = n429.send(&sample_firing_batch()).await.unwrap_err();
        assert!(err.is_retryable());
        assert_eq!(err.retry_after(), Some(Duration::from_secs(7)));

        let (url_503, _rx) = spawn_mock_server(StatusCode::SERVICE_UNAVAILABLE, None, "busy").await;
        let n503 = SlackNotifier::new(
            "s".into(),
            url_503,
            None,
            None,
            None,
            false,
            reqwest::Client::new(),
        );
        let err = n503.send(&sample_firing_batch()).await.unwrap_err();
        assert!(matches!(
            err,
            NotificationError::Transient {
                status: Some(503),
                ..
            }
        ));

        let (url_400, _rx) =
            spawn_mock_server(StatusCode::BAD_REQUEST, None, "invalid_payload").await;
        let n400 = SlackNotifier::new(
            "s".into(),
            url_400,
            None,
            None,
            None,
            false,
            reqwest::Client::new(),
        );
        let err = n400.send(&sample_firing_batch()).await.unwrap_err();
        assert!(matches!(
            err,
            NotificationError::Permanent { status: 400, .. }
        ));
    }
}
