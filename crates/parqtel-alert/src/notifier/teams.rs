use async_trait::async_trait;
use serde::Serialize;

use crate::error::NotificationError;

use super::{
    classify_http_response, classify_reqwest_error, format_batch_title, format_labels,
    highest_severity, severity_str, NotificationBatch, Notifier,
};

/// Microsoft Teams Adaptive Card webhook notifier.
#[derive(Debug, Clone)]
pub struct TeamsNotifier {
    name: String,
    webhook_url: String,
    title: Option<String>,
    send_resolved: bool,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct TeamsMessage {
    #[serde(rename = "type")]
    msg_type: &'static str,
    attachments: Vec<TeamsAttachment>,
}

#[derive(Serialize)]
struct TeamsAttachment {
    #[serde(rename = "contentType")]
    content_type: &'static str,
    content: AdaptiveCard,
}

#[derive(Serialize)]
struct AdaptiveCard {
    #[serde(rename = "$schema")]
    schema: &'static str,
    #[serde(rename = "type")]
    card_type: &'static str,
    version: &'static str,
    body: Vec<CardElement>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum CardElement {
    TextBlock {
        text: String,
        weight: &'static str,
        size: &'static str,
        color: &'static str,
    },
    FactSet {
        facts: Vec<CardFact>,
    },
}

#[derive(Serialize)]
struct CardFact {
    title: &'static str,
    value: String,
}

impl TeamsNotifier {
    pub fn new(
        name: String,
        webhook_url: String,
        title: Option<String>,
        send_resolved: bool,
        client: reqwest::Client,
    ) -> Self {
        Self {
            name,
            webhook_url,
            title,
            send_resolved,
            client,
        }
    }

    fn build_message(&self, batch: &NotificationBatch) -> TeamsMessage {
        let is_firing = batch.status == "firing";
        let color = if is_firing { "Attention" } else { "Good" };
        let heading = match &self.title {
            Some(prefix) if !prefix.is_empty() => {
                format!("{prefix}: {}", format_batch_title(batch))
            }
            _ => format_batch_title(batch),
        };

        let alert_summary = batch
            .alerts
            .iter()
            .map(|a| {
                let val = a.value.map(|v| format!(" (value={v})")).unwrap_or_default();
                format!("{} [{}]{val}", a.rule_name, a.status)
            })
            .collect::<Vec<_>>()
            .join("; ");

        let facts = vec![
            CardFact {
                title: "Severity",
                value: severity_str(highest_severity(&batch.alerts)).to_string(),
            },
            CardFact {
                title: "Status",
                value: batch.status.clone(),
            },
            CardFact {
                title: "Labels",
                value: format_labels(&batch.group_labels),
            },
            CardFact {
                title: "Alerts",
                value: if alert_summary.is_empty() {
                    "none".to_string()
                } else {
                    alert_summary
                },
            },
        ];

        TeamsMessage {
            msg_type: "message",
            attachments: vec![TeamsAttachment {
                content_type: "application/vnd.microsoft.card.adaptive",
                content: AdaptiveCard {
                    schema: "http://adaptivecards.io/schemas/adaptive-card.json",
                    card_type: "AdaptiveCard",
                    version: "1.4",
                    body: vec![
                        CardElement::TextBlock {
                            text: heading,
                            weight: "Bolder",
                            size: "Medium",
                            color,
                        },
                        CardElement::FactSet { facts },
                    ],
                },
            }],
        }
    }
}

#[async_trait]
impl Notifier for TeamsNotifier {
    fn name(&self) -> &str {
        &self.name
    }

    fn channel_type(&self) -> &'static str {
        "teams"
    }

    fn send_resolved(&self) -> bool {
        self.send_resolved
    }

    fn target_url(&self) -> Option<&str> {
        Some(&self.webhook_url)
    }

    async fn send(&self, batch: &NotificationBatch) -> Result<(), NotificationError> {
        let msg = self.build_message(batch);
        let resp = self
            .client
            .post(&self.webhook_url)
            .json(&msg)
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
    async fn test_teams_adaptive_card_firing_and_resolved() {
        let (url, mut rx) = spawn_mock_server(StatusCode::OK, None, "1").await;
        let notifier = TeamsNotifier::new(
            "teams-sre".into(),
            url,
            Some("Parqtel".into()),
            true,
            reqwest::Client::new(),
        );
        assert_eq!(notifier.name(), "teams-sre");
        assert_eq!(notifier.channel_type(), "teams");
        assert!(notifier.send_resolved());

        notifier.send(&sample_firing_batch()).await.unwrap();
        let (_headers, body) = rx.recv().await.unwrap();
        assert_eq!(body["type"], "message");
        let att = &body["attachments"][0];
        assert_eq!(
            att["contentType"],
            "application/vnd.microsoft.card.adaptive"
        );
        let card = &att["content"];
        assert_eq!(
            card["$schema"],
            "http://adaptivecards.io/schemas/adaptive-card.json"
        );
        assert_eq!(card["type"], "AdaptiveCard");
        assert_eq!(card["version"], "1.4");
        assert_eq!(card["body"][0]["type"], "TextBlock");
        assert_eq!(
            card["body"][0]["text"],
            "Parqtel: [FIRING:1] High Error Rate"
        );
        assert_eq!(card["body"][0]["color"], "Attention");
        assert_eq!(card["body"][1]["type"], "FactSet");
        assert_eq!(card["body"][1]["facts"][0]["title"], "Severity");
        assert_eq!(card["body"][1]["facts"][0]["value"], "critical");
        assert_eq!(card["body"][1]["facts"][1]["title"], "Status");
        assert_eq!(card["body"][1]["facts"][1]["value"], "firing");

        notifier.send(&sample_resolved_batch()).await.unwrap();
        let (_headers, body) = rx.recv().await.unwrap();
        let card = &body["attachments"][0]["content"];
        assert_eq!(card["body"][0]["color"], "Good");
        assert_eq!(
            card["body"][0]["text"],
            "Parqtel: [RESOLVED] High Error Rate"
        );
        assert_eq!(card["body"][1]["facts"][1]["value"], "resolved");
    }

    #[test]
    fn test_teams_empty_alert_list_renders_none() {
        let notifier = TeamsNotifier::new(
            "t".into(),
            "http://127.0.0.1/hook".into(),
            Some(String::new()),
            false,
            reqwest::Client::new(),
        );
        let mut batch = sample_firing_batch();
        batch.alerts.clear();
        batch.group_labels.clear();

        let json = serde_json::to_value(notifier.build_message(&batch)).unwrap();
        // Empty (not absent) title override falls back to the plain batch title.
        assert_eq!(
            json["attachments"][0]["content"]["body"][0]["text"],
            "[FIRING:1] alertgroup-1"
        );
        let body = &json["attachments"][0]["content"]["body"][1]["facts"];
        assert_eq!(body[2]["title"], "Labels");
        assert_eq!(body[2]["value"], "none");
        assert_eq!(body[3]["title"], "Alerts");
        assert_eq!(body[3]["value"], "none");
    }

    #[test]
    fn test_teams_alert_summary_without_value() {
        let notifier = TeamsNotifier::new(
            "t".into(),
            "http://127.0.0.1/hook".into(),
            None,
            false,
            reqwest::Client::new(),
        );
        let mut batch = sample_firing_batch();
        batch.alerts[0].value = None;

        let json = serde_json::to_value(notifier.build_message(&batch)).unwrap();
        let facts = &json["attachments"][0]["content"]["body"][1]["facts"];
        assert_eq!(facts[3]["value"], "High Error Rate [firing]");
    }

    #[tokio::test]
    async fn test_teams_http_error_classification() {
        let (url_429, _rx) =
            spawn_mock_server(StatusCode::TOO_MANY_REQUESTS, Some("5"), "throttle").await;
        let n429 = TeamsNotifier::new("t".into(), url_429, None, false, reqwest::Client::new());
        let err = n429.send(&sample_firing_batch()).await.unwrap_err();
        assert_eq!(err.retry_after(), Some(Duration::from_secs(5)));

        let (url_500, _rx) =
            spawn_mock_server(StatusCode::INTERNAL_SERVER_ERROR, None, "err").await;
        let n500 = TeamsNotifier::new("t".into(), url_500, None, false, reqwest::Client::new());
        assert!(n500
            .send(&sample_firing_batch())
            .await
            .unwrap_err()
            .is_retryable());

        let (url_404, _rx) = spawn_mock_server(StatusCode::NOT_FOUND, None, "missing").await;
        let n404 = TeamsNotifier::new("t".into(), url_404, None, false, reqwest::Client::new());
        assert!(matches!(
            n404.send(&sample_firing_batch()).await.unwrap_err(),
            NotificationError::Permanent { status: 404, .. }
        ));
    }
}
