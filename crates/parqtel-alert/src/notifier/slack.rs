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
        let analysis = batch
            .alerts
            .iter()
            .find_map(|alert| alert.rca.as_ref())
            .map(|rca| {
                let mut text = rca.primary_cause.clone();
                if !rca.evidence.is_empty() {
                    text.push('\n');
                    text.push_str(&rca.evidence.join("\n"));
                }
                text
            })
            .unwrap_or_else(|| "No analysis captured".to_string());
        let evidence = batch
            .alerts
            .iter()
            .find_map(|alert| alert.evidence_url.clone())
            .unwrap_or_else(|| "No evidence snapshot".to_string());
        let query = batch
            .alerts
            .iter()
            .find_map(|alert| alert.evidence.as_ref().map(|e| e.query.clone()))
            .unwrap_or_else(|| "n/a".to_string());
        let graph = batch
            .alerts
            .iter()
            .find_map(|alert| alert.evidence.as_ref().map(|e| e.sparkline(30)))
            .filter(|graph| !graph.is_empty())
            .unwrap_or_else(|| "n/a".to_string());

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
            SlackField {
                title: "Query",
                value: query,
                short: false,
            },
            SlackField {
                title: "Graph",
                value: graph,
                short: false,
            },
            SlackField {
                title: "Analysis",
                value: analysis,
                short: false,
            },
            SlackField {
                title: "Evidence",
                value: evidence,
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

    fn target_url(&self) -> Option<&str> {
        Some(&self.webhook_url)
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
        assert_eq!(att["fields"][4]["title"], "Query");
        assert!(att["fields"][4]["value"]
            .as_str()
            .unwrap()
            .contains("rate(errors_total"));
        assert_eq!(att["fields"][5]["title"], "Graph");
        assert!(!att["fields"][5]["value"].as_str().unwrap().is_empty());
        assert_eq!(att["fields"][6]["title"], "Analysis");
        assert!(att["fields"][6]["value"]
            .as_str()
            .unwrap()
            .contains("crossed its threshold"));
        assert_eq!(att["fields"][7]["title"], "Evidence");
        assert!(att["fields"][7]["value"]
            .as_str()
            .unwrap()
            .ends_with("evidence.svg"));

        let resolved = sample_resolved_batch();
        notifier.send(&resolved).await.unwrap();
        let (_headers, body) = rx.recv().await.unwrap();
        let att = &body["attachments"][0];
        assert_eq!(att["color"], "#16a34a");
        assert_eq!(att["title"], "[RESOLVED] High Error Rate");
    }

    #[test]
    fn test_slack_payload_fallbacks() {
        let notifier = SlackNotifier::new(
            "s".into(),
            "http://127.0.0.1/hook".into(),
            None,
            None,
            None,
            false,
            reqwest::Client::new(),
        );
        let field = |p: &SlackPayload, title: &str| {
            p.attachments[0]
                .fields
                .iter()
                .find(|f| f.title == title)
                .map(|f| f.value.clone())
                .unwrap()
        };

        // No summary annotation -> falls back to the alert count description.
        let mut batch = sample_firing_batch();
        batch.alerts[0].annotations.clear();
        let payload = notifier.build_payload(&batch);
        assert_eq!(payload.channel, None);
        assert_eq!(
            payload.attachments[0].text,
            "1 alert(s) in group alertgroup-1"
        );
        assert_eq!(field(&payload, "Service"), "checkout");

        // `description` annotation is used when `summary` is absent.
        batch.alerts[0]
            .annotations
            .insert("description".into(), "desc text".into());
        assert_eq!(
            notifier.build_payload(&batch).attachments[0].text,
            "High Error Rate: desc text"
        );

        // group_labels fallbacks: service, then alert labels, then "unknown".
        batch.group_labels.remove("service.name");
        batch
            .group_labels
            .insert("service".into(), "checkout-svc".into());
        assert_eq!(
            field(&notifier.build_payload(&batch), "Service"),
            "checkout-svc"
        );
        batch.group_labels.clear();
        assert_eq!(
            field(&notifier.build_payload(&batch), "Service"),
            "checkout"
        );
        batch.alerts[0].labels.clear();
        assert_eq!(field(&notifier.build_payload(&batch), "Service"), "unknown");

        // Value formatting: trailing zeros trimmed, missing values render "n/a".
        batch.alerts[0].value = Some(1.5);
        assert_eq!(field(&notifier.build_payload(&batch), "Value"), "1.5");
        batch.alerts[0].value = Some(2.0);
        assert_eq!(field(&notifier.build_payload(&batch), "Value"), "2");
        batch.alerts[0].value = None;
        assert_eq!(field(&notifier.build_payload(&batch), "Value"), "n/a");
        batch.alerts.clear();
        assert_eq!(field(&notifier.build_payload(&batch), "Value"), "n/a");
        assert_eq!(field(&notifier.build_payload(&batch), "Labels"), "none");
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
