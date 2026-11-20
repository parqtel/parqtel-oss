use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use parqtel_core::config::parse_duration;
use serde::Serialize;
use std::collections::BTreeMap;

use crate::error::NotificationError;

use super::{
    classify_http_response, classify_reqwest_error, severity_str, NotificationBatch, Notifier,
};

#[derive(Debug, Serialize)]
struct AlertmanagerAlert<'a> {
    labels: BTreeMap<String, String>,
    annotations: &'a BTreeMap<String, String>,
    #[serde(rename = "startsAt")]
    starts_at: DateTime<Utc>,
    #[serde(rename = "endsAt")]
    ends_at: DateTime<Utc>,
    #[serde(rename = "generatorURL")]
    generator_url: &'static str,
}

/// Pushes alerts to the Alertmanager v2 API.
#[derive(Debug, Clone)]
pub struct AlertmanagerNotifier {
    name: String,
    url: String,
    resend_ttl: ChronoDuration,
    client: reqwest::Client,
}

impl AlertmanagerNotifier {
    pub fn new(
        name: String,
        base_url: String,
        resend_ttl: &str,
        client: reqwest::Client,
    ) -> Result<Self, NotificationError> {
        let mut url = reqwest::Url::parse(&base_url)
            .map_err(|e| NotificationError::Config(format!("invalid Alertmanager URL: {e}")))?;
        if url.query().is_some() || url.fragment().is_some() {
            return Err(NotificationError::Config(
                "Alertmanager base URL cannot contain a query or fragment".into(),
            ));
        }
        let path = format!("{}/api/v2/alerts", url.path().trim_end_matches('/'));
        url.set_path(&path);

        let ttl = parse_duration(resend_ttl).map_err(NotificationError::Config)?;
        let resend_ttl = ChronoDuration::from_std(ttl).map_err(|e| {
            NotificationError::Config(format!("Alertmanager resend_ttl is out of range: {e}"))
        })?;

        Ok(Self {
            name,
            url: url.to_string(),
            resend_ttl,
            client,
        })
    }
}

#[async_trait]
impl Notifier for AlertmanagerNotifier {
    fn name(&self) -> &str {
        &self.name
    }

    fn channel_type(&self) -> &'static str {
        "alertmanager"
    }

    fn send_resolved(&self) -> bool {
        true
    }

    fn target_url(&self) -> Option<&str> {
        Some(&self.url)
    }

    async fn send(&self, batch: &NotificationBatch) -> Result<(), NotificationError> {
        let now = Utc::now();
        let mut alerts = Vec::with_capacity(batch.alerts.len());
        for alert in &batch.alerts {
            let mut labels = alert.labels.clone();
            labels.insert("alertname".into(), alert.rule_name.clone());
            labels.insert("rule_id".into(), alert.rule_id.clone());
            labels.insert("severity".into(), severity_str(alert.severity).into());

            let ends_at = if alert.status == "resolved" {
                alert.ends_at.unwrap_or(now)
            } else {
                now.checked_add_signed(self.resend_ttl).ok_or_else(|| {
                    NotificationError::Config("Alertmanager endsAt is out of range".into())
                })?
            };

            alerts.push(AlertmanagerAlert {
                labels,
                annotations: &alert.annotations,
                starts_at: alert.starts_at,
                ends_at,
                generator_url: "",
            });
        }

        let response = self
            .client
            .post(&self.url)
            .json(&alerts)
            .send()
            .await
            .map_err(classify_reqwest_error)?;
        classify_http_response(response).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::notifier::test_util::{sample_firing_batch, sample_resolved_batch};
    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::{extract::State, Router};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    async fn capture_post(
        State(tx): State<mpsc::UnboundedSender<Vec<u8>>>,
        body: Bytes,
    ) -> StatusCode {
        let _ = tx.send(body.to_vec());
        StatusCode::OK
    }

    async fn spawn_alertmanager() -> (String, mpsc::UnboundedReceiver<Vec<u8>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let app = Router::new()
            .route("/api/v2/alerts", post(capture_post))
            .with_state(tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(std::future::IntoFuture::into_future(axum::serve(
            listener, app,
        )));
        tokio::spawn(async move { server.await.is_ok() });
        (format!("http://{addr}"), rx)
    }

    fn notifier(url: String) -> AlertmanagerNotifier {
        AlertmanagerNotifier::new("am".into(), url, "60s", reqwest::Client::new()).unwrap()
    }

    fn labels_json(body: &[u8]) -> &[u8] {
        let labels_key = b"\"labels\":";
        let annotations_key = b",\"annotations\":";
        let start = body
            .windows(labels_key.len())
            .position(|window| window == labels_key)
            .unwrap()
            + labels_key.len();
        let end = body[start..]
            .windows(annotations_key.len())
            .position(|window| window == annotations_key)
            .unwrap()
            + start;
        &body[start..end]
    }

    #[tokio::test]
    async fn posts_firing_and_resolved_alertmanager_v2_payloads() {
        let (url, mut rx) = spawn_alertmanager().await;
        let notifier = notifier(url);
        let batch = sample_firing_batch();
        let before = Utc::now();
        notifier.send(&batch).await.unwrap();
        let firing: serde_json::Value = serde_json::from_slice(&rx.recv().await.unwrap()).unwrap();
        let after = Utc::now();

        assert_eq!(firing[0]["labels"]["alertname"], "High Error Rate");
        assert_eq!(firing[0]["labels"]["rule_id"], "high-error-rate");
        assert_eq!(firing[0]["labels"]["severity"], "critical");
        assert_eq!(firing[0]["labels"]["env"], "prod");
        assert_eq!(firing[0]["labels"]["service.name"], "checkout");
        assert_eq!(
            firing[0]["annotations"]["summary"],
            "Error rate exceeded 5%"
        );
        assert_eq!(firing[0]["startsAt"], "2026-11-17T10:00:00Z");
        assert_eq!(firing[0]["generatorURL"], "");
        let firing_ends_at = DateTime::parse_from_rfc3339(firing[0]["endsAt"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        assert!(firing_ends_at >= before + ChronoDuration::seconds(60));
        assert!(firing_ends_at <= after + ChronoDuration::seconds(60));

        notifier.send(&sample_resolved_batch()).await.unwrap();
        let resolved: serde_json::Value =
            serde_json::from_slice(&rx.recv().await.unwrap()).unwrap();
        assert_eq!(resolved[0]["endsAt"], "2026-11-17T10:05:00Z");
    }

    #[tokio::test]
    async fn notifier_instances_send_byte_identical_label_sets() {
        let (url, mut rx) = spawn_alertmanager().await;
        let first = notifier(url.clone());
        let second = notifier(url);
        let batch = sample_firing_batch();

        first.send(&batch).await.unwrap();
        second.send(&batch).await.unwrap();
        let first_body = rx.recv().await.unwrap();
        let second_body = rx.recv().await.unwrap();
        assert_eq!(labels_json(&first_body), labels_json(&second_body));
    }
}
