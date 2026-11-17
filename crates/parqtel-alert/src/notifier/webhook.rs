use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::BTreeMap;

use crate::error::NotificationError;

use super::{classify_http_response, classify_reqwest_error, NotificationBatch, Notifier};

/// Generic Alertmanager-style HTTP JSON webhook notifier.
#[derive(Debug, Clone)]
pub struct WebhookNotifier {
    name: String,
    url: String,
    send_resolved: bool,
    headers: HeaderMap,
    client: reqwest::Client,
}

impl WebhookNotifier {
    pub fn new(
        name: String,
        url: String,
        send_resolved: bool,
        headers: BTreeMap<String, String>,
        client: reqwest::Client,
    ) -> Result<Self, NotificationError> {
        let mut header_map = HeaderMap::with_capacity(headers.len());
        for (k, v) in headers {
            let hname = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                NotificationError::Config(format!("invalid webhook header name '{k}': {e}"))
            })?;
            let hval = HeaderValue::from_str(&v).map_err(|e| {
                NotificationError::Config(format!("invalid webhook header value for '{k}': {e}"))
            })?;
            header_map.insert(hname, hval);
        }

        Ok(Self {
            name,
            url,
            send_resolved,
            headers: header_map,
            client,
        })
    }
}

#[async_trait]
impl Notifier for WebhookNotifier {
    fn name(&self) -> &str {
        &self.name
    }

    fn channel_type(&self) -> &'static str {
        "webhook"
    }

    fn send_resolved(&self) -> bool {
        self.send_resolved
    }

    fn target_url(&self) -> Option<&str> {
        Some(&self.url)
    }

    async fn send(&self, batch: &NotificationBatch) -> Result<(), NotificationError> {
        let mut req = self.client.post(&self.url).json(batch);
        if !self.headers.is_empty() {
            req = req.headers(self.headers.clone());
        }
        let resp = req.send().await.map_err(classify_reqwest_error)?;
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
    async fn test_webhook_posts_alertmanager_json_with_custom_headers() {
        let (url, mut rx) = spawn_mock_server(StatusCode::OK, None, "ok").await;
        let headers = BTreeMap::from([
            ("X-Custom-Auth".to_string(), "secret-token".to_string()),
            ("X-Env".to_string(), "prod".to_string()),
        ]);
        let notifier =
            WebhookNotifier::new("wh-main".into(), url, true, headers, reqwest::Client::new())
                .unwrap();
        assert_eq!(notifier.name(), "wh-main");
        assert_eq!(notifier.channel_type(), "webhook");
        assert!(notifier.send_resolved());

        let firing = sample_firing_batch();
        notifier.send(&firing).await.unwrap();
        let (recv_headers, body) = rx.recv().await.unwrap();
        assert_eq!(recv_headers.get("x-custom-auth").unwrap(), "secret-token");
        assert_eq!(recv_headers.get("x-env").unwrap(), "prod");
        assert_eq!(body["receiver"], "ops-primary");
        assert_eq!(body["group_key"], "alertgroup-1");
        assert_eq!(body["status"], "firing");
        assert_eq!(body["group_labels"]["service.name"], "checkout");
        assert_eq!(body["alerts"][0]["rule_id"], "high-error-rate");
        assert_eq!(body["alerts"][0]["fingerprint"], 42);

        let resolved = sample_resolved_batch();
        notifier.send(&resolved).await.unwrap();
        let (_recv_headers, body) = rx.recv().await.unwrap();
        assert_eq!(body["status"], "resolved");
        assert_eq!(body["alerts"][0]["status"], "resolved");
    }

    #[test]
    fn test_webhook_rejects_invalid_headers() {
        let bad_name = BTreeMap::from([("bad header name".to_string(), "val".to_string())]);
        let bad_name_err = WebhookNotifier::new(
            "wh".into(),
            "http://127.0.0.1/hook".into(),
            false,
            bad_name,
            reqwest::Client::new(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            bad_name_err.contains("invalid webhook header name 'bad header name'"),
            "{bad_name_err}"
        );

        let bad_val = BTreeMap::from([("X-Good".to_string(), "bad\nvalue".to_string())]);
        let bad_val_err = WebhookNotifier::new(
            "wh".into(),
            "http://127.0.0.1/hook".into(),
            false,
            bad_val,
            reqwest::Client::new(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            bad_val_err.contains("invalid webhook header value for 'X-Good'"),
            "{bad_val_err}"
        );
    }

    #[tokio::test]
    async fn test_webhook_http_error_classification() {
        let (url_429, _rx) =
            spawn_mock_server(StatusCode::TOO_MANY_REQUESTS, Some("12"), "rate").await;
        let n429 = WebhookNotifier::new(
            "w".into(),
            url_429,
            false,
            BTreeMap::new(),
            reqwest::Client::new(),
        )
        .unwrap();
        let err = n429.send(&sample_firing_batch()).await.unwrap_err();
        assert_eq!(err.retry_after(), Some(Duration::from_secs(12)));

        let (url_503, _rx) = spawn_mock_server(StatusCode::SERVICE_UNAVAILABLE, None, "down").await;
        let n503 = WebhookNotifier::new(
            "w".into(),
            url_503,
            false,
            BTreeMap::new(),
            reqwest::Client::new(),
        )
        .unwrap();
        assert!(n503
            .send(&sample_firing_batch())
            .await
            .unwrap_err()
            .is_retryable());

        let (url_403, _rx) = spawn_mock_server(StatusCode::FORBIDDEN, None, "denied").await;
        let n403 = WebhookNotifier::new(
            "w".into(),
            url_403,
            false,
            BTreeMap::new(),
            reqwest::Client::new(),
        )
        .unwrap();
        assert!(matches!(
            n403.send(&sample_firing_batch()).await.unwrap_err(),
            NotificationError::Permanent { status: 403, .. }
        ));
    }
}
