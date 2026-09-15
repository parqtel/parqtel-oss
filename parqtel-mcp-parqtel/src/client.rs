//! Live HTTP client for the Parqtel server API.
//!
//! Every tool handler shares one [`ParqtelClient`]; it wraps Parqtel's
//! `{status, data|error}` JSON envelope and surfaces query-level failures
//! (HTTP 200 with `status:"error"`, which is how the Prometheus-compatible
//! API reports bad queries) as MCP application errors carrying the
//! server's message.

use serde_json::Value;
use std::time::Duration;

use parqtel_mcp_core::McpError;

/// Live HTTP client for Parqtel's Prometheus-compatible query API and
/// operational endpoints (stats, ingest rates, alerts, rules).
pub struct ParqtelClient {
    http: reqwest::Client,
    base_url: String,
}

impl ParqtelClient {
    /// Build a client for the Parqtel server at `base_url`
    /// (e.g. `http://parqtel:9090`). Trailing slashes are trimmed.
    pub fn new(base_url: &str) -> Result<Self, McpError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| McpError::ApplicationError(format!("http client init failed: {e}")))?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// Build a client from the `PARQTEL_API_URL` environment variable,
    /// defaulting to the local dev server (`http://127.0.0.1:8080`).
    pub fn from_env() -> Result<Self, McpError> {
        let base = std::env::var("PARQTEL_API_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
        Self::new(&base)
    }

    /// Base URL this client talks to (diagnostics/logging).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// GET `{base}{path}` with query params, unwrapping the Parqtel
    /// envelope. Returns the full response body; handlers pick the
    /// `data` field they need.
    pub async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value, McpError> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .get(&url)
            .query(query)
            .send()
            .await
            .map_err(|e| McpError::ApplicationError(format!("parqtel request failed: {e}")))?;
        let status = resp.status();
        let body: Value = resp.json().await.map_err(|e| {
            McpError::ApplicationError(format!(
                "parqtel returned a non-JSON response ({status}): {e}"
            ))
        })?;
        parse_envelope(status, body)
    }

    /// Instant (Prometheus `query`) evaluation of `query`.
    pub async fn query_instant(&self, query: &str) -> Result<Value, McpError> {
        self.get("/api/v1/query", &[("query", query.to_string())])
            .await
    }

    /// Range evaluation of `query` over `[start, end]` (epoch seconds)
    /// with a `step_secs` interval.
    pub async fn query_range(
        &self,
        query: &str,
        start: f64,
        end: f64,
        step_secs: u64,
    ) -> Result<Value, McpError> {
        self.get(
            "/api/v1/query_range",
            &[
                ("query", query.to_string()),
                ("start", format!("{start}")),
                ("end", format!("{end}")),
                ("step", step_secs.to_string()),
            ],
        )
        .await
    }

    /// All indexed label names (`/api/v1/labels`).
    pub async fn labels(&self) -> Result<Value, McpError> {
        self.get("/api/v1/labels", &[]).await
    }

    /// Indexed values for one label (`/api/v1/label/{name}/values`).
    /// Label names are restricted to `[a-zA-Z0-9_]` so the path segment
    /// is safe without URL encoding.
    pub async fn label_values(&self, name: &str) -> Result<Value, McpError> {
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(McpError::InvalidRequest(format!(
                "invalid label name '{name}': expected [a-zA-Z0-9_]"
            )));
        }
        self.get(&format!("/api/v1/label/{name}/values"), &[]).await
    }

    /// Log search (`/api/v1/logs`) with an optional severity floor.
    #[allow(clippy::too_many_arguments)]
    pub async fn logs(
        &self,
        query: &str,
        start: f64,
        end: f64,
        limit: usize,
        order: Option<&str>,
        severity_min: Option<&str>,
    ) -> Result<Value, McpError> {
        let mut params: Vec<(&str, String)> = vec![
            ("query", query.to_string()),
            ("start", format!("{start}")),
            ("end", format!("{end}")),
            ("limit", limit.to_string()),
        ];
        if let Some(o) = order {
            params.push(("order", o.to_string()));
        }
        if let Some(s) = severity_min {
            params.push(("severity_min", s.to_string()));
        }
        self.get("/api/v1/logs", &params).await
    }

    /// Live per-signal ingestion rates with a per-second history window
    /// (`/api/v1/ingest_rates?history_secs=N`; server clamps to 1..=900).
    pub async fn ingest_rates(&self, history_secs: Option<usize>) -> Result<Value, McpError> {
        let params: Vec<(&str, String)> = history_secs
            .map(|h| vec![("history_secs", h.to_string())])
            .unwrap_or_default();
        self.get("/api/v1/ingest_rates", &params).await
    }

    /// Current alert instances (`/api/v1/alerts`).
    pub async fn alerts(&self) -> Result<Value, McpError> {
        self.get("/api/v1/alerts", &[]).await
    }

    /// Alert rule definitions (`/api/v1/rules`).
    pub async fn rules(&self) -> Result<Value, McpError> {
        self.get("/api/v1/rules", &[]).await
    }

    /// Storage/buffer/config snapshot (`/api/v1/stats`).
    pub async fn stats(&self) -> Result<Value, McpError> {
        self.get("/api/v1/stats", &[]).await
    }
}

/// Unwrap Parqtel's `{status, data|error}` envelope. HTTP-level failures
/// and query-level failures (`status:"error"` with HTTP 200) both become
/// MCP application errors that carry the server's message.
fn parse_envelope(status: reqwest::StatusCode, body: Value) -> Result<Value, McpError> {
    let server_error = body.get("error").and_then(|v| v.as_str());
    if !status.is_success() {
        let msg = server_error.unwrap_or("unknown error");
        return Err(McpError::ApplicationError(format!(
            "parqtel {status}: {msg}"
        )));
    }
    if body.get("status").and_then(|v| v.as_str()) == Some("error") {
        let msg = server_error.unwrap_or("unknown error");
        return Err(McpError::ApplicationError(format!(
            "parqtel query failed: {msg}"
        )));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use axum::extract::Query as AxumQuery;
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    use std::collections::HashMap;

    // ── Envelope parsing (pure) ─────────────────────────────────────────

    #[test]
    fn envelope_success_passthrough() {
        let body = json!({"status": "success", "data": {"result": [1, 2, 3]}});
        let out = parse_envelope(reqwest::StatusCode::OK, body).unwrap();
        assert_eq!(out["data"]["result"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn envelope_http_error_carries_server_message() {
        let body = json!({"status": "error", "error": "parse error: unexpected }"});
        let err = parse_envelope(reqwest::StatusCode::BAD_REQUEST, body).unwrap_err();
        assert!(err.to_string().contains("parse error: unexpected }"));
        assert_eq!(
            err.to_code(),
            parqtel_mcp_core::error::ERROR_APPLICATION_START
        );
    }

    #[test]
    fn envelope_query_error_with_http_200() {
        // Parqtel reports bad queries as HTTP 200 + status:"error".
        let body = json!({"status": "error", "error": "unknown function nope()"});
        let err = parse_envelope(reqwest::StatusCode::OK, body).unwrap_err();
        assert!(err.to_string().contains("unknown function nope()"));
    }

    #[test]
    fn envelope_error_without_message_falls_back() {
        let err =
            parse_envelope(reqwest::StatusCode::INTERNAL_SERVER_ERROR, json!({})).unwrap_err();
        assert!(err.to_string().contains("unknown error"));
    }

    // ── Live wire format against a local stub server ────────────────────

    /// Spawn a stub axum server that echoes the received query params
    /// back inside the Parqtel envelope; returns its base URL.
    async fn spawn_stub() -> String {
        let app = Router::new()
            .route(
                "/api/v1/query_range",
                get(
                    |AxumQuery(q): AxumQuery<HashMap<String, String>>| async move {
                        Json(json!({"status": "success", "data": {"received": q}}))
                    },
                ),
            )
            .route(
                "/api/v1/ingest_rates",
                get(
                    |AxumQuery(q): AxumQuery<HashMap<String, String>>| async move {
                        Json(json!({"status": "success", "data": {"received": q}}))
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn query_range_sends_expected_params() {
        let base = spawn_stub().await;
        let client = ParqtelClient::new(&base).unwrap();

        let out = client
            .query_range("sum(rate(x[5m]))", 1000.5, 2000.0, 60)
            .await
            .unwrap();

        let received = &out["data"]["received"];
        assert_eq!(received["query"], "sum(rate(x[5m]))");
        assert_eq!(received["start"], "1000.5");
        assert_eq!(received["end"], "2000");
        assert_eq!(received["step"], "60");
    }

    #[tokio::test]
    async fn base_url_trailing_slash_is_trimmed() {
        let client = ParqtelClient::new("http://127.0.0.1:9/").unwrap();
        assert_eq!(client.base_url(), "http://127.0.0.1:9");
    }

    #[tokio::test]
    async fn label_values_rejects_unsafe_names() {
        let client = ParqtelClient::new("http://127.0.0.1:1").unwrap();
        for bad in ["", "../etc", "a/b", "x y"] {
            assert!(
                client.label_values(bad).await.is_err(),
                "'{bad}' must be rejected by validation"
            );
        }
        // Valid name passes validation and reaches the (nonexistent)
        // server — the failure is connection-level, not validation.
        let err = client.label_values("__name__").await.unwrap_err();
        assert!(err.to_string().contains("parqtel request failed"));
    }

    #[tokio::test]
    async fn ingest_rates_encodes_optional_history_secs() {
        let base = spawn_stub().await;
        let client = ParqtelClient::new(&base).unwrap();

        let with = client.ingest_rates(Some(900)).await.unwrap();
        assert_eq!(with["data"]["received"]["history_secs"], "900");

        let without = client.ingest_rates(None).await.unwrap();
        assert!(without["data"]["received"].get("history_secs").is_none());
    }
}
