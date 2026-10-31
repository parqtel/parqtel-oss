//! Live tool handlers wired to a [`ParqtelClient`].
//!
//! Each `handle_*` function takes the tool params and returns the MCP
//! result payload. They are plain async functions over the client so they
//! are unit-testable without the HTTP server layer; `make_*_handler`
//! constructors adapt them into `ToolHandler` closures for registration.

use serde_json::{json, Value};

use parqtel_mcp_core::error::McpError;

use crate::client::ParqtelClient;

/// Hard cap on returned log records — keeps agent context windows (and
/// MCP server memory) bounded; matches the UI's fetch cap.
pub const MAX_LOG_LIMIT: usize = 2000;

/// Cap on relative `window_secs` — 30 days.
pub const MAX_WINDOW_SECS: u64 = 30 * 24 * 3600;

/// Server-side ingest-rates wheel is 900 one-second buckets.
pub const MAX_HISTORY_SECS: usize = 900;

// ── param extraction helpers ────────────────────────────────────────────

fn p_str<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params.get(key).and_then(|v| v.as_str())
}

/// Numeric param as f64; errors on wrong types instead of silently
/// ignoring them (bad types from an agent are a request problem).
fn p_f64(params: &Value, key: &str) -> Result<Option<f64>, McpError> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_f64()
            .map(Some)
            .ok_or_else(|| McpError::InvalidRequest(format!("'{key}' must be a number"))),
    }
}

fn p_u64(params: &Value, key: &str) -> Result<Option<u64>, McpError> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v.as_u64().map(Some).ok_or_else(|| {
            McpError::InvalidRequest(format!("'{key}' must be a non-negative integer"))
        }),
    }
}

fn p_usize(params: &Value, key: &str) -> Result<Option<usize>, McpError> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v.as_u64().map(|u| Some(u as usize)).ok_or_else(|| {
            McpError::InvalidRequest(format!("'{key}' must be a non-negative integer"))
        }),
    }
}

/// Resolve the range bounds from `start`/`end` (epoch seconds) or their
/// legacy `start_ns`/`end_ns` (nanoseconds) aliases.
fn resolve_bounds(params: &Value) -> Result<Option<(f64, f64)>, McpError> {
    let start = p_f64(params, "start")?;
    let end = p_f64(params, "end")?;
    if start.is_some() || end.is_some() {
        return Ok(Some((
            start.ok_or_else(|| too_partial("start"))?,
            end.ok_or_else(|| too_partial("end"))?,
        )));
    }
    let start_ns = p_f64(params, "start_ns")?;
    let end_ns = p_f64(params, "end_ns")?;
    if start_ns.is_some() || end_ns.is_some() {
        return Ok(Some((
            start_ns.ok_or_else(|| too_partial("start_ns"))? / 1e9,
            end_ns.ok_or_else(|| too_partial("end_ns"))? / 1e9,
        )));
    }
    Ok(None)
}

fn too_partial(name: &str) -> McpError {
    McpError::InvalidRequest(format!("provide both '{name}' and its pair, or neither"))
}

pub(crate) fn now_secs() -> f64 {
    chrono::Utc::now().timestamp() as f64
}

// ── query_metrics ───────────────────────────────────────────────────────

/// Query metrics: instant (query only), explicit range (start+end) or
/// trailing window (window_secs). Range results carry the per-bucket
/// `volumeSummary` and `totalSeriesCount` provided by the server.
pub async fn handle_query_metrics(
    client: &ParqtelClient,
    params: &Value,
) -> Result<Value, McpError> {
    let query = p_str(params, "query")
        .map(str::trim)
        .filter(|q| !q.is_empty())
        .ok_or_else(|| McpError::InvalidRequest("'query' is required (PromQL expression)".into()))?
        .to_string();
    let step = p_u64(params, "step_secs")?.map(|s| s.max(1));

    if let Some((start, end)) = resolve_bounds(params)? {
        if end < start {
            return Err(McpError::InvalidRequest("'end' must be >= 'start'".into()));
        }
        let step = step.unwrap_or(60);
        let out = client.query_range(&query, start, end, step).await?;
        return Ok(json!({
            "mode": "range",
            "query": query,
            "start": start,
            "end": end,
            "step_secs": step,
            "data": out.get("data").cloned().unwrap_or(Value::Null),
        }));
    }

    if let Some(window) = p_u64(params, "window_secs")? {
        if window == 0 {
            return Err(McpError::InvalidRequest(
                "'window_secs' must be >= 1".into(),
            ));
        }
        if window > MAX_WINDOW_SECS {
            return Err(McpError::InvalidRequest(format!(
                "'window_secs' must be <= {MAX_WINDOW_SECS} (30 days)"
            )));
        }
        let step = step.unwrap_or(60);
        let now = now_secs();
        let start = now - window as f64;
        let out = client.query_range(&query, start, now, step).await?;
        return Ok(json!({
            "mode": "range",
            "query": query,
            "window_secs": window,
            "start": start,
            "end": now,
            "step_secs": step,
            "data": out.get("data").cloned().unwrap_or(Value::Null),
        }));
    }

    let out = client.query_instant(&query).await?;
    Ok(json!({
        "mode": "instant",
        "query": query,
        "data": out.get("data").cloned().unwrap_or(Value::Null),
    }))
}

// ── query_metrics_labels ────────────────────────────────────────────────

/// List all label names, or the indexed values of one label.
pub async fn handle_query_metrics_labels(
    client: &ParqtelClient,
    params: &Value,
) -> Result<Value, McpError> {
    match p_str(params, "label") {
        Some(name) => {
            let out = client.label_values(name).await?;
            Ok(json!({
                "label": name,
                "data": out.get("data").cloned().unwrap_or(Value::Null),
            }))
        }
        None => {
            let out = client.labels().await?;
            Ok(json!({
                "data": out.get("data").cloned().unwrap_or(Value::Null),
            }))
        }
    }
}

// ── query_logs ──────────────────────────────────────────────────────────

/// Search logs: default last hour, newest first, capped at
/// [`MAX_LOG_LIMIT`] records.
pub async fn handle_query_logs(client: &ParqtelClient, params: &Value) -> Result<Value, McpError> {
    let query = p_str(params, "query").unwrap_or("{}");
    let now = now_secs();
    let start = p_f64(params, "start")?.unwrap_or(now - 3600.0);
    let end = p_f64(params, "end")?.unwrap_or(now);
    if end < start {
        return Err(McpError::InvalidRequest("'end' must be >= 'start'".into()));
    }
    let limit = p_usize(params, "limit")?.unwrap_or(100).min(MAX_LOG_LIMIT);
    let order = p_str(params, "order");
    let severity_min = p_str(params, "severity_min");

    let out = client
        .logs(query, start, end, limit, order, severity_min)
        .await?;
    Ok(json!({
        "query": query,
        "start": start,
        "end": end,
        "limit": limit,
        "data": out.get("data").cloned().unwrap_or(Value::Null),
    }))
}

// ── ingest_rates ────────────────────────────────────────────────────────

/// Live ingestion rates with the per-second history window clamped to
/// the server's 15-minute wheel.
pub async fn handle_ingest_rates(
    client: &ParqtelClient,
    params: &Value,
) -> Result<Value, McpError> {
    let history = p_usize(params, "history_secs")?.map(|h| h.clamp(1, MAX_HISTORY_SECS));
    let out = client.ingest_rates(history).await?;
    Ok(json!({
        "history_secs": history,
        "data": out.get("data").cloned().unwrap_or(Value::Null),
    }))
}

// ── get_alert_history ───────────────────────────────────────────────────

/// True when the alert's labels carry `want` on any of the common
/// service/pod label keys.
fn label_matches(labels: &Value, keys: &[&str], want: &str) -> bool {
    keys.iter()
        .any(|k| labels.get(k).and_then(|v| v.as_str()) == Some(want))
}

/// Alert instances within the lookback window, optionally filtered by
/// service or pod. Alerts are matched on the usual service/pod label keys.
pub async fn handle_get_alert_history(
    client: &ParqtelClient,
    params: &Value,
) -> Result<Value, McpError> {
    let service = p_str(params, "service_name");
    let pod = p_str(params, "pod_name");
    let since_hours = p_f64(params, "since_hours")?.unwrap_or(24.0);
    if since_hours < 0.0 {
        return Err(McpError::InvalidRequest(
            "'since_hours' must be >= 0".into(),
        ));
    }
    let cutoff =
        chrono::Utc::now() - chrono::Duration::milliseconds((since_hours * 3600.0 * 1000.0) as i64);

    let out = client.alerts().await?;
    let all = out
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut matched = Vec::new();
    for alert in all {
        // started_at is RFC3339; unparseable/missing timestamps are kept
        // only when no lookback filter was requested.
        let started = alert
            .get("started_at")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.with_timezone(&chrono::Utc));
        if let Some(t) = started {
            if t < cutoff {
                continue;
            }
        } else if since_hours > 0.0 {
            continue;
        }
        let labels = alert.get("labels").cloned().unwrap_or(json!({}));
        if let Some(svc) = service {
            if !label_matches(&labels, &["service.name", "service_name", "service"], svc) {
                continue;
            }
        }
        if let Some(pod) = pod {
            if !label_matches(&labels, &["pod", "pod_name", "k8s.pod.name"], pod) {
                continue;
            }
        }
        matched.push(alert);
    }

    Ok(json!({
        "since_hours": since_hours,
        "service_name": service,
        "pod_name": pod,
        "count": matched.len(),
        "alerts": matched,
    }))
}

// ── get_noise_statistics ────────────────────────────────────────────────

/// Per-rule noise statistics derived from the live alert stream: alert
/// counts by state plus noise-score aggregates.
pub async fn handle_get_noise_statistics(
    client: &ParqtelClient,
    params: &Value,
) -> Result<Value, McpError> {
    let rule_filter = p_str(params, "rule_id");
    let out = client.alerts().await?;
    let all = out
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    // rule_id → (name, total, firing, resolved, score_sum, score_max)
    use std::collections::BTreeMap;
    let mut stats: BTreeMap<String, (String, u64, u64, u64, f64, f32)> = BTreeMap::new();
    for alert in &all {
        let rid = alert
            .get("rule_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        if let Some(f) = rule_filter {
            if rid != f {
                continue;
            }
        }
        let name = alert
            .get("rule_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = alert.get("state").and_then(|v| v.as_str()).unwrap_or("");
        let score = alert
            .get("noise_score")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let entry = stats
            .entry(rid.to_string())
            .or_insert((name, 0, 0, 0, 0.0, 0.0));
        entry.1 += 1; // total
        if state.eq_ignore_ascii_case("firing") {
            entry.2 += 1;
        } else if state.eq_ignore_ascii_case("resolved") {
            entry.3 += 1;
        }
        entry.4 += score;
        entry.5 = entry.5.max(score as f32);
    }

    let rules: Vec<Value> = stats
        .into_iter()
        .map(
            |(rid, (name, total, firing, resolved, score_sum, score_max))| {
                json!({
                    "rule_id": rid,
                    "rule_name": name,
                    "total_alerts": total,
                    "firing": firing,
                    "resolved": resolved,
                    "avg_noise_score": if total > 0 { score_sum / total as f64 } else { 0.0 },
                    "max_noise_score": score_max,
                })
            },
        )
        .collect();

    Ok(json!({
        "rule_id": rule_filter,
        "rules": rules,
        "total_alerts_seen": all.len(),
    }))
}

// ── get_topology ────────────────────────────────────────────────────────

/// Observed services (from the service_name label index) plus a storage
/// and buffer snapshot — the "lay of the land" view for agents.
pub async fn handle_get_topology(
    client: &ParqtelClient,
    params: &Value,
) -> Result<Value, McpError> {
    let namespace = p_str(params, "namespace");

    let stats_out = client.stats().await?;
    let storage = stats_out
        .pointer("/data/storage")
        .cloned()
        .unwrap_or(Value::Null);
    let buffer = stats_out
        .pointer("/data/buffer")
        .cloned()
        .unwrap_or(Value::Null);

    // service_name is the canonical dedicated column; fall back to the
    // generic `service` label for older data.
    let mut services: Vec<String> = label_list(client, "service_name").await;
    if services.is_empty() {
        services = label_list(client, "service").await;
    }
    if let Some(ns) = namespace {
        services.retain(|s| s.contains(ns));
    }

    Ok(json!({
        "namespace": namespace,
        "services": services,
        "service_count": services.len(),
        "storage": storage,
        "buffer": buffer,
        "generated_at": chrono::Utc::now().to_rfc3339(),
    }))
}

async fn label_list(client: &ParqtelClient, label: &str) -> Vec<String> {
    match client.label_values(label).await {
        Ok(v) => v
            .get("data")
            .and_then(|d| d.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

// ── ToolHandler constructors ────────────────────────────────────────────

use std::sync::Arc;

use parqtel_mcp_core::tool::{BoxToolFuture, ToolHandler};

/// Adapt a `handle_*` function into a registerable [`ToolHandler`].
macro_rules! handler_for {
    ($client:expr, $handle:ident) => {{
        let client = Arc::new($client);
        Arc::new(move |params: Value| {
            let client = client.clone();
            Box::pin(async move { $handle(&client, &params).await }) as BoxToolFuture
        })
    }};
}

/// `query_metrics` handler
pub fn make_query_metrics_handler(client: ParqtelClient) -> ToolHandler {
    handler_for!(client, handle_query_metrics)
}

/// `query_metrics_labels` handler
pub fn make_query_metrics_labels_handler(client: ParqtelClient) -> ToolHandler {
    handler_for!(client, handle_query_metrics_labels)
}

/// `query_logs` handler
pub fn make_query_logs_handler(client: ParqtelClient) -> ToolHandler {
    handler_for!(client, handle_query_logs)
}

/// `ingest_rates` handler
pub fn make_ingest_rates_handler(client: ParqtelClient) -> ToolHandler {
    handler_for!(client, handle_ingest_rates)
}

/// `get_alert_history` handler
pub fn make_get_alert_history_handler(client: ParqtelClient) -> ToolHandler {
    handler_for!(client, handle_get_alert_history)
}

/// `get_noise_statistics` handler
pub fn make_get_noise_statistics_handler(client: ParqtelClient) -> ToolHandler {
    handler_for!(client, handle_get_noise_statistics)
}

/// `get_topology` handler
pub fn make_get_topology_handler(client: ParqtelClient) -> ToolHandler {
    handler_for!(client, handle_get_topology)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use axum::extract::{Path as AxumPath, Query as AxumQuery};
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    use std::collections::HashMap;

    /// Spawn a stub Parqtel: query paths echo received params; label
    /// values, alerts and stats return canned fixture data.
    async fn spawn_stub(alerts: Value, services: Value, stats: Value) -> String {
        let alerts_route = {
            let alerts = alerts.clone();
            get(move || {
                let alerts = alerts.clone();
                async move { Json(json!({"status": "success", "data": alerts})) }
            })
        };
        let stats_route = {
            let stats = stats.clone();
            get(move || {
                let stats = stats.clone();
                async move { Json(json!({"status": "success", "data": stats})) }
            })
        };
        let app = Router::new()
            .route(
                "/api/v1/query",
                get(
                    |AxumQuery(q): AxumQuery<HashMap<String, String>>| async move {
                        Json(json!({"status": "success", "data": {"received": q}}))
                    },
                ),
            )
            .route(
                "/api/v1/query_range",
                get(
                    |AxumQuery(q): AxumQuery<HashMap<String, String>>| async move {
                        Json(json!({"status": "success", "data": {"received": q}}))
                    },
                ),
            )
            .route(
                "/api/v1/logs",
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
            )
            .route(
                "/api/v1/labels",
                get(|| async {
                    Json(json!({"status": "success", "data": ["service_name", "host"]}))
                }),
            )
            .route(
                "/api/v1/label/:name/values",
                get(|AxumPath(name): AxumPath<String>| async move {
                    let data = if name == "service_name" {
                        services.clone()
                    } else {
                        json!([])
                    };
                    Json(json!({"status": "success", "data": data}))
                }),
            )
            .route("/api/v1/alerts", alerts_route)
            .route("/api/v1/stats", stats_route);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn alert(
        id: &str,
        rule: &str,
        state: &str,
        service: &str,
        age_hours: f64,
        score: f64,
    ) -> Value {
        json!({
            "id": id,
            "rule_id": rule,
            "rule_name": format!("rule-{rule}"),
            "state": state,
            "labels": {"service.name": service},
            "noise_score": score,
            "started_at": (chrono::Utc::now()
                - chrono::Duration::milliseconds((age_hours * 3600.0 * 1000.0) as i64))
                .to_rfc3339(),
        })
    }

    // ── query_metrics ───────────────────────────────────────────────────

    #[tokio::test]
    async fn instant_mode_by_default() {
        let base = spawn_stub(json!([]), json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();
        let out = handle_query_metrics(&client, &json!({"query": " http_requests_total "}))
            .await
            .unwrap();
        assert_eq!(out["mode"], "instant");
        assert_eq!(out["query"], "http_requests_total");
        assert_eq!(out["data"]["received"]["query"], "http_requests_total");
    }

    #[tokio::test]
    async fn range_mode_with_explicit_bounds() {
        let base = spawn_stub(json!([]), json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();
        let out = handle_query_metrics(
            &client,
            &json!({"query": "up", "start": 1000.5, "end": 2000, "step_secs": 30}),
        )
        .await
        .unwrap();
        assert_eq!(out["mode"], "range");
        assert_eq!(out["start"], 1000.5);
        assert_eq!(out["step_secs"], 30);
        assert_eq!(out["data"]["received"]["step"], "30");
    }

    #[tokio::test]
    async fn legacy_ns_bounds_are_converted_to_seconds() {
        let base = spawn_stub(json!([]), json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();
        let out = handle_query_metrics(
            &client,
            &json!({"query": "up", "start_ns": 1_000_000_000.0, "end_ns": 2_000_000_000.0}),
        )
        .await
        .unwrap();
        assert_eq!(out["mode"], "range");
        assert_eq!(out["start"], 1.0);
        assert_eq!(out["end"], 2.0);
        assert_eq!(out["data"]["received"]["start"], "1");
    }

    #[tokio::test]
    async fn window_mode_uses_trailing_seconds() {
        let base = spawn_stub(json!([]), json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();
        let before = now_secs();
        let out = handle_query_metrics(&client, &json!({"query": "up", "window_secs": 900}))
            .await
            .unwrap();
        assert_eq!(out["mode"], "range");
        assert_eq!(out["window_secs"], 900);
        let start = out["start"].as_f64().unwrap();
        let end = out["end"].as_f64().unwrap();
        assert!((end - start - 900.0).abs() < 5.0, "window must span 900s");
        assert!(end >= before - 5.0 && end <= now_secs() + 5.0);
    }

    #[tokio::test]
    async fn query_metrics_rejects_bad_params() {
        let base = spawn_stub(json!([]), json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();
        // missing query
        assert!(handle_query_metrics(&client, &json!({})).await.is_err());
        // empty query
        assert!(handle_query_metrics(&client, &json!({"query": "  "}))
            .await
            .is_err());
        // end < start
        assert!(
            handle_query_metrics(&client, &json!({"query": "up", "start": 10, "end": 5}))
                .await
                .is_err()
        );
        // half-specified bounds
        assert!(
            handle_query_metrics(&client, &json!({"query": "up", "start": 10}))
                .await
                .is_err()
        );
        // window 0
        assert!(
            handle_query_metrics(&client, &json!({"query": "up", "window_secs": 0}))
                .await
                .is_err()
        );
        // wrong type
        assert!(
            handle_query_metrics(&client, &json!({"query": "up", "step_secs": "60"}))
                .await
                .is_err()
        );
    }

    // ── query_logs ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn logs_default_window_and_limit_cap() {
        let base = spawn_stub(json!([]), json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();
        let out = handle_query_logs(&client, &json!({"limit": 100_000}))
            .await
            .unwrap();
        assert_eq!(out["limit"], MAX_LOG_LIMIT, "oversized limit is capped");
        assert_eq!(out["data"]["received"]["limit"], "2000");
        assert_eq!(out["query"], "{}");
        let start = out["start"].as_f64().unwrap();
        let end = out["end"].as_f64().unwrap();
        assert!((end - start - 3600.0).abs() < 5.0, "default window is 1h");
    }

    // ── ingest_rates ────────────────────────────────────────────────────

    #[tokio::test]
    async fn ingest_rates_history_clamped_to_wheel() {
        let base = spawn_stub(json!([]), json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();
        let out = handle_ingest_rates(&client, &json!({"history_secs": 5000}))
            .await
            .unwrap();
        assert_eq!(out["history_secs"], MAX_HISTORY_SECS);
        assert_eq!(out["data"]["received"]["history_secs"], "900");

        let out = handle_ingest_rates(&client, &json!({})).await.unwrap();
        assert_eq!(out["history_secs"], Value::Null);
        assert!(out["data"]["received"].get("history_secs").is_none());
    }

    // ── get_alert_history ───────────────────────────────────────────────

    #[tokio::test]
    async fn alert_history_filters_by_window_and_service() {
        let alerts = json!([
            alert("a1", "r1", "Firing", "api", 1.0, 0.8),
            alert("a2", "r2", "Resolved", "web", 48.0, 0.2),
            // Slightly inside the 0.5h boundary so test-clock drift between
            // fixture creation and the handler's cutoff never flips it out.
            alert("a3", "r1", "Pending", "api", 0.4, 0.1),
        ]);
        let base = spawn_stub(alerts, json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();

        // Default 24h window drops the 48h-old alert.
        let out = handle_get_alert_history(&client, &json!({})).await.unwrap();
        assert_eq!(out["count"], 2);
        assert_eq!(out["alerts"][0]["id"], "a1");

        // Service filter.
        let out = handle_get_alert_history(&client, &json!({"service_name": "api"}))
            .await
            .unwrap();
        assert_eq!(out["count"], 2);

        let out = handle_get_alert_history(&client, &json!({"service_name": "web"}))
            .await
            .unwrap();
        assert_eq!(out["count"], 0, "48h-old web alert is outside the window");

        // Tight window keeps only the freshest alert.
        let out = handle_get_alert_history(&client, &json!({"since_hours": 0.5}))
            .await
            .unwrap();
        assert_eq!(out["count"], 1);
        assert_eq!(out["alerts"][0]["id"], "a3");

        // Bad param type is rejected.
        assert!(
            handle_get_alert_history(&client, &json!({"since_hours": "2"}))
                .await
                .is_err()
        );
    }

    // ── get_noise_statistics ────────────────────────────────────────────

    #[tokio::test]
    async fn noise_statistics_aggregate_per_rule() {
        let alerts = json!([
            alert("a1", "r1", "Firing", "api", 1.0, 0.8),
            alert("a2", "r1", "Resolved", "web", 2.0, 0.4),
            alert("a3", "r2", "Firing", "db", 3.0, 0.1),
        ]);
        let base = spawn_stub(alerts, json!([]), json!({})).await;
        let client = ParqtelClient::new(&base).unwrap();

        let out = handle_get_noise_statistics(&client, &json!({}))
            .await
            .unwrap();
        assert_eq!(out["total_alerts_seen"], 3);
        let rules = out["rules"].as_array().unwrap();
        assert_eq!(rules.len(), 2);
        let r1 = rules.iter().find(|r| r["rule_id"] == "r1").unwrap();
        assert_eq!(r1["total_alerts"], 2);
        assert_eq!(r1["firing"], 1);
        assert_eq!(r1["resolved"], 1);
        let avg = r1["avg_noise_score"].as_f64().unwrap();
        assert!((avg - 0.6).abs() < 1e-9, "avg noise (0.8+0.4)/2, got {avg}");

        let out = handle_get_noise_statistics(&client, &json!({"rule_id": "r2"}))
            .await
            .unwrap();
        let rules = out["rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["rule_id"], "r2");
        assert_eq!(rules[0]["firing"], 1);
    }

    // ── get_topology ────────────────────────────────────────────────────

    #[tokio::test]
    async fn topology_lists_services_with_optional_filter() {
        let stats = json!({
            "storage": {"metrics": {"blocks": 3, "rows": 100, "bytes": 2048}},
            "buffer": {"metrics_points": 42, "logs": 0, "spans": 0},
        });
        let base = spawn_stub(json!([]), json!(["api", "web", "db"]), stats).await;
        let client = ParqtelClient::new(&base).unwrap();

        let out = handle_get_topology(&client, &json!({})).await.unwrap();
        assert_eq!(out["service_count"], 3);
        assert_eq!(out["storage"]["metrics"]["blocks"], 3);
        assert_eq!(out["buffer"]["metrics_points"], 42);

        let out = handle_get_topology(&client, &json!({"namespace": "a"}))
            .await
            .unwrap();
        assert_eq!(out["service_count"], 1);
        assert_eq!(out["services"][0], "api");
    }
}
