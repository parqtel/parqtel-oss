//! Tests for the `handlers` module.

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
            get(|| async { Json(json!({"status": "success", "data": ["service_name", "host"]})) }),
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

fn alert(id: &str, rule: &str, state: &str, service: &str, age_hours: f64, score: f64) -> Value {
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
