//! Tests for the `server` module.

#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};

fn test_server() -> McpServer {
    McpServer::new(ServerConfig::default())
}

/// Regression: the bucket must start full so a client can burst its whole
/// configured per-minute quota (agents fire parallel tool calls), and must
/// reject only once the quota is genuinely exhausted.
#[test]
fn rate_limiter_allows_full_quota_burst_then_blocks() {
    let mut limiter = RateLimiter::new(600);
    for i in 0..600 {
        assert!(limiter.allow("agent"), "burst call {i} should be allowed");
    }
    assert!(!limiter.allow("agent"), "601st call must be throttled");
    // Quotas are per client.
    assert!(limiter.allow("other-agent"));
}

/// Regression: an unconfigured client id must not be starved by
/// whole-second clock granularity — many calls inside one second succeed.
#[test]
fn rate_limiter_allows_burst_within_single_second() {
    let mut limiter = RateLimiter::new(60);
    let allowed = (0..60).filter(|_| limiter.allow("agent")).count();
    assert_eq!(
        allowed, 60,
        "all 60 per-minute calls must fit in one second"
    );
    assert!(!limiter.allow("agent"));
}

fn counting_handler(counter: Arc<AtomicU64>) -> ToolHandler {
    Arc::new(move |params: Value| {
        let counter = counter.clone();
        Box::pin(async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let q = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
            Ok(json!({ "query": q, "executed": true }))
        })
    })
}

#[tokio::test]
async fn dispatches_to_registered_handler() {
    let counter = Arc::new(AtomicU64::new(0));
    let mut server = test_server();
    server.register_tool_with_handler(
        McpTool {
            name: "query_metrics".into(),
            description: "test".into(),
            input_schema: json!({}),
        },
        counting_handler(counter.clone()),
    );

    let result = execute_tool(
        &server,
        "query_metrics",
        &json!({ "name": "query_metrics", "query": "up" }),
    )
    .await
    .unwrap();

    assert_eq!(result["query"], "up");
    assert_eq!(result["executed"], true);
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Control fields (name/client_id) never reach the handler.
    let result = execute_tool(
        &server,
        "query_metrics",
        &json!({ "name": "query_metrics", "client_id": "agent-1", "query": "down" }),
    )
    .await
    .unwrap();
    assert_eq!(result["query"], "down");
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn falls_back_to_echo_without_handler() {
    let mut server = test_server();
    server.register_tool(McpTool {
        name: "legacy_tool".into(),
        description: "no live executor".into(),
        input_schema: json!({}),
    });

    let result = execute_tool(
        &server,
        "legacy_tool",
        &json!({ "name": "legacy_tool", "filter": "{}" }),
    )
    .await
    .unwrap();

    assert_eq!(result["status"], "success");
    assert_eq!(result["tool"], "legacy_tool");
    assert_eq!(result["params"]["filter"], "{}");
}

#[tokio::test]
async fn unknown_tool_is_method_not_found() {
    let server = test_server();
    let err = execute_tool(&server, "nope", &json!({ "name": "nope" }))
        .await
        .unwrap_err();
    assert!(matches!(err, McpError::MethodNotFound(_)));
    assert_eq!(err.to_code(), crate::error::ERROR_METHOD_NOT_FOUND);
}

#[tokio::test]
async fn handler_errors_propagate() {
    let mut server = test_server();
    let failing: ToolHandler = Arc::new(|_params: Value| {
        Box::pin(async { Err(McpError::ApplicationError("parqtel unreachable".into())) })
    });
    server.register_tool_with_handler(
        McpTool {
            name: "failing".into(),
            description: "test".into(),
            input_schema: json!({}),
        },
        failing,
    );

    let err = execute_tool(&server, "failing", &json!({ "name": "failing" }))
        .await
        .unwrap_err();
    assert_eq!(err.to_code(), crate::error::ERROR_APPLICATION_START);
}

/// Regression: caller-caused errors must map to 4xx so agent SDKs don't
/// retry a fixable request as if the server were down.
#[test]
fn error_http_status_mapping() {
    use axum::http::StatusCode;
    assert_eq!(
        McpError::InvalidRequest("bad".into()).to_http_status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        McpError::ParseError("bad".into()).to_http_status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        McpError::MethodNotFound("nope".into()).to_http_status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        McpError::ApplicationError("upstream".into()).to_http_status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        McpError::InternalError("boom".into()).to_http_status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

/// Regression: `build_router()` owns `/health` (with an accurate tool
/// count). Servers must NOT add a second `/health` route on top of the
/// merged router — axum panics at boot with "Overlapping method route".
#[tokio::test]
async fn router_serves_health_with_accurate_tool_count() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let mut server = test_server();
    for name in ["one", "two"] {
        server.register_tool(McpTool {
            name: name.into(),
            description: "test".into(),
            input_schema: json!({}),
        });
    }

    let res = server
        .clone()
        .build_router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["tools"], 2);
}

/// Spec-test server with one live tool.
fn spec_server() -> Arc<McpServer> {
    let mut server = test_server();
    let handler: ToolHandler = Arc::new(|params: Value| {
        Box::pin(
            async move { Ok(json!({"echo": params.get("query").cloned().unwrap_or(Value::Null)})) },
        )
    });
    server.register_tool_with_handler(
        McpTool {
            name: "query_metrics".into(),
            description: "t".into(),
            input_schema: json!({"type": "object"}),
        },
        handler,
    );
    Arc::new(server)
}

async fn post_mcp(s: Arc<McpServer>, body: Value) -> (StatusCode, Value, HeaderMap) {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    let res = (*s)
        .clone()
        .build_router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap(), headers)
}

#[tokio::test]
async fn mcp_initialize_returns_spec_handshake() {
    let (status, body, headers) = post_mcp(spec_server(), json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(body["result"]["serverInfo"]["name"], "parqtel-mcp");
    assert!(headers.contains_key(HEADER_SESSION_ID));
}

#[tokio::test]
async fn mcp_initialized_notification_is_accepted() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    let res = (*spec_server())
        .clone()
        .build_router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn mcp_tools_list_uses_spec_input_schema() {
    let (status, body, _) = post_mcp(
        spec_server(),
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let tools = body["result"]["tools"].as_array().unwrap();
    assert_eq!(tools[0]["name"], "query_metrics");
    assert_eq!(tools[0]["inputSchema"]["type"], "object");
}

#[tokio::test]
async fn mcp_tools_call_wraps_result_in_envelope() {
    let server = spec_server();
    let (status, body, _) = post_mcp(
        server.clone(),
        json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": {"name": "query_metrics", "arguments": {"query": "up"}}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["isError"], false);
    assert_eq!(body["result"]["content"][0]["type"], "text");
    assert_eq!(body["result"]["structuredContent"]["echo"], "up");
    let (_, body, _) = post_mcp(
        server,
        json!({"jsonrpc": "2.0", "id": 4, "method": "nope", "params": {}}),
    )
    .await;
    assert_eq!(body["error"]["code"], crate::error::ERROR_METHOD_NOT_FOUND);
}

#[tokio::test]
async fn mcp_unknown_tool_is_method_not_found() {
    let (_, body, _) = post_mcp(
        spec_server(),
        json!({"jsonrpc": "2.0", "id": 5, "method": "tools/call",
        "params": {"name": "missing", "arguments": {}}}),
    )
    .await;
    assert_eq!(body["error"]["code"], crate::error::ERROR_METHOD_NOT_FOUND);
}

#[tokio::test]
async fn mcp_tool_failure_sets_is_error() {
    let mut server = test_server();
    let failing: ToolHandler =
        Arc::new(|_p: Value| Box::pin(async { Err(McpError::ApplicationError("boom".into())) }));
    server.register_tool_with_handler(
        McpTool {
            name: "failing".into(),
            description: "t".into(),
            input_schema: json!({}),
        },
        failing,
    );
    let (status, body, _) = post_mcp(
        Arc::new(server),
        json!({"jsonrpc": "2.0", "id": 6, "method": "tools/call",
        "params": {"name": "failing", "arguments": {}}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["isError"], true);
}
