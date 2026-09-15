//! Server implementation for MCP

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;
use uuid::Uuid;

use crate::{
    error::McpError,
    tool::{sanitize_params, McpTool, ToolHandler},
};

/// Configuration for the MCP server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub rate_limit_requests_per_minute: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 3000,
            rate_limit_requests_per_minute: 60,
        }
    }
}

/// Rate limiter using a per-client token bucket.
///
/// The bucket holds one minute's worth of requests (`requests_per_minute`)
/// and starts full, so a client may burst up to its whole configured quota —
/// agents routinely fire several tool calls in parallel. Tokens refill
/// continuously on a millisecond clock; the previous implementation used a
/// whole-second clock and started the bucket at `requests_per_minute / 60`
/// tokens, so it could not refill inside a second and throttled callers far
/// below the configured rate.
struct RateLimiter {
    requests_per_minute: u32,
    tokens: HashMap<String, f64>,
    last_update_ms: HashMap<String, u64>,
}

impl RateLimiter {
    fn new(requests_per_minute: u32) -> Self {
        Self {
            requests_per_minute,
            tokens: HashMap::new(),
            last_update_ms: HashMap::new(),
        }
    }

    fn allow(&mut self, client_id: &str) -> bool {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        let capacity = self.requests_per_minute as f64;
        let refill_per_ms = capacity / 60_000.0;

        let entry = self.tokens.entry(client_id.to_string()).or_insert(capacity);
        let last = self
            .last_update_ms
            .entry(client_id.to_string())
            .or_insert(now_ms);

        let elapsed_ms = now_ms.saturating_sub(*last) as f64;
        *entry = (*entry + elapsed_ms * refill_per_ms).min(capacity);
        *last = now_ms;

        if *entry >= 1.0 {
            *entry -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Audit log entry for a tool call
#[derive(Debug, Serialize)]
struct AuditLogEntry {
    timestamp: String,
    request_id: String,
    tool_name: String,
    client_id: String,
    params: Value,
    result: String,
}

/// Main MCP server struct
pub struct McpServer {
    config: ServerConfig,
    tools: Vec<McpTool>,
    /// Live executors keyed by tool name. Tools registered without a
    /// handler keep the legacy echo contract (see `execute_tool`).
    handlers: HashMap<String, ToolHandler>,
    rate_limiter: Arc<std::sync::Mutex<RateLimiter>>,
}

impl McpServer {
    /// Create a new MCP server
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config: config.clone(),
            tools: Vec::new(),
            handlers: HashMap::new(),
            rate_limiter: Arc::new(std::sync::Mutex::new(RateLimiter::new(
                config.rate_limit_requests_per_minute,
            ))),
        }
    }

    /// Register a tool with the server
    pub fn register_tool(&mut self, tool: McpTool) {
        self.tools.push(tool);
    }

    /// Register a tool together with its live async executor.
    pub fn register_tool_with_handler(&mut self, tool: McpTool, handler: ToolHandler) {
        self.handlers.insert(tool.name.clone(), handler);
        self.tools.push(tool);
    }

    /// Get the list of registered tools
    pub fn get_tools(&self) -> &[McpTool] {
        &self.tools
    }

    /// Look up the live executor registered for a tool, if any.
    pub fn get_handler(&self, tool_name: &str) -> Option<&ToolHandler> {
        self.handlers.get(tool_name)
    }

    /// Build the Axum router
    pub fn build_router(self) -> Router {
        Router::new()
            .route("/health", get(health_handler))
            .route("/tools/list", get(tools_list_handler))
            .route("/tools/call", post(tools_call_handler))
            .with_state(Arc::new(self))
    }

    /// Start the server
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let app = self.build_router();

        info!("Starting MCP server on {}", addr);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}

/// Health check handler
/// Liveness/readiness probe.
///
/// Owned by the framework router so every MCP server reports the same shape
/// and an accurate tool count — servers previously registered their own
/// `/health` on top of `build_router()`, which made axum panic at boot with
/// "Overlapping method route".
async fn health_handler(State(server): State<Arc<McpServer>>) -> Json<Value> {
    Json(serde_json::json!({
        "status": "ok",
        "tools": server.get_tools().len(),
        "timestamp": Utc::now().to_rfc3339()
    }))
}

/// Tools list handler
async fn tools_list_handler(State(server): State<Arc<McpServer>>) -> Json<Value> {
    let tools: Vec<Value> = server
        .get_tools()
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema
            })
        })
        .collect();

    Json(serde_json::json!({
        "tools": tools
    }))
}

/// Tools call handler
async fn tools_call_handler(
    State(server): State<Arc<McpServer>>,
    Json(request): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let id = request
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let method = request.get("method").and_then(|v| v.as_str());

    match method {
        Some("tools/call") => {}
        Some(m) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": crate::error::ERROR_METHOD_NOT_FOUND,
                        "message": format!("Method not found: {}", m)
                    },
                    "id": id
                })),
            );
        }
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": crate::error::ERROR_INVALID_REQUEST,
                        "message": "Missing 'method' field"
                    },
                    "id": id
                })),
            );
        }
    }

    let params = request.get("params").cloned().unwrap_or(Value::Null);
    let tool_name = params.get("name").and_then(|v| v.as_str());

    let tool_name = match tool_name {
        Some(name) => name,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": crate::error::ERROR_INVALID_REQUEST,
                        "message": "Missing 'name' in params"
                    },
                    "id": id
                })),
            );
        }
    };

    let _tool = match server.get_tools().iter().find(|t| t.name == tool_name) {
        Some(t) => t,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": crate::error::ERROR_METHOD_NOT_FOUND,
                        "message": format!("Tool not found: {}", tool_name)
                    },
                    "id": id
                })),
            );
        }
    };

    let client_id = params
        .get("client_id")
        .and_then(|v| v.as_str())
        .unwrap_or("anonymous");
    {
        let mut limiter = server.rate_limiter.lock().unwrap();
        if !limiter.allow(client_id) {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": crate::error::ERROR_APPLICATION_START,
                        "message": "Rate limit exceeded"
                    },
                    "id": id
                })),
            );
        }
    }

    let request_id = Uuid::new_v4().to_string();
    let start_time = Utc::now();

    let result = match execute_tool(&server, tool_name, &params).await {
        Ok(value) => {
            let _duration = (Utc::now() - start_time).num_milliseconds();
            log_audit(
                &request_id,
                tool_name,
                client_id,
                &params,
                "success",
                _duration,
            );
            value
        }
        Err(e) => {
            let _duration = (Utc::now() - start_time).num_milliseconds();
            log_audit(
                &request_id,
                tool_name,
                client_id,
                &params,
                "error",
                _duration,
            );
            return (
                e.to_http_status(),
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": e.to_json_rpc_error(),
                    "id": id
                })),
            );
        }
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "jsonrpc": "2.0",
            "result": result,
            "id": id
        })),
    )
}

/// Execute a tool by name. Dispatches to the tool's registered live
/// handler when one exists; tools registered without an executor fall
/// back to the legacy echo contract (params round-trip) so external
/// integrators keep their existing behaviour.
async fn execute_tool(
    server: &McpServer,
    tool_name: &str,
    params: &Value,
) -> Result<Value, McpError> {
    if !server.get_tools().iter().any(|t| t.name == tool_name) {
        return Err(McpError::MethodNotFound(format!(
            "Tool not found: {}",
            tool_name
        )));
    }

    let mut tool_params = params.clone();
    if let Some(obj) = tool_params.as_object_mut() {
        obj.remove("name");
        obj.remove("client_id");
    }

    if let Some(handler) = server.get_handler(tool_name) {
        return handler(tool_params).await;
    }

    Ok(serde_json::json!({
        "status": "success",
        "tool": tool_name,
        "params": tool_params
    }))
}

/// Log audit entry
fn log_audit(
    request_id: &str,
    tool_name: &str,
    client_id: &str,
    params: &Value,
    result: &str,
    _duration_ms: i64,
) {
    let entry = AuditLogEntry {
        timestamp: Utc::now().to_rfc3339(),
        request_id: request_id.to_string(),
        tool_name: tool_name.to_string(),
        client_id: client_id.to_string(),
        params: sanitize_params(params),
        result: result.to_string(),
    };

    if let Ok(line) = serde_json::to_string(&entry) {
        info!(audit_log = line);
    }
}

#[cfg(test)]
mod tests {
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
}
