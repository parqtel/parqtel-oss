//! Server implementation for MCP
//!
//! Transports
//! - `POST /mcp` — MCP Streamable HTTP endpoint (spec 2025-06-18).
//!   Single JSON-RPC 2.0 message per request; plain `application/json`
//!   responses (SSE streaming only when the client asks for
//!   `text/event-stream`). Handles `initialize`,
//!   `notifications/initialized`, `ping`, `tools/list`, `tools/call`.
//! - `GET /mcp` — SSE keepalive stream (servers SHOULD support it).
//! - `DELETE /mcp` — session close acknowledgement.
//! - `GET /health` — liveness probe (non-MCP, reports tool count).
//! - `GET /tools/list`, `POST /tools/call` — deprecated legacy aliases.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{sse::Event, IntoResponse, Json, Response, Sse},
    routing::{get, post},
    Router,
};
use chrono::Utc;
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;
use uuid::Uuid;

use crate::{
    error::McpError,
    tool::{sanitize_params, McpTool, ToolHandler},
};

/// Latest protocol version this server speaks. Older clients keep working:
/// `initialize` succeeds for any `YYYY-MM-DD` version and the response
/// echoes back what the client asked for, per spec.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
/// Fallback when the client sends no `protocolVersion` at all.
const MCP_DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";
/// `Mcp-Session-Id` header name (spec, case-insensitive via `HeaderMap`).
const HEADER_SESSION_ID: &str = "mcp-session-id";
/// `MCP-Protocol-Version` header name (spec).
const HEADER_PROTOCOL_VERSION: &str = "mcp-protocol-version";

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
#[derive(Clone)]
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

    /// Build the Axum router.
    ///
    /// Routes: `POST/GET/DELETE /mcp` (spec Streamable HTTP), `GET /health`
    /// (liveness probe), plus deprecated `GET /tools/list` and
    /// `POST /tools/call` legacy aliases.
    pub fn build_router(self) -> Router {
        Router::new()
            .route("/health", get(health_handler))
            .route(
                "/mcp",
                post(mcp_post_handler)
                    .get(mcp_get_handler)
                    .delete(mcp_delete_handler),
            )
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

/// Tools list handler (deprecated legacy alias).
/// New clients must use `POST /mcp` with `tools/list`. Emits both
/// `inputSchema` (spec) and `input_schema` (legacy).
async fn tools_list_handler(State(server): State<Arc<McpServer>>) -> Json<Value> {
    Json(serde_json::json!({
        "tools": mcp_tools_json(&server)
    }))
}

/// Canonical tool list in spec shape (`inputSchema`, camelCase).
fn mcp_tools_json(server: &McpServer) -> Vec<Value> {
    server
        .get_tools()
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema,
                "input_schema": t.input_schema
            })
        })
        .collect()
}

fn rpc_id(request: &Value) -> Value {
    request.get("id").cloned().unwrap_or(Value::Null)
}

fn rpc_ok(id: Value, result: Value) -> Value {
    serde_json::json!({ "jsonrpc": "2.0", "result": result, "id": id })
}

fn rpc_error(id: Value, code: i64, message: String) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": message },
        "id": id
    })
}

fn protocol_version(request: &Value, headers: &HeaderMap) -> String {
    if let Some(v) = headers
        .get(HEADER_PROTOCOL_VERSION)
        .and_then(|h| h.to_str().ok())
    {
        return v.to_string();
    }
    request
        .get("params")
        .and_then(|p| p.get("protocolVersion"))
        .and_then(|v| v.as_str())
        .unwrap_or(MCP_DEFAULT_PROTOCOL_VERSION)
        .to_string()
}

/// Client prefers SSE when `Accept` mentions `text/event-stream` without
/// also accepting plain JSON.
fn wants_sse(headers: &HeaderMap) -> bool {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let accept = accept.to_ascii_lowercase();
    accept.contains("text/event-stream") && !accept.contains("application/json")
}

fn with_session_header(
    mut response: Response,
    session_id: Option<&str>,
    protocol_version: &str,
) -> Response {
    if let Some(sid) = session_id {
        if let Ok(v) = sid.parse() {
            response.headers_mut().insert(HEADER_SESSION_ID, v);
        }
    }
    if let Ok(v) = protocol_version.parse() {
        response.headers_mut().insert(HEADER_PROTOCOL_VERSION, v);
    }
    response
}

/// `POST /mcp` — Streamable HTTP endpoint (spec 2025-06-18).
/// One JSON-RPC message per request; plain `application/json` unless the
/// client prefers `text/event-stream` (then one SSE `message` event).
/// Stateless: no session required; one is minted on `initialize`.
async fn mcp_post_handler(
    State(server): State<Arc<McpServer>>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Response {
    let id = rpc_id(&request);
    let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let proto = protocol_version(&request, &headers);
    let mut session_id: Option<String> = headers
        .get(HEADER_SESSION_ID)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string);
    if method == "initialize" && session_id.is_none() {
        session_id = Some(Uuid::new_v4().to_string());
    }
    let payload = match method {
        "initialize" => rpc_ok(id, initialize_result(&request)),
        "notifications/initialized" | "notifications/cancelled" => {
            let resp = StatusCode::ACCEPTED.into_response();
            return with_session_header(resp, session_id.as_deref(), &proto);
        }
        "ping" => rpc_ok(id, serde_json::json!({})),
        "tools/list" => rpc_ok(id, serde_json::json!({ "tools": mcp_tools_json(&server) })),
        "tools/call" => match handle_spec_tools_call(&server, &request).await {
            Ok(result) => rpc_ok(id, result),
            Err(err) => {
                let body = rpc_error(id, err.to_code(), err.to_string());
                let resp = (err.to_http_status(), Json(body)).into_response();
                return with_session_header(resp, session_id.as_deref(), &proto);
            }
        },
        "" => {
            let body = rpc_error(
                id,
                crate::error::ERROR_INVALID_REQUEST,
                "Missing 'method' field".to_string(),
            );
            let resp = (StatusCode::BAD_REQUEST, Json(body)).into_response();
            return with_session_header(resp, session_id.as_deref(), &proto);
        }
        other => {
            let body = rpc_error(
                id,
                crate::error::ERROR_METHOD_NOT_FOUND,
                format!("Method not found: {other}"),
            );
            let resp = Json(body).into_response();
            return with_session_header(resp, session_id.as_deref(), &proto);
        }
    };
    if wants_sse(&headers) {
        let event = Event::default()
            .event("message")
            .json_data(&payload)
            .unwrap_or_else(|_| Event::default().data("{}"));
        let stream = stream::iter(vec![Ok::<_, axum::Error>(event)]);
        let resp = Sse::new(stream)
            .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response();
        return with_session_header(resp, session_id.as_deref(), &proto);
    }
    with_session_header(Json(payload).into_response(), session_id.as_deref(), &proto)
}

/// `GET /mcp` — SSE keepalive (spec SHOULD). Stateless: empty stream.
async fn mcp_get_handler(headers: HeaderMap) -> Response {
    let accepts_sse = headers
        .get(header::ACCEPT)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase()
        .contains("text/event-stream");
    if accepts_sse {
        let stream = stream::iter(Vec::<Result<Event, axum::Error>>::new());
        return Sse::new(stream).into_response();
    }
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(rpc_error(
            Value::Null,
            crate::error::ERROR_INVALID_REQUEST,
            "Use POST /mcp for JSON-RPC requests".to_string(),
        )),
    )
        .into_response()
}

/// `DELETE /mcp` — close session. Stateless: always acknowledge.
async fn mcp_delete_handler() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

fn initialize_result(request: &Value) -> Value {
    let requested = request
        .get("params")
        .and_then(|p| p.get("protocolVersion"))
        .and_then(|v| v.as_str())
        .unwrap_or(MCP_PROTOCOL_VERSION);
    serde_json::json!({
        "protocolVersion": requested,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": "parqtel-mcp", "version": env!("CARGO_PKG_VERSION") }
    })
}

/// Tools call handler (deprecated legacy alias).
async fn tools_call_handler(
    State(server): State<Arc<McpServer>>,
    Json(request): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let id = rpc_id(&request);
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

/// Wrap a raw tool payload in the MCP `CallToolResult` envelope.
fn spec_tool_result(payload: Value, is_error: bool) -> Value {
    let text = match &payload {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string()),
    };
    serde_json::json!({
        "content": [{ "type": "text", "text": text }],
        "structuredContent": payload,
        "isError": is_error
    })
}

/// Spec `tools/call`: `params = { name, arguments }` (+ legacy flat fallback).
/// Tool failures ride inside `isError: true` (HTTP 200); routing errors stay
/// JSON-RPC errors.
async fn handle_spec_tools_call(
    server: &Arc<McpServer>,
    request: &Value,
) -> Result<Value, McpError> {
    let params = request.get("params").cloned().unwrap_or(Value::Null);
    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::InvalidRequest("Missing 'name' in params".into()))?;
    if !server.get_tools().iter().any(|t| t.name == tool_name) {
        return Err(McpError::MethodNotFound(format!(
            "Tool not found: {tool_name}"
        )));
    }
    let client_id = params
        .get("client_id")
        .and_then(|v| v.as_str())
        .unwrap_or("anonymous");
    {
        let mut limiter = server
            .rate_limiter
            .lock()
            .map_err(|_| McpError::InternalError("rate limiter poisoned".into()))?;
        if !limiter.allow(client_id) {
            return Err(McpError::ApplicationError("Rate limit exceeded".into()));
        }
    }
    let mut tool_params = match params.get("arguments") {
        Some(Value::Object(_)) => params.get("arguments").cloned().unwrap_or(Value::Null),
        Some(Value::Null) | None => {
            let mut pp = params.clone();
            if let Some(obj) = pp.as_object_mut() {
                obj.remove("name");
                obj.remove("arguments");
                obj.remove("client_id");
            }
            pp
        }
        Some(_) => {
            return Err(McpError::InvalidRequest(
                "'arguments' must be an object".into(),
            ))
        }
    };
    if !tool_params.is_object() {
        tool_params = serde_json::json!({});
    }
    let request_id = Uuid::new_v4().to_string();
    let start_time = Utc::now();
    match execute_tool(server, tool_name, &tool_params).await {
        Ok(value) => {
            log_audit(
                &request_id,
                tool_name,
                client_id,
                &tool_params,
                "success",
                (Utc::now() - start_time).num_milliseconds(),
            );
            Ok(spec_tool_result(value, false))
        }
        Err(e) => {
            log_audit(
                &request_id,
                tool_name,
                client_id,
                &tool_params,
                "error",
                (Utc::now() - start_time).num_milliseconds(),
            );
            match &e {
                McpError::ApplicationError(_)
                | McpError::InternalError(_)
                | McpError::HttpError(_)
                | McpError::SerializationError(_)
                | McpError::IoError(_) => Ok(spec_tool_result(
                    serde_json::json!({ "error": e.to_string() }),
                    true,
                )),
                _ => Err(e),
            }
        }
    }
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
            Box::pin(async move {
                Ok(json!({"echo": params.get("query").cloned().unwrap_or(Value::Null)}))
            })
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
        let failing: ToolHandler = Arc::new(|_p: Value| {
            Box::pin(async { Err(McpError::ApplicationError("boom".into())) })
        });
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
}
