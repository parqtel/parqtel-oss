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
    tool::{sanitize_params, McpTool},
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

/// Rate limiter using token bucket algorithm
struct RateLimiter {
    requests_per_minute: u32,
    tokens: HashMap<String, f64>,
    last_update: HashMap<String, u64>,
}

impl RateLimiter {
    fn new(requests_per_minute: u32) -> Self {
        Self {
            requests_per_minute,
            tokens: HashMap::new(),
            last_update: HashMap::new(),
        }
    }

    fn allow(&mut self, client_id: &str) -> bool {
        let now = Utc::now().timestamp() as u64;
        let tokens_per_second = self.requests_per_minute as f64 / 60.0;

        let entry = self.tokens.entry(client_id.to_string()).or_insert(tokens_per_second);
        let last = self.last_update.entry(client_id.to_string()).or_insert(now);

        let elapsed = (now - *last) as f64;
        *entry = (*entry + elapsed * tokens_per_second).min(tokens_per_second * 60.0);
        *last = now;

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
    rate_limiter: Arc<std::sync::Mutex<RateLimiter>>,
}

impl McpServer {
    /// Create a new MCP server
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config: config.clone(),
            tools: Vec::new(),
            rate_limiter: Arc::new(std::sync::Mutex::new(RateLimiter::new(
                config.rate_limit_requests_per_minute,
            ))),
        }
    }

    /// Register a tool with the server
    pub fn register_tool(&mut self, tool: McpTool) {
        self.tools.push(tool);
    }

    /// Get the list of registered tools
    pub fn get_tools(&self) -> &[McpTool] {
        &self.tools
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
async fn health_handler() -> Json<Value> {
    Json(serde_json::json!({
        "status": "ok",
        "timestamp": Utc::now().to_rfc3339()
    }))
}

/// Tools list handler
async fn tools_list_handler(
    State(server): State<Arc<McpServer>>,
) -> Json<Value> {
    let tools: Vec<Value> = server
        .get_tools()
        .iter()
        .map(|t| serde_json::json!({
            "name": t.name,
            "description": t.description,
            "input_schema": t.input_schema
        }))
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
    let id = request.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");
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
                }))
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
                }))
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
                }))
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
                }))
            );
        }
    };

    let client_id = params.get("client_id").and_then(|v| v.as_str()).unwrap_or("anonymous");
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
                }))
            );
        }
    }

    let request_id = Uuid::new_v4().to_string();
    let start_time = Utc::now();

    let result = match execute_tool(server.get_tools(), tool_name, &params) {
        Ok(value) => {
            let _duration = (Utc::now() - start_time).num_milliseconds();
            log_audit(&request_id, tool_name, client_id, &params, "success", _duration);
            value
        }
        Err(e) => {
            let _duration = (Utc::now() - start_time).num_milliseconds();
            log_audit(&request_id, tool_name, client_id, &params, "error", _duration);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": e.to_json_rpc_error(),
                    "id": id
                }))
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

/// Execute a tool by name
fn execute_tool(
    tools: &[McpTool],
    tool_name: &str,
    params: &Value,
) -> Result<Value, McpError> {
    let _tool = tools.iter().find(|t| t.name == tool_name).ok_or_else(|| {
        McpError::MethodNotFound(format!("Tool not found: {}", tool_name))
    })?;

    let mut tool_params = params.clone();
    if let Some(obj) = tool_params.as_object_mut() {
        obj.remove("name");
        obj.remove("client_id");
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
