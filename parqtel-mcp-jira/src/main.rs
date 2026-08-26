//! Jira MCP server main entry point

use std::env;

use axum::{routing::get, Router};
use parqtel_mcp_core::{server::ServerConfig, McpServer};
use parqtel_mcp_jira::{
    make_add_rca_comment_tool, make_create_action_item_tool, make_create_incident_issue_tool,
    make_transition_issue_tool,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let host = env::var("MCP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = env::var("MCP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);
    let rate_limit: u32 = env::var("MCP_RATE_LIMIT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(60);

    let config = ServerConfig {
        host,
        port,
        rate_limit_requests_per_minute: rate_limit,
    };

    let addr = format!("{}:{}", config.host, config.port);
    let mut server = McpServer::new(config);

    server.register_tool(make_create_incident_issue_tool());
    server.register_tool(make_add_rca_comment_tool());
    server.register_tool(make_create_action_item_tool());
    server.register_tool(make_transition_issue_tool());

    let app = Router::new()
        .merge(server.build_router())
        .route("/health", get(health_handler));

    tracing::info!("Starting MCP server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_handler() -> axum::response::Json<serde_json::Value> {
    axum::response::Json(serde_json::json!({
        "status": "ok",
        "tools": 4,
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}
