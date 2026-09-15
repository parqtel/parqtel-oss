//! PagerDuty MCP server main entry point

use std::env;

use parqtel_mcp_core::{server::ServerConfig, McpServer};
use parqtel_mcp_pagerduty::{
    make_add_note_tool, make_create_incident_tool, make_get_oncall_tool,
    make_get_recent_incidents_tool, make_resolve_incident_tool,
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

    server.register_tool(make_create_incident_tool());
    server.register_tool(make_add_note_tool());
    server.register_tool(make_resolve_incident_tool());
    server.register_tool(make_get_oncall_tool());
    server.register_tool(make_get_recent_incidents_tool());

    let tool_count = server.get_tools().len();

    // `/health` is owned by the framework router (it reports this count);
    // registering a second `/health` here panics at boot with
    // "Overlapping method route".
    let app = server.build_router();

    tracing::info!("Starting MCP server on {} ({} tools)", addr, tool_count);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
