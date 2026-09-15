//! Parqtel self-MCP server main entry point

use std::env;

use parqtel_mcp_core::{server::ServerConfig, McpServer};
use parqtel_mcp_parqtel::handlers::{
    make_get_alert_history_handler, make_get_noise_statistics_handler, make_get_topology_handler,
    make_ingest_rates_handler, make_query_logs_handler, make_query_metrics_handler,
    make_query_metrics_labels_handler,
};
use parqtel_mcp_parqtel::{
    make_get_alert_history_tool, make_get_noise_statistics_tool, make_get_topology_tool,
    make_ingest_rates_tool, make_query_logs_tool, make_query_metrics_labels_tool,
    make_query_metrics_tool, ParqtelClient,
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

    // One shared client for every tool handler; fails fast if the
    // PARQTEL_API_URL is malformed so misconfiguration surfaces at boot.
    let client = ParqtelClient::from_env()?;
    tracing::info!(
        parqtel_api = client.base_url(),
        "MCP tools wired to Parqtel"
    );

    let mut server = McpServer::new(config);
    server.register_tool_with_handler(
        make_query_metrics_tool(),
        make_query_metrics_handler(client.clone()),
    );
    server.register_tool_with_handler(
        make_query_metrics_labels_tool(),
        make_query_metrics_labels_handler(client.clone()),
    );
    server.register_tool_with_handler(
        make_query_logs_tool(),
        make_query_logs_handler(client.clone()),
    );
    server.register_tool_with_handler(
        make_ingest_rates_tool(),
        make_ingest_rates_handler(client.clone()),
    );
    server.register_tool_with_handler(
        make_get_alert_history_tool(),
        make_get_alert_history_handler(client.clone()),
    );
    server.register_tool_with_handler(
        make_get_noise_statistics_tool(),
        make_get_noise_statistics_handler(client.clone()),
    );
    server.register_tool_with_handler(make_get_topology_tool(), make_get_topology_handler(client));
    let tool_count = server.get_tools().len();

    // `/health` is owned by the framework router (it reports this tool
    // count); adding another route here panics at boot with
    // "Overlapping method route".
    let app = server.build_router();

    tracing::info!(
        "Starting Parqtel self-MCP server on {} ({} tools)",
        addr,
        tool_count
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
