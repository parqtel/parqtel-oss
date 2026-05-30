use serde::{Deserialize, Serialize};

/// Configuration for the HTTP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// TCP address to bind to (e.g. "0.0.0.0:8080").
    pub bind_address: String,
    /// Maximum simultaneous TCP connections.
    pub max_connections: usize,
    /// Seconds to wait for in-flight requests during shutdown.
    pub shutdown_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".into(),
            max_connections: 1024,
            shutdown_timeout_secs: 30,
        }
    }
}
