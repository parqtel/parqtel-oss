use serde::{Deserialize, Serialize};

/// Configuration for the HTTP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// TCP address to bind to (e.g. "0.0.0.0:8080").
    pub bind_address: String,
    /// TCP address for the OTLP gRPC server (e.g. "0.0.0.0:4317").
    /// Set to an empty string to disable gRPC ingestion.
    #[serde(default = "default_grpc_bind_address")]
    pub grpc_bind_address: String,
    /// Maximum simultaneous TCP connections.
    pub max_connections: usize,
    /// Seconds to wait for in-flight requests during shutdown.
    pub shutdown_timeout_secs: u64,
    /// Seconds between background flush checks. Each tick asks each rotator
    /// whether its block duration has elapsed, so this bounds how late a
    /// duration-triggered flush can be.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// Seconds between alert-rule evaluation cycles.
    #[serde(default = "default_alert_interval_secs")]
    pub alert_interval_secs: u64,
    /// Seconds between retention sweeps (time-based expiry only).
    #[serde(default = "default_retention_interval_secs")]
    pub retention_interval_secs: u64,
    /// Maximum concurrent in-flight OTLP export requests per gRPC
    /// connection. Without it a single client can monopolise the shared
    /// ingest mutex that HTTP exporters also queue on.
    #[serde(default = "default_grpc_concurrency_limit")]
    pub grpc_concurrency_limit: usize,
    /// Seconds between block-index sidecar writes. Mutations mark the index
    /// dirty; this bounds how long a block can sit in memory before the
    /// sidecar catches up, and amortises many flushes into one write.
    /// Lower it if you restart often and want the index written sooner; raise
    /// it to reduce filesystem writes on a write-heavy ingest.
    #[serde(default = "default_index_persist_interval_secs")]
    pub index_persist_interval_secs: u64,
}

fn default_grpc_bind_address() -> String {
    "0.0.0.0:4317".into()
}

fn default_flush_interval_secs() -> u64 {
    5
}

fn default_alert_interval_secs() -> u64 {
    15
}

fn default_retention_interval_secs() -> u64 {
    3600
}

fn default_grpc_concurrency_limit() -> usize {
    64
}

fn default_index_persist_interval_secs() -> u64 {
    2
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".into(),
            grpc_bind_address: default_grpc_bind_address(),
            max_connections: 1024,
            shutdown_timeout_secs: 30,
            flush_interval_secs: default_flush_interval_secs(),
            alert_interval_secs: default_alert_interval_secs(),
            retention_interval_secs: default_retention_interval_secs(),
            grpc_concurrency_limit: default_grpc_concurrency_limit(),
            index_persist_interval_secs: default_index_persist_interval_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn defaults_preserve_previous_hardcoded_intervals() {
        // These were literals in main.rs and retention.rs; the defaults must
        // reproduce them exactly so upgrading is a no-op.
        let c = ServerConfig::default();
        assert_eq!(c.flush_interval_secs, 5);
        assert_eq!(c.alert_interval_secs, 15);
        assert_eq!(c.retention_interval_secs, 3600);
        assert_eq!(c.grpc_concurrency_limit, 64);
        assert_eq!(c.index_persist_interval_secs, 2);
    }

    #[test]
    fn serde_round_trips_new_knobs() {
        let c = ServerConfig {
            flush_interval_secs: 30,
            alert_interval_secs: 60,
            retention_interval_secs: 900,
            grpc_concurrency_limit: 8,
            index_persist_interval_secs: 30,
            ..Default::default()
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: ServerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.flush_interval_secs, 30);
        assert_eq!(back.alert_interval_secs, 60);
        assert_eq!(back.retention_interval_secs, 900);
        assert_eq!(back.grpc_concurrency_limit, 8);
        assert_eq!(back.index_persist_interval_secs, 30);
    }

    #[test]
    fn missing_new_knobs_fall_back_to_defaults() {
        // A config file written by an older build must still load.
        let json =
            r#"{"bind_address":"0.0.0.0:8080","max_connections":1024,"shutdown_timeout_secs":30}"#;
        let c: ServerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.flush_interval_secs, 5);
        assert_eq!(c.alert_interval_secs, 15);
        assert_eq!(c.retention_interval_secs, 3600);
        assert_eq!(c.grpc_concurrency_limit, 64);
        assert_eq!(c.index_persist_interval_secs, 2);
    }
}
