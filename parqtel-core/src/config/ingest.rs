use serde::{Deserialize, Serialize};

/// Configuration for data ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Maximum size of an incoming OTLP batch in bytes.
    pub max_body_size: usize,
    /// Whether to enable the Write-Ahead Log (WAL) for metrics.
    pub wal_enabled: bool,
    /// Whether to enable the Write-Ahead Log (WAL) for logs.
    pub log_wal_enabled: bool,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_body_size: 10 * 1024 * 1024,
            wal_enabled: false,
            log_wal_enabled: true,
        }
    }
}
