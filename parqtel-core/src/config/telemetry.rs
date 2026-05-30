use serde::{Deserialize, Serialize};

/// Configuration for logging and metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Log level (trace, debug, info, warn, error).
    pub log_level: String,
    /// Log format (text or json).
    pub log_format: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            log_format: "text".into(),
        }
    }
}

/// Configuration for the Kubernetes Custom Metrics Provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sProviderConfig {
    /// Whether the provider is enabled.
    pub enabled: bool,
    /// Address to bind the provider HTTPS listener.
    pub bind_address: String,
    /// Lookback window for metric queries (seconds).
    pub cache_expiry_secs: u64,
    /// Timeout for individual queries to parqtel storage (seconds).
    pub query_timeout_secs: u64,
    /// Maximum number of concurrent queries.
    pub max_concurrent: usize,
    /// Name of the Secret to store/load provider TLS certificates.
    pub tls_secret_name: String,
}

impl Default for K8sProviderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:6443".into(),
            cache_expiry_secs: 30,
            query_timeout_secs: 10,
            max_concurrent: 10,
            tls_secret_name: "parqtel-provider-tls".into(),
        }
    }
}
