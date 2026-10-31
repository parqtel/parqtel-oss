use serde::{Deserialize, Serialize};

/// Configuration for the query engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum number of series returned by one query.
    pub max_series: usize,
    /// Maximum number of samples per series.
    pub max_samples_per_series: usize,
    /// Default query timeout in seconds.
    pub timeout_secs: u64,
    /// Instant-query lookback window in nanoseconds: the newest sample
    /// within `[time - lookback, time]` is used for instant selectors.
    /// Prometheus semantics default: 5 minutes.
    #[serde(default = "default_lookback_delta_ns")]
    pub lookback_delta_ns: i64,
}

fn default_lookback_delta_ns() -> i64 {
    5 * 60 * 1_000_000_000
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_series: 1000,
            max_samples_per_series: 10000,
            timeout_secs: 30,
            lookback_delta_ns: default_lookback_delta_ns(),
        }
    }
}

/// Configuration for the embedded UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UIConfig {
    /// Whether to serve the embedded dashboard at /ui.
    pub enabled: bool,
}

impl Default for UIConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}
