use serde::{Deserialize, Serialize};

/// Configuration for the query engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum estimated bytes held by the process-wide block footer cache.
    #[serde(
        default = "default_footer_cache_bytes",
        deserialize_with = "deserialize_nonzero_footer_cache_bytes"
    )]
    pub footer_cache_bytes: usize,
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

impl QueryConfig {
    pub const DEFAULT_FOOTER_CACHE_BYTES: usize = 192 * 1024 * 1024;
}

fn default_footer_cache_bytes() -> usize {
    QueryConfig::DEFAULT_FOOTER_CACHE_BYTES
}

fn deserialize_nonzero_footer_cache_bytes<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bytes = usize::deserialize(deserializer)?;
    if bytes == 0 {
        return Err(serde::de::Error::custom("must be greater than 0"));
    }
    Ok(bytes)
}

fn default_lookback_delta_ns() -> i64 {
    5 * 60 * 1_000_000_000
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            footer_cache_bytes: default_footer_cache_bytes(),
            max_series: 1000,
            max_samples_per_series: 10000,
            timeout_secs: 30,
            lookback_delta_ns: default_lookback_delta_ns(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn footer_cache_budget_defaults_to_192_mib() {
        assert_eq!(QueryConfig::default().footer_cache_bytes, 192 * 1024 * 1024);
        let config: QueryConfig = serde_json::from_str(
            r#"{"max_series":1000,"max_samples_per_series":10000,"timeout_secs":30}"#,
        )
        .unwrap();
        assert_eq!(config.footer_cache_bytes, 192 * 1024 * 1024);
    }

    #[test]
    fn footer_cache_budget_rejects_zero() {
        let result = serde_json::from_str::<QueryConfig>(
            r#"{"footer_cache_bytes":0,"max_series":1000,"max_samples_per_series":10000,"timeout_secs":30}"#,
        );
        assert!(result.is_err());
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
