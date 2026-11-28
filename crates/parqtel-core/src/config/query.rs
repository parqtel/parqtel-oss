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
    /// Maximum number of blocks one query scans. Blocks past this limit are
    /// dropped from the result and reported as a truncation warning.
    #[serde(
        default = "default_max_scan_blocks",
        deserialize_with = "deserialize_nonzero_max_scan_blocks"
    )]
    pub max_scan_blocks: usize,
    /// Maximum number of blocks scanned concurrently by queries.
    #[serde(
        default = "default_max_concurrent_scans",
        deserialize_with = "deserialize_nonzero_max_concurrent_scans"
    )]
    pub max_concurrent_scans: usize,
    /// Scan slots reserved for background compaction, separate from the query
    /// pool so neither workload starves the other.
    #[serde(
        default = "default_compaction_scan_slots",
        deserialize_with = "deserialize_nonzero_compaction_scan_slots"
    )]
    pub compaction_scan_slots: usize,
    /// Writer base URLs a reader queries for unflushed rows, e.g.
    /// `["http://parqtel-writer-0:8080"]`. Empty means no fan-out: a reader
    /// answers from block storage only.
    #[serde(default)]
    pub writer_endpoints: Vec<String>,
    /// Bearer token the writers require on `/internal/v1/buffer`. Both sides
    /// must agree; an unset token disables the endpoint entirely.
    #[serde(default)]
    pub internal_auth_token: Option<String>,
    /// Per-writer timeout for a buffer query, in milliseconds.
    #[serde(default = "default_writer_fanout_timeout_ms")]
    pub writer_fanout_timeout_ms: u64,
}

impl QueryConfig {
    pub const DEFAULT_FOOTER_CACHE_BYTES: usize = 192 * 1024 * 1024;
    /// Blocks one query may scan before truncating.
    pub const DEFAULT_MAX_SCAN_BLOCKS: usize = 1024;
    /// Compaction's own scan budget.
    pub const DEFAULT_COMPACTION_SCAN_SLOTS: usize = 2;

    /// Query scan concurrency: one slot per available core, capped at 8 so a
    /// large host does not oversubscribe the blocking pool.
    pub fn default_max_concurrent_scans() -> usize {
        std::thread::available_parallelism()
            .map(|cpus| cpus.get())
            .unwrap_or(4)
            .clamp(1, 8)
    }
}

fn default_max_scan_blocks() -> usize {
    QueryConfig::DEFAULT_MAX_SCAN_BLOCKS
}

fn default_max_concurrent_scans() -> usize {
    QueryConfig::default_max_concurrent_scans()
}

fn default_compaction_scan_slots() -> usize {
    QueryConfig::DEFAULT_COMPACTION_SCAN_SLOTS
}

fn deserialize_nonzero_max_scan_blocks<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let blocks = usize::deserialize(deserializer)?;
    if blocks == 0 {
        return Err(serde::de::Error::custom(
            "max_scan_blocks must be greater than 0",
        ));
    }
    Ok(blocks)
}

fn deserialize_nonzero_max_concurrent_scans<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let slots = usize::deserialize(deserializer)?;
    if slots == 0 {
        return Err(serde::de::Error::custom(
            "max_concurrent_scans must be greater than 0",
        ));
    }
    Ok(slots)
}

fn deserialize_nonzero_compaction_scan_slots<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let slots = usize::deserialize(deserializer)?;
    if slots == 0 {
        return Err(serde::de::Error::custom(
            "compaction_scan_slots must be greater than 0",
        ));
    }
    Ok(slots)
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

fn default_writer_fanout_timeout_ms() -> u64 {
    2000
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
            writer_endpoints: Vec::new(),
            internal_auth_token: None,
            writer_fanout_timeout_ms: default_writer_fanout_timeout_ms(),
            max_scan_blocks: default_max_scan_blocks(),
            max_concurrent_scans: default_max_concurrent_scans(),
            compaction_scan_slots: default_compaction_scan_slots(),
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

    #[test]
    fn scan_limits_default_when_absent() {
        let config = QueryConfig::default();
        assert_eq!(config.max_scan_blocks, 1024);
        assert_eq!(config.compaction_scan_slots, 2);
        let expected = std::thread::available_parallelism()
            .map(|cpus| cpus.get())
            .unwrap_or(4)
            .clamp(1, 8);
        assert_eq!(config.max_concurrent_scans, expected);

        // Old JSON without the new keys still parses to the defaults.
        let parsed: QueryConfig = serde_json::from_str(
            r#"{"max_series":1000,"max_samples_per_series":10000,"timeout_secs":30}"#,
        )
        .unwrap();
        assert_eq!(parsed.max_scan_blocks, 1024);
        assert_eq!(parsed.compaction_scan_slots, 2);
        assert_eq!(parsed.max_concurrent_scans, expected);
    }

    #[test]
    fn scan_limits_reject_zero() {
        for body in [
            r#"{"max_scan_blocks":0}"#,
            r#"{"max_concurrent_scans":0}"#,
            r#"{"compaction_scan_slots":0}"#,
        ] {
            let result = serde_json::from_str::<QueryConfig>(body);
            assert!(result.is_err(), "{body} must be rejected");
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
