use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for data ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Maximum size of an incoming OTLP batch in bytes.
    pub max_body_size: usize,
    /// Whether to enable the Write-Ahead Log (WAL) for metrics.
    pub wal_enabled: bool,
    /// Whether to enable the Write-Ahead Log (WAL) for logs.
    pub log_wal_enabled: bool,
    /// Tail-sampling policy for traces (keep-all by default).
    #[serde(default)]
    pub tail_sampling: TailSamplingConfig,
}

/// Tail-sampling policy for traces: decide per trace (after all spans of a
/// batch arrive) which traces to persist, controlling storage volume while
/// keeping the metrics derived by the span-metrics RED bridge unsampled.
///
/// A trace is kept if ANY rule votes keep; rules are evaluated in the
/// order below. Probabilistic decisions hash the trace_id so an entire
/// trace lives or dies together (no orphaned fragments).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TailSamplingConfig {
    /// Keep every trace that contains an ERROR-status span.
    #[serde(default = "default_true")]
    pub keep_errors: bool,
    /// Keep traces whose root (server) span exceeds this duration in
    /// milliseconds. `None` disables the rule.
    pub slow_trace_ms: Option<u64>,
    /// Keep this fraction of the remaining traces (0.0–1.0).
    /// Defaults to 1.0 (keep all). Uses deterministic trace_id hashing.
    #[serde(default = "default_sampling_ratio")]
    pub sampling_ratio: f64,
    /// Per-service overrides applied after the global rules; an entry for
    /// a service replaces the global policy for its traces entirely.
    #[serde(default)]
    pub per_service: HashMap<String, TailSamplingConfig>,
}

impl Default for TailSamplingConfig {
    fn default() -> Self {
        Self {
            keep_errors: true,
            slow_trace_ms: None,
            sampling_ratio: 1.0,
            per_service: HashMap::new(),
        }
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_body_size: 10 * 1024 * 1024,
            wal_enabled: false,
            log_wal_enabled: true,
            tail_sampling: TailSamplingConfig::default(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_sampling_ratio() -> f64 {
    1.0
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn default_keeps_everything() {
        let cfg = IngestConfig::default();
        assert!(cfg.tail_sampling.keep_errors);
        assert_eq!(cfg.tail_sampling.sampling_ratio, 1.0);
        assert!(cfg.tail_sampling.slow_trace_ms.is_none());
        assert!(cfg.tail_sampling.per_service.is_empty());
    }
}
