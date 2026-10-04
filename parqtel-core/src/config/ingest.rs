use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for data ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Maximum size of an incoming OTLP batch in bytes.
    pub max_body_size: usize,
    /// Whether to enable the Write-Ahead Log (WAL) for metrics.
    ///
    /// The WAL bounds crash loss to the sync interval instead of the whole
    /// block window — with the 2 h default that is the difference between
    /// losing two hours of the data you wanted during an incident and losing
    /// seconds.
    pub wal_enabled: bool,
    /// Whether to enable the Write-Ahead Log (WAL) for logs.
    pub log_wal_enabled: bool,
    /// How durably a WAL append is persisted. See [`crate::wal::WalSyncMode`].
    ///
    /// `interval` is the intended default: `write` plus a periodic `fsync`, so
    /// crash loss is bounded by `wal_sync_interval_ms` rather than costing an
    /// fsync per batch.
    #[serde(default)]
    pub wal_sync_mode: crate::wal::WalSyncMode,
    /// How often the WAL is `fsync`ed, in `interval` mode.
    #[serde(default = "default_wal_sync_interval_ms")]
    pub wal_sync_interval_ms: u64,
    /// Maximum number of block encodes that may be in flight at once.
    ///
    /// A request that crosses the block cap is acknowledged once the WAL has its
    /// rows rather than after the Parquet encode, because the rows are
    /// recoverable either way. Measured on a realistic 7 MiB batch, the encode is
    /// ~125 ms of a ~300 ms request, and it serialises concurrent requests
    /// behind the flush lock.
    ///
    /// This bounds how many encodes overlap. When the queue is full a flush
    /// encodes inline instead, so backpressure degrades to the previous
    /// synchronous behaviour rather than growing an unbounded backlog.
    #[serde(default = "default_max_inflight_flushes")]
    pub max_inflight_flushes: usize,
    /// Size at which the WAL rolls to a new segment, in bytes.
    ///
    /// A segment is deleted once the commit point passes it, so the on-disk
    /// working set is bounded by roughly this size plus whatever has not been
    /// flushed yet.
    #[serde(default = "default_wal_max_segment_bytes")]
    pub wal_max_segment_bytes: u64,
    /// Tail-sampling policy for traces (keep-all by default).
    #[serde(default)]
    pub tail_sampling: TailSamplingConfig,
    /// Number of writer shards used by the metrics ingest rotator.
    ///
    /// Every ingest request for a signal serialises on one lock, and a block
    /// flush runs while that lock is held — so one flush stalls every metric
    /// request for its encode duration. Sharding lets unrelated metrics be
    /// ingested concurrently while a flush is in flight.
    ///
    /// The shards are **not** independent blocks: a flush merges every shard
    /// into one file, so the block count and query fan-out are unchanged.
    /// Durability is unchanged too — a flush is still acknowledged only once
    /// it is on disk.
    ///
    /// Clamped to 1..=256. 1 restores the previous single-writer behaviour.
    /// Raise it only where `parqtel_ingest_lock_wait_seconds` shows real
    /// contention: it costs a little memory (one writer buffer per shard) and
    /// one extra lock acquisition per push.
    #[serde(default = "default_rotator_shards")]
    pub rotator_shards: usize,
}

fn default_max_inflight_flushes() -> usize {
    4
}

fn default_wal_sync_interval_ms() -> u64 {
    1000
}

fn default_wal_max_segment_bytes() -> u64 {
    crate::wal::DEFAULT_MAX_SEGMENT_BYTES
}

fn default_rotator_shards() -> usize {
    4
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
            wal_enabled: true,
            log_wal_enabled: true,
            wal_sync_mode: crate::wal::WalSyncMode::Interval,
            wal_sync_interval_ms: default_wal_sync_interval_ms(),
            max_inflight_flushes: default_max_inflight_flushes(),
            wal_max_segment_bytes: default_wal_max_segment_bytes(),
            tail_sampling: TailSamplingConfig::default(),
            rotator_shards: default_rotator_shards(),
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
