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
    ///
    /// Also gates the background flush worker (`max_inflight_flushes`). The
    /// worker acknowledges a request once the WAL has its rows rather than once
    /// the block is on disk, which is only sound **with** a WAL: without one, an
    /// acknowledged-but-unwritten flush is unrecoverable. Disabling the WAL
    /// therefore disables the worker too, and flushing falls back to
    /// synchronous — still acknowledged only once the block is durable.
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
    /// How many distinct label sets the process-wide interner holds before it
    /// starts emptying shards.
    ///
    /// Interning lets points of one series share a single copy of their labels.
    /// A working set larger than this loses that sharing and memory grows with
    /// points instead of series. Set it to at least twice the active series
    /// count (each series is interned twice: as received, and with
    /// `service.name` for the memory buffer). The table holds only sets that
    /// were actually seen, so a high value costs nothing on a small deployment.
    #[serde(default = "default_label_interner_capacity")]
    pub label_interner_capacity: usize,
    /// Maximum buffered rows across metrics, logs, and traces. `0` disables
    /// the row bound.
    ///
    /// The buffer exists so data is queryable before a block flush; the writer
    /// and WAL hold the same rows, so dropping the oldest buffered rows only
    /// shortens the pre-flush query window, it does not lose acknowledged data.
    #[serde(default = "default_buffer_max_points")]
    pub buffer_max_points: usize,
    /// Approximate maximum buffered bytes across all signals. `0` disables the
    /// byte bound. The estimate counts point/record structure plus body and
    /// label bytes, not allocator overhead.
    #[serde(default = "default_buffer_max_bytes")]
    pub buffer_max_bytes: usize,
    /// What to do when either bound is reached.
    #[serde(default)]
    pub buffer_overflow_policy: BufferOverflowPolicy,
    /// Maximum simultaneous OTLP/HTTP ingest requests. When full, requests are
    /// rejected with `429` and `Retry-After` rather than queued without bound.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// `Retry-After` value, in seconds, returned with an ingest `429`.
    #[serde(default = "default_load_shed_retry_after_secs")]
    pub load_shed_retry_after_secs: u64,
    /// Generic JSON ingest: field carrying the timestamp.
    #[serde(default = "default_json_timestamp_field")]
    pub json_timestamp_field: String,
    /// Generic JSON ingest: field carrying the log body/message.
    #[serde(default = "default_json_body_field")]
    pub json_body_field: String,
    /// Generic JSON ingest: field carrying the severity/level.
    #[serde(default = "default_json_severity_field")]
    pub json_severity_field: String,
    /// Maximum distinct series admitted per metric. Existing series keep
    /// ingesting; new ones past the cap are dropped and counted. `0` disables.
    #[serde(default = "default_series_limit_per_metric")]
    pub series_limit_per_metric: usize,
}

/// Overload behaviour for the in-memory query buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferOverflowPolicy {
    /// Evict the oldest buffered rows and keep accepting new data.
    #[default]
    DropOldest,
    /// Refuse the new batch and keep the rows already buffered.
    RejectNew,
}

fn default_buffer_max_points() -> usize {
    5_000_000
}

fn default_buffer_max_bytes() -> usize {
    2 * 1024 * 1024 * 1024
}

fn default_max_concurrent_requests() -> usize {
    64
}

fn default_load_shed_retry_after_secs() -> u64 {
    1
}

fn default_json_timestamp_field() -> String {
    "timestamp".to_string()
}

fn default_json_body_field() -> String {
    "message".to_string()
}

fn default_json_severity_field() -> String {
    "level".to_string()
}

fn default_series_limit_per_metric() -> usize {
    1_000_000
}

fn default_label_interner_capacity() -> usize {
    crate::models::labels::DEFAULT_INTERN_CAPACITY
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
            label_interner_capacity: default_label_interner_capacity(),
            buffer_max_points: default_buffer_max_points(),
            buffer_max_bytes: default_buffer_max_bytes(),
            buffer_overflow_policy: BufferOverflowPolicy::default(),
            max_concurrent_requests: default_max_concurrent_requests(),
            load_shed_retry_after_secs: default_load_shed_retry_after_secs(),
            json_timestamp_field: default_json_timestamp_field(),
            json_body_field: default_json_body_field(),
            json_severity_field: default_json_severity_field(),
            series_limit_per_metric: default_series_limit_per_metric(),
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
