//! Query execution against block storage and the memory buffer.
//!
//! [`QueryExecutor`] is defined here with its constructors; the queries are
//! methods spread over the submodules by signal.

use parqtel_core::{BlockIndex, MemoryBuffer};
use std::sync::Arc;
use tokio::sync::RwLock;

mod correlate;
mod labels;
mod logs;
mod metrics;
mod pipeline;
mod series;
mod traces;

/// Executes queries against the storage engine.
pub struct QueryExecutor {
    index: Arc<RwLock<BlockIndex>>,
    log_index: Arc<RwLock<BlockIndex>>,
    trace_index: Arc<RwLock<BlockIndex>>,
    buffer: MemoryBuffer,
    #[allow(dead_code)]
    trace_data_dir: std::path::PathBuf,
    /// Instant-selector lookback (Prometheus lookback-delta). 5m default.
    lookback_ns: i64,
    /// Maximum distinct series a single query may materialise.
    max_series: usize,
    /// Maximum samples retained per series.
    max_samples_per_series: usize,
}

/// Snapshot of per-signal storage stats for ops surfaces (UI overview,
/// /api/v1/stats): block counts, stored row counts, on-disk bytes —
/// all from block metadata, no Parquet decoding.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SignalStats {
    pub blocks: usize,
    pub rows: usize,
    pub bytes: u64,
}

impl QueryExecutor {
    /// Default instant-selector lookback: the Prometheus lookback-delta.
    const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;
    /// Default per-query series cap, matching `query.max_series`.
    const DEFAULT_MAX_SERIES: usize = 1000;
    /// Default per-series sample cap, matching `query.max_samples_per_series`.
    const DEFAULT_MAX_SAMPLES_PER_SERIES: usize = 10_000;

    /// Sets the instant-selector lookback window (builder).
    pub fn with_query_lookback(mut self, lookback_ns: i64) -> Self {
        self.lookback_ns = lookback_ns.max(1);
        self
    }

    /// Sets the per-query result limits (builder).
    ///
    /// The AST path applies the same caps as the plan path, so a query
    /// cannot escape its configured memory bound by using PromQL features
    /// rather than the legacy shape. Both values are clamped to at least 1:
    /// a zero cap would silently return nothing.
    pub fn with_result_limits(mut self, max_series: usize, max_samples_per_series: usize) -> Self {
        self.max_series = max_series.max(1);
        self.max_samples_per_series = max_samples_per_series.max(1);
        self
    }

    /// Single construction point shared by every public constructor.
    ///
    /// The variants differ only in which optional pieces the caller supplies
    /// (a pre-built storage engine, an externally owned trace index, a shared
    /// memory buffer); everything derived — including the trace index, which
    /// must be built from the caller's data dir rather than any fixed path —
    /// happens here so the variants cannot drift apart.
    fn build(
        index: Arc<RwLock<BlockIndex>>,
        log_index: Arc<RwLock<BlockIndex>>,
        trace_index: Option<Arc<RwLock<BlockIndex>>>,
        buffer: Option<MemoryBuffer>,
        trace_data_dir: std::path::PathBuf,
    ) -> Self {
        let trace_index = trace_index.unwrap_or_else(|| {
            let mut idx = BlockIndex::new(&trace_data_dir);
            idx.load().ok();
            Arc::new(RwLock::new(idx))
        });
        Self {
            index,
            log_index,
            trace_index,
            buffer: buffer.unwrap_or_default(),
            trace_data_dir,
            lookback_ns: Self::DEFAULT_LOOKBACK_NS,
            max_series: Self::DEFAULT_MAX_SERIES,
            max_samples_per_series: Self::DEFAULT_MAX_SAMPLES_PER_SERIES,
        }
    }

    /// Creates a new [QueryExecutor] with shared block indexes.
    pub fn new(
        index: Arc<RwLock<BlockIndex>>,
        log_index: Arc<RwLock<BlockIndex>>,
        trace_data_dir: std::path::PathBuf,
    ) -> Self {
        Self::build(index, log_index, None, None, trace_data_dir)
    }

    /// Creates a new [QueryExecutor] with a memory buffer for stream-queryable data.
    pub fn with_buffer(
        index: Arc<RwLock<BlockIndex>>,
        log_index: Arc<RwLock<BlockIndex>>,
        buffer: MemoryBuffer,
        trace_data_dir: std::path::PathBuf,
    ) -> Self {
        Self::build(index, log_index, None, Some(buffer), trace_data_dir)
    }

    /// Creates a new [QueryExecutor] with a trace index and memory buffer.
    pub fn with_trace_index(
        index: Arc<RwLock<BlockIndex>>,
        log_index: Arc<RwLock<BlockIndex>>,
        trace_index: Arc<RwLock<BlockIndex>>,
        buffer: MemoryBuffer,
        trace_data_dir: std::path::PathBuf,
    ) -> Self {
        Self::build(
            index,
            log_index,
            Some(trace_index),
            Some(buffer),
            trace_data_dir,
        )
    }

    /// Returns a clone of the memory buffer for use by ingestion services.
    pub fn memory_buffer(&self) -> MemoryBuffer {
        self.buffer.clone()
    }

    /// Snapshot of per-signal storage stats for ops surfaces (UI overview,
    /// /api/v1/stats): block counts, stored row counts, on-disk bytes —
    /// all from block metadata, no Parquet decoding.
    ///
    /// Per-signal storage stats (metrics, logs, traces) from block index
    /// metadata. O(#blocks) over three in-memory indexes; never decodes
    /// Parquet or hits the filesystem.
    pub async fn storage_stats(&self) -> parqtel_core::Result<[SignalStats; 3]> {
        let (m, l, t) = {
            let metrics = self.index.read().await;
            let logs = self.log_index.read().await;
            let traces = self.trace_index.read().await;
            (
                (
                    metrics.total_blocks(),
                    metrics.total_rows(),
                    metrics.total_bytes(),
                ),
                (logs.total_blocks(), logs.total_rows(), logs.total_bytes()),
                (
                    traces.total_blocks(),
                    traces.total_rows(),
                    traces.total_bytes(),
                ),
            )
        };
        Ok([
            SignalStats {
                blocks: m.0,
                rows: m.1,
                bytes: m.2,
            },
            SignalStats {
                blocks: l.0,
                rows: l.1,
                bytes: l.2,
            },
            SignalStats {
                blocks: t.0,
                rows: t.1,
                bytes: t.2,
            },
        ])
    }

    /// The in-memory buffer (freshest, pre-flush data source).
    pub fn buffer(&self) -> &MemoryBuffer {
        &self.buffer
    }
}

#[cfg(test)]
mod tests;
