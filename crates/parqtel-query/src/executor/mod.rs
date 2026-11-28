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
    /// Writers to ask for unflushed rows. `None` on a writer or a single node.
    remote: Option<Arc<dyn crate::remote::RemoteBuffer>>,
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
            remote: None,
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

    /// Installs the writer fan-out used by a reader node (builder).
    pub fn with_remote_buffer(mut self, remote: Arc<dyn crate::remote::RemoteBuffer>) -> Self {
        self.remote = Some(remote);
        self
    }

    /// The reader's watermark: the newest block timestamp it can already see
    /// for a signal. Zero when no block is visible yet, so every buffered row
    /// on the writers is requested.
    pub(crate) async fn remote_watermark_ns(&self, signal: crate::remote::RemoteSignal) -> i64 {
        use crate::remote::RemoteSignal;
        let index = match signal {
            RemoteSignal::Metrics => &self.index,
            RemoteSignal::Logs => &self.log_index,
            RemoteSignal::Traces => &self.trace_index,
        };
        index
            .read()
            .await
            .blocks
            .iter()
            .map(|block| block.end_timestamp_ns)
            .max()
            .unwrap_or(0)
    }

    /// Newest block this node flushed itself for `signal`. Adopted remote
    /// blocks are read through a cache copy, so `is_locally_produced` is the
    /// ownership test; a writer with no local block answers 0.
    ///
    /// This exists because writers flush on skewed schedules. The reader sends
    /// one global watermark — the newest block it can see anywhere — and a
    /// writer that flushed earlier would then drop its own buffered rows in
    /// `(its flush, reader watermark]`. Block storage still serves the flushed
    /// rows, so the buffer only needs rows newer than
    /// `min(reader_watermark, local_flushed)`; the reader dedups the overlap.
    pub async fn local_flushed_watermark_ns(&self, signal: crate::remote::RemoteSignal) -> i64 {
        use crate::remote::RemoteSignal;
        let index = match signal {
            RemoteSignal::Metrics => &self.index,
            RemoteSignal::Logs => &self.log_index,
            RemoteSignal::Traces => &self.trace_index,
        };
        let store = parqtel_core::storage::block_store::store();
        index
            .read()
            .await
            .blocks
            .iter()
            .filter(|block| store.is_locally_produced(&block.path))
            .map(|block| block.end_timestamp_ns)
            .max()
            .unwrap_or(0)
    }

    /// Buffered rows from every configured writer. An unreachable writer is a
    /// warning, not a failed query: the reader still answers from block
    /// storage plus whichever writers did respond.
    pub(crate) async fn remote_rows(
        &self,
        signal: crate::remote::RemoteSignal,
        metric: Option<String>,
        start_ns: i64,
        end_ns: i64,
    ) -> crate::remote::RemoteBufferRows {
        let Some(remote) = self.remote.clone() else {
            return Default::default();
        };
        let since_ns = self.remote_watermark_ns(signal).await;
        let request = crate::remote::RemoteBufferRequest {
            signal,
            metric,
            since_ns,
            start_ns,
            end_ns,
            limit: crate::remote::MAX_BUFFER_ROWS,
        };
        match remote.fetch(request).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    signal = ?signal,
                    error = %error,
                    "writer buffer fan-out failed; answering from block storage only"
                );
                Default::default()
            }
        }
    }

    /// Appends remote points, skipping identities the local buffer already
    /// holds: a retried OTLP export can land one row on a writer and on the
    /// local buffer, and the same sample must not be counted twice.
    pub(crate) fn merge_remote_points(
        local: &mut Vec<parqtel_core::DataPoint>,
        remote: Vec<parqtel_core::DataPoint>,
    ) -> usize {
        let mut seen: std::collections::HashSet<(u64, i64)> = local
            .iter()
            .map(|point| (point.labels.fingerprint(), point.timestamp_ns))
            .collect();
        let before = local.len();
        for point in remote {
            if seen.insert((point.labels.fingerprint(), point.timestamp_ns)) {
                local.push(point);
            }
        }
        local.len() - before
    }

    /// Remote metric points for one series name, keeping only rows that match
    /// the selector and are newer than the watermark.
    pub(crate) async fn remote_points_for(
        &self,
        name: &str,
        start_ns: i64,
        end_ns: i64,
        filter: Option<&parqtel_core::storage::scanner::SeriesFilter>,
    ) -> Vec<parqtel_core::DataPoint> {
        let rows = self
            .remote_rows(
                crate::remote::RemoteSignal::Metrics,
                Some(name.to_string()),
                start_ns,
                end_ns,
            )
            .await;
        rows.points
            .into_iter()
            .filter(|(metric, _)| metric == name)
            .filter(|(_, point)| point.timestamp_ns >= start_ns && point.timestamp_ns <= end_ns)
            .filter(|(_, point)| filter.is_none_or(|keep| keep(&point.labels)))
            .map(|(_, point)| point)
            .collect()
    }

    /// Remote logs in range. The caller applies its own row predicate: the
    /// transport cannot carry a closure, and the predicated paths re-check
    /// every buffered row anyway.
    pub(crate) async fn remote_logs(
        &self,
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<parqtel_core::LogRecord> {
        let rows = self
            .remote_rows(crate::remote::RemoteSignal::Logs, None, start_ns, end_ns)
            .await;
        rows.logs
            .into_iter()
            .filter(|log| log.timestamp_ns >= start_ns && log.timestamp_ns <= end_ns)
            .collect()
    }

    /// Remote spans in range, deduplicated by span id so a span that reaches
    /// two writers' buffers (a retried export) still counts once.
    pub(crate) async fn remote_spans(&self, start_ns: i64, end_ns: i64) -> Vec<parqtel_core::Span> {
        let rows = self
            .remote_rows(crate::remote::RemoteSignal::Traces, None, start_ns, end_ns)
            .await;
        let mut seen = std::collections::HashSet::new();
        rows.spans
            .into_iter()
            .filter(|span| span.start_time_ns >= start_ns && span.start_time_ns <= end_ns)
            .filter(|span| seen.insert((span.trace_id, span.span_id)))
            .collect()
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
