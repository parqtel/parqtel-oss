use crate::decode::OtlpDecoder;
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use crate::writer::{BlockMetadata, BlockWriter, LogWriter, TraceWriter};
use bytes::Bytes;
use parqtel_core::MemoryBuffer;
use parqtel_core::{
    BlockConfig, ContentionMetrics, DataPoint, Error, LogBlockConfig, LogRecord, Metric, Result,
    SignalType, Span, TailSamplingConfig,
};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// Publishes one freshly written block: records its flush duration and rows,
/// then hands the metadata to the index channel.
///
/// Shared by every write path so a block closed mid-`push` is accounted for
/// exactly like one closed by an explicit flush. Without this the capacity
/// split in [`BlockWriter::push`] would write blocks that never appear in the
/// flush metrics — the expensive case would again be invisible.
fn record_block_written(
    meta: &BlockMetadata,
    contention: Option<&ContentionMetrics>,
    signal: SignalType,
) {
    if let Some(c) = contention {
        c.flush_started(signal).count_rows(meta.row_count as u64);
    }
    tracing::debug!(
        signal = signal.as_str(),
        rows = meta.row_count,
        block = %meta.path.display(),
        "block flushed while pushing"
    );
}

/// Handles automatic rotation and flushing of metric blocks.
pub struct BlockRotator {
    writer: BlockWriter,
    config: BlockConfig,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
}

impl BlockRotator {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: BlockWriter::new(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
        }
    }

    fn publish(&self, meta: BlockMetadata, contention: Option<&ContentionMetrics>) {
        record_block_written(&meta, contention, SignalType::Metrics);
        let _ = self.metadata_tx.send(meta);
    }

    /// Pushes a metric, closing a block first if it would not fit whole.
    ///
    /// A metric larger than a whole block is split across as many blocks as it
    /// needs rather than being partially accepted and then reported as an
    /// error (which made clients retry a batch that was already half-ingested).
    ///
    /// Returns `true` if any flush happened, so the caller drains the memory
    /// buffer and the flushed rows are not read twice.
    pub async fn push(
        &mut self,
        metric: Metric,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        let mut flushed = false;
        if self.writer.len() + metric.data_points.len() > self.config.max_rows_per_block {
            self.flush(contention).await?;
            flushed = true;
        }
        // `push` may still have had to close a block mid-metric; publish
        // whatever it wrote so no block is orphaned.
        for meta in self.writer.push(metric)? {
            flushed = true;
            self.publish(meta, contention);
        }
        Ok(flushed)
    }

    pub async fn check_and_flush(
        &mut self,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.flush(contention).await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Writes buffered rows to Parquet on the blocking thread pool so Parquet
    /// encoding/compression/disk I/O never stalls a tokio worker while the
    /// ingest mutex is held. Idempotent on an empty buffer (the old code
    /// returned a spurious "Cannot flush empty buffer" error).
    ///
    /// The whole encode happens while the caller still holds the ingest mutex,
    /// which is why the guard below exists: it makes flush wall time and the
    /// in-flight gauge observable without a profiler. The idle no-op path
    /// returns before creating a guard, so an empty flush cannot dilute the
    /// histogram.
    pub async fn flush(&mut self, contention: Option<&ContentionMetrics>) -> Result<()> {
        if self.writer.is_empty() {
            tracing::debug!("metric block flush skipped: buffer empty");
            return Ok(());
        }
        let row_count = self.writer.len();
        let started = std::time::Instant::now();
        let mut writer = std::mem::replace(&mut self.writer, BlockWriter::new(self.config.clone()));
        let flush_guard = contention.map(|c| c.flush_started(SignalType::Metrics));
        let encoded = tokio::task::spawn_blocking(move || writer.flush()).await;
        // Unwrap the JoinError separately from the writer's own Result so the
        // row count can still be attributed when the flush succeeded.
        let metadata = match encoded {
            Ok(result) => result,
            Err(e) => {
                return Err(Error::Internal(format!("flush task panicked: {}", e)));
            }
        };
        if let (Some(guard), Ok(meta)) = (flush_guard, metadata.as_ref()) {
            guard.count_rows(meta.row_count as u64);
        }
        let metadata = metadata?;
        self.last_flush = Instant::now();
        let _ = self.metadata_tx.send(metadata);
        tracing::debug!(
            signal = "metrics",
            rows = row_count,
            duration_ms = started.elapsed().as_millis(),
            "metric block flushed to parquet"
        );
        Ok(())
    }
}

/// Handles automatic rotation and flushing of log blocks.
pub struct LogRotator {
    writer: LogWriter,
    config: LogBlockConfig,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
}

impl LogRotator {
    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: LogWriter::new(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
        }
    }

    /// Pushes a log record, closing the block first when it is full.
    ///
    /// Returns `true` if a flush happened, so the caller drains the memory
    /// buffer.
    fn publish(&self, meta: BlockMetadata, contention: Option<&ContentionMetrics>) {
        record_block_written(&meta, contention, SignalType::Logs);
        let _ = self.metadata_tx.send(meta);
    }

    pub async fn push(
        &mut self,
        log: LogRecord,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        if let Some(meta) = self.writer.push(log)? {
            self.publish(meta, contention);
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn check_and_flush(
        &mut self,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.flush(contention).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn flush(&mut self, contention: Option<&ContentionMetrics>) -> Result<()> {
        if self.writer.is_empty() {
            tracing::debug!("log block flush skipped: buffer empty");
            return Ok(());
        }
        let row_count = self.writer.len();
        let started = std::time::Instant::now();
        let mut writer = std::mem::replace(&mut self.writer, LogWriter::new(self.config.clone()));
        let flush_guard = contention.map(|c| c.flush_started(SignalType::Logs));
        let encoded = tokio::task::spawn_blocking(move || writer.flush()).await;
        let metadata = match encoded {
            Ok(result) => result,
            Err(e) => {
                return Err(Error::Internal(format!("flush task panicked: {}", e)));
            }
        };
        if let (Some(guard), Ok(meta)) = (flush_guard, metadata.as_ref()) {
            guard.count_rows(meta.row_count as u64);
        }
        let metadata = metadata?;
        self.last_flush = Instant::now();
        let _ = self.metadata_tx.send(metadata);
        tracing::debug!(
            signal = "logs",
            rows = row_count,
            duration_ms = started.elapsed().as_millis(),
            "log block flushed to parquet"
        );
        Ok(())
    }
}

/// Stats for an ingestion service.
#[derive(Default)]
pub struct IngestionStats {
    pub total_batches: AtomicU64,
    pub failed_batches: AtomicU64,
    pub ingested_points: AtomicU64,
    /// Spans dropped by tail sampling (traces only).
    pub dropped_spans: AtomicU64,
}

/// Runs a blocking decode step on the blocking pool.
///
/// OTLP decode is tens of milliseconds of pure CPU for a large batch, plus one
/// `LabelSet` allocation per point. Doing it inline parks a tokio worker for
/// that whole duration, so concurrent ingest *and* query requests queue behind
/// it. `spawn_blocking` hands the CPU to a thread that is allowed to block,
/// leaving the async workers free to poll everything else.
///
/// `spawn_blocking` already propagates panics as a `JoinError`, so this does
/// not need its own catch_unwind.
async fn decode_offloading<T, F>(decode: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(decode)
        .await
        .map_err(|e| Error::Internal(format!("decode task panicked: {}", e)))?
}

/// Acquires a rotator lock, recording how long the acquisition took.
///
/// Every ingest request for a signal serializes on one mutex, and a block
/// flush runs while that mutex is held, so lock wait is the best single
/// predictor of ingest p99. The clock is read immediately after the guard is
/// returned so the contended mutex is never held while the histogram lock is
/// taken.
async fn lock_rotator<'a, T>(
    rotator: &'a Mutex<T>,
    signal: SignalType,
    contention: Option<&ContentionMetrics>,
) -> tokio::sync::MutexGuard<'a, T> {
    let started = Instant::now();
    let guard = rotator.lock().await;
    if let Some(c) = contention {
        c.record_ingest_lock_wait(signal, started.elapsed());
    }
    guard
}

/// Public service for ingesting OTLP metrics.
pub struct IngestionService {
    rotator: Arc<Mutex<BlockRotator>>,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
}

impl IngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self {
            rotator: Arc::new(Mutex::new(BlockRotator::new(config, metadata_tx))),
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            contention: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable data.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        self.memory_buffer = Some(buffer);
        self
    }

    /// Share lock-wait and flush counters with the `/metrics` renderer.
    pub fn with_contention(mut self, contention: Arc<ContentionMetrics>) -> Self {
        self.contention = Some(contention);
        self
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let metrics = decode_offloading(move || {
            let req = ExportMetricsServiceRequest::decode(body)
                .map_err(|e| Error::Validation(format!("Protobuf decode error: {e}")))?;
            OtlpDecoder::decode_metrics(req)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_metrics(metrics).await
    }

    /// Ingest already-decoded metrics directly (used by the span-metrics
    /// RED bridge, which derives metrics from trace spans).
    pub async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        self.process_metrics(metrics).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let metrics = decode_offloading(move || {
            let json: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| Error::Validation(format!("JSON parse error: {e}")))?;
            OtlpDecoder::decode_metrics_json(json)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_metrics(metrics).await
    }

    async fn process_metrics(&self, metrics: Vec<Metric>) -> Result<u64> {
        let mut count = 0;
        let mut flushed = false;
        // Write to in-memory buffer first (before rotator takes ownership).
        // Inject `service.name` from resource attributes into point labels so
        // buffered queries match the on-disk scanner behaviour (label matchers
        // can select on service.name either way).
        if let Some(ref buf) = self.memory_buffer {
            for m in &metrics {
                let svc = m.resource_attributes.get("service.name");
                let points: Vec<_> = match svc {
                    Some(svc) => m
                        .data_points
                        .iter()
                        .map(|dp| {
                            let mut labels = dp.labels.clone();
                            if labels.get("service.name").is_none() {
                                labels = labels.merge(
                                    &parqtel_core::LabelSet::try_from_iter(vec![(
                                        "service.name",
                                        svc.to_string(),
                                    )])
                                    .unwrap_or_default(),
                                );
                            }
                            DataPoint {
                                timestamp_ns: dp.timestamp_ns,
                                value: dp.value.clone(),
                                labels,
                            }
                        })
                        .collect(),
                    None => m.data_points.clone(),
                };
                buf.push_metrics(&m.name, &points).await;
            }
        }
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Metrics, contention.as_deref()).await;
        for m in metrics {
            count += m.data_points.len() as u64;
            if rotator.push(m, contention.as_deref()).await? {
                flushed = true;
            }
        }
        if rotator.check_and_flush(contention.as_deref()).await? {
            flushed = true;
        }
        drop(rotator);
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_offloaded(SignalType::Metrics).await;
            }
        }
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        tracing::debug!(
            signal = "metrics",
            ingested = count,
            flushed = flushed,
            "metrics ingestion complete"
        );
        Ok(count)
    }

    /// Checks for duration-based flush; returns `true` when a flush happened
    /// (the shared memory buffer is drained so flushed rows aren't double-read).
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        let flushed = lock_rotator(&self.rotator, SignalType::Metrics, contention.as_deref())
            .await
            .check_and_flush(contention.as_deref())
            .await?;
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_offloaded(SignalType::Metrics).await;
            }
        }
        Ok(flushed)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Metrics, contention.as_deref()).await;
        let _ = rotator.flush(contention.as_deref()).await;
        drop(rotator);
        if let Some(ref buf) = self.memory_buffer {
            buf.drain_offloaded(SignalType::Metrics).await;
        }
        Ok(())
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.total_batches.load(Ordering::Relaxed),
            self.stats.failed_batches.load(Ordering::Relaxed),
            self.stats.ingested_points.load(Ordering::Relaxed),
        )
    }
}

/// Public service for ingesting OTLP logs.
pub struct LogIngestionService {
    rotator: Arc<Mutex<LogRotator>>,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
}

impl LogIngestionService {
    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self {
            rotator: Arc::new(Mutex::new(LogRotator::new(config, metadata_tx))),
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            contention: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable data.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        self.memory_buffer = Some(buffer);
        self
    }

    /// Share lock-wait and flush counters with the `/metrics` renderer.
    pub fn with_contention(mut self, contention: Arc<ContentionMetrics>) -> Self {
        self.contention = Some(contention);
        self
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let logs = decode_offloading(move || {
            let req = ExportLogsServiceRequest::decode(body)
                .map_err(|e| Error::Validation(format!("Protobuf decode error: {e}")))?;
            OtlpDecoder::decode_logs(req)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_logs(logs).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let logs = decode_offloading(move || {
            let json: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| Error::Validation(format!("JSON parse error: {e}")))?;
            OtlpDecoder::decode_logs_json(json)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_logs(logs).await
    }

    async fn process_logs(&self, logs: Vec<LogRecord>) -> Result<u64> {
        let count = logs.len() as u64;
        let mut flushed = false;
        // Write to in-memory buffer first
        if let Some(ref buf) = self.memory_buffer {
            buf.push_logs(&logs).await;
        }
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref()).await;
        for l in logs {
            if rotator.push(l, contention.as_deref()).await? {
                flushed = true;
            }
        }
        if rotator.check_and_flush(contention.as_deref()).await? {
            flushed = true;
        }
        drop(rotator);
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_offloaded(SignalType::Logs).await;
            }
        }
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        tracing::debug!(
            signal = "logs",
            ingested = count,
            flushed = flushed,
            "logs ingestion complete"
        );
        Ok(count)
    }

    /// Checks for duration-based flush; returns `true` when a flush happened
    /// (the shared memory buffer is drained so flushed rows aren't double-read).
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        let flushed = lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref())
            .await
            .check_and_flush(contention.as_deref())
            .await?;
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_offloaded(SignalType::Logs).await;
            }
        }
        Ok(flushed)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref()).await;
        let _ = rotator.flush(contention.as_deref()).await;
        drop(rotator);
        if let Some(ref buf) = self.memory_buffer {
            buf.drain_offloaded(SignalType::Logs).await;
        }
        Ok(())
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.total_batches.load(Ordering::Relaxed),
            self.stats.failed_batches.load(Ordering::Relaxed),
            self.stats.ingested_points.load(Ordering::Relaxed),
        )
    }
}

/// Handles automatic rotation and flushing of trace blocks.
pub struct TraceRotator {
    writer: TraceWriter,
    config: BlockConfig,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
}

impl TraceRotator {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: TraceWriter::new(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
        }
    }

    /// Pushes a span, closing the block first when it is full.
    ///
    /// Returns `true` if a flush happened, so the caller drains the memory
    /// buffer.
    fn publish(&self, meta: BlockMetadata, contention: Option<&ContentionMetrics>) {
        record_block_written(&meta, contention, SignalType::Traces);
        let _ = self.metadata_tx.send(meta);
    }

    pub async fn push(
        &mut self,
        span: Span,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        if let Some(meta) = self.writer.push(span)? {
            self.publish(meta, contention);
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn check_and_flush(
        &mut self,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.flush(contention).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn flush(&mut self, contention: Option<&ContentionMetrics>) -> Result<()> {
        if self.writer.is_empty() {
            tracing::debug!("trace block flush skipped: buffer empty");
            return Ok(());
        }
        let row_count = self.writer.len();
        let started = std::time::Instant::now();
        let mut writer = std::mem::replace(&mut self.writer, TraceWriter::new(self.config.clone()));
        let flush_guard = contention.map(|c| c.flush_started(SignalType::Traces));
        let encoded = tokio::task::spawn_blocking(move || writer.flush()).await;
        let metadata = match encoded {
            Ok(result) => result,
            Err(e) => {
                return Err(Error::Internal(format!("flush task panicked: {}", e)));
            }
        };
        if let (Some(guard), Ok(meta)) = (flush_guard, metadata.as_ref()) {
            guard.count_rows(meta.row_count as u64);
        }
        let metadata = metadata?;
        self.last_flush = Instant::now();
        let _ = self.metadata_tx.send(metadata);
        tracing::debug!(
            signal = "traces",
            rows = row_count,
            duration_ms = started.elapsed().as_millis(),
            "trace block flushed to parquet"
        );
        Ok(())
    }
}

/// Public service for ingesting OTLP traces.
pub struct TraceIngestionService {
    rotator: Arc<Mutex<TraceRotator>>,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    /// When set, span-metrics RED derivation sends metric batches here
    /// (consumed by a task that feeds the metrics ingestion service).
    span_metrics_tx: Option<mpsc::UnboundedSender<Vec<Metric>>>,
    /// Tail-sampling policy; default (keep-all) short-circuits to zero cost.
    tail_sampling: TailSamplingConfig,
    contention: Option<Arc<ContentionMetrics>>,
}

impl TraceIngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self {
            rotator: Arc::new(Mutex::new(TraceRotator::new(config, metadata_tx))),
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            span_metrics_tx: None,
            tail_sampling: TailSamplingConfig::default(),
            contention: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable spans.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        self.memory_buffer = Some(buffer);
        self
    }

    /// Share lock-wait and flush counters with the `/metrics` renderer.
    pub fn with_contention(mut self, contention: Arc<ContentionMetrics>) -> Self {
        self.contention = Some(contention);
        self
    }

    /// Enable the span-metrics RED bridge: derived metrics
    /// (`traces_service_{requests,errors,duration_ms}_total`) are sent to
    /// this channel for ingestion as normal metrics.
    pub fn with_span_metrics(mut self, tx: mpsc::UnboundedSender<Vec<Metric>>) -> Self {
        self.span_metrics_tx = Some(tx);
        self
    }

    /// Set the tail-sampling policy for traces.
    pub fn with_tail_sampling(mut self, policy: TailSamplingConfig) -> Self {
        self.tail_sampling = policy;
        self
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let spans = decode_offloading(move || {
            let req = ExportTraceServiceRequest::decode(body)
                .map_err(|e| Error::Validation(format!("Protobuf decode error: {e}")))?;
            OtlpDecoder::decode_traces(req)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_traces(spans).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let spans = decode_offloading(move || {
            let json: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| Error::Validation(format!("JSON parse error: {e}")))?;
            OtlpDecoder::decode_traces_json(json)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_traces(spans).await
    }

    async fn process_traces(&self, spans: Vec<Span>) -> Result<u64> {
        let count = spans.len() as u64;
        let mut flushed = false;
        // Span-metrics RED bridge: derive metrics from the FULL span set
        // (BEFORE tail sampling) so RED rates stay accurate while trace
        // storage is sampled.
        if let Some(ref tx) = self.span_metrics_tx {
            let derived = crate::span_metrics::derive_span_metrics(&spans);
            if !derived.is_empty() {
                let _ = tx.send(derived);
            }
        }
        // Tail sampling: decide which traces to persist.
        let (spans, dropped) = crate::tail_sampling::sample_spans(&self.tail_sampling, spans);
        if dropped > 0 {
            self.stats
                .dropped_spans
                .fetch_add(dropped, Ordering::Relaxed);
        }
        // Write to in-memory buffer first so spans are queryable immediately.
        if let Some(ref buf) = self.memory_buffer {
            buf.push_spans(&spans).await;
        }
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref()).await;
        for s in spans {
            if rotator.push(s, contention.as_deref()).await? {
                flushed = true;
            }
        }
        rotator.check_and_flush(contention.as_deref()).await?;
        drop(rotator);
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_offloaded(SignalType::Traces).await;
            }
        }
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        tracing::debug!(
            signal = "traces",
            ingested = count,
            flushed = flushed,
            "traces ingestion complete"
        );
        Ok(count)
    }

    /// Checks for duration-based flush; returns `true` when a flush happened
    /// (the shared memory buffer is drained so flushed spans aren't double-read).
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        let flushed = lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref())
            .await
            .check_and_flush(contention.as_deref())
            .await?;
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_offloaded(SignalType::Traces).await;
            }
        }
        Ok(flushed)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref()).await;
        let _ = rotator.flush(contention.as_deref()).await;
        drop(rotator);
        if let Some(ref buf) = self.memory_buffer {
            buf.drain_offloaded(SignalType::Traces).await;
        }
        Ok(())
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.total_batches.load(Ordering::Relaxed),
            self.stats.failed_batches.load(Ordering::Relaxed),
            self.stats.ingested_points.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    /// Small block config so a handful of points forces a flush.
    fn tiny_metrics_config(dir: &std::path::Path) -> BlockConfig {
        BlockConfig {
            data_dir: dir.to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        }
    }

    fn one_point_metric(name: &str, ts: i64) -> Metric {
        Metric {
            name: name.into(),
            kind: parqtel_core::MetricKind::Gauge,
            data_points: vec![parqtel_core::DataPoint::new(
                ts,
                parqtel_core::MetricValue::Double(1.0),
                parqtel_core::LabelSet::default(),
            )
            .unwrap()],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_block_rotator_flush() {
        let dir = tempdir().unwrap();
        let config = tiny_metrics_config(dir.path());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rotator = BlockRotator::new(config, tx);

        rotator
            .push(one_point_metric("m1", 100), None)
            .await
            .unwrap();

        rotator.flush(None).await.unwrap();
        let meta = rx.recv().await.unwrap();
        assert_eq!(meta.row_count, 1);
        assert!(meta.path.exists());
    }

    /// An idle flush must not produce an observation: it dilutes the flush
    /// latency histogram and would show up as a p50 far below any real flush.
    #[tokio::test]
    async fn test_idle_flush_is_not_recorded() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = ContentionMetrics::new();
        let mut rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        rotator.flush(Some(&contention)).await.unwrap();
        assert!(rx.try_recv().is_err(), "no block should be written");
        assert_eq!(contention.flush_duration_count(SignalType::Metrics), 0);
        assert_eq!(contention.flush_rows(SignalType::Metrics), 0);
        assert_eq!(contention.flush_inflight(SignalType::Metrics), 0);
    }

    /// A real flush records wall time, rows written, and leaves the in-flight
    /// gauge at zero — the guard must not leak on the error path either.
    #[tokio::test]
    async fn test_flush_records_duration_rows_and_clears_inflight() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = ContentionMetrics::new();
        let mut rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        rotator
            .push(one_point_metric("m1", 100), Some(&contention))
            .await
            .unwrap();
        rotator.flush(Some(&contention)).await.unwrap();

        let meta = rx.recv().await.unwrap();
        assert_eq!(contention.flush_duration_count(SignalType::Metrics), 1);
        assert_eq!(contention.flush_rows(SignalType::Metrics), 1);
        assert_eq!(contention.flush_inflight(SignalType::Metrics), 0);
        assert_eq!(
            contention.flush_rows(SignalType::Metrics),
            meta.row_count as u64,
            "rows recorded must match the block that was written"
        );
    }

    /// Crossing the row cap triggers a flush from inside `push`, so the
    /// capacity-triggered path — the expensive case that the 5s tick never
    /// sees — must be observable too.
    #[tokio::test]
    async fn test_capacity_triggered_flush_is_recorded() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = ContentionMetrics::new();
        let mut rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        // Fill to exactly max_rows_per_block (10).
        for i in 0..10 {
            rotator
                .push(one_point_metric("m1", 100 + i), Some(&contention))
                .await
                .unwrap();
        }
        assert_eq!(contention.flush_duration_count(SignalType::Metrics), 0);

        // The 11th point crosses the cap: `push` must flush first and report
        // that it did, so the caller drains the memory buffer.
        let flushed = rotator
            .push(one_point_metric("m1", 200), Some(&contention))
            .await
            .unwrap();
        assert!(flushed, "push must report the flush so the buffer drains");

        let meta = rx.recv().await.unwrap();
        assert_eq!(meta.row_count, 10);
        assert_eq!(contention.flush_duration_count(SignalType::Metrics), 1);
        assert_eq!(contention.flush_rows(SignalType::Metrics), 10);
        assert_eq!(contention.flush_inflight(SignalType::Metrics), 0);
    }

    /// A single metric larger than a whole block must be split across blocks
    /// and ingested in full — not partially accepted and then reported as an
    /// error, which made clients retry a batch that was already half-ingested.
    #[tokio::test]
    async fn test_oversized_metric_is_split_across_blocks() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = ContentionMetrics::new();
        let mut rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        // 25 points into a block that holds 10: three blocks, no error.
        let points: Vec<_> = (0..25)
            .map(|i| {
                parqtel_core::DataPoint::new(
                    1000 + i,
                    parqtel_core::MetricValue::Double(i as f64),
                    parqtel_core::LabelSet::default(),
                )
                .unwrap()
            })
            .collect();
        let flushed = rotator
            .push(
                Metric {
                    name: "big".into(),
                    kind: parqtel_core::MetricKind::Gauge,
                    data_points: points,
                    ..Default::default()
                },
                Some(&contention),
            )
            .await
            .unwrap();
        assert!(flushed, "splitting a metric must report the flush");

        rotator.flush(Some(&contention)).await.unwrap();

        let mut metas = Vec::new();
        while let Ok(meta) = rx.try_recv() {
            metas.push(meta);
        }
        let total: usize = metas.iter().map(|m| m.row_count).sum();
        assert_eq!(
            total, 25,
            "every point must land exactly once across the split blocks"
        );
        assert!(
            metas.len() >= 3,
            "25 points at 10 per block needs at least 3 blocks, got {}",
            metas.len()
        );
        // Split blocks must be published, not orphaned: the index only learns
        // about blocks whose metadata reaches the channel.
        assert_eq!(
            contention.flush_rows(SignalType::Metrics),
            25,
            "rows from the split blocks must be accounted for"
        );
    }

    /// Every block written — including the ones the writer closes mid-push —
    /// must publish its metadata, or the data becomes unqueryable even though
    /// the bytes are on disk.
    #[tokio::test]
    async fn test_split_blocks_are_published_to_the_index() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        let points: Vec<_> = (0..25)
            .map(|i| {
                parqtel_core::DataPoint::new(
                    1000 + i,
                    parqtel_core::MetricValue::Double(i as f64),
                    parqtel_core::LabelSet::default(),
                )
                .unwrap()
            })
            .collect();
        rotator
            .push(
                Metric {
                    name: "big".into(),
                    kind: parqtel_core::MetricKind::Gauge,
                    data_points: points,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        // The two closed blocks must already be in the channel before any
        // explicit flush; the tail is still buffered.
        let mut published = 0usize;
        while let Ok(meta) = rx.try_recv() {
            assert!(meta.path.exists(), "published block must exist on disk");
            published += meta.row_count;
        }
        assert_eq!(
            published, 20,
            "the two blocks closed by the split must be published"
        );
    }

    /// A log/trace record arriving at a full buffer closes the block and starts
    /// the next one, rather than failing the request.
    #[tokio::test]
    async fn test_full_log_buffer_rolls_over_instead_of_failing() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = ContentionMetrics::new();
        let mut rotator = LogRotator::new(
            LogBlockConfig {
                data_dir: dir.path().to_path_buf(),
                max_rows_per_block: 3,
                block_duration_secs: 3600,
                ..Default::default()
            },
            tx,
        );

        let make = |ts: i64| {
            parqtel_core::LogRecord::new(
                ts,
                ts,
                9,
                "INFO".into(),
                "msg".into(),
                parqtel_core::LabelSet::default(),
                parqtel_core::LabelSet::default(),
                [0u8; 16],
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            )
        };

        for i in 0..3 {
            assert!(
                !rotator
                    .push(make(100 + i), Some(&contention))
                    .await
                    .unwrap(),
                "no flush expected while the buffer has room"
            );
        }
        // Fourth record crosses the cap: must roll over, not error.
        assert!(rotator.push(make(103), Some(&contention)).await.unwrap());

        let meta = rx.recv().await.unwrap();
        assert_eq!(meta.row_count, 3, "the full block is written");
        assert_eq!(
            contention.flush_rows(SignalType::Logs),
            3,
            "the rolled-over block must be accounted for"
        );
    }

    /// Decode runs on the blocking pool, so the failure counter must still be
    /// incremented exactly once for a bad payload — including when the decode
    /// task itself fails.
    #[tokio::test]
    async fn test_malformed_payloads_still_count_as_failures_once() {
        let dir = tempdir().unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(tiny_metrics_config(dir.path()), tx);

        // Protobuf: garbage bytes.
        assert!(service
            .ingest_proto(Bytes::from_static(&[0xff, 0xfe, 0xfd]))
            .await
            .is_err());
        // JSON: not an object.
        assert!(service
            .ingest_json(Bytes::from_static(b"not json"))
            .await
            .is_err());

        let (batches, failed, points) = service.stats();
        assert_eq!(batches, 2, "both attempts counted as batches");
        assert_eq!(failed, 2, "both counted as failures exactly once");
        assert_eq!(points, 0, "nothing ingested");
    }

    /// A large payload must decode off the runtime thread and still arrive
    /// intact, so the offload is exercised end to end rather than only on the
    /// error path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_large_payloads_decode_and_ingest() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = Arc::new(ContentionMetrics::new());
        let service = Arc::new(
            IngestionService::new(
                BlockConfig {
                    max_rows_per_block: 50,
                    block_duration_secs: 0,
                    ..tiny_metrics_config(dir.path())
                },
                tx,
            )
            .with_contention(contention.clone()),
        );

        let points = 40;
        let json = serde_json::json!({
            "resourceMetrics": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "svc"}}
                ]},
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "cpu",
                        "unit": "1",
                        "gauge": {"dataPoints": (0..points).map(|i| json!({
                            "asInt": i.to_string(),
                            "timeUnixNano": (1000 + i).to_string()
                        })).collect::<Vec<_>>()}
                    }]
                }]
            }]
        });
        let body = Bytes::from(serde_json::to_vec(&json).unwrap());

        // Two concurrent decodes on a 2-worker runtime: with inline decoding one
        // would occupy a whole worker for its duration.
        let mut handles = Vec::new();
        for _ in 0..2 {
            let service = service.clone();
            let body = body.clone();
            handles.push(tokio::spawn(async move {
                service.ingest_json(body).await.unwrap()
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap(), points as u64);
        }

        service.shutdown().await.unwrap();
        let total: usize = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|m: parqtel_core::BlockMetadata| m.row_count)
            .sum();
        assert_eq!(total, points * 2, "both batches must be written in full");
    }

    /// Every ingest request for a signal must record exactly one lock-wait
    /// observation, and the counter must be attributable per signal.
    #[tokio::test]
    async fn test_ingest_records_lock_wait_per_signal() {
        let dir = tempdir().unwrap();
        let contention = Arc::new(ContentionMetrics::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (ltx, _lrx) = mpsc::unbounded_channel();
        let (ttx, _trx) = mpsc::unbounded_channel();

        let metrics_svc = IngestionService::new(tiny_metrics_config(dir.path()), tx)
            .with_contention(contention.clone());
        let logs_svc = LogIngestionService::new(
            LogBlockConfig {
                data_dir: dir.path().join("logs"),
                ..Default::default()
            },
            ltx,
        )
        .with_contention(contention.clone());
        let traces_svc = TraceIngestionService::new(tiny_metrics_config(dir.path()), ttx)
            .with_contention(contention.clone());

        metrics_svc
            .ingest_metrics(vec![one_point_metric("m1", 100)])
            .await
            .unwrap();
        metrics_svc
            .ingest_metrics(vec![one_point_metric("m1", 200)])
            .await
            .unwrap();
        logs_svc
            .ingest_json(Bytes::from(
                serde_json::to_vec(&json!({
                    "resourceLogs": [{
                        "resource": {"attributes": [
                            {"key": "service.name", "value": {"stringValue": "svc"}}
                        ]},
                        "scopeLogs": [{
                            "logRecords": [{
                                "timeUnixNano": "100",
                                "severityNumber": 9,
                                "severityText": "INFO",
                                "body": {"stringValue": "hello"}
                            }]
                        }]
                    }]
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        traces_svc
            .ingest_json(Bytes::from(
                serde_json::to_vec(&json!({
                    "resourceSpans": [{
                        "resource": {"attributes": [
                            {"key": "service.name", "value": {"stringValue": "svc"}}
                        ]},
                        "scopeSpans": [{
                            "spans": [{
                                "traceId": "0af7651916cd43dd8448eb211c80319c",
                                "spanId": "b7ad6b7169203331",
                                "name": "op",
                                "kind": 2,
                                "startTimeUnixNano": "100",
                                "endTimeUnixNano": "200"
                            }]
                        }]
                    }]
                }))
                .unwrap(),
            ))
            .await
            .unwrap();

        assert_eq!(contention.ingest_lock_wait_count(SignalType::Metrics), 2);
        assert_eq!(contention.ingest_lock_wait_count(SignalType::Logs), 1);
        assert_eq!(contention.ingest_lock_wait_count(SignalType::Traces), 1);

        // The rendered body must be scrapeable and carry all three signals.
        let rendered = contention.render();
        assert!(rendered.contains("parqtel_ingest_lock_wait_seconds{signal=\"metrics\"}_count 2"));
        assert!(rendered.contains("parqtel_ingest_lock_wait_seconds{signal=\"logs\"}_count 1"));
        assert!(rendered.contains("parqtel_ingest_lock_wait_seconds{signal=\"traces\"}_count 1"));
    }

    /// Concurrent requests must each record a lock wait and none may be lost —
    /// this is the observation that predicts ingest p99, so a dropped
    /// observation would hide exactly the stall we are looking for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_ingest_records_every_lock_wait() {
        let dir = tempdir().unwrap();
        let contention = Arc::new(ContentionMetrics::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = Arc::new(
            IngestionService::new(tiny_metrics_config(dir.path()), tx)
                .with_contention(contention.clone()),
        );

        const CONCURRENT: usize = 32;
        let mut handles = Vec::with_capacity(CONCURRENT);
        for i in 0..CONCURRENT {
            let service = service.clone();
            handles.push(tokio::spawn(async move {
                service
                    .ingest_metrics(vec![one_point_metric("m1", 1000 + i as i64)])
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            contention.ingest_lock_wait_count(SignalType::Metrics),
            CONCURRENT as u64,
            "every request must record exactly one lock-wait observation"
        );
        assert_eq!(
            contention.ingest_lock_wait_count(SignalType::Logs),
            0,
            "log requests must not pollute the metrics series"
        );
    }

    /// The service must still work with no contention sink attached, so an
    /// embedder that does not build telemetry pays nothing.
    #[tokio::test]
    async fn test_services_work_without_contention_metrics() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        // block_duration_secs = 0 makes the duration check fire immediately,
        // so check_and_flush exercises the periodic path deterministically.
        let service = IngestionService::new(
            BlockConfig {
                block_duration_secs: 0,
                ..tiny_metrics_config(dir.path())
            },
            tx,
        );

        assert_eq!(
            service
                .ingest_metrics(vec![one_point_metric("m1", 100)])
                .await
                .unwrap(),
            1
        );
        assert!(service.check_and_flush().await.unwrap());
        service.shutdown().await.unwrap();

        let mut metas = Vec::new();
        while let Ok(meta) = rx.try_recv() {
            metas.push(meta);
        }
        assert!(!metas.is_empty(), "flushes must still publish blocks");
        let total: usize = metas.iter().map(|m| m.row_count).sum();
        assert_eq!(total, 1, "the point must be written exactly once");
    }

    #[tokio::test]
    async fn test_ingestion_service_json() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);

        let payload = json!({
            "resourceMetrics": [{
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "test_m",
                        "gauge": {"dataPoints": [{"timeUnixNano": 1000, "asDouble": 1.0}]}
                    }]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 1);
        let (total, failed, ingested) = service.stats();
        assert_eq!(total, 1);
        assert_eq!(failed, 0);
        assert_eq!(ingested, 1);
    }

    #[tokio::test]
    async fn test_log_ingestion_service_json() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = LogIngestionService::new(config, tx);

        let payload = json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": 1000,
                        "body": "test log"
                    }]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 1);
        let (total, failed, ingested) = service.stats();
        assert_eq!(total, 1);
        assert_eq!(failed, 0);
        assert_eq!(ingested, 1);
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_json() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);

        let payload = json!({
            "resource_spans": [{
                "scope_spans": [{
                    "spans": [{
                        "trace_id": "0102030405060708090a0b0c0d0e0f10",
                        "span_id": "0102030405060708",
                        "name": "test-span",
                        "kind": 1,
                        "start_time_unix_nano": "1000",
                        "end_time_unix_nano": "2000"
                    }]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 1);
        let (total, failed, ingested) = service.stats();
        assert_eq!(total, 1);
        assert_eq!(failed, 0);
        assert_eq!(ingested, 1);
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_error_paths() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 2,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);

        // Test empty body
        let res = service.ingest_json(Bytes::from("")).await;
        assert!(res.is_err());

        // Test invalid JSON
        let res = service.ingest_json(Bytes::from("{invalid json}")).await;
        assert!(res.is_err());

        // Test buffer full (3 spans > capacity of 2)
        let payload = json!({
            "resource_spans": [{
                "scope_spans": [{
                    "spans": [
                        {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s1", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"},
                        {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s2", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"},
                        {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s3", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"}
                    ]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_ingestion_service_shutdown() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 60,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);

        let payload = json!({ "resourceMetrics": [{ "scopeMetrics": [{ "metrics": [{ "name": "m", "gauge": {"dataPoints": [{"timeUnixNano": 1000, "asDouble": 1.0}]} }] }] }] });
        service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_ingestion_service_invalid_json() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);
        let res = service.ingest_json(Bytes::from("not json")).await;
        assert!(res.is_err());
        let (total, _, _) = service.stats();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn test_ingestion_service_invalid_proto() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);
        let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_log_ingestion_service_invalid_proto() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = LogIngestionService::new(config, tx);
        let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_log_ingestion_service_shutdown() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 60,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = LogIngestionService::new(config, tx);
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_invalid_proto() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);
        let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_shutdown() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 60,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_log_rotator_flush() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rotator = LogRotator::new(config, tx);

        let log = parqtel_core::LogRecord::new(
            100,
            100,
            9,
            "INFO".into(),
            "test".into(),
            parqtel_core::LabelSet::default(),
            parqtel_core::LabelSet::default(),
            [0u8; 16],
            [0u8; 8],
            0,
            "".into(),
            "".into(),
        );
        rotator.push(log, None).await.unwrap();

        rotator.flush(None).await.unwrap();
        let meta = rx.recv().await.unwrap();
        assert_eq!(meta.row_count, 1);
    }

    #[tokio::test]
    async fn test_check_and_flush_within_duration() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut rotator = BlockRotator::new(config, tx);

        rotator
            .push(
                parqtel_core::Metric {
                    name: "m".into(),
                    kind: parqtel_core::MetricKind::Gauge,
                    data_points: vec![parqtel_core::DataPoint::new(
                        100,
                        parqtel_core::MetricValue::Double(1.0),
                        parqtel_core::LabelSet::default(),
                    )
                    .unwrap()],
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        // Should not flush since duration hasn't elapsed
        rotator.check_and_flush(None).await.unwrap();
    }
}
