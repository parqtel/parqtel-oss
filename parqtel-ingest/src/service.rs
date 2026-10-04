use crate::decode::OtlpDecoder;
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use crate::writer::{BlockMetadata, BlockWriter, LogWriter, TraceWriter};
use bytes::Bytes;
use parqtel_core::wal::{WalPosition, WalWriter};
use parqtel_core::MemoryBuffer;
use parqtel_core::{
    BlockConfig, ContentionMetrics, DataPoint, Error, LogBlockConfig, LogRecord, Metric, Result,
    SignalType, Span, TailSamplingConfig,
};
use prost::Message;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

/// Milliseconds since the unix epoch. Saturates to 0 if the clock is before it.
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Routes a metric to one of `shard_count` writer shards.
///
/// Cheap and stable for the process lifetime, which is all that matters: a
/// metric must always land in the same shard so its points stay together, but
/// the mapping itself is an implementation detail and never persisted.
fn shard_for(metric_name: &str, shard_count: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric_name.hash(&mut hasher);
    (hasher.finish() % shard_count as u64) as usize
}

/// Handles automatic rotation and flushing of metric blocks.
///
/// # Sharding
///
/// Ingest is serialised on one lock per signal, and a block flush runs while
/// that lock is held, so a flush of a full block stalls *every* request for
/// that signal for the encode duration. Sharding the writers removes that
/// cross-metric contention.
///
/// The shards are deliberately **not** independent blocks: a flush takes the
/// buffers from all shards and writes **one** block, so a given ingest rate
/// still produces the same number of files. Sharding only the locks gets the
/// concurrency win without multiplying query fan-out, which independent
/// per-shard blocks would do.
///
/// # Durability
///
/// Unchanged. A flush still runs inline on the request that triggered it and
/// that request is not acknowledged until the block is on disk. Only the
/// *other* requests stop waiting. Making even the triggering request return
/// early is BL-01-14, deferred behind the WAL (BL-03-12).
pub struct BlockRotator {
    /// One writer per shard. Each is locked only for the duration of a push,
    /// never across an encode.
    shards: Vec<Mutex<BlockWriter>>,
    /// Rows currently buffered across all shards.
    ///
    /// Maintained as an atomic so the capacity check on the hot push path is a
    /// single load rather than a lock per shard. Kept exactly in step with the
    /// writers: `push` adds the rows it accepted, and the flush subtracts each
    /// shard's rows at the moment it swaps that shard out — inside the shard's
    /// own lock, so a concurrent push can never have its count erased by a
    /// later `store(0)`.
    buffered: AtomicUsize,
    /// Serialises flushes so two requests cannot each swap the buffers and
    /// write overlapping blocks. Held across the encode, but no shard lock is.
    flush_lock: tokio::sync::Mutex<()>,
    /// WAL for this signal, when enabled.
    ///
    /// Appended to **while a shard lock is held**, so WAL order and writer
    /// order are the same total order. That is what makes the position a flush
    /// commits correct: every row in the writers was logged before it was
    /// written, and no row can be written without having been logged.
    ///
    /// The trade-off is a file write under the ingest lock. The writer is
    /// buffered (64 KB) so that is a memcpy, and `fsync` only happens on the
    /// sync interval — but it will show up in
    /// `parqtel_ingest_lock_wait_seconds` and is worth watching.
    wal: Option<Arc<tokio::sync::Mutex<WalWriter>>>,
    /// Per-shard: the highest WAL position among the rows currently in that
    /// shard's writer. Taken and reset under the shard lock at swap time, so a
    /// block can only ever commit positions for rows it actually contains.
    ///
    /// A mutex rather than a plain `Vec` because `push` and `flush_locked`
    /// take `&self` — the rotator is shared behind an `Arc` and sharding
    /// deliberately made it lock-free for callers. Always taken *while holding
    /// the shard lock*, so the value it holds is always consistent with the
    /// writer it describes.
    covered: Vec<std::sync::Mutex<WalPosition>>,
    config: BlockConfig,
    /// Unix time of the last completed flush, in milliseconds. An atomic so
    /// the duration check needs no lock — flushes are serialised by
    /// `flush_lock`, so exactly one writer updates this at a time.
    last_flush_unix_ms: AtomicU64,
    max_duration_ms: u64,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
}

impl BlockRotator {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self::with_shards(config, metadata_tx, 1)
    }

    /// Creates a rotator with `shards` independent writer shards.
    ///
    /// `shards` is clamped to at least 1, so a zero-valued config yields the
    /// previous single-writer behaviour rather than an unusable rotator.
    pub fn with_shards(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
        shards: usize,
    ) -> Self {
        let shards = shards.clamp(1, 256);
        Self {
            shards: (0..shards)
                .map(|_| Mutex::new(BlockWriter::new(config.clone())))
                .collect(),
            wal: None,
            covered: (0..shards)
                .map(|_| std::sync::Mutex::new(WalPosition::START))
                .collect(),
            flush_lock: tokio::sync::Mutex::new(()),
            buffered: AtomicUsize::new(0),
            max_duration_ms: config.block_duration_secs.saturating_mul(1000),
            config,
            last_flush_unix_ms: AtomicU64::new(unix_millis()),
            metadata_tx,
        }
    }

    /// Number of writer shards. Exported so `/metrics` can show it.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Attaches a WAL. After this, every accepted metric is logged before it
    /// reaches a writer, and every flush commits the positions it covered.
    pub fn with_wal(mut self, wal: WalWriter) -> Self {
        self.wal = Some(Arc::new(Mutex::new(wal)));
        self
    }

    /// Bytes the WAL currently occupies.
    pub async fn wal_bytes(&self) -> u64 {
        match &self.wal {
            Some(w) => w.lock().await.size_bytes(),
            None => 0,
        }
    }

    fn publish(&self, meta: BlockMetadata, contention: Option<&ContentionMetrics>) {
        record_block_written(&meta, contention, SignalType::Metrics);
        let _ = self.metadata_tx.send(meta);
    }

    /// Total rows currently buffered, from the atomic counter.
    ///
    /// Only used for logging and tests; the hot capacity check reads the
    /// atomic directly.
    pub fn buffered_rows(&self) -> usize {
        self.buffered.load(Ordering::Relaxed)
    }

    /// Sum of the shard writers' actual row counts. Test/diagnostic only:
    /// comparing this against [`Self::buffered_rows`] proves the counter
    /// tracks reality.
    pub async fn actual_rows(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.lock().await.len();
        }
        total
    }

    /// Pushes a metric, flushing first if it would overflow the block cap.
    ///
    /// Checking *before* the push bounds each block at
    /// `max_rows_per_block` plus at most one request, which is what
    /// `max_rows_per_block` is documented to mean. Checking only afterwards
    /// let concurrent requests overshoot: with 16 shards and 40 clients, one
    /// block was measured at twice the cap.
    ///
    /// A metric larger than a whole block is split across as many blocks as it
    /// needs rather than being partially accepted and then reported as an
    /// error (which made clients retry a batch already half-ingested).
    ///
    /// Returns `true` if any flush happened, so the caller drains the memory
    /// buffer and the flushed rows are not read twice.
    pub async fn push(
        &self,
        metric: Metric,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        let idx = shard_for(&metric.name, self.shards.len());
        let points = metric.data_points.len();
        let mut flushed = false;

        let cap = self.config.max_rows_per_block;
        // `buffered + points > cap`, rearranged so neither side can overflow.
        let would_overflow = points > cap || self.buffered.load(Ordering::Relaxed) > cap - points;
        if would_overflow {
            self.flush_if_over_capacity(contention).await?;
            flushed = true;
        }

        let mut closed = 0usize;
        {
            let waited = Instant::now();
            let mut shard = self.shards[idx].lock().await;
            if let Some(c) = contention {
                c.record_ingest_lock_wait(SignalType::Metrics, waited.elapsed());
            }
            // Logged while the shard lock is held, so WAL order and writer
            // order are the same total order. A failure here is propagated:
            // acknowledging data we could not log would make the WAL a lie.
            if let Some(wal) = &self.wal {
                // The shard lock is held across this append so that WAL order
                // and writer order are the same total order.
                let pos = wal.lock().await.append(&metric)?;
                *self.covered[idx].lock().unwrap_or_else(|e| e.into_inner()) = pos;
            }
            // A single shard may be near its own capacity; let the writer
            // close a block rather than reject the points.
            for meta in shard.push(metric.clone())? {
                closed += meta.row_count;
                flushed = true;
                self.publish(meta, contention);
            }
            // Adjust the counter while still holding the shard lock: a flush
            // subtracts a shard's rows and swaps the writer under the same
            // lock, so counting after releasing it could count the same rows
            // twice.
            //
            // The two adjustments are deliberately separate. Every pushed point
            // is now in the writer, so all `points` are added. Separately, `closed`
            // rows left through the writer's own capacity split — and those were
            // added by *earlier* pushes, so they must be subtracted.
            //
            // Folding these into one `points - closed` was wrong whenever
            // `closed > points`, which is exactly the common case of pushing
            // into a full writer: a 20-point push into a full 2000-row writer
            // closed 2000 and added 20, so the subtraction saturated to zero and
            // neither the 20 new rows nor the 2000 departing rows were tracked.
            // The counter then drifted upward by a block's worth on every such
            // push, pinned itself above the cap, and turned every subsequent
            // push into a flush of a single metric — 2263 blocks of ~20 rows
            // instead of 24 of 2000.
            self.buffered.fetch_add(points, Ordering::Relaxed);
            self.buffered.fetch_sub(closed, Ordering::Relaxed);
        }
        Ok(flushed)
    }

    pub async fn check_and_flush(&self, contention: Option<&ContentionMetrics>) -> Result<bool> {
        if unix_millis().saturating_sub(self.last_flush_unix_ms.load(Ordering::Relaxed))
            >= self.max_duration_ms
        {
            self.flush(contention).await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Flushes if the buffered rows have reached the cap.
    ///
    /// Takes the flush lock *before* re-checking, so two requests that both
    /// crossed the cap produce one flush rather than two.
    async fn flush_if_over_capacity(&self, contention: Option<&ContentionMetrics>) -> Result<()> {
        let _permit = self.flush_lock.lock().await;
        if self.buffered.load(Ordering::Relaxed) < self.config.max_rows_per_block {
            // Another request's flush already covered the cap.
            return Ok(());
        }
        self.flush_locked(contention).await
    }

    /// Writes every shard's buffered rows to one Parquet block.
    ///
    /// The shard buffers are taken by swapping each writer out under its own
    /// short-lived lock; the encode then runs on the blocking pool with **no
    /// lock held**, which is the whole point of the sharding: concurrent
    /// requests keep pushing into the fresh buffers while this encodes.
    ///
    /// Idempotent on an empty set of shards.
    pub async fn flush(&self, contention: Option<&ContentionMetrics>) -> Result<()> {
        let _permit = self.flush_lock.lock().await;
        self.flush_locked(contention).await
    }

    /// Flush body, with the flush lock already held.
    async fn flush_locked(&self, contention: Option<&ContentionMetrics>) -> Result<()> {
        let mut writers = Vec::with_capacity(self.shards.len());
        let mut row_count = 0usize;
        // Highest WAL position among the rows this block will actually
        // contain. Taken per shard under that shard's own lock, at the moment
        // its writer is swapped, so it can never include a row that lands in
        // the *next* block.
        let mut covered = WalPosition::START;
        for (i, shard) in self.shards.iter().enumerate() {
            let mut guard = shard.lock().await;
            if guard.is_empty() {
                continue;
            }
            let taken = guard.len();
            row_count += taken;
            // Taken and reset while this shard's lock is held, so the value
            // always matches the writer being swapped.
            let mut slot = self.covered[i].lock().unwrap_or_else(|e| e.into_inner());
            covered = covered.max(*slot);
            *slot = WalPosition::START;
            // Subtract while still holding this shard's lock, so a concurrent
            // push either lands before this (and is subtracted here) or after
            // (and counts up from the decremented total). A `store(0)` after
            // the loop would erase the latter.
            self.buffered.fetch_sub(taken, Ordering::Relaxed);
            writers.push(std::mem::replace(
                &mut *guard,
                BlockWriter::new(self.config.clone()),
            ));
        }
        if writers.is_empty() {
            tracing::debug!("metric block flush skipped: all shards empty");
            return Ok(());
        }

        let started = std::time::Instant::now();
        let flush_guard = contention.map(|c| c.flush_started(SignalType::Metrics));
        let encoded = tokio::task::spawn_blocking(move || merge_and_flush(writers)).await;
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
        // Only one flush runs at a time (flush_lock), so one relaxed store is
        // enough to publish the new flush time.
        self.last_flush_unix_ms
            .store(unix_millis(), Ordering::Relaxed);
        let _ = self.metadata_tx.send(metadata);
        // Committed only now that the block is renamed. A crash before this
        // leaves the block unindexed and its WAL rows intact, so replay
        // rewrites them with no duplicate — see the ordering table in wal.rs.
        if let Some(wal) = &self.wal {
            wal.lock().await.commit(covered)?;
        }
        tracing::debug!(
            signal = "metrics",
            rows = row_count,
            duration_ms = started.elapsed().as_millis(),
            "metric block flushed to parquet"
        );
        Ok(())
    }
}

/// Merges every shard writer's rows into one block and encodes it.
fn merge_and_flush(writers: Vec<BlockWriter>) -> Result<BlockMetadata> {
    let mut merged = BlockWriter::merge(writers)?;
    merged.flush()
}

/// Handles automatic rotation and flushing of log blocks.
pub struct LogRotator {
    writer: LogWriter,
    config: LogBlockConfig,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
    /// WAL for logs, when enabled. Appended under the rotator lock, which the
    /// caller already holds, so WAL order and writer order are the same total
    /// order — the property that makes the committed position correct.
    wal: Option<WalWriter>,
    /// Highest WAL position among the rows currently in `writer`.
    covered: WalPosition,
}

impl LogRotator {
    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: LogWriter::new(config.clone()),
            wal: None,
            covered: WalPosition::START,
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

    /// Attaches a WAL. After this, every accepted record is logged before it
    /// reaches the writer.
    pub fn with_wal(mut self, wal: WalWriter) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Bytes the log WAL currently occupies.
    pub fn wal_bytes(&self) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.size_bytes())
    }

    pub async fn push(
        &mut self,
        log: LogRecord,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        // Logged first, and failures propagate: acknowledging a record we
        // could not log would make the WAL a lie.
        if let Some(wal) = self.wal.as_mut() {
            self.covered = wal.append(&log)?;
        }
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
        // Snapshot before the writer is taken: the block will contain exactly
        // the rows whose WAL positions are at or below this.
        let covered = self.covered;
        self.covered = WalPosition::START;
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
        // Committed only now the block is renamed; see wal.rs for why the
        // order matters.
        if let Some(wal) = self.wal.as_mut() {
            wal.commit(covered)?;
        }
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
    rotator: Arc<BlockRotator>,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
}

impl IngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self::with_shards(config, metadata_tx, 1, None)
    }

    /// Creates a service whose rotator uses `shards` writer shards, with an
    /// optional write-ahead log.
    ///
    /// See [`BlockRotator`] for why the shards are merged into one block
    /// rather than each producing their own.
    pub fn with_shards(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
        shards: usize,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = BlockRotator::with_shards(config, metadata_tx, shards);
        if let Some(wal) = wal {
            rotator = rotator.with_wal(wal);
        }
        Self {
            rotator: Arc::new(rotator),
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            contention: None,
        }
    }

    /// Number of writer shards in use.
    pub fn rotator_shards(&self) -> usize {
        self.rotator.shard_count()
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
        for m in metrics {
            count += m.data_points.len() as u64;
            // The rotator measures its own per-shard lock wait, so the push is
            // not wrapped in the service-level measurement the single-lock
            // design needed.
            if self.rotator.push(m, contention.as_deref()).await? {
                flushed = true;
            }
        }
        if self.rotator.check_and_flush(contention.as_deref()).await? {
            flushed = true;
        }
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
        let flushed = self.rotator.check_and_flush(contention.as_deref()).await?;
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_offloaded(SignalType::Metrics).await;
            }
        }
        Ok(flushed)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let _ = self.rotator.flush(contention.as_deref()).await;
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
    /// Bytes the log WAL currently occupies.
    pub async fn wal_bytes(&self) -> u64 {
        self.rotator.lock().await.wal_bytes()
    }

    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self::with_wal(config, metadata_tx, None)
    }

    /// Creates the service with an optional write-ahead log.
    pub fn with_wal(
        config: LogBlockConfig,
        metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = LogRotator::new(config, metadata_tx);
        if let Some(w) = wal {
            rotator = rotator.with_wal(w);
        }
        Self {
            rotator: Arc::new(Mutex::new(rotator)),
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
        self.ingest_logs(logs).await
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
        self.ingest_logs(logs).await
    }

    /// Ingests already-decoded log records.
    ///
    /// Public because WAL replay feeds recovered records back through exactly
    /// this path, so recovered telemetry is treated identically to live data.
    pub async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<u64> {
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
    /// WAL for traces, when enabled. Same contract as [`LogRotator::wal`].
    wal: Option<WalWriter>,
    /// Highest WAL position among the rows currently in `writer`.
    covered: WalPosition,
}

impl TraceRotator {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: TraceWriter::new(config.clone()),
            wal: None,
            covered: WalPosition::START,
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

    /// Attaches a WAL. After this, every accepted span is logged before it
    /// reaches the writer.
    pub fn with_wal(mut self, wal: WalWriter) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Bytes the trace WAL currently occupies.
    pub fn wal_bytes(&self) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.size_bytes())
    }

    pub async fn push(
        &mut self,
        span: Span,
        contention: Option<&ContentionMetrics>,
    ) -> Result<bool> {
        if let Some(wal) = self.wal.as_mut() {
            self.covered = wal.append(&span)?;
        }
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
        // Snapshot before the writer is taken; see LogRotator::flush.
        let covered = self.covered;
        self.covered = WalPosition::START;
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
        // Committed only now the block is renamed; see wal.rs.
        if let Some(wal) = self.wal.as_mut() {
            wal.commit(covered)?;
        }
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
        Self::with_wal(config, metadata_tx, None)
    }

    /// Creates the service with an optional write-ahead log.
    pub fn with_wal(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = TraceRotator::new(config, metadata_tx);
        if let Some(w) = wal {
            rotator = rotator.with_wal(w);
        }
        Self {
            rotator: Arc::new(Mutex::new(rotator)),
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
        self.ingest_spans(spans).await
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
        self.ingest_spans(spans).await
    }

    /// Ingests already-decoded spans.
    ///
    /// Public because WAL replay feeds recovered records back through exactly
    /// this path, so recovered telemetry is treated identically to live data.
    pub async fn ingest_spans(&self, spans: Vec<Span>) -> Result<u64> {
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
    use parqtel_core::wal::WalSyncMode;
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

    /// A minimal span, for the WAL tests.
    fn test_span(start: i64) -> Span {
        Span {
            trace_id: [7u8; 16],
            span_id: [start as u8; 8],
            trace_state: String::new(),
            name: "op".into(),
            kind: 2,
            start_time_ns: start,
            end_time_ns: start + 50,
            attributes: parqtel_core::LabelSet::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: parqtel_core::models::traces::SpanStatus {
                code: 1,
                message: String::new(),
            },
            parent_span_id: [0u8; 8],
            flags: 1,
        }
    }

    /// A one-point metric at `ts` nanoseconds, named `name`.
    fn metric_named(name: &str, ts: i64) -> Metric {
        Metric {
            name: name.into(),
            kind: parqtel_core::MetricKind::Gauge,
            data_points: vec![parqtel_core::DataPoint::new(
                ts.max(1),
                parqtel_core::MetricValue::Double(ts as f64),
                parqtel_core::LabelSet::default(),
            )
            .unwrap()],
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
        let rotator = BlockRotator::new(config, tx);

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
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

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
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

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
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        // Ten points exactly fill the cap: no flush, because the check is
        // "would this push overflow it" — so a block is never larger than the
        // cap plus one request.
        for i in 0..10 {
            let flushed = rotator
                .push(one_point_metric("m1", 100 + i), Some(&contention))
                .await
                .unwrap();
            assert!(!flushed, "no flush while the batch still fits");
        }
        assert_eq!(contention.flush_duration_count(SignalType::Metrics), 0);
        assert_eq!(rotator.buffered_rows(), 10, "the counter tracks the rows");

        // The 11th point would overflow, so the block is closed first. That
        // push must report the flush so the caller drains the memory buffer and
        // the flushed rows are not read twice.
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

    /// The shards exist to remove cross-metric contention, so a metric must
    /// always route to the same shard — otherwise one metric's rows scatter
    /// across buffers and the merge becomes order-dependent.
    #[tokio::test]
    async fn test_shard_routing_is_stable_and_bounded() {
        for count in [1usize, 2, 4, 8] {
            for name in ["cpu", "memory_used", "http_requests_total", "a", "zzz"] {
                let a = shard_for(name, count);
                assert!(a < count, "shard {a} out of range for {count} shards");
                assert_eq!(
                    shard_for(name, count),
                    a,
                    "routing for {name} must be stable across calls"
                );
            }
        }
        // Distinct metrics must be able to land on different shards, or the
        // sharding buys nothing.
        let spread: std::collections::HashSet<usize> = (0..64)
            .map(|i| shard_for(&format!("metric_{i}"), 8))
            .collect();
        assert!(spread.len() > 1, "shards must actually spread load");
    }

    /// Zero or absurd shard counts must degrade gracefully: 1 restores the
    /// previous single-writer behaviour and the upper bound keeps a bad config
    /// from allocating hundreds of writer buffers.
    #[tokio::test]
    async fn test_shard_count_is_clamped() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        for (configured, expected) in [(0usize, 1usize), (1, 1), (4, 4), (10_000, 256)] {
            let rotator =
                BlockRotator::with_shards(tiny_metrics_config(dir.path()), tx.clone(), configured);
            assert_eq!(
                rotator.shard_count(),
                expected,
                "configured {configured} should clamp to {expected}"
            );
        }
    }

    /// The whole point of sharding the locks without sharding the blocks: a
    /// flush must still write **one** file, no matter how many shards the rows
    /// came from. If this regresses to one block per shard, query fan-out
    /// multiplies silently.
    #[tokio::test]
    async fn test_sharded_flush_writes_one_block() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = ContentionMetrics::new();
        // A cap comfortably above the row count, so no rotation is triggered
        // by the cap check and the explicit flush below is the only writer.
        let rotator = BlockRotator::with_shards(
            BlockConfig {
                max_rows_per_block: 100,
                row_group_size: 50,
                block_duration_secs: 3600,
                ..tiny_metrics_config(dir.path())
            },
            tx,
            4,
        );
        assert_eq!(rotator.shard_count(), 4);

        // 16 distinct metric names spread over 4 shards.
        for i in 0..16 {
            rotator
                .push(
                    one_point_metric(&format!("m{i}"), 100 + i),
                    Some(&contention),
                )
                .await
                .unwrap();
        }
        rotator.flush(Some(&contention)).await.unwrap();

        let mut metas = Vec::new();
        while let Ok(meta) = rx.try_recv() {
            metas.push(meta);
        }
        assert_eq!(
            metas.len(),
            1,
            "shards must merge into a single block, got {} blocks",
            metas.len()
        );
        assert_eq!(
            metas[0].row_count, 16,
            "every shard's rows must be in the merged block"
        );
        assert_eq!(contention.flush_rows(SignalType::Metrics), 16);
    }

    /// A shard must not rotate its own block just because *its* slice of the
    /// cap is full — the cap is a whole-block budget. Rotating per shard would
    /// put back the block-count multiplication the merge exists to avoid.
    #[tokio::test]
    async fn test_shards_do_not_rotate_early_on_their_own_slice() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Cap 16, 4 shards: each shard's share is 4 rows. Filling all four to
        // exactly the share must not produce four blocks.
        let rotator = BlockRotator::with_shards(
            BlockConfig {
                max_rows_per_block: 16,
                row_group_size: 8,
                block_duration_secs: 3600,
                ..tiny_metrics_config(dir.path())
            },
            tx,
            4,
        );

        // 15 of 16 rows: below the cap, so nothing may be written even though
        // each shard already holds more than a quarter of it.
        for i in 0..15 {
            rotator
                .push(one_point_metric(&format!("m{i}"), 100 + i), None)
                .await
                .unwrap();
        }
        assert!(
            rx.try_recv().is_err(),
            "no block may be written before the whole-block cap is reached"
        );

        // The 16th row reaches the cap: exactly one merged block.
        rotator
            .push(one_point_metric("m15", 115), None)
            .await
            .unwrap();
        rotator.flush(None).await.unwrap();
        let mut metas = Vec::new();
        while let Ok(meta) = rx.try_recv() {
            metas.push(meta);
        }
        assert_eq!(metas.len(), 1, "the four shards must merge into one block");
        assert_eq!(metas[0].row_count, 16);
    }

    /// The buffered-row counter is what gates rotation, so it must track the
    /// writers exactly. A drift upward would mean the cap never fires again and
    /// blocks grow without bound; a drift downward would mean constant
    /// pointless flushes.
    #[tokio::test]
    async fn test_buffered_counter_tracks_the_writers() {
        let dir = tempdir().unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let rotator = BlockRotator::with_shards(
            BlockConfig {
                max_rows_per_block: 1000,
                row_group_size: 500,
                block_duration_secs: 3600,
                ..tiny_metrics_config(dir.path())
            },
            tx,
            4,
        );

        assert_eq!(rotator.buffered_rows(), 0);
        for i in 0..37 {
            rotator
                .push(one_point_metric(&format!("m{}", i % 8), 100 + i), None)
                .await
                .unwrap();
        }
        assert_eq!(
            rotator.buffered_rows(),
            37,
            "counter must equal pushed rows"
        );

        rotator.flush(None).await.unwrap();
        assert_eq!(rotator.buffered_rows(), 0, "a flush must zero the counter");

        // A mid-metric split inside the writer must not leave the counter high:
        // those rows left through the writer, not through `flush_locked`.
        let split = BlockRotator::with_shards(
            BlockConfig {
                max_rows_per_block: 5,
                row_group_size: 5,
                block_duration_secs: 3600,
                ..tiny_metrics_config(dir.path())
            },
            tx2(),
            1,
        );
        let points: Vec<_> = (0..13)
            .map(|i| {
                parqtel_core::DataPoint::new(
                    100 + i,
                    parqtel_core::MetricValue::Double(1.0),
                    parqtel_core::LabelSet::default(),
                )
                .unwrap()
            })
            .collect();
        split
            .push(
                Metric {
                    name: "split".into(),
                    kind: parqtel_core::MetricKind::Gauge,
                    data_points: points,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            split.buffered_rows(),
            13 - 10,
            "the 10 rows the writer closed mid-metric must not stay counted"
        );
    }

    fn tx2() -> mpsc::UnboundedSender<BlockMetadata> {
        mpsc::unbounded_channel().0
    }

    /// Regression test for a counter bug that only appeared under contention.
    ///
    /// The buffered-row counter drives rotation, so a drift in either
    /// direction is serious: upward pins it above the cap and turns every push
    /// into a flush (measured: 2263 blocks of ~20 rows instead of 24 of 2000);
    /// downward rotates constantly for no reason. An earlier implementation
    /// folded "rows added" and "rows the writer flushed internally" into one
    /// `points - closed`, which saturated to zero whenever a push landed in a
    /// full writer and silently lost both adjustments.
    ///
    /// Compares the counter against the shard writers' real contents at the cap
    /// boundary, for one shard and several.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_buffered_counter_matches_writers_under_contention() {
        for shards in [1usize, 4] {
            let dir = tempfile::tempdir().unwrap();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let rotator = Arc::new(BlockRotator::with_shards(
                BlockConfig {
                    max_rows_per_block: 2000,
                    row_group_size: 1000,
                    block_duration_secs: 3600,
                    ..tiny_metrics_config(dir.path())
                },
                tx,
                shards,
            ));

            let mut handles = Vec::new();
            for w in 0..40 {
                let rotator = rotator.clone();
                handles.push(tokio::spawn(async move {
                    for i in 0..60 {
                        let dps: Vec<_> = (0..20)
                            .map(|k| {
                                parqtel_core::DataPoint::new(
                                    1_000_000 * (i as i64 + 1) + k,
                                    parqtel_core::MetricValue::Double(1.0),
                                    parqtel_core::LabelSet::default(),
                                )
                                .unwrap()
                            })
                            .collect();
                        rotator
                            .push(
                                Metric {
                                    name: format!("metric_{}", (w * 60 + i) % 60),
                                    kind: parqtel_core::MetricKind::Gauge,
                                    data_points: dps,
                                    ..Default::default()
                                },
                                None,
                            )
                            .await
                            .unwrap();
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }

            assert_eq!(
                rotator.buffered_rows(),
                rotator.actual_rows().await,
                "shards={shards}: counter must equal the writers' real contents"
            );

            rotator.flush(None).await.unwrap();
            assert_eq!(
                rotator.buffered_rows(),
                0,
                "shards={shards}: a flush must zero the counter"
            );
            assert_eq!(rotator.actual_rows().await, 0);

            // Blocks must respect the cap rather than degenerating into one
            // per push, and no block may exceed it by more than one request.
            const TOTAL: usize = 40 * 60 * 20;
            const REQUEST: usize = 60 * 20;
            let mut blocks = Vec::new();
            while let Ok(m) = rx.try_recv() {
                blocks.push(m.row_count);
            }
            let written: usize = blocks.iter().sum();
            assert_eq!(written, TOTAL, "shards={shards}: no rows may be lost");
            assert!(
                blocks.len() <= TOTAL / 1000 + 40,
                "shards={shards}: {} blocks for {TOTAL} rows indicates a flush per push",
                blocks.len()
            );
            assert!(
                blocks.iter().all(|&b| b <= 2000 + REQUEST),
                "shards={shards}: a block may exceed the cap by at most one request"
            );
        }
    }

    /// Concurrent pushes across shards must not lose or duplicate rows, and a
    /// concurrent flush must not swallow a push that arrives mid-encode.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_sharded_pushes_lose_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = Arc::new(ContentionMetrics::new());
        let rotator = Arc::new(BlockRotator::with_shards(
            BlockConfig {
                max_rows_per_block: 50,
                row_group_size: 50,
                block_duration_secs: 3600,
                ..tiny_metrics_config(dir.path())
            },
            tx,
            4,
        ));

        const WORKERS: usize = 8;
        const PER_WORKER: usize = 25;
        let mut handles = Vec::new();
        for w in 0..WORKERS {
            let rotator = rotator.clone();
            let contention = contention.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..PER_WORKER {
                    // Distinct names route to distinct shards.
                    let name = format!("m{}_{i}", w % 4);
                    rotator
                        .push(
                            one_point_metric(&name, 1000 * (i as i64 + 1)),
                            Some(&contention),
                        )
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        rotator.flush(Some(&contention)).await.unwrap();

        let total: usize = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|m: BlockMetadata| m.row_count)
            .sum();
        assert_eq!(
            total,
            WORKERS * PER_WORKER,
            "every concurrently pushed point must be written exactly once"
        );
    }

    /// A single metric larger than a whole block must be split across blocks
    /// and ingested in full — not partially accepted and then reported as an
    /// error, which made clients retry a batch that was already half-ingested.
    #[tokio::test]
    async fn test_oversized_metric_is_split_across_blocks() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = ContentionMetrics::new();
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

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
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

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
    /// The property the WAL exists for: a crash must lose nothing that was
    /// acknowledged, and must not duplicate anything that was already
    /// flushed.
    ///
    /// Simulated by dropping the service without flushing (a crash) and then
    /// replaying into a fresh one, which is exactly what startup does.
    #[tokio::test]
    async fn test_wal_recovers_acknowledged_but_unflushed_points() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let config = BlockConfig {
            data_dir: data_dir.clone(),
            max_rows_per_block: 10_000, // nothing flushes on the cap
            block_duration_secs: 3600,  // nor on the timer
            ..Default::default()
        };

        // First "process": accept points, acknowledge, then crash.
        {
            let (tx, _rx) = mpsc::unbounded_channel();
            let svc = IngestionService::with_shards(
                config.clone(),
                tx,
                2,
                Some(
                    WalWriter::open_with_segment_limit(
                        &data_dir,
                        "metrics",
                        WalSyncMode::Always,
                        Duration::from_millis(0),
                        64 * 1024 * 1024,
                    )
                    .unwrap(),
                ),
            );
            for i in 0..5i64 {
                let n = svc
                    .ingest_metrics(vec![metric_named(&format!("m{i}"), i)])
                    .await
                    .unwrap();
                assert_eq!(n, 1, "the request must be acknowledged");
            }
            // Dropped without shutdown(): no flush, no commit.
        }

        // The WAL must hold the acknowledged points.
        let commit = parqtel_core::wal::read_commit(&data_dir, "metrics");
        let mut recovered = Vec::new();
        // One record per metric: the rotator appends while it holds the shard
        // lock so WAL order and writer order are the same total order.
        let stats = parqtel_core::wal::replay::<Metric, _>(&data_dir, "metrics", commit, |m| {
            recovered.push(m)
        })
        .unwrap();
        for e in std::fs::read_dir(dir.path().join("wal").join("metrics"))
            .unwrap()
            .flatten()
        {
            eprintln!(
                "  file {:?} size {:?}",
                e.path(),
                e.metadata().map(|m| m.len())
            );
        }
        assert_eq!(stats.records, 5, "every acknowledged batch must replay");
        assert_eq!(stats.skipped, 0);
        let names: Vec<String> = recovered.iter().map(|m| m.name.clone()).collect();
        assert_eq!(names, vec!["m0", "m1", "m2", "m3", "m4"]);
    }

    /// After a successful flush the WAL must be committed, so a replay finds
    /// nothing to redo. Without this every restart would re-ingest every block
    /// ever written.
    #[tokio::test]
    async fn test_wal_is_committed_once_the_block_is_durable() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let config = BlockConfig {
            data_dir: data_dir.clone(),
            max_rows_per_block: 10_000,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let svc = IngestionService::with_shards(
            config.clone(),
            tx,
            2,
            Some(
                WalWriter::open_with_segment_limit(
                    &data_dir,
                    "metrics",
                    WalSyncMode::Always,
                    Duration::from_millis(0),
                    64 * 1024 * 1024,
                )
                .unwrap(),
            ),
        );

        for i in 0..4i64 {
            svc.ingest_metrics(vec![metric_named(&format!("m{i}"), i)])
                .await
                .unwrap();
        }
        svc.shutdown().await.unwrap();
        let meta = rx.try_recv().expect("a block must have been written");
        assert_eq!(meta.row_count, 4);
        assert!(meta.path.exists(), "the block must be renamed into place");

        let commit = parqtel_core::wal::read_commit(&data_dir, "metrics");
        let mut seen = 0u64;
        let stats =
            parqtel_core::wal::replay::<Metric, _>(&data_dir, "metrics", commit, |_| seen += 1)
                .unwrap();
        assert_eq!(seen, 0, "flushed rows must not be replayed");
        assert!(stats.records == 0);
        assert!(stats.skipped > 0, "they must be counted as skipped");
    }

    /// Logs and traces get the same crash-recovery contract as metrics: a
    /// process death must lose nothing acknowledged, and a graceful flush must
    /// commit the WAL so a restart does not re-ingest it.
    #[tokio::test]
    async fn test_log_and_trace_wal_recover_and_commit() {
        for signal in ["logs", "traces"] {
            let dir = tempdir().unwrap();
            // Logs live in their own data dir; trace blocks share the metrics
            // one, because TraceWriter is built from config.storage.
            let data_dir = if signal == "logs" {
                dir.path().join("logs")
            } else {
                dir.path().to_path_buf()
            };
            std::fs::create_dir_all(&data_dir).unwrap();
            let open = || {
                WalWriter::open_with_segment_limit(
                    &data_dir,
                    signal,
                    WalSyncMode::Always,
                    Duration::from_millis(0),
                    64 * 1024 * 1024,
                )
                .unwrap()
            };
            let log_config = LogBlockConfig {
                data_dir: data_dir.clone(),
                max_rows_per_block: 10_000,
                block_duration_secs: 3600,
                ..Default::default()
            };
            let block_config = BlockConfig {
                data_dir: data_dir.clone(),
                max_rows_per_block: 10_000,
                block_duration_secs: 3600,
                ..Default::default()
            };

            // Crash: accept, acknowledge, drop without flushing.
            {
                let (ltx, _lrx) = mpsc::unbounded_channel();
                let (ttx, _trx) = mpsc::unbounded_channel();
                let logs_svc = LogIngestionService::with_wal(log_config.clone(), ltx, Some(open()));
                let traces_svc =
                    TraceIngestionService::with_wal(block_config.clone(), ttx, Some(open()));

                for i in 0..3i64 {
                    let l = parqtel_core::LogRecord::new(
                        i + 1,
                        i + 1,
                        9,
                        "INFO".into(),
                        format!("log-{i}"),
                        parqtel_core::LabelSet::default(),
                        parqtel_core::LabelSet::default(),
                        [0u8; 16],
                        [0u8; 8],
                        0,
                        "".into(),
                        "".into(),
                    );
                    assert_eq!(logs_svc.ingest_logs(vec![l]).await.unwrap(), 1);

                    let mut sp = test_span(i + 1);
                    sp.start_time_ns = i + 1;
                    sp.end_time_ns = i + 100;
                    assert_eq!(traces_svc.ingest_spans(vec![sp]).await.unwrap(), 1);
                }
            }

            let commit = parqtel_core::wal::read_commit(&data_dir, signal);
            let mut lrec = Vec::new();
            let lstats = parqtel_core::wal::replay::<parqtel_core::LogRecord, _>(
                &data_dir,
                signal,
                commit,
                |r| lrec.push(r),
            )
            .unwrap();
            let mut srec = Vec::new();
            let sstats = parqtel_core::wal::replay::<parqtel_core::Span, _>(
                &data_dir,
                signal,
                commit,
                |r| srec.push(r),
            )
            .unwrap();

            assert_eq!(
                lstats.records, 3,
                "{signal}: every acknowledged log must replay"
            );
            assert_eq!(
                sstats.records, 3,
                "{signal}: every acknowledged span must replay"
            );
            assert_eq!(
                lrec.iter().map(|l| l.body.clone()).collect::<Vec<_>>(),
                vec!["log-0", "log-1", "log-2"]
            );
            assert_eq!(srec.len(), 3);

            // Graceful flush commits, so a second replay finds nothing.
            let (ltx, mut lrx) = mpsc::unbounded_channel();
            let (ttx, mut trx) = mpsc::unbounded_channel();
            let logs_svc = LogIngestionService::with_wal(log_config.clone(), ltx, Some(open()));
            let traces_svc =
                TraceIngestionService::with_wal(block_config.clone(), ttx, Some(open()));
            // Ingest again so the writers are non-empty and `shutdown` really
            // flushes — an empty writer short-circuits before the commit, which
            // would leave the earlier records uncommitted and make this phase
            // assert the wrong thing.
            let l = parqtel_core::LogRecord::new(
                99,
                99,
                9,
                "INFO".into(),
                "log-flushed".into(),
                parqtel_core::LabelSet::default(),
                parqtel_core::LabelSet::default(),
                [0u8; 16],
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            );
            logs_svc.ingest_logs(vec![l]).await.unwrap();
            traces_svc.ingest_spans(vec![test_span(99)]).await.unwrap();
            logs_svc.shutdown().await.unwrap();
            traces_svc.shutdown().await.unwrap();
            assert!(lrx.try_recv().is_ok(), "{signal}: a block must be written");
            assert!(trx.try_recv().is_ok(), "{signal}: a block must be written");

            let commit = parqtel_core::wal::read_commit(&data_dir, signal);
            let after = parqtel_core::wal::replay::<serde_json::Value, _>(
                &data_dir,
                signal,
                commit,
                |_| {},
            )
            .unwrap();
            assert_eq!(
                after.records, 0,
                "{signal}: flushed rows must not be replayed again"
            );
        }
    }

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
        let rotator = BlockRotator::new(config, tx);

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
