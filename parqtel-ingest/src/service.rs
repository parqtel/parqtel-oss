use crate::decode::OtlpDecoder;
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use crate::writer::{BlockMetadata, BlockWriter, LogWriter, TraceWriter};
use bytes::Bytes;
use parqtel_core::storage::PendingIndex;
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
    /// Serialises flush *initiation* so two requests cannot each swap the
    /// buffers and produce overlapping blocks. With a worker running it is held
    /// only for the swap and the hand-off, not for the encode.
    flush_lock: tokio::sync::Mutex<()>,
    /// Channel to the background flush worker, when one is running.
    ///
    /// Behind a mutex because shutdown only has `&self`: the rotator is shared as
    /// an `Arc`, and awaiting the worker must not need exclusive access.
    flush_tx: std::sync::Mutex<Option<mpsc::Sender<FlushJob>>>,
    /// Handle to the worker, awaited on shutdown so nothing is left in flight.
    worker: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Buffer to drain once a flush is **durable**.
    ///
    /// The rotator owns this rather than the service because with a background
    /// worker the flush completes long after `push` returns. Draining in the
    /// service would empty the buffer before the rows were in any block, and they
    /// would be visible to neither the buffer nor a block.
    buffer: std::sync::Mutex<Option<MemoryBuffer>>,
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
    /// Orders WAL commits and buffer drops across concurrent flushes.
    order: Arc<std::sync::Mutex<FlushOrder>>,
    config: BlockConfig,
    /// Unix time of the last completed flush, in milliseconds. An atomic so
    /// the duration check needs no lock — flushes are serialised by
    /// `flush_lock`, so exactly one writer updates this at a time.
    last_flush_unix_ms: Arc<AtomicU64>,
    max_duration_ms: u64,
    metadata_tx: mpsc::UnboundedSender<PendingIndex>,
}

impl BlockRotator {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        Self::with_shards(config, metadata_tx, 1)
    }

    /// Creates a rotator with `shards` independent writer shards.
    ///
    /// `shards` is clamped to at least 1, so a zero-valued config yields the
    /// previous single-writer behaviour rather than an unusable rotator.
    pub fn with_shards(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<PendingIndex>,
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
            order: Arc::new(std::sync::Mutex::new(FlushOrder::default())),
            flush_lock: tokio::sync::Mutex::new(()),
            flush_tx: std::sync::Mutex::new(None),
            worker: std::sync::Mutex::new(None),
            buffer: std::sync::Mutex::new(None),
            buffered: AtomicUsize::new(0),
            max_duration_ms: config.block_duration_secs.saturating_mul(1000),
            config,
            last_flush_unix_ms: Arc::new(AtomicU64::new(unix_millis())),
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

    /// Gives the rotator the buffer to drain once a flush is durable.
    pub fn with_buffer(self, buffer: MemoryBuffer) -> Self {
        *self.buffer.lock().unwrap_or_else(|e| e.into_inner()) = Some(buffer);
        self
    }

    /// Attaches the buffer after the rotator has been wrapped in an `Arc`.
    pub fn set_buffer(&self, buffer: Option<MemoryBuffer>) {
        *self.buffer.lock().unwrap_or_else(|e| e.into_inner()) = buffer;
    }

    /// Starts the background flush worker.
    ///
    /// With a worker, `flush` only *schedules*: the shard writers are swapped
    /// out under their own short locks and the encode happens on a background
    /// task, so the triggering request is acknowledged as soon as the WAL has
    /// its rows. Durability is not weakened — the WAL append already happened.
    pub fn start_flush_worker(&self, max_in_flight: usize) {
        // The worker's whole contract is "the rows are already in the WAL, so
        // nothing is lost if this process dies before the encode lands". With no
        // WAL that contract is void, and running the worker would silently
        // acknowledge data that cannot be recovered.
        //
        // So refuse: leave the channel unset and let `flush_locked` take the
        // synchronous path, which is still acknowledged only once the block is
        // on disk. Degrading to the safe behaviour beats silently weakening
        // durability, and `wal_enabled = false` remains a legitimate operator
        // choice - just not one that can also have a background flush.
        if self.wal.is_none() {
            tracing::warn!(
                "metrics WAL is disabled; flushing synchronously so a request is never acknowledged before its data is recoverable"
            );
            return;
        }
        if self
            .flush_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
            return;
        }
        let max_in_flight = max_in_flight.max(1);
        // Queue depth and concurrent encodes share the same bound: the channel
        // is what makes a full queue detectable (and therefore fall back to an
        // inline encode) rather than growing without limit.
        let (tx, mut rx) = mpsc::channel::<FlushJob>(max_in_flight);
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let wal = self.wal.clone();
        let metadata_tx = self.metadata_tx.clone();
        // Shared with the workers so a *completed* background flush is what
        // resets the rotation clock; the field cannot be reassigned via `&self`.
        let last_flush = Arc::clone(&self.last_flush_unix_ms);
        let permits = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

        let handle = tokio::spawn(async move {
            // One task **per job**, bounded by the semaphore, rather than a
            // sequential loop: a single loop would encode one block at a time,
            // which is exactly the serialisation the worker exists to remove.
            let mut inflight = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    // Drain finished encodes first so the set does not grow.
                    Some(joined) = inflight.join_next(), if !inflight.is_empty() => {
                        if let Err(e) = joined {
                            tracing::error!(signal = "metrics", "flush task panicked: {e}");
                        }
                    }
                    job = rx.recv() => {
                        let Some(job) = job else { break };
                        let wal = wal.clone();
                        let metadata_tx = metadata_tx.clone();
                        let buffer = buffer.clone();
                        let permits = Arc::clone(&permits);
                        let last_flush = Arc::clone(&last_flush);
                        inflight.spawn(async move {
                            // Bound the encodes themselves; the channel bound
                            // only limits how many are queued.
                            if let Ok(_permit) =
                                Arc::clone(&permits).acquire_owned().await
                            {
                                let rows = job.row_count;
                                if let Err(e) =
                                    run_flush_job(job, &metadata_tx, wal.as_ref(), &buffer).await
                                {
                                    // The rows are still in the WAL, so an
                                    // encode failure is recoverable rather than
                                    // data loss. Log loudly and keep going.
                                    tracing::error!(
                                        signal = "metrics",
                                        rows,
                                        "background flush failed: {e}"
                                    );
                                }
                                last_flush.store(unix_millis(), Ordering::Relaxed);
                            }
                        });
                    }
                }
            }
            // Channel closed: let every queued encode finish before the worker
            // reports done, so a graceful shutdown cannot leave a block
            // half-written or skip a WAL commit.
            while inflight.join_next().await.is_some() {}
        });
        *self.worker.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        *self.flush_tx.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
    }

    /// Stops the worker after it finishes everything already queued.
    ///
    /// Dropping the sender closes the channel, so the loop ends once it has
    /// drained; awaiting the handle therefore waits for the outstanding
    /// encodes. That is what makes a graceful shutdown safe: no block is left
    /// half-written and no WAL commit is skipped.
    pub async fn stop_flush_worker(&self) {
        drop(
            self.flush_tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        );
        let handle = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
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
        // No WAL ordering requirement: these blocks were already committed by
        // the flush that is publishing them, so they must not wait again.
        let _ = self.metadata_tx.send(PendingIndex::untracked(meta));
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
        contention: Option<Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        self.push_buffered(metric, None, contention).await
    }

    /// As [`Self::push`], also adding `buffered` to the memory buffer.
    ///
    /// The buffer is fed **inside** the shard lock, next to the writer, so for
    /// every metric the buffer holds points in exactly the order the writer
    /// does. That is what lets a finished block remove precisely its own points
    /// (see `MemoryBuffer::drop_sealed_metrics`) and leave newer ones queryable.
    pub async fn push_buffered(
        &self,
        metric: Metric,
        buffered: Option<Vec<DataPoint>>,
        contention: Option<Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        let idx = shard_for(&metric.name, self.shards.len());
        let points = metric.data_points.len();
        let mut flushed = false;

        let cap = self.config.max_rows_per_block;
        // `buffered + points > cap`, rearranged so neither side can overflow.
        let would_overflow = points > cap || self.buffered.load(Ordering::Relaxed) > cap - points;
        if would_overflow {
            self.flush_if_over_capacity(contention.clone()).await?;
            flushed = true;
        }

        // The CPU half of logging a metric - serialising it and checksumming the
        // bytes - needs no lock, so it happens before any is taken. Inside the
        // shard lock only the ordered append remains.
        let wal_record = match &self.wal {
            Some(_) => Some(parqtel_core::wal::encode_record(&metric)?),
            None => None,
        };
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        let mut closed = 0usize;
        {
            let waited = Instant::now();
            let mut shard = self.shards[idx].lock().await;
            if let Some(c) = contention.as_ref() {
                c.record_ingest_lock_wait(SignalType::Metrics, waited.elapsed());
            }
            // Logged while the shard lock is held, so WAL order and writer
            // order are the same total order. A failure here is propagated:
            // acknowledging data we could not log would make the WAL a lie.
            if let (Some(wal), Some(record)) = (&self.wal, &wal_record) {
                let pos = wal.lock().await.append_encoded(record)?;
                *self.covered[idx].lock().unwrap_or_else(|e| e.into_inner()) = pos;
            }
            let epoch = shard.epoch();
            if let (Some(buffer), Some(points)) = (&buffer, &buffered) {
                buffer
                    .push_metrics_tagged(&metric.name, points, idx as u16, epoch)
                    .await;
            }
            // A single shard may be near its own capacity; let the writer
            // close a block rather than reject the points.
            for meta in shard.push(metric)? {
                closed += meta.row_count;
                flushed = true;
                self.publish(meta, contention.as_deref());
            }
            if closed > 0 {
                // The writer closed blocks by itself, so everything it held up
                // to its previous epoch is durable.
                if let Some(buffer) = &buffer {
                    let mut sealed = vec![0u64; self.shards.len()];
                    sealed[idx] = shard.epoch().saturating_sub(1);
                    buffer.drop_sealed_metrics(&sealed).await;
                }
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

    pub async fn check_and_flush(
        &self,
        contention: Option<Arc<ContentionMetrics>>,
    ) -> Result<bool> {
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
    async fn flush_if_over_capacity(
        &self,
        contention: Option<Arc<ContentionMetrics>>,
    ) -> Result<()> {
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
    pub async fn flush(&self, contention: Option<Arc<ContentionMetrics>>) -> Result<()> {
        let _permit = self.flush_lock.lock().await;
        self.flush_locked(contention).await
    }

    /// Flush body, with the flush lock already held.
    async fn flush_locked(&self, contention: Option<Arc<ContentionMetrics>>) -> Result<()> {
        let mut writers = Vec::with_capacity(self.shards.len());
        let mut sealed = vec![0u64; self.shards.len()];
        let mut row_count = 0usize;
        // Highest WAL position among the rows this block will actually contain.
        // Taken per shard under that shard's own lock, at the moment its writer
        // is swapped, so it can never include a row that lands in the *next*
        // block.
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
            // (and counts up from the decremented total). A `store(0)` after the
            // loop would erase the latter.
            self.buffered.fetch_sub(taken, Ordering::Relaxed);
            // The old writer's epoch is sealed; the replacement starts the next
            // one, so points buffered from here on are not part of this block.
            sealed[i] = guard.epoch();
            writers.push(std::mem::replace(
                &mut *guard,
                BlockWriter::with_epoch(self.config.clone(), sealed[i] + 1),
            ));
        }
        if writers.is_empty() {
            tracing::debug!("metric block flush skipped: all shards empty");
            return Ok(());
        }

        let seq = self.order.lock().unwrap_or_else(|e| e.into_inner()).begin();
        let job = FlushJob {
            seq,
            order: Arc::clone(&self.order),
            sealed,
            writers,
            covered,
            row_count,
            contention,
        };

        // Clone the sender out and drop the guard before any await: holding a
        // std MutexGuard across a suspension makes the future non-Send.
        let tx = self
            .flush_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some(tx) = tx else {
            // No worker configured: the original synchronous behaviour.
            return run_flush_job(job, &self.metadata_tx, self.wal.as_ref(), &buffer).await;
        };

        match tx.try_send(job) {
            // Queued: the caller is acknowledged now. The rows are already in the
            // WAL, so nothing is lost if this process dies before the encode
            // lands.
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(job)) => {
                tracing::warn!("flush queue full; encoding inline (backpressure)");
                run_flush_job(job, &self.metadata_tx, self.wal.as_ref(), &buffer).await
            }
            Err(mpsc::error::TrySendError::Closed(job)) => {
                // Worker gone (shutdown). Encode inline rather than drop.
                tracing::warn!("flush worker is gone; encoding inline");
                run_flush_job(job, &self.metadata_tx, self.wal.as_ref(), &buffer).await
            }
        }
    }
}

/// Orders the effects of concurrent flushes, so each takes hold only once every
/// earlier flush has.
///
/// Several blocks can be encoding at once. Two things must nevertheless happen
/// in flush order:
///
/// * **WAL commit.** A block covers a WAL range ending at its own position. If a
///   later block finished first and committed, the commit point would pass an
///   earlier block that is not on disk yet, and a crash then would lose its
///   rows: replay skips everything at or below the commit. A block that fails
///   never completes here, which holds the commit back - the WAL keeps those
///   rows and a restart replays them - rather than letting later blocks commit
///   past it.
/// * **Buffer drop.** Rows leave the memory buffer once a block holds them. If a
///   later block's rows left first, an earlier block's rows would be in neither
///   the buffer nor a block until it landed. A failed flush releases this one
///   (with nothing to drop), because its rows stay in the buffer and only the
///   WAL.
#[derive(Default)]
struct FlushOrder {
    next_seq: u64,
    wal_frontier: u64,
    wal_done: std::collections::BTreeMap<u64, WalPosition>,
    drop_frontier: u64,
    drop_done: std::collections::BTreeMap<u64, Vec<u64>>,
}

impl FlushOrder {
    /// Numbers a new flush. Called in flush order, under the flush lock.
    fn begin(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Records that flush `seq` is durable, returning the position that may now
    /// be committed, if the frontier moved.
    fn wal_complete(&mut self, seq: u64, covered: WalPosition) -> Option<WalPosition> {
        self.wal_done.insert(seq, covered);
        let mut advanced: Option<WalPosition> = None;
        while let Some(p) = self.wal_done.remove(&self.wal_frontier) {
            advanced = Some(advanced.map_or(p, |a| a.max(p)));
            self.wal_frontier += 1;
        }
        advanced
    }

    /// Records the buffer epochs flush `seq` sealed (all zero when it failed),
    /// returning the per-shard epochs that may now be dropped, if the frontier
    /// moved. Epochs only grow, so the result is the element-wise maximum.
    fn drop_complete(&mut self, seq: u64, sealed: Vec<u64>) -> Option<Vec<u64>> {
        self.drop_done.insert(seq, sealed);
        let mut released: Option<Vec<u64>> = None;
        while let Some(s) = self.drop_done.remove(&self.drop_frontier) {
            released = Some(match released {
                None => s,
                Some(mut acc) => {
                    for (a, b) in acc.iter_mut().zip(&s) {
                        *a = (*a).max(*b);
                    }
                    acc
                }
            });
            self.drop_frontier += 1;
        }
        released
    }
}

/// One block's worth of rows handed to the flush worker.
struct FlushJob {
    /// Position in flush order, for [`FlushOrder`].
    seq: u64,
    order: Arc<std::sync::Mutex<FlushOrder>>,
    /// Per writer shard, the epoch this flush sealed (0 = shard not flushed).
    sealed: Vec<u64>,
    writers: Vec<BlockWriter>,
    /// WAL position this block covers once it is durable.
    covered: WalPosition,
    row_count: usize,
    contention: Option<Arc<ContentionMetrics>>,
}

/// Encodes a job into a block and publishes it, in the order the WAL contract
/// requires: rename, publish to the index, **wait for the sidecar**, then commit
/// the WAL.
///
/// The buffer drain happens here too, and only here. Draining when a flush was
/// *scheduled* would remove rows from the memory buffer before they exist in any
/// block, leaving them invisible to queries - in neither place.
async fn run_flush_job(
    job: FlushJob,
    metadata_tx: &mpsc::UnboundedSender<PendingIndex>,
    wal: Option<&Arc<Mutex<WalWriter>>>,
    buffer: &Option<MemoryBuffer>,
) -> Result<()> {
    let (seq, order, shards) = (job.seq, Arc::clone(&job.order), job.sealed.len());
    let result = encode_publish_commit(job, metadata_tx, wal, buffer).await;
    if result.is_err() {
        // The block never landed, so nothing may leave the buffer on its
        // account - but later flushes' drops are queued behind this one and must
        // not wait for ever.
        release_buffer_drops(&order, seq, vec![0; shards], buffer).await;
    }
    result
}

/// Marks flush `seq` as done with the buffer and drops whatever that frees up,
/// in flush order. See [`FlushOrder`].
async fn release_buffer_drops(
    order: &Arc<std::sync::Mutex<FlushOrder>>,
    seq: u64,
    sealed: Vec<u64>,
    buffer: &Option<MemoryBuffer>,
) {
    let released = order
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .drop_complete(seq, sealed);
    if let (Some(buffer), Some(sealed)) = (buffer, released) {
        buffer.drop_sealed_metrics(&sealed).await;
    }
}

async fn encode_publish_commit(
    job: FlushJob,
    metadata_tx: &mpsc::UnboundedSender<PendingIndex>,
    wal: Option<&Arc<Mutex<WalWriter>>>,
    buffer: &Option<MemoryBuffer>,
) -> Result<()> {
    let FlushJob {
        seq,
        order,
        sealed,
        writers,
        covered,
        row_count,
        contention,
    } = job;

    let started = std::time::Instant::now();
    let flush_guard = contention
        .as_ref()
        .map(|c| c.flush_started(SignalType::Metrics));
    let encoded = tokio::task::spawn_blocking(move || merge_and_flush(writers)).await;
    // Unwrap the JoinError separately from the writer's own Result so the row
    // count can still be attributed when the flush succeeded.
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
    tracing::debug!(
        signal = "metrics",
        rows = row_count,
        duration_ms = started.elapsed().as_millis(),
        "metric block encoded"
    );

    // Publish, wait for the sidecar to be durable, then commit the WAL.
    // Committing first leaves a crash window in which the block is on disk,
    // absent from the sidecar and already marked recovered (BL-03-16).
    let (pending, durable) = match wal {
        Some(_) => {
            let (p, r) = PendingIndex::tracked(metadata);
            (p, Some(r))
        }
        // No WAL: nothing is committed, so there is nothing to order.
        None => (PendingIndex::untracked(metadata), None),
    };
    let _ = metadata_tx.send(pending);
    if let Some(durable) = durable {
        // Bounded: a wedged index task must not stall ingest, and committing
        // while the index is behind is exactly the window this handshake
        // closes. On timeout we do NOT commit, so the rows stay in the WAL and a
        // replay restores them into a fresh block.
        match tokio::time::timeout(INDEX_DURABILITY_TIMEOUT, durable).await {
            Ok(Ok(())) => {
                // Only the contiguous prefix of finished flushes may be
                // committed; see `FlushOrder`.
                let releasable = order
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .wal_complete(seq, covered);
                if let (Some(wal), Some(position)) = (wal, releasable) {
                    // A failed commit leaves the rows in the WAL, which only
                    // costs a replay; it must not keep them in the buffer too.
                    if let Err(e) = wal.lock().await.commit(position) {
                        tracing::error!("WAL commit failed; the rows will be replayed: {e}");
                    }
                }
            }
            Ok(Err(_)) => tracing::warn!(
                "index task dropped before the sidecar was persisted; leaving the WAL uncommitted so the rows are replayed"
            ),
            Err(_) => tracing::error!(
                timeout_secs = INDEX_DURABILITY_TIMEOUT.as_secs(),
                "timed out waiting for the block index to become durable; leaving the WAL uncommitted so the rows are replayed"
            ),
        }
    }

    // Safe now: the rows are in a block the index knows about. Draining earlier -
    // when a flush was merely *scheduled* - would leave the rows in neither the
    // buffer nor a block, and they would be invisible to every query until the
    // block landed. Only the rows this block holds leave; newer ones stay.
    release_buffer_drops(&order, seq, sealed, buffer).await;
    Ok(())
}

/// Merges every shard writer's rows into one block and encodes it.
fn merge_and_flush(writers: Vec<BlockWriter>) -> Result<BlockMetadata> {
    let mut merged = BlockWriter::merge(writers)?;
    merged.flush()
}

/// The writer half of a signal that is rotated one block at a time (logs and
/// traces; metrics have their own sharded rotator).
#[doc(hidden)]
pub trait RowWriter: Send + 'static {
    type Row: serde::Serialize + Send + Sync + 'static;
    type Config: Clone + Send + Sync + 'static;
    const SIGNAL: SignalType;
    const NAME: &'static str;

    fn create(config: Self::Config) -> Self;
    fn push_row(&mut self, row: Self::Row) -> Result<Option<BlockMetadata>>;
    fn rows(&self) -> usize;
    fn write_block(&mut self) -> Result<BlockMetadata>;
    fn max_rows(config: &Self::Config) -> usize;
    fn block_duration(config: &Self::Config) -> Duration;
    /// Adds rows to the memory buffer, in writer order.
    fn feed_buffer<'a>(
        buffer: &'a MemoryBuffer,
        rows: &'a [Self::Row],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
    /// Removes the oldest `n` rows from the memory buffer.
    fn drop_flushed<'a>(
        buffer: &'a MemoryBuffer,
        n: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

impl RowWriter for LogWriter {
    type Row = LogRecord;
    type Config = LogBlockConfig;
    const SIGNAL: SignalType = SignalType::Logs;
    const NAME: &'static str = "log";

    fn create(config: Self::Config) -> Self {
        LogWriter::new(config)
    }
    fn push_row(&mut self, row: Self::Row) -> Result<Option<BlockMetadata>> {
        self.push(row)
    }
    fn rows(&self) -> usize {
        self.len()
    }
    fn write_block(&mut self) -> Result<BlockMetadata> {
        self.flush()
    }
    fn max_rows(config: &Self::Config) -> usize {
        config.max_rows_per_block
    }
    fn block_duration(config: &Self::Config) -> Duration {
        Duration::from_secs(config.block_duration_secs)
    }
    fn feed_buffer<'a>(
        buffer: &'a MemoryBuffer,
        rows: &'a [Self::Row],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.push_logs(rows))
    }
    fn drop_flushed<'a>(
        buffer: &'a MemoryBuffer,
        n: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.drop_flushed_logs(n))
    }
}

impl RowWriter for TraceWriter {
    type Row = Span;
    type Config = BlockConfig;
    const SIGNAL: SignalType = SignalType::Traces;
    const NAME: &'static str = "trace";

    fn create(config: Self::Config) -> Self {
        TraceWriter::new(config)
    }
    fn push_row(&mut self, row: Self::Row) -> Result<Option<BlockMetadata>> {
        self.push(row)
    }
    fn rows(&self) -> usize {
        self.len()
    }
    fn write_block(&mut self) -> Result<BlockMetadata> {
        self.flush()
    }
    fn max_rows(config: &Self::Config) -> usize {
        config.max_rows_per_block
    }
    fn block_duration(config: &Self::Config) -> Duration {
        Duration::from_secs(config.block_duration_secs)
    }
    fn feed_buffer<'a>(
        buffer: &'a MemoryBuffer,
        rows: &'a [Self::Row],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.push_spans(rows))
    }
    fn drop_flushed<'a>(
        buffer: &'a MemoryBuffer,
        n: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.drop_flushed_spans(n))
    }
}

/// Rotates the blocks of one signal: log or trace rows accumulate in a writer,
/// and a full or old writer is swapped out and encoded in the background.
///
/// # Locking
///
/// The rotator is used behind one mutex, and that lock covers only the ordered
/// part of an ingest: the WAL append (one `write` for the whole request), the
/// memory-buffer push and the writer push. Encoding a block - the slow part - runs
/// on a detached task after the writer has been swapped out, so requests keep
/// flowing while it does. At most one encode is in flight: starting another
/// waits for the gate, which also makes the buffer drop exact (below) and bounds
/// memory when ingest outruns the disk.
///
/// # Buffer
///
/// The buffer is fed under the same lock as the writer, so both hold the rows in
/// the same order. When a block is durable the buffer drops its oldest
/// `row_count` rows - precisely that block's - and rows that arrived during the
/// encode stay queryable.
pub struct SerialRotator<W: RowWriter> {
    writer: W,
    config: W::Config,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<PendingIndex>,
    /// WAL for this signal, when enabled. Appended to under the rotator lock, so
    /// WAL order and writer order are the same total order - the property that
    /// makes the committed position correct.
    wal: Option<Arc<Mutex<WalWriter>>>,
    /// Highest WAL position among the rows currently in `writer`.
    covered: WalPosition,
    /// Held by the one flush task in flight.
    gate: Arc<Mutex<()>>,
    buffer: Option<MemoryBuffer>,
}

/// Handles automatic rotation and flushing of log blocks.
pub type LogRotator = SerialRotator<LogWriter>;
/// Handles automatic rotation and flushing of trace blocks.
pub type TraceRotator = SerialRotator<TraceWriter>;

impl<W: RowWriter> SerialRotator<W> {
    pub fn new(config: W::Config, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        let max_duration = W::block_duration(&config);
        Self {
            writer: W::create(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
            wal: None,
            covered: WalPosition::START,
            gate: Arc::new(Mutex::new(())),
            buffer: None,
        }
    }

    /// Attaches a WAL. After this, every accepted row is logged before it
    /// reaches the writer.
    pub fn with_wal(mut self, wal: WalWriter) -> Self {
        self.wal = Some(Arc::new(Mutex::new(wal)));
        self
    }

    /// Gives the rotator the memory buffer it keeps in step with its writer.
    pub fn set_buffer(&mut self, buffer: Option<MemoryBuffer>) {
        self.buffer = buffer;
    }

    /// Whether rows are logged to a WAL.
    pub fn has_wal(&self) -> bool {
        self.wal.is_some()
    }

    /// Bytes this signal's WAL currently occupies.
    pub async fn wal_bytes(&self) -> u64 {
        match &self.wal {
            Some(w) => w.lock().await.size_bytes(),
            None => 0,
        }
    }

    fn publish(&self, meta: BlockMetadata, contention: Option<&ContentionMetrics>) {
        record_block_written(&meta, contention, W::SIGNAL);
        let _ = self.metadata_tx.send(PendingIndex::untracked(meta));
    }

    /// Pushes one row, closing the block first when it is full.
    ///
    /// Returns `true` if a block was closed.
    pub async fn push(
        &mut self,
        row: W::Row,
        contention: Option<&Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        let wal_record = match self.wal {
            Some(_) => Some(parqtel_core::wal::encode_record(&row)?),
            None => None,
        };
        self.push_batch(vec![row], wal_record, contention).await
    }

    /// Pushes a request's rows.
    ///
    /// `wal_records` holds the rows already framed with
    /// [`parqtel_core::wal::encode_record_into`], one record per row, in order -
    /// encoded by the caller before taking the rotator lock. Everything else here
    /// is the cheap, ordered part.
    ///
    /// Returns `true` if a block was closed.
    pub async fn push_batch(
        &mut self,
        rows: Vec<W::Row>,
        wal_records: Option<Vec<u8>>,
        contention: Option<&Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        let max_rows = W::max_rows(&self.config).max(1);
        let mut flushed = false;

        // A batch that would overflow the open block closes it first, so the
        // batch is not split across blocks.
        if self.writer.rows() > 0 && self.writer.rows() + rows.len() > max_rows {
            self.start_flush(contention).await;
            flushed = true;
        }
        // A batch bigger than a whole block is split by the writer itself, which
        // writes the full blocks inline. Let any background encode finish first so
        // the buffer's oldest rows are those blocks' rows.
        if rows.len() >= max_rows {
            drop(self.gate.lock().await);
        }

        // Logged first, and failures propagate: acknowledging a row we could not
        // log would make the WAL a lie.
        if let (Some(wal), Some(records)) = (&self.wal, &wal_records) {
            self.covered = wal.lock().await.append_encoded(records)?;
        }
        if let Some(buffer) = &self.buffer {
            W::feed_buffer(buffer, &rows).await;
        }
        for row in rows {
            if let Some(meta) = self.writer.push_row(row)? {
                // The writer closed a full block by itself. Those are the oldest
                // rows in the buffer too.
                if let Some(buffer) = &self.buffer {
                    W::drop_flushed(buffer, meta.row_count).await;
                }
                self.publish(meta, contention.map(|c| c.as_ref()));
                flushed = true;
            }
        }
        Ok(flushed)
    }

    /// Starts a flush when the block duration has elapsed.
    pub async fn check_and_flush(
        &mut self,
        contention: Option<&Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.start_flush(contention).await;
            return Ok(true);
        }
        Ok(false)
    }

    /// Waits for the flush in flight, if any, without starting another.
    pub async fn wait_idle(&self) {
        drop(self.gate.lock().await);
    }

    /// Writes the open block and waits until it is done.
    pub async fn flush(&mut self, contention: Option<&Arc<ContentionMetrics>>) -> Result<()> {
        self.start_flush(contention).await;
        // The flush task holds the gate until the block is durable and the WAL
        // committed; taking it here waits for exactly that.
        drop(self.gate.lock().await);
        Ok(())
    }

    /// Swaps the writer out and encodes it on a detached task.
    ///
    /// Waits first for any earlier flush, holding the rotator lock meanwhile:
    /// that is back-pressure for a disk that cannot keep up, not a stall in the
    /// normal case.
    async fn start_flush(&mut self, contention: Option<&Arc<ContentionMetrics>>) {
        if self.writer.rows() == 0 {
            tracing::debug!("{} block flush skipped: buffer empty", W::NAME);
            return;
        }
        let permit = Arc::clone(&self.gate).lock_owned().await;
        let rows = self.writer.rows();
        // Snapshot before the writer is taken: the block will contain exactly the
        // rows whose WAL positions are at or below this.
        let covered = std::mem::replace(&mut self.covered, WalPosition::START);
        let writer = std::mem::replace(&mut self.writer, W::create(self.config.clone()));
        self.last_flush = Instant::now();

        let metadata_tx = self.metadata_tx.clone();
        let wal = self.wal.clone();
        let buffer = self.buffer.clone();
        let contention = contention.cloned();
        tokio::spawn(async move {
            let _permit = permit;
            serial_flush::<W>(writer, covered, rows, metadata_tx, wal, buffer, contention).await;
        });
    }
}

/// Encodes one block, publishes it, commits its WAL range and then removes its
/// rows from the memory buffer - in that order. See [`SerialRotator`].
async fn serial_flush<W: RowWriter>(
    mut writer: W,
    covered: WalPosition,
    row_count: usize,
    metadata_tx: mpsc::UnboundedSender<PendingIndex>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
) {
    let started = std::time::Instant::now();
    let flush_guard = contention.as_ref().map(|c| c.flush_started(W::SIGNAL));
    let encoded = tokio::task::spawn_blocking(move || writer.write_block()).await;
    let metadata = match encoded {
        Ok(Ok(meta)) => meta,
        Ok(Err(e)) => {
            // The rows are still in the WAL, so this is recoverable by a replay.
            tracing::error!(rows = row_count, "{} block flush failed: {e}", W::NAME);
            return drop_from_buffer::<W>(&buffer, row_count).await;
        }
        Err(e) => {
            tracing::error!(
                rows = row_count,
                "{} block flush task panicked: {e}",
                W::NAME
            );
            return drop_from_buffer::<W>(&buffer, row_count).await;
        }
    };
    if let Some(guard) = flush_guard {
        guard.count_rows(metadata.row_count as u64);
    }

    // Sidecar durable before the WAL moves; see the metrics flush for why.
    let (pending, durable) = match wal.is_some() {
        true => {
            let (p, r) = PendingIndex::tracked(metadata);
            (p, Some(r))
        }
        false => (PendingIndex::untracked(metadata), None),
    };
    let _ = metadata_tx.send(pending);
    if let Some(durable) = durable {
        // Bounded, and on timeout the WAL stays uncommitted so replay recovers
        // the rows.
        match tokio::time::timeout(INDEX_DURABILITY_TIMEOUT, durable).await {
            Ok(Ok(())) => {
                if let Some(wal) = &wal {
                    if let Err(e) = wal.lock().await.commit(covered) {
                        tracing::error!(
                            "{} WAL commit failed; the rows will be replayed: {e}",
                            W::NAME
                        );
                    }
                }
            }
            Ok(Err(_)) => tracing::warn!(
                "{} index task dropped before the sidecar was persisted; leaving the WAL uncommitted",
                W::NAME
            ),
            Err(_) => tracing::error!(
                "timed out waiting for the {} index to become durable; leaving the WAL uncommitted",
                W::NAME
            ),
        }
    }
    drop_from_buffer::<W>(&buffer, row_count).await;
    tracing::debug!(
        signal = W::NAME,
        rows = row_count,
        duration_ms = started.elapsed().as_millis(),
        "block flushed to parquet"
    );
}

async fn drop_from_buffer<W: RowWriter>(buffer: &Option<MemoryBuffer>, rows: usize) {
    if let Some(buffer) = buffer {
        W::drop_flushed(buffer, rows).await;
    }
}

/// How long a flush waits for the block index to become durable before giving
/// up and leaving the WAL uncommitted.
///
/// A wait with no bound would turn a wedged index task into a total ingest
/// outage, so the failure mode is deliberately "do not commit": the rows stay
/// in the WAL and a later replay restores them into a fresh block. The stale
/// block is left unindexed, which costs disk but loses no data.
const INDEX_DURABILITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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

/// The points of `metric` as the memory buffer should hold them: each carrying
/// the resource's `service.name` unless the point has its own.
///
/// Label sets are shared by many points, so the labels with `service.name`
/// added are built once per distinct set rather than once per point.
fn buffer_points(metric: &Metric) -> Vec<DataPoint> {
    let Some(svc) = metric.resource_attributes.get("service.name") else {
        return metric.data_points.clone();
    };
    let mut resolved: std::collections::HashMap<parqtel_core::LabelSet, parqtel_core::LabelSet> =
        std::collections::HashMap::new();
    metric
        .data_points
        .iter()
        .map(|dp| {
            let labels = match resolved.get(&dp.labels) {
                Some(l) => l.clone(),
                None => {
                    let l = if dp.labels.get("service.name").is_some() {
                        dp.labels.clone()
                    } else {
                        parqtel_core::LabelInterner::global().intern_with(
                            &dp.labels,
                            "service.name",
                            svc,
                        )
                    };
                    resolved.insert(dp.labels.clone(), l.clone());
                    l
                }
            };
            DataPoint {
                timestamp_ns: dp.timestamp_ns,
                value: dp.value.clone(),
                labels,
            }
        })
        .collect()
}

impl IngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        Self::with_shards(config, metadata_tx, 1, None)
    }

    /// Creates a service whose rotator uses `shards` writer shards, with an
    /// optional write-ahead log.
    ///
    /// See [`BlockRotator`] for why the shards are merged into one block
    /// rather than each producing their own.
    pub fn with_shards(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<PendingIndex>,
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
    ///
    /// The rotator gets a handle too: with a background flush worker it must
    /// drain the buffer when a block becomes *durable*, which is long after
    /// this request returns.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        self.rotator.set_buffer(Some(buffer.clone()));
        self.memory_buffer = Some(buffer);
        self
    }

    /// Share lock-wait and flush counters with the `/metrics` renderer.
    pub fn with_contention(mut self, contention: Arc<ContentionMetrics>) -> Self {
        self.contention = Some(contention);
        self
    }

    /// Starts the background flush worker.
    ///
    /// `max_in_flight` bounds concurrent encodes; when the queue is full a flush
    /// encodes inline instead, so backpressure degrades to the previous
    /// synchronous behaviour rather than growing a backlog.
    pub fn start_flush_worker(&self, max_in_flight: usize) {
        self.rotator.start_flush_worker(max_in_flight);
    }

    /// Schedules a flush without stopping the worker, so a test can observe the
    /// window between a flush being scheduled and its block becoming durable.
    pub async fn schedule_flush_for_test(&self) -> Result<bool> {
        let contention = self.contention.clone();
        self.rotator.flush(contention).await?;
        Ok(true)
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

    /// Ingests a request a transport has already decoded (gRPC), skipping the
    /// encode and decode a byte-oriented entry point would need.
    pub async fn ingest_request(&self, request: ExportMetricsServiceRequest) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let metrics = decode_offloading(move || OtlpDecoder::decode_metrics(request))
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
        let contention = self.contention.clone();
        for m in metrics {
            count += m.data_points.len() as u64;
            // The memory buffer copy carries `service.name` from the resource
            // attributes, so label matchers behave the same before and after a
            // flush. It is placed by the rotator, inside the shard lock, to keep
            // buffer order identical to writer order. The rotator measures its
            // own per-shard lock wait.
            let buffered = self.memory_buffer.as_ref().map(|_| buffer_points(&m));
            self.rotator
                .push_buffered(m, buffered, contention.clone())
                .await?;
        }
        // Duration-based rotation. Whatever a flush covers leaves the buffer
        // when its block is durable, not here.
        self.rotator.check_and_flush(contention.clone()).await?;
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        tracing::debug!(
            signal = "metrics",
            ingested = count,
            "metrics ingestion complete"
        );
        Ok(count)
    }

    /// Checks for duration-based flush; returns `true` when a flush happened
    /// (the shared memory buffer is drained so flushed rows aren't double-read).
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        self.rotator.check_and_flush(contention).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let _ = self.rotator.flush(contention.clone()).await;
        // Stops the worker after it drains, so every scheduled block is written
        // and its WAL position committed before we exit.
        self.rotator.stop_flush_worker().await;
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
    wal_enabled: bool,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
}

impl LogIngestionService {
    /// Bytes the log WAL currently occupies.
    pub async fn wal_bytes(&self) -> u64 {
        self.rotator.lock().await.wal_bytes().await
    }

    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        Self::with_wal(config, metadata_tx, None)
    }

    /// Creates the service with an optional write-ahead log.
    pub fn with_wal(
        config: LogBlockConfig,
        metadata_tx: mpsc::UnboundedSender<PendingIndex>,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = LogRotator::new(config, metadata_tx);
        let wal_enabled = wal.is_some();
        if let Some(w) = wal {
            rotator = rotator.with_wal(w);
        }
        Self {
            rotator: Arc::new(Mutex::new(rotator)),
            wal_enabled,
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            contention: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable data.
    ///
    /// The rotator gets a handle too: it feeds the buffer next to its writer and
    /// removes a block's rows once that block is durable.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        // Builder-time, before the service is shared: the lock is uncontended.
        if let Ok(mut rotator) = self.rotator.try_lock() {
            rotator.set_buffer(Some(buffer.clone()));
        }
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

    /// Ingests a request a transport has already decoded (gRPC).
    pub async fn ingest_request(&self, request: ExportLogsServiceRequest) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let logs = decode_offloading(move || OtlpDecoder::decode_logs(request))
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
        // Framing the WAL records (JSON + checksum) is the CPU-heavy half of
        // logging them and needs no lock; the rotator then writes the whole
        // request in one go.
        let wal_records = if self.wal_enabled {
            let mut bytes = Vec::with_capacity(logs.len() * 320);
            for log in &logs {
                parqtel_core::wal::encode_record_into(log, &mut bytes)?;
            }
            Some(bytes)
        } else {
            None
        };
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref()).await;
        let mut flushed = rotator
            .push_batch(logs, wal_records, contention.as_ref())
            .await?;
        if rotator.check_and_flush(contention.as_ref()).await? {
            flushed = true;
        }
        drop(rotator);
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

    /// Checks for duration-based flush; returns `true` when a flush started.
    /// The rows leave the memory buffer when their block is durable.
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref())
            .await
            .check_and_flush(contention.as_ref())
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref()).await;
        let _ = rotator.flush(contention.as_ref()).await;
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

/// Public service for ingesting OTLP traces.
pub struct TraceIngestionService {
    rotator: Arc<Mutex<TraceRotator>>,
    wal_enabled: bool,
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
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        Self::with_wal(config, metadata_tx, None)
    }

    /// Creates the service with an optional write-ahead log.
    pub fn with_wal(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<PendingIndex>,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = TraceRotator::new(config, metadata_tx);
        let wal_enabled = wal.is_some();
        if let Some(w) = wal {
            rotator = rotator.with_wal(w);
        }
        Self {
            rotator: Arc::new(Mutex::new(rotator)),
            wal_enabled,
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            span_metrics_tx: None,
            tail_sampling: TailSamplingConfig::default(),
            contention: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable spans.
    ///
    /// The rotator gets a handle too: it feeds the buffer next to its writer and
    /// removes a block's spans once that block is durable.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        // Builder-time, before the service is shared: the lock is uncontended.
        if let Ok(mut rotator) = self.rotator.try_lock() {
            rotator.set_buffer(Some(buffer.clone()));
        }
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

    /// Ingests a request a transport has already decoded (gRPC).
    pub async fn ingest_request(&self, request: ExportTraceServiceRequest) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let spans = decode_offloading(move || OtlpDecoder::decode_traces(request))
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
        // Framing the WAL records is the CPU-heavy half of logging them and
        // needs no lock; the rotator then writes the whole request in one go.
        let wal_records = if self.wal_enabled {
            let mut bytes = Vec::with_capacity(spans.len() * 512);
            for span in &spans {
                parqtel_core::wal::encode_record_into(span, &mut bytes)?;
            }
            Some(bytes)
        } else {
            None
        };
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref()).await;
        // The rotator also makes the spans queryable (memory buffer) as it
        // accepts them.
        let mut flushed = rotator
            .push_batch(spans, wal_records, contention.as_ref())
            .await?;
        if rotator.check_and_flush(contention.as_ref()).await? {
            flushed = true;
        }
        drop(rotator);
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

    /// Checks for duration-based flush; returns `true` when a flush started.
    /// The spans leave the memory buffer when their block is durable.
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref())
            .await
            .check_and_flush(contention.as_ref())
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref()).await;
        let _ = rotator.flush(contention.as_ref()).await;
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

    /// Number of WAL-written Parquet blocks in a data dir.
    fn wal_files(data_dir: &std::path::Path) -> usize {
        std::fs::read_dir(data_dir)
            .map(|d| {
                d.flatten()
                    .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// The WAL must not be committed until the sidecar holding the block is
    /// durable.
    ///
    /// This is the ordering bug behind `BL-03-16`: the flush used to commit the
    /// WAL immediately after publishing the metadata, while the sidecar was
    /// written up to two seconds later by a debounce. A crash in that window
    /// left a block on disk, absent from the sidecar and already marked
    /// recovered — so nothing replayed it and no query could find it.
    ///
    /// A deliberately slow index task stands in for a slow sidecar write: the
    /// commit must not have happened while it was still blocked.
    #[tokio::test]
    async fn test_wal_commit_waits_for_the_sidecar_to_be_durable() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let config = BlockConfig {
            data_dir: data_dir.clone(),
            max_rows_per_block: 10_000,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::unbounded_channel::<PendingIndex>();

        // Read the commit point at the moment the index task is released, before
        // the flush can possibly commit: it must still be START.
        let commit_when_released = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let index_task = tokio::spawn({
            let commit_when_released = commit_when_released.clone();
            let data_dir = data_dir.clone();
            async move {
                while let Some(pending) = rx.recv().await {
                    // Stand in for a slow sidecar write.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    commit_when_released.store(
                        parqtel_core::wal::read_commit(&data_dir, "metrics").0,
                        std::sync::atomic::Ordering::SeqCst,
                    );
                    if let Some(ack) = pending.durable {
                        let _ = ack.send(());
                    }
                }
            }
        });

        let svc = IngestionService::with_shards(
            config,
            tx,
            1,
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
        svc.ingest_metrics(vec![metric_named("ord", 1)])
            .await
            .unwrap();
        svc.shutdown().await.unwrap();
        index_task.abort();

        assert_eq!(
            commit_when_released.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the WAL was committed before the sidecar was durable"
        );
        assert_ne!(
            parqtel_core::wal::read_commit(&data_dir, "metrics").0,
            0,
            "once the index releases the flush, the WAL must be committed"
        );
    }

    /// Runs an index task the way `main.rs` does: add the block, then release
    /// the flush waiting on durability.
    ///
    /// A flush now blocks until the sidecar holding its block is written, so a
    /// test that produces blocks without one would stall until the handshake
    /// times out. Reproducing the real task keeps the tests honest about the
    /// ordering as well.
    fn spawn_index_task(
        mut rx: mpsc::UnboundedReceiver<PendingIndex>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(pending) = rx.recv().await {
                if let Some(ack) = pending.durable {
                    let _ = ack.send(());
                }
            }
        })
    }

    /// The hazard the background worker introduces: between a flush being
    /// *scheduled* and the block being *durable*, the rows live only in the
    /// WAL. Draining the memory buffer during that window would leave them in
    /// neither the buffer nor a block, and they would be invisible to queries.
    ///
    /// Driven through the service, which is the path that populates both.
    #[tokio::test]
    async fn test_background_flush_keeps_rows_visible_throughout() {
        let dir = tempdir().unwrap();
        let buffer = parqtel_core::MemoryBuffer::new();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10_000,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let svc =
            IngestionService::with_shards(config, tx, 2, None).with_memory_buffer(buffer.clone());
        svc.start_flush_worker(2);

        for i in 0..8i64 {
            svc.ingest_metrics(vec![metric_named(&format!("vis{i}"), i + 1)])
                .await
                .unwrap();
        }
        // Queryable before any flush: the rows are in the buffer.
        assert_eq!(buffer.stats().await.0, 8, "rows must be in the buffer");

        // Schedule a flush. It returns without waiting for the encode.
        svc.schedule_flush_for_test().await.unwrap();

        // Eventually the block is written, and only then is the buffer drained.
        // At no point may the two overlap, or rows would be read twice.
        let mut blocks = 0usize;
        for _ in 0..200 {
            blocks = block_file_count(dir.path()).max(blocks);
            if buffer.stats().await.0 == 0 && blocks > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(blocks > 0, "the worker must write the block");
        assert_eq!(
            buffer.stats().await.0,
            0,
            "the buffer must be drained once the block is durable"
        );
        assert_eq!(blocks, 1, "all 8 rows belong to a single block");
        svc.shutdown().await.unwrap();
    }

    /// Rows that arrive while a block is being encoded belong to the *next*
    /// block. When the encode finishes, only the rows that went into it may leave
    /// the memory buffer; draining everything would hide the newer rows from
    /// queries until a later block is written.
    #[tokio::test]
    async fn test_background_flush_keeps_rows_that_arrived_during_the_encode() {
        let dir = tempdir().unwrap();
        let buffer = parqtel_core::MemoryBuffer::new();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 100_000,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let wal = WalWriter::open(
            dir.path(),
            "metrics",
            WalSyncMode::None,
            Duration::from_secs(1),
        )
        .unwrap();
        let svc = IngestionService::with_shards(config, tx, 2, Some(wal))
            .with_memory_buffer(buffer.clone());
        svc.start_flush_worker(2);

        for i in 0..4i64 {
            svc.ingest_metrics(vec![metric_named(&format!("old{i}"), i + 1)])
                .await
                .unwrap();
        }
        // The swap happens before this returns; the encode finishes later.
        svc.schedule_flush_for_test().await.unwrap();
        for i in 0..3i64 {
            svc.ingest_metrics(vec![metric_named(&format!("new{i}"), 100 + i)])
                .await
                .unwrap();
        }

        for _ in 0..300 {
            if block_file_count(dir.path()) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Give the post-encode drain time to run.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            block_file_count(dir.path()),
            1,
            "the first block was written"
        );
        let mut names = buffer.metric_names().await;
        names.sort();
        assert_eq!(
            names,
            vec!["new0", "new1", "new2"],
            "only the rows that were flushed may leave the buffer"
        );
        svc.shutdown().await.unwrap();
    }

    /// A block may only commit once every earlier block is durable, whatever
    /// order the concurrent encodes happen to finish in.
    #[test]
    fn test_wal_commit_never_passes_an_unfinished_flush() {
        let mut f = FlushOrder::default();
        let (a, b, c) = (f.begin(), f.begin(), f.begin());
        // The latest block finishes first: nothing may be committed.
        assert_eq!(f.wal_complete(c, WalPosition(30)), None);
        // The middle one next: still blocked on the first.
        assert_eq!(f.wal_complete(b, WalPosition(20)), None);
        // The first completing releases all three, up to the highest position.
        assert_eq!(f.wal_complete(a, WalPosition(10)), Some(WalPosition(30)));
        // And the frontier keeps going for later flushes.
        let d = f.begin();
        assert_eq!(f.wal_complete(d, WalPosition(40)), Some(WalPosition(40)));
    }

    fn log_at(ts: i64, body: &str) -> parqtel_core::LogRecord {
        parqtel_core::LogRecord::new(
            ts,
            ts,
            9,
            "INFO".into(),
            body.into(),
            parqtel_core::LabelSet::default(),
            parqtel_core::LabelSet::default(),
            [0u8; 16],
            [0u8; 8],
            0,
            "".into(),
            "".into(),
        )
    }

    /// Logs: a block is written without holding the ingest path, and rows that
    /// arrive during the encode stay queryable once it lands; only the block's own
    /// rows leave the buffer.
    #[tokio::test]
    async fn test_log_flush_keeps_rows_that_arrived_during_the_encode() {
        let dir = tempdir().unwrap();
        let buffer = parqtel_core::MemoryBuffer::new();
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let svc = LogIngestionService::with_wal(
            LogBlockConfig {
                data_dir: dir.path().to_path_buf(),
                max_rows_per_block: 100_000,
                block_duration_secs: 3600,
                ..Default::default()
            },
            tx,
            Some(
                WalWriter::open(
                    dir.path(),
                    "logs",
                    WalSyncMode::None,
                    Duration::from_secs(1),
                )
                .unwrap(),
            ),
        )
        .with_memory_buffer(buffer.clone());

        svc.ingest_logs((0..5).map(|i| log_at(i + 1, "old")).collect())
            .await
            .unwrap();
        // Starts the flush and returns; the encode runs in the background.
        {
            let mut rotator = svc.rotator.lock().await;
            rotator.start_flush(None).await;
        }
        svc.ingest_logs((0..3).map(|i| log_at(100 + i, "new")).collect())
            .await
            .unwrap();

        // Wait for the block in flight, not for the open one.
        svc.rotator.lock().await.wait_idle().await;

        let left = buffer.scan_logs(0, i64::MAX).await;
        let bodies: Vec<_> = left.iter().map(|l| l.body.clone()).collect();
        assert_eq!(
            bodies,
            vec!["new", "new", "new"],
            "only the flushed rows may leave the buffer"
        );
        assert!(
            block_file_count(dir.path()) >= 1,
            "the first block must be written"
        );
    }

    /// Logs: one WAL write per request still replays record by record, and the
    /// commit point leaves nothing to replay after a clean flush.
    #[tokio::test]
    async fn test_log_batch_wal_append_replays_each_record() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let svc = LogIngestionService::with_wal(
            LogBlockConfig {
                data_dir: data_dir.clone(),
                max_rows_per_block: 100_000,
                block_duration_secs: 3600,
                ..Default::default()
            },
            tx,
            Some(
                WalWriter::open(
                    &data_dir,
                    "logs",
                    WalSyncMode::Always,
                    Duration::from_millis(0),
                )
                .unwrap(),
            ),
        );
        svc.ingest_logs((0..7).map(|i| log_at(i + 1, &format!("m{i}"))).collect())
            .await
            .unwrap();
        svc.ingest_logs(vec![log_at(50, "tail")]).await.unwrap();

        // Nothing flushed yet: a crash here replays all eight, in order.
        let mut bodies = Vec::new();
        let stats = parqtel_core::wal::replay::<parqtel_core::LogRecord, _>(
            &data_dir,
            "logs",
            parqtel_core::wal::read_commit(&data_dir, "logs"),
            |l| bodies.push(l.body),
        )
        .unwrap();
        assert_eq!(stats.records, 8);
        assert_eq!(bodies[0], "m0");
        assert_eq!(bodies[6], "m6");
        assert_eq!(bodies[7], "tail");

        // After a clean shutdown the block covers them, so nothing replays.
        svc.shutdown().await.unwrap();
        let mut replayed = 0;
        parqtel_core::wal::replay::<parqtel_core::LogRecord, _>(
            &data_dir,
            "logs",
            parqtel_core::wal::read_commit(&data_dir, "logs"),
            |_| replayed += 1,
        )
        .unwrap();
        assert_eq!(replayed, 0, "a durable block must commit its WAL range");
    }

    /// Spans take the same path as logs.
    #[tokio::test]
    async fn test_trace_flush_keeps_spans_that_arrived_during_the_encode() {
        let dir = tempdir().unwrap();
        let buffer = parqtel_core::MemoryBuffer::new();
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let svc = TraceIngestionService::with_wal(
            BlockConfig {
                data_dir: dir.path().to_path_buf(),
                max_rows_per_block: 100_000,
                block_duration_secs: 3600,
                ..Default::default()
            },
            tx,
            Some(
                WalWriter::open(
                    dir.path(),
                    "traces",
                    WalSyncMode::None,
                    Duration::from_secs(1),
                )
                .unwrap(),
            ),
        )
        .with_memory_buffer(buffer.clone());

        svc.ingest_spans((1..=4).map(test_span).collect())
            .await
            .unwrap();
        {
            let mut rotator = svc.rotator.lock().await;
            rotator.start_flush(None).await;
        }
        svc.ingest_spans((100..103).map(test_span).collect())
            .await
            .unwrap();
        svc.rotator.lock().await.wait_idle().await;

        let left = buffer.scan_spans(0, i64::MAX).await;
        assert_eq!(left.len(), 3, "only the newer spans remain buffered");
        assert!(left.iter().all(|s| s.start_time_ns >= 100));
        assert!(block_file_count(dir.path()) >= 1);
    }

    /// Buffer drops follow the same order: a later flush finishing first must
    /// not remove rows before the earlier block holding the older ones landed.
    #[test]
    fn test_buffer_drops_are_released_in_flush_order() {
        let mut f = FlushOrder::default();
        let (a, b) = (f.begin(), f.begin());
        assert_eq!(f.drop_complete(b, vec![0, 4]), None);
        // Releasing the earlier flush frees both, merged element-wise.
        assert_eq!(f.drop_complete(a, vec![3, 2]), Some(vec![3, 4]));
        // A failed flush completes with nothing to drop, unblocking the rest.
        let (c, d) = (f.begin(), f.begin());
        assert_eq!(f.drop_complete(d, vec![5, 5]), None);
        assert_eq!(f.drop_complete(c, vec![0, 0]), Some(vec![5, 5]));
    }

    /// A flush that never completes (it failed) holds back every later commit,
    /// so replay still recovers its rows.
    #[test]
    fn test_wal_commit_holds_back_behind_a_failed_flush() {
        let mut f = FlushOrder::default();
        let (_failed, ok1, ok2) = (f.begin(), f.begin(), f.begin());
        assert_eq!(f.wal_complete(ok1, WalPosition(20)), None);
        assert_eq!(f.wal_complete(ok2, WalPosition(30)), None);
    }

    /// Number of Parquet block files a data dir holds.
    fn block_file_count(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .map(|d| {
                d.flatten()
                    .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Without a WAL the worker must refuse to start.
    ///
    /// Its contract is "the rows are already logged, so nothing is lost if this
    /// process dies before the encode lands". With no WAL that is void, and
    /// running it would silently acknowledge unrecoverable data. So the channel
    /// stays unset and `flush_locked` takes the synchronous path - still
    /// acknowledged only once the block is on disk.
    #[tokio::test]
    async fn test_flush_worker_refuses_to_start_without_a_wal() {
        let dir = tempdir().unwrap();
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let rotator = BlockRotator::with_shards(tiny_metrics_config(dir.path()), tx, 2);
        rotator.start_flush_worker(4);

        assert!(
            rotator.flush_tx.lock().unwrap().is_none(),
            "no worker may be started without a WAL"
        );

        // And a flush is therefore synchronous: the block exists by the time it
        // returns, with no waiting for a background task.
        rotator.push(metric_named("nowal", 1), None).await.unwrap();
        rotator.flush(None).await.unwrap();
        assert!(
            block_file_count(dir.path()) >= 1,
            "the flush must have written its block before returning"
        );
    }

    /// With a WAL the worker does start, and a flush is scheduled rather than
    /// encoded inline.
    #[tokio::test]
    async fn test_flush_worker_starts_with_a_wal() {
        let dir = tempdir().unwrap();
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let wal = WalWriter::open_with_segment_limit(
            dir.path(),
            "metrics",
            WalSyncMode::Always,
            Duration::from_millis(0),
            64 * 1024 * 1024,
        )
        .unwrap();
        let _ = &wal;
        let rotator =
            BlockRotator::with_shards(tiny_metrics_config(dir.path()), tx, 2).with_wal(wal);
        rotator.start_flush_worker(4);
        assert!(
            rotator.flush_tx.lock().unwrap().is_some(),
            "the worker must start when a WAL is attached"
        );
        rotator.stop_flush_worker().await;
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
        let meta = rx.recv().await.unwrap().meta;
        assert_eq!(meta.row_count, 1);
        assert!(meta.path.exists());
    }

    /// An idle flush must not produce an observation: it dilutes the flush
    /// latency histogram and would show up as a p50 far below any real flush.
    #[tokio::test]
    async fn test_idle_flush_is_not_recorded() {
        let dir = tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let contention = Arc::new(ContentionMetrics::new());
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        rotator.flush(Some(Arc::clone(&contention))).await.unwrap();
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
        let contention = Arc::new(ContentionMetrics::new());
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        rotator
            .push(one_point_metric("m1", 100), Some(Arc::clone(&contention)))
            .await
            .unwrap();
        rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

        let meta = rx.recv().await.unwrap().meta;
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
        let contention = Arc::new(ContentionMetrics::new());
        let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

        // Ten points exactly fill the cap: no flush, because the check is
        // "would this push overflow it" — so a block is never larger than the
        // cap plus one request.
        for i in 0..10 {
            let flushed = rotator
                .push(
                    one_point_metric("m1", 100 + i),
                    Some(Arc::clone(&contention)),
                )
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
            .push(one_point_metric("m1", 200), Some(Arc::clone(&contention)))
            .await
            .unwrap();
        assert!(flushed, "push must report the flush so the buffer drains");

        let meta = rx.recv().await.unwrap().meta;
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
        let contention = Arc::new(ContentionMetrics::new());
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
                    Some(Arc::clone(&contention)),
                )
                .await
                .unwrap();
        }
        rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

        let mut metas = Vec::new();
        while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
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
        while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
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

    fn tx2() -> mpsc::UnboundedSender<PendingIndex> {
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
            while let Ok(p) = rx.try_recv() {
                blocks.push(p.meta.row_count);
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
                            Some(Arc::clone(&contention)),
                        )
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

        let total: usize = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|p: PendingIndex| p.meta.row_count)
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
        let contention = Arc::new(ContentionMetrics::new());
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
                Some(Arc::clone(&contention)),
            )
            .await
            .unwrap();
        assert!(flushed, "splitting a metric must report the flush");

        rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

        let mut metas = Vec::new();
        while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
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
        while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
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
        let contention = Arc::new(ContentionMetrics::new());
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

        let meta = rx.recv().await.unwrap().meta;
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
            .map(|p: PendingIndex| p.meta.row_count)
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
            let (tx, rx) = mpsc::unbounded_channel();
            let _index = spawn_index_task(rx);
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
        let (tx, rx) = mpsc::unbounded_channel();
        let _index = spawn_index_task(rx);
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
        // The index task consumed the metadata, so the durable proof is that
        // the WAL is committed: the block is on disk and covered.
        assert!(
            wal_files(&data_dir) >= 1,
            "the flush must have written a block"
        );

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
                let (ltx, lrx) = mpsc::unbounded_channel();
                let (ttx, trx) = mpsc::unbounded_channel();
                let _index_a = spawn_index_task(lrx);
                let _index_b = spawn_index_task(trx);
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
            let (ltx, lrx) = mpsc::unbounded_channel();
            let (ttx, trx) = mpsc::unbounded_channel();
            let _gc_index_a = spawn_index_task(lrx);
            let _gc_index_b = spawn_index_task(trx);
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
            // The index tasks consumed the metadata; the WAL commit is what the
            // second replay checks.

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
        while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
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
        let meta = rx.recv().await.unwrap().meta;
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
