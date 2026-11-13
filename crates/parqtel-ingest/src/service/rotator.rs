//! TODO

use super::common::{record_block_written, shard_for, unix_millis};
use super::flush::{run_flush_job, FlushJob, FlushOrder};
use crate::writer::{BlockMetadata, BlockWriter};
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::{WalPosition, WalWriter};
use parqtel_core::MemoryBuffer;
use parqtel_core::{BlockConfig, ContentionMetrics, DataPoint, Metric, Result, SignalType};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};

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
/// *other* requests stop waiting.
pub struct BlockRotator {
    /// One writer per shard. Each is locked only for the duration of a push,
    /// never across an encode.
    pub(super) shards: Vec<Mutex<BlockWriter>>,
    /// Rows currently buffered across all shards.
    ///
    /// Maintained as an atomic so the capacity check on the hot push path is a
    /// single load rather than a lock per shard. Kept exactly in step with the
    /// writers: `push` adds the rows it accepted, and the flush subtracts each
    /// shard's rows at the moment it swaps that shard out — inside the shard's
    /// own lock, so a concurrent push can never have its count erased by a
    /// later `store(0)`.
    pub(super) buffered: AtomicUsize,
    /// Serialises flush *initiation* so two requests cannot each swap the
    /// buffers and produce overlapping blocks. With a worker running it is held
    /// only for the swap and the hand-off, not for the encode.
    pub(super) flush_lock: tokio::sync::Mutex<()>,
    /// Channel to the background flush worker, when one is running.
    ///
    /// Behind a mutex because shutdown only has `&self`: the rotator is shared as
    /// an `Arc`, and awaiting the worker must not need exclusive access.
    pub(super) flush_tx: std::sync::Mutex<Option<mpsc::Sender<FlushJob>>>,
    /// Handle to the worker, awaited on shutdown so nothing is left in flight.
    pub(super) worker: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Buffer to drain once a flush is **durable**.
    ///
    /// The rotator owns this rather than the service because with a background
    /// worker the flush completes long after `push` returns. Draining in the
    /// service would empty the buffer before the rows were in any block, and they
    /// would be visible to neither the buffer nor a block.
    pub(super) buffer: std::sync::Mutex<Option<MemoryBuffer>>,
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
    pub(super) wal: Option<Arc<tokio::sync::Mutex<WalWriter>>>,
    /// Orders WAL commits and buffer drops across concurrent flushes.
    pub(super) order: Arc<std::sync::Mutex<FlushOrder>>,
    /// Where the previous flush's WAL span ended, so each block can record the
    /// span `(previous end, this end]` it covers. Only touched under
    /// `flush_lock`.
    pub(super) wal_cursor: std::sync::Mutex<WalPosition>,
    pub(super) config: BlockConfig,
    /// Unix time of the last completed flush, in milliseconds. An atomic so
    /// the duration check needs no lock — flushes are serialised by
    /// `flush_lock`, so exactly one writer updates this at a time.
    pub(super) last_flush_unix_ms: Arc<AtomicU64>,
    pub(super) max_duration_ms: u64,
    pub(super) metadata_tx: mpsc::UnboundedSender<PendingIndex>,
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
            order: Arc::new(std::sync::Mutex::new(FlushOrder::default())),
            wal_cursor: std::sync::Mutex::new(WalPosition::START),
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

    pub(super) fn publish(&self, meta: BlockMetadata, contention: Option<&ContentionMetrics>) {
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
                wal.lock().await.append_encoded(record)?;
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

        // Every shard is held for the swap, which only moves pointers. That makes
        // it one instant for the whole block, and the instant is what the WAL
        // commit depends on.
        //
        // The WAL is a single sequence shared by all shards. Swapping shard by
        // shard and committing the highest position seen would be wrong: a
        // record that lands on a shard *after* it was swapped, but before a later
        // shard was, has a lower position than that later shard's last record. It
        // belongs to the next block, yet it would sit below the commit point, and
        // a crash would skip it on replay. With all shards held, no push is in
        // flight, so every record up to the WAL's current end is in a writer being
        // taken here (or in a block a writer already closed by itself), and the
        // end of the WAL is exactly the position this block covers.
        let mut guards = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            guards.push(shard.lock().await);
        }
        let covered = match &self.wal {
            Some(wal) => wal.lock().await.position(),
            None => WalPosition::START,
        };
        for (i, guard) in guards.iter_mut().enumerate() {
            if guard.is_empty() {
                continue;
            }
            let taken = guard.len();
            row_count += taken;
            // Subtract while still holding the shard's lock, so a concurrent
            // push either lands before this (and is subtracted here) or after
            // (and counts up from the decremented total). A `store(0)` after the
            // loop would erase the latter.
            self.buffered.fetch_sub(taken, Ordering::Relaxed);
            // The old writer's epoch is sealed; the replacement starts the next
            // one, so points buffered from here on are not part of this block.
            sealed[i] = guard.epoch();
            writers.push(std::mem::replace(
                &mut **guard,
                BlockWriter::with_epoch(self.config.clone(), sealed[i] + 1),
            ));
        }
        drop(guards);
        if writers.is_empty() {
            tracing::debug!("metric block flush skipped: all shards empty");
            return Ok(());
        }

        let wal_range = self.wal.as_ref().map(|_| {
            let mut cursor = self.wal_cursor.lock().unwrap_or_else(|e| e.into_inner());
            let range = (cursor.0, covered.0);
            *cursor = covered;
            range
        });
        let seq = self.order.lock().unwrap_or_else(|e| e.into_inner()).begin();
        let job = FlushJob {
            wal_range,
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
