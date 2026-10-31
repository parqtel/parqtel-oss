//! TODO

use super::common::INDEX_DURABILITY_TIMEOUT;
use crate::writer::{BlockMetadata, BlockWriter};
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::{WalPosition, WalWriter};
use parqtel_core::MemoryBuffer;
use parqtel_core::{ContentionMetrics, Error, Result, SignalType};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

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
pub(super) struct FlushOrder {
    pub(super) next_seq: u64,
    pub(super) wal_frontier: u64,
    pub(super) wal_done: std::collections::BTreeMap<u64, WalPosition>,
    pub(super) drop_frontier: u64,
    pub(super) drop_done: std::collections::BTreeMap<u64, Vec<u64>>,
}

impl FlushOrder {
    /// Numbers a new flush. Called in flush order, under the flush lock.
    pub(super) fn begin(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Records that flush `seq` is durable, returning the position that may now
    /// be committed, if the frontier moved.
    pub(super) fn wal_complete(&mut self, seq: u64, covered: WalPosition) -> Option<WalPosition> {
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
    pub(super) fn drop_complete(&mut self, seq: u64, sealed: Vec<u64>) -> Option<Vec<u64>> {
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
pub(super) struct FlushJob {
    /// The WAL span `(from, to]` this block covers.
    pub(super) wal_range: Option<(u64, u64)>,
    /// Position in flush order, for [`FlushOrder`].
    pub(super) seq: u64,
    pub(super) order: Arc<std::sync::Mutex<FlushOrder>>,
    /// Per writer shard, the epoch this flush sealed (0 = shard not flushed).
    pub(super) sealed: Vec<u64>,
    pub(super) writers: Vec<BlockWriter>,
    /// WAL position this block covers once it is durable.
    pub(super) covered: WalPosition,
    pub(super) row_count: usize,
    pub(super) contention: Option<Arc<ContentionMetrics>>,
}

/// Encodes a job into a block and publishes it, in the order the WAL contract
/// requires: rename, publish to the index, **wait for the sidecar**, then commit
/// the WAL.
///
/// The buffer drain happens here too, and only here. Draining when a flush was
/// *scheduled* would remove rows from the memory buffer before they exist in any
/// block, leaving them invisible to queries - in neither place.
pub(super) async fn run_flush_job(
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
        wal_range,
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
    let encoded = tokio::task::spawn_blocking(move || merge_and_flush(writers, wal_range)).await;
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
    // absent from the sidecar and already marked recovered.
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
fn merge_and_flush(
    writers: Vec<BlockWriter>,
    wal_range: Option<(u64, u64)>,
) -> Result<BlockMetadata> {
    let mut merged = BlockWriter::merge(writers)?;
    merged.set_wal_range(wal_range);
    merged.flush()
}
