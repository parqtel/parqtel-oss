//! TODO

use crate::writer::BlockMetadata;
use parqtel_core::{ContentionMetrics, Error, Result, SignalType};
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use tokio::sync::Mutex;

/// Publishes one freshly written block: records its flush duration and rows,
/// then hands the metadata to the index channel.
///
/// Shared by every write path so a block closed mid-`push` is accounted for
/// exactly like one closed by an explicit flush. Without this the capacity
/// split in [`BlockWriter::push`] would write blocks that never appear in the
/// flush metrics — the expensive case would again be invisible.
pub(super) fn record_block_written(
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
pub(super) fn unix_millis() -> u64 {
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
pub(super) fn shard_for(metric_name: &str, shard_count: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric_name.hash(&mut hasher);
    (hasher.finish() % shard_count as u64) as usize
}

/// How long a flush waits for the block index to become durable before giving
/// up and leaving the WAL uncommitted.
///
/// A wait with no bound would turn a wedged index task into a total ingest
/// outage, so the failure mode is deliberately "do not commit": the rows stay
/// in the WAL and a later replay restores them into a fresh block. The stale
/// block is left unindexed, which costs disk but loses no data.
pub(super) const INDEX_DURABILITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
pub(super) async fn decode_offloading<T, F>(decode: F) -> Result<T>
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
pub(super) async fn lock_rotator<'a, T>(
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
