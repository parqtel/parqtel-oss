//! Debounced persistence for the block-index sidecars.
//!
//! Every flushed block used to re-serialise the *entire* index and rewrite it
//! while holding the write lock that every query handler contends for, making
//! query tail latency scale with the retention window. This module separates
//! the two concerns:
//!
//! * **Mutation** is in-memory and synchronous ([`BlockIndex::add`]) so it can
//!   happen under the write lock in microseconds.
//! * **Persistence** is debounced and happens here: serialise under a *read*
//!   lock (queries unaffected), then write on the blocking pool.
//!
//! The sidecar is a cache of on-disk state, not the source of truth — a crash
//! between a flush and its persist leaves orphaned block files that the
//! retention sweep already reports as missing. That is preferable to holding
//! the query read path hostage to an O(total index size) write.

use super::index::{BlockIndex, BlockIndexStore};
use crate::error::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Runs persistence passes for one [`BlockIndexStore`] until `shutdown` fires.
///
/// Passes are rate-limited to `interval`: a burst of flushes collapses into one
/// write, and an idle index costs one cheap `is_dirty` check per tick.
pub async fn run_index_persist_loop(
    index: Arc<RwLock<BlockIndex>>,
    store: Arc<BlockIndexStore>,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(50)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                persist_once(&index, &store).await;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    // Final pass: a graceful shutdown must leave the sidecar
                    // reflecting every block that was written.
                    persist_once(&index, &store).await;
                    return;
                }
            }
        }
    }
}

/// Performs one persistence pass if the index is dirty.
///
/// The dirty flag is taken first ([`BlockIndexStore::begin`]) so a burst of
/// flushes collapses into one write. A mutation that lands *during* the pass
/// sets the flag again, so nothing is lost; a failure re-marks it so the next
/// tick retries.
///
/// The payload is serialised under a **read** lock so concurrent queries
/// proceed unimpeded; only the file write runs without a lock, on the blocking
/// pool so no tokio worker is parked on disk I/O.
pub async fn persist_once(index: &Arc<RwLock<BlockIndex>>, store: &Arc<BlockIndexStore>) {
    if !store.begin() {
        return;
    }
    let payload = {
        // A read guard is enough: serialisation is pure CPU over `self`, and
        // taking it shared means queries keep running and only other *writers*
        // (the index task, compaction, retention) wait.
        let idx = index.read().await;
        match idx.serialize() {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "failed to serialise block index");
                store.mark_dirty();
                return;
            }
        }
    };

    // The closure must own the store and the payload: they outlive this frame
    // on the blocking pool. Cloning the `Arc` is a refcount bump; cloning the
    // payload `String` is one allocation, and lets `complete` record exactly
    // what was written.
    let for_write = Arc::clone(store);
    let for_completion = payload.clone();
    let write = tokio::task::spawn_blocking(move || for_write.write_payload(&payload)).await;
    match write {
        Ok(Ok(())) => store.complete(for_completion),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to persist block index");
            // Retry on the next tick rather than silently dropping the update.
            store.mark_dirty();
        }
        Err(e) => {
            tracing::error!(error = %e, "block index persist task panicked");
            store.mark_dirty();
        }
    }
}

/// Persists synchronously. For startup and shutdown paths only.
pub fn persist_blocking(index: &BlockIndex, store: &BlockIndexStore) -> Result<()> {
    let payload = index.serialize()?;
    store.write_payload(&payload)?;
    store.complete(payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::models::storage::{BlockMetadata, SignalType};
    use std::collections::HashSet;

    fn meta(path: std::path::PathBuf, start: i64) -> BlockMetadata {
        BlockMetadata {
            path,
            start_timestamp_ns: start,
            end_timestamp_ns: start + 10,
            row_count: 1,
            size_bytes: 10,
            metric_names: HashSet::from(["m".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        }
    }

    /// A pass on a clean index must do nothing — including not creating the
    /// sidecar, so a read-only deployment leaves no stray file.
    #[tokio::test]
    async fn persist_once_is_a_noop_on_a_clean_index() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        persist_once(&index, &store).await;
        assert!(!dir.path().join("index.json").exists());
    }

    #[tokio::test]
    async fn persist_once_writes_a_dirty_index() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        {
            let mut idx = index.write().await;
            idx.add(meta(dir.path().join("1.parquet"), 100));
        }
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        store.mark_dirty();

        persist_once(&index, &store).await;
        assert!(dir.path().join("index.json").exists());
        assert!(
            !store.is_dirty(),
            "a successful pass must clear the dirty flag, or every tick \
             re-persists forever and the pending-writes gauge sticks at 1"
        );

        let mut reloaded = BlockIndex::new(dir.path());
        reloaded.load().unwrap();
        assert_eq!(reloaded.total_blocks(), 1);
    }

    /// A second pass with nothing new must not rewrite the file — otherwise
    /// the debounce is not a debounce.
    #[tokio::test]
    async fn persist_once_is_idempotent_when_nothing_changed() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        {
            let mut idx = index.write().await;
            idx.add(meta(dir.path().join("1.parquet"), 100));
        }
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        store.mark_dirty();
        persist_once(&index, &store).await;
        let first = std::fs::metadata(dir.path().join("index.json"))
            .unwrap()
            .len();

        persist_once(&index, &store).await;
        assert_eq!(
            std::fs::metadata(dir.path().join("index.json"))
                .unwrap()
                .len(),
            first,
            "a clean store must not be rewritten"
        );
    }

    /// A mutation arriving while the payload is in flight must schedule
    /// another pass — the flag is taken at the start of the pass, so a
    /// `mark_dirty` after `begin()` must leave the store dirty.
    ///
    /// Waits for `begin()` to have run rather than racing it: the pass may
    /// legitimately finish first, and `complete` deliberately does not clear
    /// the flag, so the assertion holds either way.
    #[tokio::test]
    async fn mutation_during_persist_schedules_another_pass() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        {
            let mut idx = index.write().await;
            idx.add(meta(dir.path().join("1.parquet"), 100));
        }
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        store.mark_dirty();

        let persist = tokio::spawn({
            let index = index.clone();
            let store = store.clone();
            async move { persist_once(&index, &store).await }
        });

        // Wait for the pass to take the flag.
        let took_flag = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while store.is_dirty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(took_flag.is_ok(), "pass never took the dirty flag");

        index
            .write()
            .await
            .add(meta(dir.path().join("2.parquet"), 200));
        store.mark_dirty();
        persist.await.unwrap();

        assert!(
            store.is_dirty(),
            "a change made after the pass took the flag must schedule another"
        );
        persist_once(&index, &store).await;
        assert!(!store.is_dirty());

        let mut reloaded = BlockIndex::new(dir.path());
        reloaded.load().unwrap();
        assert_eq!(reloaded.total_blocks(), 2, "the late mutation must land");
    }

    /// Many mutations between passes must collapse into a single write that
    /// still captures every block — the debounce must not lose updates.
    #[tokio::test]
    async fn burst_of_mutations_persists_in_one_pass() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        for i in 0..25 {
            {
                let mut idx = index.write().await;
                idx.add(meta(dir.path().join(format!("{i}.parquet")), i * 100));
            }
            store.mark_dirty();
        }
        persist_once(&index, &store).await;

        let mut reloaded = BlockIndex::new(dir.path());
        reloaded.load().unwrap();
        assert_eq!(
            reloaded.total_blocks(),
            25,
            "one pass must capture every mutation since the last"
        );
    }

    /// The write lock must be free while the file write is in flight —
    /// otherwise the stall this series exists to remove is still there.
    #[tokio::test]
    async fn write_lock_is_not_held_during_the_file_write() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        {
            let mut idx = index.write().await;
            idx.add(meta(dir.path().join("1.parquet"), 100));
        }
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        store.mark_dirty();

        let persist = tokio::spawn({
            let index = index.clone();
            let store = store.clone();
            async move { persist_once(&index, &store).await }
        });

        // If any lock were held across the write this would deadlock or time
        // out; being able to take the write lock proves it is not.
        let mut idx = index.write().await;
        idx.add(meta(dir.path().join("2.parquet"), 200));
        drop(idx);

        persist.await.unwrap();
        let mut reloaded = BlockIndex::new(dir.path());
        reloaded.load().unwrap();
        assert!(reloaded.total_blocks() >= 1);
    }

    #[tokio::test]
    async fn loop_exits_and_flushes_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        {
            let mut idx = index.write().await;
            idx.add(meta(dir.path().join("1.parquet"), 100));
        }
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        store.mark_dirty();

        let (tx, rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(run_index_persist_loop(
            index.clone(),
            store.clone(),
            // Long interval: the shutdown branch must be what does the work.
            Duration::from_secs(3600),
            rx,
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("persist loop must exit on shutdown")
            .unwrap();

        let mut reloaded = BlockIndex::new(dir.path());
        reloaded.load().unwrap();
        assert_eq!(
            reloaded.total_blocks(),
            1,
            "a graceful shutdown must leave the sidecar complete"
        );
    }

    #[test]
    fn persist_blocking_is_usable_from_startup() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = BlockIndex::new(dir.path());
        idx.add(meta(dir.path().join("1.parquet"), 100));
        let store = BlockIndexStore::new(&idx);
        let result: Result<()> = persist_blocking(&idx, &store);
        result.unwrap();
        assert!(dir.path().join("index.json").exists());
    }
}
