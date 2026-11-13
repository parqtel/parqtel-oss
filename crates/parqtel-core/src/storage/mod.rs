pub mod block_file;
pub mod compactor;
pub mod index;
pub mod log_index;
pub mod persist;
pub mod retention;
pub mod scanner;
pub mod series_index;

pub use block_file::{footer_metadata, write_block_file, FooterIndex};
pub use compactor::Compactor;
pub use index::{advance_wal_commit, reconcile, BlockIndex, BlockIndexStore, PendingIndex};
pub use log_index::{LogIndexBuilder, LogIndexTable, LogSkipIndex, LOG_INDEX_FOOTER_KEY};
pub use persist::{persist_blocking, persist_once, run_index_persist_loop};
pub use retention::RetentionPolicy;
pub use scanner::{LogColumnPrefilter, LogRowFilter, LogScanStats, Scanner};
pub use series_index::{
    SeriesIndex, SeriesSelector, SeriesTable, SeriesTableBuilder, ValueTest,
    SERIES_INDEX_FOOTER_KEY,
};

use crate::config::BlockConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Handle for the background maintenance tasks of one signal, so a graceful
/// shutdown can stop them and flush the index exactly once.
pub struct MaintenanceHandle {
    compactor: tokio::task::JoinHandle<()>,
    retention: tokio::task::JoinHandle<()>,
    persist: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl MaintenanceHandle {
    /// Signals every task to stop.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }
    /// Signals every task to stop and waits for them, bounded by `timeout`.
    ///
    /// The persistence task performs a final pass before returning, so once
    /// this resolves the sidecar reflects every block that was written.
    pub async fn shutdown(self, timeout: std::time::Duration) {
        let _ = self.shutdown_tx.send(true);
        let _ = tokio::time::timeout(timeout, async {
            let _ = self.compactor.await;
            let _ = self.retention.await;
            let _ = self.persist.await;
        })
        .await;
    }
}

/// Starts background maintenance tasks for one signal.
///
/// Three tasks share one index lock and one store:
///
/// * the compactor merges small and tiered blocks,
/// * retention deletes expired blocks,
/// * the persistence loop writes the sidecar on a debounce.
///
/// All three take the index lock only for in-memory work — selection under a
/// read lock, publication as a single swap under the write lock. Every
/// filesystem and Parquet operation runs on the blocking pool, so no tokio
/// worker is parked on disk I/O while holding a lock every query needs.
///
/// `retention_interval_secs` controls the sweep cadence and
/// `persist_interval_secs` the sidecar debounce. The caller constructs the
/// `store` from the `BlockIndex` while it still holds it directly, which keeps
/// the sidecar path defined in exactly one place.
pub fn start_maintenance(
    index: Arc<RwLock<BlockIndex>>,
    store: Arc<BlockIndexStore>,
    config: BlockConfig,
    retention_interval_secs: u64,
    persist_interval_secs: u64,
) -> MaintenanceHandle {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let compactor = tokio::spawn(Compactor::run_loop(
        index.clone(),
        store.clone(),
        config.clone(),
        shutdown_rx.clone(),
    ));
    let retention = tokio::spawn(RetentionPolicy::run_loop(
        index.clone(),
        store.clone(),
        config,
        retention_interval_secs,
        shutdown_rx.clone(),
    ));
    let persist = tokio::spawn(run_index_persist_loop(
        index,
        store,
        std::time::Duration::from_secs(persist_interval_secs.max(1)),
        shutdown_rx,
    ));

    MaintenanceHandle {
        compactor,
        retention,
        persist,
        shutdown_tx,
    }
}

#[cfg(test)]
mod tests;
