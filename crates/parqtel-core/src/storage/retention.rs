use super::index::{BlockIndex, BlockIndexStore};
use super::ownership::OwnershipRegistry;
use crate::config::BlockConfig;
use crate::error::{Error, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Background task that deletes expired blocks.
pub struct RetentionPolicy;

impl RetentionPolicy {
    /// Sweeps expired blocks every `interval_secs`.
    ///
    /// The interval is clamped to at least one second so a zero-valued config
    /// cannot turn this into a tight loop that hammers the filesystem.
    pub async fn run_loop(
        index: Arc<RwLock<BlockIndex>>,
        store: Arc<BlockIndexStore>,
        config: BlockConfig,
        interval_secs: u64,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        Self::run_loop_scoped(
            index,
            store,
            config,
            interval_secs,
            shutdown,
            Arc::new(OwnershipRegistry::everything()),
        )
        .await;
    }

    /// As [`Self::run_loop`], but only blocks this node owns are deleted.
    pub async fn run_loop_scoped(
        index: Arc<RwLock<BlockIndex>>,
        store: Arc<BlockIndexStore>,
        config: BlockConfig,
        interval_secs: u64,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        ownership: Arc<OwnershipRegistry>,
    ) {
        let interval = Duration::from_secs(interval_secs.max(1));
        tracing::debug!(
            interval_secs = interval.as_secs(),
            retention_days = config.retention_days,
            scoped = ownership.is_scoped(),
            "retention policy started"
        );
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::debug!("retention policy stopping");
                        return;
                    }
                }
            }
            if let Err(e) =
                Self::enforce_scoped(&index, &store, config.retention_days, &ownership).await
            {
                tracing::error!("Retention failed: {}", e);
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn enforce(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        retention_days: u64,
    ) -> Result<()> {
        Self::enforce_scoped(
            index,
            store,
            retention_days,
            &OwnershipRegistry::everything(),
        )
        .await
    }

    pub(crate) async fn enforce_scoped(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        retention_days: u64,
        ownership: &OwnershipRegistry,
    ) -> Result<()> {
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let cutoff = now_ns - (retention_days as i64 * 24 * 3600 * 1_000_000_000);

        // Select under a read lock. Taking the write lock here would block
        // every query handler for the whole deletion batch.
        let to_delete: Vec<std::path::PathBuf> = {
            let idx = index.read().await;
            idx.blocks
                .iter()
                .filter(|b| b.end_timestamp_ns < cutoff && ownership.may_retire(&b.path))
                .map(|b| b.path.clone())
                .collect()
        };

        tracing::debug!(
            expired_blocks = to_delete.len(),
            cutoff_ns = cutoff,
            retention_days = retention_days,
            "retention enforcement check"
        );

        if to_delete.is_empty() {
            tracing::debug!(
                retention_days = retention_days,
                "retention policy: no expired blocks"
            );
            return Ok(());
        }

        // Unlink on the blocking pool: a large sweep can be hundreds of
        // syscalls, which must not park a tokio worker.
        let doomed: std::collections::HashSet<std::path::PathBuf> =
            to_delete.iter().cloned().collect();
        let deleting = to_delete;
        let unlink = tokio::task::spawn_blocking(move || {
            let mut failed = 0u64;
            for path in deleting {
                match crate::storage::block_store::delete(&path) {
                    Ok(_) => {}
                    Err(e) => {
                        failed += 1;
                        tracing::warn!(path = %path.display(), error = %e, "failed to delete expired block");
                    }
                }
            }
            failed
        })
        .await
        .map_err(|e| Error::Internal(format!("retention delete task panicked: {e}")))?;

        // Only drop the index entries for files we actually tried to delete.
        // Removing any other entry would silently orphan its data.
        let removed: Vec<std::path::PathBuf> = {
            let mut idx = index.write().await;
            let mut removed = Vec::new();
            idx.blocks.retain(|b| {
                if doomed.contains(&b.path) {
                    removed.push(b.path.clone());
                    false
                } else {
                    true
                }
            });
            store.mark_dirty();
            removed
        };

        tracing::info!(
            deleted_blocks = removed.len(),
            failed_deletes = unlink,
            retention_days = retention_days,
            "retention policy deleted expired blocks"
        );
        Ok(())
    }
}
