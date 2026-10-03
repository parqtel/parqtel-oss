use super::index::{BlockIndex, BlockIndexStore};
use crate::config::BlockConfig;
use crate::error::{Error, Result};
use std::fs;
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
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let interval = Duration::from_secs(interval_secs.max(1));
        tracing::debug!(
            interval_secs = interval.as_secs(),
            retention_days = config.retention_days,
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
            if let Err(e) = Self::enforce(&index, &store, config.retention_days).await {
                tracing::error!("Retention failed: {}", e);
            }
        }
    }

    pub(crate) async fn enforce(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        retention_days: u64,
    ) -> Result<()> {
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let cutoff = now_ns - (retention_days as i64 * 24 * 3600 * 1_000_000_000);

        // Select under a read lock. Taking the write lock here would block
        // every query handler for the whole deletion batch.
        let to_delete: Vec<std::path::PathBuf> = {
            let idx = index.read().await;
            idx.blocks
                .iter()
                .filter(|b| b.end_timestamp_ns < cutoff)
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
        let deleting = to_delete.clone();
        let unlink = tokio::task::spawn_blocking(move || {
            let mut failed = 0u64;
            for path in deleting {
                match fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
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

        // Only drop the index entries for files that are actually gone.
        // Removing an entry whose file survives would silently orphan data.
        let removed: Vec<std::path::PathBuf> = {
            let mut idx = index.write().await;
            let mut removed = Vec::new();
            idx.blocks.retain(|b| {
                let expired = b.end_timestamp_ns < cutoff;
                if expired {
                    removed.push(b.path.clone());
                }
                !expired
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
