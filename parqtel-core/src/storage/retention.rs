use super::index::BlockIndex;
use crate::config::BlockConfig;
use crate::error::Result;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Background task that deletes expired blocks.
pub struct RetentionPolicy;

impl RetentionPolicy {
    pub async fn run_loop(index: Arc<RwLock<BlockIndex>>, config: BlockConfig) {
        let interval = Duration::from_secs(3600);
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = Self::enforce(&index, config.retention_days).await {
                tracing::error!("Retention failed: {}", e);
            }
        }
    }

    pub(crate) async fn enforce(
        index: &Arc<RwLock<BlockIndex>>,
        retention_days: u64,
    ) -> Result<()> {
        let mut idx = index.write().await;
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let cutoff = now_ns - (retention_days as i64 * 24 * 3600 * 1_000_000_000);

        let mut to_delete = Vec::new();
        idx.blocks.retain(|b| {
            if b.end_timestamp_ns < cutoff {
                to_delete.push(b.path.clone());
                false
            } else {
                true
            }
        });

        if !to_delete.is_empty() {
            idx.save()?;
            for path in to_delete {
                let _ = fs::remove_file(path);
            }
        }
        Ok(())
    }
}
