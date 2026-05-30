use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use tracing::{error, info};

use crate::config::RulerConfig;
use crate::rule::RuleRegistry;

use super::evaluator::RulerEvaluator;

/// Schedules recording rule evaluations at their configured intervals.
pub struct RulerScheduler {
    config: RulerConfig,
    registry: RuleRegistry,
    evaluator: Arc<RulerEvaluator>,
    /// Tracks last evaluation time per group (ns).
    last_eval: Arc<RwLock<BTreeMap<String, i64>>>,
}

impl RulerScheduler {
    pub fn new(config: RulerConfig, registry: RuleRegistry, evaluator: Arc<RulerEvaluator>) -> Self {
        Self {
            config,
            registry,
            evaluator,
            last_eval: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Run the scheduler loop. Call this in a tokio::spawn.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut tick = interval(Duration::from_secs(1));
        info!("Ruler scheduler started");

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    self.evaluate_due_groups().await;
                }
                _ = shutdown.changed() => {
                    info!("Ruler scheduler shutting down");
                    break;
                }
            }
        }
    }

    async fn evaluate_due_groups(&self) {
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let groups = self.registry.get_groups();

        for group in &groups {
            let interval_secs =
                crate::rule::validator::parse_duration(&group.interval).unwrap_or(60);
            let interval_ns = interval_secs as i64 * 1_000_000_000;

            let last = {
                let state = self.last_eval.read().await;
                state.get(&group.name).copied().unwrap_or(0)
            };

            if now_ns - last >= interval_ns {
                // Truncate to interval boundary
                let eval_ts = (now_ns / interval_ns) * interval_ns;
                if let Err(e) = self.evaluator.evaluate_group(group, eval_ts).await {
                    error!("Failed to evaluate group '{}': {}", group.name, e);
                } else {
                    let mut state = self.last_eval.write().await;
                    state.insert(group.name.clone(), eval_ts);
                }
            }
        }
    }

    /// Get last evaluation timestamps (for API).
    pub async fn get_state(&self) -> BTreeMap<String, i64> {
        self.last_eval.read().await.clone()
    }

    /// Persist state to disk.
    pub async fn save_state(&self) -> crate::Result<()> {
        let state = self.last_eval.read().await;
        let json = serde_json::to_string_pretty(&*state)
            .map_err(|e| crate::Error::Io(e.to_string()))?;
        if let Some(parent) = std::path::Path::new(&self.config.state_file).parent() {
            std::fs::create_dir_all(parent).map_err(|e| crate::Error::Io(e.to_string()))?;
        }
        std::fs::write(&self.config.state_file, json)
            .map_err(|e| crate::Error::Io(e.to_string()))?;
        Ok(())
    }

    /// Load state from disk.
    pub async fn load_state(&self) -> crate::Result<()> {
        let path = std::path::Path::new(&self.config.state_file);
        if !path.exists() {
            return Ok(());
        }
        let content = std::fs::read_to_string(path).map_err(|e| crate::Error::Io(e.to_string()))?;
        let loaded: BTreeMap<String, i64> =
            serde_json::from_str(&content).map_err(|e| crate::Error::Parse(e.to_string()))?;
        let mut state = self.last_eval.write().await;
        *state = loaded;
        Ok(())
    }
}
