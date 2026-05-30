use std::cmp::Reverse;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use ulid::Ulid;

use crate::AlertInstance;
use crate::state::machine::AlertState;

/// Persisted alert store backed by JSON files.
/// In production this would use Parquet via the storage layer;
/// for this implementation we use JSON for simplicity and correctness.
#[derive(Debug, Clone)]
pub struct AlertStore {
    instances: Arc<RwLock<HashMap<Ulid, AlertInstance>>>,
    fingerprint_index: Arc<RwLock<HashMap<u64, Ulid>>>,
    data_dir: Option<PathBuf>,
}

impl AlertStore {
    pub async fn new(data_dir: Option<PathBuf>) -> Self {
        if let Some(ref dir) = data_dir {
            let _ = std::fs::create_dir_all(dir);
        }
        let store = Self {
            instances: Arc::new(RwLock::new(HashMap::new())),
            fingerprint_index: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
        };
        store.load_from_disk().await;
        store
    }

    /// Create a store without loading from disk (for tests).
    pub fn in_memory() -> Self {
        Self {
            instances: Arc::new(RwLock::new(HashMap::new())),
            fingerprint_index: Arc::new(RwLock::new(HashMap::new())),
            data_dir: None,
        }
    }

    pub async fn save(&self, instance: &AlertInstance) {
        let mut instances = self.instances.write().await;
        let mut fp_index = self.fingerprint_index.write().await;
        fp_index.insert(instance.fingerprint, instance.id);
        instances.insert(instance.id, instance.clone());
        drop(instances);
        drop(fp_index);
        self.persist_to_disk().await;
    }

    pub async fn get_by_id(&self, id: Ulid) -> Option<AlertInstance> {
        self.instances.read().await.get(&id).cloned()
    }

    pub async fn get_by_fingerprint(&self, fingerprint: u64) -> Option<AlertInstance> {
        let fp_index = self.fingerprint_index.read().await;
        let id = fp_index.get(&fingerprint)?;
        self.instances.read().await.get(id).cloned()
    }

    pub async fn list_active(&self) -> Vec<AlertInstance> {
        self.instances.read().await.values()
            .filter(|i| matches!(i.state, AlertState::Firing | AlertState::Acknowledged | AlertState::Pending))
            .cloned()
            .collect()
    }

    pub async fn list_recent(&self, since: DateTime<Utc>, limit: usize) -> Vec<AlertInstance> {
        let mut results: Vec<_> = self.instances.read().await.values()
            .filter(|i| i.updated_at >= since)
            .cloned()
            .collect();
        results.sort_by_key(|a| Reverse(a.updated_at));
        results.truncate(limit);
        results
    }

    pub async fn list_by_rule(&self, rule_id: &str, since: DateTime<Utc>) -> Vec<AlertInstance> {
        self.instances.read().await.values()
            .filter(|i| i.rule_id == rule_id && i.updated_at >= since)
            .cloned()
            .collect()
    }

    pub async fn list_suppressed(&self) -> Vec<AlertInstance> {
        self.instances.read().await.values()
            .filter(|i| i.state == AlertState::Suppressed)
            .cloned()
            .collect()
    }

    async fn persist_to_disk(&self) {
        let Some(ref dir) = self.data_dir else { return };
        let instances = self.instances.read().await;
        let data: Vec<&AlertInstance> = instances.values().collect();
        let path = dir.join("alerts.json");
        if let Ok(json) = serde_json::to_string_pretty(&data) {
            let tmp = dir.join("alerts.json.tmp");
            if std::fs::write(&tmp, &json).is_ok() {
                let _ = std::fs::rename(tmp, path);
            }
        }
    }

    async fn load_from_disk(&self) {
        let Some(ref dir) = self.data_dir else { return };
        let path = dir.join("alerts.json");
        if !path.exists() {
            return;
        }
        let Ok(content) = std::fs::read_to_string(&path) else { return };
        let Ok(alerts): Result<Vec<AlertInstance>, _> = serde_json::from_str(&content) else { return };
        let mut instances = self.instances.write().await;
        let mut fp_index = self.fingerprint_index.write().await;
        for alert in alerts {
            fp_index.insert(alert.fingerprint, alert.id);
            instances.insert(alert.id, alert);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::{AlertInstance, Severity, RuleType};
    use std::collections::BTreeMap;

    fn sample_instance(rule_id: &str) -> AlertInstance {
        let labels = BTreeMap::from([("env".to_string(), "prod".to_string())]);
        let fp = AlertInstance::compute_fingerprint(rule_id, &labels);
        AlertInstance {
            id: Ulid::new(), rule_id: rule_id.into(), rule_name: "test".into(),
            fingerprint: fp, labels, annotations: BTreeMap::new(),
            state: AlertState::Firing, severity: Severity::Warning, value: Some(95.0),
            started_at: Utc::now(), updated_at: Utc::now(), resolved_at: None,
            acknowledged_by: None, noise_score: 0.0, transition_log: vec![],
            notification_sent: false, source_rule_type: RuleType::Static,
        }
    }

    #[tokio::test]
    async fn test_save_and_get_by_id() {
        let store = AlertStore::in_memory();
        let instance = sample_instance("r1");
        store.save(&instance).await;
        let fetched = store.get_by_id(instance.id).await.unwrap();
        assert_eq!(fetched.rule_id, "r1");
    }

    #[tokio::test]
    async fn test_get_by_fingerprint() {
        let store = AlertStore::in_memory();
        let instance = sample_instance("r2");
        let fp = instance.fingerprint;
        store.save(&instance).await;
        let fetched = store.get_by_fingerprint(fp).await.unwrap();
        assert_eq!(fetched.id, instance.id);
    }

    #[tokio::test]
    async fn test_list_active() {
        let store = AlertStore::in_memory();
        let mut firing = sample_instance("r1");
        firing.state = AlertState::Firing;
        let mut resolved = sample_instance("r2");
        resolved.state = AlertState::Resolved;
        store.save(&firing).await;
        store.save(&resolved).await;
        let active = store.list_active().await;
        assert_eq!(active.len(), 1);
    }

    #[tokio::test]
    async fn test_list_recent() {
        let store = AlertStore::in_memory();
        store.save(&sample_instance("r1")).await;
        let recent = store.list_recent(Utc::now() - chrono::Duration::hours(1), 10).await;
        assert_eq!(recent.len(), 1);
    }

    #[tokio::test]
    async fn test_list_by_rule() {
        let store = AlertStore::in_memory();
        store.save(&sample_instance("target")).await;
        store.save(&sample_instance("other")).await;
        let results = store.list_by_rule("target", Utc::now() - chrono::Duration::hours(1)).await;
        assert_eq!(results.len(), 1);
    }
}
