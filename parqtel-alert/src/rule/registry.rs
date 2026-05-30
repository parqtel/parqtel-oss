use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::rule::types::AlertRule;

/// In-memory store of alert rules with CRUD operations.
#[derive(Debug, Clone)]
pub struct AlertRuleRegistry {
    rules: Arc<RwLock<HashMap<String, AlertRule>>>,
}

impl AlertRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn insert(&self, rule: AlertRule) {
        self.rules.write().await.insert(rule.id.clone(), rule);
    }

    pub async fn remove(&self, id: &str) -> Option<AlertRule> {
        self.rules.write().await.remove(id)
    }

    pub async fn get(&self, id: &str) -> Option<AlertRule> {
        self.rules.read().await.get(id).cloned()
    }

    pub async fn list_enabled(&self) -> Vec<AlertRule> {
        self.rules.read().await.values().filter(|r| r.enabled).cloned().collect()
    }

    pub async fn list_all(&self) -> Vec<AlertRule> {
        self.rules.read().await.values().cloned().collect()
    }

    pub async fn update(&self, rule: AlertRule) -> bool {
        let mut rules = self.rules.write().await;
        if rules.contains_key(&rule.id) {
            rules.insert(rule.id.clone(), rule);
            true
        } else {
            false
        }
    }

    pub async fn disable(&self, id: &str) -> bool {
        let mut rules = self.rules.write().await;
        if let Some(rule) = rules.get_mut(id) {
            rule.enabled = false;
            true
        } else {
            false
        }
    }
}

impl Default for AlertRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}
