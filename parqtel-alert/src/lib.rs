//! Alert engine for parqtel: rules, state machine, evaluation, and storage.

pub mod evaluator;
pub mod rule;
pub mod state;
pub mod store;

pub use rule::registry::AlertRuleRegistry;
pub use rule::types::{AlertRule, Condition, RuleSource, RuleType, Severity};
pub use state::machine::{AlertState, AlertStateMachine, StateTransition};
pub use store::alert_store::AlertStore;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

/// A single firing of a rule for a specific label set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertInstance {
    pub id: Ulid,
    pub rule_id: String,
    pub rule_name: String,
    pub fingerprint: u64,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub state: AlertState,
    pub severity: Severity,
    pub value: Option<f64>,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub noise_score: f32,
    pub transition_log: Vec<StateTransition>,
    pub notification_sent: bool,
    pub source_rule_type: RuleType,
}

impl AlertInstance {
    /// Compute a stable fingerprint from rule_id and sorted labels.
    pub fn compute_fingerprint(rule_id: &str, labels: &BTreeMap<String, String>) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        rule_id.hash(&mut hasher);
        for (k, v) in labels {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Event emitted when an alert transitions to Firing.
#[derive(Debug, Clone)]
pub struct AlertFiringEvent {
    pub instance: AlertInstance,
}

/// Hook trait for extending alert behavior (noise scoring, notifications, etc.)
pub trait AlertHook: Send + Sync {
    /// Called when an alert changes state.
    fn on_state_change(&self, instance: &AlertInstance, old: AlertState, new: AlertState);
    /// Called after each evaluation cycle.
    fn on_evaluation(&self, rule_id: &str, fired: bool, value: Option<f64>);
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_compute_fingerprint_stability() {
        let mut labels = BTreeMap::new();
        labels.insert("host".to_string(), "h1".to_string());
        labels.insert("env".to_string(), "prod".to_string());

        let fp1 = AlertInstance::compute_fingerprint("rule1", &labels);
        let fp2 = AlertInstance::compute_fingerprint("rule1", &labels);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_compute_fingerprint_different_rules() {
        let labels = BTreeMap::new();
        let fp1 = AlertInstance::compute_fingerprint("rule1", &labels);
        let fp2 = AlertInstance::compute_fingerprint("rule2", &labels);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_compute_fingerprint_different_labels() {
        let mut l1 = BTreeMap::new();
        l1.insert("host".to_string(), "h1".to_string());
        let mut l2 = BTreeMap::new();
        l2.insert("host".to_string(), "h2".to_string());

        let fp1 = AlertInstance::compute_fingerprint("rule1", &l1);
        let fp2 = AlertInstance::compute_fingerprint("rule1", &l2);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_alert_instance_serialization() {
        let instance = AlertInstance {
            id: Ulid::new(),
            rule_id: "r1".into(),
            rule_name: "Rule 1".into(),
            fingerprint: 12345,
            labels: BTreeMap::from([("host".into(), "h1".into())]),
            annotations: BTreeMap::new(),
            state: AlertState::Firing,
            severity: Severity::Critical,
            value: Some(99.9),
            started_at: Utc::now(),
            updated_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.0,
            transition_log: vec![],
            notification_sent: false,
            source_rule_type: RuleType::Static,
        };
        let json = serde_json::to_string(&instance).unwrap();
        let decoded: AlertInstance = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.rule_id, "r1");
        assert_eq!(decoded.fingerprint, 12345);
    }
}
