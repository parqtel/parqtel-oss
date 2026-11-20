//! Alert engine for parqtel: rules, state machine, evaluation, and storage.

pub mod builtin;
pub mod dispatcher;
pub mod egress;
pub mod error;
pub mod evaluator;
pub mod evidence;
pub mod notifier;
pub mod router;
pub mod routing;
pub mod rule;
pub mod state;
pub mod store;

pub use evidence::{AlertEvidence, EvidencePoint, RcaSummary, MAX_EVIDENCE_POINTS};
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
    /// Learned noise score: unacknowledged firings that resolved, derived from
    /// this instance's transition history (0 = acknowledged/still firing,
    /// 1 = every firing resolved unattended).
    pub noise_score: f32,
    pub transition_log: Vec<StateTransition>,
    pub notification_sent: bool,
    pub source_rule_type: RuleType,
    /// Query and bounded points captured when the alert was evaluated.
    #[serde(default)]
    pub evidence: Option<AlertEvidence>,
    /// Deterministic analysis derived from the evidence snapshot.
    #[serde(default)]
    pub rca: Option<RcaSummary>,
}

impl AlertInstance {
    /// Compute a stable fingerprint from rule_id and sorted labels.
    pub fn compute_fingerprint(rule_id: &str, labels: &BTreeMap<String, String>) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        const SEPARATOR: u8 = 0xff;

        fn update(hash: &mut u64, bytes: &[u8]) {
            for byte in bytes {
                *hash ^= u64::from(*byte);
                *hash = hash.wrapping_mul(FNV_PRIME);
            }
        }

        let mut hash = FNV_OFFSET_BASIS;
        update(&mut hash, rule_id.as_bytes());
        update(&mut hash, &[SEPARATOR]);
        for (k, v) in labels {
            update(&mut hash, k.as_bytes());
            update(&mut hash, &[SEPARATOR]);
            update(&mut hash, v.as_bytes());
            update(&mut hash, &[SEPARATOR]);
        }
        hash
    }
}

/// Event emitted for a firing transition or a noise suppression outcome.
/// The dispatcher counts suppressed events but does not deliver them.
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
    fn test_compute_fingerprint_golden_value() {
        let labels = BTreeMap::from([
            ("env".to_string(), "prod".to_string()),
            ("host".to_string(), "h1".to_string()),
        ]);

        assert_eq!(
            AlertInstance::compute_fingerprint("rule1", &labels),
            0xb18789b205ebeb66
        );
    }

    #[test]
    fn test_compute_fingerprint_separates_label_name_and_value() {
        let first = BTreeMap::from([("ab".to_string(), "c".to_string())]);
        let second = BTreeMap::from([("a".to_string(), "bc".to_string())]);

        assert_ne!(
            AlertInstance::compute_fingerprint("rule1", &first),
            AlertInstance::compute_fingerprint("rule1", &second)
        );
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

            evidence: None,

            rca: None,
        };
        let json = serde_json::to_string(&instance).unwrap();
        let decoded: AlertInstance = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.rule_id, "r1");
        assert_eq!(decoded.fingerprint, 12345);
    }
}
