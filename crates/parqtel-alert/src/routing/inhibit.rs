use crate::error::NotificationError;
use crate::routing::matcher::MatcherSet;
use crate::{AlertInstance, AlertState};
use parqtel_core::config::InhibitRuleConfig;
use std::collections::BTreeMap;

/// A compiled Alertmanager inhibition rule.
#[derive(Debug, Clone)]
pub struct CompiledInhibitRule {
    pub source_matchers: MatcherSet,
    pub target_matchers: MatcherSet,
    pub equal: Vec<String>,
}

/// Evaluates whether a target alert is inhibited by any currently firing alert.
#[derive(Debug, Clone, Default)]
pub struct InhibitionEvaluator {
    rules: Vec<CompiledInhibitRule>,
}

impl InhibitionEvaluator {
    /// Compile all configured inhibition rules.
    pub fn compile(rules: &[InhibitRuleConfig]) -> Result<Self, NotificationError> {
        let mut compiled = Vec::with_capacity(rules.len());
        for rule in rules {
            compiled.push(CompiledInhibitRule {
                source_matchers: MatcherSet::compile_all(&rule.source_matchers)?,
                target_matchers: MatcherSet::compile_all(&rule.target_matchers)?,
                equal: rule.equal.clone(),
            });
        }
        Ok(Self { rules: compiled })
    }

    /// Return the compiled inhibition rules.
    pub fn rules(&self) -> &[CompiledInhibitRule] {
        &self.rules
    }

    /// Returns `true` if `target_labels` (with `target_fingerprint`) is inhibited by any
    /// firing alert in `firing_alerts`.
    ///
    /// Only alerts with `state == AlertState::Firing` and `fingerprint != target_fingerprint`
    /// can act as an inhibiting source.
    pub fn is_inhibited(
        &self,
        target_labels: &BTreeMap<String, String>,
        target_fingerprint: u64,
        firing_alerts: &[AlertInstance],
    ) -> bool {
        for rule in &self.rules {
            if !rule.target_matchers.matches(target_labels) {
                continue;
            }
            let matched = firing_alerts.iter().any(|src| {
                src.state == AlertState::Firing
                    && src.fingerprint != target_fingerprint
                    && rule.source_matchers.matches(&src.labels)
                    && rule
                        .equal
                        .iter()
                        .all(|k| src.labels.get(k) == target_labels.get(k))
            });
            if matched {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::{RuleType, Severity};
    use chrono::Utc;
    use ulid::Ulid;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn make_alert(fp: u64, state: AlertState, pairs: &[(&str, &str)]) -> AlertInstance {
        AlertInstance {
            id: Ulid::new(),
            rule_id: "r1".into(),
            rule_name: "R1".into(),
            fingerprint: fp,
            labels: labels(pairs),
            annotations: BTreeMap::new(),
            state,
            severity: Severity::Critical,
            value: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.0,
            transition_log: vec![],
            notification_sent: false,
            source_rule_type: RuleType::Static,
        }
    }

    #[test]
    fn test_inhibit_basic_and_equal_labels() {
        let eval = InhibitionEvaluator::compile(&[InhibitRuleConfig {
            source_matchers: vec!["severity = critical".to_string()],
            target_matchers: vec!["severity = warning".to_string()],
            equal: vec!["cluster".to_string(), "service".to_string()],
        }])
        .unwrap();
        assert_eq!(eval.rules().len(), 1);

        let src = make_alert(
            1,
            AlertState::Firing,
            &[
                ("severity", "critical"),
                ("cluster", "us-east"),
                ("service", "api"),
            ],
        );

        // Matching equal labels -> inhibited.
        assert!(eval.is_inhibited(
            &labels(&[
                ("severity", "warning"),
                ("cluster", "us-east"),
                ("service", "api"),
            ]),
            2,
            std::slice::from_ref(&src),
        ));

        // Different equal label -> NOT inhibited.
        assert!(!eval.is_inhibited(
            &labels(&[
                ("severity", "warning"),
                ("cluster", "us-west"),
                ("service", "api"),
            ]),
            3,
            std::slice::from_ref(&src),
        ));

        // Missing equal label on target while present on source -> NOT inhibited.
        assert!(!eval.is_inhibited(
            &labels(&[("severity", "warning"), ("cluster", "us-east")]),
            4,
            std::slice::from_ref(&src),
        ));
    }

    #[test]
    fn test_inhibit_both_absent_equal_label_matches() {
        let eval = InhibitionEvaluator::compile(&[InhibitRuleConfig {
            source_matchers: vec!["severity = critical".to_string()],
            target_matchers: vec!["severity = warning".to_string()],
            equal: vec!["namespace".to_string()],
        }])
        .unwrap();

        let src = make_alert(1, AlertState::Firing, &[("severity", "critical")]);
        assert!(eval.is_inhibited(&labels(&[("severity", "warning")]), 2, &[src]));
    }

    #[test]
    fn test_inhibit_skips_self_and_non_firing_alerts() {
        let eval = InhibitionEvaluator::compile(&[InhibitRuleConfig {
            source_matchers: vec!["severity = critical".to_string()],
            target_matchers: vec!["severity = critical".to_string()],
            equal: vec!["service".to_string()],
        }])
        .unwrap();

        let firing_same_fp = make_alert(
            42,
            AlertState::Firing,
            &[("severity", "critical"), ("service", "api")],
        );
        // Same fingerprint cannot inhibit itself.
        assert!(!eval.is_inhibited(
            &labels(&[("severity", "critical"), ("service", "api")]),
            42,
            &[firing_same_fp],
        ));

        // Pending / Resolved / Suppressed alerts cannot inhibit.
        for state in [
            AlertState::Pending,
            AlertState::Resolved,
            AlertState::Suppressed,
        ] {
            let non_firing = make_alert(99, state, &[("severity", "critical"), ("service", "api")]);
            assert!(!eval.is_inhibited(
                &labels(&[("severity", "critical"), ("service", "api")]),
                42,
                &[non_firing],
            ));
        }
    }

    #[test]
    fn test_inhibit_compile_error() {
        let err = InhibitionEvaluator::compile(&[InhibitRuleConfig {
            source_matchers: vec!["bad =~ [".to_string()],
            target_matchers: vec![],
            equal: vec![],
        }]);
        assert!(err.is_err());

        let err2 = InhibitionEvaluator::compile(&[InhibitRuleConfig {
            source_matchers: vec![],
            target_matchers: vec!["no_op".to_string()],
            equal: vec![],
        }]);
        assert!(err2.is_err());
    }
}
