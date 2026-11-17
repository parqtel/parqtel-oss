use chrono::Utc;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use ulid::Ulid;

use crate::evaluator::threshold::evaluate_threshold;
use crate::rule::types::AlertRule;
use crate::state::machine::{AlertState, AlertStateMachine, TransitionEvent};
use crate::store::alert_store::AlertStore;
use crate::{AlertFiringEvent, AlertInstance};

/// Applies threshold outcomes to alert state and emits firing/resolved events.
/// The query loop lives in `parqtel-server::tasks`; this type only owns the
/// state machine and event sender.
pub struct EvaluationEngine {
    store: AlertStore,
    event_tx: mpsc::UnboundedSender<AlertFiringEvent>,
}

impl EvaluationEngine {
    pub fn new(store: AlertStore, event_tx: mpsc::UnboundedSender<AlertFiringEvent>) -> Self {
        Self { store, event_tx }
    }

    /// Evaluate a single rule with a provided metric value (for testing/direct use).
    pub async fn evaluate_rule_with_value(
        &self,
        rule: &AlertRule,
        value: f64,
        labels: BTreeMap<String, String>,
    ) {
        let condition_met = evaluate_threshold(&rule.condition, value);
        self.evaluate_rule_with_condition(rule, value, labels, condition_met)
            .await;
    }

    /// Apply an externally decided condition (used by the baseline anomaly
    /// detector, which does not fit the static threshold operator).
    pub async fn evaluate_rule_with_condition(
        &self,
        rule: &AlertRule,
        value: f64,
        labels: BTreeMap<String, String>,
        condition_met: bool,
    ) {
        let fingerprint = AlertInstance::compute_fingerprint(&rule.id, &labels);
        let existing = self.store.get_by_fingerprint(fingerprint).await;

        match existing {
            Some(mut instance) => {
                if condition_met {
                    let event = match instance.state {
                        AlertState::Pending => {
                            let elapsed = Utc::now()
                                .signed_duration_since(instance.started_at)
                                .num_seconds() as u64;
                            if elapsed >= rule.condition.for_duration_secs {
                                Some(TransitionEvent::DurationElapsed)
                            } else {
                                None
                            }
                        }
                        AlertState::Resolved => Some(TransitionEvent::ConditionMet),
                        _ => None,
                    };
                    if let Some(evt) = event {
                        if let Some((new_state, transition)) =
                            AlertStateMachine::transition(instance.state, evt)
                        {
                            let was_not_firing = instance.state != AlertState::Firing;
                            instance.state = new_state;
                            instance.updated_at = Utc::now();
                            instance.value = Some(value);
                            instance.transition_log.push(transition);
                            instance.noise_score = noise_score(&instance.transition_log);
                            self.store.save(&instance).await;

                            if new_state == AlertState::Firing && was_not_firing {
                                let _ = self.event_tx.send(AlertFiringEvent {
                                    instance: instance.clone(),
                                });
                                tracing::info!(
                                    rule_id = %rule.id,
                                    fingerprint = %fingerprint,
                                    value = value,
                                    "alert fired: condition met for duration"
                                );
                            }
                        }
                    } else {
                        instance.value = Some(value);
                        instance.updated_at = Utc::now();
                        self.store.save(&instance).await;
                    }
                } else {
                    let event = match instance.state {
                        AlertState::Pending | AlertState::Firing | AlertState::Acknowledged => {
                            Some(TransitionEvent::ConditionCleared)
                        }
                        _ => None,
                    };
                    if let Some(evt) = event {
                        if let Some((new_state, transition)) =
                            AlertStateMachine::transition(instance.state, evt)
                        {
                            instance.state = new_state;
                            instance.updated_at = Utc::now();
                            instance.resolved_at = Some(Utc::now());
                            instance.transition_log.push(transition);
                            instance.noise_score = noise_score(&instance.transition_log);
                            self.store.save(&instance).await;
                            if new_state == AlertState::Resolved {
                                let _ = self.event_tx.send(AlertFiringEvent {
                                    instance: instance.clone(),
                                });
                            }
                        }
                    }
                }
            }
            None => {
                if condition_met {
                    let instance = AlertInstance {
                        id: Ulid::new(),
                        rule_id: rule.id.clone(),
                        rule_name: rule.name.clone(),
                        fingerprint,
                        labels,
                        annotations: rule.annotations.clone(),
                        state: AlertState::Pending,
                        severity: rule.severity,
                        value: Some(value),
                        started_at: Utc::now(),
                        updated_at: Utc::now(),
                        resolved_at: None,
                        acknowledged_by: None,
                        noise_score: 0.0,
                        transition_log: Vec::new(),
                        notification_sent: false,
                        source_rule_type: rule.rule_type(),
                    };
                    self.store.save(&instance).await;
                }
            }
        }
    }
}

/// Fraction of firings that resolved without being acknowledged.
///
/// A rule that fires and clears unattended is noise; a rule whose firings are
/// acknowledged (or are still open) scores lower. Derived only from the
/// instance's own transition history, so it is explainable from the alert.
fn noise_score(log: &[crate::state::machine::StateTransition]) -> f32 {
    let fired = log.iter().filter(|t| t.to == AlertState::Firing).count();
    if fired == 0 {
        return 0.0;
    }
    let acknowledged = log
        .iter()
        .filter(|t| t.to == AlertState::Acknowledged)
        .count()
        .min(fired);
    let resolved = log
        .iter()
        .filter(|t| t.to == AlertState::Resolved)
        .count()
        .min(fired);
    let unacked = (fired - acknowledged) as f32 / fired as f32;
    let resolved_share = resolved as f32 / fired as f32;
    (unacked * resolved_share).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::rule::types::{Condition, Operator, Severity};
    use crate::state::machine::StateTransition;
    use chrono::Utc;

    #[test]
    fn noise_score_reflects_unacknowledged_resolves() {
        let t = |from, to| StateTransition {
            from,
            to,
            at: Utc::now(),
            reason: String::new(),
        };
        let noisy = vec![
            t(AlertState::Pending, AlertState::Firing),
            t(AlertState::Firing, AlertState::Resolved),
        ];
        assert_eq!(super::noise_score(&noisy), 1.0);
        let acked = vec![
            t(AlertState::Pending, AlertState::Firing),
            t(AlertState::Firing, AlertState::Acknowledged),
            t(AlertState::Acknowledged, AlertState::Resolved),
        ];
        assert_eq!(super::noise_score(&acked), 0.0);
    }

    fn test_rule() -> AlertRule {
        AlertRule {
            id: "t".into(),
            name: "t".into(),
            signal: "metrics".into(),
            query: "t{}".into(),
            condition: Condition {
                condition_type: "threshold".into(),
                operator: Operator::Gt,
                value: 0.5,
                for_duration_secs: 0,
            },
            severity: Severity::Warning,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            enabled: true,
            noise_suppression_threshold: 0.7,
            source: None,
        }
    }

    #[tokio::test]
    async fn test_evaluate_rule_with_value_firing_and_resolved_events() {
        let store = AlertStore::in_memory();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let rule = test_rule();
        let engine = EvaluationEngine::new(store.clone(), tx);
        let labels = BTreeMap::from([("service.name".into(), "api".into())]);

        // 1. Condition met -> Pending (no event yet)
        engine
            .evaluate_rule_with_value(&rule, 1.0, labels.clone())
            .await;
        assert!(rx.try_recv().is_err());

        // 2. Condition still met (for_duration_secs = 0) -> Firing event emitted
        engine
            .evaluate_rule_with_value(&rule, 2.0, labels.clone())
            .await;
        let firing_evt = rx.try_recv().expect("expected firing event");
        assert_eq!(firing_evt.instance.state, AlertState::Firing);

        // 3. Condition cleared -> Resolved event emitted
        engine
            .evaluate_rule_with_value(&rule, 0.1, labels.clone())
            .await;
        let resolved_evt = rx.try_recv().expect("expected resolved event");
        assert_eq!(resolved_evt.instance.state, AlertState::Resolved);
    }
}
