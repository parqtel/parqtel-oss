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
    default_noise_threshold: f32,
    noise_window_firings: usize,
}

impl EvaluationEngine {
    pub fn new(store: AlertStore, event_tx: mpsc::UnboundedSender<AlertFiringEvent>) -> Self {
        Self {
            store,
            event_tx,
            default_noise_threshold: 0.7,
            noise_window_firings: 30,
        }
    }

    pub fn with_noise_threshold(mut self, threshold: f32) -> Self {
        self.default_noise_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    pub fn with_noise_window(mut self, firings: usize) -> Self {
        self.noise_window_firings = firings.max(1);
        self
    }

    fn noise_score(&self, log: &[crate::state::machine::StateTransition]) -> f32 {
        let keep = self.noise_window_firings.saturating_mul(2);
        let start = log.len().saturating_sub(keep);
        noise_score(&log[start..])
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
        self.evaluate_rule_with_evidence(rule, value, labels, condition_met, None, None)
            .await;
    }

    pub async fn evaluate_rule_with_evidence(
        &self,
        rule: &AlertRule,
        value: f64,
        labels: BTreeMap<String, String>,
        condition_met: bool,
        evidence: Option<crate::AlertEvidence>,
        rca: Option<crate::RcaSummary>,
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
                            let prior_noise = instance.noise_score;
                            let now = Utc::now();
                            let was_rearmed = new_state == AlertState::Pending;
                            instance.state = new_state;
                            instance.updated_at = now;
                            instance.value = Some(value);
                            if evidence.is_some() {
                                instance.evidence = evidence.clone();
                            }
                            if rca.is_some() {
                                instance.rca = rca.clone();
                            }
                            if was_rearmed {
                                // A resolved instance that starts firing again is a
                                // new incident. Reusing the previous start time
                                // makes for_duration look already satisfied and
                                // reports a stale duration in the UI.
                                instance.started_at = now;
                                instance.resolved_at = None;
                                instance.acknowledged_by = None;
                                instance.notification_sent = false;
                            }
                            instance.transition_log.push(transition);
                            instance.noise_score = self.noise_score(&instance.transition_log);
                            self.store.save(&instance).await;

                            if new_state == AlertState::Firing && was_not_firing {
                                let firing_count = instance
                                    .transition_log
                                    .iter()
                                    .filter(|t| t.to == AlertState::Firing)
                                    .count();
                                let threshold = rule
                                    .noise_suppression_threshold
                                    .unwrap_or(self.default_noise_threshold)
                                    .clamp(0.0, 1.0);
                                let should_suppress = firing_count >= 3
                                    && threshold < 1.0
                                    && prior_noise >= threshold;
                                if should_suppress {
                                    if let Some((suppressed, transition)) =
                                        AlertStateMachine::transition(
                                            instance.state,
                                            TransitionEvent::NoiseSuppressed,
                                        )
                                    {
                                        instance.state = suppressed;
                                        instance.updated_at = Utc::now();
                                        instance.transition_log.push(transition);
                                        self.store.save(&instance).await;
                                        tracing::warn!(
                                            rule_id = %rule.id,
                                            fingerprint = %fingerprint,
                                            noise_score = instance.noise_score,
                                            "alert suppressed by noise policy"
                                        );
                                        let _ = self.event_tx.send(AlertFiringEvent {
                                            instance: instance.clone(),
                                        });
                                        return;
                                    }
                                }
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
                        if evidence.is_some() {
                            instance.evidence = evidence.clone();
                        }
                        if rca.is_some() {
                            instance.rca = rca.clone();
                        }
                        self.store.save(&instance).await;
                    }
                } else {
                    let event = match instance.state {
                        AlertState::Pending
                        | AlertState::Firing
                        | AlertState::Acknowledged
                        | AlertState::Suppressed
                        | AlertState::NoiseFlagged => Some(TransitionEvent::ConditionCleared),
                        _ => None,
                    };
                    if let Some(evt) = event {
                        if let Some((new_state, transition)) =
                            AlertStateMachine::transition(instance.state, evt)
                        {
                            instance.state = new_state;
                            instance.updated_at = Utc::now();
                            instance.resolved_at = Some(Utc::now());
                            if evidence.is_some() {
                                instance.evidence = evidence.clone();
                            }
                            if rca.is_some() {
                                instance.rca = rca.clone();
                            }
                            instance.transition_log.push(transition);
                            instance.noise_score = self.noise_score(&instance.transition_log);
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
                        evidence,
                        rca,
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
    use crate::RuleType;
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
            noise_suppression_threshold: None,
            source: None,
        }
    }

    #[tokio::test]
    async fn test_evidence_and_rca_are_stored_on_the_instance() {
        let store = AlertStore::in_memory();
        let (tx, _rx) = mpsc::unbounded_channel();
        let rule = test_rule();
        let engine = EvaluationEngine::new(store.clone(), tx);
        let labels = BTreeMap::from([("service.name".into(), "api".into())]);
        let fingerprint = AlertInstance::compute_fingerprint(&rule.id, &labels);
        let points = vec![
            crate::EvidencePoint {
                timestamp_ns: 1,
                value: 1.0,
            },
            crate::EvidencePoint {
                timestamp_ns: 2,
                value: 2.0,
            },
        ];
        let evidence = crate::AlertEvidence {
            query: rule.query.clone(),
            signal: rule.signal.clone(),
            window_start_ns: 0,
            window_end_ns: 2,
            operator: Some(">".into()),
            threshold: Some(0.5),
            observed: 2.0,
            points: points.clone(),
            series_count: 1,
        };
        let rca = crate::RcaSummary::threshold(&rule.name, 2.0, ">", 0.5, 0, &points);

        engine
            .evaluate_rule_with_evidence(
                &rule,
                2.0,
                labels.clone(),
                true,
                Some(evidence.clone()),
                Some(rca.clone()),
            )
            .await;
        engine
            .evaluate_rule_with_evidence(
                &rule,
                2.0,
                labels,
                true,
                Some(evidence.clone()),
                Some(rca.clone()),
            )
            .await;

        let alert = store.get_by_fingerprint(fingerprint).await.unwrap();
        assert_eq!(alert.evidence, Some(evidence));
        assert_eq!(alert.rca, Some(rca));
    }

    #[tokio::test]
    async fn test_repeated_unacknowledged_firings_are_suppressed() {
        let store = AlertStore::in_memory();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let rule = test_rule();
        let engine = EvaluationEngine::new(store.clone(), tx);
        let labels = BTreeMap::from([("service.name".into(), "api".into())]);
        let fingerprint = AlertInstance::compute_fingerprint(&rule.id, &labels);

        for _ in 0..2 {
            engine
                .evaluate_rule_with_value(&rule, 1.0, labels.clone())
                .await;
            engine
                .evaluate_rule_with_value(&rule, 1.0, labels.clone())
                .await;
            assert!(rx.try_recv().is_ok(), "first two firings notify");
            engine
                .evaluate_rule_with_value(&rule, 0.0, labels.clone())
                .await;
            assert!(rx.try_recv().is_ok(), "resolution notifies");
        }

        engine
            .evaluate_rule_with_value(&rule, 1.0, labels.clone())
            .await;
        engine.evaluate_rule_with_value(&rule, 1.0, labels).await;
        let alert = store.get_by_fingerprint(fingerprint).await.unwrap();
        assert_eq!(alert.state, AlertState::Suppressed);
        let suppressed = rx.try_recv().expect("suppression event for metrics");
        assert_eq!(suppressed.instance.state, AlertState::Suppressed);
    }

    #[tokio::test]
    async fn test_refire_resets_start_and_sustain_window() {
        let store = AlertStore::in_memory();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rule = test_rule();
        rule.condition.for_duration_secs = 60;
        let engine = EvaluationEngine::new(store.clone(), tx);
        let labels = BTreeMap::from([("service.name".into(), "api".into())]);
        let fingerprint = AlertInstance::compute_fingerprint(&rule.id, &labels);
        let old = Utc::now() - chrono::Duration::hours(1);

        let resolved = AlertInstance {
            id: Ulid::new(),
            rule_id: rule.id.clone(),
            rule_name: rule.name.clone(),
            fingerprint,
            labels: labels.clone(),
            annotations: BTreeMap::new(),
            state: AlertState::Resolved,
            severity: Severity::Warning,
            value: Some(0.0),
            started_at: old,
            updated_at: old,
            resolved_at: Some(old),
            acknowledged_by: Some("operator".into()),
            noise_score: 0.0,
            transition_log: Vec::new(),
            notification_sent: true,
            source_rule_type: RuleType::Static,

            evidence: None,

            rca: None,
        };
        store.save(&resolved).await;

        engine.evaluate_rule_with_value(&rule, 1.0, labels).await;

        let pending = store.get_by_fingerprint(fingerprint).await.unwrap();
        assert_eq!(pending.state, AlertState::Pending);
        assert!(pending.started_at > old);
        assert!(pending.resolved_at.is_none());
        assert!(pending.acknowledged_by.is_none());
        assert!(!pending.notification_sent);
        assert!(rx.try_recv().is_err(), "sustain window must restart");
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
