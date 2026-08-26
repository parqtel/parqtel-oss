use chrono::Utc;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use ulid::Ulid;

use crate::evaluator::threshold::evaluate_threshold;
use crate::rule::registry::AlertRuleRegistry;
use crate::rule::types::AlertRule;
use crate::state::machine::{AlertState, AlertStateMachine, TransitionEvent};
use crate::store::alert_store::AlertStore;
use crate::{AlertFiringEvent, AlertInstance};

/// Configuration for the evaluation engine.
#[derive(Debug, Clone)]
pub struct EvalConfig {
    pub evaluation_interval_secs: u64,
    pub evaluation_timeout_secs: u64,
}

/// The background evaluation engine.
pub struct EvaluationEngine {
    config: EvalConfig,
    registry: AlertRuleRegistry,
    store: AlertStore,
    event_tx: mpsc::UnboundedSender<AlertFiringEvent>,
}

impl EvaluationEngine {
    pub fn new(
        config: EvalConfig,
        registry: AlertRuleRegistry,
        store: AlertStore,
        event_tx: mpsc::UnboundedSender<AlertFiringEvent>,
    ) -> Self {
        Self {
            config,
            registry,
            store,
            event_tx,
        }
    }

    /// Start the evaluation loop as a background task.
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let interval = Duration::from_secs(self.config.evaluation_interval_secs);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown.changed() => {
                    tracing::info!("alert evaluation engine shutting down");
                    return;
                }
            }
            self.evaluate_all().await;
        }
    }

    async fn evaluate_all(&self) {
        let rules = self.registry.list_enabled().await;
        let timeout = Duration::from_secs(self.config.evaluation_timeout_secs);
        let mut join_set = JoinSet::new();

        for rule in rules {
            let store = self.store.clone();
            let event_tx = self.event_tx.clone();

            join_set.spawn(async move {
                let result = tokio::time::timeout(timeout, async {
                    Self::evaluate_rule(&rule, &store, &event_tx).await;
                })
                .await;
                if result.is_err() {
                    tracing::warn!(rule_id = %rule.id, "rule evaluation timed out");
                }
            });
        }

        while join_set.join_next().await.is_some() {}
    }

    async fn evaluate_rule(
        rule: &AlertRule,
        store: &AlertStore,
        _event_tx: &mpsc::UnboundedSender<AlertFiringEvent>,
    ) {
        let labels: BTreeMap<String, String> = rule.labels.clone();
        let fingerprint = AlertInstance::compute_fingerprint(&rule.id, &labels);
        let _existing = store.get_by_fingerprint(fingerprint).await;
        // In a full implementation, this queries the storage layer and evaluates conditions.
        // The query field on the rule would be executed against the metrics store.
    }

    /// Evaluate a single rule with a provided metric value (for testing/direct use).
    pub async fn evaluate_rule_with_value(
        &self,
        rule: &AlertRule,
        value: f64,
        labels: BTreeMap<String, String>,
    ) {
        let condition_met = evaluate_threshold(&rule.condition, value);
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
                            self.store.save(&instance).await;

                            if new_state == AlertState::Firing && was_not_firing {
                                let _ = self.event_tx.send(AlertFiringEvent {
                                    instance: instance.clone(),
                                });
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
                            self.store.save(&instance).await;
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::rule::types::{Condition, Operator, Severity};

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

    fn test_config() -> EvalConfig {
        EvalConfig {
            evaluation_interval_secs: 1,
            evaluation_timeout_secs: 10,
        }
    }

    #[tokio::test]
    async fn test_evaluate_rule_no_existing_instance() {
        let store = AlertStore::in_memory();
        let (tx, _rx) = mpsc::unbounded_channel();
        let rule = test_rule();
        EvaluationEngine::evaluate_rule(&rule, &store, &tx).await;
    }

    #[tokio::test]
    async fn test_evaluate_all_timeout() {
        let store = AlertStore::in_memory();
        let (tx, _rx) = mpsc::unbounded_channel();
        let registry = AlertRuleRegistry::new();
        let rule = test_rule();
        registry.insert(rule).await;

        let engine = EvaluationEngine::new(test_config(), registry, store, tx);
        engine.evaluate_all().await;
    }
}
