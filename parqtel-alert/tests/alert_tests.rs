#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use chrono::{Utc, Duration as ChronoDuration};
use tokio::sync::{mpsc, watch};

use parqtel_alert::*;
use parqtel_alert::evaluator::engine::EvaluationEngine;
use parqtel_alert::rule::types::{AlertRule, Condition, Operator, Severity, RuleSource, AiDerivedSource, LearnedAnomalySource};
use parqtel_alert::rule::yaml::{parse_rule, parse_rules_from_str, load_rules_dir};
use parqtel_alert::state::machine::{AlertState, AlertStateMachine, TransitionEvent};
use parqtel_alert::noise::scorer::{NoiseEvent, NoiseScorer};
use parqtel_alert::store::alert_store::AlertStore;
use parqtel_alert::rule::registry::AlertRuleRegistry;
use parqtel_alert::feedback::FeedbackEvent;

fn make_rule(id: &str, op: Operator, value: f64, for_dur: u64) -> AlertRule {
    AlertRule {
        id: id.into(),
        name: id.into(),
        signal: "metrics".into(),
        query: "test{}".into(),
        condition: Condition { condition_type: "threshold".into(), operator: op, value, for_duration_secs: for_dur },
        severity: Severity::Warning,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        enabled: true,
        noise_suppression_threshold: 0.7,
        source: None,
    }
}

// === feedback.rs ===

#[test]
fn test_feedback_to_noise_event() {
    let noise = FeedbackEvent::Noise;
    let signal = FeedbackEvent::Signal;
    assert_eq!(noise.to_noise_event().weight(), 0.30);
    assert_eq!(signal.to_noise_event().weight(), -0.30);
}

// === rule/registry.rs ===

#[tokio::test]
async fn test_registry_crud() {
    let reg = AlertRuleRegistry::new();
    let rule = make_rule("r1", Operator::Gt, 1.0, 0);

    // insert + get
    reg.insert(rule.clone()).await;
    assert!(reg.get("r1").await.is_some());
    assert!(reg.get("nonexistent").await.is_none());

    // list_all / list_enabled
    assert_eq!(reg.list_all().await.len(), 1);
    assert_eq!(reg.list_enabled().await.len(), 1);

    // update existing
    let mut updated = rule.clone();
    updated.name = "updated".into();
    assert!(reg.update(updated).await);

    // update non-existing
    let fake = make_rule("fake", Operator::Gt, 1.0, 0);
    assert!(!reg.update(fake).await);

    // disable
    assert!(reg.disable("r1").await);
    assert_eq!(reg.list_enabled().await.len(), 0);
    assert!(!reg.disable("nonexistent").await);

    // remove
    assert!(reg.remove("r1").await.is_some());
    assert!(reg.remove("r1").await.is_none());
    assert_eq!(reg.list_all().await.len(), 0);
}

// === rule/types.rs ===

#[test]
fn test_condition_all_operators() {
    let cond = |op| Condition { condition_type: "threshold".into(), operator: op, value: 5.0, for_duration_secs: 0 };

    assert!(cond(Operator::Gt).evaluate(6.0));
    assert!(!cond(Operator::Gt).evaluate(5.0));
    assert!(cond(Operator::Gte).evaluate(5.0));
    assert!(cond(Operator::Lt).evaluate(4.0));
    assert!(!cond(Operator::Lt).evaluate(5.0));
    assert!(cond(Operator::Lte).evaluate(5.0));
    assert!(!cond(Operator::Lte).evaluate(6.0));
    assert!(cond(Operator::Eq).evaluate(5.0));
    assert!(!cond(Operator::Eq).evaluate(5.1));
    assert!(cond(Operator::Ne).evaluate(5.1));
    assert!(!cond(Operator::Ne).evaluate(5.0));
}

#[test]
fn test_rule_type_and_is_approved() {
    // Static (no source)
    let r = make_rule("s", Operator::Gt, 1.0, 0);
    assert_eq!(r.rule_type(), RuleType::Static);
    assert!(r.is_approved());

    // Static (explicit source)
    let mut r2 = r.clone();
    r2.source = Some(RuleSource::Static);
    assert_eq!(r2.rule_type(), RuleType::Static);
    assert!(r2.is_approved());

    // LearnedAnomaly
    let mut r3 = r.clone();
    r3.source = Some(RuleSource::LearnedAnomaly(LearnedAnomalySource {
        model: "z".into(), baseline_window_days: 7, sensitivity: 2.5, updated_at: Utc::now(),
    }));
    assert_eq!(r3.rule_type(), RuleType::LearnedAnomaly);
    assert!(r3.is_approved()); // non-AI always approved

    // AiDerived unapproved
    let mut r4 = r.clone();
    r4.source = Some(RuleSource::AiDerived(AiDerivedSource {
        derived_from_rule: "x".into(), ai_model: "m".into(), confidence: 0.8,
        proposed_at: Utc::now(), approved_by: None, approved_at: None,
    }));
    assert_eq!(r4.rule_type(), RuleType::AiDerived);
    assert!(!r4.is_approved());

    // AiDerived approved
    let mut r5 = r;
    r5.source = Some(RuleSource::AiDerived(AiDerivedSource {
        derived_from_rule: "x".into(), ai_model: "m".into(), confidence: 0.8,
        proposed_at: Utc::now(), approved_by: Some("ops".into()), approved_at: Some(Utc::now()),
    }));
    assert!(r5.is_approved());
}

// === rule/yaml.rs ===

#[test]
fn test_parse_rules_from_str_multi_doc() {
    let yaml = r#"
id: rule1
name: Rule 1
query: 'metric1{}'
condition:
  type: threshold
  operator: ">"
  value: 1.0
severity: info
---
id: rule2
name: Rule 2
query: 'metric2{}'
condition:
  type: threshold
  operator: "<"
  value: 0.5
severity: critical
"#;
    let rules = parse_rules_from_str(yaml).unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].id, "rule1");
    assert_eq!(rules[1].id, "rule2");
}

#[test]
fn test_load_rules_dir() {
    let dir = tempfile::tempdir().unwrap();
    // Write a yaml file
    std::fs::write(dir.path().join("r1.yaml"), r#"
id: from-file
name: From File
query: 'x{}'
condition:
  type: threshold
  operator: ">="
  value: 0.0
severity: warning
"#).unwrap();
    // Write a yml file
    std::fs::write(dir.path().join("r2.yml"), r#"
id: from-yml
name: From Yml
query: 'y{}'
condition:
  type: threshold
  operator: "<="
  value: 10.0
severity: info
"#).unwrap();
    // Write a non-yaml file (should be ignored)
    std::fs::write(dir.path().join("ignore.txt"), "not yaml").unwrap();

    let rules = load_rules_dir(dir.path()).unwrap();
    assert_eq!(rules.len(), 2);

    // Non-existent dir returns empty
    let rules2 = load_rules_dir(std::path::Path::new("/nonexistent/path")).unwrap();
    assert!(rules2.is_empty());
}

// === state/machine.rs - all transitions ===

#[test]
fn test_all_state_transitions() {
    // Pending -> Firing
    let (s, t) = AlertStateMachine::transition(AlertState::Pending, TransitionEvent::DurationElapsed).unwrap();
    assert_eq!(s, AlertState::Firing);
    assert_eq!(t.from, AlertState::Pending);

    // Pending -> Resolved
    let (s, _) = AlertStateMachine::transition(AlertState::Pending, TransitionEvent::ConditionCleared).unwrap();
    assert_eq!(s, AlertState::Resolved);

    // Firing -> Acknowledged
    let (s, _) = AlertStateMachine::transition(AlertState::Firing, TransitionEvent::Acknowledged { by: "x".into() }).unwrap();
    assert_eq!(s, AlertState::Acknowledged);

    // Firing -> Resolved
    let (s, _) = AlertStateMachine::transition(AlertState::Firing, TransitionEvent::ConditionCleared).unwrap();
    assert_eq!(s, AlertState::Resolved);

    // Firing -> Suppressed
    let (s, _) = AlertStateMachine::transition(AlertState::Firing, TransitionEvent::NoiseSuppressed).unwrap();
    assert_eq!(s, AlertState::Suppressed);

    // Firing -> NoiseFlagged
    let (s, _) = AlertStateMachine::transition(AlertState::Firing, TransitionEvent::MarkedNoise).unwrap();
    assert_eq!(s, AlertState::NoiseFlagged);

    // Resolved -> Pending
    let (s, _) = AlertStateMachine::transition(AlertState::Resolved, TransitionEvent::ConditionMet).unwrap();
    assert_eq!(s, AlertState::Pending);

    // Suppressed -> Firing
    let (s, _) = AlertStateMachine::transition(AlertState::Suppressed, TransitionEvent::NoiseScoreDropped).unwrap();
    assert_eq!(s, AlertState::Firing);

    // NoiseFlagged -> Suppressed
    let (s, _) = AlertStateMachine::transition(AlertState::NoiseFlagged, TransitionEvent::AutoSuppressed).unwrap();
    assert_eq!(s, AlertState::Suppressed);

    // Acknowledged -> Resolved
    let (s, _) = AlertStateMachine::transition(AlertState::Acknowledged, TransitionEvent::ConditionCleared).unwrap();
    assert_eq!(s, AlertState::Resolved);

    // Acknowledged -> Firing
    let (s, _) = AlertStateMachine::transition(AlertState::Acknowledged, TransitionEvent::AckWindowExpired).unwrap();
    assert_eq!(s, AlertState::Firing);

    // Invalid transition
    assert!(AlertStateMachine::transition(AlertState::Resolved, TransitionEvent::DurationElapsed).is_none());
}

// === noise/scorer.rs ===

#[tokio::test]
async fn test_noise_all_event_weights() {
    let scorer = NoiseScorer::new(30);
    // Test all positive events
    scorer.record_event("r", NoiseEvent::AutoResolvedFast).await;
    scorer.record_event("r", NoiseEvent::AiClassifiedLowSeverity).await;
    scorer.record_event("r", NoiseEvent::CorrelatedWithHigherSeverity).await;
    scorer.record_event("r", NoiseEvent::AcknowledgedWithoutAction).await;
    let score = scorer.get_score("r").await;
    assert!(score > 0.5);

    // Test all negative events
    let scorer2 = NoiseScorer::new(30);
    scorer2.record_event("r2", NoiseEvent::AiClassifiedHighSeverity).await;
    scorer2.record_event("r2", NoiseEvent::RemediationActionTaken).await;
    scorer2.record_event("r2", NoiseEvent::IncidentCreated).await;
    let score2 = scorer2.get_score("r2").await;
    assert!(score2 < 0.5);
}

#[tokio::test]
async fn test_noise_score_empty_rule() {
    let scorer = NoiseScorer::new(30);
    // No events recorded - should return 0.0
    assert_eq!(scorer.get_score("unknown").await, 0.0);
}

#[tokio::test]
async fn test_noise_score_window_eviction() {
    let scorer = NoiseScorer::new(2); // tiny window
    scorer.record_event("r", NoiseEvent::HumanFeedbackNoise).await;
    scorer.record_event("r", NoiseEvent::HumanFeedbackNoise).await;
    // Window full, next event evicts oldest
    scorer.record_event("r", NoiseEvent::HumanFeedbackSignal).await;
    let score = scorer.get_score("r").await;
    // Window has [0.30, -0.30], avg=0.0, score=0.5
    assert!((score - 0.5).abs() < 0.01);
}

#[tokio::test]
async fn test_noise_score_clamps() {
    let scorer = NoiseScorer::new(5);
    for _ in 0..10 {
        scorer.record_event("hi", NoiseEvent::HumanFeedbackNoise).await;
    }
    assert!(scorer.get_score("hi").await <= 1.0);

    for _ in 0..10 {
        scorer.record_event("lo", NoiseEvent::HumanFeedbackSignal).await;
    }
    assert!(scorer.get_score("lo").await >= 0.0);
}

// === store/alert_store.rs ===

#[tokio::test]
async fn test_store_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let store = AlertStore::new(Some(dir.path().to_path_buf())).await;

    let instance = AlertInstance {
        id: ulid::Ulid::new(),
        rule_id: "persist-rule".into(),
        rule_name: "Persist".into(),
        fingerprint: 12345,
        labels: BTreeMap::from([("k".into(), "v".into())]),
        annotations: BTreeMap::new(),
        state: AlertState::Firing,
        severity: Severity::Critical,
        value: Some(9.9),
        started_at: Utc::now(),
        updated_at: Utc::now(),
        resolved_at: None,
        acknowledged_by: None,
        noise_score: 0.1,
        transition_log: Vec::new(),
        correlation_bundle_id: None,
        notification_sent: false,
        source_rule_type: RuleType::Static,
    };
    store.save(&instance).await;

    // Verify get_by_id
    assert!(store.get_by_id(instance.id).await.is_some());

    // Verify persistence: create new store from same dir
    let store2 = AlertStore::new(Some(dir.path().to_path_buf())).await;
    let loaded = store2.get_by_fingerprint(12345).await;
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().rule_id, "persist-rule");
}

#[tokio::test]
async fn test_store_list_methods() {
    let store = AlertStore::in_memory();
    let now = Utc::now();

    // Save a firing instance
    let i1 = AlertInstance {
        id: ulid::Ulid::new(), rule_id: "r1".into(), rule_name: "R1".into(),
        fingerprint: 1, labels: BTreeMap::new(), annotations: BTreeMap::new(),
        state: AlertState::Firing, severity: Severity::Warning, value: Some(1.0),
        started_at: now, updated_at: now, resolved_at: None, acknowledged_by: None,
        noise_score: 0.0, transition_log: Vec::new(), correlation_bundle_id: None,
        notification_sent: false, source_rule_type: RuleType::Static,
    };
    store.save(&i1).await;

    // Save a suppressed instance
    let i2 = AlertInstance {
        id: ulid::Ulid::new(), rule_id: "r2".into(), rule_name: "R2".into(),
        fingerprint: 2, labels: BTreeMap::new(), annotations: BTreeMap::new(),
        state: AlertState::Suppressed, severity: Severity::Info, value: Some(2.0),
        started_at: now, updated_at: now, resolved_at: None, acknowledged_by: None,
        noise_score: 0.8, transition_log: Vec::new(), correlation_bundle_id: None,
        notification_sent: false, source_rule_type: RuleType::Static,
    };
    store.save(&i2).await;

    // list_active: only Firing/Ack/Pending
    assert_eq!(store.list_active().await.len(), 1);

    // list_suppressed
    assert_eq!(store.list_suppressed().await.len(), 1);

    // list_recent
    let since = now - ChronoDuration::seconds(10);
    let recent = store.list_recent(since, 10).await;
    assert_eq!(recent.len(), 2);

    // list_by_rule
    let by_rule = store.list_by_rule("r1", since).await;
    assert_eq!(by_rule.len(), 1);
    assert_eq!(by_rule[0].rule_id, "r1");

    // get_by_id for non-existent
    assert!(store.get_by_id(ulid::Ulid::new()).await.is_none());
}

#[tokio::test]
async fn test_store_new_no_dir() {
    // new(None) should work without panicking
    let store = AlertStore::new(None).await;
    assert!(store.list_active().await.is_empty());
}

// === evaluator/engine.rs ===

#[tokio::test]
async fn test_engine_evaluate_all_with_noise_suppression() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig { evaluation_interval_secs: 1, evaluation_timeout_secs: 10, ..Default::default() };
    let registry = AlertRuleRegistry::new();

    let rule = make_rule("suppress-test", Operator::Gt, 0.5, 0);
    registry.insert(rule.clone()).await;

    // Create a firing instance manually
    let fp = AlertInstance::compute_fingerprint(&rule.id, &rule.labels);
    let instance = AlertInstance {
        id: ulid::Ulid::new(), rule_id: rule.id.clone(), rule_name: rule.name.clone(),
        fingerprint: fp, labels: rule.labels.clone(), annotations: BTreeMap::new(),
        state: AlertState::Firing, severity: Severity::Warning, value: Some(1.0),
        started_at: Utc::now(), updated_at: Utc::now(), resolved_at: None,
        acknowledged_by: None, noise_score: 0.0, transition_log: Vec::new(),
        correlation_bundle_id: None, notification_sent: false, source_rule_type: RuleType::Static,
    };
    store.save(&instance).await;

    // Push noise score above threshold
    for _ in 0..5 {
        scorer.record_event(&rule.id, NoiseEvent::HumanFeedbackNoise).await;
    }

    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    // Run one evaluation cycle via the run method with immediate shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let engine_clone = engine.clone();
    let handle = tokio::spawn(async move { engine_clone.run(shutdown_rx).await });
    // Let it run one cycle
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    // Instance should now be suppressed
    let updated = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(updated.state, AlertState::Suppressed);
}

#[tokio::test]
async fn test_engine_resolved_to_pending_refiring() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("refire", Operator::Gt, 0.5, 0);

    // Fire and resolve
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await; // Pending -> Firing
    engine.evaluate_rule_with_value(&rule, 0.1, BTreeMap::new()).await; // Firing -> Resolved

    // Drain firing event
    let _ = rx.try_recv();

    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Resolved);

    // Re-fire: Resolved -> Pending
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Pending);
}

#[tokio::test]
async fn test_engine_condition_not_met_no_instance() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("no-fire", Operator::Gt, 10.0, 0);
    // Value doesn't meet condition - no instance created
    engine.evaluate_rule_with_value(&rule, 5.0, BTreeMap::new()).await;
    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());
    assert!(store.get_by_fingerprint(fp).await.is_none());
}

#[tokio::test]
async fn test_engine_acknowledged_then_resolved() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("ack-resolve", Operator::Gt, 0.5, 0);
    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());

    // Create firing instance
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;

    // Manually set to Acknowledged
    let mut inst = store.get_by_fingerprint(fp).await.unwrap();
    inst.state = AlertState::Acknowledged;
    store.save(&inst).await;

    // Condition clears -> should resolve
    engine.evaluate_rule_with_value(&rule, 0.1, BTreeMap::new()).await;
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Resolved);
}

#[tokio::test]
async fn test_engine_pending_with_duration_not_elapsed() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    // Rule with long for_duration - won't transition to Firing immediately
    let rule = make_rule("long-dur", Operator::Gt, 0.5, 9999);
    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());

    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Pending);

    // Second eval - still pending (duration not elapsed)
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Pending);
    assert_eq!(inst.value, Some(1.0));
}

#[tokio::test]
async fn test_engine_firing_stays_firing_on_condition_met() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("stay-firing", Operator::Gt, 0.5, 0);
    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());

    // Create and fire
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    assert_eq!(store.get_by_fingerprint(fp).await.unwrap().state, AlertState::Firing);

    // Condition still met - stays Firing (no transition, just updates value)
    engine.evaluate_rule_with_value(&rule, 2.0, BTreeMap::new()).await;
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Firing);
    assert_eq!(inst.value, Some(2.0));
}

#[tokio::test]
async fn test_engine_condition_not_met_on_suppressed_no_change() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("suppressed-noop", Operator::Gt, 0.5, 0);
    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());

    // Create a suppressed instance directly
    let instance = AlertInstance {
        id: ulid::Ulid::new(), rule_id: rule.id.clone(), rule_name: rule.name.clone(),
        fingerprint: fp, labels: BTreeMap::new(), annotations: BTreeMap::new(),
        state: AlertState::Suppressed, severity: Severity::Warning, value: Some(1.0),
        started_at: Utc::now(), updated_at: Utc::now(), resolved_at: None,
        acknowledged_by: None, noise_score: 0.8, transition_log: Vec::new(),
        correlation_bundle_id: None, notification_sent: false, source_rule_type: RuleType::Static,
    };
    store.save(&instance).await;

    // Condition not met on suppressed instance - no state change
    engine.evaluate_rule_with_value(&rule, 0.1, BTreeMap::new()).await;
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Suppressed);
}

// === Static rule YAML deserialization ===

#[test]
fn test_static_rule_deserializes() {
    let yaml = r#"
id: http-error-rate-high
name: HTTP Error Rate High
signal: metrics
query: 'http_requests_total{status=~"5.."}'
condition:
  type: threshold
  operator: ">"
  value: 0.05
  for_duration_secs: 300
severity: warning
labels:
  team: platform
annotations:
  summary: "Error rate is high"
enabled: true
noise_suppression_threshold: 0.7
"#;
    let rule = parse_rule(yaml).unwrap();
    assert_eq!(rule.id, "http-error-rate-high");
    assert_eq!(rule.severity, Severity::Warning);
    assert_eq!(rule.condition.for_duration_secs, 300);
    assert!(rule.condition.evaluate(0.06));
    assert!(!rule.condition.evaluate(0.04));
}

#[test]
fn test_ai_derived_rule_yaml() {
    let yaml = r#"
id: ai-rule
name: AI Rule
query: 'db_pool{}'
condition:
  type: threshold
  operator: ">"
  value: 0.9
severity: warning
source:
  type: ai_derived
  derived_from_rule: base-rule
  ai_model: claude
  confidence: 0.82
  proposed_at: "2024-01-15T10:23:00Z"
"#;
    let rule = parse_rule(yaml).unwrap();
    assert_eq!(rule.rule_type(), RuleType::AiDerived);
    assert!(!rule.is_approved());
}

// === Cover Default impl for registry ===

#[test]
fn test_registry_default() {
    let _reg: AlertRuleRegistry = Default::default();
}

// === Cover the evaluate_rule else branch (no existing instance) via evaluate_all ===

#[tokio::test]
async fn test_engine_evaluate_all_no_existing_instance() {
    // This exercises the evaluate_rule path where no instance exists
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig { evaluation_interval_secs: 1, evaluation_timeout_secs: 10, ..Default::default() };
    let registry = AlertRuleRegistry::new();

    let rule = make_rule("eval-no-instance", Operator::Gt, 0.5, 0);
    registry.insert(rule).await;

    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    // Run one cycle - evaluate_rule will hit the else branch (no existing instance)
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let engine_clone = engine.clone();
    let handle = tokio::spawn(async move { engine_clone.run(shutdown_rx).await });
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
}

// === Cover the firing event send path ===

#[tokio::test]
async fn test_engine_fires_event_on_transition_to_firing() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("fire-event", Operator::Gt, 0.5, 0);

    // Create pending then fire
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;

    // Should have received a firing event
    let event = rx.try_recv();
    assert!(event.is_ok());
    assert_eq!(event.unwrap().instance.rule_id, "fire-event");
}

// === Cover Resolved -> Pending via evaluate_rule_with_value (line 159) ===

#[tokio::test]
async fn test_engine_resolved_condition_met_transitions_to_pending() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("resolved-refire", Operator::Gt, 0.5, 0);
    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());

    // Manually create a Resolved instance
    let instance = AlertInstance {
        id: ulid::Ulid::new(), rule_id: rule.id.clone(), rule_name: rule.name.clone(),
        fingerprint: fp, labels: BTreeMap::new(), annotations: BTreeMap::new(),
        state: AlertState::Resolved, severity: Severity::Warning, value: Some(0.1),
        started_at: Utc::now(), updated_at: Utc::now(), resolved_at: Some(Utc::now()),
        acknowledged_by: None, noise_score: 0.0, transition_log: Vec::new(),
        correlation_bundle_id: None, notification_sent: false, source_rule_type: RuleType::Static,
    };
    store.save(&instance).await;

    // Condition met on resolved instance -> Pending
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;
    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Pending);
}

// === Cover timeout warning path (engine.rs:68) ===
// We can't easily make evaluate_rule slow, but we can verify the engine
// handles the timeout path by using an extremely short timeout with a rule
// that has a noisy scorer lookup (still fast, but exercises the code path).

// === Cover the AlertFiringEvent send by verifying Pending->Firing emits event ===

#[tokio::test]
async fn test_engine_pending_to_firing_emits_event() {
    let store = AlertStore::in_memory();
    let scorer = NoiseScorer::new(30);
    let (tx, mut rx) = mpsc::unbounded_channel::<AlertFiringEvent>();
    let config = AlertConfig::default();
    let registry = AlertRuleRegistry::new();
    let engine = Arc::new(EvaluationEngine::new(config, registry, store.clone(), scorer, tx));

    let rule = make_rule("emit-event", Operator::Gt, 0.5, 0);
    let fp = AlertInstance::compute_fingerprint(&rule.id, &BTreeMap::new());

    // Create a Pending instance with started_at in the past so duration check passes
    let instance = AlertInstance {
        id: ulid::Ulid::new(), rule_id: rule.id.clone(), rule_name: rule.name.clone(),
        fingerprint: fp, labels: BTreeMap::new(), annotations: BTreeMap::new(),
        state: AlertState::Pending, severity: Severity::Warning, value: Some(0.6),
        started_at: Utc::now() - ChronoDuration::seconds(100), updated_at: Utc::now(),
        resolved_at: None, acknowledged_by: None, noise_score: 0.0,
        transition_log: Vec::new(), correlation_bundle_id: None,
        notification_sent: false, source_rule_type: RuleType::Static,
    };
    store.save(&instance).await;

    // Evaluate with condition met - should transition Pending->Firing and emit event
    engine.evaluate_rule_with_value(&rule, 1.0, BTreeMap::new()).await;

    let inst = store.get_by_fingerprint(fp).await.unwrap();
    assert_eq!(inst.state, AlertState::Firing);

    let event = rx.try_recv().unwrap();
    assert_eq!(event.instance.rule_id, "emit-event");
}

// === Cover timeout path ===
// The timeout path (line 68) requires evaluate_rule to take longer than the timeout.
// Since evaluate_rule is purely in-memory async, it completes instantly.
// This path is only reachable in production when the storage layer is slow.
// We accept this as untestable without mocking infrastructure.

// ============================================================
// Anomaly Detection Engine Tests
// ============================================================

mod anomaly_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    use parqtel_alert::anomaly::baseline::{AnomalyConfig, Seasonality};
    use parqtel_alert::anomaly::zscore::ZScoreModel;
    use parqtel_alert::anomaly::detector::{AnomalyDetector, DetectorConfig};
    use parqtel_alert::anomaly::grouper::{AnomalyGrouper, GrouperConfig, GroupedAnomalyEvent};
    use parqtel_alert::anomaly::detector::AnomalyDetectedEvent;
    use parqtel_alert::anomaly::{AnomalyScore, BaselineModel};
    use parqtel_alert::rule::types::Severity;

    fn make_config(sensitivity: f64, min_points: usize) -> AnomalyConfig {
        AnomalyConfig {
            model: "z_score_seasonal".into(),
            window_days: 7,
            seasonality: Seasonality::None,
            sensitivity,
            min_training_points: min_points,
        }
    }

    /// Generate N data points from a normal distribution (mean, std_dev).
    fn generate_normal_series(n: usize, mean: f64, std_dev: f64, start_ts: i64) -> Vec<(i64, f64)> {
        // Deterministic pseudo-random using a simple LCG
        let mut seed: u64 = 42;
        (0..n).map(|i| {
            // Box-Muller approximation using LCG
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let u1 = (seed as f64) / (u64::MAX as f64);
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let u2 = (seed as f64) / (u64::MAX as f64);
            let z = (-2.0 * u1.max(1e-10).ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            let val = mean + std_dev * z;
            let ts = start_ts + (i as i64) * 60_000_000_000; // 60s intervals in ns
            (ts, val)
        }).collect()
    }

    #[test]
    fn test_zscore_normal_scores_low() {
        let config = make_config(2.5, 100);
        let mut model = ZScoreModel::new(config, None);

        // Train with 200 normal points (mean=100, std=5)
        let data = generate_normal_series(200, 100.0, 5.0, 1_000_000_000_000);
        model.update("series_a", &data);

        // Score a value within 1 sigma of mean
        let ts = 1_000_000_000_000 + 200 * 60_000_000_000;
        let score = model.score("series_a", ts, 102.0); // ~0.4 sigma from mean

        assert!(score.score < 0.3, "Expected score < 0.3, got {}", score.score);
        assert!(!score.is_anomaly);
    }

    #[test]
    fn test_zscore_extreme_scores_high() {
        let config = make_config(2.5, 100);
        let mut model = ZScoreModel::new(config, None);

        // Train with 200 normal points (mean=100, std=5)
        let data = generate_normal_series(200, 100.0, 5.0, 1_000_000_000_000);
        model.update("series_b", &data);

        // Score a value 5 sigma from mean (100 + 5*5 = 125)
        let ts = 1_000_000_000_000 + 200 * 60_000_000_000;
        let score = model.score("series_b", ts, 125.0);

        assert!(score.score > 0.9, "Expected score > 0.9, got {}", score.score);
        assert!(score.is_anomaly, "Expected is_anomaly=true");
        assert!(score.deviation_sigma.abs() > 3.0);
    }

    #[test]
    fn test_baseline_reset_on_level_shift() {
        let config = make_config(2.5, 50);
        let mut model = ZScoreModel::new(config, None);

        // Train with 200 points at mean=100, std=2
        let data = generate_normal_series(200, 100.0, 2.0, 1_000_000_000_000);
        model.update("series_shift", &data);

        // Now inject 120 points at mean=120 (shift of 10 sigma sustained for 2 hours)
        let shift_start = 1_000_000_000_000 + 200 * 60_000_000_000;
        let shifted_data: Vec<(i64, f64)> = (0..120).map(|i| {
            (shift_start + (i as i64) * 60_000_000_000, 120.0 + (i as f64) * 0.01)
        }).collect();
        model.update("series_shift", &shifted_data);

        // After the level shift, scoring a value near the new level should be normal
        let ts = shift_start + 120 * 60_000_000_000;
        let score = model.score("series_shift", ts, 120.5);

        // The baseline should have reset to the new level
        assert!(score.score < 0.5, "Expected score < 0.5 after reset, got {}", score.score);
    }

    #[test]
    fn test_counter_detection_works_on_delta() {
        let config = make_config(2.5, 50);
        let mut model = ZScoreModel::new(config, None);

        // Create a monotonically increasing counter with slight rate variance
        let mut seed: u64 = 123;
        let data: Vec<(i64, f64)> = (0..200).map(|i| {
            let ts = 1_000_000_000_000 + (i as i64) * 60_000_000_000;
            // Rate ~10 per interval with small noise
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let noise = ((seed as f64) / (u64::MAX as f64) - 0.5) * 2.0; // -1 to 1
            let val = (i as f64) * 10.0 + noise * (i as f64).max(1.0) * 0.01;
            (ts, val)
        }).collect();
        model.update("counter_series", &data);

        assert!(model.is_trained("counter_series"));

        // Score a normal rate increment (last value ≈ 1990, new value ≈ 2000, delta ≈ 10)
        let last_val = data.last().map(|(_, v)| *v).unwrap_or(0.0);
        let ts = 1_000_000_000_000 + 200 * 60_000_000_000;
        let score = model.score("counter_series", ts, last_val + 10.0);

        // Normal rate should score low
        assert!(score.score < 0.5, "Expected low score for normal rate, got {}", score.score);
    }

    #[test]
    fn test_baseline_survives_restart() {
        let dir = tempdir().unwrap();
        let config = make_config(2.5, 100);

        // Train and persist
        let data = generate_normal_series(200, 50.0, 3.0, 1_000_000_000_000);
        {
            let mut model = ZScoreModel::new(config.clone(), Some(dir.path()));
            model.update("persist_series", &data);
            let score1 = model.score("persist_series", 999_999_999_999, 50.0);
            assert!(score1.score < 0.1);
        }

        // Load from disk in a new model instance
        {
            let mut model = ZScoreModel::new(config, Some(dir.path()));
            // Trigger load by calling update with empty data
            model.update("persist_series", &[]);
            let score2 = model.score("persist_series", 999_999_999_999, 50.0);
            assert!(score2.score < 0.1, "Deserialized model should produce same scores, got {}", score2.score);
        }
    }

    #[test]
    fn test_grouper_combines_correlated() {
        let config = GrouperConfig { window: Duration::from_secs(30) };
        let mut grouper = AnomalyGrouper::new(config);

        // Emit 3 anomaly events for pods in the same namespace within the window
        for i in 0..3 {
            let mut labels = HashMap::new();
            labels.insert("k8s_namespace".to_owned(), "prod".to_owned());
            labels.insert("pod".to_owned(), format!("pod-{i}"));

            let event = AnomalyDetectedEvent {
                series_id: format!("cpu_usage_pod_{i}"),
                score: AnomalyScore {
                    score: 0.8 + (i as f64) * 0.05,
                    expected_value: 50.0,
                    actual_value: 95.0,
                    deviation_sigma: 3.5,
                    is_anomaly: true,
                    model: "z_score_seasonal".into(),
                },
                timestamp_ns: 1_000_000_000_000 + (i as i64) * 1_000_000_000,
                labels,
                detected_at: chrono::Utc::now(),
            };
            grouper.ingest(event);
        }

        // Flush all to get grouped events (simulates window expiry)
        let groups = grouper.flush_all();
        assert_eq!(groups.len(), 1, "Expected 1 grouped event, got {}", groups.len());

        let group = &groups[0];
        assert_eq!(group.correlated.len(), 2); // primary + 2 correlated = 3 total
        assert!(group.topology_key.contains("k8s_namespace=prod"));
    }

    #[test]
    fn test_grouper_does_not_combine_different_namespaces() {
        let config = GrouperConfig { window: Duration::from_secs(30) };
        let mut grouper = AnomalyGrouper::new(config);

        // Event in namespace A
        let mut labels_a = HashMap::new();
        labels_a.insert("k8s_namespace".to_owned(), "ns-a".to_owned());
        grouper.ingest(AnomalyDetectedEvent {
            series_id: "series_a".into(),
            score: AnomalyScore {
                score: 0.9, expected_value: 10.0, actual_value: 50.0,
                deviation_sigma: 4.0, is_anomaly: true, model: "z_score_seasonal".into(),
            },
            timestamp_ns: 1_000_000_000_000,
            labels: labels_a,
            detected_at: chrono::Utc::now(),
        });

        // Event in namespace B
        let mut labels_b = HashMap::new();
        labels_b.insert("k8s_namespace".to_owned(), "ns-b".to_owned());
        grouper.ingest(AnomalyDetectedEvent {
            series_id: "series_b".into(),
            score: AnomalyScore {
                score: 0.85, expected_value: 10.0, actual_value: 45.0,
                deviation_sigma: 3.5, is_anomaly: true, model: "z_score_seasonal".into(),
            },
            timestamp_ns: 1_000_000_000_001,
            labels: labels_b,
            detected_at: chrono::Utc::now(),
        });

        let groups = grouper.flush_all();
        assert_eq!(groups.len(), 2, "Expected 2 separate groups, got {}", groups.len());
    }

    #[tokio::test]
    async fn test_auto_rule_generation_creates_disabled_rule() {
        let dir = tempdir().unwrap();
        let rules_dir = dir.path().join("rules");
        std::fs::create_dir_all(&rules_dir).unwrap();

        let config = DetectorConfig {
            tick_interval_secs: 60,
            data_dir: dir.path().to_path_buf(),
            rules_dir: rules_dir.clone(),
            default_anomaly: make_config(2.5, 100),
        };

        let (tx, _rx) = mpsc::unbounded_channel();
        let detector = AnomalyDetector::new(config, tx);

        // Register and train a series
        detector.register_series("test_metric".into(), make_config(2.5, 100)).await;
        let data = generate_normal_series(150, 100.0, 5.0, 1_000_000_000_000);
        detector.ingest_data("test_metric", &data, HashMap::new()).await;

        // Run auto-generation with no covered series
        detector.auto_generate_rules(&[]).await;

        // Check that a rule file was created
        let auto_dir = rules_dir.join("auto-generated");
        assert!(auto_dir.exists(), "auto-generated directory should exist");

        let entries: Vec<_> = std::fs::read_dir(&auto_dir).unwrap().collect();
        assert_eq!(entries.len(), 1, "Expected 1 auto-generated rule file");

        let content = std::fs::read_to_string(entries[0].as_ref().unwrap().path()).unwrap();
        assert!(content.contains("enabled: false"), "Rule should be disabled");
        assert!(content.contains("learned_anomaly"), "Rule should have learned_anomaly source");

        // Check pending rules
        let pending = detector.pending_rules().await;
        assert_eq!(pending.len(), 1);
        assert!(!pending[0].activated);
    }

    #[test]
    fn test_zscore_edge_case_all_zeros() {
        let config = make_config(2.5, 10);
        let mut model = ZScoreModel::new(config, None);

        let data: Vec<(i64, f64)> = (0..100).map(|i| {
            (1_000_000_000_000 + (i as i64) * 60_000_000_000, 0.0)
        }).collect();
        model.update("zeros", &data);

        // Scoring 0 should be normal
        let score = model.score("zeros", 999_999_999_999, 0.0);
        assert_eq!(score.score, 0.0);
        assert!(!score.is_anomaly);

        // Scoring any non-zero should be anomalous (std_dev = 0)
        let score = model.score("zeros", 999_999_999_999, 1.0);
        assert_eq!(score.score, 1.0);
        assert!(score.is_anomaly);
    }

    #[test]
    fn test_zscore_edge_case_single_point() {
        let config = make_config(2.5, 100);
        let mut model = ZScoreModel::new(config, None);

        model.update("single", &[(1_000_000_000_000, 42.0)]);

        // Not trained yet, should return 0 score
        let score = model.score("single", 1_000_000_000_001, 100.0);
        assert_eq!(score.score, 0.0);
        assert!(!score.is_anomaly);
        assert!(!model.is_trained("single"));
    }

    #[test]
    fn test_zscore_edge_case_nan_inf() {
        let config = make_config(2.5, 10);
        let mut model = ZScoreModel::new(config, None);

        // Include NaN and Inf in data — they should be filtered out
        let mut data: Vec<(i64, f64)> = (0..50).map(|i| {
            (1_000_000_000_000 + (i as i64) * 60_000_000_000, 100.0)
        }).collect();
        data.push((9_000_000_000_000, f64::NAN));
        data.push((9_100_000_000_000, f64::INFINITY));
        data.push((9_200_000_000_000, f64::NEG_INFINITY));

        model.update("nan_series", &data);

        // Scoring NaN should return 0
        let score = model.score("nan_series", 9_999_999_999_999, f64::NAN);
        assert_eq!(score.score, 0.0);

        // Scoring a normal value should work
        let score = model.score("nan_series", 9_999_999_999_999, 100.0);
        assert!(!score.is_anomaly);
    }

    #[test]
    fn test_zscore_edge_case_identical_values() {
        let config = make_config(2.5, 10);
        let mut model = ZScoreModel::new(config, None);

        let data: Vec<(i64, f64)> = (0..100).map(|i| {
            (1_000_000_000_000 + (i as i64) * 60_000_000_000, 42.0)
        }).collect();
        model.update("identical", &data);

        // Same value = normal
        let score = model.score("identical", 999_999_999_999, 42.0);
        assert_eq!(score.score, 0.0);

        // Different value = anomaly (std_dev is 0)
        let score = model.score("identical", 999_999_999_999, 43.0);
        assert_eq!(score.score, 1.0);
        assert!(score.is_anomaly);
    }
}
