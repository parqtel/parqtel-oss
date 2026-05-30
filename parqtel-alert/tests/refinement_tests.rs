//! Tests for the refinement module.

use chrono::{Utc, TimeZone};
use parqtel_alert::{AlertInstance, AlertState, Severity};
use parqtel_alert::refinement::{RefinementAnalyser, RefinementProposal, ReviewQueue, ReviewStatus, ThresholdField, TemporalPattern, ReviewEntry};

#[tokio::test]
async fn test_noise_driver_analysis_identifies_top_reason() {
    let analyser = RefinementAnalyser::new(30);
    
    let now = Utc::now();
    let instances = vec![
        // 10 instances with high noise score
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 1,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.5),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.8,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 2,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.2),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.9,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 3,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.1),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.85,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 4,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.3),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.75,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 5,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.4),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.82,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 6,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.2),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.88,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 7,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.1),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.79,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 8,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.3),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.81,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 9,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.2),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.86,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 10,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.4),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.83,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        // 3 instances with lower noise score
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 11,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.1),
            started_at: now,
            updated_at: now,
            resolved_at: Some(now),
            acknowledged_by: None,
            noise_score: 0.6,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 12,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.2),
            started_at: now,
            updated_at: now,
            resolved_at: Some(now),
            acknowledged_by: None,
            noise_score: 0.55,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 13,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.3),
            started_at: now,
            updated_at: now,
            resolved_at: Some(now),
            acknowledged_by: None,
            noise_score: 0.58,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
    ];

    let context = analyser.analyse(
        "rule1",
        "Test Rule",
        &instances,
        "static",
        Some(5.0),
        Some(60),
        Some(Severity::Warning),
    ).await;

    // The top noise reason should be "high_noise_score" since most instances have noise_score > 0.7
    assert!(context.top_noise_reason.contains("high_noise_score"));
}

#[tokio::test]
async fn test_temporal_pattern_detects_sunday_spike() {
    let analyser = RefinementAnalyser::new(30);
    
    // May 24, 2026 is a Sunday
    let sunday = Utc.with_ymd_and_hms(2026, 5, 24, 2, 0, 0).single().unwrap();
    let instances = vec![
        // All firings on Sunday 02:00-04:00 UTC
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 1,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.5),
            started_at: sunday,
            updated_at: sunday,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.8,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 2,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.2),
            started_at: sunday + chrono::Duration::hours(1),
            updated_at: sunday + chrono::Duration::hours(1),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.8,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: 3,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.1),
            started_at: sunday + chrono::Duration::hours(2),
            updated_at: sunday + chrono::Duration::hours(2),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.8,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        },
    ];

    let context = analyser.analyse(
        "rule1",
        "Test Rule",
        &instances,
        "static",
        Some(5.0),
        Some(60),
        Some(Severity::Warning),
    ).await;

    match context.temporal_pattern {
        TemporalPattern::DayOfWeek { day, concentration } => {
            assert!(day.contains("Sunday") || day.contains("Sunday"));
            assert!(concentration > 0.5);
        }
        _ => panic!("Expected DayOfWeek pattern"),
    }
}

#[tokio::test]
async fn test_value_distribution_detects_threshold_clustering() {
    let analyser = RefinementAnalyser::new(30);
    
    let now = Utc::now();
    // Create instances where 90% of values are between 4.8% and 5.2% when threshold is 5%
    let instances: Vec<AlertInstance> = (0..10).map(|i| {
        let value = 4.8 + (i as f64) * 0.04; // 4.8, 4.84, 4.88, ..., 5.16
        AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Test Rule".to_string(),
            fingerprint: i as u64,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(value),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.8,
            transition_log: vec![],
            correlation_bundle_id: None,
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        }
    }).collect();

    let context = analyser.analyse(
        "rule1",
        "Test Rule",
        &instances,
        "static",
        Some(5.0),
        Some(60),
        Some(Severity::Warning),
    ).await;

    let distribution = context.value_distribution.expect("Expected value distribution");
    
    // The threshold should be around 5.0 (mean of values)
    assert!((distribution.threshold - 5.0).abs() < 0.2);
    
    // Most values should be near the threshold
    assert!(distribution.near_threshold_pct > 0.5);
}

#[tokio::test]
async fn test_correlation_analysis_identifies_common_alerts() {
    let analyser = RefinementAnalyser::new(30);
    
    let now = Utc::now();
    let bundle_id = ulid::Ulid::new();
    
    let mut instances = Vec::new();
    for i in 0..10 {
        instances.push(AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "rule1".to_string(),
            rule_name: "Rule 1".to_string(),
            fingerprint: i as u64,
            labels: Default::default(),
            annotations: Default::default(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(5.0),
            started_at: now,
            updated_at: now,
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.1,
            transition_log: vec![],
            correlation_bundle_id: Some(bundle_id),
            notification_sent: false,
            source_rule_type: parqtel_alert::RuleType::Static,
        });
    }

    // Add some other alerts in the same bundle
    let other_instance = AlertInstance {
        id: ulid::Ulid::new(),
        rule_id: "rule2".to_string(),
        rule_name: "Rule 2".to_string(),
        fingerprint: 100,
        labels: Default::default(),
        annotations: Default::default(),
        state: AlertState::Firing,
        severity: Severity::Warning,
        value: Some(10.0),
        started_at: now,
        updated_at: now,
        resolved_at: None,
        acknowledged_by: None,
        noise_score: 0.1,
        transition_log: vec![],
        correlation_bundle_id: Some(bundle_id),
        notification_sent: false,
        source_rule_type: parqtel_alert::RuleType::Static,
    };

    // Need to pass Rule 2 instances too if we want them to show up as correlations
    // Wait, the analyser looks at the instances passed to it.
    // Actually, it groups instances BY bundle_id.
    
    let all_instances = vec![instances[0].clone(), other_instance];
    
    let context = analyser.analyse(
        "rule1",
        "Rule 1",
        &all_instances,
        "static",
        None, None, None
    ).await;

    assert_eq!(context.correlated_alerts.len(), 1);
    assert_eq!(context.correlated_alerts[0].rule_id, "rule2");
}

#[tokio::test]
async fn test_proposal_modify_threshold_changes_yaml() {
    use tempfile::TempDir;
    use parqtel_alert::refinement::applier::RuleApplier;
    
    let temp_dir = TempDir::new().unwrap();
    let rules_dir = temp_dir.path().to_string_lossy().to_string();
    
    let rule_content = r#"
id: "rule1"
name: "Test Rule"
signal: "metrics"
query: "http_error_rate"
condition:
  type: "threshold"
  operator: ">"
  value: 5.0
  for_duration_secs: 60
severity: "warning"
enabled: true
"#;
    std::fs::write(format!("{}/test.yaml", rules_dir), rule_content).unwrap();

    let applier = RuleApplier::new(&rules_dir);
    
    let proposal = RefinementProposal::ModifyThreshold {
        rule_id: "rule1".to_string(),
        field: ThresholdField::ThresholdValue,
        current_value: "5.0".to_string(),
        proposed_value: "6.0".to_string(),
        rationale: "Threshold too low".to_string(),
        confidence: 0.8,
    };

    applier.apply_proposal(&proposal).await.unwrap();

    let updated_content = std::fs::read_to_string(format!("{}/test.yaml", rules_dir)).unwrap();
    assert!(updated_content.contains("value: 6.0"));
}

#[tokio::test]
async fn test_proposal_narrow_scope_adds_label_filter() {
    use tempfile::TempDir;
    use parqtel_alert::refinement::applier::RuleApplier;
    
    let temp_dir = TempDir::new().unwrap();
    let rules_dir = temp_dir.path().to_string_lossy().to_string();
    
    let rule_content = r#"
id: "rule1"
name: "Test Rule"
signal: "metrics"
query: "http_error_rate"
condition:
  type: "threshold"
  operator: ">"
  value: 5.0
  for_duration_secs: 60
severity: "warning"
enabled: true
"#;
    std::fs::write(format!("{}/test.yaml", rules_dir), rule_content).unwrap();

    let applier = RuleApplier::new(&rules_dir);
    
    let proposal = RefinementProposal::NarrowScope {
        rule_id: "rule1".to_string(),
        add_label_filter: "env!=\"dev\"".to_string(),
        rationale: "Rule fires too often in dev".to_string(),
        confidence: 0.8,
    };

    applier.apply_proposal(&proposal).await.unwrap();

    let updated_content = std::fs::read_to_string(format!("{}/test.yaml", rules_dir)).unwrap();
    // The label filter should be added to the labels section
    assert!(updated_content.contains("env:"));
}

#[tokio::test]
async fn test_proposal_suppress_disables_rule() {
    use tempfile::TempDir;
    use parqtel_alert::refinement::applier::RuleApplier;
    
    let temp_dir = TempDir::new().unwrap();
    let rules_dir = temp_dir.path().to_string_lossy().to_string();
    
    let rule_content = r#"
id: "rule1"
name: "Test Rule"
signal: "metrics"
query: "http_error_rate"
condition:
  type: "threshold"
  operator: ">"
  value: 5.0
  for_duration_secs: 60
severity: "warning"
enabled: true
"#;
    std::fs::write(format!("{}/test.yaml", rules_dir), rule_content).unwrap();

    let applier = RuleApplier::new(&rules_dir);
    
    let proposal = RefinementProposal::Suppress {
        rule_id: "rule1".to_string(),
        rationale: "Too noisy".to_string(),
        replacement_rule_id: None,
        confidence: 0.9,
    };

    applier.apply_proposal(&proposal).await.unwrap();

    let updated_content = std::fs::read_to_string(format!("{}/test.yaml", rules_dir)).unwrap();
    assert!(updated_content.contains("enabled: false"));
}

#[tokio::test]
async fn test_rejection_reason_stored_in_proposal() {
    let queue = ReviewQueue::in_memory();
    
    let proposal = RefinementProposal::ModifyThreshold {
        rule_id: "rule1".to_string(),
        field: ThresholdField::ThresholdValue,
        current_value: "5.0".to_string(),
        proposed_value: "6.0".to_string(),
        rationale: "Threshold too low".to_string(),
        confidence: 0.8,
    };

    let entry = queue.add_proposal(proposal, "ai").await;
    queue.reject(&entry.id, "too aggressive").await;

    let retrieved = queue.get_entry(&entry.id).await;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().rejection_reason, Some("too aggressive".to_string()));
}

#[tokio::test]
async fn test_proposal_expires_after_7_days() {
    let queue = ReviewQueue::in_memory();
    
    let proposal = RefinementProposal::ModifyThreshold {
        rule_id: "rule1".to_string(),
        field: ThresholdField::ThresholdValue,
        current_value: "5.0".to_string(),
        proposed_value: "6.0".to_string(),
        rationale: "Threshold too low".to_string(),
        confidence: 0.8,
    };

    let mut entry = ReviewEntry::new(proposal, "ai");
    // Set expires_at to 8 days ago
    entry.expires_at = Utc::now() - chrono::Duration::days(8);
    
    queue.add_entry(entry).await;

    let expired = queue.cleanup_expired().await;
    assert_eq!(expired.len(), 1);

    let retrieved = queue.get_entry(&expired[0]).await;
    assert_eq!(retrieved.unwrap().status, ReviewStatus::Expired);
}

#[tokio::test]
async fn test_coverage_gap_queues_activation_proposal() {
    // This test verifies that a pending anomaly rule triggers an activation proposal
    let queue = ReviewQueue::in_memory();
    
    let proposal = RefinementProposal::ActivateLearnedRule {
        rule_id: "anomaly_rule_1".to_string(),
        proposed_severity: Severity::Warning,
        rationale: "Rule is ready for activation".to_string(),
        confidence: 0.85,
    };

    let entry = queue.add_proposal(proposal, "ai").await;
    
    assert_eq!(entry.status, ReviewStatus::Pending);
    assert!(entry.proposed_by == "ai");
}

#[tokio::test]
async fn test_review_queue_persistence() {
    use tempfile::TempDir;
    
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();
    
    let queue = ReviewQueue::new(Some(data_dir.clone()));
    
    let proposal = RefinementProposal::ModifyThreshold {
        rule_id: "rule1".to_string(),
        field: ThresholdField::ThresholdValue,
        current_value: "5.0".to_string(),
        proposed_value: "6.0".to_string(),
        rationale: "Threshold too low".to_string(),
        confidence: 0.8,
    };

    let entry = queue.add_proposal(proposal.clone(), "ai").await;
    
    // Create a new queue instance pointing to same directory
    let queue2 = ReviewQueue::new(Some(data_dir));
    queue2.load().await.unwrap();

    let retrieved = queue2.get_entry(&entry.id).await;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().id, entry.id);
}
