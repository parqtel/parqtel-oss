use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Severity levels for alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// The type/source of a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleType {
    Static,
    AiDerived,
    LearnedAnomaly,
}

/// Comparison operator for threshold conditions.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Operator {
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Lte,
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
}

/// Condition that triggers an alert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub operator: Operator,
    pub value: f64,
    #[serde(default)]
    pub for_duration_secs: u64,
}

impl Condition {
    /// Evaluate whether a metric value meets this condition.
    pub fn evaluate(&self, metric_value: f64) -> bool {
        match self.operator {
            Operator::Gt => metric_value > self.value,
            Operator::Gte => metric_value >= self.value,
            Operator::Lt => metric_value < self.value,
            Operator::Lte => metric_value <= self.value,
            Operator::Eq => (metric_value - self.value).abs() < f64::EPSILON,
            Operator::Ne => (metric_value - self.value).abs() >= f64::EPSILON,
        }
    }
}

/// Source metadata for AI-derived rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiDerivedSource {
    pub derived_from_rule: String,
    pub ai_model: String,
    pub confidence: f64,
    pub proposed_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
}

/// Source metadata for learned anomaly rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearnedAnomalySource {
    pub model: String,
    pub baseline_window_days: u64,
    pub sensitivity: f64,
    pub updated_at: DateTime<Utc>,
}

/// Rule source discriminator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleSource {
    Static,
    AiDerived(AiDerivedSource),
    LearnedAnomaly(LearnedAnomalySource),
}

/// An alert rule definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    #[serde(default = "default_signal")]
    pub signal: String,
    pub query: String,
    pub condition: Condition,
    pub severity: Severity,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_noise_threshold")]
    pub noise_suppression_threshold: f32,
    #[serde(default)]
    pub source: Option<RuleSource>,
}

impl AlertRule {
    pub fn rule_type(&self) -> RuleType {
        match &self.source {
            None | Some(RuleSource::Static) => RuleType::Static,
            Some(RuleSource::AiDerived(_)) => RuleType::AiDerived,
            Some(RuleSource::LearnedAnomaly(_)) => RuleType::LearnedAnomaly,
        }
    }

    /// Whether this AI-derived rule has been approved.
    pub fn is_approved(&self) -> bool {
        match &self.source {
            Some(RuleSource::AiDerived(s)) => s.approved_by.is_some(),
            _ => true,
        }
    }
}

fn default_signal() -> String {
    "metrics".into()
}

fn default_true() -> bool {
    true
}

fn default_noise_threshold() -> f32 {
    0.7
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_condition_evaluate_gt() {
        let c = Condition { condition_type: "threshold".into(), operator: Operator::Gt, value: 90.0, for_duration_secs: 0 };
        assert!(c.evaluate(91.0));
        assert!(!c.evaluate(90.0));
        assert!(!c.evaluate(89.0));
    }

    #[test]
    fn test_condition_evaluate_gte() {
        let c = Condition { condition_type: "threshold".into(), operator: Operator::Gte, value: 90.0, for_duration_secs: 0 };
        assert!(c.evaluate(90.0));
        assert!(!c.evaluate(89.9));
    }

    #[test]
    fn test_condition_evaluate_lt() {
        let c = Condition { condition_type: "threshold".into(), operator: Operator::Lt, value: 10.0, for_duration_secs: 0 };
        assert!(c.evaluate(9.0));
        assert!(!c.evaluate(10.0));
    }

    #[test]
    fn test_condition_evaluate_eq() {
        let c = Condition { condition_type: "threshold".into(), operator: Operator::Eq, value: 42.0, for_duration_secs: 0 };
        assert!(c.evaluate(42.0));
        assert!(!c.evaluate(42.1));
    }

    #[test]
    fn test_condition_evaluate_ne() {
        let c = Condition { condition_type: "threshold".into(), operator: Operator::Ne, value: 42.0, for_duration_secs: 0 };
        assert!(c.evaluate(43.0));
        assert!(!c.evaluate(42.0));
    }

    #[test]
    fn test_alert_rule_type_static() {
        let rule = AlertRule {
            id: "r1".into(), name: "test".into(), signal: "metrics".into(), query: "cpu".into(),
            condition: Condition { condition_type: "threshold".into(), operator: Operator::Gt, value: 90.0, for_duration_secs: 0 },
            severity: Severity::Warning, labels: BTreeMap::new(), annotations: BTreeMap::new(),
            enabled: true, noise_suppression_threshold: 0.7, source: None,
        };
        assert_eq!(rule.rule_type(), RuleType::Static);
        assert!(rule.is_approved());
    }

    #[test]
    fn test_alert_rule_type_ai_derived_unapproved() {
        let rule = AlertRule {
            id: "r2".into(), name: "ai".into(), signal: "metrics".into(), query: "mem".into(),
            condition: Condition { condition_type: "threshold".into(), operator: Operator::Gt, value: 80.0, for_duration_secs: 0 },
            severity: Severity::Critical, labels: BTreeMap::new(), annotations: BTreeMap::new(),
            enabled: true, noise_suppression_threshold: 0.7,
            source: Some(RuleSource::AiDerived(AiDerivedSource {
                derived_from_rule: "r1".into(), ai_model: "gpt-4".into(), confidence: 0.9,
                proposed_at: Utc::now(), approved_by: None, approved_at: None,
            })),
        };
        assert_eq!(rule.rule_type(), RuleType::AiDerived);
        assert!(!rule.is_approved());
    }

    #[test]
    fn test_severity_serialization() {
        let s = Severity::Critical;
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, "\"critical\"");
        let decoded: Severity = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, Severity::Critical);
    }

    #[test]
    fn test_operator_serialization() {
        let op = Operator::Gte;
        let json = serde_json::to_string(&op).unwrap();
        assert_eq!(json, "\">=\"");
    }
}
