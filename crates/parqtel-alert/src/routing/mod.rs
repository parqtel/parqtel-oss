pub mod inhibit;
pub mod matcher;
pub mod tree;

pub use inhibit::{CompiledInhibitRule, InhibitionEvaluator};
pub use matcher::{CompiledMatcher, MatcherSet};
pub use tree::{CompiledRoute, CompiledRoutingTree, ResolvedRoute};

use crate::error::NotificationError;
use crate::AlertInstance;
use parqtel_core::config::{AlertmanagerRoutingConfig, SilenceConfig};
use std::collections::BTreeMap;

/// Result of evaluating silences, inhibition rules, and the routing tree for an alert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEvaluationResult {
    pub silenced: bool,
    pub inhibited: bool,
    pub matched_routes: Vec<ResolvedRoute>,
}

/// Evaluates silences, inhibition rules, and the routing tree together.
#[derive(Debug, Clone, Default)]
pub struct RoutingEvaluator {
    tree: CompiledRoutingTree,
    inhibition: InhibitionEvaluator,
}

impl RoutingEvaluator {
    /// Compile a `RoutingEvaluator` from an `AlertmanagerRoutingConfig`.
    pub fn compile(cfg: &AlertmanagerRoutingConfig) -> Result<Self, NotificationError> {
        let tree = CompiledRoutingTree::compile(cfg)?;
        let inhibition = InhibitionEvaluator::compile(&cfg.inhibit_rules)?;
        Ok(Self { tree, inhibition })
    }

    /// Construct a `RoutingEvaluator` from pre-compiled components.
    pub fn new(tree: CompiledRoutingTree, inhibition: InhibitionEvaluator) -> Self {
        Self { tree, inhibition }
    }

    /// Access the compiled routing tree.
    pub fn tree(&self) -> &CompiledRoutingTree {
        &self.tree
    }

    /// Access the compiled inhibition evaluator.
    pub fn inhibition(&self) -> &InhibitionEvaluator {
        &self.inhibition
    }

    /// Evaluate silences first (`starts_at <= now_unix < ends_at`), then inhibition rules,
    /// and finally the routing tree.
    pub fn evaluate(
        &self,
        labels: &BTreeMap<String, String>,
        fingerprint: u64,
        silences: &[SilenceConfig],
        firing_alerts: &[AlertInstance],
        now_unix: i64,
    ) -> RouteEvaluationResult {
        let silenced = silences.iter().any(|s| {
            s.starts_at <= now_unix
                && now_unix < s.ends_at
                && s.match_labels.iter().all(|(k, v)| labels.get(k) == Some(v))
        });

        if silenced {
            return RouteEvaluationResult {
                silenced: true,
                inhibited: false,
                matched_routes: Vec::new(),
            };
        }

        let inhibited = self
            .inhibition
            .is_inhibited(labels, fingerprint, firing_alerts);
        if inhibited {
            return RouteEvaluationResult {
                silenced: false,
                inhibited: true,
                matched_routes: Vec::new(),
            };
        }

        RouteEvaluationResult {
            silenced: false,
            inhibited: false,
            matched_routes: self.tree.evaluate(labels),
        }
    }

    /// Evaluate only the routing tree, ignoring silences and inhibition rules.
    /// Useful for `test_route` inspection.
    pub fn match_routes_only(&self, labels: &BTreeMap<String, String>) -> Vec<ResolvedRoute> {
        self.tree.evaluate(labels)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::{AlertState, RuleType, Severity};
    use chrono::Utc;
    use parqtel_core::config::{AlertRouteConfig, InhibitRuleConfig, ReceiverConfig};
    use ulid::Ulid;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn make_cfg() -> AlertmanagerRoutingConfig {
        AlertmanagerRoutingConfig {
            receivers: vec![
                ReceiverConfig {
                    name: "default".to_string(),
                    ..Default::default()
                },
                ReceiverConfig {
                    name: "pagerduty".to_string(),
                    ..Default::default()
                },
            ],
            route: Some(AlertRouteConfig {
                receiver: Some("default".to_string()),
                routes: vec![AlertRouteConfig {
                    matchers: vec!["severity = critical".to_string()],
                    receiver: Some("pagerduty".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            inhibit_rules: vec![InhibitRuleConfig {
                source_matchers: vec!["severity = critical".to_string()],
                target_matchers: vec!["severity = warning".to_string()],
                equal: vec!["service".to_string()],
            }],
            ..Default::default()
        }
    }

    fn make_firing_alert(fp: u64, pairs: &[(&str, &str)]) -> AlertInstance {
        AlertInstance {
            id: Ulid::new(),
            rule_id: "r1".into(),
            rule_name: "R1".into(),
            fingerprint: fp,
            labels: labels(pairs),
            annotations: BTreeMap::new(),
            state: AlertState::Firing,
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

            evidence: None,

            rca: None,
        }
    }

    #[test]
    fn test_evaluate_silence_precedence_and_time_bounds() {
        let eval = RoutingEvaluator::compile(&make_cfg()).unwrap();
        let silence = SilenceConfig {
            name: "maint".into(),
            created_by: "oncall".into(),
            match_labels: labels(&[("service", "api")]),
            starts_at: 100,
            ends_at: 200,
        };
        let alert_labels = labels(&[("severity", "warning"), ("service", "api")]);
        let firing = vec![make_firing_alert(
            1,
            &[("severity", "critical"), ("service", "api")],
        )];

        // At starts_at (inclusive) -> silenced (short-circuits before inhibition).
        let res = eval.evaluate(
            &alert_labels,
            2,
            std::slice::from_ref(&silence),
            &firing,
            100,
        );
        assert!(res.silenced);
        assert!(!res.inhibited);
        assert!(res.matched_routes.is_empty());

        // At ends_at (exclusive) -> silence expired, so inhibition kicks in.
        let res_end = eval.evaluate(
            &alert_labels,
            2,
            std::slice::from_ref(&silence),
            &firing,
            200,
        );
        assert!(!res_end.silenced);
        assert!(res_end.inhibited);
        assert!(res_end.matched_routes.is_empty());

        // Before starts_at -> not silenced, inhibited.
        let res_before = eval.evaluate(&alert_labels, 2, &[silence], &firing, 99);
        assert!(!res_before.silenced);
        assert!(res_before.inhibited);
    }

    #[test]
    fn test_evaluate_routes_when_not_silenced_or_inhibited() {
        let eval = RoutingEvaluator::compile(&make_cfg()).unwrap();
        let alert_labels = labels(&[("severity", "critical"), ("service", "api")]);

        let res = eval.evaluate(&alert_labels, 1, &[], &[], 150);
        assert!(!res.silenced);
        assert!(!res.inhibited);
        assert_eq!(res.matched_routes.len(), 1);
        assert_eq!(res.matched_routes[0].receiver, "pagerduty");
        assert_eq!(res.matched_routes[0].route_path_id, "root.0");
    }

    #[test]
    fn test_compile_propagates_tree_and_inhibition_errors() {
        // Root receiver that does not exist -> routing tree compile error.
        let mut cfg = make_cfg();
        cfg.route.as_mut().unwrap().receiver = Some("missing".to_string());
        assert!(matches!(
            RoutingEvaluator::compile(&cfg),
            Err(NotificationError::Config(_))
        ));

        // Invalid inhibit matcher -> inhibition compile error.
        let mut cfg = make_cfg();
        cfg.inhibit_rules[0].source_matchers = vec!["not a matcher".to_string()];
        assert!(matches!(
            RoutingEvaluator::compile(&cfg),
            Err(NotificationError::Config(_))
        ));

        // Invalid inhibit regex -> inhibition compile error from the regex branch.
        let mut cfg = make_cfg();
        cfg.inhibit_rules[0].source_matchers = vec!["severity =~ [unclosed".to_string()];
        assert!(matches!(
            RoutingEvaluator::compile(&cfg),
            Err(NotificationError::Config(_))
        ));

        // `new` stores pre-compiled components as given.
        let tree = CompiledRoutingTree::compile(&make_cfg()).unwrap();
        let inhibition = InhibitionEvaluator::compile(&[]).unwrap();
        let eval = RoutingEvaluator::new(tree, inhibition);
        assert_eq!(
            eval.tree()
                .evaluate(&labels(&[("severity", "critical")]))
                .len(),
            1
        );
        assert!(!eval
            .inhibition()
            .is_inhibited(&labels(&[("severity", "warning")]), 1, &[]));
        assert!(RoutingEvaluator::default()
            .match_routes_only(&labels(&[("severity", "critical")]))
            .is_empty());
    }

    #[test]
    fn test_silence_requires_all_match_labels() {
        let eval = RoutingEvaluator::compile(&make_cfg()).unwrap();
        let silence = SilenceConfig {
            name: "svc".into(),
            created_by: "ops".into(),
            match_labels: labels(&[("service", "api")]),
            starts_at: 0,
            ends_at: 1000,
        };
        // Label map lacks the silence's label -> not silenced, routes evaluated.
        let res = eval.evaluate(
            &labels(&[("severity", "warning")]),
            2,
            std::slice::from_ref(&silence),
            &[],
            500,
        );
        assert!(!res.silenced);
        assert_eq!(res.matched_routes[0].receiver, "default");

        // Exact label value matches -> silenced.
        let res = eval.evaluate(
            &labels(&[("service", "api"), ("severity", "warning")]),
            2,
            &[silence],
            &[],
            500,
        );
        assert!(res.silenced);
    }

    #[test]
    fn test_match_routes_only_ignores_silences_and_inhibition() {
        let eval = RoutingEvaluator::compile(&make_cfg()).unwrap();
        let alert_labels = labels(&[("severity", "warning"), ("service", "api")]);
        let routes = eval.match_routes_only(&alert_labels);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].receiver, "default");
        assert_eq!(routes[0].route_path_id, "root");
    }
}
