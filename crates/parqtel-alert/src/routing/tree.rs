use crate::error::NotificationError;
use crate::routing::matcher::MatcherSet;
use parqtel_core::config::{parse_duration, AlertRouteConfig, AlertmanagerRoutingConfig};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

const DEFAULT_GROUP_WAIT: Duration = Duration::from_secs(30);
const DEFAULT_GROUP_INTERVAL: Duration = Duration::from_secs(300);
const DEFAULT_REPEAT_INTERVAL: Duration = Duration::from_secs(14_400);

/// A concrete route resolved for an alert after inheritance and tree matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRoute {
    pub route_path_id: String,
    pub receiver: String,
    pub group_by: Vec<String>,
    pub disable_grouping: bool,
    pub group_wait: Duration,
    pub group_interval: Duration,
    pub repeat_interval: Duration,
}

/// A compiled node in the Alertmanager routing tree.
#[derive(Debug, Clone)]
pub struct CompiledRoute {
    pub route_path_id: String,
    pub matchers: MatcherSet,
    pub continue_matching: bool,
    pub resolved: Option<ResolvedRoute>,
    pub children: Vec<CompiledRoute>,
}

/// Compiled hierarchical Alertmanager routing tree.
#[derive(Debug, Clone, Default)]
pub struct CompiledRoutingTree {
    root: Option<CompiledRoute>,
}

struct InheritedDefaults<'a> {
    receiver: Option<&'a str>,
    group_by: &'a [String],
    group_wait: Duration,
    group_interval: Duration,
    repeat_interval: Duration,
}

impl CompiledRoutingTree {
    /// Compile and validate an `AlertmanagerRoutingConfig` into a routing tree.
    pub fn compile(cfg: &AlertmanagerRoutingConfig) -> Result<Self, NotificationError> {
        let mut receivers = HashSet::with_capacity(cfg.receivers.len());
        for recv in &cfg.receivers {
            if recv.name.trim().is_empty() {
                return Err(NotificationError::Config(
                    "receiver name cannot be empty".to_string(),
                ));
            }
            if !receivers.insert(recv.name.as_str()) {
                return Err(NotificationError::Config(format!(
                    "duplicate receiver name '{}'",
                    recv.name
                )));
            }
        }

        let root = match cfg.route.as_ref() {
            Some(route_cfg) => {
                let defaults = InheritedDefaults {
                    receiver: None,
                    group_by: &[],
                    group_wait: DEFAULT_GROUP_WAIT,
                    group_interval: DEFAULT_GROUP_INTERVAL,
                    repeat_interval: DEFAULT_REPEAT_INTERVAL,
                };
                Some(compile_node(route_cfg, "root", &receivers, &defaults)?)
            }
            None => None,
        };

        Ok(Self { root })
    }

    /// Return a reference to the compiled root node, if configured.
    pub fn root(&self) -> Option<&CompiledRoute> {
        self.root.as_ref()
    }

    /// Evaluate the routing tree depth-first against `labels` and return all matched routes.
    pub fn evaluate(&self, labels: &BTreeMap<String, String>) -> Vec<ResolvedRoute> {
        let mut out = Vec::new();
        if let Some(ref root) = self.root {
            evaluate_node(root, labels, &mut out);
        }
        out
    }
}

fn compile_node(
    cfg: &AlertRouteConfig,
    path_id: &str,
    receivers: &HashSet<&str>,
    parent: &InheritedDefaults<'_>,
) -> Result<CompiledRoute, NotificationError> {
    let matchers = MatcherSet::compile_all(&cfg.matchers)?;

    let effective_receiver: Option<String> = match cfg.receiver.as_deref() {
        Some(name) => {
            if !receivers.contains(name) {
                return Err(NotificationError::Config(format!(
                    "route '{path_id}' references unknown receiver '{name}'"
                )));
            }
            Some(name.to_string())
        }
        None => parent.receiver.map(str::to_string),
    };

    if cfg.routes.is_empty() && effective_receiver.is_none() {
        return Err(NotificationError::Config(format!(
            "route '{path_id}' has no receiver and no inherited parent receiver"
        )));
    }

    let group_by = cfg
        .group_by
        .clone()
        .unwrap_or_else(|| parent.group_by.to_vec());
    let disable_grouping = group_by.len() == 1 && group_by[0] == "...";

    let group_wait = match cfg.group_wait.as_deref() {
        Some(raw) => parse_duration(raw)
            .map_err(|e| NotificationError::Config(format!("route '{path_id}'.group_wait: {e}")))?,
        None => parent.group_wait,
    };
    let group_interval = match cfg.group_interval.as_deref() {
        Some(raw) => parse_duration(raw).map_err(|e| {
            NotificationError::Config(format!("route '{path_id}'.group_interval: {e}"))
        })?,
        None => parent.group_interval,
    };
    let repeat_interval = match cfg.repeat_interval.as_deref() {
        Some(raw) => parse_duration(raw).map_err(|e| {
            NotificationError::Config(format!("route '{path_id}'.repeat_interval: {e}"))
        })?,
        None => parent.repeat_interval,
    };

    let next_defaults = InheritedDefaults {
        receiver: effective_receiver.as_deref(),
        group_by: &group_by,
        group_wait,
        group_interval,
        repeat_interval,
    };

    let mut children = Vec::with_capacity(cfg.routes.len());
    for (idx, child_cfg) in cfg.routes.iter().enumerate() {
        let child_id = format!("{path_id}.{idx}");
        children.push(compile_node(
            child_cfg,
            &child_id,
            receivers,
            &next_defaults,
        )?);
    }

    let resolved = effective_receiver.map(|receiver| ResolvedRoute {
        route_path_id: path_id.to_string(),
        receiver,
        group_by,
        disable_grouping,
        group_wait,
        group_interval,
        repeat_interval,
    });

    Ok(CompiledRoute {
        route_path_id: path_id.to_string(),
        matchers,
        continue_matching: cfg.continue_matching,
        resolved,
        children,
    })
}

fn evaluate_node(
    node: &CompiledRoute,
    labels: &BTreeMap<String, String>,
    out: &mut Vec<ResolvedRoute>,
) -> bool {
    if !node.matchers.matches(labels) {
        return false;
    }

    let mut any_child_matched = false;
    for child in &node.children {
        if evaluate_node(child, labels, out) {
            any_child_matched = true;
            if !child.continue_matching {
                break;
            }
        }
    }

    if !any_child_matched {
        if let Some(ref resolved) = node.resolved {
            out.push(resolved.clone());
        }
    }

    true
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use parqtel_core::config::ReceiverConfig;

    fn make_receivers(names: &[&str]) -> Vec<ReceiverConfig> {
        names
            .iter()
            .map(|n| ReceiverConfig {
                name: (*n).to_string(),
                ..Default::default()
            })
            .collect()
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn test_empty_routing_tree() {
        let cfg = AlertmanagerRoutingConfig::default();
        let tree = CompiledRoutingTree::compile(&cfg).unwrap();
        assert!(tree.root().is_none());
        assert!(tree
            .evaluate(&labels(&[("severity", "critical")]))
            .is_empty());
    }

    #[test]
    fn test_defaults_and_inheritance() {
        let cfg = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["default", "pagerduty"]),
            route: Some(AlertRouteConfig {
                receiver: Some("default".to_string()),
                group_by: Some(vec!["alertname".to_string(), "service".to_string()]),
                // group_wait, group_interval, repeat_interval omitted -> defaults 30s, 5m, 4h
                routes: vec![AlertRouteConfig {
                    matchers: vec!["severity = critical".to_string()],
                    receiver: Some("pagerduty".to_string()),
                    group_wait: Some("10s".to_string()),
                    // inherits group_by, group_interval, repeat_interval
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let tree = CompiledRoutingTree::compile(&cfg).unwrap();
        assert!(tree.root().is_some());

        // Matches child route.
        let crit = tree.evaluate(&labels(&[("severity", "critical")]));
        assert_eq!(crit.len(), 1);
        assert_eq!(crit[0].route_path_id, "root.0");
        assert_eq!(crit[0].receiver, "pagerduty");
        assert_eq!(crit[0].group_by, vec!["alertname", "service"]);
        assert!(!crit[0].disable_grouping);
        assert_eq!(crit[0].group_wait, Duration::from_secs(10));
        assert_eq!(crit[0].group_interval, Duration::from_secs(300));
        assert_eq!(crit[0].repeat_interval, Duration::from_secs(14_400));

        // Falls back to root route when no child matches.
        let warn = tree.evaluate(&labels(&[("severity", "warning")]));
        assert_eq!(warn.len(), 1);
        assert_eq!(warn[0].route_path_id, "root");
        assert_eq!(warn[0].receiver, "default");
        assert_eq!(warn[0].group_wait, Duration::from_secs(30));
    }

    #[test]
    fn test_disable_grouping_sentinel() {
        let cfg = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["default", "ungrouped"]),
            route: Some(AlertRouteConfig {
                receiver: Some("default".to_string()),
                group_by: Some(vec!["...".to_string()]),
                routes: vec![AlertRouteConfig {
                    matchers: vec!["team = sre".to_string()],
                    // inherits group_by: ["..."] and receiver: "default"
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let tree = CompiledRoutingTree::compile(&cfg).unwrap();
        let res = tree.evaluate(&labels(&[("team", "sre")]));
        assert_eq!(res.len(), 1);
        assert!(res[0].disable_grouping);
        assert_eq!(res[0].group_by, vec!["..."]);
        assert_eq!(res[0].receiver, "default");
    }

    #[test]
    fn test_continue_matching_true_and_false() {
        let cfg = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["default", "slack", "pagerduty", "audit"]),
            route: Some(AlertRouteConfig {
                receiver: Some("default".to_string()),
                routes: vec![
                    AlertRouteConfig {
                        matchers: vec!["severity = critical".to_string()],
                        continue_matching: true,
                        receiver: Some("slack".to_string()),
                        ..Default::default()
                    },
                    AlertRouteConfig {
                        matchers: vec!["service =~ api-.*".to_string()],
                        continue_matching: false,
                        receiver: Some("pagerduty".to_string()),
                        ..Default::default()
                    },
                    AlertRouteConfig {
                        // Should NOT be reached when child 1 matches with continue=false.
                        matchers: vec!["severity = critical".to_string()],
                        receiver: Some("audit".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };

        let tree = CompiledRoutingTree::compile(&cfg).unwrap();

        // Matches child 0 (continue=true) and child 1 (continue=false) -> stops before child 2.
        let both = tree.evaluate(&labels(&[
            ("severity", "critical"),
            ("service", "api-core"),
        ]));
        assert_eq!(both.len(), 2);
        assert_eq!(both[0].route_path_id, "root.0");
        assert_eq!(both[0].receiver, "slack");
        assert_eq!(both[1].route_path_id, "root.1");
        assert_eq!(both[1].receiver, "pagerduty");

        // Matches child 0 (continue=true), skips child 1, matches child 2.
        let first_and_third =
            tree.evaluate(&labels(&[("severity", "critical"), ("service", "db-main")]));
        assert_eq!(first_and_third.len(), 2);
        assert_eq!(first_and_third[0].route_path_id, "root.0");
        assert_eq!(first_and_third[1].route_path_id, "root.2");
    }

    #[test]
    fn test_nested_route_path_ids_and_non_matching_root() {
        let cfg = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["r1", "r2"]),
            route: Some(AlertRouteConfig {
                matchers: vec!["env = prod".to_string()],
                receiver: Some("r1".to_string()),
                routes: vec![
                    AlertRouteConfig {
                        matchers: vec!["team = a".to_string()],
                        ..Default::default()
                    },
                    AlertRouteConfig {
                        matchers: vec!["team = b".to_string()],
                        routes: vec![AlertRouteConfig {
                            matchers: vec!["severity = critical".to_string()],
                            receiver: Some("r2".to_string()),
                            group_interval: Some("1m".to_string()),
                            repeat_interval: Some("1h".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };

        let tree = CompiledRoutingTree::compile(&cfg).unwrap();

        // Root doesn't match -> empty.
        assert!(tree
            .evaluate(&labels(&[("env", "dev"), ("team", "b")]))
            .is_empty());

        // Deep child matches -> root.1.0.
        let deep = tree.evaluate(&labels(&[
            ("env", "prod"),
            ("team", "b"),
            ("severity", "critical"),
        ]));
        assert_eq!(deep.len(), 1);
        assert_eq!(deep[0].route_path_id, "root.1.0");
        assert_eq!(deep[0].receiver, "r2");
        assert_eq!(deep[0].group_interval, Duration::from_secs(60));
        assert_eq!(deep[0].repeat_interval, Duration::from_secs(3600));
    }

    #[test]
    fn test_compile_validation_errors() {
        // Dangling receiver.
        let dangling = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["slack"]),
            route: Some(AlertRouteConfig {
                receiver: Some("missing".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(CompiledRoutingTree::compile(&dangling).is_err());

        // Leaf without receiver or inherited parent receiver.
        let no_recv = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["slack"]),
            route: Some(AlertRouteConfig::default()),
            ..Default::default()
        };
        assert!(CompiledRoutingTree::compile(&no_recv).is_err());

        // Empty receiver name.
        let empty_recv = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["  "]),
            ..Default::default()
        };
        assert!(CompiledRoutingTree::compile(&empty_recv).is_err());

        // Duplicate receiver name.
        let dup_recv = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["slack", "slack"]),
            ..Default::default()
        };
        assert!(CompiledRoutingTree::compile(&dup_recv).is_err());

        // Invalid durations.
        for (gw, gi, ri) in [
            (Some("bad"), None, None),
            (None, Some("bad"), None),
            (None, None, Some("bad")),
        ] {
            let bad_dur = AlertmanagerRoutingConfig {
                receivers: make_receivers(&["slack"]),
                route: Some(AlertRouteConfig {
                    receiver: Some("slack".to_string()),
                    group_wait: gw.map(str::to_string),
                    group_interval: gi.map(str::to_string),
                    repeat_interval: ri.map(str::to_string),
                    ..Default::default()
                }),
                ..Default::default()
            };
            assert!(CompiledRoutingTree::compile(&bad_dur).is_err());
        }

        // Invalid matcher on child route.
        let bad_matcher = AlertmanagerRoutingConfig {
            receivers: make_receivers(&["slack"]),
            route: Some(AlertRouteConfig {
                receiver: Some("slack".to_string()),
                routes: vec![AlertRouteConfig {
                    matchers: vec!["broken_matcher".to_string()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(CompiledRoutingTree::compile(&bad_matcher).is_err());
    }
}
