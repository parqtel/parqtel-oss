//! Built-in alert preset packs, embedded in the binary.
//!
//! Packs stay authored as `rules/presets/*.yaml` — the same files validated
//! by the `preset_rules` static test — and are embedded here with
//! `include_str!`, so a single binary ships working alerts without volume
//! mounts (same pattern as the embedded UI). Activation is driven by
//! `[alerts.presets]`: `mode = off | auto | all`, filtered by an
//! include-only or exclude list of pack keys (plan:
//! `docs/BUILTIN_ALERT_PRESETS_PLAN.md`).

use crate::rule::registry::AlertRuleRegistry;
use crate::rule::types::AlertRule;
use crate::rule::yaml::parse_rules_from_str;
use parqtel_core::config::{PresetConfig, PresetMode};
use std::collections::HashSet;

/// One built-in alert pack embedded in the binary.
pub struct BuiltinPack {
    /// Pack key used by include/exclude configuration (kebab-case).
    pub name: &'static str,
    /// One-line description (logs, status surfaces).
    pub description: &'static str,
    /// Canary metric names; ANY match marks the pack as detected.
    pub canaries: &'static [&'static str],
    /// Multi-document YAML rule content embedded from `rules/presets/`.
    pub rules: &'static str,
}

/// Every pack shipped in the binary. Pack keys are stable configuration
/// identifiers; rule files may be renamed freely without touching config.
pub const BUILTIN_PACKS: &[BuiltinPack] = &[
    BuiltinPack {
        name: "kubernetes",
        description: "Kubernetes node, pod and workload health",
        canaries: &["kube_node_status_condition", "k8s.node.cpu.utilization"],
        rules: include_str!("../../rules/presets/kubernetes-cluster.yaml"),
    },
    BuiltinPack {
        name: "coredns",
        description: "CoreDNS cluster DNS availability",
        canaries: &["coredns_dns_requests_total", "coredns_dns_responses_total"],
        rules: include_str!("../../rules/presets/coredns.yaml"),
    },
    BuiltinPack {
        name: "external-secrets",
        description: "External Secrets Operator sync health",
        canaries: &["externalsecret_status_condition", "externalsecret_created"],
        rules: include_str!("../../rules/presets/external-secrets.yaml"),
    },
    BuiltinPack {
        name: "service-red",
        description: "RED metrics for any traced service (span-metrics bridge)",
        canaries: &["traces_service_requests_total"],
        rules: include_str!("../../rules/presets/service-red.yaml"),
    },
    BuiltinPack {
        name: "parqtel",
        description: "Parqtel self-monitoring (ingest, query, storage, buffer)",
        canaries: &["parqtel_ingested_points_total", "parqtel_ingest_gap_secs"],
        rules: include_str!("../../rules/presets/parqtel-self.yaml"),
    },
];

/// Packs selected by the include/exclude configuration.
///
/// Include-only wins over exclude when both are set (warning logged); unknown
/// pack keys are logged and ignored so layered config stays lenient.
pub fn select_packs(cfg: &PresetConfig) -> Vec<&'static BuiltinPack> {
    for key in cfg.include.iter().chain(&cfg.exclude) {
        if !BUILTIN_PACKS.iter().any(|p| p.name == key) {
            tracing::warn!(key, "unknown built-in alert pack key in config");
        }
    }
    if !cfg.include.is_empty() {
        if !cfg.exclude.is_empty() {
            tracing::warn!(
                include = ?cfg.include,
                exclude = ?cfg.exclude,
                "[alerts.presets] include and exclude both set; include wins"
            );
        }
        BUILTIN_PACKS
            .iter()
            .filter(|p| cfg.include.iter().any(|k| k == p.name))
            .collect()
    } else {
        BUILTIN_PACKS
            .iter()
            .filter(|p| !cfg.exclude.iter().any(|k| k == p.name))
            .collect()
    }
}

/// Parse a pack's embedded YAML, tagging every rule with `labels.pack`.
fn pack_rules(pack: &BuiltinPack) -> Result<Vec<AlertRule>, serde_yaml::Error> {
    let mut rules = parse_rules_from_str(pack.rules)?;
    for rule in &mut rules {
        rule.labels
            .insert("pack".to_string(), pack.name.to_string());
    }
    Ok(rules)
}

/// Insert a pack's rules, skipping ids that already exist (user rules win).
/// Returns the number of rules inserted.
pub async fn activate_pack(pack: &BuiltinPack, registry: &AlertRuleRegistry) -> usize {
    let rules = match pack_rules(pack) {
        Ok(rules) => rules,
        Err(e) => {
            tracing::warn!(pack = pack.name, error = %e, "built-in alert pack failed to parse");
            return 0;
        }
    };
    let mut inserted = 0;
    for rule in rules {
        if registry.insert_if_absent(rule).await {
            inserted += 1;
        }
    }
    inserted
}

/// First activation pass over the selected packs.
///
/// - `Off` → nothing; returns an empty pending list.
/// - `All` → activates every selected pack; returns an empty pending list.
/// - `Auto` → activates packs with at least one canary in `metric_names`;
///   returns the rest so the caller can retry them periodically.
///
/// Activation runs once per pack per process: a later API disable/delete is
/// never reverted by a subsequent pass.
pub async fn activate_initial(
    cfg: &PresetConfig,
    metric_names: &HashSet<String>,
    registry: &AlertRuleRegistry,
) -> Vec<&'static BuiltinPack> {
    if matches!(cfg.mode, PresetMode::Off) {
        return Vec::new();
    }
    let mut pending = Vec::new();
    for pack in select_packs(cfg) {
        let detected = matches!(cfg.mode, PresetMode::All)
            || pack.canaries.iter().any(|c| metric_names.contains(*c));
        if detected {
            let rules = activate_pack(pack, registry).await;
            tracing::info!(pack = pack.name, rules, mode = ?cfg.mode, "built-in alert pack activated");
        } else {
            pending.push(pack);
        }
    }
    pending
}

/// One retry pass for packs still awaiting their canary metrics; returns the
/// ones that are still missing (call until the list is empty).
pub async fn activate_detected(
    packs: Vec<&'static BuiltinPack>,
    metric_names: &HashSet<String>,
    registry: &AlertRuleRegistry,
) -> Vec<&'static BuiltinPack> {
    let mut still_pending = Vec::new();
    for pack in packs {
        if pack.canaries.iter().any(|c| metric_names.contains(*c)) {
            let rules = activate_pack(pack, registry).await;
            tracing::info!(
                pack = pack.name,
                rules,
                mode = "auto",
                "built-in alert pack activated"
            );
        } else {
            still_pending.push(pack);
        }
    }
    still_pending
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn cfg(mode: PresetMode, include: &[&str], exclude: &[&str]) -> PresetConfig {
        PresetConfig {
            mode,
            include: include.iter().map(|s| s.to_string()).collect(),
            exclude: exclude.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn metrics(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// Total rule count across every embedded pack (parsed, not hardcoded).
    fn total_rules() -> usize {
        BUILTIN_PACKS
            .iter()
            .map(|p| parse_rules_from_str(p.rules).unwrap().len())
            .sum()
    }

    #[test]
    fn manifest_is_unique_and_every_pack_parses() {
        let mut names = HashSet::new();
        let mut ids = HashSet::new();
        for pack in BUILTIN_PACKS {
            assert!(names.insert(pack.name), "duplicate pack key {}", pack.name);
            assert!(!pack.description.is_empty());
            assert!(!pack.canaries.is_empty(), "{} has no canaries", pack.name);
            let rules = parse_rules_from_str(pack.rules)
                .unwrap_or_else(|e| panic!("pack {} failed to parse: {e}", pack.name));
            assert!(!rules.is_empty(), "{} contains no rules", pack.name);
            for rule in rules {
                assert!(
                    ids.insert(rule.id.clone()),
                    "duplicate rule id across packs: {}",
                    rule.id
                );
            }
        }
    }

    #[test]
    fn include_wins_over_exclude() {
        let c = cfg(PresetMode::All, &["coredns"], &["coredns", "kubernetes"]);
        let selected: Vec<_> = select_packs(&c).iter().map(|p| p.name).collect();
        assert_eq!(selected, vec!["coredns"]);
    }

    #[test]
    fn exclude_filters_when_include_empty() {
        let c = cfg(PresetMode::Auto, &[], &["service-red"]);
        let selected: Vec<_> = select_packs(&c).iter().map(|p| p.name).collect();
        assert!(selected.contains(&"coredns"));
        assert!(!selected.contains(&"service-red"));
        assert_eq!(selected.len(), BUILTIN_PACKS.len() - 1);
    }

    #[test]
    fn unknown_keys_select_nothing_for_that_key() {
        let c = cfg(PresetMode::All, &["no-such-pack"], &[]);
        assert!(select_packs(&c).is_empty());
    }

    #[tokio::test]
    async fn off_mode_activates_nothing() {
        let registry = AlertRuleRegistry::new();
        let pending =
            activate_initial(&cfg(PresetMode::Off, &[], &[]), &metrics(&[]), &registry).await;
        assert!(pending.is_empty());
        assert!(registry.list_all().await.is_empty());
    }

    #[tokio::test]
    async fn all_mode_activates_everything_with_pack_labels() {
        let registry = AlertRuleRegistry::new();
        let pending =
            activate_initial(&cfg(PresetMode::All, &[], &[]), &metrics(&[]), &registry).await;
        assert!(pending.is_empty());
        assert_eq!(registry.list_all().await.len(), total_rules());
        let rule = registry.get("coredns-servfail-responses").await.unwrap();
        assert_eq!(rule.labels.get("pack").map(String::as_str), Some("coredns"));
    }

    #[tokio::test]
    async fn auto_mode_activates_only_detected_packs() {
        let registry = AlertRuleRegistry::new();
        let seen = metrics(&["coredns_dns_requests_total"]);
        let pending = activate_initial(&cfg(PresetMode::Auto, &[], &[]), &seen, &registry).await;
        // coredns activated; every other pack still waits for its canaries.
        assert_eq!(pending.len(), BUILTIN_PACKS.len() - 1);
        let activated = registry.list_all().await;
        assert_eq!(
            activated.len(),
            parse_rules_from_str(BUILTIN_PACKS[1].rules).unwrap().len()
        );
        assert!(activated
            .iter()
            .all(|r| r.labels.get("pack").map(String::as_str) == Some("coredns")));

        // A later pass activates the rest once their metrics arrive.
        let all_seen = metrics(&[
            "kube_node_status_condition",
            "externalsecret_status_condition",
            "traces_service_requests_total",
            "parqtel_ingested_points_total",
        ]);
        let still = activate_detected(pending, &all_seen, &registry).await;
        assert!(still.is_empty());
        assert_eq!(registry.list_all().await.len(), total_rules());
    }

    #[tokio::test]
    async fn user_rules_win_and_are_never_reinserted() {
        let registry = AlertRuleRegistry::new();
        let mut user_rule = parse_rules_from_str(BUILTIN_PACKS[1].rules)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let id = user_rule.id.clone();
        user_rule.name = "user override".into();
        user_rule.enabled = false; // user disabled it via the API
        registry.insert(user_rule).await;

        let pending =
            activate_initial(&cfg(PresetMode::All, &[], &[]), &metrics(&[]), &registry).await;
        assert!(pending.is_empty());
        assert_eq!(registry.list_all().await.len(), total_rules());
        let kept = registry.get(&id).await.unwrap();
        assert_eq!(kept.name, "user override");
        assert!(
            !kept.enabled,
            "activation must not re-enable a user-disabled rule"
        );
    }
}
