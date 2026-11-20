use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio::time::Instant;

use crate::notifier::{AlertPayload, NotificationBatch};
use crate::routing::ResolvedRoute;
use crate::{AlertInstance, AlertState};

const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0100_0000_01b3;

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash ^= u64::from(b);
        *hash = hash.wrapping_mul(FNV1A_PRIME);
    }
}

/// Unique identifier for an alert group within a routing path.
#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct GroupKey {
    pub route_path_id: String,
    /// Exact grouping labels. Keeping the map in the key avoids merging two
    /// unrelated alerts on a 64-bit hash collision.
    pub group_labels: BTreeMap<String, String>,
    pub group_key_hash: u64,
}

/// Compute a deterministic [`GroupKey`] and extracted group labels for an alert on a route.
pub fn compute_group_key(
    route: &ResolvedRoute,
    alert: &AlertInstance,
) -> (GroupKey, BTreeMap<String, String>) {
    let mut hash = FNV1A_OFFSET_BASIS;

    if route.disable_grouping {
        fnv1a_update(&mut hash, &alert.fingerprint.to_le_bytes());
        for (k, v) in &alert.labels {
            fnv1a_update(&mut hash, k.as_bytes());
            fnv1a_update(&mut hash, &[0xff]);
            fnv1a_update(&mut hash, v.as_bytes());
            fnv1a_update(&mut hash, &[0xfe]);
        }
        return (
            GroupKey {
                route_path_id: route.route_path_id.clone(),
                group_labels: alert.labels.clone(),
                group_key_hash: hash,
            },
            alert.labels.clone(),
        );
    }

    let mut keys: Vec<&str> = route.group_by.iter().map(String::as_str).collect();
    keys.sort_unstable();
    keys.dedup();

    let mut group_labels = BTreeMap::new();
    for k in keys {
        if let Some(v) = alert.labels.get(k) {
            fnv1a_update(&mut hash, k.as_bytes());
            fnv1a_update(&mut hash, &[0xff]);
            fnv1a_update(&mut hash, v.as_bytes());
            fnv1a_update(&mut hash, &[0xfe]);
            group_labels.insert(k.to_string(), v.clone());
        }
    }

    if group_labels.is_empty() {
        // Alertmanager's default is `group_by: [alertname]`. Parqtel rules do
        // not carry an alertname label, so derive one from the rule name
        // rather than collapsing every alert on the route into one group.
        let alertname = if alert.rule_name.is_empty() {
            alert.rule_id.clone()
        } else {
            alert.rule_name.clone()
        };
        fnv1a_update(&mut hash, b"alertname");
        fnv1a_update(&mut hash, &[0xff]);
        fnv1a_update(&mut hash, alertname.as_bytes());
        fnv1a_update(&mut hash, &[0xfe]);
        group_labels.insert("alertname".to_string(), alertname);
    }

    (
        GroupKey {
            route_path_id: route.route_path_id.clone(),
            group_labels: group_labels.clone(),
            group_key_hash: hash,
        },
        group_labels,
    )
}

/// Lifecycle phase of an [`AlertGroup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupPhase {
    WaitingInitial,
    Active,
    Resolved,
}

/// Aggregated group of alerts sharing a route and grouping labels.
#[derive(Debug, Clone)]
pub struct AlertGroup {
    pub key: GroupKey,
    pub route: ResolvedRoute,
    pub group_labels: BTreeMap<String, String>,
    pub phase: GroupPhase,
    pub members: HashMap<u64, AlertInstance>,
    pub created_at: Instant,
    pub last_updated: Instant,
    pub last_notified: Option<Instant>,
    pub dirty: bool,
    pub resolved_at: Option<Instant>,
}

impl AlertGroup {
    fn all_resolved(&self) -> bool {
        !self.members.is_empty()
            && self
                .members
                .values()
                .all(|m| m.state == AlertState::Resolved)
    }

    fn has_unresolved(&self) -> bool {
        self.members
            .values()
            .any(|m| m.state != AlertState::Resolved)
    }

    /// Only an unacknowledged firing alert should drive repeat notifications.
    /// An acknowledged alert remains visible and queryable, but nagging the
    /// receiver every repeat_interval is noise.
    fn has_notifiable_firing(&self) -> bool {
        self.members
            .values()
            .any(|m| matches!(m.state, AlertState::Firing | AlertState::Pending))
    }

    fn to_notification_batch(&self) -> NotificationBatch {
        let mut members: Vec<&AlertInstance> = self.members.values().collect();
        members.sort_by_key(|m| m.fingerprint);

        let status = if self.has_unresolved() {
            "firing".to_string()
        } else {
            "resolved".to_string()
        };

        let alerts = members
            .into_iter()
            .map(|m| AlertPayload {
                status: if m.state == AlertState::Resolved {
                    "resolved".to_string()
                } else {
                    "firing".to_string()
                },
                rule_id: m.rule_id.clone(),
                rule_name: m.rule_name.clone(),
                severity: m.severity,
                value: m.value,
                labels: m.labels.clone(),
                annotations: m.annotations.clone(),
                starts_at: m.started_at,
                ends_at: m.resolved_at,
                fingerprint: m.fingerprint,
                evidence_url: Some(format!("/api/v1/alerts/{}/evidence.svg", m.id)),
                evidence: m.evidence.clone(),
                rca: m.rca.clone(),
            })
            .collect();

        NotificationBatch {
            receiver: self.route.receiver.clone(),
            group_key: format!(
                "{}:{:016x}",
                self.key.route_path_id, self.key.group_key_hash
            ),
            status,
            group_labels: self.group_labels.clone(),
            alerts,
        }
    }
}

/// In-memory manager for alert grouping, batching, repeat intervals, and resolved GC.
#[derive(Debug)]
pub struct GroupManager {
    groups: HashMap<GroupKey, AlertGroup>,
    max_active_groups: usize,
    resolve_retention: Duration,
    evicted_groups: usize,
}

impl GroupManager {
    pub fn new(max_active_groups: usize, resolve_retention: Duration) -> Self {
        Self {
            groups: HashMap::new(),
            max_active_groups,
            resolve_retention,
            evicted_groups: 0,
        }
    }

    pub fn active_count(&self) -> usize {
        self.groups.len()
    }

    pub fn take_evicted_groups(&mut self) -> usize {
        std::mem::take(&mut self.evicted_groups)
    }

    pub fn get(&self, key: &GroupKey) -> Option<&AlertGroup> {
        self.groups.get(key)
    }

    /// Insert or update an alert inside its computed group.
    pub fn ingest(&mut self, route: ResolvedRoute, alert: AlertInstance, now: Instant) {
        let (key, group_labels) = compute_group_key(&route, &alert);

        if let Some(group) = self.groups.get_mut(&key) {
            group.route = route;
            group.last_updated = now;

            let state_changed = match group.members.get(&alert.fingerprint) {
                Some(existing) => existing.state != alert.state,
                None => true,
            };
            if state_changed {
                group.dirty = true;
            }

            let incoming_state = alert.state;
            group.members.insert(alert.fingerprint, alert);

            if group.phase == GroupPhase::Resolved && incoming_state == AlertState::Firing {
                group.phase = GroupPhase::Active;
                group.resolved_at = None;
                group.dirty = true;
            } else if group.all_resolved() {
                if group.resolved_at.is_none() {
                    group.resolved_at = Some(now);
                }
                group.phase = GroupPhase::Resolved;
            }
            return;
        }

        if self.max_active_groups == 0 {
            return;
        }
        if self.groups.len() >= self.max_active_groups {
            self.evict_one();
        }

        let mut members = HashMap::with_capacity(1);
        members.insert(alert.fingerprint, alert);

        self.groups.insert(
            key.clone(),
            AlertGroup {
                key,
                route,
                group_labels,
                phase: GroupPhase::WaitingInitial,
                members,
                created_at: now,
                last_updated: now,
                last_notified: None,
                dirty: true,
                resolved_at: None,
            },
        );
    }

    /// Collect all batches whose timers (`group_wait`, `group_interval`, `repeat_interval`)
    /// have elapsed at `now`, and garbage-collect expired resolved groups.
    pub fn collect_due_batches(&mut self, now: Instant) -> Vec<(ResolvedRoute, NotificationBatch)> {
        let mut due = Vec::new();
        let mut keys: Vec<GroupKey> = self.groups.keys().cloned().collect();
        keys.sort_by(|a, b| {
            a.route_path_id
                .cmp(&b.route_path_id)
                .then_with(|| a.group_key_hash.cmp(&b.group_key_hash))
        });

        for key in keys {
            let Some(group) = self.groups.get_mut(&key) else {
                continue;
            };

            match group.phase {
                GroupPhase::WaitingInitial => {
                    if now >= group.created_at + group.route.group_wait {
                        due.push((group.route.clone(), group.to_notification_batch()));
                        group.last_notified = Some(now);
                        group.dirty = false;
                        if group.all_resolved() {
                            group.phase = GroupPhase::Resolved;
                            if group.resolved_at.is_none() {
                                group.resolved_at = Some(now);
                            }
                        } else {
                            group.phase = GroupPhase::Active;
                        }
                    }
                }
                GroupPhase::Active => {
                    if group.dirty {
                        let ready = group
                            .last_notified
                            .is_none_or(|t| now >= t + group.route.group_interval);
                        if ready {
                            due.push((group.route.clone(), group.to_notification_batch()));
                            group.last_notified = Some(now);
                            group.dirty = false;
                        }
                    } else if group.has_notifiable_firing() {
                        let ready = group
                            .last_notified
                            .is_none_or(|t| now >= t + group.route.repeat_interval);
                        if ready {
                            due.push((group.route.clone(), group.to_notification_batch()));
                            group.last_notified = Some(now);
                        }
                    }
                }
                GroupPhase::Resolved => {
                    if group.dirty {
                        let ready = group
                            .last_notified
                            .is_none_or(|t| now >= t + group.route.group_interval);
                        if ready {
                            due.push((group.route.clone(), group.to_notification_batch()));
                            group.last_notified = Some(now);
                            group.dirty = false;
                        }
                    }
                }
            }
        }

        let retention = self.resolve_retention;
        self.groups.retain(|_, group| {
            !(group.phase == GroupPhase::Resolved
                && !group.dirty
                && group.resolved_at.is_some_and(|t| now >= t + retention))
        });

        due
    }

    /// Synchronize a state change made outside the evaluation loop (manual
    /// acknowledge/resolve). A manual resolve marks the group dirty so the
    /// resolved notification still goes out; a manual acknowledge does not.
    pub fn sync_alert(&mut self, alert: &AlertInstance, notify_state_change: bool, now: Instant) {
        for group in self.groups.values_mut() {
            let Some(member) = group.members.get_mut(&alert.fingerprint) else {
                continue;
            };
            let state_changed = member.state != alert.state;
            *member = alert.clone();
            group.last_updated = now;
            if notify_state_change && state_changed {
                group.dirty = true;
            }
            if group.all_resolved() {
                group.phase = GroupPhase::Resolved;
                group.resolved_at.get_or_insert(now);
            }
        }
    }

    fn evict_one(&mut self) {
        let victim = self
            .groups
            .iter()
            .filter(|(_, g)| g.phase == GroupPhase::Resolved)
            .min_by_key(|(_, g)| g.resolved_at.unwrap_or(g.last_updated))
            .map(|(k, _)| k.clone())
            .or_else(|| {
                self.groups
                    .iter()
                    .min_by_key(|(_, g)| g.last_updated)
                    .map(|(k, _)| k.clone())
            });

        if let Some(k) = victim {
            self.groups.remove(&k);
            self.evicted_groups += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::{RuleType, Severity};
    use chrono::Utc;
    use ulid::Ulid;

    fn sample_route(receiver: &str, group_by: &[&str], disable_grouping: bool) -> ResolvedRoute {
        ResolvedRoute {
            route_path_id: "root.0".to_string(),
            receiver: receiver.to_string(),
            group_by: group_by.iter().map(|s| (*s).to_string()).collect(),
            disable_grouping,
            group_wait: Duration::from_secs(30),
            group_interval: Duration::from_secs(60),
            repeat_interval: Duration::from_secs(300),
        }
    }

    fn sample_alert(fp: u64, state: AlertState, pairs: &[(&str, &str)]) -> AlertInstance {
        let labels: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        AlertInstance {
            id: Ulid::new(),
            rule_id: "r1".to_string(),
            rule_name: "High Latency".to_string(),
            fingerprint: fp,
            labels,
            annotations: BTreeMap::new(),
            state,
            severity: Severity::Critical,
            value: Some(1.0),
            started_at: Utc::now(),
            updated_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.0,
            transition_log: Vec::new(),
            notification_sent: false,
            source_rule_type: RuleType::Static,

            evidence: None,

            rca: None,
        }
    }

    #[test]
    fn test_compute_group_key_determinism_and_disable_grouping() {
        let route_sorted = sample_route("ops", &["service", "env"], false);
        let route_unsorted = sample_route("ops", &["env", "service", "env"], false);
        let a1 = sample_alert(
            1,
            AlertState::Firing,
            &[("env", "prod"), ("service", "api"), ("host", "h1")],
        );
        let a2 = sample_alert(
            2,
            AlertState::Firing,
            &[("env", "prod"), ("service", "api"), ("host", "h2")],
        );

        let (k1, l1) = compute_group_key(&route_sorted, &a1);
        let (k2, l2) = compute_group_key(&route_unsorted, &a2);
        assert_eq!(k1, k2);
        assert_eq!(l1, l2);
        assert_eq!(
            l1,
            BTreeMap::from([
                ("env".to_string(), "prod".to_string()),
                ("service".to_string(), "api".to_string())
            ])
        );

        let route_ungrouped = sample_route("ops", &["..."], true);
        let (ku1, lu1) = compute_group_key(&route_ungrouped, &a1);
        let (ku2, lu2) = compute_group_key(&route_ungrouped, &a2);
        assert_ne!(ku1, ku2);
        assert_eq!(lu1, a1.labels);
        assert_eq!(lu2, a2.labels);
    }

    #[tokio::test]
    async fn test_group_lifecycle_wait_interval_repeat_resolve_gc() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(100, Duration::from_secs(600));
        let route = sample_route("ops", &["service"], false);

        let a1 = sample_alert(10, AlertState::Firing, &[("service", "api")]);
        let (key, _) = compute_group_key(&route, &a1);
        mgr.ingest(route.clone(), a1, Instant::now());
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.get(&key).unwrap().phase, GroupPhase::WaitingInitial);

        // Before group_wait (30s): nothing due
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());

        // At group_wait (30s): flushes initial batch and transitions to Active
        tokio::time::advance(Duration::from_secs(1)).await;
        let batches = mgr.collect_due_batches(Instant::now());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.status, "firing");
        assert_eq!(batches[0].1.alerts.len(), 1);
        assert_eq!(mgr.get(&key).unwrap().phase, GroupPhase::Active);

        // Add a second alert -> dirty = true, must wait group_interval (60s)
        let a2 = sample_alert(20, AlertState::Firing, &[("service", "api")]);
        mgr.ingest(route.clone(), a2, Instant::now());
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        let batches = mgr.collect_due_batches(Instant::now());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.alerts.len(), 2);

        // Unchanged -> repeat_interval (300s)
        tokio::time::advance(Duration::from_secs(299)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        let batches = mgr.collect_due_batches(Instant::now());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.status, "firing");

        // Resolve both members -> phase becomes Resolved, dirty = true
        mgr.ingest(
            route.clone(),
            sample_alert(10, AlertState::Resolved, &[("service", "api")]),
            Instant::now(),
        );
        mgr.ingest(
            route.clone(),
            sample_alert(20, AlertState::Resolved, &[("service", "api")]),
            Instant::now(),
        );
        assert_eq!(mgr.get(&key).unwrap().phase, GroupPhase::Resolved);
        assert!(mgr.get(&key).unwrap().dirty);

        // Wait group_interval (60s) -> emits resolved batch
        tokio::time::advance(Duration::from_secs(60)).await;
        let batches = mgr.collect_due_batches(Instant::now());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.status, "resolved");
        assert!(!mgr.get(&key).unwrap().dirty);

        // Before resolve_retention (600s from resolved_at; 60s already elapsed): still present
        tokio::time::advance(Duration::from_secs(539)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        assert_eq!(mgr.active_count(), 1);

        // At resolve_retention: garbage-collected
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_empty_group_by_falls_back_to_alert_name() {
        let route = sample_route("ops", &[], false);
        let a = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        let b = sample_alert(2, AlertState::Firing, &[("service", "web")]);
        let (ka, la) = compute_group_key(&route, &a);
        let (kb, lb) = compute_group_key(&route, &b);
        assert_eq!(ka, kb);
        assert_eq!(la.get("alertname"), Some(&"High Latency".to_string()));
        assert_eq!(lb.get("alertname"), Some(&"High Latency".to_string()));
    }

    #[test]
    fn test_exact_group_labels_participate_in_equality() {
        let route = sample_route("ops", &["service"], false);
        let a = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        let b = sample_alert(2, AlertState::Firing, &[("service", "web")]);
        let (ka, _) = compute_group_key(&route, &a);
        let (kb, _) = compute_group_key(&route, &b);
        assert_ne!(ka, kb);
        if ka.group_key_hash == kb.group_key_hash {
            assert_ne!(ka, kb, "hash collision must not merge groups");
        }
    }

    #[tokio::test]
    async fn test_acknowledged_alert_stops_repeat_notifications() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(600));
        let route = sample_route("ops", &["service"], false);
        let firing = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        let (key, _) = compute_group_key(&route, &firing);
        mgr.ingest(route, firing.clone(), Instant::now());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);

        let mut acked = firing.clone();
        acked.state = AlertState::Acknowledged;
        mgr.sync_alert(&acked, false, Instant::now());

        // The old implementation repeated acknowledged alerts forever.
        tokio::time::advance(Duration::from_secs(301)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        assert_eq!(mgr.get(&key).unwrap().phase, GroupPhase::Active);
    }

    #[tokio::test]
    async fn test_manual_resolve_emits_resolved_batch() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(600));
        let route = sample_route("ops", &["service"], false);
        let firing = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        mgr.ingest(route, firing.clone(), Instant::now());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);

        let mut resolved = firing;
        resolved.state = AlertState::Resolved;
        resolved.resolved_at = Some(Utc::now());
        mgr.sync_alert(&resolved, true, Instant::now());
        tokio::time::advance(Duration::from_secs(60)).await;
        let batches = mgr.collect_due_batches(Instant::now());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.status, "resolved");
    }

    #[tokio::test]
    async fn test_resolved_group_reopens_on_firing_alert() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(600));
        let route = sample_route("ops", &["service"], false);
        let a1 = sample_alert(1, AlertState::Firing, &[("service", "auth")]);
        let (key, _) = compute_group_key(&route, &a1);

        mgr.ingest(route.clone(), a1, Instant::now());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);

        mgr.ingest(
            route.clone(),
            sample_alert(1, AlertState::Resolved, &[("service", "auth")]),
            Instant::now(),
        );
        assert_eq!(mgr.get(&key).unwrap().phase, GroupPhase::Resolved);

        // Re-firing transitions back to Active and clears resolved_at
        mgr.ingest(
            route.clone(),
            sample_alert(1, AlertState::Firing, &[("service", "auth")]),
            Instant::now(),
        );
        let grp = mgr.get(&key).unwrap();
        assert_eq!(grp.phase, GroupPhase::Active);
        assert!(grp.resolved_at.is_none());
        assert!(grp.dirty);
    }

    #[tokio::test]
    async fn test_waiting_initial_resolves_directly_to_resolved() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(60));
        let route = sample_route("ops", &["service"], false);
        let a1 = sample_alert(1, AlertState::Resolved, &[("service", "db")]);
        let (key, _) = compute_group_key(&route, &a1);

        mgr.ingest(route, a1, Instant::now());
        tokio::time::advance(Duration::from_secs(30)).await;
        let batches = mgr.collect_due_batches(Instant::now());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.status, "resolved");
        let grp = mgr.get(&key).unwrap();
        assert_eq!(grp.phase, GroupPhase::Resolved);
        assert!(grp.resolved_at.is_some());
    }

    #[test]
    fn test_new_group_ignored_when_max_active_groups_is_zero() {
        let mut mgr = GroupManager::new(0, Duration::from_secs(60));
        let route = sample_route("ops", &["service"], false);
        let a1 = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        mgr.ingest(route.clone(), a1, Instant::now());
        assert_eq!(mgr.active_count(), 0);

        // An already-known key is still updated even with a zero budget.
        let mut mgr = GroupManager::new(1, Duration::from_secs(60));
        let a1 = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        let (key, _) = compute_group_key(&route, &a1);
        mgr.ingest(route.clone(), a1, Instant::now());
        mgr.max_active_groups = 0;
        mgr.ingest(
            route,
            sample_alert(1, AlertState::Resolved, &[("service", "api")]),
            Instant::now(),
        );
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.get(&key).unwrap().phase, GroupPhase::Resolved);
    }

    #[tokio::test]
    async fn test_resolved_eventually_gc_even_without_further_notification() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(60));
        let route = sample_route("ops", &["service"], false);
        let a1 = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        let (key, _) = compute_group_key(&route, &a1);

        mgr.ingest(route.clone(), a1, Instant::now());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);

        // Resolve -> dirty, but the resolved group is never notified again because
        // the caller stops flushing; retention still expires it on the next tick.
        mgr.ingest(
            route,
            sample_alert(1, AlertState::Resolved, &[("service", "api")]),
            Instant::now(),
        );
        assert_eq!(mgr.get(&key).unwrap().phase, GroupPhase::Resolved);
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        assert_eq!(mgr.active_count(), 0);
    }

    #[tokio::test]
    async fn test_empty_member_set_reports_no_firing_and_resolves() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(600));
        let route = sample_route("ops", &["service"], false);
        let a1 = sample_alert(1, AlertState::Resolved, &[("service", "api")]);
        let (key, _) = compute_group_key(&route, &a1);
        mgr.ingest(route, a1, Instant::now());

        // Drop the only member: the group now has no members at all.
        mgr.groups.get_mut(&key).unwrap().members.clear();
        let group = mgr.get(&key).unwrap();
        assert!(!group.all_resolved());
        assert!(!group.has_unresolved());
        let batch = group.to_notification_batch();
        assert_eq!(batch.status, "resolved");
        assert!(batch.alerts.is_empty());
    }

    #[tokio::test]
    async fn test_collect_due_batches_orders_by_route_path_then_hash() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(600));
        let route_b = ResolvedRoute {
            route_path_id: "root.1".to_string(),
            ..sample_route("ops", &["service"], false)
        };
        let route_a = sample_route("ops", &["service"], false);

        // Insert in reverse order so sorting has to reorder the keys, and give the
        // earlier route two groups sharing a route_path_id to hit the hash compare.
        mgr.ingest(
            route_b.clone(),
            sample_alert(3, AlertState::Firing, &[("service", "s3")]),
            Instant::now(),
        );
        mgr.ingest(
            route_a.clone(),
            sample_alert(2, AlertState::Firing, &[("service", "s2")]),
            Instant::now(),
        );
        mgr.ingest(
            route_a.clone(),
            sample_alert(1, AlertState::Firing, &[("service", "s1")]),
            Instant::now(),
        );
        assert_eq!(mgr.active_count(), 3);

        tokio::time::advance(Duration::from_secs(30)).await;
        let batches = mgr.collect_due_batches(Instant::now());
        assert_eq!(batches.len(), 3);
        // Both root.0 groups come before the root.1 group, bit-identical order each tick.
        assert_eq!(batches[0].0.route_path_id, "root.0");
        assert_eq!(batches[1].0.route_path_id, "root.0");
        assert_eq!(batches[2].0.route_path_id, "root.1");
        let keys: Vec<&str> = batches.iter().map(|(_, b)| b.group_key.as_str()).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

    #[tokio::test]
    async fn test_repeat_interval_skipped_when_nothing_firing_and_dirty_resolved_waits() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(10, Duration::from_secs(600));
        let route = sample_route("ops", &["service"], false);
        let a1 = sample_alert(1, AlertState::Firing, &[("service", "api")]);
        mgr.ingest(route.clone(), a1, Instant::now());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);

        // No change while firing -> repeat_interval (300s) re-notifies.
        tokio::time::advance(Duration::from_secs(299)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);

        // Resolve: dirty, but inside group_interval -> nothing due yet.
        mgr.ingest(
            route.clone(),
            sample_alert(1, AlertState::Resolved, &[("service", "api")]),
            Instant::now(),
        );
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(mgr.collect_due_batches(Instant::now()).len(), 1);
        // Clean, resolved, member resolved -> no further repeats.
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(mgr.collect_due_batches(Instant::now()).is_empty());
    }

    #[tokio::test]
    async fn test_eviction_prefers_resolved_then_oldest_active() {
        tokio::time::pause();
        let mut mgr = GroupManager::new(2, Duration::from_secs(600));
        let route = sample_route("ops", &["service"], false);

        let a1 = sample_alert(1, AlertState::Firing, &[("service", "s1")]);
        let (k1, _) = compute_group_key(&route, &a1);
        mgr.ingest(route.clone(), a1, Instant::now());

        tokio::time::advance(Duration::from_secs(5)).await;
        let a2 = sample_alert(2, AlertState::Firing, &[("service", "s2")]);
        let (k2, _) = compute_group_key(&route, &a2);
        mgr.ingest(route.clone(), a2, Instant::now());

        // Resolve s2 so eviction prefers s2 over active s1
        tokio::time::advance(Duration::from_secs(5)).await;
        mgr.ingest(
            route.clone(),
            sample_alert(2, AlertState::Resolved, &[("service", "s2")]),
            Instant::now(),
        );

        tokio::time::advance(Duration::from_secs(5)).await;
        let a3 = sample_alert(3, AlertState::Firing, &[("service", "s3")]);
        let (k3, _) = compute_group_key(&route, &a3);
        mgr.ingest(route.clone(), a3, Instant::now());

        assert_eq!(mgr.active_count(), 2);
        assert!(mgr.get(&k1).is_some());
        assert!(mgr.get(&k2).is_none());
        assert!(mgr.get(&k3).is_some());

        // Both s1 and s3 are active; inserting s4 evicts oldest active (s1)
        tokio::time::advance(Duration::from_secs(5)).await;
        let a4 = sample_alert(4, AlertState::Firing, &[("service", "s4")]);
        let (k4, _) = compute_group_key(&route, &a4);
        mgr.ingest(route, a4, Instant::now());

        assert_eq!(mgr.active_count(), 2);
        assert!(mgr.get(&k1).is_none());
        assert!(mgr.get(&k3).is_some());
        assert!(mgr.get(&k4).is_some());
    }
}
