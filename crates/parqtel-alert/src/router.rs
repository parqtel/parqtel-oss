//! Alert routing (Alertmanager-lite): deliver firing alerts to webhook sinks
//! based on a route table, honoring silence windows and repeat intervals.
//!
//! Semantics:
//! - Routes are evaluated **in order; first match wins** (Alertmanager model).
//! - A route matches when the alert's severity is at or above
//!   `match_severity` AND all `match_labels` pairs are present on the alert.
//! - Silences mute **routing only** — silenced alerts still fire, persist,
//!   and remain queryable in the UI/API.
//! - Delivery: POST JSON payload to `webhook_url`; retries are the sink's
//!   responsibility (Alertmanager-style at-least-once per repeat window).

use crate::egress::EgressPolicy;
use crate::{AlertFiringEvent, AlertState, Severity};
use parqtel_core::config::{NotificationConfig, RouteConfig, SilenceConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

/// Severity ranking for `match_severity` comparisons.
fn severity_rank(sev: &Severity) -> u8 {
    match sev {
        Severity::Critical => 3,
        Severity::Warning => 2,
        Severity::Info => 1,
    }
}

fn parse_match_severity(s: &str) -> u8 {
    match s.to_ascii_lowercase().as_str() {
        "critical" => 3,
        "warning" => 2,
        "info" => 1,
        _ => 1,
    }
}

/// Runtime state for the alert router.
pub struct AlertRouter {
    config: RwLock<NotificationConfig>,
    silences: RwLock<Vec<SilenceConfig>>,
    /// Last delivery time per (route name, fingerprint).
    last_sent: Mutex<HashMap<(String, u64), Instant>>,
    client: reqwest::Client,
    egress: EgressPolicy,
    /// Directory for silence persistence (None = in-memory only).
    silence_dir: Option<std::path::PathBuf>,
    /// Runtime route table persisted by the API (None = config-file only).
    routes_path: Option<std::path::PathBuf>,
}

impl AlertRouter {
    pub fn new(config: NotificationConfig) -> Self {
        let timeout_secs = config.send_timeout_secs;
        let egress = EgressPolicy::from_config(&config.egress);
        Self {
            config: RwLock::new(config),
            silences: RwLock::new(Vec::new()),
            last_sent: Mutex::new(HashMap::new()),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(timeout_secs.max(1)))
                // A redirect to a private address would bypass the egress
                // check performed before the request.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
            egress,
            silence_dir: None,
            routes_path: None,
        }
    }

    /// Replace the egress policy, e.g. after a configuration reload.
    pub fn set_egress_policy(&mut self, config: &parqtel_core::config::EgressConfig) {
        self.egress = EgressPolicy::from_config(config);
    }

    /// Enable silence persistence in the given directory (loads existing
    /// silences from `silences.json`, atomic tmp+rename on every change).
    pub fn with_silence_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        let dir = dir.into();
        self.routes_path = Some(dir.join("alert_routes.json"));
        self.silence_dir = Some(dir);
        self.load_silences_from_disk();
        self.load_routes_from_disk();
        self
    }

    /// Replace the route table from the runtime file, if one exists.
    ///
    /// Runtime routes win over the TOML table because they are the result of a
    /// later explicit API mutation. A corrupt file is ignored with a warning so
    /// the process still starts with its configured routes.
    fn load_routes_from_disk(&self) {
        let Some(path) = &self.routes_path else {
            return;
        };
        let Ok(content) = std::fs::read_to_string(path) else {
            return;
        };
        let routes: Vec<RouteConfig> = match serde_json::from_str(&content) {
            Ok(routes) => routes,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "alert_routes.json is corrupt, using configured routes");
                return;
            }
        };
        if let Ok(mut config) = self.config.try_write() {
            config.routes = routes;
        }
    }

    fn persist_routes(&self, routes: &[RouteConfig]) {
        let Some(path) = &self.routes_path else {
            return;
        };
        let Some(dir) = path.parent() else {
            return;
        };
        let Ok(json) = serde_json::to_vec_pretty(routes) else {
            tracing::warn!("failed to serialize alert routes");
            return;
        };
        if let Err(e) = std::fs::create_dir_all(dir) {
            tracing::warn!(path = %dir.display(), error = %e, "failed to create alert state directory");
            return;
        }
        let tmp = path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, &json) {
            tracing::warn!(path = %tmp.display(), error = %e, "failed to write alert routes");
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            tracing::warn!(path = %path.display(), error = %e, "failed to replace alert routes");
        }
    }

    fn load_silences_from_disk(&self) {
        let Some(ref dir) = self.silence_dir else {
            return;
        };
        let path = dir.join("silences.json");
        let Ok(content) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(silences): Result<Vec<SilenceConfig>, _> = serde_json::from_str(&content) else {
            tracing::warn!("silences.json is corrupt, starting without silences");
            return;
        };
        // Called from the constructor before the router is shared, so
        // try_write cannot contend; fall back to a blocking read if it ever
        // does (both leave state consistent).
        let now = chrono::Utc::now().timestamp();
        let live: Vec<SilenceConfig> = silences.into_iter().filter(|s| s.ends_at > now).collect();
        if let Ok(mut guard) = self.silences.try_write() {
            *guard = live;
        }
    }

    async fn persist_silences(&self) {
        let Some(ref dir) = self.silence_dir else {
            return;
        };
        let silences = self.silences.read().await;
        // Prune expired entries so the file self-cleans over time.
        let now = chrono::Utc::now().timestamp();
        let live: Vec<&SilenceConfig> = silences.iter().filter(|s| s.ends_at > now).collect();
        let path = dir.join("silences.json");
        if let Ok(json) = serde_json::to_string_pretty(&live) {
            let _ = std::fs::create_dir_all(dir);
            let tmp = dir.join("silences.json.tmp");
            if std::fs::write(&tmp, &json).is_ok() {
                let _ = std::fs::rename(tmp, path);
            }
        }
    }

    /// Replace the full routing configuration (API-driven config reload).
    ///
    /// When a state directory is configured, the route table is written
    /// atomically so a restart keeps the API-visible configuration.
    pub async fn set_config(&self, config: NotificationConfig) {
        self.persist_routes(&config.routes);
        *self.config.write().await = config;
    }

    pub async fn config(&self) -> NotificationConfig {
        self.config.read().await.clone()
    }

    /// Add or update a silence (persisted when a silence dir is configured).
    pub async fn add_silence(&self, silence: SilenceConfig) {
        let mut silences = self.silences.write().await;
        silences.retain(|s| s.name != silence.name);
        silences.push(silence);
        drop(silences);
        self.persist_silences().await;
    }

    /// Remove a silence by name. Returns true when one was removed.
    pub async fn remove_silence(&self, name: &str) -> bool {
        let mut silences = self.silences.write().await;
        let before = silences.len();
        silences.retain(|s| s.name != name);
        let removed = silences.len() != before;
        drop(silences);
        if removed {
            self.persist_silences().await;
        }
        removed
    }

    /// Active silences (not yet expired).
    pub async fn silences(&self) -> Vec<SilenceConfig> {
        let now = chrono::Utc::now().timestamp();
        self.silences
            .read()
            .await
            .iter()
            .filter(|s| s.ends_at > now)
            .cloned()
            .collect()
    }

    /// Whether the alert matches any active silence.
    fn is_silenced(silences: &[SilenceConfig], event: &AlertFiringEvent) -> bool {
        let now = chrono::Utc::now().timestamp();
        silences.iter().any(|s| {
            s.ends_at > now
                && s.starts_at <= now
                && s.match_labels.iter().all(|(k, v)| {
                    event.instance.labels.get(k).map(String::as_str) == Some(v.as_str())
                })
        })
    }

    /// First route matching the alert, if any.
    fn match_route<'a>(
        routes: &'a [RouteConfig],
        event: &AlertFiringEvent,
    ) -> Option<&'a RouteConfig> {
        routes.iter().find(|r| {
            !r.webhook_url.is_empty()
                && severity_rank(&event.instance.severity)
                    >= parse_match_severity(&r.match_severity)
                && r.match_labels.iter().all(|(k, v)| {
                    event.instance.labels.get(k).map(String::as_str) == Some(v.as_str())
                })
        })
    }

    /// Process a firing event: find the matching route, honor silences and
    /// the repeat window, then deliver to the webhook.
    /// Returns the route name when a delivery was attempted.
    pub async fn handle_event(&self, event: AlertFiringEvent) -> Option<String> {
        // Resolved events close the loop for some receivers; Parqtel only
        // routes FIRING transitions today.
        if event.instance.state != AlertState::Firing {
            return None;
        }

        let config = self.config.read().await;
        let silences = self.silences.read().await;
        if Self::is_silenced(&silences, &event) {
            tracing::info!(
                alert = %event.instance.rule_name,
                "alert silenced, not routed"
            );
            return None;
        }
        let route = Self::match_route(&config.routes, &event)?;
        let route_name = route.name.clone();

        // Repeat window: skip when we delivered for this fingerprint recently.
        {
            let last_sent = self.last_sent.lock().await;
            let key = (route_name.clone(), event.instance.fingerprint);
            if let Some(sent_at) = last_sent.get(&key) {
                // repeat_minutes == 0 means "notify once": suppress repeats
                // for a long window (24h) rather than literally forever, so
                // the dedup map stays bounded.
                const NOTIFY_ONCE_WINDOW_SECS: u64 = 24 * 3600;
                let window = if route.repeat_minutes == 0 {
                    Duration::from_secs(NOTIFY_ONCE_WINDOW_SECS)
                } else {
                    Duration::from_secs(route.repeat_minutes * 60)
                };
                if sent_at.elapsed() < window {
                    return None;
                }
            }
        }

        let payload = serde_json::json!({
            "receiver": route_name,
            "status": "firing",
            "alerts": [{
                "status": "firing",
                "labels": event.instance.labels,
                "annotations": event.instance.annotations,
                "rule": event.instance.rule_name,
                "rule_id": event.instance.rule_id,
                "severity": format!("{:?}", event.instance.severity).to_lowercase(),
                "value": event.instance.value,
                "starts_at": event.instance.started_at.to_rfc3339(),
                "fingerprint": event.instance.fingerprint,
            }],
        });

        let url = route.webhook_url.clone();
        if let Err(e) = self.egress.check_url(&url).await {
            tracing::warn!(route = %route_name, url = %url, error = %e, "alert delivery blocked by egress policy");
            return None;
        }
        let result: Result<(), String> = match self.client.post(&url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(format!("sink returned {}", resp.status())),
            Err(e) => Err(e.to_string()),
        };

        {
            let mut last_sent = self.last_sent.lock().await;
            let key = (route_name.clone(), event.instance.fingerprint);
            last_sent.insert(key, Instant::now());
            // Bound the map so long sessions don't grow it unbounded.
            if last_sent.len() > 10_000 {
                last_sent.retain(|_, at| at.elapsed() < Duration::from_secs(24 * 3600));
            }
        }

        match result {
            Ok(()) => {
                tracing::info!(route = %route_name, alert = %event.instance.rule_name, "alert routed");
                Some(route_name)
            }
            Err(e) => {
                tracing::warn!(route = %route_name, error = %e, "alert delivery failed");
                None
            }
        }
    }
}

/// Spawns the router loop consuming firing events from the alert engine.
pub fn spawn_router(
    router: Arc<AlertRouter>,
    mut event_rx: tokio::sync::mpsc::UnboundedReceiver<AlertFiringEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            let _ = router.handle_event(event).await;
        }
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::AlertInstance;
    use std::collections::BTreeMap;

    fn make_event(severity: Severity, labels: BTreeMap<String, String>) -> AlertFiringEvent {
        let instance = AlertInstance {
            id: ulid::Ulid::new(),
            rule_id: "r1".into(),
            rule_name: "Test Alert".into(),
            fingerprint: AlertInstance::compute_fingerprint("r1", &labels),
            labels,
            annotations: BTreeMap::new(),
            state: AlertState::Firing,
            severity,
            value: Some(42.0),
            started_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.0,
            transition_log: Vec::new(),
            notification_sent: false,
            source_rule_type: crate::RuleType::Static,
        };
        AlertFiringEvent { instance }
    }

    fn route(name: &str, sev: &str, url: &str) -> RouteConfig {
        RouteConfig {
            name: name.into(),
            match_severity: sev.into(),
            match_labels: BTreeMap::new(),
            webhook_url: url.into(),
            repeat_minutes: 240,
        }
    }

    #[test]
    fn severity_matching_inclusive() {
        let routes = vec![route("criticals", "critical", "http://x")];
        let critical = make_event(Severity::Critical, BTreeMap::new());
        let warning = make_event(Severity::Warning, BTreeMap::new());
        assert!(AlertRouter::match_route(&routes, &critical).is_some());
        assert!(AlertRouter::match_route(&routes, &warning).is_none());
    }

    #[test]
    fn first_match_wins() {
        let mut all = route("catch-all", "info", "http://all");
        let mut pagers = route("pagers", "critical", "http://page");
        all.match_labels.insert("team".into(), "infra".into());
        pagers.match_labels.insert("team".into(), "infra".into());
        let routes = vec![pagers, all];
        let mut labels = BTreeMap::new();
        labels.insert("team".to_string(), "infra".to_string());
        let event = make_event(Severity::Critical, labels);
        let matched = AlertRouter::match_route(&routes, &event).unwrap();
        assert_eq!(matched.name, "pagers");
    }

    #[test]
    fn label_mismatch_blocks_route() {
        let mut r = route("infra", "info", "http://x");
        r.match_labels
            .insert("team".to_string(), "infra".to_string());
        let routes = vec![r];
        let event = make_event(Severity::Critical, BTreeMap::new());
        assert!(AlertRouter::match_route(&routes, &event).is_none());
    }

    #[tokio::test]
    async fn silence_mutes_matching_labels_in_window() {
        let mut silence = SilenceConfig {
            name: "maint".into(),
            created_by: "oncall".into(),
            match_labels: BTreeMap::new(),
            starts_at: chrono::Utc::now().timestamp() - 60,
            ends_at: chrono::Utc::now().timestamp() + 3600,
        };
        silence
            .match_labels
            .insert("service".to_string(), "api".to_string());

        let mut labels = BTreeMap::new();
        labels.insert("service".to_string(), "api".to_string());
        let event = make_event(Severity::Critical, labels.clone());
        assert!(AlertRouter::is_silenced(&[silence.clone()], &event));

        // Different service — not silenced.
        labels.insert("service".to_string(), "web".to_string());
        let other = make_event(Severity::Critical, labels);
        assert!(!AlertRouter::is_silenced(&[silence], &other));

        // Expired silence matches nothing.
        let mut expired = SilenceConfig {
            name: "old".into(),
            created_by: "x".into(),
            match_labels: BTreeMap::new(),
            starts_at: chrono::Utc::now().timestamp() - 7200,
            ends_at: chrono::Utc::now().timestamp() - 3600,
        };
        expired
            .match_labels
            .insert("service".to_string(), "api".to_string());
        let mut l2 = BTreeMap::new();
        l2.insert("service".to_string(), "api".to_string());
        let e2 = make_event(Severity::Info, l2);
        assert!(!AlertRouter::is_silenced(&[expired], &e2));
    }

    #[tokio::test]
    async fn no_routes_means_no_delivery() {
        let router = AlertRouter::new(NotificationConfig::default());
        let event = make_event(Severity::Critical, BTreeMap::new());
        assert_eq!(router.handle_event(event).await, None);
    }

    #[tokio::test]
    async fn egress_policy_blocks_loopback_webhook_before_connecting() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/hook");

        let config = NotificationConfig {
            routes: vec![route("local", "info", &url)],
            ..NotificationConfig::default()
        };
        let router = AlertRouter::new(config);

        let result = router
            .handle_event(make_event(Severity::Critical, BTreeMap::new()))
            .await;
        assert_eq!(result, None, "blocked route must not report delivery");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "egress policy must reject the destination before the TCP connect"
        );
    }
}

#[cfg(test)]
mod persistence_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::collections::BTreeMap;

    fn silence_in(name: &str, minutes: i64) -> SilenceConfig {
        let now = chrono::Utc::now().timestamp();
        let mut match_labels = BTreeMap::new();
        match_labels.insert("service".to_string(), "api".to_string());
        SilenceConfig {
            name: name.into(),
            created_by: "test".into(),
            match_labels,
            starts_at: now - 60,
            ends_at: now + minutes * 60,
        }
    }

    #[tokio::test]
    async fn silences_survive_router_restart() {
        let dir = tempfile::tempdir().unwrap();

        // Router 1: create a silence — must persist.
        let router = AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        router.add_silence(silence_in("maint", 60)).await;
        assert!(dir.path().join("silences.json").exists(), "file written");

        // Router 2 (fresh instance, same dir): silence must be reloaded.
        let router2 = AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        let silences = router2.silences().await;
        assert_eq!(silences.len(), 1);
        assert_eq!(silences[0].name, "maint");
        assert_eq!(
            silences[0].match_labels.get("service").map(String::as_str),
            Some("api")
        );
    }

    #[tokio::test]
    async fn removal_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let router = AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        router.add_silence(silence_in("a", 60)).await;
        router.add_silence(silence_in("b", 60)).await;
        assert!(router.remove_silence("a").await);

        let router2 = AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        let silences = router2.silences().await;
        assert_eq!(silences.len(), 1);
        assert_eq!(silences[0].name, "b");
    }

    #[tokio::test]
    async fn expired_silences_not_reloaded_or_persisted() {
        let dir = tempfile::tempdir().unwrap();

        // Write a file containing one expired + one live silence directly.
        let expired = silence_in("expired", -10); // ended 10 min ago
        let live = silence_in("live", 60);
        let json = serde_json::to_string_pretty(&vec![&expired, &live]).unwrap();
        std::fs::write(dir.path().join("silences.json"), json).unwrap();

        let router = AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        let silences = router.silences().await;
        assert_eq!(silences.len(), 1, "expired silence dropped on load");
        assert_eq!(silences[0].name, "live");

        // Any persist prunes expired entries from the file.
        router.add_silence(silence_in("another", 30)).await;
        let on_disk: Vec<SilenceConfig> = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("silences.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(on_disk.len(), 2);
        assert!(on_disk.iter().all(|s| s.name != "expired"));
    }

    #[tokio::test]
    async fn corrupt_file_starts_clean() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("silences.json"), "{not json").unwrap();
        let router = AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        assert!(router.silences().await.is_empty());
        // Still functional: new silences persist over the corrupt file.
        router.add_silence(silence_in("fresh", 60)).await;
        let silences = router.silences().await;
        assert_eq!(silences.len(), 1);
    }

    #[test]
    fn without_dir_silences_are_memory_only() {
        // No panic, no file access — in-memory mode for tests/embedding.
        let router = AlertRouter::new(NotificationConfig::default());
        assert!(router.silence_dir.is_none());
        assert!(router.routes_path.is_none());
    }

    #[tokio::test]
    async fn routes_survive_router_restart() {
        let dir = tempfile::tempdir().unwrap();
        let router = AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        let route = RouteConfig {
            name: "persisted".into(),
            match_severity: "critical".into(),
            match_labels: BTreeMap::new(),
            webhook_url: "https://example.com/hook".into(),
            repeat_minutes: 240,
        };
        router
            .set_config(NotificationConfig {
                routes: vec![route],
                ..NotificationConfig::default()
            })
            .await;
        assert!(
            dir.path().join("alert_routes.json").exists(),
            "route table written"
        );

        let restarted =
            AlertRouter::new(NotificationConfig::default()).with_silence_dir(dir.path());
        let routes = restarted.config().await.routes;
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].name, "persisted");
    }
}
