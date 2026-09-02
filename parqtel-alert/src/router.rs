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
}

impl AlertRouter {
    pub fn new(config: NotificationConfig) -> Self {
        let timeout_secs = config.send_timeout_secs;
        Self {
            config: RwLock::new(config),
            silences: RwLock::new(Vec::new()),
            last_sent: Mutex::new(HashMap::new()),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(timeout_secs.max(1)))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Replace the full routing configuration (API-driven config reload).
    pub async fn set_config(&self, config: NotificationConfig) {
        *self.config.write().await = config;
    }

    pub async fn config(&self) -> NotificationConfig {
        self.config.read().await.clone()
    }

    /// Add or update a silence.
    pub async fn add_silence(&self, silence: SilenceConfig) {
        let mut silences = self.silences.write().await;
        silences.retain(|s| s.name != silence.name);
        silences.push(silence);
    }

    /// Remove a silence by name. Returns true when one was removed.
    pub async fn remove_silence(&self, name: &str) -> bool {
        let mut silences = self.silences.write().await;
        let before = silences.len();
        silences.retain(|s| s.name != name);
        silences.len() != before
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
}
