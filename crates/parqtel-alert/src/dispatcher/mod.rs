pub mod group;
pub mod metrics;
pub mod worker;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::error::NotificationError;
use crate::notifier::build_notifiers;
use crate::routing::RoutingEvaluator;
use crate::AlertInstance;
use parqtel_core::config::{parse_duration, AlertmanagerRoutingConfig, SilenceConfig};

use self::group::GroupManager;
use self::metrics::RoutingMetrics;
use self::worker::{DeliveryQueue, RetryPolicy};

/// Coordinates silence/inhibition/routing evaluation, alert grouping, and async delivery.
pub struct AlertDispatcher {
    evaluator: RoutingEvaluator,
    groups: Mutex<GroupManager>,
    queue: DeliveryQueue,
    metrics: Arc<RoutingMetrics>,
}

impl AlertDispatcher {
    /// Build an [`AlertDispatcher`] from an [`AlertmanagerRoutingConfig`].
    pub fn from_config(
        cfg: &AlertmanagerRoutingConfig,
        metrics: Arc<RoutingMetrics>,
    ) -> Result<Self, NotificationError> {
        let evaluator = RoutingEvaluator::compile(cfg)?;

        let http_timeout =
            parse_duration(&cfg.global.http_timeout).map_err(NotificationError::Config)?;
        let client = reqwest::Client::builder()
            .timeout(http_timeout)
            .build()
            .map_err(|e| NotificationError::Config(format!("failed to build HTTP client: {e}")))?;

        let notifiers = build_notifiers(cfg, client)?;

        let resolve_retention =
            parse_duration(&cfg.global.resolve_timeout).map_err(NotificationError::Config)?;
        let initial_backoff =
            parse_duration(&cfg.global.retry_initial_backoff).map_err(NotificationError::Config)?;
        let max_backoff =
            parse_duration(&cfg.global.retry_max_backoff).map_err(NotificationError::Config)?;

        let policy = RetryPolicy {
            initial_backoff,
            max_backoff,
            max_attempts: cfg.global.retry_max_attempts,
        };

        let groups = Mutex::new(GroupManager::new(cfg.max_active_groups, resolve_retention));
        let queue = DeliveryQueue::new(notifiers, policy, Arc::clone(&metrics), 256);

        Ok(Self {
            evaluator,
            groups,
            queue,
            metrics,
        })
    }

    pub fn metrics(&self) -> &Arc<RoutingMetrics> {
        &self.metrics
    }

    pub fn evaluator(&self) -> &RoutingEvaluator {
        &self.evaluator
    }

    /// Evaluate silences, inhibition, and routing for `alert`, and ingest into matching groups.
    pub async fn ingest_event(
        &self,
        alert: AlertInstance,
        silences: &[SilenceConfig],
        firing_alerts: &[AlertInstance],
    ) {
        let now_unix = chrono::Utc::now().timestamp();
        let eval = self.evaluator.evaluate(
            &alert.labels,
            alert.fingerprint,
            silences,
            firing_alerts,
            now_unix,
        );

        if eval.silenced {
            self.metrics
                .inc_notification("none", "none", "suppressed_silence");
            return;
        }
        if eval.inhibited {
            self.metrics
                .inc_notification("none", "none", "suppressed_inhibit");
            return;
        }

        let now = tokio::time::Instant::now();
        let mut groups = self.groups.lock().await;
        for route in eval.matched_routes {
            groups.ingest(route, alert.clone(), now);
        }
        self.metrics.set_active_groups(groups.active_count());
    }

    /// Collect all due batches from `GroupManager` and enqueue them onto `DeliveryQueue`.
    pub async fn flush_due(&self) {
        let batches = {
            let mut groups = self.groups.lock().await;
            let batches = groups.collect_due_batches(tokio::time::Instant::now());
            self.metrics.set_active_groups(groups.active_count());
            batches
        };

        for (route, batch) in batches {
            self.queue.enqueue(route, batch);
        }
    }

    /// Spawn a background tick loop that periodically flushes due notification batches.
    pub fn spawn_tick_loop(dispatcher: Arc<Self>, tick_interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                dispatcher.flush_due().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::{AlertState, RuleType, Severity};
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::{Json, Router};
    use chrono::Utc;
    use parqtel_core::config::{
        AlertRouteConfig, InhibitRuleConfig, ReceiverConfig, WebhookReceiverConfig,
    };
    use std::collections::BTreeMap;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    async fn spawn_webhook_receiver() -> (String, mpsc::UnboundedReceiver<serde_json::Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        async fn handle(
            State(tx): State<mpsc::UnboundedSender<serde_json::Value>>,
            Json(body): Json<serde_json::Value>,
        ) -> StatusCode {
            let _ = tx.send(body);
            StatusCode::OK
        }
        let app = Router::new().route("/hook", post(handle)).with_state(tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/hook"), rx)
    }

    fn make_alert(fp: u64, pairs: &[(&str, &str)]) -> AlertInstance {
        AlertInstance {
            id: Ulid::new(),
            rule_id: "r1".to_string(),
            rule_name: "Test Alert".to_string(),
            fingerprint: fp,
            labels: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            annotations: BTreeMap::new(),
            state: AlertState::Firing,
            severity: Severity::Warning,
            value: Some(42.0),
            started_at: Utc::now(),
            updated_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            noise_score: 0.0,
            transition_log: Vec::new(),
            notification_sent: false,
            source_rule_type: RuleType::Static,
        }
    }

    fn test_config(url: String) -> AlertmanagerRoutingConfig {
        let mut cfg = AlertmanagerRoutingConfig::default();
        cfg.global.retry_initial_backoff = "10ms".to_string();
        cfg.global.retry_max_backoff = "50ms".to_string();
        cfg.receivers.push(ReceiverConfig {
            name: "wh".to_string(),
            webhook_configs: vec![WebhookReceiverConfig {
                url,
                send_resolved: true,
                headers: BTreeMap::new(),
            }],
            ..Default::default()
        });
        cfg.route = Some(AlertRouteConfig {
            receiver: Some("wh".to_string()),
            group_by: Some(vec!["service".to_string()]),
            group_wait: Some("10ms".to_string()),
            group_interval: Some("20ms".to_string()),
            repeat_interval: Some("100ms".to_string()),
            ..Default::default()
        });
        cfg.inhibit_rules.push(InhibitRuleConfig {
            source_matchers: vec!["severity = critical".to_string()],
            target_matchers: vec!["severity = warning".to_string()],
            equal: vec!["service".to_string()],
        });
        cfg
    }

    #[test]
    fn test_from_config_rejects_invalid_durations() {
        let metrics = Arc::new(RoutingMetrics::new());
        for (field, value) in [
            ("http_timeout", "bad"),
            ("resolve_timeout", "bad"),
            ("retry_initial_backoff", "bad"),
            ("retry_max_backoff", "bad"),
        ] {
            let mut cfg = test_config("http://127.0.0.1:1/hook".to_string());
            match field {
                "http_timeout" => cfg.global.http_timeout = value.to_string(),
                "resolve_timeout" => cfg.global.resolve_timeout = value.to_string(),
                "retry_initial_backoff" => cfg.global.retry_initial_backoff = value.to_string(),
                _ => cfg.global.retry_max_backoff = value.to_string(),
            }
            assert!(matches!(
                AlertDispatcher::from_config(&cfg, Arc::clone(&metrics)),
                Err(NotificationError::Config(_))
            ));
        }
    }

    #[test]
    fn test_from_config_propagates_evaluator_error() {
        // A routing config `validate()` accepts (dangling receiver is reported,
        // not rejected here) still fails when the tree compiles.
        let mut cfg = test_config("http://127.0.0.1:1/hook".to_string());
        cfg.route.as_mut().unwrap().receiver = Some("missing".to_string());
        let mut errors = Vec::new();
        cfg.validate_into("alerts", &mut errors);
        assert!(!errors.is_empty());
        assert!(matches!(
            AlertDispatcher::from_config(&cfg, Arc::new(RoutingMetrics::new())),
            Err(NotificationError::Config(_))
        ));
    }

    #[tokio::test]
    async fn test_dispatcher_accessors_and_flush_without_groups() {
        let (url, _rx) = spawn_webhook_receiver().await;
        let dispatcher =
            AlertDispatcher::from_config(&test_config(url), Arc::new(RoutingMetrics::new()))
                .unwrap();
        assert_eq!(
            dispatcher
                .evaluator()
                .match_routes_only(&BTreeMap::new())
                .len(),
            1
        );
        assert_eq!(dispatcher.metrics().active_groups(), 0);

        // No groups -> flush enqueues nothing and keeps the gauge at 0.
        dispatcher.flush_due().await;
        assert_eq!(dispatcher.metrics().active_groups(), 0);
    }

    #[tokio::test]
    async fn test_dispatcher_ingest_into_route_without_receiver() {
        // Route points at a receiver with no notifier configs -> worker drops the batch.
        let mut cfg = test_config("http://127.0.0.1:1/hook".to_string());
        cfg.receivers = vec![ReceiverConfig {
            name: "wh".to_string(),
            ..Default::default()
        }];
        let metrics = Arc::new(RoutingMetrics::new());
        let dispatcher = AlertDispatcher::from_config(&cfg, Arc::clone(&metrics)).unwrap();

        dispatcher
            .ingest_event(make_alert(1, &[("service", "api")]), &[], &[])
            .await;
        assert_eq!(metrics.active_groups(), 1);
        // Delivery finds no registered worker for the route's receiver.
        dispatcher.flush_due().await;
        assert!(
            crate::notifier::build_notifiers(&cfg, reqwest::Client::new()).unwrap()["wh"]
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_dispatcher_silences_inhibits_and_delivers() {
        let (url, mut rx) = spawn_webhook_receiver().await;
        let metrics = Arc::new(RoutingMetrics::new());
        let dispatcher = Arc::new(
            AlertDispatcher::from_config(&test_config(url), Arc::clone(&metrics)).unwrap(),
        );

        let now = Utc::now().timestamp();
        let silence = SilenceConfig {
            name: "maint".to_string(),
            created_by: "ops".to_string(),
            match_labels: BTreeMap::from([("service".to_string(), "silenced-svc".to_string())]),
            starts_at: now - 60,
            ends_at: now + 60,
        };

        // 1. Silenced alert
        dispatcher
            .ingest_event(
                make_alert(1, &[("service", "silenced-svc"), ("severity", "warning")]),
                &[silence],
                &[],
            )
            .await;
        assert_eq!(
            metrics.get_notification_count("none", "none", "suppressed_silence"),
            1
        );
        assert_eq!(metrics.active_groups(), 0);

        // 2. Inhibited alert
        let firing_critical = make_alert(2, &[("service", "api"), ("severity", "critical")]);
        dispatcher
            .ingest_event(
                make_alert(3, &[("service", "api"), ("severity", "warning")]),
                &[],
                &[firing_critical],
            )
            .await;
        assert_eq!(
            metrics.get_notification_count("none", "none", "suppressed_inhibit"),
            1
        );
        assert_eq!(metrics.active_groups(), 0);

        // 3. Normal alert -> grouped, flushed by tick loop, delivered to webhook
        let handle =
            AlertDispatcher::spawn_tick_loop(Arc::clone(&dispatcher), Duration::from_millis(5));
        dispatcher
            .ingest_event(
                make_alert(4, &[("service", "checkout"), ("severity", "warning")]),
                &[],
                &[],
            )
            .await;
        assert_eq!(metrics.active_groups(), 1);

        let payload = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(payload["receiver"], "wh");
        assert_eq!(payload["status"], "firing");
        handle.abort();
    }
}
