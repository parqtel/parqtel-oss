use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::dispatcher::metrics::RoutingMetrics;
use crate::error::NotificationError;
use crate::notifier::{NotificationBatch, Notifier};
use crate::routing::ResolvedRoute;

/// Exponential backoff retry policy for transient notification failures.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_attempts: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            max_attempts: 3,
        }
    }
}

fn backoff_with_jitter(backoff: Duration, attempt: usize, group_key: &str) -> Duration {
    if backoff.is_zero() {
        return backoff;
    }
    // Deterministic lightweight 0..25% jitter derived from group_key and attempt.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ (attempt as u64);
    for b in group_key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let quarter_nanos = (backoff.as_nanos() / 4).max(1) as u64;
    let jitter_nanos = hash % quarter_nanos;
    backoff + Duration::from_nanos(jitter_nanos)
}

/// Deliver a [`NotificationBatch`] to a single [`Notifier`] with exponential backoff retries.
pub async fn deliver_with_retry(
    notifier: &dyn Notifier,
    batch: &NotificationBatch,
    policy: &RetryPolicy,
    metrics: &RoutingMetrics,
) -> Result<(), NotificationError> {
    if batch.status == "resolved" && !notifier.send_resolved() {
        return Ok(());
    }

    let max_attempts = policy.max_attempts.max(1);
    let mut backoff = policy.initial_backoff;

    for attempt in 0..max_attempts {
        let start = tokio::time::Instant::now();
        let res = notifier.send(batch).await;
        metrics.observe_duration(
            notifier.name(),
            notifier.channel_type(),
            start.elapsed().as_secs_f64(),
        );

        match res {
            Ok(()) => {
                metrics.inc_notification(notifier.name(), notifier.channel_type(), "success");
                return Ok(());
            }
            Err(e) if !e.is_retryable() => {
                tracing::error!(
                    receiver = notifier.name(),
                    channel = notifier.channel_type(),
                    error = %e,
                    "permanent notification delivery failure"
                );
                metrics.inc_notification(notifier.name(), notifier.channel_type(), "failure");
                return Err(e);
            }
            Err(e) => {
                if attempt + 1 < max_attempts {
                    let delay = e
                        .retry_after()
                        .unwrap_or_else(|| backoff_with_jitter(backoff, attempt, &batch.group_key))
                        .min(policy.max_backoff);
                    tracing::warn!(
                        receiver = notifier.name(),
                        channel = notifier.channel_type(),
                        attempt = attempt + 1,
                        max_attempts,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "transient notification delivery failure; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    backoff = backoff.saturating_mul(2).min(policy.max_backoff);
                } else {
                    tracing::error!(
                        receiver = notifier.name(),
                        channel = notifier.channel_type(),
                        attempts = max_attempts,
                        error = %e,
                        "notification delivery exhausted all retry attempts"
                    );
                    metrics.inc_notification(notifier.name(), notifier.channel_type(), "failure");
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}

/// Per-receiver bounded delivery queues backed by isolated worker tasks.
#[derive(Debug)]
pub struct DeliveryQueue {
    senders: HashMap<String, mpsc::Sender<(ResolvedRoute, NotificationBatch)>>,
}

impl DeliveryQueue {
    /// Spawn one dedicated worker task per configured receiver.
    pub fn new(
        notifiers: HashMap<String, Vec<Arc<dyn Notifier>>>,
        policy: RetryPolicy,
        metrics: Arc<RoutingMetrics>,
        channel_capacity: usize,
    ) -> Self {
        let cap = channel_capacity.max(1);
        let mut senders = HashMap::with_capacity(notifiers.len());

        for (receiver, receiver_notifiers) in notifiers {
            let (tx, mut rx) = mpsc::channel::<(ResolvedRoute, NotificationBatch)>(cap);
            let policy_clone = policy.clone();
            let metrics_clone = Arc::clone(&metrics);
            let receiver_name = receiver.clone();

            tokio::spawn(async move {
                while let Some((_route, batch)) = rx.recv().await {
                    for notifier in &receiver_notifiers {
                        let _ = deliver_with_retry(
                            notifier.as_ref(),
                            &batch,
                            &policy_clone,
                            &metrics_clone,
                        )
                        .await;
                    }
                }
                tracing::debug!(receiver = %receiver_name, "delivery worker exited");
            });

            senders.insert(receiver, tx);
        }

        Self { senders }
    }

    /// Enqueue a batch onto the target receiver's worker without blocking other receivers.
    pub fn enqueue(&self, route: ResolvedRoute, batch: NotificationBatch) {
        let Some(tx) = self.senders.get(&route.receiver) else {
            tracing::warn!(receiver = %route.receiver, "no delivery worker registered for receiver");
            return;
        };

        if let Err(err) = tx.try_send((route, batch)) {
            tracing::warn!(error = %err, "failed to enqueue notification batch");
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::notifier::test_util::{sample_firing_batch, sample_resolved_batch};
    use crate::notifier::webhook::WebhookNotifier;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::Router;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct SequenceState {
        calls: Arc<AtomicUsize>,
        responses: Vec<(StatusCode, Option<String>, String)>,
    }

    async fn handle_seq(State(state): State<SequenceState>) -> (StatusCode, HeaderMap, String) {
        let idx = state.calls.fetch_add(1, Ordering::SeqCst);
        let (status, retry_after, body) = state
            .responses
            .get(idx)
            .or_else(|| state.responses.last())
            .cloned()
            .unwrap_or((StatusCode::OK, None, "ok".to_string()));

        let mut headers = HeaderMap::new();
        if let Some(ra) = retry_after {
            if let Ok(v) = ra.parse() {
                headers.insert(axum::http::header::RETRY_AFTER, v);
            }
        }
        (status, headers, body)
    }

    async fn spawn_sequence_server(
        responses: Vec<(StatusCode, Option<&str>, &str)>,
    ) -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = SequenceState {
            calls: Arc::clone(&calls),
            responses: responses
                .into_iter()
                .map(|(s, ra, b)| (s, ra.map(str::to_string), b.to_string()))
                .collect(),
        };
        let app = Router::new()
            .route("/hook", post(handle_seq))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/hook"), calls)
    }

    fn make_webhook(name: &str, url: String, send_resolved: bool) -> WebhookNotifier {
        WebhookNotifier::new(
            name.to_string(),
            url,
            send_resolved,
            BTreeMap::new(),
            reqwest::Client::new(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_deliver_skips_resolved_when_disabled() {
        let (url, calls) = spawn_sequence_server(vec![(StatusCode::OK, None, "ok")]).await;
        let notifier = make_webhook("ops", url, false);
        let metrics = RoutingMetrics::new();
        let policy = RetryPolicy::default();

        let res = deliver_with_retry(&notifier, &sample_resolved_batch(), &policy, &metrics).await;
        assert!(res.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            metrics.get_notification_count("ops", "webhook", "success"),
            0
        );
    }

    #[tokio::test]
    async fn test_deliver_429_retry_after_then_200() {
        let (url, calls) = spawn_sequence_server(vec![
            (StatusCode::TOO_MANY_REQUESTS, Some("0"), "slow down"),
            (StatusCode::OK, None, "ok"),
        ])
        .await;
        let notifier = make_webhook("ops", url, true);
        let metrics = RoutingMetrics::new();
        let policy = RetryPolicy {
            initial_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(30),
            max_attempts: 3,
        };

        let res = deliver_with_retry(&notifier, &sample_firing_batch(), &policy, &metrics).await;
        assert!(res.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            metrics.get_notification_count("ops", "webhook", "success"),
            1
        );
        assert_eq!(
            metrics.get_notification_count("ops", "webhook", "failure"),
            0
        );
    }

    #[tokio::test]
    async fn test_deliver_503_retries_to_200_with_paused_time() {
        let (url, calls) = spawn_sequence_server(vec![
            (StatusCode::SERVICE_UNAVAILABLE, None, "busy"),
            (StatusCode::OK, None, "ok"),
        ])
        .await;
        let notifier = make_webhook("ops", url, true);
        let metrics = Arc::new(RoutingMetrics::new());
        let policy = RetryPolicy {
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(10),
            max_attempts: 3,
        };

        tokio::time::pause();
        let metrics_c = Arc::clone(&metrics);
        let handle = tokio::spawn(async move {
            deliver_with_retry(&notifier, &sample_firing_batch(), &policy, &metrics_c).await
        });

        // Allow first request to execute and hit sleep(2s + up to 0.5s jitter)
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        let res = handle.await.unwrap();
        assert!(res.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            metrics.get_notification_count("ops", "webhook", "success"),
            1
        );
    }

    #[tokio::test]
    async fn test_deliver_400_and_401_no_retry() {
        for status in [StatusCode::BAD_REQUEST, StatusCode::UNAUTHORIZED] {
            let (url, calls) = spawn_sequence_server(vec![(status, None, "bad")]).await;
            let notifier = make_webhook("ops", url, true);
            let metrics = RoutingMetrics::new();
            let policy = RetryPolicy {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
                max_attempts: 5,
            };

            let err = deliver_with_retry(&notifier, &sample_firing_batch(), &policy, &metrics)
                .await
                .unwrap_err();
            assert!(!err.is_retryable());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                metrics.get_notification_count("ops", "webhook", "failure"),
                1
            );
        }
    }

    #[tokio::test]
    async fn test_deliver_exhausts_retries_on_persistent_503() {
        let (url, calls) =
            spawn_sequence_server(vec![(StatusCode::SERVICE_UNAVAILABLE, None, "down")]).await;
        let notifier = make_webhook("ops", url, true);
        let metrics = RoutingMetrics::new();
        let policy = RetryPolicy {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            max_attempts: 3,
        };

        let err = deliver_with_retry(&notifier, &sample_firing_batch(), &policy, &metrics)
            .await
            .unwrap_err();
        assert!(err.is_retryable());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            metrics.get_notification_count("ops", "webhook", "failure"),
            1
        );
    }

    #[tokio::test]
    async fn test_delivery_queue_dispatches_to_worker() {
        let (url, calls) = spawn_sequence_server(vec![(StatusCode::OK, None, "ok")]).await;
        let notifier: Arc<dyn Notifier> = Arc::new(make_webhook("ops", url, true));
        let metrics = Arc::new(RoutingMetrics::new());
        let mut map = HashMap::new();
        map.insert("ops".to_string(), vec![notifier]);

        let queue = DeliveryQueue::new(map, RetryPolicy::default(), Arc::clone(&metrics), 8);
        let route = ResolvedRoute {
            route_path_id: "root".to_string(),
            receiver: "ops".to_string(),
            group_by: vec![],
            disable_grouping: false,
            group_wait: Duration::from_secs(0),
            group_interval: Duration::from_secs(0),
            repeat_interval: Duration::from_secs(60),
        };
        queue.enqueue(route, sample_firing_batch());

        for _ in 0..50 {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            metrics.get_notification_count("ops", "webhook", "success"),
            1
        );
    }
}
