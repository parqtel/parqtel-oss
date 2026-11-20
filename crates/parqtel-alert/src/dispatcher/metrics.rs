use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Prometheus histogram buckets (in seconds) for notification delivery latency.
pub const DURATION_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Debug, Clone, Default)]
struct HistogramData {
    buckets: [u64; DURATION_BUCKETS.len()],
    sum: f64,
    count: u64,
}

#[derive(Debug, Default)]
struct MetricsInner {
    notifications: BTreeMap<(String, String, String), u64>,
    durations: BTreeMap<(String, String), HistogramData>,
}

/// Thread-safe Prometheus metrics for alert routing and notification delivery.
#[derive(Debug, Default)]
pub struct RoutingMetrics {
    inner: Mutex<MetricsInner>,
    active_groups: AtomicUsize,
    dropped_notifications: AtomicU64,
    evicted_groups: AtomicU64,
    suppressed_noise: AtomicU64,
}

impl RoutingMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment `parqtel_alert_notifications_total` for `(receiver, channel, status)`.
    pub fn inc_notification(&self, receiver: &str, channel: &str, status: &str) {
        if let Ok(mut guard) = self.inner.lock() {
            let key = (
                receiver.to_string(),
                channel.to_string(),
                status.to_string(),
            );
            *guard.notifications.entry(key).or_insert(0) += 1;
        }
    }

    /// Record a notification delivery duration observation in seconds.
    pub fn observe_duration(&self, receiver: &str, channel: &str, secs: f64) {
        if let Ok(mut guard) = self.inner.lock() {
            let key = (receiver.to_string(), channel.to_string());
            let hist = guard.durations.entry(key).or_default();
            hist.count += 1;
            hist.sum += secs;
            for (idx, bound) in DURATION_BUCKETS.iter().enumerate() {
                if secs <= *bound {
                    hist.buckets[idx] += 1;
                }
            }
        }
    }

    /// Set the current number of active alert groups.
    pub fn set_active_groups(&self, count: usize) {
        self.active_groups.store(count, Ordering::Relaxed);
    }

    /// Return the current number of active alert groups.
    pub fn active_groups(&self) -> usize {
        self.active_groups.load(Ordering::Relaxed)
    }

    pub fn inc_dropped_notification(&self) {
        self.dropped_notifications.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_evicted_group(&self) {
        self.evicted_groups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_suppressed_noise(&self) {
        self.suppressed_noise.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dropped_notifications(&self) -> u64 {
        self.dropped_notifications.load(Ordering::Relaxed)
    }

    /// Return the recorded notification count for `(receiver, channel, status)`.
    pub fn get_notification_count(&self, receiver: &str, channel: &str, status: &str) -> u64 {
        if let Ok(guard) = self.inner.lock() {
            guard
                .notifications
                .get(&(
                    receiver.to_string(),
                    channel.to_string(),
                    status.to_string(),
                ))
                .copied()
                .unwrap_or(0)
        } else {
            0
        }
    }

    /// Render all routing metrics in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();

        out.push_str(
            "# HELP parqtel_alert_notifications_total Total alert notifications by receiver, channel, and status.\n",
        );
        out.push_str("# TYPE parqtel_alert_notifications_total counter\n");

        if let Ok(guard) = self.inner.lock() {
            for ((receiver, channel, status), count) in &guard.notifications {
                out.push_str(&format!(
                    "parqtel_alert_notifications_total{{receiver=\"{}\",channel=\"{}\",status=\"{}\"}} {}\n",
                    escape_label_value(receiver),
                    escape_label_value(channel),
                    escape_label_value(status),
                    count
                ));
            }

            out.push_str(
                "# HELP parqtel_alert_notification_duration_seconds Notification delivery duration in seconds.\n",
            );
            out.push_str("# TYPE parqtel_alert_notification_duration_seconds histogram\n");

            for ((receiver, channel), hist) in &guard.durations {
                let r = escape_label_value(receiver);
                let c = escape_label_value(channel);
                for (idx, bound) in DURATION_BUCKETS.iter().enumerate() {
                    out.push_str(&format!(
                        "parqtel_alert_notification_duration_seconds_bucket{{receiver=\"{r}\",channel=\"{c}\",le=\"{bound}\"}} {}\n",
                        hist.buckets[idx]
                    ));
                }
                out.push_str(&format!(
                    "parqtel_alert_notification_duration_seconds_bucket{{receiver=\"{r}\",channel=\"{c}\",le=\"+Inf\"}} {}\n",
                    hist.count
                ));
                out.push_str(&format!(
                    "parqtel_alert_notification_duration_seconds_sum{{receiver=\"{r}\",channel=\"{c}\"}} {}\n",
                    hist.sum
                ));
                out.push_str(&format!(
                    "parqtel_alert_notification_duration_seconds_count{{receiver=\"{r}\",channel=\"{c}\"}} {}\n",
                    hist.count
                ));
            }
        }

        out.push_str("# HELP parqtel_alert_dropped_notifications_total Notification batches dropped because a receiver queue was full.\n");
        out.push_str("# TYPE parqtel_alert_dropped_notifications_total counter\n");
        out.push_str(&format!(
            "parqtel_alert_dropped_notifications_total {}\n",
            self.dropped_notifications.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP parqtel_alert_evicted_groups_total Groups evicted to respect max_active_groups.\n");
        out.push_str("# TYPE parqtel_alert_evicted_groups_total counter\n");
        out.push_str(&format!(
            "parqtel_alert_evicted_groups_total {}\n",
            self.evicted_groups.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP parqtel_alert_noise_suppressed_total Notifications suppressed by the noise policy.\n");
        out.push_str("# TYPE parqtel_alert_noise_suppressed_total counter\n");
        out.push_str(&format!(
            "parqtel_alert_noise_suppressed_total {}\n",
            self.suppressed_noise.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP parqtel_alert_active_groups Current number of active alert groups.\n");
        out.push_str("# TYPE parqtel_alert_active_groups gauge\n");
        out.push_str(&format!(
            "parqtel_alert_active_groups {}\n",
            self.active_groups()
        ));

        out
    }
}

fn escape_label_value(val: &str) -> String {
    let mut escaped = String::with_capacity(val.len());
    for ch in val.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_notification_counters_and_active_groups() {
        let metrics = RoutingMetrics::new();
        assert_eq!(metrics.active_groups(), 0);
        assert_eq!(metrics.get_notification_count("ops", "slack", "success"), 0);

        metrics.inc_notification("ops", "slack", "success");
        metrics.inc_notification("ops", "slack", "success");
        metrics.inc_notification("ops", "slack", "failure");
        metrics.inc_notification("none", "none", "suppressed_silence");
        metrics.inc_notification("none", "none", "suppressed_inhibit");
        metrics.set_active_groups(7);

        assert_eq!(metrics.get_notification_count("ops", "slack", "success"), 2);
        assert_eq!(metrics.get_notification_count("ops", "slack", "failure"), 1);
        assert_eq!(
            metrics.get_notification_count("none", "none", "suppressed_silence"),
            1
        );
        assert_eq!(
            metrics.get_notification_count("none", "none", "suppressed_inhibit"),
            1
        );
        assert_eq!(metrics.active_groups(), 7);
    }

    #[test]
    fn test_loss_and_noise_counters_render() {
        let metrics = RoutingMetrics::new();
        metrics.inc_dropped_notification();
        metrics.inc_dropped_notification();
        metrics.inc_evicted_group();
        metrics.inc_suppressed_noise();
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("parqtel_alert_dropped_notifications_total 2"));
        assert!(rendered.contains("parqtel_alert_evicted_groups_total 1"));
        assert!(rendered.contains("parqtel_alert_noise_suppressed_total 1"));
    }

    #[test]
    fn test_escape_label_value_backslash() {
        assert_eq!(escape_label_value(r"a\b"), r"a\\b");
        assert_eq!(escape_label_value("q\"x\ny"), "q\\\"x\\ny");
        assert_eq!(escape_label_value("plain"), "plain");
    }

    #[test]
    fn test_poisoned_lock_falls_back_to_zero() {
        let metrics = RoutingMetrics::new();
        metrics.inc_notification("ops", "slack", "success");
        // Poison the inner mutex so the `else` fallbacks run.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let guard = metrics.inner.lock();
            assert!(guard.is_ok());
            panic!("poison the metrics lock");
        }));
        assert_eq!(metrics.get_notification_count("ops", "slack", "success"), 0);
        // Inc/observe/render become no-ops but must not panic.
        metrics.inc_notification("ops", "slack", "success");
        metrics.observe_duration("ops", "slack", 1.0);
        assert!(!metrics
            .render_prometheus()
            .contains("parqtel_alert_notifications_total{"));
    }

    #[test]
    fn test_observe_duration_and_render_prometheus() {
        let metrics = RoutingMetrics::new();
        metrics.inc_notification("pd\"1", "pager\nduty", "success");
        metrics.observe_duration("pd\"1", "pager\nduty", 0.02);
        metrics.observe_duration("pd\"1", "pager\nduty", 15.0);
        metrics.set_active_groups(3);

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains(
            "parqtel_alert_notifications_total{receiver=\"pd\\\"1\",channel=\"pager\\nduty\",status=\"success\"} 1"
        ));
        assert!(rendered.contains(
            "parqtel_alert_notification_duration_seconds_bucket{receiver=\"pd\\\"1\",channel=\"pager\\nduty\",le=\"0.01\"} 0"
        ));
        assert!(rendered.contains(
            "parqtel_alert_notification_duration_seconds_bucket{receiver=\"pd\\\"1\",channel=\"pager\\nduty\",le=\"0.025\"} 1"
        ));
        assert!(rendered.contains(
            "parqtel_alert_notification_duration_seconds_bucket{receiver=\"pd\\\"1\",channel=\"pager\\nduty\",le=\"10\"} 1"
        ));
        assert!(rendered.contains(
            "parqtel_alert_notification_duration_seconds_bucket{receiver=\"pd\\\"1\",channel=\"pager\\nduty\",le=\"+Inf\"} 2"
        ));
        assert!(rendered.contains(
            "parqtel_alert_notification_duration_seconds_sum{receiver=\"pd\\\"1\",channel=\"pager\\nduty\"} 15.02"
        ));
        assert!(rendered.contains(
            "parqtel_alert_notification_duration_seconds_count{receiver=\"pd\\\"1\",channel=\"pager\\nduty\"} 2"
        ));
        assert!(rendered.contains("parqtel_alert_active_groups 3"));
    }
}
