//! Background tasks started by the server and the future that ends it.

use crate::otel_sli;
use crate::state::AppState;
use parqtel_core::{BlockIndex, BlockIndexStore, MemoryBuffer};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

/// Applies the block metadata a flush publishes to an in-memory index.
///
/// The index is updated under its write lock, but only in memory: the lock must
/// never be held across serialising and writing the whole index. A block whose WAL
/// has not been committed yet (`durable` is set) must be in the sidecar *before*
/// the flush proceeds, or a crash in between would leave the block on disk, absent
/// from the index and already marked recovered; so the sidecar is written now
/// instead of waiting for the debounce, and the flush is then told it may go on.
///
/// The task ends when every sender is gone.
pub(crate) fn spawn_index_task(
    mut rx: mpsc::UnboundedReceiver<parqtel_core::storage::PendingIndex>,
    index: Arc<RwLock<BlockIndex>>,
    store: Arc<BlockIndexStore>,
    contention: Arc<parqtel_core::ContentionMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(pending) = rx.recv().await {
            let started = Instant::now();
            {
                let mut idx = index.write().await;
                contention.record_index_lock_wait(started.elapsed());
                idx.add(pending.meta);
            }
            store.mark_dirty();
            if pending.durable.is_some() {
                let _ = parqtel_core::storage::persist_once(&index, &store).await;
            }
            if let Some(ack) = pending.durable {
                let _ = ack.send(());
            }
        }
    })
}

/// Feeds the metrics derived from server spans (request, error and duration
/// series) into the ordinary metrics ingestion path.
pub(crate) fn spawn_span_metrics_consumer(
    state: AppState,
    mut rx: mpsc::UnboundedReceiver<Vec<parqtel_core::Metric>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(metrics) = rx.recv().await {
            if let Err(e) = state.inner.ingestion_service.ingest_metrics(metrics).await {
                tracing::warn!("span-metrics ingestion failed: {e}");
            }
        }
    })
}

/// Asks each ingestion service whether its block is due, on a fixed interval,
/// and publishes the saturation gauges (buffer occupancy, RSS) so memory
/// pressure is visible *before* the OOM killer arrives.
///
/// The tick only *asks* each rotator whether its block duration has elapsed, so
/// the interval bounds how late a duration-triggered flush can be, not how often
/// blocks are written.
pub(crate) fn spawn_flush_task(state: AppState, interval_secs: u64) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            interval.tick().await;

            let buffer = state.inner.query_executor.memory_buffer();
            let (before_metrics, before_logs, before_spans) = buffer.stats().await;

            let started = Instant::now();
            let outcome = state.inner.ingestion_service.check_and_flush().await;
            record_flush("metrics", started, outcome, before_metrics, &buffer, |s| {
                s.0
            })
            .await;

            let started = Instant::now();
            let outcome = state.inner.log_ingestion_service.check_and_flush().await;
            record_flush("logs", started, outcome, before_logs, &buffer, |s| s.1).await;

            let started = Instant::now();
            let outcome = state.inner.trace_ingestion_service.check_and_flush().await;
            record_flush("traces", started, outcome, before_spans, &buffer, |s| s.2).await;

            let (metrics, logs, spans) = buffer.stats().await;
            otel_sli::record_gauges(metrics as u64, logs as u64, spans as u64);
            // Sampled here rather than in the /metrics handler: reading
            // /proc/self/status is a blocking syscall and RSS changes far more
            // slowly than a Prometheus scrape interval.
            state.inner.metrics.refresh_process_memory();
        }
    })
}

/// Records the outcome of one flush attempt: its duration and, when a block was
/// written, how many buffered rows it drained (`pick` selects this signal's count
/// from the buffer's `(metrics, logs, spans)` statistics).
async fn record_flush<E: std::fmt::Display>(
    signal: &'static str,
    started: Instant,
    outcome: Result<bool, E>,
    buffered_before: usize,
    buffer: &MemoryBuffer,
    pick: fn((usize, usize, usize)) -> usize,
) {
    match outcome {
        Ok(flushed) => {
            otel_sli::record_flush(signal, started.elapsed().as_secs_f64(), Ok(flushed));
            if flushed {
                let drained = buffered_before.saturating_sub(pick(buffer.stats().await));
                otel_sli::record_flush_rows(signal, drained as u64);
                tracing::debug!(
                    signal,
                    drained_rows = drained,
                    duration_ms = started.elapsed().as_millis(),
                    "buffer flushed to parquet block"
                );
            }
        }
        Err(e) => {
            otel_sli::record_flush(signal, started.elapsed().as_secs_f64(), Err(()));
            tracing::error!(signal, error = %e, "buffer flush failed");
        }
    }
}

/// Evaluates every enabled alert rule on a fixed interval against the same
/// lookback window instant queries use, so an alert sees what an operator would
/// see in the console.
pub(crate) fn spawn_alert_eval_task(state: AppState, interval_secs: u64) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
        loop {
            interval.tick().await;
            let rules = state.inner.alert_registry.list_enabled().await;
            for rule in rules {
                let parsed = parqtel_query::parse_query(&rule.query);
                let (
                    metric_name,
                    matchers,
                    aggregation,
                    quantile,
                    topk_n,
                    group_by,
                    group_without,
                    label_replace,
                    scalar_param,
                    clamp,
                    range_ns,
                ) = match parsed {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                // Same lookback instant queries use, so an alert sees exactly
                // the window an operator would see in the UI.
                let start_ns = now_ns.saturating_sub(state.inner.config.query.lookback_delta_ns);
                let plan = parqtel_query::QueryPlan::new_full(
                    metric_name,
                    matchers,
                    start_ns,
                    now_ns,
                    None,
                    100,
                    1000,
                    aggregation,
                    quantile,
                    topk_n,
                    group_by,
                    group_without,
                    label_replace,
                    scalar_param,
                    clamp,
                    range_ns,
                );
                let plan = match plan {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if let Ok(qr) = state.inner.query_executor.execute(plan).await {
                    for series in &qr.series {
                        if let Some(sample) = series.samples.last() {
                            let labels = series
                                .labels
                                .iter()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();
                            state
                                .inner
                                .alert_engine
                                .evaluate_rule_with_value(&rule, sample.value, labels)
                                .await;
                        }
                    }
                }
            }
        }
    })
}

/// Activates the built-in alert packs selected by `[alerts.presets]` (`off`,
/// `auto` or `all`).
///
/// Runs *before* the rules-directory pass so a file rule with the same id
/// overrides the built-in, and every insert is insert-if-absent so a rule the
/// user disabled or deleted through the API is never re-inserted (activation is
/// one-shot per pack per process).
///
/// In `auto` mode, packs whose canary metrics have not arrived yet are retried
/// every five minutes until the pending set drains; the returned task then ends
/// for good, because activations never revert.
pub(crate) async fn activate_alert_presets(state: &AppState) -> Option<JoinHandle<()>> {
    let presets = &state.inner.config.alerts.presets;
    if presets.mode == parqtel_core::config::PresetMode::Off {
        return None;
    }
    let metrics = state.inner.query_executor.list_metrics().await;
    let pending =
        parqtel_alert::builtin::activate_initial(presets, &metrics, &state.inner.alert_registry)
            .await;
    if pending.is_empty() {
        return None;
    }
    tracing::debug!(
        pending = pending.len(),
        "built-in alert packs awaiting canary metrics"
    );
    let state = state.clone();
    Some(tokio::spawn(async move {
        let mut pending = pending;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        interval.tick().await; // the startup pass already ran; wait a full interval
        while !pending.is_empty() {
            interval.tick().await;
            let metrics = state.inner.query_executor.list_metrics().await;
            pending = parqtel_alert::builtin::activate_detected(
                pending,
                &metrics,
                &state.inner.alert_registry,
            )
            .await;
        }
        tracing::debug!("all selected built-in alert packs resolved");
    }))
}

/// Loads alert rules from the configured directory into the registry. The
/// evaluator and router both key off the registry; without this, rules would only
/// exist when created through the API.
pub(crate) async fn load_alert_rules(state: &AppState) {
    let rules_dir = std::path::PathBuf::from(&state.inner.config.alerts.rules_dir);
    match parqtel_alert::rule::yaml::load_rules_dir(&rules_dir) {
        Ok(rules) => {
            let count = rules.len();
            for rule in rules {
                state.inner.alert_registry.insert(rule).await;
            }
            if count > 0 {
                tracing::info!(dir = %rules_dir.display(), rules = count, "alert rules loaded");
            } else {
                tracing::debug!(dir = %rules_dir.display(), "no alert rules found in directory");
            }
        }
        Err(e) => {
            tracing::warn!(dir = %rules_dir.display(), error = %e, "failed to load alert rules")
        }
    }
}

/// Resolves on SIGINT (ctrl-c) or SIGTERM (docker stop, pod termination, process
/// managers). Either signal must reach the flush-before-exit path, or all
/// buffered telemetry is lost.
pub(crate) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = term.recv() => {},
                }
            }
            Err(_) => {
                tokio::signal::ctrl_c().await.unwrap_or_default();
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.unwrap_or_default();
    }
}
