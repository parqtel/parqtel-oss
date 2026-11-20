//! Background tasks started by the server and the future that ends it.

use crate::otel_sli;
use crate::state::AppState;
use parqtel_alert::{AlertEvidence, RcaSummary, MAX_EVIDENCE_POINTS};
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
/// Pushes local block metadata into the shared manifest and adopts blocks
/// other nodes published. Local indexes remain authoritative for this node;
/// the manifest is the cross-node catalog.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_catalog_sync_task(
    catalog: Arc<parqtel_core::storage::ObjectCatalog>,
    indexes: [Arc<RwLock<BlockIndex>>; 3],
    stores: [Arc<BlockIndexStore>; 3],
    dirs: [std::path::PathBuf; 3],
    interval_secs: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let mut local = Vec::new();
            let mut local_keys = std::collections::HashSet::new();
            for index in &indexes {
                let idx = index.read().await;
                for block in &idx.blocks {
                    // A block adopted from another node lives only in this
                    // node's index until its cache is materialised. Publishing
                    // it as our own would resurrect entries the owner has
                    // already retired.
                    if !block.path.exists() {
                        continue;
                    }
                    let Ok(mut catalog_block) =
                        parqtel_core::storage::CatalogBlock::from_metadata(block)
                    else {
                        continue;
                    };
                    catalog_block.writer = std::env::var("HOSTNAME").ok();
                    local_keys.insert(catalog_block.key.clone());
                    local.push(catalog_block);
                }
            }
            let lease = std::env::var("HOSTNAME").ok();
            if let Err(e) = catalog.publish(local, lease).await {
                tracing::warn!(error = %e, "catalog publish failed");
                continue;
            }
            let remote = match catalog.read().await {
                Ok(Some((manifest, _))) => manifest,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(error = %e, "catalog read failed");
                    continue;
                }
            };
            for block in remote.blocks {
                if local_keys.contains(&block.key) {
                    continue;
                }
                let signal = block.signal_type.index();
                let mut idx = indexes[signal].write().await;
                let path = dirs[signal].join(&block.key);
                if idx.blocks.iter().any(|existing| existing.path == path) {
                    continue;
                }
                idx.add(block.to_metadata(&dirs[signal]));
                stores[signal].mark_dirty();
                tracing::info!(key = %block.key, "adopted remote block from catalog");
            }
        }
    })
}

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
/// Median + MAD anomaly decision. `None` means the baseline is too short to
/// decide; a flat baseline uses a relative floor so a one-off spike still
/// registers without turning tiny float noise into pages.
fn mad_stats(baseline: &[f64]) -> Option<(f64, f64)> {
    if baseline.len() < 5 {
        return None;
    }
    let mut sorted = baseline.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];
    let mut deviations: Vec<f64> = baseline
        .iter()
        .map(|value| (value - median).abs())
        .collect();
    deviations.sort_by(f64::total_cmp);
    let mad = deviations[deviations.len() / 2];
    Some((median, mad))
}

fn anomaly_condition(baseline: &[f64], current: f64, sensitivity: f64) -> Option<bool> {
    let (median, mad) = mad_stats(baseline)?;
    let scale = mad.max(1e-9).max(median.abs() * 0.001);
    Some((current - median).abs() > sensitivity.max(0.1) * scale)
}

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
                let anomaly = match &rule.source {
                    Some(parqtel_alert::RuleSource::LearnedAnomaly(source)) => Some(source.clone()),
                    _ => None,
                };
                // A threshold rule sees the instant lookback; an anomaly rule
                // needs a baseline range, so it scans its configured window.
                let (start_ns, step_ns) = match &anomaly {
                    Some(source) => (
                        now_ns.saturating_sub(
                            (source.baseline_window_days.max(1) as i64) * 86_400 * 1_000_000_000,
                        ),
                        Some(60_000_000_000),
                    ),
                    None => (
                        now_ns.saturating_sub(state.inner.config.query.lookback_delta_ns),
                        None,
                    ),
                };
                let plan = parqtel_query::QueryPlan::new_full(
                    metric_name,
                    matchers,
                    start_ns,
                    now_ns,
                    step_ns,
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
                let Some(_scan_slot) = parqtel_core::Scanner::try_reserve_background_scan_slot()
                else {
                    tracing::debug!(rule_id = %rule.id, "skipping alert evaluation while its scan slot is busy");
                    continue;
                };
                if let Ok(qr) = state.inner.query_executor.execute(plan).await {
                    for series in &qr.series {
                        let Some(sample) = series.samples.last() else {
                            continue;
                        };
                        let labels: std::collections::BTreeMap<String, String> = series
                            .labels
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        let points = AlertEvidence::bounded_points(
                            series
                                .samples
                                .iter()
                                .map(|sample| (sample.timestamp_ns, sample.value)),
                            MAX_EVIDENCE_POINTS,
                        );
                        if let Some(source) = &anomaly {
                            let baseline: Vec<f64> = series
                                .samples
                                .iter()
                                .take(series.samples.len().saturating_sub(1))
                                .map(|sample| sample.value)
                                .collect();
                            let Some(condition_met) =
                                anomaly_condition(&baseline, sample.value, source.sensitivity)
                            else {
                                continue;
                            };
                            let (median, mad) = mad_stats(&baseline).unwrap_or((0.0, 0.0));
                            let evidence = AlertEvidence {
                                query: rule.query.clone(),
                                signal: rule.signal.clone(),
                                window_start_ns: start_ns,
                                window_end_ns: now_ns,
                                operator: None,
                                threshold: None,
                                observed: sample.value,
                                points: points.clone(),
                                series_count: qr.series.len(),
                            };
                            let rca = RcaSummary::mad(
                                &rule.name,
                                sample.value,
                                median,
                                mad,
                                source.sensitivity,
                                &points,
                            );
                            state
                                .inner
                                .alert_engine
                                .evaluate_rule_with_evidence(
                                    &rule,
                                    sample.value,
                                    labels,
                                    condition_met,
                                    Some(evidence),
                                    Some(rca),
                                )
                                .await;
                        } else {
                            let condition_met = rule.condition.evaluate(sample.value);
                            let evidence = AlertEvidence {
                                query: rule.query.clone(),
                                signal: rule.signal.clone(),
                                window_start_ns: start_ns,
                                window_end_ns: now_ns,
                                operator: Some(rule.condition.operator.as_str().to_string()),
                                threshold: Some(rule.condition.value),
                                observed: sample.value,
                                points: points.clone(),
                                series_count: qr.series.len(),
                            };
                            let rca = RcaSummary::threshold(
                                &rule.name,
                                sample.value,
                                rule.condition.operator.as_str(),
                                rule.condition.value,
                                rule.condition.for_duration_secs,
                                &points,
                            );
                            state
                                .inner
                                .alert_engine
                                .evaluate_rule_with_evidence(
                                    &rule,
                                    sample.value,
                                    labels,
                                    condition_met,
                                    Some(evidence),
                                    Some(rca),
                                )
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

#[cfg(test)]
mod anomaly_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::anomaly_condition;

    #[test]
    fn flat_baseline_does_not_fire() {
        let baseline = vec![1.0; 10];
        assert_eq!(anomaly_condition(&baseline, 1.0, 3.0), Some(false));
    }

    #[test]
    fn spike_fires_and_short_baseline_is_unknown() {
        let baseline = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        assert_eq!(anomaly_condition(&baseline, 50.0, 3.0), Some(true));
        assert_eq!(anomaly_condition(&[1.0, 2.0], 3.0, 3.0), None);
    }
}
