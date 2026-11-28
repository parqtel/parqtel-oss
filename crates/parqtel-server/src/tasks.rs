//! Background tasks started by the server and the future that ends it.

use crate::otel_sli;
use crate::state::AppState;
use parqtel_alert::{AlertEvidence, RcaSummary, MAX_EVIDENCE_POINTS};
use parqtel_core::{BlockIndex, BlockIndexStore, MemoryBuffer};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

/// Per-tick catalog decisions, computed from a snapshot so they can be applied
/// without holding structure locks across the catalog await.
#[derive(Debug, Default, PartialEq, Eq)]
struct SyncPlan {
    /// Keys my locally staged blocks that the manifest should contain.
    publish: Vec<String>,
    /// Keys this node closed earlier and that must leave the manifest.
    retire: std::collections::HashSet<String>,
    /// Local paths to drop from the index and from the bucket.
    prune_local: Vec<std::path::PathBuf>,
    /// Adopted cache paths to drop from the index.
    prune_adopted: Vec<String>,
}

/// Decides one catalog tick from a snapshot of this node's indexes.
///
/// `local` entries are `(path, locally produced, present in the bucket)` for
/// every block in the index. `published` is the set of keys this task published
/// earlier; an entry in it that disappeared from the manifest was retired or
/// expired by the compactor and must never be republished. Only the compactor
/// retires: adopted keys it closed by merging or retention are keys it no longer
/// lists in its index. The rule is deliberately limited to adopted keys, so a
/// cold compactor with an empty index cannot wipe the manifest. When the
/// manifest itself is absent (`manifest_known == false`) no absence-based
/// pruning happens: a missing catalog object is not evidence of retirement.
///
/// A full multi-node end-to-end test in one process is impossible because the
/// block store is a process-global `OnceLock`; this function covers the decision
/// logic and the callers are exercised by the sharded integration setup.
fn plan_sync(
    local: &[(std::path::PathBuf, bool, bool)],
    manifest_keys: &std::collections::HashSet<String>,
    published: &std::collections::HashSet<String>,
    adopted: &std::collections::HashSet<String>,
    role_is_compactor: bool,
    manifest_known: bool,
) -> SyncPlan {
    let mut plan = SyncPlan::default();
    let mut local_keys = std::collections::HashSet::new();
    for (path, locally_produced, in_bucket) in local {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        local_keys.insert(name.to_string());
        // A cache copy of a foreign block is neither ours to publish nor ours
        // to prune: its presence is driven by the manifest adoption path.
        if !locally_produced || adopted.contains(name) {
            continue;
        }
        if published.contains(name) {
            if !manifest_known || manifest_keys.contains(name) {
                plan.publish.push(name.to_string());
            } else {
                // The compactor retired this block after we flushed it. Drop
                // the staged copy instead of bringing the key back.
                plan.prune_local.push(path.clone());
            }
        } else if *in_bucket {
            plan.publish.push(name.to_string());
        } else {
            // The block was deleted while this node was down; publishing it
            // would resurrect a key compaction already accounted for.
            plan.prune_local.push(path.clone());
        }
    }
    // An absent manifest is not evidence that every key was retired; never
    // prune from it. A compactor also has nothing to retire in a manifest that
    // does not exist.
    if !manifest_known {
        return plan;
    }
    if role_is_compactor {
        for key in adopted {
            if !local_keys.contains(key) {
                plan.retire.insert(key.clone());
            }
        }
    }
    for key in adopted {
        if !manifest_keys.contains(key) {
            plan.prune_adopted.push(key.clone());
        }
    }
    plan
}

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
    node_id: Option<String>,
    publish_local: bool,
    ownership: Arc<parqtel_core::OwnershipRegistry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Keys adopted from the manifest, remembered across ticks. A cache
        // materialisation can make `path.exists()` true for a foreign block;
        // this set keeps it from ever being published under our node id.
        let mut adopted_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Keys this task published under our writer lease, remembered so a key
        // the compactor retired while we were down is never republished.
        let mut published_keys: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        loop {
            ticker.tick().await;
            // Read the manifest before publishing so a block the catalog
            // already assigns to another writer is never retagged as ours.
            let remote = match catalog.read().await {
                Ok(Some((manifest, _))) => Some(manifest),
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!(error = %e, "catalog read failed");
                    continue;
                }
            };
            let manifest_known = remote.is_some();
            let (remote_blocks, manifest_keys, foreign_keys) = match remote {
                Some(manifest) => {
                    let keys = manifest
                        .blocks
                        .iter()
                        .map(|block| block.key.clone())
                        .collect::<std::collections::HashSet<_>>();
                    let foreign = manifest
                        .blocks
                        .iter()
                        .filter(|block| block.writer.is_some() && block.writer != node_id)
                        .map(|block| block.key.clone())
                        .collect::<std::collections::HashSet<_>>();
                    (manifest.blocks, keys, foreign)
                }
                None => (
                    Vec::new(),
                    std::collections::HashSet::new(),
                    std::collections::HashSet::new(),
                ),
            };

            // Snapshot the indexes. Every lock is dropped before any await on
            // the catalog; only the HEAD batch below runs elsewhere.
            let mut local_entries: Vec<(std::path::PathBuf, bool, bool)> = Vec::new();
            let mut blocks_by_key: std::collections::HashMap<
                String,
                parqtel_core::storage::CatalogBlock,
            > = std::collections::HashMap::new();
            let mut index_keys = std::collections::HashSet::new();
            let mut path_signal: std::collections::HashMap<std::path::PathBuf, usize> =
                std::collections::HashMap::new();
            let mut path_by_key: std::collections::HashMap<String, (usize, std::path::PathBuf)> =
                std::collections::HashMap::new();
            let mut head_requests: Vec<(usize, std::path::PathBuf)> = Vec::new();
            for (signal, index) in indexes.iter().enumerate() {
                let idx = index.read().await;
                for block in &idx.blocks {
                    let Some(name) = block.path.file_name().and_then(|name| name.to_str()) else {
                        continue;
                    };
                    let name = name.to_string();
                    index_keys.insert(name.clone());
                    path_signal.insert(block.path.clone(), signal);
                    path_by_key
                        .entry(name.clone())
                        .or_insert_with(|| (signal, block.path.clone()));
                    // Ask the installed store, not the filesystem: only the
                    // backend knows whether `path` is a locally staged block
                    // or a read-cache artefact of an adopted one.
                    let locally_produced = parqtel_core::storage::block_store::store()
                        .is_locally_produced(&block.path)
                        && !adopted_keys.contains(&name);
                    let slot = local_entries.len();
                    local_entries.push((block.path.clone(), locally_produced, false));
                    if locally_produced && !published_keys.contains(&name) {
                        head_requests.push((slot, block.path.clone()));
                    }
                    if !locally_produced || foreign_keys.contains(&name) {
                        continue;
                    }
                    let Ok(mut catalog_block) =
                        parqtel_core::storage::CatalogBlock::from_metadata(block)
                    else {
                        continue;
                    };
                    catalog_block.writer = node_id.clone();
                    blocks_by_key.insert(name, catalog_block);
                }
            }
            // Retired/compacted sources leave the index; forgetting them here
            // keeps the remembered set bounded. Their manifest entries are
            // still withdrawn by the publish filter because they are absent
            // from the local keys of this tick.
            published_keys.retain(|key| index_keys.contains(key));
            // A block flushed before this node restarted is republished only
            // if its object still exists; `head` catches a compaction or
            // retention delete that happened while the node was down. New
            // flushes publish before this tick, so they still ship.
            if !head_requests.is_empty() {
                let slots: Vec<usize> = head_requests.iter().map(|(slot, _)| *slot).collect();
                let paths: Vec<std::path::PathBuf> =
                    head_requests.iter().map(|(_, path)| path.clone()).collect();
                let count = paths.len();
                let in_bucket = tokio::task::spawn_blocking(move || {
                    paths
                        .iter()
                        .map(|path| {
                            parqtel_core::storage::block_store::store()
                                .head(path)
                                .ok()
                                .flatten()
                                .is_some()
                        })
                        .collect::<Vec<_>>()
                })
                .await
                .unwrap_or_else(|e| {
                    // Do not delete blocks because the probe failed; shipping
                    // them is recoverable, deleting live data is not.
                    tracing::warn!(error = %e, "block head probe failed");
                    vec![true; count]
                });
                for (slot, present) in slots.into_iter().zip(in_bucket) {
                    local_entries[slot].2 = present;
                }
            }

            let mut plan = plan_sync(
                &local_entries,
                &manifest_keys,
                &published_keys,
                &adopted_keys,
                ownership.is_compactor(),
                manifest_known,
            );
            // A block whose metadata could not be rebuilt, or that another
            // writer owns, is not ours to publish.
            plan.publish.retain(|key| blocks_by_key.contains_key(key));

            // Prune retired local blocks and vanished adopted caches under the
            // index write locks; delete the files afterwards on the blocking
            // pool so no lock spans I/O.
            let mut doomed: Vec<std::path::PathBuf> = Vec::new();
            for path in &plan.prune_local {
                if let Some(&signal) = path_signal.get(path) {
                    let mut idx = indexes[signal].write().await;
                    idx.remove(path);
                    stores[signal].mark_dirty();
                }
                if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                    published_keys.remove(name);
                }
                doomed.push(path.clone());
            }
            for key in &plan.prune_adopted {
                adopted_keys.remove(key);
                if let Some(pair) = path_by_key.get(key) {
                    let (signal, path) = pair;
                    let mut idx = indexes[*signal].write().await;
                    idx.remove(path);
                    stores[*signal].mark_dirty();
                    doomed.push(path.clone());
                }
            }
            if !doomed.is_empty() {
                let _ = tokio::task::spawn_blocking(move || {
                    for path in doomed {
                        let _ = parqtel_core::storage::block_store::store().delete(&path);
                    }
                })
                .await;
            }

            // A reader must not claim blocks: the manifest's writer lease is
            // what tells the owner its blocks are still live.
            if publish_local && (!plan.publish.is_empty() || !plan.retire.is_empty()) {
                let blocks: Vec<parqtel_core::storage::CatalogBlock> = plan
                    .publish
                    .iter()
                    .filter_map(|key| blocks_by_key.get(key).cloned())
                    .collect();
                match catalog
                    .publish_with_retirements(blocks, node_id.clone(), &plan.retire)
                    .await
                {
                    Ok(_) => {
                        for key in &plan.publish {
                            published_keys.insert(key.clone());
                            ownership.own(key);
                        }
                        for key in &plan.retire {
                            adopted_keys.remove(key);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "catalog publish failed");
                        continue;
                    }
                }
            }

            // Keys pruned this tick must not be adopted right back from the
            // manifest snapshot taken before the retirement was published.
            let mut remaining_keys = index_keys;
            for path in &plan.prune_local {
                if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                    remaining_keys.remove(name);
                }
            }
            for key in &plan.prune_adopted {
                remaining_keys.remove(key);
            }

            let local_node = node_id.as_deref().unwrap_or_default();
            for block in remote_blocks {
                // Compaction and retention consult this registry before
                // retiring a block; a key another writer owns must survive
                // this node's sweep.
                ownership.adopt(&block.key, block.writer.as_deref(), local_node);
                if remaining_keys.contains(&block.key) || plan.retire.contains(&block.key) {
                    continue;
                }
                let signal = block.signal_type.index();
                let mut idx = indexes[signal].write().await;
                let path = dirs[signal].join(&block.key);
                if idx.blocks.iter().any(|existing| existing.path == path) {
                    continue;
                }
                idx.add(block.to_metadata(&dirs[signal]));
                adopted_keys.insert(block.key.clone());
                stores[signal].mark_dirty();
                tracing::info!(key = %block.key, "adopted remote block from catalog");
            }
        }
    })
}

/// Applies the block metadata a flush publishes to an in-memory index.
///
/// The index is updated under its write lock, but only in memory: the lock must
/// never be held across serialising and writing the whole index. A block whose WAL
/// has not been committed yet (`durable` is set) must be in the sidecar *before*
/// the flush proceeds, or a crash in between would leave the block on disk, absent
/// from the index and already marked recovered; so the sidecar is written now
/// instead of waiting for the debounce, and the flush is then told it may go on.
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

/// Catalog-tick decisions, tested without a live block store: the store is a
/// process-global `OnceLock`, so a full multi-node end-to-end scenario cannot
/// run inside one test process. These cover the writer, compactor and reader
/// rules that decide what each tick publishes, retires and prunes.
#[cfg(test)]
mod catalog_sync_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::plan_sync;
    use std::collections::HashSet;
    use std::path::PathBuf;

    fn key_set(keys: &[&str]) -> HashSet<String> {
        keys.iter().map(|key| (*key).to_string()).collect()
    }

    fn entry(name: &str, locally_produced: bool, in_bucket: bool) -> (PathBuf, bool, bool) {
        (
            PathBuf::from(format!("/data/blocks/{name}")),
            locally_produced,
            in_bucket,
        )
    }

    #[test]
    fn writer_publishes_new_blocks_and_prunes_blocks_missing_from_the_bucket() {
        let manifest = key_set(&["fresh.pq"]);
        let published = HashSet::new();
        let adopted = HashSet::new();
        let local = vec![
            entry("fresh.pq", true, true),
            entry("deleted-while-down.pq", true, false),
        ];
        let plan = plan_sync(&local, &manifest, &published, &adopted, false, true);
        assert_eq!(plan.publish, vec!["fresh.pq".to_string()]);
        assert_eq!(
            plan.prune_local,
            vec![PathBuf::from("/data/blocks/deleted-while-down.pq")]
        );
        assert!(plan.retire.is_empty());
        assert!(plan.prune_adopted.is_empty());
    }

    #[test]
    fn writer_republishes_a_known_block_but_never_resurrects_a_retired_one() {
        let local = vec![
            entry("keep.pq", true, true),
            entry("retired.pq", true, true),
        ];
        let published = key_set(&["keep.pq", "retired.pq"]);

        let live = plan_sync(
            &local,
            &key_set(&["keep.pq", "retired.pq"]),
            &published,
            &HashSet::new(),
            false,
            true,
        );
        assert_eq!(
            live.publish,
            vec!["keep.pq".to_string(), "retired.pq".to_string()]
        );
        assert!(live.prune_local.is_empty());

        // The compactor removed `retired.pq` from the manifest after merging it.
        let after_retirement = plan_sync(
            &local,
            &key_set(&["keep.pq"]),
            &published,
            &HashSet::new(),
            false,
            true,
        );
        assert_eq!(after_retirement.publish, vec!["keep.pq".to_string()]);
        assert_eq!(
            after_retirement.prune_local,
            vec![PathBuf::from("/data/blocks/retired.pq")]
        );
    }

    #[test]
    fn reader_prunes_adopted_keys_that_left_the_manifest() {
        let adopted = key_set(&["gone.pq"]);
        let local = vec![entry("gone.pq", false, true)];
        let plan = plan_sync(
            &local,
            &HashSet::new(),
            &HashSet::new(),
            &adopted,
            false,
            true,
        );
        assert!(plan.publish.is_empty());
        assert!(plan.prune_local.is_empty());
        assert_eq!(plan.prune_adopted, vec!["gone.pq".to_string()]);
        assert!(plan.retire.is_empty());
    }

    #[test]
    fn compactor_retires_only_adopted_keys_it_no_longer_lists() {
        let adopted = key_set(&["src0.pq", "src1.pq"]);
        let local = vec![entry("merged.pq", true, true)];
        let plan = plan_sync(
            &local,
            &key_set(&["src0.pq", "src1.pq", "merged.pq", "unrelated.pq"]),
            &HashSet::new(),
            &adopted,
            true,
            true,
        );
        assert_eq!(plan.publish, vec!["merged.pq".to_string()]);
        assert_eq!(plan.retire, key_set(&["src0.pq", "src1.pq"]));
        assert!(!plan.retire.contains("unrelated.pq"));
        assert!(plan.prune_adopted.is_empty());
    }

    #[test]
    fn cold_compactor_never_retires_keys_it_never_adopted() {
        let plan = plan_sync(
            &[],
            &key_set(&["writer-a.pq", "writer-b.pq"]),
            &HashSet::new(),
            &HashSet::new(),
            true,
            true,
        );
        assert!(plan.retire.is_empty());
        assert!(plan.publish.is_empty());
        assert!(plan.prune_adopted.is_empty());
    }

    #[test]
    fn absent_manifest_does_not_prune_local_or_adopted_keys() {
        let published = key_set(&["mine.pq"]);
        let adopted = key_set(&["theirs.pq"]);
        let local = vec![entry("mine.pq", true, false)];
        let plan = plan_sync(&local, &HashSet::new(), &published, &adopted, false, false);
        assert_eq!(plan.publish, vec!["mine.pq".to_string()]);
        assert!(plan.prune_local.is_empty());
        assert!(plan.prune_adopted.is_empty());
        assert!(plan.retire.is_empty());
    }
}
