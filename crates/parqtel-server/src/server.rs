//! The `serve` subcommand: wires the ingestion services, indexes, query executor
//! and background tasks together, serves HTTP and gRPC, and shuts down in order.

use crate::recovery::{open_wal, reconcile_and_persist, replay_all_wals};
use crate::router::build_router;
use crate::state::AppState;
use crate::tasks::{
    activate_alert_presets, load_alert_rules, shutdown_signal, spawn_alert_eval_task,
    spawn_catalog_sync_task, spawn_flush_task, spawn_index_task, spawn_span_metrics_consumer,
};
use crate::{grpc, telemetry};
use flate2::write::GzEncoder;
use flate2::Compression;
use parqtel_core::{start_maintenance, BlockIndex, BlockIndexStore, Config, RetentionPolicy};
use parqtel_ingest::{IngestionService, LogIngestionService, TraceIngestionService};
use parqtel_pipeline::config::RulerConfig;
use parqtel_pipeline::ruler::{RulerEvaluator, RulerScheduler};
use parqtel_query::QueryExecutor;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc;

const UI_HTML: &str = include_str!("ui.html");

/// The embedded console, gzipped once at startup, with the ETag served for it.
fn ui_assets() -> anyhow::Result<(Vec<u8>, String)> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(UI_HTML.as_bytes())?;
    let content = encoder.finish()?;

    let mut hasher = Sha256::new();
    hasher.update(UI_HTML.as_bytes());
    let etag = format!("\"{}\"", hex::encode(hasher.finalize()));
    Ok((content, etag))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_server(
    config: Config,
    index: Arc<tokio::sync::RwLock<BlockIndex>>,
    index_store: Arc<BlockIndexStore>,
    log_index: Arc<tokio::sync::RwLock<BlockIndex>>,
    log_index_store: Arc<BlockIndexStore>,
    telemetry_guard: telemetry::TelemetryGuard,
) -> anyhow::Result<()> {
    let (ui_content, ui_etag) = ui_assets()?;
    parqtel_core::LabelInterner::set_capacity(config.ingest.label_interner_capacity);

    // Metrics pipeline.
    //
    // `ContentionMetrics` is created once and shared with the three ingestion
    // services, the three block-index tasks and the /metrics renderer, so a
    // lock wait recorded on the ingest path is the same series an operator
    // scrapes. The index writer measures how long it waits for the write lock
    // because every query handler contends for it.
    let contention = Arc::new(parqtel_core::ContentionMetrics::new());
    let (tx, rx) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
    let index_task = spawn_index_task(rx, index.clone(), index_store.clone(), contention.clone());

    // Logs pipeline
    let (log_tx, log_rx) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
    let log_index_task = spawn_index_task(
        log_rx,
        log_index.clone(),
        log_index_store.clone(),
        contention.clone(),
    );

    // Create shared in-memory buffer for stream-queryable data. The writer and
    // WAL hold the same rows, so the bounds govern queryability, not durability.
    let memory_buffer = parqtel_core::MemoryBuffer::with_limits(
        config.ingest.buffer_max_points,
        config.ingest.buffer_max_bytes,
        config.ingest.buffer_overflow_policy,
    );

    // Opened before the service so replay can feed the same path live ingest
    // uses. A WAL that cannot be opened stops startup: continuing would accept
    // data we could not promise to recover.
    let metrics_wal = if config.ingest.wal_enabled {
        match parqtel_core::wal::WalWriter::open_with_segment_limit(
            &config.storage.data_dir,
            "metrics",
            config.ingest.wal_sync_mode,
            std::time::Duration::from_millis(config.ingest.wal_sync_interval_ms.max(1)),
            config.ingest.wal_max_segment_bytes,
        ) {
            Ok(w) => Some(w),
            Err(e) => {
                tracing::error!("Failed to open the metrics WAL: {e}");
                return Err(e.into());
            }
        }
    } else {
        tracing::warn!("metrics WAL is disabled; a crash can lose up to a whole block window");
        None
    };

    let ingestion_service = IngestionService::with_shards(
        config.storage.clone(),
        tx,
        config.ingest.rotator_shards,
        metrics_wal,
    )
    .with_memory_buffer(memory_buffer.clone())
    .with_contention(contention.clone())
    .with_series_limit(config.ingest.series_limit_per_metric);
    // Background flush: a request that crosses the block cap is acknowledged
    // once the WAL has its rows rather than after the Parquet encode. Measured
    // on a 7 MiB batch the encode is ~125 ms of a ~300 ms request, and it
    // serialises concurrent requests behind the flush lock.
    ingestion_service.start_flush_worker(config.ingest.max_inflight_flushes);
    // Report the shard count so an operator can correlate lock-wait behaviour
    // with the configured concurrency. 0 means "not reported", so the gauge is
    // only meaningful once this runs.
    contention.set_ingest_rotator_shards(ingestion_service.rotator_shards());
    tracing::debug!(
        shards = ingestion_service.rotator_shards(),
        "metrics ingest rotator shard count"
    );
    let logs_wal = open_wal(
        &config,
        "logs",
        &config.logs.data_dir,
        config.ingest.log_wal_enabled,
    )?;
    // Recording rules and pipelines live beside the alert rules. Load them at
    // startup and write API-created definitions back into the same directories,
    // so a restart does not silently lose them.
    let rules_root = std::path::PathBuf::from(&config.alerts.rules_dir);
    let recording_dir = rules_root.join("recording");
    let pipeline_dir = rules_root.join("pipelines");
    let pipeline_registry = parqtel_pipeline::rule::registry::RuleRegistry::new()
        .with_dirs(recording_dir.clone(), pipeline_dir.clone());
    for dir in [&recording_dir, &pipeline_dir] {
        if let Err(e) = pipeline_registry.load_dir(dir) {
            tracing::warn!(path = %dir.display(), error = %e, "failed to load rule definitions");
        }
    }
    // Extracted pipeline metrics go through the metrics service, which owns
    // that signal's WAL and buffer. The channel is bounded: if the metric path
    // is slower than log ingest, derived metrics are dropped with a warning
    // rather than growing an unbounded backlog.
    let (pipeline_metric_tx, mut pipeline_metric_rx) =
        mpsc::channel::<Vec<parqtel_core::Metric>>(64);
    let log_ingestion_service =
        LogIngestionService::with_wal(config.logs.clone(), log_tx, logs_wal)
            .with_memory_buffer(memory_buffer.clone())
            .with_contention(contention.clone())
            .with_pipelines(pipeline_registry.clone(), pipeline_metric_tx);
    let (trace_tx, trace_rx) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
    // Span-metrics RED bridge: trace ingestion derives
    // traces_service_{requests,errors,duration_ms} metrics and feeds them
    // back through the normal metrics path.
    let (span_metrics_tx, span_metrics_rx) = mpsc::unbounded_channel::<Vec<parqtel_core::Metric>>();
    // Trace blocks live in the metrics data dir (TraceWriter is built from
    // config.storage), so the trace WAL goes there too.
    let traces_wal = open_wal(
        &config,
        "traces",
        &config.storage.data_dir,
        config.ingest.wal_enabled,
    )?;
    let trace_ingestion_service =
        TraceIngestionService::with_wal(config.storage.clone(), trace_tx, traces_wal)
            .with_memory_buffer(memory_buffer.clone())
            .with_span_metrics(span_metrics_tx)
            .with_tail_sampling(config.ingest.tail_sampling.clone())
            .with_contention(contention.clone());

    // Trace index - uses same data_dir as metrics but separate index file
    let trace_data_dir = config.storage.data_dir.join("traces");
    std::fs::create_dir_all(&trace_data_dir).unwrap_or_default();
    let mut trace_index = BlockIndex::new(&trace_data_dir);
    trace_index.load().unwrap_or_default();
    // One store, shared by the reconcile below, the index task, and the
    // persist loop. It previously existed twice — the reconcile step built one
    // and the index task captured a different one — so a dirty flag set by the
    // task was invisible to the store the reconcile step persisted through.
    let trace_index_store = Arc::new(BlockIndexStore::new(&trace_index));
    reconcile_and_persist(
        &mut trace_index,
        &trace_index_store,
        parqtel_core::SignalType::Traces,
        "traces",
    );
    tracing::debug!(
        blocks = trace_index.blocks.len(),
        "trace block index loaded"
    );
    let trace_index = Arc::new(tokio::sync::RwLock::new(trace_index));

    let trace_index_task = spawn_index_task(
        trace_rx,
        trace_index.clone(),
        trace_index_store.clone(),
        contention.clone(),
    );

    let query_executor = QueryExecutor::with_trace_index(
        index.clone(),
        log_index.clone(),
        trace_index.clone(),
        memory_buffer.clone(),
        trace_data_dir.clone(),
    )
    .with_query_lookback(config.query.lookback_delta_ns)
    .with_result_limits(config.query.max_series, config.query.max_samples_per_series);

    let retention_interval_secs = config.server.retention_interval_secs;
    let persist_interval_secs = config.server.index_persist_interval_secs;
    let metrics_maintenance = start_maintenance(
        index.clone(),
        index_store.clone(),
        config.storage.clone(),
        retention_interval_secs,
        persist_interval_secs,
    );
    let logs_maintenance = start_maintenance(
        log_index.clone(),
        log_index_store.clone(),
        config.logs.clone().into(),
        retention_interval_secs,
        persist_interval_secs,
    );
    // Traces: retention only. Trace blocks share the metrics BlockConfig
    // (TraceWriter is built from config.storage), but the compactor's
    // read_source_blocks only decodes metrics/logs schemas — running full
    // maintenance would attempt trace merges and fail on schema mismatch.
    // Without retention the trace index grows without bound (observed:
    // 62 blocks/700K rows in ~8h on the OOM-affected deployment).
    // Traces run retention only (compaction for traces is a separate decision),
    // which meant they also had **no index persist loop** - `start_maintenance`
    // owns the loop, and it is called for metrics and logs only. The trace
    // sidecar was therefore written on exactly one line of the shutdown
    // sequence, after every flush and every maintenance task. Anything slow
    // earlier in shutdown (a large WAL replay, a slow flush) meant the grace
    // period expired first and the sidecar was never written at all - leaving
    // orphaned trace blocks that reconcile cannot adopt, because reconcile
    // scans the index's own directory while trace blocks are written to its
    // parent.
    //
    // Persist the trace index continuously, like every other signal.
    let (trace_persist_shutdown_tx, trace_persist_shutdown_rx) = tokio::sync::watch::channel(false);
    let trace_persist_task = tokio::spawn(parqtel_core::storage::run_index_persist_loop(
        trace_index.clone(),
        trace_index_store.clone(),
        std::time::Duration::from_secs(config.server.index_persist_interval_secs.max(1)),
        trace_persist_shutdown_rx,
    ));

    let (trace_shutdown_tx, trace_shutdown_rx) = tokio::sync::watch::channel(false);
    let trace_retention = tokio::spawn(RetentionPolicy::run_loop(
        trace_index.clone(),
        trace_index_store.clone(),
        config.storage.clone(),
        retention_interval_secs,
        trace_shutdown_rx,
    ));

    // Replay before the listener binds, so nothing is served until recovered
    // telemetry is back in the write path, and through exactly the entry points
    // live exporters use.
    // Blocks already on disk may cover WAL ranges whose commit never happened
    // (a crash between the two); skip those instead of writing them twice.
    if config.ingest.wal_enabled {
        let data_dir = &config.storage.data_dir;
        parqtel_core::storage::advance_wal_commit(data_dir, "metrics", &*index.read().await);
        parqtel_core::storage::advance_wal_commit(data_dir, "traces", &*trace_index.read().await);
    }
    if config.ingest.log_wal_enabled {
        parqtel_core::storage::advance_wal_commit(
            &config.logs.data_dir,
            "logs",
            &*log_index.read().await,
        );
    }
    let wal_replay = replay_all_wals(
        &config,
        &ingestion_service,
        &log_ingestion_service,
        &trace_ingestion_service,
    )
    .await;

    let state = AppState::new(
        ingestion_service,
        log_ingestion_service,
        trace_ingestion_service,
        pipeline_registry,
        query_executor,
        index.clone(),
        config.clone(),
        ui_content,
        ui_etag,
        contention,
        [
            index_store.clone(),
            log_index_store.clone(),
            trace_index_store.clone(),
        ],
    )
    .await;
    tracing::debug!(
        signals = "metrics, logs, traces",
        "parqtel application state initialized"
    );

    // Shared object-storage catalog: publish our blocks and adopt others'.
    if let Some(catalog) = state.inner.object_catalog.clone() {
        spawn_catalog_sync_task(
            catalog,
            [index.clone(), log_index.clone(), trace_index.clone()],
            [
                index_store.clone(),
                log_index_store.clone(),
                trace_index_store.clone(),
            ],
            [
                config.storage.data_dir.clone(),
                config.logs.data_dir.clone(),
                config.storage.data_dir.join("traces"),
            ],
            config.object_store.catalog_interval_secs,
        );
    }

    // Recording rules: evaluate on their group intervals and write results
    // through the same metrics ingestion service as live OTLP data.
    let ruler_runtime = if config.alerts.recording_rules_enabled {
        let (ruler_tx, mut ruler_rx) = mpsc::channel::<Vec<parqtel_core::Metric>>(64);
        let sink = Arc::new(crate::ruler::ChannelMetricSink::new(ruler_tx));
        let query = Arc::new(crate::ruler::ServerQueryExecutor::new(state.clone()));
        let evaluator = Arc::new(RulerEvaluator::new(query, sink));
        let ruler_config = RulerConfig {
            rules_dir: format!("{}/recording", config.alerts.rules_dir),
            state_file: config
                .storage
                .data_dir
                .join("ruler/state.json")
                .to_string_lossy()
                .into_owned(),
            ..RulerConfig::default()
        };
        let scheduler = Arc::new(RulerScheduler::new(
            ruler_config,
            state.inner.pipeline_registry.clone(),
            evaluator,
        ));
        if let Err(e) = scheduler.load_state().await {
            tracing::warn!(error = %e, "failed to load ruler state");
        }
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task_scheduler = scheduler.clone();
        let task = tokio::spawn(async move {
            task_scheduler.run(shutdown_rx).await;
        });
        let metrics_task = {
            let state = state.clone();
            tokio::spawn(async move {
                while let Some(metrics) = ruler_rx.recv().await {
                    if let Err(e) = state.inner.ingestion_service.ingest_metrics(metrics).await {
                        tracing::warn!(error = %e, "failed to ingest recording-rule metrics");
                    }
                }
            })
        };
        Some((scheduler, shutdown_tx, task, metrics_task))
    } else {
        None
    };

    // Pipeline-extracted metrics enter the metrics WAL/buffer through the same
    // service OTLP metrics use.
    let pipeline_metrics_task = {
        let state = state.clone();
        tokio::spawn(async move {
            while let Some(metrics) = pipeline_metric_rx.recv().await {
                if let Err(e) = state.inner.ingestion_service.ingest_metrics(metrics).await {
                    tracing::warn!(error = %e, "failed to ingest pipeline-extracted metrics");
                }
            }
        })
    };

    // Span-metrics RED consumer: derived metrics flow into the metrics
    // ingestion path as normal OTLP metrics would.
    let span_metrics_task = spawn_span_metrics_consumer(state.clone(), span_metrics_rx);

    let preset_task = activate_alert_presets(&state).await;
    load_alert_rules(&state).await;

    // Alert routing: consume firing/resolved events from the eval engine.
    // The Alertmanager dispatcher wins when configured; the legacy webhook
    // route table is the fallback. Delivering through both would send every
    // notification twice.
    if state.inner.alert_dispatcher.is_some()
        && state
            .inner
            .config
            .alerts
            .notifications
            .as_ref()
            .is_some_and(|notifications| !notifications.routes.is_empty())
    {
        tracing::warn!(
            "both [alerting.routing] and legacy [alerts.notifications.routes] are configured;              the Alertmanager dispatcher wins and legacy routes are disabled"
        );
    }
    let router_state = state.clone();
    let alert_router_task = tokio::spawn(async move {
        let rx = router_state.inner.alert_event_rx.lock().await.take();
        if let Some(mut rx) = rx {
            while let Some(event) = rx.recv().await {
                if let Some(ref dispatcher) = router_state.inner.alert_dispatcher {
                    let silences = router_state.inner.alert_router.silences().await;
                    let active_alerts = router_state.inner.alert_store.list_active().await;
                    dispatcher
                        .ingest_event(event.instance.clone(), &silences, &active_alerts)
                        .await;
                } else {
                    let _ = router_state.inner.alert_router.handle_event(event).await;
                }
            }
        }
    });

    let alert_dispatcher_tick_task = state.inner.alert_dispatcher.as_ref().map(|d| {
        parqtel_alert::dispatcher::AlertDispatcher::spawn_tick_loop(
            d.clone(),
            std::time::Duration::from_secs(1),
        )
    });

    let flush_task = spawn_flush_task(state.clone(), config.server.flush_interval_secs);

    let alert_eval_task = spawn_alert_eval_task(state.clone(), config.server.alert_interval_secs);

    let router = build_router(state.clone());
    let addr = config.server.bind_address.clone();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    for (signal, r) in &wal_replay {
        tracing::info!(
            signal,
            records = r.records,
            skipped = r.skipped,
            truncated_bytes = r.truncated_bytes,
            elapsed_ms = r.elapsed.as_millis(),
            "WAL replay complete"
        );
    }
    tracing::info!("parqtel server listening on {}", addr);

    // OTLP gRPC ingestion (default :4317; disabled when address is empty).
    let grpc_addr = config.server.grpc_bind_address.clone();
    let grpc_state = state.clone();
    let grpc_task = tokio::spawn(async move {
        if let Err(e) = grpc::serve_grpc(grpc_state, &grpc_addr).await {
            if !grpc_addr.is_empty() {
                tracing::error!("OTLP gRPC server failed: {e}");
            }
        }
    });

    if let Some(tls_config) = config.server.tls.as_ref() {
        let tls = crate::tls::load(tls_config)?;
        tracing::info!("HTTP listener using TLS");
        crate::tls::serve(listener, router, tls, shutdown_signal()).await?;
    } else {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    }

    tracing::info!("Shutting down gracefully...");
    flush_task.abort();
    alert_eval_task.abort();
    grpc_task.abort();
    span_metrics_task.abort();
    pipeline_metrics_task.abort();
    if let Some((scheduler, shutdown_tx, task, metrics_task)) = &ruler_runtime {
        let _ = shutdown_tx.send(true);
        if let Err(e) = scheduler.save_state().await {
            tracing::warn!(error = %e, "failed to save ruler state");
        }
        task.abort();
        metrics_task.abort();
    }
    alert_router_task.abort();
    if let Some(task) = alert_dispatcher_tick_task {
        task.abort();
    }
    // Every task holding a clone of the app state keeps the ingestion services
    // (and so the index channels below) alive, so none may outlive this point.
    if let Some(task) = preset_task {
        task.abort();
    }

    state.inner.ingestion_service.shutdown().await?;
    state.inner.log_ingestion_service.shutdown().await?;
    state.inner.trace_ingestion_service.shutdown().await?;

    // Let the index tasks drain the block metadata the flushes above just
    // published, so the final persist pass sees every block.
    drop(state);
    let stop_timeout = std::time::Duration::from_secs(config.server.shutdown_timeout_secs.max(1));
    // Bounded: an index task only ends when every sender is gone, and a stray
    // holder must cost a timeout, not an unkillable process.
    let _ = tokio::time::timeout(stop_timeout, index_task).await;
    let _ = tokio::time::timeout(stop_timeout, log_index_task).await;
    let _ = tokio::time::timeout(stop_timeout, trace_index_task).await;

    // Maintenance tasks stop and each perform a final persist pass before
    // returning, so the sidecars are complete without anyone writing the
    // index from under a lock here.
    metrics_maintenance.shutdown(stop_timeout).await;
    logs_maintenance.shutdown(stop_timeout).await;
    let _ = trace_shutdown_tx.send(true);
    let _ = tokio::time::timeout(stop_timeout, trace_retention).await;
    // The persist loop's final pass runs inside the await, so this explicit
    // write is now a backstop rather than the only chance.
    let _ = trace_persist_shutdown_tx.send(true);
    let _ = tokio::time::timeout(stop_timeout, trace_persist_task).await;

    // Flush any buffered OTLP spans/metrics before the process exits so the
    // final self-telemetry batch is not silently dropped.
    telemetry_guard.shutdown();
    tracing::info!("Shutdown complete");

    Ok(())
}
