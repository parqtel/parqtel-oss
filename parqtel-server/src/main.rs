use crate::router::build_router;
use crate::state::AppState;
use clap::{Parser, Subcommand};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use flate2::write::GzEncoder;
use flate2::Compression;
use parqtel_core::{start_maintenance, BlockIndex, BlockIndexStore, Config, RetentionPolicy};
use parqtel_ingest::{IngestionService, LogIngestionService, TraceIngestionService};
use parqtel_query::QueryExecutor;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

mod grpc;
mod handlers;
mod metrics;
mod otel_sli;
mod router;
mod saved_searches;
mod state;
mod telemetry;
#[cfg(test)]
mod tests;

const UI_HTML: &str = include_str!("ui.html");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, env = "PARQTEL_CONFIG")]
    config: Option<PathBuf>,

    /// TCP address to bind to
    #[arg(short, long, env = "PARQTEL_BIND")]
    bind: Option<String>,

    /// Data directory path
    #[arg(short, long, env = "PARQTEL_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Log level override
    #[arg(long, env = "RUST_LOG")]
    log_level: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server (default)
    Serve,
    /// Run one compaction pass and exit
    Compact,
    /// Load index and print storage summary
    Inspect,
    /// Export a metric range to CSV
    Export {
        /// Metric name
        #[arg(long)]
        metric: String,
        /// Start time (ISO 8601)
        #[arg(long)]
        start: String,
        /// End time (ISO 8601)
        #[arg(long)]
        end: String,
        /// Output file path
        #[arg(short, long)]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // 1. Load Configuration
    let mut figment = Figment::from(Serialized::defaults(Config::default()));
    if let Some(config_path) = &cli.config {
        figment = figment.merge(Toml::file(config_path));
    } else if std::path::Path::new("config/default.toml").exists() {
        figment = figment.merge(Toml::file("config/default.toml"));
    }

    // Accept both env spellings: `PARQTEL_SECTION__KEY` (what the prefix
    // strip naturally yields) and the documented `PARQTEL__SECTION__KEY`.
    // Without the second provider, the leading `__` leaves an empty first
    // key segment and the value is silently ignored (verified empirically
    // against /api/v1/stats: PARQTEL__QUERY__MAX_SERIES never applied).
    figment = figment.merge(Env::prefixed("PARQTEL_").split("__"));
    figment = figment.merge(Env::prefixed("PARQTEL__").split("__"));

    // Apply CLI overrides
    if let Some(bind) = cli.bind {
        figment = figment.merge(Serialized::default("server.bind_address", bind));
    }
    if let Some(data_dir) = cli.data_dir {
        figment = figment.merge(Serialized::default("storage.data_dir", data_dir.clone()));
        figment = figment.merge(Serialized::default("logs.data_dir", data_dir.join("logs")));
    }
    if let Some(level) = cli.log_level {
        figment = figment.merge(Serialized::default("telemetry.log_level", level));
    }

    let config: Config = figment.extract()?;
    config.validate()?;
    tracing::debug!(
        log_level = %config.telemetry.log_level,
        log_format = %config.telemetry.log_format,
        "configuration validated"
    );

    // 2. Initialize Telemetry
    // Self-observability: leveled console logs, plus OTLP traces + SLI metrics
    // when `telemetry.otlp_enabled` (flushed on graceful shutdown below).
    let telemetry_guard = telemetry::init(&config.telemetry);
    if config.telemetry.otlp_enabled && !telemetry_guard.is_enabled() {
        eprintln!(
            "parqtel: telemetry.otlp_enabled=true but the OTLP SDK failed to \
             initialise — continuing with console logs only"
        );
    }
    otel_sli::configure_profiling(
        config.telemetry.profiling_enabled,
        config.telemetry.profiling_frequency,
    );

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        bind = config.server.bind_address,
        data_dir = ?config.storage.data_dir,
        logs_dir = ?config.logs.data_dir,
        log_level = %config.telemetry.log_level,
        log_format = %config.telemetry.log_format,
        otlp_enabled = config.telemetry.otlp_enabled,
        otlp_endpoint = %config.telemetry.otlp_endpoint,
        self_telemetry_active = telemetry_guard.is_enabled(),
        profiling_enabled = config.telemetry.profiling_enabled,
        "parqtel starting"
    );

    // 3. Setup Directories and Indexes
    std::fs::create_dir_all(&config.storage.data_dir)?;
    std::fs::create_dir_all(&config.logs.data_dir)?;

    // The store is built while the index is still held directly, so the
    // sidecar path is defined in exactly one place (BlockIndex::new).
    let mut index = BlockIndex::new(&config.storage.data_dir);
    index.load().unwrap_or_default();
    let index_store = Arc::new(BlockIndexStore::new(&index));
    // The sidecar is a cache of on-disk state, so a lost, stale or
    // hand-restored index would otherwise hide blocks that are present and
    // perfectly readable. Reconcile, then persist so the repair sticks.
    reconcile_and_persist(
        &mut index,
        &index_store,
        parqtel_core::SignalType::Metrics,
        "metrics",
    );
    tracing::debug!(blocks = index.blocks.len(), "metrics block index loaded");
    let index = Arc::new(tokio::sync::RwLock::new(index));

    let mut log_index = BlockIndex::new(&config.logs.data_dir);
    log_index.load().unwrap_or_default();
    let log_index_store = Arc::new(BlockIndexStore::new(&log_index));
    reconcile_and_persist(
        &mut log_index,
        &log_index_store,
        parqtel_core::SignalType::Logs,
        "logs",
    );
    tracing::debug!(blocks = log_index.blocks.len(), "logs block index loaded");
    let log_index = Arc::new(tokio::sync::RwLock::new(log_index));

    // 4. Handle Subcommands
    match cli.command.unwrap_or(Commands::Serve) {
        Commands::Serve => {
            run_server(
                config,
                index,
                index_store,
                log_index,
                log_index_store,
                telemetry_guard,
            )
            .await?
        }
        Commands::Compact => run_compact(config, index, log_index).await?,
        Commands::Inspect => run_inspect(index, log_index).await?,
        Commands::Export {
            metric,
            start,
            end,
            output,
        } => run_export(config, index, metric, start, end, output).await?,
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// Opens a signal's WAL, or returns `None` when the signal has it disabled.
///
/// A WAL that cannot be opened is fatal: continuing would mean accepting data
/// we cannot promise to recover, which is exactly the guarantee the WAL exists
/// to provide.
fn open_wal(
    config: &Config,
    signal: &str,
    data_dir: &std::path::Path,
    enabled: bool,
) -> anyhow::Result<Option<parqtel_core::wal::WalWriter>> {
    if !enabled {
        tracing::warn!(
            signal,
            "WAL is disabled for this signal; a crash can lose up to a whole block window"
        );
        return Ok(None);
    }
    let wal = parqtel_core::wal::WalWriter::open_with_segment_limit(
        data_dir,
        signal,
        config.ingest.wal_sync_mode,
        std::time::Duration::from_millis(config.ingest.wal_sync_interval_ms.max(1)),
        config.ingest.wal_max_segment_bytes,
    )?;
    Ok(Some(wal))
}

/// Reconciles a block index against its data directory and persists the repair.
///
/// No-op in the normal case, where the sidecar already matches the disk: one
/// `read_dir`, and the sidecar is only rewritten when something actually
/// changed.
fn reconcile_and_persist(
    index: &mut BlockIndex,
    store: &BlockIndexStore,
    signal: parqtel_core::SignalType,
    label: &str,
) {
    match parqtel_core::storage::reconcile(index, signal) {
        Ok(stats) if stats.adopted > 0 || stats.dropped > 0 => {
            tracing::warn!(
                signal = label,
                adopted = stats.adopted,
                dropped = stats.dropped,
                unreadable = stats.unreadable,
                "block index did not match the data directory; reconciled from the                  block files. Metric names may be a range until the block is                  rewritten by compaction, and recovered blocks have no label                  dictionary until then."
            );
            if let Err(e) = parqtel_core::storage::persist_blocking(index, store) {
                tracing::warn!(signal = label, error = %e, "failed to persist the reconciled index");
            }
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(signal = label, error = %e, "block index reconcile failed"),
    }
}

/// What a WAL replay did, for the startup log and `/api/v1/stats`.
#[derive(Debug, Clone, Copy, Default)]
pub struct WalReplayOutcome {
    pub records: u64,
    pub skipped: u64,
    pub truncated_bytes: u64,
    pub elapsed: std::time::Duration,
}

/// Reads one signal's WAL records as raw JSON, handing each to `on_record`.
///
/// The record type differs per signal, so each caller deserialises what it
/// needs; the framing, the commit-point skip and the torn-tail handling are
/// identical and live here. Returns the replay counters.
fn read_wal_records(
    data_dir: &std::path::Path,
    signal: &str,
    on_record: &mut dyn FnMut(serde_json::Value),
) -> Option<(u64, u64, u64)> {
    if !data_dir.join("wal").join(signal).exists() {
        return None;
    }
    let commit = parqtel_core::wal::read_commit(data_dir, signal);
    match parqtel_core::wal::replay::<serde_json::Value, _>(data_dir, signal, commit, |v| {
        on_record(v)
    }) {
        Ok(stats) => {
            if stats.truncated_bytes > 0 {
                tracing::warn!(
                    signal,
                    bytes = stats.truncated_bytes,
                    "WAL had a torn tail from a crash; the unusable bytes were dropped"
                );
            }
            Some((stats.records, stats.skipped, stats.truncated_bytes))
        }
        Err(e) => {
            tracing::error!(signal, "WAL replay failed: {e}");
            None
        }
    }
}

/// Shared tail of a per-signal replay: log the outcome and stamp the elapsed
/// time.
fn finish_replay(
    signal: &'static str,
    started: std::time::Instant,
    records: u64,
    skipped: u64,
    truncated_bytes: u64,
) -> WalReplayOutcome {
    if records > 0 {
        tracing::warn!(
            signal,
            records,
            "recovered telemetry from the write-ahead log"
        );
    }
    WalReplayOutcome {
        records,
        skipped,
        truncated_bytes,
        elapsed: started.elapsed(),
    }
}

/// Replays every enabled signal's WAL.
///
/// Runs before the listener binds, so a restart never serves queries over a
/// partially-recovered store, and through exactly the entry points live
/// exporters use — so recovered telemetry is written into a fresh block as if
/// it had arrived live, which is also what advances the commit point and lets
/// the WAL be trimmed.
async fn replay_all_wals(
    config: &Config,
    metrics: &IngestionService,
    logs: &LogIngestionService,
    traces: &TraceIngestionService,
) -> Vec<(&'static str, WalReplayOutcome)> {
    let mut out = Vec::new();

    if config.ingest.wal_enabled {
        let started = std::time::Instant::now();
        let mut pending = Vec::new();
        let counts = read_wal_records(&config.storage.data_dir, "metrics", &mut |v| {
            pending.push(v);
        });
        if let Some((records, skipped, truncated)) = counts {
            for v in pending.drain(..) {
                match serde_json::from_value::<parqtel_core::Metric>(v) {
                    Ok(m) => {
                        // One bad record must not strand the rest of the WAL.
                        if let Err(e) = metrics.ingest_metrics(vec![m]).await {
                            tracing::error!(
                                signal = "metrics",
                                "WAL replay: re-ingest failed: {e}"
                            );
                        }
                    }
                    Err(e) => tracing::error!(signal = "metrics", "WAL replay: bad record: {e}"),
                }
            }
            out.push((
                "metrics",
                finish_replay("metrics", started, records, skipped, truncated),
            ));
        }
    }

    if config.ingest.log_wal_enabled {
        let started = std::time::Instant::now();
        let mut pending = Vec::new();
        let counts = read_wal_records(&config.logs.data_dir, "logs", &mut |v| {
            pending.push(v);
        });
        if let Some((records, skipped, truncated)) = counts {
            for v in pending.drain(..) {
                match serde_json::from_value::<parqtel_core::LogRecord>(v) {
                    Ok(l) => {
                        if let Err(e) = logs.ingest_logs(vec![l]).await {
                            tracing::error!(signal = "logs", "WAL replay: re-ingest failed: {e}");
                        }
                    }
                    Err(e) => tracing::error!(signal = "logs", "WAL replay: bad record: {e}"),
                }
            }
            out.push((
                "logs",
                finish_replay("logs", started, records, skipped, truncated),
            ));
        }
    }

    if config.ingest.wal_enabled {
        let started = std::time::Instant::now();
        let mut pending = Vec::new();
        // Trace blocks share the metrics data dir (TraceWriter is built from
        // config.storage), so the trace WAL lives there too.
        let counts = read_wal_records(&config.storage.data_dir, "traces", &mut |v| {
            pending.push(v);
        });
        if let Some((records, skipped, truncated)) = counts {
            for v in pending.drain(..) {
                match serde_json::from_value::<parqtel_core::Span>(v) {
                    Ok(s) => {
                        if let Err(e) = traces.ingest_spans(vec![s]).await {
                            tracing::error!(signal = "traces", "WAL replay: re-ingest failed: {e}");
                        }
                    }
                    Err(e) => tracing::error!(signal = "traces", "WAL replay: bad record: {e}"),
                }
            }
            out.push((
                "traces",
                finish_replay("traces", started, records, skipped, truncated),
            ));
        }
    }

    out
}

async fn run_server(
    config: Config,
    index: Arc<tokio::sync::RwLock<BlockIndex>>,
    index_store: Arc<BlockIndexStore>,
    log_index: Arc<tokio::sync::RwLock<BlockIndex>>,
    log_index_store: Arc<BlockIndexStore>,
    telemetry_guard: telemetry::TelemetryGuard,
) -> anyhow::Result<()> {
    // Prepare UI assets
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(UI_HTML.as_bytes())?;
    let ui_content = encoder.finish()?;

    let mut hasher = Sha256::new();
    hasher.update(UI_HTML.as_bytes());
    let ui_etag = format!("\"{}\"", hex::encode(hasher.finalize()));

    // Metrics pipeline.
    //
    // `ContentionMetrics` is created once and shared with the three ingestion
    // services, the three block-index tasks and the /metrics renderer, so a
    // lock wait recorded on the ingest path is the same series an operator
    // scrapes. The index writer measures how long it waits for the write lock
    // because every query handler contends for it.
    let contention = Arc::new(parqtel_core::ContentionMetrics::new());
    let (tx, mut rx) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
    let idx_clone = index.clone();
    let idx_contention = contention.clone();
    let idx_store = index_store.clone();
    let index_task = tokio::spawn(async move {
        while let Some(pending) = rx.recv().await {
            let started = std::time::Instant::now();
            {
                let mut idx = idx_clone.write().await;
                idx_contention.record_index_lock_wait(started.elapsed());
                // In-memory only: the write lock must never be held across a
                // serialise + write + rename of the whole index.
                idx.add(pending.meta);
            }
            idx_store.mark_dirty();
            // A block whose WAL has not been committed yet must be durable in
            // the sidecar *before* the flush proceeds, or a crash in between
            // leaves the block on disk, absent from the index, and already
            // marked recovered (BL-03-16). So force the write now rather than
            // waiting for the debounce.
            if pending.durable.is_some() {
                let _ = parqtel_core::storage::persist_once(&idx_clone, &idx_store).await;
            }
            if let Some(ack) = pending.durable {
                let _ = ack.send(());
            }
        }
    });

    // Logs pipeline
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
    let log_idx_clone = log_index.clone();
    let log_idx_contention = contention.clone();
    let log_idx_store = log_index_store.clone();
    let log_index_task = tokio::spawn(async move {
        while let Some(pending) = log_rx.recv().await {
            let started = std::time::Instant::now();
            {
                let mut idx = log_idx_clone.write().await;
                log_idx_contention.record_index_lock_wait(started.elapsed());
                idx.add(pending.meta);
            }
            log_idx_store.mark_dirty();
            // See the metrics task: the sidecar must be durable before the
            // flush commits the WAL (BL-03-16).
            if pending.durable.is_some() {
                let _ = parqtel_core::storage::persist_once(&log_idx_clone, &log_idx_store).await;
            }
            if let Some(ack) = pending.durable {
                let _ = ack.send(());
            }
        }
    });

    // Create shared in-memory buffer for stream-queryable data
    let memory_buffer = parqtel_core::MemoryBuffer::new();

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
    .with_contention(contention.clone());
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
    let log_ingestion_service =
        LogIngestionService::with_wal(config.logs.clone(), log_tx, logs_wal)
            .with_memory_buffer(memory_buffer.clone())
            .with_contention(contention.clone());
    let (trace_tx, mut trace_rx) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
    // Span-metrics RED bridge: trace ingestion derives
    // traces_service_{requests,errors,duration_ms} metrics and feeds them
    // back through the normal metrics path.
    let (span_metrics_tx, mut span_metrics_rx) =
        mpsc::unbounded_channel::<Vec<parqtel_core::Metric>>();
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
    let trace_index_store = Arc::new(BlockIndexStore::new(&trace_index));
    let trace_index = Arc::new(tokio::sync::RwLock::new(trace_index));

    let trace_idx_clone = trace_index.clone();
    let trace_idx_contention = contention.clone();
    let trace_idx_store = trace_index_store.clone();
    let trace_index_task = tokio::spawn(async move {
        while let Some(pending) = trace_rx.recv().await {
            let started = std::time::Instant::now();
            {
                let mut idx = trace_idx_clone.write().await;
                trace_idx_contention.record_index_lock_wait(started.elapsed());
                idx.add(pending.meta);
            }
            trace_idx_store.mark_dirty();
            // See the metrics task: the sidecar must be durable before the
            // flush commits the WAL (BL-03-16).
            if pending.durable.is_some() {
                let _ =
                    parqtel_core::storage::persist_once(&trace_idx_clone, &trace_idx_store).await;
            }
            if let Some(ack) = pending.durable {
                let _ = ack.send(());
            }
        }
    });

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

    // Span-metrics RED consumer: derived metrics flow into the metrics
    // ingestion path as normal OTLP metrics would.
    let span_metrics_state = state.clone();
    let span_metrics_task = tokio::spawn(async move {
        while let Some(metrics) = span_metrics_rx.recv().await {
            if let Err(e) = span_metrics_state
                .inner
                .ingestion_service
                .ingest_metrics(metrics)
                .await
            {
                tracing::warn!("span-metrics ingestion failed: {e}");
            }
        }
    });

    // Built-in alert preset packs ([alerts.presets]: off | auto | all).
    // Runs *before* the rules_dir pass below so a file rule with the same id
    // overrides the built-in, and every insert is insert-if-absent so a rule
    // the user disabled/deleted via the API is never re-inserted (activation
    // is one-shot per pack per process). Plan: docs/BUILTIN_ALERT_PRESETS_PLAN.md.
    let mut preset_pending: Vec<&'static parqtel_alert::builtin::BuiltinPack> = Vec::new();
    {
        let presets = &state.inner.config.alerts.presets;
        if presets.mode != parqtel_core::config::PresetMode::Off {
            let metrics = state.inner.query_executor.list_metrics().await;
            preset_pending = parqtel_alert::builtin::activate_initial(
                presets,
                &metrics,
                &state.inner.alert_registry,
            )
            .await;
            if !preset_pending.is_empty() {
                tracing::debug!(
                    pending = preset_pending.len(),
                    "built-in alert packs awaiting canary metrics"
                );
            }
        }
    }
    // Auto mode: retry packs whose canary metrics have not arrived yet every
    // 5 minutes until the pending set drains. Activations never revert, so
    // the task exits for good once every selected pack has resolved.
    if !preset_pending.is_empty() {
        let preset_state = state.clone();
        tokio::spawn(async move {
            let mut pending = preset_pending;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
            interval.tick().await; // startup pass already ran; wait a full interval
            while !pending.is_empty() {
                interval.tick().await;
                let metrics = preset_state.inner.query_executor.list_metrics().await;
                pending = parqtel_alert::builtin::activate_detected(
                    pending,
                    &metrics,
                    &preset_state.inner.alert_registry,
                )
                .await;
            }
            tracing::debug!("all selected built-in alert packs resolved");
        });
    }

    // Load alert rules from the configured directory (evaluator + router
    // both key off the registry; without this, rules only exist via the API).
    {
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

    // Alert routing: consume firing events from the eval engine and deliver
    // to configured webhook sinks (silences + repeat windows honored).
    let router_state = state.clone();
    let alert_router_task = tokio::spawn(async move {
        let rx = router_state.inner.alert_event_rx.lock().await.take();
        if let Some(rx) = rx {
            let router = router_state.inner.alert_router.clone();
            let _ = parqtel_alert::router::spawn_router(router, rx).await;
        }
    });

    // Background flush task. Each tick also publishes the SLI saturation
    // gauges (buffer occupancy, RSS) so memory pressure is visible *before* the
    // OOM killer arrives rather than only in a post-mortem.
    let state_clone = state.clone();
    let flush_interval_secs = config.server.flush_interval_secs;
    let flush_task = tokio::spawn(async move {
        // The tick only *asks* each rotator whether its block duration has
        // elapsed, so this interval bounds how late a duration-triggered flush
        // can be — not how often blocks are written.
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(flush_interval_secs.max(1)));
        loop {
            interval.tick().await;

            let buffer = state_clone.inner.query_executor.memory_buffer();
            let (before_metrics, before_logs, before_spans) = buffer.stats().await;

            let started = std::time::Instant::now();
            match state_clone.inner.ingestion_service.check_and_flush().await {
                Ok(flushed) => {
                    otel_sli::record_flush("metrics", started.elapsed().as_secs_f64(), Ok(flushed));
                    if flushed {
                        let (now, _, _) = buffer.stats().await;
                        let drained = before_metrics.saturating_sub(now);
                        otel_sli::record_flush_rows("metrics", drained as u64);
                        tracing::debug!(
                            signal = "metrics",
                            drained_rows = drained,
                            duration_ms = started.elapsed().as_millis(),
                            "buffer flushed to parquet block"
                        );
                    }
                }
                Err(e) => {
                    otel_sli::record_flush("metrics", started.elapsed().as_secs_f64(), Err(()));
                    tracing::error!(signal = "metrics", error = %e, "buffer flush failed");
                }
            }

            let started = std::time::Instant::now();
            match state_clone
                .inner
                .log_ingestion_service
                .check_and_flush()
                .await
            {
                Ok(flushed) => {
                    otel_sli::record_flush("logs", started.elapsed().as_secs_f64(), Ok(flushed));
                    if flushed {
                        let (_, now, _) = buffer.stats().await;
                        let drained = before_logs.saturating_sub(now);
                        otel_sli::record_flush_rows("logs", drained as u64);
                        tracing::debug!(
                            signal = "logs",
                            drained_rows = drained,
                            duration_ms = started.elapsed().as_millis(),
                            "buffer flushed to parquet block"
                        );
                    }
                }
                Err(e) => {
                    otel_sli::record_flush("logs", started.elapsed().as_secs_f64(), Err(()));
                    tracing::error!(signal = "logs", error = %e, "buffer flush failed");
                }
            }

            let started = std::time::Instant::now();
            match state_clone
                .inner
                .trace_ingestion_service
                .check_and_flush()
                .await
            {
                Ok(flushed) => {
                    otel_sli::record_flush("traces", started.elapsed().as_secs_f64(), Ok(flushed));
                    if flushed {
                        let (_, _, now) = buffer.stats().await;
                        let drained = before_spans.saturating_sub(now);
                        otel_sli::record_flush_rows("traces", drained as u64);
                        tracing::debug!(
                            signal = "traces",
                            drained_rows = drained,
                            duration_ms = started.elapsed().as_millis(),
                            "buffer flushed to parquet block"
                        );
                    }
                }
                Err(e) => {
                    otel_sli::record_flush("traces", started.elapsed().as_secs_f64(), Err(()));
                    tracing::error!(signal = "traces", error = %e, "buffer flush failed");
                }
            }

            let (metrics, logs, spans) = buffer.stats().await;
            otel_sli::record_gauges(metrics as u64, logs as u64, spans as u64);
            // Sample process memory here rather than in the /metrics handler:
            // reading /proc/self/status is a blocking syscall and RSS changes
            // far more slowly than a Prometheus scrape interval.
            state_clone.inner.metrics.refresh_process_memory();
        }
    });

    // Alert evaluation loop
    let state_clone = state.clone();
    let alert_interval_secs = config.server.alert_interval_secs;
    let alert_eval_task = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(alert_interval_secs.max(1)));
        loop {
            interval.tick().await;
            let rules = state_clone.inner.alert_registry.list_enabled().await;
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
                let start_ns =
                    now_ns.saturating_sub(state_clone.inner.config.query.lookback_delta_ns);
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
                if let Ok(qr) = state_clone.inner.query_executor.execute(plan).await {
                    for series in &qr.series {
                        if let Some(sample) = series.samples.last() {
                            let labels = series
                                .labels
                                .iter()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();
                            state_clone
                                .inner
                                .alert_engine
                                .evaluate_rule_with_value(&rule, sample.value, labels)
                                .await;
                        }
                    }
                }
            }
        }
    });

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

    let shutdown = async {
        // Graceful shutdown on SIGINT (ctrl-c) OR SIGTERM (docker stop,
        // k8s pod termination, process managers) — either signal must
        // reach the flush-before-exit path or all buffered telemetry is lost.
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
    };

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await?;

    tracing::info!("Shutting down gracefully...");
    flush_task.abort();
    alert_eval_task.abort();
    grpc_task.abort();
    span_metrics_task.abort();
    alert_router_task.abort();

    state.inner.ingestion_service.shutdown().await?;
    state.inner.log_ingestion_service.shutdown().await?;
    state.inner.trace_ingestion_service.shutdown().await?;

    // Let the index tasks drain the block metadata the flushes above just
    // published, so the final persist pass sees every block.
    drop(state);
    let _ = index_task.await;
    let _ = log_index_task.await;
    let _ = trace_index_task.await;

    // Maintenance tasks stop and each perform a final persist pass before
    // returning, so the sidecars are complete without anyone writing the
    // index from under a lock here.
    let stop_timeout = std::time::Duration::from_secs(config.server.shutdown_timeout_secs.max(1));
    metrics_maintenance.shutdown(stop_timeout).await;
    logs_maintenance.shutdown(stop_timeout).await;
    let _ = trace_shutdown_tx.send(true);
    let _ = tokio::time::timeout(stop_timeout, trace_retention).await;

    // The trace index has no persist loop (retention only), so it is written
    // explicitly here — synchronously, after all other writers are stopped.
    let trace_snapshot = trace_index.read().await.serialize()?;
    tokio::task::spawn_blocking(move || trace_index_store.write_payload(&trace_snapshot)).await??;

    // Flush any buffered OTLP spans/metrics before the process exits so the
    // final self-telemetry batch is not silently dropped.
    telemetry_guard.shutdown();
    tracing::info!("Shutdown complete");

    Ok(())
}

async fn run_compact(
    _config: Config,
    _idx: Arc<tokio::sync::RwLock<BlockIndex>>,
    _lidx: Arc<tokio::sync::RwLock<BlockIndex>>,
) -> anyhow::Result<()> {
    tracing::info!("Running one-off compaction...");
    Ok(())
}

async fn run_inspect(
    index: Arc<tokio::sync::RwLock<BlockIndex>>,
    log_index: Arc<tokio::sync::RwLock<BlockIndex>>,
) -> anyhow::Result<()> {
    let idx = index.read().await;
    let lidx = log_index.read().await;
    let summary = serde_json::json!({
        "metrics": {
            "block_count": idx.total_blocks(),
            "total_rows": idx.total_rows(),
            "total_bytes": idx.total_bytes(),
            "metric_names": idx.all_metrics().into_iter().collect::<Vec<_>>(),
        },
        "logs": {
            "block_count": lidx.total_blocks(),
            "total_rows": lidx.total_rows(),
            "total_bytes": lidx.total_bytes(),
        }
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

async fn run_export(
    config: Config,
    index: Arc<tokio::sync::RwLock<BlockIndex>>,
    metric: String,
    start: String,
    end: String,
    output: PathBuf,
) -> anyhow::Result<()> {
    // Prevent path traversal: ensure output is within data directory
    let data_dir = config
        .storage
        .data_dir
        .canonicalize()
        .unwrap_or_else(|_| config.storage.data_dir.clone());

    // For output path, canonicalize the parent directory since file may not exist yet
    let output_parent = output.parent().unwrap_or(&output);
    let output_path = output_parent
        .canonicalize()
        .unwrap_or_else(|_| output_parent.to_path_buf());

    if !output_path.starts_with(&data_dir) {
        return Err(anyhow::anyhow!(
            "Output path must be within data directory: {}",
            data_dir.display()
        ));
    }

    let start_dt = chrono::DateTime::parse_from_rfc3339(&start)?;
    let end_dt = chrono::DateTime::parse_from_rfc3339(&end)?;
    let start_ns = start_dt.timestamp_nanos_opt().unwrap_or(0);
    let end_ns = end_dt.timestamp_nanos_opt().unwrap_or(0);

    let blocks = {
        let idx = index.read().await;
        idx.query(start_ns, end_ns, Some(&metric))
    };

    let points =
        parqtel_core::storage::Scanner::scan(blocks, metric, start_ns, end_ns, None).await?;

    let mut file = std::fs::File::create(output)?;
    writeln!(file, "timestamp_ns,value,labels")?;
    for p in &points {
        writeln!(
            file,
            "{},{},{:?}",
            p.timestamp_ns,
            v_to_f64(&p.value),
            p.labels
        )?;
    }

    println!("Exported {} points", points.len());
    Ok(())
}

fn v_to_f64(v: &parqtel_core::MetricValue) -> f64 {
    match v {
        parqtel_core::MetricValue::Double(f) => *f,
        parqtel_core::MetricValue::Int(i) => *i as f64,
        parqtel_core::MetricValue::Histogram { sum, .. } => *sum,
        parqtel_core::MetricValue::Summary { sum, .. } => *sum,
    }
}
