use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use parqtel_core::{BlockIndex, Config, StorageEngine, start_maintenance};
use parqtel_core::engine::registry::StorageEngineRegistry;
use parqtel_ingest::{IngestionService, LogIngestionService, TraceIngestionService};
use parqtel_query::QueryExecutor;
use crate::state::AppState;
use crate::router::build_router;
use std::io::Write;
use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Sha256, Digest};
use clap::{Parser, Subcommand};
use figment::{Figment, providers::{Format, Toml, Env, Serialized}};

mod state;
mod handlers;
mod router;
mod telemetry;
mod metrics;
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
    
    figment = figment.merge(Env::prefixed("PARQTEL_").split("__"));

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

    // 2. Initialize Telemetry
    telemetry::init(&config.telemetry.log_level, &config.telemetry.log_format);

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        bind = config.server.bind_address,
        data_dir = ?config.storage.data_dir,
        logs_dir = ?config.logs.data_dir,
        "parqtel starting"
    );

    // 3. Setup Directories and Indexes
    std::fs::create_dir_all(&config.storage.data_dir)?;
    std::fs::create_dir_all(&config.logs.data_dir)?;

    let mut index = BlockIndex::new(&config.storage.data_dir);
    index.load().unwrap_or_default();
    let index = Arc::new(tokio::sync::RwLock::new(index));

    let mut log_index = BlockIndex::new(&config.logs.data_dir);
    log_index.load().unwrap_or_default();
    let log_index = Arc::new(tokio::sync::RwLock::new(log_index));

    // 4. Handle Subcommands
    match cli.command.unwrap_or(Commands::Serve) {
        Commands::Serve => run_server(config, index, log_index).await?,
        Commands::Compact => run_compact(config, index, log_index).await?,
        Commands::Inspect => run_inspect(index, log_index).await?,
        Commands::Export { metric, start, end, output } => run_export(config, index, metric, start, end, output).await?,
    }

    Ok(())
}

async fn run_server(
    config: Config, 
    index: Arc<tokio::sync::RwLock<BlockIndex>>,
    log_index: Arc<tokio::sync::RwLock<BlockIndex>>,
) -> anyhow::Result<()> {
    // Build storage engine via registry
    let registry = StorageEngineRegistry::default();
    let storage_engine: Arc<dyn StorageEngine> = registry.build(&config.storage.backend, config.storage.clone())
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Prepare UI assets
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(UI_HTML.as_bytes())?;
    let ui_content = encoder.finish()?;
    
    let mut hasher = Sha256::new();
    hasher.update(UI_HTML.as_bytes());
    let ui_etag = format!("\"{}\"", hex::encode(hasher.finalize()));

    // Metrics pipeline
    let (tx, mut rx) = mpsc::unbounded_channel();
    let idx_clone = index.clone();
    let index_task = tokio::spawn(async move {
        while let Some(meta) = rx.recv().await {
            let mut idx = idx_clone.write().await;
            if let Err(e) = idx.add(meta) { tracing::error!("Failed to add block to index: {}", e); }
        }
    });

    // Logs pipeline
    let (log_tx, mut log_rx) = mpsc::unbounded_channel();
    let log_idx_clone = log_index.clone();
    let log_index_task = tokio::spawn(async move {
        while let Some(meta) = log_rx.recv().await {
            let mut idx = log_idx_clone.write().await;
            if let Err(e) = idx.add(meta) { tracing::error!("Failed to add log block to index: {}", e); }
        }
    });

    // Create shared in-memory buffer for stream-queryable data
    let memory_buffer = parqtel_core::MemoryBuffer::new();

    let ingestion_service = IngestionService::new(config.storage.clone(), tx)
        .with_memory_buffer(memory_buffer.clone());
    let log_ingestion_service = LogIngestionService::new(config.logs.clone(), log_tx)
        .with_memory_buffer(memory_buffer.clone());
    let (trace_tx, _) = mpsc::unbounded_channel();
    let trace_ingestion_service = TraceIngestionService::new(config.storage.clone(), trace_tx);

    let query_executor = QueryExecutor::with_buffer(index.clone(), log_index.clone(), memory_buffer.clone());

    start_maintenance(index.clone(), config.storage.clone());
    start_maintenance(log_index.clone(), config.logs.clone().into());

    let state = AppState::new(
        storage_engine,
        ingestion_service,
        log_ingestion_service,
        trace_ingestion_service,
        query_executor, 
        index.clone(),
        memory_buffer.clone(),
        config.clone(),
        ui_content,
        ui_etag,
    ).await;

    // Background flush task
    let state_clone = state.clone();
    let flush_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let _ = state_clone.inner.ingestion_service.check_and_flush().await;
            let _ = state_clone.inner.log_ingestion_service.check_and_flush().await;
            let _ = state_clone.inner.trace_ingestion_service.check_and_flush().await;
        }
    });

    // Alert evaluation loop
    let state_clone = state.clone();
    let alert_eval_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            let rules = state_clone.inner.alert_registry.list_enabled().await;
            for rule in rules {
                let parsed = parqtel_query::parse_query(&rule.query);
                let (metric_name, matchers, aggregation, quantile) = match parsed {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                let start_ns = now_ns - 300_000_000_000; // 5 min lookback
                let plan = parqtel_query::QueryPlan::new(
                    metric_name, matchers, start_ns, now_ns, None,
                    100, 1000, aggregation, quantile,
                );
                let plan = match plan {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if let Ok(qr) = state_clone.inner.query_executor.execute(plan).await {
                    for series in &qr.series {
                        if let Some(sample) = series.samples.last() {
                            let labels = series.labels.iter()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();
                            state_clone.inner.alert_engine.evaluate_rule_with_value(
                                &rule, sample.value, labels,
                            ).await;
                        }
                    }
                }
            }
        }
    });

    let router = build_router(state.clone());
    let addr = config.server.bind_address.clone();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("parqtel server listening on {}", addr);
    
    let shutdown = async {
        tokio::signal::ctrl_c().await.unwrap_or_default();
    };

    axum::serve(listener, router).with_graceful_shutdown(shutdown).await?;

    tracing::info!("Shutting down gracefully...");
    flush_task.abort();
    alert_eval_task.abort();
    
    state.inner.ingestion_service.shutdown().await?;
    state.inner.log_ingestion_service.shutdown().await?;
    state.inner.trace_ingestion_service.shutdown().await?;
    
    drop(state);
    let _ = index_task.await;
    let _ = log_index_task.await;
    
    index.read().await.save()?;
    log_index.read().await.save()?;
    tracing::info!("Shutdown complete");

    Ok(())
}

async fn run_compact(_config: Config, _idx: Arc<tokio::sync::RwLock<BlockIndex>>, _lidx: Arc<tokio::sync::RwLock<BlockIndex>>) -> anyhow::Result<()> {
    tracing::info!("Running one-off compaction...");
    Ok(())
}

async fn run_inspect(index: Arc<tokio::sync::RwLock<BlockIndex>>, log_index: Arc<tokio::sync::RwLock<BlockIndex>>) -> anyhow::Result<()> {
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
    _config: Config, 
    index: Arc<tokio::sync::RwLock<BlockIndex>>, 
    metric: String, 
    start: String, 
    end: String, 
    output: PathBuf
) -> anyhow::Result<()> {
    let start_dt = chrono::DateTime::parse_from_rfc3339(&start)?;
    let end_dt = chrono::DateTime::parse_from_rfc3339(&end)?;
    let start_ns = start_dt.timestamp_nanos_opt().unwrap_or(0);
    let end_ns = end_dt.timestamp_nanos_opt().unwrap_or(0);

    let blocks = {
        let idx = index.read().await;
        idx.query(start_ns, end_ns, Some(&metric))
    };

    let points = parqtel_core::storage::Scanner::scan(blocks, metric, start_ns, end_ns).await?;
    
    let mut file = std::fs::File::create(output)?;
    writeln!(file, "timestamp_ns,value,labels")?;
    for p in &points {
        writeln!(file, "{},{},{:?}", p.timestamp_ns, v_to_f64(&p.value), p.labels)?;
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
