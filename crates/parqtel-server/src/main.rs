//! `parqtel` server binary: configuration, startup and subcommand dispatch.

mod cli;
mod commands;
mod grpc;
mod handlers;
mod metrics;
mod otel_sli;
mod recovery;
mod router;
mod saved_searches;
mod server;
mod state;
mod tasks;
mod telemetry;
#[cfg(test)]
mod tests;

use crate::cli::{Cli, Commands};
use crate::recovery::reconcile_and_persist;
use clap::Parser;
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use parqtel_core::{BlockIndex, BlockIndexStore, Config};
use std::sync::Arc;

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
            server::run_server(
                config,
                index,
                index_store,
                log_index,
                log_index_store,
                telemetry_guard,
            )
            .await?
        }
        Commands::Compact => commands::run_compact(config, index, log_index).await?,
        Commands::Inspect => commands::run_inspect(index, log_index).await?,
        Commands::Export {
            metric,
            start,
            end,
            output,
        } => commands::run_export(config, index, metric, start, end, output).await?,
    }

    Ok(())
}
