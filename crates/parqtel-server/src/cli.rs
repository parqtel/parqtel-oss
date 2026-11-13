//! Command-line interface.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct Cli {
    /// Path to configuration file
    #[arg(short, long, env = "PARQTEL_CONFIG")]
    pub(crate) config: Option<PathBuf>,

    /// TCP address to bind to
    #[arg(short, long, env = "PARQTEL_BIND")]
    pub(crate) bind: Option<String>,

    /// Data directory path
    #[arg(short, long, env = "PARQTEL_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,

    /// Log level override
    #[arg(long, env = "RUST_LOG")]
    pub(crate) log_level: Option<String>,

    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
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
