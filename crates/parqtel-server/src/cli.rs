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

    /// Node role: all | writer | reader | compactor (default: all)
    #[arg(long, global = true, env = "PARQTEL_ROLE", value_parser = ["all", "writer", "reader", "compactor"])]
    pub(crate) role: Option<String>,

    /// Cross-node identity for the shared block catalog (default: HOSTNAME)
    #[arg(long, global = true, env = "PARQTEL_NODE_ID")]
    pub(crate) node_id: Option<String>,

    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Start the HTTP server (default)
    Serve,
    /// Run bounded metrics compaction passes and exit; stop serving first
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn role_and_node_id_parse_without_a_subcommand() {
        let cli = Cli::try_parse_from(["parqtel", "--role", "reader", "--node-id", "reader-2"])
            .expect("global role flags parse");
        assert_eq!(cli.role.as_deref(), Some("reader"));
        assert_eq!(cli.node_id.as_deref(), Some("reader-2"));
        assert!(cli.command.is_none());
    }

    #[test]
    fn role_and_node_id_parse_after_serve() {
        let cli = Cli::try_parse_from([
            "parqtel",
            "serve",
            "--role",
            "writer",
            "--node-id",
            "writer-1",
        ])
        .expect("role flags are global");
        assert_eq!(cli.role.as_deref(), Some("writer"));
        assert_eq!(cli.node_id.as_deref(), Some("writer-1"));
        assert!(matches!(cli.command, Some(Commands::Serve)));
    }
}
