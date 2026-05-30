use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Configuration for storage blocks (Parquet files).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockConfig {
    /// Storage backend to use (e.g. "parquet").
    #[serde(default = "default_backend")]
    pub backend: String,
    /// Path to the directory where data blocks are stored.
    pub data_dir: PathBuf,
    /// Duration of a single data block in seconds.
    pub block_duration_secs: u64,
    /// Maximum number of rows allowed in a single block.
    pub max_rows_per_block: usize,
    /// Compression codec to use for Parquet files (zstd, snappy, lz4, none).
    pub compression: String,
    /// Data retention in days.
    pub retention_days: u64,
    /// Interval between compaction passes in seconds.
    pub compaction_interval_secs: u64,
    /// Number of rows per row group in Parquet files.
    pub row_group_size: usize,
}

fn default_backend() -> String { "parquet".into() }

impl Default for BlockConfig {
    fn default() -> Self {
        Self {
            backend: "parquet".into(),
            data_dir: PathBuf::from("data"),
            block_duration_secs: 7200,
            max_rows_per_block: 1_000_000,
            compression: "zstd".into(),
            retention_days: 7,
            compaction_interval_secs: 3600,
            row_group_size: 100_000,
        }
    }
}

/// Configuration for log storage blocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogBlockConfig {
    /// Path to the directory where log blocks are stored.
    pub data_dir: PathBuf,
    /// Duration of a single log block in seconds.
    pub block_duration_secs: u64,
    /// Maximum number of rows allowed in a single block.
    pub max_rows_per_block: usize,
    /// Compression codec to use for Parquet files (zstd, snappy, lz4, none).
    pub compression: String,
    /// Data retention in days.
    pub retention_days: u64,
    /// Interval between compaction passes in seconds.
    pub compaction_interval_secs: u64,
    /// Number of rows per row group in Parquet files.
    pub row_group_size: usize,
}

impl Default for LogBlockConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/logs"),
            block_duration_secs: 1800,
            max_rows_per_block: 200_000,
            compression: "zstd".into(),
            retention_days: 3,
            compaction_interval_secs: 3600,
            row_group_size: 20_000,
        }
    }
}

impl From<LogBlockConfig> for BlockConfig {
    fn from(log: LogBlockConfig) -> Self {
        Self {
            backend: "parquet".into(),
            data_dir: log.data_dir,
            block_duration_secs: log.block_duration_secs,
            max_rows_per_block: log.max_rows_per_block,
            compression: log.compression,
            retention_days: log.retention_days,
            compaction_interval_secs: log.compaction_interval_secs,
            row_group_size: log.row_group_size,
        }
    }
}

/// Retained for compatibility but Config should be used.
pub struct RetentionConfig;
