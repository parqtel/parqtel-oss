use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    /// Compression level for codecs that support tuning (e.g. zstd 1-22).
    /// `None` uses the codec's built-in default.
    #[serde(default)]
    pub compression_level: Option<i32>,
    /// Data retention in days.
    pub retention_days: u64,
    /// Interval between compaction passes in seconds.
    pub compaction_interval_secs: u64,
    /// Number of rows per row group in Parquet files.
    pub row_group_size: usize,
    /// Maximum number of source blocks merged into one compacted block.
    ///
    /// Derived from block size rather than fixed: merging too few leaves the
    /// small-block population growing faster than compaction removes it, and
    /// merging too many makes one cycle's cost unbounded.
    #[serde(default = "default_compaction_max_merge_blocks")]
    pub compaction_max_merge_blocks: usize,
    /// Maximum number of merges a single compaction cycle performs.
    ///
    /// Bounds the work (and the disk I/O) one cycle can do, so compaction
    /// cannot monopolise the disk on a busy cluster.
    #[serde(default = "default_compaction_max_merges_per_pass")]
    pub compaction_max_merges_per_pass: usize,
}

fn default_backend() -> String {
    "parquet".into()
}

fn default_compaction_max_merge_blocks() -> usize {
    12
}

fn default_compaction_max_merges_per_pass() -> usize {
    8
}

impl Default for BlockConfig {
    fn default() -> Self {
        Self {
            backend: "parquet".into(),
            data_dir: PathBuf::from("data"),
            block_duration_secs: 7200,
            max_rows_per_block: 1_000_000,
            compression: "zstd".into(),
            compression_level: None,
            retention_days: 7,
            compaction_interval_secs: 3600,
            row_group_size: 100_000,
            compaction_max_merge_blocks: default_compaction_max_merge_blocks(),
            compaction_max_merges_per_pass: default_compaction_max_merges_per_pass(),
        }
    }
}

impl BlockConfig {
    /// Validates this block config on its own.
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        self.validate_into("storage", &mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Validation(errors.join("; ")))
        }
    }

    /// Appends any problems to `errors`, prefixed with `prefix`.
    ///
    /// Shared with [`LogBlockConfig`] via a common shape so the metrics and
    /// logs blocks cannot drift: the two carry the same knobs, and only the
    /// prefix differs.
    pub(crate) fn validate_into(&self, prefix: &str, errors: &mut Vec<String>) {
        if self.data_dir.as_os_str().is_empty() {
            errors.push(format!("{prefix}.data_dir must be a non-empty path"));
        }
        if !super::VALID_COMPRESSION_CODECS.contains(&self.compression.as_str()) {
            errors.push(format!(
                "{prefix}.compression must be one of: {}",
                super::VALID_COMPRESSION_CODECS.join(", ")
            ));
        }
        if let Some(level) = self.compression_level {
            super::validate_compression_level(
                level,
                &format!("{prefix}.compression_level"),
                errors,
            );
        }
        // `row_group_size` is the knob the scanner's row-group pruning depends
        // on: a row group larger than a whole block means pruning can never
        // help, which is a silent performance regression rather than an error.
        if self.row_group_size == 0 {
            errors.push(format!("{prefix}.row_group_size must be greater than 0"));
        } else if self.row_group_size > self.max_rows_per_block {
            errors.push(format!(
                "{prefix}.row_group_size ({}) must not exceed {prefix}.max_rows_per_block ({})",
                self.row_group_size, self.max_rows_per_block
            ));
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
    /// Compression level for codecs that support tuning (e.g. zstd 1-22).
    /// `None` uses the codec's built-in default.
    #[serde(default)]
    pub compression_level: Option<i32>,
    /// Data retention in days.
    pub retention_days: u64,
    /// Interval between compaction passes in seconds.
    pub compaction_interval_secs: u64,
    /// Number of rows per row group in Parquet files.
    pub row_group_size: usize,
    /// Maximum number of source blocks merged into one compacted block.
    #[serde(default = "default_compaction_max_merge_blocks")]
    pub compaction_max_merge_blocks: usize,
    /// Maximum number of merges a single compaction cycle performs.
    #[serde(default = "default_compaction_max_merges_per_pass")]
    pub compaction_max_merges_per_pass: usize,
}

impl Default for LogBlockConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/logs"),
            block_duration_secs: 1800,
            max_rows_per_block: 200_000,
            compression: "zstd".into(),
            compression_level: None,
            retention_days: 3,
            compaction_interval_secs: 3600,
            row_group_size: 20_000,
            compaction_max_merge_blocks: default_compaction_max_merge_blocks(),
            compaction_max_merges_per_pass: default_compaction_max_merges_per_pass(),
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
            compression_level: log.compression_level,
            retention_days: log.retention_days,
            compaction_interval_secs: log.compaction_interval_secs,
            row_group_size: log.row_group_size,
            compaction_max_merge_blocks: log.compaction_max_merge_blocks,
            compaction_max_merges_per_pass: log.compaction_max_merges_per_pass,
        }
    }
}

impl LogBlockConfig {
    /// Validates this log block config on its own.
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        self.validate_into("logs", &mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Validation(errors.join("; ")))
        }
    }

    /// Delegates to [`BlockConfig::validate_into`] so the logs block is held
    /// to exactly the same rules as the metrics block. Only the error prefix
    /// differs.
    pub(crate) fn validate_into(&self, prefix: &str, errors: &mut Vec<String>) {
        BlockConfig::from(self.clone()).validate_into(prefix, errors)
    }
}

/// Retained for compatibility but Config should be used.
pub struct RetentionConfig;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn defaults_are_valid() {
        assert!(BlockConfig::default().validate().is_ok());
        assert!(LogBlockConfig::default().validate().is_ok());
    }

    /// The log block config must be validated by exactly the same rules as
    /// the metrics one; these are the two places a rule could be added twice
    /// or forgotten.
    #[test]
    fn log_config_rejects_the_same_failures_as_metrics() {
        for bad in [
            LogBlockConfig {
                compression: "bogus".into(),
                ..Default::default()
            },
            LogBlockConfig {
                compression_level: Some(0),
                ..Default::default()
            },
            LogBlockConfig {
                row_group_size: 0,
                ..Default::default()
            },
            LogBlockConfig {
                row_group_size: usize::MAX,
                ..Default::default()
            },
            LogBlockConfig {
                data_dir: PathBuf::new(),
                ..Default::default()
            },
        ] {
            let err = bad.validate().unwrap_err().to_string();
            assert!(err.contains("logs."), "log error must be prefixed: {err}");
        }
    }

    #[test]
    fn log_to_block_conversion_preserves_compression_level() {
        let log = LogBlockConfig {
            compression_level: Some(3),
            ..Default::default()
        };
        assert_eq!(BlockConfig::from(log).compression_level, Some(3));
    }
}
