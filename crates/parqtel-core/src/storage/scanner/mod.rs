//! Block scanning: reads metric, log and trace blocks and applies the pushed-down
//! predicates of a query.
//!
//! - `block` opens a block and caches its footer and indexes.
//! - `pruning` decides which row groups and rows a scan has to read.
//! - `metrics`, `logs` and `traces` hold the scans themselves, as methods
//!   of [`Scanner`].

mod block;
mod logs;
mod metrics;
mod pruning;
mod traces;

pub use logs::{LogColumnPrefilter, LogRowFilter, LogScanStats};
pub use metrics::{metric_scan_indices, MetricScan, SeriesFilter, METRIC_SCAN_COLUMNS};
pub use pruning::{
    project_present, row_groups_in_range, row_groups_matching_metric, row_groups_matching_value,
};

/// Max concurrent blocking scans; also bounds tokio blocking-pool usage.
const MAX_CONCURRENT: usize = 16;
/// Hard cap on blocks scanned per query.
const MAX_BLOCKS: usize = 128;

/// Rows read per Arrow batch. The default (1024) makes per-batch setup - and,
/// for dictionary columns, per-batch resolution - a visible share of the work.
const SCAN_BATCH_ROWS: usize = 8192;

/// Reads and filters data points from Parquet files.
pub struct Scanner;

#[cfg(test)]
mod tests;
