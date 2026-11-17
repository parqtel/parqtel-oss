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

use crate::error::{Error, Result};
pub use logs::{LogColumnPrefilter, LogRowFilter, LogScanStats};
pub use metrics::{metric_scan_indices, MetricScan, SeriesFilter, METRIC_SCAN_COLUMNS};
pub use pruning::{
    project_present, row_groups_in_range, row_groups_matching_metric, row_groups_matching_value,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// Process-wide block-scan admission budget for the 4 GiB UAT pod.
const MAX_CONCURRENT_BLOCK_SCANS: usize = 4;
/// Dedicated slot for alert evaluation, so a saturating query load cannot
/// starve the alert loop. This is one scan above the query budget on purpose:
/// the alternative (carving it out of four) reduces query throughput and trace
/// parallelism for every deployment.
const BACKGROUND_BLOCK_SCANS: usize = 1;
/// Hard cap on blocks scanned per query.
const MAX_BLOCKS: usize = 128;

/// Rows read per Arrow batch. The default (1024) makes per-batch setup - and,
/// for dictionary columns, per-batch resolution - a visible share of the work.
const SCAN_BATCH_ROWS: usize = 8192;

fn scan_admission() -> Arc<tokio::sync::Semaphore> {
    static ADMISSION: LazyLock<Arc<tokio::sync::Semaphore>> =
        LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_BLOCK_SCANS)));
    Arc::clone(&ADMISSION)
}

fn background_admission() -> Arc<tokio::sync::Semaphore> {
    static ADMISSION: LazyLock<Arc<tokio::sync::Semaphore>> =
        LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(BACKGROUND_BLOCK_SCANS)));
    Arc::clone(&ADMISSION)
}

#[derive(Default)]
struct ScanMetrics {
    active_slots: AtomicU64,
    background_active_slots: AtomicU64,
    background_skips: AtomicU64,
    low_priority_skips: AtomicU64,
    admission_wait_ns: AtomicU64,
    blocks: AtomicU64,
    footer_cache_hits: AtomicU64,
    footer_cache_misses: AtomicU64,
    footer_load_ns: AtomicU64,
    index_estimated_bytes: AtomicU64,
    rows_considered: AtomicU64,
    rows_returned: AtomicU64,
}

fn scan_metrics() -> &'static ScanMetrics {
    static METRICS: LazyLock<ScanMetrics> = LazyLock::new(ScanMetrics::default);
    &METRICS
}

/// Cumulative scanner counters exposed by `/metrics`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScanMetricsSnapshot {
    pub active_slots: u64,
    pub slot_limit: u64,
    pub background_active_slots: u64,
    pub background_slot_limit: u64,
    pub background_skips: u64,
    pub low_priority_skips: u64,
    pub admission_wait_ns: u64,
    pub blocks: u64,
    pub footer_cache_hits: u64,
    pub footer_cache_misses: u64,
    pub footer_cache_bytes: u64,
    pub footer_load_ns: u64,
    pub index_estimated_bytes: u64,
    pub rows_considered: u64,
    pub rows_returned: u64,
}

pub(crate) struct ScanPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    slots: u64,
}

impl Drop for ScanPermit {
    fn drop(&mut self) {
        scan_metrics()
            .active_slots
            .fetch_sub(self.slots, Ordering::Relaxed);
    }
}

/// Reserved slot for alert evaluation. Separate from the query pool so a
/// saturating query load cannot starve the alert loop.
pub(crate) struct BackgroundScanPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for BackgroundScanPermit {
    fn drop(&mut self) {
        scan_metrics()
            .background_active_slots
            .fetch_sub(1, Ordering::Relaxed);
    }
}

async fn acquire_scan_slot() -> Result<ScanPermit> {
    let started = std::time::Instant::now();
    let permit = scan_admission()
        .acquire_owned()
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    let metrics = scan_metrics();
    metrics
        .admission_wait_ns
        .fetch_add(duration_ns(started.elapsed()), Ordering::Relaxed);
    metrics.active_slots.fetch_add(1, Ordering::Relaxed);
    Ok(ScanPermit {
        _permit: permit,
        slots: 1,
    })
}

fn reserve_scan_slots(count: usize) -> Option<ScanPermit> {
    let permits = u32::try_from(count).ok()?;
    if permits == 0 {
        return None;
    }
    let permit = match scan_admission().try_acquire_many_owned(permits) {
        Ok(permit) => permit,
        Err(_) => {
            scan_metrics()
                .low_priority_skips
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    scan_metrics()
        .active_slots
        .fetch_add(u64::from(permits), Ordering::Relaxed);
    Some(ScanPermit {
        _permit: permit,
        slots: u64::from(permits),
    })
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn record_scan_block() {
    scan_metrics().blocks.fetch_add(1, Ordering::Relaxed);
}

fn record_footer_cache_hit() {
    scan_metrics()
        .footer_cache_hits
        .fetch_add(1, Ordering::Relaxed);
}

fn record_footer_cache_miss() {
    scan_metrics()
        .footer_cache_misses
        .fetch_add(1, Ordering::Relaxed);
}

fn record_footer_load(elapsed: Duration, estimated_bytes: usize) {
    let metrics = scan_metrics();
    metrics
        .footer_load_ns
        .fetch_add(duration_ns(elapsed), Ordering::Relaxed);
    metrics
        .index_estimated_bytes
        .fetch_add(estimated_bytes as u64, Ordering::Relaxed);
}

fn record_scan_output(rows_considered: u64, rows_returned: usize) {
    let metrics = scan_metrics();
    metrics
        .rows_considered
        .fetch_add(rows_considered, Ordering::Relaxed);
    metrics
        .rows_returned
        .fetch_add(rows_returned as u64, Ordering::Relaxed);
}

/// Reads and filters data points from Parquet files.
pub struct Scanner;

impl Scanner {
    /// Reserves one query scan slot without waiting, for best-effort background
    /// work that may compete with queries.
    pub fn try_reserve_scan_slot() -> Option<impl Send> {
        reserve_scan_slots(1)
    }

    /// Reserves the dedicated alert-evaluation slot without waiting. Alert
    /// evaluation must not consume the query pool, or a saturating dashboard
    /// load silently skips alert cycles.
    pub fn try_reserve_background_scan_slot() -> Option<impl Send> {
        match background_admission().try_acquire_owned() {
            Ok(permit) => {
                scan_metrics()
                    .background_active_slots
                    .fetch_add(1, Ordering::Relaxed);
                Some(BackgroundScanPermit { _permit: permit })
            }
            Err(_) => {
                scan_metrics()
                    .background_skips
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Reserves several process-wide scan slots without waiting.
    pub(crate) fn try_reserve_scan_slots(count: usize) -> Option<ScanPermit> {
        reserve_scan_slots(count)
    }

    /// Cumulative process-wide scanner counters.
    pub fn scan_metrics_snapshot() -> ScanMetricsSnapshot {
        let metrics = scan_metrics();
        ScanMetricsSnapshot {
            active_slots: metrics.active_slots.load(Ordering::Relaxed),
            slot_limit: MAX_CONCURRENT_BLOCK_SCANS as u64,
            background_active_slots: metrics.background_active_slots.load(Ordering::Relaxed),
            background_slot_limit: BACKGROUND_BLOCK_SCANS as u64,
            background_skips: metrics.background_skips.load(Ordering::Relaxed),
            low_priority_skips: metrics.low_priority_skips.load(Ordering::Relaxed),
            admission_wait_ns: metrics.admission_wait_ns.load(Ordering::Relaxed),
            blocks: metrics.blocks.load(Ordering::Relaxed),
            footer_cache_hits: metrics.footer_cache_hits.load(Ordering::Relaxed),
            footer_cache_misses: metrics.footer_cache_misses.load(Ordering::Relaxed),
            footer_cache_bytes: block::cached_bytes() as u64,
            footer_load_ns: metrics.footer_load_ns.load(Ordering::Relaxed),
            index_estimated_bytes: metrics.index_estimated_bytes.load(Ordering::Relaxed),
            rows_considered: metrics.rows_considered.load(Ordering::Relaxed),
            rows_returned: metrics.rows_returned.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests;
