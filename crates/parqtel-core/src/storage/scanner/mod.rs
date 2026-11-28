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
use crate::models::storage::BlockMetadata;
pub use logs::{LogColumnPrefilter, LogRowFilter, LogScanStats};
pub use metrics::{
    metric_scan_indices, LimitedMetricScan, MetricScan, SeriesFilter, METRIC_SCAN_COLUMNS,
};
pub use pruning::{
    project_present, row_groups_in_range, row_groups_matching_metric, row_groups_matching_value,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

pub const SCAN_BLOCK_LEVELS: [&str; 7] =
    ["l0", "l1", "l2", "normalized", "other", "logs", "traces"];

fn block_level(path: &std::path::Path) -> &'static str {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if path
        .components()
        .any(|component| component.as_os_str() == "logs")
        || filename.starts_with("logs_")
    {
        "logs"
    } else if path
        .components()
        .any(|component| component.as_os_str() == "traces")
        || filename.starts_with("traces_")
    {
        "traces"
    } else if filename.starts_with("m_l0_") {
        "l0"
    } else if filename.starts_with("m_l1_") {
        "l1"
    } else if filename.starts_with("m_l2_") {
        "l2"
    } else if filename.starts_with("m_n_") {
        "normalized"
    } else {
        "other"
    }
}

fn block_level_index(path: &std::path::Path) -> usize {
    SCAN_BLOCK_LEVELS
        .iter()
        .position(|level| *level == block_level(path))
        .unwrap_or(4)
}

/// Scan admission and truncation limits, installed once at startup by
/// [`Scanner::configure_limits`]. Until then the defaults from [`QueryConfig`]
/// apply, so tests and the query examples need no startup wiring.
#[derive(Debug, Clone, Copy)]
struct ScanLimits {
    max_scan_blocks: usize,
    max_concurrent_scans: usize,
    compaction_scan_slots: usize,
}

impl ScanLimits {
    fn defaults() -> Self {
        Self {
            max_scan_blocks: crate::config::QueryConfig::DEFAULT_MAX_SCAN_BLOCKS,
            max_concurrent_scans: crate::config::QueryConfig::default_max_concurrent_scans(),
            compaction_scan_slots: crate::config::QueryConfig::DEFAULT_COMPACTION_SCAN_SLOTS,
        }
    }
}

/// Process-wide scan runtime: the configured limits and the three admission
/// pools derived from them. One cell keeps the pools and the limits they were
/// sized from impossible to disagree.
struct ScanRuntime {
    limits: ScanLimits,
    query: Arc<tokio::sync::Semaphore>,
    compaction: Arc<tokio::sync::Semaphore>,
    background: Arc<tokio::sync::Semaphore>,
}

static SCAN_RUNTIME: OnceLock<ScanRuntime> = OnceLock::new();

fn build_runtime(limits: ScanLimits) -> ScanRuntime {
    ScanRuntime {
        limits,
        query: Arc::new(tokio::sync::Semaphore::new(limits.max_concurrent_scans)),
        compaction: Arc::new(tokio::sync::Semaphore::new(limits.compaction_scan_slots)),
        background: Arc::new(tokio::sync::Semaphore::new(BACKGROUND_BLOCK_SCANS)),
    }
}

fn scan_runtime() -> &'static ScanRuntime {
    SCAN_RUNTIME.get_or_init(|| build_runtime(ScanLimits::defaults()))
}

fn scan_limits() -> ScanLimits {
    scan_runtime().limits
}

/// Blocks one query may scan before it is truncated.
pub(super) fn max_scan_blocks() -> usize {
    scan_limits().max_scan_blocks
}

/// Concurrent query scan slots; also the capacity hint for per-block task lists.
pub(super) fn scan_slot_limit() -> usize {
    scan_limits().max_concurrent_scans
}

fn install_runtime(
    cell: &'static OnceLock<ScanRuntime>,
    limits: ScanLimits,
) -> std::result::Result<(), &'static str> {
    cell.set(build_runtime(limits))
        .map_err(|_| "scan limits are already configured")
}

fn validate_limits(limits: ScanLimits) -> std::result::Result<(), &'static str> {
    if limits.max_scan_blocks == 0 {
        return Err("max_scan_blocks must be greater than 0");
    }
    if limits.max_concurrent_scans == 0 {
        return Err("max_concurrent_scans must be greater than 0");
    }
    if limits.compaction_scan_slots == 0 {
        return Err("compaction_scan_slots must be greater than 0");
    }
    Ok(())
}

/// Dedicated slot for alert evaluation, so a saturating query load cannot
/// starve the alert loop. This is one scan above the query budget on purpose:
/// the alternative (carving it out of the configured pool) reduces query
/// throughput and trace parallelism for every deployment.
const BACKGROUND_BLOCK_SCANS: usize = 1;

/// Rows read per Arrow batch. The default (1024) makes per-batch setup - and,
/// for dictionary columns, per-batch resolution - a visible share of the work.
const SCAN_BATCH_ROWS: usize = 8192;

fn scan_admission() -> Arc<tokio::sync::Semaphore> {
    Arc::clone(&scan_runtime().query)
}

fn background_admission() -> Arc<tokio::sync::Semaphore> {
    Arc::clone(&scan_runtime().background)
}

fn compaction_admission() -> Arc<tokio::sync::Semaphore> {
    Arc::clone(&scan_runtime().compaction)
}

/// Concurrent footer prefetches. Independent of the query and background pools:
/// a prefetch does only I/O and holds no scan slot, so it must neither wait for
/// one nor consume one.
const PREFETCH_BLOCK_SCANS: usize = 16;

fn prefetch_admission() -> Arc<tokio::sync::Semaphore> {
    static ADMISSION: LazyLock<Arc<tokio::sync::Semaphore>> =
        LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(PREFETCH_BLOCK_SCANS)));
    Arc::clone(&ADMISSION)
}

/// Whether a scan should prefetch these blocks' footers.
///
/// A warm cache needs no prefetch: the decode's own `open_block` is a cache
/// hit, so prefetching would only add opens. A single block gains nothing from
/// parallelism either, so it goes straight to the decode phase.
fn prefetch_needed(blocks: &[BlockMetadata]) -> bool {
    blocks.len() >= 2 && !blocks.iter().all(|block| block::footer_cached(&block.path))
}

/// Warms the footer cache for the blocks a scan is about to decode.
///
/// A cold `open_block` (file open, footer read, index parse) otherwise runs
/// inside the `spawn_blocking` task that holds a scan slot, occupying it for
/// I/O that decodes nothing. Here it runs before any slot is acquired, bounded
/// by [`PREFETCH_BLOCK_SCANS`], and the opened handle is dropped immediately.
///
/// Prefetch failures are ignored on purpose: the decode phase reopens each
/// block and reports the real error, so prefetching can never turn a readable
/// query into an error.
async fn prefetch_block_footers(blocks: &[BlockMetadata]) {
    if !prefetch_needed(blocks) {
        return;
    }
    let admission = prefetch_admission();
    let mut tasks = Vec::with_capacity(blocks.len().min(PREFETCH_BLOCK_SCANS));
    for block in blocks {
        let path = block.path.clone();
        let permit = match admission.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        tasks.push(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _ = block::open_block(&path);
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
}

#[derive(Default)]
struct ScanMetrics {
    active_slots: AtomicU64,
    background_active_slots: AtomicU64,
    background_skips: AtomicU64,
    compaction_active_slots: AtomicU64,
    compaction_skips: AtomicU64,
    low_priority_skips: AtomicU64,
    admission_wait_ns: AtomicU64,
    blocks: AtomicU64,
    footer_cache_hits: AtomicU64,
    footer_cache_misses: AtomicU64,
    footer_load_ns: AtomicU64,
    index_estimated_bytes: AtomicU64,
    blocks_by_level: [AtomicU64; SCAN_BLOCK_LEVELS.len()],
    footer_cache_hits_by_level: [AtomicU64; SCAN_BLOCK_LEVELS.len()],
    footer_cache_misses_by_level: [AtomicU64; SCAN_BLOCK_LEVELS.len()],
    footer_load_ns_by_level: [AtomicU64; SCAN_BLOCK_LEVELS.len()],
    index_estimated_bytes_by_level: [AtomicU64; SCAN_BLOCK_LEVELS.len()],
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
    pub compaction_active_slots: u64,
    pub compaction_slot_limit: u64,
    pub compaction_skips: u64,
    pub low_priority_skips: u64,
    pub admission_wait_ns: u64,
    pub blocks: u64,
    pub footer_cache_hits: u64,
    pub footer_cache_misses: u64,
    pub footer_cache_bytes: u64,
    pub footer_cache_budget_bytes: u64,
    pub footer_load_ns: u64,
    pub index_estimated_bytes: u64,
    pub blocks_by_level: [u64; SCAN_BLOCK_LEVELS.len()],
    pub footer_cache_hits_by_level: [u64; SCAN_BLOCK_LEVELS.len()],
    pub footer_cache_misses_by_level: [u64; SCAN_BLOCK_LEVELS.len()],
    pub footer_load_ns_by_level: [u64; SCAN_BLOCK_LEVELS.len()],
    pub index_estimated_bytes_by_level: [u64; SCAN_BLOCK_LEVELS.len()],
    pub rows_considered: u64,
    pub rows_returned: u64,
}

impl ScanMetricsSnapshot {
    pub const BLOCK_LEVELS: [&'static str; SCAN_BLOCK_LEVELS.len()] = SCAN_BLOCK_LEVELS;
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

/// Reserved slot from compaction's own pool. Background compaction acquires
/// these instead of query slots, so a busy dashboard cannot defer compaction
/// and a long merge cannot starve queries.
pub(crate) struct CompactionScanPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    slots: u64,
}

impl Drop for CompactionScanPermit {
    fn drop(&mut self) {
        scan_metrics()
            .compaction_active_slots
            .fetch_sub(self.slots, Ordering::Relaxed);
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

fn reserve_compaction_slots(count: usize) -> Option<CompactionScanPermit> {
    let permits = u32::try_from(count).ok()?;
    if permits == 0 {
        return None;
    }
    let permit = match compaction_admission().try_acquire_many_owned(permits) {
        Ok(permit) => permit,
        Err(_) => {
            scan_metrics()
                .compaction_skips
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    scan_metrics()
        .compaction_active_slots
        .fetch_add(u64::from(permits), Ordering::Relaxed);
    Some(CompactionScanPermit {
        _permit: permit,
        slots: u64::from(permits),
    })
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn record_scan_block(path: &std::path::Path) {
    let metrics = scan_metrics();
    metrics.blocks.fetch_add(1, Ordering::Relaxed);
    metrics.blocks_by_level[block_level_index(path)].fetch_add(1, Ordering::Relaxed);
}

fn record_footer_cache_hit(path: &std::path::Path) {
    let metrics = scan_metrics();
    let level = block_level_index(path);
    metrics.footer_cache_hits.fetch_add(1, Ordering::Relaxed);
    metrics.footer_cache_hits_by_level[level].fetch_add(1, Ordering::Relaxed);
}

fn record_footer_cache_miss(path: &std::path::Path) {
    let metrics = scan_metrics();
    let level = block_level_index(path);
    metrics.footer_cache_misses.fetch_add(1, Ordering::Relaxed);
    metrics.footer_cache_misses_by_level[level].fetch_add(1, Ordering::Relaxed);
}

fn record_footer_load(path: &std::path::Path, elapsed: Duration, estimated_bytes: usize) {
    let metrics = scan_metrics();
    let level = block_level_index(path);
    let elapsed_ns = duration_ns(elapsed);
    metrics
        .footer_load_ns
        .fetch_add(elapsed_ns, Ordering::Relaxed);
    metrics
        .index_estimated_bytes
        .fetch_add(estimated_bytes as u64, Ordering::Relaxed);
    metrics.footer_load_ns_by_level[level].fetch_add(elapsed_ns, Ordering::Relaxed);
    metrics.index_estimated_bytes_by_level[level]
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
    /// Installs the process-wide footer cache budget before the first scan.
    pub fn configure_footer_cache_budget(bytes: usize) -> std::result::Result<(), &'static str> {
        block::configure_footer_cache_budget(bytes)
    }

    /// Installs the process-wide scan limits. Call once at startup, before any
    /// scan: the admission pools are sized here, so a later call fails rather
    /// than leaving the pools and limits disagreeing.
    pub fn configure_limits(
        max_scan_blocks: usize,
        max_concurrent_scans: usize,
        compaction_scan_slots: usize,
    ) -> std::result::Result<(), &'static str> {
        let limits = ScanLimits {
            max_scan_blocks,
            max_concurrent_scans,
            compaction_scan_slots,
        };
        validate_limits(limits)?;
        install_runtime(&SCAN_RUNTIME, limits)
    }

    /// Reserves `count` slots from compaction's own scan budget without waiting.
    /// Background compaction must use these instead of query slots so neither
    /// workload starves the other.
    pub fn try_reserve_compaction_slots(count: usize) -> Option<impl Send> {
        reserve_compaction_slots(count)
    }

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

    /// Cumulative process-wide scanner counters.
    pub fn scan_metrics_snapshot() -> ScanMetricsSnapshot {
        let metrics = scan_metrics();
        let limits = scan_limits();
        ScanMetricsSnapshot {
            active_slots: metrics.active_slots.load(Ordering::Relaxed),
            slot_limit: limits.max_concurrent_scans as u64,
            background_active_slots: metrics.background_active_slots.load(Ordering::Relaxed),
            background_slot_limit: BACKGROUND_BLOCK_SCANS as u64,
            background_skips: metrics.background_skips.load(Ordering::Relaxed),
            compaction_active_slots: metrics.compaction_active_slots.load(Ordering::Relaxed),
            compaction_slot_limit: limits.compaction_scan_slots as u64,
            compaction_skips: metrics.compaction_skips.load(Ordering::Relaxed),
            low_priority_skips: metrics.low_priority_skips.load(Ordering::Relaxed),
            admission_wait_ns: metrics.admission_wait_ns.load(Ordering::Relaxed),
            blocks: metrics.blocks.load(Ordering::Relaxed),
            footer_cache_hits: metrics.footer_cache_hits.load(Ordering::Relaxed),
            footer_cache_misses: metrics.footer_cache_misses.load(Ordering::Relaxed),
            footer_cache_bytes: block::cached_bytes() as u64,
            footer_cache_budget_bytes: block::cache_budget_bytes() as u64,
            footer_load_ns: metrics.footer_load_ns.load(Ordering::Relaxed),
            index_estimated_bytes: metrics.index_estimated_bytes.load(Ordering::Relaxed),
            blocks_by_level: std::array::from_fn(|i| {
                metrics.blocks_by_level[i].load(Ordering::Relaxed)
            }),
            footer_cache_hits_by_level: std::array::from_fn(|i| {
                metrics.footer_cache_hits_by_level[i].load(Ordering::Relaxed)
            }),
            footer_cache_misses_by_level: std::array::from_fn(|i| {
                metrics.footer_cache_misses_by_level[i].load(Ordering::Relaxed)
            }),
            footer_load_ns_by_level: std::array::from_fn(|i| {
                metrics.footer_load_ns_by_level[i].load(Ordering::Relaxed)
            }),
            index_estimated_bytes_by_level: std::array::from_fn(|i| {
                metrics.index_estimated_bytes_by_level[i].load(Ordering::Relaxed)
            }),
            rows_considered: metrics.rows_considered.load(Ordering::Relaxed),
            rows_returned: metrics.rows_returned.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests;
