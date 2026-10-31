use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, SignalType};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// In-memory index of all Parquet blocks on disk.
///
/// Deliberately free of filesystem I/O. The index is read by every query
/// handler and written by the background index task, so persisting from inside
/// a mutation made every flush pay an O(total index size) serialise plus a
/// blocking `write` + `rename` **while holding the write lock**. The caller
/// now mutates in memory only and hands persistence to [`BlockIndexStore`],
/// which debounces it onto the blocking pool.
///
/// Ordering invariant: `blocks` is kept sorted by `start_timestamp_ns` after
/// every mutation, which [`Self::query`]'s binary search relies on.
pub struct BlockIndex {
    pub blocks: Vec<BlockMetadata>,
    sidecar_path: PathBuf,
}

impl BlockIndex {
    /// Creates a new [BlockIndex] for the given data directory.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            blocks: Vec::new(),
            sidecar_path: data_dir.join("index.json"),
        }
    }

    /// Loads the index from the JSON sidecar file.
    pub fn load(&mut self) -> Result<()> {
        if self.sidecar_path.exists() {
            let content = fs::read_to_string(&self.sidecar_path)?;
            self.blocks = serde_json::from_str(&content).map_err(Error::Serde)?;
        }
        self.blocks.sort_by_key(|b| b.start_timestamp_ns);
        Ok(())
    }

    /// Path of the JSON sidecar this index persists to.
    pub fn sidecar_path(&self) -> &Path {
        &self.sidecar_path
    }

    /// Serialises the index payload. Pure CPU, no filesystem access, so it is
    /// safe to call while holding a lock — though a *read* lock is preferred.
    pub fn serialize(&self) -> Result<String> {
        serde_json::to_string(&self.blocks).map_err(Error::Serde)
    }

    /// Restores a previously serialised payload, dropping the in-memory
    /// index. Used to publish a payload serialised off-lock.
    pub fn load_serialized(&mut self, payload: &str) -> Result<()> {
        self.blocks = serde_json::from_str(payload).map_err(Error::Serde)?;
        self.blocks.sort_by_key(|b| b.start_timestamp_ns);
        Ok(())
    }

    /// Adds a new block to the index. **Does not persist** — see
    /// [BlockIndexStore].
    ///
    /// The vector is sorted in place, which is what keeps [`Self::query`]'s
    /// `partition_point` correct. The sort is O(n log n) on an already sorted
    /// vector for all but the inserted element in practice; a bsearch insert
    /// would be marginal here and is left as a follow-up.
    pub fn add(&mut self, meta: BlockMetadata) {
        self.blocks.push(meta);
        self.blocks.sort_by_key(|b| b.start_timestamp_ns);
    }

    /// Removes a block from the index. **Does not persist.**
    pub fn remove(&mut self, path: &Path) {
        self.blocks.retain(|b| b.path != path);
    }

    /// Replaces a set of blocks with `new`, dropping any whose path is in
    /// `removed_paths`. Used by compaction and retention to publish a change
    /// in a single swap instead of many `retain` scans.
    pub fn replace(&mut self, removed_paths: &[PathBuf], new: Option<BlockMetadata>) {
        if !removed_paths.is_empty() {
            self.blocks.retain(|b| !removed_paths.contains(&b.path));
        }
        if let Some(meta) = new {
            self.blocks.push(meta);
        }
        self.blocks.sort_by_key(|b| b.start_timestamp_ns);
    }

    /// Finds blocks overlapping a time range, optionally filtered by metric name.
    pub fn query(
        &self,
        start_ns: i64,
        end_ns: i64,
        metric_name: Option<&str>,
    ) -> Vec<BlockMetadata> {
        let start_idx = self
            .blocks
            .partition_point(|b| b.end_timestamp_ns < start_ns);
        self.blocks[start_idx..]
            .iter()
            .take_while(|b| b.start_timestamp_ns <= end_ns)
            .filter(|b| {
                // An **empty** `metric_names` means "this block's metrics are not
                // known", not "it has none": a block recovered by `reconcile`
                // can only learn the min and max from its footer, so listing
                // just those two would make every other metric in the block
                // unfindable. Treating empty as unknown keeps the block
                // visible and only loses the name-based pruning until the block
                // is rewritten by compaction.
                metric_name
                    .as_ref()
                    .is_none_or(|n| b.metric_names.is_empty() || b.metric_names.contains(*n))
            })
            .cloned()
            .collect()
    }

    /// Number of blocks per signal type, for `/metrics` and `/api/v1/stats`.
    pub fn blocks_by_signal(&self) -> [usize; 3] {
        let mut counts = [0usize; 3];
        for b in &self.blocks {
            counts[b.signal_type.index()] += 1;
        }
        counts
    }

    pub fn total_blocks(&self) -> usize {
        self.blocks.len()
    }
    pub fn total_rows(&self) -> usize {
        self.blocks.iter().map(|b| b.row_count).sum()
    }
    pub fn total_bytes(&self) -> u64 {
        self.blocks.iter().map(|b| b.size_bytes).sum()
    }
    pub fn all_metrics(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        for b in &self.blocks {
            names.extend(b.metric_names.iter().cloned());
        }
        names
    }

    pub fn all_labels(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        for b in &self.blocks {
            names.extend(b.label_names.iter().cloned());
        }
        names
    }
}

/// Moves `signal`'s WAL commit point forward over blocks that are already on
/// disk, and returns it.
///
/// A block becomes durable (renamed, indexed) before the WAL commit that records
/// it, so a crash in between leaves rows both in a block and in the WAL. Each
/// block carries the WAL span it covers; following the chain of those spans from
/// the commit point finds that case, and replay then skips what the block holds.
/// Call after [`reconcile`], so blocks adopted from the directory count too.
pub fn advance_wal_commit(
    data_dir: &Path,
    signal: &str,
    index: &BlockIndex,
) -> crate::wal::WalPosition {
    let commit = crate::wal::read_commit(data_dir, signal);
    let ranges: Vec<(u64, u64)> = index.blocks.iter().filter_map(|b| b.wal_range).collect();
    let through = crate::wal::covered_through(commit, &ranges);
    if through > commit {
        match crate::wal::commit_position(data_dir, signal, through) {
            Ok(()) => tracing::warn!(
                signal,
                from = %commit,
                to = %through,
                "blocks already on disk cover part of the WAL; it will not be replayed again"
            ),
            Err(e) => tracing::warn!(signal, error = %e, "could not advance the WAL commit point"),
        }
    }
    through
}

/// What a reconcile pass found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileStats {
    /// Block files present on disk but absent from the index, adopted.
    pub adopted: usize,
    /// Index entries whose file no longer exists, dropped.
    pub dropped: usize,
    /// Footers actually read. Zero when the index already matched the disk,
    /// which is the normal case — the common path is one `read_dir`.
    pub scanned: usize,
    /// Files whose footer could not be read.
    pub unreadable: usize,
}

/// Whether a filename suggests it could hold `signal`.
///
/// A cheap pre-filter only. See the caller for why it is safe: the schema is
/// still checked before anything is adopted.
fn filename_may_hold_signal(path: &Path, signal: SignalType) -> bool {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    match signal {
        SignalType::Metrics => !(name.starts_with("logs_") || name.starts_with("traces_")),
        SignalType::Logs => name.starts_with("logs_"),
        SignalType::Traces => name.starts_with("traces_"),
    }
}

/// Identifies which signal a Parquet file holds, from its schema.
///
/// The data directory is shared by the metrics and trace writers, and relying
/// on the filename prefix to tell them apart would break on a rename or an older
/// naming scheme. Each schema has a column nothing else has, so the footer is
/// authoritative.
fn detect_signal(schema: &parquet::schema::types::SchemaDescriptor) -> Option<SignalType> {
    let names: Vec<&str> = schema.columns().iter().map(|c| c.name()).collect();
    // Order matters, and it is the reverse of what you would guess: the log
    // schema has a `span_id` column too (OTLP logs are correlated to spans), so
    // testing for `span_id` first classifies log blocks as traces. These are
    // each checked for a column only that signal's schema has:
    // `severity_text` only logs, `metric_kind` only metrics, `start_time_ns`
    // only traces (logs use `timestamp_ns`).
    if names.contains(&"severity_text") {
        Some(SignalType::Logs)
    } else if names.contains(&"metric_kind") {
        Some(SignalType::Metrics)
    } else if names.contains(&"start_time_ns") {
        Some(SignalType::Traces)
    } else {
        None
    }
}

/// Rebuilds missing index entries from the block files on disk, and drops
/// entries whose file has gone.
///
/// The sidecar is a cache of on-disk state, so any crash, restore-from-backup or
/// manual deletion can leave it disagreeing with the directory. Reading back only
/// from the sidecar means a lost index silently hides blocks that are present and
/// perfectly readable. This walks the difference in both directions.
///
/// Cost is one `read_dir` when the index already agrees with the disk, which is
/// the normal case; footers are parsed only for files the index does not know
/// about.
///
/// Two things cannot be recovered from a footer and come back empty:
///
/// - `label_names` / `label_values`. These are built at flush time and stored
///   only in the sidecar, so a recovered block loses its autocomplete entry
///   until it is rewritten by compaction. Queries are unaffected; only
///   `/api/v1/label/:name/values` degrades for that block.
/// - the set of `metric_names`. The column's min/max statistics describe a
///   *range*, not the names present, and `query` tests set membership — so
///   recording just those two would hide every other metric in the block.
///   Recovered blocks therefore carry an **empty** set, which `query` treats as
///   "unknown" so the block stays visible. Name-based pruning is lost until
///   compaction rewrites the block with a real flush-time index.
pub fn reconcile(index: &mut BlockIndex, signal: SignalType) -> Result<ReconcileStats> {
    let mut stats = ReconcileStats::default();
    let Some(dir) = index.sidecar_path().parent() else {
        return Ok(stats);
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(stats);
    };

    let mut on_disk: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "parquet") {
            on_disk.insert(path);
        }
    }

    // Drop entries whose file is gone (deleted by retention or compaction that
    // crashed before publishing).
    let before = index.blocks.len();
    index.blocks.retain(|b| on_disk.contains(&b.path));
    stats.dropped = before - index.blocks.len();

    // Adopt files the index does not know about.
    let known: std::collections::HashSet<PathBuf> =
        index.blocks.iter().map(|b| b.path.clone()).collect();
    for path in on_disk {
        if known.contains(&path) {
            continue;
        }
        // Cheap pre-filter before touching the file. Without it every startup
        // re-parses the footer of every *other* signal's blocks, since metrics
        // and traces share a directory. The filename is only a hint here; the
        // schema below remains the authority, so a wrong name can cause a block
        // to be missed (a lost recovery) but never a wrong adoption.
        if !filename_may_hold_signal(&path, signal) {
            continue;
        }
        stats.scanned += 1;
        match read_block_metadata(&path, signal) {
            Ok(Some(meta)) => {
                index.blocks.push(meta);
                stats.adopted += 1;
            }
            Ok(None) => {}
            Err(_) => stats.unreadable += 1,
        }
    }

    // `query` binary-searches on this ordering, so it must hold afterwards.
    index.blocks.sort_by_key(|b| b.start_timestamp_ns);
    Ok(stats)
}

/// Parses `{start}_{end}_{uuid}.parquet` (the metrics/logs/trace naming), or
/// the `logs_`/`traces_` prefixed variants.
///
/// A last resort for a block whose timestamp statistics are missing, so a
/// recovered block is not left claiming a zero-width range.
fn timestamps_from_filename(path: &Path) -> Option<(i64, i64)> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".parquet")?;
    let stem = stem
        .strip_prefix("logs_")
        .or_else(|| stem.strip_prefix("traces_"))
        .unwrap_or(stem);
    let mut parts = stem.split('_');
    let start = parts.next()?.parse().ok()?;
    let end = parts.next()?.parse().ok()?;
    Some((start, end))
}

/// Reads what a block's footer can tell us, or `None` if it is not this signal.
fn read_block_metadata(path: &Path, signal: SignalType) -> Result<Option<BlockMetadata>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| crate::error::Error::Parquet(e.to_string()))?;
    let md = builder.metadata().clone();
    let schema = md.file_metadata().schema_descr();
    if detect_signal(schema) != Some(signal) {
        return Ok(None);
    }

    let size_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let mut meta = BlockMetadata {
        path: path.to_path_buf(),
        start_timestamp_ns: 0,
        end_timestamp_ns: 0,
        // A single record batch per row group, so summing the row groups'
        // row counts is the file's row count.
        row_count: (0..md.num_row_groups())
            .map(|rg| md.row_group(rg).num_rows() as usize)
            .sum(),
        size_bytes,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: signal,
        // A block adopted without an index entry still says what it covers.
        wal_range: md
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| {
                kv.iter()
                    .find(|e| e.key == crate::wal::WAL_RANGE_FOOTER_KEY)
                    .and_then(|e| e.value.as_deref())
            })
            .and_then(crate::wal::decode_wal_range),
    };

    let columns = schema.columns();
    // Aggregate across **every** row group. Reading only row group 0 makes a
    // recovered block claim to end where its first group ends, which prunes it
    // for any later query - the block looks present in the index and silently
    // returns nothing.
    let column_range = |name: &str| -> Option<(i64, i64)> {
        let idx = columns.iter().position(|c| c.name() == name)?;
        let mut lo = i64::MAX;
        let mut hi = i64::MIN;
        let mut seen = false;
        for rg in 0..md.num_row_groups() {
            let Some(stats) = md.row_group(rg).column(idx).statistics() else {
                continue;
            };
            let (Some(min), Some(max)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) else {
                continue;
            };
            let (Some(min_bytes), Some(max_bytes)) = (min.get(..8), max.get(..8)) else {
                continue;
            };
            let (Ok(min_arr), Ok(max_arr)) = (
                <[u8; 8]>::try_from(min_bytes),
                <[u8; 8]>::try_from(max_bytes),
            ) else {
                continue;
            };
            let (min, max) = (i64::from_le_bytes(min_arr), i64::from_le_bytes(max_arr));
            lo = lo.min(min);
            hi = hi.max(max);
            seen = true;
        }
        seen.then_some((lo, hi))
    };

    // Metrics sort by `timestamp_ns`; trace rows carry `start_time_ns` instead.
    if let Some((lo, hi)) = column_range("timestamp_ns").or_else(|| column_range("start_time_ns")) {
        meta.start_timestamp_ns = lo;
        meta.end_timestamp_ns = hi;
    } else {
        // No usable statistics: fall back to the filename, which encodes
        // `{start}_{end}_...`, so the block at least gets a sane range.
        if let Some((start, end)) = timestamps_from_filename(path) {
            meta.start_timestamp_ns = start;
            meta.end_timestamp_ns = end;
        }
    }

    // `metric_names` is deliberately left **empty**. The metric_name column's
    // statistics describe the lexicographic min and max, which is a *range*,
    // not the set of names present - and `query` tests set membership, so
    // recording just those two would make every other metric in the block
    // unfindable. Empty means "unknown", which keeps the block visible; the
    // name-based pruning returns when compaction rewrites the block with a real
    // flush-time index.
    let _ = signal;

    Ok(Some(meta))
}

/// A block being handed to the index, plus an optional signal to fire once
/// the sidecar containing it is **durable**.
///
/// The WAL contract requires the index to be persisted before the WAL is
/// committed for those rows. Without that ordering, a crash in the window
/// between the two leaves a block on disk that is absent from the sidecar and
/// already committed in the WAL - so nothing replays it and no query can find
/// it.
///
/// `durable` is `None` when no WAL is attached: there is no ordering
/// requirement, so the sender does not wait.
#[derive(Debug)]
pub struct PendingIndex {
    pub meta: BlockMetadata,
    /// Fired by the index task once the sidecar write has completed.
    pub durable: Option<tokio::sync::oneshot::Sender<()>>,
}

impl PendingIndex {
    /// A block with no durability handshake: the caller is not waiting, so
    /// there is nothing to order against.
    pub fn untracked(meta: BlockMetadata) -> Self {
        Self {
            meta,
            durable: None,
        }
    }

    /// A block whose sender must wait for the sidecar to become durable.
    pub fn tracked(meta: BlockMetadata) -> (Self, tokio::sync::oneshot::Receiver<()>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            Self {
                meta,
                durable: Some(tx),
            },
            rx,
        )
    }
}

impl From<BlockMetadata> for PendingIndex {
    fn from(meta: BlockMetadata) -> Self {
        Self::untracked(meta)
    }
}

/// Owns persistence of a [`BlockIndex`] sidecar.
///
/// Mutations mark the store dirty; a background pass serialises under a
/// **read** lock (so queries are never blocked) and writes on the blocking
/// pool. This turns an O(total index size) serialise + blocking write pair per
/// flush into an amortised pass, and takes it off both the index write lock
/// and the tokio worker threads.
#[derive(Debug)]
pub struct BlockIndexStore {
    path: PathBuf,
    /// Set when the in-memory index has diverged from the sidecar.
    dirty: std::sync::atomic::AtomicBool,
    /// Payload captured by the last completed pass, so a no-op pass costs
    /// nothing and shutdown can tell whether a final write is needed.
    last_payload: std::sync::Mutex<Option<String>>,
}

impl BlockIndexStore {
    /// Creates a store for the sidecar belonging to `index`.
    pub fn new(index: &BlockIndex) -> Self {
        Self::at_path(index.sidecar_path().to_path_buf())
    }

    /// Creates a store for an explicit sidecar path.
    ///
    /// Lets a caller build the store while it still holds the `BlockIndex`
    /// directly, which keeps the sidecar path defined in exactly one place.
    pub fn at_path(path: PathBuf) -> Self {
        Self {
            path,
            dirty: std::sync::atomic::AtomicBool::new(false),
            last_payload: std::sync::Mutex::new(None),
        }
    }

    /// Path of the sidecar this store writes.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Records that the in-memory index changed.
    ///
    /// Cheap and synchronous, so it can be called while the write lock is
    /// held: it only flips a flag.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Whether a persistence pass has work to do.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Takes the dirty flag, returning whether it was set.
    ///
    /// The caller is expected to capture a payload immediately afterwards and
    /// call [`Self::complete`] with it, so a concurrent [`Self::mark_dirty`]
    /// between the two is not lost.
    pub fn begin(&self) -> bool {
        self.dirty.swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    /// Records a successfully written payload and clears any dirtiness that
    /// happened while it was in flight.
    pub fn complete(&self, payload: String) {
        if let Ok(mut slot) = self.last_payload.lock() {
            *slot = Some(payload);
        }
    }

    /// Whether the sidecar has ever been written by this store.
    pub fn has_persisted(&self) -> bool {
        self.last_payload
            .lock()
            .map(|s| s.is_some())
            .unwrap_or(false)
    }

    /// Writes `payload` to the sidecar atomically (tmp file + rename).
    ///
    /// Blocking. Call from `spawn_blocking` or a startup path — never from an
    /// async task on a worker thread.
    pub fn write_payload(&self, payload: &str) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, payload)?;
        fs::rename(tmp_path, &self.path)?;
        Ok(())
    }

    /// Size of the sidecar on disk, 0 when absent. Used for the
    /// `parqtel_index_bytes` gauge.
    pub fn size_bytes(&self) -> u64 {
        fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests;
