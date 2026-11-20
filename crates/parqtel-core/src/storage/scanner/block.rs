//! Opening a block: the footer cache and the indexes lifted out of a footer.

use crate::error::{Error, Result};
use crate::storage::log_index::{LogIndexTable, LogSkipIndex, LOG_INDEX_FOOTER_KEY};
use crate::storage::series_index::{SeriesIndex, SeriesTable, SERIES_INDEX_FOOTER_KEY};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Weak};

/// What is kept per block file: its parsed footer (with page indexes) and, for
/// metric blocks, the queryable series table that footer carried.
///
/// Block files are immutable once renamed into place, so a footer never goes
/// stale for a given (path, length, mtime). Re-reading and re-decoding it for
/// every block of every query was a fixed cost paid before any data moved - and
/// the series table is costlier still to rebuild (JSON parse, label interning,
/// postings), so it is parsed once and shared.
struct CachedBlock {
    meta: Arc<ParquetMetaData>,
    series: Option<Arc<SeriesIndex>>,
    log_index: Option<Arc<LogSkipIndex>>,
    bytes: usize,
    used: u64,
}

type BlockKey = (std::path::PathBuf, u64, i128);
type CachedBlockParts = (
    Arc<ParquetMetaData>,
    Option<Arc<SeriesIndex>>,
    Option<Arc<LogSkipIndex>>,
);

struct FooterCache {
    entries: HashMap<BlockKey, CachedBlock>,
    bytes: usize,
    budget_bytes: usize,
    started: bool,
    configured: bool,
    tick: u64,
}

/// Memory the footer cache may hold, counted in bytes rather than entries: a
/// footer with page indexes and a series table for a hundred thousand series is
/// orders of magnitude larger than one for a small log block.
const LOAD_LOCK_CLEANUP_THRESHOLD: usize = 1024;

fn footer_cache() -> &'static std::sync::Mutex<FooterCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<FooterCache>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        std::sync::Mutex::new(FooterCache {
            entries: HashMap::new(),
            bytes: 0,
            budget_bytes: crate::config::QueryConfig::DEFAULT_FOOTER_CACHE_BYTES,
            started: false,
            configured: false,
            tick: 0,
        })
    })
}

pub(super) fn configure_footer_cache_budget(bytes: usize) -> std::result::Result<(), &'static str> {
    if bytes == 0 {
        return Err("footer cache budget must be greater than 0");
    }
    let mut cache = footer_cache().lock().unwrap_or_else(|e| e.into_inner());
    if cache.configured {
        return Err("footer cache budget is already configured");
    }
    if cache.started {
        return Err("footer cache budget must be configured before the first scan");
    }
    cache.budget_bytes = bytes;
    cache.configured = true;
    Ok(())
}

fn block_load_locks() -> &'static std::sync::Mutex<HashMap<BlockKey, Weak<std::sync::Mutex<()>>>> {
    static LOCKS: std::sync::OnceLock<
        std::sync::Mutex<HashMap<BlockKey, Weak<std::sync::Mutex<()>>>>,
    > = std::sync::OnceLock::new();
    LOCKS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn block_load_lock(key: &BlockKey) -> Arc<std::sync::Mutex<()>> {
    let mut locks = block_load_locks().lock().unwrap_or_else(|e| e.into_inner());
    if locks.len() >= LOAD_LOCK_CLEANUP_THRESHOLD {
        locks.retain(|_, lock| lock.strong_count() > 0);
    }
    if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(std::sync::Mutex::new(()));
    locks.insert(key.clone(), Arc::downgrade(&lock));
    lock
}

fn cached_block(key: &BlockKey) -> Option<CachedBlockParts> {
    let mut cache = footer_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache.started = true;
    cache.tick += 1;
    let tick = cache.tick;
    cache.entries.get_mut(key).map(|entry| {
        entry.used = tick;
        (
            Arc::clone(&entry.meta),
            entry.series.clone(),
            entry.log_index.clone(),
        )
    })
}

pub(super) fn cached_bytes() -> usize {
    footer_cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .bytes
}

pub(super) fn cache_budget_bytes() -> usize {
    footer_cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .budget_bytes
}

impl FooterCache {
    /// Inserts `block`, evicting the least recently used entries until the cache
    /// is back under budget. An entry larger than the whole budget is not kept.
    pub(super) fn insert(&mut self, key: BlockKey, mut block: CachedBlock) {
        self.started = true;
        if block.bytes > self.budget_bytes {
            return;
        }
        self.tick += 1;
        block.used = self.tick;
        let added = block.bytes;
        if let Some(old) = self.entries.insert(key, block) {
            self.bytes -= old.bytes;
        }
        self.bytes += added;
        if self.bytes <= self.budget_bytes {
            return;
        }
        // Drop the least recently used until a quarter of the budget is free, so
        // eviction is not triggered again by the very next insert.
        let mut by_age: Vec<(u64, BlockKey)> = self
            .entries
            .iter()
            .map(|(k, e)| (e.used, k.clone()))
            .collect();
        by_age.sort_unstable_by_key(|(used, _)| *used);
        let target = self.budget_bytes / 4 * 3;
        for (_, k) in by_age {
            if self.bytes <= target {
                break;
            }
            if let Some(e) = self.entries.remove(&k) {
                self.bytes -= e.bytes;
            }
        }
    }
}

/// A block opened for reading.
pub(super) struct OpenedBlock {
    pub(super) file: File,
    pub(super) meta: ArrowReaderMetadata,
    /// The block's series table, when it has a valid one.
    pub(super) series: Option<Arc<SeriesIndex>>,
    /// The block's log skip index, when it has a valid one.
    pub(super) log_index: Option<Arc<LogSkipIndex>>,
}

/// A block's footer and the indexes lifted out of it.
struct LoadedBlock {
    meta: Arc<ParquetMetaData>,
    series: Option<Arc<SeriesIndex>>,
    log_index: Option<Arc<LogSkipIndex>>,
}

/// Parses the footer of `file`, lifting its series table or log index out.
///
/// Either can be megabytes of text. It is parsed into its queryable form and then
/// **removed from the cached footer**, so the text is not held a second time next
/// to the structure built from it. An index that does not parse, or that does not
/// account for exactly the file's rows, is dropped (and logged): the block is then
/// scanned without it.
fn load_block(file: &File) -> Result<LoadedBlock> {
    let md = parquet::file::metadata::ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(file)
        .map_err(|e| Error::Parquet(e.to_string()))?;

    let find = |key: &str| {
        md.file_metadata()
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|k| k.key == key))
            .and_then(|k| k.value.clone())
    };
    let (series_text, log_text) = (find(SERIES_INDEX_FOOTER_KEY), find(LOG_INDEX_FOOTER_KEY));
    if series_text.is_none() && log_text.is_none() {
        return Ok(LoadedBlock {
            meta: Arc::new(md),
            series: None,
            log_index: None,
        });
    }
    let rows = md.file_metadata().num_rows().max(0) as u64;

    let series = series_text.and_then(|text| {
        match SeriesTable::from_footer_value(&text).and_then(SeriesIndex::from_owned_table) {
            // The table must account for every row, or it describes some other file.
            Ok(idx) if idx.total_rows() == rows => Some(Arc::new(idx)),
            Ok(_) => {
                tracing::warn!("series table disagrees with the block's row count; ignoring it");
                None
            }
            Err(e) => {
                tracing::warn!("unreadable series table, scanning without it: {e}");
                None
            }
        }
    });
    let log_index = log_text.and_then(|text| {
        match LogIndexTable::from_footer_value(&text).and_then(|t| LogSkipIndex::from_table(&t)) {
            Ok(idx) if idx.rows() == rows => Some(Arc::new(idx)),
            Ok(_) => {
                tracing::warn!("log index disagrees with the block's row count; ignoring it");
                None
            }
            Err(e) => {
                tracing::warn!("unreadable log index, scanning without it: {e}");
                None
            }
        }
    });

    // Rebuild the footer without the index text.
    let fm = md.file_metadata();
    let kept: Vec<_> = fm
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .filter(|k| k.key != SERIES_INDEX_FOOTER_KEY && k.key != LOG_INDEX_FOOTER_KEY)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let stripped = parquet::file::metadata::FileMetaData::new(
        fm.version(),
        fm.num_rows(),
        fm.created_by().map(str::to_string),
        Some(kept),
        fm.schema_descr_ptr(),
        fm.column_orders().cloned(),
    );
    let mut builder = md.into_builder();
    let row_groups = builder.take_row_groups();
    let page_index = builder.take_page_index();
    let md = parquet::file::metadata::ParquetMetaDataBuilder::new(stripped)
        .set_row_groups(row_groups)
        .set_page_index(page_index)
        .build();
    Ok(LoadedBlock {
        meta: Arc::new(md),
        series,
        log_index,
    })
}

/// Opens a block and returns its reader metadata, or `None` if the file is gone.
///
/// A legacy `labels` column is requested as a dictionary: every row of a series
/// carries the same JSON text, and Parquet already stores it dictionary-encoded,
/// so this hands the reader's dictionary straight through instead of expanding
/// one string per row. New indexed blocks omit the column. Falls back to the
/// file's own schema if the override is not accepted.
pub(super) fn open_block(path: &std::path::Path) -> Result<Option<OpenedBlock>> {
    let file = match crate::storage::block_store::open(path) {
        Ok(f) => f,
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!("Block file not found, skipping: {:?}", path);
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    // Metadata is kept, not just the builder: bloom filters and the column
    // index live on it, and both prune row groups before any page is decoded.
    //
    // The page index is what makes column- and page-level pruning possible;
    // without it only row-group statistics exist. `Optional` rather than
    // `Required`, so a block without one (an older build's) is still readable.
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let stamp = file.metadata().ok().map(|m| {
        let mtime = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos() as i128);
        (m.len(), mtime)
    });
    let key = stamp.map(|(len, mtime)| (path.to_path_buf(), len, mtime));

    let cached = key.as_ref().and_then(cached_block);
    let (parsed, series, log_index) = match cached {
        Some(hit) => {
            super::record_footer_cache_hit(path);
            hit
        }
        None => {
            let load_lock = key.as_ref().map(block_load_lock);
            let _load_guard = load_lock
                .as_ref()
                .map(|lock| lock.lock().unwrap_or_else(|e| e.into_inner()));
            if let Some(hit) = key.as_ref().and_then(cached_block) {
                super::record_footer_cache_hit(path);
                hit
            } else {
                super::record_footer_cache_miss(path);
                let started = std::time::Instant::now();
                let LoadedBlock {
                    meta,
                    series,
                    log_index,
                } = load_block(&file)?;
                let index_bytes = series.as_ref().map_or(0, |s| s.approx_bytes())
                    + log_index.as_ref().map_or(0, |s| s.approx_bytes());
                super::record_footer_load(path, started.elapsed(), index_bytes);
                tracing::debug!(
                    path = ?path,
                    index_estimated_bytes = index_bytes,
                    load_ms = started.elapsed().as_millis(),
                    "block footer cache miss"
                );
                if let Some(k) = key.as_ref() {
                    let bytes = meta.memory_size() + index_bytes;
                    let mut cache = footer_cache().lock().unwrap_or_else(|e| e.into_inner());
                    cache.insert(
                        k.clone(),
                        CachedBlock {
                            meta: Arc::clone(&meta),
                            series: series.clone(),
                            log_index: log_index.clone(),
                            bytes,
                            used: 0,
                        },
                    );
                }
                (meta, series, log_index)
            }
        }
    };
    let base = ArrowReaderMetadata::try_new(parsed, options.clone())
        .map_err(|e| Error::Parquet(e.to_string()))?;

    let meta = match dictionary_labels_schema(base.schema()) {
        Some(schema) => {
            ArrowReaderMetadata::try_new(Arc::clone(base.metadata()), options.with_schema(schema))
                .unwrap_or(base)
        }
        None => base,
    };
    Ok(Some(OpenedBlock {
        file,
        meta,
        series,
        log_index,
    }))
}

/// `schema` with the `labels` column typed as `Dictionary(Int32, Utf8)`.
fn dictionary_labels_schema(schema: &arrow_schema::SchemaRef) -> Option<arrow_schema::SchemaRef> {
    use arrow_schema::DataType;
    let idx = schema.index_of("labels").ok()?;
    let field = schema.field(idx);
    if field.data_type() != &DataType::Utf8 {
        return None;
    }
    let mut fields: Vec<arrow_schema::FieldRef> = schema.fields().iter().cloned().collect();
    fields[idx] = Arc::new(field.as_ref().clone().with_data_type(DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Utf8),
    )));
    Some(Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn cached_block(bytes: usize) -> CachedBlock {
        let schema = Arc::new(parquet::schema::types::SchemaDescriptor::new(Arc::new(
            parquet::schema::parser::parse_message_type("message test {}").unwrap(),
        )));
        let file_metadata =
            parquet::file::metadata::FileMetaData::new(1, 0, None, None, schema, None);
        CachedBlock {
            meta: Arc::new(ParquetMetaData::new(file_metadata, Vec::new())),
            series: None,
            log_index: None,
            bytes,
            used: 0,
        }
    }

    fn test_cache(budget_bytes: usize) -> FooterCache {
        FooterCache {
            entries: HashMap::new(),
            bytes: 0,
            budget_bytes,
            started: false,
            configured: false,
            tick: 0,
        }
    }

    fn test_key(name: &str) -> BlockKey {
        (name.into(), 0, 0)
    }

    #[test]
    fn footer_cache_respects_budget_and_evicts_lru_entries() {
        let mut cache = test_cache(8);
        cache.insert(test_key("oldest"), cached_block(3));
        cache.insert(test_key("newer"), cached_block(3));
        assert!(cached_block_for_test(&mut cache, &test_key("oldest")));
        cache.insert(test_key("newest"), cached_block(3));

        assert_eq!(cache.bytes, 6);
        assert!(cache.entries.contains_key(&test_key("oldest")));
        assert!(!cache.entries.contains_key(&test_key("newer")));
        assert!(cache.entries.contains_key(&test_key("newest")));
    }

    #[test]
    fn footer_cache_does_not_keep_oversized_entries() {
        let mut cache = test_cache(8);
        cache.insert(test_key("oversized"), cached_block(9));

        assert!(cache.entries.is_empty());
        assert_eq!(cache.bytes, 0);
    }

    fn cached_block_for_test(cache: &mut FooterCache, key: &BlockKey) -> bool {
        cache.tick += 1;
        let tick = cache.tick;
        if let Some(entry) = cache.entries.get_mut(key) {
            entry.used = tick;
            true
        } else {
            false
        }
    }

    #[test]
    fn concurrent_misses_for_one_block_share_load_lock() {
        let dir = tempfile::tempdir().unwrap();
        let key = (dir.path().join("block.parquet"), 1, 1);
        let first = block_load_lock(&key);
        let second = block_load_lock(&key);
        assert!(Arc::ptr_eq(&first, &second));
    }
}
