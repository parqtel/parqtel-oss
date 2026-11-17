//! Writing a block file: one Parquet file with the layout the scanner prunes on.

use crate::config::compression_from_name;
use crate::error::{Error, Result};
use crate::storage::log_index::{LogIndexTable, LOG_INDEX_FOOTER_KEY};
use crate::storage::series_index::{SeriesTable, SERIES_INDEX_FOOTER_KEY};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Columns that carry a bloom filter.
///
/// Chosen because they are the columns a query filters on *before* it can skip
/// anything: the metric name selects which blocks are relevant at all, and the
/// service name is the second-level discriminator. `timestamp_ns` is already
/// covered exactly by row-group statistics, which are cheaper and exact, so a
/// bloom filter there would add size for no pruning power.
const BLOOM_METRIC_NAME: &str = "metric_name";
const BLOOM_SERVICE_NAME: &str = "service_name";

/// The log body column, which is written without a dictionary.
const PLAIN_BODY_COLUMN: &str = "body";

/// Bloom filter false-positive probability.
///
/// The reader treats a positive as "the value might be present" and decodes the
/// row group anyway, so this only affects how often that happens — never
/// correctness.
const BLOOM_FPP: f64 = 0.01;
/// Footer key listing source block filenames replaced by a compacted block.
pub const COMPACTED_SOURCES_FOOTER_KEY: &str = "parqtel.compacted_sources";

/// Truncation length for the column index, in bytes.
///
/// Long enough to keep a full path, short enough that a pathological label
/// value cannot bloat the footer.
const COLUMN_INDEX_TRUNCATE_LENGTH: usize = 64;

/// Target data page size, in bytes.
///
/// Smaller than the Parquet default so the column index has pages finer than
/// `row_group_size` to point at.
const DATA_PAGE_SIZE_LIMIT: usize = 512 * 1024;

/// The per-block index a file carries in its footer.
#[derive(Clone, Copy)]
pub enum FooterIndex<'a> {
    /// A metric block's series table.
    Series(&'a SeriesTable),
    /// A log block's skip index.
    Logs(&'a LogIndexTable),
}

/// Key/value pairs for a block's Parquet footer: the WAL span it makes durable
/// and its per-block index.
pub fn footer_metadata(
    wal_range: Option<(u64, u64)>,
    index: Option<FooterIndex<'_>>,
) -> Result<Option<Vec<parquet::file::metadata::KeyValue>>> {
    footer_metadata_with_sources(wal_range, index, &[])
}

fn footer_metadata_with_sources(
    wal_range: Option<(u64, u64)>,
    index: Option<FooterIndex<'_>>,
    compacted_sources: &[PathBuf],
) -> Result<Option<Vec<parquet::file::metadata::KeyValue>>> {
    use parquet::file::metadata::KeyValue;
    let mut out = Vec::new();
    if let Some(range) = wal_range {
        out.push(KeyValue::new(
            crate::wal::WAL_RANGE_FOOTER_KEY.to_string(),
            crate::wal::encode_wal_range(range),
        ));
    }
    match index {
        Some(FooterIndex::Series(t)) => out.push(KeyValue::new(
            SERIES_INDEX_FOOTER_KEY.to_string(),
            t.to_footer_value()?,
        )),
        Some(FooterIndex::Logs(t)) => out.push(KeyValue::new(
            LOG_INDEX_FOOTER_KEY.to_string(),
            t.to_footer_value()?,
        )),
        None => {}
    }
    if !compacted_sources.is_empty() {
        let names = compacted_sources
            .iter()
            .map(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| Error::Internal("invalid compacted source filename".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        out.push(KeyValue::new(
            COMPACTED_SOURCES_FOOTER_KEY.to_string(),
            serde_json::to_string(&names).map_err(Error::Serde)?,
        ));
    }
    Ok(if out.is_empty() { None } else { Some(out) })
}

/// Reads compacted source filenames from a block footer.
pub fn read_compacted_sources(path: &Path) -> Result<Vec<PathBuf>> {
    let file = File::open(path)?;
    let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    let value = metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.key == COMPACTED_SOURCES_FOOTER_KEY)
        })
        .and_then(|item| item.value.as_deref());
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let names: Vec<String> = serde_json::from_str(value).map_err(Error::Serde)?;
    names
        .into_iter()
        .map(|name| {
            let path = PathBuf::from(name);
            if path.components().count() != 1
                || path.file_name() != Some(path.as_os_str())
                || path
                    .extension()
                    .is_none_or(|extension| extension != "parquet")
                || path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".tmp_"))
            {
                return Err(Error::Internal(
                    "compacted source footer contains an invalid block filename".into(),
                ));
            }
            Ok(path)
        })
        .collect()
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Incremental Parquet block writer used by streaming compaction.
pub(crate) struct BlockFileWriter {
    writer: ArrowWriter<File>,
    sync_file: File,
    path: PathBuf,
}

impl BlockFileWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        path: &Path,
        schema: arrow::datatypes::SchemaRef,
        compression: &str,
        compression_level: Option<i32>,
        row_group_size: usize,
        wal_range: Option<(u64, u64)>,
        index: Option<FooterIndex<'_>>,
        compacted_sources: &[PathBuf],
    ) -> Result<Self> {
        let footer = footer_metadata_with_sources(wal_range, index, compacted_sources)?;
        let properties = writer_properties(compression, compression_level, row_group_size, footer);
        let file = File::create(path)?;
        let sync_file = file.try_clone()?;
        let writer = ArrowWriter::try_new(file, schema, Some(properties))
            .map_err(|e| Error::Parquet(e.to_string()))?;
        Ok(Self {
            writer,
            sync_file,
            path: path.to_path_buf(),
        })
    }

    pub(crate) fn write(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        self.writer
            .write(batch)
            .map_err(|e| Error::Parquet(e.to_string()))
    }

    pub(crate) fn close(self) -> Result<()> {
        let Self { writer, path, .. } = self;
        writer.close().map_err(|e| Error::Parquet(e.to_string()))?;
        crate::storage::block_store::store().publish(&path)?;
        Ok(())
    }

    pub(crate) fn close_and_sync(self) -> Result<()> {
        let Self {
            writer,
            sync_file,
            path,
        } = self;
        writer.close().map_err(|e| Error::Parquet(e.to_string()))?;
        sync_file.sync_all()?;
        crate::storage::block_store::store().publish(&path)?;
        Ok(())
    }
}

/// Writes one block to Parquet, splitting it into `row_group_size`-row groups.
///
/// `row_group_size` is not cosmetic: the scanner skips whole row groups whose
/// timestamp statistics fall outside a query window, so this is what decides
/// whether a narrow query decodes a slice of the block or all of it. It comes
/// from `BlockConfig::row_group_size` (100k for metrics, 20k for logs by
/// default). A zero would be rejected by Parquet itself, so it is clamped.
pub fn write_block_file(
    path: &Path,
    record_batch: arrow::record_batch::RecordBatch,
    compression: &str,
    compression_level: Option<i32>,
    row_group_size: usize,
    wal_range: Option<(u64, u64)>,
    index: Option<FooterIndex<'_>>,
) -> Result<()> {
    let mut writer = BlockFileWriter::create(
        path,
        record_batch.schema(),
        compression,
        compression_level,
        row_group_size,
        wal_range,
        index,
        &[],
    )?;
    writer.write(&record_batch)?;
    writer.close()
}

fn writer_properties(
    compression: &str,
    compression_level: Option<i32>,
    row_group_size: usize,
    footer: Option<Vec<parquet::file::metadata::KeyValue>>,
) -> WriterProperties {
    WriterProperties::builder()
        .set_key_value_metadata(footer)
        .set_compression(compression_from_name(compression, compression_level))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(row_group_size.max(1)))
        // A bloom filter per row group lets the reader reject a row group for a
        // metric it does not contain without decoding any page. A metrics block
        // holds many metric names, so time-range statistics alone cannot narrow
        // a single-metric query: every row group overlaps in time and all of
        // them were being decoded.
        // Per-column, so only these two pay the size cost. Enabling it for
        // every column would bloat blocks with filters nothing reads.
        // Log bodies are nearly all distinct, so a dictionary only costs a hash
        // per row to write and a copy per row to read, and falls back to plain
        // pages once it outgrows its page anyway.
        .set_column_dictionary_enabled(ColumnPath::from(PLAIN_BODY_COLUMN), false)
        .set_column_bloom_filter_enabled(ColumnPath::from(BLOOM_METRIC_NAME), true)
        .set_column_bloom_filter_enabled(ColumnPath::from(BLOOM_SERVICE_NAME), true)
        // Default false-positive probability. Named so the trade is visible:
        // a filter this good costs roughly 10 bits per distinct value.
        .set_bloom_filter_fpp(BLOOM_FPP)
        // Page-level statistics plus a bounded column index. Row-group
        // statistics are all-or-nothing at `row_group_size` granularity; the
        // column index lets a reader skip a *page* inside a group.
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_index_truncate_length(Some(COLUMN_INDEX_TRUNCATE_LENGTH))
        // Bound the page size so the column index has finer granularity than a
        // whole row group would otherwise give.
        .set_data_page_size_limit(DATA_PAGE_SIZE_LIMIT)
        .build()
}
