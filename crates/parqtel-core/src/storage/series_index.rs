//! Per-block series index for metric blocks.
//!
//! A metric block stores its rows grouped by metric and, inside a metric,
//! **one series after another** (each series in time order). That makes every
//! series a contiguous range of rows, so a block can describe itself with a
//! small table: for each series its labels, the row range it occupies and its
//! time bounds. The table is written into the Parquet footer
//! ([`SERIES_INDEX_FOOTER_KEY`]) next to the data it describes, so it cannot be
//! lost or fall out of step with the file.
//!
//! # What it buys
//!
//! A selector such as `http_requests_total{pod="api-7"}` used to decode every row
//! of the metric - one label-JSON dictionary lookup per row - just to keep a few
//! of them. With the table:
//!
//! 1. compact label ids find candidate series without looking at any row;
//! 2. the candidates' row ranges become a Parquet row selection, so pages that hold
//!    no candidate row are never read or decoded;
//! 3. the series' labels come from the table, so the `labels`, `metric_name` and
//!    `service_name` columns are not read at all.
//!
//! # Exactness
//!
//! Query-local candidate ids only *narrow* the candidates; the caller's full
//! matcher still decides each remaining series. Candidate scans use the compact
//! label ids and never retain per-value postings.

use crate::error::{Error, Result};
use crate::models::labels::{LabelInterner, LabelSet};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::de::{Error as DeError, SeqAccess, Visitor};
use serde::ser::{Error as SerError, SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::sync::{Arc, OnceLock};

/// Parquet footer key under which the series table is stored.
pub const SERIES_INDEX_FOOTER_KEY: &str = "parqtel.series_index";

const FORMAT_VERSION: u32 = 1;
const COMPRESSED_FOOTER_PREFIX: &str = "zstd+base64:v1:";
const MAX_DECOMPRESSED_FOOTER_BYTES: u64 = 512 * 1024 * 1024;

/// The serialised series table, as it appears in the Parquet footer.
///
/// Columnar (parallel arrays) so it stays compact and parses quickly even for
/// blocks with a hundred thousand series. Strings - metric names, label names and
/// values - are stored once and referred to by id.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesTable {
    v: u32,
    string_bytes: Vec<u8>,
    string_offsets: Vec<u32>,
    /// `(metric name id, first series id, series count)` per run of series. A
    /// metric appears once per `(kind, resource)` group, so it can have several.
    metrics: Vec<(u32, u32, u32)>,
    /// First row of each series.
    start: Vec<u64>,
    /// Number of rows of each series.
    len: Vec<u32>,
    min_ts: Vec<i64>,
    max_ts: Vec<i64>,
    /// Per series: flat `[name id, value id, name id, value id, ...]`, sorted by
    /// label name. Includes the `service.name` label merged in from the
    /// resource, exactly as a scan reports it.
    label_ids: Vec<u32>,
    label_offsets: Vec<u32>,
}

impl Default for SeriesTable {
    fn default() -> Self {
        Self {
            v: 0,
            string_bytes: Vec::new(),
            string_offsets: vec![0],
            metrics: Vec::new(),
            start: Vec::new(),
            len: Vec::new(),
            min_ts: Vec::new(),
            max_ts: Vec::new(),
            label_ids: Vec::new(),
            label_offsets: vec![0],
        }
    }
}

#[derive(Deserialize)]
struct SeriesTableSerde {
    v: u32,
    strings: StringArena,
    metrics: Vec<(u32, u32, u32)>,
    start: Vec<u64>,
    len: Vec<u32>,
    min_ts: Vec<i64>,
    max_ts: Vec<i64>,
    labels: LabelArena,
}

struct StringArena {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
}

impl Default for StringArena {
    fn default() -> Self {
        Self {
            bytes: Vec::new(),
            offsets: vec![0],
        }
    }
}

impl<'de> Deserialize<'de> for StringArena {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StringArenaVisitor;

        impl<'de> Visitor<'de> for StringArenaVisitor {
            type Value = StringArena;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an array of series-index strings")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut arena = StringArena::default();
                if let Some(size) = seq.size_hint() {
                    arena.offsets.reserve(size);
                }
                while let Some(string) = seq.next_element::<String>()? {
                    arena.bytes.extend_from_slice(string.as_bytes());
                    arena
                        .offsets
                        .push(u32::try_from(arena.bytes.len()).map_err(A::Error::custom)?);
                }
                arena.bytes.shrink_to_fit();
                arena.offsets.shrink_to_fit();
                Ok(arena)
            }
        }

        deserializer.deserialize_seq(StringArenaVisitor)
    }
}

struct LabelArena {
    ids: Vec<u32>,
    offsets: Vec<u32>,
}

impl Default for LabelArena {
    fn default() -> Self {
        Self {
            ids: Vec::new(),
            offsets: vec![0],
        }
    }
}

impl<'de> Deserialize<'de> for LabelArena {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LabelArenaVisitor;

        impl<'de> Visitor<'de> for LabelArenaVisitor {
            type Value = LabelArena;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an array of series label-id arrays")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut arena = LabelArena::default();
                if let Some(size) = seq.size_hint() {
                    arena.offsets.reserve(size);
                }
                while let Some(labels) = seq.next_element::<Vec<u32>>()? {
                    arena.ids.extend(labels);
                    arena
                        .offsets
                        .push(u32::try_from(arena.ids.len()).map_err(A::Error::custom)?);
                }
                arena.ids.shrink_to_fit();
                arena.offsets.shrink_to_fit();
                Ok(arena)
            }
        }

        deserializer.deserialize_seq(LabelArenaVisitor)
    }
}

impl<'de> Deserialize<'de> for SeriesTable {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = SeriesTableSerde::deserialize(deserializer)?;
        let table = Self {
            v: wire.v,
            string_bytes: wire.strings.bytes,
            string_offsets: wire.strings.offsets,
            metrics: wire.metrics,
            start: wire.start,
            len: wire.len,
            min_ts: wire.min_ts,
            max_ts: wire.max_ts,
            label_ids: wire.labels.ids,
            label_offsets: wire.labels.offsets,
        };
        Ok(table)
    }
}

struct StringValues<'a>(&'a SeriesTable);

impl Serialize for StringValues<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let count = self.0.string_offsets.len().saturating_sub(1);
        let mut seq = serializer.serialize_seq(Some(count))?;
        for id in 0..count {
            let value = self
                .0
                .string(u32::try_from(id).map_err(S::Error::custom)?)
                .ok_or_else(|| S::Error::custom("corrupt series index string offsets"))?;
            seq.serialize_element(value)?;
        }
        seq.end()
    }
}

struct LabelValues<'a>(&'a SeriesTable);

impl Serialize for LabelValues<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let count = self.0.label_offsets.len().saturating_sub(1);
        let mut seq = serializer.serialize_seq(Some(count))?;
        for series in 0..count {
            let labels = self
                .0
                .label_ids(series)
                .ok_or_else(|| S::Error::custom("corrupt series index label offsets"))?;
            seq.serialize_element(labels)?;
        }
        seq.end()
    }
}

impl Serialize for SeriesTable {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SeriesTable", 8)?;
        state.serialize_field("v", &self.v)?;
        state.serialize_field("strings", &StringValues(self))?;
        state.serialize_field("metrics", &self.metrics)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("len", &self.len)?;
        state.serialize_field("min_ts", &self.min_ts)?;
        state.serialize_field("max_ts", &self.max_ts)?;
        state.serialize_field("labels", &LabelValues(self))?;
        state.end()
    }
}

/// One contiguous metric/kind/resource run in a stored series table.
#[derive(Debug, Clone, Copy)]
pub struct SeriesTableRun<'a> {
    pub metric_name: &'a str,
    pub first_series: usize,
    pub series_count: usize,
    pub first_row: u64,
}

/// Accumulates a [`SeriesTable`] while a block's rows are laid out.
#[derive(Debug, Default)]
pub struct SeriesTableBuilder {
    table: SeriesTable,
    ids: HashMap<String, u32>,
}

impl SeriesTableBuilder {
    pub fn new() -> Self {
        Self {
            table: SeriesTable {
                v: FORMAT_VERSION,
                ..Default::default()
            },
            ids: HashMap::new(),
        }
    }

    fn string_id(&mut self, s: &str) -> u32 {
        if let Some(id) = self.ids.get(s) {
            return *id;
        }
        let id = self.table.string_offsets.len().saturating_sub(1) as u32;
        self.table.string_bytes.extend_from_slice(s.as_bytes());
        self.table
            .string_offsets
            .push(self.table.string_bytes.len() as u32);
        self.ids.insert(s.to_string(), id);
        id
    }

    /// Starts the series of a new `(metric, kind, resource)` group.
    pub fn begin_metric(&mut self, name: &str) {
        let name_id = self.string_id(name);
        let first = self.table.start.len() as u32;
        self.table.metrics.push((name_id, first, 0));
    }

    /// Adds the next series of the current metric: `rows` consecutive rows
    /// beginning at row `start`, whose timestamps lie in `[min_ts, max_ts]`.
    pub fn push_series(
        &mut self,
        labels: &LabelSet,
        start: u64,
        rows: u32,
        min_ts: i64,
        max_ts: i64,
    ) {
        let mut flat = Vec::with_capacity(labels.len() * 2);
        for (k, v) in labels.iter() {
            let (k, v) = (self.string_id(k), self.string_id(v));
            flat.push(k);
            flat.push(v);
        }
        self.table.start.push(start);
        self.table.len.push(rows);
        self.table.min_ts.push(min_ts);
        self.table.max_ts.push(max_ts);
        self.table.label_ids.extend(flat);
        self.table
            .label_offsets
            .push(self.table.label_ids.len() as u32);
        if let Some(run) = self.table.metrics.last_mut() {
            run.2 += 1;
        }
    }

    /// Number of series added so far.
    pub fn series_len(&self) -> usize {
        self.table.start.len()
    }

    pub fn finish(self) -> SeriesTable {
        self.table
    }
}

impl SeriesTable {
    /// String at `id`, or `None` when out of bounds.
    pub fn string(&self, id: u32) -> Option<&str> {
        let id = usize::try_from(id).ok()?;
        let start = *self.string_offsets.get(id)? as usize;
        let end = *self.string_offsets.get(id.checked_add(1)?)? as usize;
        std::str::from_utf8(self.string_bytes.get(start..end)?).ok()
    }

    fn string_id(&self, value: &str) -> Option<u32> {
        for id in 0..self.string_offsets.len().saturating_sub(1) {
            let id = u32::try_from(id).ok()?;
            if self.string(id) == Some(value) {
                return Some(id);
            }
        }
        None
    }

    /// Flat label id pairs for `series`, or `None` when out of bounds.
    pub fn label_ids(&self, series: usize) -> Option<&[u32]> {
        let start = *self.label_offsets.get(series)? as usize;
        let end = *self.label_offsets.get(series.checked_add(1)?)? as usize;
        self.label_ids.get(start..end)
    }

    /// Total label pairs across all series.
    pub fn label_pair_count(&self) -> usize {
        self.label_ids.len() / 2
    }

    /// Approximate heap allocation size, including vector headers and payload capacity.
    pub fn approx_heap_bytes(&self) -> usize {
        let mut bytes = self.string_bytes.capacity();
        bytes = bytes.saturating_add(
            self.string_offsets
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        );
        bytes = bytes.saturating_add(self.metrics.capacity().saturating_mul(std::mem::size_of::<(
            u32,
            u32,
            u32,
        )>()));
        bytes = bytes.saturating_add(
            self.start
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        );
        bytes = bytes.saturating_add(
            self.len
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        );
        bytes = bytes.saturating_add(
            self.min_ts
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>()),
        );
        bytes = bytes.saturating_add(
            self.max_ts
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>()),
        );
        bytes = bytes.saturating_add(
            self.label_ids
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        );
        bytes.saturating_add(
            self.label_offsets
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
    }

    /// Number of metric runs in table order.
    pub fn metric_run_count(&self) -> usize {
        self.metrics.len()
    }

    /// Metric run at `index`, or `None` when out of bounds or corrupt.
    pub fn metric_run(&self, index: usize) -> Option<SeriesTableRun<'_>> {
        let (name, first, count) = *self.metrics.get(index)?;
        let first_series = first as usize;
        Some(SeriesTableRun {
            metric_name: self.string(name)?,
            first_series,
            series_count: count as usize,
            first_row: *self.start.get(first_series)?,
        })
    }

    /// Number of series entries in table order.
    pub fn series_count(&self) -> usize {
        self.start.len()
    }

    /// Row range and time bounds for one series, without decoding its labels.
    pub fn series_bounds(&self, index: usize) -> Option<(u64, u32, i64, i64)> {
        Some((
            *self.start.get(index)?,
            *self.len.get(index)?,
            *self.min_ts.get(index)?,
            *self.max_ts.get(index)?,
        ))
    }

    /// One series entry, without building postings or populating the global
    /// label interner. Compaction uses this to keep source-index memory bounded.
    pub fn series_entry(&self, index: usize) -> Result<SeriesEntry> {
        let bad = || Error::Internal("corrupt series index: series entry out of range".into());
        let flat = self.label_ids(index).ok_or_else(bad)?;
        if flat.len() % 2 != 0 {
            return Err(Error::Internal(
                "corrupt series index: odd label id count".into(),
            ));
        }
        let (pairs, _rest) = flat.as_chunks::<2>();
        let mut labels = Vec::with_capacity(pairs.len());
        for kv in pairs {
            let name = self.string(kv[0]);
            let value = self.string(kv[1]);
            labels.push((name.ok_or_else(bad)?, value.ok_or_else(bad)?));
        }
        Ok(SeriesEntry {
            labels: LabelSet::try_from_iter(labels)?,
            start: *self.start.get(index).ok_or_else(bad)?,
            rows: *self.len.get(index).ok_or_else(bad)?,
            min_ts: *self.min_ts.get(index).ok_or_else(bad)?,
            max_ts: *self.max_ts.get(index).ok_or_else(bad)?,
        })
    }

    /// Total rows described by all series entries.
    pub fn total_rows(&self) -> u64 {
        self.len.iter().map(|rows| u64::from(*rows)).sum()
    }

    /// Compressed JSON text for the Parquet footer when it saves space.
    pub fn to_footer_value(&self) -> Result<String> {
        let json = serde_json::to_string(self).map_err(Error::Serde)?;
        let compressed = zstd::bulk::compress(json.as_bytes(), 3)?;
        let value = format!("{COMPRESSED_FOOTER_PREFIX}{}", STANDARD.encode(compressed));
        Ok(if value.len() < json.len() {
            value
        } else {
            json
        })
    }

    /// Parses compressed footer values and legacy plain-JSON values.
    pub fn from_footer_value(value: &str) -> Result<Self> {
        let table: SeriesTable = if let Some(payload) = value.strip_prefix(COMPRESSED_FOOTER_PREFIX)
        {
            let compressed = STANDARD.decode(payload).map_err(|e| {
                Error::Internal(format!("invalid compressed series index encoding: {e}"))
            })?;
            let decoder = zstd::stream::read::Decoder::new(compressed.as_slice())?;
            let mut json = Vec::new();
            decoder
                .take(MAX_DECOMPRESSED_FOOTER_BYTES + 1)
                .read_to_end(&mut json)?;
            if json.len() as u64 > MAX_DECOMPRESSED_FOOTER_BYTES {
                return Err(Error::Internal(
                    "decompressed series index exceeds 512 MiB".into(),
                ));
            }
            serde_json::from_slice(&json).map_err(Error::Serde)?
        } else {
            serde_json::from_str(value).map_err(Error::Serde)?
        };
        table.validate()?;
        Ok(table)
    }

    /// Rejects a table whose arrays disagree or whose ids are out of range, so a
    /// damaged footer degrades to the unindexed scan instead of misreading rows.
    fn validate(&self) -> Result<()> {
        let n = self.start.len();
        let bad = |what: &str| Error::Internal(format!("corrupt series index: {what}"));
        if self.v != FORMAT_VERSION {
            return Err(bad("unsupported version"));
        }
        if self.len.len() != n
            || self.min_ts.len() != n
            || self.max_ts.len() != n
            || self.label_offsets.len() != n + 1
            || self.label_offsets.first() != Some(&0)
            || self.label_offsets.last().copied() != u32::try_from(self.label_ids.len()).ok()
            || self
                .label_offsets
                .windows(2)
                .any(|offsets| offsets[0] > offsets[1])
            || self.string_offsets.is_empty()
            || self.string_offsets.first() != Some(&0)
            || self.string_offsets.last().copied() != u32::try_from(self.string_bytes.len()).ok()
            || self
                .string_offsets
                .windows(2)
                .any(|offsets| offsets[0] > offsets[1])
        {
            return Err(bad("array lengths differ"));
        }
        if self.string_offsets.windows(2).any(|offsets| {
            std::str::from_utf8(&self.string_bytes[offsets[0] as usize..offsets[1] as usize])
                .is_err()
        }) {
            return Err(bad("invalid string encoding"));
        }
        let strings = u32::try_from(self.string_offsets.len() - 1).map_err(|_| bad("overflow"))?;
        if !self.label_ids.len().is_multiple_of(2) || self.label_ids.iter().any(|id| *id >= strings)
        {
            return Err(bad("label id out of range"));
        }
        let mut next = 0u32;
        for (name, first, count) in &self.metrics {
            if *name >= strings || *first != next {
                return Err(bad("metric runs are not contiguous"));
            }
            next = first.checked_add(*count).ok_or_else(|| bad("overflow"))?;
        }
        if next as usize != n {
            return Err(bad("metric runs do not cover the series"));
        }
        Ok(())
    }
}

/// One series of a block.
#[derive(Debug, Clone)]
pub struct SeriesEntry {
    /// Labels as a scan reports them (resource `service.name` merged in).
    pub labels: LabelSet,
    /// First row.
    pub start: u64,
    /// Row count.
    pub rows: u32,
    pub min_ts: i64,
    pub max_ts: i64,
}

/// A test applied to a label's distinct values, for narrowing by a regex-like
/// matcher without visiting series.
pub type ValueTest = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// What a scan wants to know about a block's series.
#[derive(Clone, Default)]
pub struct SeriesSelector {
    /// `service.name` equality, which also prunes row groups in unindexed blocks.
    pub service: Option<String>,
    /// `label = value` for a non-empty value. Narrowing only.
    pub equals: Vec<(String, String)>,
    /// Positive tests on a label's value that an absent label can never pass
    /// (for example `label =~ "api-.*"`). Narrowing only.
    pub value_tests: Vec<(String, ValueTest)>,
    /// The complete matcher. It has the last word on every series.
    pub filter: Option<crate::storage::scanner::SeriesFilter>,
}

impl SeriesSelector {
    /// A selector that keeps every series.
    pub fn all() -> Self {
        Self::default()
    }

    /// Adds `service.name = service`.
    pub fn with_service(mut self, service: Option<String>) -> Self {
        if let Some(s) = &service {
            self.equals.push(("service.name".to_string(), s.clone()));
        }
        self.service = service;
        self
    }

    pub fn with_filter(mut self, filter: Option<crate::storage::scanner::SeriesFilter>) -> Self {
        self.filter = filter;
        self
    }
}

/// A block's series, queryable.
pub struct SeriesIndex {
    table: SeriesTable,
    /// Metric name -> `[lo, hi)` series id ranges, in id order.
    metric_ranges: HashMap<String, Vec<(u32, u32)>>,
    /// Label name -> compact table string id.
    label_name_ids: HashMap<String, u32>,
    /// Materialized label sets, shared globally when first requested.
    labels: Vec<OnceLock<LabelSet>>,
    total_rows: u64,
    approx_bytes: usize,
}

impl std::fmt::Debug for SeriesIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeriesIndex")
            .field("series", &self.series_count())
            .field("total_rows", &self.total_rows)
            .finish()
    }
}

impl SeriesIndex {
    /// Builds the queryable form of a table.
    pub fn from_table(table: &SeriesTable) -> Result<Self> {
        Self::from_owned_table(table.clone())
    }

    /// Builds the queryable form of an owned table without cloning its arrays.
    pub fn from_owned_table(table: SeriesTable) -> Result<Self> {
        table.validate()?;
        let mut metric_ranges: HashMap<String, Vec<(u32, u32)>> = HashMap::new();
        for (name, first, count) in &table.metrics {
            let Some(name) = table.string(*name) else {
                return Err(Error::Internal(
                    "corrupt series index: invalid metric name".into(),
                ));
            };
            metric_ranges
                .entry(name.to_string())
                .or_default()
                .push((*first, first + count));
        }
        let mut label_name_ids = HashMap::new();
        for series in 0..table.series_count() {
            let Some(labels) = table.label_ids(series) else {
                return Err(Error::Internal(
                    "corrupt series index: invalid label offsets".into(),
                ));
            };
            for pair in labels.as_chunks::<2>().0 {
                let Some(name) = table.string(pair[0]) else {
                    return Err(Error::Internal(
                        "corrupt series index: invalid label name".into(),
                    ));
                };
                if !label_name_ids.contains_key(name) {
                    label_name_ids.insert(name.to_string(), pair[0]);
                }
            }
        }
        let labels: Vec<OnceLock<LabelSet>> =
            (0..table.series_count()).map(|_| OnceLock::new()).collect();
        let label_slot_bytes = labels
            .capacity()
            .saturating_mul(std::mem::size_of::<OnceLock<LabelSet>>());
        let total_rows = table.total_rows();
        let approx_bytes =
            approximate_index_bytes(&table, &metric_ranges, &label_name_ids, label_slot_bytes);
        Ok(Self {
            table,
            metric_ranges,
            label_name_ids,
            labels,
            total_rows,
            approx_bytes,
        })
    }

    /// Estimated resident size, for cache accounting.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    /// Rows the table accounts for.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Number of series described by this block.
    pub fn series_count(&self) -> usize {
        self.table.series_count()
    }

    /// Row range and time bounds for one series.
    pub fn bounds(&self, id: u32) -> Option<(u64, u32, i64, i64)> {
        self.table.series_bounds(id as usize)
    }

    /// Labels for one series, materialized and globally interned on first use.
    pub fn labels(&self, id: u32) -> Result<LabelSet> {
        let corrupt = || Error::Internal("corrupt series index: invalid series labels".into());
        let cell = self.labels.get(id as usize).ok_or_else(corrupt)?;
        if let Some(labels) = cell.get() {
            return Ok(labels.clone());
        }

        let flat = self.table.label_ids(id as usize).ok_or_else(corrupt)?;
        if flat.len() % 2 != 0 {
            return Err(corrupt());
        }
        let mut pairs = Vec::with_capacity(flat.len() / 2);
        for pair in flat.as_chunks::<2>().0 {
            let name = self.table.string(pair[0]).ok_or_else(corrupt)?;
            let value = self.table.string(pair[1]).ok_or_else(corrupt)?;
            pairs.push((name, value));
        }
        let labels = LabelInterner::global()
            .intern(&pairs)
            .map_err(|error| Error::Internal(format!("corrupt series index: {error}")))?;
        let _ = cell.set(labels);
        cell.get().cloned().ok_or_else(corrupt)
    }

    #[cfg(test)]
    fn materialized_label_count(&self) -> usize {
        self.labels
            .iter()
            .filter(|labels| labels.get().is_some())
            .count()
    }

    /// Whether the block holds any series of `metric`.
    pub fn has_metric(&self, metric: &str) -> bool {
        self.metric_ranges.contains_key(metric)
    }

    /// Ids of the series of `metric` that `selector` accepts and that overlap
    /// `[start_ns, end_ns]`, in row order.
    pub fn select(
        &self,
        metric: &str,
        selector: &SeriesSelector,
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<u32> {
        // An empty name means "every metric", as it does for an unindexed scan.
        let every;
        let ranges: &[(u32, u32)] = if metric.is_empty() {
            every = (0, self.series_count() as u32);
            std::slice::from_ref(&every)
        } else {
            match self.metric_ranges.get(metric) {
                Some(r) => r,
                None => return Vec::new(),
            }
        };

        // Candidate lists are query-local; each matcher only narrows the scan.
        let mut owned_lists: Vec<Vec<u32>> = Vec::new();
        for (label, value) in &selector.equals {
            if value.is_empty() {
                return Vec::new();
            }
            let Some(label_id) = self.label_name_ids.get(label) else {
                return Vec::new();
            };
            let Some(value_id) = self.table.string_id(value) else {
                return Vec::new();
            };
            let ids = self.matching_ids(ranges, *label_id, |candidate| candidate == value_id);
            if ids.is_empty() {
                return Vec::new();
            }
            owned_lists.push(ids);
        }
        for (label, test) in &selector.value_tests {
            let Some(label_id) = self.label_name_ids.get(label) else {
                // No series has the label at all, and the test cannot be met by
                // an absent one.
                return Vec::new();
            };
            let union = self.matching_ids(ranges, *label_id, |value_id| {
                self.table.string(value_id).is_some_and(|value| test(value))
            });
            if union.is_empty() {
                return Vec::new();
            }
            owned_lists.push(union);
        }

        let candidates = if owned_lists.is_empty() {
            ranges.iter().flat_map(|(lo, hi)| *lo..*hi).collect()
        } else {
            let mut lists: Vec<&[u32]> = owned_lists.iter().map(Vec::as_slice).collect();
            intersect_all(&mut lists)
        };

        candidates
            .into_iter()
            .filter(|id| {
                self.bounds(*id).is_some_and(|(_, _, min_ts, max_ts)| {
                    min_ts <= end_ns
                        && max_ts >= start_ns
                        && selector.filter.as_ref().is_none_or(|filter| {
                            self.labels(*id).is_ok_and(|labels| filter(&labels))
                        })
                })
            })
            .collect()
    }

    fn matching_ids(
        &self,
        ranges: &[(u32, u32)],
        label_id: u32,
        mut matches: impl FnMut(u32) -> bool,
    ) -> Vec<u32> {
        let mut ids = Vec::new();
        for &(lo, hi) in ranges {
            for id in lo..hi {
                let Some(labels) = self.table.label_ids(id as usize) else {
                    continue;
                };
                if labels
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .any(|pair| pair[0] == label_id && matches(pair[1]))
                {
                    ids.push(id);
                }
            }
        }
        ids
    }
}

fn approximate_index_bytes(
    table: &SeriesTable,
    metric_ranges: &HashMap<String, Vec<(u32, u32)>>,
    label_name_ids: &HashMap<String, u32>,
    label_slot_bytes: usize,
) -> usize {
    let metric_bytes = metric_ranges.iter().fold(
        metric_ranges
            .capacity()
            .saturating_mul(std::mem::size_of::<(String, Vec<(u32, u32)>)>().saturating_mul(2)),
        |bytes, (name, ranges)| {
            bytes.saturating_add(name.capacity()).saturating_add(
                ranges
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(u32, u32)>()),
            )
        },
    );
    let label_name_bytes = label_name_ids.iter().fold(
        label_name_ids
            .capacity()
            .saturating_mul(std::mem::size_of::<(String, u32)>().saturating_mul(2)),
        |bytes, (name, _)| bytes.saturating_add(name.capacity()),
    );
    table
        .approx_heap_bytes()
        .saturating_add(metric_bytes)
        .saturating_add(label_name_bytes)
        .saturating_add(label_slot_bytes)
}

/// Elements of ascending `ids` that fall in any of the ascending `[lo, hi)`
/// ranges.
#[cfg(test)]
fn restrict(ids: &[u32], ranges: &[(u32, u32)]) -> Vec<u32> {
    let mut out = Vec::new();
    for (lo, hi) in ranges {
        let from = ids.partition_point(|id| id < lo);
        let to = ids.partition_point(|id| id < hi);
        out.extend_from_slice(&ids[from..to]);
    }
    out
}

/// Intersects ascending lists; empty input has an empty result.
fn intersect_all(lists: &mut [&[u32]]) -> Vec<u32> {
    if lists.is_empty() {
        return Vec::new();
    }
    lists.sort_by_key(|list| list.len());
    let Some((shortest, others)) = lists.split_first() else {
        return Vec::new();
    };
    if shortest.is_empty() {
        return Vec::new();
    }

    let mut cursors = vec![0; others.len()];
    let mut out = Vec::with_capacity(shortest.len());
    'candidate: for &candidate in *shortest {
        for (list, cursor) in others.iter().zip(&mut cursors) {
            let remaining = &list[*cursor..];
            *cursor += remaining.partition_point(|id| *id < candidate);
            if list.get(*cursor) != Some(&candidate) {
                continue 'candidate;
            }
            *cursor += 1;
        }
        out.push(candidate);
    }
    out
}

/// Row ranges `[start, end)` covering the given series, merged where series are
/// adjacent. `ids` must be ascending, which is row order.
pub fn row_ranges(index: &SeriesIndex, ids: &[u32]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for id in ids {
        let Some((a, rows, _, _)) = index.bounds(*id) else {
            continue;
        };
        let b = a + u64::from(rows);
        match out.last_mut() {
            Some(last) if last.1 == a => last.1 = b,
            _ => out.push((a, b)),
        }
    }
    out
}

#[cfg(test)]
mod tests;
