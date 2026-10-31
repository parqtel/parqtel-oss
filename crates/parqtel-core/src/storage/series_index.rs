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
//! 1. an *inverted index* (label name -> value -> series ids, built when the table
//!    is loaded) finds the candidate series without looking at any row;
//! 2. the candidates' row ranges become a Parquet row selection, so pages that hold
//!    no candidate row are never read or decoded;
//! 3. the series' labels come from the table, so the `labels`, `metric_name` and
//!    `service_name` columns are not read at all.
//!
//! # Exactness
//!
//! The postings only ever *narrow* the candidates; the caller's full matcher
//! still decides each remaining series. A postings bug can therefore cost speed
//! but not correctness - it could only wrongly drop a series if a posting list
//! were missing an id, which the construction (every series is listed under every
//! one of its labels) rules out and the tests pin.

use crate::error::{Error, Result};
use crate::models::labels::{LabelInterner, LabelSet};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Parquet footer key under which the series table is stored.
pub const SERIES_INDEX_FOOTER_KEY: &str = "parqtel.series_index";

const FORMAT_VERSION: u32 = 1;

/// The serialised series table, as it appears in the Parquet footer.
///
/// Columnar (parallel arrays) so it stays compact and parses quickly even for
/// blocks with a hundred thousand series. Strings - metric names, label names and
/// values - are stored once and referred to by id.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SeriesTable {
    v: u32,
    strings: Vec<String>,
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
    labels: Vec<Vec<u32>>,
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
        let id = self.table.strings.len() as u32;
        self.table.strings.push(s.to_string());
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
        self.table.labels.push(flat);
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
    /// JSON text for the Parquet footer.
    pub fn to_footer_value(&self) -> Result<String> {
        serde_json::to_string(self).map_err(Error::Serde)
    }

    /// Parses the footer value.
    pub fn from_footer_value(value: &str) -> Result<Self> {
        let table: SeriesTable = serde_json::from_str(value).map_err(Error::Serde)?;
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
            || self.labels.len() != n
        {
            return Err(bad("array lengths differ"));
        }
        let strings = self.strings.len() as u32;
        if self
            .labels
            .iter()
            .any(|l| l.len() % 2 != 0 || l.iter().any(|id| *id >= strings))
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
    pub series: Vec<SeriesEntry>,
    /// Metric name -> `[lo, hi)` series id ranges, in id order.
    metric_ranges: HashMap<String, Vec<(u32, u32)>>,
    /// Label name -> value -> ids of the series carrying it (ascending).
    postings: HashMap<String, HashMap<String, Vec<u32>>>,
    total_rows: u64,
    approx_bytes: usize,
}

impl std::fmt::Debug for SeriesIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeriesIndex")
            .field("series", &self.series.len())
            .field("total_rows", &self.total_rows)
            .finish()
    }
}

impl SeriesIndex {
    /// Builds the queryable form of a table.
    pub fn from_table(table: &SeriesTable) -> Result<Self> {
        table.validate()?;
        let interner = LabelInterner::global();
        let mut series = Vec::with_capacity(table.start.len());
        let mut postings: HashMap<String, HashMap<String, Vec<u32>>> = HashMap::new();
        let mut total_rows = 0u64;
        for i in 0..table.start.len() {
            let flat = &table.labels[i];
            let pairs: Vec<(&str, &str)> = flat
                .chunks_exact(2)
                .map(|kv| {
                    (
                        table.strings[kv[0] as usize].as_str(),
                        table.strings[kv[1] as usize].as_str(),
                    )
                })
                .collect();
            // Shared across blocks: the same series in many blocks is one set.
            let labels = interner.intern(&pairs)?;
            for (k, v) in &pairs {
                postings
                    .entry((*k).to_string())
                    .or_default()
                    .entry((*v).to_string())
                    .or_default()
                    .push(i as u32);
            }
            total_rows += u64::from(table.len[i]);
            series.push(SeriesEntry {
                labels,
                start: table.start[i],
                rows: table.len[i],
                min_ts: table.min_ts[i],
                max_ts: table.max_ts[i],
            });
        }
        let mut metric_ranges: HashMap<String, Vec<(u32, u32)>> = HashMap::new();
        for (name, first, count) in &table.metrics {
            metric_ranges
                .entry(table.strings[*name as usize].clone())
                .or_default()
                .push((*first, first + count));
        }
        let approx_bytes = table.strings.iter().map(|s| s.len() + 48).sum::<usize>()
            + series.len() * 160
            + postings.values().map(|m| m.len() * 72).sum::<usize>()
            + table.labels.iter().map(|l| l.len() * 8).sum::<usize>();
        Ok(Self {
            series,
            metric_ranges,
            postings,
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
        let ranges: &Vec<(u32, u32)> = if metric.is_empty() {
            every = vec![(0, self.series.len() as u32)];
            &every
        } else {
            match self.metric_ranges.get(metric) {
                Some(r) => r,
                None => return Vec::new(),
            }
        };

        // Candidate lists from the postings; each one is a necessary condition.
        let mut lists: Vec<Vec<u32>> = Vec::new();
        for (label, value) in &selector.equals {
            let ids = self
                .postings
                .get(label)
                .and_then(|values| values.get(value))
                .map(|ids| restrict(ids, ranges))
                .unwrap_or_default();
            if ids.is_empty() {
                return Vec::new();
            }
            lists.push(ids);
        }
        for (label, test) in &selector.value_tests {
            let Some(values) = self.postings.get(label) else {
                // No series has the label at all, and the test cannot be met by
                // an absent one.
                return Vec::new();
            };
            let mut union: Vec<u32> = Vec::new();
            for (value, ids) in values {
                if test(value) {
                    union.extend(restrict(ids, ranges));
                }
            }
            union.sort_unstable();
            union.dedup();
            if union.is_empty() {
                return Vec::new();
            }
            lists.push(union);
        }

        let candidates: Box<dyn Iterator<Item = u32>> = if lists.is_empty() {
            Box::new(ranges.iter().flat_map(|(lo, hi)| *lo..*hi))
        } else {
            lists.sort_by_key(|l| l.len());
            let mut acc = lists.swap_remove(0);
            for other in &lists {
                acc = intersect(&acc, other);
                if acc.is_empty() {
                    return Vec::new();
                }
            }
            Box::new(acc.into_iter())
        };

        candidates
            .filter(|id| {
                let s = &self.series[*id as usize];
                s.min_ts <= end_ns
                    && s.max_ts >= start_ns
                    && selector.filter.as_ref().is_none_or(|f| f(&s.labels))
            })
            .collect()
    }
}

/// Elements of ascending `ids` that fall in any of the ascending `[lo, hi)`
/// ranges.
fn restrict(ids: &[u32], ranges: &[(u32, u32)]) -> Vec<u32> {
    let mut out = Vec::new();
    for (lo, hi) in ranges {
        let from = ids.partition_point(|id| id < lo);
        let to = ids.partition_point(|id| id < hi);
        out.extend_from_slice(&ids[from..to]);
    }
    out
}

/// Intersection of two ascending lists.
fn intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// Row ranges `[start, end)` covering the given series, merged where series are
/// adjacent. `ids` must be ascending, which is row order.
pub fn row_ranges(index: &SeriesIndex, ids: &[u32]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for id in ids {
        let s = &index.series[*id as usize];
        let (a, b) = (s.start, s.start + u64::from(s.rows));
        match out.last_mut() {
            Some(last) if last.1 == a => last.1 = b,
            _ => out.push((a, b)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn ls(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (*k, *v))).unwrap()
    }

    /// cpu: h0..h3 across two envs; mem: one series. Rows are 10 per series.
    fn sample() -> SeriesIndex {
        let mut b = SeriesTableBuilder::new();
        b.begin_metric("cpu");
        let mut row = 0u64;
        for (i, (host, env)) in [
            ("h0", "prod"),
            ("h1", "prod"),
            ("h2", "stage"),
            ("h3", "stage"),
        ]
        .iter()
        .enumerate()
        {
            b.push_series(
                &ls(&[("host", host), ("env", env), ("service.name", "api")]),
                row,
                10,
                (i as i64) * 100,
                (i as i64) * 100 + 99,
            );
            row += 10;
        }
        b.begin_metric("mem");
        b.push_series(&ls(&[("host", "h0")]), row, 10, 0, 99);
        SeriesIndex::from_table(&b.finish()).unwrap()
    }

    fn pick(idx: &SeriesIndex, metric: &str, sel: &SeriesSelector) -> Vec<String> {
        idx.select(metric, sel, i64::MIN, i64::MAX)
            .into_iter()
            .map(|id| {
                idx.series[id as usize]
                    .labels
                    .get("host")
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn table_round_trips_through_the_footer_text() {
        let mut b = SeriesTableBuilder::new();
        b.begin_metric("m");
        b.push_series(&ls(&[("a", "1")]), 0, 3, 5, 9);
        b.push_series(&ls(&[("a", "2")]), 3, 2, 1, 4);
        let table = b.finish();
        let text = table.to_footer_value().unwrap();
        assert_eq!(SeriesTable::from_footer_value(&text).unwrap(), table);
    }

    #[test]
    fn corrupt_tables_are_rejected() {
        let mut b = SeriesTableBuilder::new();
        b.begin_metric("m");
        b.push_series(&ls(&[("a", "1")]), 0, 3, 5, 9);
        let good = b.finish();

        let mut t = good.clone();
        t.len.push(1);
        assert!(
            SeriesIndex::from_table(&t).is_err(),
            "array length mismatch"
        );
        let mut t = good.clone();
        t.labels[0][0] = 99;
        assert!(SeriesIndex::from_table(&t).is_err(), "id out of range");
        let mut t = good.clone();
        t.metrics[0].2 = 5;
        assert!(
            SeriesIndex::from_table(&t).is_err(),
            "runs overrun the series"
        );
        let mut t = good;
        t.v = 99;
        assert!(SeriesIndex::from_table(&t).is_err(), "unknown version");
        assert!(SeriesTable::from_footer_value("not json").is_err());
    }

    #[test]
    fn equality_postings_narrow_and_scope_to_the_metric() {
        let idx = sample();
        let eq = |l: &str, v: &str| SeriesSelector {
            equals: vec![(l.into(), v.into())],
            ..Default::default()
        };
        assert_eq!(pick(&idx, "cpu", &eq("host", "h2")), vec!["h2"]);
        assert_eq!(pick(&idx, "cpu", &eq("env", "prod")), vec!["h0", "h1"]);
        // h0 also exists under `mem`, which must not leak into `cpu`.
        assert_eq!(pick(&idx, "mem", &eq("host", "h0")), vec!["h0"]);
        assert_eq!(pick(&idx, "cpu", &eq("host", "h9")), Vec::<String>::new());
        assert_eq!(pick(&idx, "cpu", &eq("nolabel", "x")), Vec::<String>::new());
        assert_eq!(
            pick(&idx, "absent", &SeriesSelector::all()),
            Vec::<String>::new()
        );
    }

    #[test]
    fn multiple_equalities_intersect() {
        let idx = sample();
        let sel = SeriesSelector {
            equals: vec![
                ("env".into(), "stage".into()),
                ("service.name".into(), "api".into()),
            ],
            ..Default::default()
        };
        assert_eq!(pick(&idx, "cpu", &sel), vec!["h2", "h3"]);
        let none = SeriesSelector {
            equals: vec![("env".into(), "prod".into()), ("host".into(), "h3".into())],
            ..Default::default()
        };
        assert!(pick(&idx, "cpu", &none).is_empty());
    }

    #[test]
    fn value_tests_union_matching_values_and_the_filter_has_the_last_word() {
        let idx = sample();
        let test: ValueTest = Arc::new(|v| v == "h1" || v == "h3");
        let sel = SeriesSelector {
            value_tests: vec![("host".into(), test)],
            ..Default::default()
        };
        assert_eq!(pick(&idx, "cpu", &sel), vec!["h1", "h3"]);

        // The filter can refuse what the postings let through.
        let filter: crate::storage::scanner::SeriesFilter =
            Arc::new(|l: &LabelSet| l.get("env") == Some("stage"));
        let sel = SeriesSelector {
            value_tests: vec![("host".into(), Arc::new(|v: &str| v != "h0"))],
            filter: Some(filter),
            ..Default::default()
        };
        assert_eq!(pick(&idx, "cpu", &sel), vec!["h2", "h3"]);
    }

    #[test]
    fn series_outside_the_time_window_are_dropped() {
        let idx = sample();
        // h2 covers [200, 299].
        let ids = idx.select("cpu", &SeriesSelector::all(), 250, 260);
        assert_eq!(ids.len(), 1);
        assert_eq!(idx.series[ids[0] as usize].labels.get("host"), Some("h2"));
        assert!(idx
            .select("cpu", &SeriesSelector::all(), 1000, 2000)
            .is_empty());
    }

    #[test]
    fn row_ranges_merge_adjacent_series() {
        let idx = sample();
        let ids = idx.select("cpu", &SeriesSelector::all(), i64::MIN, i64::MAX);
        assert_eq!(row_ranges(&idx, &ids), vec![(0, 40)]);
        // h0 and h2 are not adjacent.
        let two: Vec<u32> = vec![ids[0], ids[2]];
        assert_eq!(row_ranges(&idx, &two), vec![(0, 10), (20, 30)]);
        assert_eq!(idx.total_rows(), 50);
    }

    #[test]
    fn the_same_series_in_two_blocks_shares_one_label_set() {
        let a = sample();
        let b = sample();
        assert!(a.series[0].labels.ptr_eq(&b.series[0].labels));
    }

    #[test]
    fn restrict_and_intersect_helpers() {
        assert_eq!(
            restrict(&[1, 3, 5, 8, 9], &[(2, 6), (8, 10)]),
            vec![3, 5, 8, 9]
        );
        assert_eq!(restrict(&[1, 3], &[]), Vec::<u32>::new());
        assert_eq!(intersect(&[1, 2, 4, 7], &[2, 3, 7, 9]), vec![2, 7]);
        assert_eq!(intersect(&[], &[1]), Vec::<u32>::new());
    }
}
