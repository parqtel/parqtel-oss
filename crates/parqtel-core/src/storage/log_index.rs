//! Per-block skip index for log blocks.
//!
//! A log block is time-ordered and cut into fixed-size *granules* of rows. For
//! each granule the writer records, in the Parquet footer
//! ([`LOG_INDEX_FOOTER_KEY`]):
//!
//! - its time bounds and highest severity, and
//! - a small bloom filter over the *terms* its rows contain: every three-byte
//!   window of the lower-cased body, and every `name=value` pair of the record's
//!   attributes and resource attributes.
//!
//! A query turns its conditions into necessary terms - a body needle of `n >= 3`
//! bytes needs all of its `n - 2` windows, an attribute equality needs its pair -
//! and reads only the granules that might hold a row meeting all of them. A needle
//! that occurs in one record out of millions touches a granule or two instead of
//! the block.
//!
//! # Exactness
//!
//! Bloom filters answer "possibly present" or "certainly absent", and only the
//! second answer is acted on, so a granule is skipped only if it provably holds no
//! match. A false positive costs a granule read, never a wrong result: the rows of
//! a surviving granule still go through the query's own predicate. Counts and
//! volume buckets are therefore identical to a scan without the index.

use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::storage::scanner::LogColumnPrefilter;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

/// Parquet footer key under which the log index is stored.
pub const LOG_INDEX_FOOTER_KEY: &str = "parqtel.log_index";

/// Rows per granule. Small enough that a rare term leaves little to read, large
/// enough that the filters stay a small share of the block (about a byte per
/// row).
pub const GRANULE_ROWS: usize = 4096;

const FORMAT_VERSION: u32 = 1;
/// Probes per term.
const HASHES: u32 = 3;
/// Bloom bits per distinct term in a granule.
const BITS_PER_TERM: usize = 8;
const MIN_BLOOM_BITS_LOG2: u8 = 9;
const MAX_BLOOM_BITS_LOG2: u8 = 21;
/// Values longer than this are indexed by their first bytes only, which can only
/// widen what a probe accepts if a longer needle is probed - so such pairs are
/// not indexed at all and a query on them is never pruned.
const MAX_PAIR_BYTES: usize = 256;

/// The serialised index, as it appears in the Parquet footer.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct LogIndexTable {
    v: u32,
    granule_rows: u32,
    rows: u64,
    min_ts: Vec<i64>,
    max_ts: Vec<i64>,
    max_severity: Vec<i32>,
    /// log2 of each granule's bloom size in bits.
    bits_log2: Vec<u8>,
    /// The granules' filters, concatenated, base64.
    bloom: String,
}

impl LogIndexTable {
    /// JSON text for the Parquet footer.
    pub fn to_footer_value(&self) -> Result<String> {
        serde_json::to_string(self).map_err(Error::Serde)
    }

    /// Parses the footer value.
    pub fn from_footer_value(value: &str) -> Result<Self> {
        let table: LogIndexTable = serde_json::from_str(value).map_err(Error::Serde)?;
        table.validate()?;
        Ok(table)
    }

    fn validate(&self) -> Result<()> {
        let bad = |what: &str| Error::Internal(format!("corrupt log index: {what}"));
        if self.v != FORMAT_VERSION {
            return Err(bad("unsupported version"));
        }
        let n = self.min_ts.len();
        if self.max_ts.len() != n || self.max_severity.len() != n || self.bits_log2.len() != n {
            return Err(bad("array lengths differ"));
        }
        if self.granule_rows == 0 {
            return Err(bad("empty granules"));
        }
        let expected = (self.rows as usize).div_ceil(self.granule_rows as usize);
        if n != expected {
            return Err(bad("granule count does not match the row count"));
        }
        if self
            .bits_log2
            .iter()
            .any(|b| !(MIN_BLOOM_BITS_LOG2..=MAX_BLOOM_BITS_LOG2).contains(b))
        {
            return Err(bad("filter size out of range"));
        }
        Ok(())
    }
}

/// splitmix64 finaliser: a cheap, well-mixed 64-bit hash of a 64-bit value.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Hash of a three-byte window of a lower-cased body.
fn trigram_hash(window: &[u8]) -> u64 {
    mix(u64::from(window[0]) << 16 | u64::from(window[1]) << 8 | u64::from(window[2]))
}

/// Hash of a `name=value` pair. Lengths are folded in so `("a", "b=c")` and
/// `("a=b", "c")` differ, and the high bit separates the pair space from the
/// trigram space.
fn pair_hash(name: &str, value: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in (name.len() as u64)
        .to_le_bytes()
        .iter()
        .chain(name.as_bytes())
        .chain(value.as_bytes())
    {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(h) | (1 << 63)
}

/// Lower-case hex of `bytes` written into `out` (which must be twice as long).
fn hex_lower<'a>(bytes: &[u8], out: &'a mut [u8]) -> &'a str {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for (i, b) in bytes.iter().enumerate() {
        out[2 * i] = DIGITS[usize::from(b >> 4)];
        out[2 * i + 1] = DIGITS[usize::from(b & 0xf)];
    }
    // Every byte written is an ASCII hex digit.
    std::str::from_utf8(&out[..2 * bytes.len()]).unwrap_or("")
}

/// The bit positions a term sets in a filter of `1 << bits_log2` bits.
fn probes(hash: u64, bits_log2: u8) -> [usize; HASHES as usize] {
    let mask = (1usize << bits_log2) - 1;
    let (h1, h2) = (hash as u32 as usize, ((hash >> 32) as u32 | 1) as usize);
    let mut out = [0usize; HASHES as usize];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = h1.wrapping_add(i.wrapping_mul(h2)) & mask;
    }
    out
}

/// Accumulates a [`LogIndexTable`] while a block's rows are laid out in order.
pub struct LogIndexBuilder {
    table: LogIndexTable,
    bloom: Vec<u8>,
    /// Hashes of the distinct terms of the granule being built.
    terms: Vec<u64>,
    /// One bit per possible trigram (2^24), set while it is in `terms`.
    seen_trigram: Vec<u64>,
    /// The trigram keys whose bit is set, to clear them without sweeping the set.
    trigram_keys: Vec<u32>,
    in_granule: usize,
    min_ts: i64,
    max_ts: i64,
    max_severity: i32,
    /// The sets of the previous row: a run of rows sharing a set is hashed once.
    last_attrs: Option<LabelSet>,
    last_resource: Option<LabelSet>,
    /// Pair terms already added for the granule, by hash.
    seen_pairs: ahash::AHashSet<u64>,
}

impl Default for LogIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LogIndexBuilder {
    pub fn new() -> Self {
        Self {
            table: LogIndexTable {
                v: FORMAT_VERSION,
                granule_rows: GRANULE_ROWS as u32,
                ..Default::default()
            },
            bloom: Vec::new(),
            terms: Vec::new(),
            seen_trigram: vec![0u64; (1 << 24) / 64],
            trigram_keys: Vec::new(),
            in_granule: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            max_severity: i32::MIN,
            last_attrs: None,
            last_resource: None,
            seen_pairs: ahash::AHashSet::new(),
        }
    }

    /// Adds the next row of the block.
    pub fn push_row(
        &mut self,
        timestamp_ns: i64,
        severity: i32,
        body: &str,
        attributes: &LabelSet,
        resource: &LabelSet,
    ) {
        self.push(timestamp_ns, severity, body, attributes, resource, None);
    }

    /// Adds the next row of the block from a record, including its trace and span
    /// ids (as the lower-case hex a query compares against).
    pub fn push_record(&mut self, log: &crate::models::logs::LogRecord) {
        self.push(
            log.timestamp_ns,
            log.severity_number,
            &log.body,
            &log.attributes,
            &log.resource_attributes,
            Some((&log.trace_id, &log.span_id)),
        );
    }

    fn push(
        &mut self,
        timestamp_ns: i64,
        severity: i32,
        body: &str,
        attributes: &LabelSet,
        resource: &LabelSet,
        ids: Option<(&[u8; 16], &[u8; 8])>,
    ) {
        self.min_ts = self.min_ts.min(timestamp_ns);
        self.max_ts = self.max_ts.max(timestamp_ns);
        self.max_severity = self.max_severity.max(severity);

        let b = body.as_bytes();
        if b.len() >= 3 {
            let mut w = [
                b[0].to_ascii_lowercase(),
                b[1].to_ascii_lowercase(),
                b[2].to_ascii_lowercase(),
            ];
            let mut i = 3;
            loop {
                let key = (usize::from(w[0]) << 16) | (usize::from(w[1]) << 8) | usize::from(w[2]);
                let (word, bit) = (key / 64, key % 64);
                if self.seen_trigram[word] & (1 << bit) == 0 {
                    self.seen_trigram[word] |= 1 << bit;
                    self.trigram_keys.push(key as u32);
                    self.terms.push(trigram_hash(&w));
                }
                if i == b.len() {
                    break;
                }
                w = [w[1], w[2], b[i].to_ascii_lowercase()];
                i += 1;
            }
        }

        let same_attrs = self
            .last_attrs
            .as_ref()
            .is_some_and(|l| l.ptr_eq(attributes));
        if !same_attrs {
            self.add_pairs(attributes);
            self.last_attrs = Some(attributes.clone());
        }
        let same_resource = self
            .last_resource
            .as_ref()
            .is_some_and(|l| l.ptr_eq(resource));
        if !same_resource {
            self.add_pairs(resource);
            self.last_resource = Some(resource.clone());
        }

        if let Some((trace_id, span_id)) = ids {
            // An all-zero id means "none" and is not worth a term.
            if trace_id.iter().any(|b| *b != 0) {
                let mut buf = [0u8; 32];
                let h = pair_hash("trace_id", hex_lower(trace_id, &mut buf));
                if self.seen_pairs.insert(h) {
                    self.terms.push(h);
                }
            }
            if span_id.iter().any(|b| *b != 0) {
                let mut buf = [0u8; 16];
                let h = pair_hash("span_id", hex_lower(span_id, &mut buf));
                if self.seen_pairs.insert(h) {
                    self.terms.push(h);
                }
            }
        }

        self.in_granule += 1;
        if self.in_granule == GRANULE_ROWS {
            self.close_granule();
        }
    }

    fn add_pairs(&mut self, labels: &LabelSet) {
        for (k, v) in labels.iter() {
            if k.len() + v.len() > MAX_PAIR_BYTES {
                continue;
            }
            let h = pair_hash(k, v);
            if self.seen_pairs.insert(h) {
                self.terms.push(h);
            }
        }
    }

    fn close_granule(&mut self) {
        if self.in_granule == 0 {
            return;
        }
        let want = (self.terms.len().max(1) * BITS_PER_TERM)
            .next_power_of_two()
            .trailing_zeros() as u8;
        let bits_log2 = want.clamp(MIN_BLOOM_BITS_LOG2, MAX_BLOOM_BITS_LOG2);
        let start = self.bloom.len();
        self.bloom.resize(start + (1usize << bits_log2) / 8, 0);
        let filter = &mut self.bloom[start..];
        for h in &self.terms {
            for p in probes(*h, bits_log2) {
                filter[p / 8] |= 1 << (p % 8);
            }
        }
        // Reset the scratch state for the next granule.
        self.terms.clear();
        for key in self.trigram_keys.drain(..) {
            self.seen_trigram[key as usize / 64] = 0;
        }
        self.seen_pairs.clear();
        self.last_attrs = None;
        self.last_resource = None;

        self.table.min_ts.push(self.min_ts);
        self.table.max_ts.push(self.max_ts);
        self.table.max_severity.push(self.max_severity);
        self.table.bits_log2.push(bits_log2);
        self.table.rows += self.in_granule as u64;
        self.in_granule = 0;
        self.min_ts = i64::MAX;
        self.max_ts = i64::MIN;
        self.max_severity = i32::MIN;
    }

    pub fn finish(mut self) -> LogIndexTable {
        self.close_granule();
        self.table.bloom = base64::engine::general_purpose::STANDARD.encode(&self.bloom);
        self.table
    }
}

/// A block's log index, ready to answer queries.
pub struct LogSkipIndex {
    granule_rows: u64,
    rows: u64,
    min_ts: Vec<i64>,
    max_ts: Vec<i64>,
    max_severity: Vec<i32>,
    bits_log2: Vec<u8>,
    /// Byte offset of each granule's filter in `bloom`.
    offsets: Vec<usize>,
    bloom: Vec<u8>,
}

impl std::fmt::Debug for LogSkipIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSkipIndex")
            .field("granules", &self.min_ts.len())
            .field("rows", &self.rows)
            .finish()
    }
}

impl LogSkipIndex {
    pub fn from_table(table: &LogIndexTable) -> Result<Self> {
        table.validate()?;
        let bloom = base64::engine::general_purpose::STANDARD
            .decode(&table.bloom)
            .map_err(|e| Error::Internal(format!("corrupt log index: {e}")))?;
        let mut offsets = Vec::with_capacity(table.bits_log2.len());
        let mut at = 0usize;
        for b in &table.bits_log2 {
            offsets.push(at);
            at += (1usize << *b) / 8;
        }
        if at != bloom.len() {
            return Err(Error::Internal(
                "corrupt log index: filters do not fill the bloom data".into(),
            ));
        }
        Ok(Self {
            granule_rows: u64::from(table.granule_rows),
            rows: table.rows,
            min_ts: table.min_ts.clone(),
            max_ts: table.max_ts.clone(),
            max_severity: table.max_severity.clone(),
            bits_log2: table.bits_log2.clone(),
            offsets,
            bloom,
        })
    }

    /// Rows the index accounts for.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Estimated resident size, for cache accounting.
    pub fn approx_bytes(&self) -> usize {
        self.bloom.len() + self.min_ts.len() * (8 + 8 + 4 + 1 + 8) + 128
    }

    fn may_contain(&self, granule: usize, hash: u64) -> bool {
        let bits = self.bits_log2[granule];
        let filter = &self.bloom[self.offsets[granule]..];
        probes(hash, bits)
            .iter()
            .all(|p| filter[p / 8] & (1 << (p % 8)) != 0)
    }

    /// Row ranges `[start, end)` of the granules that might hold a row in
    /// `[start_ns, end_ns]` meeting every condition of `pre`, merged where
    /// adjacent.
    pub fn candidate_ranges(
        &self,
        start_ns: i64,
        end_ns: i64,
        pre: Option<&LogColumnPrefilter>,
    ) -> Vec<(u64, u64)> {
        // Necessary terms of the query, hashed once.
        let mut terms: Vec<u64> = Vec::new();
        if let Some(pre) = pre {
            for needle in &pre.body_contains_lower {
                let b = needle.as_bytes();
                for w in b.windows(3) {
                    terms.push(trigram_hash(w));
                }
            }
            for (name, value) in &pre.attr_equals {
                if name.len() + value.len() <= MAX_PAIR_BYTES {
                    terms.push(pair_hash(name, value));
                }
            }
            terms.sort_unstable();
            terms.dedup();
        }
        let min_severity = pre.and_then(|p| p.min_severity);

        let mut out: Vec<(u64, u64)> = Vec::new();
        for g in 0..self.min_ts.len() {
            if self.max_ts[g] < start_ns
                || self.min_ts[g] > end_ns
                || min_severity.is_some_and(|m| self.max_severity[g] < m)
                || !terms.iter().all(|t| self.may_contain(g, *t))
            {
                continue;
            }
            let a = g as u64 * self.granule_rows;
            let b = (a + self.granule_rows).min(self.rows);
            match out.last_mut() {
                Some(last) if last.1 == a => last.1 = b,
                _ => out.push((a, b)),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn ls(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (*k, *v))).unwrap()
    }

    /// 3 granules + a short tail. Row `i` has timestamp `i + 1`; "needle-xyz" is
    /// only in row 5_000 (granule 1); "pod=p7" only in granule 2.
    fn sample() -> LogSkipIndex {
        let mut b = LogIndexBuilder::new();
        let none = LabelSet::default();
        let p7 = ls(&[("pod", "p7")]);
        let other = ls(&[("pod", "p1")]);
        let res = ls(&[("service.name", "api")]);
        for i in 0..(3 * GRANULE_ROWS + 100) {
            let body = if i == 5_000 {
                "an unusual NEEDLE-xyz appears".to_string()
            } else {
                format!("request {i} handled ok")
            };
            let attrs = if (2 * GRANULE_ROWS..3 * GRANULE_ROWS).contains(&i) {
                &p7
            } else if i % 2 == 0 {
                &other
            } else {
                &none
            };
            let sev = if i == 100 { 17 } else { 9 };
            b.push_row(i as i64 + 1, sev, &body, attrs, &res);
        }
        let table = b.finish();
        let text = table.to_footer_value().unwrap();
        LogSkipIndex::from_table(&LogIndexTable::from_footer_value(&text).unwrap()).unwrap()
    }

    fn pre(needles: &[&str], pairs: &[(&str, &str)], sev: Option<i32>) -> LogColumnPrefilter {
        LogColumnPrefilter {
            min_severity: sev,
            body_contains_lower: needles.iter().map(|s| s.to_string()).collect(),
            attr_equals: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            exact: false,
        }
    }

    #[test]
    fn a_rare_body_term_leaves_only_its_granule() {
        let ix = sample();
        let g = GRANULE_ROWS as u64;
        let got = ix.candidate_ranges(0, i64::MAX, Some(&pre(&["needle-xyz"], &[], None)));
        assert_eq!(got, vec![(g, 2 * g)]);
        // Case is folded by the caller; a common term keeps everything.
        let all = ix.candidate_ranges(0, i64::MAX, Some(&pre(&["handled"], &[], None)));
        assert_eq!(all, vec![(0, 3 * g + 100)]);
        // A term nowhere in the block.
        assert!(ix
            .candidate_ranges(0, i64::MAX, Some(&pre(&["absent-term"], &[], None)))
            .is_empty());
    }

    #[test]
    fn short_needles_and_no_prefilter_prune_nothing_but_time() {
        let ix = sample();
        let g = GRANULE_ROWS as u64;
        assert_eq!(
            ix.candidate_ranges(0, i64::MAX, Some(&pre(&["ab"], &[], None))),
            vec![(0, 3 * g + 100)]
        );
        // Timestamps are row + 1: a window inside granule 1 keeps just it.
        assert_eq!(
            ix.candidate_ranges(g as i64 + 10, g as i64 + 20, None),
            vec![(g, 2 * g)]
        );
    }

    #[test]
    fn attribute_pairs_severity_and_conjunction() {
        let ix = sample();
        let g = GRANULE_ROWS as u64;
        assert_eq!(
            ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[("pod", "p7")], None))),
            vec![(2 * g, 3 * g)]
        );
        // A resource attribute is indexed too, and matches everywhere.
        assert_eq!(
            ix.candidate_ranges(
                0,
                i64::MAX,
                Some(&pre(&[], &[("service.name", "api")], None))
            ),
            vec![(0, 3 * g + 100)]
        );
        // Severity 17 occurs only in granule 0.
        assert_eq!(
            ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[], Some(13)))),
            vec![(0, g)]
        );
        // Every condition must hold in the same granule: the needle is in granule
        // 1 and the pod in granule 2.
        assert!(ix
            .candidate_ranges(
                0,
                i64::MAX,
                Some(&pre(&["needle-xyz"], &[("pod", "p7")], None))
            )
            .is_empty());
    }

    #[test]
    fn trace_and_span_ids_narrow_to_their_granule() {
        use crate::models::logs::LogRecord;
        let mut b = LogIndexBuilder::new();
        for i in 0..(2 * GRANULE_ROWS) {
            let (trace, span) = if i == GRANULE_ROWS + 7 {
                ([0xab; 16], [0xcd; 8])
            } else {
                ([0u8; 16], [0u8; 8])
            };
            b.push_record(&LogRecord::new(
                i as i64 + 1,
                i as i64 + 1,
                9,
                "INFO".into(),
                format!("row {i}"),
                LabelSet::default(),
                LabelSet::default(),
                trace,
                span,
                0,
                "".into(),
                "".into(),
            ));
        }
        let ix = LogSkipIndex::from_table(&b.finish()).unwrap();
        let g = GRANULE_ROWS as u64;
        let trace = "ab".repeat(16);
        let span = "cd".repeat(8);
        assert_eq!(
            ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[("trace_id", &trace)], None))),
            vec![(g, 2 * g)]
        );
        assert_eq!(
            ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[("span_id", &span)], None))),
            vec![(g, 2 * g)]
        );
        // The all-zero id is "none", not a value to look up.
        assert!(ix
            .candidate_ranges(
                0,
                i64::MAX,
                Some(&pre(&[], &[("trace_id", &"00".repeat(16))], None))
            )
            .is_empty());
    }

    #[test]
    fn corrupt_tables_are_rejected() {
        let ix_table = {
            let mut b = LogIndexBuilder::new();
            for i in 0..10 {
                b.push_row(
                    i + 1,
                    9,
                    "hello world",
                    &LabelSet::default(),
                    &LabelSet::default(),
                );
            }
            b.finish()
        };
        assert!(LogSkipIndex::from_table(&ix_table).is_ok());
        let mut t = ix_table.clone();
        t.rows = 999_999;
        assert!(LogSkipIndex::from_table(&t).is_err());
        let mut t = ix_table.clone();
        t.bloom.truncate(t.bloom.len() / 2);
        assert!(LogSkipIndex::from_table(&t).is_err());
        let mut t = ix_table;
        t.v = 99;
        assert!(LogIndexTable::from_footer_value(&serde_json::to_string(&t).unwrap()).is_err());
        assert!(LogIndexTable::from_footer_value("{nope").is_err());
    }
}
