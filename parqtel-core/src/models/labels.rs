use crate::error::{Error, Result};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, OnceLock};

/// Shared storage behind a [`LabelSet`].
///
/// Entries are sorted by key with unique keys. The fingerprint and the compact
/// JSON form are derived from the entries on first use and then reused, so a
/// set that is shared by many data points pays for each of them once.
#[derive(Debug, Default)]
struct Inner {
    entries: Vec<(String, String)>,
    fingerprint: OnceLock<u64>,
    json: OnceLock<Box<str>>,
}

/// A set of labels (key-value pairs) associated with a metric data point.
///
/// Labels are immutable once built and kept as a sorted, deduplicated list
/// behind an [`Arc`]: cloning a [`LabelSet`] is a reference-count bump, and the
/// fingerprint and JSON encoding are cached on the shared value. Every
/// operation that changes a set ([`merge`](Self::merge),
/// [`with`](Self::with), [`filtered`](Self::filtered)) returns a new one.
#[derive(Debug, Clone)]
pub struct LabelSet {
    inner: Arc<Inner>,
}

impl Default for LabelSet {
    fn default() -> Self {
        static EMPTY: OnceLock<LabelSet> = OnceLock::new();
        EMPTY
            .get_or_init(|| LabelSet::from_sorted_unique(Vec::new()))
            .clone()
    }
}

impl LabelSet {
    fn from_sorted_unique(entries: Vec<(String, String)>) -> Self {
        Self {
            inner: Arc::new(Inner {
                entries,
                fingerprint: OnceLock::new(),
                json: OnceLock::new(),
            }),
        }
    }

    /// Creates a new [LabelSet] from an iterator of key-value pairs.
    ///
    /// Validates that there are no duplicate keys if the source is not already a map.
    pub fn try_from_iter<I, K, V>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut entries: Vec<(String, String)> = iter
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        if !entries.windows(2).all(|w| w[0].0 < w[1].0) {
            // Stable sort keeps the first occurrence of a duplicated key first,
            // so the error names the key a caller supplied twice.
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            if let Some(w) = entries.windows(2).find(|w| w[0].0 == w[1].0) {
                return Err(Error::Validation(format!(
                    "Duplicate key found in label set: {}",
                    w[0].0
                )));
            }
        }
        Ok(Self::from_sorted_unique(entries))
    }

    /// Number of labels in the set.
    pub fn len(&self) -> usize {
        self.inner.entries.len()
    }

    /// Whether the set holds no labels.
    pub fn is_empty(&self) -> bool {
        self.inner.entries.is_empty()
    }

    /// Returns the value associated with the given key, if any.
    pub fn get(&self, key: &str) -> Option<&str> {
        let entries = &self.inner.entries;
        entries
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| entries[i].1.as_str())
    }

    /// Returns an iterator over the labels in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.inner
            .entries
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Returns an iterator over the label keys in sorted order.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.inner.entries.iter().map(|(k, _)| k)
    }

    /// Identity of the shared storage: equal for clones, different for sets built
    /// separately. A cheap key for "have I seen this exact handle", never a
    /// substitute for equality.
    pub fn addr(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }

    /// Whether both handles point at the same shared storage.
    ///
    /// A cheap positive-only equality shortcut: two sets built separately with
    /// equal contents are equal but not `ptr_eq`.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Generates a stable numeric fingerprint for this [LabelSet].
    ///
    /// The fingerprint is derived from the sorted key-value pairs and is the same
    /// in every process, build and architecture: FNV-1a over length-prefixed
    /// names and values, then a finaliser to spread the bits. (A randomly seeded
    /// hasher is not an option: the query layer orders series by fingerprint
    /// before applying its series limit, so a fingerprint that changed on restart
    /// would change which series a capped query returns.) Computed once per
    /// shared set.
    pub fn fingerprint(&self) -> u64 {
        *self.inner.fingerprint.get_or_init(|| {
            const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
            const PRIME: u64 = 0x0000_0100_0000_01b3;
            let mut h = OFFSET;
            let mut eat = |bytes: &[u8]| {
                for b in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
                    h = (h ^ u64::from(*b)).wrapping_mul(PRIME);
                }
            };
            for (k, v) in &self.inner.entries {
                eat(k.as_bytes());
                eat(v.as_bytes());
            }
            // splitmix64 finaliser.
            h ^= h >> 30;
            h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
            h ^= h >> 27;
            h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
            h ^ (h >> 31)
        })
    }

    /// Merges another [LabelSet] into this one.
    ///
    /// If both sets contain the same key, the value from `other` takes precedence.
    pub fn merge(&self, other: &Self) -> Self {
        if other.is_empty() {
            return self.clone();
        }
        let (a, b) = (&self.inner.entries, &other.inner.entries);
        let mut out = Vec::with_capacity(a.len() + b.len());
        let (mut i, mut j) = (0, 0);
        while i < a.len() && j < b.len() {
            match a[i].0.cmp(&b[j].0) {
                Ordering::Less => {
                    out.push(a[i].clone());
                    i += 1;
                }
                Ordering::Greater => {
                    out.push(b[j].clone());
                    j += 1;
                }
                Ordering::Equal => {
                    out.push(b[j].clone());
                    i += 1;
                    j += 1;
                }
            }
        }
        out.extend_from_slice(&a[i..]);
        out.extend_from_slice(&b[j..]);
        Self::from_sorted_unique(out)
    }

    /// Copy of this set with the entries rejected by `keep` removed.
    ///
    /// One pass, O(L). The obvious alternative — repeatedly calling
    /// [`Self::merge`] once per unwanted label — is O(L²), because every
    /// `merge` copies the whole set. Aggregations and `histogram_quantile` both
    /// used to build output label sets that way, once per series per step.
    pub fn filtered<F: FnMut(&str, &str) -> bool>(&self, mut keep: F) -> Self {
        let entries: Vec<(String, String)> = self
            .inner
            .entries
            .iter()
            .filter(|(k, v)| keep(k, v))
            .cloned()
            .collect();
        if entries.len() == self.inner.entries.len() {
            return self.clone();
        }
        Self::from_sorted_unique(entries)
    }

    /// Copy of this set with `key` set to `value`.
    ///
    /// Single-pass alternative to `merge` with a one-entry set.
    pub fn with(&self, key: &str, value: &str) -> Self {
        let entries = &self.inner.entries;
        let mut out = Vec::with_capacity(entries.len() + 1);
        match entries.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
            Ok(i) => {
                out.extend_from_slice(&entries[..i]);
                out.push((key.to_string(), value.to_string()));
                out.extend_from_slice(&entries[i + 1..]);
            }
            Err(i) => {
                out.extend_from_slice(&entries[..i]);
                out.push((key.to_string(), value.to_string()));
                out.extend_from_slice(&entries[i..]);
            }
        }
        Self::from_sorted_unique(out)
    }

    /// Compact JSON object text for this set, encoded once and cached.
    ///
    /// Prefer this over [`Self::to_json`] on hot paths: sets shared by many rows
    /// are encoded a single time.
    pub fn json_str(&self) -> &str {
        self.inner.json.get_or_init(|| {
            let mut s = String::with_capacity(2 + self.inner.entries.len() * 24);
            s.push('{');
            for (i, (k, v)) in self.inner.entries.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                push_json_string(&mut s, k);
                s.push(':');
                push_json_string(&mut s, v);
            }
            s.push('}');
            s.into_boxed_str()
        })
    }

    /// Converts the [LabelSet] to a compact JSON string.
    pub fn to_json(&self) -> Result<String> {
        Ok(self.json_str().to_string())
    }

    /// Creates a [LabelSet] from a compact JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(Error::Serde)
    }
}

/// Process-wide table of recently seen label sets.
///
/// A series is scraped over and over, and every request used to rebuild its
/// label set from scratch. Resolving a request's attributes to a set that is
/// already shared makes the repeat free (no allocation) and keeps the cached
/// fingerprint and JSON warm for the life of the series, instead of recomputing
/// them for each copy.
///
/// It is a cache and nothing more: a miss builds the set the ordinary way, an
/// entry may be evicted at any time, and equality is always decided by contents,
/// so no result can depend on whether a set was interned.
pub struct LabelInterner {
    shards: Vec<std::sync::Mutex<InternShard>>,
    hasher: ahash::RandomState,
}

#[derive(Default)]
struct InternShard {
    buckets: std::collections::HashMap<u64, Vec<LabelSet>>,
    entries: usize,
}

/// Shards, and the number of distinct sets each may hold before it is emptied.
/// 16 x 16K bounds the table at ~262K series (tens of MB).
const INTERN_SHARDS: usize = 16;
const INTERN_SHARD_CAP: usize = 16 * 1024;
/// Attribute lists longer than this are not interned.
const INTERN_MAX_PAIRS: usize = 32;

impl LabelInterner {
    /// The shared instance.
    pub fn global() -> &'static LabelInterner {
        static GLOBAL: OnceLock<LabelInterner> = OnceLock::new();
        GLOBAL.get_or_init(|| LabelInterner {
            shards: (0..INTERN_SHARDS)
                .map(|_| std::sync::Mutex::new(InternShard::default()))
                .collect(),
            hasher: ahash::RandomState::with_seeds(
                0x243f_6a88_85a3_08d3,
                0x1319_8a2e_0370_7344,
                0xa409_3822_299f_31d0,
                0x082e_fa98_ec4e_6c89,
            ),
        })
    }

    /// Order-independent hash of the pairs.
    fn hash_pairs<K: AsRef<str>, V: AsRef<str>>(&self, pairs: &[(K, V)]) -> u64 {
        let mut acc = pairs.len() as u64;
        for (k, v) in pairs {
            let mut h = self.hasher.build_hasher();
            k.as_ref().hash(&mut h);
            v.as_ref().hash(&mut h);
            acc = acc.wrapping_add(h.finish());
        }
        acc
    }

    /// Resolves `pairs` (in any order) to a [`LabelSet`], reusing an equal one
    /// when it is known. Fails on a repeated key, as [`LabelSet::try_from_iter`]
    /// does.
    pub fn intern<K: AsRef<str>, V: AsRef<str>>(&self, pairs: &[(K, V)]) -> Result<LabelSet> {
        if pairs.is_empty() {
            return Ok(LabelSet::default());
        }
        let build = || LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (k.as_ref(), v.as_ref())));
        if pairs.len() > INTERN_MAX_PAIRS || has_repeated_key(pairs) {
            return build();
        }
        let hash = self.hash_pairs(pairs);
        let shard = &self.shards[(hash as usize) % INTERN_SHARDS];
        {
            let guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(bucket) = guard.buckets.get(&hash) {
                for cand in bucket {
                    // Keys are unique on both sides, so equal length plus every
                    // pair found in the candidate is equality.
                    if cand.len() == pairs.len()
                        && pairs
                            .iter()
                            .all(|(k, v)| cand.get(k.as_ref()) == Some(v.as_ref()))
                    {
                        return Ok(cand.clone());
                    }
                }
            }
        }
        let built = build()?;
        let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
        if guard.entries >= INTERN_SHARD_CAP {
            guard.buckets.clear();
            guard.entries = 0;
        }
        guard.buckets.entry(hash).or_default().push(built.clone());
        guard.entries += 1;
        Ok(built)
    }
}

impl LabelInterner {
    /// `base` with `key` set to `value`, resolved to a shared set.
    ///
    /// The same set as [`LabelSet::with`], but a repeat of an already seen
    /// combination returns the shared instance instead of building a new one.
    pub fn intern_with(&self, base: &LabelSet, key: &str, value: &str) -> LabelSet {
        let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(base.len() + 1);
        pairs.extend(base.iter().filter(|(k, _)| *k != key));
        pairs.push((key, value));
        self.intern(&pairs)
            .unwrap_or_else(|_| base.with(key, value))
    }
}

fn has_repeated_key<K: AsRef<str>, V>(pairs: &[(K, V)]) -> bool {
    for (i, (k, _)) in pairs.iter().enumerate() {
        if pairs[..i]
            .iter()
            .any(|(other, _)| other.as_ref() == k.as_ref())
        {
            return true;
        }
    }
    false
}

/// Appends `s` as a JSON string literal, escaping exactly as `serde_json` does.
fn push_json_string(out: &mut String, s: &str) {
    let needs_escape = s.bytes().any(|b| b < 0x20 || b == b'"' || b == b'\\');
    if !needs_escape {
        out.push('"');
        out.push_str(s);
        out.push('"');
        return;
    }
    // Rare path: delegate so the escaping rules stay serde_json's own.
    match serde_json::to_string(s) {
        Ok(escaped) => out.push_str(&escaped),
        Err(_) => out.push_str("\"\""),
    }
}

impl PartialEq for LabelSet {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other) || self.inner.entries == other.inner.entries
    }
}

impl Eq for LabelSet {}

impl PartialOrd for LabelSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LabelSet {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.ptr_eq(other) {
            return Ordering::Equal;
        }
        self.inner.entries.cmp(&other.inner.entries)
    }
}

impl Hash for LabelSet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.fingerprint());
    }
}

impl Serialize for LabelSet {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.inner.entries.len()))?;
        for (k, v) in &self.inner.entries {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for LabelSet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct LabelVisitor;

        impl<'de> Visitor<'de> for LabelVisitor {
            type Value = LabelSet;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a map of string labels")
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut access: A,
            ) -> std::result::Result<LabelSet, A::Error> {
                let mut entries: Vec<(String, String)> =
                    Vec::with_capacity(access.size_hint().unwrap_or(4).min(64));
                let mut sorted = true;
                while let Some((k, v)) = access.next_entry::<String, String>()? {
                    if let Some(last) = entries.last() {
                        if last.0 >= k {
                            sorted = false;
                        }
                    }
                    entries.push((k, v));
                }
                if !sorted {
                    // A repeated key keeps its last value, as a map insert would.
                    entries.reverse();
                    entries.sort_by(|a, b| a.0.cmp(&b.0));
                    entries.dedup_by(|later, earlier| later.0 == earlier.0);
                }
                Ok(LabelSet::from_sorted_unique(entries))
            }
        }

        deserializer.deserialize_map(LabelVisitor)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_label_set_sorting_and_uniqueness() {
        let labels = vec![("z", "1"), ("a", "2")];
        let set = LabelSet::try_from_iter(labels).unwrap();
        let collected: Vec<_> = set.iter().collect();
        assert_eq!(collected, vec![("a", "2"), ("z", "1")]);
    }

    #[test]
    fn test_label_set_duplicate_keys() {
        let labels = vec![("a", "1"), ("a", "2")];
        let result = LabelSet::try_from_iter(labels);
        assert!(result.is_err());
    }

    #[test]
    fn test_label_set_duplicate_keys_not_adjacent() {
        let result = LabelSet::try_from_iter(vec![("a", "1"), ("b", "x"), ("a", "2")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_label_set_fingerprint_stability() {
        let labels1 = vec![("a", "1"), ("b", "2")];
        let labels2 = vec![("b", "2"), ("a", "1")];

        let set1 = LabelSet::try_from_iter(labels1).unwrap();
        let set2 = LabelSet::try_from_iter(labels2).unwrap();

        assert_eq!(set1.fingerprint(), set2.fingerprint());
    }

    const PINNED: u64 = 4_467_025_367_253_129_411;

    #[test]
    fn test_label_set_fingerprint_value_is_pinned() {
        // The fingerprint orders series inside a query (and so decides which
        // survive a series limit), so it must not change between processes or
        // releases: pin the value itself.
        let set = LabelSet::try_from_iter(vec![("b", "2"), ("a", "1")]).unwrap();
        assert_eq!(set.fingerprint(), PINNED);
        // The value boundary matters: ("ab","c") is not ("a","bc").
        let x = LabelSet::try_from_iter(vec![("ab", "c")]).unwrap();
        let y = LabelSet::try_from_iter(vec![("a", "bc")]).unwrap();
        assert_ne!(x.fingerprint(), y.fingerprint());
    }

    #[test]
    fn test_label_set_json_roundtrip() {
        let set = LabelSet::try_from_iter(vec![("a", "1"), ("b", "2")]).unwrap();
        let json = set.to_json().unwrap();
        let decoded = LabelSet::from_json(&json).unwrap();
        assert_eq!(set, decoded);
    }

    #[test]
    fn test_json_matches_serde_json_encoding() {
        let set = LabelSet::try_from_iter(vec![
            ("plain", "v"),
            ("quote\"key", "line\nbreak"),
            ("back\\slash", "tab\there"),
            ("unicode", "héllo ✓"),
            ("ctl", "\u{1}"),
        ])
        .unwrap();
        let expected: std::collections::BTreeMap<_, _> = set
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(set.json_str(), serde_json::to_string(&expected).unwrap());
        assert_eq!(LabelSet::from_json(set.json_str()).unwrap(), set);
    }

    #[test]
    fn test_empty_set_json_and_default() {
        let d = LabelSet::default();
        assert!(d.is_empty());
        assert_eq!(d.json_str(), "{}");
        assert_eq!(LabelSet::from_json("{}").unwrap(), d);
    }

    #[test]
    fn test_deserialize_unsorted_and_repeated_keys() {
        let set = LabelSet::from_json(r#"{"b":"2","a":"1","b":"3"}"#).unwrap();
        let got: Vec<_> = set.iter().collect();
        assert_eq!(got, vec![("a", "1"), ("b", "3")]);
    }

    #[test]
    fn test_deserialize_rejects_non_string_values() {
        assert!(LabelSet::from_json(r#"{"a":1}"#).is_err());
        assert!(LabelSet::from_json(r#"["a"]"#).is_err());
    }

    #[test]
    fn test_merge_with_filtered() {
        let a = LabelSet::try_from_iter(vec![("a", "1"), ("c", "3")]).unwrap();
        let b = LabelSet::try_from_iter(vec![("b", "2"), ("c", "x")]).unwrap();
        let m = a.merge(&b);
        let got: Vec<_> = m.iter().collect();
        assert_eq!(got, vec![("a", "1"), ("b", "2"), ("c", "x")]);

        let w = a.with("b", "9").with("a", "0");
        let got: Vec<_> = w.iter().collect();
        assert_eq!(got, vec![("a", "0"), ("b", "9"), ("c", "3")]);

        let f = m.filtered(|k, _| k != "b");
        let got: Vec<_> = f.iter().collect();
        assert_eq!(got, vec![("a", "1"), ("c", "x")]);
        // Nothing removed: shares storage instead of copying.
        assert!(m.filtered(|_, _| true).ptr_eq(&m));
    }

    #[test]
    fn test_get_and_ordering_match_map_semantics() {
        let a = LabelSet::try_from_iter(vec![("a", "1"), ("b", "2")]).unwrap();
        assert_eq!(a.get("a"), Some("1"));
        assert_eq!(a.get("zz"), None);
        let b = LabelSet::try_from_iter(vec![("a", "1"), ("c", "0")]).unwrap();
        assert!(a < b);
        assert_eq!(a.cmp(&a.clone()), Ordering::Equal);
    }

    #[test]
    fn test_clone_shares_storage_and_cached_json() {
        let a = LabelSet::try_from_iter(vec![("a", "1")]).unwrap();
        let b = a.clone();
        assert!(a.ptr_eq(&b));
        assert!(std::ptr::eq(a.json_str(), b.json_str()));
    }

    #[test]
    fn interner_shares_equal_sets_in_any_order() {
        let i = LabelInterner::global();
        let a = i.intern(&[("zz_it", "1"), ("aa_it", "2")]).unwrap();
        let b = i.intern(&[("aa_it", "2"), ("zz_it", "1")]).unwrap();
        assert!(
            a.ptr_eq(&b),
            "equal contents must resolve to one shared set"
        );
        assert_eq!(
            a,
            LabelSet::try_from_iter(vec![("aa_it", "2"), ("zz_it", "1")]).unwrap()
        );
        let c = i.intern(&[("aa_it", "2"), ("zz_it", "other")]).unwrap();
        assert!(!a.ptr_eq(&c));
        assert_ne!(a, c);
    }

    #[test]
    fn intern_with_matches_with_and_shares_repeats() {
        let i = LabelInterner::global();
        let base = LabelSet::try_from_iter(vec![("iw_host", "a"), ("iw_env", "p")]).unwrap();
        let a = i.intern_with(&base, "service.name", "api");
        assert_eq!(a, base.with("service.name", "api"));
        // A repeat resolves to the very same shared set.
        let again = i.intern_with(&base.clone(), "service.name", "api");
        assert!(a.ptr_eq(&again));
        // Replacing an existing key keeps one entry for it.
        let over = i.intern_with(&base, "iw_env", "q");
        assert_eq!(over, base.with("iw_env", "q"));
        assert_eq!(over.len(), 2);
    }

    #[test]
    fn interner_rejects_repeated_keys_and_handles_empty() {
        let i = LabelInterner::global();
        assert!(i.intern(&[("k_it", "1"), ("k_it", "1")]).is_err());
        assert!(i
            .intern(&[("k_it", "1"), ("j_it", "x"), ("k_it", "2")])
            .is_err());
        assert!(i.intern::<&str, &str>(&[]).unwrap().is_empty());
    }

    #[test]
    fn interner_does_not_confuse_subsets_with_equal_length_sets() {
        let i = LabelInterner::global();
        let ab = i.intern(&[("sa_it", "1"), ("sb_it", "2")]).unwrap();
        let ac = i.intern(&[("sa_it", "1"), ("sc_it", "2")]).unwrap();
        assert!(!ab.ptr_eq(&ac));
        assert_eq!(ab.get("sc_it"), None);
        assert_eq!(ac.get("sb_it"), None);
    }
}
