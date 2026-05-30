use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use ahash::AHasher;
use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};

/// A set of labels (key-value pairs) associated with a metric data point.
///
/// Labels are stored internally as a sorted and deduplicated collection.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Default)]
pub struct LabelSet {
    /// Internal representation of labels, sorted by key.
    #[serde(flatten)]
    inner: BTreeMap<String, String>,
}

impl LabelSet {
    /// Creates a new [LabelSet] from an iterator of key-value pairs.
    ///
    /// Validates that there are no duplicate keys if the source is not already a map.
    pub fn try_from_iter<I, K, V>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut inner = BTreeMap::new();
        for (k, v) in iter {
            let key = k.into();
            if inner.contains_key(&key) {
                return Err(Error::Validation(format!("Duplicate key found in label set: {}", key)));
            }
            inner.insert(key, v.into());
        }
        Ok(Self { inner })
    }

    /// Returns the value associated with the given key, if any.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_str())
    }

    /// Returns an iterator over the labels in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.inner.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Returns an iterator over the label keys in sorted order.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.inner.keys()
    }

    /// Generates a stable numeric fingerprint for this [LabelSet].
    ///
    /// The fingerprint is derived from the sorted key-value pairs and is stable across
    /// process restarts and architectures.
    pub fn fingerprint(&self) -> u64 {
        let mut hasher = AHasher::default();
        for (k, v) in &self.inner {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Merges another [LabelSet] into this one.
    ///
    /// If both sets contain the same key, the value from `other` takes precedence.
    pub fn merge(&self, other: &Self) -> Self {
        let mut inner = self.inner.clone();
        for (k, v) in &other.inner {
            inner.insert(k.clone(), v.clone());
        }
        Self { inner }
    }

    /// Converts the [LabelSet] to a compact JSON string.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(Error::Serde)
    }

    /// Creates a [LabelSet] from a compact JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(Error::Serde)
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
    fn test_label_set_fingerprint_stability() {
        let labels1 = vec![("a", "1"), ("b", "2")];
        let labels2 = vec![("b", "2"), ("a", "1")];
        
        let set1 = LabelSet::try_from_iter(labels1).unwrap();
        let set2 = LabelSet::try_from_iter(labels2).unwrap();
        
        assert_eq!(set1.fingerprint(), set2.fingerprint());
    }

    #[test]
    fn test_label_set_json_roundtrip() {
        let set = LabelSet::try_from_iter(vec![("a", "1"), ("b", "2")]).unwrap();
        let json = set.to_json().unwrap();
        let decoded = LabelSet::from_json(&json).unwrap();
        assert_eq!(set, decoded);
    }
}
