//! Tests for the `labels` module.

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
