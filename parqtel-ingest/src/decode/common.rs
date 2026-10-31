use crate::otel::common::v1::{any_value, AnyValue, KeyValue};
use parqtel_core::{Error, LabelInterner, LabelSet, Result};
use std::borrow::Cow;

/// Text form of an attribute value, borrowed when it already is text.
///
/// Matches [`any_value_to_string`] exactly; the difference is that the (by far
/// most common) string value is not copied.
fn value_text(value: &Option<AnyValue>) -> Cow<'_, str> {
    match value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(any_value::Value::StringValue(s)) => Cow::Borrowed(s.as_str()),
        Some(any_value::Value::BoolValue(b)) => Cow::Owned(b.to_string()),
        Some(any_value::Value::IntValue(i)) => Cow::Owned(i.to_string()),
        Some(any_value::Value::DoubleValue(f)) => Cow::Owned(f.to_string()),
        Some(any_value::Value::ArrayValue(a)) => Cow::Owned(format!("{:?}", a)),
        Some(any_value::Value::KvlistValue(k)) => Cow::Owned(format!("{:?}", k)),
        Some(any_value::Value::BytesValue(b)) => Cow::Owned(format!("{:?}", b)),
        None => Cow::Borrowed(""),
    }
}

/// Label set for a protobuf attribute list.
///
/// Reads the keys and string values in place and resolves them through the
/// process-wide interner, so a series that was seen before costs a hash and a
/// compare instead of an allocation per label. Fails on a repeated key, like
/// [`LabelSet::try_from_iter`].
pub(crate) fn kv_labels(attrs: &[KeyValue]) -> Result<LabelSet> {
    let pairs: Vec<(&str, Cow<'_, str>)> = attrs
        .iter()
        .map(|a| (a.key.as_str(), value_text(&a.value)))
        .collect();
    LabelInterner::global().intern(&pairs)
}

/// `base` overlaid with `attrs`, `attrs` winning on a shared key.
///
/// Equivalent to `base.merge(&kv_labels(attrs)?)` without building either
/// intermediate set.
pub(crate) fn kv_labels_over(base: &LabelSet, attrs: &[KeyValue]) -> Result<LabelSet> {
    if base.is_empty() {
        return kv_labels(attrs);
    }
    let own: Vec<(&str, Cow<'_, str>)> = attrs
        .iter()
        .map(|a| (a.key.as_str(), value_text(&a.value)))
        .collect();
    let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(base.len() + own.len());
    pairs.extend(
        base.iter()
            .filter(|(k, _)| !own.iter().any(|(ok, _)| ok == k)),
    );
    pairs.extend(own.iter().map(|(k, v)| (*k, v.as_ref())));
    match LabelInterner::global().intern(&pairs) {
        Ok(set) => Ok(set),
        // A key repeated inside `attrs`: take the slow path for the same error
        // the unmerged set would have produced.
        Err(_) => LabelSet::try_from_iter(own.iter().map(|(k, v)| (*k, v.as_ref())))
            .map(|a| base.merge(&a)),
    }
}

pub(crate) fn any_value_to_string(av: AnyValue) -> String {
    match av.value {
        Some(any_value::Value::StringValue(s)) => s,
        Some(any_value::Value::BoolValue(b)) => b.to_string(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(any_value::Value::DoubleValue(f)) => f.to_string(),
        Some(any_value::Value::ArrayValue(a)) => format!("{:?}", a),
        Some(any_value::Value::KvlistValue(k)) => format!("{:?}", k),
        Some(any_value::Value::BytesValue(b)) => format!("{:?}", b),
        None => String::new(),
    }
}

pub(crate) fn json_attrs_to_labels(attrs: &[serde_json::Value]) -> Result<LabelSet> {
    let mut pairs = Vec::new();
    for attr in attrs {
        let key = attr
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let val_obj = attr.get("value");
        let val = if let Some(v) = val_obj {
            v.get("string_value")
                .or(v.get("stringValue"))
                .and_then(|s| s.as_str())
                .or_else(|| {
                    v.get("int_value")
                        .or(v.get("intValue"))
                        .and_then(|i| i.as_str())
                })
                .unwrap_or_default()
                .to_string()
        } else {
            String::new()
        };
        pairs.push((key, val));
    }
    LabelSet::try_from_iter(pairs)
}

pub(crate) fn parse_json_timestamp(v: Option<&serde_json::Value>) -> Result<i64> {
    match v {
        Some(v) => {
            if let Some(s) = v.as_str() {
                s.parse::<i64>()
                    .map_err(|e| Error::Validation(e.to_string()))
            } else if let Some(i) = v.as_i64() {
                Ok(i)
            } else {
                Err(Error::Validation("Invalid timestamp".into()))
            }
        }
        None => Ok(0),
    }
}

pub(crate) fn parse_json_hex<const N: usize>(v: Option<&serde_json::Value>) -> Result<[u8; N]> {
    let mut out = [0u8; N];
    if let Some(s) = v.and_then(|v| v.as_str()) {
        let bytes =
            hex::decode(s).map_err(|e| Error::Validation(format!("Invalid hex string: {}", e)))?;
        if bytes.len() != N {
            return Err(Error::Validation(format!(
                "Invalid hex length: expected {}, got {}",
                N,
                bytes.len()
            )));
        }
        out.copy_from_slice(&bytes);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::otel::common::v1::{ArrayValue, KeyValueList};

    fn kv(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    fn text(key: &str, v: &str) -> KeyValue {
        kv(key, any_value::Value::StringValue(v.into()))
    }

    /// The borrowing conversion must produce exactly the text the consuming
    /// one did, for every value kind - label values are what queries match on.
    #[test]
    fn value_text_matches_any_value_to_string_for_every_kind() {
        let cases = vec![
            Some(any_value::Value::StringValue("héllo \"q\"".into())),
            Some(any_value::Value::BoolValue(true)),
            Some(any_value::Value::IntValue(-42)),
            Some(any_value::Value::DoubleValue(2.5)),
            Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(any_value::Value::IntValue(1)),
                }],
            })),
            Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![],
            })),
            Some(any_value::Value::BytesValue(vec![1, 2, 3])),
            None,
        ];
        for value in cases {
            let av = AnyValue { value };
            assert_eq!(
                value_text(&Some(av.clone())).as_ref(),
                any_value_to_string(av.clone())
            );
        }
        assert_eq!(value_text(&None).as_ref(), "");
    }

    #[test]
    fn kv_labels_equals_try_from_iter_and_rejects_duplicates() {
        let attrs = vec![
            text("b", "2"),
            kv("a", any_value::Value::IntValue(1)),
            KeyValue {
                key: "empty".into(),
                value: None,
            },
        ];
        let got = kv_labels(&attrs).unwrap();
        let want = LabelSet::try_from_iter(vec![("a", "1"), ("b", "2"), ("empty", "")]).unwrap();
        assert_eq!(got, want);
        assert_eq!(got.json_str(), want.json_str());

        assert!(kv_labels(&[text("k", "1"), text("k", "2")]).is_err());
        assert!(kv_labels(&[]).unwrap().is_empty());
    }

    /// Span attributes overlay the resource's, and the result is the same set
    /// `resource.merge(&span_attrs)` gave.
    #[test]
    fn kv_labels_over_matches_merge_and_span_wins() {
        let resource =
            LabelSet::try_from_iter(vec![("service.name", "api"), ("env", "prod")]).unwrap();
        let attrs = vec![text("env", "stage"), text("route", "/x")];
        let got = kv_labels_over(&resource, &attrs).unwrap();
        let want = resource.merge(&kv_labels(&attrs).unwrap());
        assert_eq!(got, want);
        assert_eq!(got.get("env"), Some("stage"));
        assert_eq!(got.get("service.name"), Some("api"));

        // Empty resource reduces to the attributes alone.
        let only = kv_labels_over(&LabelSet::default(), &attrs).unwrap();
        assert_eq!(only, kv_labels(&attrs).unwrap());

        // A repeated key inside the attributes is still an error.
        assert!(kv_labels_over(&resource, &[text("r", "1"), text("r", "2")]).is_err());
    }
}
