use crate::otel::common::v1::{any_value, AnyValue};
use parqtel_core::{Error, LabelSet, Result};

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
