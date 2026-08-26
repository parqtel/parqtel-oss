use super::common::{
    any_value_to_string, json_attrs_to_labels, parse_json_hex, parse_json_timestamp,
};
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use crate::otel::logs::v1::LogRecord as ProtoLogRecord;
use parqtel_core::{Error, LabelSet, LogRecord, Result};

pub(crate) fn decode_logs(request: ExportLogsServiceRequest) -> Result<Vec<LogRecord>> {
    let mut logs = Vec::new();
    for resource_logs in request.resource_logs {
        let resource_attributes = if let Some(resource) = resource_logs.resource {
            LabelSet::try_from_iter(resource.attributes.into_iter().map(|attr| {
                (
                    attr.key,
                    attr.value.map(any_value_to_string).unwrap_or_default(),
                )
            }))?
        } else {
            LabelSet::default()
        };
        for scope_logs in resource_logs.scope_logs {
            let scope_name = scope_logs
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            let scope_version = scope_logs
                .scope
                .as_ref()
                .map(|s| s.version.clone())
                .unwrap_or_default();
            for proto_log in scope_logs.log_records {
                logs.push(convert_log(
                    proto_log,
                    resource_attributes.clone(),
                    scope_name.clone(),
                    scope_version.clone(),
                )?);
            }
        }
    }
    Ok(logs)
}

pub(crate) fn decode_logs_json(json: serde_json::Value) -> Result<Vec<LogRecord>> {
    let mut logs = Vec::new();
    let resource_logs = json
        .get("resource_logs")
        .or(json.get("resourceLogs"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Validation("Missing resource_logs".into()))?;

    for rl in resource_logs {
        let resource_labels = rl
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|a| a.as_array())
            .map(|attrs| json_attrs_to_labels(attrs))
            .transpose()?
            .unwrap_or_default();

        let scope_logs = rl
            .get("scope_logs")
            .or(rl.get("scopeLogs"))
            .and_then(|v| v.as_array());
        if let Some(sls) = scope_logs {
            for sl in sls {
                let scope = sl.get("scope");
                let scope_name = scope
                    .and_then(|s| s.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let scope_version = scope
                    .and_then(|s| s.get("version"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();

                if let Some(records) = sl
                    .get("log_records")
                    .or(sl.get("logRecords"))
                    .and_then(|v| v.as_array())
                {
                    for rec in records {
                        logs.push(json_to_log(
                            rec,
                            resource_labels.clone(),
                            scope_name.clone(),
                            scope_version.clone(),
                        )?);
                    }
                }
            }
        }
    }
    Ok(logs)
}

fn convert_log(
    proto: ProtoLogRecord,
    resource_attributes: LabelSet,
    scope_name: String,
    scope_version: String,
) -> Result<LogRecord> {
    let attributes = LabelSet::try_from_iter(proto.attributes.into_iter().map(|attr| {
        (
            attr.key,
            attr.value.map(any_value_to_string).unwrap_or_default(),
        )
    }))?;
    let mut trace_id = [0u8; 16];
    if proto.trace_id.len() == 16 {
        trace_id.copy_from_slice(&proto.trace_id);
    }
    let mut span_id = [0u8; 8];
    if proto.span_id.len() == 8 {
        span_id.copy_from_slice(&proto.span_id);
    }

    Ok(LogRecord::new(
        proto.time_unix_nano as i64,
        proto.observed_time_unix_nano as i64,
        proto.severity_number,
        proto.severity_text,
        any_value_to_string(proto.body.unwrap_or_default()),
        attributes,
        resource_attributes,
        trace_id,
        span_id,
        proto.flags,
        scope_name,
        scope_version,
    ))
}

fn json_to_log(
    rec: &serde_json::Value,
    resource_attributes: LabelSet,
    scope_name: String,
    scope_version: String,
) -> Result<LogRecord> {
    let ts = parse_json_timestamp(rec.get("time_unix_nano").or(rec.get("timeUnixNano")))?;
    let observed_ts = parse_json_timestamp(
        rec.get("observed_time_unix_nano")
            .or(rec.get("observedTimeUnixNano")),
    )
    .unwrap_or(ts);
    let severity_number = rec
        .get("severity_number")
        .or(rec.get("severityNumber"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let severity_text = rec
        .get("severity_text")
        .or(rec.get("severityText"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let body = rec
        .get("body")
        .and_then(|b| {
            b.get("string_value")
                .or(b.get("stringValue"))
                .and_then(|v| v.as_str())
                .or_else(|| b.as_str())
        })
        .unwrap_or_default()
        .to_string();
    let attributes = if let Some(attrs) = rec.get("attributes").and_then(|a| a.as_array()) {
        json_attrs_to_labels(attrs)?
    } else {
        LabelSet::default()
    };
    let trace_id = parse_json_hex::<16>(rec.get("trace_id").or(rec.get("traceId")))?;
    let span_id = parse_json_hex::<8>(rec.get("span_id").or(rec.get("spanId")))?;
    let flags = rec.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

    Ok(LogRecord::new(
        ts,
        observed_ts,
        severity_number,
        severity_text,
        body,
        attributes,
        resource_attributes,
        trace_id,
        span_id,
        flags,
        scope_name,
        scope_version,
    ))
}
