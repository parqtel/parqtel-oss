use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use crate::otel::trace::v1::Span as ProtoSpan;
use parqtel_core::{Error, LabelSet, Result, Span, SpanEvent, SpanLink, SpanStatus};
use super::common::{any_value_to_string, json_attrs_to_labels, parse_json_timestamp, parse_json_hex};

pub(crate) fn decode_traces(request: ExportTraceServiceRequest) -> Result<Vec<Span>> {
    let mut spans = Vec::new();
    for resource_spans in request.resource_spans {
        let resource_attributes = if let Some(resource) = resource_spans.resource {
            LabelSet::try_from_iter(resource.attributes.into_iter().map(|attr| {
                (attr.key, attr.value.map(any_value_to_string).unwrap_or_default())
            }))?
        } else {
            LabelSet::default()
        };
        for scope_spans in resource_spans.scope_spans {
            for proto_span in scope_spans.spans {
                spans.push(convert_span(proto_span, resource_attributes.clone())?);
            }
        }
    }
    Ok(spans)
}

pub(crate) fn decode_traces_json(json: serde_json::Value) -> Result<Vec<Span>> {
    let mut spans = Vec::new();
    let resource_spans = json.get("resource_spans").or(json.get("resourceSpans"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Validation("Missing resource_spans".into()))?;

    for rs in resource_spans {
        let _resource_labels = rs.get("resource").and_then(|r| r.get("attributes")).and_then(|a| a.as_array())
            .map(|attrs| json_attrs_to_labels(attrs)).transpose()?.unwrap_or_default();

        if let Some(sss) = rs.get("scope_spans").or(rs.get("scopeSpans")).and_then(|v| v.as_array()) {
            for ss in sss {
                if let Some(sps) = ss.get("spans").and_then(|v| v.as_array()) {
                    for sp in sps {
                        spans.push(json_to_span(sp)?);
                    }
                }
            }
        }
    }
    Ok(spans)
}

fn convert_span(proto: ProtoSpan, _resource_attributes: LabelSet) -> Result<Span> {
    let mut trace_id = [0u8; 16];
    if proto.trace_id.len() == 16 { trace_id.copy_from_slice(&proto.trace_id); }
    let mut span_id = [0u8; 8];
    if proto.span_id.len() == 8 { span_id.copy_from_slice(&proto.span_id); }
    let mut parent_span_id = [0u8; 8];
    if proto.parent_span_id.len() == 8 { parent_span_id.copy_from_slice(&proto.parent_span_id); }

    let attributes = LabelSet::try_from_iter(proto.attributes.into_iter().map(|attr| {
        (attr.key, attr.value.map(any_value_to_string).unwrap_or_default())
    }))?;

    let events = proto.events.into_iter().map(|e| {
        let attrs = LabelSet::try_from_iter(e.attributes.into_iter().map(|attr| {
            (attr.key, attr.value.map(any_value_to_string).unwrap_or_default())
        })).unwrap_or_default();
        SpanEvent { time_ns: e.time_unix_nano as i64, name: e.name, attributes: attrs }
    }).collect();

    let links = proto.links.into_iter().map(|l| {
        let mut tid = [0u8; 16];
        if l.trace_id.len() == 16 { tid.copy_from_slice(&l.trace_id); }
        let mut sid = [0u8; 8];
        if l.span_id.len() == 8 { sid.copy_from_slice(&l.span_id); }
        let attrs = LabelSet::try_from_iter(l.attributes.into_iter().map(|attr| {
            (attr.key, attr.value.map(any_value_to_string).unwrap_or_default())
        })).unwrap_or_default();
        SpanLink { trace_id: tid, span_id: sid, attributes: attrs }
    }).collect();

    let status = proto.status.map(|s| SpanStatus { code: s.code, message: s.message })
        .unwrap_or(SpanStatus { code: 0, message: String::new() });

    Ok(Span::new(
        trace_id, span_id, proto.trace_state, proto.name, proto.kind,
        proto.start_time_unix_nano as i64, proto.end_time_unix_nano as i64,
        attributes, events, links, status, parent_span_id, proto.flags,
    ))
}

fn json_to_span(sp: &serde_json::Value) -> Result<Span> {
    let trace_id = parse_json_hex::<16>(sp.get("trace_id").or(sp.get("traceId")))?;
    let span_id = parse_json_hex::<8>(sp.get("span_id").or(sp.get("spanId")))?;
    let parent_span_id = parse_json_hex::<8>(sp.get("parent_span_id").or(sp.get("parentSpanId")))?;

    let attributes = if let Some(attrs) = sp.get("attributes").and_then(|a| a.as_array()) {
        json_attrs_to_labels(attrs)?
    } else { LabelSet::default() };

    let events = if let Some(evts) = sp.get("events").and_then(|e| e.as_array()) {
        evts.iter().map(|e| {
            let attrs = e.get("attributes").and_then(|a| a.as_array())
                .map(|a| json_attrs_to_labels(a).unwrap_or_default()).unwrap_or_default();
            SpanEvent {
                time_ns: parse_json_timestamp(e.get("time_unix_nano").or(e.get("timeUnixNano"))).unwrap_or(0),
                name: e.get("name").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                attributes: attrs,
            }
        }).collect()
    } else { Vec::new() };

    let links = if let Some(lnks) = sp.get("links").and_then(|l| l.as_array()) {
        lnks.iter().map(|l| -> Result<SpanLink> {
            Ok(SpanLink {
                trace_id: parse_json_hex::<16>(l.get("trace_id").or(l.get("traceId")))?,
                span_id: parse_json_hex::<8>(l.get("span_id").or(l.get("spanId")))?,
                attributes: l.get("attributes").and_then(|a| a.as_array())
                    .map(|a| json_attrs_to_labels(a).unwrap_or_default()).unwrap_or_default(),
            })
        }).collect::<Result<Vec<_>>>()?
    } else { Vec::new() };

    let name = sp.get("name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let kind = sp.get("kind").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let start_ns = parse_json_timestamp(sp.get("start_time_unix_nano").or(sp.get("startTimeUnixNano")))?;
    let end_ns = parse_json_timestamp(sp.get("end_time_unix_nano").or(sp.get("endTimeUnixNano")))?;
    let trace_state = sp.get("trace_state").or(sp.get("traceState")).and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let flags = sp.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

    let status = sp.get("status").map(|s| SpanStatus {
        code: s.get("code").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
        message: s.get("message").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
    }).unwrap_or(SpanStatus { code: 0, message: String::new() });

    Ok(Span::new(trace_id, span_id, trace_state, name, kind, start_ns, end_ns, attributes, events, links, status, parent_span_id, flags))
}
