use super::common::{any_value_to_string, json_attrs_to_labels, parse_json_timestamp};
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::otel::metrics::v1::{
    metric::Data, number_data_point, HistogramDataPoint as ProtoHistogramDataPoint,
    Metric as ProtoMetric, NumberDataPoint as ProtoNumberDataPoint,
    SummaryDataPoint as ProtoSummaryDataPoint,
};
use parqtel_core::{DataPoint, Error, LabelSet, Metric, MetricKind, MetricValue, Result};

pub(crate) fn decode_metrics(request: ExportMetricsServiceRequest) -> Result<Vec<Metric>> {
    let mut metrics = Vec::new();
    for resource_metrics in request.resource_metrics {
        let resource_attributes = if let Some(resource) = resource_metrics.resource {
            LabelSet::try_from_iter(resource.attributes.into_iter().map(|attr| {
                (
                    attr.key,
                    attr.value.map(any_value_to_string).unwrap_or_default(),
                )
            }))?
        } else {
            LabelSet::default()
        };
        for scope_metrics in resource_metrics.scope_metrics {
            for proto_metric in scope_metrics.metrics {
                validate_proto_metric(&proto_metric)?;
                metrics.push(convert_metric(proto_metric, resource_attributes.clone())?);
            }
        }
    }
    Ok(metrics)
}

pub(crate) fn decode_metrics_json(json: serde_json::Value) -> Result<Vec<Metric>> {
    let mut metrics = Vec::new();
    let resource_metrics = json
        .get("resource_metrics")
        .or(json.get("resourceMetrics"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Validation("Missing resource_metrics".into()))?;

    for rm in resource_metrics {
        let resource_labels = rm
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|a| a.as_array())
            .map(|attrs| json_attrs_to_labels(attrs))
            .transpose()?
            .unwrap_or_default();

        let scope_metrics = rm
            .get("scope_metrics")
            .or(rm.get("scopeMetrics"))
            .and_then(|v| v.as_array());
        if let Some(sms) = scope_metrics {
            for sm in sms {
                if let Some(ms) = sm.get("metrics").and_then(|v| v.as_array()) {
                    for m in ms {
                        let name = m
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        if name.is_empty() {
                            continue;
                        }
                        let data_source = m.get("data").unwrap_or(m);

                        let kinds = [
                            ("gauge", MetricKind::Gauge),
                            ("sum", MetricKind::Sum),
                            ("histogram", MetricKind::Histogram),
                            ("summary", MetricKind::Summary),
                        ];
                        for (key, kind) in kinds {
                            if let Some(section) = data_source.get(key) {
                                if let Some(dps) = section
                                    .get("data_points")
                                    .or(section.get("dataPoints"))
                                    .and_then(|v| v.as_array())
                                {
                                    let points: Result<Vec<_>> =
                                        dps.iter().map(json_dp_to_point).collect();
                                    metrics.push(Metric {
                                        name: name.clone(),
                                        description: "".into(),
                                        unit: "".into(),
                                        kind,
                                        resource_attributes: resource_labels.clone(),
                                        data_points: points?,
                                    });
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(metrics)
}

fn validate_proto_metric(metric: &ProtoMetric) -> Result<()> {
    if metric.name.is_empty() {
        return Err(Error::Validation("Metric name cannot be empty".into()));
    }
    if metric.data.is_none() {
        return Err(Error::Validation(format!(
            "Metric {} has no data",
            metric.name
        )));
    }
    Ok(())
}

fn convert_metric(proto: ProtoMetric, resource_attributes: LabelSet) -> Result<Metric> {
    let (kind, data_points) = match proto.data {
        Some(Data::Gauge(gauge)) => (
            MetricKind::Gauge,
            convert_number_data_points(gauge.data_points)?,
        ),
        Some(Data::Sum(sum)) => (
            MetricKind::Sum,
            convert_number_data_points(sum.data_points)?,
        ),
        Some(Data::Histogram(hist)) => (
            MetricKind::Histogram,
            convert_histogram_data_points(hist.data_points)?,
        ),
        Some(Data::Summary(summary)) => (
            MetricKind::Summary,
            convert_summary_data_points(summary.data_points)?,
        ),
        _ => {
            return Err(Error::Validation(format!(
                "Unsupported metric kind for {}",
                proto.name
            )))
        }
    };
    Ok(Metric {
        name: proto.name,
        description: proto.description,
        unit: proto.unit,
        kind,
        resource_attributes,
        data_points,
    })
}

fn convert_number_data_points(protos: Vec<ProtoNumberDataPoint>) -> Result<Vec<DataPoint>> {
    protos
        .into_iter()
        .map(|proto| {
            let value = match proto.value {
                Some(number_data_point::Value::AsDouble(f)) => MetricValue::Double(f),
                Some(number_data_point::Value::AsInt(i)) => MetricValue::Int(i),
                None => MetricValue::Double(0.0),
            };
            let labels = LabelSet::try_from_iter(proto.attributes.into_iter().map(|attr| {
                (
                    attr.key,
                    attr.value.map(any_value_to_string).unwrap_or_default(),
                )
            }))?;
            DataPoint::new(proto.time_unix_nano as i64, value, labels)
        })
        .collect()
}

fn convert_histogram_data_points(protos: Vec<ProtoHistogramDataPoint>) -> Result<Vec<DataPoint>> {
    protos
        .into_iter()
        .map(|proto| {
            let value = MetricValue::Histogram {
                count: proto.count,
                sum: proto.sum.unwrap_or(0.0),
                min: proto.min,
                max: proto.max,
                boundaries: proto.explicit_bounds,
                counts: proto.bucket_counts,
            };
            let labels = LabelSet::try_from_iter(proto.attributes.into_iter().map(|attr| {
                (
                    attr.key,
                    attr.value.map(any_value_to_string).unwrap_or_default(),
                )
            }))?;
            DataPoint::new(proto.time_unix_nano as i64, value, labels)
        })
        .collect()
}

fn convert_summary_data_points(protos: Vec<ProtoSummaryDataPoint>) -> Result<Vec<DataPoint>> {
    protos
        .into_iter()
        .map(|proto| {
            let quantiles = proto
                .quantile_values
                .into_iter()
                .map(|q| (q.quantile, q.value))
                .collect();
            let value = MetricValue::Summary {
                count: proto.count,
                sum: proto.sum,
                quantiles,
            };
            let labels = LabelSet::try_from_iter(proto.attributes.into_iter().map(|attr| {
                (
                    attr.key,
                    attr.value.map(any_value_to_string).unwrap_or_default(),
                )
            }))?;
            DataPoint::new(proto.time_unix_nano as i64, value, labels)
        })
        .collect()
}

fn json_dp_to_point(dp: &serde_json::Value) -> Result<DataPoint> {
    let ts = parse_json_timestamp(dp.get("time_unix_nano").or(dp.get("timeUnixNano")))?;
    let source = dp.get("value").unwrap_or(dp);
    let val_fields = ["as_double", "asDouble", "as_int", "asInt"];
    let mut value = MetricValue::Double(0.0);
    for field in val_fields {
        if let Some(v) = source.get(field) {
            if field.contains("int") || field.contains("Int") {
                value = MetricValue::Int(
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                        .unwrap_or(0),
                );
            } else if let Some(f) = v.as_f64() {
                value = MetricValue::Double(f);
            }
            break;
        }
    }
    let labels = if let Some(attrs) = dp.get("attributes").and_then(|a| a.as_array()) {
        json_attrs_to_labels(attrs)?
    } else {
        LabelSet::default()
    };
    DataPoint::new(ts, value, labels)
}
