use parqtel_core::{DataPoint, LabelSet, Metric, MetricKind, MetricValue};

use crate::expr::CompiledCondition;
use crate::rule::schema::StageType;

use super::stage::{SignalRecord, Stage, StageResult};

/// Extracts a metric from a log record field.
pub struct MetricExtractor {
    pub name: String,
    pub metric_name: String,
    pub metric_type: ExtractedMetricType,
    pub value_field: ValueSource,
    pub dimensions: Vec<String>,
    pub condition: Option<CompiledCondition>,
}

#[derive(Debug, Clone, Copy)]
pub enum ExtractedMetricType {
    Gauge,
    Counter,
    Histogram,
}

#[derive(Debug, Clone)]
pub enum ValueSource {
    Field(String),
    Constant(f64),
}

impl Stage for MetricExtractor {
    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        StageType::MetricExtract
    }

    fn process(&self, record: SignalRecord, extracted: &mut Vec<Metric>) -> StageResult {
        // Check condition
        if let Some(cond) = &self.condition {
            if !cond.evaluate(&record.fields) {
                return StageResult::Continue(record);
            }
        }

        // Get value
        let value = match &self.value_field {
            ValueSource::Constant(v) => Some(*v),
            ValueSource::Field(f) => record.fields.get(f).and_then(|v| v.parse::<f64>().ok()),
        };

        if let Some(val) = value {
            // Build dimension labels
            let label_pairs: Vec<(String, String)> = self
                .dimensions
                .iter()
                .filter_map(|d| record.fields.get(d).map(|v| (d.clone(), v.clone())))
                .collect();
            let labels = LabelSet::try_from_iter(label_pairs).unwrap_or_default();

            let kind = match self.metric_type {
                ExtractedMetricType::Gauge => MetricKind::Gauge,
                ExtractedMetricType::Counter => MetricKind::Sum,
                ExtractedMetricType::Histogram => MetricKind::Histogram,
            };

            let metric_value = match self.metric_type {
                ExtractedMetricType::Gauge | ExtractedMetricType::Counter => {
                    MetricValue::Double(val)
                }
                ExtractedMetricType::Histogram => MetricValue::Histogram {
                    count: 1,
                    sum: val,
                    min: Some(val),
                    max: Some(val),
                    boundaries: vec![],
                    counts: vec![1],
                },
            };

            let dp = DataPoint {
                timestamp_ns: record.log.timestamp_ns,
                value: metric_value,
                labels,
            };

            extracted.push(Metric {
                name: self.metric_name.clone(),
                description: String::new(),
                unit: String::new(),
                kind,
                resource_attributes: LabelSet::default(),
                data_points: vec![dp],
            });
        }

        StageResult::Continue(record)
    }
}
