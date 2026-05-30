use std::collections::BTreeMap;

use parqtel_core::{LogRecord, Metric};

use crate::rule::schema::StageType;

/// A signal record flowing through the pipeline.
#[derive(Debug, Clone)]
pub struct SignalRecord {
    pub log: LogRecord,
    /// Extracted/enriched fields available to stages.
    pub fields: BTreeMap<String, String>,
}

impl SignalRecord {
    pub fn from_log(log: LogRecord) -> Self {
        let mut fields = BTreeMap::new();
        // Populate fields from log attributes and resource attributes
        for (k, v) in log.attributes.iter() {
            fields.insert(k.to_string(), v.to_string());
        }
        for (k, v) in log.resource_attributes.iter() {
            fields.insert(k.to_string(), v.to_string());
        }
        fields.insert("body".to_string(), log.body.clone());
        fields.insert("severity_number".to_string(), log.severity_number.to_string());
        fields.insert("severity_text".to_string(), log.severity_text.clone());
        Self { log, fields }
    }
}

/// Result of processing a single record through a stage.
pub enum StageResult {
    Continue(SignalRecord),
    Drop,
    RouteTo { destination: String, record: SignalRecord },
}

/// Trait for pipeline stages.
pub trait Stage: Send + Sync {
    fn stage_name(&self) -> &str;
    fn stage_type(&self) -> StageType;
    /// Process one signal record. Extracted metrics appended to `extracted_metrics`.
    fn process(&self, record: SignalRecord, extracted_metrics: &mut Vec<Metric>) -> StageResult;
}

/// Executes a pipeline of stages against a batch of log records.
pub struct PipelineExecutor {
    stages: Vec<Box<dyn Stage>>,
}

impl PipelineExecutor {
    pub fn new(stages: Vec<Box<dyn Stage>>) -> Self {
        Self { stages }
    }

    /// Process a batch of log records. Returns surviving records and extracted metrics.
    pub fn execute(&self, logs: Vec<LogRecord>) -> (Vec<LogRecord>, Vec<Metric>) {
        let mut surviving = Vec::with_capacity(logs.len());
        let mut extracted_metrics = Vec::new();

        'outer: for log in logs {
            let mut current = Some(SignalRecord::from_log(log));

            for stage in &self.stages {
                let record = match current.take() {
                    Some(r) => r,
                    None => continue 'outer,
                };
                match stage.process(record, &mut extracted_metrics) {
                    StageResult::Continue(r) => current = Some(r),
                    StageResult::Drop | StageResult::RouteTo { .. } => continue 'outer,
                }
            }

            if let Some(rec) = current {
                surviving.push(rec.log);
            }
        }

        (surviving, extracted_metrics)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::pipeline::metric_extractor::{ExtractedMetricType, MetricExtractor, ValueSource};
    use crate::pipeline::processor::{ParseFailureAction, RegexProcessor};
    use crate::pipeline::router::{Router, RouterAction};
    use parqtel_core::LabelSet;
    use std::collections::BTreeMap;

    fn make_log(body: &str, severity: i32) -> LogRecord {
        LogRecord::new(
            1_000_000_000, 1_000_000_001, severity, "INFO".into(),
            body.into(), LabelSet::default(), LabelSet::default(),
            [0; 16], [0; 8], 0, "test".into(), "1.0".into(),
        )
    }

    #[test]
    fn test_regex_processor_extracts_fields() {
        let re = regex::Regex::new(
            r"(?P<method>[A-Z]+) (?P<path>/[^ ]*) HTTP/[\d.]+ (?P<status>\d+) (?P<duration>\d+)ms"
        ).unwrap();
        let mut target_fields = BTreeMap::new();
        target_fields.insert("http.method".into(), "method".into());
        target_fields.insert("http.target".into(), "path".into());
        target_fields.insert("http.status_code".into(), "status".into());
        target_fields.insert("http.duration_ms".into(), "duration".into());

        let stage = RegexProcessor {
            name: "parse".into(),
            source_field: "body".into(),
            regex: re,
            target_fields,
            on_parse_failure: ParseFailureAction::KeepOriginal,
        };

        let log = make_log("GET /api/users HTTP/1.1 200 42ms", 9);
        let record = SignalRecord::from_log(log);
        let mut metrics = Vec::new();
        let result = stage.process(record, &mut metrics);

        match result {
            StageResult::Continue(r) => {
                assert_eq!(r.fields.get("http.method").unwrap(), "GET");
                assert_eq!(r.fields.get("http.target").unwrap(), "/api/users");
                assert_eq!(r.fields.get("http.status_code").unwrap(), "200");
                assert_eq!(r.fields.get("http.duration_ms").unwrap(), "42");
            }
            _ => panic!("Expected Continue"),
        }
    }

    #[test]
    fn test_metric_extractor_gauge_emits_per_record() {
        let stage = MetricExtractor {
            name: "extract".into(),
            metric_name: "request_duration".into(),
            metric_type: ExtractedMetricType::Gauge,
            value_field: ValueSource::Field("duration".into()),
            dimensions: vec!["method".into()],
            condition: None,
        };

        let mut metrics = Vec::new();
        for dur in &["10", "20", "30"] {
            let log = make_log("test", 9);
            let mut record = SignalRecord::from_log(log);
            record.fields.insert("duration".into(), dur.to_string());
            record.fields.insert("method".into(), "GET".into());
            stage.process(record, &mut metrics);
        }
        assert_eq!(metrics.len(), 3);
    }

    #[test]
    fn test_metric_extractor_counter_constant() {
        let stage = MetricExtractor {
            name: "count".into(),
            metric_name: "requests_total".into(),
            metric_type: ExtractedMetricType::Counter,
            value_field: ValueSource::Constant(1.0),
            dimensions: vec![],
            condition: None,
        };

        let mut metrics = Vec::new();
        for _ in 0..10 {
            let log = make_log("req", 9);
            let record = SignalRecord::from_log(log);
            stage.process(record, &mut metrics);
        }
        assert_eq!(metrics.len(), 10);
    }

    #[test]
    fn test_router_drop_removes_record() {
        let cond = crate::expr::DqlParser::parse("severity_number <= 4").unwrap();
        let stage = Router {
            name: "drop_debug".into(),
            condition: cond,
            action: RouterAction::Drop,
        };

        let executor = PipelineExecutor::new(vec![Box::new(stage)]);

        let mut logs = Vec::new();
        for _ in 0..5 {
            logs.push(make_log("debug msg", 4)); // DEBUG
        }
        for _ in 0..5 {
            logs.push(make_log("info msg", 9)); // INFO
        }

        let (surviving, _) = executor.execute(logs);
        assert_eq!(surviving.len(), 5);
    }

    #[test]
    fn test_pipeline_full_flow() {
        let re = regex::Regex::new(r"(?P<status>\d+)").unwrap();
        let mut target = BTreeMap::new();
        target.insert("status".into(), "status".into());

        let stages: Vec<Box<dyn Stage>> = vec![
            Box::new(RegexProcessor {
                name: "parse".into(),
                source_field: "body".into(),
                regex: re,
                target_fields: target,
                on_parse_failure: ParseFailureAction::KeepOriginal,
            }),
            Box::new(MetricExtractor {
                name: "extract".into(),
                metric_name: "status_count".into(),
                metric_type: ExtractedMetricType::Counter,
                value_field: ValueSource::Constant(1.0),
                dimensions: vec!["status".into()],
                condition: None,
            }),
        ];

        let executor = PipelineExecutor::new(stages);
        let logs = vec![make_log("status 200 ok", 9), make_log("status 500 err", 9)];
        let (surviving, metrics) = executor.execute(logs);
        assert_eq!(surviving.len(), 2);
        assert_eq!(metrics.len(), 2);
    }
}
