//! Compile YAML pipeline definitions into executable stages.
//!
//! The registry stores [`PipelineDefinition`] values verbatim for the API. This
//! module is the missing link between those values and [`PipelineExecutor`]:
//! every stage type, condition operator, and required field is validated once
//! at load time, so a typo fails loudly instead of producing silence.

use parqtel_core::LogRecord;
use regex::Regex;

use crate::expr::CompiledCondition;
use crate::rule::schema::{
    MaskRule as MaskRuleDef, PipelineDefinition, PipelineStage, StageCondition, StageType,
};

use super::masker::{MaskRule, Masker};
use super::metric_extractor::{ExtractedMetricType, MetricExtractor, ValueSource};
use super::preprocessor::Preprocessor;
use super::processor::{
    AddFieldProcessor, CastProcessor, CastType, ParseFailureAction, RegexProcessor,
};
use super::router::{Router, RouterAction};
use super::stage::{PipelineExecutor, SignalRecord, Stage};

/// A validated pipeline ready to run over log records.
pub struct CompiledPipeline {
    pub name: String,
    matcher: Option<CompiledCondition>,
    executor: PipelineExecutor,
}

impl CompiledPipeline {
    pub fn matches(&self, log: &LogRecord) -> bool {
        self.matcher
            .as_ref()
            .map(|condition| condition.evaluate(&SignalRecord::fields_for_log(log)))
            .unwrap_or(true)
    }

    pub fn execute(&self, logs: Vec<LogRecord>) -> (Vec<LogRecord>, Vec<parqtel_core::Metric>) {
        self.executor.execute(logs)
    }
}

/// A set of compiled pipelines, applied first-match-wins per log record.
#[derive(Default)]
pub struct PipelineSet {
    pipelines: Vec<CompiledPipeline>,
}

impl PipelineSet {
    pub fn from_definitions(definitions: &[PipelineDefinition]) -> crate::Result<Self> {
        let pipelines = definitions
            .iter()
            .map(compile_pipeline)
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self { pipelines })
    }

    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }

    pub fn len(&self) -> usize {
        self.pipelines.len()
    }

    /// Apply the first matching pipeline to every log. Logs with no matching
    /// pipeline pass through unchanged; extracted metrics are returned beside
    /// the surviving logs.
    pub fn execute(&self, logs: Vec<LogRecord>) -> (Vec<LogRecord>, Vec<parqtel_core::Metric>) {
        let mut surviving = Vec::with_capacity(logs.len());
        let mut metrics = Vec::new();
        for log in logs {
            match self
                .pipelines
                .iter()
                .find(|pipeline| pipeline.matches(&log))
            {
                Some(pipeline) => {
                    let (mut kept, mut extracted) = pipeline.execute(vec![log]);
                    surviving.append(&mut kept);
                    metrics.append(&mut extracted);
                }
                None => surviving.push(log),
            }
        }
        (surviving, metrics)
    }
}

/// Compile one definition, naming the pipeline and stage in every error.
pub fn compile_pipeline(definition: &PipelineDefinition) -> crate::Result<CompiledPipeline> {
    let match_config = definition.match_config.as_ref();
    let signal = match_config.map(|m| m.signal.as_str()).unwrap_or("logs");
    if signal != "logs" {
        return Err(crate::Error::Validation(format!(
            "pipeline '{}': only logs are supported, got signal '{signal}'",
            definition.name
        )));
    }
    let matcher = match match_config {
        Some(config) => Some(compile_conditions(&config.conditions).map_err(|e| {
            crate::Error::Validation(format!("pipeline '{}': match: {e}", definition.name))
        })?),
        None => None,
    };

    let mut stages: Vec<Box<dyn Stage>> = Vec::with_capacity(definition.stages.len());
    for stage in &definition.stages {
        stages.push(compile_stage(&definition.name, stage)?);
    }
    Ok(CompiledPipeline {
        name: definition.name.clone(),
        matcher,
        executor: PipelineExecutor::new(stages),
    })
}

fn stage_error(pipeline: &str, stage: &PipelineStage, message: &str) -> crate::Error {
    crate::Error::Validation(format!(
        "pipeline '{pipeline}', stage '{}': {message}",
        stage.name
    ))
}

fn required<'a>(
    pipeline: &str,
    stage: &'a PipelineStage,
    value: &'a Option<String>,
    field: &str,
) -> crate::Result<&'a str> {
    value
        .as_deref()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| stage_error(pipeline, stage, &format!("missing {field}")))
}

fn compile_stage(pipeline: &str, stage: &PipelineStage) -> crate::Result<Box<dyn Stage>> {
    match stage.stage_type {
        StageType::Preprocessor => Ok(Box::new(Preprocessor {
            name: stage.name.clone(),
        })),
        StageType::Processor => compile_processor(pipeline, stage),
        StageType::MetricExtract => {
            let metric_name = required(pipeline, stage, &stage.metric_name, "metric_name")?;
            let value_field = required(pipeline, stage, &stage.value_field, "value_field")?;
            let value_field =
                match value_field.strip_prefix("constant:") {
                    Some(value) => ValueSource::Constant(value.parse::<f64>().map_err(|_| {
                        stage_error(pipeline, stage, "constant value is not a number")
                    })?),
                    None => ValueSource::Field(value_field.to_string()),
                };
            let metric_type = match stage.metric_type.as_deref().unwrap_or("gauge") {
                "gauge" => ExtractedMetricType::Gauge,
                "counter" | "sum" => ExtractedMetricType::Counter,
                "histogram" => ExtractedMetricType::Histogram,
                other => {
                    return Err(stage_error(
                        pipeline,
                        stage,
                        &format!("unknown metric_type '{other}'"),
                    ))
                }
            };
            let condition = stage
                .condition
                .as_ref()
                .map(compile_condition)
                .transpose()
                .map_err(|e| stage_error(pipeline, stage, &e.to_string()))?;
            Ok(Box::new(MetricExtractor {
                name: stage.name.clone(),
                metric_name: metric_name.to_string(),
                metric_type,
                value_field,
                dimensions: stage.dimensions.clone().unwrap_or_default(),
                condition,
            }))
        }
        StageType::Masker => {
            let rules = stage
                .rules
                .as_ref()
                .filter(|rules| !rules.is_empty())
                .ok_or_else(|| stage_error(pipeline, stage, "masker requires rules"))?;
            let rules = rules
                .iter()
                .map(|rule| compile_mask_rule(pipeline, stage, rule))
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(Box::new(Masker {
                name: stage.name.clone(),
                rules,
            }))
        }
        StageType::Router => {
            let condition = match (&stage.condition, &stage.conditions) {
                (_, Some(conditions)) if !conditions.is_empty() => {
                    compile_conditions(conditions)
                        .map_err(|e| stage_error(pipeline, stage, &e.to_string()))?
                }
                (Some(condition), _) => compile_condition(condition)
                    .map_err(|e| stage_error(pipeline, stage, &e.to_string()))?,
                _ => {
                    return Err(stage_error(
                        pipeline,
                        stage,
                        "router requires condition or conditions",
                    ))
                }
            };
            let action = match stage.action.as_deref().unwrap_or("drop") {
                "drop" => RouterAction::Drop,
                "route" | "route_to" => {
                    let destination = stage.destination.clone().ok_or_else(|| {
                        stage_error(pipeline, stage, "route action requires destination")
                    })?;
                    RouterAction::RouteTo(destination)
                }
                other => {
                    return Err(stage_error(
                        pipeline,
                        stage,
                        &format!("unknown router action '{other}'"),
                    ))
                }
            };
            Ok(Box::new(Router {
                name: stage.name.clone(),
                condition,
                action,
            }))
        }
    }
}

fn compile_processor(pipeline: &str, stage: &PipelineStage) -> crate::Result<Box<dyn Stage>> {
    let processor = stage.processor.as_deref().unwrap_or("add_fields");
    match processor {
        "regex_extract" => {
            let source_field = required(pipeline, stage, &stage.source_field, "source_field")?;
            let pattern = required(pipeline, stage, &stage.pattern, "pattern")?;
            // YAML `pattern: |` keeps a trailing newline; the log body rarely
            // has one, and a regex that silently requires it never matches.
            let pattern = pattern.trim_end_matches(['\n', '\r']);
            let regex = Regex::new(pattern)
                .map_err(|e| stage_error(pipeline, stage, &format!("invalid regex: {e}")))?;
            let target_fields = stage.target_fields.clone().unwrap_or_default();
            if target_fields.is_empty() {
                return Err(stage_error(
                    pipeline,
                    stage,
                    "regex_extract requires target_fields",
                ));
            }
            let on_parse_failure =
                match stage.on_parse_failure.as_deref().unwrap_or("keep_original") {
                    "drop" => ParseFailureAction::Drop,
                    "keep_original" | "" => ParseFailureAction::KeepOriginal,
                    other => {
                        return Err(stage_error(
                            pipeline,
                            stage,
                            &format!("unknown on_parse_failure '{other}'"),
                        ))
                    }
                };
            Ok(Box::new(RegexProcessor {
                name: stage.name.clone(),
                source_field: source_field.to_string(),
                regex,
                target_fields,
                on_parse_failure,
            }))
        }
        "add_fields" | "add_field" => {
            let fields = stage
                .fields
                .as_ref()
                .ok_or_else(|| stage_error(pipeline, stage, "add_fields requires fields"))?
                .iter()
                .map(|(key, value)| (key.clone(), json_value_string(value)))
                .collect();
            Ok(Box::new(AddFieldProcessor {
                name: stage.name.clone(),
                fields,
                condition: None,
            }))
        }
        "cast" | "cast_fields" => {
            let fields = stage
                .fields
                .as_ref()
                .ok_or_else(|| stage_error(pipeline, stage, "cast requires fields"))?;
            let mut casts = std::collections::BTreeMap::new();
            for (field, value) in fields {
                let cast = match value.as_str().unwrap_or_default() {
                    "integer" | "int" => CastType::Integer,
                    "float" | "double" => CastType::Float,
                    other => {
                        return Err(stage_error(
                            pipeline,
                            stage,
                            &format!("unknown cast '{other}' for field '{field}'"),
                        ))
                    }
                };
                casts.insert(field.clone(), cast);
            }
            Ok(Box::new(CastProcessor {
                name: stage.name.clone(),
                fields: casts,
            }))
        }
        other => Err(stage_error(
            pipeline,
            stage,
            &format!("unknown processor '{other}'"),
        )),
    }
}

fn compile_mask_rule(
    pipeline: &str,
    stage: &PipelineStage,
    rule: &MaskRuleDef,
) -> crate::Result<MaskRule> {
    if rule.field.is_empty() {
        return Err(stage_error(pipeline, stage, "mask rule has an empty field"));
    }
    let pattern = rule.pattern.trim_end_matches(['\n', '\r']);
    let regex = Regex::new(pattern)
        .map_err(|e| stage_error(pipeline, stage, &format!("invalid mask regex: {e}")))?;
    Ok(MaskRule {
        field: rule.field.clone(),
        regex,
        replacement: rule.replacement.clone(),
    })
}

fn json_value_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn compile_conditions(conditions: &[StageCondition]) -> crate::Result<CompiledCondition> {
    let mut result = CompiledCondition::True;
    for condition in conditions {
        let compiled = compile_condition(condition)?;
        result = match result {
            CompiledCondition::True => compiled,
            left => CompiledCondition::And(Box::new(left), Box::new(compiled)),
        };
    }
    Ok(result)
}

fn compile_condition(condition: &StageCondition) -> crate::Result<CompiledCondition> {
    if condition.field.is_empty() {
        return Err(crate::Error::Validation(
            "condition has an empty field".into(),
        ));
    }
    let string_value = || {
        condition
            .value
            .as_ref()
            .map(json_value_string)
            .ok_or_else(|| crate::Error::Validation("condition requires a value".into()))
    };
    let number_value = |value: &serde_json::Value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .ok_or_else(|| crate::Error::Validation(format!("'{value}' is not a number")))
    };
    let field = condition.field.clone();
    match condition.op.as_str() {
        "=" | "==" | "eq" => Ok(CompiledCondition::Eq {
            field,
            value: string_value()?,
        }),
        "!=" | "ne" => Ok(CompiledCondition::Neq {
            field,
            value: string_value()?,
        }),
        ">" | "gt" => {
            Ok(CompiledCondition::Gt {
                field,
                value: number_value(condition.value.as_ref().ok_or_else(|| {
                    crate::Error::Validation("condition requires a value".into())
                })?)?,
            })
        }
        ">=" | "gte" => {
            Ok(CompiledCondition::Gte {
                field,
                value: number_value(condition.value.as_ref().ok_or_else(|| {
                    crate::Error::Validation("condition requires a value".into())
                })?)?,
            })
        }
        "<" | "lt" => {
            Ok(CompiledCondition::Lt {
                field,
                value: number_value(condition.value.as_ref().ok_or_else(|| {
                    crate::Error::Validation("condition requires a value".into())
                })?)?,
            })
        }
        "<=" | "lte" => {
            Ok(CompiledCondition::Lte {
                field,
                value: number_value(condition.value.as_ref().ok_or_else(|| {
                    crate::Error::Validation("condition requires a value".into())
                })?)?,
            })
        }
        "contains" => Ok(CompiledCondition::Contains {
            field,
            value: string_value()?,
        }),
        "=~" | "matches" | "regex" => {
            let pattern = string_value()?;
            let re = Regex::new(&pattern)
                .map_err(|e| crate::Error::Validation(format!("invalid regex: {e}")))?;
            Ok(CompiledCondition::Matches { field, re })
        }
        "exists" => Ok(CompiledCondition::Exists { field }),
        "in" => {
            let value = condition
                .value
                .as_ref()
                .ok_or_else(|| crate::Error::Validation("in requires a value list".into()))?;
            let values = match value {
                serde_json::Value::Array(values) => values.iter().map(json_value_string).collect(),
                other => json_value_string(other)
                    .split(',')
                    .map(|value| value.trim().to_string())
                    .collect(),
            };
            Ok(CompiledCondition::In { field, values })
        }
        "between" => {
            let value = condition
                .value
                .as_ref()
                .ok_or_else(|| crate::Error::Validation("between requires [low, high]".into()))?;
            let values = value
                .as_array()
                .filter(|values| values.len() == 2)
                .ok_or_else(|| crate::Error::Validation("between requires [low, high]".into()))?;
            Ok(CompiledCondition::Between {
                field,
                low: number_value(&values[0])?,
                high: number_value(&values[1])?,
            })
        }
        other => Err(crate::Error::Validation(format!(
            "unknown condition operator '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::rule::schema::RuleSet;
    use parqtel_core::{LabelSet, LogRecord};

    fn nginx_log(body: &str) -> LogRecord {
        LogRecord::new(
            1_000_000_000,
            1_000_000_001,
            9,
            "INFO".into(),
            body.into(),
            LabelSet::default(),
            LabelSet::try_from_iter(vec![("service.name", "nginx")]).unwrap(),
            [0; 16],
            [0; 8],
            0,
            "nginx".into(),
            "1.0".into(),
        )
    }

    #[test]
    fn example_pipeline_compiles_and_masks_and_drops() {
        let yaml = include_str!("../../../../rules/pipelines/nginx-access-logs.yaml");
        let set: RuleSet = serde_yaml_ng::from_str(yaml).unwrap();
        let pipeline_set = PipelineSet::from_definitions(&set.pipelines).unwrap();
        assert_eq!(pipeline_set.len(), 1);

        let body = r#"1.2.3.4 - - [10/Oct/2024:13:55:36 +0000] "GET /api/orders HTTP/1.1" 200 512 "-" "curl" 12.5"#;
        let (surviving, metrics) = pipeline_set.execute(vec![nginx_log(body)]);
        assert_eq!(surviving.len(), 1, "health check path is not this row");
        assert!(
            !surviving[0].body.contains("1.2.3.4"),
            "masker must rewrite the stored body: {}",
            surviving[0].body
        );
        assert!(surviving[0].body.contains("[IP_MASKED]"));
        assert!(
            metrics.iter().any(|m| m.name == "nginx_requests_total"),
            "counter extracted"
        );
        assert!(
            metrics
                .iter()
                .any(|m| m.name == "nginx_request_duration_ms"),
            "latency extracted; got {:?}",
            metrics.iter().map(|m| &m.name).collect::<Vec<_>>()
        );

        let health = r#"1.2.3.4 - - [10/Oct/2024:13:55:36 +0000] "GET /health HTTP/1.1" 200 1 "-" "curl" 1.0"#;
        let (surviving, _) = pipeline_set.execute(vec![nginx_log(health)]);
        assert!(surviving.is_empty(), "health check is dropped");
    }

    #[test]
    fn invalid_stage_names_the_pipeline_and_stage() {
        let definition = PipelineDefinition {
            name: "broken".into(),
            description: None,
            match_config: None,
            stages: vec![PipelineStage {
                stage_type: StageType::Processor,
                name: "parse".into(),
                processor: Some("regex_extract".into()),
                source_field: Some("body".into()),
                pattern: Some("(".into()),
                target_fields: None,
                on_parse_failure: None,
                fields: None,
                conditions: None,
                metric_name: None,
                metric_type: None,
                value_field: None,
                dimensions: None,
                condition: None,
                histogram_buckets: None,
                rules: None,
                action: None,
                destination: None,
            }],
        };
        let err = match compile_pipeline(&definition) {
            Ok(_) => panic!("invalid pipeline must not compile"),
            Err(error) => error.to_string(),
        };
        assert!(err.contains("broken"));
        assert!(err.contains("parse"));
        assert!(err.contains("invalid regex"));
    }
}
