use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level recording rule file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleSet {
    #[serde(default)]
    pub groups: Vec<RecordingRuleGroup>,
    #[serde(default)]
    pub pipelines: Vec<PipelineDefinition>,
}

/// A group of recording rules evaluated together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingRuleGroup {
    pub name: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    pub rules: Vec<RecordingRule>,
}

fn default_interval() -> String {
    "1m".to_string()
}

/// A single recording rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingRule {
    pub record: String,
    pub expr: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub retention_override_days: Option<u64>,
    #[serde(rename = "for", default)]
    pub for_duration: Option<String>,
}

/// A pipeline definition for ingest-time processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDefinition {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "match", default)]
    pub match_config: Option<PipelineMatch>,
    pub stages: Vec<PipelineStage>,
}

/// Match criteria for a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMatch {
    pub signal: String,
    #[serde(default)]
    pub conditions: Vec<StageCondition>,
}

/// A single pipeline stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStage {
    #[serde(rename = "type")]
    pub stage_type: StageType,
    pub name: String,
    #[serde(default)]
    pub processor: Option<String>,
    #[serde(default)]
    pub source_field: Option<String>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub target_fields: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub on_parse_failure: Option<String>,
    #[serde(default)]
    pub fields: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(default)]
    pub conditions: Option<Vec<StageCondition>>,
    // Metric extraction fields
    #[serde(default)]
    pub metric_name: Option<String>,
    #[serde(default)]
    pub metric_type: Option<String>,
    #[serde(default)]
    pub value_field: Option<String>,
    #[serde(default)]
    pub dimensions: Option<Vec<String>>,
    #[serde(default)]
    pub condition: Option<StageCondition>,
    #[serde(default)]
    pub histogram_buckets: Option<Vec<f64>>,
    // Masker fields
    #[serde(default)]
    pub rules: Option<Vec<MaskRule>>,
    // Router fields
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
}

/// Stage type enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageType {
    Preprocessor,
    Processor,
    MetricExtract,
    Masker,
    Router,
}

/// A condition used in pipeline matching and stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageCondition {
    pub field: String,
    pub op: String,
    pub value: Option<serde_json::Value>,
}

/// A PII masking rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskRule {
    pub field: String,
    pub pattern: String,
    pub replacement: String,
}
