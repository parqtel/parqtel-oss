use serde::{Deserialize, Serialize};

/// Configuration for the recording rule evaluator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulerConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_concurrency")]
    pub evaluation_concurrency: usize,
    #[serde(default = "default_max_backfill")]
    pub max_backfill_intervals: u64,
    #[serde(default = "default_state_file")]
    pub state_file: String,
    #[serde(default = "default_rules_dir")]
    pub rules_dir: String,
}

/// Configuration for the stream pipeline processor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_pipeline_rules_dir")]
    pub rules_dir: String,
    #[serde(default = "default_flush_interval")]
    pub metric_flush_interval_secs: u64,
    #[serde(default = "default_stage_timeout")]
    pub stage_timeout_ms: u64,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_executions: usize,
}

fn default_true() -> bool {
    true
}
fn default_concurrency() -> usize {
    4
}
fn default_max_backfill() -> u64 {
    10
}
fn default_state_file() -> String {
    "./data/ruler/state.json".to_string()
}
fn default_rules_dir() -> String {
    "./rules/recording".to_string()
}
fn default_pipeline_rules_dir() -> String {
    "./rules/pipelines".to_string()
}
fn default_flush_interval() -> u64 {
    60
}
fn default_stage_timeout() -> u64 {
    100
}
fn default_max_concurrent() -> usize {
    8
}

impl Default for RulerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_concurrency: 4,
            max_backfill_intervals: 10,
            state_file: default_state_file(),
            rules_dir: default_rules_dir(),
        }
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rules_dir: default_pipeline_rules_dir(),
            metric_flush_interval_secs: 60,
            stage_timeout_ms: 100,
            max_concurrent_executions: 8,
        }
    }
}
