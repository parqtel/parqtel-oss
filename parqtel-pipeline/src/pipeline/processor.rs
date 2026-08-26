use std::collections::BTreeMap;

use parqtel_core::Metric;
use regex::Regex;

use crate::rule::schema::StageType;

use super::stage::{SignalRecord, Stage, StageResult};

/// Regex extraction processor: extracts named capture groups into fields.
pub struct RegexProcessor {
    pub name: String,
    pub source_field: String,
    pub regex: Regex,
    pub target_fields: BTreeMap<String, String>,
    pub on_parse_failure: ParseFailureAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseFailureAction {
    KeepOriginal,
    Drop,
}

impl Stage for RegexProcessor {
    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        StageType::Processor
    }

    fn process(&self, mut record: SignalRecord, _extracted: &mut Vec<Metric>) -> StageResult {
        let source = record
            .fields
            .get(&self.source_field)
            .cloned()
            .unwrap_or_default();
        if let Some(caps) = self.regex.captures(&source) {
            for (target_field, capture_name) in &self.target_fields {
                if let Some(m) = caps.name(capture_name) {
                    record
                        .fields
                        .insert(target_field.clone(), m.as_str().to_string());
                }
            }
        } else if self.on_parse_failure == ParseFailureAction::Drop {
            return StageResult::Drop;
        }
        StageResult::Continue(record)
    }
}

/// Add/overwrite fields conditionally.
pub struct AddFieldProcessor {
    pub name: String,
    pub fields: BTreeMap<String, String>,
    pub condition: Option<crate::expr::CompiledCondition>,
}

impl Stage for AddFieldProcessor {
    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        StageType::Processor
    }

    fn process(&self, mut record: SignalRecord, _extracted: &mut Vec<Metric>) -> StageResult {
        let should_apply = self
            .condition
            .as_ref()
            .map(|c| c.evaluate(&record.fields))
            .unwrap_or(true);
        if should_apply {
            for (k, v) in &self.fields {
                record.fields.insert(k.clone(), v.clone());
            }
        }
        StageResult::Continue(record)
    }
}

/// Cast fields to specific types (validates and normalizes).
pub struct CastProcessor {
    pub name: String,
    pub fields: BTreeMap<String, CastType>,
}

#[derive(Debug, Clone, Copy)]
pub enum CastType {
    Integer,
    Float,
}

impl Stage for CastProcessor {
    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        StageType::Processor
    }

    fn process(&self, mut record: SignalRecord, _extracted: &mut Vec<Metric>) -> StageResult {
        for (field, cast_type) in &self.fields {
            if let Some(val) = record.fields.get(field).cloned() {
                let casted = match cast_type {
                    CastType::Integer => val.parse::<i64>().ok().map(|v| v.to_string()),
                    CastType::Float => val.parse::<f64>().ok().map(|v| v.to_string()),
                };
                if let Some(v) = casted {
                    record.fields.insert(field.clone(), v);
                }
            }
        }
        StageResult::Continue(record)
    }
}
