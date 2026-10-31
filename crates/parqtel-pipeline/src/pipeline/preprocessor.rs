use parqtel_core::Metric;

use crate::rule::schema::StageType;

use super::stage::{SignalRecord, Stage, StageResult};

/// Auto-extracts timestamp, severity, and resource fields into the fields map.
pub struct Preprocessor {
    pub name: String,
}

impl Stage for Preprocessor {
    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        StageType::Preprocessor
    }

    fn process(&self, mut record: SignalRecord, _extracted: &mut Vec<Metric>) -> StageResult {
        // Ensure standard fields are populated
        record
            .fields
            .entry("timestamp_ns".to_string())
            .or_insert_with(|| record.log.timestamp_ns.to_string());
        record
            .fields
            .entry("severity_number".to_string())
            .or_insert_with(|| record.log.severity_number.to_string());
        record
            .fields
            .entry("severity_text".to_string())
            .or_insert_with(|| record.log.severity_text.clone());
        StageResult::Continue(record)
    }
}
