//! PII masking stage.
//!
//! Masking mutates the log body (and any matching attribute) as well as the
//! pipeline field map, so the redacted value is what reaches storage. Changing
//! only `fields` would let the original text through to the writer.

use regex::Regex;

use crate::rule::schema::StageType;

use super::stage::{SignalRecord, Stage, StageResult};

/// One field/pattern/replacement rule.
#[derive(Debug, Clone)]
pub struct MaskRule {
    pub field: String,
    pub regex: Regex,
    pub replacement: String,
}

/// Applies ordered regex replacements to selected fields.
#[derive(Debug, Clone)]
pub struct Masker {
    pub name: String,
    pub rules: Vec<MaskRule>,
}

impl Masker {
    fn mask_field(record: &mut SignalRecord, field: &str, regex: &Regex, replacement: &str) {
        let Some(current) = record.fields.get(field).cloned() else {
            return;
        };
        let masked = regex.replace_all(&current, replacement).into_owned();
        if masked == current {
            return;
        }
        record.fields.insert(field.to_string(), masked.clone());
        if field == "body" {
            record.log.body = masked;
        } else if record.log.attributes.get(field).is_some() {
            let pairs = record
                .log
                .attributes
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        if k == field {
                            masked.clone()
                        } else {
                            v.to_string()
                        },
                    )
                })
                .collect::<Vec<_>>();
            if let Ok(labels) = parqtel_core::LabelSet::try_from_iter(pairs) {
                record.log.attributes = labels;
            }
        } else if record.log.resource_attributes.get(field).is_some() {
            let pairs = record
                .log
                .resource_attributes
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        if k == field {
                            masked.clone()
                        } else {
                            v.to_string()
                        },
                    )
                })
                .collect::<Vec<_>>();
            if let Ok(labels) = parqtel_core::LabelSet::try_from_iter(pairs) {
                record.log.resource_attributes = labels;
            }
        }
    }
}

impl Stage for Masker {
    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        StageType::Masker
    }

    fn process(
        &self,
        mut record: SignalRecord,
        _extracted: &mut Vec<parqtel_core::Metric>,
    ) -> StageResult {
        for rule in &self.rules {
            Self::mask_field(&mut record, &rule.field, &rule.regex, &rule.replacement);
        }
        StageResult::Continue(record)
    }
}
