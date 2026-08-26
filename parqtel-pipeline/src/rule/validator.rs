use super::schema::{PipelineDefinition, RecordingRuleGroup};

/// Validates rule definitions before activation.
pub struct RuleValidator;

impl RuleValidator {
    pub fn validate_group(&self, group: &RecordingRuleGroup) -> crate::Result<()> {
        if group.name.is_empty() {
            return Err(crate::Error::Validation(
                "Group name cannot be empty".into(),
            ));
        }
        parse_duration(&group.interval)?;
        for rule in &group.rules {
            if rule.record.is_empty() {
                return Err(crate::Error::Validation(
                    "Recording rule must have a 'record' name".into(),
                ));
            }
            if rule.expr.is_empty() {
                return Err(crate::Error::Validation(format!(
                    "Rule '{}' has empty expression",
                    rule.record
                )));
            }
        }
        Ok(())
    }

    pub fn validate_pipeline(&self, pipeline: &PipelineDefinition) -> crate::Result<()> {
        if pipeline.name.is_empty() {
            return Err(crate::Error::Validation(
                "Pipeline name cannot be empty".into(),
            ));
        }
        if pipeline.stages.is_empty() {
            return Err(crate::Error::Validation(format!(
                "Pipeline '{}' has no stages",
                pipeline.name
            )));
        }
        Ok(())
    }
}

/// Parse a duration string like "1m", "30s", "1h", "1d" into seconds.
pub fn parse_duration(s: &str) -> crate::Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::Error::Validation("Empty duration".into()));
    }
    let (num_str, suffix) = s.split_at(s.len() - 1);
    let num: u64 = num_str
        .parse()
        .map_err(|_| crate::Error::Validation(format!("Invalid duration: {}", s)))?;
    match suffix {
        "s" => Ok(num),
        "m" => Ok(num * 60),
        "h" => Ok(num * 3600),
        "d" => Ok(num * 86400),
        _ => Err(crate::Error::Validation(format!(
            "Unknown duration suffix: {}",
            suffix
        ))),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::rule::schema::{
        PipelineDefinition, PipelineStage, RecordingRule, RecordingRuleGroup, StageType,
    };
    use std::collections::BTreeMap;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), 300);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), 3600);
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("1d").unwrap(), 86400);
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn test_validate_group_empty_name() {
        let v = RuleValidator;
        let group = RecordingRuleGroup {
            name: "".into(),
            interval: "1m".into(),
            rules: vec![],
        };
        assert!(v.validate_group(&group).is_err());
    }

    #[test]
    fn test_validate_group_valid() {
        let v = RuleValidator;
        let group = RecordingRuleGroup {
            name: "test".into(),
            interval: "1m".into(),
            rules: vec![RecordingRule {
                record: "metric:name".into(),
                expr: "rate(x[5m])".into(),
                labels: BTreeMap::new(),
                description: None,
                retention_override_days: None,
                for_duration: None,
            }],
        };
        assert!(v.validate_group(&group).is_ok());
    }

    #[test]
    fn test_validate_pipeline_empty_stages() {
        let v = RuleValidator;
        let p = PipelineDefinition {
            name: "test".into(),
            description: None,
            match_config: None,
            stages: vec![],
        };
        assert!(v.validate_pipeline(&p).is_err());
    }

    #[test]
    fn test_validate_pipeline_valid() {
        let v = RuleValidator;
        let p = PipelineDefinition {
            name: "test".into(),
            description: None,
            match_config: None,
            stages: vec![PipelineStage {
                stage_type: StageType::Processor,
                name: "s1".into(),
                processor: None,
                source_field: None,
                pattern: None,
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
        assert!(v.validate_pipeline(&p).is_ok());
    }
}
