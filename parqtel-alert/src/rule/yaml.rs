use std::path::Path;
use crate::rule::types::AlertRule;

/// Parse a single alert rule from a YAML string.
pub fn parse_rule(yaml: &str) -> Result<AlertRule, serde_yaml::Error> {
    serde_yaml::from_str(yaml)
}

/// Parse multiple rules from a YAML file (supports `---` document separators).
pub fn parse_rules_from_str(yaml: &str) -> Result<Vec<AlertRule>, serde_yaml::Error> {
    let mut rules = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(yaml) {
        let rule = AlertRule::deserialize(doc)?;
        rules.push(rule);
    }
    Ok(rules)
}

/// Load all YAML rule files from a directory.
pub fn load_rules_dir(dir: &Path) -> Result<Vec<AlertRule>, Box<dyn std::error::Error + Send + Sync>> {
    let mut rules = Vec::new();
    if !dir.exists() {
        return Ok(rules);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("yaml")
            || path.extension().and_then(|e| e.to_str()) == Some("yml")
        {
            let content = std::fs::read_to_string(&path)?;
            let file_rules = parse_rules_from_str(&content)?;
            rules.extend(file_rules);
        }
    }
    Ok(rules)
}

use serde::Deserialize;
