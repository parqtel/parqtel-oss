use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tracing::{error, info};

use super::schema::{PipelineDefinition, RecordingRuleGroup, RuleSet};
use super::validator::RuleValidator;

/// Thread-safe registry of recording rules and pipeline definitions.
#[derive(Clone)]
pub struct RuleRegistry {
    groups: Arc<RwLock<BTreeMap<String, RecordingRuleGroup>>>,
    pipelines: Arc<RwLock<BTreeMap<String, PipelineDefinition>>>,
}

impl RuleRegistry {
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(BTreeMap::new())),
            pipelines: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Load all YAML files from a directory.
    pub fn load_dir(&self, dir: &Path) -> crate::Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        let entries = std::fs::read_dir(dir).map_err(|e| crate::Error::Io(e.to_string()))?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("yaml")
                || path.extension().and_then(|e| e.to_str()) == Some("yml")
            {
                if let Err(e) = self.load_file(&path) {
                    error!("Failed to load rule file {:?}: {}", path, e);
                }
            }
        }
        Ok(())
    }

    /// Load a single YAML file.
    pub fn load_file(&self, path: &Path) -> crate::Result<()> {
        let content = std::fs::read_to_string(path).map_err(|e| crate::Error::Io(e.to_string()))?;
        let rule_set: RuleSet =
            serde_yaml::from_str(&content).map_err(|e| crate::Error::Parse(e.to_string()))?;

        let validator = RuleValidator;

        for group in rule_set.groups {
            validator.validate_group(&group)?;
            if let Ok(mut groups) = self.groups.write() {
                groups.insert(group.name.clone(), group);
            }
        }
        for pipeline in rule_set.pipelines {
            validator.validate_pipeline(&pipeline)?;
            if let Ok(mut pipelines) = self.pipelines.write() {
                pipelines.insert(pipeline.name.clone(), pipeline);
            }
        }
        Ok(())
    }

    /// Remove a recording rule group by name.
    pub fn remove_group(&self, name: &str) {
        if let Ok(mut groups) = self.groups.write() {
            groups.remove(name);
        }
    }

    /// Add a recording rule group.
    pub fn add_group(&self, group: RecordingRuleGroup) {
        if let Ok(mut groups) = self.groups.write() {
            groups.insert(group.name.clone(), group);
        }
    }

    /// Add a pipeline definition.
    pub fn add_pipeline(&self, pipeline: PipelineDefinition) {
        if let Ok(mut pipelines) = self.pipelines.write() {
            pipelines.insert(pipeline.name.clone(), pipeline);
        }
    }

    /// Remove a pipeline by name.
    pub fn remove_pipeline(&self, name: &str) {
        if let Ok(mut pipelines) = self.pipelines.write() {
            pipelines.remove(name);
        }
    }

    pub fn get_groups(&self) -> Vec<RecordingRuleGroup> {
        self.groups
            .read()
            .map(|g| g.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_group(&self, name: &str) -> Option<RecordingRuleGroup> {
        self.groups.read().ok().and_then(|g| g.get(name).cloned())
    }

    pub fn get_pipelines(&self) -> Vec<PipelineDefinition> {
        self.pipelines
            .read()
            .map(|p| p.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_pipeline(&self, name: &str) -> Option<PipelineDefinition> {
        self.pipelines
            .read()
            .ok()
            .and_then(|p| p.get(name).cloned())
    }

    /// Start watching a directory for changes. Returns the watcher handle.
    pub fn watch(&self, dir: PathBuf) -> crate::Result<RecommendedWatcher> {
        let registry = self.clone();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                match event.kind {
                    EventKind::Create(_) | EventKind::Modify(_) => {
                        for path in &event.paths {
                            if path.extension().and_then(|e| e.to_str()) == Some("yaml")
                                || path.extension().and_then(|e| e.to_str()) == Some("yml")
                            {
                                info!("Reloading rule file: {:?}", path);
                                if let Err(e) = registry.load_file(path) {
                                    error!("Failed to reload {:?}: {}", path, e);
                                }
                            }
                        }
                    }
                    EventKind::Remove(_) => {
                        info!("Rule file removed: {:?}", event.paths);
                    }
                    _ => {}
                }
            }
        })
        .map_err(|e| crate::Error::Io(e.to_string()))?;

        watcher
            .watch(&dir, RecursiveMode::Recursive)
            .map_err(|e| crate::Error::Io(e.to_string()))?;
        Ok(watcher)
    }
}

impl Default for RuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_load_recording_rule_yaml() {
        let dir = tempdir().unwrap();
        let yaml = r#"
groups:
  - name: http_aggregations
    interval: 1m
    rules:
      - record: service:http_error_rate:rate5m
        expr: "rate(http_errors[5m])"
        labels:
          generated_by: ruler
"#;
        let path = dir.path().join("rules.yaml");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();

        let registry = RuleRegistry::new();
        registry.load_dir(dir.path()).unwrap();

        let groups = registry.get_groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "http_aggregations");
        assert_eq!(groups[0].rules.len(), 1);
        assert_eq!(groups[0].rules[0].record, "service:http_error_rate:rate5m");
    }

    #[test]
    fn test_load_pipeline_yaml() {
        let dir = tempdir().unwrap();
        let yaml = r#"
pipelines:
  - name: api-logs
    stages:
      - type: processor
        name: parse
"#;
        let path = dir.path().join("pipeline.yaml");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();

        let registry = RuleRegistry::new();
        registry.load_dir(dir.path()).unwrap();

        let pipelines = registry.get_pipelines();
        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].name, "api-logs");
    }

    #[test]
    fn test_remove_group() {
        let registry = RuleRegistry::new();
        let dir = tempdir().unwrap();
        let yaml = r#"
groups:
  - name: to_remove
    interval: 1m
    rules:
      - record: x
        expr: "y"
"#;
        let path = dir.path().join("r.yaml");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();
        registry.load_dir(dir.path()).unwrap();

        assert_eq!(registry.get_groups().len(), 1);
        registry.remove_group("to_remove");
        assert_eq!(registry.get_groups().len(), 0);
    }
}
