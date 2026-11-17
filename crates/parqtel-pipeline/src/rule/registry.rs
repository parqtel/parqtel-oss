use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tracing::{error, info, warn};

use super::schema::{PipelineDefinition, RecordingRuleGroup, RuleSet};
use super::validator::RuleValidator;

/// Thread-safe registry of recording rules and pipeline definitions.
#[derive(Clone)]
pub struct RuleRegistry {
    groups: Arc<RwLock<BTreeMap<String, RecordingRuleGroup>>>,
    pipelines: Arc<RwLock<BTreeMap<String, PipelineDefinition>>>,
    /// Where API-created recording-rule groups are written.
    recording_dir: Option<PathBuf>,
    /// Where API-created pipelines are written.
    pipeline_dir: Option<PathBuf>,
}

impl RuleRegistry {
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(BTreeMap::new())),
            pipelines: Arc::new(RwLock::new(BTreeMap::new())),
            recording_dir: None,
            pipeline_dir: None,
        }
    }

    /// Persist API-created definitions into the directories the server loads
    /// at startup. Without this, a restart silently discards every definition
    /// created through the REST API.
    pub fn with_dirs(mut self, recording_dir: PathBuf, pipeline_dir: PathBuf) -> Self {
        self.recording_dir = Some(recording_dir);
        self.pipeline_dir = Some(pipeline_dir);
        self
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
            serde_yaml_ng::from_str(&content).map_err(|e| crate::Error::Parse(e.to_string()))?;

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
        tracing::debug!(group = %name, "recording rule group removed");
        if let Ok(mut groups) = self.groups.write() {
            groups.remove(name);
        }
        remove_definition(&self.recording_dir, name);
    }

    /// Add a recording rule group.
    pub fn add_group(&self, group: RecordingRuleGroup) {
        tracing::debug!(group = %group.name, rules = %group.rules.len(), "recording rule group added");
        let set = RuleSet {
            groups: vec![group.clone()],
            pipelines: Vec::new(),
        };
        persist_definition(&self.recording_dir, &group.name, &set);
        if let Ok(mut groups) = self.groups.write() {
            groups.insert(group.name.clone(), group);
        }
    }

    /// Add a pipeline definition.
    pub fn add_pipeline(&self, pipeline: PipelineDefinition) {
        tracing::debug!(name = %pipeline.name, "pipeline added");
        let set = RuleSet {
            groups: Vec::new(),
            pipelines: vec![pipeline.clone()],
        };
        persist_definition(&self.pipeline_dir, &pipeline.name, &set);
        if let Ok(mut pipelines) = self.pipelines.write() {
            pipelines.insert(pipeline.name.clone(), pipeline);
        }
    }

    /// Remove a pipeline by name.
    pub fn remove_pipeline(&self, name: &str) {
        tracing::debug!(name = %name, "pipeline removed");
        if let Ok(mut pipelines) = self.pipelines.write() {
            pipelines.remove(name);
        }
        remove_definition(&self.pipeline_dir, name);
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

/// Filename-safe form of a rule or pipeline name. Names are user-controlled,
/// so a slash must not escape the configured directory.
fn definition_stem(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn definition_path(dir: &Path, name: &str, extension: &str) -> PathBuf {
    dir.join(format!("{}.{}", definition_stem(name), extension))
}

fn persist_definition(dir: &Option<PathBuf>, name: &str, set: &RuleSet) {
    let Some(dir) = dir else {
        return;
    };
    if let Err(e) = std::fs::create_dir_all(dir) {
        warn!(path = %dir.display(), error = %e, "failed to create rule directory");
        return;
    }
    let yaml = match serde_yaml_ng::to_string(set) {
        Ok(yaml) => yaml,
        Err(e) => {
            warn!(name, error = %e, "failed to serialize rule definition");
            return;
        }
    };
    let path = definition_path(dir, name, "yaml");
    let tmp = dir.join(format!(".{}.tmp", definition_stem(name)));
    if let Err(e) = std::fs::write(&tmp, yaml) {
        warn!(path = %tmp.display(), error = %e, "failed to write rule file");
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        warn!(path = %path.display(), error = %e, "failed to replace rule file");
    }
}

fn remove_definition(dir: &Option<PathBuf>, name: &str) {
    let Some(dir) = dir else {
        return;
    };
    for extension in ["yaml", "yml"] {
        let path = definition_path(dir, name, extension);
        if path.exists() {
            if let Err(e) = std::fs::remove_file(&path) {
                warn!(path = %path.display(), error = %e, "failed to remove rule file");
            }
        }
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
    fn test_api_definitions_are_written_and_reloaded() {
        let root = tempdir().unwrap();
        let recording_dir = root.path().join("recording");
        let pipeline_dir = root.path().join("pipelines");
        let registry = RuleRegistry::new().with_dirs(recording_dir.clone(), pipeline_dir.clone());

        registry.add_group(RecordingRuleGroup {
            name: "api/group".into(),
            interval: "1m".into(),
            rules: vec![super::super::schema::RecordingRule {
                record: "generated_metric".into(),
                expr: "sum(requests)".into(),
                labels: BTreeMap::new(),
                description: None,
                retention_override_days: None,
                for_duration: None,
            }],
        });
        registry.add_pipeline(PipelineDefinition {
            name: "api pipeline".into(),
            description: None,
            match_config: None,
            stages: vec![super::super::schema::PipelineStage {
                stage_type: super::super::schema::StageType::Preprocessor,
                name: "pre".into(),
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
        });

        assert!(recording_dir.join("api_group.yaml").exists());
        assert!(pipeline_dir.join("api_pipeline.yaml").exists());

        let reloaded = RuleRegistry::new().with_dirs(recording_dir.clone(), pipeline_dir.clone());
        reloaded.load_dir(&recording_dir).unwrap();
        reloaded.load_dir(&pipeline_dir).unwrap();
        assert_eq!(reloaded.get_groups().len(), 1);
        assert_eq!(reloaded.get_pipelines().len(), 1);

        reloaded.remove_group("api/group");
        reloaded.remove_pipeline("api pipeline");
        assert!(!recording_dir.join("api_group.yaml").exists());
        assert!(!pipeline_dir.join("api_pipeline.yaml").exists());
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
