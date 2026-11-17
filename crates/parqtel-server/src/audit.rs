//! Append-only JSONL audit trail for config mutations.
//!
//! One line per mutation: timestamp, actor (token name), action, target, and
//! optional before/after values. Callers pass redacted values; webhook URLs and
//! tokens must never reach this file.

use parqtel_core::config::AuditConfig;
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct AuditLog {
    path: Option<PathBuf>,
}

impl AuditLog {
    pub fn new(config: &AuditConfig, data_dir: &Path) -> Self {
        let path = if config.enabled {
            let configured = Path::new(&config.path);
            Some(if configured.is_absolute() {
                configured.to_path_buf()
            } else {
                data_dir.join(configured)
            })
        } else {
            None
        };
        Self { path }
    }

    pub fn record(
        &self,
        actor: &str,
        action: &str,
        target: &str,
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
    ) {
        let Some(path) = &self.path else {
            return;
        };
        let line = serde_json::json!({
            "ts": chrono::Utc::now().to_rfc3339(),
            "actor": actor,
            "action": action,
            "target": target,
            "before": before,
            "after": after,
        });
        let Ok(mut encoded) = serde_json::to_vec(&line) else {
            tracing::warn!("failed to serialize audit record");
            return;
        };
        encoded.push(b'\n');
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::warn!(path = %parent.display(), error = %e, "failed to create audit directory");
                return;
            }
        }
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(mut file) => {
                if let Err(e) = file.write_all(&encoded) {
                    tracing::warn!(path = %path.display(), error = %e, "failed to append audit record");
                }
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "failed to open audit log");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn appends_one_json_line_per_record() {
        let dir = tempfile::tempdir().unwrap();
        let audit = AuditLog::new(&AuditConfig::default(), dir.path());
        audit.record(
            "operator",
            "alerts.rule.create",
            "rule-1",
            None,
            Some(serde_json::json!({"id": "rule-1"})),
        );
        let content = std::fs::read_to_string(dir.path().join("audit.jsonl")).unwrap();
        let line: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(line["actor"], "operator");
        assert_eq!(line["action"], "alerts.rule.create");
        assert_eq!(line["target"], "rule-1");
        assert_eq!(line["after"]["id"], "rule-1");
    }

    #[test]
    fn disabled_audit_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let audit = AuditLog::new(
            &AuditConfig {
                enabled: false,
                path: "audit.jsonl".into(),
            },
            dir.path(),
        );
        audit.record("operator", "x", "y", None, None);
        assert!(!dir.path().join("audit.jsonl").exists());
    }
}
