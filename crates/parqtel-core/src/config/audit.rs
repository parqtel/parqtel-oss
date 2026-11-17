//! Append-only audit trail for configuration mutations.

use serde::{Deserialize, Serialize};

/// Audit configuration (`[audit]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Write one JSON line per config mutation.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// File name relative to `storage.data_dir`, or an absolute path.
    #[serde(default = "default_path")]
    pub path: String,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: default_path(),
        }
    }
}

fn default_path() -> String {
    "audit.jsonl".to_string()
}

fn default_true() -> bool {
    true
}
