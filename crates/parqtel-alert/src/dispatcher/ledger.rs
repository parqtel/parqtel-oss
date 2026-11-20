//! Local delivery ledger used to avoid re-sending a notification after a
//! process restart. It is deliberately receiver-local; HA fan-out still needs
//! a shared receiver or a leader lease.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_ENTRIES: usize = 10_000;

#[derive(Debug, Default, Serialize, Deserialize)]
struct LedgerFile {
    #[serde(default)]
    entries: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub struct DeliveryLedger {
    path: Option<PathBuf>,
    entries: Mutex<BTreeMap<String, u64>>,
}

impl DeliveryLedger {
    pub fn new(path: Option<PathBuf>) -> Self {
        let entries = path
            .as_ref()
            .and_then(|path| std::fs::read(path).ok())
            .and_then(|bytes| serde_json::from_slice::<LedgerFile>(&bytes).ok())
            .map(|file| file.entries)
            .unwrap_or_default();
        Self {
            path,
            entries: Mutex::new(entries),
        }
    }

    pub fn in_memory() -> Self {
        Self::new(None)
    }

    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    fn key(
        receiver: &str,
        group_key: &str,
        generation: u64,
        reason: &str,
        notifier: &str,
    ) -> String {
        format!("{receiver}\u{1f}{group_key}\u{1f}{generation}\u{1f}{reason}\u{1f}{notifier}")
    }

    /// `min_interval` is the route's group or repeat interval. Resolved
    /// notifications are one-shot per generation.
    pub fn should_send(
        &self,
        receiver: &str,
        group_key: &str,
        generation: u64,
        reason: &str,
        notifier: &str,
        min_interval_secs: u64,
    ) -> bool {
        let key = Self::key(receiver, group_key, generation, reason, notifier);
        let now = now_secs();
        let Ok(entries) = self.entries.lock() else {
            return true;
        };
        let Some(last) = entries.get(&key) else {
            return true;
        };
        if reason == "resolved" {
            return false;
        }
        now.saturating_sub(*last) >= min_interval_secs
    }

    pub fn mark_sent(
        &self,
        receiver: &str,
        group_key: &str,
        generation: u64,
        reason: &str,
        notifier: &str,
    ) {
        let key = Self::key(receiver, group_key, generation, reason, notifier);
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        entries.insert(key, now_secs());
        if entries.len() > MAX_ENTRIES {
            let oldest = entries
                .iter()
                .min_by_key(|(_, timestamp)| **timestamp)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        drop(entries);
        self.persist();
    }

    fn persist(&self) {
        let Some(path) = self.path.as_ref() else {
            return;
        };
        let entries = match self.entries.lock() {
            Ok(entries) => entries.clone(),
            Err(_) => return,
        };
        let body = LedgerFile { entries };
        let Ok(json) = serde_json::to_vec_pretty(&body) else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = tmp_path(path);
        if std::fs::write(&tmp, json).is_ok() {
            let _ = std::fs::rename(tmp, path);
        }
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn suppresses_within_interval_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ledger.json");
        let ledger = DeliveryLedger::new(Some(path.clone()));
        assert!(ledger.should_send("ops", "g1", 1, "initial", "slack", 60));
        ledger.mark_sent("ops", "g1", 1, "initial", "slack");
        assert!(!ledger.should_send("ops", "g1", 1, "initial", "slack", 60));
        let reloaded = DeliveryLedger::new(Some(path));
        assert!(!reloaded.should_send("ops", "g1", 1, "initial", "slack", 60));
        assert!(reloaded.should_send("ops", "g1", 2, "initial", "slack", 60));
    }

    #[test]
    fn resolved_is_one_shot_per_generation() {
        let ledger = DeliveryLedger::in_memory();
        ledger.mark_sent("ops", "g1", 1, "resolved", "pd");
        assert!(!ledger.should_send("ops", "g1", 1, "resolved", "pd", 0));
        assert!(ledger.should_send("ops", "g1", 2, "resolved", "pd", 0));
    }
}
