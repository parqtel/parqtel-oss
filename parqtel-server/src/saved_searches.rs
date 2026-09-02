//! Server-side saved searches: named, per-signal query persistence in the
//! data directory (mirrors the silences pattern: JSON sidecar, atomic
//! tmp+rename). Supersedes UI-only localStorage views so saved searches
//! survive browsers and are shareable across users.

use parqtel_core::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SavedSearch {
    /// Unique id (ULID at creation).
    pub id: String,
    /// Human name shown in lists.
    pub name: String,
    /// Signal the search targets: metrics | logs | traces.
    pub signal: String,
    /// The query text (ParQL for metrics, ParqtelQL for logs/traces).
    pub query: String,
    /// Optional preferred time-range preset in minutes (0 = keep current).
    #[serde(default)]
    pub range_minutes: u64,
    #[serde(default)]
    pub created_at: i64,
}

#[derive(Debug)]
pub struct SavedSearchStore {
    path: PathBuf,
    searches: RwLock<Vec<SavedSearch>>,
}

const FILE: &str = "saved_searches.json";

impl SavedSearchStore {
    pub fn new(data_dir: PathBuf) -> Self {
        let path = data_dir.join(FILE);
        let searches = Self::load(&path);
        Self {
            path,
            searches: RwLock::new(searches),
        }
    }

    fn load(path: &PathBuf) -> Vec<SavedSearch> {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|c| serde_json::from_str(&c).ok())
            .unwrap_or_default()
    }

    pub async fn list(&self) -> Vec<SavedSearch> {
        self.searches.read().await.clone()
    }

    pub async fn create(&self, mut s: SavedSearch) -> Result<SavedSearch> {
        if s.name.trim().is_empty() {
            return Err(parqtel_core::Error::Validation(
                "saved search name is required".into(),
            ));
        }
        if !matches!(s.signal.as_str(), "metrics" | "logs" | "traces") {
            return Err(parqtel_core::Error::Validation(
                "signal must be metrics, logs, or traces".into(),
            ));
        }
        if s.query.trim().is_empty() {
            return Err(parqtel_core::Error::Validation("query is required".into()));
        }
        s.id = ulid::Ulid::new().to_string();
        s.created_at = chrono::Utc::now().timestamp();
        let mut guard = self.searches.write().await;
        guard.push(s.clone());
        self.persist(&guard);
        Ok(s)
    }

    pub async fn delete(&self, id: &str) -> bool {
        let mut guard = self.searches.write().await;
        let before = guard.len();
        guard.retain(|s| s.id != id);
        let removed = guard.len() != before;
        if removed {
            self.persist(&guard);
        }
        removed
    }

    fn persist(&self, searches: &[SavedSearch]) {
        if let Ok(json) = serde_json::to_string_pretty(searches) {
            let tmp = self.path.with_extension("json.tmp");
            if std::fs::write(&tmp, &json).is_ok() {
                let _ = std::fs::rename(tmp, &self.path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn sample(name: &str, signal: &str) -> SavedSearch {
        SavedSearch {
            id: String::new(),
            name: name.into(),
            signal: signal.into(),
            query: "service=api error".into(),
            range_minutes: 60,
            created_at: 0,
        }
    }

    #[tokio::test]
    async fn create_list_delete_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = SavedSearchStore::new(dir.path().to_path_buf());
        let s = store.create(sample("api errors", "logs")).await.unwrap();
        assert!(!s.id.is_empty());

        // New store instance (restart) sees the persisted file.
        let store2 = SavedSearchStore::new(dir.path().to_path_buf());
        let all = store2.list().await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "api errors");

        assert!(store2.delete(&s.id).await);
        assert!(store2.list().await.is_empty());
        // Deletion persisted.
        let store3 = SavedSearchStore::new(dir.path().to_path_buf());
        assert!(store3.list().await.is_empty());
    }

    #[tokio::test]
    async fn validation_rejects_bad_input() {
        let dir = tempfile::tempdir().unwrap();
        let store = SavedSearchStore::new(dir.path().to_path_buf());
        assert!(store
            .create(SavedSearch {
                id: String::new(),
                name: "  ".into(),
                signal: "logs".into(),
                query: "x".into(),
                range_minutes: 0,
                created_at: 0,
            })
            .await
            .is_err());
        assert!(store.create(sample("bad signal", "events")).await.is_err());
        let mut empty_q = sample("empty q", "logs");
        empty_q.query = "  ".into();
        assert!(store.create(empty_q).await.is_err());
    }
}
