//! Server-side dashboards: named panels over stored queries, persisted in the
//! data directory (JSON sidecar, atomic tmp+rename) so a multi-panel layout
//! survives restarts and is shareable across users.
//!
//! Panels hold query text, never HTML: the render endpoint returns query
//! results, and the caller decides how to draw them.

use parqtel_core::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::RwLock;

/// One query panel on a dashboard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DashboardPanel {
    /// Stable id (ULID), used to address the panel in a render response.
    pub id: String,
    pub title: String,
    /// `metrics` or `logs`.
    pub signal: String,
    /// PromQL/ParQL for metrics, ParqtelQL selector for logs.
    pub query: String,
    /// Range-query step in seconds (metrics only).
    #[serde(default = "default_step")]
    pub step_secs: u64,
    /// Maximum records returned (logs only).
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_step() -> u64 {
    60
}

fn default_limit() -> usize {
    200
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Dashboard {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub panels: Vec<DashboardPanel>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

/// Request body for create/update. Kept separate from [`Dashboard`] so a
/// caller cannot choose the id or timestamps.
#[derive(Debug, Clone, Deserialize)]
pub struct DashboardInput {
    pub name: String,
    #[serde(default)]
    pub panels: Vec<DashboardPanelInput>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DashboardPanelInput {
    #[serde(default)]
    pub id: String,
    pub title: String,
    pub signal: String,
    pub query: String,
    #[serde(default)]
    pub step_secs: Option<u64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

pub struct DashboardStore {
    path: PathBuf,
    dashboards: RwLock<Vec<Dashboard>>,
}

const FILE: &str = "dashboards.json";
const MAX_PANELS: usize = 50;

impl DashboardStore {
    pub fn new(data_dir: PathBuf) -> Self {
        let path = data_dir.join(FILE);
        let dashboards = Self::load(&path);
        Self {
            path,
            dashboards: RwLock::new(dashboards),
        }
    }

    fn load(path: &PathBuf) -> Vec<Dashboard> {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|c| serde_json::from_str(&c).ok())
            .unwrap_or_default()
    }

    pub async fn list(&self) -> Vec<Dashboard> {
        self.dashboards.read().await.clone()
    }

    pub async fn get(&self, id: &str) -> Option<Dashboard> {
        self.dashboards
            .read()
            .await
            .iter()
            .find(|d| d.id == id)
            .cloned()
    }

    pub async fn create(&self, input: DashboardInput) -> Result<Dashboard> {
        let now = chrono::Utc::now().timestamp();
        let dashboard = Dashboard {
            id: ulid::Ulid::new().to_string(),
            name: validate_name(&input.name)?,
            panels: validate_panels(input.panels)?,
            created_at: now,
            updated_at: now,
        };
        let mut guard = self.dashboards.write().await;
        guard.push(dashboard.clone());
        self.persist(&guard);
        Ok(dashboard)
    }

    pub async fn update(&self, id: &str, input: DashboardInput) -> Option<Dashboard> {
        let name = validate_name(&input.name).ok()?;
        let panels = validate_panels(input.panels).ok()?;
        let mut guard = self.dashboards.write().await;
        let existing = guard.iter_mut().find(|d| d.id == id)?;
        existing.name = name;
        existing.panels = panels;
        existing.updated_at = chrono::Utc::now().timestamp();
        let updated = existing.clone();
        self.persist(&guard);
        Some(updated)
    }

    pub async fn delete(&self, id: &str) -> bool {
        let mut guard = self.dashboards.write().await;
        let before = guard.len();
        guard.retain(|d| d.id != id);
        let removed = guard.len() != before;
        if removed {
            self.persist(&guard);
        }
        removed
    }

    fn persist(&self, dashboards: &[Dashboard]) {
        if let Ok(json) = serde_json::to_string_pretty(dashboards) {
            let tmp = self.path.with_extension("json.tmp");
            if std::fs::write(&tmp, &json).is_ok() {
                let _ = std::fs::rename(tmp, &self.path);
            }
        }
    }
}

fn validate_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(parqtel_core::Error::Validation(
            "dashboard name is required".into(),
        ));
    }
    if name.len() > 120 {
        return Err(parqtel_core::Error::Validation(
            "dashboard name is limited to 120 bytes".into(),
        ));
    }
    Ok(name.to_string())
}

fn validate_panels(panels: Vec<DashboardPanelInput>) -> Result<Vec<DashboardPanel>> {
    if panels.len() > MAX_PANELS {
        return Err(parqtel_core::Error::Validation(format!(
            "a dashboard is limited to {MAX_PANELS} panels"
        )));
    }
    let mut out = Vec::with_capacity(panels.len());
    for panel in panels {
        if panel.title.trim().is_empty() {
            return Err(parqtel_core::Error::Validation(
                "panel title is required".into(),
            ));
        }
        if !matches!(panel.signal.as_str(), "metrics" | "logs") {
            return Err(parqtel_core::Error::Validation(
                "panel signal must be metrics or logs".into(),
            ));
        }
        if panel.query.trim().is_empty() {
            return Err(parqtel_core::Error::Validation(
                "panel query is required".into(),
            ));
        }
        out.push(DashboardPanel {
            id: if panel.id.trim().is_empty() {
                ulid::Ulid::new().to_string()
            } else {
                panel.id
            },
            title: panel.title.trim().to_string(),
            signal: panel.signal,
            query: panel.query,
            step_secs: panel
                .step_secs
                .unwrap_or_else(default_step)
                .clamp(1, 86_400),
            limit: panel.limit.unwrap_or_else(default_limit).clamp(1, 10_000),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn input(name: &str, signal: &str) -> DashboardInput {
        DashboardInput {
            name: name.into(),
            panels: vec![DashboardPanelInput {
                id: String::new(),
                title: "requests".into(),
                signal: signal.into(),
                query: "http_requests_total".into(),
                step_secs: None,
                limit: None,
            }],
        }
    }

    #[tokio::test]
    async fn dashboard_survives_restart_and_updates() {
        let dir = tempfile::tempdir().unwrap();
        let store = DashboardStore::new(dir.path().to_path_buf());
        let created = store.create(input("API", "metrics")).await.unwrap();
        assert!(!created.id.is_empty());
        assert!(!created.panels[0].id.is_empty());

        let restarted = DashboardStore::new(dir.path().to_path_buf());
        let loaded = restarted.get(&created.id).await.unwrap();
        assert_eq!(loaded.panels.len(), 1);
        assert_eq!(loaded.panels[0].query, "http_requests_total");

        let mut next = input("API v2", "logs");
        next.panels[0].query = "service=api".into();
        let updated = restarted.update(&created.id, next).await.unwrap();
        assert_eq!(updated.name, "API v2");
        assert_eq!(updated.panels[0].signal, "logs");
        assert_eq!(
            DashboardStore::new(dir.path().to_path_buf())
                .get(&created.id)
                .await
                .unwrap()
                .name,
            "API v2"
        );

        assert!(restarted.delete(&created.id).await);
        assert!(DashboardStore::new(dir.path().to_path_buf())
            .list()
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn invalid_panels_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = DashboardStore::new(dir.path().to_path_buf());
        assert!(store.create(input(" ", "metrics")).await.is_err());
        assert!(store.create(input("d", "events")).await.is_err());
        let mut bad = input("d", "logs");
        bad.panels[0].query = String::new();
        assert!(store.create(bad).await.is_err());
    }
}
