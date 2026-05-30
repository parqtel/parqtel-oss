use serde::{Deserialize, Serialize};

/// Configuration for the alert engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Directory where alert rule YAML files are stored.
    pub rules_dir: String,
    /// Number of firings to consider for noise analysis.
    pub noise_window_firings: usize,
    /// Whether to enable auto-refinement proposals.
    pub refinement_enabled: bool,
    /// Threshold for noise suppression (0.0-1.0).
    pub noise_suppression_threshold: f32,
    /// Postmortem engine configuration.
    #[serde(default)]
    pub postmortem: Option<PostmortemConfig>,
    /// Notification router configuration.
    #[serde(default)]
    pub notifications: Option<NotificationConfig>,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            rules_dir: "rules".into(),
            noise_window_firings: 30,
            refinement_enabled: true,
            noise_suppression_threshold: 0.7,
            postmortem: Some(PostmortemConfig::default()),
            notifications: Some(NotificationConfig::default()),
        }
    }
}

/// Configuration for the notification router.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotificationConfig {
    #[serde(default = "default_dedup_flush")]
    pub dedup_flush_interval_secs: u64,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_sends: usize,
    #[serde(default = "default_send_timeout")]
    pub send_timeout_secs: u64,
    #[serde(default = "default_failure_threshold")]
    pub self_monitoring_failure_threshold: f64,
}

fn default_dedup_flush() -> u64 { 300 }
fn default_max_concurrent() -> usize { 10 }
fn default_send_timeout() -> u64 { 30 }
fn default_failure_threshold() -> f64 { 0.5 }

/// Configuration for the postmortem engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostmortemConfig {
    /// Whether the postmortem engine is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Whether to auto-generate drafts when alerts resolve.
    #[serde(default)]
    pub auto_draft: bool,
    /// Minutes to wait after resolution before generating draft.
    #[serde(default = "default_postmortem_delay")]
    pub postmortem_delay_minutes: u64,
    /// Path to custom template directory (empty = use built-in default).
    pub template_path: Option<String>,
    /// Whether to auto-publish to Notion.
    #[serde(default)]
    pub auto_publish_notion: bool,
    /// Whether to auto-publish to Google Docs.
    #[serde(default)]
    pub auto_publish_gdocs: bool,
    /// Whether to auto-publish to Slack.
    #[serde(default = "default_true")]
    pub auto_publish_slack: bool,
    /// Whether to auto-create Jira action items.
    #[serde(default)]
    pub auto_create_jira_actions: bool,
    /// Minimum incident duration in minutes to trigger postmortem.
    #[serde(default = "default_min_duration")]
    pub min_duration_minutes: u64,
    /// Minimum severity level to trigger postmortem.
    #[serde(default = "default_min_severity")]
    pub min_severity: String,
    /// Maximum number of entries in the knowledge base.
    #[serde(default = "default_max_knowledge_entries")]
    pub max_knowledge_entries: usize,
}

impl Default for PostmortemConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_draft: true,
            postmortem_delay_minutes: 5,
            template_path: None,
            auto_publish_notion: false,
            auto_publish_gdocs: false,
            auto_publish_slack: true,
            auto_create_jira_actions: false,
            min_duration_minutes: 5,
            min_severity: "warning".to_string(),
            max_knowledge_entries: 1000,
        }
    }
}

fn default_postmortem_delay() -> u64 { 5 }
fn default_true() -> bool { true }
fn default_min_duration() -> u64 { 5 }
fn default_min_severity() -> String { "warning".to_string() }
fn default_max_knowledge_entries() -> usize { 1000 }
