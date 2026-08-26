mod alert;
mod ingest;
mod query;
mod server;
mod storage;
mod telemetry;

pub use alert::{AlertConfig, NotificationConfig, PostmortemConfig};
pub use ingest::IngestConfig;
pub use query::{QueryConfig, UIConfig};
pub use server::ServerConfig;
pub use storage::{BlockConfig, LogBlockConfig, RetentionConfig};
pub use telemetry::{K8sProviderConfig, TelemetryConfig};

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

/// Global configuration for parqtel.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: BlockConfig,
    pub logs: LogBlockConfig,
    pub ingest: IngestConfig,
    pub query: QueryConfig,
    pub ui: UIConfig,
    pub telemetry: TelemetryConfig,
    pub k8s_provider: K8sProviderConfig,
    pub alerts: AlertConfig,
}

impl Config {
    /// Validates the configuration and returns all errors found.
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();

        if self.server.bind_address.is_empty() {
            errors.push("server.bind_address cannot be empty".to_string());
        }
        if self.storage.data_dir.as_os_str().is_empty() {
            errors.push("storage.data_dir must be a non-empty path".to_string());
        }
        if self.logs.data_dir.as_os_str().is_empty() {
            errors.push("logs.data_dir must be a non-empty path".to_string());
        }

        let valid_codecs = ["zstd", "snappy", "lz4", "none"];
        if !valid_codecs.contains(&self.storage.compression.as_str()) {
            errors.push(format!(
                "storage.compression must be one of: {}",
                valid_codecs.join(", ")
            ));
        }
        if !valid_codecs.contains(&self.logs.compression.as_str()) {
            errors.push(format!(
                "logs.compression must be one of: {}",
                valid_codecs.join(", ")
            ));
        }

        let valid_formats = ["text", "json"];
        if !valid_formats.contains(&self.telemetry.log_format.as_str()) {
            errors.push("telemetry.log_format must be 'text' or 'json'".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Validation(errors.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_config_defaults() {
        let config = Config::default();
        assert_eq!(config.server.bind_address, "0.0.0.0:8080");
        assert_eq!(config.storage.compression, "zstd");
        assert!(config.ui.enabled);
    }

    #[test]
    fn test_config_validation_success() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_failures() {
        let mut config = Config::default();
        config.server.bind_address = "".into();
        config.storage.data_dir = "".into();
        config.logs.data_dir = "".into();
        config.storage.compression = "invalid".into();
        config.logs.compression = "invalid".into();
        config.telemetry.log_format = "invalid".into();

        let res = config.validate();
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(err.contains("server.bind_address"));
        assert!(err.contains("storage.data_dir"));
        assert!(err.contains("logs.data_dir"));
        assert!(err.contains("storage.compression"));
        assert!(err.contains("logs.compression"));
        assert!(err.contains("telemetry.log_format"));
    }

    #[test]
    fn test_log_block_config_conversion() {
        let log_config = LogBlockConfig::default();
        let block_config: BlockConfig = log_config.into();
        assert_eq!(block_config.data_dir, PathBuf::from("data/logs"));
    }

    #[test]
    fn test_postmortem_config_defaults() {
        let config = PostmortemConfig::default();
        assert!(config.enabled);
        assert!(config.auto_draft);
        assert_eq!(config.postmortem_delay_minutes, 5);
        assert_eq!(config.min_duration_minutes, 5);
        assert_eq!(config.min_severity, "warning");
    }

    #[test]
    fn test_ingest_config_defaults() {
        let config = IngestConfig::default();
        assert_eq!(config.max_body_size, 10 * 1024 * 1024);
        assert!(!config.wal_enabled);
        assert!(config.log_wal_enabled);
    }

    #[test]
    fn test_query_config_defaults() {
        let config = QueryConfig::default();
        assert_eq!(config.max_series, 1000);
        assert_eq!(config.max_samples_per_series, 10000);
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn test_telemetry_config_defaults() {
        let config = TelemetryConfig::default();
        assert_eq!(config.log_level, "info");
        assert_eq!(config.log_format, "text");
    }

    #[test]
    fn test_k8s_provider_config_defaults() {
        let config = K8sProviderConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind_address, "0.0.0.0:6443");
        assert_eq!(config.cache_expiry_secs, 30);
    }

    #[test]
    fn test_alert_config_defaults() {
        let config = AlertConfig::default();
        assert_eq!(config.rules_dir, "rules");
        assert_eq!(config.noise_window_firings, 30);
        assert!(config.refinement_enabled);
    }

    #[test]
    fn test_notification_config_defaults() {
        let config = NotificationConfig::default();
        assert_eq!(config.max_concurrent_sends, 0);
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = Config::default();
        let json = serde_json::to_string(&config).unwrap();
        let decoded: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.server.bind_address, "0.0.0.0:8080");
    }

    #[test]
    fn test_server_config_defaults() {
        let config = ServerConfig::default();
        assert_eq!(config.max_connections, 1024);
        assert_eq!(config.shutdown_timeout_secs, 30);
    }

    #[test]
    fn test_block_config_defaults() {
        let config = BlockConfig::default();
        assert_eq!(config.backend, "parquet");
        assert_eq!(config.block_duration_secs, 7200);
        assert_eq!(config.max_rows_per_block, 1_000_000);
        assert_eq!(config.retention_days, 7);
        assert_eq!(config.row_group_size, 100_000);
    }

    #[test]
    fn test_notification_config_serde_defaults() {
        let json = r#"{}"#;
        let config: NotificationConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.dedup_flush_interval_secs, 300);
        assert_eq!(config.max_concurrent_sends, 10);
        assert_eq!(config.send_timeout_secs, 30);
        assert_eq!(config.self_monitoring_failure_threshold, 0.5);
    }

    #[test]
    fn test_postmortem_config_serde_defaults() {
        let json = r#"{"enabled": true, "auto_draft": true}"#;
        let config: PostmortemConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.postmortem_delay_minutes, 5);
        assert!(config.auto_publish_slack);
        assert_eq!(config.min_duration_minutes, 5);
        assert_eq!(config.min_severity, "warning");
        assert_eq!(config.max_knowledge_entries, 1000);
    }

    #[test]
    fn test_block_config_serde_backend_default() {
        let json = r#"{"data_dir": "/tmp", "block_duration_secs": 60, "max_rows_per_block": 100, "compression": "zstd", "retention_days": 1, "compaction_interval_secs": 60, "row_group_size": 10}"#;
        let config: BlockConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.backend, "parquet");
    }

    #[test]
    fn test_log_block_config_defaults() {
        let config = LogBlockConfig::default();
        assert_eq!(config.data_dir, PathBuf::from("data/logs"));
        assert_eq!(config.block_duration_secs, 1800);
        assert_eq!(config.max_rows_per_block, 200_000);
        assert_eq!(config.retention_days, 3);
    }
}
