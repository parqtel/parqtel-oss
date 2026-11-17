//! Optional object-storage backend configuration.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectStoreConfig {
    /// Upload blocks to the configured bucket. Local files remain the read
    /// cache and WAL staging area.
    pub enabled: bool,
    pub bucket: String,
    pub region: String,
    /// Custom endpoint for MinIO or another S3-compatible service.
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    /// Key prefix inside the bucket.
    pub prefix: String,
    /// Cache directory; defaults to `<storage.data_dir>/cache` when unset.
    pub cache_dir: Option<PathBuf>,
    /// Cache byte quota. Least-recently-used files are evicted first.
    pub cache_bytes: u64,
    /// How often the shared manifest is published and polled.
    pub catalog_interval_secs: u64,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bucket: String::new(),
            region: "us-east-1".into(),
            endpoint: None,
            access_key: None,
            secret_key: None,
            prefix: "parqtel".into(),
            cache_dir: None,
            cache_bytes: 1 << 30,
            catalog_interval_secs: 5,
        }
    }
}

impl ObjectStoreConfig {
    pub fn validate_into(&self, errors: &mut Vec<String>) {
        if !self.enabled {
            return;
        }
        if self.bucket.trim().is_empty() {
            errors.push("object_store.bucket is required when enabled".into());
        }
        if self.region.trim().is_empty() {
            errors.push("object_store.region is required when enabled".into());
        }
        if self.cache_bytes == 0 {
            errors.push("object_store.cache_bytes must be greater than 0".into());
        }
        if self.prefix.trim().is_empty() {
            errors.push("object_store.prefix cannot be empty".into());
        }
        if self.catalog_interval_secs == 0 {
            errors.push("object_store.catalog_interval_secs must be greater than 0".into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_config_needs_no_bucket() {
        let cfg = ObjectStoreConfig::default();
        let mut errors = Vec::new();
        cfg.validate_into(&mut errors);
        assert!(errors.is_empty());
    }

    #[test]
    fn enabled_config_validates_required_fields() {
        let cfg = ObjectStoreConfig {
            enabled: true,
            bucket: String::new(),
            region: String::new(),
            cache_bytes: 0,
            prefix: String::new(),
            ..Default::default()
        };
        let mut errors = Vec::new();
        cfg.validate_into(&mut errors);
        assert_eq!(errors.len(), 4);
    }
}
