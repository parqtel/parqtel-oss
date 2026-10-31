//! Storage engine registry — the single place to register new backends.

use std::collections::HashMap;
use std::sync::Arc;

use super::parquet::ParquetStorageEngine;
use super::StorageEngine;
use crate::config::BlockConfig;
use crate::error::{Error, Result};

type Factory = Box<dyn Fn(BlockConfig) -> Arc<dyn StorageEngine> + Send + Sync>;

/// Maps backend name strings to factory functions.
pub struct StorageEngineRegistry {
    factories: HashMap<String, Factory>,
}

impl StorageEngineRegistry {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a custom backend factory.
    pub fn register<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(BlockConfig) -> Arc<dyn StorageEngine> + Send + Sync + 'static,
    {
        self.factories.insert(name.to_string(), Box::new(factory));
    }

    /// Register the built-in Parquet backend.
    pub fn register_parquet(&mut self) {
        self.register("parquet", |config| {
            Arc::new(ParquetStorageEngine::new(config))
        });
    }

    /// Build a storage engine by backend name.
    pub fn build(&self, backend: &str, config: BlockConfig) -> Result<Arc<dyn StorageEngine>> {
        match self.factories.get(backend) {
            Some(factory) => Ok(factory(config)),
            None => {
                let supported: Vec<&str> = self.factories.keys().map(|s| s.as_str()).collect();
                Err(Error::Config(format!(
                    "unsupported storage backend '{}'. Supported: {:?}",
                    backend, supported
                )))
            }
        }
    }

    /// List registered backend names.
    pub fn backends(&self) -> Vec<&str> {
        self.factories.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for StorageEngineRegistry {
    fn default() -> Self {
        let mut r = Self::new();
        r.register_parquet();
        r
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_default_has_parquet() {
        let reg = StorageEngineRegistry::default();
        assert!(reg.backends().contains(&"parquet"));
    }

    #[test]
    fn test_register_custom_backend() {
        let mut reg = StorageEngineRegistry::new();
        reg.register("custom", |config| {
            Arc::new(ParquetStorageEngine::new(config))
        });
        assert!(reg.backends().contains(&"custom"));
    }

    #[test]
    fn test_build_custom_backend() {
        let dir = tempdir().unwrap();
        let mut reg = StorageEngineRegistry::new();
        reg.register("custom", |config| {
            Arc::new(ParquetStorageEngine::new(config))
        });
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        assert!(reg.build("custom", config).is_ok());
    }

    #[test]
    fn test_build_unknown_backend_error() {
        let reg = StorageEngineRegistry::new();
        let config = BlockConfig::default();
        let result = reg.build("unknown", config);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("unsupported storage backend"));
    }

    #[test]
    fn test_backends_list() {
        let mut reg = StorageEngineRegistry::new();
        reg.register_parquet();
        reg.register("lance", |config| {
            Arc::new(ParquetStorageEngine::new(config))
        });
        let backends = reg.backends();
        assert!(backends.contains(&"parquet"));
        assert!(backends.contains(&"lance"));
        assert_eq!(backends.len(), 2);
    }
}
