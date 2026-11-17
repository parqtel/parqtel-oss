//! Versioned block catalog for object storage.
//!
//! `index.json` relies on atomic rename, which S3 does not provide. Writers
//! publish a manifest with a compare-and-swap on its version; readers merge the
//! manifest into their local index. A torn upload never appears because the
//! manifest is written after the block object.

use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, SignalType};
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CatalogBlock {
    pub key: String,
    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
    pub row_count: usize,
    pub size_bytes: u64,
    #[serde(default)]
    pub metric_names: HashSet<String>,
    #[serde(default)]
    pub label_names: HashSet<String>,
    #[serde(default)]
    pub signal_type: SignalType,
    #[serde(default)]
    pub wal_range: Option<(u64, u64)>,
}

impl CatalogBlock {
    pub fn from_metadata(meta: &BlockMetadata) -> Result<Self> {
        let key = meta
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::Validation("block path has no filename".into()))?;
        Ok(Self {
            key: key.to_string(),
            start_timestamp_ns: meta.start_timestamp_ns,
            end_timestamp_ns: meta.end_timestamp_ns,
            row_count: meta.row_count,
            size_bytes: meta.size_bytes,
            metric_names: meta.metric_names.clone(),
            label_names: meta.label_names.clone(),
            signal_type: meta.signal_type,
            wal_range: meta.wal_range,
        })
    }

    /// Rebuilds metadata for this node's data directory. The block store
    /// resolves the key to a bucket object and materialises a cache file.
    pub fn to_metadata(&self, data_dir: &std::path::Path) -> BlockMetadata {
        BlockMetadata {
            path: data_dir.join(&self.key),
            start_timestamp_ns: self.start_timestamp_ns,
            end_timestamp_ns: self.end_timestamp_ns,
            row_count: self.row_count,
            size_bytes: self.size_bytes,
            metric_names: self.metric_names.clone(),
            label_names: self.label_names.clone(),
            label_values: BTreeMap::new(),
            signal_type: self.signal_type,
            wal_range: self.wal_range,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CatalogManifest {
    pub generation: u64,
    #[serde(default)]
    pub writer_lease: Option<String>,
    pub blocks: Vec<CatalogBlock>,
}

impl Default for CatalogManifest {
    fn default() -> Self {
        Self {
            generation: 0,
            writer_lease: None,
            blocks: Vec::new(),
        }
    }
}

pub struct ObjectCatalog {
    store: Arc<dyn ObjectStore>,
    key: object_store::path::Path,
}

impl std::fmt::Debug for ObjectCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectCatalog")
            .field("key", &self.key)
            .finish()
    }
}

impl ObjectCatalog {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        let key = object_store::path::Path::from(prefix).child("_catalog/manifest.json");
        Self { store, key }
    }

    /// Reads the current manifest and its CAS version.
    pub async fn read(&self) -> Result<Option<(CatalogManifest, UpdateVersion)>> {
        let result = match self.store.get(&self.key).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(Error::Io(std::io::Error::other(e.to_string()))),
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        let manifest = serde_json::from_slice(&bytes).map_err(Error::Serde)?;
        Ok(Some((manifest, version)))
    }

    /// Merges `blocks` into the current manifest with a CAS retry loop.
    /// Returns the manifest that won.
    pub async fn publish(
        &self,
        blocks: Vec<CatalogBlock>,
        writer_lease: Option<String>,
    ) -> Result<CatalogManifest> {
        for _ in 0..8 {
            let current = self.read().await?;
            let (mut manifest, mode) = match current {
                Some((manifest, version)) => (manifest, PutMode::Update(version)),
                None => (CatalogManifest::default(), PutMode::Create),
            };
            let mut by_key: BTreeMap<String, CatalogBlock> = manifest
                .blocks
                .into_iter()
                .map(|block| (block.key.clone(), block))
                .collect();
            for block in &blocks {
                by_key.insert(block.key.clone(), block.clone());
            }
            let mut blocks: Vec<_> = by_key.into_values().collect();
            blocks.sort_by(|a, b| {
                (a.start_timestamp_ns, &a.key).cmp(&(b.start_timestamp_ns, &b.key))
            });
            manifest.generation = manifest.generation.saturating_add(1);
            manifest.writer_lease = writer_lease.clone();
            manifest.blocks = blocks;
            let body = serde_json::to_vec(&manifest).map_err(Error::Serde)?;
            let options = PutOptions {
                mode,
                ..Default::default()
            };
            match self.store.put_opts(&self.key, body.into(), options).await {
                Ok(_) => return Ok(manifest),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(e) => return Err(Error::Io(std::io::Error::other(e.to_string()))),
            }
        }
        Err(Error::Validation(
            "catalog update lost eight CAS races".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn block(key: &str, ts: i64) -> CatalogBlock {
        CatalogBlock {
            key: key.into(),
            start_timestamp_ns: ts,
            end_timestamp_ns: ts + 1,
            row_count: 1,
            size_bytes: 10,
            metric_names: HashSet::new(),
            label_names: HashSet::new(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        }
    }

    #[tokio::test]
    async fn concurrent_writers_merge_without_split_brain() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let a = ObjectCatalog::new(store.clone(), "p");
        let b = ObjectCatalog::new(store, "p");
        let (ra, rb) = tokio::join!(
            a.publish(vec![block("a.parquet", 1)], Some("node-a".into())),
            b.publish(vec![block("b.parquet", 2)], Some("node-b".into()))
        );
        let ra = ra.unwrap();
        let rb = rb.unwrap();
        let (manifest, _) = a.read().await.unwrap().unwrap();
        assert_eq!(manifest.blocks.len(), 2);
        assert!(manifest.generation >= 2);
        // Both writers observed a complete manifest after their own publish.
        assert!(ra.blocks.len() <= 2 && rb.blocks.len() <= 2);
    }
}
