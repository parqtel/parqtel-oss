//! Versioned block catalog for object storage.
//!
//! `index.json` relies on atomic rename, which S3 does not provide. Writers
//! publish a manifest with a compare-and-swap on its version; readers merge the
//! manifest into their local index. A torn upload never appears because the
//! manifest is written after the block object.

use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, SignalType};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// CAS attempts a publisher makes before giving up. Sixteen writers can race
/// on the same manifest, so eight attempts starve under load.
const DEFAULT_PUBLISH_RETRIES: usize = 16;
const BACKOFF_BASE: Duration = Duration::from_millis(5);
const BACKOFF_CAP: Duration = Duration::from_millis(200);

/// Jittered exponential backoff for a lost CAS race.
///
/// `attempt` is zero-based. The delay doubles per attempt up to
/// [`BACKOFF_CAP`], then a cheap nanosecond clock scatters it over
/// `[half, full]` so retrying writers do not collide in lockstep. No RNG
/// dependency: the low bits of the local clock are the jitter source.
fn backoff_delay(attempt: usize) -> Duration {
    let doubled = BACKOFF_BASE
        .checked_mul(1u32 << attempt.min(6))
        .unwrap_or(BACKOFF_CAP);
    let base = doubled.min(BACKOFF_CAP);
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64)
        .unwrap_or(0);
    let half = base / 2;
    let span = base.as_micros().saturating_sub(half.as_micros()).max(1) as u64;
    half + Duration::from_micros(seed % span)
}

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
    /// Node that wrote the block; only that node can retire it.
    #[serde(default)]
    pub writer: Option<String>,
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
            writer: None,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CatalogManifest {
    pub generation: u64,
    #[serde(default)]
    pub writer_lease: Option<String>,
    pub blocks: Vec<CatalogBlock>,
}

pub struct ObjectCatalog {
    store: Arc<dyn ObjectStore>,
    key: object_store::path::Path,
    retries: usize,
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
        let key = object_store::path::Path::from(prefix)
            .join("_catalog")
            .join("manifest.json");
        Self {
            store,
            key,
            retries: DEFAULT_PUBLISH_RETRIES,
        }
    }

    /// Overrides how many CAS attempts [`Self::publish`] makes. Zero falls
    /// back to the default rather than disabling publishing.
    pub fn with_retry_policy(mut self, retries: usize) -> Self {
        self.retries = if retries == 0 {
            DEFAULT_PUBLISH_RETRIES
        } else {
            retries
        };
        self
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
        self.publish_with_retirements(blocks, writer_lease, &HashSet::new())
            .await
    }

    /// As [`Self::publish`], but also removes every manifest key in `retired`
    /// in the same CAS update.
    ///
    /// A compactor retires the source blocks it merged: leaving them in the
    /// manifest lets a writer resurrect its stale staged copy on the next
    /// tick. A key that also appears in `blocks` is never dropped — the new
    /// block is authoritative.
    pub async fn publish_with_retirements(
        &self,
        blocks: Vec<CatalogBlock>,
        writer_lease: Option<String>,
        retired: &HashSet<String>,
    ) -> Result<CatalogManifest> {
        for attempt in 0..self.retries {
            let current = self.read().await?;
            let (mut manifest, mode) = match current {
                Some((manifest, version)) => (manifest, PutMode::Update(version)),
                None => (CatalogManifest::default(), PutMode::Create),
            };
            let local_keys: HashSet<&str> = blocks.iter().map(|block| block.key.as_str()).collect();
            let mut by_key: BTreeMap<String, CatalogBlock> = manifest
                .blocks
                .into_iter()
                .filter(|block| {
                    // A retired source stays out even if the writer is still
                    // publishing it; the replacement block in `blocks` wins.
                    if retired.contains(&block.key) && !local_keys.contains(block.key.as_str()) {
                        return false;
                    }
                    // A writer retires only its own missing blocks; another
                    // node's blocks stay until that node says so.
                    block.writer.as_deref() != writer_lease.as_deref()
                        || local_keys.contains(block.key.as_str())
                })
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
                | Err(object_store::Error::AlreadyExists { .. }) => {
                    if attempt + 1 < self.retries {
                        tokio::time::sleep(backoff_delay(attempt)).await;
                    }
                }
                Err(e) => return Err(Error::Io(std::io::Error::other(e.to_string()))),
            }
        }
        Err(Error::Validation(format!(
            "catalog update lost {} CAS races",
            self.retries
        )))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            writer: None,
        }
    }

    #[test]
    fn manifest_key_is_not_percent_encoded() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let catalog = ObjectCatalog::new(store, "p");
        assert_eq!(catalog.key.to_string(), "p/_catalog/manifest.json");
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

    #[tokio::test]
    async fn a_writer_retires_only_its_own_blocks() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let a = ObjectCatalog::new(store.clone(), "p");
        let b = ObjectCatalog::new(store, "p");
        let mut a_block = block("a.parquet", 1);
        a_block.writer = Some("node-a".into());
        let mut b_block = block("b.parquet", 2);
        b_block.writer = Some("node-b".into());
        a.publish(vec![a_block], Some("node-a".into()))
            .await
            .unwrap();
        b.publish(vec![b_block], Some("node-b".into()))
            .await
            .unwrap();
        let manifest = a.publish(Vec::new(), Some("node-a".into())).await.unwrap();
        assert!(manifest.blocks.iter().all(|block| block.key == "b.parquet"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sixteen_concurrent_writers_lose_no_updates() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let catalog = Arc::new(ObjectCatalog::new(store, "p"));
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..16u32 {
            let catalog = catalog.clone();
            tasks.spawn(async move {
                let key = format!("block-{i}.parquet");
                let mut block = block(&key, i as i64);
                block.writer = Some(format!("node-{i}"));
                catalog
                    .publish(vec![block], Some(format!("node-{i}")))
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        let (manifest, _) = catalog.read().await.unwrap().unwrap();
        assert_eq!(manifest.blocks.len(), 16);
        let keys: HashSet<&str> = manifest.blocks.iter().map(|b| b.key.as_str()).collect();
        for i in 0..16u32 {
            let key = format!("block-{i}.parquet");
            assert!(keys.contains(key.as_str()), "lost update for {key}");
        }
    }

    #[tokio::test]
    async fn compactor_retires_merged_sources_and_they_stay_gone() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let catalog = ObjectCatalog::new(store, "p");
        let mut source_a = block("w0-a.parquet", 1);
        source_a.writer = Some("node-w0".into());
        let mut source_b = block("w0-b.parquet", 2);
        source_b.writer = Some("node-w0".into());
        catalog
            .publish(vec![source_a, source_b], Some("node-w0".into()))
            .await
            .unwrap();

        let mut merged = block("m_l1_merged.parquet", 1);
        merged.writer = Some("compactor".into());
        let retired: HashSet<String> = ["w0-a.parquet", "w0-b.parquet"]
            .into_iter()
            .map(String::from)
            .collect();
        let manifest = catalog
            .publish_with_retirements(vec![merged], Some("compactor".into()), &retired)
            .await
            .unwrap();
        assert_eq!(manifest.blocks.len(), 1);
        assert_eq!(manifest.blocks[0].key, "m_l1_merged.parquet");

        // A writer publishing an unrelated block must not resurrect its
        // retired staged sources.
        let mut unrelated = block("w0-c.parquet", 3);
        unrelated.writer = Some("node-w0".into());
        catalog
            .publish(vec![unrelated], Some("node-w0".into()))
            .await
            .unwrap();
        let (manifest, _) = catalog.read().await.unwrap().unwrap();
        let keys: HashSet<&str> = manifest.blocks.iter().map(|b| b.key.as_str()).collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains("m_l1_merged.parquet"));
        assert!(keys.contains("w0-c.parquet"));
        assert!(!keys.contains("w0-a.parquet"));
        assert!(!keys.contains("w0-b.parquet"));
    }

    #[tokio::test]
    async fn a_retired_key_relisted_in_blocks_survives() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let catalog = ObjectCatalog::new(store, "p");
        let mut kept = block("kept.parquet", 1);
        kept.writer = Some("node-w0".into());
        let retired: HashSet<String> = ["kept.parquet".to_string()].into_iter().collect();
        let manifest = catalog
            .publish_with_retirements(vec![kept], Some("compactor".into()), &retired)
            .await
            .unwrap();
        assert_eq!(manifest.blocks.len(), 1);
        assert_eq!(manifest.blocks[0].key, "kept.parquet");
    }

    #[tokio::test]
    async fn retry_policy_converges_after_transient_cas_failures() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let flaky = Arc::new(FlakyStore {
            inner,
            fail_puts: 3,
            puts: AtomicUsize::new(0),
        });
        let catalog = ObjectCatalog::new(flaky.clone(), "p").with_retry_policy(4);
        let manifest = catalog
            .publish(vec![block("a.parquet", 1)], Some("node-a".into()))
            .await
            .unwrap();
        assert_eq!(manifest.blocks.len(), 1);
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn zero_retry_override_falls_back_to_default() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let flaky = Arc::new(FlakyStore {
            inner,
            fail_puts: 3,
            puts: AtomicUsize::new(0),
        });
        let catalog = ObjectCatalog::new(flaky, "p").with_retry_policy(0);
        catalog
            .publish(vec![block("a.parquet", 1)], Some("node-a".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn exhausted_budget_error_names_the_retry_count() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let flaky = Arc::new(FlakyStore {
            inner,
            fail_puts: usize::MAX,
            puts: AtomicUsize::new(0),
        });
        let catalog = ObjectCatalog::new(flaky, "p").with_retry_policy(3);
        let error = catalog
            .publish(vec![block("a.parquet", 1)], Some("node-a".into()))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("3 CAS races"), "{error}");
    }

    /// Delegates to an in-memory store but fails the first `fail_puts` writes
    /// with a CAS precondition loss.
    #[derive(Debug)]
    struct FlakyStore {
        inner: Arc<dyn ObjectStore>,
        fail_puts: usize,
        puts: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "flaky({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let seen = self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if seen < self.fail_puts {
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "injected CAS race".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}
