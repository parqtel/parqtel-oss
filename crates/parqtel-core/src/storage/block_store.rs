//! The seam between block I/O and the storage location.
//!
//! The live path uses this trait for reads, deletes, and metadata so a future
//! object-store backend can populate a local cache and hand the scanner a
//! file, without teaching the scanner about a bucket SDK. Local mode keeps
//! every key as a filesystem path and is byte-for-byte the previous behavior.

use crate::error::{Error, Result};
use object_store::ObjectStoreExt;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

/// Metadata a store can answer without opening the block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockObject {
    pub key: PathBuf,
    pub size_bytes: u64,
}

/// Block I/O used by the scanner, compaction, and retention.
///
/// Keys are paths in local mode. An object-store implementation maps the same
/// key into its bucket and materialises a cache file for [`Self::open`].
pub trait BlockStore: Send + Sync + std::fmt::Debug {
    /// Opens a block for Parquet reading. The returned file must support
    /// `Seek`; object stores satisfy this with a local cache file.
    fn open(&self, key: &Path) -> Result<File>;

    /// Deletes a block. `Ok(false)` means it was already absent.
    fn delete(&self, key: &Path) -> Result<bool>;

    /// Size and key, or `None` when the block is absent.
    fn head(&self, key: &Path) -> Result<Option<BlockObject>>;

    /// Lists immediate block files under `prefix`.
    fn list(&self, prefix: &Path) -> Result<Vec<BlockObject>>;

    /// Called after a writer closes a local block. Local mode is a no-op;
    /// object storage uploads here.
    fn publish(&self, _key: &Path) -> Result<()> {
        Ok(())
    }
}

/// Local filesystem store. Absolute keys are used as-is; relative keys resolve
/// against `root`.
#[derive(Debug, Clone)]
pub struct LocalBlockStore {
    root: PathBuf,
}

impl LocalBlockStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, key: &Path) -> PathBuf {
        if key.is_absolute() || self.root.as_os_str().is_empty() {
            key.to_path_buf()
        } else {
            self.root.join(key)
        }
    }
}

impl BlockStore for LocalBlockStore {
    fn open(&self, key: &Path) -> Result<File> {
        File::open(self.resolve(key)).map_err(Error::Io)
    }

    fn delete(&self, key: &Path) -> Result<bool> {
        let path = self.resolve(key);
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Io(e)),
        }
    }

    fn head(&self, key: &Path) -> Result<Option<BlockObject>> {
        let path = self.resolve(key);
        match fs::metadata(path) {
            Ok(meta) if meta.is_file() => Ok(Some(BlockObject {
                key: key.to_path_buf(),
                size_bytes: meta.len(),
            })),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io(e)),
        }
    }

    fn list(&self, prefix: &Path) -> Result<Vec<BlockObject>> {
        let dir = self.resolve(prefix);
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::Io(e)),
        };
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let meta = entry.metadata().map_err(Error::Io)?;
            if meta.is_file() && entry.path().extension().is_some_and(|ext| ext == "parquet") {
                out.push(BlockObject {
                    key: entry.path(),
                    size_bytes: meta.len(),
                });
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }
}

static BLOCK_STORE: OnceLock<Arc<dyn BlockStore>> = OnceLock::new();

/// Installs the process-wide store. Called once by the server; tests that do
/// not install one use [`LocalBlockStore`] rooted at `/`, so absolute paths
/// behave exactly as before.
pub fn install(store: Arc<dyn BlockStore>) -> Result<()> {
    BLOCK_STORE
        .set(store)
        .map_err(|_| Error::Validation("block store is already installed".into()))
}

/// The process-wide store.
pub fn store() -> &'static Arc<dyn BlockStore> {
    BLOCK_STORE.get_or_init(|| Arc::new(LocalBlockStore::new(Path::new("/"))))
}

/// Opens a block through the configured store.
pub fn open(key: &Path) -> Result<File> {
    store().open(key)
}

/// Deletes a block through the configured store.
pub fn delete(key: &Path) -> Result<bool> {
    store().delete(key)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn local_store_roundtrip_and_parity() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let key = dir.path().join("block.parquet");
        let mut file = File::create(&key).unwrap();
        file.write_all(b"parquet").unwrap();

        let mut via_store = store.open(&key).unwrap();
        let mut bytes = String::new();
        via_store.read_to_string(&mut bytes).unwrap();
        assert_eq!(bytes, "parquet");
        assert_eq!(store.head(&key).unwrap().unwrap().size_bytes, 7);
        assert_eq!(store.list(dir.path()).unwrap().len(), 1);
        assert!(store.delete(&key).unwrap());
        assert!(!store.delete(&key).unwrap());
        assert!(store.head(&key).unwrap().is_none());
    }

    #[test]
    fn relative_keys_resolve_against_root() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let key = Path::new("relative.parquet");
        File::create(dir.path().join(key)).unwrap();
        assert!(store.head(key).unwrap().is_some());
        assert!(store.open(key).is_ok());
    }
}

/// Object-storage store backed by `object_store`, with a bounded local read
/// cache. `open` materialises a cache file because Parquet reading needs
/// `Seek`; `publish` uploads a locally closed block.
#[derive(Debug)]
pub struct ObjectBlockStore {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: object_store::path::Path,
    cache_dir: PathBuf,
    cache_bytes: u64,
    handle: tokio::runtime::Handle,
}

impl ObjectBlockStore {
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        cache_dir: PathBuf,
        cache_bytes: u64,
        handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        fs::create_dir_all(&cache_dir)?;
        Ok(Self {
            store,
            prefix: object_store::path::Path::from(prefix),
            cache_dir,
            cache_bytes,
            handle,
        })
    }

    /// Builds an S3 client from explicit configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn s3(
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        prefix: &str,
        cache_dir: PathBuf,
        cache_bytes: u64,
        handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        let mut builder = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(region);
        if let Some(endpoint) = endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        if let (Some(access), Some(secret)) = (access_key, secret_key) {
            builder = builder
                .with_access_key_id(access)
                .with_secret_access_key(secret);
        }
        let store = builder
            .build()
            .map_err(|e| Error::Internal(format!("S3 client: {e}")))?;
        Self::new(Arc::new(store), prefix, cache_dir, cache_bytes, handle)
    }

    /// Catalog over the same bucket and prefix.
    pub fn catalog(&self, prefix: &str) -> crate::storage::ObjectCatalog {
        crate::storage::ObjectCatalog::new(self.store.clone(), prefix)
    }

    fn key(&self, key: &Path) -> object_store::path::Path {
        let name = key
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| key.to_string_lossy().into_owned());
        self.prefix.clone().join(name)
    }

    fn cache_path(&self, key: &Path) -> PathBuf {
        let name = key
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "block.parquet".into());
        self.cache_dir.join(name)
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> Result<F::Output> {
        if tokio::runtime::Handle::try_current().is_ok() {
            // Only legal on a multi-thread runtime; the server and scans are.
            Ok(tokio::task::block_in_place(|| self.handle.block_on(future)))
        } else {
            Ok(self.handle.block_on(future))
        }
    }

    fn evict(&self) {
        let mut entries: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
        let Ok(dir) = fs::read_dir(&self.cache_dir) else {
            return;
        };
        for entry in dir.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let used = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
            entries.push((entry.path(), meta.len(), used));
        }
        let mut total: u64 = entries.iter().map(|(_, size, _)| *size).sum();
        if total <= self.cache_bytes {
            return;
        }
        entries.sort_by_key(|(_, _, used)| *used);
        for (path, size, _) in entries {
            if total <= self.cache_bytes {
                break;
            }
            if fs::remove_file(&path).is_ok() {
                total = total.saturating_sub(size);
            }
        }
    }
}

impl BlockStore for ObjectBlockStore {
    fn open(&self, key: &Path) -> Result<File> {
        let cached = self.cache_path(key);
        if cached.exists() {
            return File::open(cached).map_err(Error::Io);
        }
        let object_key = self.key(key);
        let bytes = self
            .block_on(async { self.store.get(&object_key).await?.bytes().await })?
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        let tmp = cached.with_extension("tmp");
        fs::write(&tmp, &bytes)?;
        fs::rename(tmp, &cached)?;
        self.evict();
        File::open(cached).map_err(Error::Io)
    }

    fn delete(&self, key: &Path) -> Result<bool> {
        let _ = fs::remove_file(self.cache_path(key));
        let object_key = self.key(key);
        match self.block_on(async { self.store.delete(&object_key).await })? {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Io(std::io::Error::other(e.to_string()))),
        }
    }

    fn head(&self, key: &Path) -> Result<Option<BlockObject>> {
        let object_key = self.key(key);
        match self.block_on(async { self.store.head(&object_key).await })? {
            Ok(meta) => Ok(Some(BlockObject {
                key: key.to_path_buf(),
                size_bytes: meta.size,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::Io(std::io::Error::other(e.to_string()))),
        }
    }

    fn list(&self, prefix: &Path) -> Result<Vec<BlockObject>> {
        let list_prefix = if prefix.as_os_str().is_empty() {
            self.prefix.clone()
        } else {
            self.prefix
                .clone()
                .join(prefix.to_string_lossy().into_owned())
        };
        let listed = self.block_on(async {
            use futures::StreamExt;
            let mut stream = self.store.list(Some(&list_prefix));
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item);
            }
            out
        })?;
        let mut out = Vec::new();
        for item in listed {
            let item = item.map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
            let name = item.location.filename().unwrap_or_default();
            out.push(BlockObject {
                key: PathBuf::from(name),
                size_bytes: item.size,
            });
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    fn publish(&self, key: &Path) -> Result<()> {
        let bytes = fs::read(key)?;
        let object_key = self.key(key);
        self.block_on(async {
            self.store
                .put(&object_key, bytes::Bytes::from(bytes).into())
                .await
        })?
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        Ok(())
    }
}

#[cfg(test)]
mod object_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::io::{Read, Write};

    #[tokio::test(flavor = "multi_thread")]
    async fn object_store_roundtrip_and_bounded_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        let local = ObjectBlockStore::new(
            store,
            "blocks",
            dir.path().join("cache"),
            8,
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let src = dir.path().join("block.parquet");
        let mut file = File::create(&src).unwrap();
        file.write_all(b"parquet").unwrap();
        local.publish(&src).unwrap();
        assert!(local.head(&src).unwrap().is_some());

        fs::remove_file(&src).unwrap();
        let mut opened = local.open(&src).unwrap();
        let mut got = String::new();
        opened.read_to_string(&mut got).unwrap();
        assert_eq!(got, "parquet");
        assert_eq!(local.list(Path::new("")).unwrap().len(), 1);
        assert!(local.delete(&src).unwrap());
        assert!(local.head(&src).unwrap().is_none());
    }
}
