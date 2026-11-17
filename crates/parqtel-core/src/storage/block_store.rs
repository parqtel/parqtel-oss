//! The seam between block I/O and the storage location.
//!
//! The live path uses this trait for reads, deletes, and metadata so a future
//! object-store backend can populate a local cache and hand the scanner a
//! file, without teaching the scanner about a bucket SDK. Local mode keeps
//! every key as a filesystem path and is byte-for-byte the previous behavior.

use crate::error::{Error, Result};
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
