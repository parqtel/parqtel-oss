//! Cross-node block ownership.
//!
//! In a sharded deployment every node sees the whole shared catalog, so a
//! compactor or retention sweep can meet blocks another writer owns. Deleting
//! one of those would destroy data that node is still serving. This registry
//! records which block keys the catalog assigns to another writer; retention
//! and compaction ask it before retiring anything.
//!
//! Local single-node deployments construct [`OwnershipRegistry::everything`],
//! whose checks always pass, so behaviour there is unchanged.

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Records the block keys that belong to other writers.
#[derive(Debug, Clone, Default)]
pub struct OwnershipRegistry {
    /// Keys published by another writer. Empty means "no scoping": every block
    /// is retirable, which is the local single-node behaviour.
    foreign: Arc<RwLock<HashSet<String>>>,
    /// Whether ownership is scoped at all. A scoped registry never retires a
    /// block it cannot prove is local.
    scoped: bool,
    /// A compactor is the single authority for its bucket/prefix: it owns the
    /// merged output and may retire any source it was asked to merge.
    compactor: bool,
}

impl OwnershipRegistry {
    /// Registry that permits every retirement (single-node mode).
    pub fn everything() -> Self {
        Self::default()
    }

    /// Scoped registry: only blocks this node staged locally and the catalog
    /// does not assign to another writer may be retired.
    pub fn scoped() -> Self {
        Self {
            foreign: Arc::new(RwLock::new(HashSet::new())),
            scoped: true,
            compactor: false,
        }
    }

    /// Compactor registry: every block in the shared catalog is retirable.
    ///
    /// A compactor has no staged local copy of writer blocks, so the scoped
    /// check would refuse every merge and retention sweep. It is also the only
    /// node allowed to retire a key another writer produced, because it is the
    /// single authority for that bucket/prefix.
    pub fn compactor() -> Self {
        Self {
            foreign: Arc::new(RwLock::new(HashSet::new())),
            scoped: true,
            compactor: true,
        }
    }

    /// True when the registry distinguishes owners.
    pub fn is_scoped(&self) -> bool {
        self.scoped
    }

    /// True when this registry belongs to a compactor node.
    pub fn is_compactor(&self) -> bool {
        self.compactor
    }

    /// Records the catalog's owner for `key`. A `None` writer is a legacy
    /// entry no node has claimed, so it stays retirable.
    pub fn adopt(&self, key: &str, writer: Option<&str>, node_id: &str) {
        if !self.scoped {
            return;
        }
        let Ok(mut foreign) = self.foreign.write() else {
            return;
        };
        match writer {
            Some(owner) if owner != node_id => {
                foreign.insert(key.to_string());
            }
            _ => {
                foreign.remove(key);
            }
        }
    }

    /// Marks `key` as published by this node.
    pub fn own(&self, key: &str) {
        if !self.scoped {
            return;
        }
        if let Ok(mut foreign) = self.foreign.write() {
            foreign.remove(key);
        }
    }

    /// True when this node may delete or merge `path`.
    ///
    /// A scoped registry requires both: the catalog does not assign the key to
    /// another writer, and the block is staged locally rather than being a
    /// read-cache artefact of an adopted remote block.
    pub fn may_retire(&self, path: &Path) -> bool {
        if !self.scoped || self.compactor {
            return true;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        let foreign = self
            .foreign
            .read()
            .map(|keys| keys.contains(name))
            .unwrap_or(true);
        !foreign && crate::storage::block_store::store().is_locally_produced(path)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn staged(name: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::write(&path, b"block").unwrap();
        (dir, path)
    }

    #[test]
    fn everything_retires_scoped_keys_too() {
        let (_dir, path) = staged("a.parquet");
        let registry = OwnershipRegistry::everything();
        registry.adopt("a.parquet", Some("other"), "me");
        assert!(registry.may_retire(&path));
    }

    #[test]
    fn scoped_registry_keeps_foreign_keys() {
        let (_dir, path) = staged("a.parquet");
        let registry = OwnershipRegistry::scoped();
        registry.adopt("a.parquet", Some("other"), "me");
        assert!(!registry.may_retire(&path));
    }

    #[test]
    fn scoped_registry_retires_own_staged_blocks() {
        let (_own_dir, own) = staged("b.parquet");
        let registry = OwnershipRegistry::scoped();
        registry.adopt("b.parquet", Some("me"), "me");
        assert!(registry.may_retire(&own));

        registry.adopt("b.parquet", None, "me");
        assert!(
            registry.may_retire(&own),
            "legacy untagged blocks stay retirable"
        );
    }

    #[test]
    fn scoped_registry_will_not_retire_a_cache_only_path() {
        let registry = OwnershipRegistry::scoped();
        let missing = std::path::Path::new("/nonexistent/cache/m_l1_x.parquet");
        registry.adopt("m_l1_x.parquet", Some("other"), "me");
        assert!(!registry.may_retire(missing));
    }

    #[test]
    fn owning_a_key_clears_a_stale_foreign_tag() {
        let (_dir, path) = staged("c.parquet");
        let registry = OwnershipRegistry::scoped();
        registry.adopt("c.parquet", Some("other"), "me");
        assert!(!registry.may_retire(&path));
        registry.own("c.parquet");
        assert!(registry.may_retire(&path));
    }

    #[test]
    fn compactor_retires_a_foreign_key_without_local_staging() {
        let registry = OwnershipRegistry::compactor();
        assert!(registry.is_compactor());
        assert!(registry.is_scoped());
        let missing = std::path::Path::new("/nonexistent/cache/m_l0_writer.parquet");
        registry.adopt("m_l0_writer.parquet", Some("writer-0"), "compactor");
        assert!(
            registry.may_retire(missing),
            "the compactor is the single authority for the bucket and has no local copy"
        );
    }

    #[test]
    fn scoped_registry_still_refuses_a_foreign_key_without_local_staging() {
        let registry = OwnershipRegistry::scoped();
        let missing = std::path::Path::new("/nonexistent/cache/m_l0_writer.parquet");
        registry.adopt("m_l0_writer.parquet", Some("writer-0"), "compactor");
        assert!(!registry.may_retire(missing));
    }
}
