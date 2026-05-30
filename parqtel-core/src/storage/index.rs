use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use crate::error::{Error, Result};
use crate::models::storage::BlockMetadata;

/// In-memory index of all Parquet blocks on disk.
pub struct BlockIndex {
    pub blocks: Vec<BlockMetadata>,
    sidecar_path: PathBuf,
}

impl BlockIndex {
    /// Creates a new [BlockIndex] for the given data directory.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            blocks: Vec::new(),
            sidecar_path: data_dir.join("index.json"),
        }
    }

    /// Loads the index from the JSON sidecar file.
    pub fn load(&mut self) -> Result<()> {
        if self.sidecar_path.exists() {
            let content = fs::read_to_string(&self.sidecar_path)?;
            self.blocks = serde_json::from_str(&content).map_err(Error::Serde)?;
        }
        self.blocks.sort_by_key(|b| b.start_timestamp_ns);
        Ok(())
    }

    /// Persists the index to the JSON sidecar file atomically.
    pub fn save(&self) -> Result<()> {
        let content = serde_json::to_string_pretty(&self.blocks).map_err(Error::Serde)?;
        let tmp_path = self.sidecar_path.with_extension("tmp");
        fs::write(&tmp_path, content)?;
        fs::rename(tmp_path, &self.sidecar_path)?;
        Ok(())
    }

    /// Adds a new block to the index and saves it.
    pub fn add(&mut self, meta: BlockMetadata) -> Result<()> {
        self.blocks.push(meta);
        self.blocks.sort_by_key(|b| b.start_timestamp_ns);
        self.save()
    }

    /// Removes a block from the index and saves it.
    pub fn remove(&mut self, path: &Path) -> Result<()> {
        self.blocks.retain(|b| b.path != path);
        self.save()
    }

    /// Finds blocks overlapping a time range, optionally filtered by metric name.
    pub fn query(&self, start_ns: i64, end_ns: i64, metric_name: Option<&str>) -> Vec<BlockMetadata> {
        let start_idx = self.blocks.partition_point(|b| b.end_timestamp_ns < start_ns);
        self.blocks[start_idx..].iter()
            .take_while(|b| b.start_timestamp_ns <= end_ns)
            .filter(|b| metric_name.as_ref().is_none_or(|n| b.metric_names.contains(*n)))
            .cloned()
            .collect()
    }

    pub fn total_blocks(&self) -> usize { self.blocks.len() }
    pub fn total_rows(&self) -> usize { self.blocks.iter().map(|b| b.row_count).sum() }
    pub fn total_bytes(&self) -> u64 { self.blocks.iter().map(|b| b.size_bytes).sum() }
    pub fn all_metrics(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        for b in &self.blocks {
            names.extend(b.metric_names.iter().cloned());
        }
        names
    }

    pub fn all_labels(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        for b in &self.blocks {
            names.extend(b.label_names.iter().cloned());
        }
        names
    }
}
