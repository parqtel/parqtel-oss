//! Metric and label-name / label-value discovery.

use super::QueryExecutor;
use parqtel_core::Scanner;
use std::collections::HashSet;

impl QueryExecutor {
    /// Returns a list of all known metric names.
    pub async fn list_metrics(&self) -> HashSet<String> {
        let mut names = self.index.read().await.all_metrics();
        // Include metrics from in-memory buffer
        for n in self.buffer.metric_names().await {
            names.insert(n);
        }
        names
    }

    /// Returns all label names across both metrics and logs.
    pub async fn list_labels(&self, _metric: Option<&str>) -> HashSet<String> {
        let mut names = self.index.read().await.all_labels();
        names.extend(self.log_index.read().await.all_labels());
        // Include labels from in-memory buffer
        for n in self.buffer.label_names().await {
            names.insert(n);
        }
        names
    }

    /// Returns all values for a given label name across both metrics and logs.
    pub async fn list_label_values(&self, label: &str) -> HashSet<String> {
        // Merge per-block label-value dictionaries from metadata
        // (flush-time index) instead of decoding blocks. Blocks whose
        // metadata lacks the field (pre-index blocks) fall back to a scan.
        // The in-memory buffer is the freshest source: on a busy cluster the
        // last-5-blocks window can miss labels whose series only live in the
        // buffer (or whose recent blocks predate the index), so buffered
        // values are merged too.
        let mut values = self.buffer.label_values(label).await;
        let mut scan_metric = Vec::new();
        let mut scan_logs = Vec::new();

        {
            let idx = self.index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(5);
            for block in &idx.blocks[start..] {
                match block.label_values.get(label) {
                    Some(vs) => values.extend(vs.iter().cloned()),
                    None if block.label_names.contains(label) => scan_metric.push(block.clone()),
                    None => {}
                }
            }
        }
        {
            let idx = self.log_index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(5);
            for block in &idx.blocks[start..] {
                match block.label_values.get(label) {
                    Some(vs) => values.extend(vs.iter().cloned()),
                    None if block.label_names.contains(label) => scan_logs.push(block.clone()),
                    None => {}
                }
            }
        }

        for block in scan_metric {
            if let Ok(points) = Scanner::scan(vec![block], "".into(), 0, i64::MAX, None).await {
                for p in points {
                    if let Some(v) = p.labels.get(label) {
                        values.insert(v.to_string());
                    }
                }
            }
        }
        for block in scan_logs {
            if let Ok(logs) = Scanner::scan_logs(vec![block], 0, i64::MAX).await {
                for l in logs {
                    if let Some(v) = l.attributes.get(label) {
                        values.insert(v.to_string());
                    }
                    if let Some(v) = l.resource_attributes.get(label) {
                        values.insert(v.to_string());
                    }
                }
            }
        }

        values
    }

    /// Recent label values, most-recent-first, bounded and prefix-filtered.
    ///
    /// Builder autocomplete path: for high-cardinality labels (thousands of
    /// distinct values) returning the full set is useless and expensive.
    /// This walks the flush-time `label_values` index of the most recent
    /// blocks (recency = block order; blocks are appended in time order)
    /// plus the in-memory buffer, newest first, applies the optional
    /// `prefix` filter server-side, and stops once `limit` values are
    /// collected. Worst case inspects the last `max_blocks` blocks —
    /// no full-history scan.
    pub async fn recent_label_values(
        &self,
        label: &str,
        prefix: Option<&str>,
        limit: usize,
        max_blocks: usize,
    ) -> Vec<String> {
        let limit = limit.clamp(1, 100);
        let max_blocks = max_blocks.clamp(1, 10);
        let mut out: Vec<String> = Vec::with_capacity(limit);
        let mut seen: HashSet<String> = HashSet::new();

        // In-memory buffer is always the freshest source.
        for v in self.buffer.recent_label_values(label, prefix, limit).await {
            if seen.insert(v.clone()) {
                out.push(v);
            }
        }
        if out.len() >= limit {
            out.truncate(limit);
            return out;
        }

        // Newest blocks first (blocks are stored in ascending time order).
        let blocks: Vec<_> = {
            let idx = self.index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(max_blocks);
            idx.blocks[start..].to_vec()
        };
        // Iterate newest → oldest so recency maps to insertion order.
        for block in blocks.iter().rev() {
            if let Some(vs) = block.label_values.get(label) {
                // BTreeSet iterates ascending; the flush-time index has no
                // recency order, but a stable order keeps results
                // deterministic across calls.
                for v in vs {
                    if prefix.is_none_or(|p| v.starts_with(p)) && seen.insert(v.clone()) {
                        out.push(v.clone());
                        if out.len() >= limit {
                            return out;
                        }
                    }
                }
            }
        }
        out
    }
}
