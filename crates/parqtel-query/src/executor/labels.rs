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
        // Values come from the indexes' shared label-value dictionaries
        // (filled at flush time) instead of decoding blocks. A label the
        // dictionary has never seen but a recent block lists (a block adopted
        // from disk carries no values) falls back to scanning those blocks.
        // The in-memory buffer is the freshest source and is merged too.
        let mut values = self.buffer.label_values(label).await;
        let mut scan_metric = Vec::new();
        let mut scan_logs = Vec::new();

        {
            let idx = self.index.read().await;
            match idx.label_values(label) {
                Some(vs) => values.extend(vs.iter().cloned()),
                None => {
                    let start = idx.blocks.len().saturating_sub(5);
                    scan_metric.extend(
                        idx.blocks[start..]
                            .iter()
                            .filter(|b| b.label_names.contains(label))
                            .cloned(),
                    );
                }
            }
        }
        {
            let idx = self.log_index.read().await;
            match idx.label_values(label) {
                Some(vs) => values.extend(vs.iter().cloned()),
                None => {
                    let start = idx.blocks.len().saturating_sub(5);
                    scan_logs.extend(
                        idx.blocks[start..]
                            .iter()
                            .filter(|b| b.label_names.contains(label))
                            .cloned(),
                    );
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

    /// Recent label values, freshest first, bounded and prefix-filtered.
    ///
    /// Builder autocomplete path: for high-cardinality labels (thousands of
    /// distinct values) returning the full set is useless and expensive.
    /// The in-memory buffer supplies the freshest values; the metrics index's
    /// label-value dictionary fills the rest in stable (ascending) order. The
    /// optional `prefix` filter is applied server-side and collection stops at
    /// `limit`. `max_blocks` is kept for API compatibility; the dictionary
    /// already covers every indexed block.
    pub async fn recent_label_values(
        &self,
        label: &str,
        prefix: Option<&str>,
        limit: usize,
        _max_blocks: usize,
    ) -> Vec<String> {
        let limit = limit.clamp(1, 100);
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

        let idx = self.index.read().await;
        if let Some(vs) = idx.label_values(label) {
            for v in vs {
                if prefix.is_none_or(|p| v.starts_with(p)) && seen.insert(v.clone()) {
                    out.push(v.clone());
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        out
    }
}
