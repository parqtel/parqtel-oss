//! Log queries: filtered listing, ParqtelQL search and field discovery.

use super::QueryExecutor;
use crate::matcher::evaluate_matchers;
use crate::models::LogQueryResult;
use parqtel_core::{Result, Scanner};
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

impl QueryExecutor {
    /// Executes a log query and returns matching [LogRecord]s.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_logs(
        &self,
        start_ns: i64,
        end_ns: i64,
        matchers: Vec<crate::matcher::LabelMatcher>,
        limit: usize,
        order_desc: bool,
        severity_min: Option<i32>,
        search: Option<String>,
    ) -> Result<LogQueryResult> {
        let start_time = Instant::now();
        let blocks = {
            let idx = self.log_index.read().await;
            idx.query(start_ns, end_ns, None)
        };

        // Build the row predicate once: severity → body search → matchers.
        // It is pushed down INTO the block scan (runs inside the blocking
        // task), so only matching rows materialize — bounded by `limit` —
        // while the scan still counts every match and buckets volume.
        // Previously every row of every block in range was decoded into
        // memory before filtering: the primary RSS driver under the UI's
        // 10s auto-refresh.
        let make_filter =
            |matchers: Vec<crate::matcher::LabelMatcher>,
             severity_min: Option<i32>,
             search: Option<String>|
             -> std::sync::Arc<dyn Fn(&parqtel_core::LogRecord) -> bool + Send + Sync> {
                // The needle is prepared once, not per row.
                let needle = search
                    .as_ref()
                    .map(|p| parqtel_core::text::CiNeedle::new(&p.to_ascii_lowercase()));
                std::sync::Arc::new(move |log| {
                    if let Some(min) = severity_min {
                        if log.severity_number < min {
                            return false;
                        }
                    }
                    if let Some(ref needle) = needle {
                        if !needle.is_in(&log.body) {
                            return false;
                        }
                    }
                    if matchers.is_empty() {
                        return true;
                    }
                    let all_labels = log.attributes.merge(&log.resource_attributes);
                    evaluate_matchers(&matchers, &all_labels, "")
                })
            };

        // Unflushed logs: only the rows that match are copied out of the buffer.
        let buffered_logs = {
            let keep = make_filter(matchers.clone(), severity_min, search.clone());
            self.buffer
                .scan_logs_filtered(start_ns, end_ns, |l| keep(l))
                .await
        };
        if blocks.is_empty() && buffered_logs.is_empty() {
            return Ok(LogQueryResult {
                logs: Vec::new(),
                execution_time: start_time.elapsed(),
                total_logs_count: 0,
                volume_summary: vec![0; 60],
            });
        }

        // Severity and body search are decidable from two columns, so the scan
        // rejects on them before decoding a row. Folding the needle to ASCII
        // lowercase makes the scan's test the same comparison as `contains_ci`.
        let prefilter = parqtel_core::storage::LogColumnPrefilter {
            min_severity: severity_min,
            body_contains_lower: search
                .as_ref()
                .map(|s| vec![s.to_ascii_lowercase()])
                .unwrap_or_default(),
            // Equalities narrow which granules are read; the matcher still
            // judges every row that is.
            attr_equals: matchers
                .iter()
                .filter(|m| {
                    m.op == crate::matcher::MatchOp::Equal
                        && !m.value.is_empty()
                        && m.name != "__name__"
                })
                .map(|m| (m.name.clone(), m.value.clone()))
                .collect(),
            // With no label matchers the row predicate is exactly the two
            // column conditions.
            exact: matchers.is_empty(),
        };

        let window_ns = (end_ns - start_ns) / 60;
        let mut total_logs_count = 0usize;
        let mut volume_summary = vec![0u64; 60];

        // Filter the buffered (unflushed) rows in place with the same
        // predicate — small, and never part of the disk scan.
        let mut buffered_matching = buffered_logs;
        if !buffered_matching.is_empty() {
            let filter = make_filter(matchers.clone(), severity_min, search.clone());
            let mut buckets = vec![0u64; 60];
            let mut kept = Vec::with_capacity(buffered_matching.len());
            for log in buffered_matching {
                if filter(&log) {
                    if window_ns > 0 {
                        let bucket =
                            ((log.timestamp_ns - start_ns) / window_ns).clamp(0, 59) as usize;
                        buckets[bucket] += 1;
                    }
                    kept.push(log);
                }
            }
            total_logs_count += kept.len();
            for (acc, b) in volume_summary.iter_mut().zip(buckets) {
                *acc += b;
            }
            buffered_matching = kept;
        }

        let mut filtered = if blocks.is_empty() {
            buffered_matching
        } else {
            let filter = make_filter(matchers, severity_min, search);
            // keep_newest follows the ordering contract: `desc` (the UI's
            // logs view) returns the newest `limit` matches — Loki-style
            // "last N logs" — while `asc` keeps the oldest.
            let (disk_logs, stats) = Scanner::scan_logs_counted_prefiltered(
                blocks,
                start_ns,
                end_ns,
                limit,
                order_desc,
                Some(filter),
                Some(prefilter),
            )
            .await?;
            total_logs_count += stats.total_matched;
            for (acc, b) in volume_summary.iter_mut().zip(stats.volume_buckets) {
                *acc += b;
            }
            let mut logs = disk_logs;
            logs.extend(buffered_matching);
            logs
        };

        // Apply ordering to the (much smaller) merged set.
        if order_desc {
            filtered.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ns));
        } else {
            filtered.sort_by_key(|a| a.timestamp_ns);
        }
        filtered.truncate(limit);

        Ok(LogQueryResult {
            logs: filtered,
            execution_time: start_time.elapsed(),
            total_logs_count,
            volume_summary,
        })
    }

    /// Executes a trace query and returns matching spans.
    /// At petabyte scale, caps the number of blocks scanned and pushes limit down.
    /// ParqtelQL search over logs: reuses the scan/windowing of query_logs
    /// and applies the parsed SearchQuery clauses + terms post-scan.
    /// ParqtelQL search with a boolean predicate tree (OR support).
    /// AND-only shapes use the flat SearchQuery path via search_logs.
    pub async fn search_logs_predicate(
        &self,
        start_ns: i64,
        end_ns: i64,
        pred: &crate::logql::Predicate,
        limit: usize,
        order_desc: bool,
    ) -> Result<LogQueryResult> {
        let start_time = Instant::now();
        let blocks = {
            let idx = self.log_index.read().await;
            idx.query(start_ns, end_ns, None)
        };

        let window_ns = (end_ns - start_ns) / 60;
        let extra = std::collections::HashMap::new();
        // Predicate pushed down into the scan (see query_logs): only
        // matching rows materialize, bounded by `limit`; totals and volume
        // buckets still cover every match.
        // Prepared once, then applied to every row: compiling the query's
        // regexes and lowercasing its needles inside the row loop cost a
        // regex compilation per clause per row.
        let prepared_pred = crate::logql::PreparedPredicate::new(pred);
        let prepared_pred = std::sync::Arc::new(prepared_pred);
        let pred_filter = {
            let prepared_pred = prepared_pred.clone();
            let extra = extra.clone();
            std::sync::Arc::new(move |log: &parqtel_core::LogRecord| {
                prepared_pred.matches(log, &extra)
            }) as std::sync::Arc<dyn Fn(&parqtel_core::LogRecord) -> bool + Send + Sync>
        };
        let apply_pred = {
            let prepared_pred = prepared_pred.clone();
            let extra = extra.clone();
            move |log: &parqtel_core::LogRecord| prepared_pred.matches(log, &extra)
        };

        let buffered_logs = self
            .buffer
            .scan_logs_filtered(start_ns, end_ns, &apply_pred)
            .await;
        let mut total_logs_count = 0usize;
        let mut volume_summary = vec![0u64; 60];
        let mut buffered_matching: Vec<parqtel_core::LogRecord> = Vec::new();
        if !buffered_logs.is_empty() {
            for log in buffered_logs {
                if apply_pred(&log) {
                    if window_ns > 0 {
                        let bucket =
                            ((log.timestamp_ns - start_ns) / window_ns).clamp(0, 59) as usize;
                        volume_summary[bucket] += 1;
                    }
                    total_logs_count += 1;
                    buffered_matching.push(log);
                }
            }
        }

        let mut filtered = if blocks.is_empty() {
            buffered_matching
        } else {
            let (disk, stats) = Scanner::scan_logs_counted_prefiltered(
                blocks,
                start_ns,
                end_ns,
                limit,
                order_desc,
                Some(pred_filter),
                Some(prepared_pred.column_prefilter_without_extra()),
            )
            .await?;
            total_logs_count += stats.total_matched;
            for (acc, b) in volume_summary.iter_mut().zip(stats.volume_buckets) {
                *acc += b;
            }
            let mut all = disk;
            all.extend(buffered_matching);
            all
        };
        if order_desc {
            filtered.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ns));
        } else {
            filtered.sort_by_key(|a| a.timestamp_ns);
        }
        filtered.truncate(limit);
        Ok(LogQueryResult {
            logs: filtered,
            execution_time: start_time.elapsed(),
            total_logs_count,
            volume_summary,
        })
    }

    pub async fn search_logs(
        &self,
        start_ns: i64,
        end_ns: i64,
        search: &crate::logql::SearchQuery,
        limit: usize,
        order_desc: bool,
    ) -> Result<LogQueryResult> {
        let start_time = Instant::now();
        let blocks = {
            let idx = self.log_index.read().await;
            idx.query(start_ns, end_ns, None)
        };

        let window_ns = (end_ns - start_ns) / 60;
        let extra = std::collections::HashMap::new();
        // Search pushed down into the scan (see query_logs).
        // Prepared once for the same reason as the predicate path above.
        let prepared_search = std::sync::Arc::new(crate::logql::PreparedLogQuery::new(search));
        let search_filter = {
            let prepared_search = prepared_search.clone();
            let extra = extra.clone();
            std::sync::Arc::new(move |log: &parqtel_core::LogRecord| {
                prepared_search.matches(log, &extra)
            }) as std::sync::Arc<dyn Fn(&parqtel_core::LogRecord) -> bool + Send + Sync>
        };
        let apply_search = {
            let prepared_search = prepared_search.clone();
            let extra = extra.clone();
            move |log: &parqtel_core::LogRecord| prepared_search.matches(log, &extra)
        };

        let buffered_logs = self
            .buffer
            .scan_logs_filtered(start_ns, end_ns, &apply_search)
            .await;
        if blocks.is_empty() && buffered_logs.is_empty() {
            return Ok(LogQueryResult {
                logs: Vec::new(),
                execution_time: start_time.elapsed(),
                total_logs_count: 0,
                volume_summary: vec![0; 60],
            });
        }

        let mut total_logs_count = 0usize;
        let mut volume_summary = vec![0u64; 60];
        let mut buffered_matching: Vec<parqtel_core::LogRecord> = Vec::new();
        if !buffered_logs.is_empty() {
            for log in buffered_logs {
                if apply_search(&log) {
                    if window_ns > 0 {
                        let bucket =
                            ((log.timestamp_ns - start_ns) / window_ns).clamp(0, 59) as usize;
                        volume_summary[bucket] += 1;
                    }
                    total_logs_count += 1;
                    buffered_matching.push(log);
                }
            }
        }

        let mut filtered = if blocks.is_empty() {
            buffered_matching
        } else {
            let (disk, stats) = Scanner::scan_logs_counted_prefiltered(
                blocks,
                start_ns,
                end_ns,
                limit,
                order_desc,
                Some(search_filter),
                Some(prepared_search.column_prefilter_without_extra()),
            )
            .await?;
            total_logs_count += stats.total_matched;
            for (acc, b) in volume_summary.iter_mut().zip(stats.volume_buckets) {
                *acc += b;
            }
            let mut all = disk;
            all.extend(buffered_matching);
            all
        };

        if order_desc {
            filtered.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ns));
        } else {
            filtered.sort_by_key(|a| a.timestamp_ns);
        }
        filtered.truncate(limit);

        Ok(LogQueryResult {
            logs: filtered,
            execution_time: start_time.elapsed(),
            total_logs_count,
            volume_summary,
        })
    }

    /// Returns all known field names across the log schema.
    pub async fn get_log_fields(&self) -> (Vec<String>, Vec<String>) {
        let dedicated = vec![
            "service_name".into(),
            "service_version".into(),
            "k8s_namespace".into(),
            "k8s_pod_name".into(),
            "k8s_container_name".into(),
            "k8s_node_name".into(),
            "severity_text".into(),
            "body".into(),
        ];

        // Sample common attributes from recent blocks
        let mut common = HashSet::new();
        let blocks = {
            let idx = self.log_index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(3);
            idx.blocks[start..].to_vec()
        };

        for b in blocks {
            for name in b.label_names {
                if !dedicated.contains(&name) {
                    common.insert(name);
                }
            }
        }

        (dedicated, common.into_iter().collect())
    }

    /// Returns distinct values for a given log field, sorted by frequency.
    pub async fn get_log_field_values(&self, field: &str, limit: usize) -> Vec<String> {
        // Label-value index: blocks flushed since the index landed carry
        // distinct-value dictionaries in metadata — merge those instead of
        // decoding whole blocks. Old blocks (no entry for the field) fall
        // back to a scan, still bounded to the most recent 5.
        let (blocks, indexed) = {
            let idx = self.log_index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(5);
            let recent = idx.blocks[start..].to_vec();
            // Use indexed metadata for every recent block that has an
            // entry for this field; scan only those that do not.
            let mut freq_inner = BTreeMap::new();
            let mut needs_scan = Vec::new();
            let mut all_indexed = true;
            for b in recent {
                match b.label_values.get(field) {
                    Some(values) => {
                        for v in values {
                            *freq_inner.entry(v.clone()).or_insert(0u64) += 1;
                        }
                    }
                    None => {
                        all_indexed = false;
                        needs_scan.push(b);
                    }
                }
            }
            if all_indexed {
                (Vec::new(), freq_inner)
            } else {
                // Partial coverage: fall back to the full scan (correct
                // counts) — index-only when coverage is complete.
                (needs_scan, freq_inner)
            }
        };

        if blocks.is_empty() && !indexed.is_empty() {
            let mut entries: Vec<_> = indexed.into_iter().collect();
            entries.sort_by_key(|b| std::cmp::Reverse(b.1));
            return entries.into_iter().take(limit).map(|(v, _)| v).collect();
        }

        // Fallback scan path (pre-index blocks).
        let mut freq = indexed;
        let blocks = if blocks.is_empty() {
            let idx = self.log_index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(5);
            idx.blocks[start..].to_vec()
        } else {
            blocks
        };
        for block in blocks {
            if let Ok(logs) = Scanner::scan_logs(vec![block], 0, i64::MAX).await {
                for l in logs {
                    let val = if let Some(v) = l.attributes.get(field) {
                        Some(v)
                    } else {
                        l.resource_attributes.get(field)
                    };

                    if let Some(v) = val {
                        *freq.entry(v.to_string()).or_insert(0u64) += 1;
                    }
                }
            }
        }

        let mut entries: Vec<_> = freq.into_iter().collect();
        entries.sort_by_key(|b| std::cmp::Reverse(b.1));
        entries.into_iter().take(limit).map(|(v, _)| v).collect()
    }
}
