//! Trace queries.

use super::QueryExecutor;
use crate::pipeline_exec::Row;
use parqtel_core::{Result, Scanner};
use std::time::Instant;

impl QueryExecutor {
    /// Traces → rows: one row per span (G9b).
    pub(super) async fn fetch_trace_rows(
        &self,
        start_ns: i64,
        end_ns: i64,
        first_filter: &Option<crate::logql::Predicate>,
    ) -> Result<Vec<Row>> {
        use serde_json::json;
        let spans = self.query_traces(start_ns, end_ns, None, 10_000).await?;
        let mut rows = Vec::with_capacity(spans.len());
        for s in spans {
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("timestamp_ns".to_string(), json!(s.start_time_ns));
            fields.insert("name".to_string(), json!(s.name));
            fields.insert("trace_id".to_string(), json!(hex::encode(s.trace_id)));
            fields.insert(
                "duration_ms".to_string(),
                json!(s.duration_ns() as f64 / 1_000_000.0),
            );
            fields.insert(
                "status".to_string(),
                json!(match s.status.code {
                    2 => "ERROR",
                    1 => "OK",
                    _ => "UNSET",
                }),
            );
            fields.insert(
                "kind".to_string(),
                json!(match s.kind {
                    1 => "internal",
                    2 => "server",
                    3 => "client",
                    4 => "producer",
                    5 => "consumer",
                    _ => "unspecified",
                }),
            );
            for (k, v) in s.attributes.iter() {
                fields.insert(k.to_string(), json!(v));
            }
            if let Some(svc) = s.attributes.get("service.name") {
                fields.insert("service".to_string(), json!(svc));
            }
            let row = Row { fields };
            if let Some(pred) = first_filter {
                if !crate::pipeline_exec::row_matches_pred(&row, pred) {
                    continue;
                }
            }
            rows.push(row);
        }
        Ok(rows)
    }

    pub async fn query_traces(
        &self,
        start_ns: i64,
        end_ns: i64,
        trace_id_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<parqtel_core::Span>> {
        self.query_traces_filtered(start_ns, end_ns, trace_id_filter, limit, None)
            .await
    }

    /// Trace search with ParqtelQL span predicates pushed down INTO the
    /// scan pipeline (before the result cap), so `filter` no longer loses
    /// matches to the pre-filter span cap.
    pub async fn query_traces_filtered(
        &self,
        start_ns: i64,
        end_ns: i64,
        trace_id_filter: Option<&str>,
        limit: usize,
        filter: Option<&crate::logql::SearchQuery>,
    ) -> Result<Vec<parqtel_core::Span>> {
        Ok(self
            .query_traces_filtered_counted(start_ns, end_ns, trace_id_filter, limit, filter)
            .await?
            .spans)
    }

    /// [query_traces_filtered] with volume stats: the true span total for
    /// the range (exact, from block metadata — no Parquet decode), the
    /// bounded-scan match count, and whether the result was truncated.
    pub async fn query_traces_filtered_counted(
        &self,
        start_ns: i64,
        end_ns: i64,
        trace_id_filter: Option<&str>,
        limit: usize,
        filter: Option<&crate::logql::SearchQuery>,
    ) -> Result<crate::models::TraceQueryResult> {
        let start_time = Instant::now();
        // Cap blocks scanned to bound I/O — most recent blocks first for
        // relevance. 256 covers a week of hourly blocks or a day of
        // 5-minute log-trace flush intervals (raised from 64 after the
        // multi-block baseline showed coverage loss at scale).
        const MAX_BLOCKS: usize = 256;
        let mut blocks = {
            let idx = self.trace_index.read().await;
            idx.query(start_ns, end_ns, None)
        };
        // Exact volume from block metadata — the row_count of every block
        // overlapping the range. Never requires decoding a single span.
        let total_spans_in_range: usize = blocks.iter().map(|b| b.row_count).sum();

        // Scan more when predicates apply (they reduce the result, so a
        // larger scan keeps filtered result sets meaningful).
        let scan_cap = if filter.is_some() { 10_000 } else { 200 };
        let scan_cap = scan_cap.max(limit);
        // Reverse so newest blocks are scanned first (more useful for debugging)
        blocks.reverse();
        blocks.truncate(MAX_BLOCKS);
        let mut spans = if blocks.is_empty() {
            Vec::new()
        } else {
            Scanner::scan_traces(blocks, start_ns, end_ns, scan_cap).await?
        };

        // Merge in spans still buffered in memory (not yet flushed to a block).
        // Buffered spans cannot overlap flushed blocks: the buffer is drained
        // whenever a flush completes, so a plain extend is safe.
        spans.extend(self.buffer.scan_spans(start_ns, end_ns).await);

        // Sort newest-first for deterministic, relevance-ordered results.
        spans.sort_unstable_by_key(|s| std::cmp::Reverse(s.start_time_ns));

        // Filter by trace_id if provided
        if let Some(tid) = trace_id_filter {
            let tid_lower = tid.to_lowercase();
            spans.retain(|s| hex::encode(s.trace_id) == tid_lower);
        }

        // ParqtelQL span predicates (push-down, before the limit). Prepared
        // once so the clause regexes are not compiled per span.
        if let Some(sq) = filter {
            let prepared = crate::logql::PreparedSpanQuery::new(sq);
            spans.retain(|s| prepared.matches(s));
        }

        let spans_matched = spans.len();
        let truncated = spans.len() > limit;
        spans.truncate(limit);

        Ok(crate::models::TraceQueryResult {
            spans,
            execution_time: start_time.elapsed(),
            total_spans_in_range,
            spans_matched,
            truncated,
        })
    }
}
