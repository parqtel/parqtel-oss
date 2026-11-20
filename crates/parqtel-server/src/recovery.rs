//! Startup recovery: opening the write-ahead logs, repairing the block indexes and
//! replaying acknowledged-but-unflushed telemetry.

use parqtel_core::{BlockIndex, BlockIndexStore, Config};
use parqtel_ingest::{IngestionService, LogIngestionService, TraceIngestionService};

/// Opens a signal's WAL, or returns `None` when the signal has it disabled.
///
/// A WAL that cannot be opened is fatal: continuing would mean accepting data
/// we cannot promise to recover, which is exactly the guarantee the WAL exists
/// to provide.
pub(crate) fn open_wal(
    config: &Config,
    signal: &str,
    data_dir: &std::path::Path,
    enabled: bool,
) -> anyhow::Result<Option<parqtel_core::wal::WalWriter>> {
    if !enabled {
        tracing::warn!(
            signal,
            "WAL is disabled for this signal; a crash can lose up to a whole block window"
        );
        return Ok(None);
    }
    let wal = parqtel_core::wal::WalWriter::open_with_segment_limit(
        data_dir,
        signal,
        config.ingest.wal_sync_mode,
        std::time::Duration::from_millis(config.ingest.wal_sync_interval_ms.max(1)),
        config.ingest.wal_max_segment_bytes,
    )?;
    Ok(Some(wal))
}

/// Reconciles a block index against its data directory and persists the repair.
///
/// No-op in the normal case, where the sidecar already matches the disk: one
/// `read_dir`, and the sidecar is only rewritten when something actually
/// changed.
pub(crate) fn reconcile_and_persist(
    index: &mut BlockIndex,
    store: &BlockIndexStore,
    signal: parqtel_core::SignalType,
    label: &str,
) {
    match parqtel_core::storage::reconcile(index, signal) {
        Ok(stats) if stats.adopted > 0 || stats.dropped > 0 => {
            tracing::warn!(
                signal = label,
                adopted = stats.adopted,
                dropped = stats.dropped,
                unreadable = stats.unreadable,
                "block index did not match the data directory; reconciled from the                  block files. Metric names may be a range until the block is                  rewritten by compaction, and recovered blocks have no label                  dictionary until then."
            );
            if let Err(e) = parqtel_core::storage::persist_blocking(index, store) {
                tracing::warn!(signal = label, error = %e, "failed to persist the reconciled index");
            }
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(signal = label, error = %e, "block index reconcile failed"),
    }
}

/// What a WAL replay did, for the startup log and `/api/v1/stats`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WalReplayOutcome {
    pub records: u64,
    pub skipped: u64,
    pub truncated_bytes: u64,
    pub elapsed: std::time::Duration,
}

/// Shared tail of a per-signal replay: log the outcome and stamp the elapsed
/// time.
fn finish_replay(
    signal: &'static str,
    started: std::time::Instant,
    records: u64,
    skipped: u64,
    truncated_bytes: u64,
) -> WalReplayOutcome {
    if records > 0 {
        tracing::warn!(
            signal,
            records,
            "recovered telemetry from the write-ahead log"
        );
    }
    WalReplayOutcome {
        records,
        skipped,
        truncated_bytes,
        elapsed: started.elapsed(),
    }
}

/// Points a WAL record's label sets at shared instances.
///
/// Deserialising builds a fresh set for every point, so a replayed backlog held
/// one copy of the same labels per sample, and each carried its own cached JSON.
/// The live OTLP path already interns; this gives replay the same sharing.
fn intern_set(labels: &parqtel_core::LabelSet) -> parqtel_core::LabelSet {
    let interner = parqtel_core::LabelInterner::global();
    let pairs: Vec<(&str, &str)> = labels.iter().collect();
    interner.intern(&pairs).unwrap_or_else(|_| labels.clone())
}

fn intern_labels(metric: &mut parqtel_core::Metric) {
    metric.resource_attributes = intern_set(&metric.resource_attributes);
    for dp in &mut metric.data_points {
        dp.labels = intern_set(&dp.labels);
    }
}

/// Replays the metrics WAL segment by segment, ingesting each segment before
/// moving to the next, so recovery flushes as it recovers instead of buffering
/// a whole-signal backlog before ingesting.
async fn replay_metrics(
    data_dir: &std::path::Path,
    metrics: &IngestionService,
) -> anyhow::Result<(u64, u64, u64)> {
    let dir = parqtel_core::wal::signal_dir(data_dir, "metrics");
    if !dir.exists() {
        return Ok((0, 0, 0));
    }
    let commit = parqtel_core::wal::read_commit(data_dir, "metrics");
    let segments = parqtel_core::wal::segment_lengths(&dir)?;
    let mut pending = Vec::new();
    let mut records = 0u64;
    let mut skipped = 0u64;
    let mut truncated = 0u64;
    for (seq, len) in segments {
        let stats = parqtel_core::wal::replay_segment::<serde_json::Value, _>(
            &dir,
            seq,
            commit,
            len,
            &mut |v: serde_json::Value| pending.push(v),
        )?;
        records += stats.records;
        skipped += stats.skipped;
        truncated += stats.truncated_bytes;
        for v in pending.drain(..) {
            match serde_json::from_value::<parqtel_core::Metric>(v) {
                Ok(mut m) => {
                    intern_labels(&mut m);
                    // One bad record must not strand the rest of the WAL.
                    if let Err(e) = metrics.ingest_metrics(vec![m]).await {
                        tracing::error!(signal = "metrics", "WAL replay: re-ingest failed: {e}");
                    }
                }
                Err(e) => tracing::error!(signal = "metrics", "WAL replay: bad record: {e}"),
            }
        }
    }
    Ok((records, skipped, truncated))
}

/// Replays the logs WAL segment by segment, ingesting each segment before
/// moving to the next.
async fn replay_logs(
    data_dir: &std::path::Path,
    logs: &LogIngestionService,
) -> anyhow::Result<(u64, u64, u64)> {
    let dir = parqtel_core::wal::signal_dir(data_dir, "logs");
    if !dir.exists() {
        return Ok((0, 0, 0));
    }
    let commit = parqtel_core::wal::read_commit(data_dir, "logs");
    let segments = parqtel_core::wal::segment_lengths(&dir)?;
    let mut pending = Vec::new();
    let mut records = 0u64;
    let mut skipped = 0u64;
    let mut truncated = 0u64;
    for (seq, len) in segments {
        let stats = parqtel_core::wal::replay_segment::<serde_json::Value, _>(
            &dir,
            seq,
            commit,
            len,
            &mut |v: serde_json::Value| pending.push(v),
        )?;
        records += stats.records;
        skipped += stats.skipped;
        truncated += stats.truncated_bytes;
        for v in pending.drain(..) {
            match serde_json::from_value::<parqtel_core::LogRecord>(v) {
                Ok(mut l) => {
                    l.attributes = intern_set(&l.attributes);
                    l.resource_attributes = intern_set(&l.resource_attributes);
                    if let Err(e) = logs.ingest_logs(vec![l]).await {
                        tracing::error!(signal = "logs", "WAL replay: re-ingest failed: {e}");
                    }
                }
                Err(e) => tracing::error!(signal = "logs", "WAL replay: bad record: {e}"),
            }
        }
    }
    Ok((records, skipped, truncated))
}

/// Replays the traces WAL segment by segment, ingesting each segment before
/// moving to the next.
async fn replay_traces(
    data_dir: &std::path::Path,
    traces: &TraceIngestionService,
) -> anyhow::Result<(u64, u64, u64)> {
    // Trace blocks share the metrics data dir (TraceWriter is built from
    // config.storage), so the trace WAL lives there too.
    let dir = parqtel_core::wal::signal_dir(data_dir, "traces");
    if !dir.exists() {
        return Ok((0, 0, 0));
    }
    let commit = parqtel_core::wal::read_commit(data_dir, "traces");
    let segments = parqtel_core::wal::segment_lengths(&dir)?;
    let mut pending = Vec::new();
    let mut records = 0u64;
    let mut skipped = 0u64;
    let mut truncated = 0u64;
    for (seq, len) in segments {
        let stats = parqtel_core::wal::replay_segment::<serde_json::Value, _>(
            &dir,
            seq,
            commit,
            len,
            &mut |v: serde_json::Value| pending.push(v),
        )?;
        records += stats.records;
        skipped += stats.skipped;
        truncated += stats.truncated_bytes;
        for v in pending.drain(..) {
            match serde_json::from_value::<parqtel_core::Span>(v) {
                Ok(mut s) => {
                    s.attributes = intern_set(&s.attributes);
                    for event in &mut s.events {
                        event.attributes = intern_set(&event.attributes);
                    }
                    for link in &mut s.links {
                        link.attributes = intern_set(&link.attributes);
                    }
                    if let Err(e) = traces.ingest_spans(vec![s]).await {
                        tracing::error!(signal = "traces", "WAL replay: re-ingest failed: {e}");
                    }
                }
                Err(e) => tracing::error!(signal = "traces", "WAL replay: bad record: {e}"),
            }
        }
    }
    Ok((records, skipped, truncated))
}

/// Replays every enabled signal's WAL.
///
/// Runs before the listener binds, so a restart never serves queries over a
/// partially-recovered store, and through exactly the entry points live
/// exporters use — so recovered telemetry is written into a fresh block as if
/// it had arrived live, which is also what advances the commit point and lets
/// the WAL be trimmed.
pub(crate) async fn replay_all_wals(
    config: &Config,
    metrics: &IngestionService,
    logs: &LogIngestionService,
    traces: &TraceIngestionService,
) -> Vec<(&'static str, WalReplayOutcome)> {
    let mut out = Vec::new();

    if config.ingest.wal_enabled {
        let started = std::time::Instant::now();
        match replay_metrics(&config.storage.data_dir, metrics).await {
            Ok((records, skipped, truncated)) => out.push((
                "metrics",
                finish_replay("metrics", started, records, skipped, truncated),
            )),
            Err(e) => tracing::error!(signal = "metrics", "WAL replay failed: {e}"),
        }
    }

    if config.ingest.log_wal_enabled {
        let started = std::time::Instant::now();
        match replay_logs(&config.logs.data_dir, logs).await {
            Ok((records, skipped, truncated)) => out.push((
                "logs",
                finish_replay("logs", started, records, skipped, truncated),
            )),
            Err(e) => tracing::error!(signal = "logs", "WAL replay failed: {e}"),
        }
    }

    if config.ingest.wal_enabled {
        let started = std::time::Instant::now();
        // Trace blocks share the metrics data dir (TraceWriter is built from
        // config.storage), so the trace WAL lives there too.
        match replay_traces(&config.storage.data_dir, traces).await {
            Ok((records, skipped, truncated)) => out.push((
                "traces",
                finish_replay("traces", started, records, skipped, truncated),
            )),
            Err(e) => tracing::error!(signal = "traces", "WAL replay failed: {e}"),
        }
    }

    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use parqtel_core::{
        BlockConfig, DataPoint, LabelSet, LogBlockConfig, LogRecord, Metric, MetricValue, Span,
        SpanEvent, SpanLink, SpanStatus,
    };
    use std::time::Duration;

    fn labels(value: &str) -> LabelSet {
        LabelSet::try_from_iter([("attribute", value)]).unwrap()
    }

    fn write_wal<T: serde::Serialize>(data_dir: &std::path::Path, signal: &str, records: &[T]) {
        let mut wal = parqtel_core::wal::WalWriter::open(
            data_dir,
            signal,
            parqtel_core::wal::WalSyncMode::None,
            Duration::ZERO,
        )
        .unwrap();
        for record in records {
            wal.append(record).unwrap();
        }
    }

    fn test_log(
        timestamp_ns: i64,
        attributes: LabelSet,
        resource_attributes: LabelSet,
    ) -> LogRecord {
        LogRecord::new(
            timestamp_ns,
            timestamp_ns + 1,
            9,
            "INFO".into(),
            format!("log-{timestamp_ns}"),
            attributes,
            resource_attributes,
            [0; 16],
            [0; 8],
            0,
            "recovery-test".into(),
            "1".into(),
        )
    }

    fn test_span(
        start_time_ns: i64,
        attributes: LabelSet,
        event_attributes: LabelSet,
        link_attributes: LabelSet,
    ) -> Span {
        Span::new(
            [0; 16],
            [0; 8],
            String::new(),
            format!("span-{start_time_ns}"),
            1,
            start_time_ns,
            start_time_ns + 10,
            attributes,
            vec![SpanEvent {
                time_ns: start_time_ns + 1,
                name: "event".into(),
                attributes: event_attributes,
            }],
            vec![SpanLink {
                trace_id: [1; 16],
                span_id: [1; 8],
                attributes: link_attributes,
            }],
            SpanStatus {
                code: 0,
                message: String::new(),
            },
            [0; 8],
            0,
        )
    }

    #[test]
    fn replayed_points_with_equal_labels_share_one_set() {
        let point = |ts| DataPoint {
            timestamp_ns: ts,
            value: MetricValue::Double(1.0),
            labels: LabelSet::try_from_iter([("pod", "recovery-test-pod")]).unwrap(),
        };
        let mut m = Metric {
            name: "m".into(),
            data_points: vec![point(1), point(2)],
            ..Default::default()
        };
        assert_ne!(
            m.data_points[0].labels.addr(),
            m.data_points[1].labels.addr()
        );
        intern_labels(&mut m);
        assert_eq!(
            m.data_points[0].labels.addr(),
            m.data_points[1].labels.addr()
        );
        assert_eq!(
            m.data_points[0].labels.get("pod"),
            Some("recovery-test-pod")
        );
    }

    #[tokio::test]
    async fn replayed_logs_preserve_records_and_intern_attribute_sets() {
        let dir = tempfile::tempdir().unwrap();
        let attributes = labels("shared-log-attributes");
        let resource_attributes = labels("shared-log-resource");
        let originals = vec![
            test_log(10, attributes.clone(), resource_attributes.clone()),
            test_log(20, attributes.clone(), resource_attributes.clone()),
            test_log(
                30,
                labels("distinct-log-attributes"),
                resource_attributes.clone(),
            ),
        ];
        write_wal(dir.path(), "logs", &originals);

        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let buffer = parqtel_core::MemoryBuffer::new();
        let service = LogIngestionService::new(
            LogBlockConfig {
                data_dir: dir.path().join("log-blocks"),
                ..Default::default()
            },
            tx,
        )
        .with_memory_buffer(buffer.clone());

        assert_eq!(replay_logs(dir.path(), &service).await.unwrap(), (3, 0, 0));
        let replayed = buffer.scan_logs(0, 100).await;
        assert_eq!(replayed, originals);
        assert!(replayed[0].attributes.ptr_eq(&replayed[1].attributes));
        assert!(!replayed[0].attributes.ptr_eq(&replayed[2].attributes));
        assert!(replayed[0]
            .resource_attributes
            .ptr_eq(&replayed[1].resource_attributes));
    }

    #[tokio::test]
    async fn replayed_spans_preserve_records_and_intern_all_attribute_sets() {
        let dir = tempfile::tempdir().unwrap();
        let shared = labels("shared-span-attributes");
        let event_attributes = labels("shared-event-attributes");
        let link_attributes = labels("shared-link-attributes");
        let originals = vec![
            test_span(
                10,
                shared.clone(),
                event_attributes.clone(),
                link_attributes.clone(),
            ),
            test_span(
                30,
                shared.clone(),
                event_attributes.clone(),
                link_attributes.clone(),
            ),
            test_span(
                50,
                labels("distinct-span-attributes"),
                event_attributes.clone(),
                link_attributes.clone(),
            ),
        ];
        write_wal(dir.path(), "traces", &originals);

        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let buffer = parqtel_core::MemoryBuffer::new();
        let service = TraceIngestionService::new(
            BlockConfig {
                data_dir: dir.path().join("trace-blocks"),
                ..Default::default()
            },
            tx,
        )
        .with_memory_buffer(buffer.clone());

        assert_eq!(
            replay_traces(dir.path(), &service).await.unwrap(),
            (3, 0, 0)
        );
        let replayed = buffer.scan_spans(0, 100).await;
        assert_eq!(replayed, originals);
        assert!(replayed[0].attributes.ptr_eq(&replayed[1].attributes));
        assert!(!replayed[0].attributes.ptr_eq(&replayed[2].attributes));
        assert!(replayed[0].events[0]
            .attributes
            .ptr_eq(&replayed[1].events[0].attributes));
        assert!(replayed[0].links[0]
            .attributes
            .ptr_eq(&replayed[1].links[0].attributes));
    }
}
