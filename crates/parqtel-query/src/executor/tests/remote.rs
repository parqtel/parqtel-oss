//! Reader-tier buffer fan-out.
//!
//! A reader must see the seconds the writers have not flushed yet, and must
//! ask for exactly the rows it cannot already see. These tests pin both halves:
//! the watermark that goes into the request and the merge into the result.

use super::*;
use crate::remote::{
    RemoteBuffer, RemoteBufferRequest, RemoteBufferRows, RemoteFuture, RemoteSignal,
};
use parqtel_core::models::storage::{BlockMetadata, SignalType};
use parqtel_core::storage::BlockIndex;
use std::sync::Mutex;

struct FakeRemote {
    requests: Mutex<Vec<RemoteBufferRequest>>,
    points: Vec<(String, parqtel_core::DataPoint)>,
    logs: Vec<parqtel_core::LogRecord>,
}

impl FakeRemote {
    fn new(points: Vec<(String, parqtel_core::DataPoint)>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            points,
            logs: Vec::new(),
        }
    }
}

impl RemoteBuffer for FakeRemote {
    fn fetch(&self, request: RemoteBufferRequest) -> RemoteFuture<'_> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request);
            Ok(RemoteBufferRows {
                points: self.points.clone(),
                logs: self.logs.clone(),
                ..Default::default()
            })
        })
    }
}

fn labels(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (*k, *v))).unwrap()
}

fn point(timestamp_ns: i64, value: f64) -> parqtel_core::DataPoint {
    parqtel_core::DataPoint {
        timestamp_ns,
        value: MetricValue::Double(value),
        labels: labels(&[("host", "r1")]),
    }
}

fn reader(remote: Arc<FakeRemote>) -> (QueryExecutor, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
    let log_index = Arc::new(RwLock::new(BlockIndex::new(&dir.path().join("logs"))));
    let exec =
        QueryExecutor::new(index, log_index, dir.path().join("traces")).with_remote_buffer(remote);
    (exec, dir)
}

/// The plan path must return the remote samples too, and only once per
/// timestamp when a writer and the local buffer both hold one.
#[tokio::test]
async fn remote_points_are_merged_into_the_plan_result() {
    let remote = Arc::new(FakeRemote::new(vec![
        ("cpu".to_string(), point(2000, 2.0)),
        ("cpu".to_string(), point(3000, 3.0)),
    ]));
    let (exec, _dir) = reader(remote.clone());
    // A local buffered row at the same timestamp must not duplicate: the
    // writers own disjoint rows, but a retried export may land one twice.
    exec.memory_buffer()
        .push_metrics("cpu", &[point(2000, 2.0)])
        .await;

    let plan = QueryPlan::new("cpu".into(), vec![], 0, 4000, None, 10, 100, None, None).unwrap();
    let result = exec.execute(plan).await.unwrap();

    let samples: Vec<i64> = result
        .series
        .iter()
        .flat_map(|series| series.samples.iter().map(|sample| sample.timestamp_ns))
        .collect();
    assert_eq!(
        samples,
        vec![2000, 3000],
        "remote rows must be in the result"
    );
}

/// The request asks only for rows newer than the newest block the reader can
/// already see; without that every query would re-transfer the whole buffer.
#[tokio::test]
async fn the_request_carries_the_newest_block_timestamp() {
    let remote = Arc::new(FakeRemote::new(Vec::new()));
    let (exec, _dir) = reader(remote.clone());
    exec.index.write().await.add(BlockMetadata {
        path: std::path::PathBuf::from("/nonexistent/cpu.parquet"),
        start_timestamp_ns: 0,
        end_timestamp_ns: 7000,
        row_count: 1,
        size_bytes: 1,
        metric_names: ["cpu".to_string()].into_iter().collect(),
        label_names: Default::default(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });

    assert_eq!(exec.remote_watermark_ns(RemoteSignal::Metrics).await, 7000);
    let rows = exec
        .remote_rows(RemoteSignal::Metrics, Some("cpu".into()), 0, 9000)
        .await;
    assert!(rows.points.is_empty());

    let requests = remote.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].since_ns, 7000);
    assert_eq!(requests[0].start_ns, 0);
    assert_eq!(requests[0].end_ns, 9000);
    assert_eq!(requests[0].metric.as_deref(), Some("cpu"));
}

/// A reader with no writer configured answers from block storage only: no
/// request is attempted and the query still succeeds.
#[tokio::test]
async fn no_writer_configured_means_no_fan_out() {
    let dir = tempdir().unwrap();
    let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
    let log_index = Arc::new(RwLock::new(BlockIndex::new(&dir.path().join("logs"))));
    let exec = QueryExecutor::new(index, log_index, dir.path().join("traces"));
    let rows = exec.remote_rows(RemoteSignal::Logs, None, 0, 1000).await;
    assert!(rows.logs.is_empty());
    assert!(rows.points.is_empty() && rows.spans.is_empty());
}

/// The AST path merges remote points as well: `/api/v1/query` uses it, and a
/// query API that only fans out on the plan path would be inconsistent.
#[tokio::test]
async fn remote_points_are_merged_into_the_ast_result() {
    let remote = Arc::new(FakeRemote::new(vec![("cpu".to_string(), point(1500, 1.5))]));
    let (exec, _dir) = reader(remote);
    let expr = crate::parser::parse_expr("cpu").unwrap();
    let result = exec.execute_ast(&expr, 0, 4000, Some(1000)).await.unwrap();
    // The AST path evaluates on the step grid, so only the value is asserted;
    // the point being present at all is what proves the merge happened.
    assert_eq!(result.series.len(), 1);
    assert!(!result.series[0].samples.is_empty());
    assert!(result.series[0]
        .samples
        .iter()
        .all(|sample| sample.value == 1.5));
    assert!(result.series[0]
        .samples
        .iter()
        .all(|sample| (0..=4000).contains(&sample.timestamp_ns)));
}
