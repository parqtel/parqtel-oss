//! The per-signal services: decoding, errors and contention metrics.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

/// Decode runs on the blocking pool, so the failure counter must still be
/// incremented exactly once for a bad payload — including when the decode
/// task itself fails.
#[tokio::test]
async fn test_malformed_payloads_still_count_as_failures_once() {
    let dir = tempdir().unwrap();
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = IngestionService::new(tiny_metrics_config(dir.path()), tx);

    // Protobuf: garbage bytes.
    assert!(service
        .ingest_proto(Bytes::from_static(&[0xff, 0xfe, 0xfd]))
        .await
        .is_err());
    // JSON: not an object.
    assert!(service
        .ingest_json(Bytes::from_static(b"not json"))
        .await
        .is_err());

    let (batches, failed, points) = service.stats();
    assert_eq!(batches, 2, "both attempts counted as batches");
    assert_eq!(failed, 2, "both counted as failures exactly once");
    assert_eq!(points, 0, "nothing ingested");
}

/// A large payload must decode off the runtime thread and still arrive
/// intact, so the offload is exercised end to end rather than only on the
/// error path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_large_payloads_decode_and_ingest() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    let service = Arc::new(
        IngestionService::new(
            BlockConfig {
                max_rows_per_block: 50,
                block_duration_secs: 0,
                ..tiny_metrics_config(dir.path())
            },
            tx,
        )
        .with_contention(contention.clone()),
    );

    let points = 40;
    let json = serde_json::json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "svc"}}
            ]},
            "scopeMetrics": [{
                "metrics": [{
                    "name": "cpu",
                    "unit": "1",
                    "gauge": {"dataPoints": (0..points).map(|i| json!({
                        "asInt": i.to_string(),
                        "timeUnixNano": (1000 + i).to_string()
                    })).collect::<Vec<_>>()}
                }]
            }]
        }]
    });
    let body = Bytes::from(serde_json::to_vec(&json).unwrap());

    // Two concurrent decodes on a 2-worker runtime: with inline decoding one
    // would occupy a whole worker for its duration.
    let mut handles = Vec::new();
    for _ in 0..2 {
        let service = service.clone();
        let body = body.clone();
        handles.push(tokio::spawn(async move {
            service.ingest_json(body).await.unwrap()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), points as u64);
    }

    service.shutdown().await.unwrap();
    let total: usize = std::iter::from_fn(|| rx.try_recv().ok())
        .map(|p: PendingIndex| p.meta.row_count)
        .sum();
    assert_eq!(total, points * 2, "both batches must be written in full");
}

/// Every ingest request for a signal must record exactly one lock-wait
/// observation, and the counter must be attributable per signal.
#[tokio::test]
async fn test_ingest_records_lock_wait_per_signal() {
    let dir = tempdir().unwrap();
    let contention = Arc::new(ContentionMetrics::new());
    let (tx, _rx) = mpsc::unbounded_channel();
    let (ltx, _lrx) = mpsc::unbounded_channel();
    let (ttx, _trx) = mpsc::unbounded_channel();

    let metrics_svc = IngestionService::new(tiny_metrics_config(dir.path()), tx)
        .with_contention(contention.clone());
    let logs_svc = LogIngestionService::new(
        LogBlockConfig {
            data_dir: dir.path().join("logs"),
            ..Default::default()
        },
        ltx,
    )
    .with_contention(contention.clone());
    let traces_svc = TraceIngestionService::new(tiny_metrics_config(dir.path()), ttx)
        .with_contention(contention.clone());

    metrics_svc
        .ingest_metrics(vec![one_point_metric("m1", 100)])
        .await
        .unwrap();
    metrics_svc
        .ingest_metrics(vec![one_point_metric("m1", 200)])
        .await
        .unwrap();
    logs_svc
        .ingest_json(Bytes::from(
            serde_json::to_vec(&json!({
                "resourceLogs": [{
                    "resource": {"attributes": [
                        {"key": "service.name", "value": {"stringValue": "svc"}}
                    ]},
                    "scopeLogs": [{
                        "logRecords": [{
                            "timeUnixNano": "100",
                            "severityNumber": 9,
                            "severityText": "INFO",
                            "body": {"stringValue": "hello"}
                        }]
                    }]
                }]
            }))
            .unwrap(),
        ))
        .await
        .unwrap();
    traces_svc
        .ingest_json(Bytes::from(
            serde_json::to_vec(&json!({
                "resourceSpans": [{
                    "resource": {"attributes": [
                        {"key": "service.name", "value": {"stringValue": "svc"}}
                    ]},
                    "scopeSpans": [{
                        "spans": [{
                            "traceId": "0af7651916cd43dd8448eb211c80319c",
                            "spanId": "b7ad6b7169203331",
                            "name": "op",
                            "kind": 2,
                            "startTimeUnixNano": "100",
                            "endTimeUnixNano": "200"
                        }]
                    }]
                }]
            }))
            .unwrap(),
        ))
        .await
        .unwrap();

    assert_eq!(contention.ingest_lock_wait_count(SignalType::Metrics), 2);
    assert_eq!(contention.ingest_lock_wait_count(SignalType::Logs), 1);
    assert_eq!(contention.ingest_lock_wait_count(SignalType::Traces), 1);

    // The rendered body must be scrapeable and carry all three signals.
    let rendered = contention.render();
    assert!(rendered.contains("parqtel_ingest_lock_wait_seconds{signal=\"metrics\"}_count 2"));
    assert!(rendered.contains("parqtel_ingest_lock_wait_seconds{signal=\"logs\"}_count 1"));
    assert!(rendered.contains("parqtel_ingest_lock_wait_seconds{signal=\"traces\"}_count 1"));
}

/// Concurrent requests must each record a lock wait and none may be lost —
/// this is the observation that predicts ingest p99, so a dropped
/// observation would hide exactly the stall we are looking for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_ingest_records_every_lock_wait() {
    let dir = tempdir().unwrap();
    let contention = Arc::new(ContentionMetrics::new());
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = Arc::new(
        IngestionService::new(tiny_metrics_config(dir.path()), tx)
            .with_contention(contention.clone()),
    );

    const CONCURRENT: usize = 32;
    let mut handles = Vec::with_capacity(CONCURRENT);
    for i in 0..CONCURRENT {
        let service = service.clone();
        handles.push(tokio::spawn(async move {
            service
                .ingest_metrics(vec![one_point_metric("m1", 1000 + i as i64)])
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(
        contention.ingest_lock_wait_count(SignalType::Metrics),
        CONCURRENT as u64,
        "every request must record exactly one lock-wait observation"
    );
    assert_eq!(
        contention.ingest_lock_wait_count(SignalType::Logs),
        0,
        "log requests must not pollute the metrics series"
    );
}

#[tokio::test]
async fn test_services_work_without_contention_metrics() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    // block_duration_secs = 0 makes the duration check fire immediately,
    // so check_and_flush exercises the periodic path deterministically.
    let service = IngestionService::new(
        BlockConfig {
            block_duration_secs: 0,
            ..tiny_metrics_config(dir.path())
        },
        tx,
    );

    assert_eq!(
        service
            .ingest_metrics(vec![one_point_metric("m1", 100)])
            .await
            .unwrap(),
        1
    );
    assert!(service.check_and_flush().await.unwrap());
    service.shutdown().await.unwrap();

    let mut metas = Vec::new();
    while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
        metas.push(meta);
    }
    assert!(!metas.is_empty(), "flushes must still publish blocks");
    let total: usize = metas.iter().map(|m| m.row_count).sum();
    assert_eq!(total, 1, "the point must be written exactly once");
}

#[tokio::test]
async fn test_ingestion_service_json() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = IngestionService::new(config, tx);

    let payload = json!({
        "resourceMetrics": [{
            "scopeMetrics": [{
                "metrics": [{
                    "name": "test_m",
                    "gauge": {"dataPoints": [{"timeUnixNano": 1000, "asDouble": 1.0}]}
                }]
            }]
        }]
    });

    let count = service
        .ingest_json(Bytes::from(payload.to_string()))
        .await
        .unwrap();
    assert_eq!(count, 1);
    let (total, failed, ingested) = service.stats();
    assert_eq!(total, 1);
    assert_eq!(failed, 0);
    assert_eq!(ingested, 1);
}

#[tokio::test]
async fn test_log_ingestion_service_json() {
    let dir = tempdir().unwrap();
    let config = LogBlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = LogIngestionService::new(config, tx);

    let payload = json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": 1000,
                    "body": "test log"
                }]
            }]
        }]
    });

    let count = service
        .ingest_json(Bytes::from(payload.to_string()))
        .await
        .unwrap();
    assert_eq!(count, 1);
    let (total, failed, ingested) = service.stats();
    assert_eq!(total, 1);
    assert_eq!(failed, 0);
    assert_eq!(ingested, 1);
}

#[tokio::test]
async fn test_trace_ingestion_service_json() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = TraceIngestionService::new(config, tx);

    let payload = json!({
        "resource_spans": [{
            "scope_spans": [{
                "spans": [{
                    "trace_id": "0102030405060708090a0b0c0d0e0f10",
                    "span_id": "0102030405060708",
                    "name": "test-span",
                    "kind": 1,
                    "start_time_unix_nano": "1000",
                    "end_time_unix_nano": "2000"
                }]
            }]
        }]
    });

    let count = service
        .ingest_json(Bytes::from(payload.to_string()))
        .await
        .unwrap();
    assert_eq!(count, 1);
    let (total, failed, ingested) = service.stats();
    assert_eq!(total, 1);
    assert_eq!(failed, 0);
    assert_eq!(ingested, 1);
}

#[tokio::test]
async fn test_trace_ingestion_service_error_paths() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 2,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = TraceIngestionService::new(config, tx);

    // Test empty body
    let res = service.ingest_json(Bytes::from("")).await;
    assert!(res.is_err());

    // Test invalid JSON
    let res = service.ingest_json(Bytes::from("{invalid json}")).await;
    assert!(res.is_err());

    // Test buffer full (3 spans > capacity of 2)
    let payload = json!({
        "resource_spans": [{
            "scope_spans": [{
                "spans": [
                    {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s1", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"},
                    {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s2", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"},
                    {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s3", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"}
                ]
            }]
        }]
    });

    let count = service
        .ingest_json(Bytes::from(payload.to_string()))
        .await
        .unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_ingestion_service_shutdown() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 60,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = IngestionService::new(config, tx);

    let payload = json!({ "resourceMetrics": [{ "scopeMetrics": [{ "metrics": [{ "name": "m", "gauge": {"dataPoints": [{"timeUnixNano": 1000, "asDouble": 1.0}]} }] }] }] });
    service
        .ingest_json(Bytes::from(payload.to_string()))
        .await
        .unwrap();
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ingestion_service_invalid_json() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = IngestionService::new(config, tx);
    let res = service.ingest_json(Bytes::from("not json")).await;
    assert!(res.is_err());
    let (total, _, _) = service.stats();
    assert_eq!(total, 1);
}

#[tokio::test]
async fn test_ingestion_service_invalid_proto() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = IngestionService::new(config, tx);
    let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_log_ingestion_service_invalid_proto() {
    let dir = tempdir().unwrap();
    let config = LogBlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = LogIngestionService::new(config, tx);
    let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_log_ingestion_service_shutdown() {
    let dir = tempdir().unwrap();
    let config = LogBlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 60,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = LogIngestionService::new(config, tx);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_trace_ingestion_service_invalid_proto() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = TraceIngestionService::new(config, tx);
    let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_trace_ingestion_service_shutdown() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 60,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let service = TraceIngestionService::new(config, tx);
    service.shutdown().await.unwrap();
}
