//! Log queries.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[tokio::test]
async fn test_query_logs_empty() {
    let index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
    let log_index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
    let trace_data_dir = PathBuf::from("/tmp/ne/traces");
    std::fs::create_dir_all(&trace_data_dir).ok();
    let exec = QueryExecutor::new(index, log_index, trace_data_dir);
    let res = exec
        .query_logs(0, 100, vec![], 10, false, None, None)
        .await
        .unwrap();
    assert!(res.logs.is_empty());
    assert_eq!(res.total_logs_count, 0);
}

#[tokio::test]
async fn test_query_logs_with_data() {
    let (exec, _dir) = setup_with_data().await;
    let res = exec
        .query_logs(0, 5000, vec![], 10, false, None, None)
        .await
        .unwrap();
    assert_eq!(res.logs.len(), 1);
    assert_eq!(res.total_logs_count, 1);
}

#[tokio::test]
async fn test_query_logs_severity_filter() {
    let (exec, _dir) = setup_with_data().await;
    // severity_min=13 (WARN) should filter out our INFO (9) log
    let res = exec
        .query_logs(0, 5000, vec![], 10, false, Some(13), None)
        .await
        .unwrap();
    assert!(res.logs.is_empty());
}

#[tokio::test]
async fn test_query_logs_search_filter() {
    let (exec, _dir) = setup_with_data().await;
    let res = exec
        .query_logs(0, 5000, vec![], 10, false, None, Some("request".into()))
        .await
        .unwrap();
    assert_eq!(res.logs.len(), 1);
    let res2 = exec
        .query_logs(0, 5000, vec![], 10, false, None, Some("nonexistent".into()))
        .await
        .unwrap();
    assert!(res2.logs.is_empty());
}

#[tokio::test]
async fn test_query_logs_ordering() {
    let (exec, _dir) = setup_with_data().await;
    let res = exec
        .query_logs(0, 5000, vec![], 10, true, None, None)
        .await
        .unwrap();
    assert_eq!(res.logs.len(), 1); // only 1 log, ordering still works
}

#[tokio::test]
async fn test_get_log_fields_with_data() {
    let (exec, _dir) = setup_with_data().await;
    let (dedicated, _common) = exec.get_log_fields().await;
    assert!(dedicated.contains(&"body".to_string()));
    assert!(dedicated.contains(&"service_name".to_string()));
}

#[tokio::test]
async fn test_get_log_field_values() {
    let (exec, _dir) = setup_with_data().await;
    let values = exec.get_log_field_values("service_name", 10).await;
    assert!(values.contains(&"web".to_string()));
}
