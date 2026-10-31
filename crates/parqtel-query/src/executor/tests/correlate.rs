//! Correlation by service, pod and namespace.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[tokio::test]
async fn test_correlation_with_service_name() {
    let (exec, _dir) = setup_with_data().await;
    let labels = LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap();
    let res = exec
        .correlate("metric", 2000, labels, "log", 5000, 10)
        .await
        .unwrap();
    assert_eq!(res.correlation_dimension_used, "service_name");
}

#[tokio::test]
async fn test_correlation_pod_uid_dimension() {
    let (exec, _dir) = setup_with_data().await;
    let labels = LabelSet::try_from_iter(vec![("k8s_pod_uid", "uid-123")]).unwrap();
    let res = exec
        .correlate("metric", 2000, labels, "log", 5000, 10)
        .await
        .unwrap();
    assert_eq!(res.correlation_dimension_used, "k8s_pod_uid");
}

#[tokio::test]
async fn test_correlation_pod_namespace_dimension() {
    let (exec, _dir) = setup_with_data().await;
    let labels =
        LabelSet::try_from_iter(vec![("k8s_pod_name", "pod-1"), ("k8s_namespace", "prod")])
            .unwrap();
    let res = exec
        .correlate("metric", 2000, labels, "log", 5000, 10)
        .await
        .unwrap();
    assert_eq!(res.correlation_dimension_used, "k8s_pod_name+namespace");
}

#[tokio::test]
async fn test_correlation_namespace_only() {
    let (exec, _dir) = setup_with_data().await;
    let labels = LabelSet::try_from_iter(vec![("k8s_namespace", "prod")]).unwrap();
    let res = exec
        .correlate("metric", 2000, labels, "log", 5000, 10)
        .await
        .unwrap();
    assert_eq!(res.correlation_dimension_used, "k8s_namespace");
}

#[tokio::test]
async fn test_correlation_metric_target() {
    let (exec, _dir) = setup_with_data().await;
    let labels = LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap();
    let res = exec
        .correlate("log", 2000, labels, "metric", 5000, 10)
        .await
        .unwrap();
    assert_eq!(res.correlation_dimension_used, "service_name");
}

#[tokio::test]
async fn test_empty_correlation_no_dimension() {
    let (exec, _dir) = setup_with_data().await;
    let labels = LabelSet::try_from_iter(vec![("random_key", "val")]).unwrap();
    let res = exec
        .correlate("metric", 2000, labels, "log", 5000, 10)
        .await
        .unwrap();
    assert_eq!(res.correlation_dimension_used, "none");
    assert!(res.correlated.is_empty());
}
