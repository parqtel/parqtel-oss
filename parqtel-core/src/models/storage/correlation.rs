use crate::models::labels::LabelSet;
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, DictionaryArray, StringArray};

#[derive(Default)]
pub(crate) struct CorrelationLabels {
    pub service_name: Option<String>,
    pub service_version: Option<String>,
    pub k8s_namespace: Option<String>,
    pub k8s_pod_name: Option<String>,
    pub k8s_pod_uid: Option<String>,
    pub k8s_container_name: Option<String>,
    pub k8s_node_name: Option<String>,
}

/// Extracts correlation labels from a LabelSet, returning the remaining labels and the extracted correlation.
pub(crate) fn extract_correlation_labels(labels: &LabelSet) -> (LabelSet, CorrelationLabels) {
    let mut correlation = CorrelationLabels::default();
    let mut remaining = Vec::new();

    for (k, v) in labels.iter() {
        match k {
            "service.name" => correlation.service_name = Some(v.to_string()),
            "service.version" => correlation.service_version = Some(v.to_string()),
            "k8s.namespace.name" => correlation.k8s_namespace = Some(v.to_string()),
            "k8s.pod.name" => correlation.k8s_pod_name = Some(v.to_string()),
            "k8s.pod.uid" => correlation.k8s_pod_uid = Some(v.to_string()),
            "k8s.container.name" => correlation.k8s_container_name = Some(v.to_string()),
            "k8s.node.name" => correlation.k8s_node_name = Some(v.to_string()),
            _ => remaining.push((k, v)),
        }
    }

    (
        LabelSet::try_from_iter(remaining).unwrap_or_default(),
        correlation,
    )
}

/// Reads correlation columns from a record batch row starting at `start_idx`.
pub(crate) fn row_to_correlation(
    batch: &RecordBatch,
    row: usize,
    start_idx: usize,
) -> CorrelationLabels {
    CorrelationLabels {
        service_name: get_dict_val(batch, row, start_idx),
        service_version: get_dict_val(batch, row, start_idx + 1),
        k8s_namespace: get_dict_val(batch, row, start_idx + 2),
        k8s_pod_name: get_dict_val(batch, row, start_idx + 3),
        k8s_pod_uid: get_dict_val(batch, row, start_idx + 4),
        k8s_container_name: get_dict_val(batch, row, start_idx + 5),
        k8s_node_name: get_dict_val(batch, row, start_idx + 6),
    }
}

/// Re-injects correlation labels back into a LabelSet.
pub(crate) fn inject_correlation(mut labels: LabelSet, correlation: CorrelationLabels) -> LabelSet {
    if let Some(v) = correlation.service_name {
        labels =
            labels.merge(&LabelSet::try_from_iter(vec![("service.name", v)]).unwrap_or_default());
    }
    if let Some(v) = correlation.service_version {
        labels = labels
            .merge(&LabelSet::try_from_iter(vec![("service.version", v)]).unwrap_or_default());
    }
    if let Some(v) = correlation.k8s_namespace {
        labels = labels
            .merge(&LabelSet::try_from_iter(vec![("k8s.namespace.name", v)]).unwrap_or_default());
    }
    if let Some(v) = correlation.k8s_pod_name {
        labels =
            labels.merge(&LabelSet::try_from_iter(vec![("k8s.pod.name", v)]).unwrap_or_default());
    }
    if let Some(v) = correlation.k8s_pod_uid {
        labels =
            labels.merge(&LabelSet::try_from_iter(vec![("k8s.pod.uid", v)]).unwrap_or_default());
    }
    if let Some(v) = correlation.k8s_container_name {
        labels = labels
            .merge(&LabelSet::try_from_iter(vec![("k8s.container.name", v)]).unwrap_or_default());
    }
    if let Some(v) = correlation.k8s_node_name {
        labels =
            labels.merge(&LabelSet::try_from_iter(vec![("k8s.node.name", v)]).unwrap_or_default());
    }
    labels
}

fn get_dict_val(batch: &RecordBatch, row: usize, idx: usize) -> Option<String> {
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()?;
    if arr.is_null(row) {
        return None;
    }
    let values = arr.values().as_any().downcast_ref::<StringArray>()?;
    Some(values.value(arr.keys().value(row) as usize).to_string())
}
