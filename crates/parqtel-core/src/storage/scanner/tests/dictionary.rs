//! The `labels` column is read as a dictionary.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::{DataPoint, LabelSet, Metric, MetricKind, MetricValue, StorageModel};
use parquet::arrow::ArrowWriter;

/// The `labels` column is requested as a dictionary; if the override were
/// ever silently rejected the scan would still be correct but lose its fast
/// path, so assert it is the type the reader hands back.
#[test]
fn labels_are_read_as_a_dictionary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.parquet");
    let metric = Metric {
        name: "m".into(),
        kind: MetricKind::Gauge,
        data_points: (1..=50)
            .map(|i| {
                DataPoint::new(
                    i,
                    MetricValue::Double(i as f64),
                    LabelSet::try_from_iter(vec![("host", format!("h{}", i % 3))]).unwrap(),
                )
                .unwrap()
            })
            .collect(),
        ..Default::default()
    };
    let chunk = StorageModel::metrics_to_chunk(&[metric]).unwrap();
    let mut w = ArrowWriter::try_new(File::create(&path).unwrap(), chunk.schema(), None).unwrap();
    w.write(&chunk).unwrap();
    w.close().unwrap();

    let opened = open_block(&path).unwrap().unwrap();
    let (file, meta) = (opened.file, opened.meta);
    let idx = meta.schema().index_of("labels").unwrap();
    assert!(
        matches!(
            meta.schema().field(idx).data_type(),
            arrow_schema::DataType::Dictionary(_, _)
        ),
        "labels should be requested as a dictionary, got {:?}",
        meta.schema().field(idx).data_type()
    );
    let mut reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta)
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(matches!(
        batch
            .column(batch.schema().index_of("labels").unwrap())
            .data_type(),
        arrow_schema::DataType::Dictionary(_, _)
    ));

    // A second open is served from the footer cache and behaves the same.
    let again = open_block(&path).unwrap().unwrap().meta;
    assert_eq!(again.schema().as_ref(), batch.schema_ref().as_ref());
}
