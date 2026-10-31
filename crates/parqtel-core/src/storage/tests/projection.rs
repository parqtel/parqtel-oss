//! Column projection on the metrics scan.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

/// Numeric view of a `MetricValue`, for comparing decoded points.
fn as_f64(v: &MetricValue) -> f64 {
    match v {
        MetricValue::Double(d) => *d,
        MetricValue::Int(i) => *i as f64,
        _ => f64::NAN,
    }
}

/// Column projection must not change a single decoded value.
///
/// The metrics decoder indexes columns positionally and a projection
/// renumbers them, so a mistake here returns the *wrong column* rather than
/// erroring — a label where a timestamp should be, or `metric_name` where
/// `value_complex` should be. That is silent data corruption, not a crash,
/// so it needs a test that would actually notice.
///
/// Timestamps and values are compared against a full unprojected decode.
/// Labels are compared *by content* rather than by fingerprint against
/// `row_to_point`, because the two decoders legitimately differ there: the
/// scan merges the dedicated `service_name` column back into the series
/// labels while `row_to_point` leaves it in the resource attributes. Both
/// are correct for their consumer, so comparing them would assert a
/// difference that was already there.
#[test]
fn projection_does_not_change_decoded_points() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("projected.parquet");
    // 10 series x 100 points, so labels vary per series and a
    // mis-positioned column cannot pass by coincidence.
    let block = prune_test_metrics(1000);
    write_block_with(&path, &block, 100, true, true);
    let meta = BlockMetadata {
        path: path.clone(),
        start_timestamp_ns: 1,
        end_timestamp_ns: 1000,
        row_count: 1000,
        size_bytes: 0,
        metric_names: HashSet::from(["prune.cpu".to_string()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    };

    let projected =
        scanner::Scanner::scan_block(meta.clone(), "prune.cpu".into(), 0, i64::MAX, None).unwrap();

    let file = fs::File::open(&path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();
    let mut expected = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        for row in 0..batch.num_rows() {
            let (name, _kind, _labels, dp) = StorageModel::row_to_point(&batch, row).unwrap();
            assert_eq!(name, "prune.cpu");
            expected.push((dp.timestamp_ns, as_f64(&dp.value)));
        }
    }

    assert_eq!(
        projected.len(),
        expected.len(),
        "projection changed how many points were decoded"
    );
    for (i, (got, want)) in projected.iter().zip(expected.iter()).enumerate() {
        assert_eq!(got.timestamp_ns, want.0, "row {i}: timestamp differs");
        assert_eq!(as_f64(&got.value), want.1, "row {i}: value differs");
    }

    // `host` is the only label the fixture sets, and it varies per series,
    // so a `labels` column read from the wrong position would show up here.
    assert!(!projected.is_empty());
    for p in &projected {
        let host = p.labels.get("host").unwrap_or_else(|| {
            panic!(
                "row at ts {} has no host label; labels were read from the wrong column",
                p.timestamp_ns
            )
        });
        // Fixture: host is `h{i % 10}` for the i-th point of each series.
        let expected_host = format!("h{}", ((p.timestamp_ns - 1) % 10));
        assert_eq!(
            host, expected_host,
            "labels were read from the wrong column"
        );
    }
}

/// The projection must resolve to the columns the decoder expects, at the
/// positions the decoder reads them from.
#[test]
fn projected_schema_resolves_to_the_expected_columns() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("resolve.parquet");
    write_block_with(&path, &prune_test_metrics(50), 50, true, true);
    let file = fs::File::open(&path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = parquet::arrow::ProjectionMask::leaves(
        builder.parquet_schema(),
        scanner::project_present(builder.parquet_schema(), &scanner::METRIC_SCAN_COLUMNS),
    );
    let mut reader = builder.with_projection(mask).build().unwrap();
    let batch = reader.next().unwrap().unwrap();

    // The projected schema carries only the scanned columns.
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        scanner::METRIC_SCAN_COLUMNS.to_vec(),
        "projected schema should hold exactly the scanned columns, in order"
    );

    // And every decoder index resolves inside it.
    let idx = scanner::metric_scan_indices(&batch.schema()).unwrap();
    for (pos, col) in idx.iter().zip(scanner::METRIC_SCAN_COLUMNS) {
        assert_eq!(*pos, names.iter().position(|n| *n == col).unwrap());
        assert!(
            *pos < batch.num_columns(),
            "{col} resolved outside the projected batch"
        );
    }
}

/// A projection naming a column the file lacks must be intersected away
/// rather than erroring, so blocks written by older builds still read.
#[test]
fn projection_is_intersected_with_the_file_schema() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("intersect.parquet");
    write_block_with(&path, &prune_test_metrics(50), 50, true, true);

    let file = fs::File::open(&path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let present = scanner::project_present(builder.parquet_schema(), &scanner::METRIC_SCAN_COLUMNS);
    assert_eq!(
        present.len(),
        scanner::METRIC_SCAN_COLUMNS.len(),
        "a current-schema block should project every scanned column"
    );

    // Names that do not exist in the file are dropped, not passed through.
    let missing = scanner::project_present(
        builder.parquet_schema(),
        &["timestamp_ns", "not_a_column", "labels"],
    );
    assert_eq!(missing.len(), 2, "unknown columns must be dropped");
}
