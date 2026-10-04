//! Measures what a per-row JSON `labels` column costs, and whether
//! dictionary-encoding it buys anything (BL-03-04).
//!
//! **Answer: nothing.** The metrics schema declares `labels` and
//! `value_complex` as plain `Utf8` while every neighbouring column is
//! dictionary-encoded, which looked like the largest remaining storage item.
//! It is not, and this is why:
//!
//! * Parquet **dictionary-encodes string columns by default**. Reading the
//!   encodings back out of a block written with a plain `Utf8` column shows
//!   `RLE_DICTIONARY`, exactly as for an explicitly dictionary-typed column.
//!   So the Arrow-level change adds an encoding that is already applied.
//! * zstd then compresses what remains. 9 MB of raw labels JSON across
//!   100 000 rows lands at 810 KB, and a series dictionary would save bytes
//!   the dictionary encoding already saved.
//! * Write cost is a wash as well, so there is no CPU argument either.
//!
//! Kept as the evidence for closing BL-03-04 without a schema change: an
//! on-disk format migration is expensive and risky, and this is what says it
//! would buy nothing.
//!
//! Run: cargo run --release -p parqtel-core --example probe_labels_size
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use arrow_array::types::Int32Type;
use arrow_array::{
    builder::{Float64Builder, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder},
    ArrayRef,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parqtel_core::error::Result;
use std::sync::Arc;

const ROW_GROUPS: usize = 20;
const ROWS_PER_GROUP: usize = 5_000;
const SERIES: usize = 200;

fn labels_for(i: usize) -> String {
    // Shaped like real labels: several keys, repeated per series.
    format!(
        r#"{{"host":"host-{}","region":"r{}","instance":"10.0.{}.{}:8080","job":"api","tier":"t{}"}}"#,
        i % 50,
        i % 8,
        i % 4,
        i % 256,
        i % 3
    )
}

fn write(
    path: &std::path::Path,
    dictionary: bool,
    value_complex_dict: bool,
    compression: bool,
) -> Result<u64> {
    let label_ty = if dictionary {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    } else {
        DataType::Utf8
    };
    let vc_ty = if value_complex_dict {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    } else {
        DataType::Utf8
    };
    let schema = Schema::new(vec![
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("labels", label_ty, false),
        Field::new("value_float", DataType::Float64, true),
        Field::new("value_complex", vc_ty, true),
    ]);

    let mut ts = TimestampNanosecondBuilder::new();
    let mut vf = Float64Builder::new();
    let mut labels_dict = StringDictionaryBuilder::<Int32Type>::new();
    let mut labels_plain = StringBuilder::new();
    let mut vc_dict = StringDictionaryBuilder::<Int32Type>::new();
    let mut vc_plain = StringBuilder::new();

    let total = ROW_GROUPS * ROWS_PER_GROUP;
    for r in 0..total {
        let s = r % SERIES;
        ts.append_value(r as i64 * 1_000_000_000);
        vf.append_value(s as f64);
        let json = labels_for(s);
        if dictionary {
            labels_dict.append_value(&json);
        } else {
            labels_plain.append_value(&json);
        }
        // Histogram payloads repeat per series too.
        let complex = format!(r#"{{"count":{},"sum":{}}}"#, s * 10, s as f64 * 1.5);
        if value_complex_dict {
            vc_dict.append_value(&complex);
        } else {
            vc_plain.append_value(&complex);
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        if dictionary {
            Arc::new(labels_dict.finish())
        } else {
            Arc::new(labels_plain.finish())
        },
        Arc::new(vf.finish()),
        if value_complex_dict {
            Arc::new(vc_dict.finish())
        } else {
            Arc::new(vc_plain.finish())
        },
    ];
    let batch = arrow_array::RecordBatch::try_new(Arc::new(schema), columns)
        .map_err(|e| parqtel_core::Error::Arrow(e.to_string()))?;

    let file = std::fs::File::create(path)?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(if compression {
            parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
        } else {
            parquet::basic::Compression::UNCOMPRESSED
        })
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();
    let mut w =
        parquet::arrow::arrow_writer::ArrowWriter::try_new(file, batch.schema(), Some(props))
            .map_err(|e| parqtel_core::Error::Parquet(e.to_string()))?;
    w.write(&batch)
        .map_err(|e| parqtel_core::Error::Parquet(e.to_string()))?;
    w.close()
        .map_err(|e| parqtel_core::Error::Parquet(e.to_string()))?;
    Ok(std::fs::metadata(path)
        .map_err(parqtel_core::Error::Io)?
        .len())
}

fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let rows = (ROW_GROUPS * ROWS_PER_GROUP) as u64;
    println!("{rows} rows, {SERIES} distinct series, {ROWS_PER_GROUP} rows/row-group, zstd\n");

    println!(
        "{:<26} {:>12} {:>12} {:>10}",
        "configuration", "Utf8 (now)", "Dictionary", "change"
    );
    for (label, compression) in [("zstd (production)", true), ("uncompressed", false)] {
        let plain = write(&dir.path().join("plain.parquet"), false, false, compression)?;
        let dict = write(&dir.path().join("dict.parquet"), true, false, compression)?;
        println!(
            "{:<26} {:>12} {:>12} {:>9.1}%",
            label,
            plain,
            dict,
            100.0 * (dict as f64 - plain as f64) / plain as f64
        );
    }
    // Decisive: what encoding did Parquet actually use for the plain Utf8
    // column? If it is already dictionary-encoded, then changing the *Arrow*
    // type buys nothing, because Parquet dictionary-encodes string columns by
    // default either way.
    println!("\nencoding Parquet chose per column (row group 0, plain Utf8 build):");
    let f = std::fs::File::open(dir.path().join("plain.parquet")).unwrap();
    let md = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .metadata()
        .clone();
    for i in 0..md.row_group(0).num_columns() {
        let c = md.row_group(0).column(i);
        let enc = c.encodings();
        println!(
            "  {:<16} {:?}",
            c.column_path(),
            enc.map(|e| format!("{e:?}")).collect::<Vec<_>>()
        );
    }
    println!(
        "\n(raw labels JSON is ~90 B/row, so {} rows is {} MB of text)",
        rows,
        rows * 90 / 1_000_000
    );

    // Does the Arrow-level dictionary change WRITE cost? The per-row
    // `labels.to_json()` happens either way; what differs is whether Arrow
    // builds a dictionary while writing. If that is also a wash, the schema
    // change has no benefit on either axis.
    println!("\nwrite cost of the Arrow dictionary:");
    for (label, dictionary) in [("Utf8 (current)", false), ("Dictionary", true)] {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let started = std::time::Instant::now();
            write(&dir.path().join("timing.parquet"), dictionary, false, true).unwrap();
            best = best.min(started.elapsed().as_secs_f64());
        }
        println!("  {label:<18} {best:.3} s");
    }
    Ok(())
}
