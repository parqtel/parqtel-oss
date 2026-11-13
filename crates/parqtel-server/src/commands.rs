//! One-shot subcommands: `compact`, `inspect` and `export`.

use parqtel_core::{BlockIndex, Config};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) async fn run_compact(
    _config: Config,
    _idx: Arc<tokio::sync::RwLock<BlockIndex>>,
    _lidx: Arc<tokio::sync::RwLock<BlockIndex>>,
) -> anyhow::Result<()> {
    tracing::info!("Running one-off compaction...");
    Ok(())
}

pub(crate) async fn run_inspect(
    index: Arc<tokio::sync::RwLock<BlockIndex>>,
    log_index: Arc<tokio::sync::RwLock<BlockIndex>>,
) -> anyhow::Result<()> {
    let idx = index.read().await;
    let lidx = log_index.read().await;
    let summary = serde_json::json!({
        "metrics": {
            "block_count": idx.total_blocks(),
            "total_rows": idx.total_rows(),
            "total_bytes": idx.total_bytes(),
            "metric_names": idx.all_metrics().into_iter().collect::<Vec<_>>(),
        },
        "logs": {
            "block_count": lidx.total_blocks(),
            "total_rows": lidx.total_rows(),
            "total_bytes": lidx.total_bytes(),
        }
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

pub(crate) async fn run_export(
    config: Config,
    index: Arc<tokio::sync::RwLock<BlockIndex>>,
    metric: String,
    start: String,
    end: String,
    output: PathBuf,
) -> anyhow::Result<()> {
    // Prevent path traversal: ensure output is within data directory
    let data_dir = config
        .storage
        .data_dir
        .canonicalize()
        .unwrap_or_else(|_| config.storage.data_dir.clone());

    // For output path, canonicalize the parent directory since file may not exist yet
    let output_parent = output.parent().unwrap_or(&output);
    let output_path = output_parent
        .canonicalize()
        .unwrap_or_else(|_| output_parent.to_path_buf());

    if !output_path.starts_with(&data_dir) {
        return Err(anyhow::anyhow!(
            "Output path must be within data directory: {}",
            data_dir.display()
        ));
    }

    let start_dt = chrono::DateTime::parse_from_rfc3339(&start)?;
    let end_dt = chrono::DateTime::parse_from_rfc3339(&end)?;
    let start_ns = start_dt.timestamp_nanos_opt().unwrap_or(0);
    let end_ns = end_dt.timestamp_nanos_opt().unwrap_or(0);

    let blocks = {
        let idx = index.read().await;
        idx.query(start_ns, end_ns, Some(&metric))
    };

    let points =
        parqtel_core::storage::Scanner::scan(blocks, metric, start_ns, end_ns, None).await?;

    let mut file = std::fs::File::create(output)?;
    writeln!(file, "timestamp_ns,value,labels")?;
    for p in &points {
        writeln!(
            file,
            "{},{},{:?}",
            p.timestamp_ns,
            v_to_f64(&p.value),
            p.labels
        )?;
    }

    println!("Exported {} points", points.len());
    Ok(())
}

pub(crate) fn v_to_f64(v: &parqtel_core::MetricValue) -> f64 {
    match v {
        parqtel_core::MetricValue::Double(f) => *f,
        parqtel_core::MetricValue::Int(i) => *i as f64,
        parqtel_core::MetricValue::Histogram(h) => h.sum,
        parqtel_core::MetricValue::Summary(s) => s.sum,
    }
}
