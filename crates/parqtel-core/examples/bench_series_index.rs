//! Measures series-table parsing and index construction in separate processes.
//!
//! Run `fixture <path>` once, then run each `measure <step> <path>` in its own
//! process. Steps: `parse`, `open`, `select`, and `select-broad`.

use parqtel_core::models::labels::LabelSet;
use parqtel_core::storage::{SeriesIndex, SeriesSelector, SeriesTable, SeriesTableBuilder};
use std::error::Error;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use std::time::Instant;

const SERIES: usize = 283_000;
const METRICS: usize = 3_400;
const ROWS_PER_SERIES: u32 = 4;

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message)
}

fn fixture(path: &str) -> Result<(), Box<dyn Error>> {
    let mut builder = SeriesTableBuilder::new();
    let mut series_id = 0usize;
    for metric_id in 0..METRICS {
        builder.begin_metric(&format!("metric_{metric_id}"));
        let count = SERIES / METRICS + usize::from(metric_id < SERIES % METRICS);
        for _ in 0..count {
            let labels = LabelSet::try_from_iter([
                ("pod", format!("pod-{series_id}")),
                ("namespace", format!("namespace-{}", series_id % 50)),
                ("service.name", format!("service-{}", series_id % 200)),
                ("node", format!("node-{}", series_id % 16)),
                ("container", format!("container-{}", series_id % 8)),
                ("cluster", format!("cluster-{}", series_id % 4)),
                ("region", format!("region-{}", series_id % 6)),
                ("team", format!("team-{}", series_id % 10)),
                ("env", format!("env-{}", series_id % 3)),
                ("zone", format!("zone-{}", series_id % 8)),
                ("job", format!("job-{}", series_id % 12)),
                ("instance", format!("instance-{}", series_id % 16)),
                ("account", format!("account-{}", series_id % 20)),
                ("component", format!("component-{}", series_id % 5)),
            ])?;
            builder.push_series(
                &labels,
                (series_id as u64) * u64::from(ROWS_PER_SERIES),
                ROWS_PER_SERIES,
                1,
                4,
            );
            series_id += 1;
        }
    }
    let value = builder.finish().to_footer_value()?;
    std::fs::write(path, value)?;
    Ok(())
}

fn status_kib(status: &str, key: &str) -> io::Result<u64> {
    let line = status
        .lines()
        .find(|line| line.starts_with(key))
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, format!("missing {key}")))?;
    let kib = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, format!("invalid {key}")))?
        .parse::<u64>()
        .map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
    Ok(kib)
}

fn memory_kib() -> io::Result<(u64, u64)> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    Ok((
        status_kib(&status, "VmHWM:")?,
        status_kib(&status, "VmRSS:")?,
    ))
}

fn measure(step: &str, path: &str) -> Result<(), Box<dyn Error>> {
    let value = std::fs::read_to_string(path)?;
    let started = Instant::now();
    let mut table = Some(SeriesTable::from_footer_value(&value)?);
    let mut index = None;
    let keep_table = step == "parse";
    let (approx_bytes, selected) = match step {
        "parse" => (0, 0),
        "open" => {
            let parsed = table
                .take()
                .ok_or_else(|| io::Error::other("parsed table missing"))?;
            let opened = SeriesIndex::from_owned_table(parsed)?;
            let approx_bytes = opened.approx_bytes();
            index = Some(opened);
            (approx_bytes, 0)
        }
        "select" => {
            let parsed = table
                .take()
                .ok_or_else(|| io::Error::other("parsed table missing"))?;
            let opened = SeriesIndex::from_owned_table(parsed)?;
            let selector = SeriesSelector {
                equals: vec![("pod".to_string(), "pod-0".to_string())],
                ..Default::default()
            };
            let ids = opened.select("metric_0", &selector, i64::MIN, i64::MAX);
            for id in &ids {
                std::hint::black_box(opened.labels(*id)?);
            }
            let approx_bytes = opened.approx_bytes();
            let selected = ids.len();
            index = Some(opened);
            (approx_bytes, selected)
        }
        "select-broad" => {
            let parsed = table
                .take()
                .ok_or_else(|| io::Error::other("parsed table missing"))?;
            let opened = SeriesIndex::from_owned_table(parsed)?;
            let selector = SeriesSelector {
                filter: Some(Arc::new(|_| true)),
                ..Default::default()
            };
            let ids = opened.select("metric_0", &selector, i64::MIN, i64::MAX);
            let approx_bytes = opened.approx_bytes();
            let selected = ids.len();
            index = Some(opened);
            (approx_bytes, selected)
        }
        _ => return Err(invalid_input("step must be parse, open, select, or select-broad").into()),
    };
    if !keep_table {
        drop(table.take());
    }
    drop(value);
    let elapsed = started.elapsed();
    let (hwm_kib, rss_kib) = memory_kib()?;
    if keep_table {
        std::hint::black_box(&table);
    } else {
        std::hint::black_box(&index);
    }
    println!(
        "step={step} hwm_mib={:.1} rss_mib={:.1} ms={:.1} approx_bytes={approx_bytes} selected={selected}",
        hwm_kib as f64 / 1024.0,
        rss_kib as f64 / 1024.0,
        elapsed.as_secs_f64() * 1_000.0
    );
    Ok(())
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command, path] if command == "fixture" => fixture(path),
        [command, step, path] if command == "measure" => measure(step, path),
        _ => Err(
            invalid_input("usage: bench_series_index fixture <path> | measure <step> <path>")
                .into(),
        ),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
