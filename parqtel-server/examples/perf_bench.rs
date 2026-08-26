//! Standalone performance benchmark for hot paths.
//!
//! Run with: cargo run --release -p parqtel-server --example perf_bench
//!
//! Measures (median of N iterations):
//!   1. ingest   — OTLP JSON decode + memory buffer push
//!   2. flush    — buffer → Parquet write (encode + compress + fs)
//!   3. scan     — Scanner::scan across all seeded blocks on disk
//!   4. query    — full QueryExecutor::execute with matcher + downsample

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parqtel_core::{BlockConfig, BlockIndex, MemoryBuffer, Scanner};
use parqtel_ingest::decode::OtlpDecoder;
use parqtel_ingest::writer::BlockWriter;
use parqtel_query::executor::QueryExecutor;
use parqtel_query::plan::{AggregationOp, QueryPlan};
use tokio::sync::RwLock;

const BLOCKS: usize = 12;
const POINTS_PER_BLOCK: usize = 50_000;
const SERIES: usize = 100; // distinct label sets per block
const INGEST_BATCHES: usize = 40;
const POINTS_PER_BATCH: usize = 2_500;
const CONCURRENCY: usize = 8;
const ITERS: usize = 5;

fn bench_config(dir: &Path) -> BlockConfig {
    BlockConfig {
        data_dir: dir.to_path_buf(),
        max_rows_per_block: POINTS_PER_BLOCK,
        ..Default::default()
    }
}

/// Generate a deterministic JSON OTLP metrics payload with `n` data points
/// spread over `SERIES` label sets.
fn gen_batch(base_ts: i64, n: usize) -> String {
    let mut s = String::with_capacity(n * 90);
    s.push_str(r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"bench.cpu","gauge":{"dataPoints":["#);
    for i in 0..n {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            r#"{{"timeUnixNano":{},"asDouble":{},"attributes":[{{"key":"host","value":{{"stringValue":"h{}"}}}},{{"key":"env","value":{{"stringValue":"prod"}}}}]}}"#,
            base_ts + i as i64 * 1_000_000,
            (i % 1000) as f64 / 10.0,
            i % SERIES,
        ));
    }
    s.push_str("]}}]}]}]}");
    s
}

fn decode(json: &str) -> Vec<parqtel_core::Metric> {
    let v: serde_json::Value = serde_json::from_slice(json.as_bytes()).unwrap();
    OtlpDecoder::decode_metrics_json(v).unwrap()
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort_unstable();
    v[v.len() / 2]
}

fn report(name: &str, d: Duration, ops: u64) -> f64 {
    println!(
        "{:<8} {:>10.2} ms/op  {:>10.2} µs/point  {:>14.0} points/s",
        name,
        d.as_secs_f64() * 1e3,
        d.as_secs_f64() * 1e6 / ops as f64,
        ops as f64 / d.as_secs_f64(),
    );
    ops as f64 / d.as_secs_f64()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let config = bench_config(dir.path());
    std::fs::create_dir_all(&config.data_dir).unwrap();

    // ── Seed disk blocks via the real ingest flush path ───────────────────
    println!("Seeding {} blocks × {} points…", BLOCKS, POINTS_PER_BLOCK);
    let mut seeder = BlockWriter::new(config.clone());
    let mut index = BlockIndex::new(&config.data_dir);
    for b in 0..BLOCKS {
        let base = 1_700_000_000_000_000_000i64 + (b as i64) * 3_600_000_000_000;
        for m in decode(&gen_batch(base, POINTS_PER_BLOCK)) {
            seeder.push(m).unwrap();
        }
        index.add(seeder.flush().unwrap()).unwrap();
    }

    // ── Setup ─────────────────────────────────────────────────────────────
    let index = Arc::new(RwLock::new(index));
    let log_index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/bench-logs"))));
    let trace_index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/bench-traces"))));
    let buffer = MemoryBuffer::new();
    let executor =
        QueryExecutor::with_trace_index(index.clone(), log_index, trace_index, buffer.clone());
    let flush_dir = dir.path().join("flush");
    let mut writer = BlockWriter::new(bench_config(&flush_dir));

    let batches: Vec<String> = (0..INGEST_BATCHES)
        .map(|i| {
            gen_batch(
                2_000_000_000_000_000_000 + i as i64 * 10_000_000_000,
                POINTS_PER_BATCH,
            )
        })
        .collect();
    decode(&batches[0]); // warmup decode paths

    // ── 0. Concurrent service ingest (mutex + flush contention) ───────────
    // CONCURRENCY workers push batches through the real IngestionService;
    // each worker accumulates exactly one block worth of points, forcing a
    // Parquet flush at the end of every iteration.
    use bytes::Bytes as BenchBytes;
    let (btx, brx) = tokio::sync::mpsc::unbounded_channel();
    let svc = Arc::new(
        parqtel_ingest::service::IngestionService::new(bench_config(&flush_dir), btx)
            .with_memory_buffer(buffer.clone()),
    );
    drop(brx);
    svc.ingest_json(BenchBytes::from(batches[0].clone()))
        .await
        .unwrap(); // warmup

    println!(
        "\n=== concurrent service ingest ({} workers x {} batches x {} pts, 1 flush each) ===",
        CONCURRENCY, INGEST_BATCHES, POINTS_PER_BATCH
    );
    let submitted = CONCURRENCY * INGEST_BATCHES * POINTS_PER_BATCH;
    let accepted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut times = Vec::new();
    for _ in 0..ITERS {
        accepted.store(0, std::sync::atomic::Ordering::Relaxed);
        let t0 = Instant::now();
        let mut handles = Vec::new();
        for _w in 0..CONCURRENCY {
            let svc = svc.clone();
            let acc = accepted.clone();
            let payload: Vec<BenchBytes> = batches.iter().cloned().map(BenchBytes::from).collect();
            handles.push(tokio::spawn(async move {
                for j in payload {
                    let n = svc.ingest_json(j).await.unwrap();
                    acc.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        times.push(t0.elapsed());
    }
    report("svc-ingest", median(times), submitted as u64);
    println!(
        "             accepted {}/{} points (drops = old overflow behavior)",
        accepted.load(std::sync::atomic::Ordering::Relaxed),
        submitted
    );

    // ── 1. Ingest throughput ──────────────────────────────────────────────
    println!(
        "\n=== ingest (JSON decode + buffer push, {}×{} pts) ===",
        INGEST_BATCHES, POINTS_PER_BATCH
    );
    let mut times = Vec::new();
    for _ in 0..ITERS {
        let t0 = Instant::now();
        for json in &batches {
            let metrics = decode(json);
            for m in &metrics {
                buffer.push_metrics(&m.name, &m.data_points).await;
            }
        }
        times.push(t0.elapsed());
    }
    let rate_ingest = report(
        "ingest",
        median(times),
        (INGEST_BATCHES * POINTS_PER_BATCH) as u64,
    );

    // ── 2. Flush (Parquet encode + compress + fs) ─────────────────────────
    println!("\n=== flush ({} pts → Parquet) ===", POINTS_PER_BLOCK);
    let seed_json = gen_batch(3_000_000_000_000_000_000, POINTS_PER_BLOCK);
    let mut times = Vec::new();
    for _ in 0..ITERS {
        for m in decode(&seed_json) {
            writer.push(m).unwrap();
        }
        let t0 = Instant::now();
        writer.flush().unwrap();
        times.push(t0.elapsed());
    }
    let rate_flush = report("flush", median(times), POINTS_PER_BLOCK as u64);

    // ── 3. Raw block scan ─────────────────────────────────────────────────
    println!("\n=== scan (all {} blocks, full range) ===", BLOCKS);
    let blocks = index.read().await.query(0, i64::MAX, Some("bench.cpu"));
    assert_eq!(blocks.len(), BLOCKS);
    let mut times = Vec::new();
    for _ in 0..ITERS {
        let t0 = Instant::now();
        let pts = Scanner::scan(blocks.clone(), "bench.cpu".into(), 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(pts.len(), BLOCKS * POINTS_PER_BLOCK);
        times.push(t0.elapsed());
    }
    let rate_scan = report("scan", median(times), (BLOCKS * POINTS_PER_BLOCK) as u64);

    // ── 3b. Narrow-range scan (index + row-group statistics pruning) ──────
    // Middle half of block 5's data: index prunes the other 11 files,
    // row-group timestamp statistics prune ~half the rows of the scanned file.
    println!("\n=== scan narrow (1/12 blocks, middle half of its data) ===");
    let seed0 = 1_700_000_000_000_000_000i64;
    let block_span = 3_600_000_000_000i64; // spacing used when seeding
    let data_span = POINTS_PER_BLOCK as i64 * 1_000_000; // points are 1 ms apart
    let n_start = seed0 + 5 * block_span + data_span / 4;
    let n_end = n_start + data_span / 2 - 1; // scanner bounds are inclusive
    let mut times = Vec::new();
    for _ in 0..ITERS {
        let t0 = Instant::now();
        let pts = Scanner::scan(blocks.clone(), "bench.cpu".into(), n_start, n_end)
            .await
            .unwrap();
        assert_eq!(pts.len() as usize, POINTS_PER_BLOCK / 2);
        times.push(t0.elapsed());
    }
    report("scan-narrow", median(times), (POINTS_PER_BLOCK / 2) as u64);

    // ── 4. Full query execution ───────────────────────────────────────────
    println!("\n=== query execute (full range, avg downsample @60s) ===");
    let q_start = 1_700_000_000_000_000_000i64;
    let q_end = q_start + BLOCKS as i64 * 3_600_000_000_000;
    let mut times = Vec::new();
    for _ in 0..ITERS {
        let plan = QueryPlan::new(
            "bench.cpu".into(),
            vec![],
            q_start,
            q_end,
            Some(60_000_000_000),
            10_000,
            10_000_000,
            Some(AggregationOp::Avg),
            None,
        )
        .unwrap();
        let t0 = Instant::now();
        let res = executor.execute(plan).await.unwrap();
        assert_eq!(res.series.len(), SERIES);
        times.push(t0.elapsed());
    }
    let rate_query = report("query", median(times), (BLOCKS * POINTS_PER_BLOCK) as u64);

    // ── 4b. Narrow query execution (pruning through full executor) ────────
    println!("\n=== query execute (narrow range, avg downsample @60s) ===");
    let mut times = Vec::new();
    for _ in 0..ITERS {
        let plan = QueryPlan::new(
            "bench.cpu".into(),
            vec![],
            n_start,
            n_end,
            Some(60_000_000_000),
            10_000,
            10_000_000,
            Some(AggregationOp::Avg),
            None,
        )
        .unwrap();
        let t0 = Instant::now();
        executor.execute(plan).await.unwrap();
        times.push(t0.elapsed());
    }
    let rate_query_narrow = report(
        "query-narrow",
        median(times),
        (BLOCKS * POINTS_PER_BLOCK / 2) as u64,
    );

    // ── Summary ───────────────────────────────────────────────────────────
    println!("\n--- summary (points/s, higher is better) ---");
    println!("ingest : {:.0}", rate_ingest);
    println!("flush  : {:.0}", rate_flush);
    println!("scan   : {:.0}", rate_scan);
    println!("query  : {:.0}", rate_query);
    println!("q-narr : {:.0}", rate_query_narrow);
}
