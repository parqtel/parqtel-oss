//! End-to-end load driver for a running `parqtel` server.
//!
//! Sends real OTLP/HTTP protobuf traffic from many concurrent clients and then
//! issues a mixed read workload, reporting throughput and latency percentiles.
//! It talks to the server over the network, so it measures the whole request
//! path (HTTP, decode, buffer, WAL, flush, query) rather than one component.
//!
//! ```text
//! parqtel --bind 127.0.0.1:18080 --data-dir /tmp/pq &
//! cargo run --release -p parqtel-server --example e2e_bench -- \
//!     --url http://127.0.0.1:18080 --workers 8 --secs 15
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parqtel_ingest::otel::collector::logs::v1::ExportLogsServiceRequest;
use parqtel_ingest::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use parqtel_ingest::otel::collector::trace::v1::ExportTraceServiceRequest;
use parqtel_ingest::otel::common::v1::{any_value, AnyValue, KeyValue};
use parqtel_ingest::otel::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use parqtel_ingest::otel::metrics::v1::{
    metric, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use parqtel_ingest::otel::resource::v1::Resource;
use parqtel_ingest::otel::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;

#[derive(Clone)]
struct Args {
    url: String,
    workers: usize,
    secs: u64,
    series: usize,
    metrics: usize,
    points_per_req: usize,
    logs_per_req: usize,
    spans_per_req: usize,
    query_secs: u64,
    phases: String,
    /// Stop an ingest phase after this many requests (0 = run for `secs`).
    reqs: u64,
    /// Concurrent clients for the read workload.
    query_conc: usize,
}

fn parse_args() -> Args {
    let mut a = Args {
        url: "http://127.0.0.1:18080".into(),
        workers: 8,
        secs: 10,
        series: 2000,
        metrics: 10,
        points_per_req: 5000,
        logs_per_req: 1000,
        spans_per_req: 500,
        query_secs: 10,
        phases: "metrics,logs,traces,query".into(),
        reqs: 0,
        query_conc: 8,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let v = argv.get(i + 1).cloned().unwrap_or_default();
        match argv[i].as_str() {
            "--url" => a.url = v,
            "--workers" => a.workers = v.parse().unwrap(),
            "--secs" => a.secs = v.parse().unwrap(),
            "--series" => a.series = v.parse().unwrap(),
            "--metrics" => a.metrics = v.parse().unwrap(),
            "--points-per-req" => a.points_per_req = v.parse().unwrap(),
            "--logs-per-req" => a.logs_per_req = v.parse().unwrap(),
            "--spans-per-req" => a.spans_per_req = v.parse().unwrap(),
            "--query-secs" => a.query_secs = v.parse().unwrap(),
            "--phases" => a.phases = v,
            "--reqs" => a.reqs = v.parse().unwrap(),
            "--query-conc" => a.query_conc = v.parse().unwrap(),
            other => panic!("unknown arg {other}"),
        }
        i += 2;
    }
    a
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn kv(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(val.into())),
        }),
    }
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![
            kv("service.name", service),
            kv("k8s.namespace.name", "prod"),
            kv("k8s.pod.name", &format!("{service}-pod-0")),
        ],
        dropped_attributes_count: 0,
    }
}

/// One scrape of `series_per_req` series for `metric_idx`, at tick `tick`.
fn metrics_request(
    metric_idx: usize,
    series_lo: usize,
    series_hi: usize,
    ts: u64,
    tick: u64,
) -> Vec<u8> {
    let points: Vec<NumberDataPoint> = (series_lo..series_hi)
        .map(|s| NumberDataPoint {
            attributes: vec![
                kv("host", &format!("h{s}")),
                kv("env", if s % 2 == 0 { "prod" } else { "stage" }),
                kv("region", &format!("r{}", s % 4)),
            ],
            start_time_unix_nano: 0,
            time_unix_nano: ts,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(
                (tick as f64) * 1.5 + (s % 97) as f64,
            )),
        })
        .collect();
    let data = if metric_idx & 1 == 0 {
        metric::Data::Gauge(Gauge {
            data_points: points,
        })
    } else {
        metric::Data::Sum(Sum {
            data_points: points,
            aggregation_temporality: 2,
            is_monotonic: true,
        })
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource(&format!("svc-{}", metric_idx % 5))),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: format!("bench_m{metric_idx}"),
                    description: String::new(),
                    unit: String::new(),
                    data: Some(data),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

fn logs_request(n: usize, ts: u64, seq: u64) -> Vec<u8> {
    let recs: Vec<LogRecord> = (0..n)
        .map(|i| {
            let sev = [9, 9, 9, 13, 17][i % 5];
            LogRecord {
                time_unix_nano: ts + i as u64,
                observed_time_unix_nano: ts + i as u64,
                severity_number: sev,
                severity_text: ["INFO", "INFO", "INFO", "WARN", "ERROR"][i % 5].into(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(format!(
                        "request {} handled for user u{} path=/api/v{}/items status={} took {}ms",
                        seq * n as u64 + i as u64,
                        i % 500,
                        i % 3,
                        if sev == 17 { 500 } else { 200 },
                        (i * 7) % 900
                    ))),
                }),
                attributes: vec![kv("http.method", "GET"), kv("pod", &format!("p{}", i % 20))],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![],
                span_id: vec![],
            }
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource(&format!("svc-{}", seq % 5))),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: recs,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

fn traces_request(n: usize, ts: u64, seq: u64) -> Vec<u8> {
    let spans: Vec<Span> = (0..n)
        .map(|i| {
            let mut tid = vec![0u8; 16];
            tid[..8].copy_from_slice(&(seq * 1000 + (i / 5) as u64).to_be_bytes());
            let mut sid = vec![0u8; 8];
            sid.copy_from_slice(&(seq * 100_000 + i as u64 + 1).to_be_bytes());
            Span {
                trace_id: tid,
                span_id: sid,
                trace_state: String::new(),
                parent_span_id: vec![],
                name: format!("op-{}", i % 12),
                kind: if i % 5 == 0 { 2 } else { 3 },
                start_time_unix_nano: ts + i as u64 * 1000,
                end_time_unix_nano: ts + i as u64 * 1000 + 1_000_000 + (i as u64 % 50) * 100_000,
                attributes: vec![kv("http.route", &format!("/r{}", i % 8))],
                dropped_attributes_count: 0,
                events: vec![],
                dropped_events_count: 0,
                links: vec![],
                dropped_links_count: 0,
                status: Some(Status {
                    message: String::new(),
                    code: if i % 23 == 0 { 2 } else { 1 },
                }),
                flags: 0,
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource(&format!("svc-{}", seq % 5))),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

static MAX_TS: AtomicU64 = AtomicU64::new(0);

struct Stats {
    lat_us: Vec<u64>,
    items: u64,
    errors: u64,
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx] as f64 / 1000.0
}

fn report(name: &str, mut s: Stats, wall: Duration) {
    s.lat_us.sort_unstable();
    println!(
        "{:<22} reqs={:<7} err={:<4} items/s={:<11.0} p50={:>8.2}ms p95={:>8.2}ms p99={:>8.2}ms max={:>8.2}ms min={:>8.2}ms",
        name,
        s.lat_us.len(),
        s.errors,
        s.items as f64 / wall.as_secs_f64(),
        pct(&s.lat_us, 0.50),
        pct(&s.lat_us, 0.95),
        pct(&s.lat_us, 0.99),
        pct(&s.lat_us, 1.0),
        pct(&s.lat_us, 0.0),
    );
}

async fn ingest_phase(
    args: &Args,
    client: &reqwest::Client,
    path: &'static str,
    items_per_req: usize,
    build: Arc<dyn Fn(u64, u64) -> Vec<u8> + Send + Sync>,
    max_reqs: u64,
) -> (Stats, Duration) {
    let deadline = Instant::now() + Duration::from_secs(if max_reqs > 0 { 600 } else { args.secs });
    let seq = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..args.workers {
        let client = client.clone();
        let url = format!("{}{}", args.url, path);
        let seq = seq.clone();
        let build = build.clone();
        handles.push(tokio::spawn(async move {
            let mut st = Stats {
                lat_us: Vec::new(),
                items: 0,
                errors: 0,
            };
            while Instant::now() < deadline {
                let n = seq.fetch_add(1, Ordering::Relaxed);
                if max_reqs > 0 && n >= max_reqs {
                    break;
                }
                let body = build(n, now_ns());
                let t = Instant::now();
                let r = client
                    .post(&url)
                    .header("content-type", "application/x-protobuf")
                    .body(body)
                    .send()
                    .await;
                match r {
                    Ok(resp) if resp.status().is_success() => {
                        let _ = resp.bytes().await;
                        st.lat_us.push(t.elapsed().as_micros() as u64);
                        st.items += items_per_req as u64;
                    }
                    Ok(resp) => {
                        if st.errors == 0 {
                            eprintln!("HTTP {} on {}", resp.status(), url);
                        }
                        st.errors += 1;
                    }
                    Err(e) => {
                        if st.errors == 0 {
                            eprintln!("send error: {e}");
                        }
                        st.errors += 1;
                    }
                }
            }
            st
        }));
    }
    let mut all = Stats {
        lat_us: Vec::new(),
        items: 0,
        errors: 0,
    };
    for h in handles {
        let s = h.await.unwrap();
        all.lat_us.extend(s.lat_us);
        all.items += s.items;
        all.errors += s.errors;
    }
    (all, t0.elapsed())
}

async fn query_phase(args: &Args, client: &reqwest::Client) {
    let max_ts = MAX_TS.load(Ordering::Relaxed);
    let end = if max_ts > 0 {
        max_ts / 1_000_000_000 + 1
    } else {
        now_ns() / 1_000_000_000
    };
    let wall_end = now_ns() / 1_000_000_000 + 60;
    let start_15m = end - 900;
    let start_1h = end - 3600;
    let q = |query: &str, start: u64, step: &str| -> String {
        format!(
            "{}/api/v1/query_range?query={}&start={}&end={}&step={}",
            args.url,
            urlencoding::encode(query),
            start,
            end,
            step
        )
    };
    let instant = |query: &str| -> String {
        format!(
            "{}/api/v1/query?query={}&time={}",
            args.url,
            urlencoding::encode(query),
            end
        )
    };
    let logs = |search: &str| -> String {
        format!(
            "{}/api/v1/logs?query=%7B%7D&start={}&end={}&limit=100&search={}",
            args.url,
            wall_end - 3660,
            wall_end,
            urlencoding::encode(search)
        )
    };
    let queries: Vec<(&str, String)> = vec![
        (
            "selector-one-series",
            q("bench_m0{host=\"h17\"}", start_15m, "15s"),
        ),
        ("sum-by-env", q("sum by (env) (bench_m0)", start_15m, "15s")),
        (
            "rate-sum-by-region",
            q("sum by (region) (rate(bench_m1[1m]))", start_15m, "15s"),
        ),
        ("avg-all-1h", q("avg(bench_m2)", start_1h, "60s")),
        (
            "topk-rate",
            q("topk(5, rate(bench_m3[1m]))", start_15m, "30s"),
        ),
        ("instant-sum", instant("sum(bench_m4)")),
        ("logs-search", logs("status=500")),
        // A needle that occurs in a single record of the whole dataset.
        ("logs-search-rare", logs("request 150000 handled")),
        (
            "traces-search",
            format!(
                "{}/v1/traces/search?start={}&end={}&limit=100",
                args.url,
                wall_end - 3660,
                wall_end
            ),
        ),
        (
            "label-values",
            format!("{}/api/v1/label/host/values", args.url),
        ),
    ];
    for (name, url) in queries {
        // Sanity: a query that returns nothing measures nothing.
        let probe = client.get(&url).send().await.unwrap().text().await.unwrap();
        let v: serde_json::Value = serde_json::from_str(&probe).unwrap_or_default();
        let n = match &v["data"] {
            serde_json::Value::Array(a) => a.len(),
            d => d["result"].as_array().map(|a| a.len()).unwrap_or(0),
        };
        println!(
            "   [{name}] result_items={n} bytes={}{}",
            probe.len(),
            if n == 0 && probe.len() < 300 {
                "  <-- EMPTY"
            } else {
                ""
            }
        );
        let deadline = Instant::now() + Duration::from_secs(args.query_secs.min(4));
        let t0 = Instant::now();
        let mut handles = Vec::new();
        let conc = args.query_conc.max(1);
        for _ in 0..conc {
            let client = client.clone();
            let url = url.clone();
            handles.push(tokio::spawn(async move {
                let mut st = Stats {
                    lat_us: Vec::new(),
                    items: 0,
                    errors: 0,
                };
                while Instant::now() < deadline {
                    let t = Instant::now();
                    match client.get(&url).send().await {
                        Ok(r) if r.status().is_success() => {
                            let b = r.bytes().await.map(|b| b.len()).unwrap_or(0);
                            st.lat_us.push(t.elapsed().as_micros() as u64);
                            st.items += 1;
                            let _ = b;
                        }
                        Ok(r) => {
                            if st.errors == 0 {
                                eprintln!("query {name}: HTTP {}", r.status());
                            }
                            st.errors += 1;
                        }
                        Err(_) => st.errors += 1,
                    }
                }
                st
            }));
        }
        let mut all = Stats {
            lat_us: Vec::new(),
            items: 0,
            errors: 0,
        };
        for h in handles {
            let s = h.await.unwrap();
            all.lat_us.extend(s.lat_us);
            all.items += s.items;
            all.errors += s.errors;
        }
        report(name, all, t0.elapsed());
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.workers * 2)
        .build()
        .unwrap();
    println!(
        "== e2e_bench url={} workers={} secs={} series={} metrics={} points/req={}",
        args.url, args.workers, args.secs, args.series, args.metrics, args.points_per_req
    );

    // Metrics: each request is one metric × a slice of its series at one tick.
    let slices = args.series.div_ceil(args.points_per_req.min(args.series));
    let per_req = args.points_per_req.min(args.series);
    let nmetrics = args.metrics;
    let series = args.series;
    let base_ts = now_ns() - 10 * 60 * 1_000_000_000;

    if args.phases.contains("metrics") {
        let build: Arc<dyn Fn(u64, u64) -> Vec<u8> + Send + Sync> = Arc::new(move |n, _now| {
            let slice = (n as usize) % slices;
            let metric_idx = ((n as usize) / slices) % nmetrics;
            let tick = (n as usize) / (slices * nmetrics);
            let lo = slice * per_req;
            let hi = (lo + per_req).min(series);
            let ts = base_ts + tick as u64 * 1_000_000_000;
            MAX_TS.fetch_max(ts, Ordering::Relaxed);
            metrics_request(metric_idx, lo, hi, ts, tick as u64)
        });
        let (st, wall) =
            ingest_phase(&args, &client, "/v1/metrics", per_req, build, args.reqs).await;
        report("ingest-metrics(points)", st, wall);
    }
    if args.phases.contains("logs") {
        let n = args.logs_per_req;
        let build: Arc<dyn Fn(u64, u64) -> Vec<u8> + Send + Sync> =
            Arc::new(move |seq, now| logs_request(n, now, seq));
        let (st, wall) = ingest_phase(&args, &client, "/v1/logs", n, build, args.reqs / 4).await;
        report("ingest-logs(records)", st, wall);
    }
    if args.phases.contains("traces") {
        let n = args.spans_per_req;
        let build: Arc<dyn Fn(u64, u64) -> Vec<u8> + Send + Sync> =
            Arc::new(move |seq, now| traces_request(n, now, seq));
        let (st, wall) = ingest_phase(&args, &client, "/v1/traces", n, build, args.reqs / 4).await;
        report("ingest-traces(spans)", st, wall);
    }
    if args.phases.contains("query") {
        println!(
            "-- read workload ({} concurrent clients per query)",
            args.query_conc
        );
        query_phase(&args, &client).await;
    }
}
