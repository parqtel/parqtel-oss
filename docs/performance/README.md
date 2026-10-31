# Performance

Every number here was measured against a running build and says which build. Where a
measurement showed an optimisation was not worth doing, that is recorded as well.

## Where to read more

| Page | Subject |
|---|---|
| [Hot-path optimisations](hot-path.md) | First round of ingest and scan changes |
| [Query evaluation](query-evaluation.md) | Label sets, streaming steps, prepared log queries |
| [Block and row-group pruning](block-pruning.md) | Statistics, bloom filters, projection |
| [Series table](series-index.md) | Per-block series index for metric blocks |
| [Log search](log-index.md) | Per-block skip index and two-phase scan for logs |
| [Compaction](compaction.md) | Merging blocks and keeping their indexes |
| [Durability and recovery](durability.md) | WAL, commit ordering, block-index repair |
| [Sizing](../operations/performance-sizing.md) | Memory and disk for your ingest rate |

## End-to-end ingest and query performance

Everything above measures one component in-process. This section measures the
running server, because that is where most of the remaining cost turned out to
be: the cost of a request is dominated by how data moves between the stages
(decode, buffer, WAL, flush, scan, evaluate), not by any one stage.

Raw outputs: `e2e_before.txt` / `e2e_after.txt` (three runs each) and
`perf_bench_before.txt` / `perf_bench_after.txt`.

### Method

- Machine: Apple M1, 8 cores, 8 GB RAM, release build (`lto = "fat"`).
- Driver: `parqtel-server/examples/e2e_bench.rs`, a Rust client that sends real
  OTLP/HTTP **protobuf** from 8 concurrent connections, then runs each read
  query with one client (no queueing, so latency is the cost of the query).
- Fixed workload so both builds read identical data: `--reqs 1200` is 2.4 M
  metric points (10 metrics x 2 000 series), 300 K log records and 150 K spans.
  Each read query is probed first and flagged if it returns nothing.
- Medians of three runs, each against a fresh data directory.

```bash
cargo build --release -p parqtel-server --bin parqtel --example e2e_bench
PARQTEL_BIND=127.0.0.1:18080 PARQTEL_DATA_DIR=/tmp/pq target/release/parqtel serve &
target/release/examples/e2e_bench --url http://127.0.0.1:18080 \
    --workers 8 --reqs 1200 --phases metrics,logs,traces,query --query-conc 1
```

### Results

| | before | after (first pass) | after, with series table and log index |
|---|---|---|---|
| metrics ingest | 0.62 M points/s | 1.98 M points/s | 1.85 M points/s |
| metrics ingest p99 | 55 ms | 17 ms | 16.8 ms |
| logs ingest | 0.10 M records/s | 1.80 M records/s | 1.59 M records/s |
| logs ingest p99 | 538 ms | 10.3 ms | 10.8 ms |
| traces ingest (OTLP/HTTP protobuf) | rejected (400) | 1.83 M spans/s | 1.66 M spans/s |
| single-series selector, p50 | 156 ms | 14.7 ms | 0.43 ms |
| `sum by (env) (m)`, p50 | 184 ms | 28 ms | 7.2 ms |
| `sum by (region) (rate(m[1m]))`, p50 | 182 ms | 30 ms | 8.7 ms |
| `avg(m)` over 1 h, p50 | not measured | 30 ms (thin build) | 6.7 ms |
| `topk(5, rate(m[1m]))`, p50 | 182 ms | 29 ms | 7.2 ms |
| instant `sum(m)`, p50 | 107 ms | 17 ms | 3.5 ms |
| log search, term in 20% of records, p50 | 284 ms | 102 ms | 24.7 ms |
| log search, term in one record, p50 | not measured | 29 ms (thin build) | 14.6 ms |
| peak RSS | 2.3 GB | 0.8 GB | 0.67 GB |

The last column is one run of the final fat-LTO build (`lto = "fat"`), the others
are the medians of three. The ingest figures differ from the middle column by
about as much as separate runs of one build differ from each other (the
thin-LTO builds measured 1.5-1.7 M logs/s both before and after the indexes);
building the indexes during a flush is not a visible cost.

In-process (`perf_bench`): full scan 12.9 M -> 101 M points/s, executor query
3.9 M -> 22.5 M points/s, Parquet flush 1.0 M -> 2.7 M points/s.

Two rows of the end-to-end run are **not comparable** and are left out of the
table on purpose. Trace search (0.04 ms) and label values (0.3 ms) were faster
before only because the old build rejected every protobuf trace and, after each
flush, emptied the memory buffer including rows that had just arrived; both
answered from almost nothing. They now answer from real data (21 ms and 3 ms).

With many small blocks (50 K rows, 36 to 47 files) the gain is the same shape:
selector 106 -> 15 ms, aggregation 138 -> 36 ms, ingest 0.64 -> 1.9 M points/s.

### What changed, and why it mattered

| Change | Effect |
|---|---|
| `LabelSet` is an immutable `Arc`-backed sorted list with a cached fingerprint and JSON; OTLP attributes resolve through a bounded interner | The label map was deep-copied at every hand-off and its JSON re-serialised for every row. Cloning is now a refcount bump |
| `DataPoint` boxes histogram/summary payloads (112 -> 32 bytes) | Every point paid for the rare large variant |
| Block flush groups points by series with a compact sort key and appends per-series Arrow columns once per series | Was a clone of every point into a `BTreeMap`, then per-row string work |
| Label matchers run inside the block scan, once per distinct series; `labels` is read as a dictionary; Parquet footers are cached | A one-series selector used to decode 100% of its metric and discard 99.95% |
| Log search rejects rows on timestamp, severity and body before decoding the other 16 columns | Late materialisation |
| Logs and traces: WAL records framed before the lock and written with one `write`; blocks encoded on a detached task | Was one `write` per row and an inline encode under the single ingest lock |
| Series grouping runs on the blocking pool without a map lookup per point | It parked an async worker for the length of a scan |
| gRPC passes the decoded request on instead of encoding and decoding it again | One full round trip per request on the default transport |

### Correctness problems found by measuring

Several were invisible to the existing tests and are described in the
changelog: the buffer losing fresh rows after each flush, WAL replay skipping the
record after the commit point, an out-of-order WAL commit across concurrent
flushes, protobuf traces over HTTP, gzip bodies, and a SIGTERM hang. The
crash-recovery check is `kill -9` under load followed by comparing rows
acknowledged with rows recovered. Across repeated runs on the final build every
acknowledged row of every signal is recovered. In one of six early runs the kill
landed between a block becoming durable and its WAL commit and the block's rows
came back a second time; blocks now record the WAL span they cover and startup
skips it (covered by a test, since that window is milliseconds wide).

### Tried and not adopted

- **mimalloc as the global allocator** improved single-series selector latency by
  roughly 25% and left total server CPU unchanged, but raised peak RSS from 1.55
  to 2.1 GB under concurrent load. For a deliberately small footprint that is the
  wrong trade, so it was reverted.
