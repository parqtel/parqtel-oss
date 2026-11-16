# Memory-efficient Parqtel metric scans: inverted indexes, Parquet, and SereneDB

Research note · 2026-11-15

Scope: primary-source review and repository-code assessment. No product code or deployment changes were made. UAT measurements below were supplied by the operator and were not independently reproduced.

## Conclusion

Parqtel already maintains a per-block inverted index. It maps label name/value pairs to series IDs, then turns selected series into Parquet row selections and reads four value/time columns. That helps selective queries; it does not bound memory for broad, long-range scans. [SeriesIndex](../../crates/parqtel-core/src/storage/series_index.rs#L270-L353) · [indexed scan](../../crates/parqtel-core/src/storage/scanner/metrics.rs#L467-L498)

Current code expands each compressed footer into a full in-memory `SeriesIndex`, interns label sets in a process-wide cache, accumulates matching rows as `DataPoint`s, and applies result limits later. Each `scan_filtered` call also creates its own 16-permit semaphore. Concurrent UI requests can multiply these per-call budgets; serial alert queries can overlap UI work. These are code-backed risk factors, not proof of the allocator that caused either OOMKill. [footer load](../../crates/parqtel-core/src/storage/scanner/block.rs#L107-L194) · [label interner](../../crates/parqtel-core/src/models/labels.rs#L264-L325) · [scan scheduling](../../crates/parqtel-core/src/storage/scanner/metrics.rs#L191-L232) · [query materialization](../../crates/parqtel-query/src/executor/metrics.rs#L254-L310)

The smallest next step is a process-wide scan/index-load budget plus phase-level memory instrumentation. Change postings representation only if measurements show index expansion dominates. Stream query results only if raw-point accumulation dominates. Compaction is not a fix for the reported third OOM: the container died before the first scheduled pass, and the 1M-row blocks are excluded by current merge thresholds.

## UAT evidence and limits

### Block shape and index sizes

Reported new blocks contain about 1,000,300 rows and 283,589 series. Total Parquet size is about 4.92 MB; the `zstd+base64` footer value is 2.27 MB, while its raw JSON is about 43.26 MB. The old same-shape file was about 51.4 MB, including a 5.4 MB labels column and about 43.2 MB raw footer. The current `approx_bytes` estimate is 105.3 MB (100.4 MiB) per block. The UAT also reports `label_interner_capacity=2,097,152` label sets. These figures describe different layers: compressed file bytes, decompressed JSON, a heuristic for the expanded Rust structure, and a count—not byte limit—of interned sets.

Two 100.4 MiB estimates total about 200.8 MiB, already above the 192 MiB footer-cache budget. A 5–15-minute range over blocks about two minutes long spans roughly 3–8 blocks, or about 0.3–0.8 GiB of index estimates if all corresponding indexes remain live. Sixteen live indexes would be about 1.6 GiB. These are rough concurrency bounds, not RSS predictions; returned points, parse-time temporaries, in-memory buffers, Parquet batches, and file cache add other memory.

### Restart and OOM timeline

- Current `restartCount=3` includes one operator-caused restart: debug cleanup sent `SIGTERM` to the app through a shared PID namespace. The operator owns that incident. The later OOMKills occurred at 12:01:04 and 12:08:47.
- The third container was OOM-killed less than eight minutes after startup. cAdvisor reported `container_memory_usage_bytes` rising from 2,116 MiB at 12:08:15 to 4,063.8 MiB at 12:08:25/40, against a 4 GiB limit. Reported working set was 3,057 MiB, RSS 2,749 MiB, and cache 1,025 MiB. Worker-node `MemoryPressure` stayed `False`.
- Third-container CPU peaked near 1.76 cores under a 2-core limit; maximum throttling was about 37%. CPU pressure may affect latency, but it does not explain the memory attribution.
- At the 12:35Z follow-up, the latest container is Ready (`restartCount=3`, started 12:09Z), with 1.88 GiB cgroup memory and a 755K-point buffer. The OTel exporter remains healthy: queue 12/2,000, failures 0, and 92.4M points sent over 9,784 seconds, about 9.45K points/second.
- The compactor waits one full configured interval before its first pass. The default interval is 3,600 seconds, so it could not have compacted during a container lifetime under eight minutes. [compactor loop](../../crates/parqtel-core/src/storage/compactor.rs#L65-L97) · [default interval](../../crates/parqtel-core/src/config/storage.rs#L56-L69)

The third OOM is consistent with a query/index/materialization spike: memory rose rapidly, scan paths retain large per-block indexes and point vectors, and the interner may retain label sets after block-cache eviction. Compaction had not started. Treat this as a high-confidence working hypothesis, not a proven root cause. Allocator profiling was disabled, so no exact allocation stack is available. The cAdvisor RSS and cache figures also mean heap-only attribution is not justified.

## What primary sources establish

### Prometheus: block indexes and lazy postings

Prometheus documents a per-block index file that maps metric names and labels to series in separate chunk files, with background compaction of its two-hour blocks. Its current reader opens the index through a memory-mapped file and retains sparse postings-offset entries—every 32nd label value, plus boundaries—to save memory. The postings API exposes `Next` and `Seek`; intersections advance iterators rather than first constructing one flat result vector. [Storage layout and compaction](https://prometheus.io/docs/prometheus/latest/storage/#on-disk-layout) · [memory-mapped reader and sparse offsets](https://github.com/prometheus/prometheus/blob/main/tsdb/index/index.go#L1004-L1088) · [postings iterators and intersection](https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings.go#L552-L667)

This is evidence that persisted, offset-addressable postings can avoid rebuilding every posting as a heap `HashMap<String, HashMap<String, Vec<id>>>`. It is not a claim that memory-mapped pages are free: touched pages still consume resident/page-cache memory. Prometheus also uses its own TSDB block format, not Parquet.

### Parquet and Arrow: page pruning is not a series index

Parquet’s optional page index stores per-page column statistics and row/page offsets so readers can skip pages. The specification explicitly lists a secondary index over arbitrary unsorted keys as a non-goal. Parquet Bloom filters can cheaply answer “definitely absent” for a column chunk, but they are not per-series postings. [Page Index non-goals](https://parquet.apache.org/docs/file-format/pageindex/#non-goals) · [Bloom Filter](https://parquet.apache.org/docs/file-format/bloomfilter/#problem-statement)

Arrow’s Parquet reader supports projection and `RowSelection`; row selections avoid fetching rejected rows into memory. Its `with_batch_size` setting controls output `RecordBatch` size as a hint and does not cap total query memory. Parqtel already uses row selection and projection on indexed blocks. Those mechanisms lower decoded I/O and batch memory, but they cannot bound `MetricScan.points` after the scanner collects every matched row. [Arrow `with_row_selection`](https://docs.rs/parquet/60.1.0/parquet/arrow/arrow_reader/struct.ArrowReaderBuilder.html#method.with_row_selection) · [Arrow `with_batch_size`](https://docs.rs/parquet/60.1.0/parquet/arrow/arrow_reader/struct.ArrowReaderBuilder.html#method.with_batch_size)

DataFusion illustrates a shared execution-memory budget: large, growing operators reserve memory and spill or fail when the pool is full. Its source explicitly warns that source operators and flowing `RecordBatch`es are not tracked automatically; consumers must account for batches they retain. This supports a shared budget for Parqtel’s large allocations, but not a claim that a pool alone would cap every Parquet/query allocation. [DataFusion memory-pool design](https://github.com/apache/datafusion/blob/main/datafusion/execution/src/memory_pool/mod.rs#L38-L68)

### SereneDB: verified project and relevant analogy

The matching project is **SereneDB**, [`serenedb/serenedb`](https://github.com/serenedb/serenedb), not a metrics TSDB. Its official README describes a search-and-analytics database powered by IResearch and says it can query Parquet directly. GitHub also has an unrelated project named `SereneDB`, a browser vector-retrieval engine; it is not the relevant source here. [SereneDB README](https://github.com/serenedb/serenedb#readme) · [unrelated name collision](https://github.com/kstyagi23/SereneDB#readme)

SereneDB’s external-data documentation says an inverted index can be built over Parquet while source files remain in place; index creation reads only needed columns and pushes projection/pruning into the reader. Its index is a separate postings structure, refreshed with `REINDEX` when external data changes. IResearch’s `PostingsReader` opens separate posting inputs and returns posting iterators; it also exposes mapped-memory accounting. This is useful evidence for a separate, lazily accessed index design, not proof of Parqtel performance or a drop-in implementation. [External-data indexing](https://github.com/serenedb/serenedb/blob/main/docs/sql/indexes/inverted/external-data.md#L10-L10) · [projection and reindexing](https://github.com/serenedb/serenedb/blob/main/docs/sql/indexes/inverted/external-data.md#L60-L132) · [IResearch posting inputs and mapped-memory accounting](https://github.com/serenedb/serenedb/blob/main/iresearch/formats/posting/reader.hpp#L38-L66) · [posting inputs opened for random access](https://github.com/serenedb/serenedb/blob/main/iresearch/formats/posting/reader.hpp#L97-L115) · [iterator construction](https://github.com/serenedb/serenedb/blob/main/iresearch/formats/posting/reader.hpp#L253-L267)

SereneDB’s maintenance guide also uses bounded, similar-sized segment compaction and treats row groups as scan work units. Those are relevant fanout/scheduling ideas. Its write-memory limit is not a query-memory limit, and its search segment layout differs from Parqtel’s immutable metric blocks. [SereneDB maintenance](https://github.com/serenedb/serenedb/blob/main/docs/sql/indexes/inverted/maintenance.md#L34-L50) · [row-group scheduling](https://github.com/serenedb/serenedb/blob/main/docs/sql/indexes/inverted/maintenance.md#L92-L98)

## Parqtel code-backed risk factors

### Footer compression does not bound index residency

Parqtel serializes the series table as zstd-compressed, base64 JSON. On a cold open, it decodes the footer, decompresses into a JSON buffer, deserializes `SeriesTable`, then constructs `SeriesIndex`. `from_table` interns labels and eagerly builds every series entry and all nested postings maps. The footer text is removed from cached Parquet metadata only after this work. The reported `approx_bytes` value is a formula, not allocator accounting; cold-load intermediates can raise peak above the steady estimate. [footer encoding/decoding](../../crates/parqtel-core/src/storage/series_index.rs#L143-L178) · [eager index and estimate](../../crates/parqtel-core/src/storage/series_index.rs#L290-L353) · [footer stripping after parse](../../crates/parqtel-core/src/storage/scanner/block.rs#L107-L194)

The 192 MiB budget counts cache entries using Parquet metadata size plus `SeriesIndex::approx_bytes`. It is not a process-wide resident-memory limit: active scans hold `Arc` clones after cache eviction, and concurrent cache misses can build the same block index because loading occurs outside the cache lock. [cache budget](../../crates/parqtel-core/src/storage/scanner/block.rs#L37-L41) · [cache references and cold-load path](../../crates/parqtel-core/src/storage/scanner/block.rs#L230-L264)

### Process-wide label interning can outlive block-cache entries

`SeriesIndex::from_table` passes every series label set through `LabelInterner::global`; eligible sets are interned. `LabelSet` is an `Arc` wrapper, and each interner shard stores its own `LabelSet` clone. Evicting a `SeriesIndex` therefore does not necessarily free its label data: the process-wide interner may retain its clone until that shard is cleared; other live `Arc`s can retain it longer. [series interning](../../crates/parqtel-core/src/storage/series_index.rs#L290-L328) · [`LabelSet` ownership](../../crates/parqtel-core/src/models/labels.rs#L21-L31) · [interner shards and capacity](../../crates/parqtel-core/src/models/labels.rs#L264-L325) · [stored clones and shard clearing](../../crates/parqtel-core/src/models/labels.rs#L340-L377)

The UAT-reported capacity of 2,097,152 sets divides across 16 shards, or 131,072 entries per shard. On the next new set after a shard reaches that threshold, it clears the shard wholesale. Sets over 32 pairs bypass interning. This count-based cache has no byte accounting, so its retained-memory cost is unknown from the setting alone. Treat it as a plausible persistent-memory contributor, not proven OOM attribution. [capacity setting](../../crates/parqtel-core/src/config/ingest.rs#L76-L91) · [server applies config](../../crates/parqtel-server/src/server.rs#L45-L46) · [capacity division and clear behavior](../../crates/parqtel-core/src/models/labels.rs#L286-L310) · [shard clear and pair limit](../../crates/parqtel-core/src/models/labels.rs#L293-L377)

### Per-query concurrency and output retention

`scan_filtered` creates `Semaphore::new(MAX_CONCURRENT)` inside each call. It can spawn block tasks for up to `MAX_BLOCKS=128`, then joins them afterward. Completed per-block `MetricScan` outputs can remain in their task handles until joined. Concurrent UI requests each get a separate 16-permit semaphore; alert evaluations also call the same `QueryExecutor`. [per-call semaphore and delayed join](../../crates/parqtel-core/src/storage/scanner/metrics.rs#L191-L232) · [limits and batch size](../../crates/parqtel-core/src/storage/scanner/mod.rs#L21-L28)

The indexed scan reads selected rows and four columns, but returns every selected row as a `DataPoint`. `execute_ast` retains `scan.points` in `raw`; `build_series_data` groups the full raw vector before truncating to `max_series` and `max_samples_per_series`. The plan-based path used by alert evaluation also receives all `scan.points` before applying its per-series caps. [AST scan path](../../crates/parqtel-query/src/executor/metrics.rs#L62-L103) · [group-before-truncate](../../crates/parqtel-query/src/executor/series.rs#L21-L101) · [plan path](../../crates/parqtel-query/src/executor/metrics.rs#L254-L310)

The actual server alert task runs every 15 seconds by default. It evaluates rules serially and invokes `query_executor.execute(plan)` for each rule; each plan uses limits of 100 series and 1,000 samples, but those limits do not constrain the scanner’s accumulated `Vec<DataPoint>`. Concurrent UI panels can overlap that alert work. [alert interval](../../crates/parqtel-core/src/config/server.rs#L21-L51) · [alert scans and limits](../../crates/parqtel-server/src/tasks.rs#L134-L205)

### Compaction did not cause the third OOM and is not a safe emergency lever

The default background compaction interval is one hour, and the loop sleeps before its first pass. The small-block pass selects blocks under 10,000 rows; tiered compaction excludes blocks at or above 500,000 rows. The reported 1M-row blocks are therefore not candidates. The one-shot `parqtel compact` command is currently a stub. [first-pass delay](../../crates/parqtel-core/src/storage/compactor.rs#L65-L97) · [eligibility thresholds](../../crates/parqtel-core/src/storage/compactor.rs#L100-L124) · [tiered threshold](../../crates/parqtel-core/src/storage/compactor.rs#L190-L222) · [CLI stub](../../crates/parqtel-server/src/commands.rs#L8-L15)

Forcing large-block compaction would be risky under a 4 GiB limit. `read_source_blocks` accumulates decoded rows from all source blocks in vectors; metric rewrite then sorts and groups those points before emitting a new series table. The default merge cap is 12 blocks. Compaction can reduce block fanout, but the current writer rebuilds the same full per-block index format; it does not make one block’s expanded index smaller. [decode into vectors](../../crates/parqtel-core/src/storage/compactor.rs#L324-L463) · [sort/group/rewrite](../../crates/parqtel-core/src/storage/compactor.rs#L466-L596) · [merge cap](../../crates/parqtel-core/src/config/storage.rs#L25-L54)

## Smallest viable design and validation

1. **Measure before changing index format.** Add per-query phase timings and counters for selected blocks, index-cache hits/misses/evictions, concurrent cold index builds, estimated live index bytes, interner entries/shard clears, selected series/rows, `MetricScan.points.len()`, and grouping time. Correlate them with cgroup RSS/working-set/cache and active-buffer size. Run cold- and warm-cache 5- and 15-minute scans with one request, concurrent panels, and alert evaluation enabled. This should separate index-load peaks from raw-result and retained-interner memory.
2. **Add a process-wide scan/index-load admission limit.** The current semaphore is per request, so lower `MAX_CONCURRENT` alone cannot cap aggregate work. Start conservatively for 4 GiB, then tune from measured peaks. Count active buffers and file cache as well as `SeriesIndex`; `approx_bytes` is not a hard budget.
3. **Choose one targeted follow-up from the profile.** If index construction dominates, keep immutable per-block indexes but move toward compact, offset-addressable postings and iterator-based intersection; decode only queried postings and selected series metadata. Preserve legacy footer reading and current fail-open behavior. Do not add a second global postings index. If returned points dominate, stream record batches into the narrowest compatible per-series/PromQL accumulator and apply limits before retaining all raw samples; preserve `points_scanned`, volume summaries, and query semantics.
4. **Keep compaction separate.** Revisit bounded compaction for fanout and old block formats only after scan headroom is stable. Bound merge bytes/rows and memory, and never force multi-block full decode during an OOM investigation.

Use the existing indexed-versus-unindexed equivalence test as the correctness gate when changing postings or scan consumption. [test](../../crates/parqtel-core/src/storage/scanner/tests/indexed.rs#L183-L220)

The main caveat is unchanged: source evidence and the OOM timeline strongly support a concurrency/index/materialization hypothesis, but without an allocator profile they cannot identify the exact allocation that crossed the limit.
