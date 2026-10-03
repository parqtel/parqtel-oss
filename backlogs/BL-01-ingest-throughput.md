# BL-01 — Ingestion Throughput & Latency

**Domain:** `parqtel-ingest`, `parqtel-core::buffer`, `parqtel-server::{grpc,router,handlers}`
**Goal:** sub-second ingest latency, 100k+ points/sec sustained, no global stall longer than 100 ms.

Severity legend: **C** = Critical (blocks the SLO), **H** = High, **M** = Medium, **L** = Low.
Effort: S ≤ 2d, M ≤ 1w, L > 1w.

---

## BL-01-01 (C) — Single global ingest mutex is held across the entire Parquet flush

**Evidence**
- One `Arc<Mutex<BlockRotator>>` per signal, process-wide: `parqtel-ingest/src/service.rs:160`, `:304`, `:470`.
- The lock is taken once per request and held across the whole push loop **and** the flush: `service.rs:244-254` (metrics), `:351-360` (logs), `:554-561` (traces).
- `BlockRotator::flush` awaits `spawn_blocking(...).await` (`service.rs:73-75`) — i.e. the mutex is held for Parquet encode + zstd + `File::create` + `write` + `rename`.

**Gap.** Every OTLP request on a signal (HTTP *and* gRPC) serializes on one lock. When the writer crosses `max_rows_per_block` (1 000 000, `config/storage.rs:36`), the requesting batch performs the whole flush inline — realistically hundreds of ms to seconds for a full block — while every other ingest request *and* the 5-second flush ticker (`parqtel-server/src/main.rs:396`) and `check_and_flush` (`service.rs:274-275`) queue behind it. During that window throughput is one in-flight request per signal and client latency equals the flush duration. At 100k pts/s the 1M-row cap is reached every ~10 s, so a large fraction of wall time is spent with ingest globally blocked.

Note: `tokio::sync::Mutex` is the correct primitive here (there is an `.await` inside), but its *scope* is wrong, and there is no `try_lock` fast path, no queue-depth metric and no sharding.

**Resolution.**
1. Decouple: `flush()` should `mem::take` the writer, hand ownership to a dedicated **flush worker task** over a bounded channel, and release the lock immediately. The rotator then guards only an in-memory `Vec` push (nanoseconds). Return 200 without waiting for the encode.
2. Preserve crash-safety ordering: the flush worker must publish `BlockMetadata` to the index **after** the rename, and shutdown must drain the channel before exit (`IngestionService::shutdown`, `service.rs:284-291`).
3. Add `ingest.max_inflight_flushes` (`Semaphore`) for backpressure, plus a `parqtel_ingest_flush_lock_wait_seconds` histogram so lock contention is observable.
4. If synchronous flush must remain as a fallback (WAL disabled), shard the rotator into N buckets keyed by `metric_name` hash so unrelated metrics do not contend, and only block the flushing bucket.

**Acceptance.** Ingest p99 < 250 ms for a 10k-point batch while a flush is in flight; no endpoint observes a stall > 100 ms during a 1M-row flush; lock-wait p99 < 10 ms.

**Effort** L · **Risk** Medium (durability semantics change; gated by WAL work BL-03-12)

---

## BL-01-02 (C) — Per-point `LabelSet` (`BTreeMap<String,String>`) clone storm on the ingest path

**Evidence** — allocations per data point, end to end:

| # | Site | Cost |
|---|------|------|
| 1 | `decode/metrics.rs:160-165`, `:183-188`, `:208-213` — fresh `LabelSet::try_from_iter` per point; `any_value_to_string` (`decode/common.rs:4-15`) allocates a `String` per attribute and `format!("{:?}", …)` for array/kvlist/bytes | 1 B-tree + L strings |
| 2 | `service.rs:217-240` — `dp.labels.clone()` then `.merge(&LabelSet::try_from_iter(vec![("service.name", svc.to_string())]))`; `merge` (`labels.rs:72-78`) clones the whole map *again*; no-`service.name` path does `m.data_points.clone()` (full `Vec` deep clone) | 2–3 B-trees + `Vec` |
| 3 | `parqtel-core/src/buffer.rs:37` — `extend_from_slice` copies those clones again | +L strings |
| 4 | `parqtel-ingest/src/writer.rs:155-172` — group key `((*ctx.name).clone(), ctx.kind, (*ctx.resource).clone())` per row, plus `ctx.dp.clone()` into a `BTreeMap` group | 2 B-trees per row |
| 5 | `parqtel-core/src/models/storage/writer.rs:57` — `dp.labels.to_json()` → full `serde_json::to_string` **per row** | 1 String + JSON encode per row |
| 6 | `storage/correlation.rs:17-38` — 7 correlation values cloned + `LabelSet` rebuilt per metric | 1 B-tree |

**Gap.** ~6 heap-allocated B-tree nodes (≈500 B each) plus ≈2× the label `String`s per point. At 100k pts/s that is >300k B-tree allocations/sec per stage, and stages 4–5 are single-threaded inside the `spawn_blocking` flush — i.e. inside the lock held by BL-01-01. This is the single largest CPU/allocator cost in decode+flush.

**Resolution.**
- Introduce an interned, immutable label representation: `LabelSet` backed by a sorted `Box<[(InternedStr, InternedStr)]>` where `InternedStr` is an index into a process-global interner (`DashMap`/`append-only Vec` + `RwLock`). Clone becomes a pointer copy.
- Fold `service.name` into the point at **decode** time (it is already known from the resource), which deletes `service.rs:214-243` entirely.
- Key the flush grouping on the already-shared `Arc<String>` / `Arc<LabelSet>` in `DataPointContext` (`writer.rs:22-27`) instead of cloning per row.
- Serialize the labels JSON **once per series**, memoized on the `u64` fingerprint already available at `labels.rs:60-67`, replacing the per-row `to_json`.
- Introduce `LabelSet::from_sorted_iter` / `with_capacity` single-pass builders and retire `merge`-per-label (also fixes BL-02-02).

**Acceptance.** `cargo run --release -p parqtel-server --example perf_bench`: ingest (decode+push) and flush (50k pts) both ≥ 3× current. Allocation count per point measured with `heaptrack`/`dhat` drops by ≥ 70 %.

**Effort** XL · **Risk** High (touches every crate; label-set semantics must stay identical)

---

## BL-01-03 (C) — `MemoryBuffer` is unbounded; no eviction, no byte accounting, no backpressure

**Evidence** — `parqtel-core/src/buffer.rs:14-27`: three `RwLock`-guarded collections with only an *initial* `with_capacity` (64 / 5 000 / 5 000). No max size, no byte accounting, no eviction, no TTL anywhere in the file. Push methods only `extend_from_slice` (`:30-54`); the only eviction is `drain_*` via `mem::take` (`:112-152`), which runs exclusively after a block flush.

**Gap.** Points live in the buffer until the block writer flushes, which under the default `block_duration_secs = 7200` (`config/storage.rs:35`) means "practically never" unless 1M rows accumulate. Each buffered `DataPoint` carries a `LabelSet` (~600–800 B once the B-tree node and strings are counted), and the data is stored **twice** — once in `MemoryBuffer`, once moved into `BlockWriter`. 1M points ≈ 0.6–0.8 GB per copy; peak RSS ≈ 1.5 GB+ for a single block's worth. There is no overflow policy: sustained ingest is an OOM, and `otel_sli.rs:204-216` only observes the pressure after the fact.

**Resolution.**
- Add explicit bounds (`buffer.max_points`, `buffer.max_bytes`, per-signal) with a documented drop-oldest policy and a `parqtel_buffer_dropped_total` counter.
- Shed at the handler when full (return 429/503) rather than growing without limit; make the policy configurable between `drop_oldest` and `reject_new`.
- Track approximate resident bytes so the limit is meaningful for logs/spans (large `Vec`-bearing structs) as well as metrics.
- Long term, store buffer entries as compact columnar Arrow batches instead of `Vec<DataPoint>` + B-trees (pairs with BL-01-02).

**Acceptance.** RSS is bounded by configuration under sustained 200k pts/s ingest for 1 hour with zero unbounded growth; dropped-point counter exported on `/metrics`.

**Effort** M · **Risk** Low

---

## BL-01-04 (H) — `block_duration_secs` (7200 s default) is effectively dead; no byte-based flush trigger

**Evidence**
- Default `block_duration_secs: 7200` (2 hours) at `parqtel-core/src/config/storage.rs:35`; logs 1800 s at `:69`.
- The 5-second background ticker (`parqtel-server/src/main.rs:396`) only compares elapsed time against that threshold (`service.rs:53-59`, `:117-123`).
- The only real trigger is a row count: `writer.len() + dps > max_rows_per_block` (`service.rs:46`, `:110`).

**Gap.** Time-based rotation never fires in practice, so blocks are 1M rows / potentially minutes-to-hours of wall time. That is wrong for query latency (huge files, coarse row-group pruning granularity, memory spikes on decode) and for freshness of compaction. There is no max-bytes trigger and no max-rotation-latency.

**Resolution.** Add `max_bytes_per_block` and `max_flush_interval_secs` (default ~30 s) and check both in `push`/`check_and_flush`; retune `block_duration_secs` default to 60–300 s and `max_rows_per_block` to a size that lands in the 50k–250k range for the target ingest rate. Expose `row_group_size` as the tuning knob it already is (`writer.rs:385-391` documents that it controls pruning granularity).

**Acceptance.** Under 100k pts/s, blocks rotate every ≤ 60 s, no block exceeds `max_bytes_per_block`, and a narrow 5-minute query touches ≤ 2 blocks.

**Effort** S · **Risk** Low (config default change — announce in release notes)

---

## BL-01-05 (H) — OTLP decode runs inline on async workers; JSON path materialises a full DOM

**Evidence**
- `ExportMetricsServiceRequest::decode(body)` and `OtlpDecoder::decode_*` are called directly on the request future: `service.rs:180-188`, `:197-205`, `:324-332`, `:334-342`, `:511-519`, `:521-529`. `spawn_blocking` appears only in `flush` (`service.rs:73`).
- JSON path: `serde_json::from_slice::<serde_json::Value>(&body)` (`service.rs:199`, `:336`, `:523`) builds a complete DOM, then `decode/metrics.rs:33-101` walks it with `.get()` chains, allocating `String`s for names (`metrics.rs:58-62`) and `name.clone()` per datapoint group (`:84`).
- `#[tokio::main]` (`parqtel-server/src/main.rs:80`) with default multi-thread runtime; `worker_threads` is not configured.

**Gap.** Decoding a 10 MB OTLP batch is tens of ms of pure CPU plus ~1 `BTreeMap` per point. On a 2–4 core container that occupies 1–2 of the total workers, so ingest and query requests queue behind it. The JSON path additionally peaks at ~2–3× the payload (bytes + DOM + converted model).

**Resolution.**
- Wrap the decode step of each `ingest_*` entry point in `spawn_blocking` (or a dedicated ingest blocking pool), then hand the decoded model to the lock-protected push.
- Replace the `Value` DOM with typed `#[derive(Deserialize)]` structs mirroring the OTLP/JSON schema (or `RawValue` + targeted extraction) so intermediate Strings are not materialised.
- Size the runtime explicitly (BL-04-07).

**Acceptance.** Sustained 100k pts/s JSON ingest leaves query p99 unchanged versus an idle ingest load; ingest CPU per point drops ≥ 40 % on the JSON path.

**Effort** M · **Risk** Medium (JSON decode is behaviour-sensitive; keep the existing `decode_test.rs` suite green)

---

## BL-01-06 (M) — gRPC path serialises the already-decoded request, then re-decodes it

**Evidence** — `parqtel-server/src/grpc.rs:61`, `:111`, `:161`: `prost::Message::encode_to_vec(&request.into_inner())` then `ingest_proto(Bytes::from(body))`, which calls `ExportMetricsServiceRequest::decode(body)` at `service.rs:182`.

**Gap.** Every gRPC export pays a full extra serialise + deserialise pass and one full-size heap allocation, purely to reuse the HTTP entry point. For a 10 MB batch that is ~20 MB of avoidable memcpy plus a full parse — on the path most production exporters use (`:4317` is the OTLP default).

**Resolution.** Expose `ingest_metrics_proto(ExportMetricsServiceRequest)` / `ingest_logs_proto` / `ingest_traces_proto` on `IngestionService` that take the already-decoded prost message and skip the re-decode. Keep `ingest_proto(Bytes)` for the HTTP protobuf route. Take the wire-byte count from `prost::Message::encoded_len()` rather than materialising the buffer.

**Acceptance.** gRPC ingest CPU per batch drops ≥ 30 %; both paths produce identical `ingested_points` counts for the same payload (covered by `parqtel-server/tests/otlp_export.rs`).

**Effort** S · **Risk** Low

---

## BL-01-07 (M) — No request coalescing or micro-batching between handlers and rotators

**Evidence** — every request independently decodes, pushes, and runs a capacity check (`service.rs:46`, `:110`, `:430`). Per-request fixed costs: `Arc::new(metric.name)` + `Arc::new(resource)` per metric (`writer.rs:43-44`), `name.to_string()` per metric (`buffer.rs:35`, allocated even when the key already exists), a fresh `Vec<DataPoint>` for buffer injection (`service.rs:217-240`), and `Vec::with_capacity(128)` (~20 KB) for **every new** series name (`buffer.rs:36`) — 128× over-allocation for a series that sends one point.

**Gap.** Exporter scrapes arrive in small, bursty batches (typical OTLP: 5–50 ms apart, 100s–1000s of points). Paying the full fixed cost per request leaves most of the benefit of batching unrealised, and small pushes interact badly with the `max_rows_per_block` boundary check.

**Resolution.**
- Add a bounded coalescing queue per signal (e.g. 5 ms or 50k points, whichever first) consumed by a single ingest task; decode stays per-request (it's CPU-bound and already parallelisable) but the push/flush decision becomes batch-oriented.
- Intern the metric-name key so `MemoryBuffer` can use `entry(Arc<str>)` and avoid the per-request `String`.
- `reserve_exact(data_points.len())` instead of `with_capacity(128)` at `buffer.rs:36`.

**Acceptance.** At 1000 scrapes/s of 50 points each, ingest CPU per point drops ≥ 25 % versus today.

**Effort** M · **Risk** Medium (adds a queue and a latency knob; must keep shutdown drain correct)

---

## BL-01-08 (M) — Flush-time label index build allocates ~3 `String`s per label per row

**Evidence** — `parqtel-ingest/src/writer.rs:91-118` (metrics) and `:232-257` (logs):
- `metric_names.insert((*ctx.name).clone())` — a `String` clone **per row** even though the name is an `Arc<String>` shared by the whole metric (`writer.rs:43`).
- Per label key: `label_names.insert(label.clone())`, `label_values.entry(label.clone())`, then `.map(|s| s.to_string())` → **3 allocations per label per row**.

**Gap.** ~6 labels/row × 1M rows ≈ 18M allocations in a single flush, all inside the blocking flush that the ingest mutex is held across (BL-01-01).

**Resolution.** Iterate over the **distinct** `(name, resource)` pairs rather than per-row contexts: hoist the `metric_names` insertion to `BlockWriter::push` (once per metric) and build `label_values` from the `BTreeMap` groups produced by `reconstruct_metrics`, borrowing `&str` from the existing `Arc<LabelSet>`s. Make `MAX_VALUES_PER_FIELD` (`writer.rs:86`, `:227`) configurable rather than a literal.

**Acceptance.** Flush (50k pts) ≥ 20 % faster; allocation count during flush drops by an order of magnitude.

**Effort** S · **Risk** Low

---

## BL-01-09 (M) — `drain_*` deallocates up to a million records on a runtime worker

**Evidence** — `parqtel-core/src/buffer.rs:112-152` returns the drained `Vec<(String, Vec<DataPoint>)>` / `Vec<LogRecord>` / `Vec<Span>` by value; callers discard it (`service.rs:255-259`, `:276-280`, `:287-289`, `:361-365`, `:382-386`, `:393-395`, `:562-566`, `:583-587`, `:594-596`).

**Gap.** The free loop for up to 1M `DataPoint`s (each a B-tree node plus several `String`s) runs synchronously on whichever tokio worker happened to trigger the flush — millions of `free()` calls in one uninterruptible burst, immediately after the flush that already blocked the runtime. Spans are worse: large structs with `Vec` fields.

**Resolution.** Hand the drained value to a `spawn_blocking` drop task (or a dedicated janitor task with a bounded queue). Once BL-01-02 lands, this cost largely disappears on its own.

**Acceptance.** Post-flush latency spike attributable to teardown < 10 ms (measured by the `parqtel_flush_duration_seconds` SLI split into encode vs. teardown).

**Effort** S · **Risk** Low

---

## BL-01-10 (M) — Resource and scope attributes cloned per metric / span / log record

**Evidence**
- `decode/metrics.rs:26` — `resource_attributes.clone()` per metric.
- `decode/traces.rs:23` — per span (**a `BTreeMap` clone per span**); `decode/traces.rs:94` and `:58` — `resource_attributes.merge(&attributes)` clones the resource map again per span.
- `decode/logs.rs:35-38` — `resource_attributes.clone()` + `scope_name.clone()` + `scope_version.clone()` **per log record**.

**Gap.** For a resource shared by every record in a batch, the correct representation is one shared `Arc<LabelSet>`. As written, a 10k-record batch performs 30k+ unnecessary B-tree clones.

**Resolution.** Wrap resource/scope in `Arc<LabelSet>` in the decoded models; merge once per distinct `(resource, point-attributes)` pair, memoised on the point's attribute fingerprint. Falls out naturally from BL-01-02's interner.

**Acceptance.** Zero `LabelSet` clones attributable to resource/scope in the decode path (verified by allocation profile).

**Effort** M · **Risk** Medium (public model types change)

---

## BL-01-11 (M) — `BlockWriter::push` can fail mid-metric: partial push plus a client error

**Evidence** — `parqtel-ingest/src/writer.rs:47-57`: capacity is checked *inside* the per-point loop, so a metric whose points straddle the boundary pushes part of its points and then returns `Error::Validation("Block writer buffer is full")`. The `ponytail` comment at `service.rs:42-43` acknowledges it.

**Gap.** Correctness and load amplification: the request is rejected although some rows were already accepted into the writer **and** the memory buffer (double-accounted), and the client — seeing an error — retries the whole batch, amplifying load exactly when the system is already at capacity. The rotator's pre-check (`service.rs:46`) only helps when the whole batch fits; a single batch larger than `max_rows_per_block` still overflows.

**Resolution.** Split the incoming `Vec<DataPoint>` across the boundary in `BlockRotator::push`, flushing once and continuing, so a request is never partially accepted and never spuriously failed. Apply the same rule to `LogRotator`/`TraceRotator`.

**Acceptance.** A 2.5M-point single batch ingests successfully across three blocks with no error, no duplicate points, and no double-count in the memory buffer (test with the existing flush-signal propagation path).

**Effort** S · **Risk** Low

---

## BL-01-12 (M) — Span-metrics RED bridge: 3 `LabelSet` clones per span + unbounded channel

**Evidence** — `parqtel-ingest/src/span_metrics.rs:41-86`: `service.clone()` + `operation.clone()` for the map key, `span.name.clone()`, then a 3-entry `LabelSet` built per span and `.clone()`d twice more (`:75`, `:80`). Derived metrics re-enter the metrics path through an **unbounded** channel (`service.rs:540`, consumed at `parqtel-server/src/main.rs:298-309`).

**Gap.** ~40k B-tree allocations for a 10k-span batch, synchronously inside the trace handler. The unbounded channel means a trace-heavy workload can queue arbitrarily many derived-metric batches behind the same metrics mutex as external traffic, with no drop signal.

**Resolution.** Build one `LabelSet` per **series** (the `Acc` entry already groups by series — hoist it out of the per-span loop), use `Arc<LabelSet>`, bound the channel with `mpsc::channel(N)` and count drops.

**Acceptance.** Trace ingest CPU per span drops ≥ 40 %; `parqtel_span_metrics_dropped_total` exposed.

**Effort** S · **Risk** Low

---

## BL-01-13 (L) — Miscellaneous ingest-path hygiene

| # | Gap | Evidence | Resolution |
|---|-----|----------|------------|
| a | `MemoryBuffer` write lock taken once per metric per request | `service.rs:241` inside `for m in &metrics`; `buffer.rs:34` each iteration; each `.await` on `tokio::RwLock` is a yield point | Group by name and push once per name, or add `push_metrics_multi` |
| b | `LabelSet::try_from_iter` double B-tree lookup and rejects the whole batch on a duplicate key | `parqtel-core/src/models/labels.rs:27-38` — `contains_key` then `insert`, `String` allocated before the check | Use the entry API once; last-wins or per-record skip rather than failing the entire OTLP request (SDKs routinely merge resource + point attributes) |
| c | `tracing::debug!` in per-request hot paths | `service.rs:263`, `:369`, `:570`; `buffer.rs:32`, `:43`, `:51` — `buffer.rs:32` formats `metric = %name` for every metric of every request | Move to sampled/trace level or remove |
| d | `Uuid::new_v4()` per block filename | `writer.rs:126`, `:264`, `:355` | Use the already-vendored monotonic `ulid` — also makes block ordering lexically stable |
| e | `fs::create_dir_all` + `fs::metadata` per flush | `writer.rs:131`, `:139` (and `:269`/`:277`, `:360`/`:368`) — directory is already created at `main.rs:150-151` | Create once at startup; take size from the writer's byte counter |
| f | `/v1/traces` wired to the JSON handler only | `parqtel-server/src/router.rs:45` vs. content negotiation for metrics/logs (`:41`, `:43`; `handlers/ingest.rs:255-278`) | Wire content negotiation for traces too — currently protobuf exporters hitting `/v1/traces` fail at `service.rs:523` and are pushed onto slower paths |
| g | `TraceWriter::buffer` growth cycle | `writer.rs:302-312` — fresh multi-hundred-MB `Vec` allocation per flush | Reuse the allocation across flushes (swap-and-clear with retained capacity) |

---

## Verification commands

```bash
# ingest + flush + scan + query baselines
cargo run --release -p parqtel-server --example perf_bench

# lock contention and flush stalls
curl -s localhost:8080/metrics | grep -E 'parqtel_(ingest|flush|buffer|index)_'

# CPU/alloc profile of the ingest path (pprof already wired)
go tool pprof -http=:8081 'http://localhost:8080/debug/pprof/profile?seconds=30'

# sustained load + memory
python3 scripts/load_gen.py --help
python3 scripts/check-memory.sh
```
