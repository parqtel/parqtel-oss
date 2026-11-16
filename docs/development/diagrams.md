# Architecture diagrams

Visual reference for Parqtel's internal structure. Every diagram is
[Mermaid](https://mermaid.js.org/), so it renders on GitHub, in the docs site and
in any Mermaid-aware Markdown viewer. The prose around each diagram carries the
detail; the diagram carries the shape.

Related reading:

- [Architecture](architecture.md) — the narrative version of these diagrams.
- [Developer guide](developer-guide.md) — where the code lives.
- [Glossary](glossary.md) — the terms used in the labels.

## 1. System context

What talks to Parqtel, and what Parqtel talks to.

```mermaid
flowchart LR
    subgraph producers["Telemetry producers"]
        SDK["OpenTelemetry SDKs<br/>metrics · logs · traces"]
        PROM["Prometheus<br/>remote-write"]
        APP["Applications<br/>OTLP exporters"]
    end

    subgraph parqtel["Parqtel (single binary)"]
        ING["Ingest<br/>gRPC :4317 · HTTP :8080"]
        SRV["Query + console<br/>/api/v1/* · /ui"]
        ALERT["Alert engine"]
    end

    subgraph storage["Storage"]
        BLOCKS["Parquet blocks<br/>zstd · columnar"]
        IDX["Block index sidecars<br/>index.json"]
    end

    subgraph consumers["Consumers"]
        GRAF["Grafana<br/>SimpleJSON datasource"]
        PROMAPI["Prometheus<br/>HTTP API"]
        OPS["Operators<br/>curl · kubectl"]
        LLM["LLM agents<br/>MCP servers"]
    end

    SDK -->|"OTLP gRPC :4317"| ING
    SDK -->|"OTLP HTTP /v1/*"| ING
    APP --> ING
    PROM --> ING

    ING --> BLOCKS
    ING --> IDX
    IDX --> SRV
    BLOCKS --> SRV

    SRV --> GRAF
    SRV --> PROMAPI
    SRV --> OPS
    ALERT --> OPS
    ALERT --> LLM
    ING -->|"span-metrics RED bridge"| ALERT
    SRV -.->|"query"| ALERT
```

**Facts behind the picture.** Ingest is unauthenticated in the OSS build — put
a proxy in front of `:8080` and `:4317` (see
[known issues](../operations/known-issues.md)). The MCP servers are separate
binaries that query Parqtel over HTTP; they are not in the ingest path.

## 2. Crate dependency graph

Fifteen workspace members; every signal crate depends only on `parqtel-core`.

```mermaid
flowchart TD
    SRV["parqtel-server<br/>binary: parqtel"]
    ING["parqtel-ingest"]
    QRY["parqtel-query"]
    ALR["parqtel-alert"]
    PIPE["parqtel-pipeline"]
    CORE["parqtel-core<br/>models · storage · engine · config · wal"]
    MCPC["parqtel-mcp-core<br/>JSON-RPC · rate limit · tool registry"]

    SRV --> ING
    SRV --> QRY
    SRV --> ALR
    SRV --> PIPE
    ING --> CORE
    QRY --> CORE
    ALR --> CORE
    PIPE --> CORE
    SRV --> CORE

    subgraph mcp["MCP tool servers"]
        S1["mcp-slack"]
        S2["mcp-pagerduty"]
        S3["mcp-jira"]
        S4["mcp-notion"]
        S5["mcp-discord"]
        S6["mcp-gdocs"]
        S7["mcp-parqtel"]
    end

    S1 --> MCPC
    S2 --> MCPC
    S3 --> MCPC
    S4 --> MCPC
    S5 --> MCPC
    S6 --> MCPC
    S7 --> MCPC
```

Crates are one directory per concern under `crates/`. `parqtel-core` holds the
data models, the storage engine, the write-ahead log and the layered
configuration; it depends on no other workspace crate.

## 3. Ingest path (metrics)

One request, end to end. The same shape applies to logs and traces.

```mermaid
sequenceDiagram
    autonumber
    participant C as OTel SDK
    participant H as Axum handler
    participant D as parqtel-ingest::decode
    participant B as MemoryBuffer
    participant W as BlockRotator<br/>(4 writer shards)
    participant L as Write-Ahead Log
    participant P as Parquet block file
    participant I as BlockIndex

    C->>H: POST /v1/metrics (protobuf or JSON)
    H->>H: gunzip body · size check
    H->>D: OTLP payload
    D->>D: decode · intern label sets
    D->>B: push points (+ service.name label)
    Note over B: immediately queryable
    D->>W: push points (sharded)

    W->>L: append records, frame WAL span
    Note over W,L: commit position taken<br/>while every shard is locked
    W->>P: encode + zstd on spawn_blocking
    P->>I: BlockMetadata (mpsc)
    Note over P: block is durable here
    W->>B: drop sealed epochs
    Note over B: no double-counting
```

**Ordering guarantee.** A block's WAL commit position is the WAL's end taken
while every writer shard is locked, and buffer drops are released in flush
order. Each block records the WAL span `(from, to]` it covers
(`parqtel.wal_range` in the Parquet footer), so startup can advance the commit
point and skip records already in a block.

## 4. Trace ingest: RED bridge and tail sampling

Trace ingestion does two things before a span reaches storage: it derives RED
metrics, then it samples the trace.

```mermaid
flowchart TD
    IN["Spans decoded<br/>(resource attrs merged into span attrs)"]
    RED["RED bridge<br/>traces_service_requests_total<br/>traces_service_errors_total<br/>traces_service_duration_ms_total"]
    METRICS["Metrics ingest path<br/>(unsampled, full rate)"]
    TS["Tail sampling policy<br/>keep_errors → slow_trace_ms → sampling_ratio"]
    KEEP["Trace survives"]
    DROP["Trace dropped<br/>(counted in dropped_spans)"]
    STORE["Traces ingest path<br/>→ memory buffer → blocks"]

    IN --> RED
    RED --> METRICS
    IN --> TS
    TS -->|any rule votes keep| KEEP
    TS --> DROP
    KEEP --> STORE
```

Sampling runs **after** RED derivation, so RED metric rates stay exact while
storage volume shrinks. Probabilistic decisions hash the `trace_id`, so a whole
trace lives or dies together — no orphaned fragments. Per-service overrides
replace the global policy for that service entirely.

## 5. Query path

Three surfaces, one storage engine. The pruning cascade is the same for all of
them.

```mermaid
flowchart TD
    Q["Query request<br/>instant · range · log search · trace search · pipeline"]
    ADM["Scan admission<br/>process-wide 4 slots"]
    IDX["1. Block index lookup<br/>time range + metric name"]
    SER["2. Series table<br/>(parqtel.series_index in the footer)"]
    RG["3. Row-group pruning<br/>timestamp · metric_name · service_name stats"]
    PROJ["4. Column projection<br/>4 of 14 metric columns"]
    DEC["Row predicate<br/>(full matcher — always the final judge)"]
    BUF["Memory buffer rows"]
    EVAL["Aggregation / evaluation<br/>per step, on the blocking pool"]
    OUT["Result"]

    Q --> ADM
    ADM --> IDX
    IDX --> SER
    SER --> RG
    RG --> PROJ
    PROJ --> DEC
    DEC --> EVAL
    BUF --> EVAL
    EVAL --> OUT
```

- The series table only ever **narrows** candidates; the full matcher still
  judges every candidate series, so a block without a valid table falls back to
  a plain scan with identical results.
- Log search consults the block's skip index (`parqtel.log_index`) first: per
  4096-row granule it holds time bounds, the highest severity and a bloom
  filter over body three-byte windows and `name=value` attribute pairs. Only
  granules that can hold a match are read.
- The memory buffer is merged with the block results. It is drained when the
  block holding those rows becomes durable, so buffered and flushed rows never
  double-count.
- Instant queries use the Prometheus-default 5-minute lookback
  (`query.lookback_delta_ns`).

## 6. Block lifecycle

Metric blocks form two levels. The streaming compactor coalesces canonical
blocks into 5-minute windows.

```mermaid
stateDiagram-v2
    [*] --> Flushed: ingest flush
    Flushed: "L0 · m_l0_<start>_<end>_<id>.parquet<br/>written per flush, series-sorted"
    Flushed --> Normalized: "legacy block<br/>≤500K rows, one per pass"
    Normalized: "L0-normalised · m_n_…<br/>series order agrees with its index"
    Normalized --> LevelOne: "streaming k-way merge<br/>within one 5-minute bucket"
    Flushed --> LevelOne: "streaming k-way merge<br/>within one 5-minute bucket"
    LevelOne: "L1 · m_l1_…<br/>one 5-minute window per block"
    LevelOne --> Retired: "sources deleted after<br/>a 5-minute query grace period"
    Retired --> [*]: retention sweep
```

**How a merge works.** A streaming k-way merge reads one Arrow batch at a time
from each source, coalesces series by their final query-visible labels, and
writes a single output block. The output footer records its source files
(`parqtel.compacted_sources`), so a crash after the merge but before the source
deletion is repaired at startup: reconcile drops the superseded sources.
Sources are deleted only after a 5-minute grace period, so a query that was
already running against them still finds them.

**Bounds.** One pass merges at most 8 sources, coalesces at most 2 groups,
normalises at most 1 block, and moves at most 16 M rows. Passes run every 5
minutes, and `parqtel compact` runs the same bounded passes offline.

**WAL safety.** A merge is only attempted when every source's WAL range is at
or below the metrics WAL commit point — those records can no longer be
replayed. Mixed or non-contiguous ranges are skipped.

## 7. Storage layout on disk

```text
data/
├── m_l0_<start>_<end>_<id>.parquet   metric blocks (L0 — one per flush)
├── m_l1_<start>_<end>_<id>.parquet   metric blocks (L1 — one per 5-minute window)
├── m_n_<start>_<end>_<id>.parquet    metric blocks (normalised legacy layout)
├── index.json                        metric block index sidecar
├── logs/
│   ├── <id>.parquet                  log blocks
│   └── index.json                    log block index sidecar
├── traces/
│   ├── <id>.parquet                  trace blocks
│   └── index.json                    trace block index sidecar
└── wal/
    ├── metrics/                      metrics WAL segments
    ├── logs/                         logs WAL segments
    └── traces/                       traces WAL segments
```

Each sidecar is written atomically (temp file + rename) and holds the block
metadata plus **one shared label-value dictionary** for the whole signal:

```json
{
  "blocks": [ { "path": "m_l1_…parquet", "row_count": 8123, "…": "…" } ],
  "label_values": { "http_method": ["GET", "POST"], "…": "…" }
}
```

The dictionary is capped at 10 000 values per field and is shared by every
block, which is why the sidecar shrank from 73.5 MiB to 12.6 MiB (17 %) on a
97-block dataset. The loader still accepts the legacy bare-array shape and
folds its per-block values in, so existing sidecars migrate on first start.
Older binaries cannot parse the new shape.

## 8. Concurrency and admission control

```mermaid
flowchart TD
    REQ["Async request"] --> SEM["spawn_blocking<br/>(semaphore permit acquired first)"]
    SEM --> POOL["Blocking pool<br/>Parquet encode · scans · OTLP decode"]
    REQ --> ADM{"Scan budget<br/>4 slots, process-wide"}
    ADM -->|slot free| POOL
    ADM -->|full, low priority| SKIP["Skip and count<br/>parqtel_scan_low_priority_skips_total"]
    ADM -->|full, user query| WAIT["Wait for admission<br/>parqtel_scan_admission_wait_seconds_total"]
    WAIT --> POOL
    POOL --> FOOT["Footer / series-index cache<br/>one shared load per block"]
```

- **Metrics rotator shards** (`ingest.rotator_shards`, default 4) let unrelated
  metric ingest proceed while a flush is in flight. The shards are not
  independent blocks — a flush merges them into one file.
- **The memory buffer is drained by durability, not by scheduling.** Draining
  early would leave rows in neither the buffer nor any block, invisible to
  every query until the encode landed.
- **Alert evaluation reserves a scan slot without waiting** and skips the
  cycle when none is free, so alerting never starves interactive queries.
- **The label interner** (`ingest.label_interner_capacity`, default 1 048 576)
  lets every point of a series share one copy of its labels. Set it to at
  least twice the active series count.

## 9. Alert path

```mermaid
flowchart LR
    R["YAML rules<br/>rules_dir + built-in presets"] --> REG["AlertRuleRegistry<br/>watched with notify"]
    REG --> LOOP["Evaluation loop<br/>every server.alert_interval_secs"]
    LOOP --> Q["Query executor<br/>single selector or aggregation"]
    Q --> TH["Threshold on the last sample<br/>of every result series"]
    TH --> SM["State machine<br/>Inactive → Pending → Firing → Resolved"]
    SM --> STORE["AlertStore<br/>transition history"]
    SM --> ROUTE["Notification routes<br/>severity + label match"]
    ROUTE --> HOOK["Webhook"]
    ROUTE --> MCP["MCP servers<br/>Slack · PagerDuty · Jira · Notion"]
```

## 10. Query surfaces

One storage engine, three syntaxes.

```mermaid
flowchart TD
    U["Client"] --> PQL["ParQL<br/>PromQL-compatible"]
    U --> LQL["ParqtelQL<br/>logs + traces"]
    U --> PIPE["PQL Pipelines<br/>fetch │ filter │ parse │ stats │ correlate"]

    PQL --> E1["Pratt parser → AST<br/>per-step evaluation"]
    LQL --> E2["Lenient boolean grammar<br/>prepared predicates"]
    PIPE --> E3["Unified field map<br/>fetch │ filter │ parse │ stats │ correlate"]

    E1 --> EX["QueryExecutor"]
    E2 --> EX
    E3 --> EX
    EX --> SC["Scanner<br/>index → series table → row groups → projection"]
    SC --> BLK["Parquet blocks + memory buffer"]
```

- **ParQL** — instant and range queries, aggregations with `by`/`without`,
  binary operators with `on()`/`ignoring()`/`group_left`, subqueries, offset.
  Instant selectors use the 5-minute lookback.
- **ParqtelQL** — terms, phrases, wildcards, `-exclusion`, `OR`/`AND`/`NOT`,
  field operators (`=/!=/=~/>=/</:`), ranges (`a-b`), `field:*` exists, `body:`
  contains, severity thresholds. Trace predicates are pushed into the scan
  before result caps are applied.
- **Pipelines** — `fetch logs|metrics|traces | filter <pql> | parse "<re>" as f |
  stats count/avg/min/max/sum/p50/p95/p99 by … interval=… | limit | correlate
  traces|logs window=…`.
