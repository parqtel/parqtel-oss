# Architecture diagrams

Visual reference for Parqtel's internal structure. The diagrams are **animated
SVG** — the motion carries the data flow (dashes travel along every edge, nodes
breathe in a wave, subgraph frames lift) so direction and throughput read at a
glance. There is no JavaScript and no external asset: each file is a
self-contained SVG rendered from a Mermaid source.

- **Motion stops automatically** under `prefers-reduced-motion: reduce`, and the
  diagrams stay fully legible as still images.
- **Sources** are Mermaid files in [`docs/assets/diagrams/src/system-context.mmd`](../assets/diagrams/src/system-context.mmd)
  — that is the editable truth.
- **Regenerate** after editing a source (or the palette/motion stylesheets):

  ```bash
  npm install -g @mermaid-js/mermaid-cli   # once
  ./scripts/diagrams/build.sh
  ```

  The palette lives in [`parqtel-mermaid-config.json`](../assets/diagrams/parqtel-mermaid-config.json)
  and the animation vocabulary in [`parqtel-motion.css`](../assets/diagrams/parqtel-motion.css).

Related reading:

- [Architecture](architecture.md) — the narrative version of these diagrams.
- [Developer guide](developer-guide.md) — where the code lives.
- [Glossary](glossary.md) — the terms used in the labels.

## 1. System context

What talks to Parqtel, and what Parqtel talks to.

![System context: telemetry producers feed Parqtel's ingest over OTLP gRPC and HTTP; Parqtel writes Parquet blocks and block-index sidecars; Grafana, Prometheus, operators and LLM agents read from the query and alert surfaces](../assets/diagrams/system-context.svg)

**Facts behind the picture.** Ingest is unauthenticated in the OSS build — put
a proxy in front of `:8080` and `:4317` (see
[known issues](../operations/known-issues.md)). The MCP server (`parqtel-mcp-parqtel`) is a separate
binary that queries Parqtel over HTTP; it is not in the ingest path.

## 2. Crate dependency graph

Eight workspace members; every signal crate depends only on `parqtel-core`.

![Crate dependency graph: parqtel-server depends on ingest, query, alert, pipeline and core; each signal crate depends on core; parqtel-mcp-parqtel depends on parqtel-mcp-core](../assets/diagrams/crate-graph.svg)

Crates are one directory per concern under `crates/`. `parqtel-core` holds the
data models, the storage engine, the write-ahead log and the layered
configuration; it depends on no other workspace crate.

## 3. Ingest path (metrics)

One request, end to end. The same shape applies to logs and traces.

![Ingest sequence: the OTel SDK posts to the Axum handler, which gunzips and size-checks the body, decodes it, pushes to the memory buffer and the sharded rotator, frames the WAL records, encodes the Parquet block on the blocking pool and updates the block index, then drops the sealed buffer epochs](../assets/diagrams/ingest-path.svg)

**Ordering guarantee.** A block's WAL commit position is the WAL's end taken
while every writer shard is locked, and buffer drops are released in flush
order. Each block records the WAL span `(from, to]` it covers
(`parqtel.wal_range` in the Parquet footer), so startup can advance the commit
point and skip records already in a block.

## 4. Trace ingest: RED bridge and tail sampling

Trace ingestion does two things before a span reaches storage: it derives RED
metrics, then it samples the trace.

![Trace ingest flow: spans are decoded with resource attributes merged in; the RED bridge derives traces_service_requests, errors and duration metrics into the metrics path; the tail-sampling policy keeps error and slow traces and samples the rest by trace-id hash; kept traces go to the trace ingest path](../assets/diagrams/trace-red-sampling.svg)

Sampling runs **after** RED derivation, so RED metric rates stay exact while
storage volume shrinks. Probabilistic decisions hash the `trace_id`, so a whole
trace lives or dies together — no orphaned fragments. Per-service overrides
replace the global policy for that service entirely.

## 5. Query path

Three surfaces, one storage engine. The pruning cascade is the same for all of
them.

![Query path: a request takes a scan-admission slot, then walks block-index lookup, series table, row-group pruning and column projection, applies the full row predicate, merges memory-buffer rows and evaluates on the blocking pool](../assets/diagrams/query-pruning.svg)

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

![Block lifecycle: an ingest flush writes an L0 block; legacy blocks are normalised into m_n_ files; a streaming k-way merge coalesces L0 and normalised blocks into one L1 block per 5-minute bucket; sources are deleted after a 5-minute query grace period and the block is retired by retention](../assets/diagrams/block-lifecycle.svg)

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

![Storage layout: the data directory holds metric block files named by level, the metric index sidecar with the shared label-value dictionary, logs and traces directories each with their own blocks and index, and a wal directory with one subdirectory per signal](../assets/diagrams/storage-layout.svg)

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

![Concurrency model: async requests hand blocking work to a bounded pool, a process-wide four-slot budget admits block scans, low-priority background work is skipped while the budget is full, and footer or series-index misses for one block share a single load](../assets/diagrams/admission-control.svg)

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

![Alert path: YAML rules from the rules directory and the built-in presets feed the rule registry, the evaluation loop queries the executor every alert interval, thresholds the last sample of every result series, drives the Inactive to Pending to Firing to Resolved state machine, stores transition history, exposes firing alerts via /api/v1/alerts and parqtel-mcp-parqtel, and routes to webhooks](../assets/diagrams/alert-path.svg)

## 10. Query surfaces

One storage engine, three syntaxes.

![Query surfaces: ParQL, ParqtelQL and PQL Pipelines parse into the query executor, which scans via index, series table, row groups and column projection over Parquet blocks and the memory buffer](../assets/diagrams/query-surfaces.svg)

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