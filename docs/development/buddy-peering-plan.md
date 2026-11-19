# Buddy peering plan: two-node leader/follower HA

Status: design only. No product code in this branch. Based on `origin/main` `63a9cf1`.
Revision 2 (2026-11-19) fixes lease contention, query gaps, rule outputs, alert
state handoff, demotion data loss and ingest routing found in review.

Goal: run at least two Parqtel instances. One **leader** owns ingest, WAL and block
writes, and serves its recent window to peers. **Followers** execute recording and
alert rules, answer queries from blocks, and download blocks from S3/Azure by query
time range. Two object-store leases drive failover.

**Scope.** This adds availability and offloads queries and rules from the leader.
It does **not** scale ingest: the leader still takes every write. Ingest scaling
needs sharding and is out of scope.

## Invariants

1. One ingest leader at a time, fenced by a lease epoch. Only the leader appends to
   its WAL and writes blocks. Followers forward writes; they never write WAL.
2. Every accepted record is in the leader WAL before ACK (unchanged).
3. Blocks have one owner (`CatalogBlock.writer = node_id`). Only the owner publishes,
   compacts or retires a block. Followers read remote blocks through the cache.
4. A query returns each sample once. Data newer than the **cutoff** comes only from
   the leader; data older than the cutoff comes only from blocks.
5. Rules evaluate on exactly one node: the holder of the **rules lease**.
6. A demoted leader flushes and publishes its WAL-backed buffer before it follows.
7. Opt-in: with `[cluster] enabled = false`, single-node behavior is unchanged.

## Target architecture

```text
              OTLP (HTTP + gRPC) via one Service across all pods
                                  |
          +-----------------------+------------------------+
          |                       |                        |
          v                       v                        v
   +-------------+  forward   +---------------------+  forward  +-------------+
   | FOLLOWER A  |----------->|       LEADER        |<----------| FOLLOWER B  |
   | rules lease |  QueryRecent| ingest lease        |           | (standby    |
   | block query |<-----------| WAL, buffer, blocks |           |  rules)     |
   | cache       |            | compaction,retention|           | block query |
   +------+------+            +----------+----------+           +------+------+
          |                              | publish                      |
          +---------------- read --------+------------------------------+
                                         v
                 S3 / Azure: blocks, _catalog/manifest.json,
                 _cluster/ingest-lease.json, _cluster/rules-lease.json,
                 _cluster/alert-state.json
```

Minimum: **2 pods**. Recommended: **3** so rules and queries survive a leader
failover without falling back onto the new leader.

## Reuse already in the tree

| Existing | Path | Use |
|---|---|---|
| `ObjectCatalog` CAS on `manifest.json` | `crates/parqtel-core/src/storage/object_catalog.rs` | block catalog only (lease moves out) |
| `spawn_catalog_sync_task` | `crates/parqtel-server/src/tasks.rs` | publish owned blocks, adopt remote ones |
| `BlockStore::publish` on block close | `writer.rs`, `compactor.rs`, `stream_compactor.rs` | upload L0/L1 blocks |
| `ObjectBlockStore` bounded cache, on-demand `open` | `crates/parqtel-core/src/storage/block_store.rs` | time-range downloads |
| `MemoryBuffer`, `QueryExecutor` | `crates/parqtel-core/src/buffer.rs`, `crates/parqtel-query/src/executor` | leader recent window |
| tonic server | `crates/parqtel-server/src/grpc.rs` | add internal `Peer` service |
| alert eval, ruler, `ChannelMetricSink` | `tasks.rs`, `server.rs`, `ruler.rs` | gate by rules lease; forward outputs |
| `AlertStore` (`alerts.json`), silences | `crates/parqtel-alert/src/store/alert_store.rs`, `router.rs` | snapshot to shared state |
| Helm caps `replicaCount` at 1 | `deploy/helm/parqtel/values.schema.json` | allow ≥2 with cluster |

## Configuration

New `[cluster]` (env `PARQTEL__CLUSTER__*`):

```toml
[cluster]
enabled = false
node_id = ""                 # default: HOSTNAME
role = "auto"                # auto | leader | follower (fixed roles: tests only)
advertise_addr = ""          # pod DNS:port for peer gRPC, e.g. parqtel-0.parqtel-peer:4318
peer_discovery = "dns"       # dns (headless service) | static
peers = []                   # used when peer_discovery = static
bind_addr = "0.0.0.0:4318"
lease_ttl_secs = 30
lease_renew_secs = 10
clock_skew_margin_secs = 5   # a lease is treated as expired only after ttl + margin
recent_window_secs = 300     # query cutoff = now - recent_window_secs
recent_max_points = 2000000
forward_timeout_secs = 10
```

HA validation (fail startup when violated, `enabled = true` only):

- `object_store.enabled = true` (leases and blocks live there);
- `storage.block_duration_secs < recent_window_secs` and the same for logs, so every
  record older than the cutoff is in a published block (see query routing);
- `lease_renew_secs * 2 < lease_ttl_secs`.

`ObjectStoreConfig` gains `provider = "s3" | "azure"` and Azure fields
(`azure_account`, `azure_container`, key or SAS via env). Requires the `azure`
feature of `object_store` in the workspace `Cargo.toml`.

## Leases (separate objects, not the catalog manifest)

Renewing a lease every 10 s on `manifest.json` would race the 5 s catalog sync,
which already gives up after eight CAS losses. Each lease is its own object:

```json
// _cluster/ingest-lease.json and _cluster/rules-lease.json
{"holder": "parqtel-0", "epoch": 17, "renewed_ms": 1795000000000, "ttl_ms": 30000}
```

- `acquire(node)`: read with version; if absent, or `now > renewed_ms + ttl + margin`,
  write `epoch + 1` with `PutMode::Create`/`Update(version)`. One CAS winner.
- `renew(node, epoch)`: CAS only if holder and epoch match.
- `release(node, epoch)`: CAS to an expired record on graceful shutdown.
- The existing `CatalogManifest.writer_lease: Option<String>` is left as is for
  compatibility and becomes informational.

**Fencing.** The leader re-reads the ingest lease before each block publish and
before committing a WAL position; a mismatched epoch stops ingest at once. Leases use
wall-clock time, so the margin covers bounded skew, and fencing covers the rest.

**Prerequisite (B0.5).** CAS needs conditional PUT. Verify `PutMode::Create` and
`PutMode::Update` against Ceph RGW (UAT), MinIO and Azure Blob with `object_store`
0.14 before writing lease code. If a backend fails, HA is unsupported there.

## Role state machine

```text
start -> recover WAL locally (any role) -> candidate
candidate --acquire ingest lease--> leader
candidate --held by another--> follower
leader --renew fails twice or epoch mismatch--> draining
draining --flush buffer, publish owned blocks, commit WAL--> follower
follower --ingest lease expired--> candidate
```

- **Startup:** every pod replays its own WAL first. A pod that was leader before a
  crash flushes those records into blocks it owns and publishes them before it
  competes for the lease, so a restarted ex-leader never loses its tail.
- **Draining:** stop accepting writes (forward instead), seal and flush the buffer,
  publish, then become follower. A failed publish keeps the blocks local and retries;
  they stay owned by this node and are adopted by peers once published.
- **Rules lease** is independent: prefer a follower; the leader takes it only when no
  follower is healthy, and releases it when one returns (handoff below).

## Ingest routing: forward to the leader

One Kubernetes Service spans all pods (no RBAC, no label patching).

- HTTP `/v1/*` and OTLP gRPC on a follower forward the raw request to the leader over
  the peer channel, preserving content type and encoding, and relay the response.
- When no leader is known, return `503` with `Retry-After`; the OTel collector
  retries (it already has `retry_on_failure`).
- Cost: one extra hop for a share of writes; at UAT rate (~4.5 MB/s) this is small.
- Optional later: point the collector at the leader pod via a `role=leader` label set
  by the leader (needs a Role/RoleBinding) to remove the hop.

## Query routing: time cutoff, no cross-node dedupe

Followers see the catalog late (sync interval), while the leader drops buffered points
once their block is durable. Merging buffer and blocks by fingerprint would lose or
double those points. Instead each range has one source:

```text
cutoff = now - recent_window_secs        (computed once per query)

[start, min(end, cutoff))  -> local + adopted blocks (follower or leader)
[max(start, cutoff), end]  -> leader QueryRecent: leader buffer + leader blocks
```

- Valid because HA validation forces every record older than the cutoff into a
  published block, and the catalog sync interval is far below `recent_window_secs`.
- On the leader, the whole query runs locally as today.
- Series limits (`max_series`, `max_samples_per_series`) apply after joining the two
  halves; the full matcher still decides every series.
- Logs and traces use the same split; trace-ID lookups query both halves.
- If the leader is unreachable, the follower returns block data up to the cutoff with
  a `warnings` entry (`recent data unavailable`), not an error.

## Rules on the follower

- Alert evaluation and the ruler start only on the rules-lease holder.
- **Recording-rule outputs** currently enter the local `ingestion_service`
  (`server.rs`, ruler sink task). On a follower they are forwarded to the leader
  through the same path as external writes.
- **Alert state.** `alerts.json` and silences are per pod. The rules holder snapshots
  alert state and silences to the object _cluster/alert-state.json after each evaluation cycle
  (CAS, epoch-fenced by the rules lease). A new holder loads it before its first
  cycle, so firing alerts keep their state and notifications are not re-sent.
- **Handoff:** the old holder finishes its cycle, writes the snapshot, releases the
  lease; the new holder acquires it and loads the snapshot. Missed cycles during a
  crash are acceptable (bounded by the lease TTL).

## Block sync and time-range download

- Keep `spawn_catalog_sync_task`; set `writer` to `cluster.node_id`.
- Adopted remote blocks are indexed with paths under `<data_dir>/cache/`; the object
  store's `open` materializes them on first read.
- Before a block scan, prefetch only blocks overlapping the query range, with bounded
  parallelism and the existing `cache_bytes` quota. Never download the whole catalog.
- Compaction and retention run only on the block owner (normally the leader). A new
  leader starts compacting only its own blocks; blocks left by a dead ex-leader are
  read-only until a later "adopt orphan ownership" task (out of scope).

## Failure semantics

| Event | Outcome |
|---|---|
| Leader pod restarts (same PVC) | new leader within lease TTL + margin; restarted pod replays and publishes its WAL tail, then follows. No data loss. |
| Leader node and PVC lost | records after its last published block are lost (RPO ≤ block duration, < `recent_window_secs`). WAL shadowing to object storage is a later option. |
| Follower lost | leader takes rules lease when no follower is healthy; queries go to remaining pods. |
| Object store down | leader keeps ingesting locally; publish and lease renew retry. If renew fails past TTL the leader drains; with no store, HA cannot elect and ingest returns 503 until it recovers. Document this trade-off. |
| Split brain | epoch fencing stops the stale leader at its next publish or WAL commit. |

## Metrics and health

`parqtel_cluster_role{role}`, `parqtel_cluster_lease_epoch{lease}`,
`parqtel_cluster_lease_renew_failures_total{lease}`, `parqtel_peer_up{peer}`,
`parqtel_peer_forwarded_requests_total{kind}`, `parqtel_peer_recent_requests_total`,
`parqtel_query_recent_unavailable_total`, `parqtel_object_download_bytes_total`,
`parqtel_object_cache_hits_total`. `/ready` stays per pod (a follower that can forward
is ready).

## Tasks

Each task is one PR, delegated to the lowest-cost model that passes its gate,
reviewed, gated and committed as `sonu202604 <sonu202604@gmail.com>`.

### B0 — Design doc
This document. Gate: `make docs-check`.

### B0.5 — CAS spike (blocking)
- Files: `crates/parqtel-core/examples/cas_probe.rs` (new).
- Work: create/update/conflict against an endpoint from env; report which backends
  honor conditional PUT. Run against MinIO locally and Ceph RGW on UAT (read-only
  bucket prefix `parqtel-cas-probe/`). Azure if credentials exist.
- Gate: probe output recorded in this doc. Stop if Ceph RGW fails.

### B1 — Cluster config and validation
- Files: `crates/parqtel-core/src/config/mod.rs`, new `config/cluster.rs`,
  `config/object_storage.rs` (provider field only), `docs/operations/configuration.md`.
- Work: `ClusterConfig`; HA validation rules above; env mapping.
- Gate: `cargo test -p parqtel-core config`, `make lint`.

### B2 — Lease objects
- Files: new `crates/parqtel-core/src/storage/lease.rs`, `storage/mod.rs`, tests.
- Work: `Lease::{acquire, renew, release, read}` on any `ObjectStore`; TTL + margin;
  epoch monotonic; concurrent-acquire test with `InMemory` (one winner).
- Gate: `cargo test -p parqtel-core lease`, clippy.

### B3 — Peer gRPC skeleton
- Files: `crates/parqtel-server/proto/peer.proto`, `build.rs`, `Cargo.toml`,
  `grpc.rs`, new `peer.rs`.
- Work: `Status`, `Forward` (raw body + content type + signal), `QueryRecent`
  (stub returning `Unimplemented`). Separate listener on `cluster.bind_addr`.
- Gate: `cargo test -p parqtel-server peer`, `cargo build -p parqtel-server`.

### B4 — Role manager with drain
- Files: new `crates/parqtel-server/src/cluster.rs`, `server.rs`, `state.rs`,
  `metrics.rs`.
- Work: state machine above; startup replay-then-publish; drain on demotion; epoch
  check hook used by block publish and WAL commit.
- Gate: tests with `InMemory` store: election, renew loss, drain publishes buffered
  rows, restarted ex-leader publishes its tail.

### B5 — Ingest forwarding
- Files: `router.rs`, `handlers/ingest.rs`, `handlers/remote_write.rs`, `grpc.rs`,
  `peer.rs`.
- Work: follower forwards every write path; 503 + `Retry-After` without a leader;
  forwarded-request metric.
- Gate: two in-process nodes; write to follower, read on leader; `make lint`.

### B6 — Time-cutoff query split
- Files: `crates/parqtel-query/src/executor/{mod,metrics,logs,traces}.rs`,
  `crates/parqtel-server/src/peer.rs`.
- Work: `RecentSource` trait; `QueryRecent` real implementation with point and byte
  caps; cutoff split; limits after join; degraded warning path.
- Gate: equivalence test — same query on leader directly vs via follower gives
  identical series and samples, including a block flushed during the query.

### B7 — Rules lease, forwarded outputs, shared alert state
- Files: `server.rs`, `tasks.rs`, `ruler.rs`, `crates/parqtel-alert/src/store/alert_store.rs`,
  `crates/parqtel-alert/src/router.rs` (silence export/import), and a new rules submodule beside the role manager.
- Work: start/stop eval + ruler on lease; forward ruler sink output to the leader;
  snapshot/load alert state and silences; handoff.
- Gate: handoff test — a firing alert stays firing with no second notification.

### B8 — Azure backend and range prefetch
- Files: `Cargo.toml` (`object_store` `azure` feature), `config/object_storage.rs`,
  `storage/block_store.rs`, `tasks.rs`, scanner prefetch hook.
- Work: `ObjectBlockStore::azure`; prefetch overlapping blocks only; writer =
  `node_id`; owner-only compaction/retention.
- Gate: `cargo test -p parqtel-core block_store`; prefetch test counts downloads.

### B9 — Helm and docs
- Files: `deploy/helm/parqtel/{values.yaml,values.schema.json,templates/*}`,
  `docs/operations/deployment.md`, new `docs/operations/high-availability.md`.
- Work: allow `replicaCount >= 2` when `cluster.enabled`; headless peer Service;
  pod anti-affinity; startup probe `failureThreshold: 120`; PVC `30Gi` default for HA;
  persist the UAT env overrides (alert interval, inflight flushes) as values.
- Gate: `helm lint` with HA values, `make docs-check`.

### B10 — Failover tests
- Files: `scripts/checks/ha_failover.sh` or `tests/e2e/` (k3d + MinIO).
- Work: kill leader (new leader, ingest resumes, tail published, no gap); kill
  follower (leader takes rules); restart ex-leader; object-store outage; stale-leader
  fencing; no duplicate notifications.
- Gate: script green locally.

### B11 — UAT rollout (manual approval)
Not delegated. Requires B0.5 to pass on Ceph RGW, a bucket and credentials, and
approval. Validate 6 h soak, memory < 4 GiB per pod, zero restarts, query parity.

## Decisions taken

- Leader/follower over dual-ingest or active/standby (operator decision).
- Ingest routing: follower forwarding through one Service.
- Leases: separate objects with epoch fencing; catalog unchanged.
- Query merge: time cutoff, not fingerprint dedupe.

## Open decisions (before B4)

- 2 or 3 pods on UAT.
- `recent_window_secs` (default 300) versus block duration: UAT currently runs
  `block_duration_secs = 7200`; HA needs ≤ 240 (row-count cuts already make UAT blocks
  ~2 min, but the bound must hold by config).
- Whether object-store outage should keep the current leader ingesting past its TTL
  (availability) or stop it (consistency). The plan chooses consistency.

## Goal text for the coding agent

> Implement buddy peering tasks B0.5–B10 from `docs/development/buddy-peering-plan.md`
> on branch `feat/buddy-peering`, in order, one task per subagent. Stop after B0.5 if
> Ceph RGW lacks conditional PUT. Review each diff, run its gate, commit as
> `sonu202604 <sonu202604@gmail.com>`. With `[cluster] enabled = false`, single-node
> behavior must stay unchanged. Do not deploy to UAT (B11) without approval.
