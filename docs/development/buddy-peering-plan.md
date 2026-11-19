# Buddy peering plan: two-node leader/follower HA

Status: design only. No product code in this branch. Based on `origin/main` `63a9cf1`.

Goal: run at least two Parqtel instances. One **leader** owns OTLP ingest and serves recent, in-memory data to peers. Each **follower** executes recording and alert rules, serves block-backed queries, publishes compacted blocks to object storage, and downloads blocks from object storage by query time range. Peer health and a shared lease provide failover.

## Invariants

1. Exactly one leader at a time. A follower must never accept external OTLP writes.
2. Every accepted record is written to the leader WAL before its buffer ACK.
3. Blocks have one owner (`CatalogBlock.writer`). Only the owner publishes, retires, or compacts them.
4. A follower never modifies a block it did not write. Remote blocks are read-only via the object-store cache.
5. A query sees leader buffer data plus local/remote blocks with no double counting. Buffer and blocks are disjoint by `wal_range`.
6. Rules execute on exactly one node: the follower when one is healthy, otherwise the leader in single-node fallback.
7. Feature is opt-in. With `[cluster] enabled = false`, behavior is unchanged and 1 replica still works.

## Target architecture

```text
                         OTLP / HTTP / gRPC
                                 |
                                 v
                     +------------------------+
                     |        LEADER          |
                     |  ingest + WAL + buffer  |
                     |  block publish          |
                     |  PeerService (gRPC)     |
                     +-----------+------------+
                                 | lease / peer RPC / recent points
          +----------------------+----------------------+
          |                                             |
          v                                             v
+---------------------------+              +---------------------------+
|        FOLLOWER A         |<------------>|        FOLLOWER B         |
| rules (alerts + record)   |  peer health | rules standby / quorum    |
| block queries             |              | block queries             |
| object-store sync/load    |              | object-store sync/load    |
+---------------------------+              +---------------------------+
                                 |
                                 v
                  S3 / Azure bucket: blocks + _catalog/manifest.json
```

Minimum deployment: **2 pods**. Recommended: **3 pods** (leader + 2 followers) so rules keep running after one follower failure. With 2 pods, leader must run rules in fallback when the follower is down.

## Reuse already in the tree

| Existing | Path | Use |
|---|---|---|
| `ObjectCatalog` CAS manifest with `generation` and `writer_lease` | `crates/parqtel-core/src/storage/object_catalog.rs` | leader lease + block catalog |
| `spawn_catalog_sync_task` publishes local blocks, adopts remote keys | `crates/parqtel-server/src/tasks.rs` | block sync skeleton |
| `BlockStore::publish` on block close | `writer.rs`, `compactor.rs`, `stream_compactor.rs` | upload compacted block |
| `ObjectBlockStore` bounded cache + `open` downloads on demand | `crates/parqtel-core/src/storage/block_store.rs` | timestamp-based downloads |
| `MemoryBuffer` and `QueryExecutor` | `crates/parqtel-core/src/buffer.rs`, `crates/parqtel-query/src/executor` | recent query source |
| tonic OTLP server | `crates/parqtel-server/src/grpc.rs` | add internal `PeerService` |
| alert eval and ruler tasks | `tasks.rs`, `server.rs`, `ruler.rs` | gate by role |
| Helm schema caps replicas at 1 | `deploy/helm/parqtel/values.schema.json` | change to min 2 when cluster enabled |

## Configuration

Add `[cluster]` to `Config` in `crates/parqtel-core/src/config`:

```toml
[cluster]
enabled = false
node_id = ""                # defaults HOSTNAME
role = "auto"               # auto | leader | follower (manual override for tests)
advertise_addr = ""         # e.g. parqtel-0.parqtel-headless:4318
peers = []                  # static peer URLs; empty = DNS/service discovery
bind_addr = "0.0.0.0:4318"
lease_ttl_secs = 30
lease_renew_secs = 10
buffer_lookback_secs = 300  # how far back peers request recent points
leader_rules_fallback = true
```

Extend `ObjectStoreConfig` with `provider = "s3" | "azure"` plus `azure_account`, `azure_container`, `azure_key` / connection string env fallback. Keep S3 defaults identical.

## Wire protocol (internal, not public OTLP)

New `crates/parqtel-server/proto/peer.proto` and `build.rs` (tonic). One service:

```proto
service Peer {
  rpc Status(StatusRequest) returns (StatusResponse);
  rpc QueryRecent(RecentRequest) returns (RecentResponse);
  rpc GetBlock(BlockRequest) returns (BlockResponse); // streamed bytes, cache hint
}

message StatusRequest {}
message StatusResponse {
  string node_id = 1;
  string role = 2;        // leader | follower
  uint64 lease_epoch = 3;
  int64 lease_expires_ms = 4;
  uint64 wal_position = 5;
  uint64 buffered_points = 6;
  bool ready = 7;
}
message RecentRequest {
  string metric = 1;
  string matchers_json = 2;
  int64 start_ns = 3;
  int64 end_ns = 4;
}
message RecentResponse {
  bytes points_json = 1;   // Vec<DataPoint> JSON, bounded by server limit
}
message BlockRequest { string key = 1; }
message BlockResponse { bytes parquet = 1; }
```

`QueryRecent` must be read-only and bounded: max points and max bytes from config; reject requests older than `buffer_lookback_secs`.

## Leader lease

Store lease in the object-store catalog manifest:

```json
{"generation": 42, "writer_lease": {"node": "parqtel-0", "epoch": 17, "expires_ms": 1794...}, "blocks": [...]}
```

- `try_acquire(node, ttl)`: CAS on generation; succeed only if lease absent or expired.
- `renew(node, epoch, ttl)`: CAS only if `node` and `epoch` match; bump generation each renew.
- `release(node, epoch)`: CAS; best effort on shutdown.
- If no object store configured, support static `role = leader` / `follower` for local tests only. HA requires object storage.
- Split-brain fence: every block publish and query of recent data checks lease epoch; a demoted leader stops ingest within one renew interval.

## Role state machine

```text
start -> candidate
candidate --acquire lease--> leader
candidate --lease held by other--> follower
leader --renew fails twice--> candidate
follower --leader Status unhealthy for 3 intervals--> candidate
manual role override -> fixed leader/follower (no lease writes)
```

On transition:
- to leader: start OTLP handlers, flush task, WAL replay; stop alert eval and ruler unless `leader_rules_fallback` and no healthy follower.
- to follower: stop OTLP handlers (return 503 with `Retry-After`), stop flush of new data, start alert eval + ruler, start catalog sync and peer status polling.
- never drop in-flight WAL on demotion; finish the current encode then stop.

## Query routing

`QueryExecutor` gains a `PeerBuffer` trait:

```rust
#[async_trait]
trait PeerBuffer {
    async fn recent(&self, metric: &str, matchers: &SeriesSelector,
                    start_ns: i64, end_ns: i64) -> Result<Vec<DataPoint>>;
}
```

Leader: uses local buffer as today; serves `QueryRecent` to followers.
Follower: queries local blocks (including adopted remote blocks) and calls `PeerBuffer::recent` for the overlap window; merges by `(fingerprint, timestamp)` and applies `max_series` / `max_samples_per_series` after merge. Full row predicate remains the final judge. Block results are already disjoint from leader buffer by WAL commit; dedupe is still required for overlapping adopted blocks during a flush/grace window.

## Block sync and timestamp download

- Keep `spawn_catalog_sync_task`. Change `writer` from `HOSTNAME` to `cluster.node_id`.
- Follower adopts remote blocks into its `BlockIndex` but records `path` under `<data_dir>/cache/<key>`; `ObjectBlockStore::open` materialises the file.
- Add `prefetch_for_query(blocks, start_ns, end_ns)` on follower: for blocks whose time range overlaps the query, call `BlockStore::open` with bounded parallelism, respecting `cache_bytes`. Never download all blocks.
- Object storage backends: S3 via `Elasticsearch/AmazonS3Builder` unchanged; Azure via `MicrosoftAzureBuilder` with account/container/key. Both expose `ObjectCatalog` and `ObjectBlockStore`.
- Retention and compaction remain leader-only. Follower must not delete objects; owner-only delete already enforced by `writer` field.

## Failure semantics

- Leader loss: follower acquires lease after TTL; last unflushed points (≤ `storage.block_duration_secs`, default 2h, configurable to ≤60s in HA) may be lost unless WAL is replicated. Phase 1 documents this as RPO; Phase 5 can add WAL shadowing.
- Follower loss: leader enables rules fallback; no query loss.
- Object-store outage: leader keeps ingesting locally; catalog publish errors are logged and retried; follower serves cached blocks only.
- Split brain: lease epoch check rejects writes from stale leader; metrics expose `parqtel_cluster_epoch`.

## Metrics and health

Add to `/metrics`: `parqtel_cluster_role{role}`, `parqtel_cluster_lease_epoch`, `parqtel_cluster_lease_expires_ms`, `parqtel_peer_status{peer,state}`, `parqtel_peer_recent_requests_total`, `parqtel_object_download_bytes_total`, `parqtel_object_cache_hits_total`. `/ready` returns 503 on follower for ingest paths; query paths stay ready.

## Tasks (ordered; each is one PR, lowest-cost model that passes gates)

### B0 — Design doc (current task)
- Files: this document.
- Gate: `make docs-check` (after adding doc to docs index if required). Done when committed.

### B1 — Cluster config + validation
- Files: `crates/parqtel-core/src/config/mod.rs`, new `cluster.rs`, `docs/operations/configuration.md`, config tests.
- Work: add `ClusterConfig` above; validation; Figment env mapping `PARQTEL__CLUSTER__*`; docs.
- Gate: `cargo test -p parqtel-core config`, `make lint`.

### B2 — Object-store lease on catalog
- Files: `crates/parqtel-core/src/storage/object_catalog.rs`, tests.
- Work: `try_acquire`, `renew`, `read_lease`, `release`; CAS; expiry; epoch monotonic. Keep `publish` preserving lease.
- Gate: `cargo test -p parqtel-core object_catalog`, clippy.

### B3 — Peer gRPC skeleton + Status
- Files: new `crates/parqtel-server/proto/peer.proto`, `crates/parqtel-server/build.rs`, `crates/parqtel-server/Cargo.toml`, `grpc.rs`, new `peer.rs`, tests.
- Work: generate tonic client/server; register service; `Status` returns role/epoch/WAL/buffer stats.
- Gate: `cargo test -p parqtel-server peer`, `cargo build -p parqtel-server`.

### B4 — Role manager
- Files: new `crates/parqtel-server/src/cluster.rs`, `server.rs`, `state.rs`, `metrics.rs`, tests.
- Work: candidate/leader/follower loop; lease acquire/renew; peer status polling; role in `AppState`; gates for ingest/flush and alert/ruler tasks; manual override.
- Gate: unit tests with in-memory object store; `cargo test -p parqtel-server cluster`.

### B5 — OTLP ingress gating + forwarding
- Files: `router.rs`, `handlers/ingest.rs`, `grpc.rs`, `peer.rs`, integration tests.
- Work: follower rejects or forwards to leader over `PeerService`; leader only writes WAL. Content type preserved. 503 response includes leader address.
- Gate: `cargo test -p parqtel-server ingest`, `make lint`.

### B6 — Recent buffer sharing
- Files: `crates/parqtel-query/src/executor/mod.rs`, `metrics.rs`, `crates/parqtel-server/src/peer.rs`, tests.
- Work: `PeerBuffer` trait; `QueryRecent` RPC with bounds; follower merge/dedupe; `max_series` after merge; equivalence test comparing leader-direct vs follower-proxied query.
- Gate: `cargo test -p parqtel-query executor`, `cargo test -p parqtel-server peer`.

### B7 — Rules only on follower
- Files: `server.rs`, `tasks.rs`, `ruler.rs`, tests.
- Work: start alert eval and ruler only when role=follower or fallback; stop on promote; state persistence unchanged; exactly-one evaluation test via shared alert store counter.
- Gate: `cargo test -p parqtel-server ruler`, `cargo test -p parqtel-alert`.

### B8 — Azure object store + timestamp prefetch
- Files: `config/object_storage.rs`, `block_store.rs`, `storage/index.rs` (catalog adoption), `tasks.rs`, tests.
- Work: Azure builder; `prefetch_for_query`; `node_id` writer; adopt remote blocks only into cache paths; retention owner check.
- Gate: `cargo test -p parqtel-core block_store`, `cargo test -p parqtel-core object_catalog`.

### B9 — Deployment manifests
- Files: `deploy/helm/parqtel/values.yaml`, `values.schema.json`, templates for headless service and anti-affinity, `docs/operations/deployment.md`, `docs/guides/...`.
- Work: `replicaCount >= 2` when cluster enabled; headless peer service; startup probe `failureThreshold: 120` (matches UAT fix); PVC per pod; env for cluster + object store; HPA validation message.
- Gate: `helm lint deploy/helm/parqtel`, `make docs-check`.

### B10 — Chaos/failover tests
- Files: `tests/e2e/` Go tests or scripted kind/k3d tests under `scripts/checks/`.
- Work: 3-node local stack; kill leader, assert new leader, ingest resumes, no duplicate alerts; kill follower, assert leader fallback rules; object-store outage; split brain fence.
- Gate: chosen harness green in CI-like local run.

### B11 — UAT rollout (manual decision)
- Not a coding task. Requires explicit approval. Validate 6h soak, memory <4Gi, no restarts, catalog parity, query results match across leader/follower.

## Suggested goal text for the coding agent

> Implement buddy peering tasks B1–B10 from `docs/development/buddy-peering-plan.md` on branch `feat/buddy-peering` in order. One task per subagent, review each diff, run its gate, commit as `sonu202604 <sonu202604@gmail.com>`. Keep the feature opt-in: with `[cluster] enabled = false`, single-node behavior must be unchanged. Do not deploy to UAT or start B11 without approval.

## Open decisions before B4

- 2 vs 3 pods on UAT. 2 satisfies the stated minimum but leaves no follower after a leader failover; `leader_rules_fallback` covers it.
- RPO for the last unflushed window. Recommend HA config `storage.block_duration_secs = 60`, `flush_interval_secs = 2`.
- Static peer list vs headless-service DNS. Recommend headless service; static list only for local tests.
- WAL shadowing to object storage (Phase 5, not in B1–B10).
