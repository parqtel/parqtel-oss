# ADR 0001: Object storage uses a block-store seam, not `StorageEngine`

Status: accepted; P2-02 local seam implemented

## Context

The server's live read/write path is `IngestionService`/`LogIngestionService` →
`BlockWriter`/`SerialRotator` → `BlockIndex` + `Scanner`. `parqtel-core` also
contains a `StorageEngine` trait with a `ParquetStorageEngine` implementation,
but the server does not use it: that engine keeps a private in-memory index and
bypasses the WAL, memory buffer, shared index, compaction and retention.

Adding S3/GCS/Azure behind `StorageEngine` would therefore change an unused
code path. It would not make a server query read from a bucket.

## Decision

Introduce a `BlockStore` seam used by the code that actually owns blocks:

```rust
#[async_trait]
pub trait BlockStore: Send + Sync {
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<BlockVersion>;
    async fn get(&self, key: &str) -> Result<Arc<[u8]>>;
    async fn head(&self, key: &str) -> Result<Option<BlockMeta>>;
    async fn list(&self, prefix: &str) -> Result<Vec<BlockMeta>>;
    async fn delete(&self, key: &str, version: &BlockVersion) -> Result<()>;
}
```

* The WAL and staging remain local. Blocks are uploaded after the local file is
  durable; the local copy is a read cache, not the source of truth.
* `BlockIndex` remains the query-facing catalog in local mode. In shared mode a
  versioned manifest is authoritative.
* Scans, compaction and retention call `BlockStore`, never a bucket SDK
  directly.
* `ParquetStorageEngine` stays as the embedder/test backend and is not extended.

## Implementation status

P2-02 is implemented: `storage::block_store` defines the trait,
`LocalBlockStore` implements it, the server installs one at startup, and the
scanner and retention paths read/delete through it.

P2-03 is implemented: `ObjectBlockStore` uploads on close and serves reads
from a bounded cache. P2-04 is implemented: `ObjectCatalog` keeps a generation
and CAS version for the catalog manifest; writers merge by block key, and
the server's catalog task publishes local blocks and adopts remote ones.

## Shared catalog

`index.json` relies on atomic rename, which an object store does not provide.
Phase 2 therefore adds a per-signal manifest:

```json
{
  "generation": 42,
  "writer_lease": "node-a/2026-11-17T10:00:00Z",
  "blocks": [{"key": "...", "start_ns": 0, "end_ns": 0, "rows": 0, "bytes": 0}]
}
```

Writers publish with a conditional PUT on `generation` and a short lease;
readers poll/list and keep the last good manifest. Compaction publishes a new
generation that removes sources and adds the merged block; readers must finish
in-flight scans against the old generation (grace period), then the sources are
deleted.

## Failure modes to cover in P2-02…P2-05

- Torn upload: uploaded object is invisible until the manifest references it.
- Duplicate flush after crash: local WAL replay may produce the same block key;
  the manifest treats the key as idempotent and keeps one entry.
- Compaction race: two writers compact the same sources; the CAS loser retries
  from the new manifest.
- Query during delete: a reader pins the old manifest generation; deletion waits
  for the pin/grace period.
- Cache pressure: query pins prevent eviction of blocks being scanned; cache
  bytes are bounded and reported.

## Consequences

- P2-02 is a refactor with parity tests in local mode; no behavior change.
- P2-03 adds S3 and a local read cache; single-writer upload first.
- P2-04 is the multi-node gate; without it, multiple pods must not write.
- P2-05 ports compaction/retention and is where most object-store edge cases
  live.
- The Helm single-writer guard remains until P2-04 lands.
