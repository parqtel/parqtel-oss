# ADR 0001: Object storage uses a block-store seam, not `StorageEngine`

Status: accepted and implemented; one writer per prefix

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
pub trait BlockStore: Send + Sync {
    fn open(&self, key: &Path) -> Result<File>;
    fn delete(&self, key: &Path) -> Result<bool>;
    fn head(&self, key: &Path) -> Result<Option<BlockObject>>;
    fn list(&self, prefix: &Path) -> Result<Vec<BlockObject>>;
    fn publish(&self, key: &Path) -> Result<()>;
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
P2-05 is implemented: compaction and retention read/delete through
`BlockStore`, and catalog blocks carry a writer field so retirement is scoped
to the node that owns them. The 2026-11 object-storage QA found and fixed
temporary-name uploads, local staging resurrection, an encoded catalog key,
trace-block reconciliation and active-cache eviction; those fixes are part of
the current implementation.

## Shared catalog

`index.json` relies on atomic rename, which an object store does not provide.
Phase 2 therefore adds a per-signal manifest:

```json
{
  "generation": 42,
  "writer_lease": "node-a",
  "blocks": [{"key": "...", "start_timestamp_ns": 0, "end_timestamp_ns": 0,
              "row_count": 0, "size_bytes": 0, "writer": "node-a"}]
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
- P2-03 added S3 and a local read cache; uploads happen after the final rename.
- P2-04 added the shared catalog; readers can adopt blocks, but it is not a
  writer lease.
- P2-05 ported compaction/retention and is where most object-store edge cases
  live.
- The Helm single-writer guard remains because there is **no writer fencing**.
  One writer per prefix is the supported topology.
- GCS and Azure Blob are still out of scope; the seam is generic, but only S3 is
  wired.
