<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Large Object Writer + Chunk IO Interface + Location (R94)

This draft expands the implementation of the large-object writer, the
shared `ChunkIoWriter` async interface, and the `Location` type. Problem,
use scenarios, dependencies, and acceptance live in
[`doc/backlog/R94-chunkdb-large-object-writer.md`](../backlog/R94-chunkdb-large-object-writer.md);
the root design is
[`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
(§5.3 Chunk, §5.5 chunk types, §8 Allocation Flow, §9 Lifecycle, §10.6
`append_chunk`, §11 EC). Architecture decisions and rationale are in the
root design; this doc does not repeat them.

Already landed:
- `crow-chunkdb-client` (R85) — management RPCs
  (`allocate_chunk`/`append_chunk`/`seal_chunk`/`query_chunk`/`delete_chunk`/
  `delete_chunk_range`/`update_chunk_strip`/`list_chunks`), endpoint cache,
  R99 range routing. Management-only; R94 consumes it unchanged.
- `crow-diskio-client` (R105) — `DiskioClient::write`/`read`/`fsync` over
  the crow-rpc transport (`&RpcServer` + `&Connection` per diskio server).
- `crow-common::ec` (R85) — `EcScheme { data_num, code_num }`,
  `encode(scheme, &[u8])` (splits + zero-pads last shard, returns data +
  parity shards), `encode_parity`, `decode`.
- `crow-test-harness` — `KvCluster`, `DiskioProcess`/`DiskioStartOpts`,
  `connect_to_diskio`, `seed_hardware`, diskdb harness.

## 1. `crow-chunk-client` crate

### 1.1 Why

`crow-chunkdb-client` is management-only (chunk lifecycle RPCs) and must
stay that way: chunk management (allocate/append/seal/delete) is a
distinct concern from chunk data IO (EC encode, diskio write/fsync, strip
preallocation). R94, R106, and R107 all need a shared home for the data
path — the `Location` type, the `ChunkIoWriter` trait, the writers, the
reader, prefetch helpers. Putting them in `crow-chunkdb-client` would
pull diskio + EC + tokio pipeline deps into the management client. A new
`crow-chunk-client` lib owns the data path and calls into
`crow-chunkdb-client` (management) and `crow-diskio-client` (block IO).

### 1.2 How

New workspace member `lib/crow-chunk-client/`. Add to `members` in the
root `Cargo.toml`. `Cargo.toml` deps:

```toml
[dependencies]
crow-chunkdb-client = { workspace = true }   # management RPCs
crow-diskio-client  = { workspace = true }   # block write/fsync (R105)
crow-common         = { workspace = true }   # EcScheme, ec::encode
crow-protocol       = { workspace = true }   # ChunkId, *ChunkRequest types
tokio    = { workspace = true, features = ["rt", "rt-multi-thread", "sync", "io-util", "macros"] }
bytes    = "1"
prost    = { workspace = true }
thiserror = { workspace = true }
tracing  = { workspace = true }
async-trait = "0.1"
futures = { version = "0.3", default-features = false }   # AsyncRead glue
```

`AsyncRead` comes from `tokio::io` (re-export of `futures::AsyncRead` via
the `io-util` feature is not enough; use `tokio::io::AsyncRead` which
requires `features = ["io-util"]`). The `write_stream` signature uses
`impl AsyncRead + Unpin + Send`.

`src/lib.rs` re-exports the public surface:

```rust
pub mod error;
pub mod location;
pub mod io;
pub mod prefetch;
pub mod traits;
pub mod writer;

pub use error::{IoError, Result};
pub use location::Location;
pub use io::{ChunkIoWriter, FeedStatus, BackpressurePolicy};
pub use traits::{ChunkAllocator, BlockWriter};
pub use writer::large_object::{LargeObjectWriter, WriterConfig};
pub use prefetch::ChunkPrefetch;
pub use writer::pool::WriterPool;
```

### 1.3 Trait seams for testability

The writer calls a small subset of `ChunkdbClient` (allocate, append,
seal, delete, update_chunk_strip, query) and `DiskioClient` (write,
fsync). To enable class-level mocking in integration tests (mock
allocate-chunk-strip, free-block, inject delays/errors) without running
real servers, the writer is generic over two traits defined in this
crate:

```rust
/// Chunk lifecycle operations the writer needs. Mirrors the subset of
/// `ChunkdbClient` methods used by the data path. `ChunkdbClient`
/// implements this; integration tests use a mock impl.
#[async_trait]
pub trait ChunkAllocator: Send + Sync {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse>;
    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse>;
    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<()>;
    async fn delete_chunk(&self, chunk_id: ChunkId) -> Result<()>;
    async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<()>;
    async fn query_chunk(&self, chunk_id: ChunkId) -> Result<QueryChunkResponse>;
}

/// Block-level IO the writer needs. Mirrors the subset of
/// `DiskioClient` methods used by the data path. `DiskioClient`
/// implements this; integration tests use a mock impl.
#[async_trait]
pub trait BlockWriter: Send + Sync {
    async fn write(&self, disk_id: DiskId, zone_index: u64, offset: u64, data: Bytes) -> Result<()>;
    async fn fsync(&self, disk_id: DiskId) -> Result<()>;
}
```

`ChunkdbClient` and `DiskioClient` get `impl ChunkAllocator for
ChunkdbClient` and `impl BlockWriter for DiskioClient` in this crate
(or in their own crates if that's cleaner — decide at impl time).
`LargeObjectWriter` and `WriterPool` are generic over `A: ChunkAllocator`
and `W: BlockWriter` (see §4.2, §10.2). E2E tests construct the writer
with real `ChunkdbClient` + `DiskioClient`; integration tests use mock
impls that record calls and inject delays/errors.

### 1.4 Edge cases

- `unsafe_code = deny` applies (workspace lint). No `unsafe` needed in
  this crate — EC `unsafe` lives in `crow-common::ec_isal`.
- Clippy `pedantic = warn`; gate with `pixi run cargo clippy --all-targets
  -- -D warnings`.

## 2. `Location` type

### 2.1 Why

There is no addressing unit for object data within chunks. Both the
writer (R94, R106) and the reader (R107) need a common type that records
where a contiguous byte range of one object lives within one chunk, plus
the object-level logical offset so a multi-chunk object reads back as one
contiguous stream.

### 2.2 How

`lib/crow-chunk-client/src/location.rs`:

```rust
use crow_protocol::common::ChunkId;   // { high: u64, low: u64 }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub chunk_id: ChunkId,
    pub offset: u64,           // byte offset within the chunk [start)
    pub length: u64,           // byte length stored in this chunk
    pub logical_offset: u64,   // object-level logical offset
    pub logical_length: u64,   // object-level logical length covered here
}
```

- An object spanning N chunks has a `Vec<Location>` of N entries ordered
  by `logical_offset`, contiguous and non-overlapping.
- `offset` is always 0 for R94 (the writer fills each dedicated chunk from
  offset 0); the field exists for R106 (shared chunks, objects packed at
  arbitrary offsets) and R107 (range reads).
- Serialization: protobuf (`Location` proto, new message in
  `crow-protocol/src/proto/chunkdb_type.proto`) for KV-stored object
  metadata, plus a compact binary encoding (`to_bytes`/`from_bytes`: 16
  chunk_id + 4×8 = 48 bytes) for inline embedding. The compact form is
  `<48 bytes` per location, satisfying the acceptance size check.

New proto message:

```proto
message Location {
  crow.common.ChunkId chunk_id      = 1;
  uint64              offset        = 2;
  uint64              length        = 3;
  uint64              logical_offset = 4;
  uint64              logical_length = 5;
}
```

### 2.3 Edge cases

- Empty object (size 0) → `Vec<Location>` is empty; no chunk allocated.
  Documented; R94's `write_stream` returns `Ok(vec![])` without touching
  chunkdb/diskio.
- `logical_length` may be < `length` in future hole-punch scenarios; R94
  always sets them equal.

## 3. `ChunkIoWriter` async interface

### 3.1 Why

R106's small-object writer is push-driven (an HTTP handler pushes object
bytes as they arrive). R94's `LargeObjectWriter` is stream-driven but
also exposes a manual push API. Both need a common push-based trait with
explicit backpressure so a caller can either block (dedicated upload
task) or back off (shared handler task returning 503). A bare `bool`
return is ambiguous; `FeedStatus` answers "do you need more data?"
directly.

### 3.2 How

`lib/crow-chunk-client/src/io.rs`:

```rust
/// Result of `on_data` — does the writer need more data?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedStatus {
    /// Buffer stored; writer has capacity — send more data.
    Continue,
    /// Buffer stored; writer is at capacity — pause feeding.
    /// Poll `require_data()` before resuming.
    Pause,
}

/// Caller-side backpressure strategy. Selects how to react when
/// `require_data()` returns false. Not a property of the writer.
#[derive(Debug, Clone, Copy)]
pub enum BackpressurePolicy {
    /// Dedicated upload task: ignore `require_data`, call `on_data`
    /// directly — it blocks until capacity. Use when the task has
    /// nothing else to do.
    Blocking,
    /// Shared handler task: check `require_data()` first; if false,
    /// return 503 / apply TCP flow control — never block the handler.
    NonBlocking,
}

#[async_trait]
pub trait ChunkIoWriter: Send {
    /// Push a data buffer. **Always stores the buffer** (awaits until
    /// internal capacity is available — never rejects). Returns
    /// `Continue` if the next push would not block, `Pause` if the
    /// writer is now at capacity.
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus>;
    /// End of input: flush, seal, return the `Location` array.
    async fn on_finish(&mut self) -> Result<Vec<Location>>;
    /// Abort: return `Location`s of already-sealed chunks for cleanup.
    async fn on_error(&mut self) -> Result<Vec<Location>>;
    /// Non-async pre-check: `true` if `on_data` would not block now.
    fn require_data(&self) -> bool;
}
```

Contract (encoded in doc comments, per R94 Open Questions resolution):
`on_data` never rejects a buffer — "always store" pushes the retry-loop
complexity into the writer (one place) instead of every caller.
`require_data` is a cheap hint; the two caller strategies are selected by
`BackpressurePolicy` on the caller side, not by the writer.

`LargeObjectWriter` (§4) implements `ChunkIoWriter`; its `on_data` feeds
the same fetch→write pipeline that `write_stream` drives internally.
In `write_stream` mode, the fetch stage pulls from `AsyncRead` and
pushes `Bytes` blocks to the block channel. In `on_data` (push) mode,
there is no fetch stage task — `on_data` sends the caller's `Bytes`
directly to the same block channel (with the same backpressure: awaits
when the channel is full / `max_cached_buffer` exhausted). The main
write task and parity tasks are identical in both modes — they consume
from the block channel and don't know or care whether blocks came from
a fetch stage or a push caller. `require_data` is a non-async check of
the block channel's current capacity: `true` if the channel is not
full (an `on_data` push would not block), `false` if it is full.

### 3.3 Edge cases

- `on_data` called after `on_finish` or `on_error` → `IoError::Finished`.
- `on_finish` called twice → `IoError::Finished`.
- `on_error` with no sealed chunks → returns `Ok(vec![])`.

## 4. `LargeObjectWriter` + `WriterConfig`

### 4.1 Why

A large object (> EC strip data capacity) needs a dedicated chunk per
object (or a chain of chunks for very large objects) with direct EC strip
writes. The writer must drive a block-granularity pipeline so the first
disk write starts after 1 MB (one data block), not after the full 4 MB
strip, and so parity compute/write/fsync overlaps the next strip's data
writes.

### 4.2 How

`lib/crow-chunk-client/src/writer/large_object.rs`:

```rust
pub struct WriterConfig {
    pub max_chunk_size: u64,          // default 1 GB (rotation threshold)
    pub prealloc_depth: usize,        // default 2 strips ahead
    pub parity_depth: usize,          // default 2 parity tasks in flight
    pub chunk_prefetch_depth: usize,  // default 1 chunk ahead
    pub read_buffer_size: usize,      // default 1 MB (one data block)
    pub max_cached_buffer: usize,     // default 4 MB (un-written budget)
}

impl Default for WriterConfig { /* defaults above */ }

pub struct LargeObjectWriter<A: ChunkAllocator, W: BlockWriter> {
    chunkdb: A,
    diskio: W,
    rpc_server: RpcServer,            // shared diskio transport
    diskio_conns: HashMap<DiskId, Connection>,  // per-disk connection cache
    ec_scheme: EcScheme,
    config: WriterConfig,
    // pipeline state (initialized on write_stream / first on_data):
    //   block_rx channel (fetch → main write)
    //   strip_tx channel (main write → parity task pool)
    //   prealloc strip/channel handles
    //   accumulated Locations
}

impl<A: ChunkAllocator, W: BlockWriter> LargeObjectWriter<A, W> {
    pub fn new(
        chunkdb: A,
        diskio: W,
        rpc_server: RpcServer,
        ec_scheme: EcScheme,
        config: WriterConfig,
    ) -> Self;

    /// Primary API: stream-driven. Drives fetch + main write + parity
    /// pipeline internally. `object_size` known → pre-calculate strip/
    /// chunk count for planning but still pre-allocate `prealloc_depth`
    /// ahead; `None` → streaming, on-demand strips.
    pub async fn write_stream(
        &mut self,
        reader: impl AsyncRead + Unpin + Send,
        object_size: Option<u64>,
    ) -> Result<Vec<Location>>;
}
```

`ChunkId` generation: `crow_common` has the 128-bit generator (R85). The
writer mints a `ChunkId` with the Repo type prefix (0) for each chunk it
allocates.

**Chunk category clarification**: the proto `AllocateChunkRequest` has
`chunk_type` (purpose: `CHUNK_TYPE_REPO`) but no Metadata/Shared/Specific
category field — the design doc §5.3 "Type: Metadata, Shared, Specific"
is a usage concept, not a landed proto field. R94 allocates
`CHUNK_TYPE_REPO` + EC strips; "Specific" is the usage pattern (one
object per chunk), not a proto value. R106's "Shared" is likewise a
Repo chunk shared across many small objects. No proto change needed for
the category.

`AllocateChunkRequest` built by the writer:
`{ chunk_id, write_granularity: 1024 (1 MB units), strip_count: 1
(initial — start writing immediately), strip_type: EC, data_num,
code_num, copy_count: 0, chunk_type: REPO }`. Subsequent strips via
`AppendChunkRequest { chunk_id, strip_size, strip_count: 1, strip_type:
EC, data_num, code_num, copy_count: 0 }`.

### 4.3 Edge cases

- Empty object (`object_size == Some(0)` or stream EOF immediately) →
  return `Ok(vec![])`, no allocation.
- `object_size` provided but stream yields fewer bytes → seal at actual
  written length; `Location.length` reflects actual bytes, not the
  hinted size.
- `object_size` provided but stream yields more bytes → keep writing,
  allocate beyond the planned strip count (treat hint as planning only).

## 5. Pipeline stages

### 5.1 Why

Block-granularity overlap (fetch 1 MB → write data block immediately →
hand parity off in background → advance) is what makes the first disk
write start after 1 MB and keeps three strips in flight (N parity, N+1
data, N+2 fetch) without unbounded memory.

### 5.2 How

`lib/crow-chunk-client/src/writer/pipeline.rs`. `write_stream` launches:

a. **Prealloc task** (§6) — `tokio::spawn`. Allocates chunk 1 with 1
   strip, then appends strips up to `prealloc_depth` ahead of the write
   cursor. Delivers `(strip_index, EcStrip placement)` via a bounded
   `mpsc` channel (capacity `prealloc_depth`). Pre-allocates the next
   chunk via `ChunkPrefetch` (§6) when the current chunk is within
   `prealloc_depth` strips of `max_chunk_size`.

b. **Fetch stage** — reads from the `AsyncRead` in ≤
   `read_buffer_size` (1 MB) per `poll_read`. A socket read may return
   less (64 KB); the fetch stage accumulates into a 1 MB `BytesMut`,
   and when it reaches `read_buffer_size` (or EOF), freezes it to
   `Bytes` and sends `Bytes` to the main write task via a bounded
   channel. The bounded channel capacity implements
   `max_cached_buffer` backpressure: the fetch stage awaits when the
   channel is full (un-written data budget exhausted). In-flight
   parity data is bounded separately by `parity_depth` (§10.2) — the
   two limits are independent. Each 1 MB block is sent immediately —
   no wait for the full 4 MB strip. The fetch stage does NOT track
   block indices or strip boundaries — it just sends sequential 1 MB
   `Bytes` blocks; the main write task tracks block/strip indices.

c. **Main write task** — the central coordinator. It receives from
   TWO channels:
   - **Prealloc placement channel** (bounded `mpsc`, capacity
     `prealloc_depth`): delivers `(strip_index, EcStrip placement)`
     — the disk segments assigned to each strip.
   - **Fetch block channel** (bounded `mpsc`, capacity derived from
     `max_cached_buffer / read_buffer_size`): delivers sequential
     `Bytes` blocks from the fetch stage.

   At the start of each new strip, the main write task **first awaits
   the next placement** from the prealloc channel (blocks if prealloc
   fell behind — backpressure). Then it receives `data_num` blocks
   from the fetch channel, one at a time. For EC 4+1, the 4 data
   blocks ARE the raw strip data split into 1 MB — no EC needed for
   data. The main write task tracks `block_idx = received_count %
   data_num` and `strip_idx = received_count / data_num` internally
   (the fetch stage sends raw blocks, no index). Writes block `i` to
   the strip's segment `i` via `BlockWriter::write(segment.disk_id,
   segment.zone_index, byte_offset, block_bytes)`. `byte_offset` =
   `segment.unit_offset * unit_size_bytes` (the `BlockWriter::write`
   offset is in bytes; `Segment.unit_offset` is in units — convert at
   the call site). When all `data_num` blocks of strip N are written,
   hands the strip's data `Bytes` (shared via ref count, no copy) to a
   parity task (bounded `parity_depth` semaphore), records the parity
   task's `JoinHandle` in the current chunk's handle list, and
   **immediately advances** to strip N+1 block 0 — no wait for EC.
   On partial last strip (EOF before `data_num` blocks — partial
   strips only occur at EOF, never mid-chunk since rotation happens at
   strip boundaries), writes only the filled blocks, releases the
   empty ones, hands off for partial EC, records `sealed_length` (in
   units) for `seal_chunk`.

d. **Parity tasks** — one per strip, `tokio::spawn`, bounded by a
   `parity_depth`-slot semaphore. Receives the strip's data shards
   (full or partial). EC-encodes parity via the new
   `crow_common::ec::encode_parity_from_shards` (§7), writes the
   `code_num` parity blocks to the remaining segments via
   `BlockWriter::write`, then `fsync`s all `total_blocks` disks of the
   strip via `BlockWriter::fsync`. On completion, releases the
   semaphore slot.

e. **Drain** — on EOF: fetch sends the partial last block → main write
   writes it + hands off the last parity task → `write_stream` joins all
   in-flight parity tasks for the current chunk (previous chunks'
   parity tasks were already joined at rotation; at EOF without
   rotation, only the current chunk's tasks are in flight) → seals the
   current chunk → returns the accumulated `Vec<Location>`.

### 5.3 Edge cases

- Stream returns 0 bytes on a non-EOF `poll_read` (socket stall) →
  fetch stage retries the read (standard `AsyncRead` semantics); no
  spurious EOF.
- Partial last block (< 1 MB) → sent as a short `Bytes`; main write
  writes the short block to disk (diskio accepts `data.len()` < 1 MB);
  parity task gets a short last shard (partial EC, §7).
- Parity semaphore full (parity_depth in flight) → main write task
  blocks at the hand-off until a slot opens (backpressure on the write
  path).

## 6. Bounded preallocation + chunk prefetch

### 6.1 Why

A 1 TB object must not allocate all 250K strips at once (allocation
pressure on chunkdb + KV metadata). Bounded depth (default 2 strips + 1
chunk ahead) keeps memory and allocation rate bounded regardless of
object size while keeping the write cursor fed.

### 6.2 How

`lib/crow-chunk-client/src/prefetch.rs`:

a. **Strip preallocation** — background task. Allocates chunk 1 with 1
   strip (`allocate_chunk`, `strip_count: 1`). Then loops: while the
   number of strips allocated-but-not-yet-consumed by the write cursor
   is < `prealloc_depth`, call `append_chunk(chunk_id, strip_count: 1)`
   and push the new strip's placement onto the bounded channel. If
   `object_size` is known, pre-calculate `total_strips = ceil(size /
   strip_data_capacity)` and `total_chunks` for planning/logging only —
   still only `prealloc_depth` ahead. When the current chunk's
   accumulated size ≥ `max_chunk_size` after allocating a full strip
   (same threshold check as the main write task's rotation in §8.2),
   stop appending to it and switch to the prefetched next chunk. The
   prealloc task and main write task independently reach the same
   "chunk full" decision because both count strips allocated/written
   to the current chunk and use the same `≥ max_chunk_size` check.

b. **Chunk prefetch** (`ChunkPrefetch`) — pre-allocates the next chunk
   (1 strip) via `allocate_chunk` when the current chunk is within
   `prealloc_depth` strips of `max_chunk_size`, up to
   `chunk_prefetch_depth` (default 1) chunk ahead. Delivers the new
   `ChunkId` + strip 0 placement via a `oneshot` channel. R106 reuses
   this for shared-chunk pre-allocation.

c. **Backpressure** — if prealloc falls behind (allocation slower than
   write), the main write task awaits on the strip channel (bounded) —
   the next strip is not ready. The fetch stage continues filling
   `max_cached_buffer`, then blocks when full. No data lost.

### 6.3 Edge cases

- `allocate_chunk` / `append_chunk` transient error → prealloc task
  retries with backoff (reuses `ChunkdbClient`'s built-in retry). After
  exhausting retries, sends an error into the strip channel; the main
  write task surfaces it as `IoError::AllocationFailed`.
- `object_size = None` (streaming) → prealloc starts with 1 strip and
  keeps `prealloc_depth` ahead indefinitely; chunk prefetch triggers
  when the current chunk crosses `max_chunk_size`.

## 7. EC extension: shard-based + partial encode

### 7.1 Why

The existing `crow_common::ec::encode(scheme, &[u8])` re-splits a
contiguous buffer into `data_num` equal shards and zero-pads the last
shard. R94's pipeline already has the data as `data_num` separate 1 MB
`Bytes` blocks (written to disk as-is) and must not copy them into a
contiguous buffer just to re-split. The last strip may be partial (e.g.
2 MB of 4 MB) — isa-l supports partial EC (encode parity from fewer
than `data_num` data blocks, no padding); the R94 Open Question requires
verifying this with a UT first.

### 7.2 How

Add to `lib/crow-common/rust/src/ec.rs`:

```rust
/// Encode parity from pre-split data shards. `data_shards.len()` must
/// be `data_num` for a full strip, or `< data_num` for a partial strip
/// (last strip of an object). All shards must be equal length for a
/// full strip; for a partial strip the present shards are equal length
/// and the missing ones are treated as zero (no padding written to
/// disk — the reader reads only `sealed_length` bytes).
/// Returns `code_num` parity shards, each the same length as a data
/// shard.
pub fn encode_parity_from_shards(
    scheme: EcScheme,
    data_shards: &[&[u8]],
) -> Result<Vec<Vec<u8>>>;
```

Implementation: build the full `data_num` shard vector (present shards +
zero-filled placeholders for missing ones, all same length), call
`isal_encode` on the data shard slices, return the parity. The data
placeholders are never written to disk — only parity is. This reuses the
existing isa-l FFI path; no new C++ code.

**Verification gate (R94 Open Question)**: before using
`encode_parity_from_shards` in the pipeline, write a UT that encodes
parity from 2 of 4 data shards (2 MB of 4 MB), then decodes with 2 data
+ 1 parity and verifies the 2 data shards reconstruct exactly. If isa-l
does not support partial EC as expected, **stop and involve the user**
per the R94 Open Question — do not fall back to padding.

### 7.3 Edge cases

- Full strip (`data_shards.len() == data_num`) → standard EC, parity
  covers all data.
- Partial strip (`data_shards.len() < data_num`) → parity from present
  shards; missing shards treated as zero for isa-l's matrix; reader
  reads only `sealed_length` bytes.
- Single-block object (< 1 MB) → 1 partial data shard, 1 parity shard;
  `sealed_length` = actual bytes.

## 8. Chunk rotation

### 8.1 Why

Very large objects (> `max_chunk_size`) span multiple chunks. Chunk size
is always a multiple of strip data capacity (the writer only appends
whole strips), so rotation happens at strip boundaries — never mid-strip.
`max_chunk_size` is a threshold: after a full strip, if the chunk's
accumulated size ≥ `max_chunk_size`, seal and rotate.

### 8.2 How

In the main write task, after completing a full strip (all `data_num`
data blocks written + handed to parity), check
`current_chunk_bytes + strip_data_capacity > max_chunk_size` (or `≥` —
use `≥` so a chunk that exactly hits the threshold rotates after the
strip that reaches it). If rotating:

a. Join in-flight parity tasks for the current chunk (wait for
   fsyncs) — the main write task tracks a `Vec<JoinHandle>` of parity
   tasks spawned for the current chunk; at rotation, it awaits all of
   them, then clears the list.
b. `seal_chunk(chunk_id, sealed_length = current_chunk_bytes / unit)`.
c. Record `Location { chunk_id, offset: 0, length:
   current_chunk_bytes, logical_offset, logical_length:
   current_chunk_bytes }`; advance `logical_offset` by
   `current_chunk_bytes`.
d. Switch to the prefetched next chunk (await `ChunkPrefetch`'s oneshot
   if not ready).
e. Prealloc task starts appending strips to the new chunk + prefetching
   the next chunk.

The `Location` array accumulates one entry per rotated chunk, ordered by
`logical_offset`.

### 8.3 Edge cases

- Prefetch fell behind (next chunk not ready at rotation) → main write
  task blocks at step (d) until the oneshot delivers. Backpressure; no
  data lost.
- `max_chunk_size` not a multiple of strip data capacity → rotation
  still happens at strip boundaries (the threshold is crossed after a
  full strip); the actual chunk size is the multiple-of-strip value at
  or just above the threshold. Documented in R94 Open Questions
  (resolved).

## 9. Completion + error handling (R94 basic)

### 9.1 Why

`write_stream` must seal the final chunk and return the `Location` array
on success. On error/abort, it must not leak partial chunks and must
return already-sealed `Location`s for caller cleanup. R94 ships coarse
whole-strip retry; R110 refines to single-block replacement.

### 9.2 How

Completion (§5.2 step e): join all parity tasks → `seal_chunk` → return
`Vec<Location>`.

Error / abort (`on_error` or `Drop`):

a. Cancel in-flight pipeline tasks (abort fetch, main write, parity
   tasks — drop the channel senders / use a `CancellationToken`).
b. Free partial (non-sealed) chunks via `delete_chunk`.
c. Return `Location`s of already-sealed chunks for caller cleanup.

Whole-strip retry (R94): on a diskio write failure for any block of a
strip, retry the whole strip — `append_chunk` a new strip with a fresh
placement (chunkdb picks new disks), re-write all `data_num` data blocks
+ parity to the new strip, free the failed strip's segments via
`update_chunk_strip` (or `delete_chunk` of a temp chunk). Up to 3
retries; on exhaustion, `IoError::WriteFailed` with the partial
`Location` array. R110 replaces this with single-block replacement
(keep successful blocks, re-allocate only the failed block); R94's error
paths (`CancellationToken`, partial-`Location` return, `delete_chunk`
cleanup) are the integration points R110 hooks into.

`Drop` impl: calls abort as a safety net if the caller drops the writer
without `on_finish`/`on_error`. Uses a `CancellationToken` flagged on
drop; the pipeline tasks observe it and exit.

### 9.3 Edge cases

- EC encode failure (parity task) → parity task returns
  `IoError::EcEncodeFailed`; the pipeline aborts immediately (R94 ships
  abort — no retry, since EC encode is a CPU/isal error, not a placement
  issue; retrying with a new placement won't help). R110 may refine this
  to mark the strip degraded (data durable, parity missing) or retry with
  a fallback encoder; R94's abort path is the integration point.
- `delete_chunk` fails during cleanup → log + continue (best-effort
  cleanup; the partial chunk stays Active and is reaped later by a
  future GC task, out of R94 scope).

## 10. High-parallel write / memory budget

### 10.1 Why

Many concurrent large-object uploads must be bounded by available RAM,
not by object size — a 1 TB and a 50 MB object use the same ~15 MB. A
`WriterPool` rejects new writes when the budget is exhausted, enabling
backpressure up the call stack.

### 10.2 How

`lib/crow-chunk-client/src/writer/pool.rs`:

```rust
pub struct WriterPool<A: ChunkAllocator + Clone, W: BlockWriter + Clone> {
    chunkdb: A,
    diskio: W,
    rpc_server: RpcServer,
    ec_scheme: EcScheme,
    config: WriterConfig,
    memory_budget: usize,
    in_use: AtomicUsize,   // sum of per-writer footprints
}

impl<A: ChunkAllocator + Clone, W: BlockWriter + Clone> WriterPool<A, W> {
    pub fn new(/* deps + memory_budget */) -> Self;
    /// Acquire a writer. Returns `MemoryBudgetExhausted` if the budget
    /// is full. Each writer's footprint =
    ///   max_cached_buffer + read_buffer_size
    ///   + parity_depth * (data_num * read_buffer_size + code_num * read_buffer_size)
    /// (data buffers shared via Bytes ref count, but resident until EC
    /// compute completes).
    pub fn try_acquire(&self) -> Result<LargeObjectWriter<A, W>>;
}
```

Per-writer footprint (4+1 EC, 1 MB blocks, defaults):
`max_cached_buffer` (4 MB) + `read_buffer_size` (1 MB, the block being
written) + `parity_depth` (2) × (data_num (4) + code_num (1)) × 1 MB =
4 + 1 + 2 × 5 = **15 MB**. `max_concurrent = memory_budget / 15 MB`.

**Memory accounting note** (R94 Open Question): the 15 MB assumes both
in-flight parity tasks can be in the EC compute phase simultaneously,
each holding the strip's 4 data blocks via shared `Bytes` refs. In
practice EC compute (isa-l) is fast vs disk write + fsync, so the two
parity tasks may stagger (one computing while the other is in parity
write/fsync with data refs already released) — realistic steady-state
peak ~11 MB. R94 uses the conservative 15 MB for the budget calculation.
A ref-count tracking mechanism (increment on EC submit, decrement on EC
job finish, with leak detection at shutdown) catches missed decrements
during development. `max_cached_buffer` covers only fetch-cache data,
not parity-held data — the two limits are independent (fetch
backpressure vs total memory budget).

### 10.3 Edge cases

- Budget exhausted → `try_acquire` returns
  `IoError::MemoryBudgetExhausted`; the caller retries when a writer is
  released (release decrements `in_use` on `Drop`).
- Writer dropped mid-write → `Drop` aborts the pipeline (§9) and
  decrements `in_use`.

## Scope

New files (all under `lib/crow-chunk-client/`):
- `Cargo.toml` — workspace member, deps per §1.2.
- `src/lib.rs` — re-exports.
- `src/error.rs` — `IoError` enum + `Result`.
- `src/location.rs` — `Location` + proto/binary ser/de.
- `src/io.rs` — `ChunkIoWriter` trait, `FeedStatus`, `BackpressurePolicy`.
- `src/traits.rs` — `ChunkAllocator`, `BlockWriter` trait seams (§1.3)
  + impls for `ChunkdbClient` / `DiskioClient`.
- `src/prefetch.rs` — strip preallocation task + `ChunkPrefetch`.
- `src/writer/mod.rs` — writer module root.
- `src/writer/large_object.rs` — `LargeObjectWriter`, `WriterConfig`,
  `write_stream`, rotation, completion, error/abort, `Drop`.
- `src/writer/pipeline.rs` — fetch + main write + parity task plumbing.
- `src/writer/pool.rs` — `WriterPool`.
- `tests/large_object_writer.rs` — unit + integration tests.
- `tests/large_object_writer_e2e.rs` — E2E tests with real servers.

Modified files:
- `Cargo.toml` (root) — add `lib/crow-chunk-client` to `members`.
- `lib/crow-common/rust/src/ec.rs` — add
  `encode_parity_from_shards` (§7).
- `lib/crow-protocol/src/proto/chunkdb_type.proto` — add `Location`
  message (§2.2); regenerate proto bindings.

No changes to `crow-chunkdb-client` (consumed unchanged) or
`crow-diskio-client` (consumed unchanged).

## Complexity

**High.** The pipeline (fetch → main write → background parity with
bounded depth, block-granularity overlap, chunk rotation, backpressure)
is the genuinely hard part — coordinating three concurrent stages with
bounded memory, correct drain on EOF, and crash-safe abort. New code is
most of the crate; reused pieces are `crow-chunkdb-client` (management
RPCs), `crow-diskio-client` (block IO), and `crow-common::ec` (isa-l,
extended with one new entry point). Main challenges: (1) the
partial-EC UT gate (§7) — must verify isa-l partial encode before
building the pipeline on top of it; (2) correct backpressure accounting
so `max_cached_buffer` and `parity_depth` together bound memory without
deadlock (fetch blocked → main write blocked → parity never drains);
(3) chunk rotation without stalling (prefetch must stay ahead);
(4) `Drop` abort that reliably cancels in-flight tasks and frees
partial chunks.

## Test Design

### Unit tests (UT)

`tests/large_object_writer.rs` (pure logic, no external deps):

- **`Location` proto round-trip**: build a `Location`, encode to proto,
  decode, assert all fields preserved.
- **`Vec<Location>` 3-entry round-trip**: 3 multi-chunk entries, encode
  + decode, assert 3 entries with correct `chunk_id`/`offset`/`length`/
  `logical_offset`/`logical_length`.
- **`Location` binary size**: encode to compact binary, assert
  `< 64 bytes`.
- **`ChunkIoWriter` mock**: a mock impl — `on_data` returns
  `FeedStatus::Continue`, `on_finish` returns a `Vec<Location>`,
  `on_error` returns a `Vec<Location>`; verifies the trait is
  implementable and the contract is sound.
- **`encode_parity_from_shards` full strip**: 4 data shards (1 MB each),
  encode parity (1 shard), decode with 4 data + 1 parity, assert data
  reconstructs. **This is the R94 Open Question verification gate.**
- **`encode_parity_from_shards` partial strip (2 of 4)**: 2 data shards
  (1 MB each), encode parity, decode with 2 data + 1 parity (2 missing
  treated as zero), assert the 2 present shards reconstruct exactly.
  **If this fails, stop and involve the user — do not fall back to
  padding.**
- **`encode_parity_from_shards` single-block (1 of 4, < 1 MB)**: 1
  short data shard, encode parity, assert parity length matches shard
  length.
- **`WriterConfig` defaults**: `Default::default()` yields
  `max_chunk_size = 1 GB`, `prealloc_depth = 2`, `parity_depth = 2`,
  `chunk_prefetch_depth = 1`, `read_buffer_size = 1 MB`,
  `max_cached_buffer = 4 MB`.
- **Empty object**: `write_stream` with an empty `Cursor` +
  `object_size = Some(0)` → `Ok(vec![])`, no chunkdb/diskio calls
  (verify with a mock that records calls).
- **`on_data` after `on_finish`**: returns `IoError::Finished`.
- **`on_finish` called twice**: first call returns `Vec<Location>`;
  second call returns `IoError::Finished`.
- **`on_error` with no sealed chunks**: returns `Ok(vec![])`, no
  cleanup calls.
- **Object size hint mismatch — fewer bytes**: `object_size = Some(50 MB)`
  but stream yields only 30 MB → seals at 30 MB, `Location.length = 30 MB`
  (hint is planning only, not enforced).
- **Object size hint mismatch — more bytes**: `object_size = Some(30 MB)`
  but stream yields 50 MB → writer allocates beyond planned strip count,
  seals at 50 MB, `Location.length = 50 MB`.
- **Object size exactly equals strip data capacity**: 4 MB object with
  4+1 EC (4 MB strip capacity) → 1 full strip, no partial EC, chunk
  sealed with `sealed_length = 4 MB`.

### Integration tests (class-level mock `ChunkAllocator` + `BlockWriter`)

`tests/large_object_writer.rs` (integration section):

- **Pipeline concurrency**: write 50 MB with a mock `BlockWriter`
  (10 ms per block write) + mock `ChunkAllocator`; instrument to verify
  the main write task
  writes block 1 of strip N+1 before strip N's parity task completes —
  three strips in flight simultaneously.
- **Fetch granularity**: stream returns 512 KB per `poll_read`; verify
  2 reads per block, 4 blocks per strip, first disk write starts after
  1 MB (not 4 MB).
- **Bounded preallocation**: write 50 MB (13 strips, `prealloc_depth =
  2`); verify `allocate_chunk` called with `strip_count = 1`,
  `append_chunk` called 12 times, never > 2 strips in the prealloc
  channel.
- **Chunk prefetch**: write 100 MB (`max_chunk_size = 8 MB`,
  `prealloc_depth = 2`); verify `allocate_chunk` for chunk N+1 before
  chunk N is sealed.
- **Backpressure**: mock `BlockWriter` 10 ms/block + instant stream; verify
  fetch blocks when `max_cached_buffer` (4 MB) is full; un-written data
  never exceeds 4 MB; no data loss.
- **Streaming (unknown size)**: `write_stream(Cursor(50 MB), None)` →
  correct strip count, chunk sealed, `Location` correct.
- **Partial strip**: write 2 MB (strip capacity 4 MB) → 1 partial strip,
  `sealed_length = 2 MB`, `Location.length = 2 MB`.
- **Whole-strip retry**: inject diskio write failure on the 3rd strip →
  writer retries the whole strip with a new allocation (up to 3
  retries); verify `append_chunk` for the replacement strip + the
  failed strip's segments freed.
- **EC encode failure abort**: inject EC encode failure (mock
  `encode_parity_from_shards` returns error) → pipeline aborts
  immediately, returns `IoError::EcEncodeFailed`, no retry (R94 ships
  abort; R110 refines).
- **Prefetch fell behind at rotation**: delay `allocate_chunk` for the
  next chunk so it's not ready when rotation triggers → main write task
  blocks at rotation until the oneshot delivers; no data lost, pipeline
  resumes once the chunk arrives.
- **`on_error` after 2 sealed chunks**: returns 2 `Location`s, frees
  the partial 3rd chunk (verify `delete_chunk` called on the partial).
- **Drop mid-write**: drop the writer after 1 strip → verify the
  partial chunk is deleted via `query_chunk` (mock returns NotFound).
- **`WriterPool` budget**: `memory_budget = 30 MB` (2 × 15 MB) → 2
  concurrent `try_acquire` succeed; 3rd returns
  `IoError::MemoryBudgetExhausted`; after one drops, the 3rd succeeds.
- **Per-writer memory bounded**: 50 MB and 500 MB objects use the same
  ~15 MB (verify via channel-depth + parity-semaphore assertions).

### End-to-end tests (real servers)

`tests/large_object_writer_e2e.rs` (real kv-server + diskdb + chunkdb,
no mocks; prefix with `pixi run clean-env &&`):

- **E2E Case 1 — single chunk (50 MB)**: start 1 kv-server (group 0),
  1 diskdb (5 disks), 1 chunkdb. `write_stream(Cursor(50 MB),
  Some(50 MB))`, `EcScheme::new(4,1)`, `max_chunk_size = 1 GB`,
  `prealloc_depth = 2` → 13 EC strips (12 full + 1 partial 2 MB),
  chunk sealed, returns 1 `Location { chunk_id, [0, 50MB), logical
  [0, 50MB) }`. `query_chunk` → Sealed, 13 strips, strip 12
  `sealed_length = 2 MB`. Read back 50 MB via a test reader using the
  Location → identical bytes.
- **E2E Case 2 — chunk rotation (100 MB)**: same topology.
  `write_stream(Cursor(100 MB), Some(100 MB))`, `max_chunk_size = 8 MB`
  (2 strips/chunk), `prealloc_depth = 2` → 13 chunks, 13 `Location`
  entries with `logical_offset` 0, 8, 16, ..., 96 MB; `length` = 8 MB
  for entries 0–11, 4 MB for entry 12. All 13 chunks Sealed via
  `query_chunk`; chunks 1–12 have 2 strips, chunk 13 has 1 strip. Read
  back 100 MB via the 13 Locations → identical bytes, contiguous.

## Module Structure

```
lib/crow-chunk-client/              # NEW workspace member
├── Cargo.toml
├── src/
│   ├── lib.rs                      # re-exports
│   ├── error.rs                    # IoError, Result
│   ├── location.rs                 # Location + proto/binary ser/de
│   ├── io.rs                       # ChunkIoWriter, FeedStatus, BackpressurePolicy
│   ├── traits.rs                   # ChunkAllocator, BlockWriter trait seams + impls
│   ├── prefetch.rs                 # strip prealloc task, ChunkPrefetch
│   └── writer/
│       ├── mod.rs
│       ├── large_object.rs         # LargeObjectWriter, WriterConfig, write_stream
│       ├── pipeline.rs             # fetch + main write + parity plumbing
│       └── pool.rs                 # WriterPool
└── tests/
    ├── large_object_writer.rs      # UT + integration (mock ChunkAllocator/BlockWriter)
    └── large_object_writer_e2e.rs  # E2E (real servers)

lib/crow-common/rust/src/ec.rs      # MODIFIED: + encode_parity_from_shards
lib/crow-protocol/src/proto/chunkdb_type.proto  # MODIFIED: + Location message
Cargo.toml                          # MODIFIED: + lib/crow-chunk-client member
```

## Config Extensions

`WriterConfig` (new, in `crow-chunk-client`):

- `max_chunk_size: u64` — default 1 GB. Rotation threshold (multiple of
  strip data capacity; rotation at strip boundaries).
- `prealloc_depth: usize` — default 2. Strips allocated ahead of the
  write cursor.
- `parity_depth: usize` — default 2. Parity tasks in flight.
- `chunk_prefetch_depth: usize` — default 1. Chunks allocated ahead.
- `read_buffer_size: usize` — default 1 MB. Fetch read granularity
  (one data block).
- `max_cached_buffer: usize` — default 4 MB. Un-written data budget
  (fetch backpressure).

`WriterPool`:

- `memory_budget: usize` — required arg. Total memory for concurrent
  writers; `max_concurrent = memory_budget / per_writer_footprint`.

No server-side config changes (the writer is a client library; the
chunkdb/diskdb servers are unchanged).

## Server Wiring

None. `crow-chunk-client` is a client library — it is linked by callers
(future object store, R106's small-object writer, tests), not by the
chunkdb/diskdb/kv-server binaries. The E2E tests start the existing
kv-server + diskdb + chunkdb binaries via `crow-test-harness` and
construct a `LargeObjectWriter` in-process.

## Open Questions

None remaining for the design. The one implementation-time gate is the
partial-EC UT (§7.2 / Test Design) — if isa-l does not support partial
encode as expected, stop and involve the user per the R94 backlog doc's
Open Question. All other R94 Open Questions (max chunk size vs KV value
size, unaligned `max_chunk_size`, `on_data` backpressure semantics,
memory peak accounting) are resolved in the backlog doc and reflected
above.
