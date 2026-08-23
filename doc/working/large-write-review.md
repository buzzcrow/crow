<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Large-Object Writer — OO Refactor Review

A design draft for restructuring `crow-chunk-client`'s large-object write
path around object-oriented stage classes. The current code (R94, landed)
is functionally correct but organized as free functions operating on
loose structs; this draft defines the target class shape, their
responsibilities and dependencies, and the write flow that wires them.

- Root design: `doc/design/chunkio/design-crow-chunkio.md` §3 (Write
  Flow), §5 (EC Integration), §6 (Chunk Rotation and Location).
- Architecture decisions and rationale (two trait seams, push vs stream
  driving modes, parity hand-off decoupling) are in the root design;
  this doc does not repeat them.
- Already landed: `LargeObjectWriter`, `WriterConfig`, `WriterPool`,
  `ChunkIoWriter` trait, `Location`, `ChunkAllocator`/`BlockWriter`
  trait seams, `DiskioBlockWriter`, prealloc task, parity tasks
  (`lib/crow-chunk-client/src/`).

## 1. Design Goals

- **One config, shared by all writers.** A single `ChunkClientConfig`
  holds every tunable (write-path, large-write, prefetch, parity,
  memory budget). `WriterConfig` is removed; its fields fold into the
  shared config. Writers read the fields they need and ignore the rest.
- **No template/generic writer.** The current `LargeObjectWriter<A, W>`
  generic-over-trait-seams is dropped. Concrete writers hold concrete
  collaborators (`ChunkPrefetch`, `DiskWriter`, `ChunkWriter`).
  Testability moves to the `DiskWriter` seam (§6) — one injection
  point, not two.
- **Two large-object writer variants.** `LargeObjectWriter` accepts a
  non-blocking stream — data already in buffer, the stream can
  continue providing data without any stop or wait. Efficient for the
  common case. `LargeAsyncObjectWriter` accepts an async stream
  (`AsyncRead`) — a more complex flow with fetch stage + backpressure.
  Both own the drive loop (see next bullet); both share the same
  `ChunkWriter`/`ChunkPrefetch`/`DiskWriter` collaborators.
- **Drive loop lives only in `LargeObjectWriter`.** `ChunkWriter` is a
  wrapper of a `ChunkInfo` object that provides write ability — it
  does **not** participate in the drive loop. `LargeObjectWriter`
  rotates chunks, fetches chunks. Inside the `ChunkWriter` layer, it
  iterates strips and triggers append-strip. Inside a strip, it can
  replace a block in a strip. All references to the chunkdb client for
  chunk operations go through `ChunkWriter` (seal/delete) and
  `ChunkPrefetch` (allocate/append).
- **One chunk type: strip chunk.** Only `ChunkWriter` exists; no
  chunk-type enum at the writer level. `ChunkWriter` owns a reference
  to the current `ChunkInfo` (the protobuf `Chunk` response object —
  node, disk, strip layout) and drives strip writers inside it.
- **Strip writer is an enum, not a trait object.** `StripWriter` is a
  Rust enum: `Ec(EcStripWriter)` | `Mirror(MirrorStripWriter)`.
  Follows Rust conventions — monomorphic, heapless dispatch, easy to
  add new variants. `ChunkWriter` matches on the enum.
- **Strip writer is the disk-block boundary owner.** `StripWriter`
  accepts a buffer, writes data blocks to disk, and (for EC) computes
  parity and writes parity blocks. The large-write path feeds
  disk-block-aligned buffers, so the cross-block flow-control branch is
  not exercised — but the `StripWriter` API supports unaligned buffers
  for future callers (R106 small-object writer).
- **Prefetch is a class, not a free task.** `ChunkPrefetch` pre-creates
  chunks at start (default 1; configurable for testing) and continues
  appending strips to the current chunk during the write, returning the
  latest `ChunkInfo` to `ChunkWriter` on each append.
- **Placeholder for mirror strips.** `MirrorStripWriter` is declared as
  a stub so the `StripWriter` enum shape is fixed; the large-write
  flow only constructs `EcStripWriter`. Filled in by R93
  (mirror-to-EC conversion) and R106.
- **Placeholder for small-object writer.** `SmallObjectWriter` is
  declared as a stub sharing the `ChunkIoWriter` interface and the
  `ChunkWriter`/`StripWriter`/`DiskWriter` collaborators. Filled in by
  R106 (small-object writer — multiple objects per chunk, dynamic strip
  pool).
- **Separate worker classes, no shared trait.** `EcWorker` (owned by
  `EcStripWriter`) and `HashWorker` (owned by the object-level writer,
  future) are separate structs with separate APIs. They live at
  different layers (strip vs object), have different queue lengths and
  capabilities — a shared `Worker` trait would force a false
  uniformity. `EcWorker` does **streaming compute**: push data buffers
  to the worker, the worker computes incrementally; after the last
  push (signaled by a finish/last-task marker), take the computed
  result. EC compute overlaps with data-block writes.
- **Module hierarchy by concept, not by file.** Modules group by
  abstraction level: `writer/` (object-level: large/small),
  `chunk/` (chunk + strip writers, later chunk/strip readers),
  `worker/` (EC, later hash), `io/` (disk IO). Coding against
  high-level concepts is cleaner than coding against flat files.

## 2. Class Catalog

Responsibilities, public API, and dependencies. Field types are
indicative; exact ownership (owned vs `Arc`) is decided in §7.

### 2.1 `ChunkClientConfig` (value object)

- **Responsibility:** hold every tunable for the chunk data path.
  Shared by `LargeObjectWriter`, `WriterPool`, `ChunkPrefetch`,
  `ChunkWriter`, `EcStripWriter`. Replaces `WriterConfig`.
- **Fields (subset, grouped by owner):**
  - *Write path:* `read_buffer_size` (default 1 MB), `max_cached_buffer`
    (default 4 MB).
  - *Large write:* `max_chunk_size` (default 1 GB), `prealloc_depth`
    (default 2), `parity_depth` (default 2), `chunk_prefetch_depth`
    (default 1).
  - *Prefetch:* `prefetch_chunk_count` (default 1; raise to 10 for
    throughput-heavy workloads).
  - *Memory:* `memory_budget` (used by `WriterPool`; default per-pool).
- **Methods:** `Default::default()`, `validate() -> Result<()>`
  (rejects `read_buffer_size == 0`, `max_chunk_size < strip_data_capacity`,
  etc.), `per_writer_memory(ec_scheme) -> usize` (the formula currently
  duplicated at `large_object.rs:108` and `pool.rs:49` lives here once).
- **Depends on:** `EcScheme` (for `per_writer_memory`).

### 2.2 `DiskWriter` (trait + concrete impls)

- **Responsibility:** the block-IO seam. Write a data block to a disk
  at a (zone, offset); fsync a disk. Wraps `DiskioClient` internally in
  the production impl. **This is the only test-injection point** —
  replacing the prior two-trait-seam design with one.
- **Trait:**
  ```rust
  #[async_trait]
  pub trait DiskWriter: Send + Sync {
      async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()>;
      async fn fsync(&self, disk_id: DiskId) -> Result<()>;
  }
  ```
  The `Segment` parameter carries `disk_id`, `zone_index`,
  `unit_offset` — `DiskWriter` resolves them to the diskio RPC. This
  removes the 4 repeated `seg.disk_id.as_ref().ok_or_else(...)` +
  `seg.unit_offset * unit_bytes` patterns from the current code.
- **Concrete impls:**
  - `DiskioBlockWriter` (renamed from current `DiskioBlockWriter`, now
    implementing `DiskWriter` not `BlockWriter`): wraps
    `DiskioClient` + `RpcServer` + `Connection`. Production path.
  - `LocalFileDiskWriter` (new, test-only, behind `test-util`):
    writes blocks to per-disk files under a temp dir. Enables UT-level
    write-flow tests without `crow-test-harness`'s diskio/diskdb
    harness — fast, hermetic, no FFI.
- **Depends on:** `crow-diskio-client`, `crow-rpc-ffi` (production impl
  only); `Segment`, `DiskId`, `Bytes`.

### 2.3 `ChunkAllocator` (trait — retained)

- **Responsibility:** chunk lifecycle RPCs (`allocate_chunk`,
  `append_chunk`, `seal_chunk`, `delete_chunk`, `query_chunk`,
  `update_chunk_strip`). Mirrors the subset of `ChunkdbClient` methods
  the data path uses. Kept as a trait so `ChunkPrefetch` and
  `ChunkWriter` can be tested with a mock allocator without spinning a
  real chunkdb server — this seam is about *chunk metadata RPCs*, not
  block IO, so it stays distinct from `DiskWriter`.
- **Depends on:** `crow-protocol::chunkdb::rpc` request/response types.
- **Note:** the `Arc<T>` blanket impl at `traits.rs:48-78` is retained.

### 2.4 `ChunkPrefetch` (class)

- **Responsibility:** pre-create chunks and pre-append strips ahead of
  the write cursor. Replaces `spawn_prealloc_task` + `run_prealloc`
  (`prefetch.rs:53-85`, `prefetch.rs:175-241`) with a class whose
  `run()` method is the loop and whose fields replace the 7 loop
  parameters.
- **Public API:**
  ```rust
  pub struct ChunkPrefetch {
      allocator: Arc<dyn ChunkAllocator>,
      ec_scheme: EcScheme,
      config: Arc<ChunkClientConfig>,
      chunk_type_byte: u8,
      // internal: placement channel sender, planned strip count,
      // current chunk id, strips-in-current-chunk counter.
  }

  impl ChunkPrefetch {
      pub fn new(allocator, ec_scheme, config, chunk_type_byte) -> Self;
      /// Spawn the prefetch task. Returns the placement receiver +
      /// join handle. Caller drives the receiver from `ChunkWriter`.
      pub fn spawn(self, object_size: Option<u64>)
          -> (mpsc::Receiver<Result<StripPlacement>>, JoinHandle<()>);
      /// On-demand strip allocation when the prefetch task has
      /// finished but more data remains. Appends to the current
      /// chunk if it has room, else allocates a new chunk. Called
      /// by `ChunkWriter` (not the prefetch task itself).
      pub async fn on_demand(
          &self,
          current_chunk_id: Option<ChunkId>,
          strips_in_current_chunk: u32,
      ) -> Result<StripPlacement>;
  }
  ```
- **Behavior:**
  - On `spawn`: pre-creates `prefetch_chunk_count` chunks (default 1)
    upfront, each with 1 strip, before sending the first placement —
    gives `ChunkWriter` a warm start.
  - Then appends strips up to `prealloc_depth` ahead of the cursor,
    bounded by the placement channel capacity (backpressure).
  - When the current chunk reaches `max_chunk_size`, allocates a new
    chunk with 1 strip and continues.
  - If `object_size` is known, plans total strip count and stops after
    allocating that many; the main flow calls `on_demand` for overflow.
- **Depends on:** `ChunkAllocator`, `EcScheme`, `ChunkClientConfig`,
  `StripPlacement`, `crow-protocol`.

### 2.5 `StripPlacement` (value object — gains methods)

- **Responsibility:** a strip's placement — which chunk, index within
  chunk, EC segments, unit size. Stays a plain struct but gains
  behavior to kill the repeated extraction logic.
- **New methods:**
  ```rust
  impl StripPlacement {
      pub fn unit_bytes(&self) -> u64 { u64::from(self.unit_kb) * 1024 }
      pub fn segment(&self, i: usize) -> Result<&Segment>;
      pub fn disk_id(&self, i: usize) -> Result<DiskId>;
      pub fn zone_offset(&self, i: usize) -> Result<u64>; // seg.unit_offset * unit_bytes
  }
  ```
- **Depends on:** `crow-protocol::diskdb::rpc::Segment`,
  `crow-diskio-client::DiskId`.

### 2.6 `StripWriter` (enum) + `StripResult`

- **Responsibility:** own one strip. Accept data buffers, write data
  blocks to disk via `DiskWriter`, and on strip completion compute +
  write parity (EC variant). Return a `StripResult` to the parent
  `ChunkWriter`. Implemented as a Rust enum (not a trait object) —
  monomorphic, heapless dispatch, easy to add new variants.
- **Enum:**
  ```rust
  pub enum StripWriter {
      Ec(EcStripWriter),
      Mirror(MirrorStripWriter),  // placeholder stub
  }

  impl StripWriter {
      pub async fn push(&mut self, buffer: Bytes) -> Result<FeedStatus>;
      pub async fn finish(&mut self) -> Result<StripResult>;
      pub async fn abort(&mut self) -> Result<StripResult>;
      pub fn ready(&self) -> bool;
  }
  ```
  Methods match-dispatch to the active variant.
- **`StripResult` (response):**
  ```rust
  pub struct StripResult {
      pub chunk_id: ChunkId,
      pub strip_index_in_chunk: u32,
      pub data_blocks_written: u32,
      pub bytes_written: u64,
      pub partial: bool,            // true if last block was < unit_bytes
      pub parity_handles: Vec<JoinHandle<Result<()>>>,
  }
  ```
  The `parity_handles` field is the key hand-off: the strip writer
  spawns parity tasks but does **not** join them — it returns the
  handles upward so `ChunkWriter` can join them at chunk rotation or
  completion (matches the current decoupling in
  `design-crow-chunkio.md` §3: "advance to strip N+1 without waiting
  for parity").
- **`EcStripWriter` (enum variant, used by large-write flow):**
  - Fields: `placement: StripPlacement`, `disk_writer: Arc<dyn DiskWriter>`,
    `ec_worker: EcWorker` (owns the EC computation state — see §2.6a),
    `next_block: usize`, `parity_batch: ParityBatch` (per-strip — see
    §2.8), config.
  - `push(buffer)`: writes `buffer` to `placement.segment(next_block)`
    via `DiskWriter::write` **and** feeds `buffer` to `ec_worker.push()`
    so the worker computes the code part incrementally as data arrives.
    On write failure, calls back to `ChunkWriter` for a fresh
    placement (see §3 flow). Increments `next_block`; when
    `next_block == data_num`, the strip is full → `finish` is invoked
    by `ChunkWriter` (not by `push`).
  - `finish()`: the `EcWorker` has already computed the parity shards
    incrementally during `push` calls — `finish` only *writes* the code
    part to disk. Each parity disk block can be written in parallel
    (the `ParityBatch` owned by this strip writer manages the parallel
    writes + joins them). Then returns `StripResult` with the parity
    handles.
  - **Aligned-buffer fast path:** the large-write flow feeds
    `unit_bytes`-aligned buffers, so each `push` is exactly one data
    block — no accumulation, no cross-block split. The unaligned
    branch (accumulate partial buffers until `unit_bytes`, then write)
    is implemented but only exercised by R106.
- **`MirrorStripWriter` (placeholder, stub):**
  ```rust
  pub struct MirrorStripWriter { /* fields TBD */ }
  // Methods on StripWriter::Mirror todo!()
  ```
  Declared so the `StripWriter` enum shape is fixed and R93/R106 can
  fill it in. The large-write flow never constructs it. Mirror strips
  have no parity, so a `MirrorStripWriter` owns no `EcWorker` — EC is
  an `EcStripWriter`-specific concern.
- **Depends on:** `DiskWriter`, `EcWorker`, `StripPlacement`, `EcScheme`,
  `ChunkClientConfig`, `crow-common::ec`.

### 2.6a `EcWorker` (class — streaming compute, no shared trait)

- **Responsibility:** the EC computation layer, separate from IO.
  Accepts data buffers incrementally and computes parity shards as
  data arrives (streaming compute). Pure compute — does **not** touch
  disk, which makes it trivially unit-testable. Owned by each
  `EcStripWriter`.
- **No shared `Worker` trait.** `EcWorker` and the future `HashWorker`
  are separate structs with separate APIs. They live at different
  layers (strip vs object), have different queue lengths and
  capabilities — a shared trait would force a false uniformity.
- **Public API:**
  ```rust
  pub struct EcWorker {
      ec_scheme: EcScheme,
      data_shards: Vec<Bytes>,
      shards_received: usize,
      // streaming compute state (per-shard incremental EC state)
  }

  impl EcWorker {
      pub fn new(ec_scheme: EcScheme) -> Self;
      /// Feed one data shard. Worker computes incrementally —
      /// parity shards are built up as each data shard arrives,
      /// overlapping EC compute with the next data block's disk write.
      pub fn push(&mut self, buffer: &Bytes) -> Result<()>;
      /// Signal: all data shards pushed. Finalize the computation
      /// and return the code_num parity shards.
      pub fn finish(&mut self) -> Result<Vec<Vec<u8>>>;
      /// Reset to accept a new strip (reuse the worker across strips
      /// in the same EcStripWriter to avoid re-allocating state).
      pub fn reset(&mut self);
  }
  ```
- **Streaming compute design:** `push` feeds a data shard to the
  worker, which computes incrementally. After the last `push`
  (signaled by `finish`), the worker finalizes and the caller takes
  the computed parity shards. This overlaps EC compute with
  data-block writes — by the time the strip is finish-ready, the
  parity shards are already (mostly) computed and `EcStripWriter` only
  has to write them to disk.
- **Depends on:** `crow-common::ec`, `Bytes`, `IoError`.

### 2.6b `HashWorker` (placeholder, stub — separate from EcWorker)

- **Responsibility:** compute MD5 or SHA-256 over the whole object.
  Owned by the *object-level* writer (`LargeObjectWriter` /
  `SmallObjectWriter`), not the strip writer — the digest spans all
  chunks. Separate struct, separate API from `EcWorker` (no shared
  trait). Different queue length and capability than `EcWorker`.
- **Stub:**
  ```rust
  pub struct HashWorker { /* fields TBD — MD5/SHA-256 state */ }
  impl HashWorker {
      // pub fn new(algorithm: HashAlgorithm) -> Self;
      // pub fn push(&mut self, buffer: &Bytes) -> Result<()>;
      // pub fn finish(&mut self) -> Result<Vec<u8>>;
      // pub fn reset(&mut self);
  }
  ```
- **Depends on:** `Bytes`, `IoError`. Declared to fix the module
  shape; not implemented in this refactor.

### 2.7 `ChunkWriter` (class — chunk-info wrapper + write ability)

- **Responsibility:** wrap the current `ChunkInfo` (the protobuf `Chunk`
  from the latest `allocate_chunk`/`append_chunk` response — node ids,
  disk ids, strip layout) and provide write ability for one chunk. It
  is **not** involved in the drive loop — `LargeObjectWriter` owns the
  drive loop (rotates chunks, fetches chunks). `ChunkWriter` iterates
  strips within the current chunk, triggers append-strip, and can
  replace a block in a strip. All references to the chunkdb client for
  chunk operations (seal, delete, append-strip) go through
  `ChunkWriter`.
- **Does NOT own:** the drive loop, chunk rotation, prefetch
  coordination, EOF handling, or `Location` accumulation across chunks.
  Those are `LargeObjectWriter`'s job.
- **Public API:**
  ```rust
  pub struct ChunkWriter {
      allocator: Arc<dyn ChunkAllocator>,
      disk_writer: Arc<dyn DiskWriter>,
      ec_scheme: EcScheme,
      config: Arc<ChunkClientConfig>,
      // current chunk state:
      chunk_info: Option<Chunk>,        // latest Chunk protobuf
      current_chunk_id: Option<ChunkId>,
      bytes_in_chunk: u64,
      strips_in_chunk: u32,
      // current strip:
      current_strip: Option<StripWriter>,  // enum, not Box<dyn>
  }

  impl ChunkWriter {
      pub fn new(allocator, disk_writer, ec_scheme, config) -> Self;
      /// Open a chunk from a pre-allocated placement (from
      /// ChunkPrefetch). Stores the ChunkInfo.
      pub async fn open(&mut self, placement: StripPlacement) -> Result<()>;
      /// Push a data block to the current strip. If the strip is
      /// full, finishes it (returns StripResult with parity handles),
      /// then appends a new strip to the current chunk via
      /// ChunkAllocator::append_chunk and opens the next strip.
      /// Does NOT rotate chunks — that's LargeObjectWriter's job.
      pub async fn push(&mut self, buffer: Bytes) -> Result<FeedStatus>;
      /// Append a new strip to the current chunk. Returns the new
      /// strip's placement. Called internally by push when the
      /// current strip is full.
      async fn append_strip(&mut self) -> Result<StripPlacement>;
      /// Replace a block in the current strip (retry on write
      /// failure — allocates a fresh strip placement on the same
      /// chunk and re-writes the buffered blocks).
      async fn replace_block(&mut self, block_index: usize) -> Result<()>;
      /// Finish the current strip, seal the chunk, return the
      /// StripResult + chunk's Location. Called by LargeObjectWriter
      /// at chunk rotation or completion.
      pub async fn seal(&mut self) -> Result<(StripResult, Location)>;
      /// Abort: cancel in-flight strip writes, delete the partial
      /// (unsealed) chunk. Returns already-finished StripResults.
      pub async fn abort(&mut self) -> Result<Vec<StripResult>>;
      /// Non-async capacity hint.
      pub fn ready(&self) -> bool;
      /// Accessor for the current chunk info (LargeObjectWriter
      /// reads it to decide rotation).
      pub fn chunk_info(&self) -> Option<&Chunk>;
      pub fn bytes_in_chunk(&self) -> u64;
  }
  ```
- **`ChunkInfo` field:** `chunk_info: Option<Chunk>` is the protobuf
  `Chunk` object from the most recent allocation/append response. It
  carries the node/disk/strip metadata the strip writer needs to issue
  writes. `ChunkWriter` updates it on each `append_chunk` (the response
  returns the full updated chunk). The strip writer reads segments
  from the current `StripPlacement`, which is derived from
  `chunk_info`'s last strip.
- **Depends on:** `ChunkAllocator`, `DiskWriter`, `StripWriter` (enum,
  constructs `EcStripWriter` variant), `StripPlacement`, `StripResult`,
  `Location`, `ChunkClientConfig`, `crow-protocol`.

### 2.8 `ParityBatch` (class — owned by EcStripWriter)

- **Responsibility:** track in-flight parity task handles for one EC
  strip. When writing an EC strip, each disk block (data + parity) can
  be written in parallel — `ParityBatch` manages the parallel writes
  and joins them. Owned by each `EcStripWriter` (per-strip, not
  per-chunk). Replaces the duplicated join/abort logic at
  (`pipeline.rs:417`, `pipeline.rs:449`, `pipeline.rs:487-489`).
- **Public API:**
  ```rust
  pub struct ParityBatch {
      handles: Vec<JoinHandle<Result<()>>>,
  }
  impl ParityBatch {
      pub fn new() -> Self;
      /// Spawn a parallel write task (one per disk block — data or
      /// parity) and track its handle. Each disk block in the strip
      /// can be written in parallel.
      pub fn spawn(&mut self, handle: JoinHandle<Result<()>>);
      /// Join all in-flight writes, return first error.
      pub async fn join_all(&mut self) -> Result<()>;
      /// Abort all in-flight writes.
      pub fn abort_all(&mut self);
  }
  ```
  Note: no semaphore here — parallelism is bounded by the number of
  blocks in the strip (data_num + code_num), which is fixed by the EC
  scheme. The old `parity_depth` semaphore (which bounded *cross-strip*
  parity task spawn rate) is no longer needed since parity is now
  per-strip and joined at strip finish.
- **Depends on:** `JoinHandle`, `IoError`.

### 2.9 `LargeObjectWriter` (non-blocking stream — owns drive loop)

- **Responsibility:** the user-facing entry point for the efficient
  common case. Accepts a **non-blocking stream** — data already in
  buffer, the stream can continue providing data without any stop or
  wait. **Owns the drive loop**: rotates chunks, fetches chunks from
  `ChunkPrefetch`, coordinates EOF, and accumulates `Location`s across
  chunks. Implements `ChunkIoWriter` for push mode. No generics —
  concrete types throughout.
- **Drive loop (lives here, not in `ChunkWriter`):**
  - Await next `StripPlacement` from `ChunkPrefetch`.
  - If new chunk id ≠ current → rotate: seal current `ChunkWriter`,
    record `Location`, create new `ChunkWriter` for the new chunk.
  - Open the placement in `ChunkWriter`, push data blocks.
  - On EOF → finish current `ChunkWriter`, seal, collect final
    `Location`, return all `Location`s.
- **Public API:**
  ```rust
  pub struct LargeObjectWriter {
      allocator: Arc<dyn ChunkAllocator>,
      disk_writer: Arc<dyn DiskWriter>,
      ec_scheme: EcScheme,
      config: Arc<ChunkClientConfig>,
      chunk_writer: Option<ChunkWriter>,
      prefetch: Option<ChunkPrefetch>,
      // drive-loop state:
      locations: Vec<Location>,       // accumulated across chunks
      logical_offset: u64,
      finished: bool,
      // future: hash_worker: Option<HashWorker>,  // R-later
  }

  impl LargeObjectWriter {
      pub fn new(allocator, disk_writer, ec_scheme, config) -> Self;
      pub fn per_writer_memory(&self) -> usize;
      /// Non-blocking stream write. Data is already in buffer —
      /// no fetch stage, no backpressure waits.
      pub async fn write(&mut self, buffers: impl Iterator<Item = Bytes>,
                         object_size: Option<u64>) -> Result<Vec<Location>>;
  }
  // impl ChunkIoWriter for LargeObjectWriter { on_data / on_finish / on_error / require_data }
  ```
- **Future: `HashWorker` field.** When R-later adds whole-object
  digests, `LargeObjectWriter` gains `hash_worker: Option<HashWorker>`.
  Every data buffer pushed to `ChunkWriter` is also fed to
  `hash_worker.push()`; `on_finish`/`write` finalize the digest and
  store it in object metadata. `HashWorker` is a separate struct (no
  shared `Worker` trait — see §2.6a/§2.6b).
- **Depends on:** `ChunkWriter`, `ChunkPrefetch`, `DiskWriter`,
  `ChunkAllocator`, `ChunkClientConfig`, `ChunkIoWriter`, `Location`.

### 2.9a `LargeAsyncObjectWriter` (async stream — owns drive loop)

- **Responsibility:** the async-stream variant. Accepts an `AsyncRead`
  stream — a more complex flow with a fetch stage + backpressure
  (the stream may stall, yield partial reads, etc.). Owns the same
  drive loop as `LargeObjectWriter` (rotate, fetch, EOF, accumulate
  `Location`s) but adds a fetch stage that pulls from `AsyncRead` and
  feeds `ChunkWriter`. Implements `ChunkIoWriter` for push mode.
- **Public API:**
  ```rust
  pub struct LargeAsyncObjectWriter {
      allocator: Arc<dyn ChunkAllocator>,
      disk_writer: Arc<dyn DiskWriter>,
      ec_scheme: EcScheme,
      config: Arc<ChunkClientConfig>,
      chunk_writer: Option<ChunkWriter>,
      prefetch: Option<ChunkPrefetch>,
      locations: Vec<Location>,
      logical_offset: u64,
      finished: bool,
      // future: hash_worker: Option<HashWorker>,
  }

  impl LargeAsyncObjectWriter {
      pub fn new(allocator, disk_writer, ec_scheme, config) -> Self;
      pub fn per_writer_memory(&self) -> usize;
      /// Async stream write. Fetch stage pulls from AsyncRead,
      /// accumulates to block granularity, feeds ChunkWriter.
      /// Backpressure via max_cached_buffer.
      pub async fn write_stream(&mut self, reader: impl AsyncRead + Unpin + Send,
                                object_size: Option<u64>) -> Result<Vec<Location>>;
  }
  // impl ChunkIoWriter for LargeAsyncObjectWriter { on_data / on_finish / on_error / require_data }
  ```
- **Difference from `LargeObjectWriter`:** the async variant runs a
  fetch stage (`run_fetch_stage` — free function, pure IO glue) that
  pulls from `AsyncRead` in ≤ `read_buffer_size` chunks, accumulates
  to full blocks, and sends `Bytes` to the drive loop. The non-blocking
  variant skips the fetch stage — buffers are already in memory.
- **Depends on:** `ChunkWriter`, `ChunkPrefetch`, `DiskWriter`,
  `ChunkAllocator`, `ChunkClientConfig`, `ChunkIoWriter`, `Location`,
  `run_fetch_stage` (free function).

### 2.9b `SmallObjectWriter` (placeholder, stub — R106)

- **Responsibility:** the small-object counterpart to
  `LargeObjectWriter`. Multiple small objects share one active chunk
  (dynamic strip pool, strips appended on demand per object). Implements
  the same `ChunkIoWriter` interface so callers are agnostic to object
  size. Declared as a stub so the `writer/` module shape is fixed; R106
  fills it in.
- **Stub:**
  ```rust
  pub struct SmallObjectWriter {
      allocator: Arc<dyn ChunkAllocator>,
      disk_writer: Arc<dyn DiskWriter>,
      ec_scheme: EcScheme,
      config: Arc<ChunkClientConfig>,
      // R106: shared active chunk, dynamic strip pool, per-object
      // offset tracking within the chunk. Reuses ChunkWriter +
      // EcStripWriter + EcWorker; the difference is chunk *sharing*
      // across objects and strip *leasing* rather than dedicated
      // strips.
      // hash_worker: Option<HashWorker>,  // R-later
  }
  // impl ChunkIoWriter for SmallObjectWriter { todo!() }
  ```
- **Reuse:** `SmallObjectWriter` reuses `ChunkWriter`, `EcStripWriter`,
  `EcWorker`, `DiskWriter`, `ChunkAllocator` unchanged. The R106 work
  is the *sharing policy* (which active chunk to append to, when to
  seal and rotate an active chunk) — the strip/chunk/worker primitives
  are the same as the large-object path.
- **Depends on:** `ChunkWriter`, `DiskWriter`, `ChunkAllocator`,
  `ChunkClientConfig`, `ChunkIoWriter`, `Location`. (Not
  `ChunkPrefetch` — small objects do not pre-allocate; they append to
  an already-active chunk.)

### 2.10 `WriterPool` (class — minor change)

- **Responsibility:** bound concurrent writers by memory budget.
  Unchanged except: drops its own `per_writer_memory` formula
  (`pool.rs:49-54`), delegates to `config.per_writer_memory(ec_scheme)`.
  Drops the `<A, W>` generics; holds concrete `Arc<dyn ChunkAllocator>`
  + `Arc<dyn DiskWriter>` + `Arc<ChunkClientConfig>` and clones them
  into each writer's `new`.
- **Depends on:** `LargeObjectWriter`, `LargeAsyncObjectWriter`,
  `SmallObjectWriter` (future), `ChunkClientConfig`.

### 2.11 `ChunkIoWriter`, `Location`, `IoError` (unchanged)

- `ChunkIoWriter` trait (`io.rs`), `Location` (`location.rs`),
  `IoError`/`Result` (`error.rs`) stay as-is. They are the stable
  public surface; the refactor is purely about the implementation
  behind `ChunkIoWriter`.

## 3. Write Flow

The end-to-end large-write flow, restated in terms of the new classes.
The **drive loop lives in `LargeObjectWriter`** (or
`LargeAsyncObjectWriter`) — it rotates chunks, fetches chunks from
`ChunkPrefetch`, coordinates EOF, and accumulates `Location`s.
`ChunkWriter` is a chunk-info wrapper with write ability — it iterates
strips, triggers append-strip, and can replace blocks, but does not
participate in the drive loop.

Two writer variants share the same `ChunkWriter` + `ChunkPrefetch`
collaborators:
- `LargeObjectWriter` — non-blocking stream (data already in buffer).
- `LargeAsyncObjectWriter` — async stream (`AsyncRead` + fetch stage).

### 3.1 Non-blocking stream mode (`LargeObjectWriter::write`)

```
LargeObjectWriter::write(buffers, object_size)
  │
  a. If object_size == Some(0) → return Ok(vec![]).
  │
  b. Construct ChunkPrefetch, spawn it with object_size.
     ChunkPrefetch pre-creates prefetch_chunk_count chunks (default 1),
     each with 1 strip, before sending the first StripPlacement.
  │
  c. Drive loop (owned by LargeObjectWriter):
  │     loop {
  │       1. Await next StripPlacement from ChunkPrefetch.
  │          On None (prefetch done) + data remains:
  │            placement = ChunkPrefetch::on_demand(...).
  │          On None + no data: break.
  │       2. If new chunk id ≠ current → rotate:
  │            (strip_result, location) = current_chunk.seal().await?
  │            locations.push(location).
  │            Create new ChunkWriter, open(placement).
  │       3. Else: ChunkWriter::open(placement) (first strip of chunk)
  │          or ChunkWriter continues with the next strip.
  │       4. For each data block from buffers:
  │            ChunkWriter::push(buffer)
  │              → EcStripWriter::push(buffer):
  │                  • DiskWriter::write(data block) — parallel with
  │                    other blocks in the strip
  │                  • EcWorker::push(&buffer) — streaming EC compute
  │                  • on write failure: ChunkWriter::replace_block()
  │                    allocates a fresh strip placement on the same
  │                    chunk, re-writes buffered blocks (up to 3 retries)
  │       5. When strip is full (data_num blocks):
  │            strip_result = EcStripWriter::finish()
  │              → EcWorker::finish() returns computed parity shards
  │              → ParityBatch::spawn() writes each parity disk block
  │                in parallel + fsyncs
  │              → ParityBatch::join_all() (per-strip, joined at finish)
  │            ChunkWriter appends next strip via append_chunk if more
  │              data remains.
  │          If EOF: break.
  │     }
  │
  d. Seal the final chunk:
  │     (strip_result, location) = current_chunk.seal().await?
  │     locations.push(location).
  │     (delete_chunk if empty — the partial-empty case at
  │      pipeline.rs:468-476)
  │
  e. Abort prefetch (no longer needed).
  │
  f. Return locations.
```

### 3.2 Async stream mode (`LargeAsyncObjectWriter::write_stream`)

Same drive loop as §3.1, but with a fetch stage that pulls from
`AsyncRead`:

```
LargeAsyncObjectWriter::write_stream(reader, object_size)
  │
  a-b. Same as §3.1 (empty check, construct + spawn ChunkPrefetch).
  │
  c. Run fetch stage + drive loop concurrently:
  │
  │   run_fetch_stage(reader, block_tx, read_buffer_size)
  │     • reads ≤ read_buffer_size per call from reader
  │     • accumulates to full blocks, sends Bytes on block_tx
  │     • on EOF sends partial last block, drops sender
  │     (free function — pure IO, no state)
  │
  │   Drive loop (owned by LargeAsyncObjectWriter):
  │     Same as §3.1 steps c.1-c.5, but receives blocks from block_rx
  │     instead of from an iterator. Backpressure: if block_rx is full
  │     (max_cached_buffer), the fetch stage blocks — throttling the
  │     AsyncRead to disk-write speed.
  │
  d-f. Same as §3.1.
```

### 3.3 Push mode (`ChunkIoWriter::on_data` / `on_finish` / `on_error`)

Both `LargeObjectWriter` and `LargeAsyncObjectWriter` implement
`ChunkIoWriter`. The drive loop is lazy-started on the first `on_data`:

- `on_data(buffer)`: if `chunk_writer.is_none()`, run steps (b)-(c)
  above (spawn prefetch, start drive loop). Feed `buffer` into the
  drive loop. Returns `FeedStatus::Continue` if capacity remains,
  else `Pause`.
- `on_finish()`: signal EOF to the drive loop, run step (d), return
  `Location`s.
- `on_error()`: signal cancel, `ChunkWriter::abort()` (ParityBatch
  abort_all, delete partial chunk), return already-sealed `Location`s.
- `require_data()`: non-async capacity probe on the current
  `ChunkWriter`.

### 3.4 Chunk rotation (inside `LargeObjectWriter`)

When `ChunkPrefetch` delivers a placement with a new `chunk_id`, or
when `bytes_in_chunk + incoming_strip_bytes > max_chunk_size`:

a. `(strip_result, location) = current_chunk.seal().await?` —
   `ChunkWriter` seals the chunk: finishes the current strip,
   `ParityBatch::join_all()` for the strip, `seal_chunk` RPC via
   `ChunkAllocator`, returns the chunk's `Location`.
b. `locations.push(location)`.
c. `logical_offset += bytes_in_chunk`.
d. Create a new `ChunkWriter`, `open(new_placement)` — stores the new
   `ChunkInfo`, constructs the first `EcStripWriter`.

### 3.5 Strip-write retry (inside `ChunkWriter` + `EcStripWriter`)

The whole-strip retry at `pipeline.rs:145-229` becomes a collaboration:

- `EcStripWriter::push` returns `Err(IoError::WriteFailed(...))` on
  disk write failure.
- `ChunkWriter::replace_block()` catches it, calls
  `ChunkAllocator::append_chunk` for a fresh `StripPlacement` on the
  same chunk, resets the strip writer with the new placement, and
  re-feeds the buffered data shards. Up to `MAX_RETRIES = 3` attempts;
  on exhaustion returns `IoError::WriteFailed`.
- The failed strip's old segments are leaked (R94 coarse behavior;
  R110 refines to single-block replacement).

### 3.6 EC parity sub-flow (EcWorker streaming compute + EcStripWriter parallel IO)

The EC compute and the parity-disk-write are split across two classes.
`EcWorker` does **streaming compute** — parity shards are built up as
each data shard arrives, overlapping EC compute with data-block writes.

**During `EcStripWriter::push` (streaming compute, overlaps with data writes):**
```
EcStripWriter::push(buffer)
  │
  a. DiskWriter::write(placement.segment(next_block), unit_bytes, buffer)
  │    └─ data block goes to disk (no EC wait)
  b. EcWorker::push(&buffer)
  │    └─ worker computes parity incrementally as each data shard
  │       arrives — streaming compute, overlapping with the next
  │       block's data write
  c. next_block += 1
```

**At `EcStripWriter::finish` (parallel parity write + join):**
```
EcStripWriter::finish()
  │
  a. parity = EcWorker::finish()?
  │    └─ finalizes the streaming computation, returns code_num
  │       parity shards (already mostly computed during push calls)
  b. for i in 0..code_num:
       ParityBatch::spawn(DiskWriter::write(parity_segment[i], parity[i]))
     └─ each parity disk block written in parallel
  c. for each unique disk_id in placement:
       ParityBatch::spawn(DiskWriter::fsync(disk_id))
  d. ParityBatch::join_all()  ── per-strip, joined at finish
  e. EcWorker::reset()  ── reuse the worker for the next strip
  f. return StripResult { parity_handles, bytes_written, ... }
```

This is the "EC flow sub-pipeline" — `EcWorker` owns the streaming
compute, `EcStripWriter` owns the parallel IO (via `ParityBatch`).
`ChunkWriter` sees only `StripResult`; it does not know EC details.

## 4. Class Dependency Graph

```
                    ┌──────────────────────┐
                    │  ChunkClientConfig   │  (value, shared)
                    └──────────┬───────────┘
                               │ read by all
          ┌────────────────────┼──────────────────────────┐
          ▼                    ▼                          ▼
   ChunkPrefetch          ChunkWriter          ┌──────────────────────┐
          │                    │                │ LargeObjectWriter    │
          │ allocates          │ owns           │ LargeAsyncObjectWriter│
          ▼                    ▼                │ SmallObjectWriter(stub)│
   ChunkAllocator    StripWriter (enum)        └──────────┬───────────┘
   (trait)            │                                    │ owns drive loop
                ▲     ├─ Ec(EcStripWriter)                 ▼
                │     └─ Mirror(MirrorStripWriter, stub)  ChunkWriter
                │           │                             ChunkPrefetch
                │           │ owns                        HashWorker (future)
                │           ▼
                │      EcWorker  ──── separate struct (no shared trait)
                │      ParityBatch (per-strip, parallel block writes)
                │           │
                │           │ uses
                │           ▼
                │      DiskWriter (trait)
                │           │
                │           │ impls
                │           ▼
                │      DiskioBlockWriter  ←─── test: LocalFileDiskWriter
                │
                └─── also used by ChunkWriter (seal/delete/append) and
                     ChunkPrefetch (allocate/append)
```

Key edges:
- `LargeObjectWriter` / `LargeAsyncObjectWriter` → own the drive loop,
  `ChunkWriter`, `ChunkPrefetch`, `DiskWriter`, `ChunkAllocator`,
  `ChunkClientConfig`; future → `HashWorker`.
- `SmallObjectWriter` (stub) → `ChunkWriter`, `DiskWriter`,
  `ChunkAllocator`, `ChunkClientConfig`; future → `HashWorker`. Not
  `ChunkPrefetch` (small objects append to an active chunk).
- `ChunkWriter` → `StripWriter` (enum, constructs `Ec` variant),
  `ChunkAllocator` (seal/delete/append), `ChunkClientConfig`. Does
  **not** own the drive loop.
- `EcStripWriter` → `DiskWriter` (data + parity block IO),
  `EcWorker` (streaming EC compute), `ParityBatch` (parallel writes),
  `StripPlacement`, `EcScheme`.
- `EcWorker` → `crow-common::ec` only (pure compute, no IO). Separate
  struct — no shared `Worker` trait with `HashWorker`.
- `HashWorker` (future) → separate struct, owned by object-level
  writer. Different queue length and capability than `EcWorker`.
- `ChunkPrefetch` → `ChunkAllocator`, `StripPlacement`,
  `ChunkClientConfig`.

## 5. Module Structure

Modules group by **concept level**, not by file. Coding against
high-level concepts (object writer, chunk writer, strip writer, worker,
io writer) is cleaner than coding against a flat file list. The
hierarchy mirrors the dependency graph in §4 — each level only depends
downward.

```
lib/crow-chunk-client/src/
  lib.rs                  re-exports
  io.rs                   ChunkIoWriter trait, FeedStatus, BackpressurePolicy
                          (unchanged — the public push interface)
  location.rs             Location (unchanged)
  error.rs                IoError, Result (unchanged)
  config.rs               ChunkClientConfig (new — replaces WriterConfig)

  io/                     ── block-IO layer (the DiskWriter seam)
    mod.rs                re-exports
    disk_writer.rs        DiskWriter trait + DiskioBlockWriter (production,
                          wraps DiskioClient) — renamed from diskio_writer.rs
    local_file.rs         LocalFileDiskWriter (test-only, behind test-util;
                          writes blocks to per-disk files under a temp dir)

  worker/                 ── computation layer (pure compute, no IO)
    mod.rs                re-exports (no shared Worker trait)
    ec_worker.rs          EcWorker (streaming EC compute, owned by
                          EcStripWriter; separate struct)
    hash_worker.rs        HashWorker (placeholder stub, R-later; MD5/SHA-256
                          whole-object digest, owned by object-level writer;
                          separate struct, no shared trait with EcWorker)

  chunk/                  ── chunk + strip layer (chunk-info wrapper + strip
                          │  primitives; later: chunk/strip readers for R107)
    mod.rs                re-exports
    chunk_writer.rs       ChunkWriter (chunk-info wrapper + write ability;
                          iterates strips, triggers append-strip, replaces
                          blocks; does NOT own the drive loop)
    chunk_prefetch.rs     ChunkPrefetch (class — replaces spawn_prealloc_task
                          + run_prealloc; pre-creates chunks, appends strips)
    strip.rs              StripPlacement (with methods) + StripWriter enum
                          + StripResult
    ec_strip_writer.rs    EcStripWriter (owns EcWorker + ParityBatch; writes
                          data + parity blocks via DiskWriter in parallel)
    parity_batch.rs       ParityBatch (per-strip parallel write tracker)
    mirror_strip_writer.rs MirrorStripWriter (placeholder stub, R93/R106)
    chunk_reader.rs       (placeholder, R107 — chunk read flow)
    strip_reader.rs       (placeholder, R107 — strip read flow)

  writer/                 ── object-level layer (user-facing entry points;
                          │  owns the drive loop)
    mod.rs                re-exports
    large_object.rs       LargeObjectWriter (non-blocking stream; owns drive
                          loop + ChunkWriter + ChunkPrefetch; future
                          HashWorker slot)
    large_async_object.rs LargeAsyncObjectWriter (async stream + fetch stage;
                          owns drive loop; same collaborators as above)
    small_object.rs       SmallObjectWriter (placeholder stub, R106)
    pool.rs               WriterPool (no generics)
    fetch.rs              run_fetch_stage (free function — pure IO glue,
                          moved from pipeline.rs; used by LargeAsyncObjectWriter
                          only; no state, stays free)

  traits.rs               ChunkAllocator trait + Arc blanket impl +
                          ChunkdbClient impl (chunk-metadata RPC seam;
                          BlockWriter trait removed — folded into DiskWriter)
```

### 5.1 Module dependency rules

- `writer/` depends on `chunk/`, `worker/`, `io/`, `traits`, `config`.
  Never depends upward. Owns the drive loop.
- `chunk/` depends on `worker/`, `io/`, `traits`, `config`. Does **not**
  depend on `writer/` — chunk/strip writers are reusable primitives.
  Does **not** own the drive loop.
- `worker/` depends on `crow-common::ec`, `Bytes`, `error`. Does **not**
  depend on `io/` or `chunk/` — workers are pure compute. No shared
  `Worker` trait — `EcWorker` and `HashWorker` are separate structs.
- `io/` depends on `crow-diskio-client`, `crow-rpc-ffi`, `crow-protocol`,
  `error`. Does not depend on `worker/` or `chunk/`.
- `traits.rs` depends on `crow-protocol`, `error`. The leaf metadata seam.

### 5.2 What gets removed

- `src/writer/pipeline.rs` — its 525 LOC split into:
  `writer/large_object.rs` + `writer/large_async_object.rs` (drive loop),
  `chunk/chunk_writer.rs` (ChunkWriter), `chunk/ec_strip_writer.rs`
  (EcStripWriter + the `write_strip_with_retry` body),
  `worker/ec_worker.rs` (the `encode_parity_from_shards` call, now
  streaming), `chunk/parity_batch.rs` (ParityBatch),
  `writer/fetch.rs` (`run_fetch_stage`).
- `src/writer.rs` — flattens into `writer/mod.rs`.
- `src/prefetch.rs` — becomes `chunk/chunk_prefetch.rs`.
- `src/diskio_writer.rs` — becomes `io/disk_writer.rs`.

## 6. Testability

The current two-trait-seam design (`ChunkAllocator` + `BlockWriter`)
is replaced by **one block-IO seam (`DiskWriter`) + one chunk-metadata
seam (`ChunkAllocator`)**. The split is cleaner:

- `ChunkAllocator` stays a trait because chunk-metadata RPCs
  (allocate/append/seal/delete) are the natural mock boundary for
  testing chunk rotation, prefetch, and retry logic without a real
  chunkdb server.
- `BlockWriter` is folded into `DiskWriter` because the production
  impl needs `DiskioClient` + `RpcServer` + `Connection` together —
  they are one unit, not two. `DiskWriter::write` takes a `Segment`
  directly, removing the repeated disk_id/zone/offset extraction.
- `LocalFileDiskWriter` (new, test-only) writes blocks to per-disk
  files under a temp dir. This enables UT-level write-flow tests
  (strip writer, chunk rotation, parity hand-off) without the
  `crow-test-harness` diskio/diskdb harness — fast, hermetic, no FFI.
  The existing E2E tests (`tests/large_object_writer_e2e.rs`) keep
  using `DiskioBlockWriter` against the real harness.

## 7. Resolved Questions

All design questions are resolved. Decisions recorded below; the class
definitions (§2), flow (§3), dependency graph (§4), and module
structure (§5) reflect these resolutions.

- **Drive loop ownership — resolved: `LargeObjectWriter` only.**
  `ChunkWriter` is a wrapper of a `ChunkInfo` object that provides
  write ability. It does **not** participate in the drive loop.
  `LargeObjectWriter` rotates chunks, fetches chunks. Inside the
  `ChunkWriter` layer, it iterates strips and triggers append-strip.
  Inside a strip, it can replace a block in a strip. All references to
  the chunkdb client for chunk operations go through `ChunkWriter`
  (seal/delete/append) and `ChunkPrefetch` (allocate/append).
  Additionally: two writer variants — `LargeObjectWriter` (non-blocking
  stream, data already in buffer, efficient) and
  `LargeAsyncObjectWriter` (async stream, complex flow with fetch
  stage + backpressure). Both own the drive loop.

- **`StripWriter` trait object vs enum — resolved: enum.**
  `StripWriter` is a Rust enum: `Ec(EcStripWriter)` |
  `Mirror(MirrorStripWriter)`. Follows Rust conventions — monomorphic,
  heapless dispatch, easy to add new variants. `ChunkWriter` matches
  on the enum.

- **`ParityBatch` ownership — resolved: per-`EcStripWriter`.**
  Parity belongs to each `EcStripWriter`. When writing an EC strip,
  each disk block (data + parity) can be written in parallel.
  `ParityBatch` is owned by the strip writer, joined at strip finish.
  No cross-strip semaphore — parallelism is bounded by the number of
  blocks in the strip (fixed by the EC scheme).

- **`prefetch_chunk_count` default — resolved: 1.**
  Default 1 (minimal latency). Configurable during test for
  throughput experiments.

- **`EcWorker` streaming compute — resolved: streaming.**
  `EcWorker::push` feeds data to the worker, the worker computes
  incrementally (streaming compute). After the worker finishes all
  push tasks (signaled by `finish`), the caller takes the computed
  result. EC compute overlaps with data-block writes — by the time
  the strip is finish-ready, the parity shards are already (mostly)
  computed.

- **`Worker` trait shape — resolved: separate, no shared trait.**
  `EcWorker` (owned by `EcStripWriter`, strip-level) and `HashWorker`
  (owned by the object-level writer, future) are separate structs with
  separate APIs. They live at different layers, have different queue
  lengths and capabilities — a shared `Worker` trait would force a
  false uniformity. 
