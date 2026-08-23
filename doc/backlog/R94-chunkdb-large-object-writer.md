<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R94: chunkdb — Large Object Writer + Chunk IO Interface + Location

**Problem**

chunkdb allocates chunks and manages strip metadata, but there is no
writer that takes an object's byte stream and writes it to chunk
strips via diskio. The chunkdb design §2 Non-Goals: "chunkdb manages
chunk metadata; it does not perform data I/O. Callers (a future object
store, chunkio service) write to the allocated disk blocks themselves."
No such caller exists.

Large objects (size > EC strip data capacity, e.g. > 8 MB for 8+4 EC
with 1 MB blocks, the design doc's default EC scheme §5.2/§14) need a
dedicated chunk per object — sharing a chunk across multiple large
objects would require complex offset management and GC. A dedicated
chunk maps one object to one chunk (or a chain of chunks for very
large objects), with direct EC strip writes. The chunkdb design §15
Future Work lists "Specific chunk type (direct EC write for large
objects)" — this is that requirement.

The E2E tests and pipeline examples below use 4+1 EC (4 MB strip data
capacity, 5 disks) instead of 8+4 (8 MB, 12 disks) to keep the test
topology small — 1 diskdb with 5 disks instead of 12. The pipeline
logic is identical for any EC scheme; only the block/strip counts
change.

Additionally, there is no shared chunk IO async interface or `Location`
type. Both the large-object writer (R94) and the small-object writer
(R106) need a common interface for feeding data into a chunk and a
common `Location` type for recording where object data landed. R94
defines these; R106 and R107 (read flow) depend on them.

**Current behavior + impact**: No object writer exists. No `Location`
type. No chunk IO async interface. The chunkdb client
(`lib/crow-chunkdb-client/`) is management-only — chunk lifecycle RPCs
(allocate/append/seal/delete/query/list/update_chunk_strip) with
endpoint caching, retry, and R99 range routing. It has no data-path
API and should stay that way: chunk management (allocate, delete,
change chunk status) is a distinct concern from chunk data IO (EC
encode, diskio write/fsync, strip preallocation). R94 introduces a
new `crow-chunk-client` lib for the data path; `crow-chunkdb-client`
remains the management client that `crow-chunk-client` calls into.
R106 (small object writer) and R107 (read flow) cannot be built
without the interface and `Location` type defined here.

**Design pointers**: chunkdb root design §5.3 (Chunk — container for
strips; chunk category Metadata/Shared/Specific — "Specific" for
large objects), §5.5 (Chunk type values — Repo/WAL/BTree page/Page
index), §8
(Allocation Flow), §9 (Chunk Lifecycle — Active → Sealed), §10.6
(`append_chunk` RPC — dynamically add strips to an Active chunk),
§11 (EC Encoding — isa-l encode at strip level). The writer uses
chunkdb's `allocate_chunk` to get a chunk, `append_chunk` to add
strips dynamically, and `seal_chunk` when the object is fully written.
Data I/O goes through diskio (R105).

**Use scenarios**:

- **Single-chunk large object**: A 50 MB object is uploaded. The
  writer allocates a specific chunk (EC 4+1, 4 MB data capacity per
  strip), writes 13 EC strips (52 MB capacity, 50 MB used), seals the
  chunk. Returns a single `Location` array with one entry:
  `{chunk_id, [0, 50MB)}`. Expected: the object is durably stored
  across 13 EC strips, readable via R107.

- **Multi-chunk very large object**: A 2.5 GB object is uploaded.
  Max chunk size is ~1 GB (configurable, limited by chunk metadata
  size in KV). The writer allocates chunk 1, fills it to ~1 GB (250
  EC strips for 4+1 at 4 MB/strip), rotates to chunk 2, fills to ~1
  GB, rotates to chunk 3 for the remaining ~0.5 GB. Returns a
  `Location` array with 3 entries: `{chunk1, [0, 1GB)}`,
  `{chunk2, [0, 1GB)}`, `{chunk3, [0, 0.5GB)}`. Expected: the object
  spans 3 chunks, each sealed independently, readable via R107 as a
  contiguous object.

- **Dynamic strip addition**: The writer is mid-write on a chunk and
  the current strip is full. The preallocation task has already
  called `append_chunk` to add the next EC strip (bounded depth
  ahead), so the main write task advances to the pre-allocated strip
  without stalling. The object size was known upfront (50 MB), so the
  writer pre-calculated 13 strips for planning, but only pre-allocates
  `prealloc_depth` (default 2) strips ahead at any time. Expected: no
  write stall while waiting for strip allocation — the next strip is
  already allocated before the current one is full (unless allocation
  falls behind, in which case backpressure kicks in).

- **Object size known upfront**: The caller provides the object size
  to `write_stream`. The writer pre-calculates the strip count
  (ceil(size / strip_data_capacity)) and chunk count for planning,
  but still only pre-allocates `prealloc_depth` strips ahead — not all
  strips at once. Expected: the preallocation pipeline stays ahead of
  the write cursor; no allocation stall on the write path (unless
  allocation is slower than write, in which case backpressure kicks
  in).

- **Object size unknown (streaming)**: The caller passes
  `object_size = None` to `write_stream`. The writer allocates strips
  on-demand as data fills the current strip. Expected: correct
  behavior with potential allocation stalls (acceptable for streaming
  uploads; the preallocation pipeline still runs but starts with 1
  strip).

- **Write error mid-object**: The diskio write to the 3rd EC strip
  fails (disk error). The writer retries the write to a re-allocated
  strip (via `append_chunk` with a new placement). If retries are
  exhausted, the writer returns an error to the caller with the
  partial `Location` array (chunks written so far) for cleanup.
  Expected: no data loss for successfully written strips; the caller
  can abort and free the partial chunks. (This is R94's basic
  whole-strip retry; R110 refines it to single-block replacement —
  keep successful blocks, re-allocate only the failed block via
  `update_chunk_strip`. R94 ships with the coarse approach; R110
  hardens it.)

**End-to-end test flow**

Two E2E cases that exercise the real code path (no mocks): a
single-chunk write and a multi-chunk write with chunk rotation. Both
use the same server topology and the stream-based pipeline API.

**Server topology** (started by the test harness before the test):

- **1 kv-server** — bootstraps group 0 (system group: service
  registry, range bindings, sysdata). The chunkdb and diskdb instances
  register here; `ChunkdbClient` discovers them via
  `ServiceRegistryClient`.
- **1 diskdb** — manages **5 disks** (one per EC block for 4+1: 4
  data + 1 parity). In production these would be on different nodes;
  for the E2E test, one diskdb instance with 5 disks is sufficient —
  placement picks a different disk per block within the strip.
- **1 chunkdb** — chunk metadata server. Allocates chunks, places
  strips across the 5 disks, persists chunk metadata to group 0.
- **test process** — uses `crow-chunk-client` directly:
  `ChunkdbClient` (management RPCs) + `DiskioClient` (block IO) +
  `LargeObjectWriter` (the writer under test). No HTTP layer, no
  object store. The test creates an `AsyncRead` stream (in-memory
  `Cursor<Vec<u8>>` for the test object bytes), calls
  `writer.write_stream(stream, object_size)`, and verifies the
  returned `Vec<Location>`.

**EC scheme for both cases**: `EcScheme::new(4, 1)` — 4 data blocks +
1 parity block per strip, 1 MB block size → strip data capacity =
4 MB, 5 disks total.

**Pipeline architecture** (same for both cases):

The writer runs a fetch stage and a main write task, with per-strip
parity tasks spawned in the background. For EC 4+1, the 4 data blocks
ARE the raw strip data split into 1 MB chunks — they need no encoding
and can be written to disk immediately. The fetch stage reads ≤ 1 MB
per `AsyncRead` call (one data block) and sends each block to the main
write task immediately — no need to wait for the full 4 MB strip
before starting disk writes. The main write task writes each 1 MB data
block to disk as it arrives. When all `data_num` (4) blocks of a strip
are written, the main write task hands off the strip's data blocks to a
background parity task and **immediately advances to the next strip's
first block** without waiting for EC or parity write to complete. The
background parity task computes the 1 parity block, writes it to the
5th disk, and fsyncs all 5 disks. Multiple strips' parity tasks run
concurrently (bounded by `parity_depth`). On `write_stream` completion,
the writer joins all in-flight parity tasks (waits for all fsyncs), then
seals the chunk.

If disk write is slower than network receive, un-written data
accumulates in the fetch stage's cache. The `max_cached_buffer`
(configurable, default 4 MB = 1 strip) limits how much un-written data
is held in memory. When the cache is full, the fetch stage blocks
(backpressure) — this throttles the stream to the disk write speed.
Memory is bounded regardless of object size.

```
                    ┌────────────────────┐
                    │  Prealloc Task    │  background, bounded:
                    │  depth = 2 strips │  at most 2 strips + 1 chunk
                    │  + 1 chunk ahead  │  allocated ahead of cursor
                    └─────────────┬──────────┘
                             │ allocated strips/chunks
                             ▼
  AsyncRead ──► [Fetch] ──► block_buf ──► [Main Write Task] ───────► disk (data)
             read ≤1MB    (1 MB per     │  write 1 data block → 1 disk
             per-block       block)        │  (immediately, no EC wait)
             send to write                  │
             max_cached_buffer              │  when all 4 blocks of strip N written:
             = 4 MB (default)               ├──── hand off ─────────────────────────
             (backpressure if full)         │                     ▼
                                          │   [Parity Task N] (background)
                                          │   EC encode → 1 parity block
                                          │   write parity → 5th disk
                                          │   fsync all 5 disks
                                          │   (bounded: parity_depth=2)
                                          ▼
                                     advance to strip N+1, block 0
                                     (no wait for parity)
```

- **Fetch stage**: reads from the `AsyncRead` stream in ≤ 1 MB per
  call (one data block size). A single socket read may return less
  (64 KB, 512 KB) — the fetch stage accumulates to 1 MB, then sends
  the block to the main write task immediately. Does not wait for the
  full strip (4 MB) before sending — each 1 MB block is sent as soon
  as it's read. The fetch stage does not track block indices or strip
  boundaries — it sends sequential `Bytes` blocks; the main write
  task tracks indices. The `max_cached_buffer` (default 4 MB) limits
  total un-written data in the fetch channel: blocks sent to the main
  write task but not yet written to disk count against this budget.
  When the channel is full (disk write slower than receive), the fetch
  stage blocks (backpressure). On EOF with a partial last block, sends
  the partial block.
- **Main write task**: the central coordinator. At the start of each
  new strip, it first awaits the strip's placement from the prealloc
  channel (blocks if prealloc fell behind). Then receives 1 MB blocks
  one at a time from the fetch channel, tracks block index within the
  current strip (`count % data_num`, 0–3 for 4+1), writes each to the
  strip's corresponding data segment via `BlockWriter::write` (one
  disk per block, no EC wait). When all `data_num` (4) blocks of strip
  N are written, hands off the strip's data blocks to a background
  parity task (bounded by `parity_depth` — if the parity task pool is
  full, blocks until a slot opens), records the parity task's
  `JoinHandle` in the current chunk's handle list. Then **immediately
  advances to the next strip's first block** — does not wait for EC
  compute, parity write, or fsync. On partial last strip (EOF before
  all `data_num` blocks filled — partial strips only occur at EOF,
  never mid-chunk), writes only the filled data blocks, releases the
  empty ones, hands off to parity for partial EC, and records
  `sealed_length`. On chunk full, triggers rotation (join parity +
  seal + Location + switch to prefetched chunk).
- **Parity tasks** (one per strip, background `tokio::spawn`): receives
  the strip's data blocks, EC-encodes via
  `crow_common::ec::encode_parity_from_shards(4+1, data_shards)`
  → 1 parity block (1 MB), writes it to the 5th segment via
  `BlockWriter::write`, `fsync` all 5 disks via `BlockWriter::fsync`.
  Bounded by `parity_depth` (default 2) — at most 2 parity tasks in
  flight. While strip N's parity task is computing + writing parity,
  the main write task is already writing strip N+1's data blocks, and
  strip N+2's blocks are being fetched.
- **Prealloc task**: background `tokio::spawn`. Allocates the first
  chunk with **1 strip** (minimum to start writing immediately), then
  allocates remaining strips via `append_chunk` ahead of the write
  cursor, up to `prealloc_depth` (default 2) strips ahead. If object
  size is known, pre-calculates total strip/chunk count for planning,
  but still only pre-allocates `prealloc_depth` ahead — a 1 TB object
  does not allocate all 250K strips at once. Pre-allocates the next
  chunk when the current chunk is within `prealloc_depth` strips of
  full. If prealloc falls behind the write cursor, the main write task
  blocks (backpressure) until the next strip is ready.

**Memory per writer** (4+1 EC, 1 MB blocks): `max_cached_buffer`
(default 4 MB un-written data in fetch cache) + 1 block being written
(1 MB) + up to `parity_depth` (2) parity tasks. Each parity task
holds the strip's data blocks (4 × 1 MB = 4 MB, shared via `Bytes`
ref count — not copied, but still resident until EC compute
completes) + 1 parity block (1 MB, allocated during compute).
Peak: 4 + 1 + 2 × (4 + 1) = 15 MB. Bounded regardless of object
size. High parallel write: N concurrent writers = N × 15 MB, limited
by available memory (configurable `memory_budget`; the writer pool
rejects new writes when budget is exhausted).

---

**Case 1: 50 MB object, single chunk (no rotation)**

Parameters: `object_size = Some(50 MB)`, `ec_scheme = 4+1`,
`max_chunk_size = 1 GB` (default), `prealloc_depth = 2`.

Math: `strip_count = ceil(50 MB / 4 MB) = 13` (12 full strips = 48 MB
+ 1 partial strip = 2 MB). Total strip data = 52 MB < 1 GB →
`chunk_count = 1`, no rotation.

Step-by-step:

1. **Construct + start pipeline** —
   `LargeObjectWriter::new(chunkdb, diskio, EcScheme::new(4,1),
   WriterConfig { max_chunk_size: 1 GB, prealloc_depth: 2,
   parity_depth: 2, read_buffer_size: 1 MB, max_cached_buffer: 4 MB })`.
   - Writer pre-calculates: 13 strips, 1 chunk (known size).
   - Prealloc task: `chunkdb.allocate_chunk(Repo, Specific, EC 4+1,
     initial_strips=1)` → `chunk_id` + strip 0 placement (5 segments).
     Immediately starts allocating strip 1 via `append_chunk`.
   - Pipeline starts: fetch stage and main write task launch as
     concurrent tasks connected by a block channel; parity tasks
     spawn per strip in the background.

2. **Pipeline runs — strips 0–11 (full, 4 MB each)**:
   - Fetch: reads 1 MB from stream → sends block 0 to main write
     task. Reads 1 MB → sends block 1. Reads 1 MB → block 2.
     Reads 1 MB → block 3. Strip N complete (4 × 1 MB). Starts
     fetching strip N+1 block 0 immediately.
   - Main write task (strip N): writes block 0 to disk 0 as soon as
     it arrives (no wait for full strip). Writes block 1 to disk 1.
     Block 2 to disk 2. Block 3 to disk 3. All 4 data blocks written
     → hands off data blocks to parity task N → **immediately
     advances to strip N+1 block 0** (no wait for EC/parity/fsync).
   - Parity task N (background):
     `ec::encode_parity_from_shards(4+1, data_shards)` → 1 parity
     block, writes to 5th disk, `fsync` all 5 disks. Bounded by
     `parity_depth = 2` — at most 2 parity tasks in flight.
   - Overlap: while parity task N computes + writes parity, main write
     task writes strip N+1's data blocks, and fetch fills strip N+2's
     blocks. Three strips in flight simultaneously (N parity, N+1
     data, N+2 fetch). If disk write is slower than receive, fetch
     blocks when `max_cached_buffer` (4 MB) is full.

3. **Pipeline runs — strip 12 (partial, 2 MB)**:
   - Fetch: reads 1 MB → block 0. Reads 1 MB → block 1. EOF
     (2 MB total, < 4 MB strip capacity). Sends partial block 1.
   - Main write task: writes block 0 (1 MB) to disk 0, block 1
     (1 MB) to disk 1. Blocks 2 and 3 are empty — not written to
     disk (released). Hands off to parity task 12 for partial EC
     (isa-l encodes parity from 2 data blocks, no padding).
     Records `sealed_length = 2 MB`.
   - Parity task 12 (background): computes parity from the 2
     written data blocks, writes to 5th disk, `fsync`.

4. **Finish** — main write task done (EOF). Join all in-flight parity
   tasks (wait for all fsyncs to complete). Then:
   - `chunkdb.seal_chunk(chunk_id)` → Active → Sealed.
   - Returns `Vec<Location>` with 1 entry:
     `{chunk_id, offset: 0, length: 50 MB, logical_offset: 0,
     logical_length: 50 MB}`.

5. **Verify** — test checks:
   - 1 Location entry, `chunk_id` matches, `offset = 0`,
     `length = 50 MB`, `logical_offset = 0`, `logical_length = 50 MB`.
   - `chunkdb.query_chunk(chunk_id)` → chunk state = Sealed, 13 strips
     (12 full + 1 partial with `sealed_length = 2 MB`).
   - Data integrity: read back 50 MB via a test reader (or R107) using
     the Location → identical bytes.

---

**Case 2: 100 MB object, chunk rotation (2 strips per chunk)**

Parameters: `object_size = Some(100 MB)`, `ec_scheme = 4+1`,
`max_chunk_size = 8 MB` (2 strips × 4 MB — deliberately small to
trigger rotation), `prealloc_depth = 2`.

Math: `strip_count = ceil(100 MB / 4 MB) = 25` (all full strips, 100
/ 4 = 25 exactly). `max_chunk_size = 8 MB` → 2 strips per chunk →
`chunk_count = ceil(25 / 2) = 13` (12 chunks × 2 strips = 24 strips =
96 MB + 1 chunk × 1 strip = 4 MB = 100 MB total).

Step-by-step:

1. **Construct + start pipeline** —
   `LargeObjectWriter::new(chunkdb, diskio, EcScheme::new(4,1),
   WriterConfig { max_chunk_size: 8 MB, prealloc_depth: 2,
   parity_depth: 2, read_buffer_size: 1 MB, max_cached_buffer: 4 MB })`.
   - Writer pre-calculates: 25 strips, 13 chunks.
   - Prealloc task: allocates chunk 1 with 1 strip. Starts allocating
     strip 1 (via `append_chunk`) + pre-fetching chunk 2 (via
     `ChunkPrefetch`, depth 1 chunk ahead).

2. **Pipeline runs — chunk 1 (strips 0–1, 8 MB)**:
   - Fetch reads 1 MB at a time, sends each block to main write task.
     Main write task writes each block to disk immediately, hands off
     parity per strip, advances to next strip's first block without
     waiting for EC.
   - After strip 1: chunk 1 reaches `max_chunk_size` (8 MB, 2 strips)
     → **rotation** in the main write task:
     - Join in-flight parity tasks for chunk 1 (wait for fsyncs).
     - `chunkdb.seal_chunk(chunk_1)` → Sealed.
     - Record `Location` for chunk 1: `{chunk_1, offset: 0, length:
       8 MB, logical_offset: 0, logical_length: 8 MB}`.
     - Switch to pre-fetched chunk 2 (from `ChunkPrefetch`).
     - Prealloc task starts allocating strips in chunk 2 + pre-fetching
       chunk 3.
   - Pipeline continues without stalling (chunk 2 was pre-fetched).

3. **Pipeline runs — chunks 2–12** — each chunk: 2 strips × 4 MB =
   8 MB, sealed, Location recorded. After chunk 12: 24 strips written
   = 96 MB, 12 Locations accumulated.
   - `logical_offset` advances by 8 MB per chunk: 0, 8, 16, ..., 88.

4. **Pipeline runs — chunk 13 (strip 24, last 4 MB)**:
   - Fetch: reads 4 × 1 MB blocks → EOF. Main write task writes 4
     data blocks (one per block as it arrives), hands off to parity
     task 24. Full strip, no padding. Only 1 strip in this chunk.
   - Parity task 24 (background): computes parity, writes to 5th disk,
     `fsync`.

5. **Finish** — main write task done (EOF). Join all in-flight parity
   tasks (wait for all fsyncs). Then:
   - `chunkdb.seal_chunk(chunk_13)` → Sealed.
   - Record `Location` for chunk 13: `{chunk_13, offset: 0, length:
     4 MB, logical_offset: 96 MB, logical_length: 4 MB}`.
   - Returns `Vec<Location>` with 13 entries, ordered by
     `logical_offset`: [0, 8), [8, 16), ..., [88, 96), [96, 100).

6. **Verify** — test checks:
   - 13 Location entries, `logical_offset` = 0, 8, 16, ..., 96 MB.
   - `length` = 8 MB for entries 0–11, 4 MB for entry 12.
   - `logical_length` matches: 8 MB × 12 + 4 MB = 100 MB.
   - All 13 chunks Sealed via `query_chunk`.
   - Chunk 1–12 have 2 strips each; chunk 13 has 1 strip.
   - Data integrity: read back 100 MB via the 13 Locations → identical
     bytes, contiguous (no gaps).

**Solution**

A stream-based large-object writer in the new `crow-chunk-client` lib
that writes one object to one or more dedicated chunks (Specific chunk
category), using EC strips directly. The writer accepts an `AsyncRead`
stream (two source types: no-lock continuous, or socket-like async IO
that can block) and drives a pipeline with a fetch stage, a main write
task, and per-strip background parity tasks. For EC 4+1, the 4 data
blocks are raw data — the fetch stage reads 1 MB at a time (one data
block) and sends each block to the main write task immediately, so the
first disk write starts after just 1 MB. The main write task writes
each block to disk as it arrives; when all 4 blocks of a strip are
written, it hands off to a background parity task and immediately
advances to the next strip without waiting for EC. The parity task
computes the parity block, writes it, and fsyncs, bounded by
`parity_depth` (default 2). If disk write is slower than receive,
`max_cached_buffer` (default 4 MB) throttles the stream (backpressure).
A bounded preallocation task allocates strips and chunks ahead of the
write cursor (default 2 strips + 1 chunk ahead), starting with just 1
strip so writing begins immediately; the bound prevents a 1 TB object
from allocating all strips at once. `crow-chunk-client` owns the data
path — the `Location` type, the `ChunkIoWriter` async interface, the
large-object writer, the pipeline, and chunk/strip prefetch helpers
— and calls into `crow-chunkdb-client` (management RPCs: allocate,
append, seal, delete) and `crow-diskio-client` (block writes, fsync).
R106 (small-object writer) and R107 (read flow) live in the same lib
and reuse the interface and `Location` type. Memory per writer is
bounded (~15 MB for 4+1 EC — data buffers shared via `Bytes` ref
count between write and EC, not duplicated, but still resident until
EC compute completes) regardless of object size; high parallel
write is limited by a configurable memory budget.

**One-line summary**: A new `crow-chunk-client` lib owning the chunk
data path — a stream-based large-object writer with a block-granularity
pipeline (fetch 1 MB → write data block immediately → hand off parity
per strip in background), bounded strip/chunk preallocation,
`max_cached_buffer` backpressure, chunk rotation, and the shared
`ChunkIoWriter` interface + `Location` type used by all chunk data-path
components (R106, R107).

**Numbered work items**:

1. **`crow-chunk-client` crate** (`lib/crow-chunk-client/`) — new
   workspace member owning the chunk data path. `Cargo.toml` depends
   on `crow-chunkdb-client` (management RPCs), `crow-diskio-client`
   (block write/fsync, R105), `crow-common` (`EcScheme`, `ec::encode`
   — isa-l EC), `crow-protocol` (`ChunkId`, request/response types),
   plus `tokio`, `bytes`, `prost`, `thiserror`, `tracing`. Add to
   `members` in the root `Cargo.toml`. `src/lib.rs` re-exports
   `Location`, `ChunkIoWriter`, `LargeObjectWriter`, `WriterConfig`,
   `ChunkPrefetch`, `WriterPool`, and the `IoError` error enum. This
   lib is the home for all chunk IO code — R106 (small-object writer)
   and R107 (read flow) add their modules here too.
   `crow-chunkdb-client` stays management-only.

2. **`Location` type** (`lib/crow-chunk-client/src/location.rs`) —
   the addressing unit for object data within chunks:
   ```rust
   pub struct Location {
       pub chunk_id: ChunkId,       // 128-bit chunk ID
       pub offset: u64,             // byte offset within the chunk [start)
       pub length: u64,             // byte length in this chunk
       pub logical_offset: u64,     // object-level logical offset
       pub logical_length: u64,     // object-level logical length
   }
   ```
   A `Location` describes where a contiguous range of object data
   lives within one chunk. An object that spans multiple chunks has a
   `Vec<Location>` — one entry per chunk, ordered by `logical_offset`.
   Serialization: protobuf (for KV storage) + binary (for compact
   encoding in object metadata). The `logical_offset`/`logical_length`
   fields support future hole-punching and sparse objects.

3. **`ChunkIoWriter` async interface**
   (`lib/crow-chunk-client/src/io.rs`) — the shared push-based trait
   for chunk data-path writers, used by R106 (small-object writer
   where the caller pushes individual objects):
   ```rust
   /// Result of `on_data` — does the writer need more data?
   pub enum FeedStatus {
       /// Buffer stored; writer has capacity — send more data.
       Continue,
       /// Buffer stored; writer is at capacity — pause feeding.
       /// Check `require_data()` before resuming.
       Pause,
   }

   #[async_trait]
   pub trait ChunkIoWriter: Send {
       async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus, IoError>;
       async fn on_finish(&mut self) -> Result<Vec<Location>, IoError>;
       async fn on_error(&mut self) -> Result<Vec<Location>, IoError>;
       fn require_data(&self) -> bool;
   }
   ```
   - `on_data` pushes a data buffer to the writer. **Always stores
     the buffer** (awaits until internal capacity is available —
     never rejects). Returns `Ok(FeedStatus::Continue)` if the
     writer has capacity for the next push; `Ok(FeedStatus::Pause)`
     if the writer is now at capacity (the next `on_data` will
     block). The buffer is never dropped.
   - `require_data` is a non-async hint the caller can poll before
     calling `on_data`: returns `true` if `on_data` would not block,
     `false` if it would. Lets the caller decide: push anyway
     (block), or back off (yield / return 503).
   - `on_finish` signals end of input; the writer flushes, seals,
     and returns the `Vec<Location>`.
   - `on_error` aborts the write; returns `Location`s of already-
     sealed chunks for caller cleanup.
   See Open Questions for the full contract with caller examples
   (blocking vs non-blocking strategies).
   `LargeObjectWriter` (item 4) also implements this trait for
   callers who want manual push, but its primary API is the
   stream-based `write_stream` (item 5) which drives the pipeline
   internally. In `write_stream` mode, the fetch stage pulls from
   `AsyncRead` and pushes `Bytes` to the block channel. In `on_data`
   (push) mode, there is no fetch stage — `on_data` sends the
   caller's `Bytes` directly to the same block channel (with the same
   backpressure). The main write task and parity tasks are identical
   in both modes. `require_data` is a non-async check of the block
   channel's capacity. R107 (read flow) uses the `Location` type but
   not this trait.

4. **`LargeObjectWriter` + `WriterConfig`**
   (`lib/crow-chunk-client/src/writer/large_object.rs`) — the
   large-object writer. Generic over `A: ChunkAllocator` +
   `W: BlockWriter` (trait seams for testability — see design §1.3;
   `ChunkdbClient` implements `ChunkAllocator`, `DiskioClient`
   implements `BlockWriter`). Constructor takes `A`, `W`, `EcScheme`,
   and a `WriterConfig`:
   ```rust
   pub struct WriterConfig {
       pub max_chunk_size: u64,       // default 1 GB
       pub prealloc_depth: usize,     // default 2 strips ahead
       pub parity_depth: usize,       // default 2 parity tasks in flight
       pub chunk_prefetch_depth: usize, // default 1 chunk ahead
       pub read_buffer_size: usize,   // default 1 MB (one data block)
       pub max_cached_buffer: usize,   // default 4 MB (max un-written data)
   }
   ```
   The primary API is stream-based:
   ```rust
   pub async fn write_stream(
       &mut self,
       reader: impl AsyncRead + Unpin + Send,
       object_size: Option<u64>,
   ) -> Result<Vec<Location>, IoError>;
   ```
   `reader` is an `AsyncRead` — two source types both implement it:
   a no-lock source (in-memory, file — `poll_read` returns
   immediately) and a socket source (TCP, HTTP body — `poll_read` may
   return `Pending`, the async runtime handles blocking). The writer
   does not distinguish; the pipeline pulls data via `reader` and the
   runtime yields on blocking reads. `object_size` is optional — if
   known, the writer pre-calculates strip/chunk count for planning but
   still only pre-allocates `prealloc_depth` ahead; if `None`,
   streaming mode (on-demand strip count). `LargeObjectWriter` also
   implements `ChunkIoWriter` (item 3) for manual push, but
   `write_stream` is the recommended API.

5. **Pipeline stages** (`lib/crow-chunk-client/src/writer/pipeline.rs`)
   — a fetch stage, a main write task, and per-strip background parity
   tasks. Launched by `write_stream`:
   - **Fetch stage**: reads from the `AsyncRead` stream in ≤
     `read_buffer_size` chunks (default 1 MB — one data block size).
     A single socket read may return less (64 KB, 512 KB) — the fetch
     stage accumulates to 1 MB, then sends the block to the main write
     task immediately. Does not wait for the full strip (4 MB) before
     sending — each 1 MB block is sent as soon as it's read, so the
     first disk write starts after just 1 MB. The fetch stage does not
     track block indices or strip boundaries — it sends sequential
     `Bytes` blocks; the main write task tracks indices. The
     `max_cached_buffer` (default 4 MB) limits total un-written data
     in the fetch channel: blocks sent to the main write task but not
     yet written to disk count against this budget. When the channel
     is full (disk write slower than receive), the fetch stage blocks
     (backpressure). On EOF with a partial last block, sends the
     partial block.
   - **Main write task**: the central coordinator. At the start of
     each new strip, it first awaits the strip's placement from the
     prealloc channel (blocks if prealloc fell behind). Then receives
     1 MB blocks one at a time from the fetch channel, tracks block
     index within the current strip (`count % data_num`, 0–3 for 4+1),
     writes each to the strip's corresponding data segment via
     `BlockWriter::write` (one disk per block, no EC wait). When all
     `data_num` (4) blocks of strip N are written, hands off the
     strip's data blocks to a background parity task (bounded by
     `parity_depth`, default 2 — if the parity task pool is full,
     blocks until a slot opens), records the parity task's
     `JoinHandle` in the current chunk's handle list. Then
     **immediately advances to the next strip's first block** — does
     not wait for EC compute, parity write, or fsync. On partial last
     strip (EOF before all `data_num` blocks filled — partial strips
     only occur at EOF, never mid-chunk), writes only the filled data
     blocks, releases the empty ones, hands off to parity for partial
     EC, and records `sealed_length`. On chunk full, triggers rotation
     (item 7).
   - **Parity tasks** (one per strip, background `tokio::spawn`):
     receives the strip's data blocks (full or partial), EC-encodes
     via `crow_common::ec::encode_parity_from_shards(scheme,
     data_shards)` → `code_num` parity blocks (1 × 1 MB for 4+1),
     writes them to the remaining segments via `BlockWriter::write`,
     `fsync` all disks via `BlockWriter::fsync`. Bounded by
     `parity_depth` (default 2) — at most `parity_depth` parity tasks
     in flight. While strip N's parity task computes + writes parity,
     the main write task is already writing strip N+1's data blocks,
     and strip N+2's blocks are being fetched.
   The pipeline drains on EOF: fetch sends partial last block → main
   write task writes it + hands off parity for last strip → all
   parity tasks complete (join) → `write_stream` seals the chunk and
   returns the `Location` array. Memory in flight:
   `max_cached_buffer` (4 MB) + 1 block being written (1 MB) + up to
   `parity_depth` (2) parity tasks, each holding the strip's data
   blocks (4 MB, shared via `Bytes` ref count — not copied, but
   resident until EC compute completes) + 1 parity block (1 MB).
   Peak: 4 + 1 + 2 × (4 + 1) = 15 MB for 4+1 EC.

6. **Bounded preallocation + chunk prefetch**
   (`lib/crow-chunk-client/src/prefetch.rs`) — a background
   `tokio::spawn` task that allocates strips and chunks ahead of the
   write cursor, with a bounded depth to prevent allocation pressure
   on very large objects:
   - **Strip preallocation**: allocates the first chunk with **1
     strip** (minimum to start writing immediately), then allocates
     remaining strips via `append_chunk` up to `prealloc_depth`
     (default 2) strips ahead of the write cursor. If `object_size` is
     known, pre-calculates total strip count for planning but still
     only pre-allocates `prealloc_depth` ahead — a 1 TB object does
     not allocate all 250K strips at once. When the write cursor
     advances, the task allocates the next strip to maintain the
     depth. If prealloc falls behind (allocation slower than write),
     the main write task blocks (backpressure) until the next strip is
     ready.
   - **Chunk prefetch**: pre-allocates the next chunk via
     `allocate_chunk` when the current chunk is within
     `prealloc_depth` strips of full, up to `chunk_prefetch_depth`
     (default 1) chunk ahead. Delivers the allocated chunk via a
     `oneshot` channel. The small-object writer (R106) also uses this
     to pre-allocate shared chunks.

7. **Chunk rotation** (`lib/crow-chunk-client/src/writer/large_object.rs`)
   — when the current chunk's accumulated size reaches `max_chunk_size`
   (default 1 GB, limited by chunk metadata size in KV — a chunk with
   250 EC strips has 250 × 5 = 1250 segment entries, near the KV
   value size limit), the main write task performs rotation without
   stalling the pipeline. The writer does not set chunk size directly
   — it can only `append_chunk` (add strips) and observe the resulting
   size. Chunk size is always a multiple of strip data capacity (sum
   of appended strips), so rotation happens at strip boundaries, never
   mid-strip. `max_chunk_size` is a threshold: when the current chunk
   reaches or exceeds it after a full strip, the writer seals and
   rotates:
   - Join in-flight parity tasks for the current chunk (wait for
     fsyncs).
   - `chunkdb_client::seal_chunk(chunk_id)` → Active → Sealed.
   - Record the `Location` for the filled chunk: `{chunk_id, [0,
     written_bytes), logical_offset, logical_length}`.
   - Switch to the pre-fetched chunk (from `ChunkPrefetch`). If the
     next chunk is not yet allocated (prefetch fell behind), the main
     write task blocks until it's ready.
   - Prealloc task starts allocating strips in the new chunk +
     pre-fetching the next chunk.
   The `Location` array accumulates one entry per rotated chunk.

8. **Completion + error handling**
   (`lib/crow-chunk-client/src/writer/large_object.rs`) —
   `write_stream` completion: main write task
   finishes (EOF), joins all in-flight parity tasks (waits for all
   fsyncs), then seals the current chunk, returns the
   `Location` array. Error / abort (`on_error` or `Drop`):
   - Abort in-flight pipeline tasks (cancel fetch, main write, parity tasks).
   - Free partial chunks via `chunkdb_client::delete_chunk` (or
     `delete_chunk_range` for partial chunks).
   - Return `Location`s of already-sealed chunks for caller cleanup.
   - `Drop` impl calls abort as a safety net if the caller drops the
     writer without finishing.
   R94 ships with coarse error handling: whole-strip retry (up to 3
   attempts) on diskio write failure, abort on EC encode failure.
   R110 (large-write IO error handling) refines this to single-block
   replacement + negative list + degraded strip tracking; R94's
   error paths are the integration points R110 hooks into.

9. **High-parallel write / memory budget**
   (`lib/crow-chunk-client/src/writer/pool.rs`) — a `WriterPool` that
   bounds concurrent writers by a configurable `memory_budget`. Each
   writer's footprint is bounded (~15 MB for 4+1 EC:
   `max_cached_buffer` (4 MB) + 1 block write (1 MB) + up to 2 parity
   tasks (10 MB — each holds 4 MB data via shared `Bytes` ref + 1 MB
   parity block; data not copied but resident until EC compute
   completes)). `max_concurrent = memory_budget /
   per_writer_memory`. The pool rejects new writes (returns
   `IoError::MemoryBudgetExhausted`) when the budget is full, enabling
   backpressure up the call stack. This allows serving many parallel
   large-object uploads limited only by available RAM, not by object
   size — a 1 TB object and a 50 MB object use the same ~15 MB.

**Flow diagram**:

```
Caller              crow-chunk-client (pipeline)          chunkdb-client      diskio-client
  |                  +-------------------------+          (mgmt RPCs)         (R105)
  |                  | Prealloc Task           |          allocate/append/    write/fsync
  |                  | depth=2 strips+1 chunk  |          seal/delete
  |                  +-----------+-------------+
  |                              | allocated strips/chunks
  |                              v
  | write_stream(reader, 50MB)   |
  |----------------------------->|
  |                             | allocate_chunk(1 strip)
  |                             |-------------------------->|
  |                             |<--------------------------|
  |                             |                           |
  |                  +----------+---------------------------+------------------------+
  |                  | Fetch    | Main Write Task           | Parity Tasks           |
  |                  | (read    | (write data blocks,       | (EC compute + write    |
  |                  |  <=1MB)  |  advance immediately)     |  parity + fsync)       |
  |                  |          |                           | (bounded: depth=2)     |
  |                  |          |                           |                        |
  |  reader -------->| read 1MB |                           |                        |
  |  (AsyncRead)     | =block 0 |                           |                        |
  |                  |--------->| write block 0 -> disk 0   |                        |
  |                  | read 1MB |                           |                        |
  |                  | =block 1 |                           |                        |
  |                  |--------->| write block 1 -> disk 1   |                        |
  |                  | read 1MB |                           |                        |
  |                  | =block 2 |---------> write block 2   |                        |
  |                  |--------->|                        -->| disk 2                 |
  |                  | read 1MB |                           |                        |
  |                  | =block 3 |                           |                        |
  |                  |--------->| write block 3 -> disk 3   |                        |
  |                  |          |                           |                        |
  |                  |          | all 4 blocks done:        |                        |
  |                  |          | hand off data --> [Parity Task 0]                  |
  |                  |          |                   EC encode -> 1 parity block      |
  |                  | read 1MB | advance to N+1   write parity -> 5th disk         |
  |                  | =blk 0   | (no wait)        fsync all 5 disks               |
  |                  |--------->| write blk 0      (runs in background)             |
  |                  |    ^     |    ^ three strips in flight: N parity, N+1 data, |
  |                  |    |     |    | N+2 fetch (all concurrent)                   |
  |                  | read...  |                                                   |
  |                  |  ...     |  max_cached_buffer=4MB: if disk slow, fetch      |
  |                  |          |  blocks (backpressure)                            |
  |                  |          |                                                   |
  |  EOF             |          |                                                   |
  |---------------------------->| send partial last block                            |
  |                  | 2MB      |---------> write data + hand off [Parity Task 12]  |
  |                  |          |                                                   |
  |                  |          | JOIN all parity tasks (wait for all fsyncs)       |
  |                  |          |                  |                               |
  |                  |          |          seal_chunk                              |
  |                  |          |-------------------------->|                       |
  |                  |          |<--------------------------|                       |
  | <--------------------------|                                                   |
  | Vec<Location> = [          |                                                   |
  |   {chunk_id, [0, 50MB),    |                                                   |
  |    logical [0, 50MB)}      |                                                   |
  | ]                          |                                                   |
  |                  +----------+---------------------------+------------------------+
```

**Edge cases at a glance**:

- Object size unknown (streaming, `object_size = None`) → strips
  allocated on-demand; the preallocation pipeline starts with 1 strip
  and allocates ahead by `prealloc_depth`. If allocation is slower
  than the write rate, the main write task blocks (backpressure) until
  the next strip is ready. Acceptable for streaming.
- Stream returns small partial reads (e.g. 64 KB per socket read) →
  the fetch stage accumulates to 1 MB (one data block) across
  multiple reads, then sends the block to the main write task. No
  data loss; the first disk write starts after 1 MB, not after the
  full 4 MB strip.
- Object size < strip data capacity (e.g. 2 MB with 4 MB strips) →
  one partial strip, EC-encoded via isa-l partial EC (no padding),
  empty data blocks released, `sealed_length = 2 MB`. The chunk has
  one strip. (See Open Questions — partial EC direction pending UT
  verification.)
- Object size exactly equals strip data capacity → one full strip,
  no partial EC needed.
- Chunk rotation at strip boundary → the current chunk is sealed
  after the last full strip, a new chunk is switched to from
  `ChunkPrefetch`, and the next strip starts in the new chunk. The
  `Location` array has one entry per chunk with contiguous
  `logical_offset` ranges. The pipeline does
  not stall if the next chunk was pre-fetched. This is the normal
  rotation case (E2E Case 2 exercises it).
- Last strip of object is partial (not rotation) → the final strip
  is sealed with `sealed_length` < strip data capacity, EC-encoded
  via isa-l partial EC (no padding), empty data blocks released.
  This is the normal end-of-object case, not a rotation event.
- Preallocation falls behind (allocation slower than write) → the
  main write task blocks waiting for the next strip. The fetch stage
  continues filling `max_cached_buffer`, then blocks when it's full.
  Backpressure propagates up the pipeline; no data is lost.
- EC encode failure → the parity task returns an error; the
  pipeline aborts immediately, `write_stream` returns
  `IoError::EcEncodeFailed`. No retry in R94 — EC encode is a CPU/isal
  error, not a placement issue, so retrying with a new placement won't
  help. R110 may refine this to mark the strip degraded (data durable,
  parity missing) or retry with a fallback encoder; R94 ships with
  abort and the abort path is the integration point for R110.
- diskio write failure on one of 5 EC blocks (4+1) → the write stage
  retries the whole strip with a new allocation (R94's basic
  approach). Partially written blocks on the failed strip are freed
  via `update_chunk_strip`. R110 replaces this with single-block
  replacement: keep the 4 successful blocks, re-allocate only the
  failed block on a healthy disk, `update_chunk_strip` to replace
  just that segment. R94 ships with whole-strip retry; R110 hardens
  it.
- Caller drops the writer without finishing → `Drop` impl aborts the
  pipeline (cancels fetch, main write, parity tasks), frees partial
  chunks. This is a safety net; the caller should call `on_error`
  explicitly.
- Compression (future) → when compression is added, the compressed
  size differs from input size. The writer uses the streaming path
  (`object_size = None`, on-demand strip allocation). Not implemented
  in R94; noted for future.
- Memory budget exhausted → `WriterPool` rejects new `write_stream`
  calls with `IoError::MemoryBudgetExhausted`. Existing writers
  continue; the caller retries when a writer finishes.

**Dependencies**

- **Lib structure**: R94 introduces `lib/crow-chunk-client/` (new
  workspace member) as the chunk data-path lib. It depends on:
  - `crow-chunkdb-client` (landed, R85) — management RPCs:
    `allocate_chunk`, `append_chunk`, `seal_chunk`, `delete_chunk`,
    `delete_chunk_range`, `update_chunk_strip`, `query_chunk`. The
    management client stays unchanged; R94 only consumes it.
  - `crow-diskio-client` (R105) — `DiskIoClient::write` + `fsync`
    for EC block writes.
  - `crow-common` (landed) — `EcScheme`, `ec::encode` (isa-l).
  - `crow-protocol` — `ChunkId`, allocate/append/seal request types.
- **Depends on**: **R105** (disk IO engine) — writes EC blocks via
  `DiskIoClient`. **chunkdb** (landed, R85) — management RPCs (via
  `crow-chunkdb-client`). **crow-common EC** (landed) — isa-l encode.
- **Depended on by**:
  - **R106** (small object writer) — uses the `ChunkIoWriter` trait
    and `Location` type defined here; R106's `SmallObjectWriter`,
    `PipelineManager`, and `WriterMetrics` modules also live in
    `crow-chunk-client` (paths in the R106 doc must be updated from
    `lib/crow-chunkdb-client/src/writer/...` to
    `lib/crow-chunk-client/src/writer/...`).
  - **R107** (chunk read flow) — uses the `Location` type to locate
    and read object data; R107's `ChunkReader` also lives in
    `crow-chunk-client` (`lib/crow-chunk-client/src/reader.rs`, path
    in the R107 doc must be updated from
    `lib/crow-chunkdb-client/src/reader.rs`).
  - **R110** (large-write IO error handling) — hardens R94's
    coarse error handling (whole-strip retry → single-block
    replacement + negative list + degraded strip tracking; EC encode
    failure abort → degraded-strip or fallback-encoder retry). R110
    modifies `large_object.rs` to add single-block replacement on
    write failure and refined EC encode failure handling; R94's error
    paths (abort, `CancellationToken`, partial-`Location` return,
    `delete_chunk` cleanup) are the integration points.

**Acceptance**

**Crate scaffolding**:
- `crow-chunk-client` builds as a workspace member; `lib.rs`
  re-exports `Location`, `ChunkIoWriter`, `LargeObjectWriter`,
  `WriterConfig`, `ChunkPrefetch`, `WriterPool`, `IoError`;
  `Cargo.toml` lists `crow-chunkdb-client`, `crow-diskio-client`,
  `crow-common`, `crow-protocol`, `tokio` (rt, sync, io-util),
  `bytes`, `prost`, `thiserror`, `tracing` as deps. Unit test (compile
  + re-export check).

**Location type**:
- `Location` serializes to protobuf and back → fields preserved.
  Unit test.
- `Vec<Location>` with 3 entries (multi-chunk object) → serialized
  and deserialized → 3 entries with correct `chunk_id`, `offset`,
  `length`, `logical_offset`, `logical_length`. Unit test.
- `Location` binary encoding is compact (< 64 bytes per location).
  Unit test (check serialized size).

**ChunkIoWriter interface**:
- A mock writer implementing `ChunkIoWriter` → `on_data` returns
  `FeedStatus::Continue`, `on_finish` returns `Vec<Location>`,
  `on_error` returns `Vec<Location>`. Unit test (verifies the trait
  is implementable and the contract is sound).
- `on_data` called after `on_finish` → `IoError::Finished`. Unit test.
- `on_finish` called twice → second call returns `IoError::Finished`.
  Unit test.
- `on_error` with no sealed chunks → `Ok(vec![])`, no cleanup calls.
  Unit test.

**LargeObjectWriter — size hint edge cases**:
- `object_size = Some(50 MB)` but stream yields only 30 MB → seals at
  30 MB, `Location.length = 30 MB` (hint is planning only). Integration
  test.
- `object_size = Some(30 MB)` but stream yields 50 MB → writer
  allocates beyond planned strip count, seals at 50 MB. Integration
  test.
- Object size exactly equals strip data capacity (4 MB with 4+1 EC) →
  1 full strip, no partial EC, `sealed_length = 4 MB`. Integration
  test.

**LargeObjectWriter — stream API, single chunk (E2E Case 1)**:
- Start 1 kv-server (group 0), 1 diskdb (5 disks), 1 chunkdb.
  `LargeObjectWriter::write_stream(Cursor(50 MB bytes),
  Some(50 MB))` with `EcScheme::new(4,1)`, `max_chunk_size = 1 GB`,
  `prealloc_depth = 2` → 13 EC strips (12 full + 1 partial 2 MB),
  chunk sealed, returns 1 `Location`: `{chunk_id, [0, 50MB),
  logical [0, 50MB)}`. E2E test (real servers, no mocks).
- `query_chunk(chunk_id)` → Sealed, 13 strips, strip 12
  `sealed_length = 2 MB`. E2E test.
- Data integrity: read back 50 MB via a test reader using the
  Location → identical bytes. E2E test.

**LargeObjectWriter — chunk rotation (E2E Case 2)**:
- Same topology. `write_stream(Cursor(100 MB bytes), Some(100 MB))`
  with `max_chunk_size = 8 MB` (2 strips/chunk), `prealloc_depth = 2`
  → 13 chunks allocated, 13 `Location` entries with `logical_offset`
  0, 8, 16, ..., 96 MB; `length` = 8 MB for entries 0–11, 4 MB for
  entry 12. E2E test (real servers).
- All 13 chunks Sealed via `query_chunk`; chunks 1–12 have 2 strips,
  chunk 13 has 1 strip. E2E test.
- Data integrity: read back 100 MB via the 13 Locations → identical
  bytes, contiguous (no gaps). E2E test.

**Pipeline concurrency**:
- Write a 50 MB object → verify the main write task writes block 1 of
  strip N+1 before strip N's parity task completes (instrument: data
  write of N+1 starts while parity task N is still running), and strip
  N+2's blocks are being fetched — three strips in flight
  simultaneously. Integration test (mock diskio with delay +
  mock chunkdb).
- Fetch stage reads ≤ `read_buffer_size` (1 MB) per `AsyncRead` call;
  each 1 MB block is sent to the main write task immediately (no wait
  for full strip). Integration test (use a stream that returns 512 KB
  per read, verify 2 reads per block, 4 blocks per strip, first disk
  write starts after 1 MB not 4 MB).

**Bounded preallocation**:
- Write a 50 MB object (13 strips, `prealloc_depth = 2`) → at most 2
  strips allocated ahead of the write cursor at any time; first chunk
  allocated with 1 strip (writing starts immediately). Integration
  test (verify `allocate_chunk` initial_strips=1, `append_chunk` call
  count = 12, never more than 2 strips in prealloc channel).
- Write a 100 MB object (25 strips, `max_chunk_size = 8 MB`,
  `prealloc_depth = 2`) → chunk prefetch allocates the next chunk
  before the current one is full. Integration test (verify
  `allocate_chunk` called for chunk N+1 before chunk N is sealed).
- Prefetch fell behind at rotation: delay `allocate_chunk` for the
  next chunk so it's not ready when rotation triggers → main write
  task blocks at rotation until the chunk arrives; no data lost,
  pipeline resumes. Integration test (inject allocation delay).

**Backpressure (disk slower than receive)**:
- `write_stream` with a slow diskio mock (10 ms per block write) and
  a fast stream (instant reads) → fetch stage blocks when
  `max_cached_buffer` (4 MB) is full; un-written data never exceeds
  4 MB. Integration test (verify fetch blocks, no data loss).

**Streaming (unknown size)**:
- `write_stream(Cursor(50 MB bytes), None)` → correct number of strips
  written, chunk sealed, `Location` correct. Integration test (stream
  source does not provide size; writer allocates strips on-demand).

**Partial strip**:
- Write a 2 MB object (strip capacity 4 MB for 4+1) → 1 partial strip,
  `sealed_length = 2 MB`, `Location.length = 2 MB`. Integration test.

**Error handling** (R94 basic — whole-strip retry for diskio write
failure; EC encode failure aborts; R110 refines both):
- `write_stream` with a diskio write failure on the 3rd strip → writer
  retries the whole strip with a new allocation (up to 3 retries). If
  all retries fail, returns `IoError`. Integration test (inject
  diskio error). R110's acceptance tests cover single-block
  replacement; R94 tests only the coarse retry path.
- `write_stream` with an EC encode failure (parity task) → pipeline
  aborts immediately, returns `IoError::EcEncodeFailed`, no retry.
  Integration test (inject EC encode error). R110 may refine to
  degraded-strip or fallback-encoder retry; R94 tests the abort path.
- `on_error` after 2 chunks are sealed → returns 2 `Location`s (the
  sealed chunks), frees the partial 3rd chunk. Integration test.
- `on_error` with no sealed chunks → returns `Ok(vec![])`, no cleanup
  calls. Integration test.
- Writer dropped without finishing → `Drop` impl aborts pipeline,
  frees partial chunks. Integration test (drop mid-write, verify
  partial chunk is deleted via `query_chunk`).

**High-parallel write / memory budget**:
- `WriterPool` with `memory_budget = 30 MB` (2 × 15 MB for 4+1 EC)
  → 2 concurrent `write_stream` calls succeed; a 3rd returns
  `IoError::MemoryBudgetExhausted` until one finishes. Integration
  test.
- Each writer's memory footprint is bounded regardless of object
  size: a 50 MB object and a 500 MB object use the same ~15 MB
  (verify via memory tracking or channel depth assertions).
  Integration test.

**Test commands**: `pixi run cargo test -p crow-chunk-client --test
large_object_writer` (unit + integration), `pixi run clean-env && pixi
run cargo test -p crow-chunk-client --test large_object_writer_e2e`
(E2E with real servers), `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Partial EC strip encoding**: When the last strip is not full
  (e.g. 2 MB of 4 MB capacity), how is the EC encoding handled?
  Chosen direction: isa-l supports partial EC — no padding needed.
  EC-encode only the written data blocks, release the empty data
  blocks (don't write them to disk), keep the code (parity) blocks,
  record `sealed_length` = 2 MB. The reader reads only 2 MB and
  ignores the released blocks. This must be verified with a unit
  test on isa-l EC first (`crow_common::ec::encode` with a partial
  data range), then used in the pipeline flow. If the UT shows isa-l
  does not support partial EC as expected, **stop and involve the
  user** — do not fall back to padding. The user has done partial EC
  with isa-l before and expects it to work; a negative UT result
  means something is wrong with the test or the isa-l binding, not
  that the approach is invalid.

- **Max chunk size vs KV value size** (resolved): The 1 GB default
  is driven by chunk metadata size in KV (each strip has 5 segment
  entries for 4+1 EC, each ~40 bytes → 200 bytes per strip → 250
  strips = 50 KB of strip metadata, plus chunk header). Decision:
  50 KB is acceptable — the btree page size is 64 KB, so chunk
  metadata fits in one page. The actual KV value size limit should
  still be verified against crow-kv's max value size config, but 50
  KB is well within expected limits. Compression of chunk metadata
  is future work (would allow larger chunks or more strips per
  chunk).

- **Memory peak accounting**: The corrected per-writer memory
  estimate (~15 MB for 4+1 EC, 1 MB blocks, default config) assumes
  both in-flight parity tasks can be in the EC compute phase
  simultaneously, each holding the strip's 4 data blocks via shared
  `Bytes` refs. In practice, EC compute (isa-l) is fast relative to
  disk write + fsync, so the two parity tasks may stagger (one
  computing while the other is in parity write/fsync, where data
  refs are already released). The realistic steady-state peak may
  be closer to ~11 MB (4 cache + 1 write + 4 parity data + 2 parity
  blocks). The design draft should confirm the exact accounting and
  whether `max_cached_buffer` should cover parity-held data too
  (making the flow-control limit match the memory limit). The
  `WriterPool` `memory_budget` calculation depends on this figure.
  Implementation direction: ref count on the data buffer —
  increment when submitting to the EC flow, decrement when the EC
  job finishes. Handle errors carefully: every error path must
  decrement the count (no leak on failure). A tracking mechanism
  that reports leak periods (or at shutdown) would catch missed
  decrements during development.

- **Unaligned `max_chunk_size`** (resolved): The writer does not set
  chunk size directly — it can only `append_chunk` (add strips) and
  observe the resulting chunk size. Chunk size is always a multiple
  of strip data capacity (sum of appended strips). Rotation happens
  after a full strip is written, never mid-strip. The
  `max_chunk_size` config is a threshold: when the current chunk's
  accumulated size reaches or exceeds it, the writer finishes the
  current strip, seals, and rotates. No mid-strip rotation logic is
  needed. The edge case in the Solution section is updated
  accordingly.

- **`ChunkIoWriter` `on_data` backpressure semantics** (resolved):
  The trait uses `FeedStatus` (not `bool`) for `on_data`'s return —
  a bare `bool` is ambiguous ("does true mean full or ready?"). The
  enum answers the question directly: "do you need more data?"

  ```rust
  pub enum FeedStatus {
      Continue,  // buffer stored, writer has capacity — send more
      Pause,     // buffer stored, writer at capacity — pause feeding
  }
  ```

  Contract:

  - `on_data(buf)` **always stores the buffer** (awaits until
    internal capacity is available — never rejects). Returns
    `Ok(FeedStatus::Continue)` if the writer has capacity for the
    next push; `Ok(FeedStatus::Pause)` if the writer is now at
    capacity (the next `on_data` will block). The buffer is never
    dropped or rejected.
  - `require_data()` is a non-async, cheap pre-check. Returns
    `true` if `on_data` would not block (writer has capacity now);
    `false` if `on_data` would block. Lets the caller decide: push
    anyway (block), or back off (yield / return 503).
  - `on_finish()` signals end of input; the writer flushes, seals,
    and returns `Vec<Location>`.
  - `on_error()` aborts; returns `Location`s of already-sealed
    chunks for caller cleanup.

  The key decision: `on_data` never rejects a buffer. If it
  returned `Pause` *without storing*, the caller would be stuck
  holding a `Bytes` it already constructed — every caller would
  need a retry loop with its own buffer storage. "Always store"
  pushes that complexity into the writer (one place) instead.

  The "two modes" from R106's doc ("returns `false` from
  `require_data()` or blocks (configurable)") are not two modes of
  `on_data` — they are two **caller strategies** for dealing with
  backpressure, selected by a `BackpressurePolicy` on the caller
  side:

  - **Blocking strategy** (dedicated upload task): ignore
    `require_data`, call `on_data` directly. It blocks until
    capacity. Fine when the task has nothing else to do.
    Example — R94 manual push:
    ```
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 { break; }
        let status = writer.on_data(Bytes::copy_from_slice(&buf[..n])).await?;
        // blocks here if max_cached_buffer (4 MB) is full
        // status is Continue or Pause — dedicated task ignores it
    }
    let locations = writer.on_finish().await?;
    ```

  - **Non-blocking strategy** (HTTP handler on shared tokio tasks):
    check `require_data()` first; if false, return 503 / apply TCP
    flow control — never block the handler task. Example — R106
    Axum handler:
    ```
    if !writer.require_data() {
        return Response::builder().status(503).body("retry later")?;
    }
    let status = writer.on_data(bytes).await?;
    // won't block long (we pre-checked); status tells us if next
    // request should also check require_data first
    if status == FeedStatus::Pause {
        // writer is full — next request should check require_data
    }
    ```

  The design draft should encode this contract in the trait's doc
  comments and add a `BackpressurePolicy` enum (`Blocking` /
  `NonBlocking`) on the caller side. Affects R106's caller
  integration.
