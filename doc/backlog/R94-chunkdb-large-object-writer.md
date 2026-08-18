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
with 1 MB blocks) need a dedicated chunk per object — sharing a chunk
across multiple large objects would require complex offset management
and GC. A dedicated chunk maps one object to one chunk (or a chain of
chunks for very large objects), with direct EC strip writes. The
chunkdb design §15 Future Work lists "Specific chunk type (direct EC
write for large objects)" — this is that requirement.

Additionally, there is no shared chunk IO async interface or `Location`
type. Both the large-object writer (R94) and the small-object writer
(R106) need a common interface for feeding data into a chunk and a
common `Location` type for recording where object data landed. R94
defines these; R106 and R107 (read flow) depend on them.

**Current behavior + impact**: No object writer exists. No `Location`
type. No chunk IO async interface. The chunkdb client
(`lib/crow-chunkdb-client/`) has chunk lifecycle RPCs (allocate/seal/
delete/query) but no data-path API. R106 (small object writer) and
R107 (read flow) cannot be built without the interface and `Location`
type defined here.

**Design pointers**: chunkdb root design §5.3 (Chunk — container for
strips), §5.5 (Chunk types — "Specific" for large objects), §8
(Allocation Flow), §9 (Chunk Lifecycle — Active → Sealed), §10.6
(`append_chunk` RPC — dynamically add strips to an Active chunk),
§11 (EC Encoding — isa-l encode at strip level). The writer uses
chunkdb's `allocate_chunk` to get a chunk, `append_chunk` to add
strips dynamically, and `seal_chunk` when the object is fully written.
Data I/O goes through diskio (R105).

**Use scenarios**:

- **Single-chunk large object**: A 50 MB object is uploaded. The
  writer allocates a specific chunk (EC 8+4, 8 MB data capacity per
  strip), writes 7 EC strips (56 MB capacity, 50 MB used), seals the
  chunk. Returns a single `Location` array with one entry:
  `{chunk_id, [0, 50MB)}`. Expected: the object is durably stored
  across 7 EC strips, readable via R107.

- **Multi-chunk very large object**: A 2.5 GB object is uploaded.
  Max chunk size is ~1 GB (configurable, limited by chunk metadata
  size in KV). The writer allocates chunk 1, fills it to ~1 GB (125
  EC strips), rotates to chunk 2, fills to ~1 GB, rotates to chunk 3
  for the remaining ~0.5 GB. Returns a `Location` array with 3
  entries: `{chunk1, [0, 1GB)}`, `{chunk2, [0, 1GB)}`,
  `{chunk3, [0, 0.5GB)}`. Expected: the object spans 3 chunks, each
  sealed independently, readable via R107 as a contiguous object.

- **Dynamic strip addition**: The writer is mid-write on a chunk and
  the current strip is full. The writer calls `append_chunk` to add a
  new EC strip, then continues writing to the new strip. The object
  size was known upfront (50 MB), so the writer pre-calculated 7
  strips and pre-allocated them via a producer-consumer pipeline.
  Expected: no write stall while waiting for strip allocation — the
  next strip is already allocated before the current one is full.

- **Object size known upfront**: The caller provides the object size
  before the first `on_data` call. The writer calculates the strip
  count (ceil(size / strip_data_capacity)) and pre-allocates strips
  in parallel. Expected: all strips are allocated by the time data
  arrives, no allocation stall on the write path.

- **Object size unknown (streaming)**: The caller does not provide
  the object size. The writer allocates strips on-demand as data
  fills the current strip. Expected: correct behavior with potential
  allocation stalls (acceptable for streaming uploads).

- **Write error mid-object**: The diskio write to the 3rd EC strip
  fails (disk error). The writer retries the write to a re-allocated
  strip (via `append_chunk` with a new placement). If retries are
  exhausted, the writer returns an error to the caller with the
  partial `Location` array (chunks written so far) for cleanup.
  Expected: no data loss for successfully written strips; the caller
  can abort and free the partial chunks.

**Solution**

A large-object writer in the chunkdb client library that writes one
object to one or more dedicated chunks (specific chunk type), using
EC strips directly. The writer uses a producer-consumer pattern to
pre-allocate strips ahead of the write cursor. It defines the shared
`ChunkIoWriter` async interface and the `Location` type that R106
(small-object writer) and R107 (read flow) also use.

**One-line summary**: A large-object writer using dedicated chunks with
direct EC strips, producer-consumer strip preallocation, chunk rotation
at ~1 GB, defining the shared `ChunkIoWriter` interface and `Location`
type for all chunk data-path components.

**Numbered work items**:

1. **`Location` type** (`lib/crow-chunkdb-client/src/location.rs`) —
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

2. **`ChunkIoWriter` async interface**
   (`lib/crow-chunkdb-client/src/io.rs`) — the shared trait for all
   chunk data-path writers:
   ```rust
   #[async_trait]
   pub trait ChunkIoWriter: Send {
       /// Feed a data buffer to the writer. Returns true if more
       /// data is needed, false if the writer is done or full.
       async fn on_data(&mut self, buffer: Bytes) -> Result<bool, IoError>;

       /// Called when all data has been fed. The writer flushes
       /// pending writes, seals chunks, and returns the final
       /// Location array.
       async fn on_finish(&mut self) -> Result<Vec<Location>, IoError>;

       /// Called on error. The writer aborts, frees partial chunks,
       /// and returns any Locations that were already durably written.
       async fn on_error(&mut self) -> Result<Vec<Location>, IoError>;

       /// Whether the writer needs more data before it can proceed.
       /// Used for backpressure — returns false when internal buffers
       /// are full.
       fn require_data(&self) -> bool;
   }
   ```
   This is the interface that R106 (small-object writer) also
   implements and that R107 (read flow) uses to understand the
   `Location` format. The interface is intentionally simple — no
   async servlet flow, no Jetty, no HTTP. The caller (a future object
   store HTTP handler, or a test) drives the writer by calling
   `on_data` in a loop.

3. **`LargeObjectWriter`**
   (`lib/crow-chunkdb-client/src/writer/large_object.rs`) —
   implements `ChunkIoWriter` for large objects. Constructor takes
   `object_size: Option<u64>` (None for streaming), `ec_scheme:
   EcScheme` (e.g. 8+4), `max_chunk_size: u64` (default 1 GB). On
   construction:
   - If `object_size` is known: calculate `strip_count = ceil(size /
     strip_data_capacity)` and `chunk_count = ceil(total_strip_data /
     max_chunk_size)`. Pre-allocate the first chunk's initial strips
     via `chunkdb_client::allocate_chunk`.
   - If `object_size` is None: allocate one chunk with one strip,
     add strips on-demand.
   - Start a producer-consumer strip preallocation pipeline: a
     background task calls `append_chunk` to allocate the next strip
     before the write cursor reaches it. The pipeline depth is
     configurable (default 2 strips ahead).

4. **Strip write logic** (`writer/large_object.rs`) — `on_data`
   accumulates data into the current strip's buffer. When the buffer
   reaches `strip_data_capacity` (e.g. 8 MB for 8+4):
   - EC-encode the data via `crow-common` isa-l (8 data blocks + 4
     parity blocks).
   - Write the 12 blocks to the strip's segments via
     `DiskIoClient::write` (R105), in parallel.
   - `fsync` all 12 disks.
   - Advance to the next pre-allocated strip.
   - If the chunk is full (reached `max_chunk_size`): seal the chunk,
     allocate a new chunk, start a new preallocation pipeline.
   Return `true` if more data is needed.

5. **Chunk rotation** (`writer/large_object.rs`) — when the current
   chunk reaches `max_chunk_size` (default 1 GB, limited by chunk
   metadata size in KV — a chunk with 125 EC strips has 125 × 12 =
   1500 segment entries, which is near the KV value size limit):
   - Seal the current chunk via `chunkdb_client::seal_chunk`.
   - Record the `Location` for the filled chunk: `{chunk_id, [0,
     written_bytes), logical_offset, logical_length}`.
   - Allocate a new chunk via `chunkdb_client::allocate_chunk`.
   - Start a new preallocation pipeline for the new chunk.
   The `Location` array accumulates one entry per rotated chunk.

6. **Completion + seal** (`writer/large_object.rs`) — `on_finish`:
   - Flush any partial strip data (if the last strip is not full,
     EC-encode with padding or write as a partial EC strip — the
     strip's `sealed_length` records the actual data length).
   - `fsync` all disks.
   - Seal the current chunk.
   - Return the `Location` array.
   `on_error`:
   - Abort in-flight writes.
   - Free partial chunks via `chunkdb_client::delete_chunk` (or
     `delete_chunk_range` for partial chunks).
   - Return `Location`s that were already durably written (sealed
     chunks) for caller cleanup.

7. **Chunk prefetch** (`lib/crow-chunkdb-client/src/prefetch.rs`) —
   a `ChunkPrefetch` helper that pre-allocates chunks in the
   background. The large-object writer uses it to have the next chunk
   ready before the current one is full. The small-object writer
   (R106) also uses it to pre-allocate shared chunks. Configurable
   prefetch depth (default 1 chunk ahead). Uses
   `chunkdb_client::allocate_chunk` in a background `tokio::spawn`
   task with a `oneshot` channel to deliver the allocated chunk.

**Flow diagram**:

```
Caller                    LargeObjectWriter              chunkdb        diskio (R105)
  │                            │                           │                │
  │ new(object_size=50MB,      │                           │                │
  │     ec=8+4, max_chunk=1GB) │                           │                │
  │───────────────────────────►│                           │                │
  │                            │ calc: 7 strips, 1 chunk   │                │
  │                            │ allocate_chunk(EC 8+4)    │                │
  │                            │──────────────────────────►│                │
  │                            │◄──────────────────────────│                │
  │                            │ start strip preallocation │                │
  │                            │ (pipeline depth=2)        │                │
  │                            │                           │                │
  │ on_data(8MB)               │                           │                │
  │───────────────────────────►│                           │                │
  │                            │ fill strip 0 (8MB)        │                │
  │                            │ EC encode → 12 blocks     │                │
  │                            │ write 12 blocks (parallel)│                │
  │                            │──────────────────────────────────────────►│
  │                            │◄──────────────────────────────────────────│
  │                            │ fsync 12 disks            │                │
  │                            │──────────────────────────────────────────►│
  │                            │◄──────────────────────────────────────────│
  │                            │ advance to strip 1        │                │
  │                            │ (pre-allocated)           │                │
  │                            │                           │                │
  │ on_data(8MB) ...           │                           │                │
  │───────────────────────────►│ (repeat for strips 1-6)  │                │
  │                            │                           │                │
  │ on_data(2MB)               │                           │                │
  │───────────────────────────►│ fill strip 6 (partial)   │                │
  │                            │ EC encode (padded)        │                │
  │                            │ write + fsync             │                │
  │                            │                           │                │
  │ on_finish()                │                           │                │
  │───────────────────────────►│ seal_chunk                │                │
  │                            │──────────────────────────►│                │
  │                            │◄──────────────────────────│                │
  │ ◄──────────────────────────│                           │                │
  │ Vec<Location> = [          │                           │                │
  │   {chunk_id, [0, 50MB),    │                           │                │
  │    logical [0, 50MB)}      │                           │                │
  │ ]                          │                           │                │
```

**Edge cases at a glance**:

- Object size unknown (streaming) → strips allocated on-demand;
  potential allocation stall when the current strip fills and the
  next is not yet allocated. Acceptable for streaming; the
  preallocation pipeline still runs but starts late.
- Object size < strip data capacity (e.g. 5 MB with 8 MB strips) →
  one partial strip, EC-encoded with padding, `sealed_length` = 5 MB.
  The chunk has one strip.
- Object size exactly equals strip data capacity → one full strip,
  no padding.
- Chunk rotation mid-strip → the current strip is sealed as a partial
  strip (with `sealed_length`), the chunk is sealed, a new chunk is
  allocated, and the remaining data starts in a new strip in the new
  chunk. The `Location` array has two entries with contiguous
  `logical_offset` ranges.
- EC encode failure → the strip write fails; the writer retries with
  a new strip allocation (different placement). If retries (default
  3) are exhausted, `on_data` returns `IoError::EcEncodeFailed`.
- diskio write failure on one of 12 EC blocks → the strip write fails;
  retry with a new strip allocation. The partially written blocks on
  the failed strip are freed via `update_chunk_strip` (replace with
  the new strip).
- Caller drops the writer without `on_finish` or `on_error` → the
  `Drop` impl calls `on_error` (abort + free partial chunks). This
  is a safety net; the caller should call `on_error` explicitly.
- Compression (future) → when compression is added, the object size
  is not known upfront (compressed size differs from input size).
  The writer falls back to the streaming path (on-demand strip
  allocation). Not implemented in R94; noted for future.

**Dependencies**

- **Depends on**: **R105** (disk IO engine) — writes EC blocks via
  `DiskIoClient`. **chunkdb** (landed, R85) — `allocate_chunk`,
  `append_chunk`, `seal_chunk`, `update_chunk_strip` RPCs.
  **crow-common EC** (landed) — isa-l encode.
- **Depended on by**:
  - **R106** (small object writer) — uses the `ChunkIoWriter` trait
    and `Location` type defined here.
  - **R107** (chunk read flow) — uses the `Location` type to locate
    and read object data.

**Acceptance**

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
  `true`, `on_finish` returns `Vec<Location>`, `on_error` returns
  `Vec<Location>`. Unit test (verifies the trait is implementable and
  the contract is sound).

**LargeObjectWriter — single chunk**:
- Write a 50 MB object with known size (8+4 EC, 1 MB blocks) → 7 EC
  strips written, chunk sealed, `on_finish` returns 1 `Location`:
  `{chunk_id, [0, 50MB), logical [0, 50MB)}`. Integration test (mock
  diskio + mock chunkdb).
- Data integrity: read back the 50 MB via R107 (or a test reader)
  → identical bytes. Integration test.

**LargeObjectWriter — multi-chunk**:
- Write a 2.5 GB object (max_chunk_size = 1 GB) → 3 chunks allocated,
  3 `Location` entries with `logical_offset` 0, 1GB, 2GB. Integration
  test.
- Data integrity across 3 chunks: read back 2.5 GB → identical bytes.
  Integration test.

**Dynamic strip addition**:
- Write a 50 MB object → `append_chunk` is called to add strips
  dynamically; the preallocation pipeline has the next strip ready
  before the current one is full (no write stall). Integration test
  (verify `append_chunk` call count = 6, one per additional strip
  beyond the initial allocation).

**Streaming (unknown size)**:
- Write a 50 MB object with `object_size = None` → correct number of
  strips written, chunk sealed, `Location` correct. Integration test.

**Partial strip**:
- Write a 5 MB object (strip capacity 8 MB) → 1 partial strip,
  `sealed_length` = 5 MB, `Location.length` = 5 MB. Integration test.

**Error handling**:
- `on_data` with a diskio write failure on the 3rd strip → writer
  retries with a new strip (up to 3 retries). If all retries fail,
  returns `IoError`. Integration test (inject diskio error).
- `on_error` after 2 chunks are sealed → returns 2 `Location`s (the
  sealed chunks), frees the partial 3rd chunk. Integration test.
- Writer dropped without `on_finish`/`on_error` → `Drop` impl frees
  partial chunks. Integration test (drop mid-write, verify partial
  chunk is deleted via `query_chunk`).

**Chunk prefetch**:
- `ChunkPrefetch` with depth 1 → the next chunk is allocated before
  the current one is full. Integration test (verify `allocate_chunk`
  is called ahead of time).

**Test commands**: `pixi run cargo test -p crow-chunkdb-client --test
large_object_writer`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Partial EC strip encoding**: When the last strip is not full
  (e.g. 5 MB of 8 MB capacity), how is the EC encoding handled? Options:
  (a) pad with zeros to full capacity, EC-encode the full 8 MB,
  record `sealed_length` = 5 MB — the reader reads only 5 MB and
  ignores padding; (b) EC-encode only the 5 MB with a smaller
  `data_num` (e.g. 5 data blocks instead of 8) — requires variable
  EC schemes per strip. Option (a) is simpler and preserves the EC
  scheme; (b) is more space-efficient but complex. Current design
  assumes (a). Confirm.

- **Max chunk size vs KV value size**: The 1 GB default is driven by
  chunk metadata size in KV (each strip has 12 segment entries for
  8+4 EC, each ~40 bytes → 480 bytes per strip → 125 strips = 60 KB
  of strip metadata, plus chunk header). The actual KV value size
  limit should be verified against crow-kv's max value size config.
  If the limit is lower, the default `max_chunk_size` must be lower
  (e.g. 512 MB for 60 strips = ~29 KB metadata). Needs measurement.
