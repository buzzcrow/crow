<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-Layer Drive Loop + Own-Protobuf Refactor

A design draft for the second refactor of `crow-chunk-client`'s large-object
write path. The first refactor (landed, `plan-large-write-refactor.md`)
extracted OO stage classes but left the drive loop in the object layer and
transcribed protobuf responses into parallel Rust structs. This draft moves
the strip-level drive loop into `ChunkWriter` and eliminates the parallel
types by owning protobuf directly.

- Root design: `doc/design/chunkio/design-crow-chunkio.md` §3 (Write
  Flow), §5 (EC Integration), §6 (Chunk Rotation and Location).
- Prior design draft: `doc/working/large-write-review.md` (the OO
  refactor — landed).
- Architecture decisions and rationale (two trait seams, push vs stream
  driving modes, parity hand-off decoupling) are in the root design;
  this doc does not repeat them.
- Already landed: `LargeObjectWriter`, `LargeAsyncObjectWriter`,
  `ChunkWriter`, `ChunkPrefetch`, `EcStripWriter`, `EcWorker`,
  `ParityBatch`, `StripPlacement`, `StripWriter` enum, `Location`,
  `ChunkClientConfig`, `WriterPool`, `DiskWriter` trait,
  `DiskioBlockWriter`, `LocalFileDiskWriter`.

## 1. Why

### 1.1 The drive loop is in the wrong layer

The prior refactor (§2.7 of `large-write-review.md`) explicitly placed
the drive loop in `LargeObjectWriter`: "rotates chunks, fetches chunks
from `ChunkPrefetch`, coordinates EOF, and accumulates `Location`s."
`ChunkWriter` was a passive wrapper — "does NOT own the drive loop."

The result is triplicated strip-rotation + chunk-rotation logic across
three drive loops:

- `LargeObjectWriter::ensure_open_strip` + `on_data` —
  `large_object.rs:107-152`, `large_object.rs:213-216`.
- `LargeAsyncObjectWriter::on_data` (same `need_open` +
  `apply_placement` + `finish_strip`) — `large_async_object.rs:271-307`.
- `LargeAsyncObjectWriter::write_stream` drive loop —
  `large_async_object.rs:184-227`.

`ensure_open_strip` and `apply_placement` are near-identical
chunk-rotation logic. The `if is_strip_full { finish_strip }` pattern
appears 3×. Upcoming strip-level features (whole-strip retry per §7 of
the root design, `replace_block` per plan 2.3, partial last strip per
§3.1) would add more coordination logic to *both* writers' drive loops.

### 1.2 `StripPlacement` is lossy transcription of protobuf

`allocate_chunk` / `append_chunk` / `query_chunk` all return a full
`Chunk` protobuf (`chunkdb_type.proto:92`):

```protobuf
message Chunk {
  ChunkId id = 1;
  ChunkState state = 2;
  uint32 capacity = 5;
  uint32 sealed_length = 6;
  repeated ChunkStrip strips = 7;
  ChunkType chunk_type = 8;
}
```

`extract_placement_from_chunk` (`chunk_prefetch.rs:232-249`) transcribes
four fields out of this into a hand-rolled `StripPlacement`:

```rust
Ok(StripPlacement {
    chunk_id: chunk.id?,
    strip_index_in_chunk: strip_index,
    segments: ec.segments.clone(),
    unit_kb: strip.unit_kb,
})
```

This throws away `state`, `capacity`, `sealed_length`,
`strip_sequence`, `data_num`, `code_num`, `ec_state`, and all other
strips. `ChunkWriter` then manually tracks `bytes_in_chunk` and
`strips_in_chunk` — counters that already exist in the protobuf.

### 1.3 `Location` is the same pattern

`location.rs:19-25` is a 5-field struct identical to the protobuf
`Location` (`chunkdb_type.proto:121`). `to_proto` / `from_proto` is
pure transcription; `to_proto_bytes` / `from_proto_bytes` are trivial
prost wrappers. The only real logic is `to_bytes` / `from_bytes` (the
compact 48-byte big-endian encoding).

## 2. Solution

### 2.1 Drive loop relocation: object → chunk layer

The layering becomes:

- **Object layer** (`LargeObjectWriter`, `LargeAsyncObjectWriter`) —
  controls *chunks*: which chunk to write to, rotation between chunks,
  `Location` accumulation, block feeding. Does not see strips or
  placements.
- **Chunk layer** (`ChunkWriter`) — controls *strips* within one chunk:
  append strips, rotate strips, manage the write cursor, own the
  prefetch buffer. Does not see chunk rotation or `Location` arrays.
- **Strip layer** (`EcStripWriter`) — controls *blocks*: write data
  blocks, compute + write parity, fsync. Owns the `ChunkStrip`
  protobuf.

`ChunkWriter` is per-chunk. The object layer creates a new one on each
chunk rotation. The object layer's rotation logic is now trivial:
"if `ChunkWriter::is_full()`, seal → record `Location` → open new
`ChunkWriter`." No placement stream, no `chunk_id` comparison, no
strip-level coordination.

### 2.2 `ChunkWriter` owns the `Chunk` protobuf

```rust
pub struct ChunkWriter {
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    ec_scheme: EcScheme,
    config: Arc<ChunkClientConfig>,
    chunk: Option<Arc<Chunk>>,         // shared with EcStripWriter, ref-counted
    write_cursor: u32,                 // index of next strip to write (into chunk.strips)
    bytes_in_chunk: u64,               // derived from sealed strips; chunk.sealed_length is authoritative at seal
    current_strip: Option<EcStripWriter>,
    parity_handles: Vec<JoinHandle<Result<()>>>,  // accumulated parity tasks, joined at seal
    // strip prefetch (internal — see §2.4)
    object_size: Option<u64>,          // total object size, for strip prefetch planning
    strips_remaining: Option<usize>,   // strips not yet allocated (known-size objects)
    prefetch_handle: Option<JoinHandle<()>>,
    prefetch_rx: Option<mpsc::Receiver<Result<Chunk>>>,
}
```

- `open(chunk, object_size)` — wrap the `Chunk` protobuf from an
  `allocate_chunk` response in `Arc`, store it. `write_cursor = 0`.
  The `Chunk` already has 1 strip (from `allocate_chunk` with
  `strip_count=1`) — open it immediately, no strip-prefetch wait.
  Compute `strips_remaining` from `object_size` if known (total
  strips for the object minus strips already in this chunk + prior
  chunks). Start the internal strip prefetch task (§2.4) with the
  planning info.
- `push(buffer)` — write to `current_strip`. If the strip is full,
  finish it (spawns parity tasks, returns handles — no join),
  collect `parity_handles`, advance `write_cursor`, open the next
  strip (from `chunk.strips[write_cursor]` if pre-appended, or via
  `append_chunk` if none available — see §2.4). Strip N+1's data
  writes overlap with strip N's parity writes + fsyncs.
- `is_full()` — `bytes_in_chunk >= max_chunk_size` (chunk rotation
  threshold; the object layer checks this after each push).
- `seal()` — join all `parity_handles` (wait for all in-flight
  parity writes + fsyncs to complete), then `seal_chunk` RPC,
  return `ProtoLocation`. Parity durability is guaranteed at seal
  time, not at strip finish.
- `abort()` — cancel in-flight parity tasks + strip prefetch,
  `delete_chunk` RPC.

`chunk.strips.len() - write_cursor` is the number of pre-appended
strips available. No separate `VecDeque<StripPlacement>` — the
`Chunk` protobuf *is* the strip buffer.

`object_size` enables smarter strip prefetch: for a known-size
object, the prefetch task plans total strips upfront and stops
pre-appending when enough are allocated (no over-allocation). For
unknown-size objects (`object_size = None`), the prefetch task
pre-appends indefinitely up to `max_chunk_size`, same as today.

### 2.3 `EcStripWriter` shares `Arc<Chunk>` + strip index

```rust
pub struct EcStripWriter {
    chunk: Arc<Chunk>,                 // shared with ChunkWriter, ref-counted
    strip_index: u32,                  // index into chunk.strips
    disk_writer: Arc<dyn DiskWriter>,
    ec_worker: EcWorker,
    ec_scheme: EcScheme,
    next_block: usize,
    data_blocks_written: u32,
    bytes_written: u64,
    partial: bool,
    finished: bool,
}
```

`finish()` calls `parity_writer::spawn_parity_writes(...)` and
returns `StripResult { parity_handles: Vec<JoinHandle<Result<()>>> }`
**without joining**. The write cursor advances to the next strip
immediately — strip N+1's data writes overlap with strip N's parity
writes + fsyncs. This matches the root design (§3): "hand off to
background parity task, record parity task handle, immediately
advance to strip N+1, block 0 (no wait for parity)."

`ChunkWriter` collects `parity_handles` from each finished strip.
`ChunkWriter::seal()` joins all accumulated handles before calling
`seal_chunk` — parity durability is guaranteed at seal time, not at
strip finish. The root design (§6): "the main write task joins
in-flight parity tasks for the current chunk, calls `seal_chunk`."

`parity_depth` (default 2) bounds in-flight parity tasks. When the
parity pool is full, `finish_strip()` blocks at hand-off —
backpressure on the write path, decoupled from the fetch cache
(root design §4).

`EcStripWriter` does not clone the `ChunkStrip`. It holds `Arc<Chunk>`
+ `strip_index`, reading `self.chunk.strips[self.strip_index]` for
segment data. `ChunkWriter` owns the same `Arc<Chunk>`; both share
the protobuf by ref count.

No clone is needed because:

- `EcStripWriter` is owned by `ChunkWriter` and never outlives it.
  A `&ChunkStrip` borrow would be the natural fit, but Rust's borrow
  checker rejects it: `EcStripWriter` is a field of `ChunkWriter`, so
  borrowing from `ChunkWriter.chunk` into `EcStripWriter` is a
  self-referential struct. `Arc<Chunk>` breaks the cycle —
  `EcStripWriter` owns its own `Arc`, not a `&` into `ChunkWriter`.
- The strip-prefetch task replaces `self.chunk` with a new `Chunk`
  (from `append_chunk` response) when it appends a strip. A `&` into
  the old `Chunk` would dangle. `Arc<Chunk>` is safe: the old `Arc`
  in `EcStripWriter` keeps the old `Chunk` alive until the strip
  finishes, then drops naturally. The old and new `Chunk` both
  contain the current strip's segments (the response is cumulative).
- `EcStripWriter::finish` spawns `tokio::spawn` parity tasks that
  need segment data across await points. A `&ChunkStrip` can't cross
  `tokio::spawn` (lifetime). The spawned tasks clone the `Arc<Chunk>`
  (atomic increment, ~1ns) and read segments from it — no data clone.

The `StripPlacement` helper methods (`unit_bytes`, `segment(i)`,
`disk_id(i)`, `zone_offset(i)`) move onto `EcStripWriter` as private
methods, reading from `self.chunk.strips[self.strip_index]`. They're
accessors into the protobuf, no parallel data.

`StripPlacement` is deleted. `StripResult` stays (it's the
chunk↔strip response, not a protobuf mirror).

### 2.3a `parity_writer.rs` — parity spawn helper

The parity spawn logic (parallel parity writes + deduplicated fsyncs)
is extracted from `EcStripWriter::finish()` into a free function in
`chunk/parity_writer.rs`. This separates the parity write
orchestration from the strip writer's data-path logic.

```rust
/// Spawn parity write + fsync tasks for a finished strip.
/// Returns JoinHandles without joining — caller joins at seal time.
pub fn spawn_parity_writes(
    chunk: &Arc<Chunk>,
    strip_index: u32,
    parity_shards: Vec<Vec<u8>>,
    disk_writer: &Arc<dyn DiskWriter>,
    ec_scheme: &EcScheme,
) -> Result<Vec<JoinHandle<Result<()>>>>
```

The function:
1. For each parity shard `i`, spawn a `tokio::spawn` task that
   writes the shard to segment `data_num + i` via `DiskWriter::write`.
2. Spawn deduplicated `fsync` tasks (one per unique `disk_id` in the
   strip's segments, same as current `EcStripWriter::finish`).
3. Return all `JoinHandle`s — no join.

`ParityBatch` is deleted. The old `parity_batch.rs` module becomes
`parity_writer.rs` with this single free function. The batch-join
semantics are gone — `ChunkWriter` owns the `Vec<JoinHandle>` and
joins at `seal()`.

### 2.4 Prefetch restructure

Prefetch splits into two levels:

**Strip prefetch (inside `ChunkWriter`):** a background task appends
strips to the current chunk ahead of the write cursor, bounded by
`prealloc_depth`. The `append_chunk` response replaces
`self.chunk` with a new `Arc<Chunk>` (it's cumulative — has all
strips including the new ones). The old `Arc<Chunk>` in any in-flight
`EcStripWriter` stays alive until that strip finishes. `write_cursor`
lags behind `chunk.strips.len()`.

The prefetch task uses `object_size` (passed to `open`) for planning:

- **Known size** (`object_size = Some(n)`) — compute
  `total_strips = n.div_ceil(strip_data_capacity)`. The prefetch
  task knows how many strips this chunk needs (clamped to
  `strips_per_chunk = max_chunk_size / strip_data_capacity`) and
  stops pre-appending when `chunk.strips.len() >= min(total_strips,
  strips_per_chunk)`. No over-allocation — the last chunk of a
  known-size object gets exactly the strips it needs.
- **Unknown size** (`object_size = None`) — the prefetch task
  pre-appends indefinitely up to `strips_per_chunk`, same as today.
  On EOF, the object layer seals the partial chunk.

```
ChunkWriter internals:
    chunk: Option<Arc<Chunk>>      // cumulative protobuf, Arc-swapped on each append
    write_cursor: u32              // lags behind chunk.strips.len()
    object_size: Option<u64>       // planning input
    strips_remaining: Option<usize> // strips not yet allocated (known-size)
    prefetch_handle: JoinHandle<()>  // background append task

    open(chunk, object_size):
        self.chunk = Arc::new(chunk)  // already has 1 strip from allocate_chunk
        self.write_cursor = 0
        self.object_size = object_size
        self.strips_remaining = compute_remaining(object_size, ...)
        start strip prefetch task  // uses object_size + strips_remaining

    push(buffer):
        if current_strip full → finish_strip(), write_cursor += 1
        if current_strip none → next_strip()
            // fast path: chunk.strips[write_cursor] exists (pre-appended)
            // slow path: append_chunk RPC (prefetch fell behind)
        current_strip.push(buffer)

    strip prefetch task:
        loop:
            if strips_remaining == Some(0) → stop (known-size, all allocated)
            if chunk.strips.len() >= strips_per_chunk → stop (chunk full)
            if write_cursor + prealloc_depth <= chunk.strips.len() → wait
            new_chunk = append_chunk(strip_count=1 or batch).await?
            chunk = Arc::new(new_chunk)  // Arc-swap
            strips_remaining -= 1 (if known)
```

When `write_cursor + prealloc_depth > chunk.strips.len()`, the
prefetch task appends more strips. When the chunk reaches
`max_chunk_size` worth of strips (or `strips_remaining` hits 0 for
known-size), the prefetch task stops — the object layer will rotate
or finish.

**Chunk prefetch (object layer):** the object layer pre-allocates the
next chunk when the current one is near full, so rotation doesn't
stall on `allocate_chunk`. This is a small mechanism — a single
`Option<Chunk>` buffered ahead, filled by a background task or
on-demand. The object layer's rotation becomes:

```
if current_chunk.is_full():
    location = current_chunk.seal().await?
    locations.push(location)
    current_chunk = ChunkWriter::open(next_chunk)  // from buffer or on-demand
```

### 2.5 `Location` → `ProtoLocation`

Delete `location.rs`. Use `crow_protocol::chunkdb::rpc::Location`
(`ProtoLocation`) directly throughout. The compact 48-byte encoding
becomes free functions in `crow-protocol` or `crow-chunk-client`:

```rust
pub fn location_to_bytes(loc: &ProtoLocation) -> Vec<u8>;
pub fn location_from_bytes(data: &[u8]) -> Result<ProtoLocation, &'static str>;
```

`ChunkIoWriter::on_finish` returns `Vec<ProtoLocation>`. The
`to_proto_bytes` / `from_proto_bytes` calls in tests become
`loc.encode_to_vec()` / `ProtoLocation::decode(data)` — prost's
`Message` trait already provides these.

`ProtoLocation.chunk_id` is `Option<ChunkId>` (proto3 message fields
are always optional). The current hand-rolled `Location` has
`chunk_id: ChunkId` (non-optional) — a real invariant. Consumers do
`.chunk_id.unwrap_or_default()`, the standard prost pattern. If the
non-optional invariant matters, a thin newtype wrapper avoids field
duplication:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location(pub ProtoLocation);
```

This is a 1-field wrapper, not a 5-field parallel struct. The
constructor enforces non-optional `chunk_id`; the compact encoding
lives on the wrapper. **Decision: use `ProtoLocation` directly** —
the `Option<ChunkId>` is already the norm in the protobuf layer, and
adding a wrapper just to enforce one invariant is more glue than it
saves. Revisit if consumers accumulate `unwrap_or_default` bugs.

### 2.6 `ChunkPrefetch` repurposed: chunk-level prefetch

`ChunkPrefetch` currently streams `StripPlacement` values to the
object layer. With the strip-level drive loop moved into `ChunkWriter`,
strip prefetch becomes internal to `ChunkWriter`. `ChunkPrefetch` is
**not deleted** — it is repurposed to prefetch *chunks* for the object
layer.

The split:

- **Strip prefetch** — moves *inside* `ChunkWriter` as an internal
  background task. Appends strips to the current chunk ahead of the
  write cursor. No public API; `ChunkWriter` owns it. The
  `append_strip` free function moves into `ChunkWriter` (or private
  `chunk/alloc.rs`).
- **Chunk prefetch** — `ChunkPrefetch` stays at the object layer.
  Pre-allocates the next `Chunk` (via `allocate_chunk` RPC) ahead of
  rotation, so `ChunkWriter::open` doesn't block waiting for
  `allocate_chunk`. Its API changes from streaming `StripPlacement`
  to providing pre-allocated `Chunk` values:

```rust
pub struct ChunkPrefetch {
    allocator: Arc<dyn ChunkAllocator>,
    ec_scheme: EcScheme,
    config: Arc<ChunkClientConfig>,
    chunk_type_byte: u8,
}

impl ChunkPrefetch {
    pub fn new(allocator, ec_scheme, config, chunk_type_byte) -> Self;

    /// Spawn the chunk-prefetch task. Returns a receiver of
    /// pre-allocated `Chunk` values + join handle. Each `Chunk` has
    /// 1 strip (the minimum to start writing); `ChunkWriter`'s
    /// internal strip prefetch appends more as needed.
    pub fn spawn(self)
        -> (mpsc::Receiver<Result<Chunk>>, JoinHandle<()>);

    /// On-demand chunk allocation when the prefetch task has
    /// finished but more data remains. Called by the object layer
    /// when the prefetch receiver is exhausted.
    pub async fn on_demand(&self) -> Result<Chunk>;
}
```

The `allocate_new_chunk` free function stays in `chunk_prefetch.rs`
(used by `ChunkPrefetch` and potentially by `ChunkWriter` for the
first chunk). The `append_strip` free function moves to
`chunk_writer.rs` (or private `chunk/alloc.rs`) — it's a
`ChunkAllocator` call that `ChunkWriter`'s internal strip prefetch
uses.

The object layer holds `ChunkPrefetch` + its receiver. On chunk
rotation, it pulls the next `Chunk` from the receiver (fast path —
pre-allocated) or calls `on_demand` (slow path — prefetch fell
behind). The `Chunk` is passed to `ChunkWriter::open`, which wraps it
in `Arc` and starts the internal strip prefetch.

### 2.7 Object layer shrinks

`LargeObjectWriter` and `LargeAsyncObjectWriter` lose:
- `ensure_open_strip`, `next_placement`, `apply_placement`,
  `on_demand_placement` (moved into `ChunkWriter`)
- Strip-level coordination (`if is_strip_full { finish_strip }`) —
  now internal to `ChunkWriter::push`.

They keep:
- `chunk_writer: Option<ChunkWriter>` — the current chunk's writer.
  Interaction is just `push` / `is_full` / `seal` / `abort`.
- `chunk_prefetch: Option<ChunkPrefetch>` — pre-allocates the next
  `Chunk` so rotation doesn't block on `allocate_chunk`.
- `chunk_prefetch_rx: Option<mpsc::Receiver<Result<Chunk>>>` —
  receiver for pre-allocated chunks.
- `chunk_prefetch_handle: Option<JoinHandle<()>>` — prefetch task
  handle (aborted on finish/error).
- `object_size: Option<u64>` — total object size, passed through to
  `ChunkWriter::open` for strip prefetch planning. Set on the first
  `on_data` (if the caller provides it) or on `write_stream` entry.
- `locations: Vec<ProtoLocation>` — accumulated across chunks.
- `logical_offset: u64` — for `Location` assembly.
- Chunk rotation logic (seal + pull next `Chunk` from prefetch +
  open new `ChunkWriter` with remaining `object_size` info).
- Block feeding (from `Iterator<Bytes>` or `AsyncRead` + fetch stage).

The `on_data` / `on_finish` / `on_error` / `require_data` trait
methods stay. `on_data` becomes: ensure `ChunkWriter` is open (first
push — pull from chunk prefetch, pass `object_size`), `push(buffer)`,
check `is_full` → rotate (seal + pull next chunk + open with
remaining size). No strip-level logic, no `StripPlacement`.

## 3. Updated Write Flow

### 3.1 Non-blocking stream mode

```
LargeObjectWriter::on_data(buffer)
  │
  a. If chunk_writer is None → open first chunk:
  │     chunk = chunk_prefetch_rx.recv().await?  // pre-allocated, no block
  │     chunk_writer = ChunkWriter::open(chunk, object_size)
  │       // chunk already has 1 strip (from allocate_chunk) — write immediately
  │       // starts internal strip prefetch with object_size for planning
  │
  b. chunk_writer.push(buffer)
  │     └─ EcStripWriter::push(buffer):
  │          • DiskWriter::write(data block)
  │          • EcWorker::push(&buffer) — streaming compute
  │     └─ if strip full → finish_strip():
  │          • spawn parity writes + fsyncs (background, no join)
  │          • collect parity_handles
  │          • advance write_cursor, open next strip immediately
  │          • strip N+1 data writes overlap with strip N parity writes
  │
  c. If chunk_writer.is_full() → rotate:
  │     location = chunk_writer.seal().await?
  │       └─ join all parity_handles (wait for in-flight parity + fsyncs)
  │       └─ seal_chunk RPC
  │     locations.push(location with logical_offset)
  │     logical_offset += location.length
  │     chunk = chunk_prefetch_rx.recv().await?  // pre-allocated, no block
  │     remaining = object_size.map(|s| s.saturating_sub(logical_offset))
  │     chunk_writer = ChunkWriter::open(chunk, remaining)
  │       // strip prefetch plans for the remaining bytes, not the whole object
  │
  d. Return FeedStatus (Continue if chunk_writer.ready(), else Pause)

LargeObjectWriter::on_finish()
  │
  e. Finish current strip if partial (spawn partial parity, no join).
  f. location = chunk_writer.seal().await?
  │     └─ join all parity_handles (wait for all in-flight parity + fsyncs)
  │     └─ seal_chunk RPC
  g. locations.push(location)
  h. Abort chunk-prefetch task + strip-prefetch task inside chunk_writer.
  i. Return locations.
```

### 3.2 Async stream mode

Same as §3.1, but `on_data` receives blocks from the fetch stage
(`run_fetch_stage` pulls from `AsyncRead`, sends `Bytes` on a channel).
The drive loop is the same — `push` to `ChunkWriter`, check `is_full`,
rotate. Backpressure: if `chunk_writer` is not `ready()`, the fetch
stage's channel fills (`max_cached_buffer`), throttling the stream.

The `write_stream` method runs fetch + drive loop concurrently via
`tokio::join!`, same as today — but the drive loop body is the
simpler §3.1 flow, not the current placement-stream flow.

### 3.3 Chunk rotation

Chunk rotation is now object-layer, but trivial:

```
if chunk_writer.is_full():
    location = chunk_writer.seal().await?
    locations.push(ProtoLocation {
        chunk_id: location.chunk_id,
        offset: 0,
        length: location.length,
        logical_offset: self.logical_offset,
        logical_length: location.length,
    })
    self.logical_offset += location.length
    // Pull next chunk from ChunkPrefetch (pre-allocated, no block).
    // If prefetch exhausted, on-demand allocate.
    next_chunk = chunk_prefetch_rx.recv().await?
                 .or_else(|| chunk_prefetch.on_demand().await)
    // Pass remaining object size for strip prefetch planning.
    remaining = self.object_size.map(|s| s.saturating_sub(self.logical_offset))
    chunk_writer = ChunkWriter::open(next_chunk, remaining)
```

No `chunk_id` comparison, no placement stream, no `need_rotate`
arithmetic. `is_full()` is a single boolean check. The pre-allocated
`Chunk` from `ChunkPrefetch` means rotation doesn't stall on
`allocate_chunk` RPC — the next chunk is already waiting in the
receiver.

### 3.4 Strip rotation (inside `ChunkWriter`)

```
ChunkWriter::push(buffer)
  │
  a. If current_strip is full or none → next_strip():
  │     if write_cursor < chunk.strips.len():
  │       current_strip = EcStripWriter::new(Arc::clone(&chunk), write_cursor, ...)
  │     else:
  │       // Prefetch fell behind — append on-demand.
  │       new_chunk = Arc::new(allocator.append_chunk(...).await?)
  │       chunk = new_chunk  // Arc-swap; old Arc in any in-flight strip stays alive
  │       current_strip = EcStripWriter::new(Arc::clone(&chunk), write_cursor, ...)
  │
  b. current_strip.push(buffer)
  │     └─ DiskWriter::write + EcWorker::push (streaming compute)
  │
  c. If current_strip.is_full() → finish_strip():
  │     strip_result = current_strip.finish().await?
  │       └─ EcWorker::finish() → parity shards
  │       └─ tokio::spawn() → parallel parity writes + fsyncs (background)
  │       └─ return immediately with parity_handles (NO join)
  │     parity_handles.extend(strip_result.parity_handles)
  │     bytes_in_chunk += strip_result.bytes_written
  │     write_cursor += 1
  │     current_strip = None  // opened on next push
  │     // Strip N+1's data writes overlap with strip N's parity writes + fsyncs
  │
  d. Return FeedStatus

ChunkWriter::seal()
  │
  │  join all parity_handles  // wait for all in-flight parity + fsyncs
  │  seal_chunk RPC           // parity durability guaranteed before seal
  │  return ProtoLocation
```

## 4. Scope

### `lib/crow-chunk-client/src/`

- `chunk/chunk_writer.rs` — **major rewrite.** Owns `Arc<Chunk>`,
  strip-level drive loop, strip prefetch (internal background task).
  Gains `is_full()`, `push` (auto-rotates strips), `parity_handles`
  collection. `seal()` joins all parity handles before `seal_chunk`
  RPC. Loses `open(StripPlacement)` / `continue_strip` /
  `append_strip` as public API (internal now).
- `chunk/ec_strip_writer.rs` — **modified.** Holds `Arc<Chunk>` +
  `strip_index` instead of `StripPlacement`. `finish()` calls
  `parity_writer::spawn_parity_writes` and returns
  `StripResult { parity_handles }` **without joining** — write
  cursor advances immediately. Helper methods (`unit_bytes`,
  `segment(i)`, `disk_id(i)`, `zone_offset(i)`) move here as
  private methods reading from `self.chunk.strips[self.strip_index]`.
  `ParityBatch` is removed (no join at strip finish); parity spawn
  logic moves to `parity_writer::spawn_parity_writes`. Parity tasks
  are bare `JoinHandle`s collected by `ChunkWriter`.
- `chunk/strip.rs` — **slimmed.** `StripPlacement` removed.
  `StripWriter` enum + `StripResult` stay in `strip.rs`.
- `chunk/chunk_prefetch.rs` — **repurposed.** Streams `Chunk` values
  (not `StripPlacement`) for the object layer's chunk rotation.
  `allocate_new_chunk` stays here. `append_strip` moves to
  `chunk_writer.rs` (or private `chunk/alloc.rs`) — used by
  `ChunkWriter`'s internal strip prefetch.
- `chunk/chunk.rs` — update re-exports. Remove `StripPlacement`,
  `ParityBatch`. Keep `ChunkWriter`, `ChunkPrefetch`,
  `EcStripWriter`, `StripWriter`, `StripResult`. `parity_writer`
  stays a `pub mod` (free function, no struct to re-export).
- `writer/large_object.rs` — **major rewrite.** Lose
  `ensure_open_strip`, `next_placement`, `apply_placement`,
  `start_pipeline`. Keep `chunk_prefetch` + `chunk_prefetch_rx` +
  `chunk_prefetch_handle` (now receiving `Chunk`, not
  `StripPlacement`). `on_data` becomes: ensure `ChunkWriter` open
  (pull from chunk prefetch), `push`, check `is_full` → rotate.
- `writer/large_async_object.rs` — **major rewrite.** Same
  simplification as `large_object.rs`. `write_stream` drive loop
  uses the simpler §3.1 flow. `on_demand_placement`,
  `apply_placement`, `receive_and_push` removed or simplified.
- `location.rs` — **deleted.** Use `ProtoLocation` directly. Compact
  encoding functions (`location_to_bytes` / `location_from_bytes`)
  move to `crow-protocol` (decision: §9 — `Location` is a protocol
  type, the encoding is a protocol concern, and it is passed over
  RPC by other services).
- `lib.rs` — update re-exports. Remove `Location`, `StripPlacement`.
  Keep `ChunkPrefetch`. Re-export `ProtoLocation` from
  `crow-protocol`.
- `io.rs` — `ChunkIoWriter::on_finish` / `on_error` return
  `Vec<ProtoLocation>` instead of `Vec<Location>`.

### `lib/crow-protocol/`

- Compact `Location` encoding functions (`location_to_bytes`,
  `location_from_bytes`) — new, if placed here. Or keep in
  `crow-chunk-client`.

### Tests

- `tests/large_object_writer.rs` — replace `Location` with
  `ProtoLocation`. Remove `StripPlacement` references. Update
  `LargeObjectWriter` construction.
- `tests/write_stream.rs` — same migration. Update
  `LargeAsyncObjectWriter` usage.
- `tests/large_object_writer_e2e.rs` — same migration.
- `tests/common/mod.rs` — no change to `LocalFileDiskWriter` (it
  implements `DiskWriter`, unaffected).
- `tests/strip_test.rs` — **deleted** (tests `StripPlacement`
  methods, which move to `EcStripWriter` private methods). Replace
  with `EcStripWriter` accessor tests if needed.
- `tests/ec_worker_test.rs` — no change (`EcWorker` unchanged).
- `tests/parity_batch_test.rs` — renamed to `parity_writer_test.rs`.
  Tests `spawn_parity_writes`: verify parallel write + fsync spawn,
  deduplicated fsyncs, no join (returns handles).

## 5. Complexity

**Medium.** The drive-loop relocation is the hard part — moving
strip-rotation + prefetch logic from two object-layer writers into
`ChunkWriter` without breaking the 53 existing tests. The own-
protobuf changes are mechanical (delete parallel types, use protobuf
directly). No new external dependencies, no new RPCs, no protocol
changes. The main challenge is test migration: tests currently
construct `StripPlacement` and `Location` directly; they'll need to
construct `Chunk` / `ChunkStrip` / `ProtoLocation` instead, which is
more verbose but more honest.

## 6. Test Design

### Unit tests

- **`EcStripWriter` with owned `ChunkStrip`** — construct an
  `EcStripWriter` from a `ChunkStrip` protobuf, push N data blocks,
  finish, verify parity shards match `encode_parity_from_shards`.
  Verify `segment(i)` / `disk_id(i)` / `zone_offset(i)` accessors
  return correct values from the protobuf. Replaces the deleted
  `strip_test.rs`.
- **`ChunkWriter` strip rotation** — open a `ChunkWriter` with a
  `Chunk` containing 3 pre-appended strips. Push `data_num * 3`
  blocks. Verify 3 strips written, `bytes_in_chunk` correct,
  `write_cursor == 3`. Uses `LocalFileDiskWriter` +
  `MockChunkAllocator`.
- **`ChunkWriter` on-demand append** — open with a `Chunk` containing
  1 strip. Push `data_num * 2` blocks. Verify `append_chunk` RPC
  called once (second strip appended on-demand). Verify data on disk
  matches.
- **`ChunkWriter` is_full + seal** — push enough data to exceed
  `max_chunk_size`. Verify `is_full()` returns true. `seal()` →
  verify `seal_chunk` RPC called with correct `sealed_length`. Verify
  returned `ProtoLocation` fields.
- **`ChunkWriter` abort** — push partial data, `abort()`. Verify
  `delete_chunk` RPC called. Verify in-flight strip writes cancelled.
- **`ChunkWriter` empty chunk seal** — open, immediately `seal()`
  with no pushes. Verify `delete_chunk` called (empty chunk), not
  `seal_chunk`.

### Integration tests (existing, migrated)

- `tests/large_object_writer.rs` — Location → ProtoLocation, remove
  StripPlacement. Verify push-mode write + Location array.
- `tests/write_stream.rs` — same migration. Verify stream-mode write,
  chunk rotation, data reconstruction.
- `tests/large_object_writer_e2e.rs` — same migration. Verify E2E
  with real diskio/chunkdb.

### E2E tests

- **Multi-chunk object** — write an object larger than
  `max_chunk_size`. Verify N `ProtoLocation`s, contiguous
  `logical_offset` ordering, data reconstructs correctly across
  chunks.
- **Object with known size** — `write_stream` with `object_size`
  known. Verify prefetch plans correctly, no over-allocation.
- **Object with unknown size** — `write_stream` with
  `object_size = None`. Verify on-demand strip allocation works when
  prefetch exhausts.
- **Parity overlap** — write a multi-strip object. Verify that
  strip N+1's data writes start before strip N's parity writes +
  fsyncs complete (parity is background, joined at seal). Verify
  data reconstructs correctly (parity was eventually written +
  fsynced before seal).

## 7. Module Structure (changes from current)

```
lib/crow-chunk-client/src/
  lib.rs                  updated re-exports (remove Location, StripPlacement;
                          keep ChunkPrefetch; add ProtoLocation re-export)
  io.rs                   ChunkIoWriter returns Vec<ProtoLocation>
  error.rs                unchanged
  config.rs               unchanged
  location.rs             DELETED (use ProtoLocation; compact encoding
                          moves to crow-protocol or location_encoding.rs)

  chunk/                  ── chunk + strip layer (NOW owns strip-level drive loop)
    chunk.rs              updated re-exports (remove StripPlacement; keep ChunkPrefetch)
    chunk_writer.rs       MAJOR REWRITE — owns Arc<Chunk>, strip drive loop,
                          strip prefetch (internal); is_full/push/seal/abort API
    ec_strip_writer.rs    MODIFIED — holds Arc<Chunk> + strip_index, accessor methods
    chunk_prefetch.rs     REPURPOSED — streams Chunk (not StripPlacement) for
                          object-layer chunk rotation; allocate_new_chunk stays;
                          append_strip moves to chunk_writer.rs
    parity_writer.rs      RENAMED from parity_batch.rs — spawn_parity_writes
                          free function (spawn writes + fsyncs, return handles,
                          no join); ParityBatch struct deleted
    strip.rs              SLIMMED — StripWriter enum + StripResult only (no
                          StripPlacement); stays in strip.rs (§9 decision)
    mirror_strip_writer.rs unchanged (stub)
    chunk_reader.rs       unchanged (R107 placeholder)
    strip_reader.rs       unchanged (R107 placeholder)

  writer/                 ── object-level layer (chunk rotation + block feeding)
    large_object.rs       MAJOR REWRITE — loses strip/placement logic; keeps
                          ChunkPrefetch (now chunk-level) + chunk rotation
    large_async_object.rs MAJOR REWRITE — same simplification
    small_object.rs       unchanged (stub)
    pool.rs               unchanged (constructs LargeObjectWriter)
    fetch.rs              unchanged (run_fetch_stage free function)

  worker/                 ── unchanged
    ec_worker.rs          unchanged
    hash_worker.rs        unchanged (stub)

  disk_io/                ── unchanged
    disk_writer.rs        unchanged

  traits.rs               unchanged (ChunkAllocator trait)
```

## 8. Config Extensions

None. `ChunkClientConfig` fields are unchanged. `prealloc_depth` is
now consumed inside `ChunkWriter` (strip prefetch). `chunk_prefetch_depth`
is consumed by `ChunkPrefetch` (chunk-level prefetch, same as before
but now prefetching `Chunk` values instead of `StripPlacement`).

## 9. Open Questions

- **`StripWriter` enum + `StripResult` home.** With `strip.rs`
  slimmed (no `StripPlacement`), the enum + result can stay in
  `strip.rs` or fold into `chunk.rs`. Minor — decide during
  implementation.
  ai-todo: stay in strip.rs

- **Compact `Location` encoding placement.** `crow-protocol` (shared,
  usable by other crates) vs `crow-chunk-client` (local, only the
  data path uses it). Lean: `crow-protocol`, since `Location` is a
  protocol type and the encoding is a protocol concern. Decide
  during implementation.
ai-todo : to crow-protocol, all protobuf and flat buffer will in crow-protocol. The location will be used by different serices, need pass by RPC