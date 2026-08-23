<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R106: chunkdb — Small Object Shared Chunk Writer

**Problem**

Small objects (size < EC strip data capacity, e.g. < 8 MB for 8+4 EC)
are too small to justify a dedicated chunk. Allocating one chunk per
16 KB object would waste 99.8% of the chunk's strip capacity and
overwhelm chunkdb with chunk metadata. Small objects must share
chunks — multiple objects packed into the same chunk, each recording
its `Location` (chunk_id + offset range) for later retrieval.

The challenge is throughput + aggregation. A naive shared-chunk writer
that writes each object to diskio individually would have terrible
I/O efficiency: 16 KB writes to `O_DIRECT` disks are sector-aligned
but waste I/O bandwidth on small random writes. The writer needs to
aggregate multiple small objects into batch writes (patch writes to a
shared chunk buffer) to maximize bandwidth and TPS.

Additionally, small-object workloads are bursty and variable. A fixed
number of writer pipelines cannot adapt: too few pipelines under high
load starve throughput; too many under low load waste threads and
fragment aggregation. The pipeline count must scale dynamically with
load.

**Current behavior + impact**: No small-object writer exists. The
`ChunkIoWriter` interface and `Location` type are defined in R94 but
no shared-chunk implementation exists. Without R106, small objects
cannot be stored efficiently — the only option is the large-object
writer (R94) with one chunk per object, which is wasteful for small
objects.

**Design pointers**: chunkdb root design §5.5 (Chunk types — "Shared"
for small objects), §5.2 (Strip — mirror vs EC), §10.6 (`append_chunk`
— add strips to an Active chunk). R94 defines the `ChunkIoWriter`
trait and `Location` type. R93 defines the mirror→EC conversion that
R106 depends on (write 3 mirrors first, convert to EC in background).
The reference architecture is the reference's `SharedObjWriter` +
`Write2M1ECChunkHandler` (producer-consumer, shared chunk rotation,
2-mirror-1-EC write strategy).

**Use scenarios**:

- **16 KB object upload (high TPS)**: 1000 concurrent 16 KB object
  uploads arrive. Each is routed to a pipeline (round-robin or
  least-loaded). The pipeline's worker thread aggregates multiple 16
  KB buffers into a batch, writes the batch to the shared chunk's
  mirror strips via diskio, and returns `Location` to each caller.
  Expected: high TPS via aggregation — 1000 small writes become ~10
  batched mirror writes.

- **Mixed small object sizes**: Objects of 4 KB, 16 KB, 64 KB, and
  256 KB arrive concurrently. All are < 8 MB (EC strip threshold) so
  all go to shared chunks. The pipeline aggregates them into batches
  regardless of individual size, packing them into the shared chunk
  buffer. Expected: each object gets a `Location` with its correct
  offset and length within the chunk.

- **Mirror-first write + return**: A 16 KB object is written to 3
  mirror strips (3 diskio writes to 3 nodes). The writer returns
  `Location` to the caller after the 3 mirror writes complete (fast
  — 3 parallel writes, no EC encode on the critical path). In the
  background, when enough mirror strips are written, the conversion
  task (R93) converts them to EC strips. Expected: low write latency
  (3 mirror writes, no EC encode wait); storage overhead is 3×
  temporarily, dropping to 1.5× after background conversion.

- **Dynamic pipeline scale-out**: Load increases from 100 to 5000
  concurrent uploads. The pipeline manager detects queue depth
  growing and adds more pipelines (worker threads + shared chunks).
  Expected: throughput scales with pipeline count until disk
  bandwidth is saturated; no manual tuning.

- **Dynamic pipeline scale-in**: Load drops from 5000 to 100. The
  pipeline manager detects idle pipelines and removes them, draining
  their shared chunks (seal + hand off to conversion). Expected:
  resource usage drops; no wasted threads.

- **Shared chunk rotation**: A shared chunk (256 MB) is full. The
  pipeline seals it and starts writing to a new pre-allocated shared
  chunk. The sealed chunk is handed to the conversion task (R93) for
  mirror→EC. Expected: no write stall during rotation — the new
  chunk is pre-allocated via `ChunkPrefetch` (R94).

- **Chunk full mid-batch**: The pipeline is writing a batch of 20
  objects when the shared chunk runs out of space after the 15th
  object. The pipeline seals the chunk, rotates to the pre-allocated
  chunk, and writes the remaining 5 objects to the new chunk.
  Expected: the batch completes with `Location`s spanning two chunks;
  no data loss or retry needed.

**Solution**

A small-object shared chunk writer in the chunkdb client library that
implements `ChunkIoWriter` (R94) using a dynamic pool of write
pipelines. Each pipeline has a worker thread that fetches queued
object buffers and writes them in batches to a shared chunk (256 MB,
pre-allocated). Writes go to 3 mirror strips first (fast return), with
background mirror→EC conversion (R93). The pipeline count scales
dynamically based on queue depth to maximize bandwidth and aggregation.

**One-line summary**: A dynamic-pool small-object writer with per-
pipeline worker threads, batch aggregation into 256 MB shared chunks,
mirror-first writes with background EC conversion, and automatic
pipeline scale in/out for max BW + TPS.

**Numbered work items**:

1. **`SmallObjectWriter`**
   (`lib/crow-chunk-client/src/writer/small_object.rs`) — implements
   `ChunkIoWriter` (R94). The entry point for small-object writes.
   Constructor takes `ec_scheme`, `mirror_copy_count` (default 3),
   `shared_chunk_size` (default 256 MB), `pipeline_config`. The
   writer does not own a chunk directly — it routes each `on_data`
   call to a pipeline managed by the `PipelineManager`. Each object
   gets a `PendingWrite` handle that is resolved with a `Location`
   when the pipeline's worker writes the batch containing that
   object's data.

2. **`PipelineManager`**
   (`lib/crow-chunk-client/src/writer/pipeline.rs`) — manages a
   dynamic pool of `WritePipeline`s. Each pipeline has:
   - An inbound queue (`tokio::sync::mpsc` or a lock-free MPSC) of
     `PendingWrite` entries (object data + completion `oneshot`).
   - A worker task (async, not OS thread — runs on the tokio runtime)
     that drains the queue, aggregates buffers into a batch, and
     writes the batch to the pipeline's shared chunk.
   - A shared chunk (pre-allocated via `ChunkPrefetch`, R94) with a
     current write offset.
   The manager routes new objects to pipelines via round-robin or
   least-loaded (shortest queue) selection.

3. **Dynamic scaling** (`writer/pipeline.rs`) — the manager monitors
   each pipeline's queue depth and adjusts the pool size:
   - **Scale-out**: If total queue depth across all pipelines exceeds
     `scale_out_threshold` (default 100 pending writes) for
     `scale_out_interval` (default 1 second), add a new pipeline
     (spawn a worker task, allocate a shared chunk via prefetch).
   - **Scale-in**: If a pipeline's queue depth is 0 for
     `scale_in_idle_secs` (default 30 seconds), drain it (seal the
     shared chunk, hand to R93 conversion), remove the pipeline.
   - **Min/max bounds**: `min_pipelines` (default 1), `max_pipelines`
     (default = number of disks × 2, or configurable). The manager
     never goes below min or above max.
   - **Scaling metric**: queue depth is the primary metric; disk
     bandwidth utilization (from diskio metrics) is a secondary
     guard — do not scale out if disks are saturated (would just add
     queue contention without throughput gain).

4. **Batch aggregation** (`writer/pipeline.rs`) — the worker thread
   drains the inbound queue in batches:
   - Collect up to `max_batch_objects` (default 64) or
     `max_batch_bytes` (default 1 MB) from the queue, whichever is
     reached first. Wait up to `batch_timeout` (default 1 ms) for
     more objects to fill the batch.
   - Pack the objects into the shared chunk's buffer at consecutive
     offsets. Each object records its `[offset, offset+length)` in
     the chunk.
   - If the shared chunk doesn't have enough space for the full
     batch: write what fits, seal the chunk, rotate to the pre-
     allocated chunk, write the rest.
   - Write the batch to 3 mirror strips via diskio (R105): 3 parallel
     `DiskIoClient::write` calls to 3 nodes, then `fsync` all 3.
   - Resolve each object's `oneshot` with its `Location`.

5. **Mirror-first write strategy** (`writer/pipeline.rs`) — the
   worker writes to 3 mirror strips (not EC) on the write path:
   - Allocate 3 mirror strip blocks via chunkdb `append_chunk` (or
     pre-allocated as part of the shared chunk's strip pool).
   - Write the batch data to all 3 mirror replicas in parallel via
     diskio.
   - `fsync` all 3 disks.
   - Return success to all objects in the batch (resolve `oneshot`s).
   - The mirror strips are marked as "pending EC conversion" in the
     chunk metadata. The background conversion task (R93) picks them
     up and converts to EC strips when the chunk is sealed or when a
     strip's offset range is fully written.

6. **Shared chunk lifecycle** (`writer/pipeline.rs`) — each pipeline
   owns one active shared chunk at a time:
   - **Allocation**: pre-allocated via `ChunkPrefetch` (R94) with
     mirror strips (3 copies, 1 MB blocks). The chunk starts with a
     configurable number of mirror strips (default 4 strips = 4 MB
     initial capacity). New mirror strips are appended via
     `append_chunk` as the chunk fills.
   - **Write**: objects are packed at consecutive offsets. The
     current write offset is tracked atomically (the worker is
     single-threaded per pipeline, so no lock needed).
   - **Rotation**: when the chunk reaches `shared_chunk_size` (256
     MB), seal it via `chunkdb_client::seal_chunk`, hand it to the
     R93 conversion task, and switch to the pre-allocated next
     chunk.
   - **Pre-allocation**: `ChunkPrefetch` keeps 1 chunk ahead (R94's
     prefetch helper, configured for shared chunks with mirror
     strips).

7. **Backpressure** (`writer/pipeline.rs`) — if all pipelines' queues
   are full (each queue has a configurable capacity, default 256
   pending writes), `require_data()` returns `false` and `on_data`
   returns `FeedStatus::Pause`. The caller (object store HTTP
   handler) applies backpressure to the upstream (e.g. HTTP 503 or
   TCP flow control) — see R94's `BackpressurePolicy` (blocking vs
   non-blocking caller strategies). This prevents unbounded memory
   growth under extreme load. When queues drain, `require_data()`
   returns `true` and `on_data` returns `FeedStatus::Continue`
   again.

8. **Metrics** (`lib/crow-chunk-client/src/writer/metrics.rs`) —
   `WriterMetrics` with counters: `objects_written`,
   `bytes_written`, `batches_submitted`, `avg_batch_size`,
   `pipelines_active`, `pipelines_scaled_out`,
   `pipelines_scaled_in`, `chunks_sealed`, `mirror_writes`,
   `mirror_write_latency` (histogram), `queue_depth` (gauge per
   pipeline). Exposed via the chunkdb-client's metrics endpoint.

**Flow diagram**:

```
Caller A (16KB) ─┐
Caller B (16KB) ─┤        SmallObjectWriter        PipelineManager
Caller C (64KB) ─┼──► on_data() ──► route ──► ┌──► Pipeline 1 ──┐
Caller D (4KB) ──┤                          │   (queue + worker)│
Caller E (16KB) ─┘                          │                   │
                                            │   Worker:         │
                                            │   drain queue     │
                                            │   aggregate batch │
                                            │   [A,B,C,D,E]     │
                                            │   pack into chunk │
                                            │   write 3 mirrors │
                                            │   ┌──────┬──────┐ │
                                            │   │mirror│mirror│ │
                                            │   │  1   │  2   │ │
                                            │   └──────┴──────┘ │
                                            │   fsync all 3     │
                                            │   resolve oneshots│
                                            │   ┌──────────────┐│
                                            │   │Location[A]   ││
                                            │   │Location[B]   ││
                                            │   │Location[C]   ││
                                            │   │Location[D]   ││
                                            │   │Location[E]   ││
                                            │   └──────────────┘│
                                            │                   │
                                            ├──► Pipeline 2 ──┐ │
                                            │   (queue + worker)│
                                            │                   │
                                            └──► Pipeline N     │
                                                (dynamic)       │
                                                    │           │
                                          scale out │           │ scale in
                                          (depth>100)│           │ (idle 30s)
                                                    ▼           ▼
                                            ┌─────────────────────┐
                                            │ Shared chunk (256MB)│
                                            │ mirror strips       │
                                            │ ──► sealed ──► R93  │
                                            │     (mirror→EC)     │
                                            └─────────────────────┘
```

**Edge cases at a glance**:

- Object larger than `max_batch_bytes` (e.g. 2 MB with 1 MB batch
  limit) → written as its own batch (no aggregation); still goes to
  mirror strips. The batch size limit is a soft target, not a hard
  cap.
- Object larger than EC strip threshold (8 MB) → the writer rejects
  it with `IoError::ObjectTooLarge` — the caller should use the
  large-object writer (R94) instead. The `SmallObjectWriter`
  constructor takes an `object_size_threshold` for this check.
- Shared chunk runs out of strip space mid-batch → the batch is split
  across two chunks; objects in the first chunk get `Location`s in
  chunk 1, objects in the second get `Location`s in chunk 2. No
  retry needed.
- Mirror write failure (one of 3 replicas fails) → the worker retries
  the failed replica with a new block allocation (via `append_chunk`
  with a new placement). If all 3 fail, the batch fails and all
  objects' `oneshot`s resolve with `IoError`.
- Pipeline worker panic → the `PipelineManager` detects the worker
  task's `JoinError`, reassigns the queue's pending writes to other
  pipelines, and replaces the pipeline.
- Scale-out during burst → new pipeline starts with a pre-allocated
  chunk from `ChunkPrefetch`; if the prefetch is not ready, the new
  pipeline waits for allocation (brief stall, acceptable during
  scale-out).
- Scale-in during drain → the pipeline stops accepting new writes,
  finishes its current batch, seals the chunk, and exits. In-flight
  `oneshot`s are resolved before exit.
- `on_error` (caller aborts) → the writer resolves all pending
  `oneshot`s with `IoError::Aborted`; the shared chunks are not
  freed (other objects' data is in them) — only the aborted object's
  range is marked for GC (future, R92).

**Dependencies**

- **Depends on**: **R94** (large object writer) — uses the
  `ChunkIoWriter` trait, `Location` type, and `ChunkPrefetch` helper.
  **R93** (mirror→EC conversion) — the sealed shared chunks are
  handed to R93 for background conversion. R106 can land before R93
  is fully implemented (mirror strips are correct, just space-
  inefficient); R93 is the long-term efficiency mechanism.
  **R105** (disk IO engine) — writes mirror blocks via
  `DiskIoClient`. **chunkdb** (landed, R85) — `allocate_chunk`,
  `append_chunk`, `seal_chunk`.
- **Depended on by**: nothing (terminal data-path component). R107
  (read flow) reads the `Location`s that R106 produces but does not
  depend on R106's implementation — only on the `Location` type (R94).

**Acceptance**

**Shared chunk write**:
- Write 100 × 16 KB objects via `SmallObjectWriter` → all 100
  `on_finish` calls return a `Location` with correct `chunk_id`,
  `offset`, `length`. Integration test (mock diskio + chunkdb).
- Data integrity: read back each object via R107 (or test reader)
  using its `Location` → identical bytes. Integration test.

**Batch aggregation**:
- Write 100 × 16 KB objects → the number of diskio write calls is
  ≤ ceil(100 × 16 KB / 1 MB) = 2 batches (assuming 1 MB batch size),
  not 100 individual writes. Integration test (count diskio write
  calls).
- Batch with mixed sizes (4 KB + 16 KB + 64 KB + 256 KB) → all
  objects packed into one batch, one mirror write. Integration test.

**Mirror-first write**:
- `on_finish` for a 16 KB object returns after 3 mirror writes
  complete (no EC encode on the critical path). Integration test
  (verify no EC encode call during `on_finish`).
- After `on_finish`, the mirror strips are marked "pending EC
  conversion" in chunk metadata. Integration test (query chunk,
  verify strip state).

**Dynamic scaling**:
- Start with 1 pipeline, submit 500 concurrent objects → pipeline
  count increases (scale-out). Integration test (verify
  `pipelines_active` > 1 after burst).
- After burst, wait 30 seconds → pipeline count decreases (scale-in).
  Integration test (verify `pipelines_active` returns to min).
- `max_pipelines` is respected — never exceed the configured max.
  Integration test (submit extreme load, verify pipeline count ≤ max).
- Scale-out does not trigger when disk bandwidth is saturated
  (diskio reports high utilization). Integration test (mock diskio
  with slow writes, verify pipelines don't scale beyond disk
  capacity).

**Chunk rotation**:
- Write enough objects to fill a 256 MB shared chunk → chunk is
  sealed, new chunk is allocated, writes continue without stall.
  Integration test (verify `chunks_sealed` = 1, writes continue).
- Pre-allocated chunk is ready before rotation (no allocation stall).
  Integration test (verify `allocate_chunk` is called ahead of time
  via `ChunkPrefetch`).

**Chunk full mid-batch**:
- Batch of 20 objects, chunk runs out of space after 15th → 15
  `Location`s in chunk 1, 5 `Location`s in chunk 2, all 20
  `on_finish` calls succeed. Integration test.

**Backpressure**:
- All pipelines' queues full → `require_data()` returns `false`,
  `on_data` returns `FeedStatus::Pause`. Integration test (fill
  queues, verify backpressure signal).
- After queues drain → `require_data()` returns `true`, `on_data`
  returns `FeedStatus::Continue`. Integration test.

**Error handling**:
- One mirror replica write fails → worker retries with new block.
  Integration test (inject diskio error on one replica).
- All 3 mirror writes fail → batch fails, all objects' `on_finish`
  return `IoError`. Integration test.
- Pipeline worker panic → pending writes reassigned to other
  pipelines, all `on_finish` eventually succeed. Integration test
  (inject worker panic).
- Object > `object_size_threshold` (8 MB) → `on_data` returns
  `IoError::ObjectTooLarge`. Unit test.

**Metrics**:
- After writing 1000 objects in batches of ~10 → `objects_written` =
  1000, `batches_submitted` ≈ 100, `avg_batch_size` ≈ 10.
  Integration test (verify metrics snapshot).

**Test commands**: `pixi run cargo test -p crow-chunkdb-client --test
small_object_writer`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Worker thread vs async task**: The reference uses OS
  threads for pipeline workers. In Rust/tokio, an async task on the
  runtime is cheaper and integrates with the async diskio client
  (R105). But a dedicated thread avoids tokio runtime contention
  under high load. Trade-off: async task (cheaper, integrates with
  async diskio, but contends for runtime threads) vs dedicated
  thread (isolated, but needs a bridge to async diskio via
  `spawn_blocking` or a channel). Current design assumes async task.
  Needs profiling under load to decide.

- **Mirror strip pre-allocation vs on-demand**: Should the shared
  chunk pre-allocate all mirror strips up to 256 MB (256 mirror
  strips × 1 MB × 3 replicas = 768 MB of disk space reserved) or
  allocate strips on-demand as the chunk fills (less reservation, but
  allocation stalls possible)? Pre-allocation wastes space if the
  chunk is not filled; on-demand risks stalls. Hybrid: pre-allocate
  a small batch (4 strips = 4 MB) and append more as needed, with a
  background preallocation task staying ahead. Current design uses
  the hybrid approach. Confirm.

- **Conversion trigger for active chunks**: R93's conversion policy
  defaults to converting sealed chunks. But shared chunks may stay
  Active for a long time (continuously receiving writes). Should
  R93 also convert fully-written mirror strips in Active chunks
  (strip-level conversion, not chunk-level)? This is noted in R93's
  use scenarios but the policy needs to be coordinated between R93
  and R106. Decision: R93 converts at strip level for Active chunks
  (a strip whose offset range is fully written is eligible), and at
  chunk level for Sealed chunks. Confirm this is the right split.

---

<!-- Reference implementation details: see ~/.codeium/windsurf/memories/global_rules.md -->
