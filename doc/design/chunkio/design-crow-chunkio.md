<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: Chunk IO Data Path (Overview)

The chunk IO data path is the client-side layer that writes and reads
object data as EC-encoded strips across diskio servers, using chunkdb
for chunk lifecycle management (allocate, append, seal, delete). It
lives in the `crow-chunk-client` crate and is consumed by object store
layers and application upload handlers. The chunkdb server design
(chunk lifecycle, placement, EC integration) is in
[`doc/design/chunkdb/design-crow-chunkdb.md`](../chunkdb/design-crow-chunkdb.md);
the diskio block IO engine is in
[`doc/design/diskio/design-crow-diskio.md`](../diskio/design-crow-diskio.md).
This doc does not repeat their architecture — it covers the data path
that sits between them: the write pipeline, its backpressure and memory
model, and the design choices that make a 1 TB upload cost the same
~15 MB of RAM as a 50 MB one.

## Table of Contents

- [1. Non-Goals](#1-non-goals)
- [2. Key Design Decisions](#2-key-design-decisions)
- [3. Write Flow](#3-write-flow)
- [4. Backpressure and Memory Budget](#4-backpressure-and-memory-budget)
- [5. EC Integration](#5-ec-integration)
- [6. Chunk Rotation and Location](#6-chunk-rotation-and-location)
- [7. Completion and Error Handling](#7-completion-and-error-handling)
- [8. Interaction with Neighbors](#8-interaction-with-neighbors)
- [9. Tunables and Defaults](#9-tunables-and-defaults)

## 1. Non-Goals

- **No small-object writer.** Shared-chunk packing for many small
  objects is a separate component. The `ChunkIoWriter` trait and
  `Location` type are designed for reuse, but the packing policy is not
  part of this design.
- **No reader.** The read path (location resolution, strip fetch, EC
  decode, range reads) is a separate component.
- **No single-block replacement on write failure.** The error path
  retries whole strips and frees failed segments; in-place single-block
  repair is a future refinement and an integration point, not a v1
  behavior.
- **No GC of leaked partial chunks.** Best-effort cleanup on abort
  leaves Active chunks for a future reaper; this doc does not specify
  the reaper.
- **No server-side changes.** The writer is a client library; chunkdb,
  diskio, and kv-server binaries are unchanged by this design.

## 2. Key Design Decisions

- **Block-granularity pipeline, not strip-granularity.** The first disk
  write starts after 1 MB (one data block), not after the full 4 MB
  strip. Three strips stay in flight simultaneously (N parity, N+1
  data, N+2 fetch) without unbounded memory. Strip-granularity would
  double time-to-first-byte and halve steady-state throughput.
- **Push-based "always store" contract.** `ChunkIoWriter::on_data`
  never rejects a buffer; it awaits until internal capacity is free.
  This puts the retry-loop in one place (the writer) instead of every
  caller. A `FeedStatus` (`Continue` / `Pause`) answers "would the next
  push block?" so a dedicated upload task can ignore it and block,
  while a shared handler task can pre-check via the non-async
  `require_data` hint and return 503 instead of stalling.
- **Bounded preallocation, not eager allocation.** A 1 TB object does
  not allocate all 250K strips at once. The prealloc task stays only
  `prealloc_depth` strips (default 2) ahead of the write cursor,
  keeping allocation rate and KV metadata pressure bounded regardless
  of object size while keeping the cursor fed.
- **Shard-based EC, no re-split copy.** The pipeline already holds data
  as separate 1 MB `Bytes` blocks. Re-splitting a contiguous buffer
  just to feed `crow_common::ec::encode` would copy 4 MB per strip for
  no benefit. `encode_parity_from_shards` takes pre-split shards
  directly and reuses the existing isa-l FFI path — no new C++ code.
- **Whole-strip retry, not single-block retry.** On a diskio write
  failure for any block of a strip, the writer retries the whole strip
  at a fresh placement. This keeps the strip's data/parity placement
  atomic and avoids degraded-strip bookkeeping in v1; single-block
  replacement is left as a future integration point.
- **Memory budget per pool, not per object.** A `WriterPool` tracks a
  total `memory_budget` and an atomic `in_use` counter; `try_acquire`
  rejects with `MemoryBudgetExhausted` when full, enabling backpressure
  up the call stack. Per-writer footprint is constant (~15 MB peak for
  4+1 EC, 1 MB blocks, defaults), so `max_concurrent = budget /
  per-writer-footprint` — a 1 TB and a 50 MB upload cost the same RAM.
- **Two trait seams for testability.** `LargeObjectWriter` is generic
  over one chunk-lifecycle seam and one block-IO seam, so integration
  tests inject mock impls without real servers; E2E tests use real
  clients. The seams are not a runtime polymorphism optimization —
  they exist so the pipeline can be tested in isolation.

## 3. Write Flow

The writer runs four concurrent stages: a fetch stage, a main write
task, a bounded pool of background parity tasks, and a background
prealloc task. `LargeObjectWriter` exposes two driving modes over the
same pipeline — stream mode (`write_stream`, pulling from an `AsyncRead`)
and push mode (the `ChunkIoWriter` trait, §2). In stream mode a fetch
stage pulls from `AsyncRead`; in push mode the caller's bytes go
directly to the block channel. The main write and parity tasks are
identical in both modes.

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

The flow, step by step:

- **Fetch.** Reads from the `AsyncRead` stream in ≤ 1 MB per call (one
  data block). A single socket read may return less (64 KB, 512 KB);
  the fetch stage accumulates to 1 MB, then sends the block to the main
  write task immediately — it does not wait for the full 4 MB strip.
  The fetch stage sends sequential `Bytes` blocks and does not track
  block indices or strip boundaries; the main write task owns indexing.
  On EOF with a partial last block, the partial block is sent.
- **Main write.** The central coordinator. At the start of each new
  strip it awaits the strip's placement from the prealloc channel
  (blocks if prealloc fell behind). It receives 1 MB blocks one at a
  time, tracks the block index within the current strip
  (`count % data_num`, 0–3 for 4+1), and writes each to the strip's
  corresponding data segment via `BlockWriter::write` — one disk per
  block, no EC wait. When all `data_num` blocks of strip N are written,
  it hands the strip's data blocks off to a background parity task
  (bounded by `parity_depth`; blocks at hand-off if the pool is full),
  records the parity task handle in the current chunk's handle list,
  and **immediately advances to strip N+1, block 0** — it does not wait
  for EC compute, parity write, or fsync. On chunk full it triggers
  rotation (§6).
- **Parity.** One background task per strip. It receives the strip's
  data blocks, EC-encodes via `encode_parity_from_shards` (§5) into
  `code_num` parity blocks, writes them to the remaining segments via
  `BlockWriter::write`, and `fsync`s all disks via `BlockWriter::fsync`.
  While strip N's parity task computes + writes parity, the main write
  task is already writing strip N+1's data blocks and strip N+2's
  blocks are being fetched.
- **Preallocation.** Allocates the first chunk with **1 strip** (the
  minimum to start writing immediately), then appends remaining strips
  via `append_chunk` ahead of the write cursor, up to `prealloc_depth`
  (default 2) strips ahead. If the object size is known, it
  pre-calculates total strip/chunk count for planning but still only
  pre-allocates `prealloc_depth` ahead. It pre-allocates the next chunk
  (via `ChunkPrefetch`, 1 strip) when the current chunk is within
  `prealloc_depth` strips of `max_chunk_size`, up to
  `chunk_prefetch_depth` ahead, and delivers the new `ChunkId` via a
  oneshot channel. If prealloc falls behind the cursor, the main write
  task blocks on the bounded strip channel (backpressure).

### 3.1 Partial Last Strip

Partial strips occur only at EOF, never mid-chunk. When EOF arrives
before all `data_num` blocks of the current strip are filled, the main
write task writes only the filled data blocks, releases the empty ones,
hands the partial set off to parity for partial EC (§5), and records
`sealed_length` for `seal_chunk`.

## 4. Backpressure and Memory Budget

Two independent limits bound the pipeline; neither depends on object
size.

- **`max_cached_buffer`** (default 4 MB = one strip) bounds un-written
  data in the fetch channel — blocks sent to the main write task but
  not yet written to disk. When disk write is slower than network
  receive, the cache fills; once full, the fetch stage blocks and
  throttles the stream to the disk write speed.
- **`parity_depth`** (default 2) bounds in-flight parity tasks. When
  the parity pool is full, the main write task blocks at hand-off —
  backpressure on the write path, decoupled from the fetch cache.

If prealloc falls behind, the main write task awaits on the bounded
strip channel; the fetch stage keeps filling `max_cached_buffer`, then
blocks when full. No data is lost.

### 4.1 Per-Writer Footprint

For 4+1 EC, 1 MB blocks, defaults:

- `max_cached_buffer` — 4 MB un-written data in fetch cache.
- 1 block being written — 1 MB.
- Up to `parity_depth` (2) parity tasks, each holding the strip's data
  blocks (4 × 1 MB = 4 MB, shared via `Bytes` ref count — not copied,
  but resident until EC compute completes) + 1 parity block (1 MB).

Peak: 4 + 1 + 2 × (4 + 1) = **15 MB**. The conservative 15 MB assumes
both in-flight parity tasks hold data refs simultaneously; realistic
steady-state peak is ~11 MB, since EC compute is fast relative to disk
write + fsync and parity tasks stagger.

### 4.2 WriterPool

`WriterPool` tracks a `memory_budget` and an atomic `in_use` counter.
`try_acquire` returns `MemoryBudgetExhausted` when the budget is full;
release decrements `in_use` on `Drop`. `max_concurrent =
memory_budget / per-writer-footprint`. Many concurrent large-object
uploads are thus bounded by available RAM, not by object size — the
pool rejects new writes when the budget is exhausted, propagating
backpressure up the call stack.

## 5. EC Integration

The pipeline holds data as separate 1 MB `Bytes` blocks; the existing
`crow_common::ec::encode` re-splits a contiguous buffer into shards,
which would force a 4 MB copy per strip just to re-split. Instead,
`encode_parity_from_shards(scheme, data_shards)` takes pre-split data
shards directly and reuses the existing isa-l FFI path — no new C++
code.

`data_shards.len()` is `data_num` for a full strip, or `< data_num`
for a partial strip. isa-l supports partial EC: missing shards are
treated as zero for the encoding matrix, so no padding is written to
disk — the reader reads only `sealed_length` bytes. The function
returns `code_num` parity shards.

Edge cases:

- Full strip → standard EC.
- Partial strip → parity from present shards; reader reads only
  `sealed_length` bytes.
- Single-block object (< 1 MB) → 1 partial data shard, 1 parity shard.

## 6. Chunk Rotation and Location

Very large objects (`> max_chunk_size`) span multiple chunks. Chunk
size is always a multiple of strip data capacity — the writer only
appends whole strips — so rotation happens at strip boundaries, never
mid-strip. After completing a full strip, if the chunk's accumulated
size ≥ `max_chunk_size`, the main write task joins in-flight parity
tasks for the current chunk, calls `seal_chunk`, records a `Location`
entry, and switches to the prefetched next chunk (blocking if not
ready). The `Location` array accumulates one entry per rotated chunk,
ordered by `logical_offset`.

### 6.1 Location

A `Location` records which chunk holds a contiguous byte range of an
object, the byte range within that chunk, and the object-level logical
offset/length so a multi-chunk object reads back as one contiguous
stream. An object spanning N chunks has N locations ordered by logical
offset, contiguous and non-overlapping. The within-chunk offset is
always 0 for the large-object writer (dedicated chunks filled from the
start); it exists for future shared-chunk packing and range reads.
Serialization is protobuf for KV storage and a compact binary form for
inline embedding in object metadata.

Edge cases:

- Empty object (size 0) → `Vec<Location>` is empty; no chunk allocated.
- `logical_length` may be < `length` in future hole-punch scenarios; the
  writer always sets them equal.
- `max_chunk_size` not a multiple of strip data capacity → rotation
  still happens at strip boundaries; the actual chunk size is the
  multiple-of-strip value at or just above the threshold.

## 7. Completion and Error Handling

`write_stream` must seal the final chunk and return the `Location`
array on success. On error or abort it must not leak partial chunks and
must return already-sealed `Location`s for caller cleanup.

- **Completion** — join all parity tasks → `seal_chunk` → return
  `Vec<Location>`.
- **Error / abort** (`on_error` or `Drop`) — cancel in-flight pipeline
  tasks via a cancellation token, free partial (non-sealed) chunks via
  `delete_chunk`, return `Location`s of already-sealed chunks.
- **Whole-strip retry** — on a diskio write failure for any block of a
  strip, retry the whole strip: `append_chunk` a new strip with a fresh
  placement, re-write all data + parity, free the failed strip's
  segments. Up to 3 retries; on exhaustion, `IoError::WriteFailed` with
  the partial `Location` array. The abort/cleanup paths are integration
  points for future single-block replacement.
- **`Drop`** — calls abort as a safety net if the caller drops the
  writer without `on_finish` / `on_error`.

Edge cases:

- `on_data` after `on_finish` / `on_error` → `IoError::Finished`.
- `on_finish` twice → `IoError::Finished`.
- `on_error` with no sealed chunks → `Ok(vec![])`.
- EC encode failure → pipeline aborts immediately (no retry — EC encode
  is a CPU/isal error, not a placement issue). A future refinement may
  mark the strip degraded or retry with a fallback encoder.
- `delete_chunk` fails during cleanup → log + continue (best-effort;
  the partial chunk stays Active and is reaped by a future GC task).

## 8. Interaction with Neighbors

- **chunkdb** — the writer calls allocate / append / seal / delete /
  update_chunk_strip / query via `ChunkAllocator`. chunkdb handles
  placement and lifecycle; the writer is unaware of internal placement
  logic — it receives `Segment` placements and writes to them.
- **diskio** — the writer writes data + parity blocks via
  `BlockWriter::write` and flushes via `BlockWriter::fsync`.
  `DiskioBlockWriter` wraps `DiskioClient` + `RpcServer` +
  `Connection`.
- **crow-common EC** — `encode_parity_from_shards` is the shard-based +
  partial-encode entry point used by parity tasks.
- **crow-protocol** — `ChunkId`, `*ChunkRequest` types, `Segment`,
  `Location` message.

## 9. Tunables and Defaults

| Knob | Default | Role |
| --- | --- | --- |
| `max_chunk_size` | 1 GB | Chunk rotation threshold. |
| `prealloc_depth` | 2 strips | Strip preallocation ahead of write cursor. |
| `parity_depth` | 2 tasks | In-flight parity task bound. |
| `chunk_prefetch_depth` | 1 chunk | Chunk prefetch ahead of rotation. |
| fetch granularity | 1 MB | One data block per fetch call. |
| `max_cached_buffer` | 4 MB | Un-written data budget in fetch channel (one strip). |
| `memory_budget` | (pool) | `WriterPool` total; `max_concurrent = budget / per-writer-footprint`. |

The writer is a client library — no server-side config changes.
