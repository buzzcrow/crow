<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Large Object Writer + Chunk IO Interface + Location (R94) Plan

Design draft: [`doc/working/design-chunkdb-large-object-writer.md`](design-chunkdb-large-object-writer.md).
Backlog doc: [`doc/backlog/R94-chunkdb-large-object-writer.md`](../backlog/R94-chunkdb-large-object-writer.md).
Goal: ship the `crow-chunk-client` crate with the `Location` type,
`ChunkIoWriter` interface, and stream-based `LargeObjectWriter` pipeline,
plus the `encode_parity_from_shards` EC extension, satisfying the R94
acceptance criteria.

Tasks are dependency-ordered. One task in progress at a time.

## Phase 0 — Verification gate

- [ ] **Partial-EC UT gate**: add `encode_parity_from_shards` to
  `lib/crow-common/rust/src/ec.rs` (full + partial + single-block
  variants) and write the 3 UTs in
  `lib/crow-common/rust/tests/ec.rs` (full strip, 2-of-4 partial,
  1-of-4 short). Run `pixi run test-common`. **If the partial-EC UT
  fails, stop and involve the user** (R94 Open Question) — do not fall
  back to padding. Files: `lib/crow-common/rust/src/ec.rs`,
  `lib/crow-common/rust/tests/ec.rs`.

## Phase 1 — Crate scaffolding + shared types

- [ ] **`crow-chunk-client` crate skeleton**: create `lib/crow-chunk-client/`
  with `Cargo.toml` (deps per design §1.2), `src/lib.rs` (re-exports),
  `src/error.rs` (`IoError` enum: `AllocationFailed`, `WriteFailed`,
  `EcEncodeFailed`, `MemoryBudgetExhausted`, `Finished`, `Internal`,
  plus `From<ChunkdbClientError>`/`From<DiskioError>`/`From<EcError>`),
  `src/traits.rs` (`ChunkAllocator` + `BlockWriter` trait seams per
  design §1.3, + `impl ChunkAllocator for ChunkdbClient` / `impl
  BlockWriter for DiskioClient`), empty `src/io.rs`, `src/location.rs`,
  `src/prefetch.rs`, `src/writer/{mod,large_object,pipeline,pool}.rs`
  stubs. Add `lib/crow-chunk-client` to root `Cargo.toml` `members`.
  Verify `pixi run cargo build -p crow-chunk-client` compiles. Files:
  `lib/crow-chunk-client/Cargo.toml`, `lib/crow-chunk-client/src/**`,
  `Cargo.toml`.
- [ ] **`Location` proto + type**: add `Location` message to
  `lib/crow-protocol/src/proto/chunkdb_type.proto`, regenerate proto
  bindings. Implement `Location` in `src/location.rs` with proto
  round-trip + compact binary (`to_bytes`/`from_bytes`, 48 bytes).
  Files: `lib/crow-protocol/src/proto/chunkdb_type.proto`,
  `lib/crow-chunk-client/src/location.rs`.
- [ ] **`Location` UTs**: proto round-trip (single + 3-entry),
  binary size < 64 bytes. Files:
  `lib/crow-chunk-client/tests/large_object_writer.rs`.

## Phase 2 — `ChunkIoWriter` interface

- [ ] **`ChunkIoWriter` trait + `FeedStatus` + `BackpressurePolicy`**:
  implement in `src/io.rs` per design §3.2 with full doc-comment
  contract (always-store, `require_data` hint, two caller strategies).
  Files: `lib/crow-chunk-client/src/io.rs`.
- [ ] **`ChunkIoWriter` mock UT**: mock impl verifying
  `on_data`/`on_finish`/`on_error`/`require_data` contract +
  `on_data`-after-`on_finish` → `IoError::Finished`. Files:
  `lib/crow-chunk-client/tests/large_object_writer.rs`.

## Phase 3 — Prefetch + preallocation

- [ ] **Strip preallocation task**: implement in `src/prefetch.rs` —
  background task, `allocate_chunk(strip_count=1)` then
  `append_chunk(strip_count=1)` up to `prealloc_depth` ahead, bounded
  `mpsc` channel, retry-on-transient, error-into-channel on exhaustion.
  Chunk-fullness check: after allocating a full strip, if accumulated
  size ≥ `max_chunk_size`, stop appending and switch to prefetched
  next chunk (same threshold as main write task's rotation — design
  §6.2a, §8.2). Files: `lib/crow-chunk-client/src/prefetch.rs`.
- [ ] **`ChunkPrefetch`**: implement chunk pre-allocation (1 strip) via
  `allocate_chunk` when current chunk within `prealloc_depth` strips of
  `max_chunk_size`, `oneshot` delivery, `chunk_prefetch_depth` bound.
  Files: `lib/crow-chunk-client/src/prefetch.rs`.

## Phase 4 — Pipeline + writer

- [ ] **`WriterConfig` + `LargeObjectWriter::new`**: implement
  `WriterConfig` (defaults per design Config Extensions) + constructor
  generic over `A: ChunkAllocator` / `W: BlockWriter` (design §1.3,
  §4.2), holding `A`/`W`/`RpcServer`/per-disk `Connection`
  cache/`EcScheme`/`WriterConfig`. Files:
  `lib/crow-chunk-client/src/writer/large_object.rs`.
- [ ] **Fetch stage**: implement in `src/writer/pipeline.rs` —
  `AsyncRead` loop, accumulate to `read_buffer_size`, send `Bytes`
  (no block index — main write task tracks indices) to main write
  task via bounded channel (capacity = `max_cached_buffer /
  read_buffer_size`), `max_cached_buffer` backpressure (fetch awaits
  when channel full), partial-last-block on EOF. Files:
  `lib/crow-chunk-client/src/writer/pipeline.rs`.
- [ ] **Main write task**: two-channel coordinator — await strip
  placement from prealloc channel at start of each strip (blocks if
  prealloc behind), then receive `data_num` blocks from fetch channel;
  track `block_idx = count % data_num`, `strip_idx = count /
  data_num`; write each data block to its segment via
  `BlockWriter::write` (convert `Segment.unit_offset` → byte offset);
  on full strip hand off to parity + record JoinHandle in current
  chunk's handle list + advance immediately; handle partial last
  strip (EOF only) + `sealed_length`. Files:
  `lib/crow-chunk-client/src/writer/pipeline.rs`.
- [ ] **Parity tasks**: `tokio::spawn` per strip, bounded by
  `parity_depth` semaphore, `encode_parity_from_shards` + write parity
  blocks via `BlockWriter::write` + `BlockWriter::fsync` all strip
  disks. Files: `lib/crow-chunk-client/src/writer/pipeline.rs`.
- [ ] **`write_stream` orchestration**: launch prealloc + fetch + main
  write + parity, drain on EOF (join parity → seal → return
  `Vec<Location>`), wire `CancellationToken` for abort. Files:
  `lib/crow-chunk-client/src/writer/large_object.rs`.
- [ ] **Chunk rotation**: in main write task, after a full strip check
  `current_chunk_bytes ≥ max_chunk_size` → join current chunk's parity
  tasks (await all `JoinHandle`s in the chunk's handle list, clear
  list), `seal_chunk`, record `Location`, switch to prefetched chunk,
  advance `logical_offset`. Files:
  `lib/crow-chunk-client/src/writer/large_object.rs`.
- [ ] **Completion + error/abort + `Drop`**: `on_finish`/`on_error`
  paths, whole-strip retry (up to 3, `append_chunk` new placement +
  free failed strip), `Drop` abort via `CancellationToken` +
  `delete_chunk` partial cleanup, return sealed `Location`s. Files:
  `lib/crow-chunk-client/src/writer/large_object.rs`.
- [ ] **`ChunkIoWriter` impl for `LargeObjectWriter`**: `on_data`
  sends `Bytes` directly to the block channel (same channel the fetch
  stage uses in `write_stream` mode — no fetch stage task in push
  mode), awaits on full channel (backpressure); `on_finish`/`on_error`
  delegate to pipeline drain/abort; `require_data` checks block
  channel capacity (non-async). Files:
  `lib/crow-chunk-client/src/writer/large_object.rs`.

## Phase 5 — Writer pool

- [ ] **`WriterPool`**: implement `try_acquire` with
  `memory_budget`/`in_use` atomic accounting, per-writer footprint
  formula (design §10.2), `MemoryBudgetExhausted` rejection,
  `Drop`-decrement. Generic over `A: ChunkAllocator + Clone` /
  `W: BlockWriter + Clone` (design §10.2). Files:
  `lib/crow-chunk-client/src/writer/pool.rs`.

## Phase 6 — Integration tests (class-level mocks)

- [ ] **Integration test harness**: mock `ChunkAllocator` +
  mock `BlockWriter` impls (class-level mocks per design §1.3 — mock
  allocate-chunk-strip, free-block, write, fsync) recording calls +
  injectable delays/errors. The writer is constructed with the mock
  impls via its generic trait params. Files:
  `lib/crow-chunk-client/tests/large_object_writer.rs`.
- [ ] **Pipeline concurrency + fetch granularity + bounded prealloc +
  chunk prefetch + backpressure + streaming + partial strip + whole-
  strip retry + EC encode failure abort + prefetch-fell-behind-at-
  rotation + on_error + Drop + WriterPool budget tests**: per design
  Test Design (Integration). Files:
  `lib/crow-chunk-client/tests/large_object_writer.rs`.

## Phase 7 — E2E tests (real servers)

- [ ] **E2E harness wiring**: extend `crow-test-harness` usage to start
  1 kv-server + 1 diskdb (5 disks) + 1 chunkdb; construct
  `LargeObjectWriter` in-process with a real `DiskioClient` +
  `ChunkdbClient`. Files:
  `lib/crow-chunk-client/tests/large_object_writer_e2e.rs`.
- [ ] **E2E Case 1 (50 MB single chunk)** + **E2E Case 2 (100 MB chunk
  rotation)**: per design Test Design (E2E) — verify `Vec<Location>`,
  `query_chunk` state + strip counts + `sealed_length`, read-back data
  integrity. Files:
  `lib/crow-chunk-client/tests/large_object_writer_e2e.rs`.

## Phase 8 — Quality gate + commit

- [ ] **Lint**: `pixi run cargo fmt --all -- --check`,
  `pixi run cargo clippy --all-targets -- -D warnings`. Fix up to 3
  times.
- [ ] **Affected tests**: `pixi run test-common` (EC UT gate),
  `pixi run cargo test -p crow-chunk-client --test
  large_object_writer`, `pixi run clean-env && pixi run cargo test -p
  crow-chunk-client --test large_object_writer_e2e`. All must pass.
- [ ] **Commit**: implementation commits (one per phase or grouped for
  small phases) + final commit including design draft + plan doc.

## File list

- `Cargo.toml` (root) — add `lib/crow-chunk-client` to `members`.
- `lib/crow-chunk-client/Cargo.toml` — new crate manifest.
- `lib/crow-chunk-client/src/lib.rs` — re-exports.
- `lib/crow-chunk-client/src/error.rs` — `IoError`, `Result`.
- `lib/crow-chunk-client/src/location.rs` — `Location` + ser/de.
- `lib/crow-chunk-client/src/io.rs` — `ChunkIoWriter`, `FeedStatus`,
  `BackpressurePolicy`.
- `lib/crow-chunk-client/src/traits.rs` — `ChunkAllocator`,
  `BlockWriter` trait seams + impls for `ChunkdbClient` / `DiskioClient`.
- `lib/crow-chunk-client/src/prefetch.rs` — strip prealloc +
  `ChunkPrefetch`.
- `lib/crow-chunk-client/src/writer/mod.rs` — module root.
- `lib/crow-chunk-client/src/writer/large_object.rs` —
  `LargeObjectWriter`, `WriterConfig`, `write_stream`, rotation,
  completion, error/abort, `Drop`, `ChunkIoWriter` impl.
- `lib/crow-chunk-client/src/writer/pipeline.rs` — fetch + main write
  + parity plumbing.
- `lib/crow-chunk-client/src/writer/pool.rs` — `WriterPool`.
- `lib/crow-chunk-client/tests/large_object_writer.rs` — UT +
  integration.
- `lib/crow-chunk-client/tests/large_object_writer_e2e.rs` — E2E.
- `lib/crow-common/rust/src/ec.rs` — `encode_parity_from_shards`.
- `lib/crow-common/rust/tests/ec.rs` — partial-EC UTs (gate).
- `lib/crow-protocol/src/proto/chunkdb_type.proto` — `Location`
  message.

## Test checklist

UT (no clean-env):
- `Location` proto round-trip (single + 3-entry).
- `Location` binary size < 64 bytes.
- `ChunkIoWriter` mock contract + `on_data`-after-`on_finish`
  `Finished` error.
- `on_finish` called twice → second returns `IoError::Finished`.
- `on_error` with no sealed chunks → `Ok(vec![])`.
- `encode_parity_from_shards` full strip round-trip (decode verifies).
- `encode_parity_from_shards` 2-of-4 partial strip — **gate; stop if
  fails**.
- `encode_parity_from_shards` 1-of-4 short shard.
- `WriterConfig` defaults.
- Empty object → `Ok(vec![])`, no calls.
- `pixi run test-common` (EC UTs).

Integration (class-level mock `ChunkAllocator` + `BlockWriter`):
- Pipeline concurrency (3 strips in flight).
- Fetch granularity (512 KB reads → 1 MB blocks → first write at 1 MB).
- Bounded preallocation (≤ 2 strips ahead, `allocate_chunk` strip_count
  = 1, 12 `append_chunk`).
- Chunk prefetch (chunk N+1 allocated before chunk N sealed).
- Prefetch fell behind at rotation (delayed `allocate_chunk` → main
  write blocks at rotation, no data lost).
- Backpressure (fetch blocks at 4 MB, no loss).
- Streaming (`object_size = None`).
- Partial strip (2 MB → `sealed_length = 2 MB`).
- Object size exactly equals strip data capacity (4 MB → 1 full strip,
  no partial EC).
- Size hint mismatch — fewer bytes (hint 50 MB, stream 30 MB → seals
  at 30 MB).
- Size hint mismatch — more bytes (hint 30 MB, stream 50 MB → seals
  at 50 MB).
- Whole-strip retry (inject 3rd-strip diskio failure, up to 3 retries).
- EC encode failure abort (inject EC error → `IoError::EcEncodeFailed`,
  no retry).
- `on_error` after 2 sealed chunks (returns 2 `Location`s, frees
  partial).
- Drop mid-write (partial chunk deleted).
- `WriterPool` budget (30 MB → 2 writers, 3rd rejected).
- Per-writer memory bounded (50 MB == 500 MB footprint).

E2E (real servers, `pixi run clean-env &&`):
- Case 1: 50 MB single chunk — 1 `Location`, 13 strips, strip 12
  `sealed_length = 2 MB`, read-back identical.
- Case 2: 100 MB rotation — 13 `Location`s, `logical_offset` 0/8/.../96
  MB, 13 chunks Sealed, chunks 1–12 = 2 strips, chunk 13 = 1 strip,
  read-back contiguous identical.

Commands:
- `pixi run test-common`
- `pixi run cargo test -p crow-chunk-client --test large_object_writer`
- `pixi run clean-env && pixi run cargo test -p crow-chunk-client --test large_object_writer_e2e`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
