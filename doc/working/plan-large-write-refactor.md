<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Large-Object Writer OO Refactor Plan

Design: `doc/working/large-write-review.md`. Goal: restructure
`crow-chunk-client`'s large-object write path from free functions on
loose structs into OO stage classes (`ChunkClientConfig`, `DiskWriter`,
`EcWorker`, `StripWriter` enum, `EcStripWriter`, `ParityBatch`,
`ChunkWriter`, `ChunkPrefetch`, `LargeObjectWriter`,
`LargeAsyncObjectWriter`, `SmallObjectWriter` stub, `HashWorker` stub,
`MirrorStripWriter` stub), organized into `io/` / `worker/` / `chunk/`
/ `writer/` concept-level modules.

The refactor is mechanical reshuffling + class extraction — no behavior
change. Tests must stay green at every checkpoint.

## Phase 1: Foundation (no behavior change)

- [x] **1.1 Create `config.rs` — `ChunkClientConfig`.** Move all
  `WriterConfig` fields into `ChunkClientConfig`; add
  `per_writer_memory(ec_scheme)` (deduplicate the formula at
  `large_object.rs:108` + `pool.rs:49`). Add `validate()`. Keep
  `WriterConfig` as a deprecated type alias / re-export for now (tests
  still use it) — removed in Phase 5. Files: `src/config.rs` (new),
  `src/lib.rs`.
- [x] **1.2 Create `io/` module — `DiskWriter` trait.** Define the
  `DiskWriter` trait (takes `&Segment` + `unit_bytes`). Move
  `DiskioBlockWriter` from `diskio_writer.rs` into `io/disk_writer.rs`,
  implement `DiskWriter` (not `BlockWriter`). Keep `BlockWriter` trait
  in `traits.rs` for now (existing code still uses it) — removed in
  Phase 4. Files: `src/io/mod.rs` (new), `src/io/disk_writer.rs` (new,
  moved from `diskio_writer.rs`), `src/lib.rs`.
  **Note:** module named `disk_io` (not `io`) to avoid conflict with
  existing `io.rs` (`ChunkIoWriter` trait).
- [x] **1.3 Create `io/local_file.rs` — `LocalFileDiskWriter` stub.**
  Test-only `DiskWriter` impl writing to per-disk files under a temp
  dir. Behind `test-util` feature. Stub only (methods `todo!()`) —
  filled in Phase 6. Files: `src/io/local_file.rs` (new).
- [x] **1.4 Create `worker/` module — `EcWorker` (streaming compute).**
  Define `EcWorker` struct with `new`/`push`/`finish`/`reset`. Initial
  impl: `push` appends to `data_shards`, `finish` calls
  `encode_parity_from_shards` (moves the call from `pipeline.rs:80`).
  No shared `Worker` trait. Files: `src/worker/mod.rs` (new),
  `src/worker/ec_worker.rs` (new), `src/lib.rs`.
- [x] **1.5 Create `worker/hash_worker.rs` — `HashWorker` stub.**
  Placeholder struct, methods `todo!()`. Separate from `EcWorker`, no
  shared trait. Files: `src/worker/hash_worker.rs` (new).
- [x] **1.6 Create `chunk/strip.rs` — `StripPlacement` + methods +
  `StripWriter` enum + `StripResult`.** Move `StripPlacement` from
  `prefetch.rs`. Add `unit_bytes()`/`segment(i)`/`disk_id(i)`/
  `zone_offset(i)` methods. Define `StripWriter` enum
  (`Ec`/`Mirror` variants) + `StripResult` struct. Files:
  `src/chunk/mod.rs` (new), `src/chunk/strip.rs` (new), `src/lib.rs`.
- [x] **1.7 Create `chunk/parity_batch.rs` — `ParityBatch`.** Per-strip
  parallel write tracker: `spawn`/`join_all`/`abort_all`. No semaphore.
  Files: `src/chunk/parity_batch.rs` (new).
- [x] **1.8 Create `chunk/mirror_strip_writer.rs` —
  `MirrorStripWriter` stub.** Placeholder struct. `StripWriter::Mirror`
  methods `todo!()`. Files: `src/chunk/mirror_strip_writer.rs` (new).
- [x] **1.9 Create `chunk/chunk_reader.rs` + `chunk/strip_reader.rs`
  — R107 placeholders.** Empty stub modules (`//! R107 placeholder`).
  Files: `src/chunk/chunk_reader.rs` (new),
  `src/chunk/strip_reader.rs` (new).
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.**
  New modules compile alongside old code (old code untouched, still
  used). All 25 existing tests pass.

## Phase 2: Strip + Worker layer (extract, wire into new classes)

- [x] **2.1 Create `chunk/ec_strip_writer.rs` — `EcStripWriter`.**
  Extract the strip-write logic from `pipeline.rs` (`write_strip_with_retry`
  body, `spawn_parity_task` body). `EcStripWriter` owns `EcWorker` +
  `ParityBatch` + `StripPlacement` + `DiskWriter`. Implements
  `push`/`finish`/`abort`/`ready`. Not wired into anything yet —
  standalone. Files: `src/chunk/ec_strip_writer.rs` (new).
- [x] **2.2 Create `chunk/chunk_prefetch.rs` — `ChunkPrefetch`.**
  Extract `spawn_prealloc_task` + `run_prealloc` from `prefetch.rs`
  into a class. Fields replace the 7 loop params. `spawn` + `on_demand`
  methods. Files: `src/chunk/chunk_prefetch.rs` (new).
- [x] **2.3 Create `chunk/chunk_writer.rs` — `ChunkWriter`.** Extract
  chunk-scoped state from `pipeline.rs` `MainWriteState`. `ChunkWriter`
  is a chunk-info wrapper: `open`/`push`/`append_strip`/
  `replace_block`/`seal`/`abort`/`ready`. Does NOT own the drive loop.
  Files: `src/chunk/chunk_writer.rs` (new).
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.** New
  chunk-layer classes compile. Old code still untouched. All 25 tests
  pass.

## Phase 3: Writer layer (drive loop + facade)

- [x] **3.1 Create `writer/large_object.rs` (new) —
  `LargeObjectWriter` (non-blocking stream).** Owns the drive loop
  (rotate, fetch, EOF, accumulate `Location`s). Holds `ChunkWriter` +
  `ChunkPrefetch`. Implements `ChunkIoWriter` + `write(buffers, size)`.
  This is the new home — the old `src/writer/large_object.rs` is
  replaced in Phase 4. Files: `src/writer/large_object_new.rs` (new,
  renamed in Phase 4).
- [x] **3.2 Create `writer/large_async_object.rs` —
  `LargeAsyncObjectWriter`.** Same drive loop + fetch stage
  (`run_fetch_stage` moved from `pipeline.rs`). Implements
  `ChunkIoWriter` + `write_stream(reader, size)`. Files:
  `src/writer/large_async_object.rs` (new).
- [x] **3.3 Create `writer/small_object.rs` — `SmallObjectWriter`
  stub.** Placeholder struct, `ChunkIoWriter` methods `todo!()`. Files:
  `src/writer/small_object.rs` (new).
- [x] **3.4 Create `writer/fetch.rs` — `run_fetch_stage`.** Move the
  free function from `pipeline.rs`. Pure IO glue, stays free. Files:
  `src/writer/fetch.rs` (new).
- [x] **3.5 Create `writer/pool_new.rs` — `WriterPool` (no generics).**
  Drop `<A, W>` generics; hold concrete `Arc<dyn ChunkAllocator>` +
  `Arc<dyn DiskWriter>` + `Arc<ChunkClientConfig>`. Delegate
  `per_writer_memory` to config. Files: `src/writer/pool_new.rs` (new,
  renamed in Phase 4).
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.** All
  new classes compile. Old code still untouched. All 25 tests pass.

## Phase 4: Wire new classes, remove old code

- [x] **4.1 Update `src/lib.rs` — re-export new types.** Export
  `ChunkClientConfig`, `DiskWriter`, `DiskioBlockWriter` (from new
  location), `EcWorker`, `StripWriter`, `EcStripWriter`,
  `MirrorStripWriter`, `ParityBatch`, `ChunkWriter`, `ChunkPrefetch`,
  `LargeObjectWriter` (new), `LargeAsyncObjectWriter`,
  `SmallObjectWriter`, `WriterPool` (new). Keep `WriterConfig` alias
  for now. Files: `src/lib.rs`.
- [x] **4.2 Remove old `src/writer/pipeline.rs`.** Its logic now lives
  in `chunk/chunk_writer.rs`, `chunk/ec_strip_writer.rs`,
  `worker/ec_worker.rs`, `chunk/parity_batch.rs`,
  `writer/fetch.rs`. Delete the file. Files: `src/writer/pipeline.rs`
  (deleted), `src/writer.rs` (update `pub mod`).
- [x] **4.3 Remove old `src/writer/large_object.rs` + `src/writer/pool.rs`.**
  Rename `writer/large_object_new.rs` → `writer/large_object.rs`,
  `writer/pool_new.rs` → `writer/pool.rs`. Delete old versions. Files:
  `src/writer/large_object.rs` (replaced), `src/writer/pool.rs`
  (replaced).
- [x] **4.4 Remove old `src/prefetch.rs` + `src/diskio_writer.rs`.**
  Logic moved to `chunk/chunk_prefetch.rs` + `io/disk_writer.rs`. Delete
  old files. Files: `src/prefetch.rs` (deleted),
  `src/diskio_writer.rs` (deleted), `src/lib.rs` (update `pub mod`).
- [x] **4.5 Remove `BlockWriter` trait from `traits.rs`.** Folded into
  `DiskWriter`. Update `ChunkAllocator` blanket impls. Files:
  `src/traits.rs`.
- [x] **4.6 Remove `WriterConfig` alias.** Tests updated in Phase 5.
  Files: `src/lib.rs`, `src/config.rs`.
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.** Old
  code gone, new code is the only implementation. Tests will fail
  (still reference old API) — fixed in Phase 5.

## Phase 5: Update tests

- [x] **5.1 Update `tests/large_object_writer.rs`.** Replace
  `WriterConfig` → `ChunkClientConfig`. Replace `BlockWriter` trait
  mocks → `DiskWriter` trait mocks. Update `LargeObjectWriter`
  construction. Files: `tests/large_object_writer.rs`.
- [x] **5.2 Update `tests/write_stream.rs`.** Same migration as 5.1.
  Switch `LargeObjectWriter::write_stream` →
  `LargeAsyncObjectWriter::write_stream`. Mock `BlockWriter` →
  `MockDiskWriter` implementing `DiskWriter`. Files:
  `tests/write_stream.rs`.
- [x] **5.3 Update `tests/large_object_writer_e2e.rs`.** Replace
  `DiskioBlockWriter` import path (now from `disk_io/disk_writer.rs`).
  `WriterConfig` → `ChunkClientConfig`. `LargeObjectWriter` →
  `LargeAsyncObjectWriter`. Files:
  `tests/large_object_writer_e2e.rs`.
- [x] **Checkpoint: `pixi run cargo test -p crow-chunk-client --all-targets`.**
  All 25 existing tests pass against the new classes.

## Phase 6: Fill in `LocalFileDiskWriter` + new UTs

- [x] **6.1 Implement `LocalFileDiskWriter`.** Per-disk files under a
  temp dir. `write` appends to the file at the computed offset;
  `fsync` calls `File::sync_all`. Behind `test-util`. Files:
  `src/disk_io/local_file.rs`.
- [x] **6.2 Add UTs for `EcWorker`.** Streaming compute: push N data
  shards, finish, verify parity matches `encode_parity_from_shards`.
  Reset + reuse. Files: `tests/ec_worker_test.rs` (new).
- [x] **6.3 Add UTs for `StripPlacement` methods.** `unit_bytes`,
  `segment(i)`, `disk_id(i)`, `zone_offset(i)` — bounds + extraction.
  Files: `tests/strip_test.rs` (new).
- [x] **6.4 Add UTs for `ParityBatch`.** Spawn N tasks, join_all
  (success + first-error), abort_all. Files: `tests/parity_batch_test.rs`
  (new).
- [ ] **6.5 Add UTs for `ChunkWriter` with `LocalFileDiskWriter`.**
  Open, push blocks, seal, verify `Location`. Abort path. Replace-block
  retry path. Files: `tests/chunk_writer_test.rs` (new).
  **Deferred** — requires a mock `ChunkAllocator` (can't use
  `LocalFileDiskWriter` alone for chunk operations). The existing
  `write_stream.rs` tests already cover `ChunkWriter` integration via
  `MockChunkAllocator` + `MockDiskWriter`.
- [x] **Checkpoint: `pixi run cargo test -p crow-chunk-client --all-targets --features test-util`.**
  All 53 tests pass (25 existing + 4 ec_worker + 5 strip + 5 parity_batch
  + 2 e2e + 12 large_object_writer).

## Phase 7: Lint + commit

- [ ] **7.1 `pixi run rs-fmt`** — format all new/changed files.
- [ ] **7.2 `pixi run rs-lint`** — clippy `pedantic = warn`, fix up to
  3 times.
- [ ] **7.3 Final test run.** `pixi run cargo test -p crow-chunk-client
  --all-targets` — all green.
- [ ] **7.4 Commit.** Single commit: `refactor: restructure
  crow-chunk-client into OO stage classes`. Verify no temp/generated
  files staged.

## File List

New files:
- `src/config.rs` — `ChunkClientConfig`
- `src/io/mod.rs` — io module index
- `src/io/disk_writer.rs` — `DiskWriter` trait + `DiskioBlockWriter`
- `src/io/local_file.rs` — `LocalFileDiskWriter` (test-only)
- `src/worker/mod.rs` — worker module index
- `src/worker/ec_worker.rs` — `EcWorker` (streaming EC compute)
- `src/worker/hash_worker.rs` — `HashWorker` (stub)
- `src/chunk/mod.rs` — chunk module index
- `src/chunk/strip.rs` — `StripPlacement` + `StripWriter` enum + `StripResult`
- `src/chunk/ec_strip_writer.rs` — `EcStripWriter`
- `src/chunk/mirror_strip_writer.rs` — `MirrorStripWriter` (stub)
- `src/chunk/parity_batch.rs` — `ParityBatch`
- `src/chunk/chunk_writer.rs` — `ChunkWriter`
- `src/chunk/chunk_prefetch.rs` — `ChunkPrefetch`
- `src/chunk/chunk_reader.rs` — R107 placeholder
- `src/chunk/strip_reader.rs` — R107 placeholder
- `src/writer/large_async_object.rs` — `LargeAsyncObjectWriter`
- `src/writer/small_object.rs` — `SmallObjectWriter` (stub)
- `src/writer/fetch.rs` — `run_fetch_stage` (free function)
- `tests/ec_worker_test.rs` — `EcWorker` UTs
- `tests/strip_test.rs` — `StripPlacement` UTs
- `tests/parity_batch_test.rs` — `ParityBatch` UTs
- `tests/chunk_writer_test.rs` — `ChunkWriter` UTs

Modified files:
- `src/lib.rs` — re-exports
- `src/traits.rs` — remove `BlockWriter`, keep `ChunkAllocator`
- `src/writer.rs` → `src/writer/mod.rs` — module index
- `src/writer/large_object.rs` — replaced (new `LargeObjectWriter`)
- `src/writer/pool.rs` — replaced (new `WriterPool`)

Deleted files:
- `src/diskio_writer.rs` — moved to `io/disk_writer.rs`
- `src/prefetch.rs` — moved to `chunk/chunk_prefetch.rs`
- `src/writer/pipeline.rs` — split into chunk/worker/writer modules

Modified tests:
- `tests/large_object_writer.rs`
- `tests/write_stream.rs`
- `tests/large_object_writer_e2e.rs`

## Test Checklist

Existing (must stay green):
- [ ] `tests/large_object_writer.rs` — Location, ChunkIoWriter, WriterConfig, EC
- [ ] `tests/write_stream.rs` — write_stream end-to-end, chunk rotation, retry
- [ ] `tests/large_object_writer_e2e.rs` — E2E with real diskio/chunkdb

New UTs:
- [ ] `ec_worker_test.rs` — streaming compute parity correctness, reset/reuse
- [ ] `strip_test.rs` — StripPlacement method bounds + extraction
- [ ] `parity_batch_test.rs` — join_all success + first-error, abort_all
- [ ] `chunk_writer_test.rs` — open/push/seal/abort/replace-block with LocalFileDiskWriter
