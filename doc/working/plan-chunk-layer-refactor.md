<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-Layer Drive Loop + Own-Protobuf Refactor Plan

Design: `doc/working/design-chunk-layer-refactor.md`. Goal: move the
strip-level drive loop from the object layer into `ChunkWriter`, own
protobuf types directly (`Chunk`, `ChunkStrip`, `ProtoLocation`), and
delete the parallel Rust types (`StripPlacement`, `Location`).

The refactor is behavior-preserving — no protocol changes, no new
RPCs, no config changes. Tests must stay green at every checkpoint.

## Phase 1: `ChunkWriter` owns `Arc<Chunk>` + `ChunkPrefetch` streams `Chunk`

`EcStripWriter` is unchanged this phase — it still takes
`StripPlacement`. `ChunkWriter` holds `Arc<Chunk>` and bridges to
`EcStripWriter` via the existing `extract_placement_from_chunk`
helper (extracts a `StripPlacement` from `chunk.strips[idx]`). This
avoids any throwaway bridge code — the helper already exists.

- [x] **1.1 Repurpose `ChunkPrefetch` to stream `Chunk` instead of
  `StripPlacement`.** Behavior-preserving: only the sent type
  changes. `spawn(object_size)` keeps its parameter (strip planning
  stays in `ChunkPrefetch` until Phase 3.2 moves it into
  `ChunkWriter`). `run` sends the full cumulative `Chunk` protobuf
  after each strip-append (same loop structure — one `Chunk` per
  strip, cumulative). `on_demand(current_chunk_id,
  strips_in_current_chunk)` keeps its params, returns `Chunk`.
  `allocate_new_chunk` / `append_strip` return `Chunk` (not
  `StripPlacement`). `extract_placement_from_chunk` stays as the
  bridge (used by `ChunkWriter` in 1.3/1.4, not by `ChunkPrefetch`).
  `append_strip` stays in `chunk_prefetch.rs` for now (moves in
  Phase 4.2). Files: `src/chunk/chunk_prefetch.rs`.
- [x] **1.2 Change `ChunkWriter` to own `Option<Arc<Chunk>>`.**
  Replace `current_chunk_id: Option<ChunkId>` + manual counters with
  `chunk: Option<Arc<Chunk>>`. Add `write_cursor: u32` for the next
  strip index. Add `object_size: Option<u64>` +
  `strips_remaining: Option<usize>` for strip prefetch planning
  (used in Phase 3.2; stored now, no behavior yet). `bytes_in_chunk`
  stays (derived; `chunk.sealed_length` is authoritative at seal
  time). Files: `src/chunk/chunk_writer.rs`.
- [x] **1.3 Change `open` to take `Chunk` + `object_size` instead
  of `StripPlacement`.** Wrap in `Arc`, store it, set
  `write_cursor = 0`, open the first strip from `chunk.strips[0]`
  (already present from `allocate_chunk` — no prefetch wait) by
  extracting a `StripPlacement` via `extract_placement_from_chunk`
  and passing it to `EcStripWriter::new` (unchanged). Compute
  `strips_remaining` from `object_size`. Files:
  `src/chunk/chunk_writer.rs`.
- [x] **1.4 Change `continue_strip` to use `chunk.strips`.** When
  the next strip is pre-appended (in `chunk.strips`), extract its
  `StripPlacement` and pass to `EcStripWriter::new` (bridge). When
  not, call `append_chunk` internally (move `append_strip` logic
  from `chunk_prefetch.rs` — or call the existing free function).
  Files: `src/chunk/chunk_writer.rs`, `src/chunk/chunk_prefetch.rs`.
- [x] **1.5 Add `is_full()` to `ChunkWriter`.** Returns
  `bytes_in_chunk >= max_chunk_size`. Files:
  `src/chunk/chunk_writer.rs`.
- [x] **1.6 Update object layer to pass `Chunk` to `ChunkWriter`.**
  `LargeObjectWriter` and `LargeAsyncObjectWriter` receive `Chunk`
  from `ChunkPrefetch` and pass it to `ChunkWriter::open`. The
  `chunk_id` comparison for rotation now compares `chunk.id` from
  the received `Chunk`. Files: `src/writer/large_object.rs`,
  `src/writer/large_async_object.rs`.
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.**
  All 53 tests pass. `ChunkWriter` owns `Arc<Chunk>`; `ChunkPrefetch`
  streams `Chunk`; `EcStripWriter` still takes `StripPlacement`
  (bridge via `extract_placement_from_chunk`).

## Phase 2: Own-protobuf in `EcStripWriter` (remove the bridge)

- [x] **2.1 Change `EcStripWriter` to hold `Arc<Chunk>` + index,
  add accessor methods.** Replace `placement: StripPlacement` with
  `chunk: Arc<Chunk>` + `strip_index: u32`. Update `new()` to take
  `Arc<Chunk>` + index. Move `unit_bytes`, `segment(i)`,
  `disk_id(i)`, `zone_offset(i)` from `StripPlacement` onto
  `EcStripWriter` as private methods reading from
  `self.chunk.strips[self.strip_index]`. Update `push` / `finish` to
  read segments from the protobuf. Spawned parity tasks clone the
  `Arc<Chunk>` (no data clone). **Keep the current `finish()`
  behavior (join parity at strip finish) for now** — the parity
  handoff decoupling is Phase 3.1. Files:
  `src/chunk/ec_strip_writer.rs`.
- [x] **2.2 Update `ChunkWriter` to pass `Arc<Chunk>` + index to
  `EcStripWriter`.** In `open` / `continue_strip`, pass
  `Arc::clone(&chunk)` + strip index instead of extracting a
  `StripPlacement`. Remove the `extract_placement_from_chunk` bridge
  from `ChunkWriter`. Files: `src/chunk/chunk_writer.rs`,
  `src/chunk/chunk_prefetch.rs`.
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.**
  All 53 tests pass. `EcStripWriter` now shares `Arc<Chunk>`;
  `StripPlacement` still exists (in `strip.rs`) but is no longer
  used by the write path — removed in Phase 4.

## Phase 3: Move drive loop into `ChunkWriter`

- [x] **3.1 Add strip-level drive loop to `ChunkWriter::push` +
  decouple parity join.** `push` auto-rotates strips: if
  `current_strip` is full, finish it, advance `write_cursor`, open
  the next strip (from `chunk.strips` or via `append_chunk`). The
  object layer's `if is_strip_full { finish_strip }` logic moves
  here. **Parity handoff decoupling:** `EcStripWriter::finish()`
  spawns parity writes + fsyncs via `tokio::spawn` and returns
  `StripResult { parity_handles }` **without joining**. `ChunkWriter`
  collects `parity_handles` from each finished strip. `seal()` joins
  all accumulated handles before `seal_chunk` RPC. This overlaps
  strip N+1's data writes with strip N's parity writes + fsyncs
  (root design §3). Remove `ParityBatch` (no join at strip finish).
  Extract parity spawn logic (parallel writes + deduplicated fsyncs)
  from `EcStripWriter::finish()` into `parity_writer::spawn_parity_writes`
  free function in `src/chunk/parity_writer.rs` (renamed from
  `parity_batch.rs`). Files: `src/chunk/chunk_writer.rs`,
  `src/chunk/ec_strip_writer.rs`, `src/chunk/parity_batch.rs` →
  `src/chunk/parity_writer.rs`.
- [x] **3.2 Add strip prefetch to `ChunkWriter`.** Internal
  background task that appends strips to `self.chunk` ahead of
  `write_cursor`, bounded by `prealloc_depth`. Uses `object_size` +
  `strips_remaining` for planning: known-size objects stop
  pre-appending when enough strips are allocated (no
  over-allocation); unknown-size objects pre-append up to
  `strips_per_chunk`. The `append_chunk` response replaces
  `self.chunk` (Arc-swap). `ChunkWriter` owns `prefetch_handle` +
  `prefetch_rx`. Files: `src/chunk/chunk_writer.rs`.
- [x] **3.3 Simplify `LargeObjectWriter::on_data`.** Remove
  `ensure_open_strip`, `next_placement`, `start_pipeline`. `on_data`
  becomes: ensure `ChunkWriter` open (pull first `Chunk` from
  `ChunkPrefetch`, pass `object_size`), `push(buffer)`, check
  `is_full()` → rotate (seal + pull next `Chunk` + open with
  remaining size). Keep `chunk_prefetch` + `chunk_prefetch_rx` +
  `chunk_prefetch_handle` (now chunk-level, receiving `Chunk`).
  Files: `src/writer/large_object.rs`.
- [x] **3.4 Simplify `LargeAsyncObjectWriter::on_data` +
  `write_stream`.** Remove `apply_placement`,
  `on_demand_placement`, `receive_and_push`. The drive loop uses the
  simpler `push` + `is_full` → rotate flow (pull next `Chunk` from
  `ChunkPrefetch` on rotation, pass `object_size` / remaining size to
  `ChunkWriter::open`). The fetch stage stays (`run_fetch_stage`).
  Files: `src/writer/large_async_object.rs`.
- [x] **3.5 Update `finish_pipeline` / `abort_pipeline`.** Simplify
  — abort `chunk_prefetch_handle` (object layer) + strip-prefetch
  task inside `ChunkWriter`. `seal_current` stays. Files:
  `src/writer/large_object.rs`, `src/writer/large_async_object.rs`.
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.**
  Drive loop is in `ChunkWriter`. Object layer is simplified. Tests
  will need migration (Phase 4).

## Phase 4: Delete `StripPlacement` + `Location`

- [x] **4.1 Delete `StripPlacement` from `strip.rs`.** Remove the
  struct + methods. `StripWriter` enum + `StripResult` stay in
  `strip.rs` (§9 decision). Files: `src/chunk/strip.rs`.
- [x] **4.2 Move `append_strip` out of `chunk_prefetch.rs`.** The
  `append_strip` free function (used by `ChunkWriter`'s internal
  strip prefetch) moves to `chunk_writer.rs` (or private
  `chunk/alloc.rs`). `ChunkPrefetch` class stays in
  `chunk_prefetch.rs` — it now only handles chunk-level prefetch
  (`allocate_new_chunk` + streaming `Chunk` values).
  `extract_placement_from_chunk` is removed (no longer needed —
  `ChunkPrefetch` sends the full `Chunk`). Files:
  `src/chunk/chunk_prefetch.rs`, `src/chunk/chunk_writer.rs`.
- [x] **4.3 Replace `Location` with `ProtoLocation`.** Delete
  `src/location.rs`. Update `ChunkIoWriter` trait to return
  `Vec<ProtoLocation>`. Update `ChunkWriter::seal` to return
  `ProtoLocation`. Move compact encoding (`to_bytes` / `from_bytes`)
  to `crow-protocol` as `location_to_bytes` / `location_from_bytes`
  (§9 decision — `Location` is a protocol type, passed over RPC by
  other services). Files: `src/location.rs` (deleted), `src/io.rs`,
  `src/chunk/chunk_writer.rs`, `src/lib.rs`, `lib/crow-protocol/`.
- [x] **4.4 Update `lib.rs` re-exports.** Remove `Location`,
  `StripPlacement`. Keep `ChunkPrefetch`. Re-export `ProtoLocation`
  from `crow-protocol`. Files: `src/lib.rs`.
- [x] **Checkpoint: `pixi run cargo build -p crow-chunk-client`.**
  Old types gone. Tests will fail (reference old API) — fixed in
  Phase 5.

## Phase 5: Update tests

- [x] **5.1 Update `tests/large_object_writer.rs`.** Replace
  `Location` → `ProtoLocation`. Remove `StripPlacement` construction.
  Update `LargeObjectWriter` usage (no placement stream). Files:
  `tests/large_object_writer.rs`.
- [x] **5.2 Update `tests/write_stream.rs`.** Same migration. Update
  `LargeAsyncObjectWriter::write_stream` usage. Replace
  `Location` assertions with `ProtoLocation`. Files:
  `tests/write_stream.rs`.
- [x] **5.3 Update `tests/large_object_writer_e2e.rs`.** Replace
  `Location` → `ProtoLocation`. Update writer construction. Files:
  `tests/large_object_writer_e2e.rs`.
- [x] **5.4 Delete `tests/strip_test.rs`.** Tests `StripPlacement`
  methods, which no longer exist. Replace with `EcStripWriter`
  accessor tests if coverage is needed. Files: `tests/strip_test.rs`
  (deleted).
- [x] **5.5 Update `tests/common/mod.rs` if needed.** `LocalFileDiskWriter`
  implements `DiskWriter` — unaffected. Check for `Location` /
  `StripPlacement` references. Files: `tests/common/mod.rs`.
- [x] **Checkpoint: `pixi run cargo test -p crow-chunk-client
  --all-targets`.** All tests pass against the new API.

## Phase 6: New unit tests for `ChunkWriter` drive loop

- [x] **6.1 UT: `ChunkWriter` strip rotation.** Open with a `Chunk`
  containing 3 pre-appended strips. Push `data_num * 3` blocks.
  Verify 3 strips written, `bytes_in_chunk` correct, `write_cursor
  == 3`. Files: `tests/chunk_writer_test.rs` (new).
- [x] **6.2 UT: `ChunkWriter` on-demand append.** Open with 1 strip.
  Push `data_num * 2` blocks. Verify `append_chunk` called once.
  Files: `tests/chunk_writer_test.rs`.
- [x] **6.3 UT: `ChunkWriter` is_full + seal.** Push enough to
  exceed `max_chunk_size`. Verify `is_full()`, `seal_chunk` RPC,
  `ProtoLocation` fields. Files: `tests/chunk_writer_test.rs`.
- [x] **6.4 UT: `ChunkWriter` abort.** Push partial, `abort()`.
  Verify `delete_chunk` called, in-flight cancelled. Files:
  `tests/chunk_writer_test.rs`.
- [x] **6.5 UT: `ChunkWriter` empty seal.** Open, immediately seal.
  Verify `delete_chunk` (not `seal_chunk`). Files:
  `tests/chunk_writer_test.rs`.
- [x] **6.6 UT: `EcStripWriter` with owned `ChunkStrip`.** Construct
  from `ChunkStrip` protobuf, push N blocks, finish, verify parity.
  Verify accessor methods. Files: `tests/ec_strip_writer_test.rs`
  (new) or extend `tests/ec_worker_test.rs`.
- [x] **Checkpoint: `pixi run cargo test -p crow-chunk-client
  --all-targets --features test-util`.** All tests pass (existing +
  new).

## Phase 7: Lint + commit

- [x] **7.1 `pixi run rs-fmt`** — format all new/changed files.
- [ ] **7.2 `pixi run rs-lint`** — clippy `pedantic = warn`, fix up
  to 3 times.
- [ ] **7.3 Final test run.** `pixi run cargo test -p crow-chunk-client
  --all-targets` — all green.
- [ ] **7.4 Commit.** Single commit: `refactor: move drive loop into
  chunk layer, own protobuf types directly`. Verify no temp/generated
  files staged.
- [ ] **7.5 Fold design draft into root design.** Update
  `doc/design/chunkio/design-crow-chunkio.md` §3 (Write Flow), §6
  (Chunk Rotation), §7 (Error Handling) to reflect the chunk-layer
  drive loop + own-protobuf model. Delete
  `doc/working/design-chunk-layer-refactor.md` + this plan.

## File List

New files:
- `tests/chunk_writer_test.rs` — `ChunkWriter` drive-loop UTs
- `tests/ec_strip_writer_test.rs` — `EcStripWriter` with `Arc<Chunk>`
  (or extend `ec_worker_test.rs`)
- `src/chunk/alloc.rs` — `append_strip` free function (if not
  inlined into `chunk_writer.rs`)

Modified files:
- `src/chunk/chunk_writer.rs` — major rewrite (owns `Arc<Chunk>`,
  drive loop, strip prefetch, parity_handles collection + join at seal)
- `src/chunk/ec_strip_writer.rs` — holds `Arc<Chunk>` + strip_index,
  accessor methods, `finish()` calls `parity_writer::spawn_parity_writes`
- `src/chunk/parity_batch.rs` → `src/chunk/parity_writer.rs` —
  renamed; `ParityBatch` struct deleted; `spawn_parity_writes` free
  function added (spawn writes + fsyncs, return handles, no join)
- `src/chunk/chunk_prefetch.rs` — repurposed (streams `Chunk` for
  object-layer chunk rotation; `append_strip` moved out;
  `extract_placement_from_chunk` removed)
- `src/chunk/strip.rs` — slimmed (enum + result only) or deleted
- `src/chunk/chunk.rs` — updated re-exports
- `src/writer/large_object.rs` — major rewrite (simplified; keeps
  `ChunkPrefetch` for chunk-level prefetch)
- `src/writer/large_async_object.rs` — major rewrite (simplified)
- `src/io.rs` — `ChunkIoWriter` returns `Vec<ProtoLocation>`
- `src/lib.rs` — updated re-exports

Deleted files:
- `src/location.rs` — replaced by `ProtoLocation`

Modified tests:
- `tests/large_object_writer.rs`
- `tests/write_stream.rs`
- `tests/large_object_writer_e2e.rs`
- `tests/common/mod.rs` (if `Location` / `StripPlacement` referenced)

Deleted tests:
- `tests/strip_test.rs` — `StripPlacement` methods removed

## Test Checklist

Existing (must stay green):
- [ ] `tests/large_object_writer.rs` — push-mode write, Location
  array, EC parity
- [ ] `tests/write_stream.rs` — stream-mode write, chunk rotation,
  data reconstruction
- [ ] `tests/large_object_writer_e2e.rs` — E2E with real
  diskio/chunkdb
- [ ] `tests/ec_worker_test.rs` — streaming EC compute (unchanged)
- [ ] `tests/parity_batch_test.rs` → `tests/parity_writer_test.rs`
  — repurposed: test `spawn_parity_writes` (parallel write + fsync
  spawn, deduplicated fsyncs, returns handles without joining).
  `ParityBatch` join/abort tests removed.

New UTs:
- [ ] `chunk_writer_test.rs` — strip rotation, on-demand append,
  is_full + seal, abort, empty seal, **parity overlap** (finish
  returns handles without joining; seal joins all handles; verify
  strip N+1 data writes start before strip N parity completes)
- [ ] `ec_strip_writer_test.rs` — `Arc<Chunk>` + strip_index,
  accessor methods, parity correctness, **finish returns
  parity_handles without joining**
