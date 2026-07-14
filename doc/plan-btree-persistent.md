<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: crowtree Persistent Storage Refactor — Remaining Tasks

Parent: [`design-crowtree-storage.md`](design/design-crowtree-storage.md)

## Remaining Tasks

### Task 1: IoUringEngine (Stage 2 — Linux only)

**Goal**: Implement `IoUringEngine` as the production Linux I/O engine. Submits `io_uring` SQEs for read/write/fsync, completions via CQ polling in the `Reactor` event loop. This task is deferred to Stage 2 — requires Linux build and debug environment.

**Design**:
- `IoUringEngine` implements `IoEngine`:
  - `submit_read`/`submit_write` → push SQE with `IORING_OP_READ`/`IORING_OP_WRITE`, register callback in CQ.
  - `submit_fsync` → push SQE with `IORING_OP_FSYNC`.
  - Completions polled by `Reactor` event loop, callbacks invoked on CQ events.
- `BlockPageStore` dispatches to the correct extent's fd — `IoUringEngine` handles multi-fd naturally (each SQE carries its own fd).
- `ct_open`: selects `IoUringEngine` when `CROWTREE_HAVE_LIBURING` is defined.
- Build: `CMakeLists.txt` detects `liburing` dev package, defines `CROWTREE_HAVE_LIBURING`.

**Changes**:
- [ ] `crowtree/include/crowtree/io_engine.h`: Add `IoUringEngine` declaration.
- [ ] `crowtree/src/io_uring_engine.cpp` (new): `IoUringEngine` implementation.
- [ ] `crowtree/src/reactor.cpp`: Integrate `io_uring` CQ polling into event loop.
- [ ] `crowtree/src/c_api.cpp`: Select `IoUringEngine` when `CROWTREE_HAVE_LIBURING`.
- [ ] `crowtree/CMakeLists.txt`: Detect `liburing`, add `io_uring_engine.cpp`.
- [ ] `crowtree/ffi/build.rs`: Link `liburing` when available.

**Files**:
- `crowtree/include/crowtree/io_engine.h`
- `crowtree/src/io_uring_engine.cpp` (new)
- `crowtree/src/reactor.cpp`
- `crowtree/src/c_api.cpp`
- `crowtree/CMakeLists.txt`
- `crowtree/ffi/build.rs`

**Test**:
- [ ] Async read/write round-trip via `IoUringEngine` on Linux.
- [ ] Multi-extent I/O: write spanning 2+ block files, verify correct fd dispatch.
- [ ] fsync via `io_uring` (`IORING_OP_FSYNC`).
- [ ] Reactor integration: completions arrive via CQ polling.
- [ ] All existing tests pass with `IoUringEngine` on Linux.

**Note**: This task requires a Linux machine with `liburing` installed. Build and debug on Linux. Not blocking Stage 1 — `DirectIoEngine` covers all platforms.

---

### Task 2: Online block compaction via snapshot relocation

**Goal**: Reclaim sparse block files by controlling where `SpaceAllocator` places new writes during snapshot. No standalone compaction pass — sparse blocks drain naturally as dirty pages are rewritten to dense blocks.

**Approach**: Exclude gaps in sparse blocks from `SpaceAllocator`. New allocations go to dense blocks or append to fresh blocks. Over multiple snapshots, sparse blocks lose all live pages and become deletable.

**Rejected alternative**: Explicit `compact_blocks()` API that force-rewrites all live pages from sparse blocks in a single snapshot. Higher I/O burst, more complex. Kept as future extension only.

#### Design Review (validated against code)

Key findings from reviewing `persist.cpp`, `block_page_store.cpp`, `block_page_store.h`:

1. **Dirty pages have `durable_addr == kNoAddr`** — they don't have a "current block" to check. The plan's original wording ("check if the page's current block is sparse") is incorrect. The actual mechanism is simpler: **filter gaps in `SpaceAllocator`**, not check per-page. When building the allocator, exclude gaps that fall within sparse blocks. New allocations naturally land in dense blocks or fresh blocks.

2. **`SpaceAllocator` is rebuilt every snapshot** (`prepare_snapshot_locked()`, persist.cpp:335). It's a local variable, not persistent state. Gap filtering is applied during `build_allocator()` — clean, no lasting state.

3. **`SpaceAllocator` is block-unaware** — it has a flat `(addr, len)` gap list. Needs `block_size` to compute per-block free ratios and filter gaps. `BlockPageStore::block_size()` is already exposed.

4. **Block deletion needs two-generation safety** — same rule as old image cleanup (persist.cpp header comment). After anchor N commits, a block with zero live pages in anchor N might still be referenced by anchor N-1 (crash fallback). Delete only after anchor N+1 commits and the block still has zero live pages.

5. **Only applies to array-of-blocks mode** (`open_blocks`). Single-medium (`open_mem`, `open`) and `TextPageStore` don't have multiple blocks.

6. **Per-block live page count** — after snapshot commit, scan the new segment directory to count live pages per block. Blocks with zero live pages are deletion candidates. O(live_pages), bounded.

#### Task Breakdown

- [x] **2a**: Add §2.5 "Block Compaction" to `doc/design/design-crowtree-storage.md`
  - Sparse block detection: per-block free ratio from gap list, threshold (70%)
  - Gap filtering in `SpaceAllocator`: exclude gaps in sparse blocks during `build_allocator()`
  - Block deletion: two-generation rule, per-block live page count from segment directory
  - Crash safety: identical to normal snapshot + two-generation deletion rule
  - Cost: zero extra I/O, O(gaps) for free ratio computation, O(live_pages) for deletion check
  - Scope: array-of-blocks mode only

- [x] **2b**: Add `block_size` parameter to `build_allocator()` and sparse-block gap filtering
  - `persist.cpp`: `build_allocator()` takes optional `block_size` (0 = no filtering)
  - Compute per-block free ratio from gap list
  - Exclude gaps in blocks above threshold from the allocator's gap list
  - When `block_size == 0` (single-medium, TextPageStore), no filtering — current behavior unchanged

- [ ] **2c**: Add `delete_block()` to `BlockPageStore`
  - `block_page_store.h`: `Status delete_block(uint32_t block_idx)`
  - `block_page_store.cpp`: close fd, remove file from `extents_`, unlink `.blk-{NNNN}` file
  - Safe only after snapshot commit confirms zero live pages in the block

- [ ] **2d**: Integrate block deletion into snapshot commit path
  - `persist.cpp`: after `commit_prepared_snapshot()`, scan segment directory for per-block live page count
  - Track blocks with zero live pages across two snapshots (two-generation rule)
  - Call `BlockPageStore::delete_block()` for blocks empty in both current and previous snapshot
  - Only when `PageStore` is `BlockPageStore` with array-of-blocks mode

- [ ] **2e**: Tests
  - Write enough data to fill 3+ blocks with small block_size
  - Delete half the keys, run GC, snapshot → verify sparse block gaps are not reused
  - Continue writing + snapshotting → verify sparse block drains and is eventually deleted
  - Crash recovery test: verify no reference to deleted block after reopen
  - Verify single-medium and TextPageStore paths are unaffected

#### Gaps (resolved)

**Gap 1**: ~~`SpaceAllocator` is in `persist.cpp` anonymous namespace~~ — **Resolved**: pass `block_size` as parameter to `build_allocator()`. 0 = no filtering.

**Gap 2**: ~~`PageStore` base class doesn't expose `block_size()`~~ — **Resolved**: add `virtual uint64_t block_size() const { return 0; }` to `PageStore`, overridden by `BlockPageStore`.

**Gap 3**: ~~Threshold configurability~~ — **Resolved**: compile-time constant for now.

**Gap 4**: ~~Per-block live page counting~~ — **No gap**: compute from `PreparedSnapshot` in-memory, no disk re-read.

---


## Execution Order

- [ ] **Task 1** — IoUringEngine (Stage 2, Linux only, builds on Stage 1 IoEngine)
- [ ] **Task 2** — Online block compaction via snapshot relocation (2a→2b→2c→2d→2e, builds on Stage 1 array-of-blocks)

