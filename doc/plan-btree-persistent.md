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

### Task 2: Online block compaction via snapshot relocation — design only

**Goal**: Design how block files are compacted by integrating page relocation into the normal snapshot path. No standalone compaction pass — sparse blocks drain naturally as dirty pages are rewritten during snapshots. Produce a design document, not code.

**Problem**:

After GC runs (`collect_garbage` in `crowtree.cpp`), dead pages become gaps in `SpaceAllocator`. Over time, a block file may have most of its space as gaps — physically occupying disk but logically free. The question is: how to reclaim this space without a separate compaction operation.

**Chosen approach — online relocation during snapshot**:

Snapshot already does what compaction needs: it holds `write_mutex_`, writes pages to `PageStore`, updates mapping table slot words, and commits atomically via anchor + segment images + segment directory. The key insight is that compaction is just *choosing where pages land* during snapshot.

**Design**:

1. **Sparse block detection**:
   - After GC, compute per-block free ratio: `gaps / block_size`.
   - Blocks with free ratio above a threshold (e.g., 70%) are marked as relocation candidates.

2. **Relocation during snapshot write**:
   - When the snapshot path writes a dirty page, check if the page's current block is a relocation candidate.
   - If yes, allocate a new address in a fresh (dense) block via `SpaceAllocator` instead of reusing the old address.
   - The page is written to the new location; the old address becomes a gap.
   - The mapping table slot word is updated to the new address — same as a normal snapshot write, just to a different address.

3. **What gets relocated**:
   - Only **dirty pages** are rewritten during snapshot. Clean pages keep their old address.
   - This means single snapshot only compacts pages that were modified since last snapshot.
   - Over multiple snapshots, as pages are touched and rewritten, sparse blocks gradually drain.
   - For high-churn workloads, compaction is fast (most pages are dirty each snapshot).
   - For low-churn workloads, blocks are naturally dense — compaction rarely needed.

4. **Block deletion**:
   - After snapshot commit, check if any block has zero live pages (all gaps).
   - If so, delete the block file. This is safe — the snapshot is durable, all live pages have new addresses.
   - `SpaceAllocator` already tracks gaps; a per-block live-page count can be derived from the gap map.

5. **Crash safety**:
   - Identical to normal snapshot crash safety. If the process dies mid-snapshot:
     - Old blocks still exist → old addresses still valid.
     - New block has copied pages → new addresses valid.
     - Recovery uses the anchor's snapshot to determine which addresses are live.
   - No additional crash safety logic needed — relocation is just a normal snapshot write to a different address.

6. **Cost**:
   - Zero additional I/O beyond what snapshot already does. The page would be written anyway — we just choose a different destination address.
   - The only overhead is the sparse-block check (a set lookup per dirty page).

7. **Supplement — explicit compaction for immediate space reclaim**:
   - If disk space is urgent and waiting for natural drain is too slow, an explicit `compact_blocks()` API can be added later.
   - It would force-rewrite all live pages from sparse blocks (not just dirty ones) in a single snapshot.
   - This is a future extension, not part of the initial design.

**Deliverable**: A design section in `doc/design/design-crowtree-storage.md` (new §2.5 "Block Compaction") documenting the online relocation approach. No code changes.

**Files**:
- `doc/design/design-crowtree-storage.md` (add §2.5)

**Test**: N/A (design only)

---


## Execution Order

- [ ] **Task 1** — IoUringEngine (Stage 2, Linux only, builds on Stage 1 IoEngine)
- [ ] **Task 2** — Online block compaction via snapshot relocation (design only, builds on Stage 1 array-of-blocks)

