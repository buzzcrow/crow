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

### Task 2: Block compaction / merge — analysis & design (no implementation)

**Goal**: Analyze whether and how block files should be merged/compacted after GC creates significant free space within blocks. Produce a design document, not code. This is a follow-up to the array-of-blocks design (Stage 1, Task 3).

**Problem statement**:

After GC runs (`collect_garbage` in `crowtree.cpp`), dead pages are retired and their addresses become gaps in `SpaceAllocator`. Over time, a block file may have most of its space as gaps — logically free but physically occupying disk. The question is: should we merge sparsely-used blocks into denser ones, and if so, how?

**Core challenge — page relocation**:

Merging blocks requires moving live pages to new locations. This changes page addresses, which are stored in the mapping table's slot words (`slot_word::unloaded_iu_index`). Every relocated page's slot word must be updated. This is not a simple file-level operation — it touches the entire mapping table.

**Design analysis**:

1. **When to trigger compaction**:
   - After GC, compute per-block free ratio: `gaps / block_size`.
   - If a block's free ratio exceeds a threshold (e.g., 70%), it's a compaction candidate.
   - Compaction should be batched — don't compact one block at a time. Collect N sparse blocks, allocate one new dense block, copy live pages, update mapping table, then delete old blocks.

2. **Page relocation mechanics**:
   - For each live page in a candidate block:
     a. Allocate new address in `SpaceAllocator` (in the new dense block).
     b. `read_at(old_addr)` → `write_at(new_addr)` — copy the page blob.
     c. Update the mapping table slot word: `slot_word::unloaded_iu_index` → new address.
     d. The old address becomes a gap.
   - Segment images and segment directory may also need relocation if they reside in candidate blocks.

3. **Mapping table update**:
   - The mapping table (`mapping_`) stores slot words in segments. Each slot word encodes `(iu_index, iu_count)` for unloaded pages.
   - Relocating a page means updating its slot word's `iu_index` to the new address.
   - If the page is resident (in-memory), the slot word holds a pointer — no address change needed until eviction.
   - **This is the expensive part**: scanning all segments to find and update slot words for relocated pages.
   - Optimization: build a relocation map `{old_addr → new_addr}` before starting, then walk segments once.

4. **Crash safety**:
   - Compaction must be crash-safe. If the process dies mid-compaction:
     - Old blocks still exist (not yet deleted) → old addresses still valid.
     - New block has copied pages → new addresses valid.
     - The mapping table may have a mix of old and new addresses.
     - On recovery, the anchor's snapshot determines which addresses are live. If compaction wasn't committed (no new snapshot), old addresses are used.
   - **Approach**: compaction is a multi-step operation that completes atomically with a new snapshot:
     1. Copy live pages to new block.
     2. Update mapping table slot words.
     3. Write new snapshot (anchor + segment images + segment directory).
     4. After snapshot is durable, delete old blocks.
   - Steps 1-3 are the same as a normal snapshot — the compaction just changes *where* pages live before snapshotting.

5. **Interaction with ongoing I/O**:
   - Compaction should hold `write_mutex_` (same as GC and snapshot).
   - Resident pages are unaffected — they're in memory. Only unloaded (on-disk) pages have addresses that change.
   - After compaction, resident pages' eventual eviction writes to the new address (slot word already updated).

6. **Cost vs benefit**:
   - Compaction reads all live pages from sparse blocks + writes them to a dense block = I/O cost proportional to live data.
   - Benefit: frees disk space (deletes sparse block files).
   - For write-heavy workloads with high churn, compaction reclaims significant space.
   - For read-heavy or append-mostly workloads, blocks are naturally dense — compaction rarely needed.
   - **Recommendation**: implement as an explicit operator-triggered operation (not automatic), similar to LSM compaction triggers. Add a `compact_blocks()` API that the operator calls when disk usage is high.

7. **Alternative: online relocation during snapshot**:
   - Instead of a separate compaction pass, integrate relocation into the normal snapshot path.
   - During snapshot, if a page's current block is sparse, relocate it to a denser block as part of the snapshot write.
   - This amortizes compaction cost across snapshots — no separate I/O burst.
   - Risk: increases snapshot latency unpredictably. Better as a follow-up optimization.

**Deliverable**: A design section in `doc/design/design-crowtree-storage.md` (new §2.5 "Block Compaction") documenting the analysis above. No code changes in this plan.

**Files**:
- `doc/design/design-crowtree-storage.md` (add §2.5)

**Test**: N/A (design only)

---


## Execution Order

- [ ] **Task 1** — IoUringEngine (Stage 2, Linux only, builds on Stage 1 IoEngine)
- [ ] **Task 2** — Block compaction / merge analysis & design (no implementation, builds on Stage 1 array-of-blocks)

