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

**Goal**: Design how sparse block files are reclaimed by relocating dirty pages to dense blocks during normal snapshot writes. No standalone compaction pass.

**Approach**: Online relocation during snapshot — when writing a dirty page, if its current block is sparse (>70% gaps), allocate a new address in a fresh block instead of reusing the old one. Zero extra I/O; crash safety identical to normal snapshot. Sparse blocks drain gradually over multiple snapshots as pages are touched. Empty blocks are deleted after snapshot commit.

**Rejected alternative**: Explicit `compact_blocks()` API that force-rewrites all live pages from sparse blocks in a single snapshot. Higher I/O burst, more complex. Kept as future extension only.

**Deliverable**: Design section in `doc/design/design-crowtree-storage.md` (§2.5 "Block Compaction") covering sparse detection, relocation mechanics, block deletion, crash safety, and cost analysis.

**Files**:
- `doc/design/design-crowtree-storage.md` (add §2.5)

**Test**: N/A (design only)

---


## Execution Order

- [ ] **Task 1** — IoUringEngine (Stage 2, Linux only, builds on Stage 1 IoEngine)
- [ ] **Task 2** — Online block compaction via snapshot relocation (design only, builds on Stage 1 array-of-blocks)

