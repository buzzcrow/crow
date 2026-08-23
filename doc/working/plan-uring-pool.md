<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R109: Multi-Pipeline io_uring Engine (`DiskIOUring`) Plan

Design draft: `doc/working/design-uring-pool.md`.
Backlog doc: `doc/backlog/R109-common-diskio-uring.md`.
Goal: replace `Reactor` with `DiskIOUring` (multi-pipeline, fd→pipeline
routing, shared poll threads, batch submit, kernel cancel-by-fd), thin
out `UringEngine`, update `BlockAsyncPageStore` + FFI, and migrate all
tree + diskio tests to the new API.

## Phase 1 — `DiskIOUring` core (crow-common)

- [ ] **1.1 Create `diskio_uring.h`**: New header
  `lib/crow-common/cpp/include/crow-common/diskio_uring.h`. Defines
  `DiskIOUring` class with `Topology`, `PipelineConfig`,
  `PollThreadGroupConfig`, `PollingMode` (reuse from reactor.h),
  `HybridConfig`, `SqpollConfig`. Public API: `register_fd`,
  `unregister_fd`, `submit_read`/`submit_write`/`submit_fsync`,
  `cancel_fd`, `in_flight_count`, `eventfds`. Internal `Pipeline`
  struct (one `io_uring`, lock-free SQE claim from reactor.cpp) and
  `PollThread` struct (multi-CQ polling loop, eventfd+epoll wakeup,
  busy-poll↔event-wait transition). `fd_table` vector sized to
  `ulimit -n` (queried via `sysconf(_SC_OPEN_MAX)`). Files:
  `lib/crow-common/cpp/include/crow-common/diskio_uring.h`.
- [ ] **1.2 Create `diskio_uring.cpp`**: Implement `DiskIOUring`.
  Move the lock-free SQE claim mechanism, deferred-delete free list,
  and three polling modes from `reactor.cpp` into `Pipeline`. Add
  `PollThread` with multi-CQ `io_uring_peek_cqe`/`iouring_wait_cqe`
  loop, `thread_sleeping_` protocol, eventfd write coalescing. Add
  `fd_table` routing, `cancel_fd` via `IORING_ASYNC_CANCEL_FD` (with
  `-ENOSYS` fallback for kernel < 6.0). Add `eventfds()` returning
  one eventfd per pipeline. Files:
  `lib/crow-common/cpp/src/diskio_uring.cpp`.
- [ ] **1.3 Update CMakeLists.txt**: Add `diskio_uring.cpp` to the
  liburing-gated source list in
  `lib/crow-common/cpp/CMakeLists.txt`. When liburing is not found,
  exclude `diskio_uring.cpp` (same as `reactor.cpp`). Files:
  `lib/crow-common/cpp/CMakeLists.txt`.
- [ ] **1.4 Add `DiskIOUring` unit tests**: New test file
  `lib/crow-common/cpp/tests/diskio_uring_test.cpp`. Tests: single-
  pipeline basic submit+complete, multi-pipeline explicit routing,
  auto-assign by load, auto-assign stickiness, unregistered fd→p0,
  batch coalescing, eventfd write coalescing, busy-poll zero-eventfd,
  client-side cancel flag, `cancel_fd` via `IORING_ASYNC_CANCEL_FD`,
  `cancel_fd` doesn't affect other fds, multi-CQ polling (1 thread 2
  pipelines), CQ isolation (2 threads 2 pipelines), busy-poll→event-
  wait transition. Files:
  `lib/crow-common/cpp/tests/diskio_uring_test.cpp`.

## Phase 2 — `UringEngine` thin-out (diskio)

- [ ] **2.1 Update `uring_engine.h`**: Replace `Reactor reactor_` with
  `DiskIOUring uring_`. Remove `InFlightShard`, `shards_`,
  `shard()`, `kInFlightShards`. Constructor takes
  `DiskIOUring::Topology` (or keeps `ring_entries` for backward compat
  → builds a 1-pipeline topology). `submit_*` delegates to
  `uring_.submit_*(disk->fd(), ...)`. `cancel_disk` delegates to
  `uring_.cancel_fd(disk->fd())`. Remove `in_flight_count(DiskId)`
  (breaking API change — noted in design doc). Files:
  `app/crow-diskio/src/engine/uring/uring_engine.h`.
- [ ] **2.2 Update `uring_engine.cpp`**: Implement the thinned
  `UringEngine`. `submit_write`/`submit_read`/`submit_fsync` call
  `uring_.submit_*` with `disk->fd()` directly (no per-disk tracking).
  `cancel_disk` calls `uring_.cancel_fd(disk->fd())`. O_DIRECT
  alignment validation stays in `UringEngine` (it's diskio's
  responsibility, not `DiskIOUring`'s). Files:
  `app/crow-diskio/src/engine/uring/uring_engine.cpp`.
- [ ] **2.3 Update diskio tests**: Migrate
  `app/crow-diskio/tests/uring_engine_test.cpp` and
  `app/crow-diskio/tests/sq_full_test.cpp` to the new API. Remove
  `in_flight_count(DiskId)` assertions (replaced by
  `uring_.in_flight_count(fd)` if needed, or drop the test). Update
  `cancel_disk` tests to verify `cancel_fd` behavior. Add multi-
  pipeline topology tests (NVMe 1-pipeline/disk, HDD shared, mixed).
  Files: `app/crow-diskio/tests/uring_engine_test.cpp`,
  `app/crow-diskio/tests/sq_full_test.cpp`.

## Phase 3 — `BlockAsyncPageStore` + btree wiring

- [ ] **3.1 Add `all_extent_fds()` to `BlockPageStore`**: New method
  returning fds of all live, non-deleted extents (array-of-blocks) or
  the single medium's fd (single-file mode). Unlike `dirty_fds()`,
  does not clear the dirty flag. Files:
  `lib/crow-tree/include/crow-tree/block_page_store.h`,
  `lib/crow-tree/src/block_page_store.cpp`.
- [ ] **3.2 Update `BlockAsyncPageStore`**: Constructor takes
  `DiskIOUring*` instead of `Reactor*`. `submit_read`/`submit_write`
  route through `uring_->submit_*(fd, ...)` where fd comes from
  `store_->fd_for_offset`. `submit_fsync` uses `store_->dirty_fds()`
  + `uring_->submit_fsync(fd, ...)` per fd. `cancel(op_id)` becomes a
  no-op override (base class keeps the virtual). Cross-extent write
  split preserved (`WriteState` fan-out unchanged). Files:
  `lib/crow-tree/include/crow-tree/async_page_store.h`,
  `lib/crow-tree/src/block_async_page_store.cpp`.
- [ ] **3.3 Update `options.h`**: Rename `async_reactor` to
  `async_uring`, change type from `Reactor*` to `DiskIOUring*`.
  Files: `lib/crow-tree/include/crow-tree/options.h`.
- [ ] **3.4 Update `c_api.cpp` ct_tree struct**: Rename `reactor` to
  `uring`, change type from `unique_ptr<Reactor>` to
  `unique_ptr<DiskIOUring>`. Files:
  `lib/crow-tree/src/c_api.cpp`.
- [ ] **3.5 Update `ct_open` in `c_api.cpp`**: Construct
  `DiskIOUring` with 1-pipeline topology (`{256, Hybrid}`,
  `poll_thread_groups = {{0}}`). Register all extent fds via
  `store->all_extent_fds()` + `uring->register_fd(fd)`. Wire
  `o.async_uring` and `o.async_page_store`. Files:
  `lib/crow-tree/src/c_api.cpp`.
- [ ] **3.6 Update `ct_reactor_eventfd` → `ct_uring_eventfds`**:
  Replace single-eventfd function with
  `ct_uring_eventfds(const ct_tree*, int32_t*, size_t)` — fills
  caller-allocated array, returns count. Update
  `lib/crow-tree/include/crow-tree/c_api.h` declaration. Files:
  `lib/crow-tree/include/crow-tree/c_api.h`,
  `lib/crow-tree/src/c_api.cpp`.
- [ ] **3.7 Update tree reactor tests**: Migrate
  `lib/crow-tree/tests/unit/reactor_test.cpp` to use `DiskIOUring`
  instead of `Reactor`. Rename test fixture from `Reactor` to
  `DiskIOUring`. Update `BlockAsyncPageStore` tests to construct with
  `DiskIOUring*`. Add multi-pipeline `DiskIOUring` tests (2 pipelines,
  shared poll thread). Add `cancel_fd` test. Add `all_extent_fds()`
  test. Files: `lib/crow-tree/tests/unit/reactor_test.cpp` → rename
  to `diskio_uring_test.cpp` (or keep filename, update content).
- [ ] **3.8 Update tree integration tests**: Migrate
  `lib/crow-tree/tests/integration/async_get_test.cpp` and
  `async_scan_test.cpp` — replace `ct_reactor_eventfd` calls with
  `ct_uring_eventfds`. The async path still works the same way (poll-
  until-done); only the eventfd query changes. `r6_test.cpp` line 39
  reference update. Files:
  `lib/crow-tree/tests/integration/async_get_test.cpp`,
  `lib/crow-tree/tests/integration/async_scan_test.cpp`,
  `lib/crow-tree/tests/integration/r6_test.cpp`.
- [ ] **3.9 Update CMakeLists.txt**: Update
  `lib/crow-tree/CMakeLists.txt` to exclude
  `block_async_page_store.cpp` when liburing not found (already
  done) — no change needed if `DiskIOUring` comes from crowcommon.
  Update test exclusion if reactor_test.cpp is renamed. Files:
  `lib/crow-tree/CMakeLists.txt`.

## Phase 4 — FFI multi-eventfd pump (Rust)

- [ ] **4.1 Update `sys.rs`**: Replace `ct_reactor_eventfd` binding
  with `ct_uring_eventfds(t: *const ct_tree, fds: *mut i32, max:
  size_t) -> size_t`. Files: `lib/crow-tree/ffi/src/sys.rs`.
- [ ] **4.2 Update `tree.rs`**: Replace `eventfd_pump:
  OnceLock<Option<EventfdPump>>` with `eventfd_pumps:
  OnceLock<Vec<EventfdPump>>`. `reactor_eventfd()` →
  `uring_eventfds()` returning `Vec<RawFd>`. `eventfd_notify()`
  spawns one `EventfdPump` per eventfd, all sharing one
  `Arc<Notify>`. `is_reactor_available()` checks non-empty eventfd
  list. `Drop` aborts all pump tasks. Files:
  `lib/crow-tree/ffi/src/tree.rs`.
- [ ] **4.3 Update `reactor.rs` (FFI module)**: `EventfdPump` struct
  unchanged (notify + task). Add helper to query all eventfds via
  `ct_uring_eventfds`. Files: `lib/crow-tree/ffi/src/reactor.rs`.
- [ ] **4.4 Update FFI tests**: If there are FFI tests referencing
  `ct_reactor_eventfd`, update them. Check
  `lib/crow-tree/ffi/tests/` or `lib/crow-tree/ffi/src/` for test
  modules. Files: `lib/crow-tree/ffi/` (as found).

## Phase 5 — diskio server topology wiring

- [ ] **5.1 Build topology from disk classification**: In the diskio
  server startup, build `DiskIOUring::Topology` per design doc §7.3:
  NVMe → one pipeline/disk (Sqpoll), SATA → one pipeline/4-8 disks
  (Hybrid), HDD → one shared Hybrid pipeline (entries=2048). Register
  each disk's fd to its pipeline. `attach_wq = true` when ≥2 Sqpoll
  pipelines. Files: `app/crow-diskio/src/server/` (find the startup
  wiring point).
- [ ] **5.2 Update `IoEngine` interface if needed**: Check if
  `IoEngine` needs changes for the new `UringEngine` constructor
  signature. Files: `app/crow-diskio/src/engine/io_engine.h`.

## Phase 6 — Cleanup + old `Reactor` removal

- [ ] **6.1 Remove old `Reactor`**: Delete `reactor.h` and
  `reactor.cpp` from crow-common. Update all remaining references
  (R66 WAL design doc reference, any stray includes). Files:
  `lib/crow-common/cpp/include/crow-common/reactor.h` (delete),
  `lib/crow-common/cpp/src/reactor.cpp` (delete).
- [ ] **6.2 Remove old reactor tests**: Delete
  `lib/crow-common/cpp/tests/reactor_batch_test.cpp` and
  `reactor_polling_test.cpp` (replaced by `diskio_uring_test.cpp`).
  Files: `lib/crow-common/cpp/tests/reactor_batch_test.cpp` (delete),
  `lib/crow-common/cpp/tests/reactor_polling_test.cpp` (delete).
- [ ] **6.3 Update CMakeLists.txt exclusions**: Remove `reactor.cpp`
  exclusion logic, replace with `diskio_uring.cpp` exclusion. Files:
  `lib/crow-common/cpp/CMakeLists.txt`.

## File list

- `lib/crow-common/cpp/include/crow-common/diskio_uring.h` — new
- `lib/crow-common/cpp/src/diskio_uring.cpp` — new
- `lib/crow-common/cpp/tests/diskio_uring_test.cpp` — new
- `lib/crow-common/cpp/include/crow-common/reactor.h` — delete
- `lib/crow-common/cpp/src/reactor.cpp` — delete
- `lib/crow-common/cpp/tests/reactor_batch_test.cpp` — delete
- `lib/crow-common/cpp/tests/reactor_polling_test.cpp` — delete
- `lib/crow-common/cpp/CMakeLists.txt` — update source exclusion
- `app/crow-diskio/src/engine/uring/uring_engine.h` — thin out
- `app/crow-diskio/src/engine/uring/uring_engine.cpp` — thin out
- `app/crow-diskio/tests/uring_engine_test.cpp` — migrate to new API
- `app/crow-diskio/tests/sq_full_test.cpp` — migrate to new API
- `lib/crow-tree/include/crow-tree/block_page_store.h` — add `all_extent_fds()`
- `lib/crow-tree/src/block_page_store.cpp` — implement `all_extent_fds()`
- `lib/crow-tree/include/crow-tree/async_page_store.h` — `DiskIOUring*` ctor
- `lib/crow-tree/src/block_async_page_store.cpp` — route via `DiskIOUring`
- `lib/crow-tree/include/crow-tree/options.h` — `async_reactor` → `async_uring`
- `lib/crow-tree/include/crow-tree/c_api.h` — `ct_uring_eventfds` declaration
- `lib/crow-tree/src/c_api.cpp` — `ct_tree` struct + `ct_open` + `ct_uring_eventfds`
- `lib/crow-tree/tests/unit/reactor_test.cpp` — migrate to `DiskIOUring`
- `lib/crow-tree/tests/integration/async_get_test.cpp` — `ct_uring_eventfds`
- `lib/crow-tree/tests/integration/async_scan_test.cpp` — `ct_uring_eventfds`
- `lib/crow-tree/tests/integration/r6_test.cpp` — reference update
- `lib/crow-tree/CMakeLists.txt` — test exclusion update
- `lib/crow-tree/ffi/src/sys.rs` — `ct_uring_eventfds` binding
- `lib/crow-tree/ffi/src/tree.rs` — multi-eventfd pump
- `lib/crow-tree/ffi/src/reactor.rs` — helper update
- `app/crow-diskio/src/server/` — topology wiring (find exact file)

## Test checklist

### Unit tests (crow-common `DiskIOUring`)

- [ ] Single-pipeline basic submit + complete (read/write/fsync)
- [ ] Multi-pipeline explicit routing (register_fd with pipeline index)
- [ ] Auto-assign distributes by load (lowest in-flight)
- [ ] Auto-assign is sticky (same fd always routes to same pipeline)
- [ ] Unregistered fd routes to pipeline 0 with warning
- [ ] Batch coalescing under burst (≤2 io_uring_enter for 32 concurrent)
- [ ] Eventfd write coalescing (1 eventfd write for 100 concurrent)
- [ ] Busy-poll mode — zero eventfd writes on submit
- [ ] Client-side callback suppression (cancel flag, no UAF, ASan clean)
- [ ] `cancel_fd` via `IORING_ASYNC_CANCEL_FD` (all callbacks -ECANCELED)
- [ ] `cancel_fd` does not affect other fds on same pipeline
- [ ] Multi-CQ polling: 2 pipelines, 1 thread, both CQs drained
- [ ] CQ isolation: 2 pipelines, 2 threads, CQE by correct thread
- [ ] Busy-poll → event-wait transition (epoll_wait appears after idle)

### Unit tests (tree `BlockAsyncPageStore` + `DiskIOUring`)

- [ ] Single-extent write via `DiskIOUring` round-trips
- [ ] Cross-extent write split (2 extents, both CQEs, one callback)
- [ ] Cross-extent write teardown via `WriteState` cancel flag (ASan clean)
- [ ] `cancel(op_id)` is a no-op (callback still fires)
- [ ] `all_extent_fds()` returns all live fds (not just dirty)
- [ ] `BlockAsyncPageStore` fsync via `DiskIOUring` per-fd

### Unit tests (diskio `UringEngine` thinned)

- [ ] O_DIRECT alignment validation (size=100 → -EINVAL)
- [ ] `cancel_disk` delegates to `cancel_fd` (all callbacks -ECANCELED)
- [ ] Write/read/fsync round-trip via thinned `UringEngine`
- [ ] Multiple concurrent writes all complete
- [ ] Null disk returns error

### Integration tests (tree async via `DiskIOUring`)

- [ ] `AsyncGet` miss after eviction completes via `DiskIOUring`
- [ ] `AsyncFlushSnapshot` flush + snapshot round-trip
- [ ] `AsyncSnapshot` block backend round-trip via `DiskIOUring`
- [ ] `ct_uring_eventfds` returns correct count + fds

### E2E tests (diskio multi-pipeline topology)

- [ ] NVMe topology (2 disks, 2 pipelines, bad-disk isolation)
- [ ] HDD topology (3 disks, 1 shared pipeline, bad-disk isolation)
- [ ] Mixed topology (NVMe + HDD, 2 pipelines, no cross-routing)
- [ ] Shared poll thread for 2 pipelines (no CQE starvation)
- [ ] Batch submit under load (syscall count ≈ 100, not 3200)

### E2E tests (btree async via `DiskIOUring`)

- [ ] `AsyncCrowtree::get` demand-load miss via `DiskIOUring`
- [ ] Multi-pipeline btree (2 pipelines, both pumps, shared Notify)
- [ ] Pump spawn failure resilience (latency regression, no hang)

### E2E tests (FFI multi-eventfd pump)

- [ ] 2-pipeline uring, 2 pumps, both share one Notify
- [ ] Pump spawn failure → future still completes

### Test commands

- `pixi run test-tree-ct` — tree C++ tests (reactor_test, async_get, async_scan)
- `pixi run test-tree-ffi` — FFI tests (multi-eventfd pump)
- `pixi run test-diskio-ct` — diskio C++ tests (uring_engine, sq_full)
- `pixi run test-common` — crow-common tests (diskio_uring_test)
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
- `clang-format --dry-run --Werror` (changed `.cpp`/`.h`)
- `tree-lint` (clang-tidy, changed C++)
