<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskio — Disk IO Engine (R105) Plan

- **Design draft**: [`doc/working/design-diskio-disk-io-engine.md`](design-diskio-disk-io-engine.md)
- **Backlog doc**: [`doc/backlog/R105-diskio-disk-io-engine.md`](../backlog/R105-diskio-disk-io-engine.md)
- **Goal**: Implement the per-node disk IO engine (io_uring + pwrite/pread
  fallback) with RPC service + Rust client, sharing a lifted reactor in
  crow-common.

## Phase 1: Reactor Lift to crow-common (work item 1a)

- [ ] **1a-1: Move reactor.h to crow-common** — relocate
  `lib/crow-tree/include/crow-tree/reactor.h` to
  `lib/crow-common/cpp/include/crow-common/reactor.h`. Rename namespace
  `crow::tree` → `crow::common`. Rename guard `CROW_TREE_HAVE_LIBURING`
  → `CROW_HAVE_LIBURING`. Update the `#error` message.
  Files: `lib/crow-common/cpp/include/crow-common/reactor.h` (new),
  `lib/crow-tree/include/crow-tree/reactor.h` (delete).
- [ ] **1a-2: Move reactor.cpp to crow-common** — relocate
  `lib/crow-tree/src/reactor.cpp` to
  `lib/crow-common/cpp/src/reactor.cpp`. Update include path +
  namespace. Change `set_current_thread_name("ct-reactor")` to
  `"cr-reactor"`.
  Files: `lib/crow-common/cpp/src/reactor.cpp` (new),
  `lib/crow-tree/src/reactor.cpp` (delete).
- [ ] **1a-3: Update crow-common CMakeLists.txt** — add liburing
  `find_path`/`find_library`, conditional source exclusion (when
  liburing not found), `CROW_HAVE_LIBURING` compile definition, and
  liburing link. Mirror the pattern from crow-tree's CMakeLists.txt.
  Files: `lib/crow-common/cpp/CMakeLists.txt`.
- [ ] **1a-4: Update crow-tree CMakeLists.txt** — remove liburing
  `find_path`/`find_library` + conditional link + source exclusion
  (reactor.cpp, block_async_page_store.cpp). Remove
  `CROW_TREE_HAVE_LIBURING` definition. crow-tree now gets liburing
  transitively via crowcommon. Keep the source exclusion for
  `block_async_page_store.cpp` when liburing is not available (it
  still uses the reactor, which is now in crow-common but still
  Linux-only). The exclusion condition changes to check
  `CROW_HAVE_LIBURING` (propagated from crowcommon).
  Files: `lib/crow-tree/CMakeLists.txt`.
- [ ] **1a-5: Update crow-tree source includes + namespace** — change
  all `#include "crow-tree/reactor.h"` →
  `#include "crow-common/reactor.h"`, `crow::tree::Reactor` →
  `crow::common::Reactor`, `CROW_TREE_HAVE_LIBURING` →
  `CROW_HAVE_LIBURING` in all crow-tree source + header files.
  Files: `lib/crow-tree/include/crow-tree/crow-tree.h`,
  `lib/crow-tree/include/crow-tree/options.h`,
  `lib/crow-tree/src/crow-tree.cpp`,
  `lib/crow-tree/src/c_api.cpp`,
  `lib/crow-tree/src/persist.cpp`,
  `lib/crow-tree/src/block_async_page_store.cpp`.
- [ ] **1a-6: Update crow-tree tests** — same include/namespace/guard
  changes in test files.
  Files: `lib/crow-tree/tests/unit/reactor_test.cpp`,
  `lib/crow-tree/tests/integration/async_get_test.cpp`.
- [ ] **1a-7: Update crow-tree-ffi build.rs** — change
  `CROW_TREE_HAVE_LIBURING` → `CROW_HAVE_LIBURING` in the build script
  (the define that gates the FFI reactor code).
  Files: `lib/crow-tree/ffi/build.rs`.
- [ ] **1a-8: Verify build + tests** — `pixi run test-tree-ct`,
  `pixi run test-tree-ffi`. All must pass with no behavior change.

## Phase 2: Polling Modes (work item 1b)

- [ ] **1b-1: Add PollingMode config to Reactor** — add
  `PollingMode` enum, `HybridConfig`, `SqpollConfig` structs, and
  constructor overload to `reactor.h`. Store mode + config in private
  members.
  Files: `lib/crow-common/cpp/include/crow-common/reactor.h`,
  `lib/crow-common/cpp/src/reactor.cpp`.
- [ ] **1b-2: Implement Hybrid mode in run()** — busy-poll via
  `io_uring_peek_cqe` with `busy_poll_budget` threshold, transition
  to `io_uring_submit_and_wait` event-wait. Track busy-poll vs
  wait-mode iteration counter.
  Files: `lib/crow-common/cpp/src/reactor.cpp`.
- [ ] **1b-3: Implement Sqpoll mode** —
  `io_uring_queue_init_flags` with `IORING_SETUP_SQPOLL`, set
  `sq_thread_idle`, detect `IORING_SQ_NEED_WAKEUP` + call
  `io_uring_enter(IORING_ENTER_SQ_WAKEUP)`.
  Files: `lib/crow-common/cpp/src/reactor.cpp`.
- [ ] **1b-4: Add polling mode unit tests** — Hybrid busy-poll
  counter test, Sqpoll syscall-elimination test.
  Files: `lib/crow-common/cpp/tests/reactor_polling_test.cpp` (new).

## Phase 3: Batched SQE Submission (work item 1c)

- [ ] **1c-1: Refactor submit_locked for batched submit** — remove
  per-SQE `io_uring_submit()` call; set `pending_submit_` atomic
  flag instead. Add `pending_submit_` member.
  Files: `lib/crow-common/cpp/include/crow-common/reactor.h`,
  `lib/crow-common/cpp/src/reactor.cpp`.
- [ ] **1c-2: Submit in run() loop** — at top of each iteration, if
  `pending_submit_` is true, call `io_uring_submit()` (or
  `io_uring_submit_and_wait` in Hybrid wait phase). Clear flag.
  Files: `lib/crow-common/cpp/src/reactor.cpp`.
- [ ] **1c-3: Add batched submit unit test** — verify 100 SQEs
  submitted in one `io_uring_submit()` call via syscall counting.
  Files: `lib/crow-common/cpp/tests/reactor_batch_test.cpp` (new).

## Phase 4: IoEngine + UringEngine (work item 2)

- [ ] **4-1: Create app/crow-diskio skeleton** — CMakeLists.txt,
  dio_main.cpp, dio_server.h/cpp, dio_config.h/cpp. Empty main that
  parses config and starts an empty RpcServer.
  Files: `app/crow-diskio/CMakeLists.txt` (new),
  `app/crow-diskio/src/dio_main.cpp` (new),
  `app/crow-diskio/src/dio_server.{h,cpp}` (new),
  `app/crow-diskio/src/dio_config.{h,cpp}` (new).
- [ ] **4-2: IoEngine virtual base** — define `IoEngine` interface
  with `submit_write`/`submit_read`/`submit_fsync`/`cancel_disk`.
  Files: `app/crow-diskio/src/engine/io_engine.h` (new).
- [ ] **4-3: UringEngine** — implement `UringEngine` wrapping
  `crow::common::Reactor`. Per-disk in-flight tracking. Linked
  timeouts. O_DIRECT alignment check.
  Files: `app/crow-diskio/src/engine/uring/uring_engine.{h,cpp}` (new).
- [ ] **4-4: Add submit_cancel + submit_linked to Reactor** —
  reactor methods for `IORING_OP_ASYNC_CANCEL` and linked-timeout
  I/O submission.
  Files: `lib/crow-common/cpp/include/crow-common/reactor.h`,
  `lib/crow-common/cpp/src/reactor.cpp`.
- [ ] **4-5: UringEngine unit tests** — write/read/fsync round-trip,
  in-flight tracking, cancellation, linked timeout.
  Files: `app/crow-diskio/tests/uring_engine_test.cpp` (new).

## Phase 5: BlockingEngine (work item 3)

- [ ] **5-1: BlockingEngine implementation** — thread pool with
  pwrite/pread/fdatasync. Job queue + worker threads.
  Files: `app/crow-diskio/src/engine/blocking/blocking_engine.{h,cpp}` (new).
- [ ] **5-2: BlockingEngine unit tests** — write/read round-trip on
  FileDisk, fsync durability, thread pool backpressure.
  Files: `app/crow-diskio/tests/blocking_engine_test.cpp` (new).

## Phase 6: DummyEngine + MemDisk (work item 4)

- [ ] **6-1: MemDisk implementation** — drop-write + rule-based read
  with cached pattern buffer. Pattern generation seeded by disk_id +
  zone, mixed with logical_object_offset.
  Files: `app/crow-diskio/src/disk/mem_disk.{h,cpp}` (new).
- [ ] **6-2: DummyEngine implementation** — immediate success on
  write, MemDisk read on read.
  Files: `app/crow-diskio/src/engine/dummy/dummy_io_engine.{h,cpp}` (new).
- [ ] **6-3: DummyEngine + MemDisk unit tests** — write drop, read
  determinism, logical_object_offset mixing, wrap-around, two-read
  identity.
  Files: `app/crow-diskio/tests/dummy_engine_test.cpp` (new).

## Phase 7: SimulatedEngine + SimulatedDisk (work item 5)

- [ ] **7-1: SimulatedDisk + DiskProperties** — wraps a disk +
  latency/error properties.
  Files: `app/crow-diskio/src/disk/simulated_disk.{h,cpp}` (new).
- [ ] **7-2: SimulatedEngine implementation** — wraps another
  IoEngine, injects latency + errors per disk properties.
  Files: `app/crow-diskio/src/engine/simulated/simulated_io_engine.{h,cpp}` (new).
- [ ] **7-3: SimulatedEngine unit tests** — error rate 1.0/0.0/0.5,
  latency range, fixed latency degenerate case.
  Files: `app/crow-diskio/tests/simulated_engine_test.cpp` (new).

## Phase 8: Disk Abstraction + DiskSet + Zone (work item 6)

- [ ] **8-1: Disk virtual base + Zone** — Disk interface with fd,
  type, o_direct, block_size, engine, find_zone. Zone struct.
  Files: `app/crow-diskio/src/disk/disk.{h,cpp}` (new),
  `app/crow-diskio/src/disk/zone.{h,cpp}` (new).
- [ ] **8-2: BlockDisk + FileDisk** — BlockDisk (O_DIRECT block
  device), FileDisk (regular file).
  Files: `app/crow-diskio/src/disk/block_disk.{h,cpp}` (new),
  `app/crow-diskio/src/disk/file_disk.{h,cpp}` (new).
- [ ] **8-3: DiskSet** — HashMap<DiskId, shared_ptr<Disk>>, init
  from config, find_disk.
  Files: `app/crow-diskio/src/disk/disk_set.{h,cpp}` (new).
- [ ] **8-4: Disk unit tests** — BlockDisk open, FileDisk open,
  DiskSet find_disk, unknown disk_id error.
  Files: `app/crow-diskio/tests/disk_test.cpp` (new).

## Phase 9: Flatbuffer Schemas (work item 8)

- [ ] **9-1: diskio.fbs schema** — DiskWriteRequest/Response,
  DiskReadRequest/Response, DiskFsyncRequest/Response,
  FBDiskIoRetCode enum.
  Files: `lib/crow-protocol/src/fbs/diskio.fbs` (new).
- [ ] **9-2: Register message type IDs** — add diskio message types
  (3600s range) to the msg_type enum or diskio's own enum.
  Files: `lib/crow-protocol/src/fbs/msg_type.fbs`.
- [ ] **9-3: Update CMake schema build** — add diskio.fbs to the
  flatbuffer schema build.
  Files: `lib/crow-protocol/CMakeLists.txt` (or relevant schema build).

## Phase 10: RPC Service (work item 7)

- [ ] **10-1: Write handler** — parse DiskWriteRequest, resolve
  disk + zone, compute phys_offset, submit to engine, async
  completion → submit response.
  Files: `app/crow-diskio/src/rpc/msg_disk_write_request.{h,cpp}` (new).
- [ ] **10-2: Read handler** — parse DiskReadRequest, allocate read
  buffer, submit to engine, async completion → submit response with
  data payload.
  Files: `app/crow-diskio/src/rpc/msg_disk_read_request.{h,cpp}` (new).
- [ ] **10-3: Fsync handler** — parse DiskFsyncRequest, submit
  fsync, async completion → submit response.
  Files: `app/crow-diskio/src/rpc/msg_disk_fsync_request.{h,cpp}` (new).
- [ ] **10-4: Handler dispatch + server wiring** — register handlers
  with RpcServer, wire DiskSet + engines.
  Files: `app/crow-diskio/src/rpc/dio_server_msg_handler.{h,cpp}` (new),
  `app/crow-diskio/src/dio_server.{h,cpp}` (update).
- [ ] **10-5: RPC integration tests** — write/read/fsync via local
  server + client, error cases.
  Files: `app/crow-diskio/tests/rpc_test.cpp` (new).

## Phase 11: Rust Client Library (work item 9)

- [ ] **11-1: crow-diskio-client crate skeleton** — Cargo.toml,
  lib.rs, error.rs (IoError enum).
  Files: `lib/crow-diskio-client/Cargo.toml` (new),
  `lib/crow-diskio-client/src/lib.rs` (new),
  `lib/crow-diskio-client/src/error.rs` (new).
- [ ] **11-2: DiskIoClient implementation** — write/read/fsync
  methods wrapping crow-rpc-ffi. Topology routing by segment.node_id.
  Files: `lib/crow-diskio-client/src/client.rs` (new).
- [ ] **11-3: Proto bindings** — flatbuffer-generated Rust types
  for diskio messages.
  Files: `lib/crow-diskio-client/src/proto.rs` (new).
- [ ] **11-4: Client unit tests** — mock RPC, verify request
  building + response parsing.
  Files: `lib/crow-diskio-client/tests/client_test.rs` (new).

## Phase 12: Configuration + Startup (work item 10)

- [ ] **12-1: DioConfig + CLI parsing** — config struct, CLI args,
  validate().
  Files: `app/crow-diskio/src/dio_config.{h,cpp}` (update).
- [ ] **12-2: Server startup sequence** — connect to group-0,
  auto-discover disks, init DiskSet, register handlers, listen,
  register with service registry.
  Files: `app/crow-diskio/src/dio_main.cpp` (update),
  `app/crow-diskio/src/dio_server.{h,cpp}` (update).
- [ ] **12-3: Startup integration test** — server starts, registers
  with group-0, serves I/O.
  Files: `app/crow-diskio/tests/startup_test.cpp` (new).

## Phase 13: SQ Full Backpressure Tests

- [ ] **13-1: SQ full backpressure tests** — tiny-SQ reactor + slow
  disk, good-disk isolation, cancellation frees slots, BlockingEngine
  backpressure analog.
  Files: `app/crow-diskio/tests/sq_full_test.cpp` (new).

## File List

New files:
- `lib/crow-common/cpp/include/crow-common/reactor.h`
- `lib/crow-common/cpp/src/reactor.cpp`
- `lib/crow-common/cpp/tests/reactor_polling_test.cpp`
- `lib/crow-common/cpp/tests/reactor_batch_test.cpp`
- `app/crow-diskio/CMakeLists.txt`
- `app/crow-diskio/src/dio_main.cpp`
- `app/crow-diskio/src/dio_server.{h,cpp}`
- `app/crow-diskio/src/dio_config.{h,cpp}`
- `app/crow-diskio/src/engine/io_engine.h`
- `app/crow-diskio/src/engine/uring/uring_engine.{h,cpp}`
- `app/crow-diskio/src/engine/blocking/blocking_engine.{h,cpp}`
- `app/crow-diskio/src/engine/dummy/dummy_io_engine.{h,cpp}`
- `app/crow-diskio/src/engine/simulated/simulated_io_engine.{h,cpp}`
- `app/crow-diskio/src/disk/disk.{h,cpp}`
- `app/crow-diskio/src/disk/block_disk.{h,cpp}`
- `app/crow-diskio/src/disk/file_disk.{h,cpp}`
- `app/crow-diskio/src/disk/mem_disk.{h,cpp}`
- `app/crow-diskio/src/disk/simulated_disk.{h,cpp}`
- `app/crow-diskio/src/disk/disk_set.{h,cpp}`
- `app/crow-diskio/src/disk/zone.{h,cpp}`
- `app/crow-diskio/src/rpc/dio_server_msg_handler.{h,cpp}`
- `app/crow-diskio/src/rpc/msg_disk_write_request.{h,cpp}`
- `app/crow-diskio/src/rpc/msg_disk_read_request.{h,cpp}`
- `app/crow-diskio/src/rpc/msg_disk_fsync_request.{h,cpp}`
- `app/crow-diskio/tests/` (multiple test files)
- `lib/crow-diskio-client/Cargo.toml`
- `lib/crow-diskio-client/src/{lib,client,error,proto}.rs`
- `lib/crow-diskio-client/tests/`
- `lib/crow-protocol/src/fbs/diskio.fbs`

Modified files:
- `lib/crow-common/cpp/CMakeLists.txt`
- `lib/crow-tree/CMakeLists.txt`
- `lib/crow-tree/include/crow-tree/crow-tree.h`
- `lib/crow-tree/include/crow-tree/options.h`
- `lib/crow-tree/src/crow-tree.cpp`
- `lib/crow-tree/src/c_api.cpp`
- `lib/crow-tree/src/persist.cpp`
- `lib/crow-tree/src/block_async_page_store.cpp`
- `lib/crow-tree/tests/unit/reactor_test.cpp`
- `lib/crow-tree/tests/integration/async_get_test.cpp`
- `lib/crow-tree/ffi/build.rs`
- `lib/crow-protocol/src/fbs/msg_type.fbs`
- `lib/crow-protocol/CMakeLists.txt` (or schema build)

Deleted files:
- `lib/crow-tree/include/crow-tree/reactor.h`
- `lib/crow-tree/src/reactor.cpp`

## Test Checklist

Unit tests:
- [ ] Reactor builds under CROW_HAVE_LIBURING on Linux
- [ ] Reactor submit_read + submit_write round-trip
- [ ] Hybrid busy-poll counter (sustained I/O stays busy-poll)
- [ ] Sqpoll syscall elimination (strace count = 0)
- [ ] Batched submission (100 SQEs in one io_uring_submit)
- [ ] UringEngine write/read/fsync round-trip (O_DIRECT BlockDisk)
- [ ] UringEngine per-disk in-flight tracking
- [ ] UringEngine IORING_OP_ASYNC_CANCEL
- [ ] UringEngine linked timeout (100ms cancel)
- [ ] BlockingEngine write/read round-trip (FileDisk)
- [ ] BlockingEngine fsync durability
- [ ] BlockingEngine thread pool backpressure (100 concurrent)
- [ ] MemDisk write drop + immediate success
- [ ] MemDisk read determinism (with/without logical_object_offset)
- [ ] MemDisk read wrap-around
- [ ] MemDisk two-read identity
- [ ] SimulatedDisk error rate 1.0/0.0/0.5
- [ ] SimulatedDisk latency range + fixed latency
- [ ] BlockDisk O_DIRECT open
- [ ] FileDisk pwrite at offset
- [ ] DiskSet find_disk + unknown disk_id error
- [ ] O_DIRECT alignment violation → InvalidAlignment
- [ ] Partial write → PartialWrite (no retry)
- [ ] Flatbuffer round-trip encode/decode
- [ ] Message type IDs registered

Integration / E2E tests:
- [ ] DiskIoClient::write → server writes, returns success
- [ ] DiskIoClient::read → returns correct bytes
- [ ] DiskIoClient::read with logical_offset → mem-disk rule content
- [ ] DiskIoClient::fsync → flushes, returns success
- [ ] Write to error-rate-1.0 disk → IoError::Io
- [ ] Zone offset computation (zone_index=2, zone_offset=4096)
- [ ] DiskIoClient routing by segment.node_id
- [ ] Connection error → client treats as failure, retry succeeds
- [ ] Server registers with group-0 service registry
- [ ] Server auto-discovers disk list from group-0
- [ ] Node restart → handles re-opened, client retry
- [ ] SQ full backpressure (tiny-SQ + slow disk)
- [ ] SQ full + good-disk isolation (shared ring)
- [ ] SQ full + cancellation frees slots
- [ ] Reactor topology: two disks shared reactor (HDD)
- [ ] Reactor topology: per-disk reactor (NVMe)
- [ ] IORING_SETUP_ATTACH_WQ (Linux 5.18+)
- [ ] crow-tree regression: test-tree-ct, test-tree-ffi pass unchanged

Test commands:
- `pixi run test-tree-ct` (reactor relocation regression)
- `pixi run test-tree-ffi` (reactor relocation regression)
- `pixi run test-diskio-ct` (C++ ctest, engine + server + disk types)
- `pixi run test-diskio-client` (Rust cargo test, client crate)
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
- `clang-format --dry-run --Werror` (changed .cpp/.h)
- `tree-lint` (clang-tidy, changed C++)
