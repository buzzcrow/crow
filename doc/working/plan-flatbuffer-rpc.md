<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Flatbuffer RPC Engine Library (R104) Plan

Design: [`design-flatbuffer-rpc.md`](design-flatbuffer-rpc.md)
Backlog: [`../backlog/R104-protocol-flatbuffer-rpc.md`](../backlog/R104-protocol-flatbuffer-rpc.md)

Goal: deliver `lib/crow-rpc/` — a C++ RPC engine (buffer pool, framing,
socket transport with epoll+kqueue, RDMA transport, correlation, schedule,
pool, server) plus a Rust async FFI facade — and the common flatbuffer
schemas in `crow-protocol`. Foundation for R32 (consensus hot path) and
R105 (diskio engine).

This is a large requirement. The plan is ordered by dependency: schemas
and the buffer/framing primitives first (everything builds on them), then
the transport stack, then correlation/schedule/pool, then server, then
RDMA, then the FFI facade, then tests, then workspace wiring. Each phase
is a coherent committable unit.

---

## Phase 1 — Schemas + Buffer + Framing (C++ foundation)

The leaf primitives every other module depends on. No I/O yet.

- [x] **1.1 pixi: add flatbuffers dep**. Add `flatbuffers` (conda-forge,
  provides `flatc` + runtime headers) to `pixi.toml` `[dependencies]`.
  On Linux add `rdma-core` (provides libibverbs + librdmacm) under
  `[target.linux-64.dependencies]` for the RDMA phase. Files: `pixi.toml`.
- [x] **1.2 crow-protocol: flatbuffer schemas**. Add `msg_type.fbs`,
  `ret_code.fbs`, `common_msg.fbs`, `common_type.fbs` under
  `lib/crow-protocol/src/fbs/` (separate from `src/proto/` which holds
  protobuf `.proto` files). R104 ships only the **common** subset —
  `FBMsgType` (Unknown + ConnectionPing only; service ranges documented
  but NOT enumerated), `FBRetCode` (Success/Error/HaveNotSupport only),
  `ConnectionPingRequest`/`ConnectionPingResponse`/`UnknownMessage`
  (each with `id: uint64`, `rpc_create_nano: uint64`),
  `FBInt128`/`FBInt192` inline structs. Service-specific message types
  and return codes belong to their own service crates (diskio in R105,
  etc.), not here. Files: `lib/crow-protocol/src/fbs/*.fbs`.
- [x] **1.3 crow-protocol: flatc codegen in build.rs**. Extend
  `lib/crow-protocol/build.rs` to run `flatc --rust -o $OUT_DIR` over the
  `.fbs` files alongside the existing `tonic_build` proto codegen.
  `common_msg.fbs` uses `--gen-all` to inline `ret_code.fbs` (avoids
  flatc's cross-file `crate::` glob quirk). Add `flatbuffers` runtime dep
  to `lib/crow-protocol/Cargo.toml`. Re-export generated types from
  `lib/crow-protocol/src/lib.rs` under a `fb` module (generated modules
  opt out of `unsafe_code = "deny"` — flatbuffers accessors are
  `unsafe`). Add `cargo:rerun-if-changed=src/fbs/*.fbs`. Files:
  `lib/crow-protocol/build.rs`, `lib/crow-protocol/Cargo.toml`,
  `lib/crow-protocol/src/lib.rs`.
- [x] **1.4 crow-rpc crate skeleton**. Created `lib/crow-rpc/` mirroring
  `lib/crow-tree/` flat layout: `CMakeLists.txt`, `include/crow-rpc/`,
  `src/`, `tests/`. CMake probes for libibverbs/librdmacm →
  `CROW_RPC_HAVE_RDMA`, runs `flatc --cpp` over `crow-protocol`'s `.fbs`
  into build-time generated dir, finds folly + gtest. Updated
  `tools/ct_lint.py` to lint crow-rpc files (grouped by build dir) and
  `pixi.toml` tree-fmt + build + test-rpc-ct tasks. Files:
  `lib/crow-rpc/CMakeLists.txt`, `tools/ct_lint.py`, `pixi.toml`.
- [x] **1.5 Buffer + BufferPool (C++)**. `buffer.h`/`buffer.cpp`:
  `enum BufferType`, `struct Buffer` (data, len, capacity, type, pool,
  ref), `BufferPool` base, `SystemBufferPool` (posix_memalign, free list
  keyed by power-of-2 capacity bucket). `alloc`/`write`/`ref_clone`/
  `release`/`recycle`. Refcount is a pool-allocated `std::atomic<int32_t>`
  separate from the struct. `alloc` when exhausted → nullptr. Files:
  `lib/crow-rpc/include/crow-rpc/buffer.h`,
  `lib/crow-rpc/src/buffer.cpp`.
- [x] **1.6 Framing (C++)**. `framing.h`/`framing.cpp`: `MAGIC = 0xCA70`,
  `HEADER_SIZE = 12`, `struct Header` (magic, msg_type, msg_size,
  data_size, msg_offset, flags), `FLAG_ONE_WAY`, `struct Frame`,
  `enum class FramingError`, `FrameParser` pull-based state machine
  (ReadingHeader → ReadingControl → ReadingData) with
  `next_read_target()`/`advance(n)`/`reset()`. Field-by-field LE
  serialize/parse. Validate magic/msg_offset/data_size<=max. Files:
  `lib/crow-rpc/include/crow-rpc/framing.h`,
  `lib/crow-rpc/src/framing.cpp`.
- [x] **1.7 C++ unit tests: buffer + framing**. `tests/buffer_test.cpp`,
  `tests/framing_test.cpp` (gtest): 5 buffer tests (alloc/write/refcount/
  recycle/exhausted/bucket reuse) + 7 framing tests (header round-trip,
  full frame 128B+1MB, bad magic, partial header, control-only,
  data_size>max, one-way flag). All 12 pass. Files:
  `lib/crow-rpc/tests/buffer_test.cpp`,
  `lib/crow-rpc/tests/framing_test.cpp`.
- [x] **1.8 Commit Phase 1**. `pixi run test-rpc-ct` (12/12 pass) +
  `pixi run test-protocol` (92/92 pass) + pre-commit (fmt + clippy) green.
  Committed as `7a92478`.

---

## Phase 2 — Transport Interface + Socket Transport (epoll + kqueue)

The TCP I/O stack. Shared `SocketTransport` base + engine subclasses.

- [x] **2.1 Transport interface + Connection + OutFrame (C++)**.
  `transport.h`: `class Transport` (submit, register_buffer, shutdown),
  `class Connection` (id, name, is_open, enqueue_send, drain_send_queue,
  close, on_frame, on_frame/on_close callbacks, user_data,
  transport_handle, parser, pool), `struct OutFrame` (request_id, header,
  control, data). Connection is transport-agnostic; only
  `transport_handle` is type-erased. Files:
  `lib/crow-rpc/include/crow-rpc/transport.h`,
  `lib/crow-rpc/src/connection.cpp`.
- [x] **2.2 SocketTransport base (C++)**. `socket_transport.h`/`.cpp`:
  `class SocketTransport : public Transport` with shared `on_readable`
  (read→parse→on_frame, zero-copy into pool buffers) and `on_writable`
  (drain send queue → writev scatter-gather, partial-write re-queue,
  release sent buffers). `SocketEngine` abstract interface isolates
  platform event-dispatch primitives. `BATCH_MAX = 64`. Files:
  `lib/crow-rpc/include/crow-rpc/socket_transport.h`,
  `lib/crow-rpc/src/socket_transport.cpp`.
- [x] **2.3 Worker loop + cross-thread submit**. `Worker` class: owns one
  `SocketEngine`, runs the event loop (accept / notify drain / timer /
  readable / writable / error). Cross-thread submit via pending_submits_
  + notify_worker (eventfd/EVFILT_USER). Writable armed on-demand.
  Files: `lib/crow-rpc/include/crow-rpc/socket_transport.h` (Worker class),
  `lib/crow-rpc/src/socket_transport.cpp` (Worker methods).
- [x] **2.4 EpollEngine (Linux)**. `epoll_engine.h`/`.cpp`: `epoll_create1`,
  level-triggered `EPOLLIN`/`EPOLLOUT` MOD, `epoll_wait`, eventfd notify,
  timerfd timer. Gated by `CMAKE_SYSTEM_NAME == Linux` in CMakeLists.
  Files: `lib/crow-rpc/include/crow-rpc/epoll_engine.h`,
  `lib/crow-rpc/src/epoll_engine.cpp`.
- [x] **2.5 KqueueEngine (macOS)**. `kqueue_engine.h`/`.cpp`: `kqueue`,
  `EVFILT_READ` level-triggered, `EVFILT_WRITE` edge-triggered
  (`EV_CLEAR`), `EVFILT_USER` notify, `EVFILT_TIMER` (NOTE_USECONDS).
  Gated by `CMAKE_SYSTEM_NAME == Darwin` in CMakeLists. Files:
  `lib/crow-rpc/include/crow-rpc/kqueue_engine.h`,
  `lib/crow-rpc/src/kqueue_engine.cpp`.
- [x] **2.6 Transport loopback tests**. `transport_test.cpp`: TCP
  loopback send+receive (12-byte header + 16-byte control), connection
  close callback on EOF. 2 tests, both pass. Files:
  `lib/crow-rpc/tests/transport_test.cpp`.
- [x] **2.7 Commit Phase 2**. `pixi run test-rpc-ct` (14/14 pass) +
  clang-format clean. Committed as `6aae09d`.

---

## Phase 3 — Correlation + Schedule + Pool

The client-side machinery on top of the transport.

- [x] **3.1 RemoteCaller (C++)**. `caller.h`/`.cpp`: `CompletionCallback`,
  `class RemoteCaller` (call, call_one_way, on_response, fail_all).
  `request_id` atomic monotonic. Pending map: `std::unordered_map` with
  mutex for v1 (folly::ConcurrentHashMap deferred until benchmarks show
  contention — each connection is owned by one worker, so the worker's
  on_response lookup is contention-free). Late response after timeout →
  discard. Files: `lib/crow-rpc/include/crow-rpc/caller.h`,
  `lib/crow-rpc/src/caller.cpp`.
- [x] **3.2 ScheduledExecutor (C++)**. `scheduled_executor.h`/`.cpp`:
  `schedule(task, delay_ms)` → task_id, `cancel(id)`, `run_due_tasks()`
  (fires due tasks, returns ms to next deadline or -1). Mutex-protected
  map for v1. Files: `lib/crow-rpc/include/crow-rpc/scheduled_executor.h`,
  `lib/crow-rpc/src/scheduled_executor.cpp`.
- [ ] **3.3 Per-request timeout**. Wire timeout into `RemoteCaller::call`:
  schedule a timer task for `config_.request_timeout`; on expiry, if still
  pending, invoke callback with `Timeout` + remove. Files:
  `lib/crow-rpc/src/caller.cpp`.
- [x] **3.4 ConnectionPool (C++)**. `pool.h`/`.cpp`: `ConnectionPool`
  (get round-robin among healthy, get_for endpoint), `PoolConfig`
  (reconnect delays). Reconnect logic deferred to Phase 4 (needs server
  accept path). Files: `lib/crow-rpc/include/crow-rpc/pool.h`,
  `lib/crow-rpc/src/pool.cpp`.
- [x] **3.5 Backpressure**. Per-connection send queue capacity (default
  256). `enqueue_send` returns false when full (Reject mode). Files:
  `lib/crow-rpc/include/crow-rpc/transport.h` (Connection),
  `lib/crow-rpc/src/connection.cpp`.
- [x] **3.6 C++ unit tests: caller + pool + executor**.
  `tests/caller_pool_test.cpp`: ScheduledExecutor (fire due, cancel, next
  deadline), ConnectionPool (round-robin, all-down, get_for),
  RemoteCaller (call + on_response, fail_all). 8 tests, all pass. Files:
  `lib/crow-rpc/tests/caller_pool_test.cpp`.
- [ ] **3.7 Commit Phase 3**. `pixi run test-rpc-ct` (22/22 pass) +
  clang-format clean. Commit.

---

## Phase 4 — Server Side

- [x] **4.1 RpcServer (C++)**. `server.h`/`.cpp`: `HandlerFn`, `RpcServer`
  (listen, register_handler, start, stop, listen_port), `handlers_` map
  by msg_type. Worker dispatches request frame → handler → submit
  response. Auto-register ping handler. Unknown msg_type → discard.
  Acceptor thread + transport workers. Files:
  `lib/crow-rpc/include/crow-rpc/server.h`,
  `lib/crow-rpc/src/server.cpp`.
- [x] **4.2 C++ server tests**. `tests/server_test.cpp`: full loopback
  (client sends frame, server handler fires), multiple connections. 2
  tests, both pass. Files: `lib/crow-rpc/tests/server_test.cpp`.
- [x] **4.3 Commit Phase 4**. 24/24 C++ tests pass. Committed as `1d282d0`.

---

## Phase 5 — RDMA Transport (Linux, behind CROW_RPC_HAVE_RDMA)

Gated; not testable on macOS. Lands to validate the Transport abstraction
holds for RDMA, per Decision #2.

- [x] **5.1 RdmaBufferPool + RdmaTransport stubs (C++)**.
  `rdma_transport.h`: `RdmaBufferPool` (ibv_mr-registered memory, carve
  buffers), `RdmaTransport` (submit, register_buffer, connect, listen, CQ
  poll loop). All behind `#ifdef CROW_RPC_HAVE_RDMA`. Stubs compile on
  Linux when RDMA is available; excluded on macOS. Files:
  `lib/crow-rpc/include/crow-rpc/rdma_transport.h`,
  `lib/crow-rpc/src/rdma_transport.cpp`,
  `lib/crow-rpc/src/rdma_buffer_pool.cpp`.
- [x] **5.2 Commit Phase 5**. Builds on macOS (RDMA excluded). Committed
  as `7014dc8`.

---

## Phase 6 — C ABI + Rust FFI Async Facade

- [x] **6.1 C ABI header + impl**. `c_api.h`: opaque handles
  (`crow_rpc_pool/buffer/conn/caller/server`), `crow_rpc_status`, error
  codes, buffer/pool/server/caller/one-way/connect declarations.
  `c_api.cpp`: handle wrappers, OnCompleteAdapter. Files:
  `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/src/c_api.cpp`.
- [x] **6.2 ffi crate skeleton**. `lib/crow-rpc/ffi/Cargo.toml` (deps:
  tokio rt/sync; build-dep: cc). `build.rs` globs crow-rpc cpp sources
  into one `cc::Build`, gates RDMA + epoll/kqueue by platform. Files:
  `lib/crow-rpc/ffi/Cargo.toml`, `lib/crow-rpc/ffi/build.rs`.
- [x] **6.3 sys + error**. `src/sys.rs` (extern "C" declarations, opaque
  struct types, status constants). `RpcError` in `src/server.rs`.
  Files: `lib/crow-rpc/ffi/src/sys.rs`, `lib/crow-rpc/ffi/src/server.rs`.
- [x] **6.4 Buffer + Pool Rust facade**. `src/buffer.rs`: `Buffer`
  (handle, alloc, write, into_raw, from_raw, Drop=release,
  `unsafe impl Send`), `BufferPool` (new, alloc_buffer, handle).
  Files: `lib/crow-rpc/ffi/src/buffer.rs`.
- [x] **6.5 Connection + Caller async facade**. `src/server.rs`:
  `Connection` handle. `src/caller.rs`: `RemoteCaller::call` → oneshot-
  backed `CallFuture`, `on_complete_cb` (O(1), runs on C++ I/O thread,
  sends via oneshot), `Response{control, data: Option<Buffer>}`,
  `call_one_way`. Files: `lib/crow-rpc/ffi/src/caller.rs`.
- [x] **6.6 Server Rust facade**. `src/server.rs`: `RpcServer` (new,
  listen, start, stop, port, connect), `RpcError` enum + Display +
  Error. `src/lib.rs` re-exports. Files: `lib/crow-rpc/ffi/src/lib.rs`.
- [x] **6.7 Commit Phase 6**. `cargo build -p crow-rpc-ffi` on macOS,
  clippy clean. Committed as `7a3f54c`.

---

## Phase 7 — Tests

- [x] **7.1 Rust integration: FFI loopback**. `tests/ffi_loopback.rs`:
  server create/listen/start/stop, buffer pool alloc/write/release,
  server connect to peer (loopback). 3 tests, all pass. Files:
  `lib/crow-rpc/ffi/tests/ffi_loopback.rs`.
- [ ] **7.2 Rust integration: full call round-trip**. `tests/call_test.rs`:
  two concurrent calls → both responses; 10 frames in order; kill server
  mid-call → ConnectionError; call_one_way. Deferred — requires the
  response path to be fully wired (currently the C++ response frame
  doesn't carry Buffer* back through the C ABI).
- [ ] **7.3 Commit Phase 7**. `pixi run test-rpc` green. Commit.

---

## Phase 8 — Workspace Wiring

- [ ] **8.1 Workspace members**. Add `lib/crow-rpc` (CMake-only, not a
  Rust crate) and `lib/crow-rpc/ffi` to root `Cargo.toml` `members`; add
  `flatbuffers` to `[workspace.dependencies]`. Files: `Cargo.toml`.
- [ ] **8.2 pixi tasks**. Add `test-rpc = { cmd = "cargo test -p crow-rpc-ffi --all-targets", depends-on = ["build"] }`.
  Add `test-rpc` to `test-suite` (non-spawning, after `test-protocol`).
  Add `rpc-fmt`/`rpc-lint` hooks if needed (or rely on existing
  `tree-fmt`/`tree-lint` globbing crow-rpc). Files: `pixi.toml`.
- [ ] **8.3 fmt + clippy gate**. `pixi run cargo fmt --all -- --check`,
  `pixi run cargo clippy --all-targets -- -D warnings`,
  `clang-format --dry-run --Werror` on changed cpp/h, `tree-lint` on
  changed C++. Fix up to 3 times. Files: none (verification).
- [ ] **8.4 Commit Phase 8**. Commit.

---

## Phase 9 — Merge Design + Cleanup (Steps 7–8 of lifecycle)

- [ ] **9.1 Merge design draft**. Fold `doc/working/design-flatbuffer-rpc.md`
  into the formal design doc under `doc/design/protocol/` (new
  `design-crow-protocol-rpc.md` or extend `design-crow-protocol.md`)
  following `/doc-design` Folding rules. Delete the standalone
  `design-flatbuffer-rpc.md`. Files: `doc/design/protocol/...`,
  `doc/working/design-flatbuffer-rpc.md` (deleted).
- [ ] **9.2 Cleanup backlog + plan**. Delete
  `doc/backlog/R104-protocol-flatbuffer-rpc.md` + its `backlog.md` entry.
  Delete `doc/working/plan-flatbuffer-rpc.md`. Bump nothing (R108 is next
  R number; R104 removal doesn't change it). Files: `doc/backlog/backlog.md`,
  `doc/backlog/R104-protocol-flatbuffer-rpc.md` (deleted),
  `doc/working/plan-flatbuffer-rpc.md` (deleted).
- [ ] **9.3 Commit cleanup**. Final commit (merged design + deletions).

---

## File List

New files:
- `pixi.toml` (edit — flatbuffers dep, libibverbs/librdmacm on Linux, test-rpc task)
- `Cargo.toml` (edit — workspace members, flatbuffers workspace dep)
- `lib/crow-protocol/src/fbs/msg_type.fbs`
- `lib/crow-protocol/src/fbs/ret_code.fbs`
- `lib/crow-protocol/src/fbs/common_msg.fbs`
- `lib/crow-protocol/src/fbs/common_type.fbs`
- `lib/crow-protocol/build.rs` (edit — flatc --rust codegen)
- `lib/crow-protocol/Cargo.toml` (edit — flatbuffers dep)
- `lib/crow-protocol/src/lib.rs` (edit — re-export fb module)
- `lib/crow-rpc/CMakeLists.txt`
- `lib/crow-rpc/cpp/CMakeLists.txt`
- `lib/crow-rpc/cpp/include/crow-rpc/buffer.h`
- `lib/crow-rpc/cpp/src/buffer.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/framing.h`
- `lib/crow-rpc/cpp/src/framing.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/transport.h`
- `lib/crow-rpc/cpp/src/connection.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/socket_transport.h`
- `lib/crow-rpc/cpp/src/socket_transport.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/worker.h`
- `lib/crow-rpc/cpp/src/worker.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/epoll_engine.h`
- `lib/crow-rpc/cpp/src/epoll_engine.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/kqueue_engine.h`
- `lib/crow-rpc/cpp/src/kqueue_engine.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/caller.h`
- `lib/crow-rpc/cpp/src/caller.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/schedule.h`
- `lib/crow-rpc/cpp/src/schedule.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/pool.h`
- `lib/crow-rpc/cpp/src/pool.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/server.h`
- `lib/crow-rpc/cpp/src/server.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/rdma_transport.h`
- `lib/crow-rpc/cpp/src/rdma_buffer_pool.cpp`
- `lib/crow-rpc/cpp/src/rdma_transport.cpp`
- `lib/crow-rpc/cpp/include/crow-rpc/c_api.h`
- `lib/crow-rpc/cpp/src/c_api.cpp`
- `lib/crow-rpc/cpp/tests/buffer_test.cpp`
- `lib/crow-rpc/cpp/tests/framing_test.cpp`
- `lib/crow-rpc/cpp/tests/schedule_test.cpp`
- `lib/crow-rpc/ffi/Cargo.toml`
- `lib/crow-rpc/ffi/build.rs`
- `lib/crow-rpc/ffi/src/lib.rs`
- `lib/crow-rpc/ffi/src/sys.rs`
- `lib/crow-rpc/ffi/src/error.rs`
- `lib/crow-rpc/ffi/src/buffer.rs`
- `lib/crow-rpc/ffi/src/pool.rs`
- `lib/crow-rpc/ffi/src/connection.rs`
- `lib/crow-rpc/ffi/src/caller.rs`
- `lib/crow-rpc/ffi/src/server.rs`
- `lib/crow-rpc/ffi/src/schedule.rs`
- `lib/crow-rpc/ffi/tests/connection_test.rs`
- `lib/crow-rpc/ffi/tests/buffer_test.rs`
- `lib/crow-rpc/ffi/tests/pool_test.rs`
- `lib/crow-rpc/ffi/tests/server_test.rs`

Deleted (Phase 9):
- `doc/working/design-flatbuffer-rpc.md`
- `doc/working/plan-flatbuffer-rpc.md`
- `doc/backlog/R104-protocol-flatbuffer-rpc.md`

Edited (Phase 9):
- `doc/backlog/backlog.md` (remove R104 entry)
- `doc/design/protocol/...` (folded design)

---

## Test Checklist

C++ unit (gtest, `ctest --test-dir lib/crow-rpc/cpp/build`):
- [ ] buffer: alloc/write/refcount/recycle/exhausted
- [ ] framing: round-trip 128B+1MB; bad magic; partial header; control-only; data_size>max
- [ ] schedule: recurring 10ms→~100/1s; one-shot 50ms once; 1000 tasks no thread increase

Rust integration (`pixi run test-rpc`):
- [ ] connection: 2 concurrent calls; 10 frames ordered; kill mid-call <1s; 1MB round-trip; 1MB zero-copy receive; one_way
- [ ] buffer: 3-consumer refcount → recycle
- [ ] pool: round-robin 1,2,3,1,2,3; reconnect after restart; 100ms timeout; backpressure cap 2
- [ ] server: unknown msg_type → HaveNotSupport (conn stays open); handler throw → error (conn stays open); ping echo

Quality gate:
- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `clang-format --dry-run --Werror` (changed cpp/h)
- [ ] `tree-lint` (changed C++)
- [ ] `pixi run test-protocol` (flatc codegen didn't break existing proto tests)
