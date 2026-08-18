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
- [ ] **2.7 Commit Phase 2**. `pixi run test-rpc-ct` (14/14 pass) +
  clang-format clean. Commit.

---

## Phase 3 — Correlation + Schedule + Pool

The client-side machinery on top of the transport.

- [ ] **3.1 RemoteCaller (C++)**. `caller.h`/`.cpp`: `CompletionCallback`,
  `class RemoteCaller` (call, call_one_way, on_response, fail_all).
  `request_id` atomic monotonic per connection. Pending map:
  `folly::ConcurrentHashMap<request_id, CompletionCallback>` (folly is
  already a pixi dep). Late response after timeout → log + discard. Files:
  `lib/crow-rpc/cpp/include/crow-rpc/caller.h`,
  `lib/crow-rpc/cpp/src/caller.cpp`.
- [ ] **3.2 ScheduledExecutor (C++)**. `schedule.h`/`.cpp`: priority queue
  of `TimerEntry{deadline, task, recurring, cancelled}`, `schedule_task`,
  `schedule_recurring` (returns `TimerHandle`), `cancel`, `tick` (pop due,
  run, reschedule recurring, reset timer to next deadline). Per-worker
  timer; no thread-per-timer. Files:
  `lib/crow-rpc/cpp/include/crow-rpc/schedule.h`,
  `lib/crow-rpc/cpp/src/schedule.cpp`.
- [ ] **3.3 Per-request timeout**. Wire timeout into `RemoteCaller::call`:
  schedule a timer task for `config_.request_timeout`; on expiry, if still
  pending, invoke callback with `Timeout` + remove. Files:
  `lib/crow-rpc/cpp/src/caller.cpp`.
- [ ] **3.4 ConnectionPool + reconnect (C++)**. `pool.h`/`.cpp`:
  `ConnectionPool` (get round-robin among healthy, get_for endpoint),
  `PoolConfig` (request_timeout, retry_count, reconnect_initial/max_delay,
  reconnect_max_retries). Reconnect task scheduled on engine timer with
  exponential backoff; on success swap into pool slot. Files:
  `lib/crow-rpc/cpp/include/crow-rpc/pool.h`,
  `lib/crow-rpc/cpp/src/pool.cpp`.
- [ ] **3.5 Backpressure**. Per-connection send queue capacity
  (`config_.send_queue_capacity`, default 256). `BackpressureMode::Reject`
  (try_enqueue → `Backpressure` error) vs `::Await`. Validate capacity ≥ 1.
  Files: `lib/crow-rpc/cpp/src/connection.cpp`,
  `lib/crow-rpc/cpp/include/crow-rpc/connection.h` (config).
- [ ] **3.6 C++ unit tests: schedule**. `tests/schedule_test.cpp`:
  recurring 10ms → ~100 in ~1s; one-shot 50ms fires once; 1000 concurrent
  tasks → no thread increase. Files:
  `lib/crow-rpc/cpp/tests/schedule_test.cpp`.
- [ ] **3.7 Commit Phase 3**. Commit.

---

## Phase 4 — Server Side

- [ ] **4.1 RpcServer (C++)**. `server.h`/`.cpp`: `HandlerFn`, `RpcServer`
  (listen, register_handler, start, stop), `handlers_` map by msg_type.
  Worker dispatches request frame → handler → submit response. Auto-
  register ping handler (`ConnectionPingRequest` → echo `id`). Unknown
  msg_type → `UnknownMessage`/`HaveNotSupport`, connection stays open.
  Handler exception → error response, connection stays open. `offload_pool`
  for slow handlers. Files:
  `lib/crow-rpc/cpp/include/crow-rpc/server.h`,
  `lib/crow-rpc/cpp/src/server.cpp`.
- [ ] **4.2 Commit Phase 4**. Commit.

---

## Phase 5 — RDMA Transport (Linux, behind CROW_RPC_HAVE_RDMA)

Gated; not testable on macOS. Lands to validate the Transport abstraction
holds for RDMA, per Decision #2.

- [ ] **5.1 RdmaBufferPool (C++)**. `rdma_transport.h`: pre-register a
  large MR at construction, carve buffers as offsets, recycle without
  unregistering. `#if defined(__linux__) && CROW_RPC_HAVE_RDMA`. Files:
  `lib/crow-rpc/cpp/include/crow-rpc/rdma_transport.h`,
  `lib/crow-rpc/cpp/src/rdma_buffer_pool.cpp`.
- [ ] **5.2 RdmaTransport (C++)**. `submit` (build send WR, `ibv_post_send`),
  `register_buffer` (Registered passthrough / System→copy into send pool),
  `run_loop` (poll CQ, send completion→recycle, recv completion→parser→
  on_frame, refill recv WRs), CM setup (rdmacm bind/listen/accept +
  resolve/connect). Files:
  `lib/crow-rpc/cpp/src/rdma_transport.cpp`.
- [ ] **5.3 CMake RDMA gate**. `CROW_RPC_HAVE_RDMA` probe + exclude RDMA
  sources when not found (mirror crow-tree liburing gate). Files:
  `lib/crow-rpc/CMakeLists.txt`, `lib/crow-rpc/cpp/CMakeLists.txt`.
- [ ] **5.4 Commit Phase 5**. Build on Linux (if available) or gate-compile
  check. Commit.

---

## Phase 6 — C ABI + Rust FFI Async Facade

- [ ] **6.1 C ABI header**. `c_api.h`: opaque handles
  (`crow_rpc_pool/buffer/conn/caller/server/sched`), `crow_rpc_status`,
  buffer/pool/caller/one-way/server/schedule declarations. Exception-free,
  mirrors `crow-tree/c_api.h`. Files:
  `lib/crow-rpc/cpp/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/cpp/src/c_api.cpp`.
- [ ] **6.2 ffi crate skeleton**. `lib/crow-rpc/ffi/Cargo.toml` (deps:
  tokio rt/sync, crow-protocol, thiserror, tracing; build-dep: cc).
  `build.rs` globs crow-rpc cpp sources into one `cc::Build` (mirror
  crow-tree/ffi/build.rs), links folly/flatbuffers, gates RDMA + epoll/
  kqueue by platform. Files: `lib/crow-rpc/ffi/Cargo.toml`,
  `lib/crow-rpc/ffi/build.rs`.
- [ ] **6.3 sys + error**. `src/sys.rs` (extern "C" declarations),
  `src/error.rs` (`RpcError`, status→error mapping). Files:
  `lib/crow-rpc/ffi/src/sys.rs`, `lib/crow-rpc/ffi/src/error.rs`.
- [ ] **6.4 Buffer + Pool Rust facade**. `src/buffer.rs`: `Buffer` (handle,
  alloc, write, ref_clone, Drop=release, `unsafe impl Send`).
  `src/pool.rs`: `BufferPool` handle. Files:
  `lib/crow-rpc/ffi/src/buffer.rs`, `lib/crow-rpc/ffi/src/pool.rs`.
- [ ] **6.5 Connection + Caller async facade**. `src/connection.rs`:
  `Connection` handle. `src/caller.rs`: `RemoteCaller::call` → oneshot-
  backed `impl Future`, `on_complete_cb` (O(1), runs on C++ I/O thread,
  sends via oneshot), `Response{control, data: Option<Buffer>}`,
  `call_one_way`. Files: `lib/crow-rpc/ffi/src/connection.rs`,
  `lib/crow-rpc/ffi/src/caller.rs`.
- [ ] **6.6 Server + Schedule Rust facade**. `src/server.rs`: `RpcServer`
  async facade, handler registration. `src/schedule.rs`:
  `ScheduledExecutor` async facade. `src/lib.rs` re-exports. Files:
  `lib/crow-rpc/ffi/src/server.rs`, `lib/crow-rpc/ffi/src/schedule.rs`,
  `lib/crow-rpc/ffi/src/lib.rs`.
- [ ] **6.7 Commit Phase 6**. `cargo build -p crow-rpc-ffi` on macOS.
  Commit.

---

## Phase 7 — Tests

- [ ] **7.1 Rust integration: connection + writer**. `tests/connection_test.rs`:
  two concurrent calls → both responses; 10 frames in order; kill server
  mid-call → ConnectionError < 1s; 1MB data payload round-trip; server
  returns 1MB → zero-copy (ptr_eq to read buffer); call_one_way. Uses
  in-process echo `RpcServer` on `127.0.0.1:0`. Files:
  `lib/crow-rpc/ffi/tests/connection_test.rs`.
- [ ] **7.2 Rust integration: buffer + pool**. `tests/buffer_test.rs`:
  ref_clone for 3 consumers, drop all → recycled (next alloc reuses).
  `tests/pool_test.rs`: 3 conns round-robin 1,2,3,1,2,3; restart server →
  reconnect; 100ms timeout on 500ms handler; Reject mode cap 2 → 3rd
  Backpressure. Files: `lib/crow-rpc/ffi/tests/buffer_test.rs`,
  `lib/crow-rpc/ffi/tests/pool_test.rs`.
- [ ] **7.3 Rust integration: server**. `tests/server_test.rs`: unknown
  msg_type → UnknownMessage/HaveNotSupport, conn stays open; handler
  throws → error response, conn stays open; ping → matching id. Files:
  `lib/crow-rpc/ffi/tests/server_test.rs`.
- [ ] **7.4 Commit Phase 7**. `pixi run test-rpc` green. Commit.

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
