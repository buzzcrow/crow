<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Loopback + Message Layer Plan

Goal: restructure `lib/crow-rpc` by concept, implement the missing
message encode/decode + dispatch layer, and write end-to-end loopback
tests (C++ multi-threaded load test + Rust FFI example test) that prove
a client can send a 512-byte-data request and receive a response.

## Gap Analysis

The current code has buffer, framing, transport (epoll/kqueue), server
acceptor, and RemoteCaller skeletons — but the end-to-end path is
broken:

1. **No message layer.** Frames carry raw `control` / `data` bytes; there
   is no flatbuffer encode/decode, no `Message` / `Request` / `Response`
   abstraction, no `MsgHandler` dispatch by `msg_type`. The reference has
   `msg_handler`, `fb_msg_handler`, `message`, `msg_request`,
   `msg_response` — CROW has nothing.
2. **Server can't send responses.** `RpcServer::dispatch` has a TODO and
   deletes the response frame instead of submitting it back through the
   transport.
3. **Client connect not wired through transport.** `crow_rpc_connect`
   creates a raw socket but doesn't register it with a worker — the
   connection never receives responses.
4. **RemoteCaller::call submit path incomplete.** `call()` builds an
   `OutFrame` and calls `transport->submit()`, but the connection's
   `on_frame` callback isn't set to route responses back to
   `on_response`.
5. **No end-to-end loopback test.** Existing tests use raw socket writes
   or manually call `on_response`. No test exercises: client calls →
   transport sends → server receives → handler runs → server responds →
   client receives → callback fires.
6. **No multi-threaded load test.** No test with multiple threads +
   multiple connections + 512-byte data per request.

## Folder Structure

```
lib/crow-rpc/
  include/crow-rpc/
    buffer.h              # shared (unchanged)
    framing.h             # shared (unchanged)
    transport.h           # shared (Connection, OutFrame, Transport iface)
    pool.h                # shared (ConnectionPool)
    scheduled_executor.h  # shared (unchanged)
    c_api.h               # shared (C ABI)
    server/
      server.h            # RpcServer
      handler.h           # MsgHandler, HandlerFn, dispatch by msg_type
      message.h           # Message, Request, Response (flatbuffer wrappers)
    client/
      caller.h            # RemoteCaller
    transport/
      socket_transport.h  # SocketTransport, Worker, SocketEngine
      epoll/
        epoll_engine.h
      kqueue/
        kqueue_engine.h
      rdma/
        rdma_transport.h
  src/
    buffer.cpp
    framing.cpp
    connection.cpp
    pool.cpp
    scheduled_executor.cpp
    c_api.cpp
    server/
      server.cpp
      handler.cpp
      message.cpp
    client/
      caller.cpp
    transport/
      socket_transport.cpp
      epoll/
        epoll_engine.cpp
      kqueue/
        kqueue_engine.cpp
      rdma/
        rdma_transport.cpp
        rdma_buffer_pool.cpp
  tests/
    buffer_test.cpp       # unchanged
    framing_test.cpp      # unchanged
    transport_test.cpp    # unchanged
    caller_pool_test.cpp  # unchanged
    loopback_test.cpp     # NEW: end-to-end C++ loopback
    load_test.cpp         # NEW: multi-threaded load test
  ffi/
    (unchanged structure, updated build.rs paths + new loopback test)
```

## Tasks

### Phase 1: Folder Restructure

- [ ] **1.1 Move headers into subfolders.** Move `server.h` →
  `server/server.h`, `caller.h` → `client/caller.h`,
  `socket_transport.h` → `transport/socket_transport.h`,
  `epoll_engine.h` → `transport/epoll/epoll_engine.h`,
  `kqueue_engine.h` → `transport/kqueue/kqueue_engine.h`,
  `rdma_transport.h` → `transport/rdma/rdma_transport.h`. Keep
  `buffer.h`, `framing.h`, `transport.h`, `pool.h`,
  `scheduled_executor.h`, `c_api.h` at root. Files: all
  `include/crow-rpc/*.h`.
- [ ] **1.2 Move sources into subfolders.** Mirror the header structure
  for `src/`. Files: all `src/*.cpp`.
- [ ] **1.3 Update CMakeLists.txt.** The `GLOB_RECURSE` already handles
  subfolders, but platform-specific exclusions need path updates.
  Files: `CMakeLists.txt`.
- [ ] **1.4 Update FFI build.rs.** The `cc::Build` already recurses, but
  include paths and platform exclusions need path updates. Files:
  `ffi/build.rs`.
- [ ] **1.5 Update all #include paths.** Every `#include "crow-rpc/X.h"`
  becomes `#include "crow-rpc/server/X.h"` etc. Files: all `.cpp`,
  `.h`, test files, FFI source.
- [ ] **1.6 Build + run existing tests.** Verify no regressions from the
  move. Files: `build/`.

### Phase 2: Message Encode/Decode Layer

- [ ] **2.1 Create `server/message.h` + `message.cpp`.** Define
  `Message` (base: holds `FBMsgType`, `request_id`, `rpc_create_nano`,
  `ret_code`), `Request` (wraps a flatbuffer control buffer + raw data),
  `Response` (builds a flatbuffer response to send back). Encode/decode
  uses the generated flatbuffer headers (`common_msg_generated.h`).
  Files: `include/crow-rpc/server/message.h`, `src/server/message.cpp`.
- [ ] **2.2 Create `server/handler.h` + `handler.cpp`.** Define
  `MsgHandler` (dispatch by `msg_type`: `make_message()` factory +
  `process_message()` callback), `HandlerFn` (the simple
  `std::function` the server already uses, kept for backward compat).
  Built-in ping handler: parse `ConnectionPingRequest`, build
  `ConnectionPingResponse`. Files: `include/crow-rpc/server/handler.h`,
  `src/server/handler.cpp`.
- [ ] **2.3 Add `build_request` / `build_response` helpers.** Functions
  that take a `BufferPool`, allocate control + data buffers, serialize
  the flatbuffer control message, and return an `OutFrame` ready for
  `transport->submit()`. Files: `src/server/message.cpp`.

### Phase 3: Wire End-to-End Path

- [ ] **3.1 Wire server response sending.** In `RpcServer::dispatch`,
  when a handler returns a response `Frame*`, wrap it in an `OutFrame`
  and call `transport_->submit(conn, out_frame)`. The response
  `Frame*` carries `control` / `data` as `Buffer*` (change handler
  return type to return `OutFrame*` or a struct with `Buffer*`
  fields). Files: `src/server/server.cpp`,
  `include/crow-rpc/server/server.h`.
- [ ] **3.2 Wire client connect through transport.** Add
  `SocketTransport::connect(addr, port)` that creates a non-blocking
  socket, connects, creates a `Connection`, registers it with a worker,
  and returns the `shared_ptr<Connection>`. Files:
  `src/transport/socket_transport.cpp`,
  `include/crow-rpc/transport/socket_transport.h`.
- [ ] **3.3 Wire RemoteCaller to transport receive path.** When
  `RemoteCaller::call()` submits a request, set the connection's
  `on_frame` callback to route response frames to
  `caller->on_response()`. The callback checks `frame->header.flags`
  for one-way (skip correlation) vs request-response (look up
  `request_id` from the flatbuffer control, invoke callback). Files:
  `src/client/caller.cpp`, `include/crow-rpc/client/caller.h`.
- [ ] **3.4 Wire C ABI `crow_rpc_connect` through transport.** Update
  `crow_rpc_connect` to use `SocketTransport::connect()` instead of raw
  socket. The returned `Connection` is registered with a worker and can
  receive responses. Files: `src/c_api.cpp`.
- [ ] **3.5 Wire C ABI `crow_rpc_caller_call` end-to-end.** Ensure the
  full path works: C ABI → RemoteCaller::call → transport submit →
  server handler → response → transport receive → on_complete callback.
  Files: `src/c_api.cpp`.

### Phase 4: C++ Loopback Tests

- [ ] **4.1 Simple loopback test.** Server listens, client connects
  through transport, sends a ping request, receives a ping response.
  Verify `request_id` matches, `ret_code == Success`. Files:
  `tests/loopback_test.cpp`.
- [ ] **4.2 Echo handler test.** Register a custom echo handler
  (`msg_type=100`) that returns the request data as the response data.
  Client sends 512-byte data, verifies the response data matches.
  Files: `tests/loopback_test.cpp`.
- [ ] **4.3 Multi-threaded load test.** Start server with N workers.
  Spawn T client threads, each creates C connections, sends R requests
  with 512-byte data per request. Server echo handler responds. Verify
  all requests get responses with matching data. Config: T=4, C=2,
  R=100, 512B data. Files: `tests/load_test.cpp`.

### Phase 5: Rust FFI Loopback Test

- [ ] **5.1 Rust FFI loopback test.** Using `crow_rpc_ffi`: create
  server, listen, start. Create `RemoteCaller`, connect, send a request
  with 512-byte data, await response via `tokio::runtime`. Verify
  response data matches. This serves as the example for how other Rust
  crates (diskio, consensus) use the RPC library. Files:
  `ffi/tests/ffi_loopback.rs`.
- [ ] **5.2 Rust FFI multi-threaded test.** Spawn multiple tokio tasks,
  each sending requests concurrently. Verify all responses arrive.
  Files: `ffi/tests/ffi_loopback.rs`.

### Phase 6: Verification

- [ ] **6.1 C++ build + all tests pass.** `cmake --build` +
  `./crow_rpc_tests`. Files: `build/`.
- [ ] **6.2 Rust FFI build + tests pass.** `pixi run cargo test -p
  crow-rpc-ffi`. Files: `ffi/`.
- [ ] **6.3 clang-format + clippy clean.** Run formatters on all changed
  files. Files: all changed `.cpp`/`.h`/`.rs`.

## File List

- `lib/crow-rpc/include/crow-rpc/buffer.h` — unchanged
- `lib/crow-rpc/include/crow-rpc/framing.h` — unchanged
- `lib/crow-rpc/include/crow-rpc/transport.h` — unchanged
- `lib/crow-rpc/include/crow-rpc/pool.h` — unchanged
- `lib/crow-rpc/include/crow-rpc/scheduled_executor.h` — unchanged
- `lib/crow-rpc/include/crow-rpc/c_api.h` — minor updates (if new C ABI
  functions needed)
- `lib/crow-rpc/include/crow-rpc/server/server.h` — moved + updated
  (response dispatch)
- `lib/crow-rpc/include/crow-rpc/server/handler.h` — NEW
- `lib/crow-rpc/include/crow-rpc/server/message.h` — NEW
- `lib/crow-rpc/include/crow-rpc/client/caller.h` — moved + updated
  (on_frame wiring)
- `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h` — moved +
  updated (connect method)
- `lib/crow-rpc/include/crow-rpc/transport/epoll/epoll_engine.h` — moved
- `lib/crow-rpc/include/crow-rpc/transport/kqueue/kqueue_engine.h` —
  moved
- `lib/crow-rpc/include/crow-rpc/transport/rdma/rdma_transport.h` —
  moved
- `lib/crow-rpc/src/buffer.cpp` — unchanged
- `lib/crow-rpc/src/framing.cpp` — unchanged
- `lib/crow-rpc/src/connection.cpp` — unchanged
- `lib/crow-rpc/src/pool.cpp` — unchanged
- `lib/crow-rpc/src/scheduled_executor.cpp` — unchanged
- `lib/crow-rpc/src/c_api.cpp` — updated (connect through transport,
  caller_call end-to-end)
- `lib/crow-rpc/src/server/server.cpp` — moved + updated (response
  dispatch)
- `lib/crow-rpc/src/server/handler.cpp` — NEW
- `lib/crow-rpc/src/server/message.cpp` — NEW
- `lib/crow-rpc/src/client/caller.cpp` — moved + updated (on_frame
  wiring)
- `lib/crow-rpc/src/transport/socket_transport.cpp` — moved + updated
  (connect method)
- `lib/crow-rpc/src/transport/epoll/epoll_engine.cpp` — moved
- `lib/crow-rpc/src/transport/kqueue/kqueue_engine.cpp` — moved
- `lib/crow-rpc/src/transport/rdma/rdma_transport.cpp` — moved
- `lib/crow-rpc/src/transport/rdma/rdma_buffer_pool.cpp` — moved
- `lib/crow-rpc/tests/loopback_test.cpp` — NEW
- `lib/crow-rpc/tests/load_test.cpp` — NEW
- `lib/crow-rpc/tests/*.cpp` — #include path updates
- `lib/crow-rpc/CMakeLists.txt` — path updates
- `lib/crow-rpc/ffi/build.rs` — path updates
- `lib/crow-rpc/ffi/src/*.rs` — #include path updates (via build.rs)
- `lib/crow-rpc/ffi/tests/ffi_loopback.rs` — NEW tests

## Test Checklist

### C++ Unit Tests
- [ ] `loopback_test.cpp`: SimplePing — client sends ping, receives
  ping response, request_id matches
- [ ] `loopback_test.cpp`: EchoHandler — client sends 512B data, echo
  handler responds, data matches
- [ ] `load_test.cpp`: MultiThreadLoad — 4 threads × 2 connections ×
  100 requests × 512B data, all responses verified

### Rust FFI Tests
- [ ] `ffi_loopback.rs`: FfiLoopback — server + caller + 512B data +
  await response
- [ ] `ffi_loopback.rs`: FfiConcurrent — multiple tokio tasks sending
  concurrently

### Existing Tests (no regression)
- [ ] `buffer_test.cpp` — all 5 cases pass
- [ ] `framing_test.cpp` — all 7 cases pass
- [ ] `transport_test.cpp` — both cases pass
- [ ] `caller_pool_test.cpp` — all 6 cases pass
- [ ] `server_test.cpp` — both cases pass
