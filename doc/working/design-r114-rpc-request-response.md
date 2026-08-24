<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# crow-rpc Request-Response Completion + RequestId Generator (R114)

Backlog doc: `doc/backlog/R114-rpc-streaming-support.md`.
Root design: `doc/design/rpc/design-crow-rpc.md` §4.2 (Request/Response
Correlation), §4.4 (Server Side), §6 (Flatbuffer Wrapper Convention).
Architecture decisions and rationale are in the root design; this doc
does not repeat them.

R104 shipped crow-rpc with request-response and one-way messages. The
client side (`RpcClient`) sends requests and routes responses by
`request_id`. The server side (`RpcServer` + `HandlerRegistry`)
dispatches incoming requests by `msg_type` and submits responses via
`transport->submit`. Both halves work independently but are wired
one-directionally: the client only routes responses, the server only
dispatches requests. R114 completes the model so either side can send
requests and receive responses — no streaming, no protocol change.

## 1. RequestId Generator (crow-common)

### 1.1 Why

The `request_id` generator is duplicated: C++
`RpcClient::next_request_id_` (`client/client.h` L162, used by 4 test
call sites), Rust `DiskioClient::next_req_id` (`crow-diskio-client/
src/client.rs` L86). R117's `CrowkvClient` will need the same
generator. One definition in `crow-common` replaces all copies.

### 1.2 How

**Rust** (`lib/crow-common/rust/src/request_id.rs`):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(pub u64);

impl RequestId {
    pub fn as_u64(self) -> u64 { self.0 }
}

pub struct RequestIdGen {
    counter: AtomicU64,
}

impl RequestIdGen {
    pub fn new() -> Self { Self { counter: AtomicU64::new(1) } }
    pub fn next(&self) -> RequestId {
        RequestId(self.counter.fetch_add(1, Ordering::Relaxed))
    }
}
```

`RequestId` is a `Copy` newtype — not accidentally interchangeable
with raw `u64` at call sites. `as_u64()` is the FFI boundary
conversion. `RequestIdGen` is per-client (not global) — `request_id`
only needs uniqueness within one client's pending map, and per-client
counters yield smaller numbers → smaller slab pool + pending hashmap.

Re-export from `lib/crow-common/rust/src/lib.rs`:
`pub mod request_id;` + `pub use request_id::{RequestId, RequestIdGen};`.

**C++** (`lib/crow-common/cpp/include/crow-common/request_id.h`):

```cpp
namespace crow::common {
class RequestIdGen {
  public:
    RequestIdGen() : counter_(1) {}
    uint64_t next() {
        return counter_.fetch_add(1, std::memory_order_relaxed);
    }
  private:
    std::atomic<uint64_t> counter_;
};
} // namespace crow::common
```

Header-only (no `.cpp` needed). The CMake `GLOB_RECURSE` picks up
sources automatically; headers are found via the existing
`target_include_directories`.

**C++ `RpcClient::next_request_id_` removal**: delete the
`next_request_id_` member and the `next_request_id()` method from
`client/client.h`. Update the 4 test call sites to use
`crow::common::RequestIdGen`:
- `tests/load_test.cpp` L142, L276, L402
- `tests/client_pool_test.cpp` L254

Each test creates a `RequestIdGen gen;` and calls `gen.next()` instead
of `caller->next_request_id()`.

**Rust `DiskioClient` update**: add `crow-common` dependency to
`crow-diskio-client/Cargo.toml`. Replace
`next_req_id: AtomicU64` with `request_id_gen: RequestIdGen`.
Replace `next_id()` with `self.request_id_gen.next().as_u64()`.

Edge cases:
- `RequestIdGen::new()` starts at 1 (not 0) — request_id 0 is
  reserved as "uninitialized" in the slab pool (`SLOT_FREE` state).
- Concurrent `next()` calls never return the same value
  (`fetch_add` is atomic).

## 2. on_response Returns bool

### 2.1 Why

Currently `RpcClient::on_response(request_id, frame)` is `void` and
**deletes the frame** if the request_id is not found in the pending
map (`client.cpp` L190-191: `delete response; return;`). This eats
request frames when the connection carries both responses (to
client-sent requests) and requests (server-initiated). The caller
needs to know whether the frame was consumed so it can dispatch it as
a request.

### 2.2 How

Change `on_response` signature from `void` to `bool`:

```cpp
// Returns true if request_id was found (frame consumed, callback
// invoked). Returns false if not found (frame NOT consumed — caller
// owns it and must dispatch or delete it).
bool on_response(uint64_t request_id, Frame *response);
```

Implementation change: on the "not found" paths (slab miss + map
miss), return `false` instead of `delete response; return;`. On the
"found" paths (slab hit + map hit), return `true` after invoking the
callback (existing behavior — the callback consumes the frame via
`frame_to_c_handles`).

The slab path has two miss cases:
- Slot state is not `SLOT_PENDING_READY` → fall through to map (no
  frame deletion here — existing behavior).
- Slot state is `SLOT_PENDING_READY` but `request_id` doesn't match →
  fall through to map (existing behavior).
- Map miss → was `delete response; return;`, now `return false;`.

Edge cases:
- Late response after timeout (reaper already claimed the slot) →
  map miss → return `false`. The caller dispatches it as a request.
  But it's not a request — it's a stale response. The handler
  registry won't have a handler for its `msg_type` (response types
  aren't registered as handlers), so the frame is dropped. This is
  correct — a stale response should be dropped, not crash.
- Duplicate response (same request_id arrives twice) → first one
  matches and consumes the slot; second one misses → return `false`
  → dropped by the caller. Correct.

## 3. Server-Side Request-Response Correlation

### 3.1 Why

The server dispatches incoming requests by `msg_type`
(`RpcServer::dispatch`). For WatchNotify, the server sends notify
**requests** to clients and awaits ack **responses**. The server
needs request-response correlation: send a request, track its
`request_id` in a pending map, route the ack when it arrives. This is
exactly what `RpcClient` provides (`send` + `on_response` + pending
map + reaper). The server reuses `RpcClient` for this — no new
correlation mechanism.

### 3.2 How

**`RpcServer` changes** (`include/crow-rpc/server/server.h`,
`src/server/server.cpp`):

New member:
```cpp
RpcClient *request_client_{nullptr}; // set via set_request_client
```

New method:
```cpp
void set_request_client(RpcClient *client) { request_client_ = client; }
```

`dispatch()` modified — handler-first order, then response routing:
```cpp
void RpcServer::dispatch(Frame *frame, Connection *conn) {
    // Try request dispatch first — if a handler is registered for
    // this msg_type, dispatch as a request. This ensures request
    // frames are not intercepted by on_response (which matches by
    // request_id and can't distinguish a request from its ack).
    HandlerFn handler = handlers_.get_handler(msg_type);
    if (handler) {
        // dispatch as request (existing handler logic)
        ...
        return;
    }
    // No handler for this msg_type — try response routing (ack to
    // a server-sent request). on_response consumes the frame if the
    // request_id is in the request client's pending map.
    if (request_client_ != nullptr &&
        request_client_->on_response(frame->request_id, frame)) {
        return; // ack routed, frame consumed
    }
    // Unknown msg_type and not an ack — send UnknownMessage or drop.
    ...
}
```

The handler-first order is correct because request types have
handlers registered (dispatched as requests), while response types
(acks) don't have handlers → `on_response` routes them. This avoids
the loopback ambiguity where a request frame's `request_id` matches
the `request_client_`'s pending map (the frame would be incorrectly
consumed as a "response" if `on_response` were tried first).

The `request_client_` does NOT call `attach()` — it's not attached to
any specific connection. Its `on_response` is called directly from
`dispatch()` when an incoming frame's `request_id` matches a pending
entry.

The server sends requests via `request_client_->send(transport_,
conn, request_id, ...)` — same `send()` API the client uses. The
`transport_` and `conn` are available from the server's dispatch
context.

**Connection close handling**: the server's acceptor loop sets
`on_close` to fail pending entries on the request client:
```cpp
conn->set_on_close([this](Connection *) {
    if (request_client_ != nullptr) {
        request_client_->fail_all(RpcError::ConnectionClosed);
    }
});
```

`fail_all` fails ALL pending entries (not per-connection). For R114's
test scope (one connection), this is correct. For production (R117's
WatchNotify with multiple watcher connections), R117 creates a
per-connection `RpcClient` or adds connection-scoped fail_all. R114
delivers the mechanism; R117 refines it.

**C ABI** (`include/crow-rpc/c_api.h`, `src/c_api.cpp`):
```c
void crow_rpc_server_set_request_client(
    crow_rpc_server_t server, crow_rpc_client_t client);
```
Implementation: `server->server->set_request_client(client->client);`

**Rust FFI** (`ffi/src/server.rs`):
```rust
pub fn set_request_client(&self, client: &RpcClient) {
    unsafe { sys::crow_rpc_server_set_request_client(
        self.handle, client.handle()) };
}
```

Edge cases:
- `request_client_` is null (not set) → `dispatch` skips response
  routing, behaves as before (request-only dispatch). Backward
  compatible.
- Server sends a request, client drops → `fail_all` fires with
  `ConnectionClosed` → the pending callback receives the error.
- Server sends a request, client doesn't ack → reaper times out the
  entry → the pending callback receives `Timeout`.

## 4. Client-Side Request Handler Dispatch

### 4.1 Why

The client routes incoming responses by `request_id`
(`RpcClient::attach` → `on_response`). For WatchNotify, the client
receives notify **requests** from the server and sends ack
**responses**. The client needs request handler dispatch: look up a
handler by `msg_type`, invoke it, submit the response. This mirrors
the server-side `HandlerRegistry` + `transport->submit` pattern.

### 4.2 How

**`RpcClient` changes** (`include/crow-rpc/client/client.h`,
`src/client/client.cpp`):

New members:
```cpp
// Handler registry for incoming requests (server→client direction).
// Maps msg_type → (C callback, user_data). Same trampoline pattern
// as the server-side c_handler_trampoline.
std::mutex handler_mu_;
std::unordered_map<uint16_t,
    std::pair<crow_rpc_handler_fn, void*>> request_handlers_;
// Transport for submitting UnknownMessage responses when no handler
// matches. Set via set_transport. Null = drop unmatched frames.
Transport *transport_{nullptr};
```

New methods:
```cpp
void register_handler(uint16_t msg_type,
                      crow_rpc_handler_fn callback,
                      void *user_data);
void set_transport(Transport *t) { transport_ = t; }
void dispatch_request(Frame *frame, Connection *conn);
```

`attach()` modified — sets on_frame to combined routing:
```cpp
void RpcClient::attach(Connection *conn) {
    conn->set_on_frame([this](Frame *frame, Connection *c) {
        if (!on_response(frame->request_id, frame)) {
            dispatch_request(frame, c);
        }
    });
}
```

`dispatch_request()` — new method:
```cpp
void RpcClient::dispatch_request(Frame *frame, Connection *conn) {
    uint16_t msg_type = frame->header.msg_type;
    bool is_one_way = (frame->header.flags & FLAG_ONE_WAY) != 0;

    crow_rpc_handler_fn cb = nullptr;
    void *user_data = nullptr;
    {
        std::lock_guard<std::mutex> lock(handler_mu_);
        auto it = request_handlers_.find(msg_type);
        if (it != request_handlers_.end()) {
            cb = it->second.first;
            user_data = it->second.second;
        }
    }

    if (cb != nullptr) {
        // Same trampoline as server-side c_handler_trampoline:
        // extract fields, invoke C callback, delete frame, return
        // nullptr (async — callback submits response later).
        invoke_handler_trampoline(cb, user_data, frame, conn);
        return;
    }

    // No handler registered for this msg_type.
    if (is_one_way) {
        delete frame;
        return;
    }
    if (transport_ != nullptr) {
        // Send UnknownMessage response (same as server-side behavior).
        OutFrame *resp = handle_unknown(frame, conn);
        delete frame;
        if (resp != nullptr) {
            transport_->submit(conn, resp);
        }
    } else {
        // No transport — drop the frame. The server's request will
        // time out; the retry/WSCritical logic handles it.
        delete frame;
    }
}
```

The `invoke_handler_trampoline` is the same logic as the server-side
`c_handler_trampoline` in `c_api.cpp` — extract `request_id`,
`rpc_create_nano`, `msg_type`, control/data pointers, `conn_handle`,
invoke the C callback, delete the frame. The callback submits the
response later via `crow_rpc_server_submit_response` (the Rust
closure captures the server handle, same as the server-side handler
pattern).

**C ABI** (`include/crow-rpc/c_api.h`, `src/c_api.cpp`):
```c
void crow_rpc_client_register_handler(
    crow_rpc_client_t client, uint16_t msg_type,
    crow_rpc_handler_fn callback, void *user_data);

void crow_rpc_client_set_transport(
    crow_rpc_client_t client, crow_rpc_server_t server);
```

`crow_rpc_client_set_transport` implementation:
`client->client->set_transport(server->server->transport());`

**Rust FFI** (`ffi/src/client.rs`):

```rust
/// A borrowed view of an incoming client-side request, passed to a
/// handler registered via `RpcClient::register_handler`. Same shape
/// as ServerRequest — the handler submits the response via
/// `RpcServer::submit_response` using the captured server handle.
pub struct ClientRequest<'a> {
    pub request_id: u64,
    pub rpc_create_nano: u64,
    pub msg_type: u16,
    pub control: &'a [u8],
    pub data: Option<&'a [u8]>,
    pub conn_handle: *mut std::ffi::c_void,
}

pub fn register_handler<F>(&self, msg_type: u16, handler: F)
where F: Fn(ClientRequest<'_>) + Send + 'static;

pub fn set_transport(&self, server: &RpcServer);
```

The `register_handler` implementation mirrors `RpcServer::register_handler`:
box the closure, pass the box pointer as `user_data`, use the same
trampoline pattern. The trampoline is a new `extern "C" fn` that
constructs a `ClientRequest` and invokes the closure — identical to
`rust_handler_trampoline` in `server.rs` but with `ClientRequest`
instead of `ServerRequest`.

The handler closure captures the server handle (as `Arc<RpcServer>`)
so it can call `server.submit_response(conn_handle, ...)` to send the
ack response back. Same pattern as the server-side handler test
(`ffi_handler_test.rs`).

Edge cases:
- Client receives a response frame (matching a pending client-sent
  request) → `on_response` returns `true` → routed to pending
  callback, not handler dispatch. Correct.
- Client receives a request frame with unregistered `msg_type` and
  `transport_` is set → `UnknownMessage` response sent. If
  `transport_` is null → frame dropped, server times out.
- Client receives a one-way request with unregistered `msg_type` →
  frame dropped (no response expected).
- No handlers registered + no transport set → `attach()` behavior
  degrades to response routing only (handler dispatch finds nothing,
  drops the frame). Backward compatible with existing callers.

## 5. Test Design

### 5.1 Unit Tests (UT)

**RequestIdGen** (in `lib/crow-common/rust/tests/` or inline):
- `RequestIdGen::next()` returns monotonically increasing values.
  Concurrent `next()` from N threads never returns duplicates.
- `RequestId::as_u64()` returns the inner value; `RequestId` is
  `Copy` + `PartialEq` + `Hash`.

**C++ RequestIdGen** (in `lib/crow-common/cpp/tests/`):
- `RequestIdGen::next()` returns monotonically increasing values.
  Concurrent `next()` from N threads never returns duplicates.

### 5.2 End-to-End Tests (E2E)

New file: `lib/crow-rpc/ffi/tests/ffi_request_test.rs`.

All tests use a single server + single client connection (loopback).

**Server→client request-response** (the new path):
- Server registers a handler for a custom msg_type (e.g. 300). Client
  calls `set_transport(server)` + `register_handler(300, handler)` +
  `attach(conn)`. Server calls `set_request_client(client)` + sends a
  request via `client.send(server, conn, request_id, ...)`. Client
  handler receives the request, submits an ack response via
  `server.submit_response(conn_handle, ...)`. Server's `send` callback
  fires with the ack. Assert: ack `request_id` matches, response
  received within timeout.

**Client→server request-response** (regression):
- Existing `ping_loopback` pattern — client sends a request, server's
  built-in ping handler responds. Assert: response received. This is
  already covered by `ffi_loopback.rs`; the new test file includes
  one regression case to verify the `on_response` return-bool change
  doesn't break the existing path.

**Server→client, client drops (fail_all)**:
- Server sends a request, client does NOT register a handler (and
  does NOT set transport → frame dropped). Server's reaper times out
  the entry → `send` callback fires with `Timeout`. Assert: callback
  receives `Timeout` status within reaper timeout.

**Server→client, connection drops (fail_all)**:
- Server sends a request, then drops the connection. Server's
  `on_close` fires `fail_all(ConnectionClosed)`. Assert: callback
  receives `ConnectionClosed` status.

**Client receives unregistered msg_type**:
- Server sends a request with a msg_type the client hasn't
  registered. Client has `transport_` set → sends `UnknownMessage`
  response. Assert: server receives `UnknownMessage` response (or
  timeout if the server doesn't have a handler for UnknownMessage —
  in that case, assert the server's request times out, which proves
  the client didn't process it).

**Client receives response (not request)**:
- Client sends a request to the server (ping), server responds. The
  response frame's `request_id` matches the client's pending entry →
  `on_response` returns `true` → routed to pending callback, NOT to
  handler dispatch. Assert: response received via `CallFuture`, not
  via the handler. This is the regression case for the routing
  precedence rule.

## Scope

**crow-common (Rust)**:
- `lib/crow-common/rust/src/request_id.rs` — new: `RequestId` newtype + `RequestIdGen` struct.
- `lib/crow-common/rust/src/lib.rs` — add `pub mod request_id` + re-exports.

**crow-common (C++)**:
- `lib/crow-common/cpp/include/crow-common/request_id.h` — new: `RequestIdGen` class.
- `lib/crow-common/cpp/tests/request_id_test.cpp` — new: unit test for `RequestIdGen`.

**crow-rpc (C++)**:
- `lib/crow-rpc/include/crow-rpc/client/client.h` — `on_response` returns `bool`; add `register_handler`, `set_transport`, `dispatch_request`, handler registry members; remove `next_request_id_` + `next_request_id()`.
- `lib/crow-rpc/src/client/client.cpp` — `on_response` returns `bool` (don't delete on miss); `attach` sets combined routing; `dispatch_request` implementation; `register_handler` + `set_transport` implementations.
- `lib/crow-rpc/include/crow-rpc/server/server.h` — add `request_client_` member + `set_request_client` method.
- `lib/crow-rpc/src/server/server.cpp` — `dispatch` tries `on_response` first; acceptor loop sets `on_close` to `fail_all`.
- `lib/crow-rpc/include/crow-rpc/c_api.h` — add `crow_rpc_client_register_handler`, `crow_rpc_client_set_transport`, `crow_rpc_server_set_request_client`.
- `lib/crow-rpc/src/c_api.cpp` — implement the 3 new C ABI functions; extract handler trampoline into a shared helper (reused by both server and client dispatch).
- `lib/crow-rpc/tests/load_test.cpp` — replace `next_request_id()` with `RequestIdGen`.
- `lib/crow-rpc/tests/client_pool_test.cpp` — replace `next_request_id()` with `RequestIdGen`.

**crow-rpc-ffi (Rust)**:
- `lib/crow-rpc/ffi/src/sys.rs` — add extern declarations for the 3 new C ABI functions.
- `lib/crow-rpc/ffi/src/client.rs` — add `register_handler`, `set_transport`, `ClientRequest` struct, client handler trampoline.
- `lib/crow-rpc/ffi/src/server.rs` — add `set_request_client`.
- `lib/crow-rpc/ffi/tests/ffi_request_test.rs` — new: E2E tests for both request-response directions.

**crow-diskio-client (Rust)**:
- `lib/crow-diskio-client/Cargo.toml` — add `crow-common` dependency.
- `lib/crow-diskio-client/src/client.rs` — replace `next_req_id: AtomicU64` with `RequestIdGen`; update `next_id()`.

## Complexity

Medium. The core changes are small (on_response return bool, dispatch
tries response routing first, client gets a handler registry). The
main challenge is the C ABI + FFI plumbing: 3 new C ABI functions, 2
new Rust FFI methods, a new trampoline, and a new test file. The
RequestIdGen consolidation is trivial. The `on_response` return-bool
change is the riskiest — it changes frame ownership semantics on the
miss path (was: delete; now: caller owns). All existing callers of
`on_response` are updated to handle the return value.

## Module Structure

```
lib/crow-common/rust/src/
  request_id.rs          NEW — RequestId newtype + RequestIdGen
  lib.rs                 MOD — re-export request_id
lib/crow-common/cpp/include/crow-common/
  request_id.h           NEW — RequestIdGen class
lib/crow-common/cpp/tests/
  request_id_test.cpp    NEW — RequestIdGen unit test
lib/crow-rpc/include/crow-rpc/
  c_api.h                MOD — 3 new C ABI declarations
  c_api_internal.h       MOD — (if needed for shared trampoline)
  client/client.h        MOD — on_response bool, handler registry, set_transport
  server/server.h        MOD — request_client_ + set_request_client
lib/crow-rpc/src/
  client/client.cpp      MOD — on_response bool, attach combined, dispatch_request
  server/server.cpp      MOD — dispatch tries on_response, on_close fail_all
  c_api.cpp              MOD — 3 new C ABI impls, shared trampoline
lib/crow-rpc/tests/
  load_test.cpp          MOD — RequestIdGen replaces next_request_id
  client_pool_test.cpp   MOD — RequestIdGen replaces next_request_id
lib/crow-rpc/ffi/src/
  sys.rs                 MOD — 3 new extern declarations
  client.rs              MOD — register_handler, set_transport, ClientRequest, trampoline
  server.rs              MOD — set_request_client
lib/crow-rpc/ffi/tests/
  ffi_request_test.rs    NEW — E2E tests for both directions
lib/crow-diskio-client/
  Cargo.toml             MOD — add crow-common dep
  src/client.rs          MOD — RequestIdGen replaces next_req_id
```

## Config Extensions

None.

## Server Wiring

No server wiring changes for R114. The `set_request_client` and
`register_handler` APIs are called by R117 (WatchNotify) and R32
(LearnerStream) when they migrate to crow-rpc. R114 provides the
mechanism + tests; the service-level wiring is the consumers' scope.

## Open Questions

None — all design decisions resolved in `doc/working/todo_fb.md`
§ "R114 — Revised Design" (including "Copy Streaming" and "Pull vs
Push"). The `fail_all` limitation (fails all pending entries, not
per-connection) is noted as R117's refinement scope.
