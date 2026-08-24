<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R114 — crow-rpc Request-Response Completion Plan

Design: `doc/working/design-r114-rpc-request-response.md`.
Backlog: `doc/backlog/R114-rpc-streaming-support.md`.
Goal: complete the request-response model on both sides of crow-rpc
(server can send requests, client can handle requests) and
consolidate the request_id generator into crow-common.

## Phase 1: RequestId Generator

- [ ] **Rust RequestIdGen**: Add `lib/crow-common/rust/src/request_id.rs`
  with `RequestId(u64)` newtype + `RequestIdGen` struct. Re-export from
  `lib.rs`. Files: `lib/crow-common/rust/src/request_id.rs`,
  `lib/crow-common/rust/src/lib.rs`.
- [ ] **Rust RequestIdGen unit test**: Add test for monotonicity +
  concurrent uniqueness. Files: `lib/crow-common/rust/src/request_id.rs`
  (inline `#[cfg(test)]` module).
- [ ] **C++ RequestIdGen**: Add
  `lib/crow-common/cpp/include/crow-common/request_id.h` with
  `RequestIdGen` class. Files:
  `lib/crow-common/cpp/include/crow-common/request_id.h`.
- [ ] **C++ RequestIdGen unit test**: Add
  `lib/crow-common/cpp/tests/request_id_test.cpp`. Files:
  `lib/crow-common/cpp/tests/request_id_test.cpp`.

## Phase 2: on_response Returns bool

- [ ] **Change on_response signature**: `void` → `bool` in
  `client/client.h` + `client.cpp`. On miss, return `false` (don't
  delete frame). On hit, return `true` after callback. Files:
  `lib/crow-rpc/include/crow-rpc/client/client.h`,
  `lib/crow-rpc/src/client/client.cpp`.
- [ ] **Update attach() callback**: `attach()` sets on_frame to
  combined routing: `if (!on_response(...)) { dispatch_request(...); }`.
  Files: `lib/crow-rpc/src/client/client.cpp`.

## Phase 3: Client-Side Handler Dispatch

- [ ] **Add handler registry to RpcClient**: `register_handler`,
  `set_transport`, `dispatch_request` methods + handler map +
  transport member in `client.h` + `client.cpp`. Files:
  `lib/crow-rpc/include/crow-rpc/client/client.h`,
  `lib/crow-rpc/src/client/client.cpp`.
- [ ] **Extract shared handler trampoline**: Move
  `c_handler_trampoline` logic into a shared helper in `c_api.cpp` (or
  `c_api_internal.h`) so both server and client dispatch can invoke C
  callbacks the same way. Files: `lib/crow-rpc/src/c_api.cpp`,
  `lib/crow-rpc/include/crow-rpc/c_api_internal.h`.
- [ ] **C ABI for client handler**: Add
  `crow_rpc_client_register_handler` + `crow_rpc_client_set_transport`
  to `c_api.h` + `c_api.cpp`. Files:
  `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/src/c_api.cpp`.
- [ ] **Rust FFI client handler**: Add `register_handler`,
  `set_transport`, `ClientRequest` struct, client trampoline to
  `client.rs`. Add extern declarations to `sys.rs`. Files:
  `lib/crow-rpc/ffi/src/client.rs`,
  `lib/crow-rpc/ffi/src/sys.rs`.

## Phase 4: Server-Side Request-Response

- [ ] **Add request_client_ to RpcServer**: `set_request_client`
  method + member in `server.h`. Modify `dispatch()` to try
  `on_response` first. Set `on_close` to `fail_all` in acceptor loop.
  Files: `lib/crow-rpc/include/crow-rpc/server/server.h`,
  `lib/crow-rpc/src/server/server.cpp`.
- [ ] **C ABI for server request client**: Add
  `crow_rpc_server_set_request_client` to `c_api.h` + `c_api.cpp`.
  Files: `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/src/c_api.cpp`.
- [ ] **Rust FFI server set_request_client**: Add method to
  `server.rs` + extern to `sys.rs`. Files:
  `lib/crow-rpc/ffi/src/server.rs`,
  `lib/crow-rpc/ffi/src/sys.rs`.

## Phase 5: Remove next_request_id + Update Consumers

- [ ] **Remove next_request_id_ from RpcClient**: Delete the member +
  method from `client.h`. Files:
  `lib/crow-rpc/include/crow-rpc/client/client.h`.
- [ ] **Update C++ test call sites**: Replace
  `caller->next_request_id()` with `crow::common::RequestIdGen` in
  `load_test.cpp` (L142, L276, L402) + `client_pool_test.cpp` (L254).
  Files: `lib/crow-rpc/tests/load_test.cpp`,
  `lib/crow-rpc/tests/client_pool_test.cpp`.
- [ ] **Update DiskioClient**: Add `crow-common` dep to
  `Cargo.toml`. Replace `next_req_id: AtomicU64` with `RequestIdGen`.
  Update `next_id()`. Files: `lib/crow-diskio-client/Cargo.toml`,
  `lib/crow-diskio-client/src/client.rs`.

## Phase 6: E2E Tests

- [ ] **Write ffi_request_test.rs**: Server→client request-response,
  client→server regression, server→client timeout (fail_all),
  server→client connection drop (fail_all), client unregistered
  msg_type, client response routing precedence. Files:
  `lib/crow-rpc/ffi/tests/ffi_request_test.rs`.

## File List

- `lib/crow-common/rust/src/request_id.rs` — NEW: RequestId + RequestIdGen
- `lib/crow-common/rust/src/lib.rs` — MOD: re-export request_id
- `lib/crow-common/cpp/include/crow-common/request_id.h` — NEW: RequestIdGen
- `lib/crow-common/cpp/tests/request_id_test.cpp` — NEW: unit test
- `lib/crow-rpc/include/crow-rpc/client/client.h` — MOD: on_response bool, handler registry, set_transport, remove next_request_id
- `lib/crow-rpc/src/client/client.cpp` — MOD: on_response bool, attach combined, dispatch_request, register_handler, set_transport
- `lib/crow-rpc/include/crow-rpc/server/server.h` — MOD: request_client_ + set_request_client
- `lib/crow-rpc/src/server/server.cpp` — MOD: dispatch tries on_response, on_close fail_all
- `lib/crow-rpc/include/crow-rpc/c_api.h` — MOD: 3 new C ABI declarations
- `lib/crow-rpc/include/crow-rpc/c_api_internal.h` — MOD: shared trampoline helper (if needed)
- `lib/crow-rpc/src/c_api.cpp` — MOD: 3 new C ABI impls, shared trampoline
- `lib/crow-rpc/tests/load_test.cpp` — MOD: RequestIdGen
- `lib/crow-rpc/tests/client_pool_test.cpp` — MOD: RequestIdGen
- `lib/crow-rpc/ffi/src/sys.rs` — MOD: 3 new extern declarations
- `lib/crow-rpc/ffi/src/client.rs` — MOD: register_handler, set_transport, ClientRequest, trampoline
- `lib/crow-rpc/ffi/src/server.rs` — MOD: set_request_client
- `lib/crow-rpc/ffi/tests/ffi_request_test.rs` — NEW: E2E tests
- `lib/crow-diskio-client/Cargo.toml` — MOD: add crow-common dep
- `lib/crow-diskio-client/src/client.rs` — MOD: RequestIdGen

## Test Checklist

**Unit**:
- [ ] RequestIdGen monotonicity + concurrent uniqueness (Rust)
- [ ] RequestIdGen monotonicity + concurrent uniqueness (C++)
- [ ] RequestId::as_u64() returns inner value

**Integration (ffi_request_test.rs)**:
- [ ] Server→client request-response: server sends, client handles + acks
- [ ] Client→server regression: client sends ping, server responds
- [ ] Server→client timeout: client doesn't ack, reaper times out
- [ ] Server→client connection drop: fail_all fires ConnectionClosed
- [ ] Client unregistered msg_type: UnknownMessage or drop
- [ ] Client response routing precedence: response routed to pending, not handler

**Integration (existing tests, regression)**:
- [ ] ffi_loopback.rs: ping_loopback still passes
- [ ] ffi_handler_test.rs: custom_rust_handler_loopback still passes
- [ ] crow-diskio-client tests: DiskioClient with RequestIdGen

**Commands**:
- `pixi run test-common` (RequestIdGen Rust unit test)
- `pixi run test-rpc-ffi` (ffi_request_test + existing loopback/handler tests)
- `pixi run test-rpc-ct` (C++ tests: RequestIdGen + load_test + client_pool_test)
- `pixi run test-diskio-client` (DiskioClient with RequestIdGen)
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
