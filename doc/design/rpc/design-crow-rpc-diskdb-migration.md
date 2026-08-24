<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0 -->

# CROW - Design: diskdb crow-rpc Service

Depends on: [`design-crow-rpc.md`](design-crow-rpc.md) §3 (Wire Format),
§4 (Control Plane), §6 (Flatbuffer Wrapper Convention);
[`design-crow-rpc-tcp.md`](design-crow-rpc-tcp.md) §2 (SocketTransport).
Satisfies: `design-crow-rpc.md` §4.4 (Server Side — Rust handler
dispatch), §6 (Flatbuffer Wrapper Convention — diskdb schema).

The diskdb service uses crow-rpc (flatbuffer over TCP). This doc covers
the server-side handler wiring, client-side transport, error model, and
the `conn_handle` lifetime safety in `SocketTransport`. Architecture
and rationale for the crow-rpc engine itself are in
`design-crow-rpc.md` §3–§6; this doc does not repeat them.

## Table of Contents

- [1. Server-side handler wiring](#1-server-side-handler-wiring)
- [2. Client-side transport](#2-client-side-transport)
- [3. Error model](#3-error-model)
- [4. Port allocation + reserved schemas](#4-port-allocation--reserved-schemas)
- [5. conn_handle lifetime safety](#5-conn_handle-lifetime-safety)
- [6. rpc_endpoint field](#6-rpc_endpoint-field)

## 1. Server-side handler wiring

### 1.1 Why

diskdb's service is Rust (`DiskdbRpcService`). The diskio reference is
C++ and registers handlers via `server.register_handler(...)`) directly.
Rust servers need a C ABI bridge to register handlers —
`RpcServer::register_handler<F>` provides this. Without it, Rust
servers cannot use crow-rpc.

### 1.2 Handler module

`DiskdbRpcService` — a struct holding the diskdb service dependencies
(`container`, `kv`, `storage`, `zone_loader`, `recalc`, `scan_state`,
`metrics`). Method `register_handlers(&self, server: &RpcServer)`
registers one handler per request `msg_type` (11 handlers).

Each handler closure:
a. Parses the request flatbuffer from `ServerRequest::control` via
   `flatbuffers::root::<FBAllocateBlocksRequest>(req.control)` (or the
   appropriate type).
b. Extracts the domain fields (zero-copy reads through the root
   pointer — no owned struct, no per-field copy except where the
   diskdb logic already owns the type).
c. Calls the diskdb logic (`model::alloc::allocate_blocks`,
   `compact_zone`, `ScanState::trigger`, etc.).
d. Builds the response flatbuffer (`FlatBufferBuilder` → `finish` →
   bytes), and submits via `server.submit_response(req.conn_handle,
   &ctrl_bytes, data, resp_msg_type, req.request_id)`.

The handler runs on the C++ I/O worker thread. The diskdb logic is
synchronous (in-memory allocator + KV client calls that are already
async via `DdbKvClient`). For the synchronous allocator path
(`AllocateBlocks`), the handler does the work inline and submits the
response before returning. For paths that call async KV ops, the
handler spawns a tokio task (the diskdb server runs a tokio runtime)
and submits the response from the task.

### 1.3 Edge cases

- Request flatbuffer parse fails (malformed/truncated) → respond with
  `ret_code=InvalidArgument`, `error_msg="invalid request flatbuffer"`.
- `disk_group_id` not owned by this instance → `ret_code=NotOwner`.
- Allocator returns no space → `ret_code=NoSpace`.
- KV client error during persist → `ret_code=Internal`.
- `conn_handle` invalidated before the async task submits →
  `submit_response` returns `ConnectionClosed`; logged at `warn!`, no
  retry (the client will retry).

## 2. Client-side transport

### 2.1 Why

The client uses `RpcClient` + `Connection` (per-endpoint). The
`disk_group_id → endpoint` cache + retry logic live in the client; the
transport layer is crow-rpc.

### 2.2 Structure

`DiskdbClient` keeps `svc: ServiceRegistryClient` + endpoint cache
(`DashMap<DiskGroupId, String>`) + `disk_to_dg` reverse map. The
`with_rpc_transport()` builder sets an `Option<Arc<DiskdbRpcTransport>>`;
when set, all 11 RPC methods dispatch via crow-rpc.

`DiskdbRpcTransport` holds a shared `RpcServer` (connection owner), a
shared `RpcClient` (completion pool), and a `DashMap<String,
Connection>` per-endpoint connection pool. Each RPC method:
a. Resolve endpoint for `disk_group_id` (cache → refresh on miss).
b. Get/create the `Connection` for that endpoint via `conn_for()`.
c. Build the request flatbuffer (`FlatBufferBuilder` → `finish` →
   bytes).
d. `rpc.call(&server, &conn, req_id, control, data, msg_type)` →
   `CallFuture`.
e. Await the future, parse the response flatbuffer, check `ret_code`.
f. On `RpcError` (transport): if `is_retryable()`, retry per
   `RetryConfig`; else return `DiskdbClientError::Rpc`.
g. On `ret_code != Success` (protocol error): map to
   `DiskdbClientError` variant (e.g. `NoSpace` → `Rpc("no space")`).

### 2.3 Edge cases

- Endpoint cache stale (instance moved) → `NotOwner` in response →
  refresh cache, retry once.
- Connection dropped mid-call → `ConnectionClosed` → retry on a fresh
  connection (reconnect).
- All endpoints down → `AllDown` → `DiskdbClientError::Unreachable`.

## 3. Error model

Error mapping:
- `From<RpcError> for DiskdbClientError` — transport errors.
  `ConnectionClosed`/`Timeout`/`SendQueueFull`/`ConnectionError` are
  retryable (`is_retryable()`); `RegistrationFailed`/`AllDown` are not.
- Response `ret_code` (`FBDiskdbRetCode`) → `DiskdbClientError` —
  protocol errors carried in the response body, not transport.
  `NoSpace`/`NotOwner`/`DiskNotFound`/`DiskGroupNotFound`/`Degraded` →
  `DiskdbClientError::Rpc(code_name)`; `InvalidArgument`/`Internal` →
  `DiskdbClientError::Rpc(msg)`.

## 4. Port allocation + reserved schemas

The diskdb server runs the crow-rpc server on `DISKDB_RPC_BASE` (in
`ports.rs`). The service registry stores the endpoint; clients use it
via `with_rpc_transport()`. The legacy `diskdb_service` and
`diskio_service` schemas are reserved in `crow-protocol`.

## 5. conn_handle lifetime safety

### 5.1 Problem

The dispatch callback runs on the C++ I/O worker thread. For handlers
that call async KV ops (`DdbKvClient`), the handler spawns a tokio
task and submits the response from it. The `conn_handle` is a raw
`Connection*` pointer; if the connection closes between spawn and
submit, the pointer dangles (use-after-free risk).

### 5.2 Fix

`SocketTransport` holds a live-connection registry that maps
`Connection*` → `weak_ptr<Connection>`. Connections are registered
when created (`create_connection`) and unregistered when closed (via
the `Connection::on_close` callback).

`submit()` looks up the connection before accessing it:
- If the connection is alive → `weak_ptr::lock()` returns a
  `shared_ptr`, and `submit()` proceeds normally.
- If the connection was closed and freed (stale handle) →
  `weak_ptr::lock()` returns null, and `submit()` frees the frame and
  returns false (no crash).
- If the connection was never registered (test/direct connection) →
  `lookup_conn()` returns `nullopt`, and `submit()` falls through to
  direct access (backward compat for tests).

## 6. rpc_endpoint field

The `rpc_endpoint` field is defined in 3 flatbuffer schemas
(`InstanceValue`, `ChunkdbRangeBindingValue` in `sysdata_type.fbs`;
`NotMyRangeHint` in `chunkdb_type.fbs`). Flatbuffers binary wire format
uses field offsets (not field names), so field renames are
wire-compatible. All Rust, TS, and C++ references use `rpc_endpoint`.
