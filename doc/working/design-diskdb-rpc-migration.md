<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskdbService gRPC → crow-rpc Migration (R115)

Implementation design draft for R115. The problem, dependencies, and
acceptance criteria are in
[`doc/backlog/R115-diskdb-rpc-migration.md`](../backlog/R115-diskdb-rpc-migration.md).
Architecture and rationale (flatbuffer wrapper convention, wire format,
handler dispatch) are in `doc/design/rpc/design-crow-rpc.md` §4–§6; this
doc does not repeat them.

**Already landed**: R104 (crow-rpc engine + FFI), R105 (diskio migration
— the reference pattern: `diskio.fbs`, `dio_server.cpp`,
`diskio-client/src/client.rs`). The `diskdb.fbs` schema + `msg_type.fbs`
3000s range + `diskdb_fb` re-exports are committed. The C API dispatch
callback (`crow_rpc_server_register_handler` + Rust
`RpcServer::register_handler` + `ServerRequest`) is committed — this
unblocked Rust-side server handlers (diskdb's server is Rust, unlike
diskio's C++ server). `RpcError::is_retryable()` is committed (shared
retryable classification for all migrations).

## 1. Server-side handler wiring

### 1.1 Why

diskdb's service is Rust (`app/crow-diskdb/src/service/diskdb_service.rs`,
899 lines, tonic `DiskdbService` trait). The diskio reference (R105) is
C++ and registers handlers via `server.register_handler(...)` directly.
Rust servers need a C ABI bridge to register handlers — added in the
dispatch-callback commit (`RpcServer::register_handler<F>`). Without it,
R115/R32/R117 (all Rust servers) cannot use crow-rpc.

### 1.2 Handler module

New module `app/crow-diskdb/src/service/diskdb_rpc_service.rs` — a
`DiskdbRpcService` struct holding the same dependencies as the tonic
`DiskdbService` (`container`, `kv`, `storage`, `zone_loader`, `recalc`,
`scan_state`, `metrics`). Method `register_handlers(&self, server:
&RpcServer)` registers one handler per request `msg_type` (11 handlers).

Each handler closure:
a. Parses the request flatbuffer from `ServerRequest::control` via
   `flatbuffers::root::<FBAllocateBlocksRequest>(req.control)` (or the
   appropriate type).
b. Extracts the domain fields (zero-copy reads through the root pointer —
   no owned struct, no per-field copy except where the existing diskdb
   logic already owns the type).
c. Calls the existing diskdb logic (`model::alloc::allocate_blocks`,
   `compact_zone`, `ScanState::trigger`, etc.) — reused verbatim from the
   tonic handler bodies.
d. Builds the response flatbuffer (`FlatBufferBuilder` → `finish` →
   bytes), and submits via `server.submit_response(req.conn_handle,
   &ctrl_bytes, data, resp_msg_type, req.request_id)`.

The handler runs on the C++ I/O worker thread. The existing diskdb logic
is synchronous (in-memory allocator + KV client calls that are already
async via `DdbKvClient`). For the synchronous allocator path
(`AllocateBlocks`), the handler does the work inline and submits the
response before returning. For paths that call async KV ops, the handler
spawns a tokio task (the diskdb server already runs a tokio runtime) and
submits the response from the task.

### 1.3 Edge cases

- Request flatbuffer parse fails (malformed/truncated) → respond with
  `ret_code=InvalidArgument`, `error_msg="invalid request flatbuffer"`.
- `disk_group_id` not owned by this instance → `ret_code=NotOwner`.
- Allocator returns no space → `ret_code=NoSpace`.
- KV client error during persist → `ret_code=Internal`.
- `conn_handle` invalidated before the async task submits →
  `submit_response` returns `ConnectionClosed`; logged at `warn!`, no
  retry (the client will retry).

## 2. Client-side rewrite

### 2.1 Why

The client (`lib/crow-diskdb-client/src/client.rs`, 565 lines) uses
tonic `Channel` pools + `tonic::Status` error mapping. The crow-rpc
client uses `RpcClient` + `Connection` (per-endpoint). The
`disk_group_id → endpoint` cache + retry logic stay; only the transport
layer changes.

### 2.2 New client structure

`DiskdbClient` keeps `svc: ServiceRegistryClient` + endpoint cache
(`DashMap<DiskGroupId, String>`) + `disk_to_dg` reverse map. Replaces
`channels: DashMap<String, Channel>` with `conns:
DashMap<String, (RpcServer, Connection)>` (one `RpcServer`+`Connection`
per endpoint — the `RpcServer` is the connection owner; the `RpcClient`
is shared). Adds `rpc: RpcClient` (shared, completion-pool-sized) +
`next_req_id: AtomicU64` (to be replaced by `RequestIdGen` from
`crow-common` per R114 work item 1 — deferred to R114).

Each RPC method (`allocate_blocks`, `free_blocks`, etc.):
a. Resolve endpoint for `disk_group_id` (cache → refresh on miss).
b. Get/create the `Connection` for that endpoint.
c. Build the request flatbuffer (`FlatBufferBuilder` → `finish` →
   `Buffer::from_bytes`).
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

## 3. Error model parity

`map_status` (tonic `Status` → `DiskdbClientError`) is replaced by:
- `From<RpcError> for DiskdbClientError` — transport errors.
  `ConnectionClosed`/`Timeout`/`SendQueueFull`/`ConnectionError` are
  retryable (`is_retryable()`); `RegistrationFailed`/`AllDown` are not.
- Response `ret_code` (FBDiskdbRetCode) → `DiskdbClientError` —
  protocol errors carried in the response body, not transport.
  `NoSpace`/`NotOwner`/`DiskNotFound`/`DiskGroupNotFound`/`Degraded` →
  `DiskdbClientError::Rpc(code_name)`; `InvalidArgument`/`Internal` →
  `DiskdbClientError::Rpc(msg)`.

## 4. Mixed rollout + cutover

Per `todo_fb.md` Suggestion #5 (Option A — separate ports): the diskdb
server runs the tonic gRPC server on the existing port and the crow-rpc
server on a new port (`DISKDB_RPC_BASE` in `ports.rs`, offset from
`DISKDB_GRPC_BASE`). The service registry stores both ports during the
rollout window; clients pick based on their transport config flag.

Cutover: after all clients are migrated, the tonic server startup is
removed in a follow-up commit. `diskdb_service.proto` stays in
`crow-protocol` as a legacy/reserved schema (same as
`diskio_service.proto` after R105).

## 5. `grpc_endpoint` → `rpc_endpoint` rename

Deferred to a standalone commit (todo_fb.md Suggestion #2). The rename
touches 48 files (broader than the 10 listed in the todo) including proto
wire fields (which must stay `grpc_endpoint` for backward compat), TS UI,
and e2e tests. It is not blocking R115 — the client can read the
`grpc_endpoint` sysdata field into a Rust field named `rpc_endpoint`
(field name != wire name via prost's `#[prost(rename)]`). Filed as an
open issue for review.

## Scope

- `app/crow-diskdb/src/service/diskdb_rpc_service.rs` — new: crow-rpc
  handler set (11 handlers), reuses existing diskdb logic.
- `app/crow-diskdb/src/service.rs` — add `pub mod diskdb_rpc_service`.
- `app/crow-diskdb/src/main.rs` — add crow-rpc `RpcServer` startup
  (listen on `DISKDB_RPC_BASE`, register handlers, start).
- `lib/crow-protocol/src/fb_wrappers/diskdb.rs` — new: zero-copy
  wrappers for diskdb request/response types.
- `lib/crow-protocol/src/fb_wrappers.rs` — new: `pub mod fb_wrappers`
  index (or inline in `lib.rs`).
- `lib/crow-diskdb-client/src/client.rs` — rewrite: tonic → crow-rpc
  transport.
- `lib/crow-diskdb-client/src/lib.rs` — error type adjustments.
- `lib/crow-protocol/src/ports.rs` — add `DISKDB_RPC_BASE`.

## Complexity

**High.** The schema + dispatch callback + `is_retryable` are done. The
remaining work is the server handler rewrite (899-line tonic service →
11 crow-rpc handlers, reusing the diskdb logic bodies), the client
rewrite (565-line tonic client → crow-rpc client), error mapping, mixed
rollout wiring, and integration tests for all 11 RPCs + error paths.
The diskdb logic itself is unchanged — the risk is in the transport
plumbing and the async-handler pattern (spawning tokio tasks from the
C++ I/O thread callback).

## Test Design

### Unit tests (UT)

- `fb_wrappers::diskdb` — parse a built `FBAllocateBlocksRequest` via
  the wrapper, verify field accessors return the built values
  (zero-copy read). Parse a response with `ret_code=NoSpace`, verify
  accessor.
- `DiskdbClientError` mapping — `From<RpcError>` covers all variants;
  `is_retryable()` classification matches the retry policy.

### End-to-end tests (E2E)

- `allocate_blocks_crow_rpc` — start a diskdb instance with the crow-rpc
  server, allocate blocks via the crow-rpc client, verify the returned
  segments match the in-memory allocator state.
- `free_and_commit_crow_rpc` — allocate, free, commit via crow-rpc,
  verify state changes match the gRPC path.
- `query_capacity_crow_rpc` — populate a disk-group, query via crow-rpc,
  verify the response matches the gRPC response shape.
- `compact_and_scan_crow_rpc` — trigger compaction + scan via crow-rpc,
  verify state + response.
- `error_no_space` — exhaust a disk-group's space, allocate via crow-rpc,
  verify `ret_code=NoSpace` → `DiskdbClientError::Rpc`.
- `error_not_owner` — contact the wrong diskdb instance, verify
  `ret_code=NotOwner` → client refreshes cache + retries.
- `transport_connection_closed` — kill the diskdb server mid-call,
  verify `ConnectionClosed` → retry on reconnect.
- `mixed_rollout` — run both gRPC + crow-rpc servers; a gRPC client
  hits the gRPC port, a crow-rpc client hits the crow-rpc port, both
  succeed.

## Module Structure

```
app/crow-diskdb/src/
  service/
    diskdb_rpc_service.rs   # new — 11 crow-rpc handlers
  service.rs                # add pub mod diskdb_rpc_service
  main.rs                   # add crow-rpc server startup
lib/crow-protocol/src/
  fb_wrappers/
    diskdb.rs               # new — zero-copy wrappers
  fb_wrappers.rs            # new — module index
  ports.rs                  # add DISKDB_RPC_BASE
lib/crow-diskdb-client/src/
  client.rs                 # rewrite — tonic → crow-rpc
  lib.rs                    # error type adjustments
```

## Config Extensions

- `DiskdbConfig` — add `rpc_port` field (default `DISKDB_RPC_BASE`).
  `validate()` checks the port is non-zero.

## Server Wiring

1. `main.rs` — after the tonic server starts, create `RpcServer::new`,
   `listen("0.0.0.0", rpc_port)`, `register_handlers` via
   `DiskdbRpcService`, `start()`.
2. The crow-rpc server runs on its own thread pool (C++ I/O workers);
   the tokio runtime continues to serve the tonic server + async KV ops.
3. Shutdown: stop the crow-rpc server before the tonic server.

## Open Questions

1. **`grpc_endpoint` → `rpc_endpoint` rename scope** — the todo lists
   ~10 files but the rename touches 48 files (proto wire fields, TS UI,
   e2e tests). The proto wire field name must stay `grpc_endpoint`
   (backward compat). Decision needed: do the rename as a standalone
   commit before R115 (per the todo), or defer and use
   `#[prost(rename)]` so the Rust field is `rpc_endpoint` while the
   wire name stays `grpc_endpoint`? See todo_fb.md Open Issues.

2. **Async handler pattern from the C++ I/O thread** — the dispatch
   callback runs on the C++ I/O worker thread. For handlers that call
   async KV ops (`DdbKvClient`), the handler must spawn a tokio task
   and submit the response from it. The `conn_handle` must remain valid
   until the task completes (the transport owns the connection; it stays
   alive until the client disconnects or the server stops). Need to
   verify the handle lifetime is safe across this boundary — the
   `submit_response` C API dereferences `conn_handle` as a
   `Connection*`. If the connection is closed between spawn and submit,
   `submit_response` must handle a stale handle gracefully (return an
   error, not crash). This needs verification in the C++ transport
   (`SocketTransport::submit` with a closed connection).
