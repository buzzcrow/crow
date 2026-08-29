<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: ChunkdbService RPC

Depends on: [`design-crowdb-rpc.md`](../rpc/design-crowdb-rpc.md) §4.4, §5, §6; [`design-crowdb-chunkdb.md`](design-crowdb-chunkdb.md) §4
Satisfies: [`design-crowdb-chunkdb.md`](design-crowdb-chunkdb.md) §4 (RPC layer)

The chunkdb service (Allocate/Append/Query/Seal/Delete/DeleteRange/
UpdateStrip/ListChunks) uses the **crowdb-rpc flatbuffer transport** — the
same engine as the KV consensus hot path, on a dedicated port with a
dedicated schema (`chunkdb.fbs`). The transport is selected
programmatically (`ChunkdbClient::with_rpc_transport`). Architecture
decisions and rationale live in the root RPC design; this doc covers the
chunkdb-specific schema, handler, and transport detail.

## Table of Contents

- [1. Port Allocation](#1-port-allocation)
- [2. Flatbuffer Schema (chunkdb.fbs)](#2-flatbuffer-schema-chunkdbfbs)
- [3. Zero-Copy Wrapper Classes](#3-zero-copy-wrapper-classes)
- [4. Server-Side Handler (ChunkdbRpcService)](#4-server-side-handler-chunkdbrpcservice)
- [5. Client-Side Transport (ChunkdbRpcTransport)](#5-client-side-transport-chunkdbrpctransport)
- [6. ChunkdbClient Transport Selection](#6-chunkdbclient-transport-selection)
- [7. Server Wiring](#7-server-wiring)
- [8. Error Model](#8-error-model)

---

## 1. Port Allocation

The chunkdb RPC port is `CHUNKDB_RPC_BASE = 9961` (stride 1, paired
with HTTP 9972).

```rust
pub const CHUNKDB_RPC_BASE: u16 = 9961;
```

Fills the gap between diskdb RPC (9931-9940) and chunkdb HTTP
(9972). Stride 1 (one port per instance). The `ChunkdbRpc` variant
in `ServicePort` provides `base() => CHUNKDB_RPC_BASE`, `stride() => 1`.

## 2. Flatbuffer Schema (chunkdb.fbs)

File: `lib/crowdb-protocol/src/fbs/chunkdb.fbs`

Includes `common_type.fbs` (provides `FBInt128`) and `diskdb.fbs`
(provides `FBSegment` as an inline struct in the `crowdb.diskdb`
namespace).

**Enums:**

- `FBChunkdbRetCode` — Success, InvalidArgument, NotFound, AlreadyExists,
  FailedPrecondition, Aborted, Internal, Unavailable, NotMyRange,
  StripIndexOutOfRange
- `FBEcState` — NoParity, Parity
- `FBChunkState` — Init, Active, Sealed, Deleted
- `FBStripType` — Mirror, Ec
- `FBChunkType` — Repo, Wal, BtreePage, PageIndex

**Nested types:**

- `FBMirrorStrip` — `segments:[FBSegment]`
- `FBEcStrip` — `data_num`, `code_num`, `ec_state`, `segments:[FBSegment]`
- `FBStripBody` union — None, Mirror, Ec (the strip body union)
- `FBChunkStrip` — `chunk_offset`, `strip_sequence`, `unit_kb`,
  `capacity`, `create_ts_ms`, `sealed_ts_ms`, `sealed_length`,
  `strip_type`, `strip_body:FBStripBody`, `usage_bitmap:[ubyte]`
- `FBChunk` — `id:FBInt128`, `state`, `create_ts_ms`, `sealed_ts_ms`,
  `capacity`, `sealed_length`, `strips:[FBChunkStrip]`, `chunk_type`

**Request/response tables:**

All 8 request types follow the same shape: `id` + `rpc_create_nano`
first, then the schema fields. All 8 response types carry `id` +
`rpc_create_nano` + `ret_code` + `error_msg` + `range_start` +
`range_end` (diagnostic for `NotMyRange`) + the response data.

The `range_start`/`range_end` fields are on every response (default 0) —
they are only populated when `ret_code = NotMyRange`. This avoids a
separate `NotMyRangeHint` message.

`UpdateChunkStripRequest` embeds a `FBChunkStrip` (the `strip` field).
`ListChunksResponse` carries `chunks:[FBChunk]` +
`next_token:FBInt128`.

**Message type IDs** (3300-3315 range):

- `EAllocateChunkRequest = 3300` … `EListChunksResponse = 3315`

**Build + re-exports:**

`build.rs` adds `chunkdb.fbs` to `fbs_files` + a `flatc --rust --gen-all`
invocation (inlines `common_type.fbs` + `diskdb.fbs` so `FBInt128` +
`FBSegment` resolve). `lib.rs` adds the `chunkdb_generated` private
module + `chunkdb_fb` public re-export module.

## 3. Zero-Copy Wrapper Classes

File: `lib/crowdb-protocol/src/fb_wrappers/chunkdb.rs`

One `Ref` wrapper per response type (8 total). Each follows the
zero-copy pattern: parse the root pointer once, read fields through it
without copying.

```rust
pub struct FBAllocateChunkResponseRef<'a> {
    root: Option<FBAllocateChunkResponse<'a>>,
}
```

Each `Ref` provides: `new`, `valid`, `ret_code`, `error_msg`,
`request_id`, `ok`, `range_start`, `range_end`, plus per-response data
accessor (e.g. `chunk()` for chunk-carrying responses, `chunks()` +
`next_token()` for `ListChunksResponse`).

Invalid buffer → `valid()` returns false, `ret_code()` returns
`Internal`, all data accessors return `None`/0.

## 4. Server-Side Handler (ChunkdbRpcService)

File: `app/crowdb-chunkdb/src/service/chunkdb_rpc_service.rs`

The `ChunkdbRpcService` struct holds `Arc<LifecycleHandler>` + a
`tokio::runtime::Handle`. The handler delegates to `LifecycleHandler`
for all 8 RPCs — same logic as the crowdb-rpc `ChunkdbService`, different
wire format. No transparent forwarding (chunkdb has no leader — range
routing is client-side via `RangeBindingClient`).

`register_handlers` wires one handler per request `msg_type` into the
`RpcServer`, using the `make_handler` closure pattern. Each handler:

a. Extract `request_id`, `rpc_create_nano`, `msg_type` from
   `ServerRequest`.
b. Parse the request flatbuffer via `flatbuffers::root::<FB<Type>Request>`.
   On parse failure, submit an error response with
   `ret_code = InvalidArgument`.
c. Extract fields from the request (chunk_id, strip_type, etc.).
d. Spawn a tokio task (via `Handle::spawn`) that calls the
   `LifecycleHandler` method.
e. On success, build the flatbuffer response with `ret_code = Success`
   + the chunk data.
f. On `LifecycleError::NotMyRange`, build the response with
   `ret_code = NotMyRange` + `range_start`/`range_end`.
g. On other errors, map `LifecycleError` to `FBChunkdbRetCode` +
   `error_msg`.
h. Submit via `server.submit_response`.

**Response builders:** one `build_<type>_response` function per
response type. The `Chunk` → `FBChunk` conversion builds nested
`FBChunkStrip` + `FBMirrorStrip`/`FBEcStrip` + `FBSegment` vectors —
the most complex part since the `Chunk` flatbuffer has nested `Vec<ChunkStrip>`
with a `oneof strip` field.

## 5. Client-Side Transport (ChunkdbRpcTransport)

File: `lib/crowdb-chunkdb-client/src/rpc_transport.rs`

```rust
pub struct ChunkdbRpcTransport {
    server: Arc<RpcServer>,
    rpc: Arc<RpcClient>,
    connections: DashMap<String, Connection>,
    next_req_id: AtomicU64,
}
```

The `RpcServer` is the client-side transport — it does not listen but
establishes connections to remote endpoints. `conn_for(endpoint)`
normalizes the endpoint, connects, and caches the `Connection`.

8 `send_*` methods (one per RPC): build request flatbuffer →
`rpc.call(&server, &conn, req_id, control, None, msg_type)` →
`fut.await` → parse response via `Ref` wrapper → map to the response
type → return.

The response types are still returned — the caller's retry +
`RangeBindingClient` logic is unchanged; only the wire send changes.

## 6. ChunkdbClient Transport Selection

File: `lib/crowdb-chunkdb-client/src/client.rs`

`ChunkdbClient` holds an `Option<Arc<ChunkdbRpcTransport>>` field, set
via `with_rpc_transport`. Each public method (`allocate_chunk`,
`append_chunk`, `query_chunk`, `seal_chunk`, `delete_chunk`,
`delete_chunk_range`, `update_chunk_strip`, `list_chunks`) checks
`self.rpc_transport` first:

a. When set, delegate to the transport's `send_*` method via
   `with_rpc_retry`: resolve endpoint via `range_binding.route(chunk_id)`
   or `first_endpoint()`, call `transport.send_*(endpoint, req)`, on
   `NotMyRange` refresh binding + re-route, on transient error retry
   with backoff.

The `with_rpc_retry` helper implements the retry loop — retry semantics
and `RangeBindingClient` integration, with the crowdb-rpc wire send.

## 7. Server Wiring

File: `app/crowdb-chunkdb/src/main.rs`

The chunkdb server runs inside `#[tokio::main]` so
`tokio::runtime::Handle::current()` is available. The crowdb-rpc server
is the chunkdb RPC transport.

Startup sequence:

1. Load config from TOML.
2. Build KV client for group-0 topology access.
3. Create topology cache + refresh loop + notify handler.
4. Create binding cache + chunk store.
5. Load range binding from group-0.
6. Spawn range binding notifier.
7. Register service-registry keep-alive.
8. Create diskdb client pool + chunk allocator.
9. Create per-chunk lock map + metrics.
10. Spawn sweep task for idle lock reaping.
11. Create lifecycle handler.
12. Create crowdb-rpc server: parse `rpc_listen_addr`, `RpcServer::new`,
      `listen`, `start`, `ChunkdbRpcService::new(handler, Handle)`,
      `register_handlers`.
13. Start HTTP health + metrics + cache invalidation server.
14. On shutdown: stop crowdb-rpc server via shutdown signal.

The `handler` is shared between the crowdb-rpc service — same
`Arc<LifecycleHandler>`.

The `rpc_listen_addr` config field (default `0.0.0.0:9961`) is
validated as a `SocketAddr` in `ChunkdbConfig::validate`.

## 8. Error Model

**`LifecycleError` → `FBChunkdbRetCode` mapping:**

- `InvalidStateTransition` → `FailedPrecondition`
- `ChunkNotFound` → `NotFound`
- `ChunkAlreadyExists` → `AlreadyExists`
- `StateConflict` → `Aborted`
- `Allocation` / `Storage` → `Internal`
- `InvalidRequest` → `InvalidArgument`
- `NotMyRange { bucket }` → `NotMyRange` + `range_start`/`range_end`
- `LockBusy` / `LockTimeout` → `Unavailable`
- `StripIndexOutOfRange` → `StripIndexOutOfRange`

**`FBChunkdbRetCode` → `ChunkdbClientError` mapping** (client side):

- `Success` → Ok
- `InvalidArgument` / `StripIndexOutOfRange` → `InvalidArgument`
- `NotFound` → `NotFound`
- `AlreadyExists` → `AlreadyExists`
- `FailedPrecondition` → `FailedPrecondition`
- `Aborted` → `Aborted`
- `Internal` → `Internal`
- `Unavailable` → `Unavailable` (transient — retryable)
- `NotMyRange` → `NotMyRange` (transient — refresh binding + re-route)

**`RpcError` → `ChunkdbClientError` mapping:**

- All `RpcError` variants → `ChunkdbClientError::Rpc` (not retryable
  by default; connection-level errors surface as `Unreachable` during
  endpoint resolution)

`NotMyRange` is a response, not an error — the client checks `ret_code`
and re-routes. The `is_transient` predicate covers `Unavailable`,
`DeadlineExceeded`, `Unreachable`, and `NotMyRange`.
