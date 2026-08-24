<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkdbService gRPC → crow-rpc Migration (R116)

Implementation design draft for R116. Backlog doc:
`doc/backlog/R116-chunkdb-rpc-migration.md`. Root design:
`doc/design/rpc/design-crow-rpc.md` §6 (Flatbuffer Wrapper Convention),
§5 (Schema + Build), §4.4 (Server Side). Closest reference: R117
(`doc/design/kv/design-crow-kv-rpc-client.md` — client-facing KV RPC,
same `with_rpc_transport` programmatic selection, same zero-copy `Ref`
wrapper pattern) and R115 (diskdb migration — same unary-only pattern +
port offset convention). R115 (diskdb), R32 (KV consensus), R117 (KV
client-facing) are landed; R116 is the terminal migration item.
Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. Port allocation

### 1.1 Why

R115 established the separate-port convention: gRPC server stays on
the old port, crow-rpc server listens on a new port, clients pick via
`with_rpc_transport()`. R116 follows the same pattern. The chunkdb
gRPC port is `CHUNKDB_GRPC_BASE = 9971` (stride 2, paired with HTTP
9972). The crow-rpc port needs a new base with stride 1 (one port per
instance, matching diskdb RPC).

### 1.2 How

Add to `lib/crow-protocol/src/ports.rs`:

```rust
pub const CHUNKDB_RPC_BASE: u16 = 9961;
```

Fills the gap between diskdb RPC (9931-9940) and chunkdb gRPC
(9971-9990). Stride 1. Add `ChunkdbRpc` variant to `ServicePort` enum
with `base() => CHUNKDB_RPC_BASE`, `stride() => 1`. Re-export
`CHUNKDB_RPC_BASE` from `lib.rs`.

The client derives the chunkdb RPC port from the gRPC port via:
`rpc_port = grpc_port + (CHUNKDB_RPC_BASE - CHUNKDB_GRPC_BASE)` =
`grpc_port - 10` (subtraction, matching R115's diskdb pattern).

Edge cases:
- Port 9961-9970 range is currently unused — no conflict.
- If `grpc_port - 10` falls below 1 (impossible — gRPC base is 9971),
  return `InvalidEndpoint`.

## 2. Flatbuffer schema (`chunkdb.fbs`)

### 2.1 Why

The `.proto` schemas (`chunkdb_service.proto`, `chunkdb_op.proto`,
`chunkdb_type.proto`) define 8 unary RPCs + nested types
(`ChunkStrip` with `oneof strip`, `Chunk`, `MirrorStrip`, `EcStrip`).
R116 converts these to `.fbs` following the R117/R115 convention:
every request/response carries `id` + `rpc_create_nano` first, every
response carries `ret_code` + `error_msg`, `FBInt128` for `ChunkId`,
`FBSegment` reused from `diskdb.fbs`.

### 2.2 Schema structure

File: `lib/crow-protocol/src/fbs/chunkdb.fbs`

```fbs
include "common_type.fbs";
include "diskdb.fbs";

namespace crow.chunkdb.proto;
```

Including `diskdb.fbs` makes `FBSegment` available (it's an inline
struct in the `crow.diskdb.proto` namespace). `common_type.fbs`
provides `FBInt128`.

**Enums:**

```fbs
enum FBChunkdbRetCode : int16 {
    Success = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    FailedPrecondition = 4,
    Aborted = 5,
    Internal = 6,
    Unavailable = 7,
    NotMyRange = 8,
    StripIndexOutOfRange = 9,
}

enum FBEcState : int16 { NoParity = 0, Parity = 1 }
enum FBChunkState : int16 { Init = 0, Active = 1, Sealed = 2, Deleted = 3 }
enum FBStripType : int16 { Mirror = 0, Ec = 1 }
enum FBChunkType : int16 { Repo = 0, Wal = 1, BtreePage = 2, PageIndex = 3 }
```

**Nested types:**

```fbs
table FBMirrorStrip {
    segments:[crow.diskdb.proto.FBSegment];
}

table FBEcStrip {
    data_num:uint32;
    code_num:uint32;
    ec_state:FBEcState;
    segments:[crow.diskdb.proto.FBSegment];
}

union FBStripBody {
    None = 0,
    Mirror = 1,
    Ec = 2,
}

table FBChunkStrip {
    chunk_offset:uint32;
    strip_sequence:uint32;
    unit_kb:uint32;
    capacity:uint32;
    create_ts_ms:uint64;
    sealed_ts_ms:uint64;
    sealed_length:uint32;
    strip_type:FBStripType;
    strip_body:FBStripBody;
    usage_bitmap:[ubyte];
}

table FBChunk {
    id:crow.rpc.proto.FBInt128;
    state:FBChunkState;
    create_ts_ms:uint64;
    sealed_ts_ms:uint64;
    capacity:uint32;
    sealed_length:uint32;
    strips:[FBChunkStrip];
    chunk_type:FBChunkType;
}
```

The proto `oneof strip { MirrorStrip mirror_strip; EcStrip ec_strip; }`
maps to a flatbuffers `union FBStripBody`. The `None = 0` variant is
the default (flatbuffers unions require a `NONE` case at 0).

**Request/response tables:**

All 8 request types follow the same shape: `id` + `rpc_create_nano`
first, then the proto fields. All 8 response types carry `id` +
`rpc_create_nano` + `ret_code` + `error_msg` + `range_start` +
`range_end` (diagnostic for `NotMyRange`) + the response data.

The `range_start`/`range_end` fields are on every response (default
0) — they are only populated when `ret_code = NotMyRange`. This
avoids a separate `NotMyRangeHint` message and matches the R117
pattern where `not_leader_hint` is a field on the response table.

```fbs
table FBAllocateChunkRequest {
    id:uint64;
    rpc_create_nano:uint64;
    chunk_id:crow.rpc.proto.FBInt128;
    write_granularity:uint32;
    strip_count:uint32;
    strip_type:FBStripType;
    data_num:uint32;
    code_num:uint32;
    copy_count:uint32;
    chunk_type:FBChunkType;
}

table FBAllocateChunkResponse {
    id:uint64;
    rpc_create_nano:uint64;
    ret_code:FBChunkdbRetCode;
    error_msg:string;
    range_start:uint32;
    range_end:uint32;
    chunk:FBChunk;
}

// AppendChunkRequest, QueryChunkRequest, SealChunkRequest,
// DeleteChunkRequest, DeleteChunkRangeRequest, UpdateChunkStripRequest,
// ListChunksRequest — same pattern.
```

`UpdateChunkStripRequest` embeds a `FBChunkStrip` (the `strip`
field). `ListChunksResponse` carries `chunks:[FBChunk]` +
`next_token:crow.rpc.proto.FBInt128`.

### 2.3 Message type IDs

Add to `lib/crow-protocol/src/fbs/msg_type.fbs` (3300s range,
reserved):

```fbs
EAllocateChunkRequest = 3300,
EAllocateChunkResponse = 3301,
EAppendChunkRequest = 3302,
EAppendChunkResponse = 3303,
EQueryChunkRequest = 3304,
EQueryChunkResponse = 3305,
ESealChunkRequest = 3306,
ESealChunkResponse = 3307,
EDeleteChunkRequest = 3308,
EDeleteChunkResponse = 3309,
EDeleteChunkRangeRequest = 3310,
EDeleteChunkRangeResponse = 3311,
EUpdateChunkStripRequest = 3312,
EUpdateChunkStripResponse = 3313,
EListChunksRequest = 3314,
EListChunksResponse = 3315,
```

### 2.4 Build + re-exports

`build.rs`: add `src/fbs/chunkdb.fbs` to `fbs_files` + a
`flatc --rust --gen-all` invocation (inlines `common_type.fbs` +
`diskdb.fbs` so `FBInt128` + `FBSegment` resolve). Output:
`chunkdb_generated.rs`.

`lib.rs`: add `chunkdb_generated` private module + `chunkdb_fb` public
re-export module:

```rust
pub mod chunkdb_fb {
    pub use crate::chunkdb_generated::crow::chunkdb::proto::*;
    pub use crate::chunkdb_generated::crow::diskdb::proto::FBSegment;
    pub use crate::chunkdb_generated::crow::rpc::proto::FBInt128;
}
```

Note: `--gen-all` inlines `diskdb.fbs` too, so `FBSegment` is
available under `crow::diskdb::proto` inside `chunkdb_generated`. It
is type-distinct from `diskdb_fb::FBSegment` but has the same layout
— use `chunkdb_fb::FBSegment` when constructing chunkdb request args.

Edge cases:
- `FBSegment` is an inline struct (fixed-layout) — flatbuffers embeds
  it directly in vectors, no vtable overhead.
- `usage_bitmap` as `[ubyte]` — flatbuffers rejects `[[ubyte]]`, so
  no wrapper needed (unlike R117's `FBBytes` for `WatchNotify`).
- `FBStripBody` union with `None = 0` — the default; a strip with no
  body is invalid but parseable.

## 3. Zero-copy wrappers (`fb_wrappers/chunkdb.rs`)

### 3.1 Why

R117 established the zero-copy `Ref` wrapper pattern
(`FBKvResponseRef` etc.). R115 deferred this (parses into owned proto
types — see Open Issues in `todo_fb.md`). R116 follows R117's pattern
from the start: the client transport parses responses via `Ref`
wrappers, reading fields through the root pointer without copying.

### 3.2 How

File: `lib/crow-protocol/src/fb_wrappers/chunkdb.rs`

One `Ref` per response type (8 total). Each follows the R117 pattern:

```rust
pub struct FBAllocateChunkResponseRef<'a> {
    root: Option<FBAllocateChunkResponse<'a>>,
}

impl<'a> FBAllocateChunkResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self { Self { root: parse_root::<FBAllocateChunkResponse>(buf) } }
    pub fn valid(&self) -> bool { self.root.is_some() }
    pub fn ret_code(&self) -> FBChunkdbRetCode { ... }
    pub fn error_msg(&self) -> Option<&'a str> { ... }
    pub fn request_id(&self) -> Option<u64> { ... }
    pub fn ok(&self) -> bool { ... }
    pub fn range_start(&self) -> u32 { ... }
    pub fn range_end(&self) -> u32 { ... }
    pub fn chunk(&self) -> Option<FBChunk<'a>> { ... }
}
```

The `chunk()` accessor returns the flatbuffer `FBChunk` reference
directly — the caller converts to proto `Chunk` only at the boundary
where it needs owned data. This matches R117's `value()` accessor
returning `Option<&[u8]>`.

Register in `fb_wrappers.rs`: `pub mod chunkdb;`

Edge cases:
- Invalid buffer → `valid()` returns false, `ret_code()` returns
  `Internal`, all data accessors return `None`/0.
- `NotMyRange` response → `ok()` returns false, `range_start()`/
  `range_end()` carry the diagnostic bucket.

## 4. Server-side handler (`chunkdb_rpc_service.rs`)

### 4.1 Why

The tonic `ChunkdbService` (`app/crow-chunkdb/src/service.rs`)
delegates to `LifecycleHandler` for all 8 RPCs. The crow-rpc handler
set mirrors this: same `LifecycleHandler` dependency, same logic
bodies, different wire format. No transparent forwarding (chunkdb has
no leader — range routing is client-side via `RangeBindingClient`).

### 4.2 How

Convert `service.rs` from a single file to a directory:
`app/crow-chunkdb/src/service/mod.rs` (re-exports) +
`app/crow-chunkdb/src/service/chunkdb_service.rs` (existing tonic
service) + `app/crow-chunkdb/src/service/chunkdb_rpc_service.rs`
(new crow-rpc handler set).

`ChunkdbRpcService` struct:

```rust
pub struct ChunkdbRpcService {
    handler: Arc<LifecycleHandler>,
    rt: Handle,
}
```

`register_handlers` wires one handler per request `msg_type` into the
`RpcServer`, using the same `make_handler` closure pattern as R117's
`KvRpcService`. Each handler:

a. Extract `request_id`, `rpc_create_nano`, `msg_type` from
   `ServerRequest`.
b. Parse the request flatbuffer via `flatbuffers::root::<FB<Type>Request>`.
   On parse failure, submit an error response with
   `ret_code = InvalidArgument` and return.
c. Extract fields from the request (chunk_id, strip_type, etc.).
d. Spawn a tokio task (via `self.rt.spawn`) that calls the
   `LifecycleHandler` method (same logic as the tonic handler).
e. On success, build the flatbuffer response with `ret_code = Success`
   + the chunk data.
f. On `LifecycleError::NotMyRange { bucket }`, build the response with
   `ret_code = NotMyRange` + `range_start`/`range_end` = bucket.
g. On other errors, map `LifecycleError` to `FBChunkdbRetCode` + 
   `error_msg`.
h. Submit via `server.submit_response(conn_handle, &ctrl, None,
   msg_type, req_id)`.

**Error mapping** (`LifecycleError` → `FBChunkdbRetCode`):

- `InvalidStateTransition` → `FailedPrecondition`
- `ChunkNotFound` → `NotFound`
- `ChunkAlreadyExists` → `AlreadyExists`
- `StateConflict` → `Aborted`
- `Allocation` / `Storage` → `Internal`
- `InvalidRequest` → `InvalidArgument`
- `NotMyRange { bucket }` → `NotMyRange` + `range_start`/`range_end`
- `LockBusy` / `LockTimeout` → `Unavailable`
- `StripIndexOutOfRange` → `StripIndexOutOfRange`

**Response builders:** one `build_<type>_response` function per
response type, using `FlatBufferBuilder` + `FB<Type>ResponseArgs`.
The `Chunk` → `FBChunk` conversion builds nested `FBChunkStrip` +
`FBMirrorStrip`/`FBEcStrip` + `FBSegment` vectors. This is the most
complex part — the proto `Chunk` has nested `Vec<ChunkStrip>` with a
`oneof strip` field.

**Request parsers:** extract fields from the flatbuffer request,
convert `FBStripType`/`FBChunkType` to proto enums, convert
`FBInt128` to `ChunkId`, convert `FBChunkStrip` to proto `ChunkStrip`
(for `UpdateChunkStripRequest`).

Edge cases:
- Handler runs on the C++ I/O thread — sync validation inline, async
  work spawned via `Handle`.
- `submit_response` takes a raw `conn_handle` — `unsafe` confined to
  the submit call (same as R115/R117).
- `NotMyRange` is a response, not an error — the client checks
  `ret_code` and re-routes.

## 5. Client transport (`rpc_transport.rs`)

### 5.1 Why

R115's `DiskdbRpcTransport` and R117's `KvRpcTransport` established
the client-side crow-rpc transport pattern: `RpcServer` (manages
connections, does not listen) + `RpcClient` (request/response
correlation) + `DashMap<String, Connection>` (connection cache) +
`AtomicU64` (request ID). R116 follows the same pattern.

### 5.2 How

File: `lib/crow-chunkdb-client/src/rpc_transport.rs`

```rust
pub struct ChunkdbRpcTransport {
    server: Arc<RpcServer>,
    rpc: RpcClient,
    connections: DashMap<String, Connection>,
    next_req_id: AtomicU64,
}
```

`conn_for(endpoint)`: normalize endpoint, derive RPC port via
`grpc_port - 10`, `server.connect(host, rpc_port)`, `rpc.attach(&conn)`,
cache in `connections`.

8 `send_*` methods (one per RPC): build request flatbuffer →
`rpc.call(&server, &conn, req_id, control, None, msg_type)` →
`fut.await` → parse response via `Ref` wrapper → map to proto
response type → return.

The `Ref` wrapper is used for zero-copy response parsing (R117
pattern). The proto response types are still returned (the caller's
retry + `RangeBindingClient` logic is unchanged — only the wire send
changes).

**NotMyRange handling:** when `ret_code = NotMyRange`, return
`ChunkdbClientError::NotMyRange` with the diagnostic bucket. The
client's `with_retry` logic already handles `NotMyRange` by
refreshing the binding cache + re-routing.

**Error mapping** (`RpcError` → `ChunkdbClientError`):
- `ConnectionClosed` / `ConnectionError` → `Unreachable` (retryable)
- `Timeout` → `DeadlineExceeded` (retryable)
- `SendQueueFull` → `Unavailable` (retryable)
- `RegistrationFailed` / `AllDown` / `InvalidArg` → `Rpc` (not
  retryable)

Add `crow-rpc-ffi` + `flatbuffers` to `crow-chunkdb-client`
Cargo.toml.

Edge cases:
- Connection to a removed chunkdb instance → `RpcError::ConnectionClosed`
  → `Unreachable` → retry on next endpoint.
- `ChunkSealed` → `ret_code = FailedPrecondition` → client allocates a
  new chunk (caller logic, not transport).

## 6. ChunkdbClient transport selection

### 6.1 Why

R115's `DiskdbClient::with_rpc_transport` and R117's
`CrowkvClient::with_rpc_transport` established the programmatic
transport selection pattern. R116 follows: add an
`Option<Arc<ChunkdbRpcTransport>>` field, each public method checks
it first.

### 6.2 How

File: `lib/crow-chunkdb-client/src/client.rs`

Add field + builder:

```rust
pub struct ChunkdbClient {
    // ... existing fields ...
    rpc_transport: Option<Arc<ChunkdbRpcTransport>>,
}

pub fn with_rpc_transport(mut self, transport: Arc<ChunkdbRpcTransport>) -> Self {
    self.rpc_transport = Some(transport);
    self
}
```

Each public method (`allocate_chunk`, `append_chunk`, `query_chunk`,
`seal_chunk`, `delete_chunk`, `delete_chunk_range`,
`update_chunk_strip`, `list_chunks`) checks `self.rpc_transport`
first:

a. When set, delegate to the transport's `send_*` method via a new
   `with_rpc_retry` helper (mirrors R115's pattern): resolve endpoint
   via `range_binding.route(chunk_id)` or `first_endpoint()`, call
   `transport.send_*(endpoint, req)`, on `NotMyRange` refresh binding
   + re-route, on transient error retry with backoff.
b. When not set, the existing tonic `with_retry` path.

The `with_rpc_retry` helper is structurally identical to R115's
`DiskdbClient::with_rpc_retry` — same retry loop, same
`RangeBindingClient` integration, only the send function differs.

Edge cases:
- `range_binding` is `None` (v1 non-sharded mode) → route to
  `first_endpoint()`.
- `range_binding.route()` fails → fall back to `first_endpoint()`.

## 7. Server wiring (`main.rs`)

### 7.1 Why

The chunkdb server runs inside `#[tokio::main]` so
`tokio::runtime::Handle::current()` is available (same as R117's
`kv_server.rs`). The crow-rpc server starts alongside the tonic
server during mixed rollout.

### 7.2 How

File: `app/crow-chunkdb/src/main.rs`

Add `start_chunkdb_rpc_server` function (mirrors R117's
`start_client_rpc_server`):

a. Derive the crow-rpc port from the gRPC listen address:
   `rpc_port = grpc_port + (CHUNKDB_RPC_BASE - CHUNKDB_GRPC_BASE)`.
b. Create `RpcServer::new(None)`, `server.listen(ip, rpc_port)`,
   `server.start()`.
c. Create `ChunkdbRpcService::new(handler, Handle::current())`,
   `service.register_handlers(&server)`.
d. Store the server handle for shutdown.

The `handler` (already constructed for the tonic service) is shared
between the tonic and crow-rpc paths — same `Arc<LifecycleHandler>`.

Add `crow-rpc-ffi` + `flatbuffers` to `crow-chunkdb` Cargo.toml.

Shutdown: stop the crow-rpc server before the tonic server stops
(same order as R117).

Edge cases:
- Port derivation fails (out of range) → log error, continue without
  crow-rpc (graceful degradation — the tonic server still works).
- `server.listen` fails → log error, continue without crow-rpc.

## Scope

**`lib/crow-protocol`**:
- `src/fbs/chunkdb.fbs` — new flatbuffer schema (8 RPCs + nested types)
- `src/fbs/msg_type.fbs` — add 3300-3315 message type IDs
- `build.rs` — add `chunkdb.fbs` to `fbs_files` + `flatc --gen-all`
- `src/lib.rs` — add `chunkdb_generated` module + `chunkdb_fb` re-export
- `src/ports.rs` — add `CHUNKDB_RPC_BASE = 9961` + `ChunkdbRpc` variant
- `src/fb_wrappers.rs` — add `pub mod chunkdb;`
- `src/fb_wrappers/chunkdb.rs` — new zero-copy `Ref` wrappers (8 response types)

**`lib/crow-chunkdb-client`**:
- `Cargo.toml` — add `crow-rpc-ffi` + `flatbuffers` deps
- `src/lib.rs` — add `pub mod rpc_transport;` + re-export `ChunkdbRpcTransport`
- `src/rpc_transport.rs` — new crow-rpc client transport (8 `send_*` methods)
- `src/client.rs` — add `rpc_transport` field + `with_rpc_transport` + per-method selection

**`app/crow-chunkdb`**:
- `Cargo.toml` — add `crow-rpc-ffi` + `flatbuffers` deps
- `src/service.rs` → `src/service/mod.rs` + `src/service/chunkdb_service.rs` (move existing tonic service)
- `src/service/chunkdb_rpc_service.rs` — new crow-rpc handler set (8 handlers)
- `src/main.rs` — add `start_chunkdb_rpc_server` + shutdown integration

**`lib/crow-protocol/tests`**:
- `chunkdb_wrappers_test.rs` — new unit tests for `Ref` wrappers

**`lib/crow-protocol/tests/ports_test.rs`**:
- Add `CHUNKDB_RPC_BASE` + `ChunkdbRpc` port allocation test

## Complexity

**Medium.** The schema conversion is mechanical (8 RPCs, well-defined
proto types). The server handler set mirrors R117's `KvRpcService`
pattern — same `make_handler` closure, same `spawn + submit_response`
flow. The main challenge is the `Chunk` → `FBChunk` response builder:
the proto `Chunk` has nested `Vec<ChunkStrip>` with a `oneof strip`
field (mirror vs EC), each strip containing `Vec<Segment>`. Building
this in flatbuffers requires nested `FlatBufferBuilder` vector
construction (build segments → build strip body → build strip → build
chunk). The client transport is a direct copy of R115's
`DiskdbRpcTransport` with chunkdb types substituted. No new
architectural patterns — everything reuses R115/R117 established
patterns.

## Test Design

### Unit tests (UT)

**Port allocation** (`lib/crow-protocol/tests/ports_test.rs`):
- `CHUNKDB_RPC_BASE = 9961` → `ServicePort::ChunkdbRpc.port(0) = 9961`,
  `port(1) = 9962`, stride 1. Verify no overlap with
  `DiskdbGrpc` (9941) or `ChunkdbGrpc` (9971).

**Flatbuffer wrappers** (`lib/crow-protocol/tests/chunkdb_wrappers_test.rs`):
- Build a `FBAllocateChunkResponse` with `ret_code = Success` + a
  chunk → parse via `FBAllocateChunkResponseRef::new` → verify
  `ok()`, `ret_code()`, `chunk()` accessors return correct values.
- Build a response with `ret_code = NotMyRange` + `range_start = 42` →
  parse → verify `ok()` = false, `range_start()` = 42.
- Build a response with `ret_code = Internal` + `error_msg` → parse →
  verify `error_msg()` returns the string.
- Parse a malformed buffer (too short / wrong type) → verify
  `valid()` = false, `ret_code()` = `Internal`.
- Build a `FBChunkStrip` with `strip_body = Mirror` + segments →
  parse → verify the union accessor returns the mirror variant.
- Build a `FBChunkStrip` with `strip_body = Ec` + data_num/code_num →
  parse → verify the ec variant fields.

### End-to-end tests (E2E)

**Transport parity** (in `crow-chunkdb` integration tests):
- Start a chunkdb instance with both gRPC + crow-rpc servers →
  allocate a chunk via gRPC client → allocate a chunk via crow-rpc
  client → verify both produce valid chunks with identical structure.
- `AppendChunk` over crow-rpc → verify the strip is appended to the
  chunk (query via gRPC, compare).
- `QueryChunk` / `SealChunk` / `DeleteChunk` / `DeleteChunkRange` /
  `ListChunks` over crow-rpc → verify results match gRPC path.
- `UpdateChunkStrip` over crow-rpc → verify the strip is updated.
- `NotMyRange` response over crow-rpc → verify client receives
  `ChunkdbClientError::NotMyRange` + diagnostic bucket.

**Error model**:
- Kill chunkdb mid-call → crow-rpc `ConnectionClosed` → client
  retries on next endpoint.
- `AppendChunk` on a sealed chunk → `ret_code = FailedPrecondition`
  → client receives `ChunkdbClientError::FailedPrecondition`.

**Mixed rollout**:
- A chunkdb instance running both gRPC + crow-rpc: gRPC client
  connects to gRPC port, crow-rpc client to crow-rpc port, both
  succeed.

## Module Structure

```
lib/crow-protocol/
├── src/fbs/
│   └── chunkdb.fbs                    # NEW — 8 RPCs + nested types
├── src/fbs/msg_type.fbs               # MOD — add 3300-3315
├── build.rs                           # MOD — add chunkdb.fbs codegen
├── src/lib.rs                         # MOD — chunkdb_generated + chunkdb_fb
├── src/ports.rs                       # MOD — CHUNKDB_RPC_BASE + ChunkdbRpc
├── src/fb_wrappers.rs                 # MOD — add pub mod chunkdb
├── src/fb_wrappers/
│   └── chunkdb.rs                     # NEW — 8 Ref wrappers
└── tests/
    ├── chunkdb_wrappers_test.rs       # NEW — wrapper unit tests
    └── ports_test.rs                  # MOD — add ChunkdbRpc test

lib/crow-chunkdb-client/
├── Cargo.toml                         # MOD — add crow-rpc-ffi + flatbuffers
├── src/lib.rs                         # MOD — add rpc_transport module
├── src/rpc_transport.rs               # NEW — ChunkdbRpcTransport (8 send_* methods)
└── src/client.rs                      # MOD — with_rpc_transport + per-method selection

app/crow-chunkdb/
├── Cargo.toml                         # MOD — add crow-rpc-ffi + flatbuffers
├── src/lib.rs                         # MOD — service mod (if dir conversion)
├── src/service.rs → src/service/      # CONVERT — file to directory
│   ├── mod.rs                         # NEW — re-exports
│   ├── chunkdb_service.rs             # MOVED — existing tonic service
│   └── chunkdb_rpc_service.rs         # NEW — crow-rpc handler set
└── src/main.rs                        # MOD — start_chunkdb_rpc_server + shutdown
```

## Config Extensions

None. The crow-rpc port is derived from the gRPC port via a fixed
offset — no new config field. The client enables crow-rpc via
`with_rpc_transport()` (programmatic, same as R115/R117).

## Server Wiring

`app/crow-chunkdb/src/main.rs` startup sequence (additions marked
with `+`):

1. Load config from TOML.
2. Build KV client for group-0 topology access.
3. Create topology cache + refresh loop + notify handler.
4. Create binding cache + chunk store.
5. Load range binding from group-0 (R99).
6. Spawn range binding notifier.
7. Register service-registry keep-alive.
8. Create diskdb client pool + chunk allocator.
9. Create per-chunk lock map + metrics.
10. Spawn sweep task for idle lock reaping.
11. Create lifecycle handler + gRPC service.
12. Start HTTP health + metrics + cache invalidation server.
+ 13. Start crow-rpc server (`start_chunkdb_rpc_server`): derive port,
       listen, register handlers, store handle.
14. Start tonic gRPC server (blocking `serve_with_shutdown`).
15. On shutdown: stop crow-rpc server, then tonic server stops via
    shutdown signal, await background tasks.

## Open Questions

None. All patterns are established by R115/R117. The `Chunk` →
`FBChunk` response builder is the most complex piece but has a clear
template (R115's nested `DiskGroupInfo` builder with `Vec<ZoneUsage>`
+ `Vec<DiskInfo>`).
