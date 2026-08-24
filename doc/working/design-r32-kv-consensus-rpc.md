<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Consensus Hot Path → crow-rpc Migration (R32)

Implementation design draft for migrating the internal KV consensus
RPC path from tonic/gRPC to the R104 `crow-rpc` flatbuffer RPC library.

Backlog doc: `doc/backlog/R32-kv-custom-rust-rpc.md` (problem,
scenarios, acceptance criteria, resolved questions).
Root design: `doc/design/rpc/design-crow-rpc.md` §6 (Flatbuffer
Wrapper Convention), `doc/design/rpc/design-crow-rpc-diskdb-migration.md`
(R115's proven pattern). Architecture decisions and rationale are in
the root design; this doc does not repeat them.

Already landed: R104 (crow-rpc engine), R114 (bidi request-response),
R115 (diskdb migration — proof-of-pattern). R32 follows R115's layout
for the KV consensus path.

## 1. Flatbuffer Schema (`kv_consensus.fbs`)

### 1.1 Why

`pxos.proto` (370 lines) defines the consensus wire contract: Prepare/
Promise, Accept/Accepted, PreVote/RequestVote, Heartbeat/StepDown,
ChosenNotification/BatchChosenNotification, FetchGap, LearnerStream
oneof wrappers, SnapshotRequest/SnapshotStreamItem/SnapshotHeader.
R115 proved the full `.fbs` conversion approach (no prost bridge);
R32 follows the same path.

### 1.2 Schema structure

`lib/crow-protocol/src/fbs/kv_consensus.fbs` — mirrors `pxos.proto`
field-for-field, following R115's `diskdb.fbs` conventions:

- `include "common_type.fbs";` for `FBInt128` (not used by consensus
  messages, but included for consistency with R115).
- `namespace crow.kv_consensus.proto;`
- Every request/response table carries `id` (request_id) +
  `rpc_create_nano` as its first two fields, matching `diskdb.fbs`.
- `FBAcceptedValue` is a table (not a struct — it has a `payload: [ubyte]`
  vector, which requires a vtable).
- `FBDedupTag` is an inline struct (fixed-layout, two `uint64` fields).
- `FBLearnerStreamRequest` / `FBLearnerStreamResponse` use a `union` for
  the frame oneof (matching proto's `oneof frame`).

Message type IDs registered in `msg_type.fbs` (1000–1099 range):

```
// kv consensus service (range 1000s, see kv_consensus.fbs — R32)
EPrepareRequest = 1000,
EPromiseResponse = 1001,
EAcceptRequest = 1002,
EAcceptedResponse = 1003,
EPreVoteRequest = 1004,
EPreVoteResponse = 1005,
ERequestVoteRequest = 1006,
ERequestVoteResponse = 1007,
EHeartbeatRequest = 1008,
EHeartbeatResponse = 1009,
EStepDownRequest = 1010,
EStepDownResponse = 1011,
EChosenNotification = 1012,       // fire-and-forget (no response)
EBatchChosenNotification = 1013,  // fire-and-forget (no response)
EFetchGapRequest = 1014,
EFetchGapResponse = 1015,
ESnapshotRequest = 1016,
ESnapshotResponse = 1017,
```

No separate LearnerStream request/response msg_types — each frame type
within the stream has its own msg_type. The persistent connection carries
a mix of these msg_types; the server dispatches each frame independently
by its msg_type. This is simpler than a union wrapper and avoids the
flatbuffer union codegen complexity.

### 1.3 Field mapping notes

- `NotLeaderHint` is NOT a separate message — it is fields on the
  response tables (`not_leader_hint:string` + `term:uint64` +
  `membership_epoch:uint64`). The current proto carries leader hints
  via `term_stale`/`epoch_mismatch` booleans + the term/epoch fields;
  the flatbuffer schema preserves these fields directly. R117's
  client-facing `NotLeaderHint` (leader endpoint string) is a separate
  model on the client-facing response tables.
- `AcceptedValue.payload` (proto `bytes`) → `[ubyte]` vector in
  flatbuffer. The data payload (log entry bytes) can also be carried as
  the frame's data buffer (zero-copy) for large payloads; the control
  buffer's `payload` field is used for small payloads. Decision: use
  the control buffer's `[ubyte]` vector for now (matches proto
  semantics); the data-buffer path is a future optimization for large
  log entries.
- `StepDownRequest.reason` (proto `string`) → `string` in flatbuffer.

### 1.4 Build integration

`lib/crow-protocol/build.rs`:
a. Add `"src/fbs/kv_consensus.fbs"` to the `fbs_files` array (for
   `rerun-if-changed`).
b. Add a new `flatc --rust --gen-all` invocation for
   `kv_consensus.fbs` (inlines `common_type.fbs` so `FBInt128`
   resolves, matching `diskio.fbs` / `diskdb.fbs`).

`lib/crow-protocol/src/lib.rs`:
a. Add `mod kv_consensus_generated { ... }` (same `allow` block as
   `diskdb_generated`).
b. Add `pub mod kv_consensus_fb { pub use crate::kv_consensus_generated::crow::kv_consensus::proto::*; pub use crate::kv_consensus_generated::crow::rpc::proto::FBInt128; }`.

Edge cases:
- `FBAcceptedValue` absent in `FBAcceptRequest` → server handler
  returns an error response (matches current `Status::invalid_argument`
  path in `handle_accept_inner`).
- Empty `dedup_tags` vector → client falls back to legacy
  `client_id`/`seq` fields (matches current logic).

## 2. Zero-Copy Wrapper Classes

### 2.1 Why

`design-crow-rpc.md` §6 specifies zero-copy `FB<Type>Ref` wrappers: hold
a `&[u8]` reference to the control buffer, parse the root on
construction, expose typed accessors that read through the root pointer.
R115 deferred this (parses into owned proto types per call); R32
implements it properly because the Paxos hot path is the perf-critical
path where per-response allocation would partially offset the h2-lock
recovery.

### 2.2 Wrapper definitions

`lib/crow-protocol/src/fb_wrappers/mod.rs` — new module (the
`fb_wrappers/` directory does not exist yet; R115 used re-exports
instead).

`lib/crow-protocol/src/fb_wrappers/kv_consensus.rs` — one `Ref` struct
per response type:

```rust
pub struct FBPromiseResponseRef<'a> {
    root: flatbuffers::Result<'a, FBPromiseResponse<'a>>,
}
impl<'a> FBPromiseResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self { Self { root: flatbuffers::root::<FBPromiseResponse>(buf) } }
    pub fn valid(&self) -> bool { self.root.is_ok() }
    pub fn request_id(&self) -> Option<u64> { self.root.as_ref().ok()?.id() }
    pub fn slot(&self) -> Option<u64> { self.root.as_ref().ok()?.slot() }
    pub fn round(&self) -> Option<u64> { self.root.as_ref().ok()?.round() }
    pub fn rejected(&self) -> bool { self.root.as_ref().ok().is_some_and(|r| r.rejected()) }
    pub fn term(&self) -> Option<u64> { self.root.as_ref().ok()?.term() }
    pub fn term_stale(&self) -> bool { self.root.as_ref().ok().is_some_and(|r| r.term_stale()) }
    pub fn membership_epoch(&self) -> Option<u64> { self.root.as_ref().ok()?.membership_epoch() }
    pub fn epoch_mismatch(&self) -> bool { self.root.as_ref().ok().is_some_and(|r| r.epoch_mismatch()) }
    // ... remaining fields
}
```

Same pattern for: `FBAcceptedResponseRef`, `FBHeartbeatResponseRef`,
`FBPreVoteResponseRef`, `FBRequestVoteResponseRef`, `FBStepDownResponseRef`,
`FBFetchGapResponseRef`, `FBSnapshotResponseRef`.

Request wrappers are NOT needed — the server handler reads the request
flatbuffer directly via `flatbuffers::root::<FBPrepareRequest>(req.control)`
(matching R115's `validate_allocate` pattern). The client builds
requests with `FlatBufferBuilder` and doesn't need to read them back.

Edge cases:
- Malformed flatbuffer → `valid()` returns false; the caller treats it
  as a transport error (maps to `PxReplicaError::Internal`).
- Missing optional field → accessor returns `None` / default; the caller
  handles the missing-field case (matches current proto semantics where
  proto3 defaults apply).

## 3. Server-Side Handler (`px_rpc_service.rs`)

### 3.1 Why

Replace the tonic `PxReplicaService` + `PxSnapshotService` with a
crow-rpc handler set. Follows R115's `diskdb_rpc_service.rs` pattern:
one struct holding the same dependencies (`Arc<PxKvStore>` + tokio
`Handle`), `register_handlers` wires one handler per msg_type into the
`RpcServer`.

### 3.2 Handler structure

`lib/crow-kv/src/rpc/px_rpc_service.rs` — new file. Lives inside
`crow-kv` (not `crow-kv-server`) because the handlers need
`pub(crate)` methods on `PxGroup` / `PxLearner` / `PxLocalReplica`
(`handle_fetch_gap`, `update_chosen_frontier`, `record_dedup_tags`,
`note_chosen`, `record_gap`, `wake_apply_loop`,
`incr_chosen_notice_*`). R115's `diskdb_rpc_service.rs` could live in
the binary because it only used the public `DdbDiskGroupContainer`
API; R32's handler needs crate-internal access, so it sits alongside
the existing tonic `px_service.rs`.

```rust
pub struct PxRpcService {
    store: Arc<PxKvStore>,
    rt: Handle,
}
```

`register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>)` —
registers 15 handlers (one per request msg_type). Uses the same
`make_handler` closure pattern as R115.

### 3.3 Handler implementations

Each handler mirrors the corresponding tonic method in `px_service.rs`:

**Prepare** (`EPrepareRequest`):
a. Parse `flatbuffers::root::<FBPrepareRequest>(req.control)`.
b. Look up group + replica via `store.get_group(group_id)`.
c. Membership-epoch fence check (same as `px_service.rs` L131-147).
d. Call `ReplicaHandler::on_prepare(replica, slot, ballot, term, group_id)`.
e. Build `FBPromiseResponse` via `FlatBufferBuilder`, `submit_response`.

**Accept** (`EAcceptRequest`):
a. Parse `FBAcceptRequest`, extract `FBAcceptedValue` + dedup_tags.
b. Membership-epoch fence check.
c. Call `ReplicaHandler::on_accept(replica, &entry, group_id)`.
d. Record dedup tags on accepted.
e. Build `FBAcceptedResponse`, `submit_response`.

**PreVote / RequestVote / Heartbeat / StepDown** — same pattern: parse
→ look up group → call `ReplicaHandler::on_*` → build response →
`submit_response`.

**ChosenNotification** (`EChosenNotification`):
a. Parse `FBChosenNotification`.
b. Ballot-verified apply (same as `handle_chosen_notice` in
   `px_service.rs` L466-528).
c. NO `submit_response` — fire-and-forget.

**BatchChosenNotification** (`EBatchChosenNotification`):
a. Parse `FBBatchChosenNotification`.
b. Loop over slot range, ballot-verified apply (same as
   `handle_batch_chosen` L532-573).
c. NO `submit_response` — fire-and-forget.

**FetchGap** (`EFetchGapRequest`):
a. Parse `FBFetchGapRequest`.
b. Call `group.handle_fetch_gap(slot)`.
c. If value found: build `FBFetchGapResponse`, `submit_response`.
d. If no value: NO `submit_response` (follower retries on timeout).

**Snapshot** (`ESnapshotRequest`):
**Deferred from R32.** The C++ `RpcServer` has no `set_max_data_size`
API — the 4 MiB default is per-`Connection`. Adding it requires C++
changes (follow-up task). Since `join_via_snapshot` is not the hot
path (runs once per new replica join), it stays on gRPC during R32.
The `ESnapshotRequest`/`ESnapshotResponse` msg_types + schema tables
are defined now so the follow-up only needs the handler + client
rewrite.

### 3.4 Epoch-mismatch fence responses

The membership-epoch fence (early-return in `prepare`/`accept`) builds
a response with `epoch_mismatch: true` + the responder's epoch. This is
preserved exactly — the flatbuffer response sets the same fields.

### 3.5 Error handling

`PxReplicaError` → no `Status` (tonic is gone). Errors are carried in
the response body:
- `GroupNotFound` → response with `ret_code` field (added to each
  response table as `ret_code:FBKvRetCode`, default `Success`). Set to
  `NotFound` for group-not-found, `Unavailable` for shutting-down,
  `Internal` for internal errors.
- This is a new field not in the proto — the proto used tonic `Status`
  codes. The flatbuffer response carries the error code inline.

`FBKvRetCode` enum (new, in `kv_consensus.fbs`):
```
enum FBKvRetCode : int16 {
    Success = 0,
    NotFound = 1,
    Unavailable = 2,
    Internal = 3,
    InvalidArgument = 4,
}
```

Every response table gets `ret_code:FBKvRetCode` + `error_msg:string`
fields (matching R115's `FBDiskdbRetCode` convention).

Edge cases:
- Group not found → `ret_code = NotFound`, `submit_response` with the
  error response. Client maps to `PxReplicaError::GroupNotFound`.
- Store shutting down → `ret_code = Unavailable`.
- Malformed request flatbuffer → `ret_code = InvalidArgument`.
- `submit_response` fails (connection dropped) → log warning, no retry
  (the client will time out and reconnect).

## 4. Client-Side Transport (`px_rpc_transport.rs`)

### 4.1 Why

Replace the tonic `PxServiceClient<Channel>` in `PxRemoteReplica` with
an R104 `RpcClient` + connection pool. Follows R115's
`DiskdbRpcTransport` structure.

### 4.2 Transport structure

`lib/crow-kv/src/rpc/px_rpc_transport.rs` — new file.

```rust
pub struct PxRpcTransport {
    server: Arc<RpcServer>,
    rpc: RpcClient,
    connections: DashMap<String, Connection>,
    next_req_id: AtomicU64,
}
```

`conn_for(endpoint)` — resolves the crow-rpc port from the gRPC
endpoint (port offset: `KV_SERVER_GRPC_BASE - KV_RPC_BASE`), connects
via `server.connect()`, attaches to `rpc`. Cached in `DashMap`.

### 4.3 RPC methods

Each unary RPC method mirrors the corresponding `PxRemoteReplica` method:

**send_prepare**:
a. Build `FBPrepareRequest` via `FlatBufferBuilder`.
b. `rpc.call(&server, &conn, req_id, control, None, EPrepareRequest)`.
c. Await `CallFuture`, parse response via `FBPromiseResponseRef`.
d. Map to `PxPrepareReply` (Promised/TermStale/Rejected/EpochMismatch).
e. On `RpcError`: if `is_retryable()`, retry; else `PxReplicaError`.

Same pattern for: `send_pre_vote`, `send_request_vote`, `send_heartbeat`,
`send_step_down`.

**send_accept** (via LearnerStream — see §5):
Routed through the persistent LearnerStream connection, not the unary
transport.

### 4.4 Error mapping

`From<RpcError> for PxReplicaError`:
- `ConnectionClosed` / `ConnectionError` → `PxReplicaError::Internal`
  (retryable — caller retries on a fresh connection).
- `Timeout` → `PxReplicaError::Internal` (caller may retry or fail).
- `SendQueueFull` → `PxReplicaError::Internal` (backpressure).
- `RegistrationFailed` / `AllDown` / `InvalidArg` →
  `PxReplicaError::Internal` (non-retryable).

Response `ret_code` mapping:
- `Success` → proceed with the response fields.
- `NotFound` → `PxReplicaError::GroupNotFound`.
- `Unavailable` → `PxReplicaError::ShuttingDown`.
- `Internal` / `InvalidArgument` → `PxReplicaError::Internal`.

Edge cases:
- Endpoint cache stale (replica moved) → response `NotFound` → refresh
  endpoint cache, retry once.
- Connection dropped mid-call → `ConnectionClosed` → reconnect, retry.
- All endpoints down → `AllDown` → `PxReplicaError::Internal`.

## 5. LearnerStream Client Rewrite

### 5.1 Why

The current `PxLearnerStream` (`learner_stream.rs`) maintains a tonic
bidi stream with a background task that sends outbound frames and reads
inbound responses, correlating by `request_id` via a `PendingMap`. Under
crow-rpc, this becomes pipelined unary `call()`s on a persistent
connection — simpler, reuses the standard `RpcClient::call()` path.

### 5.2 New structure

The `PxLearnerStream` struct is rewritten to hold:
- `Arc<PxRpcTransport>` (shared transport — reuses the connection pool).
- `endpoint: String` (the peer's crow-rpc endpoint).
- `PendingMap` (unchanged — `request_id → oneshot::Sender`).
- `cmd_tx: mpsc::Sender<OutboundCmd>` (unchanged — the user-facing API).

The background task is simplified:
a. Read `OutboundCmd` from `cmd_rx`.
b. For request-response frames (Accept, Heartbeat, FetchGap):
   - Build the flatbuffer request.
   - `rpc.call(&server, &conn, req_id, control, None, msg_type)` →
     `CallFuture`.
   - Spawn a task to await the `CallFuture`, parse the response via
     the zero-copy wrapper, and send the result through `reply_tx`.
   - Insert `reply_tx` into `PendingMap` (for timeout/cancellation).
c. For fire-and-forget frames (ChosenNotification, BatchChosenNotification):
   - Build the flatbuffer request.
   - `rpc.send(&server, &conn, req_id, control, None, msg_type, None,
     null)` — no completion callback, no `CallFuture`.
  . Drop `reply_tx` (it's `None` for fire-and-forget).

### 5.3 Connection lifetime

The connection is obtained from `PxRpcTransport::conn_for(endpoint)` —
shared with the unary RPC path. The connection is persistent (lives for
the lifetime of the `PxRemoteReplica`). On transport failure:
a. The `CallFuture` resolves with `ConnectionClosed`.
b. The background task fails all pending oneshots with
   `PxReplicaError::Internal("stream reset")`.
c. The transport reconnects on the next `conn_for` call (the old
   connection is evicted from the `DashMap`).

### 5.4 Backpressure

The `cmd_rx` channel capacity is `learner_stream_window_frames` (same
as current). When the window is full, `cmd_tx.send()` blocks — this is
the same flow-control as the current implementation.

Edge cases:
- Connection drop mid-flight → `CallFuture` resolves with
  `ConnectionClosed` → fail the pending oneshot.
- Fire-and-forget frame on a dropped connection → `rpc.send()` returns
  `ConnectionError` → log warning, drop the frame (same as current
  fire-and-forget semantics — ChosenNotification is best-effort).
- FetchGap with no value → server does not respond → client times out
  via `rpc_timeout` → `PxReplicaError::Internal("fetch gap timeout")`.

## 6. Snapshot Client Rewrite

**Deferred from R32.** `join_via_snapshot` stays on gRPC. The C++
`RpcServer` has no `set_max_data_size` API (4 MiB default per
`Connection`); adding it requires C++ changes. A follow-up task will:
1. Add `crow_rpc_server_set_max_data_size` to the C API.
2. Implement the `ESnapshotRequest` server handler.
3. Rewrite `join_via_snapshot` to use `PxRpcTransport::snapshot()`.

The schema tables (`FBSnapshotRequest`/`FBSnapshotResponse`) + msg_types
are defined in R32 so the follow-up only needs the handler + client
rewrite.

## 7. Server Wiring

### 7.1 Port allocation

Add to `lib/crow-protocol/src/ports.rs`:
```
/// crow-kv-server crow-rpc consensus listener — base port (R32
/// migration). Separate from the gRPC port so both servers run
/// simultaneously during the mixed-rollout window. Inter-KV-server
/// only (replica-to-replica Paxos). R117 adds a separate
/// client-facing port. Stride 1 (one port per instance).
pub const KV_RPC_BASE: u16 = 28101;
```

Port offset: `KV_SERVER_GRPC_BASE (28001) - KV_RPC_BASE (28101) = -100`.
The crow-rpc port is `grpc_port + 100`. (Positive offset — the crow-rpc
port is higher than the gRPC port, avoiding collision with the gRPC
listener pool `28001–28200`. `KV_RPC_BASE = 28101` starts after the
gRPC pool's max practical range.)

Add `KvServerRpc` to `ServicePort` enum + `base()` / `stride()`.

### 7.2 Server lifecycle

`lib/crow-kv/src/cluster/kv_server.rs` — add a new `start_rpc_server()`
method to `Arc<PxKvStore>` (or a new trait impl). Runs alongside the
existing gRPC server:

a. Create `RpcServer::with_engines(None, io_engines, io_workers)`.
b. Create `PxRpcService::new(self.clone(), Handle::current())`.
c. `service.register_handlers(&Arc::new(server))`.
d. `server.listen(addr, rpc_port)` + `server.start()`.
e. Store the `RpcServer` handle in a new `rpc_server_state` field on
   `PxKvStore` (for shutdown).
f. Update local replica endpoints with the crow-rpc endpoint (so peers
   know where to connect).

### 7.3 Mixed rollout

During the rollout window, both servers run:
- gRPC server on `grpc_port` (existing `start()`).
- crow-rpc server on `rpc_port` (new `start_rpc_server()`).

The client selects transport based on config (same as R115's
`with_rpc_transport()` pattern). When all peers are migrated, the gRPC
server is removed.

### 7.4 Shutdown

`PxKvStore::shutdown_server` is extended to also stop the crow-rpc
server (`server.stop()`). The cascade shutdown order is unchanged:
stop both servers → join tasks → abort on timeout.

Edge cases:
- crow-rpc port already in use → `server.listen()` returns
  `RpcError::ConnectionError` → logged, server start fails with a clear
  message (same as gRPC bind failure).
- gRPC server fails to start but crow-rpc succeeds → the server runs
  with crow-rpc only (peers must be configured to use crow-rpc).

## 8. Connection::from_handle FFI Helper

### 8.1 Why

R114's open issue: `RpcClient::send()` takes `&Connection`, but a
server-side handler only has the raw `conn_handle` from
`ServerRequest`. R117's WatchNotify needs this. R32 resolves it to
unblock R117.

### 8.2 Implementation

`lib/crow-rpc/ffi/src/server.rs` — add:

```rust
impl Connection {
    /// Construct a `Connection` wrapper from a raw `conn_handle`
    /// obtained from `ServerRequest`. The connection is owned by the
    /// transport; this wrapper is a borrow (no-op `Drop`). Safe to
    /// use for the duration of the handler's async work (the transport
    /// keeps the connection alive until it drops).
    pub fn from_handle(handle: sys::crow_rpc_conn_t) -> Self {
        Self { handle }
    }
}
```

This lets a server-side handler call:
```rust
let conn = Connection::from_handle(req.conn_handle as sys::crow_rpc_conn_t);
rpc.call(&server, &conn, req_id, control, None, msg_type)?;
```

R32 itself does not use this (LearnerStream's server side only sends
responses via `submit_response`), but the helper is added + tested in
R32 to unblock R117.

Edge cases:
- `conn_handle` is stale (connection dropped after the handler started)
  → `rpc.call()` returns `ConnectionClosed` (the C++ transport detects
  the stale handle).
- `conn_handle` is null → `Connection::from_handle(null)` →
  `rpc.call()` returns `InvalidArg` (the C++ transport rejects null).

## Scope

**lib/crow-protocol:**
- `src/fbs/kv_consensus.fbs` — new flatbuffer schema (all pxos.proto
  messages converted).
- `src/fbs/msg_type.fbs` — add 18 new msg_type entries (1000–1017).
- `build.rs` — add `kv_consensus.fbs` to `fbs_files` + new `flatc`
  invocation.
- `src/lib.rs` — add `kv_consensus_generated` module + `kv_consensus_fb`
  re-export + `fb_wrappers` module.
- `src/fb_wrappers/mod.rs` — new module root.
- `src/fb_wrappers/kv_consensus.rs` — zero-copy `Ref` wrappers for 8
  response types.
- `src/ports.rs` — add `KV_RPC_BASE` + `KvServerRpc` variant.

**lib/crow-rpc/ffi:**
- `src/server.rs` — add `Connection::from_handle()` constructor.

**lib/crow-kv:**
- `src/rpc/px_rpc_transport.rs` — new client transport (replaces
  `PxServiceClient<Channel>` usage in `remote_replica.rs`).
- `src/rpc/mod.rs` — export new module.
- `src/cluster/learner_stream.rs` — rewrite to use `PxRpcTransport`
  (pipelined unary `call()`s instead of tonic bidi stream).
- `src/cluster/remote_replica.rs` — replace `PxServiceClient<Channel>`
  with `PxRpcTransport`; update `send_prepare`/`send_pre_vote`/
  `send_request_vote`/`send_heartbeat`/`send_step_down`.
- `src/cluster/group_membership.rs` — rewrite `join_via_snapshot` to
  use `PxRpcTransport::snapshot()`.
- `src/cluster/kv_server.rs` — add `start_rpc_server()` + shutdown.
- `src/cluster/px_kv_store.rs` — add `rpc_server_state` field.

**lib/crow-kv:**
- `src/rpc/px_rpc_service.rs` — new server handler module (14 handlers;
  snapshot deferred).
- `src/rpc/mod.rs` — export new module.

**tools:**
- `bench-kv-rpc.sh` — new benchmark script (gRPC baseline vs crow-rpc).

**tests:**
- `lib/crow-kv/tests/rpc_migration_test.rs` — new integration test.

## Complexity

**High.** The schema conversion is mechanical (20 message types,
field-for-field). The server handler is a line-by-line port of
`px_service.rs` (same logic, different wire format). The genuinely hard
parts are:

1. **LearnerStream rewrite** — the current bidi stream background task
   is non-trivial (connection management, pending map, reconnect
   backoff). The pipelined-unary model is simpler but the rewrite
   touches the hot path and must preserve ordering semantics (Accept
   frames must not overtake each other on the same connection —
   crow-rpc's MPSC queue preserves FIFO order per connection, so this
   is satisfied).
2. **Mixed rollout** — both gRPC and crow-rpc servers run
   simultaneously. The client must select the correct transport per
   peer. Endpoint propagation (the local replica's crow-rpc endpoint
   must be advertised to peers) requires careful coordination.
3. **Snapshot as single-frame** — deviates from the gRPC streaming
   model. The 64 MiB frame limit is a new constraint; must verify
   `snapshot_export` output sizes in practice.

The zero-copy wrappers and `Connection::from_handle` are Low complexity
(mechanical, following established patterns).

## Test Design

### Unit tests (UT)

- **Schema round-trip**: build each request/response type via
  `FlatBufferBuilder`, parse via `flatbuffers::root`, verify all fields.
  One test per message type (18 tests). `pixi run test-protocol`.
- **Zero-copy wrapper accessors**: for each `Ref` struct, construct
  from a valid flatbuffer + from a malformed buffer, verify `valid()`,
  accessor return values, and `None`/default on missing fields. One
  test per wrapper (8 tests). `pixi run test-protocol`.
- **`Connection::from_handle`**: construct from a valid handle (via a
  test `RpcServer` + `connect`), verify `handle()` returns the same
  pointer; construct from null, verify it doesn't panic. `pixi run
  test-rpc-ffi`.
- **Error mapping**: `From<RpcError> for PxReplicaError` — verify each
  `RpcError` variant maps to the correct `PxReplicaError`. `pixi run
  test-kv-core`.
- **Port computation**: `ServicePort::KvServerRpc.port(0)` ==
  `KV_RPC_BASE`; `port(1)` == `KV_RPC_BASE + 1`. `pixi run
  test-protocol`.

### End-to-end tests (E2E)

- **Prepare/Accept over crow-rpc**: 3-node cluster, all using crow-rpc.
  Submit a Put → leader runs Prepare + Accept over crow-rpc → verify
  the value is replicated to all followers. `pixi run test-kv-server`.
- **NotLeaderHint over crow-rpc**: send a Put to a follower → follower
  returns `NotLeaderHint` (via `term_stale`/`epoch_mismatch` fields) →
  client redirects to the leader → Put succeeds. `pixi run
  test-kv-server`.
- **LearnerStream catch-up**: start a 3-node cluster, stop one follower,
  write N entries, restart the follower → follower catches up via
  LearnerStream (Accept + ChosenNotification frames over crow-rpc) →
  reaches `CaughtUp` state. `pixi run test-kv-server`.
- **Snapshot join via crow-rpc**: create a group with data, add a new
  non-voting replica → new replica calls `join_via_snapshot` over
  crow-rpc → receives the snapshot in one frame → `snapshot_import` →
  seeds learner frontier → catches up. `pixi run test-kv-server`.
- **Mixed rollout**: 3-node cluster, 2 nodes on crow-rpc, 1 on gRPC →
  verify consensus works (Prepare/Accept succeed across mixed
  transports). `pixi run test-kv-server`.
- **Connection drop mid-call**: kill a follower mid-Accept → leader's
  `CallFuture` resolves with `ConnectionClosed` → leader retries on a
  fresh connection → Accept succeeds. `pixi run test-kv-server`.
- **Fire-and-forget ChosenNotification**: leader sends
  ChosenNotification to a follower over crow-rpc (no response expected)
  → follower applies the chosen value → verify the follower's chosen
  frontier advances. `pixi run test-kv-server`.
- **FetchGap over crow-rpc**: follower has a gap at slot N → sends
  FetchGap over crow-rpc → leader responds with the chosen value →
  follower applies. `pixi run test-kv-server`.

## Module Structure

```
lib/crow-protocol/src/fbs/
  kv_consensus.fbs              — new: consensus flatbuffer schema
  msg_type.fbs                  — modified: +18 msg_type entries
lib/crow-protocol/src/fb_wrappers/
  mod.rs                        — new: fb_wrappers module root
  kv_consensus.rs               — new: zero-copy Ref wrappers
lib/crow-protocol/src/
  lib.rs                        — modified: +kv_consensus_generated +fb_wrappers
  ports.rs                      — modified: +KV_RPC_BASE +KvServerRpc
lib/crow-protocol/
  build.rs                      — modified: +kv_consensus.fbs codegen
lib/crow-rpc/ffi/src/
  server.rs                     — modified: +Connection::from_handle
lib/crow-kv/src/rpc/
  px_rpc_service.rs             — new: crow-rpc server handlers (14)
  px_rpc_transport.rs           — new: crow-rpc client transport
  mod.rs                        — modified: +px_rpc_service +px_rpc_transport module
lib/crow-kv/src/cluster/
  learner_stream.rs             — modified: rewrite for pipelined unary
  remote_replica.rs             — modified: PxServiceClient → PxRpcTransport
  group_membership.rs           — modified: join_via_snapshot rewrite
  kv_server.rs                  — modified: +start_rpc_server +shutdown
  px_kv_store.rs                — modified: +rpc_server_state field
app/crow-kv-server/src/
  main.rs                       — modified: call start_rpc_server()
tools/
  bench-kv-rpc.sh               — new: gRPC baseline vs crow-rpc bench
lib/crow-kv/tests/
  rpc_migration_test.rs         — new: integration tests
```

## Config Extensions

- `PxElectionConfig`: no new fields (the transport selection is
  per-peer, not per-config). The `with_rpc_transport()` builder pattern
  (from R115) is used on `PxRemoteReplica` to select crow-rpc vs gRPC.
- `KV_RPC_BASE` (ports.rs): new constant, default `28101`.

## Server Wiring

1. `main.rs`: after `registry.start()` (existing gRPC server start),
   call `store.start_rpc_server()` (new).
2. `start_rpc_server()`: create `RpcServer`, register handlers, listen
   on `rpc_port`, start. Store handle in `rpc_server_state`.
3. `shutdown_server()`: stop both gRPC + crow-rpc servers (cascade).
4. Endpoint propagation: `set_endpoint` now also sets the crow-rpc
   endpoint (derived via port offset) so peers can connect.
