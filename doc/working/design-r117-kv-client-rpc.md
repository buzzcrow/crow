<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KvService Client-Facing gRPC → crow-rpc Migration (R117)

Backlog: `doc/backlog/R117-kv-client-rpc-migration.md`.
Root design: `doc/design/kv/design-crow-kv-rpc.md` (R32 consensus
wire protocol — R117's client-facing doc is a sibling, not a subset),
`doc/design/rpc/design-crow-rpc.md` §6 (Flatbuffer Wrapper
Convention), `doc/design/kv/design-crow-kv-watch-notify.md`
(WatchNotify). Migration rules: `doc/working/todo_fb.md` "Rules to
Follow During Migration".

Already landed (R32, post-R32 code state): `kv_consensus.fbs` schema
conventions, `FB<Type>Ref` zero-copy wrapper pattern
(`fb_wrappers/kv_consensus.rs`), `px_rpc_service.rs` server handler
pattern, `px_rpc_transport.rs` client transport pattern,
`Connection::from_handle` FFI helper, `start_rpc_server` /
`rpc_transport` wiring, msg types 1000–1017. R117 reuses all of these
for the client-facing surface. Architecture decisions and rationale
are in the root design docs; this doc does not repeat them.

## 1. Flatbuffer Schema (`kv_client.fbs`)

### 1.1 Why a separate schema + ret-code enum

R32's `kv_consensus.fbs` covers the inter-KV-server Paxos path
(term/epoch `NotLeaderHint` fields, `FBKvRetCode` without
`JournalScanGcGap`). The client-facing surface has different redirect
semantics (a string `not_leader_hint` endpoint the client retries at
via its topology cache — no Paxos term/epoch reasoning) and one extra
error (`JournalScanGcGap`). Reusing `FBKvRetCode` would conflate the
two redirect models and force the consensus schema to carry a
client-only error. A separate `kv_client.fbs` + `FBKvClientRetCode`
keeps the two surfaces independently evolvable, matching how the
`.proto` files are already split (`pxos.proto` vs `kv.proto`).

### 1.2 Schema layout

`lib/crow-protocol/src/fbs/kv_client.fbs` (new), namespace
`crow.kv_client.proto`. Conventions follow `kv_consensus.fbs`:
every request/response table starts with `id` (request_id) +
`rpc_create_nano`; every response carries `ret_code` +
`error_msg`; `NotLeaderHint` is a string field on the response tables,
not a separate message.

```
include "common_type.fbs";
namespace crow.kv_client.proto;

enum FBKvClientRetCode : int16 {
    Success = 0,
    NotFound = 1,
    NotLeader = 2,
    Unavailable = 3,
    Internal = 4,
    InvalidArgument = 5,
    JournalScanGcGap = 6,
}

enum FBReadMode : int16 {
    Linearizable = 0,
    MinSlot = 1,
}
```

`FBReadMode` is defined here (R32 did not define it — `kv_consensus.fbs`
has no read-mode field). The numeric values match `kv.proto`'s
`ReadMode` (0=Linearizable, 1=MinSlot) so the client maps
`ReadMode as i32` → `FBReadMode` directly.

Unary request/response tables (one pair per RPC). Each request carries
`forwarded:bool` (the loop-guard that replaces the
`x-crow-kv-forwarded` metadata header — crow-rpc has no metadata).
Field numbers are append-only and documented inline:

- `FBKvSetRequest` / `FBKvResponse` (Put) — `key`, `value`, `seq`,
  `ttl_ms`, `client_id`, `request_id`, `request_create_ms`,
  `group_id`, `forwarded`. Response: `ok`, `revision`, `error`,
  `not_found`, `not_leader_hint`, `request_id`, `request_create_ms`,
  `value`, `read_slot`, `safe_slot`, `error_code` (FBKvClientRetCode).
- `FBKvGetRequest` / `FBKvResponse` (Get) — reuses `FBKvResponse`.
  Request: `key`, `request_id`, `request_create_ms`, `group_id`,
  `read_mode`, `min_slot`, `forwarded`.
- `FBKvDeleteRequest` / `FBKvResponse` (Delete) — `key`, `seq`,
  `client_id`, `request_id`, `request_create_ms`, `group_id`.
- `FBKvBatchItem` (table) + `FBKvBatchWriteRequest` / `FBKvResponse`
  — `items: [FBKvBatchItem]`, `seq`, `client_id`, `request_id`,
  `request_create_ms`, `group_id`.
- `FBKvScanRequest` / `FBKvScanResponse` — `prefix`, `limit`,
  `request_id`, `request_create_ms`, `group_id`, `read_mode`,
  `start_after`, `end_key`, `min_slot`, `keys_only`, `count_only`,
  `deadline_ms`, `forwarded`. Response: `ok`, `error`, `truncated`,
  `items: [FBKvScanItem]`, `request_id`, `request_create_ms`,
  `read_slot`, `not_leader_hint`, `error_code`, `count`, `timed_out`.
  `FBKvScanItem` = `{key, value}` (table).
- `FBKvJournalScanRequest` / `FBKvJournalScanResponse` — `group_id`,
  `min_slot`, `max_slot`, `key_prefix`, `limit`, `request_id`,
  `request_create_ms`, `read_mode`, `forwarded`. Response: `ok`,
  `error`, `ops: [FBKvJournalOp]`, `truncated`, `last_op_slot`,
  `read_slot`, `error_code`, `not_leader_hint`, `request_id`,
  `request_create_ms`. `FBKvJournalOp` = `{key, value, is_delete,
  slot}` (table).
- `FBCreateSnapshotRequest` / `FBCreateSnapshotResponse` — `group_id`,
  `read_mode`, `min_slot`. Response: `ok`, `error`, `snapshot_handle`,
  `at_slot`, `error_code`, `not_leader_hint`.
- `FBListSnapshotsRequest` / `FBListSnapshotsResponse` — `group_id`.
  Response: `ok`, `error`, `snapshots: [FBSnapshotInfo]`.
  `FBSnapshotInfo` = `{snapshot_handle, at_slot, lease_remaining_ms}`.
- `FBSnapshotScanRequest` / `FBSnapshotScanResponse` —
  `snapshot_handle`, `prefix`, `start_after`, `limit`, `group_id`.
  Response: `ok`, `error`, `truncated`, `items: [FBKvScanItem]`,
  `error_code`.
- `FBReleaseSnapshotRequest` / `FBReleaseSnapshotResponse` —
  `snapshot_handle`, `group_id`. Response: `ok`, `error`.

WatchNotify frames (persistent-connection server-push model, NOT R114
bidi `Stream`):

- `FBWatchSubscribe` (client→server, fire-and-forget `send`) —
  `version`, `group_id`, `prefix`.
- `FBWatchUnsubscribe` (client→server, fire-and-forget `send`) —
  `group_id`, `prefix`.
- `FBWatchNotify` (server→client push, fire-and-forget `send`) —
  `group_id`, `prefix`, `keys: [FBBytes]`, `slot`, `values: [FBBytes]`.
  Flatbuffers does NOT support `[[ubyte]]` nested vectors directly
  (verified: `flatc` rejects with "nested vector types not supported
  (wrap in table first)"). Use a wrapper table `FBBytes { data:[ubyte] }`
  for each key/value, then `keys: [FBBytes]` / `values: [FBBytes]`.
- `FBWatchNotifyError` (server→client push) — `group_id`,
  `not_leader_hint`, `error`.

Edge cases:
- `keys`/`values` use a wrapper table `FBBytes { data:[ubyte] }` —
  flatbuffers rejects `[[ubyte]]` nested vectors. The wrapper reads
  `keys()` → `Vector<Offset<FBBytes>>` and iterates, calling
  `.data()` on each to get the inner `&[u8]`.
- `forwarded: bool` default false — old clients that don't set it are
  treated as not-forwarded (the server forwards once). Field is
  additive; no wire break.

### 1.3 Message type IDs (1100–1199)

`msg_type.fbs` extended (R32 used 1000–1017; 1018–1099 stays reserved
for consensus):

```
// kv client-facing service (range 1100s, see kv_client.fbs — R117)
EKvSetRequest = 1100,
EKvResponse = 1101,           // Put/Delete/BatchWrite share this
EKvGetRequest = 1102,
EKvDeleteRequest = 1103,
EKvBatchWriteRequest = 1104,
EKvScanRequest = 1105,
EKvScanResponse = 1106,
EKvJournalScanRequest = 1107,
EKvJournalScanResponse = 1108,
ECreateSnapshotRequest = 1109,
ECreateSnapshotResponse = 1110,
EListSnapshotsRequest = 1111,
EListSnapshotsResponse = 1112,
ESnapshotScanRequest = 1113,
ESnapshotScanResponse = 1114,
EReleaseSnapshotRequest = 1115,
EReleaseSnapshotResponse = 1116,
EWatchSubscribe = 1117,       // client→server (fire-and-forget)
EWatchUnsubscribe = 1118,     // client→server (fire-and-forget)
EWatchNotify = 1119,          // server→client push
EWatchNotifyError = 1120,     // server→client push
```

Put/Delete/BatchWrite share `EKvResponse` (1101) — they all return
`FBKvResponse`, distinguished by the request msg_type the server
dispatched on. The response msg_type is the same for all three
(matching how `kv.proto` uses one `KvResponse` for Put/Get/Delete/
BatchWrite). Get also returns `FBKvResponse` → `EKvResponse`.

### 1.4 Build integration

`build.rs`: add `kv_client.fbs` to `fbs_files` + a new
`flatc --rust --gen-all` invocation (inlines `common_type.fbs`).
`lib.rs`: add `kv_client_generated` module + `kv_client_fb` re-export
(mirroring `kv_consensus_fb`).

## 2. Zero-Copy Wrappers (`fb_wrappers/kv_client.rs`)

### 2.1 Why

The client transport parses responses via `FB<Type>Ref` wrappers
(zero-copy, no owned intermediate — todo_fb.md core rule). The server
handler reads requests directly via `flatbuffers::root` (no wrapper
needed for requests, same as R32).

### 2.2 Wrapper set

`lib/crow-protocol/src/fb_wrappers/kv_client.rs` (new) + register in
`lib/crow-protocol/src/fb_wrappers.rs` (the module root —
`pub mod kv_consensus;` lives here; R117 adds `pub mod kv_client;`).
The `parse_root` helper is currently private to `kv_consensus.rs`;
R117 hoists it to `fb_wrappers.rs` (module-private `pub(super)`) so
both `kv_consensus.rs` and `kv_client.rs` reuse it without duplication.

Wrappers (one `Ref` struct per response type, `parse_root` helper
shared with `kv_consensus.rs` — hoist `parse_root` into the
`fb_wrappers` module root so both files reuse it):

- `FBKvResponseRef` — `ok`, `revision`, `error`, `not_found`,
  `not_leader_hint() -> Option<&str>`, `request_id`, `value() ->
  Option<&[u8]>`, `read_slot`, `safe_slot`, `error_code() ->
  FBKvClientRetCode`.
- `FBKvScanResponseRef` — `ok`, `error`, `truncated`, `items() ->
  Option<...>` (vector iter), `read_slot`, `not_leader_hint`,
  `error_code`, `count`, `timed_out`.
- `FBKvJournalScanResponseRef` — `ok`, `error`, `ops()` (vector iter),
  `truncated`, `last_op_slot`, `read_slot`, `error_code`,
  `not_leader_hint`.
- `FBCreateSnapshotResponseRef`, `FBListSnapshotsResponseRef`,
  `FBSnapshotScanResponseRef`, `FBReleaseSnapshotResponseRef`.
- `FBWatchNotifyRef` — `group_id`, `prefix() -> Option<&[u8]>`,
  `keys()` / `values()` (nested vector iter), `slot`.
- `FBWatchNotifyErrorRef` — `group_id`, `not_leader_hint() ->
  Option<&str>`, `error() -> Option<&str>`.

The scan/journal `items`/`ops` vectors are accessed via the flatbuffer
vector reference; the client converts to `Bytes` at the boundary
(matching `client.rs`'s existing `GetOutcome`/`ScanOutcome`/`JournalOp`
which already use `Bytes`).

Edge cases:
- Malformed buffer → `valid()` returns false; caller maps to
  `Error::Transport`.
- Missing optional field → accessor returns `None`/default.

## 3. Server-Side Handler (`kv_rpc_service.rs`)

### 3.1 Why a parallel handler set

The tonic `KvStoreService` (`kv_service.rs`) stays during mixed
rollout. R117 adds `KvRpcService` alongside it (same `PxKvStore`
delegation), dispatching by `msg_type` on the client-facing crow-rpc
port. Both run simultaneously; clients pick the port.

### 3.2 Structure

`lib/crow-kv/src/rpc/kv_rpc_service.rs` (new), mirroring
`px_rpc_service.rs` layout: `KvRpcService` struct holds
`Arc<PxKvStore>` + `Handle`; `register_handlers` wires one handler per
request `msg_type` into the client-facing `RpcServer`;
`make_handler` closure pattern; `handle_X` methods.

Each unary handler:
a. Parse request via `flatbuffers::root::<FB<Type>Request>(req.control)`.
   On parse failure → `submit_error` with `InvalidArgument`.
b. Dispatch to the existing `PxKvStore` logic
   (`kv_put`/`kv_get`/`kv_delete`/`kv_batch_write`/`kv_scan`/
   `kv_journal_scan`/`kv_create_snapshot`/`kv_list_snapshots`/
   `kv_snapshot_scan`/`kv_release_snapshot`) — the same `KvStore` trait
   methods the tonic handler calls. The async path spawns a tokio task
   via `self.rt.spawn` and submits the response from the task.
c. Build the response flatbuffer via `FlatBufferBuilder`, `finish`,
   `submit_response`.

### 3.3 Transparent leader-forwarding

`Get`/`Scan`/`JournalScan` preserve the forward step (linearizable
reads only; `MinSlot` served locally). The loop-guard
`x-crow-kv-forwarded` metadata header becomes the `forwarded: bool`
field on the request flatbuffer. The handler:

a. If `read_mode == Linearizable && !req.forwarded` and
   `forward_target_for(group_id)` returns `Some(endpoint)` → re-issue
   the request to the leader via the client-facing crow-rpc client
   transport (Work Item 4's `KvRpcTransport`, held by the server for
   forwarding). Set `forwarded = true` on the re-issued request. On
   success, submit the leader's response. On forward failure, serve
   stale local + set `not_leader_hint = endpoint` (same fallback as
   the tonic handler).
b. Else serve locally.

The server holds an `Arc<KvRpcTransport>` for outbound forwards
(stored in `rpc_server_state` alongside the consensus `PxRpcTransport`,
or a new `client_rpc_transport` field). The forward uses
`transport.send_get(...)` etc. — the same methods the client uses.

### 3.4 WatchNotify server-push handler

`WatchSubscribe` handler (msg_type 1117):
a. Parse `FBWatchSubscribe` → `group_id`, `prefix`.
b. `store.get_group(group_id)`. If none → push `FBWatchNotifyError`
   via `RpcClient::send` on the connection (from
   `Connection::from_handle(req.conn_handle)`), error="group not
   found". No `submit_response` (fire-and-forget).
c. If `!group.local_replica().is_leader()` → push
   `FBWatchNotifyError` with `not_leader_hint =
   group.leader_endpoint()`.
d. Else → build `Connection` via `from_handle`, register it in the
   group's `WatchRegistry` (refactored, §4) with the `(Connection,
   Arc<RpcClient>, Arc<RpcServer>)` push target. Track the
   `(group_id, watcher_id, prefix)` for cleanup.

`WatchUnsubscribe` handler (msg_type 1118): parse →
`WatchRegistry::unsubscribe`.

Connection-close cleanup: crow-rpc does not deliver a connection-close
callback to the handler. The `WatchRegistry::emit` path detects a dead
push target when `RpcClient::send` returns `ConnectionClosed` — the
registry removes the watcher lazily on the first failed push (same
role as the tonic `try_send` → `Closed` cleanup). The safety-net
poller covers missed notifications during the gap.

### 3.5 WatchRegistry refactor

`watch_registry.rs` `Watcher` struct currently holds
`mpsc::Sender<Result<WatchNotifyResponse, tonic::Status>>`. Refactored
to an abstract push target:

```rust
enum PushTarget {
    Tonic(mpsc::Sender<Result<WatchNotifyResponse, tonic::Status>>),
    CrowRpc(CrowRpcPushTarget),
}
struct CrowRpcPushTarget {
    conn: Connection,
    rpc: Arc<RpcClient>,
    server: Arc<RpcServer>,
}
```

`emit` matches on the target: `Tonic` → `try_send` (existing path);
`CrowRpc` → build `FBWatchNotify` flatbuffer + `rpc.send(server, conn,
req_id, control, None, EWatchNotify, noop_completion(), null)` (fire-
and-forget). On `ConnectionClosed`/`ConnectionError` → remove the
watcher (lazy cleanup) + increment `closed_watchers`.

`subscribe` gains an overload accepting `CrowRpcPushTarget` (returns
`watcher_id`). The existing tonic `subscribe` delegates to
`PushTarget::Tonic`.

Edge cases:
- Mixed-transport group: a group may have both tonic-stream watchers
  and crow-rpc-push watchers during rollout. `emit` iterates all
  watchers per prefix and pushes to each by its target type — both
  coexist.
- `fail_all` scoping: see §6.
- Lazy cleanup race: a dead watcher may receive one extra failed
  `send` before removal — harmless (fire-and-forget, no pending
  entry).

## 4. Client-Side Transport (`kv_rpc_transport.rs`)

### 4.1 Why a parallel transport

`CrowkvClient` keeps its tonic `ConnectionPool` for the legacy path;
R117 adds `KvRpcTransport` selected via `with_rpc_transport` (same
mixed-rollout mechanism as R32's `PxRemoteReplica::with_rpc_transport`).

### 4.2 Structure

`lib/crow-kv-client/src/kv_rpc_transport.rs` (new), mirroring
`px_rpc_transport.rs`: `KvRpcTransport` holds `Arc<RpcServer>` +
`Arc<RpcClient>` + `DashMap<String, Connection>` + `AtomicU64`
next_req_id. `conn_for(endpoint)` derives the client-facing crow-rpc
port from the gRPC port via `KV_CLIENT_RPC_BASE` offset (§7).

Unary methods (`send_put`, `send_get`, `send_delete`, `send_batch_write`,
`send_scan`, `send_journal_scan`, `send_create_snapshot`,
`send_list_snapshots`, `send_snapshot_scan`, `send_release_snapshot`):
build request flatbuffer → `rpc.call` → await → parse via `FB<Type>Ref`
wrapper → map to the existing outcome types (`WriteOutcome`,
`GetOutcome`, `ScanOutcome`, `JournalScanOutcome`) or a raw response
struct. `NotLeaderHint` parsed from `not_leader_hint` string field →
fed into `CrowkvClient`'s existing retry + topology-cache logic.

### 4.3 CrowkvClient transport selection

`CrowkvClient` gains an `Option<Arc<KvRpcTransport>>` field
(`rpc_transport`). `with_rpc_transport(transport)` sets it. Each public
method (`put`/`get`/`delete`/`batch_write`/`scan`/`scan_count`/
`journal_scan`) checks `self.rpc_transport.get()` first: when set,
delegate to the transport's `send_*` (with the existing retry/
topology/`NotLeaderHint`/metrics wrapping — only the wire send
changes); when not set, the existing tonic path. The snapshot methods
(`create_snapshot`/`list_snapshots`/`snapshot_scan`/`release_snapshot`)
are not yet on `CrowkvClient` (they live on the server only) — R117
adds client methods for them only if the FFI/consumers need them;
the server handler covers them regardless.

### 4.4 WatchNotifyClient rewrite

`watch_notify.rs` becomes a persistent-connection + client-side
handler model:
a. `subscribe` opens a connection to the leader via
   `KvRpcTransport::conn_for` (dedicated `RpcClient` — see §6).
b. Registers a client-side handler via `RpcClient::register_handler`
   for `EWatchNotify` (1119) + `EWatchNotifyError` (1120): the handler
   parses `FBWatchNotify` → forwards to the user's
   `mpsc::Sender<WatchNotify>`; `FBWatchNotifyError` with non-empty
   `not_leader_hint` → `topology.set_leader` + signal reconnect.
c. Sends `FBWatchSubscribe` as fire-and-forget `send()` on the
   connection.
d. The reader loop becomes a reconnect loop: on reconnect signal or
   connection drop, re-resolve leader, re-open connection, re-register
   handler, re-send `FBWatchSubscribe`. The safety-net poller covers
   the gap (unchanged).

Edge cases:
- `WatchNotify` frame shape: `FBWatchNotify` carries `keys`/`values`
  as `[[ubyte]]`; the handler builds a `WatchNotify` proto struct
  (re-exported to callers) from the flatbuffer fields so the public
  API (`WatchSubscription.notify_rx: mpsc::Receiver<WatchNotify>`) is
  unchanged.
- Mid-stream drop: the client-side handler stops firing (connection
  closed); the reconnect loop re-establishes. No `fail_all` impact
  (dedicated `RpcClient`, §6).

## 5. FFI Layer Preservation

`ffi.rs` wraps `HardwareClient`/`ServiceRegistryClient`, both wrap
`CrowkvClient`. Migrating `CrowkvClient`'s transport (§4.3) migrates
the FFI path automatically. The C ABI (`crow_hw_*`/`crow_svc_*`) is
unchanged — the `rpc_endpoint` parameter is already renamed
(todo_fb.md §2). No FFI source changes beyond what §4.3 does to
`CrowkvClient`. Verified by existing FFI tests.

## 6. Error Model + `fail_all` Scoping

### 6.1 Error mapping

`RpcError` → `KvClientError`/`Error` variants, reusing
`RpcError::is_retryable()` (todo_fb.md §1, already done):
- `ConnectionClosed` → retry on next connection (same as gRPC
  `Unavailable`).
- `Timeout` → `Error::Transport` (caller retry budget).
- `SendQueueFull` → retry with backoff.
- `RegistrationFailed`/`AllDown`/`InvalidArg` → non-retryable
  `Error::Transport`.
- `NotLeaderHint` is a protocol-level response (`not_leader_hint`
  string + `FBKvClientRetCode::NotLeader`), not a transport error —
  the client's retry/topology-cache logic is unchanged.
- `JournalScanGcGap` → `FBKvClientRetCode::JournalScanGcGap` →
  `Error::JournalScanGcGap` (no retry; caller full-scan rebuild).

### 6.2 `fail_all` scoping (todo_fb.md Open Issue, R117 scope)

**Decision: dedicated `RpcClient` per WatchNotifyClient.** R114's
`fail_all(ConnectionClosed)` fires for ALL pending entries on the
`request_client_`, not per-connection. WatchNotify uses fire-and-
forget `send()` in both directions (subscribe/unsubscribe from client,
notify from server) — these create NO pending `call()` entries, so
`fail_all` never fires for WatchNotify traffic itself. The risk is
*cross-contamination*: if the WatchNotifyClient shared the unary
transport's `RpcClient`, a watcher-connection drop could trigger
`fail_all` and fail pending unary `call()`s on other connections.

A dedicated `RpcClient` for the WatchNotifyClient scopes any `fail_all`
to the watch client (which has no pending entries anyway). This needs
no FFI change (`fail_all` semantics untouched) and matches the
existing surface. The unary `KvRpcTransport` keeps its own
`RpcClient`. This is the clearly-superior option over a
connection-scoped `fail_all` (which would require C++ FFI changes +
reaper logic per connection).

## 7. Mixed Rollout + Client-Facing Port

### 7.1 Port

`lib/crow-protocol/src/ports.rs`: add
`KV_CLIENT_RPC_BASE: u16 = 28201` (stride 1, outside the consensus
range 28101–28300). Add `ServicePort::KvServerClientRpc` variant.
`KV_CLIENT_RPC_BASE - KV_SERVER_GRPC_BASE = 200` is the client-facing
port offset (parallel to R32's `RPC_PORT_OFFSET = 100`).

### 7.2 Server wiring

`kv_server.rs`: add `start_client_rpc_server` (parallel to
`start_rpc_server`) — binds the client-facing crow-rpc port
(`grpc_port + 200`), registers `KvRpcService` handlers, creates a
`KvRpcTransport` for server-side forwards, stores in a new
`client_rpc_server_state` field on `PxKvStore`. `shutdown_server`
stops both crow-rpc servers. `main.rs` calls `start_client_rpc_server`
after `start_rpc_server`.

### 7.3 Cutover

Both tonic + crow-rpc client-facing servers run simultaneously. Clients
switch via `with_rpc_transport`. After all clients migrated, the tonic
`KvServiceServer` is removed in a follow-up commit; `kv.proto` stays as
legacy/reserved.

## Scope

- `lib/crow-protocol/src/fbs/kv_client.fbs` — new schema (10 unary
  pairs + 4 WatchNotify frames + 2 enums).
- `lib/crow-protocol/src/fbs/msg_type.fbs` — +21 entries (1100–1120).
- `lib/crow-protocol/build.rs` — +`kv_client.fbs` codegen.
- `lib/crow-protocol/src/lib.rs` — +`kv_client_generated` +
  `kv_client_fb` re-export.
- `lib/crow-protocol/src/fb_wrappers/` — +`kv_client.rs`, +`pub mod
  kv_client;` + hoist `parse_root` in `fb_wrappers.rs`.
- `lib/crow-protocol/src/ports.rs` — +`KV_CLIENT_RPC_BASE` +
  `KvServerClientRpc` variant.
- `lib/crow-kv/src/rpc/kv_rpc_service.rs` — new server handler.
- `lib/crow-kv/src/rpc.rs` — +module export.
- `lib/crow-kv/src/cluster/watch_registry.rs` — refactor `PushTarget`
  enum (tonic + crow-rpc).
- `lib/crow-kv/src/cluster/kv_server.rs` — +`start_client_rpc_server`
  + `client_rpc_server_state`.
- `lib/crow-kv/src/cluster/px_kv_store.rs` —
  +`client_rpc_server_state` field.
- `lib/crow-kv-client/src/kv_rpc_transport.rs` — new client transport.
- `lib/crow-kv-client/src/client.rs` — +`rpc_transport` field +
  `with_rpc_transport` + per-method transport selection.
- `lib/crow-kv-client/src/watch_notify.rs` — rewrite reader loop
  (persistent connection + client handler).
- `lib/crow-kv-client/src/lib.rs` — +module export.
- `lib/crow-kv-client/src/error.rs` — +`RpcError` mapping helper.
- `app/crow-kv-server/src/main.rs` — +`start_client_rpc_server` call.
- `lib/crow-kv/tests/rpc_migration_test.rs` — +R117 client-facing
  integration tests (or sibling `kv_client_rpc_test.rs`).

## Complexity

**High.** 10 unary RPCs + WatchNotify persistent-connection push +
WatchRegistry mixed-transport refactor + new port + server/client
wiring. The unary RPCs are mechanical (mirror R32's pattern). The
genuinely hard parts: (1) WatchNotify server-push via
`Connection::from_handle` + `RpcClient::send` with lazy dead-watcher
cleanup (no connection-close callback), (2) the `WatchRegistry`
`PushTarget` enum keeping both transports working during rollout, (3)
the `[[ubyte]]` nested-vector flatbuffer encoding for `keys`/`values`,
(4) the client-side handler registration for server-initiated notify
frames. Most infrastructure (FFI helpers, wrapper pattern, handler
pattern, transport pattern, port-derivation) is reused from R32.

## Test Design

### Unit tests (UT)

- **Schema round-trip** (`tests/kv_client_wrappers_test.rs`): build
  each response flatbuffer → parse via `Ref` wrapper → verify every
  accessor. Malformed buffer → `valid() == false`. `not_leader_hint`
  string round-trip. `FBBytes`-wrapped keys/values round-trip
  (`FBWatchNotify`).
- **`FBKvClientRetCode` mapping**: `JournalScanGcGap` distinct from
  `NotLeader`.
- **Port computation**: `KvServerClientRpc.port(i) == 28201 + i`;
  offset `KV_CLIENT_RPC_BASE - KV_SERVER_GRPC_BASE == 200`.
- **`WatchRegistry` mixed push**: register one tonic watcher + one
  crow-rpc watcher on the same prefix; `emit` pushes to both; crow-rpc
  target with dead connection → lazy removal + `closed_watchers` inc.
- **Error mapping**: `RpcError` → `Error` variant table
  (`ConnectionClosed`→retryable, `Timeout`→retryable,
  `RegistrationFailed`→non-retryable).
- **`FBBytes` wrapper**: build `FBWatchNotify` with multiple keys/
  values → parse → verify each key/value matches.

### End-to-end tests (E2E)

`rpc_migration_test.rs` (extend) or `kv_client_rpc_test.rs` (new),
3-node in-process cluster with `start_client_rpc_server` +
`CrowkvClient::with_rpc_transport`:

- **Put/Get/Delete over crow-rpc**: client puts, gets, deletes; verify
  state + response parity with gRPC.
- **BatchWrite over crow-rpc**: batch apply parity.
- **Scan/JournalScan over crow-rpc**: result-set parity + pagination.
- **CreateSnapshot/ListSnapshots/SnapshotScan/ReleaseSnapshot over
  crow-rpc**: parity.
- **NotLeaderHint over crow-rpc**: send to follower →
  `not_leader_hint` → client redirects to leader → succeeds.
- **ConnectionClosed retry**: kill leader mid-call → client retries on
  next connection.
- **WatchNotify over crow-rpc**: client subscribes, writes happen,
  client's handler receives `FBWatchNotify` for matching keys.
- **WatchNotify mid-stream reconnect**: kill connection mid-stream →
  client reconnects, re-subscribes, receives subsequent notifications.
- **WatchNotify NotLeaderHint**: subscribe on follower →
  `FBWatchNotifyError` with hint → client reconnects to leader.
- **Mixed rollout**: gRPC client + crow-rpc client both succeed
  against the same server (both ports up).
- **Leader-forward over crow-rpc**: Get on follower with
  Linearizable → forwarded to leader → response parity; `forwarded`
  loop-guard prevents double-hop.

## Module Structure

```
lib/crow-protocol/src/fbs/kv_client.fbs          # new schema
lib/crow-protocol/src/fbs/msg_type.fbs           # +21 msg types
lib/crow-protocol/build.rs                       # +codegen
lib/crow-protocol/src/lib.rs                     # +kv_client_fb
lib/crow-protocol/src/fb_wrappers/kv_client.rs   # new wrappers
lib/crow-protocol/src/ports.rs                   # +KV_CLIENT_RPC_BASE
lib/crow-kv/src/rpc/kv_rpc_service.rs            # new server handler
lib/crow-kv/src/rpc.rs                           # +export
lib/crow-kv/src/cluster/watch_registry.rs        # PushTarget refactor
lib/crow-kv/src/cluster/kv_server.rs             # +start_client_rpc_server
lib/crow-kv/src/cluster/px_kv_store.rs           # +client_rpc_server_state
lib/crow-kv-client/src/kv_rpc_transport.rs       # new client transport
lib/crow-kv-client/src/client.rs                 # +transport selection
lib/crow-kv-client/src/watch_notify.rs           # rewrite reader loop
lib/crow-kv-client/src/lib.rs                    # +export
lib/crow-kv-client/src/error.rs                  # +RpcError mapping
app/crow-kv-server/src/main.rs                   # +start_client_rpc_server
lib/crow-kv/tests/kv_client_rpc_test.rs          # new E2E tests
lib/crow-protocol/tests/kv_client_wrappers_test.rs # new UT
```

## Config Extensions

None — transport selection is programmatic (`with_rpc_transport`), no
config field added (matches R32/R115 mixed-rollout mechanism).

## Server Wiring

1. `main.rs` `create_and_start_stores`: after `store.start().await`
   (gRPC) and `store.start_rpc_server(rt)` (consensus crow-rpc), call
   `store.start_client_rpc_server(rt)` (client-facing crow-rpc).
2. `start_client_rpc_server` derives the client-facing port
   (`grpc_port + 200`), creates a second `RpcServer`, registers
   `KvRpcService` handlers, creates a `KvRpcTransport` for server-side
   forwards, stores both in `client_rpc_server_state`.
3. `shutdown_server` stops the client-facing crow-rpc server alongside
   the consensus + gRPC servers.

## Open Questions

None — the `fail_all` scoping (todo_fb.md Open Issue) is resolved in
§6.2 (dedicated `RpcClient` per WatchNotifyClient, no FFI change).
