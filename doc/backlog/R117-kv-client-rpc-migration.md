<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R117: kv — KvService (Client-Facing) gRPC → crow-rpc Migration

**Problem**

KvService is the client-facing RPC surface (`kv.proto` L181,
`crow-kv/src/rpc/kv_service.rs`). It exposes Put, Get, Delete,
BatchWrite, Scan, JournalScan, CreateSnapshot, ListSnapshots,
SnapshotScan, ReleaseSnapshot (10 unary RPCs) and WatchNotify (1
bi-directional streaming RPC). Unlike R32 (which migrates the
internal replica-to-replica Paxos path), R117 migrates the
client→server path — the surface that `crow-kv-client` and the FFI
consumers (primarily `crow-diskio`) call.

R32 §"Current behavior" explicitly excludes the client-facing
surface: "The management API (Axum HTTP) and client-facing surface
are separate and unaffected." R117 fills that gap. Without it, the
client→server path stays on gRPC, retaining the h2-lock throughput
loss for client workloads and leaving the `WatchNotify` bi-directional
stream on tonic indefinitely.

**Current behavior + impact**: All 11 KvService RPCs go through
tonic/gRPC. The server-side `KvStoreService` (`kv_service.rs`) is a
tonic `KvService` impl that delegates to `PxKvStore`. Three of the
read RPCs (`Get`, `Scan`, `JournalScan`) perform **transparent
server-side leader-forwarding** for linearizable reads: when this
node is not the group leader and the leader endpoint is known, the
request is re-issued to the leader via a process-wide tonic
`Channel` cache (`forward_channel_cache`, `kv_service.rs` L47) +
`KvServiceClient`, with an `x-crow-kv-forwarded` loop-guard header
that makes the hop at-most-once. `MinSlot` reads are served locally
(no forward). The `crow-kv-client` library (`client.rs`) wraps a
tonic `KvServiceClient` over a `ConnectionPool` of tonic `Channel`s
(`pool.rs`); the FFI layer (`ffi.rs`) wraps `HardwareClient` /
`ServiceRegistryClient`, both of which wrap `CrowkvClient` — so the
FFI reaches KvService through `CrowkvClient`'s tonic transport (the
FFI C ABI does not touch tonic directly). `WatchNotify` is a tonic
bi-directional stream: the client sends watch registrations, the
server streams change notifications. The per-group `WatchRegistry`
(`watch_registry.rs`) holds `mpsc::Sender<Result<WatchNotifyResponse,
tonic::Status>>` per watcher — it is **coupled to tonic** (the
`Watcher` struct stores a tonic-typed sender). The h2-lock costs
throughput on concurrent client writes (Put/BatchWrite) sharing a
connection.

**Design pointers**: `design-crow-rpc.md` §6 (Flatbuffer Wrapper
Convention), §5 (Schema + Build), §4.4 (Server Side).
`design-crow-kv-rpc.md` (R32 consensus wire protocol — R117's
client-facing doc is a sibling, not a subset).
`design-crow-kv-watch-notify.md` (WatchNotify bi-directional stream,
per-group `WatchRegistry`, apply-path trigger). R32 migrates the
internal Paxos path; R117 migrates the client-facing path.

**R32 outputs R117 builds on (post-R32 code state)** — R117 reuses
the patterns R32 established; these are the concrete reference files:

- `lib/crow-protocol/src/fbs/kv_consensus.fbs` — schema conventions:
  every request/response table starts with `id` (request_id) +
  `rpc_create_nano`; every response carries `ret_code` +
  `error_msg`; `NotLeaderHint` is **fields on the response table**
  (R32: `term`/`term_stale`/`membership_epoch`/`epoch_mismatch`), not
  a separate message. `FBKvRetCode` enum (Success/NotFound/
  Unavailable/Internal/InvalidArgument). `FBDedupTag` inline struct.
  R117's client-facing schema follows the same conventions but with
  a **separate ret-code enum** (see Work Item 1).
- `lib/crow-protocol/src/fbs/msg_type.fbs` — R32 registered consensus
  msg types **1000–1017** (Prepare/Promise, Accept/Accepted, PreVote,
  RequestVote, Heartbeat, StepDown, ChosenNotification,
  BatchChosenNotification, FetchGap, SnapshotRequest/Response). R117
  takes **1100–1199** (no overlap; the 1018–1099 gap stays reserved
  for consensus). Note: the R32 doc and todo_fb.md say "R32 takes
  1000–1099" — the actual registered range is 1000–1017.
- `lib/crow-protocol/src/fb_wrappers/kv_consensus.rs` — zero-copy
  `FB<Type>Ref` wrapper pattern: `parse_root` helper, `new(&[u8])`,
  `valid()`, typed accessors reading through the root pointer. R117
  defines `FB<Type>Ref` wrappers for the client-facing responses in a
  new `fb_wrappers/kv_client.rs` (same layout).
- `lib/crow-kv/src/rpc/px_rpc_service.rs` — server handler pattern:
  `PxRpcService::register_handlers` + `make_handler` + `handle_X` +
  `submit_response` (sync error path inline, async success path via
  `rt.spawn`). R117's server handler follows this layout.
- `lib/crow-kv/src/rpc/px_rpc_transport.rs` — client transport
  pattern: `PxRpcTransport` holds `Arc<RpcServer>` + `Arc<RpcClient>` +
  `DashMap<endpoint, Connection>`; `conn_for` derives the crow-rpc port
  from the gRPC port via a fixed offset; `send_X` methods build a
  `FlatBufferBuilder`, `finish`, `Buffer::from_bytes`, `rpc.call`,
  parse the response via the `Ref` wrapper. R117's client transport
  follows this layout.
- `lib/crow-rpc/ffi/src/server.rs` — `Connection::from_handle(raw)`
  (R32 work item 7): lets a server-side handler obtain a `Connection`
  from the raw `conn_handle` in `ServerRequest` and call
  `RpcClient::send`/`call` on it. **Unblocks R117's WatchNotify
  server→client push** (the server pushes notify frames to the
  watcher's connection).
- `lib/crow-rpc/ffi/src/client.rs` — `RpcClient::register_handler(
  msg_type, handler)`: client-side handler for **server-initiated**
  frames (a frame whose `request_id` is not in the client's pending
  map). This is the client half of the WatchNotify server-push model.
- `lib/crow-protocol/src/ports.rs` — `KV_RPC_BASE = 28101` (R32
  consensus port, inter-KV-server only). R117 adds a **separate
  client-facing port** `KV_CLIENT_RPC_BASE` (Work Item 7).
- `lib/crow-kv/src/cluster/kv_server.rs` — `start_rpc_server` /
  `rpc_transport` / `stop_rpc_server` wiring (R32). R117 adds a
  parallel `start_client_rpc_server` for the client-facing port.

**Use scenarios**:

- **Concurrent client writes**: Multiple client threads submit Put
  / BatchWrite / Delete to the same leader over shared connections.
  Under gRPC, concurrent writers on one connection funnel through
  the h2 lock. Under crow-rpc, each call is a framed message on the
  per-connection MPSC queue — no userspace lock. Expected:
  throughput scales with thread:connection ratio.

- **Client scan**: A client issues a Scan or JournalScan request.
  Under crow-rpc, the scan is a unary request-response (the result
  set fits in one response frame; large scans paginate via
  repeated calls, same as gRPC). Expected: no contract change.

- **WatchNotify stream**: A client opens a persistent connection,
  sends watch registrations (prefix patterns), and receives change
  notifications as the KV store applies writes. Under crow-rpc,
  this is a **persistent connection with server→client push** (not
  an R114 bidi `Stream` — see Dependencies): the client sends
  `WatchSubscribe`/`WatchUnsubscribe` as fire-and-forget `send()`
  frames on the connection and registers a client-side handler
  (`RpcClient::register_handler`) for the notify msg_type; the
  server's subscribe handler registers the connection (via
  `Connection::from_handle`) in the group's `WatchRegistry`, and
  the apply path pushes notify frames back over that connection via
  `RpcClient::send`. The connection is long-lived (minutes to
  hours). Expected: the client receives all notifications for
  matching writes; the safety-net poller covers missed
  notifications (unchanged).

- **FFI consumer (crow-diskio)**: The C++ diskio client calls
  KvService via the FFI layer (`crow-kv-client/src/ffi.rs`). After
  migration, the FFI layer wraps the crow-rpc client instead of the
  tonic client. Expected: no FFI API change — the C ABI stays the
  same; only the internal transport changes.

- **Mixed rollout**: A kv-server runs both gRPC and crow-rpc
  servers. gRPC clients connect to the gRPC port; crow-rpc clients
  to the crow-rpc port. After all clients migrated, gRPC server
  removed. Expected: no downtime, no consensus disruption.

**Solution**

Migrate KvService from tonic/gRPC to the R104 `crow-rpc` library.
10 unary RPCs migrate directly (same pattern as R32's consensus
handlers). `WatchNotify` migrates to a **persistent connection with
server→client push** — not an R114 bidi `Stream` (the R114 streaming
primitives are not exposed in the Rust FFI; see Dependencies). The
`.proto` schema is converted to `.fbs` (full conversion). Zero-copy
wrapper classes per §6. The FFI C ABI is preserved — only the
internal transport changes. The client-facing `NotLeaderHint` (a
string leader endpoint) is preserved as a response field; it is
**not** the same model as R32's consensus `NotLeaderHint`
(term/epoch fields) — the two serve different redirect semantics.
The `WatchRegistry` is refactored to support a crow-rpc push target
alongside the tonic sender (mixed rollout).

**One-line summary**: Replace gRPC on the KvService client-facing
path with crow-rpc, converting 10 unary RPCs + the `WatchNotify`
persistent-connection server-push stream to flatbuffer-over-TCP,
preserving the FFI C ABI and protocol semantics.

**Numbered work items**:

1. **Flatbuffer schemas for KvService** (`lib/crow-protocol/src/
   fbs/kv_client.fbs`) — convert `kv.proto` (KvService messages)
   to `.fbs`, following `kv_consensus.fbs` conventions (every
   request/response table starts with `id` + `rpc_create_nano`;
   every response carries `ret_code` + `error_msg`). Message types:
   FBKvSetRequest (Put), FBKvGetRequest, FBKvDeleteRequest,
   FBKvBatchWriteRequest, FBKvScanRequest, FBKvJournalScanRequest,
   FBCreateSnapshotRequest, FBListSnapshotsRequest,
   FBSnapshotScanRequest, FBReleaseSnapshotRequest — each with its
   response table. WatchNotify frames: FBWatchSubscribe,
   FBWatchUnsubscribe (client→server, fire-and-forget `send`),
   FBWatchNotify (server→client push), FBWatchNotifyError
   (server→client push on non-leader/error). The client-facing
   `NotLeaderHint` is a **string `not_leader_hint` field** on the
   response tables (mirroring `kv.proto`'s `KvResponse`/
   `KvScanResponse`/`KvJournalScanResponse`/`CreateSnapshotResponse`)
   — NOT R32's term/epoch field model (different redirect
   semantics: the client retries at the hinted endpoint string via
   its topology cache; it does not reason about Paxos term/epoch).
   Define a **separate `FBKvClientRetCode` enum** in `kv_client.fbs`
   (Success/NotFound/NotLeader/Unavailable/Internal/
   JournalScanGcGap) — do NOT reuse R32's `FBKvRetCode`, which lacks
   `JournalScanGcGap` and whose `NotLeader` is encoded as response
   fields rather than a ret code. The `ReadMode` enum (Linearizable/
   MinSlot) is shared with the proto; define `FBReadMode` in
   `kv_client.fbs` (or in `common_type.fbs` if R32 already defined
   it — check first). Register message type IDs in the **1100–1199**
   sub-range in `msg_type.fbs` (R32 used 1000–1017; the 1018–1099
   gap stays reserved for consensus). Files:
   `lib/crow-protocol/src/fbs/kv_client.fbs` (new),
   `lib/crow-protocol/src/fbs/msg_type.fbs`,
   `lib/crow-protocol/build.rs`, `lib/crow-protocol/src/lib.rs`
   (re-export as `pub mod kv_client_fb`).

2. **Zero-copy wrapper classes** (`lib/crow-protocol/src/
   fb_wrappers/`) — define `FB<Type>Ref` wrappers for the KvService
   response types per §6, following the `fb_wrappers/kv_consensus.rs`
   pattern (`parse_root` helper, `new(&[u8])`, `valid()`, typed
   accessors reading through the root pointer — no per-field copy).
   Include `not_leader_hint()` (returns `Option<&str>`) and
   `error_code()` (`FBKvClientRetCode`) accessors on response
   wrappers. The scan responses' `items`/`ops` vectors are accessed
   via the flatbuffer vector reference (zero-copy `Bytes` conversion
   at the boundary, matching `client.rs`'s existing `GetOutcome`/
   `ScanOutcome`/`JournalOp` which already use `Bytes`). Files:
   `lib/crow-protocol/src/fb_wrappers/kv_client.rs` (new),
   `lib/crow-protocol/src/fb_wrappers/mod.rs` (add module).

3. **Server-side migration** (`lib/crow-kv/src/rpc/`) — add a
   crow-rpc handler set alongside the tonic `KvStoreService`
   (mixed rollout). Follow the `px_rpc_service.rs` pattern
   (`register_handlers` + `make_handler` + `handle_X` +
   `submit_response`; sync error path inline, async success path
   via `rt.spawn`). Each unary handler dispatches by `msg_type` to
   the existing `PxKvStore` logic (`kv_put`/`kv_get`/`kv_delete`/
   `kv_batch_write`/`kv_scan`/`kv_journal_scan`/
   `kv_create_snapshot`/`kv_list_snapshots`/`kv_snapshot_scan`/
   `kv_release_snapshot`) — the same logic bodies as the tonic
   `KvStoreService`. The **transparent leader-forwarding** for
   linearizable `Get`/`Scan`/`JournalScan` is preserved: the
   crow-rpc handler re-issues the request to the leader via the
   crow-rpc client transport (Work Item 4) instead of the tonic
   `forward_kv_get`/`forward_kv_scan`/`forward_kv_journal_scan`
   helpers; the `x-crow-kv-forwarded` loop-guard becomes a field
   on the request flatbuffer (`forwarded: bool`) since crow-rpc
   has no metadata headers. The `WatchNotify` handler uses the
   **persistent-connection + server-push model** (NOT R114's
   `StreamHandlerFn` — see Dependencies): the subscribe handler
   builds a `Connection` via `Connection::from_handle(req.conn_handle)`
   and registers it in the group's `WatchRegistry`; the apply path
   pushes `FBWatchNotify` frames to registered connections via
   `RpcClient::send` (fire-and-forget). The `WatchRegistry`
   (`watch_registry.rs`) is refactored to hold an abstract push
   target — either a tonic `mpsc::Sender` (legacy path) or a
   crow-rpc `(Connection, Arc<RpcClient>, Arc<RpcServer>)` triple
   (new path) — so both transports can coexist during mixed
   rollout. The crow-rpc server runs on the new client-facing port
   (Work Item 7) alongside the tonic server. Files:
   `lib/crow-kv/src/rpc/kv_rpc_service.rs` (new — mirror
   `px_rpc_service.rs` layout), `lib/crow-kv/src/cluster/
   watch_registry.rs` (refactor push target),
   `lib/crow-kv/src/cluster/kv_server.rs` (add
   `start_client_rpc_server`), `app/crow-kv-server/src/startup.rs`
   (wiring).

4. **Client-side migration** (`lib/crow-kv-client/src/`) — add a
   crow-rpc transport alongside the tonic `ConnectionPool` and
   select via a `with_rpc_transport` flag (same mixed-rollout
   mechanism as R32's `PxRemoteReplica::with_rpc_transport`).
   Follow the `px_rpc_transport.rs` pattern: a `KvRpcTransport`
   holding `Arc<RpcServer>` + `Arc<RpcClient>` +
   `DashMap<endpoint, Connection>`, `conn_for` deriving the
   client-facing crow-rpc port from the gRPC port via the
   `KV_CLIENT_RPC_BASE` offset, and `send_put`/`send_get`/.../
   `send_release_snapshot` methods that build a `FlatBufferBuilder`,
   `finish`, `Buffer::from_bytes`, `rpc.call`, and parse the
   response via the `FB<Type>Ref` wrappers. `NotLeaderHint` is
   parsed from the response's `not_leader_hint` string field and
   fed into the existing retry + topology-cache logic (unchanged).
   `CrowkvClient`'s `ConnectionPool` (tonic) stays for the legacy
   path; the new transport is selected per-client. The
   `WatchNotifyClient` (`watch_notify.rs`) becomes a **persistent
   connection with client-side handler**: it opens a connection to
   the leader, registers a client-side handler via
   `RpcClient::register_handler` for the `FBWatchNotify` msg_type
   (forwards notify frames to the user's `mpsc::Receiver<WatchNotify>`),
   and sends `FBWatchSubscribe`/`FBWatchUnsubscribe` as
   fire-and-forget `send()` frames on the same connection. On
   `FBWatchNotifyError` with a non-empty `not_leader_hint`, it
   reconnects to the hinted leader and re-subscribes (existing
   behavior). Files: `lib/crow-kv-client/src/kv_rpc_transport.rs`
   (new), `lib/crow-kv-client/src/client.rs` (transport selection),
   `lib/crow-kv-client/src/watch_notify.rs` (rewrite reader loop),
   `lib/crow-kv-client/src/pool.rs` (unchanged — stays for legacy).

5. **FFI layer preservation** (`lib/crow-kv-client/src/ffi.rs`) —
   the C ABI (`crow_hw_*` / `crow_svc_*` functions) stays the same.
   The FFI wraps `HardwareClient` / `ServiceRegistryClient`, both
   of which wrap `CrowkvClient` — so migrating `CrowkvClient`'s
   transport (Work Item 4) automatically migrates the FFI path; the
   FFI boundary (C struct layouts, function signatures) is
   preserved and the C++ consumer (`crow-diskio`) sees no
   difference. The `grpc_endpoint` → `rpc_endpoint` parameter rename
   is **already done** (todo_fb.md §2 — `ffi.rs` L261 already reads
   `rpc_endpoint`; no ABI change, it is a `*const c_char` either
   way). No FFI source changes required for the migration beyond
   what Work Item 4 does to `CrowkvClient`. Files:
   `lib/crow-kv-client/src/ffi.rs` (no change expected),
   `lib/crow-kv-client/include/crow-kv-client/c_api.h` (no change
   expected). Verify with the existing FFI tests.

6. **Error model parity** — map crow-rpc `RpcError` to
   `KvClientError` variants. Reuse R115's `RpcError::is_retryable()`
   helper (`lib/crow-rpc/ffi/src/server.rs` — **already done**,
   todo_fb.md §1): `ConnectionClosed`/`Timeout`/`SendQueueFull`/
   `ConnectionError` are retryable; `RegistrationFailed`/`AllDown`/
   `InvalidArg` are not. `ConnectionClosed` → retry on next
   connection (same as gRPC `Unavailable`). `Timeout` →
   `KvClientError::Timeout`. `SendQueueFull` → retry with backoff.
   `NotLeaderHint` is a **protocol-level response** (carried in the
   `not_leader_hint` string field + `FBKvClientRetCode::NotLeader`),
   not a transport error — the client's retry/topology-cache logic
   is unchanged. `JournalScanGcGap` maps to
   `FBKvClientRetCode::JournalScanGcGap` (no R32 equivalent — the
   caller falls back to a full-scan rebuild, same as the proto
   `KV_ERROR_JOURNAL_SCAN_GC_GAP`). WatchNotify stream errors:
   mid-stream `ConnectionClosed` → the `WatchNotifyClient`
   reconnects and re-registers watches (existing behavior,
   unchanged); the safety-net poller covers missed notifications.
   **Per-connection `fail_all` scoping** (todo_fb.md Open Issues):
   R114's `fail_all(ConnectionClosed)` fires for ALL pending entries
   on the `request_client_`, not per-connection. WatchNotify with
   multiple watcher connections needs per-connection scoping —
   either a per-connection `RpcClient` or a connection-scoped
   `fail_all`. Flagged as R117's scope; resolve in the design draft.
   Files: `lib/crow-kv-client/src/error.rs`,
   `lib/crow-kv-client/src/client.rs`,
   `lib/crow-kv-client/src/watch_notify.rs`.

7. **Mixed rollout + cutover + client-facing port** — same pattern
   as R32/R115: both tonic and crow-rpc servers run simultaneously,
   clients switch via `with_rpc_transport`, gRPC server removed in
   a follow-up commit. `kv.proto` stays as legacy/reserved. Add a
   **separate client-facing crow-rpc port** `KV_CLIENT_RPC_BASE`
   to `lib/crow-protocol/src/ports.rs` (R32's `KV_RPC_BASE = 28101`
   is consensus-only / inter-KV-server; the client-facing path is
   exposed to outside services — different trust boundary, authz,
   and connection-pool sizing, so it gets its own port, per R32's
   §Resolved Questions). Pick a base outside the consensus range
   (e.g. 28201, stride 1) and document it in `ports.rs`. The
   `KvRpcTransport::conn_for` derives the client-facing crow-rpc
   port from the gRPC port via the `KV_CLIENT_RPC_BASE` offset
   (parallel to R32's `RPC_PORT_OFFSET`). Add a
   `ServicePort::KvServerClientRpc` variant. Files:
   `lib/crow-protocol/src/ports.rs`,
   `app/crow-kv-server/src/startup.rs` (server wiring),
   `lib/crow-kv/src/cluster/kv_server.rs`
   (`start_client_rpc_server`).

**Flow diagram**:

```
Unary RPCs (Put, Get, Delete, BatchWrite, Scan, ...)

crow-kv-client ─┐                  crow-kv-client ─┐
  thread A     ─┼─► tonic ──►       thread A     ─┼─► RpcClient ──► MPSC queue
  thread B     ─┤    (h2 lock)      thread B     ─┤    (no lock)       │
               ┘                   thread C     ─┘                     │
                                                          Writer task
                                                          writev() ──► TCP
                                                                │
                                                                ▼
                                                         Server reader
                                                         dispatch by msg_type
                                                         KvService handler
                                                         → state machine

WatchNotify (persistent connection + server→client push)

Client                              Server
  │── send(Subscribe, prefix) ─────►│  subscribe handler
  │                                  │  Connection::from_handle(conn_handle)
  │                                  │  WatchRegistry::register(conn, prefix)
  │◄────── send(Notify, keys) ───────│  apply-path emit → RpcClient::send(conn)
  │   ... (long-lived connection)    │
  │── send(Unsubscribe, prefix) ────►│  WatchRegistry::unregister
  │   (client-side register_handler │
  │    for the Notify msg_type)      │
  │                                   │
```

**Edge cases at a glance**:

- `NotLeaderHint` with a stale leader hint → client's topology
  cache refresh handles this; no change.
- WatchNotify mid-stream connection drop → `WatchNotifyClient`
  reconnects, re-registers watches, resumes. Safety-net poller
  covers missed notifications during the gap. Same semantics as
  gRPC stream reconnect.
- FFI consumer (crow-diskio) after migration → C ABI unchanged;
  the C++ consumer sees no difference. Verified by the existing
  FFI tests.
- Mixed gRPC + crow-rpc during rollout → both servers run; clients
  switch via config. After all clients migrated, gRPC server
  removed.
- Large scan result → paginated via repeated unary calls (same as
  gRPC); no streaming needed for Scan.
- Server-side leader-forward rewrite → the tonic `forward_kv_get`/
  `forward_kv_scan`/`forward_kv_journal_scan` helpers (which use a
  tonic `KvServiceClient` + `forward_channel_cache`) are replaced by
  crow-rpc client calls. The `x-crow-kv-forwarded` loop-guard
  metadata header (crow-rpc has no metadata) becomes a `forwarded:
  bool` field on the request flatbuffer; the receiving handler
  checks it to skip its own forward step (at-most-one-hop
  guarantee preserved).
- `WatchRegistry` mixed transport → during rollout a group may have
  both tonic-stream watchers and crow-rpc-push watchers registered.
  The refactored `WatchRegistry` holds an abstract push target
  (enum/closure) so `emit` can push to either; a tonic watcher's
  `mpsc::Sender` and a crow-rpc watcher's `(Connection, RpcClient)`
  coexist. After cutover the tonic arm is removed.

**Dependencies**

- **Depends on**: R104 (crow-rpc — finished), **R32** (consensus
  migration — **done**; establishes the `kv_consensus.fbs` schema
  conventions, the `FB<Type>Ref` zero-copy wrapper pattern, the
  `px_rpc_service.rs` server handler pattern, the `px_rpc_transport.rs`
  client transport pattern, the `Connection::from_handle` FFI helper,
  and the `start_rpc_server`/`rpc_transport` wiring that R117 mirrors
  for the client-facing port). R32 registered msg types 1000–1017;
  R117 takes 1100–1199.
- **R114 streaming primitives — NOT used by R117.** R114 is marked
  "finished" in backlog.md, but its bidirectional `Stream`/
  `StreamHandlerFn`/`StreamReceiver` primitives are **not exposed in
  the Rust FFI** (`lib/crow-rpc/ffi/src/` has only `RpcClient::send`/
  `call`/`register_handler` + `RpcServer::register_handler`/
  `submit_response` + `Connection::from_handle`). R117's `WatchNotify`
  is therefore modeled as a **persistent connection with server→client
  push** — the same conclusion R32 reached for `LearnerStream`
  (persistent-connection unary, not R114 bidi). The client sends
  subscribe/unsubscribe as `send()` frames and registers a client-side
  handler (`register_handler`) for the notify msg_type; the server
  pushes notify frames via `RpcClient::send` on a `Connection`
  obtained from `from_handle`. This reuses the existing FFI surface —
  no new streaming primitive is needed. (If a future requirement
  exposes R114's `Stream` in Rust, WatchNotify could be retrofitted,
  but the persistent-connection model is sufficient and simpler.)
- **R32 did NOT migrate `SnapshotService`** — `snapshot_service.rs`
  is still tonic (the consensus new-member snapshot install stream).
  `kv_consensus.fbs` defines `FBSnapshotRequest`/`FBSnapshotResponse`
  (msg types 1016/1017) but they are **unwired/reserved**. This is
  separate from R117's scope: R117 migrates the **client-facing**
  snapshot RPCs (`CreateSnapshot`/`ListSnapshots`/`SnapshotScan`/
  `ReleaseSnapshot` — unary, pin/scan/release a point-in-time L1
  view), not the consensus snapshot install stream. No conflict.
- **Depended on by**: nothing (terminal migration item).

**Acceptance**

**Transport parity (unary)**:
- Put / Get / Delete over crow-rpc produce the same state change
  + response as over gRPC. Integration test (3-node cluster,
  submit via crow-rpc, verify).
- BatchWrite over crow-rpc produces the same batch apply as over
  gRPC. Integration test.
- Scan / JournalScan over crow-rpc return the same result set as
  over gRPC. Integration test.
- CreateSnapshot / ListSnapshots / SnapshotScan / ReleaseSnapshot
  over crow-rpc produce the same result as over gRPC. Integration
  test.

**Transport parity (WatchNotify persistent-connection push)**:
- WatchNotify over crow-rpc: a client registers a watch (sends
  `FBWatchSubscribe`), writes happen, the client's registered
  handler receives `FBWatchNotify` frames for matching keys.
  Integration test.
- WatchNotify mid-stream reconnect: client reconnects, re-registers,
  receives subsequent notifications. Integration test (kill
  connection mid-stream).

**Error model**:
- `NotLeaderHint` over crow-rpc → client redirects to hinted
  leader. Integration test.
- crow-rpc `ConnectionClosed` → client retries on next connection.
  Integration test (kill leader mid-call).
- crow-rpc `Timeout` → client returns `KvClientError::Timeout`.
  Integration test.

**FFI preservation**:
- The FFI C ABI is unchanged — existing C++ consumers (crow-diskio)
  compile + run without modification. Verified by existing FFI
  tests (`pixi run cargo test -p crow-kv-client --test ffi_*`).

**Mixed rollout**:
- A kv-server running both gRPC and crow-rpc: gRPC client connects
  to gRPC port, crow-rpc client to crow-rpc port, both succeed.
  Integration test.

**Zero-copy wrapper**:
- The kv-server handler parses requests via `FB<Type>Ref` wrappers
  (no owned intermediate, no field copy). Verified by code review.

**Test commands**: `pixi run cargo test -p crow-kv-client`,
`pixi run cargo test -p crow-kv --test rpc_migration` (extend the
existing R32 test file or add a sibling `kv_client_rpc_test.rs`),
`pixi run cargo test -p crow-kv-server`,
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.
