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
tonic/gRPC. The `crow-kv-client` library manages a process-wide
cache of tonic `Channel`s keyed by leader endpoint
(`kv_service.rs` L47). The FFI layer (`crow-kv-client/src/ffi.rs`)
wraps the tonic client for C++ consumers. `WatchNotify` is a
bi-directional stream: the client sends watch registrations, the
server streams change notifications. The h2-lock costs throughput
on concurrent client writes (Put/BatchWrite) sharing a connection.

**Design pointers**: `design-crow-rpc.md` §6 (Flatbuffer Wrapper
Convention), §5 (Schema + Build), §4.4 (Server Side).
`design-crow-kv-rpc.md` (wire protocol, KvService, WatchNotify).
`design-crow-kv-watch-notify.md` (WatchNotify bi-directional stream,
per-group `WatchRegistry`, apply-path trigger). R32 migrates the
internal Paxos path; R117 migrates the client-facing path. R114
(streaming support) is required for `WatchNotify`.

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
  this is a bi-directional stream (R114). The stream is long-lived
  (minutes to hours). Expected: the client receives all
  notifications for matching writes; the safety-net poller covers
  missed notifications (unchanged).

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
10 unary RPCs migrate directly (same pattern as R115). `WatchNotify`
migrates to a crow-rpc bi-directional stream (R114). The `.proto`
schema is converted to `.fbs` (full conversion). Zero-copy wrapper
classes per §6. The FFI C ABI is preserved — only the internal
transport changes. The `NotLeaderHint` error model (already
migrated by R32 on the internal path) is reused.

**One-line summary**: Replace gRPC on the KvService client-facing
path with crow-rpc, converting 10 unary RPCs + 1 bi-directional
stream (`WatchNotify`) to flatbuffer-over-TCP, preserving the FFI
C ABI and protocol semantics.

**Numbered work items**:

1. **Flatbuffer schemas for KvService** (`lib/crow-protocol/src/
   fbs/kv_client.fbs`) — convert `kv.proto` (KvService messages)
   to `.fbs`. Message types: KvSetRequest (Put), KvGetRequest,
   KvDeleteRequest, KvBatchWriteRequest, KvScanRequest,
   KvJournalScanRequest, CreateSnapshotRequest, ListSnapshotsRequest,
   SnapshotScanRequest, ReleaseSnapshotRequest — each with its
   response type. WatchNotifyRequest + WatchNotifyResponse for the
   bi-directional stream. The `NotLeaderHint` payload (leader
   endpoint + membership epoch) is a field in the response table
   (same as R32's consensus responses). Register message type IDs
   in the 1000s range in `msg_type.fbs` (shared with R32's
   consensus messages — coordinate the sub-range split, e.g.
   consensus 1000-1099, client 1100-1199). Files:
   `lib/crow-protocol/src/fbs/kv_client.fbs` (new),
   `lib/crow-protocol/src/fbs/msg_type.fbs`,
   `lib/crow-protocol/build.rs`, `lib/crow-protocol/src/lib.rs`.

2. **Zero-copy wrapper classes** (`lib/crow-protocol/src/
   fb_wrappers/`) — define `FB<Type>Ref` wrappers for KvService
   request/response types per §6. Include `NotLeaderHint` accessor
   on response wrappers. Files:
   `lib/crow-protocol/src/fb_wrappers/kv_client.rs` (new).

3. **Server-side migration** (`lib/crow-kv/src/rpc/kv_service.rs`)
   — replace the tonic `KvService` server with a crow-rpc
   `RpcServer` handler set. Each unary handler dispatches by
   `msg_type` to the existing KV logic (state machine apply, scan,
   snapshot). The `WatchNotify` handler uses R114's
   `StreamHandlerFn` — it receives watch registrations via the
   `StreamReader` and pushes notifications via the `StreamWriter`.
   The crow-rpc server runs alongside the tonic server during
   mixed rollout. Files: `lib/crow-kv/src/rpc/kv_service.rs`
   (rewrite), `app/crow-kv-server/src/main.rs` or equivalent
   server wiring.

4. **Client-side migration** (`lib/crow-kv-client/src/`) — replace
   the tonic `Channel` cache with a crow-rpc `RpcClient` +
   `ConnectionPool`. The `NotLeaderHint` is parsed from the
   flatbuffer response via the wrapper; the existing retry logic
   (`crow-kv-client` retry + topology cache) is unchanged. The
   `WatchNotifyClient` becomes a crow-rpc bi-directional stream
   (R114's `Stream` + `StreamReceiver`). Files:
   `lib/crow-kv-client/src/` (client transport rewrite),
   `lib/crow-kv/src/rpc/kv_service.rs` (client-side types, if
   co-located).

5. **FFI layer preservation** (`lib/crow-kv-client/src/ffi.rs`) —
   the C ABI (`crow_kv_client_*` functions) stays the same. The
   internal transport changes from tonic to crow-rpc, but the FFI
   boundary (C struct layouts, function signatures) is preserved.
   The `grpc_endpoint` parameter name in the FFI (`ffi.rs` L261)
   is renamed to `rpc_endpoint` (mechanical, no ABI change — it's
   a `*const c_char` either way). Files:
   `lib/crow-kv-client/src/ffi.rs`,
   `lib/crow-kv-client/include/crow-kv-client/c_api.h`.

6. **Error model parity** — map crow-rpc `RpcError` to
   `KvClientError` variants (already partially done by R32 for the
   internal path). `ConnectionClosed` → retry on next connection
   (same as gRPC `Unavailable`). `Timeout` → `KvClientError::Timeout`.
   `SendQueueFull` → retry with backoff. `NotLeaderHint` is a
   protocol-level response (carried in `ret_code`), not a transport
   error. WatchNotify stream errors: mid-stream `ConnectionClosed`
  → the `WatchNotifyClient` reconnects and re-registers watches
  (existing behavior, unchanged). Files: `lib/crow-kv-client/src/`.

7. **Mixed rollout + cutover** — same pattern as R115/R116: both
   servers run simultaneously, clients switch via config, gRPC
   server removed in a follow-up commit. `kv.proto` stays as
   legacy/reserved. Files: `app/crow-kv-server/src/` (server
   wiring).

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

WatchNotify (bi-directional stream, R114)

Client                              Server
  │── StreamOpen(WatchNotify) ─────►│  WatchNotifyHandler
  │◄────────── StreamOpenResp ──────│
  │── Frame(watch reg, prefix) ────►│  WatchRegistry::register
  │◄────────── Frame(notify, key) ──│  apply-path trigger → emit
  │   ... (long-lived)              │
  │── StreamClose ─────────────────►│  WatchRegistry::unregister
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

**Dependencies**

- **Depends on**: R104 (crow-rpc — finished), **R114** (streaming
  support — for `WatchNotify`), **R32** (consensus migration —
  validates the `NotLeaderHint` flatbuffer error model on the KV
  path and establishes the `kv_rpc.fbs` schema sub-range). R32
  should land first to validate the KV-specific migration pattern.
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

**Transport parity (streaming)**:
- WatchNotify over crow-rpc: a client registers a watch, writes
  happen, client receives notifications for matching keys.
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
`pixi run cargo test -p crow-kv`, `pixi run cargo test -p
crow-kv-server`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.
