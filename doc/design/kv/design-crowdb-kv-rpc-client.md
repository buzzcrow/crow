<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Client-Facing KV RPC

Depends on: [`design-crowdb-kv-rpc.md`](design-crowdb-kv-rpc.md) §3, §5, §10, §11, §12; [`design-crowdb-rpc.md`](../rpc/design-crowdb-rpc.md) §3, §4, §6
Satisfies: [`design-crowdb-kv.md`](design-crowdb-kv.md) §3, §10.1

The client-facing KV service (Put/Get/Delete/BatchWrite/Scan/
JournalScan/CreateSnapshot/ListSnapshots/SnapshotScan/ReleaseSnapshot +
WatchNotify) uses the **crowdb-rpc flatbuffer transport** — the same
engine as the consensus hot path, but on a separate port and with a
dedicated schema (`kv_client.fbs`). The transport selection is
programmatic (`CrowdbClient::with_rpc_transport`).

## Table of Contents

- [1. Design Principles](#1-design-principles)
- [2. Message Surface](#2-message-surface)
- [3. Flatbuffer Schema (kv_client.fbs)](#3-flatbuffer-schema-kv_clientfbs)
- [4. Port Allocation](#4-port-allocation)
- [5. Server-Side Handler (KvRpcService)](#5-server-side-handler-kvrpcservice)
- [6. Transparent Leader-Forwarding](#6-transparent-leader-forwarding)
- [7. WatchNotify Server-Push](#7-watchnotify-server-push)
- [8. Client-Side Transport (KvRpcTransport)](#8-client-side-transport-kvrpctransport)
- [9. CrowdbClient Transport Selection](#9-crowkvclient-transport-selection)
- [10. WatchNotifyClient](#10-watchnotifyclient)
- [11. Zero-Copy Wrapper Classes](#11-zero-copy-wrapper-classes)
- [12. Error Model](#12-error-model)

---

## 1. Design Principles

- **Same engine, separate port.** The client-facing crowdb-rpc server
  binds `rpc_port + 200` (the `KV_CLIENT_RPC_BASE` offset). The
  consensus crowdb-rpc server binds `rpc_port + 100`. Both run
  simultaneously on the same node.
- **Same retry/topology/NotLeaderHint logic.** The client-side
  transport returns the existing crowdb-rpc response types
  (`KvResponse`, `KvScanResponse`, `KvJournalScanResponse`), so the
  retry loop, topology cache, and `NotLeaderHint` handling in
  `CrowdbClient` are unchanged — only the wire send changes.
- **Fire-and-forget WatchNotify.** The server pushes `FBWatchNotify`
  frames to subscribed clients via `RpcClient::send` (no response
  expected). Dead watchers are lazily removed on the first failed
  push.

## 2. Message Surface

10 unary request/response pairs + 4 WatchNotify frames:

- **Put**: `FBKvSetRequest` → `FBKvResponse`
- **Get**: `FBKvGetRequest` → `FBKvResponse`
- **Delete**: `FBKvDeleteRequest` → `FBKvResponse`
- **BatchWrite**: `FBKvBatchWriteRequest` → `FBKvResponse`
- **Scan**: `FBKvScanRequest` → `FBKvScanResponse`
- **JournalScan**: `FBKvJournalScanRequest` → `FBKvJournalScanResponse`
- **CreateSnapshot**: `FBCreateSnapshotRequest` → `FBCreateSnapshotResponse`
- **ListSnapshots**: `FBListSnapshotsRequest` → `FBListSnapshotsResponse`
- **SnapshotScan**: `FBSnapshotScanRequest` → `FBSnapshotScanResponse`
- **ReleaseSnapshot**: `FBReleaseSnapshotRequest` → `FBReleaseSnapshotResponse`
- **WatchSubscribe**: `FBWatchSubscribe` (fire-and-forget, no response)
- **WatchUnsubscribe**: `FBWatchUnsubscribe` (fire-and-forget, no response)
- **WatchNotify**: `FBWatchNotify` (server→client push, fire-and-forget)
- **WatchNotifyError**: `FBWatchNotifyError` (server→client push, fire-and-forget)

## 3. Flatbuffer Schema (kv_client.fbs)

The schema lives in `lib/crowdb-protocol/src/fbs/kv_client.fbs`. Codegen
runs via `crowdb-protocol`'s `build.rs`, producing `kv_client_generated.rs`
re-exported as `crowdb_protocol::kv_client_fb`.

- **`[[ubyte]]` workaround.** Flatbuffers does not support nested
  vectors (`[[ubyte]]`). The `FBBytes` wrapper table (`{ data: [ubyte] }`)
  is used for byte-vector fields in `FBWatchNotify` (`keys`, `values`).
- **`FBKvClientRetCode` enum.** Maps 1:1 to the `KvErrorCode`:
  `Success`, `NotLeader`, `Unavailable`, `Internal`, `JournalScanGcGap`,
  `InvalidArgument`.
- **`FBReadMode` enum.** `Linearizable` / `MinSlot`, matching the
  `ReadMode`.
- **`forwarded: bool` field.** On `FBKvGetRequest`, `FBKvScanRequest`,
  and `FBKvJournalScanRequest` — the loop-guard for transparent
  leader-forwarding (the `forwarded` field replaces the
  `x-crowdb-kv-forwarded` metadata header).

## 4. Port Allocation

The client-facing crowdb-rpc port is derived from the base port:

```
client_rpc_port = rpc_port + (KV_CLIENT_RPC_BASE - KV_SERVER_RPC_BASE)
                = rpc_port + 200
```

This is parallel to the consensus-side offset (`rpc_port + 100`). The
`KV_CLIENT_RPC_BASE` constant lives in `crowdb-protocol::ports`. The port
mapping is registered as `KvServerClientRpc` in the port-claim registry.

## 5. Server-Side Handler (KvRpcService)

`KvRpcService` (in `lib/crowdb-kv/src/rpc/kv_rpc_service.rs`) holds
`Arc<PxKvStore>` + `Handle` (tokio runtime) + `Arc<KvClientRpcForwarder>`.
`register_handlers` wires one handler per request `msg_type` into the
client-facing `RpcServer`.

Each unary handler:
a. Parse request via `flatbuffers::root::<FB<Type>Request>(req.control)`.
   On parse failure → `submit_error` with `InvalidArgument`.
b. Dispatch to the existing `KvStore` trait methods
   (`kv_put`/`kv_get`/`kv_delete`/`kv_batch_write`/`kv_scan`/
   `kv_journal_scan`/`kv_create_snapshot`/`kv_list_snapshots`/
   `kv_snapshot_scan`/`kv_release_snapshot`) — the same methods the
   crowdb-rpc handler calls. The async path spawns a tokio task via
   `self.rt.spawn` and submits the response from the task.
c. Build the response flatbuffer via `FlatBufferBuilder`, `finish`,
   `submit_response`.

Server startup: `start_client_rpc_server` (in `kv_server.rs`) creates
the `RpcServer`, binds the client-facing port, constructs the
`KvClientRpcForwarder` + `KvRpcService`, registers handlers, and stores
the server state in `PxKvStore::client_rpc_server_state`. `main.rs`
calls it after `start_rpc_server`. `shutdown_server` stops both
crowdb-rpc servers.

## 6. Transparent Leader-Forwarding

`Get`/`Scan`/`JournalScan` preserve the forward step (linearizable
reads only; `MinSlot` served locally). The loop-guard
`x-crowdb-kv-forwarded` metadata header becomes the `forwarded: bool`
field on the request flatbuffer. The handler:

a. If `read_mode == Linearizable && !req.forwarded` and
   `forward_target_for(group_id)` returns `Some(endpoint)` → re-issue
   the request to the leader via `KvClientRpcForwarder`. Set
   `forwarded = true` on the re-issued request. On success, submit the
   leader's response. On forward failure, serve stale local + set
   `not_leader_hint = endpoint`.
b. Else serve locally.

`KvClientRpcForwarder` lives in `crowdb-kv` itself (not `crowdb-kv-client`)
to avoid a crate cycle. It holds an `Arc<RpcServer>` + `Arc<RpcClient>`
+ connection cache, builds the request flatbuffer with
`forwarded = true`, calls `rpc.call()`, and returns the raw response
control buffer.

## 7. WatchNotify Server-Push

`WatchSubscribe` handler:
a. Parse `FBWatchSubscribe` → `group_id`, `prefix`.
b. `store.get_group(group_id)`. If none → push `FBWatchNotifyError`
   via `RpcClient::send` on the inbound connection, error="group not
   found". No `submit_response` (fire-and-forget).
c. If `!group.local_replica().is_leader()` → push
   `FBWatchNotifyError` with `not_leader_hint = group.leader_endpoint()`.
d. Else → build `Connection` via `from_handle`, register it in the
   group's `WatchRegistry` with a `CrowdbRpcPushTarget`
   (`(Connection, Arc<RpcClient>, Arc<RpcServer>)`).

`WatchRegistry` uses a `PushTarget` enum:
- `CrowdbRpc` — `Arc<CrowdbRpcPushTarget>` (crowdb-rpc push path).

`emit` builds `FBWatchNotify` flatbuffer + `rpc.send` (fire-and-
forget). On `ConnectionClosed`/`ConnectionError` → increment
`closed_watchers` (lazy cleanup; the watcher is removed on the next
emit pass or via the safety-net poller).

Connection-close cleanup: crowdb-rpc does not deliver a connection-close
callback to the handler. The `WatchRegistry::emit` path detects a dead
push target when `RpcClient::send` returns `ConnectionClosed`. The
safety-net poller covers missed notifications during the gap.

## 8. Client-Side Transport (KvRpcTransport)

`KvRpcTransport` (in `lib/crowdb-kv-client/src/kv_rpc_transport.rs`)
mirrors `PxRpcTransport`: holds `Arc<RpcServer>` + `Arc<RpcClient>` +
`DashMap<String, Connection>` + `AtomicU64` next_req_id. `conn_for`
derives the client-facing crowdb-rpc port from the base port via the
`KV_CLIENT_RPC_BASE` offset.

Unary methods (`send_put`, `send_get`, `send_delete`, `send_batch_write`,
`send_scan`, `send_journal_scan`): build request flatbuffer →
`rpc.call` → await → parse via `Ref` wrapper → map to the existing
crowdb-rpc response types (`KvResponse`, `KvScanResponse`,
`KvJournalScanResponse`). `NotLeaderHint` parsed from the
`not_leader_hint` string field → fed into `CrowdbClient`'s existing
retry + topology-cache logic.

## 9. CrowdbClient Transport Selection

`CrowdbClient` holds an `Option<Arc<KvRpcTransport>>` field
(`rpc_transport`). `with_rpc_transport(transport)` sets it. Each public
method (`put`/`get`/`delete`/`batch_write`/`scan`/`scan_count`/
`journal_scan`) checks `self.rpc_transport` first: when set, delegate
to the transport's `send_*` (with the existing retry/topology/
`NotLeaderHint`/metrics wrapping — only the wire send changes).

## 10. WatchNotifyClient

`WatchNotifyClient` uses the crowdb-rpc transport path:

- **crowdb-rpc path**: persistent
  connection + client-side handler. The reader loop resolves the leader,
  opens a connection via `KvRpcTransport::get_conn`, registers
  handlers for `FBWatchNotify` + `FBWatchNotifyError` via
  `RpcClient::register_handler`, sends `FBWatchSubscribe` as
  fire-and-forget `send()`. On `FBWatchNotifyError` with non-empty
  `not_leader_hint` → `topology.set_leader` + reconnect signal. A
  periodic liveness check (fire-and-forget ping every 5s) detects
  dead connections — crowdb-rpc does not deliver a connection-close
  callback.

## 11. Zero-Copy Wrapper Classes

`FBKvResponseRef`, `FBKvScanResponseRef`, `FBKvJournalScanResponseRef`,
`FBCreateSnapshotResponseRef`, `FBListSnapshotsResponseRef`,
`FBSnapshotScanResponseRef`, `FBReleaseSnapshotResponseRef`,
`FBWatchNotifyRef` — zero-copy views over the response flatbuffer
control buffer. Each provides null-safe accessors (`valid()`,
`ret_code()`, `error_msg()`, `ok()`, etc.). See
[`design-crowdb-rpc.md`](../rpc/design-crowdb-rpc.md) §6 for the wrapper
convention.

## 12. Error Model

`FBKvClientRetCode` → `KvErrorCode` mapping:

- `Success` → `KvErrorNone`
- `NotLeader` → `KvErrorNotLeader`
- `Unavailable` → `KvErrorUnavailable`
- `JournalScanGcGap` → `KvErrorJournalScanGcGap`
- `Internal` / `InvalidArgument` / unknown → `KvErrorInternal`

`RpcError` → `Error` variants:
- `ConnectionClosed` → retry on next connection (same as crowdb-rpc
  `Unavailable`).
- `Timeout` → `Error::Transport` (caller retry budget).
- `SendQueueFull` → retry with backoff.
- `RegistrationFailed`/`AllDown`/`InvalidArg` → non-retryable
  `Error::Transport`.
- `NotLeaderHint` is a protocol-level response (`not_leader_hint`
  string + `FBKvClientRetCode::NotLeader`), not a transport error —
  the client's retry/topology-cache logic is unchanged.
- `JournalScanGcGap` → `Error::JournalScanGcGap` (caller falls back
  to a full-scan rebuild).

## References

- [`design-crowdb-kv-rpc.md`](design-crowdb-kv-rpc.md) — consensus RPC wire
  protocol (same engine, different port + schema).
- [`design-crowdb-rpc.md`](../rpc/design-crowdb-rpc.md) — RPC engine
  overview (framing, connection pool, handler pattern, wrapper
  convention).
- [`design-crowdb-kv-watch-notify.md`](design-crowdb-kv-watch-notify.md) —
  WatchNotify design (trie, prefix matching, safety-net poller).
