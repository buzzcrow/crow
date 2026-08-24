<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R117 KvService Client-Facing RPC Migration Plan

Design: `doc/working/design-r117-kv-client-rpc.md`.
Backlog: `doc/backlog/R117-kv-client-rpc-migration.md`.
Goal: migrate the KvService client-facing path (10 unary RPCs +
WatchNotify persistent-connection server-push) from tonic/gRPC to
crow-rpc, preserving the FFI C ABI + protocol semantics.

## Phase 1: Schema + Build

- [ ] **1.1 Create `kv_client.fbs`**: convert `kv.proto` (KvService
  messages + WatchNotify frames) to flatbuffer tables. Add
  `FBKvClientRetCode` + `FBReadMode` enums. Every request/response
  table starts with `id` + `rpc_create_nano`; every response carries
  `ret_code` + `error_msg`; `not_leader_hint` is a string field.
  Requests carry `forwarded: bool` (loop-guard). WatchNotify:
  `FBWatchSubscribe`/`FBWatchUnsubscribe` (client→server),
  `FBWatchNotify` (`keys`/`values` as `[[ubyte]]`)/`FBWatchNotifyError`
  (server→client). Files:
  `lib/crow-protocol/src/fbs/kv_client.fbs` (new).
- [ ] **1.2 Extend `msg_type.fbs`**: add 21 entries (1100–1120).
  Files: `lib/crow-protocol/src/fbs/msg_type.fbs`.
- [ ] **1.3 Update `build.rs`**: add `kv_client.fbs` to `fbs_files` +
  new `flatc --rust --gen-all` invocation. Files:
  `lib/crow-protocol/build.rs`.
- [ ] **1.4 Update `lib.rs`**: add `kv_client_generated` module +
  `kv_client_fb` re-export. Files: `lib/crow-protocol/src/lib.rs`.
- [ ] **1.5 Verify schema compiles**: `pixi run cargo build -p
  crow-protocol`. Fix flatc/codegen issues.

## Phase 2: Zero-Copy Wrappers

- [ ] **2.1 Hoist `parse_root`**: move the `parse_root` helper from
  `kv_consensus.rs` to `fb_wrappers.rs` as `pub(super)`. Files:
  `lib/crow-protocol/src/fb_wrappers.rs`,
  `lib/crow-protocol/src/fb_wrappers/kv_consensus.rs`.
- [ ] **2.2 Register + write `kv_client.rs` wrappers**: add
  `pub mod kv_client;` to `fb_wrappers.rs`. Write `FBKvResponseRef`,
  `FBKvScanResponseRef`, `FBKvJournalScanResponseRef`,
  `FBCreateSnapshotResponseRef`, `FBListSnapshotsResponseRef`,
  `FBSnapshotScanResponseRef`, `FBReleaseSnapshotResponseRef`,
  `FBWatchNotifyRef`, `FBWatchNotifyErrorRef`. Files:
  `lib/crow-protocol/src/fb_wrappers/kv_client.rs` (new),
  `lib/crow-protocol/src/fb_wrappers.rs`.
- [ ] **2.3 Write wrapper unit tests**: round-trip build → parse →
  accessor for each wrapper; malformed → `valid() == false`;
  `[[ubyte]]` keys/values round-trip; `not_leader_hint` string.
  Files: `lib/crow-protocol/tests/kv_client_wrappers_test.rs` (new).
- [ ] **2.4 Run wrapper tests**: `pixi run test-protocol`.

## Phase 3: Ports

- [ ] **3.1 Add `KV_CLIENT_RPC_BASE`**: new port constant `28201` +
  `ServicePort::KvServerClientRpc` variant + stride/base wiring.
  Files: `lib/crow-protocol/src/ports.rs`.
- [ ] **3.2 Verify ports compile**: `pixi run cargo build -p
  crow-protocol`.

## Phase 4: Server Handler

- [ ] **4.1 Create `kv_rpc_service.rs`**: `KvRpcService` struct +
  `register_handlers` + `make_handler` closure pattern (mirror
  `px_rpc_service.rs`). Files: `lib/crow-kv/src/rpc/kv_rpc_service.rs`
  (new), `lib/crow-kv/src/rpc.rs` (+export).
- [ ] **4.2 Implement Put/Get/Delete/BatchWrite handlers**: parse →
  delegate to `PxKvStore::kv_*` → build `FBKvResponse` →
  `submit_response`. Get preserves leader-forward (`forwarded` field
  loop-guard). Files: `lib/crow-kv/src/rpc/kv_rpc_service.rs`.
- [ ] **4.3 Implement Scan/JournalScan handlers**: parse → `kv_scan`/
  `kv_journal_scan` → build `FBKvScanResponse`/`FBKvJournalScanResponse`
  → `submit_response`. Leader-forward preserved. Files: same.
- [ ] **4.4 Implement snapshot handlers**: CreateSnapshot/
  ListSnapshots/SnapshotScan/ReleaseSnapshot → build response →
  `submit_response`. Files: same.
- [ ] **4.5 Implement WatchNotify handlers**: `FBWatchSubscribe` →
  `Connection::from_handle` + `WatchRegistry::subscribe` (crow-rpc
  push target) or push `FBWatchNotifyError`; `FBWatchUnsubscribe` →
  `WatchRegistry::unsubscribe`. Fire-and-forget (no `submit_response`).
  Files: same.
- [ ] **4.6 Verify server compiles**: `pixi run cargo build -p
  crow-kv`.

## Phase 5: WatchRegistry Refactor

- [ ] **5.1 Add `PushTarget` enum**: `Tonic(mpsc::Sender)` +
  `CrowRpc(CrowRpcPushTarget { conn, rpc, server })`. Refactor
  `Watcher` to hold `PushTarget`. Files:
  `lib/crow-kv/src/cluster/watch_registry.rs`.
- [ ] **5.2 Add crow-rpc `subscribe` overload**: accepts
  `CrowRpcPushTarget`, returns `watcher_id`. Files: same.
- [ ] **5.3 Refactor `emit`**: match on `PushTarget`; `Tonic` →
  `try_send` (existing); `CrowRpc` → build `FBWatchNotify` +
  `rpc.send` (fire-and-forget); on `ConnectionClosed`/`ConnectionError`
  → lazy watcher removal + `closed_watchers` inc. Files: same.
- [ ] **5.4 Verify cluster compiles**: `pixi run cargo build -p
  crow-kv`.

## Phase 6: Client Transport

- [ ] **6.1 Create `kv_rpc_transport.rs`**: `KvRpcTransport` struct +
  `conn_for` (derives client-facing port via `KV_CLIENT_RPC_BASE`
  offset) + `next_id`. Files: `lib/crow-kv-client/src/kv_rpc_transport.rs`
  (new), `lib/crow-kv-client/src/lib.rs` (+export).
- [ ] **6.2 Implement unary send methods**: `send_put`, `send_get`,
  `send_delete`, `send_batch_write`, `send_scan`, `send_journal_scan`,
  `send_create_snapshot`, `send_list_snapshots`, `send_snapshot_scan`,
  `send_release_snapshot` — build request → `rpc.call` → await →
  parse via `Ref` wrapper → map to outcome types. Files: same.
- [ ] **6.3 Implement error mapping**: `RpcError` → `Error` variants
  (reuse `is_retryable`); `FBKvClientRetCode::NotLeader` →
  `not_leader_hint` string; `JournalScanGcGap` → `Error::JournalScanGcGap`.
  Files: `lib/crow-kv-client/src/error.rs`, same.
- [ ] **6.4 Verify client compiles**: `pixi run cargo build -p
  crow-kv-client`.

## Phase 7: CrowkvClient Transport Selection

- [ ] **7.1 Add `rpc_transport` field + `with_rpc_transport`**:
  `Option<Arc<KvRpcTransport>>` on `CrowkvClient`. Files:
  `lib/crow-kv-client/src/client.rs`.
- [ ] **7.2 Wire per-method transport selection**: `put`/`get`/
  `delete`/`batch_write`/`scan`/`scan_count`/`journal_scan` check
  `self.rpc_transport.get()` first → delegate to `send_*` (with
  existing retry/topology/`NotLeaderHint`/metrics wrapping); else
  tonic path. Files: same.
- [ ] **7.3 Verify client compiles**: `pixi run cargo build -p
  crow-kv-client`.

## Phase 8: WatchNotifyClient Rewrite

- [ ] **8.1 Rewrite `watch_notify.rs`**: persistent connection +
  client-side handler. Dedicated `RpcClient` (§6.2 `fail_all`
  scoping). `register_handler` for `EWatchNotify`/`EWatchNotifyError`;
  `send()` `FBWatchSubscribe`; reconnect loop re-registers +
  re-subscribes. Files: `lib/crow-kv-client/src/watch_notify.rs`.
- [ ] **8.2 Verify client compiles**: `pixi run cargo build -p
  crow-kv-client`.

## Phase 9: Server Wiring

- [ ] **9.1 Add `client_rpc_server_state`**: new field on `PxKvStore`
  (parallel to `rpc_server_state`). Files:
  `lib/crow-kv/src/cluster/px_kv_store.rs`.
- [ ] **9.2 Add `start_client_rpc_server`**: bind client-facing port
  (`grpc_port + 200`), register `KvRpcService` handlers, create
  `KvRpcTransport` for server-side forwards, store in
  `client_rpc_server_state`. Files:
  `lib/crow-kv/src/cluster/kv_server.rs`.
- [ ] **9.3 Update `shutdown_server`**: stop client-facing crow-rpc
  server too. Files: `lib/crow-kv/src/cluster/kv_server.rs`.
- [ ] **9.4 Call from `main.rs`**: after `start_rpc_server`. Files:
  `app/crow-kv-server/src/main.rs`.
- [ ] **9.5 Verify server starts**: `pixi run cargo build -p
  crow-kv-server`.

## Phase 10: Integration Tests

- [ ] **10.1 Write `kv_client_rpc_test.rs`**: 3-node in-process cluster
  with `start_client_rpc_server` + `CrowkvClient::with_rpc_transport`.
  Tests: Put/Get/Delete/BatchWrite/Scan/JournalScan/snapshot RPCs over
  crow-rpc; NotLeaderHint redirect; ConnectionClosed retry;
  WatchNotify subscribe + notify; WatchNotify mid-stream reconnect;
  WatchNotify NotLeaderHint; mixed rollout (gRPC + crow-rpc);
  leader-forward. Files: `lib/crow-kv/tests/kv_client_rpc_test.rs`
  (new).
- [ ] **10.2 Run integration tests**: `pixi run clean-env && pixi run
  test-kv-server`. Fix failures (up to 5 retries).
- [ ] **10.3 Run client tests**: `pixi run test-kv-client`.

## Phase 11: Quality Gate + Commit

- [ ] **11.1 fmt**: `pixi run cargo fmt --all -- --check`.
- [ ] **11.2 clippy**: `pixi run cargo clippy --all-targets -- -D
  warnings`.
- [ ] **11.3 Run all affected tests**: `pixi run test-protocol`,
  `pixi run test-kv-core`, `pixi run test-kv-client`,
  `pixi run clean-env && pixi run test-kv-server`.
- [ ] **11.4 Commit**: implementation + design draft + plan.

## File List

- `lib/crow-protocol/src/fbs/kv_client.fbs` — new schema
- `lib/crow-protocol/src/fbs/msg_type.fbs` — +21 entries
- `lib/crow-protocol/build.rs` — +codegen
- `lib/crow-protocol/src/lib.rs` — +kv_client_fb
- `lib/crow-protocol/src/fb_wrappers.rs` — +kv_client module + parse_root hoist
- `lib/crow-protocol/src/fb_wrappers/kv_client.rs` — new wrappers
- `lib/crow-protocol/src/fb_wrappers/kv_consensus.rs` — parse_root hoist
- `lib/crow-protocol/src/ports.rs` — +KV_CLIENT_RPC_BASE
- `lib/crow-protocol/tests/kv_client_wrappers_test.rs` — new UT
- `lib/crow-kv/src/rpc/kv_rpc_service.rs` — new server handler
- `lib/crow-kv/src/rpc.rs` — +export
- `lib/crow-kv/src/cluster/watch_registry.rs` — PushTarget refactor
- `lib/crow-kv/src/cluster/kv_server.rs` — +start_client_rpc_server
- `lib/crow-kv/src/cluster/px_kv_store.rs` — +client_rpc_server_state
- `lib/crow-kv-client/src/kv_rpc_transport.rs` — new client transport
- `lib/crow-kv-client/src/client.rs` — +transport selection
- `lib/crow-kv-client/src/watch_notify.rs` — rewrite reader loop
- `lib/crow-kv-client/src/lib.rs` — +export
- `lib/crow-kv-client/src/error.rs` — +RpcError mapping
- `app/crow-kv-server/src/main.rs` — +start_client_rpc_server call
- `lib/crow-kv/tests/kv_client_rpc_test.rs` — new E2E tests

## Test Checklist

**Unit:**
- [ ] Schema round-trip (10 unary response types + 2 WatchNotify frames)
- [ ] Zero-copy wrapper accessors (9 wrappers)
- [ ] `[[ubyte]]` keys/values round-trip
- [ ] `FBKvClientRetCode` mapping (JournalScanGcGap vs NotLeader)
- [ ] Port computation (KvServerClientRpc, offset 200)
- [ ] `WatchRegistry` mixed push (tonic + crow-rpc, lazy dead-watcher removal)
- [ ] Error mapping (RpcError → Error variants)

**Integration:**
- [ ] Put/Get/Delete over crow-rpc (3-node cluster)
- [ ] BatchWrite over crow-rpc
- [ ] Scan/JournalScan over crow-rpc
- [ ] CreateSnapshot/ListSnapshots/SnapshotScan/ReleaseSnapshot over crow-rpc
- [ ] NotLeaderHint redirect over crow-rpc
- [ ] ConnectionClosed retry (kill leader mid-call)
- [ ] WatchNotify subscribe + notify over crow-rpc
- [ ] WatchNotify mid-stream reconnect
- [ ] WatchNotify NotLeaderHint
- [ ] Mixed rollout (gRPC + crow-rpc both succeed)
- [ ] Leader-forward over crow-rpc (forwarded loop-guard)
