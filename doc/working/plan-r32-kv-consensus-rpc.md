<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R32 KV Consensus RPC Migration Plan

Design: `doc/working/design-r32-kv-consensus-rpc.md`.
Backlog: `doc/backlog/R32-kv-custom-rust-rpc.md`.
Goal: migrate the KV internal consensus RPC from tonic/gRPC to
crow-rpc, recovering the ~17% h2-lock throughput loss.

## Phase 1: Schema + Build

- [ ] **1.1 Create `kv_consensus.fbs`**: convert all `pxos.proto`
  messages to flatbuffer tables. Add `FBKvRetCode` enum. Add
  `ret_code` + `error_msg` fields to every response table. Files:
  `lib/crow-protocol/src/fbs/kv_consensus.fbs` (new).
- [ ] **1.2 Extend `msg_type.fbs`**: add 18 msg_type entries (1000–
  1017). Files: `lib/crow-protocol/src/fbs/msg_type.fbs`.
- [ ] **1.3 Update `build.rs`**: add `kv_consensus.fbs` to `fbs_files`
  + new `flatc --rust --gen-all` invocation. Files:
  `lib/crow-protocol/build.rs`.
- [ ] **1.4 Update `lib.rs`**: add `kv_consensus_generated` module +
  `kv_consensus_fb` re-export. Files: `lib/crow-protocol/src/lib.rs`.
- [ ] **1.5 Verify schema compiles**: `pixi run cargo build -p
  crow-protocol`. Fix any flatc/codegen issues.

## Phase 2: Zero-Copy Wrappers

- [ ] **2.1 Create `fb_wrappers` module**: `mod.rs` + register in
  `lib.rs`. Files: `lib/crow-protocol/src/fb_wrappers/mod.rs` (new),
  `lib/crow-protocol/src/lib.rs`.
- [ ] **2.2 Write `kv_consensus.rs` wrappers**: `FBPromiseResponseRef`,
  `FBAcceptedResponseRef`, `FBHeartbeatResponseRef`,
  `FBPreVoteResponseRef`, `FBRequestVoteResponseRef`,
  `FBStepDownResponseRef`, `FBFetchGapResponseRef`,
  `FBSnapshotResponseRef`. Files:
  `lib/crow-protocol/src/fb_wrappers/kv_consensus.rs` (new).
- [ ] **2.3 Write wrapper unit tests**: round-trip build → parse →
  accessor verification for each wrapper. Malformed buffer →
  `valid() == false`. Files:
  `lib/crow-protocol/tests/kv_consensus_wrappers_test.rs` (new).
- [ ] **2.4 Run wrapper tests**: `pixi run test-protocol`.

## Phase 3: FFI Helper + Ports

- [ ] **3.1 Add `Connection::from_handle`**: constructor in
  `lib/crow-rpc/ffi/src/server.rs`. Files:
  `lib/crow-rpc/ffi/src/server.rs`.
- [ ] **3.2 Add `from_handle` test**: construct from valid handle +
  null handle. Files: `lib/crow-rpc/ffi/tests/ffi_request_test.rs`.
- [ ] **3.3 Add `KV_RPC_BASE`**: new port constant + `KvServerRpc`
  enum variant. Files: `lib/crow-protocol/src/ports.rs`.
- [ ] **3.4 Run FFI tests**: `pixi run test-rpc-ffi`.

## Phase 4: Server Handler

- [ ] **4.1 Create `px_rpc_service.rs`**: `PxRpcService` struct +
  `register_handlers` + `make_handler` closure pattern. Files:
  `app/crow-kv-server/src/px_rpc_service.rs` (new),
  `app/crow-kv-server/src/lib.rs`.
- [ ] **4.2 Implement Prepare handler**: parse `FBPrepareRequest` →
  epoch fence → `on_prepare` → build `FBPromiseResponse` →
  `submit_response`. Files: `app/crow-kv-server/src/px_rpc_service.rs`.
- [ ] **4.3 Implement Accept handler**: parse `FBAcceptRequest` →
  extract `FBAcceptedValue` + dedup_tags → epoch fence → `on_accept`
  → record dedup tags → build `FBAcceptedResponse` →
  `submit_response`. Files: `app/crow-kv-server/src/px_rpc_service.rs`.
- [ ] **4.4 Implement election handlers**: PreVote, RequestVote,
  Heartbeat, StepDown — parse → `on_*` → build response →
  `submit_response`. Files:
  `app/crow-kv-server/src/px_rpc_service.rs`.
- [ ] **4.5 Implement ChosenNotification + BatchChosenNotification**:
  parse → ballot-verified apply → NO `submit_response` (fire-and-
  forget). Files: `app/crow-kv-server/src/px_rpc_service.rs`.
- [ ] **4.6 Implement FetchGap handler**: parse → `handle_fetch_gap`
  → build `FBFetchGapResponse` if value found → `submit_response`
  (or silent if no value). Files:
  `app/crow-kv-server/src/px_rpc_service.rs`.
- [ ] **4.7 Implement Snapshot handler**: parse → `snapshot_export`
  → build `FBSnapshotResponse` (control) + full bytes (data) →
  `submit_response`. Files:
  `app/crow-kv-server/src/px_rpc_service.rs`.
- [ ] **4.8 Verify server compiles**: `pixi run cargo build -p
  crow-kv-server`.

## Phase 5: Client Transport

- [ ] **5.1 Create `px_rpc_transport.rs`**: `PxRpcTransport` struct +
  `conn_for` + `next_id`. Files: `lib/crow-kv/src/rpc/px_rpc_transport.rs`
  (new), `lib/crow-kv/src/rpc/mod.rs`.
- [ ] **5.2 Implement unary RPC methods**: `send_prepare`,
  `send_pre_vote`, `send_request_vote`, `send_heartbeat`,
  `send_step_down` — build request → `rpc.call` → await → parse via
  wrapper → map to reply type. Files:
  `lib/crow-kv/src/rpc/px_rpc_transport.rs`.
- [ ] **5.3 Implement `snapshot` method**: build `FBSnapshotRequest`
  → `rpc.call` → await → parse header from control + bytes from data
  → return. Files: `lib/crow-kv/src/rpc/px_rpc_transport.rs`.
- [ ] **5.4 Implement error mapping**: `From<RpcError> for
  PxReplicaError` + response `ret_code` mapping. Files:
  `lib/crow-kv/src/rpc/px_rpc_transport.rs`.
- [ ] **5.5 Verify client compiles**: `pixi run cargo build -p
  crow-kv`.

## Phase 6: LearnerStream Rewrite

- [ ] **6.1 Rewrite `PxLearnerStream`**: replace tonic bidi stream
  with pipelined unary `call()`s on persistent connection. Accept/
  Heartbeat/FetchGap → `call()` + `CallFuture` + `reply_tx`.
  ChosenNotification/BatchChosenNotification → `send()` (fire-and-
  forget). Files: `lib/crow-kv/src/cluster/learner_stream.rs`.
- [ ] **6.2 Update `PxRemoteReplica`**: replace `PxServiceClient<
  Channel>` with `Arc<PxRpcTransport>`. Update `send_prepare`/
  `send_pre_vote`/`send_request_vote`/`send_heartbeat`/
  `send_step_down` to use the transport. Files:
  `lib/crow-kv/src/cluster/remote_replica.rs`.
- [ ] **6.3 Rewrite `join_via_snapshot`**: replace
  `SnapshotServiceClient` with `PxRpcTransport::snapshot()`. Files:
  `lib/crow-kv/src/cluster/group_membership.rs`.
- [ ] **6.4 Verify cluster compiles**: `pixi run cargo build -p
  crow-kv`.

## Phase 7: Server Wiring

- [ ] **7.1 Add `start_rpc_server`**: new method on `Arc<PxKvStore>`.
  Create `RpcServer` + `PxRpcService` + register handlers + listen +
  start. Store handle in `rpc_server_state`. Files:
  `lib/crow-kv/src/cluster/kv_server.rs`,
  `lib/crow-kv/src/cluster/px_kv_store.rs`.
- [ ] **7.2 Update `shutdown_server`**: stop both gRPC + crow-rpc
  servers. Files: `lib/crow-kv/src/cluster/kv_server.rs`.
- [ ] **7.3 Update endpoint propagation**: `set_endpoint` also
  records the crow-rpc endpoint (port offset). Files:
  `lib/crow-kv/src/cluster/remote_replica.rs` or
  `lib/crow-kv/src/cluster/replica.rs`.
- [ ] **7.4 Call `start_rpc_server` from `main.rs`**: after existing
  gRPC `start()`. Files: `app/crow-kv-server/src/main.rs`.
- [ ] **7.5 Verify server starts**: `pixi run cargo build -p
  crow-kv-server`.

## Phase 8: Integration Tests

- [ ] **8.1 Write `rpc_migration_test.rs`**: Prepare/Accept over
  crow-rpc, NotLeaderHint, LearnerStream catch-up, Snapshot join,
  mixed rollout, connection drop, fire-and-forget ChosenNotification,
  FetchGap. Files: `lib/crow-kv/tests/rpc_migration_test.rs` (new).
- [ ] **8.2 Run integration tests**: `pixi run clean-env && pixi run
  test-kv-server`. Fix failures (up to 5 retries).

## Phase 9: Benchmark

- [ ] **9.1 Write `bench-kv-rpc.sh`**: 2T:1C + 1T:1C read bench, gRPC
  baseline vs crow-rpc. Files: `tools/bench-kv-rpc.sh` (new).
- [ ] **9.2 Run benchmark**: verify ~17% loss is recovered at 2T:1C
  + no regression at 1T:1C.

## Phase 10: Quality Gate + Commit

- [ ] **10.1 fmt**: `pixi run cargo fmt --all -- --check`.
- [ ] **10.2 clippy**: `pixi run cargo clippy --all-targets -- -D
  warnings`.
- [ ] **10.3 Run all affected tests**: `pixi run test-protocol`,
  `pixi run test-rpc-ffi`, `pixi run test-kv-core`,
  `pixi run clean-env && pixi run test-kv-server`.
- [ ] **10.4 Commit**: implementation + design draft + plan.

## File List

- `lib/crow-protocol/src/fbs/kv_consensus.fbs` — new schema
- `lib/crow-protocol/src/fbs/msg_type.fbs` — +18 entries
- `lib/crow-protocol/build.rs` — +codegen
- `lib/crow-protocol/src/lib.rs` — +module +re-export
- `lib/crow-protocol/src/fb_wrappers/mod.rs` — new
- `lib/crow-protocol/src/fb_wrappers/kv_consensus.rs` — new
- `lib/crow-protocol/src/ports.rs` — +KV_RPC_BASE
- `lib/crow-protocol/tests/kv_consensus_wrappers_test.rs` — new
- `lib/crow-rpc/ffi/src/server.rs` — +from_handle
- `lib/crow-rpc/ffi/tests/ffi_request_test.rs` — +from_handle test
- `lib/crow-kv/src/rpc/px_rpc_transport.rs` — new
- `lib/crow-kv/src/rpc/mod.rs` — +module
- `lib/crow-kv/src/cluster/learner_stream.rs` — rewrite
- `lib/crow-kv/src/cluster/remote_replica.rs` — modify
- `lib/crow-kv/src/cluster/group_membership.rs` — modify
- `lib/crow-kv/src/cluster/kv_server.rs` — +start_rpc_server
- `lib/crow-kv/src/cluster/px_kv_store.rs` — +rpc_server_state
- `app/crow-kv-server/src/px_rpc_service.rs` — new
- `app/crow-kv-server/src/lib.rs` — +module
- `app/crow-kv-server/src/main.rs` — +start_rpc_server call
- `tools/bench-kv-rpc.sh` — new
- `lib/crow-kv/tests/rpc_migration_test.rs` — new

## Test Checklist

**Unit:**
- [ ] Schema round-trip (18 message types)
- [ ] Zero-copy wrapper accessors (8 wrappers)
- [ ] `Connection::from_handle` (valid + null)
- [ ] Error mapping (`RpcError` → `PxReplicaError`)
- [ ] Port computation (`KvServerRpc`)

**Integration:**
- [ ] Prepare/Accept over crow-rpc (3-node cluster)
- [ ] NotLeaderHint redirect
- [ ] LearnerStream catch-up
- [ ] Snapshot join via crow-rpc
- [ ] Mixed rollout (gRPC + crow-rpc)
- [ ] Connection drop mid-call
- [ ] Fire-and-forget ChosenNotification
- [ ] FetchGap over crow-rpc

**Benchmark:**
- [ ] 2T:1C read throughput (h2-lock loss recovered)
- [ ] 1T:1C read throughput (no regression)
