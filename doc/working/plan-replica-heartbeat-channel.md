<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R53 Plan — Separate gRPC Channel for Leader Heartbeats

## Task breakdown

- [ ] T1: Remove E5 reserve from `config.rs`
- [ ] T2: Remove heartbeat path + E5 reserve from `learner_stream.rs`
- [ ] T3: Add dedicated heartbeat channel to `remote_replica.rs`, rewrite `send_heartbeat`
- [ ] T4: Update `design-crow-kv-rpc.md` (§3, §4, §6.3)
- [ ] T5: Run `cargo fmt --check`, `cargo clippy -- -D warnings`, `pixi run test-core`
- [ ] T6: Commit implementation + working docs

## File-level changes

### `lib/crow-kv/src/common/config.rs`
- Remove `pub learner_stream_heartbeat_reserve: usize` field (line 278).
- Remove `learner_stream_heartbeat_reserve: 8` from `DEFAULT` (line 314).
- Remove `learner_stream_heartbeat_reserve: 2` from `for_tests` (line 338).
- Remove `learner_stream_heartbeat_reserve: 4` from `for_e2e` (line 371).
- Remove the field's doc comment block (lines 273-278).

### `lib/crow-kv/src/cluster/learner_stream.rs`
- Module doc: remove `Heartbeat` from the multiplexing description; remove
  invariant 1 (heartbeats cannot reorder ahead of Accept).
- Remove `Heartbeat(HeartbeatResponse)` from `LearnerStreamReply` enum.
- Remove `FrameKind` enum entirely.
- Remove `kind: FrameKind` field from `OutboundCmd`.
- Remove `heartbeat_reserve`, `non_heartbeat_count`, `window` fields from
  `PxLearnerStream`.
- Remove `heartbeat_reserve`, `non_heartbeat_count` from `PxLearnerStream::new`
  (both the struct literal and the `run_learner_stream` call args).
- Remove `pub async fn send_heartbeat` method.
- Simplify `dispatch`: remove the E5 CAS loop and kind check; just
  `self.cmd_tx.try_send(cmd)` with the Full/Closed error mapping.
- Remove `non_heartbeat_count` parameter from `run_learner_stream` and
  the `if cmd.kind != FrameKind::Heartbeat` decrement in the send loop.
- Remove `non_heartbeat_count` parameter from `fail_queued_commands` and
  the `if cmd.kind != FrameKind::Heartbeat` decrement.
- Remove `Heartbeat(r)` branch from `dispatch_response`.
- Remove `HeartbeatRequest`, `HeartbeatResponse` from the `use crate::rpc` import
  if no longer used (check: `dispatch_response` no longer references them).

### `lib/crow-kv/src/cluster/remote_replica.rs`
- Module doc: update — `LearnerStream` now carries `Accept` +
  `ChosenNotification`; heartbeats go via a dedicated unary channel.
- Add `heartbeat_client: OnceCell<Box<PxServiceClient<Channel>>>` field.
- Add `async fn get_heartbeat_client(&self) -> Result<&PxServiceClient<Channel>, tonic::Status>`
  — mirrors `get_client()` with a separate `Endpoint`/channel.
- Rewrite `send_heartbeat`: build `RpcHeartbeatRequest`, call
  `get_heartbeat_client()`, `client.heartbeat(rpc_req)` with
  `tokio::time::timeout(self.rpc_timeout, ...)`, map response to
  `HeartbeatReply`. Mirror `send_prepare`'s timeout/error structure.
- Remove `heartbeat_reserve: usize` field.
- Remove `heartbeat_reserve` from `new()`, `with_config()`.
- Remove `learner_stream_heartbeat_reserve` from the `PxElectionConfig`
  construction in `learner_stream()`.
- `shutdown()`: no explicit change needed — `heartbeat_client` drops on
  `PxRemoteReplica` drop, same as `grpc_client`. The `status()` check
  stays on `grpc_client` (main channel).

### `doc/design/kv/design-crow-kv-rpc.md`
- §3 "Why a Dedicated Bidi Stream": rewrite invariant 1 — heartbeats now
  move to a separate unary channel; document the relaxed FIFO invariant
  and the term-fence safety argument. Update the intro paragraph (now
  carries `Accept` + `Chosen`, not `Heartbeat`).
- §4 "Service Definitions": update the bidi stream bullet —
  `LearnerStream` (steady-state `Accept` + `Chosen` multiplexed); add
  note that `Heartbeat` unary RPC is used for steady-state heartbeats
  over a dedicated channel.
- §6.3 "Heartbeat Reserved Capacity": remove the section entirely (E5 is
  gone). Update ToC to remove the §6.3 entry.

## Test checklist

- [ ] `cargo fmt --check` passes
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `pixi run test-core` passes (includes `g4_learner_stream_test`,
      all election/heartbeat/lease tests)
- [ ] No new clippy warnings from removed fields/imports

## Dependency ordering

T1 (config) → T2 (learner_stream) → T3 (remote_replica) — T2 and T3 both
depend on T1 (the config field removal), and T3 depends on T2 (the
`send_heartbeat` removal from `PxLearnerStream`). T4 (design doc) is
independent and can be done in parallel with the code changes.
