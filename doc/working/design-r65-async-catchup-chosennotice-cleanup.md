<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: R65 — Apply Correctness Fix + ChosenNotice Out-of-order Apply + Async Catch-up + Snapshot Fallback

**Reference**: `doc/backlog/R65-kv-async-catchup-chosennotice-cleanup.md`
(full problem statement, solution, alternatives, scope, metrics, acceptance criteria)

## Problem Summary

Four issues in the leader→follower replication and catch-up path:

1. **Correctness bug**: `handle_accept_inner` advances `known_commit_slot` on
   Accept, causing followers to apply un-chosen values (leader may crash before
   quorum). Violates Paxos safety — same class of bug as applying before
   `leaderCommit` in Raft.
2. **ChosenNotice doesn't trigger apply** and lacks ballot verification —
   follower can't safely apply chosen slots it already has (may have stale
   lower-ballot value).
3. **Catch-up replay blocks heartbeat round** — synchronous inline catch-up
   in `run_heartbeat_round` (up to 64 `send_accept().await` per lagging
   follower) can exceed `heartbeat_interval`, triggering spurious elections.
4. **No snapshot fallback** for severely lagging followers.

## Proposed Approach

### Change 1: Accept path stores only (correctness fix)

- `handle_accept_inner` (`px_service.rs:610-627`): remove
  `update_chosen_frontier`, `advance_known_commit_slot`, `wake_apply_loop`.
  Accept path becomes `on_accept` only (store to acceptor + WAL persist).
- `run_bulk_phase1` (`group_election.rs:184-297`): after sweep completes,
  add `replica.advance_known_commit_slot(replica.contiguous_chosen())` +
  `replica.wake_apply_loop()`. This fixes the R63 follower→leader transition
  deadlock that motivated the Accept-path advancement.

### Change 2: ChosenNotice ballot-verified out-of-order apply

- **Proto**: add `ballot_round: u64` to `ChosenNotification` in `pxos.proto`.
- `fan_out_chosen_notice` (`group.rs:2542`): include `entry.ballot.round` in
  the `RpcChosenNotification`.
- `send_chosen_notice` (`remote_replica.rs:541`): add `ballot_round` param.
- **ChosenNotice handler** (`px_service.rs:436-465`): replace `note_chosen`
  with ballot-verified logic:
  - `accepted.ballot == chosen_ballot` → `update_chosen_frontier` +
    `wake_apply_loop` (apply now, out-of-order OK)
  - `accepted.ballot < chosen_ballot` (stale) → record gap → FetchGap
  - `accepted_at(slot).is_none()` → `note_chosen` (high-water mark) →
    record gap → FetchGap
- **BatchChosenNotice handler** (`px_service.rs:466-511`): same ballot
  verification per slot. Add `ballot_round` to `BatchChosenNotification`
  proto (single round for the whole range — leader's current ballot).
- **Apply loop** (`local_replica.rs:1683`): change target to
  `max(known_commit_slot, last_chosen_slot)`. Add chosen-ness check:
  only apply if slot ≤ `contiguous_chosen` OR in learner's out-of-order set.
  Skip accepted-but-not-chosen slots. Record gaps for FetchGap.
- **Learner** (`learner.rs`): add `is_chosen(slot) -> bool` query.

### Change 3: Follower-driven FetchGap catch-up

- **Proto**: add `FetchGapRequest` (slot, term, group_id, leader_id) +
  `fetch_gap` frame in `LearnerStreamRequest` oneof. Add `FetchGapResponse`
  (slot, term, ballot_round, leader_id, payload, group_id) +
  `fetch_gap_reply` frame in `LearnerStreamResponse` oneof.
- **Follower-side** (`local_replica.rs`):
  - Gap detection: ChosenNotice for missing/stale slot, or apply loop finds
    missing slot in committed range.
  - `send_fetch_gap` method: send FetchGap frame via LearnerStream, await
    reply. Bounded by `MAX_INFLIGHT_FETCHGAP` (default 16).
  - On reply: overwrite stale/missing value in acceptor with chosen value,
    `update_chosen_frontier` + `wake_apply_loop`.
  - Snapshot threshold: if gap count > `catchup_snapshot_threshold` (default
    `bulk_prepare_window` = 1024), request snapshot install instead.
- **Leader-side** (`px_service.rs` FetchGap frame handler + `group.rs`):
  - Leader has value → reply with full entry (payload + ballot + term).
  - Leader doesn't have value → run classic Paxos (`repair_once`) to
    resolve slot, then reply with resolved value (or NoOp).
- **`remote_replica.rs`**: add `send_fetch_gap` wrapper.
- **Strip catch-up from `run_heartbeat_round`** (`group_election.rs:527-658`):
  remove `send_batch_chosen_notice` + full-accept catch-up loop. Heartbeat
  round becomes pure liveness + lease (one RTT, no catch-up).

### Change 4: Snapshot fallback (folded into change 3)

- Follower detects gap count > `catchup_snapshot_threshold` → requests
  snapshot install via existing mechanism (`design-crow-kv-state-machine.md`
  §6.4). After install, `contiguous_applied` jumps to snapshot slot.

### Change 5: Metrics

- `ReplicationRegistryHandles` on `PxLocalReplica` (follower-side counters +
  gauges) and `LeaderReplicationHandles` on `PxGroup` (leader-side counters).
- Prefix: `s.{store_id}.g.{group_id}.r.{replica_id}{role_tag}.paxos.{metric}.{suffix}`
- Counters: stale_ballot, missing_value, fetchgap.sent/received/
  leader_has_value/leader_classic_paxos/noop_filled, snapshot.
  catchup_triggered, bulk_phase1.sweep_slots/noop_filled/value_recovered,
  apply.skipped_not_chosen.
- Gauges: gap_count, fetchgap.inflight, last_chosen_slot, known_commit_slot.

### Change 6: AsyncCrowtree cleanup

- Remove `AsyncCrowtree::apply_put`, `apply_delete`, `put`, `del`
  (`crow-tree/ffi/src/lib.rs:1574-1602`).
- Update 19 call sites in `ffi_test.rs` (18) + `async_get_bench.rs` (1) to
  use synchronous `Crowtree` API via `handle()`.

## Alternatives Considered

See R65 requirement doc §"Alternatives considered" (A–G). All rejected with
rationale. Key rejections: Accept-path apply with chosen flag (A — leader
doesn't know chosen-ness at send time), heartbeat-only apply (B — loses
parallel-slot advantage), `select!` arm catch-up (C — still couples to
heartbeat task), dedicated runtime (D — `Runtime::drop` is blocking, breaks
`tokio::time::pause()` in tests).

## Acceptance Criteria

See R65 requirement doc §acceptance criteria (lines 756-803). Key:
- `handle_accept_inner` does NOT advance `known_commit_slot` or wake apply.
- Follower applies on ChosenNotice when accepted ballot == chosen ballot.
- Follower does NOT apply on stale ballot (lower) → FetchGap instead.
- Out-of-order apply: slot 5 applied before slot 3 resolved.
- Follower applies on heartbeat `committed_safe_slot`.
- Accepted-but-not-chosen slot is NOT applied.
- FetchGap for missing/stale slots; leader replies with chosen value.
- New leader advances `known_commit_slot` after bulk Phase 1.
- `run_heartbeat_round` does not call `send_accept` or
  `send_batch_chosen_notice`.
- Catch-up is follower-driven (no `run_catchup_loop`, no `peer_state`).
- FetchGap bounded by `MAX_INFLIGHT_FETCHGAP`.
- Snapshot fallback at gap-count threshold.
- Heartbeat round latency ≤ one RPC round-trip regardless of follower lag.
- All existing `crow-kv` tests pass.
- New tests for each acceptance criterion above.
