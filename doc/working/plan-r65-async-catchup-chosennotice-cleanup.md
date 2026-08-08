<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: R65 — Apply Correctness Fix + ChosenNotice Out-of-order Apply + Async Catch-up + Snapshot Fallback

**Reference**: `doc/backlog/R65-kv-async-catchup-chosennotice-cleanup.md`
**Design**: `doc/working/design-r65-async-catchup-chosennotice-cleanup.md`

## Implementation Phases

Each phase ends with running the full test suite to catch regressions early.
Tests are run with `pixi run clean-env && pixi run test-kv-core` and
`pixi run clean-env && pixi run test-kv-server` (the two affected crates).
At major milestones, run ALL test commands.

---

## Phase 1: Proto Changes (additive, no behavior change)

- [x] 1.1 Add `ballot_round: u64` field to `ChosenNotification` in `pxos.proto`
- [x] 1.2 Add `ballot_round: u64` field to `BatchChosenNotification` in `pxos.proto`
- [x] 1.3 Add `FetchGapRequest` message (slot, term, group_id, leader_id) to `pxos.proto`
- [x] 1.4 Add `FetchGapResponse` message (slot, term, ballot_round, leader_id, payload, group_id) to `pxos.proto`
- [x] 1.5 Add `fetch_gap` frame to `LearnerStreamRequest` oneof
- [x] 1.6 Add `fetch_gap_reply` frame to `LearnerStreamResponse` oneof
- [x] 1.7 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings` — verify proto compiles
- [x] 1.8 Run `pixi run clean-env && pixi run test-kv-core` — verify no regression (647 tests pass)

## Phase 2: Accept Path Correctness Fix (Change 1)

- [ ] 2.1 Remove `update_chosen_frontier`, `advance_known_commit_slot`, `wake_apply_loop` from `handle_accept_inner` (`px_service.rs:610-627`). Keep `record_dedup_tags` (dedup is still needed). Accept path becomes `on_accept` only.
- [ ] 2.2 Add `known_commit_slot = max(known_commit_slot, contiguous_chosen)` + `wake_apply_loop()` at end of `run_bulk_phase1` (`group_election.rs`, after line 294). This fixes the follower→leader transition that R63's Accept-path advancement was handling.
- [ ] 2.3 Update `handle_accept_inner` comment to explain the new apply discipline (Accept = store only, apply driven by ChosenNotice + heartbeat).
- [ ] 2.4 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 2.5 Run `pixi run clean-env && pixi run test-kv-core` — expect some test changes needed (tests that relied on Accept-path apply)
- [ ] 2.6 Fix any test failures from 2.5 (tests that assumed follower applies on Accept)
- [ ] 2.7 Run `pixi run clean-env && pixi run test-kv-server` — verify server tests pass

## Phase 3: ChosenNotice Ballot Verification + Out-of-order Apply (Change 2)

- [ ] 3.1 Update `fan_out_chosen_notice` (`group.rs:2542`) to include `entry.ballot.round` in `RpcChosenNotification`
- [ ] 3.2 Update `send_chosen_notice` (`remote_replica.rs:541`) to accept and pass `ballot_round` parameter
- [ ] 3.3 Update all `send_chosen_notice` call sites to pass `entry.ballot.round`
- [ ] 3.4 Update `send_batch_chosen_notice` (`remote_replica.rs:568`) to accept and pass `ballot_round`
- [ ] 3.5 Add `is_chosen(slot) -> bool` to `PxLearner` (`learner.rs`) — checks `contiguous_chosen` or out-of-order set
- [ ] 3.6 Rewrite ChosenNotice handler (`px_service.rs:436-465`): ballot-verified apply logic (match → apply, stale → gap, missing → gap)
- [ ] 3.7 Rewrite BatchChosenNotice handler (`px_service.rs:466-511`): same ballot verification per slot
- [ ] 3.8 Update apply loop (`local_replica.rs:1683`): change target to `max(known_commit_slot, last_chosen_slot)`, add chosen-ness check before applying, record gaps for FetchGap
- [ ] 3.9 Add gap tracking infrastructure to `PxLocalReplica` (gap set, FetchGap inflight counter)
- [ ] 3.10 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 3.11 Run `pixi run clean-env && pixi run test-kv-core` — fix regressions
- [ ] 3.12 Run `pixi run clean-env && pixi run test-kv-server` — fix regressions

## Phase 4: Follower-driven FetchGap Catch-up (Change 3, part A — follower side)

- [ ] 4.1 Add `send_fetch_gap` method to `PxRemoteReplica` (`remote_replica.rs`) — sends FetchGap frame via LearnerStream, awaits reply
- [ ] 4.2 Add `FetchGapRequest`/`FetchGapResponse` frame handling to `PxLearnerStream` (`learner_stream.rs`)
- [ ] 4.3 Add follower-side FetchGap sending logic to `PxLocalReplica` (`local_replica.rs`): detect gap → send FetchGap → on reply, overwrite stale value in acceptor + `update_chosen_frontier` + `wake_apply_loop`
- [ ] 4.4 Add `MAX_INFLIGHT_FETCHGAP` bound (default 16) to limit outstanding FetchGap requests
- [ ] 4.5 Wire gap detection → FetchGap sending in the ChosenNotice handler and apply loop
- [ ] 4.6 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 4.7 Run `pixi run clean-env && pixi run test-kv-core` — fix regressions

## Phase 5: Leader-side FetchGap Handler (Change 3, part B — leader side)

- [ ] 5.1 Add FetchGap frame handler to LearnerStream bidi handler (`px_service.rs`): parse `FetchGapRequest`, delegate to group
- [ ] 5.2 Add `handle_fetch_gap` method to `PxGroup` (`group.rs`): look up slot in acceptor → reply with value, or trigger `repair_once` → reply with resolved value
- [ ] 5.3 Build `FetchGapResponse` proto message with chosen value + ballot + term
- [ ] 5.4 Handle edge cases: leader itself has gap, slot not yet chosen, NoOp fill
- [ ] 5.5 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 5.6 Run `pixi run clean-env && pixi run test-kv-core` — fix regressions
- [ ] 5.7 Run `pixi run clean-env && pixi run test-kv-server` — fix regressions

## Phase 6: Strip Catch-up from Heartbeat Round (Change 3, part C)

- [ ] 6.1 Remove `send_batch_chosen_notice` + full-accept catch-up loop from `run_heartbeat_round` (`group_election.rs:527-658`)
- [ ] 6.2 Heartbeat round becomes: send heartbeats, collect replies, check higher term, renew lease, note peer applied — no catch-up
- [ ] 6.3 Remove or retain `send_batch_chosen_notice` method (keep for now, may be used by FetchGap path)
- [ ] 6.4 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 6.5 Run `pixi run clean-env && pixi run test-kv-core` — fix regressions
- [ ] 6.6 Run `pixi run clean-env && pixi run test-kv-server` — fix regressions

## Phase 7: Snapshot Fallback (Change 4)

- [ ] 7.1 Add `catchup_snapshot_threshold` config (default: `bulk_prepare_window` = 1024) to `PxElectionConfig`
- [ ] 7.2 Add gap count check in follower gap-detection logic: if gap count > threshold → request snapshot install
- [ ] 7.3 Wire snapshot install request via existing snapshot mechanism
- [ ] 7.4 After snapshot install, skip FetchGap until snapshot completes, then resume normal operation
- [ ] 7.5 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 7.6 Run `pixi run clean-env && pixi run test-kv-core` — fix regressions

## Phase 8: Metrics (Change 5)

- [ ] 8.1 Add `ReplicationRegistryHandles` struct to `local_replica.rs` (follower-side counters + gauges)
- [ ] 8.2 Add `LeaderReplicationHandles` struct to `group.rs` (leader-side counters)
- [ ] 8.3 Register handles via `set_metrics_registry` with `r.{replica_id}{role_tag}` prefix
- [ ] 8.4 Increment counters at each branch point: ChosenNotice handler (stale_ballot, missing_value), FetchGap send/receive, apply loop skip, snapshot trigger, bulk Phase 1 sweep/noop/value_recovered
- [ ] 8.5 Update gauges: gap_count, fetchgap.inflight, last_chosen_slot, known_commit_slot
- [ ] 8.6 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 8.7 Run `pixi run clean-env && pixi run test-kv-core` — fix regressions

## Phase 9: AsyncCrowtree Cleanup (mechanical, independent)

- [ ] 9.1 Remove `AsyncCrowtree::apply_put`, `apply_delete`, `put`, `del` from `crow-tree/ffi/src/lib.rs` (lines 1574-1602)
- [ ] 9.2 Update 18 call sites in `ffi_test.rs` to use synchronous `Crowtree` API via `handle()`
- [ ] 9.3 Update 1 call site in `async_get_bench.rs` to use synchronous API
- [ ] 9.4 Run `pixi run cargo fmt --check && pixi run cargo clippy -- -D warnings`
- [ ] 9.5 Run `pixi run clean-env && pixi run test-tree-ffi` — verify FFI tests pass

## Phase 10: New Tests

- [ ] 10.1 Test: Follower does NOT apply on Accept (value in acceptor, not engine)
- [ ] 10.2 Test: Follower applies on ChosenNotice when accepted ballot == chosen ballot
- [ ] 10.3 Test: Follower does NOT apply on ChosenNotice when accepted ballot < chosen ballot (stale → FetchGap)
- [ ] 10.4 Test: Out-of-order apply (slot 5 applied before slot 3 resolved)
- [ ] 10.5 Test: Follower applies on heartbeat `committed_safe_slot`
- [ ] 10.6 Test: Accepted-but-not-chosen slot is NOT applied (chosen-ness check)
- [ ] 10.7 Test: Follower sends FetchGap for missing slot; leader replies with value
- [ ] 10.8 Test: Follower sends FetchGap for stale slot; leader replies with chosen value; follower overwrites + applies
- [ ] 10.9 Test: Leader resolves own gap via classic Paxos then replies to FetchGap
- [ ] 10.10 Test: New leader applies accepted-as-follower slots after bulk Phase 1
- [ ] 10.11 Test: Heartbeat not delayed when follower lags by 1000+ slots
- [ ] 10.12 Test: Snapshot fallback triggers when gap count exceeds threshold
- [ ] 10.13 Test: Large-value (1 MB) FetchGap does not block heartbeat delivery
- [ ] 10.14 Test: Metric counter assertions (stale_ballot, missing_value, fetchgap.sent == stale + missing, etc.)
- [ ] 10.15 Run `pixi run clean-env && pixi run test-kv-core` — all new tests pass
- [ ] 10.16 Run `pixi run clean-env && pixi run test-kv-server` — all tests pass

## Phase 11: Full Test Suite + R64 Cleanup

- [ ] 11.1 Run ALL test commands:
  - `pixi run clean-env && pixi run test-tree-ct`
  - `pixi run clean-env && pixi run test-tree-ffi`
  - `pixi run clean-env && pixi run test-kv-core`
  - `pixi run clean-env && pixi run test-kv-server`
  - `pixi run clean-env && pixi run test-console-cli`
  - `pixi run clean-env && pixi run test-console-server`
  - `pixi run clean-env && pixi run test-console-ui`
- [ ] 11.2 Fix any remaining regressions
- [ ] 11.3 Mark R64 as superseded in `backlog.md` (update or remove its entry)
- [ ] 11.4 Commit implementation (code + tests + design + plan docs)

## Phase 12: Merge Design + Cleanup

- [ ] 12.1 Fold design doc into formal design doc (`design-crow-kv-state-machine.md` or appropriate)
- [ ] 12.2 Delete `doc/working/design-r65-*.md` and `doc/working/plan-r65-*.md`
- [ ] 12.3 Delete `doc/backlog/R65-kv-async-catchup-chosennotice-cleanup.md`
- [ ] 12.4 Remove R65 entry from `doc/backlog/backlog.md` Item Index
- [ ] 12.5 Commit cleanup (second and final commit)

## Phase 13: Local CI Check

- [ ] 13.1 `pixi run cargo fmt --all -- --check`
- [ ] 13.2 `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] 13.3 `pixi run clang-format --dry-run --Werror` (changed `.cpp`/`.h`)
- [ ] 13.4 All test commands (each separately, all must pass)
- [ ] 13.5 `tree-lint` (clang-tidy, changed C++)

---

## File Impact Summary

- `lib/crow-kv/src/rpc/proto/pxos.proto` — proto changes (ballot_round, FetchGap)
- `lib/crow-kv/src/rpc/px_service.rs` — Accept path fix, ChosenNotice/BatchChosen handler rewrite, FetchGap handler
- `lib/crow-kv/src/cluster/group.rs` — fan_out_chosen_notice update, FetchGap handler method, leader metrics
- `lib/crow-kv/src/cluster/group_election.rs` — strip catch-up from heartbeat, bulk Phase 1 known_commit_slot advance
- `lib/crow-kv/src/cluster/local_replica.rs` — apply loop changes, gap tracking, FetchGap sending, follower metrics
- `lib/crow-kv/src/cluster/remote_replica.rs` — send_chosen_notice ballot_round, send_fetch_gap wrapper
- `lib/crow-kv/src/cluster/learner_stream.rs` — FetchGap frame support
- `lib/crow-kv/src/paxos/learner.rs` — is_chosen query
- `lib/crow-tree/ffi/src/lib.rs` — remove AsyncCrowtree methods
- `lib/crow-tree/ffi/tests/ffi_test.rs` — update call sites
- `lib/crow-tree/ffi/examples/async_get_bench.rs` — update call site
- `lib/crow-kv/tests/election/` — new tests
- `lib/crow-kv/src/common/config.rs` — catchup_snapshot_threshold config
- `doc/backlog/backlog.md` — R64 superseded, R65 entry removal (Phase 12)

## Test Checkpoint Schedule

Full test suite runs at these checkpoints:
- After Phase 2 (Accept path fix — highest regression risk)
- After Phase 3 (ChosenNotice rewrite — second highest risk)
- After Phase 6 (heartbeat strip — liveness risk)
- After Phase 10 (all new tests)
- Phase 11 (final full suite)

Intermediate phases run `test-kv-core` + `test-kv-server` only.

## Blocked

(none yet)
