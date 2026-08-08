<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: Value-less Catch-up + Background Apply Loop (R63)

Design: `doc/working/design-catchup-apply.md`

## Task breakdown

- [ ] **T1: Proto** — add `BatchChosenNotification` message + `batch_chosen` frame
  in `LearnerStreamRequest` oneof (`pxos.proto`).
- [ ] **T2: LearnerStream sender** — add `send_batch_chosen()` fire-and-forget
  method to `PxLearnerStream` (`learner_stream.rs`).
- [ ] **T3: RemoteReplica wrapper** — add `send_batch_chosen_notice()` to
  `PxRemoteReplica` (`remote_replica.rs`).
- [ ] **T4: Background apply loop** — add `known_commit_slot`, `apply_notify`,
  `apply_loop_cancel`, `apply_loop_handle` fields to `PxLocalReplica`. Add
  `ensure_apply_loop()`, `wake_apply_loop()`, `advance_known_commit_slot()`,
  `stop_apply_loop()` methods. Implement the apply loop as a spawned async
  task with skip-and-continue for missing slots (`local_replica.rs`).
- [ ] **T5: handle_heartbeat** — replace async `apply_committed_up_to` call
  with store-and-signal: `fetch_max` into `known_commit_slot`, signal
  `apply_notify`, return immediately (`local_replica.rs`).
- [ ] **T6: handle_accept_inner** — replace `learn_chosen` with
  `update_chosen_frontier` + `record_dedup_tags` + `advance_known_commit_slot`
  + `wake_apply_loop` (`px_service.rs`).
- [ ] **T7: BatchChosen frame handler** — new `BatchChosen` arm in the
  LearnerStream bidi loop: loop over slots, `update_chosen_frontier` for
  present ones, `advance_known_commit_slot` + `wake_apply_loop`
  (`px_service.rs`).
- [ ] **T8: Leader catch-up** — send `BatchChosenNotice` before the full-accept
  loop in `run_heartbeat_round` catch-up replay (`group_election.rs`).
- [ ] **T9: Remove `apply_committed_up_to`** — delete the old method, its
  logic is absorbed into the background apply loop.
- [ ] **T10: Update existing tests** — heartbeat tests that check
  `contiguous_applied` after `on_heartbeat` need to await the apply fence.
- [ ] **T11: New tests** — skip-gap apply, batch chosen frontier advance,
  heartbeat reply not delayed by apply, follower-wins-election deadlock
  regression.
- [ ] **T12: Lint + test** — `cargo fmt --check`, `cargo clippy -- -D warnings`,
  relevant tests.

## File-level changes

- `lib/crow-kv/src/rpc/proto/pxos.proto` — new message + oneof field.
- `lib/crow-kv/src/cluster/learner_stream.rs` — `send_batch_chosen()`.
- `lib/crow-kv/src/cluster/remote_replica.rs` — `send_batch_chosen_notice()`.
- `lib/crow-kv/src/cluster/local_replica.rs` — apply loop infrastructure,
  `handle_heartbeat` change, remove `apply_committed_up_to`, `shutdown` stop.
- `lib/crow-kv/src/rpc/px_service.rs` — `handle_accept_inner` defer,
  `BatchChosen` frame handler.
- `lib/crow-kv/src/cluster/group_election.rs` — send batch notice in catch-up.
- `lib/crow-kv/tests/election/heartbeat_test.rs` — update for async apply.
- `lib/crow-kv/tests/election/apply_loop_test.rs` — new test file.

## Dependency ordering

T1 (proto) → T2 (stream) → T3 (remote) → T4 (apply loop) → T5 (heartbeat)
→ T6 (accept defer) → T7 (batch handler) → T8 (leader catch-up) → T9 (remove
old) → T10 (update tests) → T11 (new tests) → T12 (lint).

## Design decisions

- **Apply loop as async task, not `spawn_blocking`**: the existing
  `spawn_learn_chosen` (R17) uses `tokio::spawn` with `apply_entry().await`.
  `KVEngine::apply` has no genuine `Pending` path today, so the async task
  never actually suspends. Following the same pattern keeps consistency.
  The design doc's `spawn_blocking` suggestion is a future optimization if
  apply CPU cost becomes measurable.
- **Apply loop cancel token owned by `PxLocalReplica`**, not per-tenure. The
  loop runs for the replica's lifetime; it's lazily spawned on first
  `wake_apply_loop()` call (from heartbeat or accept) and cancelled in
  `shutdown()`. Simpler than per-tenure lifecycle and avoids the need for
  the election driver to manage it.
- **`ensure_apply_loop` called from `wake_apply_loop`**: any caller that
  wakes the loop also ensures it's running. This handles the case where a
  follower accepts slots before its first heartbeat.

## Test checklist

- [ ] `apply_loop_skips_missing_slot_and_applies_subsequent`
- [ ] `batch_chosen_advances_frontier_for_present_slots`
- [ ] `heartbeat_reply_not_delayed_by_apply`
- [ ] `follower_accepts_then_wins_election_applies` (deadlock regression)
- [ ] All existing election tests pass
