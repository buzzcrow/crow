# CrowKV Test Task Backlog

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see `test.md`.
For ignored / flaky tests and root causes, see `plan-ut.md`.

## Election Unit

- [x] `on_step_down` handler: strict-fence policy (only accepts if still leader at requested term) (`step_down_test.rs`).
- [x] `frontier_triple` consistency under concurrent role transitions (`frontier_test.rs`).

## Replica

- [x] Multi-slot WAL replay ordering edge cases (gaps in accepted log during restore) (`replay_ordering_test.rs`).
- [x] Concurrent learn_chosen + on_accept race on the same slot (`concurrent_test.rs`).

## Group

- [ ] **PeerStream** (`cluster/peer_stream.rs`): bidi-stream framing, flow control, parallel in-flight slots, stream re-establish after drop.
- [ ] **Recovery above the durable-commit watermark** via bulk Phase 1 / heartbeat catch-up on a fresh follower (see `plan-ut.md` §2.6).
- [ ] **Leader-kill + restart no-data-loss** at full speed (blocked by repair-correctness; `plan-ut.md` §1.4 / §2.3).
- [ ] Two-replica even-quorum behaviour (no progress without both up) as an explicit assertion.

## Store

- [ ] **Multi-node, multi-group store**: ≥3 nodes each hosting the same set of groups; assert per-group isolation and independent leadership.
- [ ] Per-group WAL-root isolation on one node (no cross-group slot/key bleed) at the store layer.
- [ ] Store-wide graceful shutdown with multiple active groups under load.

## Deployment

- [ ] Re-enable the four ignored process-level tests once their root causes (`plan-ut.md` §1.1, §2.1, §2.4) are fixed.
- [ ] Multi-store-per-node process test that mirrors the Web UI multi-store topology end-to-end.
