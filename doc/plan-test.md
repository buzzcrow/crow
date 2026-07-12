# CrowKV Test Task Backlog

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see `test.md`.

## Election Unit

- [x] `on_step_down` handler: strict-fence policy (only accepts if still leader at requested term) (`step_down_test.rs`).
- [x] `frontier_triple` consistency under concurrent role transitions (`frontier_test.rs`).

## Replica

- [x] Multi-slot WAL replay ordering edge cases (gaps in accepted log during restore) (`replay_ordering_test.rs`).
- [x] Concurrent learn_chosen + on_accept race on the same slot (`concurrent_test.rs`).
- [ ] **WAL GC safe slot integration**: `crowkv/src/wal/gc.rs` uses `safe_slot = u64::MAX`. Needs snapshot persistence and a slot marker (e.g. `contiguous_applied` / durable-commit watermark) so GC can safely truncate below the applied frontier. Add a dedicated GC test once the slot marker is implemented.

## Group

- [ ] **KV operation correctness**: all op types and orderings through group `propose` — Put, overwrite, Delete, delete non-existent, batch with multiple puts, intra-batch last-wins, put-then-delete, delete-then-put, empty batch, mixed ops across slots. Verify via `engine_get` on all replicas (see `test.md` KV op correctness rule).
- [ ] **PeerStream** (`cluster/peer_stream.rs`): bidi-stream framing, flow control, parallel in-flight slots, stream re-establish after drop.
- [ ] **Recovery above the durable-commit watermark** via bulk Phase 1 / heartbeat catch-up on a fresh follower.
- [ ] **Leader-kill + restart no-data-loss** at full speed (blocked by repair-correctness).
- [ ] Two-replica even-quorum behaviour (no progress without both up) as an explicit assertion.
- [ ] **Leader change simulation**: start 3-node cluster, write keys, force step-down, wait for new leader, write more keys, force another step-down and re-election, verify all keys readable through final leader. Assert `highest_seen_slot` >= max slot written; no Accepted records missing. Location: `crowkv/tests/group/g3_leader_change_test.rs` (new file, same pattern as `g1_step_down_survival_test.rs`). Timings: use aggressive `for_tests()` (5ms heartbeat, 30–60ms election, 25ms lease).

## Store

- [ ] **KV operation correctness**: all op types and orderings through `PxKvStore` public API (`kv_put`, `kv_delete`, `kv_batch_write`) — same checklist as group layer. Verify via `kv_get` and `engine_get` (see `test.md` KV op correctness rule).
- [ ] **Multi-node, multi-group store**: ≥3 nodes each hosting the same set of groups; assert per-group isolation and independent leadership.
- [ ] Per-group WAL-root isolation on one node (no cross-group slot/key bleed) at the store layer.
- [ ] Store-wide graceful shutdown with multiple active groups under load.

## Deployment

- [ ] Re-enable the four ignored process-level tests once their root causes are fixed.
- [ ] Multi-store-per-node process test that mirrors the Web UI multi-store topology end-to-end.
