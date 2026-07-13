# CrowKV Test Task Backlog

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/design-test.md`](design/design-test.md).

## Election Unit

- [x] `on_step_down` handler: strict-fence policy (only accepts if still leader at requested term) (`step_down_test.rs`).
- [x] `frontier_triple` consistency under concurrent role transitions (`frontier_test.rs`).

## Replica

- [x] Multi-slot WAL replay ordering edge cases (gaps in accepted log during restore) (`replay_ordering_test.rs`).
- [x] Concurrent learn_chosen + on_accept race on the same slot (`concurrent_test.rs`).
- [ ] **WAL GC safe slot integration**: `crowkv/src/wal/gc.rs` uses `safe_slot = u64::MAX`. Needs snapshot persistence and a slot marker (e.g. `contiguous_applied` / durable-commit watermark) so GC can safely truncate below the applied frontier. Add a dedicated GC test once the slot marker is implemented.

## Group

- [ ] **KV operation correctness**: all op types and orderings through group `propose` — Put, overwrite, Delete, delete non-existent, batch with multiple puts, intra-batch last-wins, put-then-delete, delete-then-put, empty batch, mixed ops across slots. Verify via `engine_get` on all replicas (see [`design/design-test.md`](design/design-test.md) KV op correctness rule).
- [ ] **LearnerStream** (`cluster/learner_stream.rs`): bidi-stream framing, flow control, parallel in-flight slots, stream re-establish after drop.
- [ ] **Recovery above the durable-commit watermark** via bulk Phase 1 / heartbeat catch-up on a fresh follower.
- [ ] **Leader-kill + restart no-data-loss** at full speed (blocked by repair-correctness).
- [ ] Two-replica even-quorum behaviour (no progress without both up) as an explicit assertion.
- [ ] **Leader change simulation**: start 3-node cluster, write keys, force step-down, wait for new leader, write more keys, force another step-down and re-election, verify all keys readable through final leader. Assert `highest_seen_slot` >= max slot written; no Accepted records missing. Location: `crowkv/tests/group/g3_leader_change_test.rs` (new file, same pattern as `g1_step_down_survival_test.rs`). Timings: use aggressive `for_tests()` (5ms heartbeat, 30–60ms election, 25ms lease).

## Store

- [ ] **KV operation correctness**: all op types and orderings through `PxKvStore` public API (`kv_put`, `kv_delete`, `kv_batch_write`) — same checklist as group layer. Verify via `kv_get` and `engine_get` (see [`design/design-test.md`](design/design-test.md) KV op correctness rule).
- [ ] **Multi-node, multi-group store**: ≥3 nodes each hosting the same set of groups; assert per-group isolation and independent leadership.
- [ ] Per-group WAL-root isolation on one node (no cross-group slot/key bleed) at the store layer.
- [ ] Store-wide graceful shutdown with multiple active groups under load.

## Deployment

- [ ] Re-enable the four ignored process-level tests once their root causes are fixed.
- [ ] Multi-store-per-node process test that mirrors the Web UI multi-store topology end-to-end.

## Failing tests (2026-07-12 rerun)

Decision: keep the aggressive `test` election profile for multi-process tests
because they run on the same physical node (no long RPC time). The failures
below are fixed by adding leader-refresh / retry logic instead of slowing the
profile down.

After the fixes all six suites pass:

| Suite | Result |
|-------|--------|
| test-ct | pass (291/291) |
| test-core | pass |
| test-server | pass |
| test-cli | pass |
| test-web | pass |
| test-ui | pass (23/23) |

### test-server: cluster_e2e_test.rs — "not leader" mid-test (EASY, FIXED)

- [x] **`e2e_three_node_cluster_kv_put_batch_delete`** — Fixed.

**Root cause:** `crowkv-server/tests/testkit/process.rs` hardcodes
`--election-profile test` (5ms heartbeat, 25ms lease). The 25ms lease expires
under OS scheduling jitter during the real 3-node process test, so the
cluster loses leadership before the delete batch is applied.

**Fix:** added a `run_kv_op_with_retry` helper that re-finds the current leader
and retries the operation when the RPC returns "not leader", while keeping the
`test` election profile.

### test-cli: kv_cli_test.rs — "not leader" on scan/list (EASY, FIXED)

- [x] CLI testkit crowtree corruption (`bench_run_write_smoke` failing with
      `CrowtreeEngine::open(...) failed: Corruption`) — fixed by deploying
      the test server into an isolated temp workspace with `bin/` and `log/`
      subdirs and cleaning up on drop.
- [x] **`kv_put_get_delete_round_trip`** — Fixed.

**Root cause:** `crowkv-console/cli/tests/testkit/console.rs::spawn_upstream`
uses `--election-profile test`. The 25ms lease expires while the test is
exercising CLI commands, causing the single node to step down and later scans
to fail.

**Fix:** kept `spawn_upstream` on the `test` profile and added a retry loop
around the seed `kv put`s and the final `kv list` so transient leader loss is
retried.

