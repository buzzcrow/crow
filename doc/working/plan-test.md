<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV Test Task Backlog

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/design-test.md`](../design/design-test.md).

## Suite Timing

Measured on 2026-07-16. All six suites passed with zero failures.

| Suite | Result | Tests | macOS |
| --- | --- | --- | --- |
| `test-ct` | pass | 328/328 | 8.3 s |
| `test-core` | pass | 404 | 8.9 s |
| `test-server` | pass | 47 | 9.4 s |
| `test-cli` | pass | 56 | 9.6 s |
| `test-mgmt-api` | pass | 49 | 39.1 s |
| `test-ui` | pass | 26/26 | 39.3 s |

## Election Unit

All tasks completed.

## Replica

- [ ] **WAL GC safe slot integration**: `crowkv/src/wal/gc.rs` uses `safe_slot = u64::MAX`. Needs snapshot persistence and a slot marker (e.g. `contiguous_applied` / durable-commit watermark) so GC can safely truncate below the applied frontier. Add a dedicated GC test once the slot marker is implemented.

## Group

- [ ] **KV operation correctness**: all op types and orderings through group `propose` — Put, overwrite, Delete, delete non-existent, batch with multiple puts, intra-batch last-wins, put-then-delete, delete-then-put, empty batch, mixed ops across slots. Verify via `engine_get` on all replicas.
- [ ] **KV edge-case keys**: empty key, large key (≥1KB), special-bytes key (null, high-UTF8, whitespace), large value (≥1MB), small value (1 byte), empty value. At least one test covering all edge cases through group propose.
- [ ] **LearnerStream** (`cluster/learner_stream.rs`): bidi-stream framing, flow control, parallel in-flight slots, stream re-establish after drop.
- [ ] **Recovery above the durable-commit watermark** via bulk Phase 1 / heartbeat catch-up on a fresh follower.
- [ ] **Leader-kill + restart no-data-loss** at full speed (blocked by repair-correctness).
- [ ] Two-replica even-quorum behaviour (no progress without both up) as an explicit assertion.
- [ ] **Leader change simulation**: start 3-node cluster, write keys, force step-down, wait for new leader, write more keys, force another step-down and re-election, verify all keys readable through final leader. Location: `crowkv/tests/group/g3_leader_change_test.rs`.
- [ ] **Reconfig — add replica catch-up**: 3-node group with existing data, add 4th replica, verify new replica catches up (data visible via scan) within 10 s.
- [ ] **Reconfig — remove non-leader**: 3-node group, remove a non-leader replica, verify group continues to accept KV ops (quorum intact).
- [ ] **Reconfig — remove leader**: 3-node group, remove the leader, verify new leader elected within 10 s, verify KV ops resume.

## Store

- [ ] **KV operation correctness**: all op types and orderings through `PxKvStore` public API (`kv_put`, `kv_delete`, `kv_batch_write`) — same checklist as group layer.
- [ ] **KV edge-case keys**: same edge-case coverage as group layer, through `PxKvStore` public API.
- [ ] **Multi-node, multi-group store**: ≥3 nodes each hosting the same set of groups; assert per-group isolation and independent leadership.
- [ ] Per-group WAL-root isolation on one node (no cross-group slot/key bleed) at the store layer.
- [ ] Store-wide graceful shutdown with multiple active groups under load.

## Deployment

- [ ] Re-enable the four ignored process-level tests once their root causes are fixed.
- [ ] Multi-store-per-node process test that mirrors the Web UI multi-store topology end-to-end.
- [ ] **Leader change via API**: 3-node process cluster, trigger step-down via HTTP API, poll `/ready` until new leader, verify KV ops continue.
- [ ] **Reconfig via API — add replica**: 3-node cluster, add 4th replica via HTTP API, poll `/ready` until caught up, verify data on new node.
- [ ] **Reconfig via API — remove leader**: 3-node cluster, remove leader via HTTP API, poll `/ready` until new leader, verify KV ops resume.
- [x] **Async operation API**: trigger step-down, verify `202 {operation_id}`, poll `GET /operations/:id` until `Completed`. — done in `async_ops_test.rs`
- [x] **Readiness API**: verify `GET /groups/:gid/ready` returns `200` when ready, `503` when no leader, `503` with lag info when replica is behind. — done in `async_ops_test.rs`
- [x] **Backward compat**: existing tests pass with `?sync=true` on step-down, remove replica, add replica endpoints. — done in `server_api_test.rs` + `async_ops_test.rs`

## E2E / Playwright UI Test Implementation Plan

The test suite follows the tiered strategy defined in
[`design/design-test.md`](../design/design-test.md) UI E2E Layer.
This plan covers both **enhancing existing tests** to fit the tiered system
and **creating new tests** to fill coverage gaps.

### Phase 0 — Shared Infrastructure

- [ ] **setupCluster() helper**: Add to `consoleSetup.ts`. Accepts a topology
  descriptor: `{ nodeCount, storeCount, groupsPerStore, replicasPerGroup,
  rackPrefix, portBase }`. Creates racks, nodes, deploys servers, creates
  stores, adds groups with replicas, waits for leaders. Returns `{ nodes,
  stores, groups, apiBase }`. All existing setup sequences (seedRackAndNode +
  deployNodeServer + createStore + addGroup + waitForLeader) should be
  refactored to call this helper internally.
- [ ] **topology presets**: Define `SIMPLE` (3 nodes, 1 store, 1 group, 3
  replicas) and `COMPLEX` (8 nodes, 2 stores, 2 groups per store, 3 replicas
  per group on random subsets) as named constants for comparative tests.

### Phase 1 — Enhance Existing Tests (Tier 1 consolidation)

Refactor existing tests to use shared helpers, add missing assertions, and
prepare them for topology parameterization.

- [ ] **00-smoke**: Refactor to use `setupCluster()`. Keep as Tier 0 smoke
  test on SIMPLE topology. Add explicit assertions for console errors
  (already collected but only checked at end — add mid-test checks).
- [ ] **05-store-group-replica-chain**: Refactor setup to use
  `setupCluster()`. Add assertion that the group appears in the Logical view
  tree with the correct store parent.
- [ ] **06-cross-jump**: Add reverse direction assertion (Physical Node to
  Logical Store "Show store X in cluster" button). Currently only tests
  Logical Replica to Physical Node.
- [ ] **09-kv-put-get**: Add overwrite assertion (put same key with new
  value, verify get returns new value). Add assertion that revision
  increments.
- [ ] **10-kv-scan**: Add prefix-filter assertion (put keys with different
  prefixes, scan with prefix, verify only matching keys returned).
- [ ] **18-full-chain**: Refactor to use `setupCluster(SIMPLE)`. This test
  already does everything via UI — keep that but use the helper for setup
  state verification.
- [ ] **19-large-cluster**: Add KV put/get assertion after leader election
  (currently only verifies leaders are elected, not that KV ops work on the
  multi-group cluster).
- [ ] **20-ui-behaviors**: Add assertion for dialog cancel (open dialog,
  click cancel, verify no entity created).
- [ ] **26-kv-demo**: Add assertion that demo keys appear in scan after
  inject, and that scan is empty after delete-all.

### Phase 2 — New Tier 1 Tests (single-feature coverage)

All implemented and passing (39/39). Toast assertions removed, timeouts
tightened, `evaluate` click bypass for toast-overlay blocking.

- [x] **27-server-lifecycle**: Ping node, Restart server, Stop server — all via context menu. Verify backend state change (server status via API). Stop should make the node's Deploy menu item reappear.
- [x] **28-kv-advanced-ops**: Delete Prefix, Delete Selected (checkbox + button), inline delete (per-row trash), copy-to-clipboard on Get result. Each in isolation on a single-group store.
- [x] **29-kv-load-more**: Put >100 keys, scan, verify truncated indicator + Load More button, click Load More, verify additional rows appear.
- [x] **30-kv-all-groups-mode**: 2+ groups in one store, switch to All Groups, verify scan aggregates, Get disabled, Put distributes to a random group.
- [x] **31-kv-auto-scan-toggle**: Toggle auto-scan off, put a key, verify scan table does NOT auto-refresh. Toggle on, put again, verify it does.
- [x] **32-inspector-activity-tab**: Perform a mutation, open Inspector Activity tab, verify entry appears with action/target/status. Click Clear log, verify entries removed.
- [x] **33-inspector-cross-jump-node-to-store**: Select a physical node hosting a store, verify "Show store X in cluster" button, click it, verify view switches to Logical and store selected.
- [x] **34-sidebar-filter**: Create multiple racks/nodes, type in Filter, verify tree narrows. Clear filter, verify all items return.
- [x] **35-header-refresh**: Modify backend via API (add rack), click Refresh, verify new rack appears without page reload.
- [x] **36-health-pill-states**: Verify "Unknown" with no groups, "Healthy" with all-healthy groups, "Degraded" when a group loses leadership.
- [x] **37-dialog-duplicate-id**: Add rack with existing ID, verify error toast and dialog stays open. Same for node and store.

### Phase 3 — New Tier 2 Tests (complex topology & multi-store)

All Tier 2 tests use `setupCluster()` with both SIMPLE and COMPLEX presets.
If a test passes on SIMPLE but fails on COMPLEX, the gap is multi-node interaction.

- [ ] **38-multi-store-isolation**: 2 stores on overlapping node sets. Put/Get/Delete on store A does not affect store B. Scan on store A returns only store A keys. Groups in different stores have independent leaders.
- [ ] **39-subset-group-operations**: 8 nodes deployed, create a group on a random 3-node subset. Verify leader election, KV put/get/delete all work. Create a second group on a different 3-node subset (overlapping by 1). Verify both groups operate independently.
- [ ] **40-multi-group-same-store**: 1 store, 3 groups, each on a different 3-node subset of 5 nodes. Verify per-group leader election, independent KV operations, scan in All Groups mode aggregates correctly.
- [ ] **41-comparative-standard-suite**: Refactor existing 00-smoke test to accept a topology parameter. Run once with 3-node simple topology, once with 8-node complex topology (2 stores, subset groups). Both must pass. This becomes the regression baseline for multi-node changes.

### Phase 4 — New Tier 3 Tests (reconfig & partial degradation)

These test the reconfig feature: stopping/deleting nodes while groups are active,
verifying the cluster continues to operate correctly with reduced membership.
R12 (async operation API) is now implemented — these tests should use the
async operation pattern (trigger → poll `/operations/:id` → poll `/ready`)
instead of blocking on the HTTP call.

- [ ] **42-stop-server-keeps-group**: 3-node group, stop the server on a non-leader node via context menu. Verify group still accepts puts/gets (quorum intact). Verify health pill shows Degraded. Restart the stopped server, verify group returns to full health.
- [ ] **43-stop-leader-reelection**: 3-node group, identify the leader, stop the leader's server. Verify a new leader is elected within 10s. Verify KV put/get still works. Restart the old leader, verify it rejoins as follower.
- [ ] **44-delete-node-after-group**: 5-node group, delete a non-leader node. Verify group still operates (quorum 4-of-5). Delete another non-leader. Verify group still operates (quorum 3-of-5). Stop here — do not go below majority.
- [ ] **45-add-replica-to-running-group**: 3-node group with active KV data, add a 4th replica via context menu. Verify new replica catches up (data visible via scan on the new node's store). Verify group still accepts writes.
- [ ] **46-multi-store-reconfig**: 2 stores on 5 nodes. Stop a server that participates in both stores. Verify both stores' groups handle the loss correctly (degraded but functional if quorum holds). Restart, verify recovery.

