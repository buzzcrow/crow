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

---

## Unit Layer — paxos/acceptor

Source: `crowkv/src/paxos/acceptor.rs`. Tests: `acceptor_test.rs` (6 tests).
Existing: prepare_promise, reject_lower_ballot, accept_after_promise,
accept_rejects_lower_ballot, prepare_returns_previously_accepted_value,
ballot_ordering_is_total.

- [x] **Equal-ballot accept is idempotent**
- [x] **Accept without prior prepare**
- [x] **Multi-slot isolation**
- [x] **`highest_seen_slot` monotonicity**
- [x] **`accepted_log_tip`**

## Unit Layer — paxos/learner

Source: `crowkv/src/paxos/learner.rs`. Tests: `learner_test.rs` (4), `learner_dedup_test.rs` (10), `learner_async_test.rs` (1). Coverage is thorough.

- [x] **`seed_resume_frontier`**: `pub(crate)` — covered indirectly via replica persistence tests (`restore_from_replay_with_engine`). Not directly testable from integration tests.

## Unit Layer — paxos/error

Source: `crowkv/src/paxos/error.rs`. Tests: `error_test.rs` (3 of 11 variants: PrepareRejected, AcceptRejected, TransportFailure).

- [x] **Error classifier — remaining 8 variants**: all 11 variants now tested.

## Unit Layer — kv/mem_kv + kv/op

Source: `crowkv/src/kv/`. Tests: `mem_kv_test.rs` (9 + conformance), `op_codec_test.rs` (11), `kv_future_test.rs` (5), `conformance.rs` (shared). Coverage is thorough.

- [x] **Delete non-existent key is a no-op**

## Unit Layer — wal/record

Source: `crowkv/src/wal/record.rs`. Tests: `record_tests.rs` (9). No gaps identified.

## Election Unit

Source: `crowkv/src/election/`. Tests: 8 files, 72 tests. All tasks completed.

## WAL Subsystem

Source: `crowkv/src/wal/`. Tests: 12 files, ~92 tests. Coverage is thorough.

- [ ] **WAL disk-loss recovery**: simulate fsync failure or file loss after write — verify engine surfaces error and reads/replays are consistent with last durable state. Feature-dependent per design-test.md.

## Slot Subsystem

Source: `crowkv/src/paxos/slot_list.rs`, `slot_node.rs`. Tests: `slot_list_test.rs` (18 tests).

- [x] **Concurrent insert at disjoint ranges**
- [x] **Concurrent insert + read**
- [x] **Concurrent insert + trim + reclaim**
- [x] **Multiple guards pin same chunk**
- [x] **Progressive trim across chunks**

## Replica

Source: `crowkv/src/cluster/local_replica.rs`. Tests: 10 files, ~56 tests.

- [ ] **WAL GC safe slot integration**: `crowkv/src/wal/gc.rs` uses `safe_slot = u64::MAX`. Needs snapshot persistence and a slot marker so GC can safely truncate below the applied frontier. Add a dedicated GC test once the slot marker is implemented.

## Group

Source: `crowkv/src/cluster/group.rs`. Tests: 20 files, ~56 tests.

- [x] **KV operation correctness**: 8 tests covering all op types and orderings through group gRPC KV API.
- [x] **KV edge-case keys**: 6 tests covering empty value, 1-byte value, 1KB key, 100KB value, special-bytes key, whitespace key.
- [ ] **LearnerStream** (`cluster/learner_stream.rs`): blocked — LearnerStream infrastructure not yet implemented.
- [ ] **Recovery above durable-commit watermark**: blocked — bulk Phase 1 catch-up for fresh follower not yet implemented.
- [ ] **Leader-kill + restart no-data-loss**: blocked by repair-correctness.
- [x] **Two-replica even-quorum behaviour**: `two_replica_even_quorum_writes_succeed_with_both_up` — 2-node cluster writes succeed with both up.
- [x] **Leader change simulation**: `leader_change_simulation` — 3-node cluster, two consecutive step-downs, all keys survive.
- [ ] **Reconfig — add replica catch-up**: blocked — `add_replica` API not yet implemented.
- [ ] **Reconfig — remove non-leader**: blocked — `remove_replica` API not yet implemented.
- [ ] **Reconfig — remove leader**: blocked — `remove_replica` API not yet implemented.

## Store

Source: `crowkv/src/store/`. Tests: 6 files, 23 tests (node, multi_group,
status, health, shutdown, persistence).

- [x] **KV operation correctness**: 8 tests through `PxKvStore` public API covering all op types and orderings.
- [x] **KV edge-case keys**: 5 tests covering empty value, 1-byte value, 1KB key, 100KB value, special-bytes key.
- [ ] **Multi-node, multi-group store**: blocked — needs multi-node store test harness (not yet built).
- [ ] **Per-group WAL-root isolation**: blocked — WAL-root per-group not yet configurable in test harness.
- [ ] **Store-wide graceful shutdown under load**: blocked — needs multi-group load test harness.

## Deployment

Source: `crowkv-server/`. Tests: 6 files, 53 tests (server_api, async_ops,
cli_parse, cluster_e2e, startup, snapshot_join_e2e).

- [ ] Re-enable the four ignored process-level tests once their root causes are fixed.
- [ ] Multi-store-per-node process test that mirrors the Web UI multi-store topology end-to-end.
- [x] **Leader change via API**: covered by `async_step_down_returns_operation_id` + `ready_endpoint_after_full_cluster_lifecycle` in `async_ops_test.rs`.
- [ ] **Reconfig via API — add replica**: blocked — `add_replica` API not yet implemented.
- [ ] **Reconfig via API — remove leader**: blocked — `remove_replica` API not yet implemented.
- [ ] **Network partition between processes**: blocked — needs network partition simulation infrastructure.
- [ ] **Graceful shutdown under load**: blocked — needs load generation + multi-node shutdown harness.
- [x] **Async operation API**: trigger step-down, verify `202 {operation_id}`, poll `GET /operations/:id` until `Completed`. — done in `async_ops_test.rs`
- [x] **Readiness API**: verify `GET /groups/:gid/ready` returns `200` when ready, `503` when no leader, `503` with lag info when replica is behind. — done in `async_ops_test.rs`
- [x] **Backward compat**: existing tests pass with `?sync=true` on step-down, remove replica, add replica endpoints. — done in `server_api_test.rs` + `async_ops_test.rs`

## Console Mgmt API Layer

Source: `crowkv-console/`. Tests: web 13 files (~37 tests), shared/cli 7
files (~9 tests). Covers REST routes, CLI commands, API forwarding, health
aggregation, config persistence, OpenAPI proxy. No gaps identified.

## crowtree C++ Tests

Source: `crowtree/tests/`. Tests: 334 tests (unit: 26 files, integration:
24 files). Covers cell encoding, leaf/frame/inner pages, delta replay,
consolidation, mapping table, epoch manager, split/merge, snapshot
roundtrip, crash recovery, C API, async get/scan, eviction, compression,
persist, write/read paths, stress. No gaps identified.

## Rust FFI / Cross-Engine Parity

Source: `crowkv/tests/kv/crowtree_engine_test.rs`. Tests: conformance
suite (shared with `InMemKV`), async pending path, durable reopen,
cross-engine parity, clear. No gaps identified.

## E2E / Playwright UI Test Implementation Plan

The test suite follows the tiered strategy defined in
[`design/design-test.md`](../design/design-test.md) UI E2E Layer.
This plan covers both **enhancing existing tests** to fit the tiered system
and **creating new tests** to fill coverage gaps.

### Phase 0 — Shared Infrastructure

- [x] **setupCluster() helper**: Implemented in `consoleSetup.ts`. Accepts `TopologyDescriptor`, creates racks/nodes/servers/stores/groups, waits for leaders. Used by test 41.
- [x] **topology presets**: `SIMPLE` (3 nodes, 1 store, 1 group) and `COMPLEX` (8 nodes, 2 stores, 2 groups/store) defined as constants in `consoleSetup.ts`.

### Phase 1 — Enhance Existing Tests (Tier 1 consolidation)

Refactor existing tests to use shared helpers, add missing assertions, and
prepare them for topology parameterization.

- [x] **00-smoke**: Mid-test console error checks added after deploy, group creation, and KV ops.
- [x] **05-store-group-replica-chain**: Added tree parent-child assertion (S-57 expanded, G-570 visible under it).
- [x] **06-cross-jump**: Reverse direction (Physical Node → Logical Store) already implemented.
- [x] **09-kv-put-get**: Add overwrite assertion (put same key with new
  value, verify get returns new value). Add assertion that revision
  increments.
- [x] **10-kv-scan**: Add prefix-filter assertion (put keys with different
  prefixes, scan with prefix, verify only matching keys returned).
- [x] **18-full-chain**: Added API verification after server deployment (both nodes' pid checked).
- [x] **19-large-cluster**: Add KV put/get assertion after leader election
  (currently only verifies leaders are elected, not that KV ops work on the
  multi-group cluster).
- [x] **20-ui-behaviors**: Added dialog cancel test (fill Add Rack form, cancel, verify rack not created via UI + API).
- [x] **26-kv-demo**: Add assertion that demo keys appear in scan after
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

- [x] **38-multi-store-isolation**: 2 stores on separate node sets. Put/Get/Delete on store A does not affect store B. Scan on store A returns only store A keys.
- [x] **39-subset-group-operations**: 5 nodes, 2 groups on overlapping 3-node subsets (overlap by 1). Verify leader election, KV put/get/delete, cross-group isolation.
- [x] **40-multi-group-same-store**: 1 store, 3 groups on different 3-node subsets of 5 nodes. Per-group leader election, independent KV operations, scan isolation.
- [x] **41-comparative-standard-suite**: Smoke suite run on SIMPLE (3 nodes) and COMPLEX (8 nodes, 2 stores, 4 groups) topologies. Both pass.

### Phase 4 — New Tier 3 Tests (reconfig & partial degradation)

These test the reconfig feature: stopping/deleting nodes while groups are active,
verifying the cluster continues to operate correctly with reduced membership.
R12 (async operation API) is now implemented — these tests should use the
async operation pattern (trigger → poll `/operations/:id` → poll `/ready`)
instead of blocking on the HTTP call.

- [x] **42-stop-server-keeps-group**: 3-node group, stop non-leader, verify quorum intact and KV ops work. Restart, verify full health.
- [x] **43-stop-leader-reelection**: 3-node group, stop leader, verify new leader elected within 10s. KV ops continue. Restart old leader, verify rejoin.
- [x] **44-delete-node-after-group**: 5-node group, delete 2 non-leader nodes sequentially. Group operates down to 3-of-5 (exact majority).
- [x] **45-add-replica-to-running-group**: 3-node group with KV data, add 4th replica. Verify group still accepts writes and original keys readable.
- [x] **46-multi-store-reconfig**: 2 stores on overlapping nodes. Stop shared non-leader, both stores maintain quorum. Restart, verify recovery.

### Phase 5 — New Tier 3 Coverage Gaps

- [ ] **Async operation UI feedback**: trigger step-down or reconfig via
  UI, verify the UI shows progress feedback (spinner/status indicator)
  and polls the async operation API until completion. Design doc says
  "the UI should show progress feedback and poll the async operation
  API until completion".

