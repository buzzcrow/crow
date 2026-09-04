<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Diskdb Behavioral Conformance Plan

This plan implements
[`design-diskdb-behavior-conformance.md`](design-diskdb-behavior-conformance.md)
and [R130](../backlog/R130-diskdb-behavior-conformance.md). It is ordered
by data-safety risk: prevent corruption and false writability first, then
repair control-plane behavior, ownership, test structure, and benchmarks.
Data migration and owner handoff remain in R102.

## Phase 1: Data-Safety Regression Tests

- [x] **Cover partial allocation rollback**: force a multi-block request
  to claim some ranges and then exhaust capacity; prove all claims are
  returned and free-space accounting is unchanged. Files:
  `app/crowdb-diskdb/tests/disk_alloc.rs`.
- [x] **Cover missing bind isolation**: publish an owner without a bind
  and prove diskdb neither creates a writable group nor reads/writes
  zone records through `(0, 0)`. Files:
  `app/crowdb-diskdb/tests/keepalive_sync.rs`.
- [ ] **Cover single startup ownership**: instrument loading and prove
  each startup disk is loaded exactly once and into the group retained
  by the container. Files:
  `app/crowdb-diskdb/tests/startup_sync.rs`.
- [x] **Cover failed recovery quarantine**: fail journal replay and full
  scan; prove the disk and process do not become writable and group 0
  does not retain `Up`. Files:
  `app/crowdb-diskdb/tests/recovery.rs`.

## Phase 2: Allocation and Startup Safety

- [x] **Return or roll back every allocation claim**: preserve partial
  claims through the model/orchestration boundary and release incomplete
  results before returning failure. Preserve KV persistence errors rather
  than translating them to `NoSpace`. Files:
  `app/crowdb-diskdb/src/model/disk_group.rs`,
  `app/crowdb-diskdb/src/orchestration/alloc.rs`.
- [x] **Reject incomplete ownership views**: build owner, bind, group,
  node, and disk state before publishing any delta. Missing binds and
  failed reads retain the last-known-good state and set degraded mode.
  Files: `app/crowdb-diskdb/src/liveness/keepalive.rs`.
- [x] **Unify startup loading**: use one whole-group startup loader;
  reserve per-disk loading for disks discovered after startup. Fence
  completion by current owner/bind generation. Files:
  `app/crowdb-diskdb/src/main.rs`,
  `app/crowdb-diskdb/src/liveness/keepalive.rs`.
- [~] **Keep failed recovery non-writable**: do not substitute empty
  zones after failed recovery. Persist failure/offline status before a
  later reconciliation can enable the disk. Files:
  `app/crowdb-diskdb/src/recovery/zone_loader.rs`,
  `app/crowdb-diskdb/src/liveness/keepalive.rs`.

## Phase 3: Client Routing and Service State

- [x] **Replace routing snapshots**: remove stale endpoint and
  disk-to-group entries on refresh. Treat `NotOwner` as a topology
  invalidation and bounded retry signal. Files:
  `lib/crowdb-diskdb-client/src/client.rs`.
- [x] **Complete the client mutation surface**: expose
  `commit_blocks` through `DiskdbClient` and add component E2E coverage.
  Files: `lib/crowdb-diskdb-client/src/client.rs`,
  `lib/crowdb-diskdb-client/tests/diskdb_full_flow_test.rs` (move to
  `allocation_lifecycle_test.rs` in Phase 6).
- [x] **Use one mutation gate**: allocate, commit, and free must apply
  the same lifecycle, degraded, ownership, and bind checks. Files:
  `app/crowdb-diskdb/src/service/diskdb_rpc_service.rs`.
- [x] **Make readiness truthful**: return ready only for `Up` and
  non-degraded state, with HTTP 503 otherwise. Files:
  `app/crowdb-diskdb/src/health.rs`.

## Phase 4: Owner Creation

- [x] **Commit group and owner together**: for serialized management
  creates, select the least-loaded live diskdb instance, break ties by
  stable instance ID, and batch the disk-group and owner records. Files:
  `lib/crowdb-kv-client/src/hardware.rs`,
  `app/crowdb-web/src/lifecycle.rs`.
- [x] **Fail closed on assignment**: reject creation when no live owner
  exists or group-0 persistence fails; keep local configuration and
  group-0 state consistent through compensation. Files:
  `app/crowdb-web/src/lifecycle.rs`,
  `lib/crowdb-console-shared/src/ops/hardware.rs`.
- [x] **Enforce immutable ownership**: reject a different owner and
  permit only same-instance lease renewal. Do not add owner handoff or
  data migration. Files: `lib/crowdb-kv-client/src/hardware.rs`,
  `app/crowdb-web/src/diskdb.rs`,
  `lib/crowdb-console-shared/src/clients/console.rs`.
- [~] **Verify serial balancing**: create 13 disk-groups through the
  management API with three live groups and assert one owner per group
  with counts `[4, 4, 5]`. Files:
  `lib/crowdb-diskdb-client/tests/owner_creation.rs`,
  `lib/crowdb-test-harness/src/diskdb.rs`.

## Phase 5: Lock-Free Publication and Module Boundaries

- [ ] **Remove hot-path blocking locks**: use a lock-free group map,
  atomic status/bind fields, immutable `ArcSwap` disk and active-zone
  snapshots, and atomic zone health. Review recovery-only locks
  separately before retaining any. Files:
  `app/crowdb-diskdb/src/model/`,
  `app/crowdb-diskdb/src/state/`.
- [ ] **Split oversized modules**: separate observation, reconciliation,
  heartbeat, loading, RPC validation, and RPC mutations so feature tests
  target stable boundaries. Files:
  `app/crowdb-diskdb/src/liveness/`,
  `app/crowdb-diskdb/src/service/`.

## Phase 6: Feature-Grouped Tests

- [ ] **Move inline tests to integration targets**: group server tests by
  startup/sync, ownership, allocation lifecycle, recovery/compaction,
  hardware lifecycle, query/metrics, and scanner/admin. Files:
  `app/crowdb-diskdb/tests/`.
- [ ] **Make client tests the component E2E boundary**: remove duplicate
  full-flow scenarios and add feature-named client files for every public
  behavior, including stale routing and owner retry. Files:
  `lib/crowdb-diskdb-client/tests/`.
- [ ] **Never silently skip E2E**: missing required binaries must fail
  with an actionable message or be built by the test command. Files:
  `lib/crowdb-diskdb-client/tests/common/`, pixi task definitions.

## Phase 7: Console CLI Benchmark

- [ ] **Add diskdb benchmark verbs**: define allocate and mix commands,
  `mem|block` modes, duration, concurrency, allocation size, and seed.
  Files: `app/crowdb-cli/src/commands/bench/`.
- [ ] **Provision the benchmark topology**: start three nodes, one
  disk-group per node, and four disks per group; reuse memory block mode
  where selected. Files: `lib/crowdb-test-harness/src/diskdb.rs`.
- [ ] **Implement allocate mode**: stop only at cluster-wide exhaustion
  or the time limit and drain in-flight operations. Record the stop
  reason. Files: `app/crowdb-cli/src/commands/bench/`.
- [ ] **Implement 70/30 mix mode**: choose allocate/free deterministically,
  free only members of a non-duplicated live set, and restore failed
  frees. There is no free-only benchmark. Files:
  `app/crowdb-cli/src/commands/bench/`.
- [ ] **Verify and report**: compare the live set, durable records, and
  recalculated space totals; report throughput, latency, operation counts,
  RPC failures, correctness failures, and final capacity. Files:
  `app/crowdb-cli/src/commands/bench/`.

## Test Checklist

### Unit and integration

- [ ] Partial multi-block failure releases all bitmap claims.
- [ ] Persistence failure remains distinguishable from exhaustion.
- [ ] Missing or partial group-0 views do not publish writable state.
- [ ] Startup loads each retained disk exactly once.
- [ ] Failed recovery remains quarantined across reconciliation.
- [ ] Route refresh removes stale entries and retries `NotOwner`.
- [ ] All mutations reject degraded state.
- [ ] Degraded readiness returns false and HTTP 503.
- [ ] Owner replacement conflicts; same-owner renewal changes expiry only.
- [ ] Mixed selection maintains live-set and accounting invariants.

### Component end to end

- [ ] Thirteen serialized creates across three owners produce `[4, 4, 5]`.
- [ ] Missing owner and owner-write failure reject disk-group creation.
- [ ] Startup becomes writable only after successful zone load.
- [ ] Allocate, commit, and free work through `DiskdbClient`.
- [ ] Endpoint removal and `NotOwner` refresh route to the current owner.
- [ ] Allocate benchmark covers 12 disks and verifies exhaustion/deadline.
- [ ] Mix benchmark verifies 70/30 selection, durable live set, and space.

### Commands

- [ ] `pixi run -- cargo fmt --all -- --check`
- [ ] `pixi run -- cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run test-kv-client`
- [ ] `pixi run clean-env && pixi run test-console-server`
- [ ] `pixi run clean-env && pixi run test-diskdb`
- [ ] `pixi run clean-env && pixi run test-diskdb-client`
- [ ] `pixi run clean-env && pixi run test-console-cli`
