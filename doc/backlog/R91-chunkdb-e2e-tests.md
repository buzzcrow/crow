<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R91: chunkdb — E2E Tests

**Problem**:

- **Current behavior + impact** — The chunkdb components (R85
  foundation, R86 topology, R87 placement, R88 storage, R89 lifecycle,
  R90 client) each have unit + integration tests, but there is no
  end-to-end test that wires all components together with real KV +
  diskdb instances and verifies the full stack works as a system.
  Without E2E tests, integration bugs at component boundaries (topology
  cache feeding stale data to placement, routing sending a write to
  the wrong KV group during migration, lifecycle leaving orphan disk
  blocks after a failed delete) would only surface in production. E2E
  tests are the final acceptance gate for the v1 chunkdb scope (design
  §14 — "E2E tests" is listed in v1).
- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §14 (implementation scope — v1 includes E2E tests),
  [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
  §11 (diskdb crate layout — `tests/` with `common/cluster.rs`
  harness), `app/crow-diskdb/tests/diskdb_e2e_test.rs` (the pattern to
  follow: start a real 3-dg `crow-kv-server` cluster, seed hardware
  metadata into group-0, run diskdb + chunkdb in-process, verify via
  KV reads + `QueryCapacityStats`). aioss analog: aioss chunkdb has
  integration tests starting metadb + diskdb + chunkdb in-process;
  CROW follows the same harness pattern with CROW's `KvCluster` test
  helper (design §14 — direct port of the test harness structure,
  adapted to CROW's `KvCluster` + `HardwareClient`).
- **Use scenarios** —
  - **Full lifecycle E2E** — the test starts a KV cluster (group-0 +
    data groups), seeds hardware (3 racks, 5 nodes, 8 disk-groups,
    disks), starts diskdb instances + chunkdb in-process, then via
    `ChunkdbClient` (R90): allocates a mirror chunk, queries it,
    appends strips, seals it, deletes it; verifies state transitions
    + that disk blocks are freed (diskdb `QueryCapacityStats` busy
    count returns to baseline).
  - **Topology refresh E2E** — the test starts the stack, triggers a
    disk-group status change (`Ok` → `Bad`) via `HardwareClient`,
    verifies chunkdb's `TopologyCache` updates (next allocation
    excludes the `Bad` disk-group) within the watch/notify latency
    (≤ 1s) or the periodic refresh interval (≤ 30s).
  - **Placement strategies E2E** — the test allocates a mirror chunk
    (3 copies) on a 3-rack cluster and verifies each replica is on a
    distinct rack (via diskdb `Segment` locations); allocates an EC
    chunk (8+4) and verifies blocks are across ≥3 racks, max 4 per
    node.
  - **Routing + migration E2E** — the test allocates chunks, triggers
    a bucket-range migration (update binding table in group-0),
    verifies dual-write during migration, verifies reads fall back to
    the old group, verifies cleanup deletes old copies after
    migration completes.
  - **Rollback E2E** — the test allocates a mirror chunk where one
    diskdb instance is configured to fail `AllocateBlocks`; verifies
    the successful allocations are freed (no orphan blocks) and the
    `AllocateChunk` call returns an error.
  - **Client retry E2E** — the test stops a chunkdb instance
    mid-call, verifies the `ChunkdbClient` retries against another
    registered instance and the call succeeds.

**Solution**:

**One-line summary**: add an E2E test suite under
`app/crow-chunkdb/tests/` that starts a real KV cluster + diskdb +
chunkdb in-process (reusing the `KvCluster` harness from diskdb tests)
and verifies the full stack — lifecycle, topology, placement, routing,
migration, rollback, client retry — via `ChunkdbClient`.

1. **E2E test harness** —
   `app/crow-chunkdb/tests/common/` (new, mirrors
   `app/crow-diskdb/tests/common/`):
   - `cluster.rs` — `ChunkdbCluster` helper: starts a `KvCluster`
     (group-0 + data groups), seeds hardware metadata (racks/nodes/
     disk-groups/disks) into group-0 via `HardwareClient`, starts
     diskdb instances (sync loop + allocate/free), starts chunkdb
     in-process (topology refresh + gRPC server), returns a
     `ChunkdbClient` connected to the chunkdb instance.
   - `fixtures.rs` — `make_chunk_id`, `make_disk_id`, default test
     topology (3 racks, 5 nodes, 8 disk-groups, 4 zones/disk, 128
     units/zone, 1 MB unit size — matching the diskdb E2E defaults).

2. **Lifecycle E2E test** —
   `app/crow-chunkdb/tests/lifecycle_e2e_test.rs`:
   - `allocate_mirror_chunk_e2e` — allocate a 3-copy mirror chunk,
     query it, verify `state=Active` + 3 strips with `Segment`s on
     distinct racks; append 2 strips; verify strip count grows; seal
     with `seal_length`; verify `state=Sealed`; delete; verify
     `state=Deleted` + diskdb busy count returns to baseline.
   - `allocate_ec_chunk_e2e` — allocate an 8+4 EC chunk, verify
     `state=Active` + 12 `Segment`s across ≥3 racks, max 4/node;
     `EC_STATE_NO_PARITY`; seal + delete; verify disk blocks freed.
   - `invalid_transition_e2e` — append to a sealed chunk →
     `FailedPrecondition`; seal a deleted chunk →
     `FailedPrecondition`.
   - `concurrent_seal_delete_e2e` — spawn 2 tasks, one seals + one
     deletes the same chunk; verify one wins, the other gets
     `Aborted`; no torn state.

3. **Topology E2E test** —
   `app/crow-chunkdb/tests/topology_e2e_test.rs`:
   - `disk_group_status_change_e2e` — set a disk-group `Bad` via
     `HardwareClient`; verify the next allocation excludes it (within
     watch/notify latency or refresh interval).
   - `node_maintenance_e2e` — set a node `Maintenance`; verify its
     disk-groups are excluded from placement.
   - `missed_notify_recovery_e2e` — drop the watch/notify connection,
     change a disk-group status, reconnect, verify the periodic
     refresh corrects the cache.

4. **Placement E2E test** —
   `app/crow-chunkdb/tests/placement_e2e_test.rs`:
   - `mirror_distinct_racks_e2e` — 3-copy mirror on 3 racks → each
     replica on a distinct rack (verify via `Segment` disk-group →
     node → rack mapping).
   - `ec_safe_mode_e2e` — 8+4 EC on 12 nodes/3 racks → 12 blocks,
     max 4/node, ≥3 racks.
   - `ec_unsafe_mode_e2e` — 8+4 EC on 3 nodes → unsafe fallback, 4/
     node; verify warning logged.
   - `negative_hint_e2e` — allocate with a negative hint excluding
     rack 1 → no `Segment` on rack 1.

5. **Routing + migration E2E test** —
   `app/crow-chunkdb/tests/routing_e2e_test.rs`:
   - `hash_routing_e2e` — allocate 100 chunks; verify they distribute
     across KV groups per the binding table (no single group has >
     2× the expected share).
   - `migration_dual_write_e2e` — allocate chunks in a bucket range,
     trigger migration (update binding table + migration state),
     verify dual-write during `Copying`, verify read fallback, verify
     cleanup deletes old copies.
   - `binding_cache_refresh_e2e` — update the binding table in
     group-0; verify chunkdb's binding cache updates within
     watch/notify latency.

6. **Rollback E2E test** —
   `app/crow-chunkdb/tests/rollback_e2e_test.rs`:
   - `partial_failure_rollback_e2e` — configure one diskdb instance
     to fail `AllocateBlocks`; allocate a 3-copy mirror chunk;
     verify the successful allocations are freed (diskdb busy count
     unchanged); `AllocateChunk` returns an error.

7. **Client retry E2E test** —
   `app/crow-chunkdb/tests/client_retry_e2e_test.rs`:
   - `transient_failure_retry_e2e` — stop a chunkdb instance
     mid-call; verify `ChunkdbClient` retries against another
     registered instance; call succeeds.
   - `not_leader_hint_retry_e2e` — inject a `NotLeaderHint`; verify
     the client refreshes + retries against the hint.

**Flow diagram**:

```
  ChunkdbCluster harness (item 1)
       │
       ├── start KvCluster (group-0 + data groups)
       ├── seed hardware (racks/nodes/dg/disks) via HardwareClient
       ├── start diskdb instances (sync loop + allocate/free)
       ├── start chunkdb (topology refresh + gRPC server)
       └── return ChunkdbClient
                │
                ▼
  E2E test files (items 2-7)
       │
       ├── lifecycle:  allocate → query → append → seal → delete
       ├── topology:    status change → verify cache + placement
       ├── placement:   mirror/EC → verify rack/node spread
       ├── routing:     hash → verify KV group distribution
       ├── migration:   dual-write → fallback → cleanup
       ├── rollback:    inject failure → verify no orphan blocks
       └── client:      stop instance → verify retry
                │
                ▼
  assertions via ChunkdbClient + HardwareClient + diskdb QueryCapacityStats
```

- **Edge cases at a glance**:
  - Test cluster startup race (chunkdb starts before diskdb is ready)
    → the harness waits for diskdb `disks_ready` (reuse
    `wait_for_disks_ready` from diskdb tests) before starting chunkdb.
  - Watch/notify latency > test timeout → tests use a generous
    timeout (≥ 2× refresh interval) for notify-dependent assertions;
    or trigger a synchronous refresh in the test to avoid flakiness.
  - Migration background copy task is slow → tests can drive the copy
    synchronously or poll for completion with a timeout.
  - KV cluster port conflicts across test runs → the harness uses
    ephemeral ports (reuse `KvCluster` pattern).
  - diskdb `AllocateBlocks` failure injection → the harness exposes a
    test-only flag on the diskdb instance to force-fail the next N
    allocations (or the test stops the diskdb instance mid-call).

**Dependencies**:

- **R85-R90** — all chunkdb components must be landed; E2E tests
  exercise the full stack.
- **diskdb core (R70-R76)** — the test harness starts real diskdb
  instances; reuses `app/crow-diskdb/tests/common/cluster.rs`
  patterns (`KvCluster`, `wait_for_disks_ready`).
- **crow-kv core** — the test harness starts a real KV cluster
  (group-0 + data groups).
- **`HardwareClient`** + **`ServiceRegistryClient`** in
  `crow-kv-client` — the harness seeds hardware metadata + discovers
  chunkdb endpoints.
- **`ChunkdbClient`** (R90) — the test driver for all chunkdb
  operations.

**Acceptance**:

**Lifecycle E2E**:
- `allocate_mirror_chunk_e2e` — allocate 3-copy mirror, query, append
  2 strips, seal, delete → state transitions `Active → Sealed →
  Deleted`; diskdb busy count returns to baseline after delete. E2E
  test.
- `allocate_ec_chunk_e2e` — allocate 8+4 EC → 12 `Segment`s across
  ≥3 racks, max 4/node, `EC_STATE_NO_PARITY`; seal + delete; disk
  blocks freed. E2E test.
- `invalid_transition_e2e` — append to sealed → `FailedPrecondition`;
  seal deleted → `FailedPrecondition`. E2E test.
- `concurrent_seal_delete_e2e` — one wins, other gets `Aborted`; no
  torn state. E2E test.

**Topology E2E**:
- `disk_group_status_change_e2e` — disk-group `Bad` → next allocation
  excludes it (within 30s). E2E test.
- `node_maintenance_e2e` — node `Maintenance` → its disk-groups
  excluded. E2E test.
- `missed_notify_recovery_e2e` — notify dropped → periodic refresh
  corrects cache within 30s. E2E test.

**Placement E2E**:
- `mirror_distinct_racks_e2e` — 3 copies on 3 distinct racks. E2E
  test.
- `ec_safe_mode_e2e` — 8+4, 12 blocks, max 4/node, ≥3 racks. E2E
  test.
- `ec_unsafe_mode_e2e` — 8+4 on 3 nodes → unsafe fallback, 4/node.
  E2E test.
- `negative_hint_e2e` — exclude rack 1 → no `Segment` on rack 1.
  E2E test.

**Routing + migration E2E**:
- `hash_routing_e2e` — 100 chunks distribute across KV groups per
  binding table (no group > 2× expected share). E2E test.
- `migration_dual_write_e2e` — dual-write during `Copying`; read
  fallback to old group; cleanup deletes old copies. E2E test.
- `binding_cache_refresh_e2e` — binding table update → cache
  refreshes within watch/notify latency. E2E test.

**Rollback E2E**:
- `partial_failure_rollback_e2e` — one diskdb fails `AllocateBlocks`
  → successful allocations freed (busy count unchanged); error
  returned. E2E test.

**Client retry E2E**:
- `transient_failure_retry_e2e` — stop chunkdb mid-call → client
  retries against another instance; succeeds. E2E test.
- `not_leader_hint_retry_e2e` — `NotLeaderHint` → client refreshes +
  retries against hint; succeeds. E2E test.

**Lint + test commands**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` (all E2E tests pass).

**Open Questions**:

- **Failure injection mechanism for rollback + retry tests** — how
  does the test force a diskdb `AllocateBlocks` failure or a chunkdb
  instance stop mid-call? Options: (a) test-only flags on the diskdb
  / chunkdb instances (e.g. `fail_next_n_allocations`); (b) stop the
  instance's gRPC server mid-call (coarse but realistic); (c) network
  partition (complex). Trade-off: (a) is precise but adds test-only
  code to production crates; (b) is realistic but timing-sensitive;
  (c) is the most realistic but heaviest. Recommendation: (a) for
  rollback (precise failure point), (b) for client retry (realistic
  instance loss). Design decision.
- **Migration test timing — drive synchronously or poll?** The
  migration background copy task runs async; the test needs to verify
  the dual-write + cleanup phases. Options: (a) expose a test-only
  synchronous migration driver (run the copy + cleanup inline); (b)
  poll for phase transitions with a timeout. Trade-off: (a) is
  deterministic but adds test-only code; (b) is realistic but
  potentially flaky. Recommendation: (b) with a generous timeout (≥
  10s) — realistic and the migration phases are observable via
  group-0 state. Design decision.
