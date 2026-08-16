<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R102: diskdb — Dynamic Disk-Group Binding Migration

## Problem

### Current behavior + impact

diskdb disk-group → paxos group binding is operator-manual today: the
operator writes `BindMapValue` (disk-group → `(store_id, group_id)`)
and `OwnerMapValue` (disk-group → diskdb instance) via the console
through `HardwareClient` (design §5 "Map semantics"). When a paxos
data group is added or a diskdb instance moves, the operator must
manually rebind each affected disk-group. This is operationally
fragile — a diskdb instance crash leaves its disk-groups unbound
until manual intervention, and adding capacity (a new paxos group)
requires the operator to manually rebalance disk-group assignments
for load distribution.

R99 built the common binding framework (binding client, binding
monitor, reject-and-retry protocol) for chunkdb instance sharding.
GAP-R99-5 decided that diskdb disk-group → paxos-group rebinding —
replacing the operator-manual `BindMapValue` write with automatic
monitoring + rebinding — should be a separate follow-up requirement.
This is R102: diskdb should reuse the same framework.

### Design pointers

- `doc/design/diskdb/design-crow-diskdb.md` §3.2 (disk-group → paxos
  group binding via a table, not hash — dynamic scaling without
  rehashing), §5 ("Map semantics" — `OwnerMapValue`,
  `BindMapValue` are operator-manual today).
- `doc/design/kv/design-crow-kv-group0.md` §2.1 (`crow-kv-client` is
  the single sysdata API surface), §2.6 (two monitoring models: push
  + pull).
- `doc/backlog/R99-kv-dynamic-range-binding-framework.md` — common
  binding framework (`RangeBindingClient`, binding monitor,
  reject-and-retry protocol).
- aioss analog: aioss uses manual metadb partition binding (operator
  assigns chunks to metadb partitions); CROW adds dynamic rebinding
  via the monitor.

### Use scenarios

- **New paxos group added** — operator adds a new paxos data group
  for capacity; the binding monitor detects the new group (via
  topology / service registry), rebalances disk-group assignments to
  spread load, rebinds affected disk-groups to the new paxos group.
- **diskdb instance crashes** — a diskdb instance crashes (service
  registry expiry); the binding monitor detects the loss, rebinds its
  disk-groups to surviving instances (updating `OwnerMapValue`), and
  rebinds their paxos groups if needed (updating `BindMapValue`).
  No operator intervention needed.
- **Operator triggers rebalance** — operator requests a load
  rebalance; the monitor splits disk-group assignments across paxos
  groups for load balancing, migrating data as needed.

## Solution

Reuse R99's common binding framework to dynamically rebind diskdb
disk-groups to paxos groups, replacing the operator-manual
`BindMapValue` write with automatic monitoring + rebinding.

### Work items

1. **diskdb binding schema** — `lib/crow-protocol/src/proto/sysdata_type.proto`
   (extend): add `DiskdbBindingValue` (disk-group-id → paxos-group-id
   `(store_id, group_id)` + diskdb instance endpoint); stored in
   group-0 with key pattern `/diskdb/dg_bind/<disk_group_id>`. Add a
   `DiskdbBindingMigrationValue` for migration state (old paxos group,
   new paxos group, state: `Copying`/`Cutover`/`Complete`), following
   the R99 `ChunkdbRangeMigrationValue` pattern. Add
   `ERROR_CODE_NOT_MY_BINDING` to `error_code.proto` for the
   reject-and-retry protocol.
2. **diskdb binding client** — `lib/crow-kv-client/src/` (extend):
   add `DiskdbBindingClient` (fetch + cache + watch/notify),
   following R99's `RangeBindingClient` pattern. Caches
   `DashMap<disk_group_id, (store_id, group_id, endpoint)>`,
   subscribes to watch/notify for real-time updates. Provides
   `route(disk_group_id) -> (store_id, group_id, endpoint)` for
   clients to route zone-record writes to the correct paxos group.
   Reject-and-retry: on `NotMyBinding` error, refresh the binding
   cache, re-route, retry — follows the `NotLeaderHint` retry pattern
   in `crow-kv-client/src/config.rs`.
3. **diskdb server binding enforcement** — `app/crow-diskdb/src/`
   (extend): add a binding guard (following `range_guard.rs` pattern
   from chunkdb). On every RPC, check the disk-group is bound to this
   instance's paxos group; reject with `NotMyBinding` error if not.
   Binding is read from group-0 at startup + updated via
   watch/notify. The existing `DdbDiskGroup.bind` field
   (`model/disk_group.rs:32`, `RwLock<(u64, u64)>`) is populated from
   the binding client instead of operator config.
4. **Monitor integration** — the binding monitor (relocated to
   `crow-kv-server` per R99 rework, `app/crow-kv-server/src/`) handles
   diskdb disk-group rebinding in addition to chunkdb range binding;
   same leader-gated background task. On paxos group add or diskdb
   instance join/leave (service registry event), computes a new
   disk-group → paxos-group assignment, writes the updated binding
   table to group-0, and triggers migration for rebound disk-groups.
5. **Migration flow** — when a disk-group is rebound to a new paxos
   group, the old paxos group stops accepting writes for that
   disk-group, zone records are migrated (or confirmed already
   replicated) to the new paxos group, and the new paxos group starts
   serving. States: `Copying` → `Cutover` → `Complete`, following the
   R103 chunkdb range migration pattern. During `Cutover`, reads
   serve from both old and new; writes go to the new paxos group
   only.

### Flow diagram

```
  binding monitor (crow-kv-server, leader-gated)
       │
       ├── monitor service registry + topology (paxos group add, diskdb instance join/leave)
       ├── on change: compute new disk-group → paxos-group assignment
       ├── write updated DiskdbBindingValue to group-0
       ├── write DiskdbBindingMigrationValue (Copying) to group-0
       │
       ▼
  watch/notify pushes update to all diskdb instances + clients
       │
       ├── old diskdb instance: binding guard rejects writes with NotMyBinding
       │   (includes current binding: new paxos group + endpoint)
       │
       └── new diskdb instance: binding guard accepts; serves writes on new paxos group
           │
           ├── Copying: migrate zone records (or confirm replication)
           ├── Cutover: reads from both, writes to new only
           └── Complete: old paxos group stops serving; new fully serves
```

### Edge cases at a glance

- **disk-group with no data** — fast cutover: `Copying` is a no-op
  (no zone records to migrate), jumps directly to `Cutover` →
  `Complete`.
- **Monitor crash mid-migration** — the `DiskdbBindingMigrationValue`
  persists in group-0; the new monitor leader reads it and resumes
  (or aborts if the new paxos group is unavailable).
- **Concurrent rebinding requests** — the monitor leader serializes
  rebinding writes (only the leader performs balancing); no
  conflicting concurrent migrations.

## Dependencies

- Depends on **R99** — common binding framework (`RangeBindingClient`
  pattern, binding monitor, reject-and-retry protocol).
- **R99 rework** (Phase 4-5 of `doc/working/plan-gaps-r99-r100.md`)
  must land first — the binding monitor must be relocated to
  `crow-kv-server` (leader-gated background task on group-0
  replicas) before diskdb rebinding can be added to the same
  monitor.
- **R103** (chunkdb range migration) is a sibling requirement — both
  reuse the common framework's migration state pattern; R102 and R103
  can proceed in parallel but should share the migration state
  abstractions.

## Acceptance

### diskdb binding schema

- `DiskdbBindingValue` encodes `disk_group_id` → `(store_id,
  group_id)` + instance endpoint → round-trip serialize/deserialize
  preserves all fields. `[Unit test]`
- `DiskdbBindingMigrationValue` encodes old/new paxos group + state
  → round-trip preserves all fields and state enum. `[Unit test]`
- Binding value stored at `/diskdb/dg_bind/<disk_group_id>` in
  group-0 → `DiskdbBindingClient` fetches and decodes it correctly.
  `[Integration test]`

### diskdb binding client

- `DiskdbBindingClient::route(disk_group_id)` with a cached binding
  → returns the correct `(store_id, group_id, endpoint)`. `[Unit test]`
- Binding table updated in group-0 → watch/notify fires →
  `DiskdbBindingClient` cache updates within 1s → next `route`
  returns the new binding. `[Integration test]`
- Client receives `NotMyBinding` → refreshes binding cache →
  re-routes → retries against the correct paxos group → succeeds.
  `[Integration test]`

### diskdb server binding enforcement

- diskdb instance receives an RPC for a disk-group bound to its
  paxos group → binding guard allows → RPC processes normally.
  `[Unit test]`
- diskdb instance receives an RPC for a disk-group NOT bound to its
  paxos group → binding guard rejects with `NotMyBinding` including
  the current binding (new paxos group + endpoint). `[Integration test]`
- `DdbDiskGroup.bind` is populated from the binding client at startup
  (not operator config) → assert `bind` matches group-0 value.
  `[Integration test]`

### Monitor integration

- A new paxos data group is added to the topology → monitor detects
  it within the polling interval → rebalances disk-group assignments
  → writes updated bindings to group-0 → affected diskdb instances
  pick up the new binding within 2× the polling interval.
  `[Integration test]`
- A diskdb instance crashes (service registry expiry) → monitor
  rebinds its disk-groups to surviving instances → updates
  `OwnerMapValue` + `DiskdbBindingValue` in group-0 → clients refresh
  + retry against the new owner. `[Integration test]`
- Monitor follower (non-leader) does not perform rebinding → assert
  no binding writes from followers. `[Integration test]`

### Migration flow

- disk-group rebound: migration value written as `Copying` → zone
  records migrated to new paxos group → state advances to `Cutover`
  → reads serve from both → writes go to new only → monitor
  finalizes `Complete` → old paxos group stops serving.
  `[Integration test]`
- disk-group with no data: `Copying` is a no-op → fast cutover to
  `Complete` → new paxos group serves immediately. `[Integration test]`
- E2E test: full migration lifecycle driven by monitor with live
  clients → assert no permanent request failures beyond transient
  `NotMyBinding` redirects. `[E2E test]`

### Edge cases

- Monitor leader crashes mid-migration → new leader reads persisted
  `DiskdbBindingMigrationValue` → migration resumed or aborted
  consistently. `[Integration test]`
- Concurrent rebinding requests for different disk-groups → monitor
  leader serializes → both complete without conflict.
  `[Integration test]`
- Client with stale cache during `Cutover` sends write to old paxos
  group → `NotMyBinding` → client follows hint and retries on new
  paxos group successfully. `[Integration test]`

### Build & style

- `pixi run test-diskdb`
- `pixi run test-kv-server`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`

## Open Questions

- **Copy data or just rebind?** If zone records are already
  replicated across paxos groups (e.g. via a future cross-group
  replication mechanism), migration can skip the `Copying` phase and
  jump to `Cutover`. If not, zone records must be copied from the old
  paxos group to the new one. Decision needed: does CROW v1 guarantee
  cross-group replication, or must R102 implement a data copy phase?
- **Rebalance algorithm** — how does the monitor decide which
  disk-groups to rebind when a new paxos group is added? Options:
  (a) round-robin reassignment, (b) load-aware (rebind the most
  loaded disk-groups to the new group). Needs a design decision.
