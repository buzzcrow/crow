<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R102: diskdb — Dynamic Disk-Group Binding Migration

## Problem

### Current behavior + impact

diskdb disk-group → KV-group binding is operator-manual today: the
operator writes `BindMapValue` (disk-group → `(store_id, group_id)`)
and `OwnerMapValue` (disk-group → diskdb instance) via the console
through `HardwareClient` (design §5 "Map semantics"). Three gaps
remain for cluster-expansion scenarios:

1. **No safe migration flow on bind change.** When an existing
   disk-group is rebound to a different KV-group, the operator
   writes a new `BindMapValue` directly — but the disk-group's zone
   records (busy/free/snapshot) are physically stored on the old
   KV-group, keyed by `disk_id` (no `disk_group_id`/`group_id` in
   the key). The diskdb sync loop silently overwrites `dg.bind`, so
   recovery/compaction/scanner immediately scan the new KV-group and
   conclude old busy blocks are free (data corruption), and frees of
   old blocks target the wrong group (leaks). The existing disk-move
   code (`http_move_disk` + `copy_disk_records`) has the right shape
   (Maintenance quiescence + copy + flip) but uses a fixed 10 s sleep
   with a latent free/compaction race during the copy, and is
   disk-scope only — there is no disk-group-scope bind-change flow.
2. **No binding client or enforcement.** Clients route zone-record
   writes to a KV-group with no check that the disk-group is actually
   bound there; a stale route silently corrupts. diskdb instances
   accept RPCs for any disk-group with no `NotMyBinding` reject — a
   misrouted write goes to the wrong KV-group undetected.
3. **No imbalance visibility.** The operator has no warning when the
   disk-group → KV-group distribution is significantly imbalanced
   (e.g. one KV-group hosts far more disk-groups than others after
   uneven growth). Rebalancing is left to operator vigilance.

R99 built the common binding framework (binding client, binding
monitor, reject-and-retry protocol) for chunkdb instance sharding.
GAP-R99-5 decided that diskdb disk-group → KV-group rebinding should
be a separate follow-up requirement. This is R102: diskdb reuses the
binding client + enforcement + reject-and-retry protocol (work items
1-3) and the binding monitor (work item 4, warning-only — diskdb bind
changes are rare; auto-rebalancing is appropriate for chunkdb but
not for diskdb), and adds the safe migration flow (work item 5)
shared between disk-group bind change and disk move.

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

### Use scenarios

diskdb bind changes are rare in practice — the common capacity-
expansion patterns do not require rebinding:

- **New node + new KV-group (common, no migration)** — operator adds
  a new node, creates a new KV-group, adds the node's disk-groups,
  and binds them to the new KV-group at creation time. The disk-groups
  are new (no records); the bind is set before any writes. No
  migration needed. R102's binding client + enforcement (work items
  1-3) ensure clients route to the correct KV-group from the start.
- **New node, KV-group replica moved (common, no diskdb migration)**
  — KV-groups are pre-created (replicas on existing nodes); adding a
  node means moving a KV-group replica to the new node (Paxos
  reconfiguration — journal replication to the new replica). The
  disk-group → KV-group binding does not change; diskdb records stay
  on the same KV-group. This is a KV-layer migration, transparent to
  diskdb. Not R102's scope.
- **Rebind existing disk-group to a different KV-group (rare,
  requires migration)** — an existing disk-group with live records is
  rebound from KV-group A to KV-group B (e.g. to evacuate a KV-group
  that is being retired, or to rebalance after uneven growth). The
  operator triggers the rebind via the console; the console drives
  the five-step migration flow (work item 5). This is the case that
  requires copying records.
- **diskdb instance crashes (rare, ownership reassignment)** — a
  diskdb instance crashes (service registry expiry); the monitor
  warns about the lost instance. The operator reassigns its
  disk-groups to a surviving instance (updating `OwnerMapValue`). If
  the surviving instance is on the same KV-group, no record migration
  is needed (just an ownership change). If on a different KV-group,
  the operator triggers the five-step migration flow.
- **Monitor warns about imbalance** — the binding monitor observes
  the disk-group → KV-group distribution and emits a warning when it
  is significantly imbalanced (e.g. one KV-group hosts far more
  disk-groups than others). The operator decides whether to act
  (trigger rebinds via the console) or ignore. The monitor does NOT
  auto-trigger rebinding for diskdb — unlike chunkdb (R99), where
  auto-rebalancing is appropriate.

## Solution

Reuse R99's common binding framework for the binding client +
enforcement + reject-and-retry protocol (work items 1-3). The binding
monitor (work item 4) observes the disk-group → KV-group distribution
and emits imbalance warnings, but does NOT auto-trigger rebinding for
diskdb — rebinding is operator-triggered via the console (diskdb bind
changes are rare; auto-rebalancing is appropriate for chunkdb but not
for diskdb). Bind change and disk move share one five-step migration
flow (Maintenance → copy → switch → cleanup → Up) at different scopes
(work item 5), reusing the existing `http_move_disk` +
`copy_disk_records` pattern.

### Work items

1. **diskdb binding schema** — `lib/crow-protocol/src/fbs/common_type.fbs`
   (extend): add `DiskdbBindingValue` (disk-group-id → paxos-group-id
   `(store_id, group_id)` + diskdb instance endpoint); stored in
   group-0 with key pattern `/diskdb/dg_bind/<disk_group_id>`. Add a
   `MigrationIntentValue` carrying the migration scope (disk-group or
   single disk), `old_bind` `(store_id, group_id)`, `new_bind`
   `(store_id, group_id)`, and optional `disk_id` (present for
   disk-level moves, absent for disk-group bind changes); stored at
   `/diskdb/migrate/<disk_group_id>` (disk-group scope) or
   `/diskdb/migrate/<disk_group_id>/<disk_id_hex>` (disk scope). The
   intent serves double duty: a write-quiescence signal (the diskdb
   instance reads it and suspends frees + compaction for the affected
   disk/disk-group during the copy, before the bind flip) and a
   crash-recovery marker (a new monitor leader reads it and resumes).
   No `Copying`/`Cutover`/`Complete` state enum — progress is derived
   from comparing the current `BindMapValue` to `new_bind` (copy done
   iff bind already points to `new_bind`). Add
   `ERROR_CODE_NOT_MY_BINDING` to `ret_code.fbs` for the
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
4. **Monitor integration (warning-only)** — the binding monitor
   (relocated to `crow-kv-server` per R99 rework,
   `app/crow-kv-server/src/`) observes the disk-group → KV-group
   distribution alongside chunkdb range binding; same leader-gated
   background task. For diskdb, the monitor computes the current
   distribution and emits an imbalance warning (log + metric) when
   one KV-group hosts significantly more disk-groups than others, or
   when a diskdb instance is absent from the service registry (crash
   suspicion). The monitor does NOT auto-trigger rebinding or
   migration for diskdb — unlike chunkdb (R99) where auto-rebalancing
   is appropriate. Rebinding is operator-triggered via the console
   (work item 5's flow). The monitor's warning gives the operator the
   signal to act; the console provides the safe migration path.
5. **Migration flow** — disk-group bind change and disk move share
   one flow at different scopes (whole disk-group vs one disk), reusing
   the existing `http_move_disk` + `copy_disk_records` pattern
   (`app/crow-web/src/lifecycle.rs`). Both are operator-triggered via
   the console (the monitor only warns; it does not drive migration).
   The flow is five steps:
   (a) **Quiesce** — the console sets the disk-group (or disk) to
   `Maintenance` in group 0 and writes a `MigrationIntentValue`;
   allocates block (`permits(Maintenance, Allocate) = false`); the
   diskdb instance reads the intent on the next sync tick (or via
   watch/notify) and suspends frees + compaction for the affected
   disk/disk-group; reads still serve from the old bind (`dg.bind`
   unchanged).
   (b) **Copy** — `copy_disk_records(old_bind, new_bind, disk_id)`
   per disk (one disk for disk move; every disk in the group for
   bind change): prefix-scan `ZoneKey`/`BusyBlockKey`/`FreeBlockKey`
   by `DiskId` from the old bind, batch-write to the new bind.
   Idempotent — re-running overwrites the same keys, so crash-resume
   is safe.
   (c) **Switch** — write the new `BindMapValue` to group 0 (bind
   change) or add the disk to its new disk-group placement (disk
   move, whose new disk-group already has its own bind). The next
   sync tick updates `dg.bind` → new bind; reads + writes now target
   the new bind. The diskdb instance clears its in-memory migrating
   flag (frees + compaction resume on the new bind).
   (d) **Cleanup** — delete the migrated records from the old bind
   (per-disk prefix scan + `batch_write` of `Delete` ops). For disk
   move, only the moved disk's records (other disks still use the
   old bind); for bind change, all disks in the group.
   (e) **Resume** — set the disk-group (or disk) to `Up` and delete
   the `MigrationIntentValue`; allocates resume on the new bind.
   No `Copying`/`Cutover`/`Complete` state machine, no dual-reads —
   `Maintenance` is the quiescence signal, the bind write is the
   cutover, the intent deletion is the completion marker.

### Flow diagram

```
  driver: console (crow-web), operator-triggered
          (monitor only warns — does not drive migration)
       │
  (a) QUIESCE
       ├── set disk-group (or disk) to Maintenance in group 0
       ├── write MigrationIntentValue { old_bind, new_bind, disk_id? } to group 0
       ▼
  watch/notify / next sync tick → diskdb instance
       ├── allocates blocked (Maintenance)
       ├── suspends frees + compaction for affected disk/disk-group (intent)
       └── reads still serve from old bind (dg.bind unchanged)
       │
  (b) COPY  — copy_disk_records(old_bind, new_bind, disk_id) per disk
       ├── prefix-scan ZoneKey / BusyBlockKey / FreeBlockKey by DiskId from old bind
       └── batch_write Put to new bind  (idempotent — safe to re-run on resume)
       │
  (c) SWITCH
       ├── bind change: write new BindMapValue to group 0
       ├── disk move: add disk to new placement (new disk-group's bind already set)
       ▼
  next sync tick → diskdb updates dg.bind → new bind
       ├── reads + writes now target new bind
       └── diskdb clears migrating flag (frees + compaction resume on new bind)
       │
  (d) CLEANUP — delete migrated records from old bind (per-disk prefix scan + batch_delete)
       │
  (e) RESUME
       ├── set disk-group (or disk) to Up
       └── delete MigrationIntentValue → allocates resume on new bind
```

### Edge cases at a glance

- **disk-group with no data** — fast flip: copy is a no-op (no zone
  records to scan), switch + cleanup + resume proceed immediately.
- **Console crash mid-migration** — the `MigrationIntentValue`
  persists in group-0; the operator retries (or a new console
  session reads the intent) and resumes from the appropriate step
  (re-run copy — idempotent; then switch; then cleanup; then
  resume). The diskdb instance keeps the migrating flag set (frees
  + compaction stay suspended) until the intent is deleted.
- **Free or compaction during copy** — the diskdb instance's
  migrating flag (from the intent) suspends frees + compaction for
  the affected disk/disk-group before the bind flip, so no
  `Delete BusyBlockKey + Put FreeBlockKey` or compaction
  `batch_write` races with the copy. After the flip, frees +
  compaction resume on the new bind and are correct.
- **Concurrent rebinding requests** — rebinding is operator-triggered
  via the console; the console serializes bind changes and disk moves
  on the same disk-group via the `MigrationIntentValue` (one intent
  per scope key; the console rejects a second intent while one
  exists). The monitor does not trigger concurrent migrations (it
  only warns).
- **Disk move to a disk-group on the same kv-group** — copy is a
  no-op (old_bind == new_bind); the flow still quiesces, flips the
  placement, and resumes. Detected by comparing `old_bind` and
  `new_bind` in the intent.

## Dependencies

- Depends on **R99** — common binding framework (`RangeBindingClient`
  pattern, binding monitor, reject-and-retry protocol).
- **R99 rework** (Phase 4-5 of `doc/working/plan-gaps-r99-r100.md`)
  must land first — the binding monitor must be relocated to
  `crow-kv-server` (leader-gated background task on group-0
  replicas) before diskdb rebinding can be added to the same
  monitor.
- **R103** (chunkdb range migration) is a sibling requirement. R102
  and R103 no longer share a migration state machine — chunkdb holds
  user chunk data and may need online dual-read migration (R103),
  while diskdb is a metadata allocator whose brief write pause with
  retry is tolerable, so R102 uses the simpler quiesce + copy + flip
  flow. They can still proceed in parallel.
- **Disk move** (`app/crow-web/src/lifecycle.rs:1630`
  `http_move_disk` + `copy_disk_records`) is the existing
  disk-scope migration; R102 generalizes the same flow to
  disk-group scope and adds the `MigrationIntentValue` for
  write-quiescence + crash safety (the current disk-move code uses
  a fixed 10 s sleep and has a latent free/compaction race during
  the copy — R102 fixes both by sharing the intent-based flow).

## Acceptance

### diskdb binding schema

- `DiskdbBindingValue` encodes `disk_group_id` → `(store_id,
  group_id)` + instance endpoint → round-trip serialize/deserialize
  preserves all fields. `[Unit test]`
- `MigrationIntentValue` encodes scope (disk-group or disk) +
  `old_bind` + `new_bind` + optional `disk_id` → round-trip
  preserves all fields. `[Unit test]`
- Binding value stored at `/diskdb/dg_bind/<disk_group_id>` in
  group-0 → `DiskdbBindingClient` fetches and decodes it correctly.
  `[Integration test]`
- Migration intent stored at `/diskdb/migrate/<disk_group_id>` (or
  `.../<disk_id_hex>`) → console + diskdb instance fetch and decode
  it correctly. `[Integration test]`

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

### Monitor integration (warning-only)

- The disk-group → KV-group distribution is significantly imbalanced
  (one KV-group hosts far more disk-groups than others) → monitor
  emits an imbalance warning (log + metric) within the polling
  interval → assert no `BindMapValue` or `OwnerMapValue` writes from
  the monitor. `[Integration test]`
- A diskdb instance is absent from the service registry (crash
  suspicion) → monitor emits a warning → assert no auto-rebind of
  its disk-groups (the operator triggers reassignment via the
  console). `[Integration test]`
- Monitor follower (non-leader) does not emit warnings or perform
  writes → assert no binding writes + no warning metrics from
  followers. `[Integration test]`

### Migration flow

- disk-group bind change (A → B): console sets disk-group to
  `Maintenance` + writes `MigrationIntentValue` → diskdb suspends
  frees + compaction → `copy_disk_records` copies all disks'
  records from A to B → console writes new `BindMapValue` → diskdb
  updates `dg.bind` → B → console deletes old records from A →
  console sets disk-group to `Up` + deletes intent → allocates
  resume on B. `[Integration test]`
- disk move (old disk-group → new disk-group on a different
  kv-group): same five-step flow at disk scope — disk set to
  `Maintenance`, one disk's records copied, disk added to new
  placement, old records deleted, disk set to `Up`. Reuses
  `copy_disk_records`. `[Integration test]`
- disk-group with no data: copy is a no-op (no records to scan) →
  switch + cleanup + resume proceed immediately → new bind serves
  allocates. `[Integration test]`
- disk move to a disk-group on the same kv-group (`old_bind ==
  new_bind`): copy is skipped → placement flip + resume proceed.
  `[Integration test]`
- E2E test: full bind-change lifecycle driven by the console with
  live clients → assert no permanent request failures beyond
  transient `NotMyBinding` redirects, and no busy-block records lost
  (a block allocated before the migration is still busy on the new
  bind after resume). `[E2E test]`

### Edge cases

- Console crashes mid-migration → `MigrationIntentValue` persists →
  operator retries (or a new console session reads the intent) →
  re-runs copy (idempotent) → completes switch + cleanup + resume.
  `[Integration test]`
- Free or compaction attempted during copy (before bind flip) →
  diskdb migrating flag suspends it → no `Delete BusyBlockKey` or
  compaction `batch_write` races with the copy → records on new
  bind match old bind exactly. `[Integration test]`
- Concurrent rebinding requests for different disk-groups → console
  serializes via per-scope intents → both complete without conflict.
  `[Integration test]`
- Second migration intent for the same scope while one exists →
  console rejects (one intent per scope key) → no conflicting
  concurrent migrations. `[Integration test]`
- Client with stale binding cache sends write to old paxos group
  after the flip → `NotMyBinding` → client refreshes cache +
  retries on new paxos group successfully. `[Integration test]`

### Build & style

- `pixi run test-diskdb`
- `pixi run test-kv-server`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`

## Open Questions

- **Copy data or just rebind?** Resolved: **copy is required.** Zone
  records are physically stored on the disk-group's bound kv-group
  (`dg.bind` routes every `persist_busy`/`persist_free`/`put_zone`/
  recovery/compaction/scanner read+write). Record keys are
  `disk_id`-keyed (no `disk_group_id`/`store_id`/`group_id`), so a
  bare bind change to a different kv-group leaves the records
  stranded on the old kv-group — the sync loop silently overwrites
  `dg.bind`, after which recovery/compaction/scanner scan the new
  kv-group and conclude old busy blocks are free (data corruption)
  and frees of old blocks target the wrong group (leaks). The
  five-step flow copies records before the flip. No cross-group
  replication is assumed in CROW v1.
- **Imbalance warning threshold** — what ratio triggers the
  monitor's imbalance warning (e.g. max/min disk-groups per KV-group
  > 2×, or absolute deviation from the mean)? Needs a tunable
  default + a design decision on whether to warn on disk-group count
  alone or also factor in per-disk-group capacity/usage.
