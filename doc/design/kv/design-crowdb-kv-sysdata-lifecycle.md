<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Sysdata Lifecycle (ID Reuse Safety + Disk Move)

Depends on: `design-crowdb-kv-group0.md` (sysdata schema), `design-crowdb-kv-server.md` (HTTP mgmt API), `design-crowdb-diskdb.md` (disk status management), `design-crowdb-console.md` (console API)
Satisfies: `design-crowdb-kv-group0.md` §2.8 (hardware admin), `design-crowdb-kv-server.md` §2.4 (internal mgmt API), `design-crowdb-diskdb.md` §6 (Init-state zone load)

This sub-design covers the lifecycle of group-0 sysdata records
across add/remove/move/reset operations: server-side group/store
cleanup, client cache eviction, cascading deletes, console sysdata
sync, diskdb Init-state zone load, disk move, and cluster reset.
Architecture decisions live in the root docs linked above; this doc
provides the implementation detail for each lifecycle path.

## Table of Contents

- [1. Server-side group/store cleanup](#1-server-side-groupstore-cleanup)
- [2. Client cache eviction + rename](#2-client-cache-eviction--rename)
- [3. HardwareClient cascading cleanup](#3-hardwareclient-cascading-cleanup)
- [4. Console sysdata sync — rack/node](#4-console-sysdata-sync--racknode)
- [5. Console disk-group/disk handlers](#5-console-disk-groupdisk-handlers)
- [6. diskdb Init-state zone load](#6-diskdb-init-state-zone-load)
- [7. Disk move](#7-disk-move)
- [8. Cluster reset](#8-cluster-reset)
- [9. Invariants](#9-invariants)
- [10. Test Design](#10-test-design)

## 1. Server-side group/store cleanup

### 1.1 Why

`PxKvStore::remove_group` cancels the tenure token and removes the
group from the in-memory `DashMap`, but does not delete the WAL dir,
engine dir, or update `node-config.json`. On server restart, the
group resurrects from the persisted node config — stale state from a
removed group collides with a new group re-created at the same
`(store_id, group_id)`. Same gap for `remove_store`. The HTTP
handlers call `PxKvStore` but never touch `node_config` or the
filesystem.

### 1.2 Group cleanup handler

`remove_group` handler (`app/crowdb-kv-server/src/mgmt/group_ops.rs`):

a. Get the store from the registry (existing).
b. Call `store.remove_group(gid)` — cancels tenure, removes from
   DashMap (existing).
c. Delete engine dir: `store_crowdb_tree_path(&config.data_root, sid, gid)`.
   Use `tokio::fs::remove_dir_all`.
d. Delete WAL group segments: the WAL root is
   `store_wal_root(&config.wal_root, sid)`; WAL segments are files
   within this dir named by group. Delete only the group's segment
   files, not the whole store dir (the store dir may contain other
   groups' segments). Scan the dir and delete files matching the
   group's naming pattern; if no files remain, leave the empty dir.
e. Call `node_config_store.remove_group(sid, gid)` to update
   `node-config.json`.
f. Log at `info!` level: `store_id`, `group_id`, "group removed +
   dirs deleted + node_config updated".

Edge cases:
- Dir deletion fails (permission, partial) → log `warn!`, continue
  with node_config update. The node_config update is the critical
  fence against resurrection; a stale dir is harmless.
- `node_config_store` not available (test env) → skip silently.
- Tenure cancellation race: `PxKvStore::remove_group` cancels the
  token synchronously; running tasks hold `Arc<PxGroup>` until they
  notice the cancel. Dir deletion while a task is mid-write is safe;
  the task writes to a file handle it already opened; deleting the dir
  removes the directory entry but the file remains until the handle
  closes.

### 1.3 Store cleanup handler

`remove_store` handler (`app/crowdb-kv-server/src/mgmt/store_ops.rs`):

a. Remove the store from the registry + graceful shutdown (existing).
b. Delete engine store dir: `{data_root}/store{sid}/` (cascades all
   group subdirs).
c. Delete WAL store dir: `store_wal_root(&config.wal_root, sid)` —
   the whole store dir, all group segments.
d. Call `node_config_store.remove_store(sid)`.

Edge cases: same as group cleanup.

### 1.4 Exposing NodeConfigStore to handlers

`KvStoreRegistry` has `config: CrowDBConfig` which holds
`config_root: PathBuf`. The handler builds a
`NodeConfigStore::new(&config.config_root)` on each call (cheap:
just a path join). No need to store a long-lived `NodeConfigStore` in
the registry; the handler is a cold path.

## 2. Client cache eviction + rename

### 2.1 Why

`TopologyCache::merge` only inserts into `leaders` and `replicas`;
never evicts. A removed group's stale leader endpoint self-heals via
`NotLeaderHint`, but the stale entry lingers. `write_slot_highwater`
is the critical case: a stale `min_slot` high-watermark does NOT
self-heal. A `MinSlot` read against a reused group ID with a stale
high-watermark silently returns empty results forever (new group's
revisions start from 0, but the high-watermark is stuck at the old
group's max revision).

### 2.2 TopologyCache::merge eviction

`merge` iterates `body.stores` and inserts. Eviction:

a. Before the insert loop, collect the set of `(store_id, group_id)`
   present in the fresh `body` into a `HashSet<(u64, u64)>`.
b. After the insert loop, remove entries from `leaders` and
   `replicas` whose key is not in the fresh set.
c. The eviction is best-effort; if `merge` is called with a partial
   topology, this would evict valid entries. Mitigation:
   `fetch_and_merge` fetches from a single seed and gets the full
   topology body. If the seed returns a partial view, the eviction
   would temporarily drop entries that reappear on the next refresh.
   Acceptable; the stale leader self-heals via `NotLeaderHint`.

Edge cases:
- Empty body (all stores gone) → evict everything. Correct.
- Body fetch from a stale seed → temporary over-eviction; self-heals
  on next refresh from a fresh seed.

### 2.3 write_slot_highwater eviction

The `write_slot_highwater` field (renamed from `write_watermark`,
accurate: it's a paxos slot number, not a generic watermark;
"highwater" conveys monotonic-max) lives on `CrowdbClient`, not
`TopologyCache`. A callback from `TopologyCache::merge` to
`CrowdbClient` wires the eviction:

a. `TopologyCache::new` takes an optional `eviction_hook`:
   `Option<Arc<dyn Fn(&HashSet<(u64, u64)>) + Send + Sync>>`.
b. `merge` collects evicted keys (the set difference between current
   cache keys and fresh body keys), calls the hook with the evicted
   set.
c. `CrowdbClient::new` creates the cache with a hook that removes
   evicted keys from `write_slot_highwater`.

Edge cases:
- No hook (standalone `TopologyCache` in tests) → skip eviction
  callback, just evict from `leaders`/`replicas`.
- Hook is a simple DashMap remove, cannot fail.

## 3. HardwareClient cascading cleanup

### 3.1 Why

`HardwareClient::remove_*` only deletes the single record for that
entity. No cascading cleanup of derived records. Removing a
disk-group leaves its `OwnerMapKey`, `BindMapKey`,
`DiskGroupUsageKey`, and all child `DiskKey` records orphaned in
group 0. Removing a node leaves all child disk-groups + disks.
Removing a rack leaves all child nodes + their disk-groups + disks.

### 3.2 Cascading remove methods

`remove_*_cascade` methods on `HardwareClient` (existing single-record
`remove_*` kept for internal use):

a. `remove_disk_cascade(rack, node, dg, disk_id)`:
   - `remove_disk(rack, node, dg, disk_id)` (existing)
   - Disks have no derived records below them in group 0. (Zone/Busy/
     Free records live on the disk-group's bind, not group 0.)

b. `remove_disk_group_cascade(rack, node, dg_id)`:
   - List disks in the group: `list_disks_in_group(rack, node, dg_id)`
   - For each disk: `remove_disk(rack, node, dg_id, &disk_id)`
   - `remove_owner(rack, node, dg_id)` (existing)
   - `remove_bind(rack, node, dg_id)` (existing)
   - Remove `DiskGroupUsageKey` for `dg_id`
   - `remove_disk_group(rack, node, dg_id)` (existing)

c. `remove_node_cascade(rack, node_id)`:
   - List disk-groups on the node: `list_disk_groups_on_node(rack, node_id)`
   - For each dg: `remove_disk_group_cascade(rack, node_id, dg_id)`
   - `remove_node(rack, node_id)` (existing)

d. `remove_rack_cascade(rack_id)`:
   - List nodes in the rack: `list_nodes_in_rack(rack_id)`
   - For each node: `remove_node_cascade(rack_id, node_id)`
   - `remove_rack(rack_id)` (existing)

Edge cases:
- List fails mid-cascade → log `warn!`, continue with what was
  listed. Partial cleanup is better than no cleanup.
- Concurrent add during cascade → the new entity's records survive
  (the cascade lists before deleting; a concurrent add after the list
  is not in the list). Correct.

## 4. Console sysdata sync — rack/node

### 4.1 Why

The rack/node add/remove HTTP handlers only mutate the console config
TOML. They never call `HardwareClient` to sync group 0 sysdata. Only
`cluster_init` Phase 5 writes sysdata (one-time bootstrap).

### 4.2 HardwareClient helper for handlers

`build_hardware_client` in `app/crowdb-web/src/mgmt.rs`:

```rust
pub(crate) async fn build_hardware_client(
    state: &AppState,
) -> Option<HardwareClient>
```

Finds any node hosting group 0 via the monitor cache, seeds a
`CrowdbClient` with that endpoint, and wraps it in a
`HardwareClient`. Returns `None` if no group-0 endpoint is known.

### 4.3 Rack/node handler changes

- `http_add_rack`: after `cfg.add_rack` + persist, call
  `hw.add_rack(id, &RackValue { ... })`.
- `http_remove_rack`: after `cfg.remove_rack` + persist, call
  `hw.remove_rack_cascade(id)`.
- `http_add_node`: after `cfg.add_node` + persist, call
  `hw.add_node(rack_id, id, &NodeValue { ... })`.
- `http_remove_node`: after `cfg.remove_node` + persist, call
  `hw.remove_node_cascade(rack_id, id)`.

Edge cases:
- Group 0 not yet initialized → `build_hardware_client` returns None
  → skip sysdata sync, log `warn!`. The config TOML is updated; when
  `cluster_init` runs, Phase 5 writes the full hierarchy.
- Sysdata write fails → log `warn!`, return success. The console
  config is the operator's intent; sysdata is derived.

## 5. Console disk-group/disk handlers

### 5.1 Why

Disk-group/disk console HTTP handlers provide add/remove/move flows
for operators via the console API.

### 5.2 ConsoleConfig extensions

`DiskGroupEntry` and `DiskEntry` structs in
`lib/crowdb-console-shared/src/config.rs`:

```rust
pub struct DiskGroupEntry {
    pub id: DiskGroupId,
    pub node_id: NodeId,
    pub rack_id: RackId,
    pub name: String,
}

pub struct DiskEntry {
    pub disk_id: String,
    pub disk_group_id: DiskGroupId,
    pub node_id: NodeId,
    pub rack_id: RackId,
    pub disk_type: String,
    pub capacity_bytes: u64,
    pub zone_size_bytes: u64,
    pub unit_size_bytes: u32,
}
```

`ConsoleConfig` holds `disk_groups: Vec<DiskGroupEntry>` and
`disks: Vec<DiskEntry>`. `add_disk_group` / `remove_disk_group` /
`add_disk` / `remove_disk` methods follow the existing rack/node
pattern (duplicate check, foreign key check, conflict-on-remove).

### 5.3 HTTP handlers

- `http_add_disk_group` — POST `/api/nodes/:node_id/disk-groups`
- `http_remove_disk_group` — DELETE `/api/nodes/:node_id/disk-groups/:dg_id`
- `http_list_node_disk_groups` — GET `/api/nodes/:node_id/disk-groups`
- `http_get_node_disk_group` — GET `/api/nodes/:node_id/disk-groups/:dg_id`
- `http_add_disk` — POST `/api/nodes/:node_id/disk-groups/:dg_id/disks`
- `http_remove_disk` — DELETE `/api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`
- `http_list_disks_in_group` — GET `/api/nodes/:node_id/disk-groups/:dg_id/disks`
- `http_get_disk` — GET `/api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`
- `http_move_disk` — POST `/api/disks/:disk_id/move` (see §7)

Each add/remove handler follows the rack/node pattern: update
`ConsoleConfig` + persist + call `HardwareClient` method.

### 5.4 ConsoleClient methods

`add_disk_group`, `remove_disk_group`, `list_disk_groups`, `add_disk`,
`remove_disk`, `list_disks`, `move_disk`: thin HTTP wrappers in
`lib/crowdb-console-shared/src/clients/console.rs`.

### 5.5 CLI commands

`DiskGroupVerb { Add, Remove, List }` in
`app/crowdb-cli/src/commands/disk_group.rs`.
`DiskVerb { Add, Remove, List, Move }` in
`app/crowdb-cli/src/commands/disk.rs`.
Both follow the existing `rack.rs` pattern.

## 6. diskdb Init-state zone load

### 6.1 Why

`disk_add_init` created empty in-memory `DdbZone`s for every new
disk, wrote baseline `ZoneValue` records, then called
`rebuild_allocating_disks`, adding the disk to the allocating set
with empty zones. If the disk has existing KV records (ownership
transfer, disk-group reassignment, disk move), the empty zones cause
allocations to overwrite used blocks, causing silent data corruption.

### 6.2 Init-state load path

a. `DdbDisk::new` defaults `effective_status` to `HwStatus::Init`.
b. `reconcile_disks` new-disk path: create `DdbDisk` with
   `effective_status = Init` and no zones. Attach metrics. Add to
   `dg.disks`. Spawn a background zone load task.
c. Background zone load task: for each zone index, call
   `load_zone_inner` (strategy 2 + strategy 1 fallback, same as
   `ZoneLoader::load_disk_group`). When all zones loaded:
   - Transition `Init → disk_value.status` via
     `HwStateMachine::transition_disk`. The target status comes from
     the `DiskValue` in group 0: `Up` for a fresh disk, `Maintenance`
     for a moved disk, `Offline` for an operator-set offline disk.
   - If `disk_value.status` is not a legal `Init → *` transition
     (e.g. `Suspect`/`Missing`/`Bad`), fall back to `Offline` and log
     `warn!`.
   - Call `rebuild_active_zones` + `rebuild_allocating_disks`.
d. On load failure (strategy 2 + strategy 1 both fail for any zone):
   transition `Init → Offline`, log `error!`. No recovery scan (disk
   was never `Up`).

### 6.3 Startup path

`run_zone_load` uses `ZoneLoader::load_disk_group` which creates
disks with `DdbDisk::new` (defaults to `Init`). The startup path
keeps its deferred-background design: the initial keepalive tick
creates Init disks (no IO, no zones: fast), `run_zone_load` loads
zones in the background, and the server transitions to `Up` when all
disks are `Up`. `load_disk_group` transitions `Init → Up` after all
zones are loaded for each disk.

### 6.4 reconcile_absent_disk skip logic

`reconcile_absent_disk` behavior:

a. `Bad` → skip (keep in memory, recovery scan running). Existing.
b. `Offline`, `Maintenance`, `Init` → remove from `dg.disks`
   directly. The disk's `DiskKey` was deleted from group 0 (moved or
   removed); absence means the disk is gone.
c. `Up`, `Suspect`, `Missing` → miss-count → `Missing` → `Bad` +
   recovery scan. Existing behavior.

`remove_disk_from_memory` helper on `DdbDiskGroup` removes from both
`disks` vec and `disk_index`, then calls
`rebuild_allocating_disks`.

Edge cases:
- Disk in `Maintenance` absent from sync but not actually moved (sync
  glitch) → removed from memory, reappears on next sync as a new Init
  disk, reloads zones. Correct; the disk's records are on the bind,
  reload is safe.
- Disk in `Offline` absent from sync → same as Maintenance. Correct.

## 7. Disk move

### 7.1 Why

A physical disk needs to move from
`(old_rack, old_node, old_dg)` to `(new_rack, new_node, new_dg)`,
keeping `DiskId` unchanged, without a full recovery scan. The disk's
records (zone/busy/free) are keyed by `DiskId` only, so a literal
key-value copy from the old bind to the new bind suffices.

### 7.2 Move handler

`http_move_disk` — POST `/api/disks/:disk_id/move` with body:
```json
{
  "new_rack_id": 2,
  "new_node_id": 3,
  "new_disk_group_id": 1
}
```

Handler flow:
a. Resolve the disk's current placement from `ConsoleConfig`.
b. Set the disk to `Maintenance` in group 0.
c. Copy records from old bind to new bind (see §7.3).
d. Update group 0 placement: `remove_disk` from old, `add_disk` to
   new with `Maintenance` status.
e. Update `ConsoleConfig`: move the `DiskEntry` to the new
   disk-group. Persist.
f. The old placement's diskdb instance sees the disk disappear from
   sync → `reconcile_absent_disk` removes the Maintenance disk from
   memory (§6.4).
g. The new placement's diskdb instance sees the disk appear → Init
   load path → loads zones from the new bind → `Init → Maintenance`.
h. Operator brings the disk back: `hw.set_disk_status(...)` to
   `Offline` then `Up` (or directly `Offline → Up`).

Edge cases:
- Copy fails mid-way → the disk is in Maintenance at the old
  placement, records partially copied to the new bind. Operator can
  retry the move or abort. The partial copy on the new bind is
  harmless; orphaned records overwritten on retry or cleaned up if
  the new bind's disk-group is removed.
- No bind exists for the new disk-group → handler returns 409. The
  operator must create the bind via a separate API before the move.

### 7.3 Record copy implementation

`copy_disk_records` in `app/crowdb-web/src/lifecycle.rs`:

```rust
async fn copy_disk_records(
    kv: &CrowdbClient,
    old_bind: (u64, u64),
    new_bind: (u64, u64),
    disk_id: &DiskId,
) -> u64
```

a. Scan old bind with `DiskId` prefix for each record type:
   `ZoneKey::prefix_for_disk`, `BusyBlockKey::prefix_for_disk`,
   `FreeBlockKey::prefix_for_disk`.
b. Batch-write to new bind. Batch size bounded (100 records per
   `batch_write`).
c. Return count for logging.

All disk-level records share the `DiskId` in their key, so a single
prefix scan per record type suffices.

## 8. Cluster reset

### 8.1 Why

`http_internal_reset` tears down the cluster but does not clean group
0 sysdata (hardware hierarchy) or KV-cluster topology records. After
reset, stale sysdata records remain.

### 8.2 Reset changes

Before the existing reset flow (groups → stores → servers → nodes →
racks removed from config):

a. Capture rack IDs and store IDs from config + monitor cache.
b. Build `HardwareClient` (same helper as §4.2).
c. `hw.remove_rack_cascade(rack_id)` for each rack.
d. Clean KV-cluster topology records via `KVClusterMetaClient`:
   `meta.remove_store(store_id)` for each store.

Edge cases:
- Group 0 already gone (servers stopped) →
  `build_hardware_client` returns None → skip sysdata cleanup, log
  `warn!`. The servers are stopped, so the sysdata is inaccessible
  anyway; a fresh `cluster_init` will overwrite it.

## 9. Invariants

- **I1 — ID reuse safety**: After a group/store/disk is removed, its
  ID can be safely reused. The node_config update (groups/stores) or
  group-0 sysdata deletion (disks/disk-groups/nodes/racks) is the
  fence against resurrection.
- **I2 — Init before allocatable**: A disk is never allocatable
  while in `Init` state. Zones must be loaded from the bind before
  the disk transitions to `Up`.
- **I3 — Move preserves DiskId**: Disk move copies records
  key-for-key from old bind to new bind. The `DiskId` is unchanged.
- **I4 — Cache eviction on topology change**: When a group disappears
  from topology, its `write_slot_highwater` entry is evicted to
  prevent stale min-slot reads against a reused group ID.

## 10. Test Design

### Unit tests

- **TopologyCache eviction**: build a cache with 3 groups via
  `merge`; merge a body with only 2 groups; assert the 3rd group's
  `leaders` + `replicas` entries are evicted.
- **write_slot_highwater eviction**: write to group (1,1) to populate
  `write_slot_highwater`; trigger topology refresh with a body
  missing group (1,1); assert `write_slot_highwater` no longer
  contains (1,1).
- **HardwareClient cascading remove**: mock group 0 with a rack →
  node → disk-group → disk hierarchy; call `remove_rack_cascade`;
  assert all derived records deleted.
- **ConsoleConfig disk-group/disk add/remove**: add disk-group to
  unknown node → validation error; add duplicate disk → conflict;
  remove disk-group with disks → conflict.
- **DdbDisk::new default Init**: assert
  `DdbDisk::new(...).effective_status == HwStatus::Init`.
- **reconcile_absent_disk skip logic**: Bad → kept; Offline/Maintenance/
  Init → removed; Up → miss-counted.
- **Init-state load failure**: zone load fails → `Init → Offline`.

### End-to-end tests

- **Server-side group cleanup**: create group → remove group →
  restart server → assert group not recreated.
- **ID reuse end-to-end**: remove group → re-create at same
  `(store_id, group_id)` → write + read → assert correct data.
- **Console sysdata sync**: add/remove rack/node/disk-group/disk via
  console API → assert group 0 sysdata reflects changes.
- **Disk move**: move a disk to a new disk-group → assert records
  copied, disk available at new placement, no full recovery scan.
- **Cluster reset**: `POST /internal/reset` → assert group 0 sysdata
  clean + config TOML clean → re-init → assert clean state.
