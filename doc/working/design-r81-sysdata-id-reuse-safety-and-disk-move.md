<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# sysdata ID Reuse Safety + Disk Move (R81)

This draft expands the solution for
[`doc/backlog/R81-sysdata-id-reuse-safety-and-disk-move.md`](../backlog/R81-sysdata-id-reuse-safety-and-disk-move.md)
into implementation detail. Architecture decisions and rationale are in
the root design docs:
[`design/kv/design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md)
(group-0 sysdata schema),
[`design/kv/design-crow-kv-server.md`](../design/kv/design-crow-kv-server.md)
(server lifecycle, HTTP management API),
[`design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
(diskdb architecture, disk status management),
[`design/console/design-crow-console.md`](../design/console/design-crow-console.md)
(console core, two-hierarchy API). This doc does not repeat them.

Already landed: R70–R76 (core diskdb server, allocation, recovery,
metrics, scanning, health probing), R73 (zone load strategy 2 + 1
fallback), R74 (per-disk hot-path metrics), R76 (unified recovery
path, `HwStateMachine`), R26 (`MinSlot` read-your-writes watermark),
R78 (watch/notify).

## 1. Server-side group/store cleanup

### 1.1 Why

`PxKvStore::remove_group` (`lib/crow-kv/src/cluster/px_kv_store.rs:428`)
cancels the tenure token and removes the group from the in-memory
`DashMap`, but does not delete the WAL dir, engine dir, or update
`node-config.json`. On server restart, the group resurrects from the
persisted node config — stale state from a removed group collides with
a new group re-created at the same `(store_id, group_id)`. Same gap
for `remove_store`. The HTTP handlers
(`app/crow-kv-server/src/mgmt/group_ops.rs:424`,
`app/crow-kv-server/src/mgmt/store_ops.rs:165`) call `PxKvStore` but
never touch `node_config` or the filesystem.

### 1.2 Group cleanup handler

`remove_group` handler (`app/crow-kv-server/src/mgmt/group_ops.rs`):

a. Get the store from the registry (existing).
b. Call `store.remove_group(gid)` — cancels tenure, removes from
   DashMap (existing).
c. Delete engine dir: `store_crow_tree_path(&config.data_root, sid, gid)`
   (`app/crow-kv-server/src/startup.rs:70`). Use `tokio::fs::remove_dir_all`.
d. Delete WAL group segments: the WAL root is
   `store_wal_root(&config.wal_root, sid)` (`startup.rs:65`); WAL
   segments are files within this dir named by group. Delete only the
   group's segment files, not the whole store dir (the store dir may
   contain other groups' segments). Scan the dir and delete files
   matching the group's naming pattern; if no files remain, leave the
   empty dir (the store dir is shared).
e. Call `node_config_store.remove_group(sid, gid)` to update
   `node-config.json` (`lib/crow-kv/src/cluster/node_config.rs:195`).
f. Log at `info!` level: `store_id`, `group_id`, "group removed +
   dirs deleted + node_config updated".

Edge cases:
- Dir deletion fails (permission, partial) → log `warn!`, continue
  with node_config update. The node_config update is the critical
  fence against resurrection; a stale dir is harmless (the group is
  not in node_config, so it won't be recreated on restart).
- `node_config_store` not available (test env) → skip silently. The
  handler must tolerate `config_root` being empty.
- Tenure cancellation race: `PxKvStore::remove_group` already cancels
  the token synchronously; running tasks hold `Arc<PxGroup>` until
  they notice the cancel. Dir deletion while a task is mid-write is
  safe — the task writes to a file handle it already opened; deleting
  the dir removes the directory entry but the file remains until the
  handle closes. No corruption, just a brief window where the task
  writes to an unlinked file.

### 1.3 Store cleanup handler

`remove_store` handler (`app/crow-kv-server/src/mgmt/store_ops.rs`):

a. Remove the store from the registry + graceful shutdown (existing).
b. Delete engine store dir: `{data_root}/store{sid}/` (cascades all
   group subdirs).
c. Delete WAL store dir: `store_wal_root(&config.wal_root, sid)` —
   the whole store dir, all group segments.
d. Call `node_config_store.remove_store(sid)` (currently
   `#[allow(dead_code)]` — remove the allow and wire it).

Edge cases: same as group cleanup (dir deletion failure is
non-fatal; node_config update is the fence).

### 1.4 Exposing NodeConfigStore to handlers

`KvStoreRegistry` (`app/crow-kv-server/src/store_registry.rs:37`)
has `config: CrowKVConfig` which holds `config_root: PathBuf`. The
handler builds a `NodeConfigStore::new(&config.config_root)` on each
call (cheap — just a path join). No need to store a long-lived
`NodeConfigStore` in the registry; the handler is a cold path.

## 2. Client cache eviction + rename

### 2.1 Why

`TopologyCache::merge` (`lib/crow-kv-client/src/topology.rs:125`)
only inserts into `leaders` and `replicas` — never evicts. A removed
group's stale leader endpoint self-heals via `NotLeaderHint`, but the
stale entry lingers. `write_watermark`
(`lib/crow-kv-client/src/client.rs:175`) is the critical case: a
stale `min_slot` high-watermark does NOT self-heal. A `MinSlot` read
against a reused group ID with a stale high-watermark silently
returns empty results forever (new group's revisions start from 0,
but the high-watermark is stuck at the old group's max revision).

### 2.2 TopologyCache::merge eviction

`merge` currently iterates `body.stores` and inserts. Add eviction:

a. Before the insert loop, collect the set of `(store_id, group_id)`
   present in the fresh `body` into a `HashSet<(u64, u64)>`.
b. After the insert loop, remove entries from `leaders` and
   `replicas` whose key is not in the fresh set.
c. The eviction is best-effort — if `merge` is called with a partial
   topology (some stores missing), this would evict valid entries.
   Mitigation: `fetch_and_merge` fetches from a single seed and gets
   the full topology body; the body contains all stores the seed
   knows about. If the seed returns a partial view, the eviction
   would temporarily drop entries that reappear on the next refresh.
   Acceptable — the stale leader self-heals via `NotLeaderHint`, and
   the replica list is only used for `AnyReplica` read distribution.

Edge cases:
- Empty body (all stores gone) → evict everything. Correct.
- Body fetch from a stale seed → temporary over-eviction; self-heals
  on next refresh from a fresh seed.

### 2.3 write_slot_highwater eviction

The `write_watermark` field is renamed to `write_slot_highwater`
(accurate: it's a paxos slot number, not a generic watermark;
"highwater" conveys monotonic-max). The eviction needs access to the
fresh topology set, but `write_slot_highwater` lives on `CrowkvClient`,
not `TopologyCache`. Two options:

**Chosen: callback from `TopologyCache::merge` to `CrowkvClient`.**
`TopologyCache` holds an `Option<Arc<dyn Fn(&HashSet<(u64, u64)>) + Send + Sync>>`
eviction callback, set by `CrowkvClient::new`. When `merge` evicts a
group, it calls the callback with the evicted keys; `CrowkvClient`'s
callback removes those keys from `write_slot_highwater`.

a. `TopologyCache::new` takes an optional `eviction_hook`.
b. `merge` collects evicted keys (the set difference between current
   cache keys and fresh body keys), calls the hook with the evicted
   set.
c. `CrowkvClient::new` creates the cache with a hook that removes
   evicted keys from `write_slot_highwater`.

Edge cases:
- No hook (standalone `TopologyCache` in tests) → skip eviction
  callback, just evict from `leaders`/`replicas`.
- Hook fails / panics → catch_unwind? No — the hook is a simple
  DashMap remove, cannot fail. No catch needed.

### 2.4 Rename

Rename `write_watermark` → `write_slot_highwater` in
`lib/crow-kv-client/src/client.rs`. Update all references:
- Field declaration (line 175)
- `new()` initialization (line 205)
- `record_write` (line 454)
- `read_your_writes_slot` (line 461) — method name stays (public API);
  only the field name changes.

## 3. HardwareClient cascading cleanup

### 3.1 Why

`HardwareClient::remove_*` (`lib/crow-kv-client/src/hardware.rs`)
only deletes the single record for that entity. No cascading cleanup
of derived records. Removing a disk-group leaves its `OwnerMapKey`,
`BindMapKey`, `DiskGroupUsageKey`, and all child `DiskKey` records
orphaned in group 0. Removing a node leaves all child disk-groups +
disks. Removing a rack leaves all child nodes + their disk-groups +
disks.

### 3.2 Cascading remove methods

Add `remove_*_cascade` methods to `HardwareClient` (keep the existing
single-record `remove_*` for internal use):

a. `remove_disk_cascade(rack, node, dg, disk_id)`:
   - `remove_disk(rack, node, dg, disk_id)` (existing)
   - That's it — disks have no derived records below them in group 0.
     (Zone/Busy/Free records live on the disk-group's bind, not group 0.)

b. `remove_disk_group_cascade(rack, node, dg_id)`:
   - List disks in the group: `list_disks_in_group(rack, node, dg_id)`
   - For each disk: `remove_disk(rack, node, dg_id, &disk_id)`
   - `remove_owner(rack, node, dg_id)` (existing)
   - `remove_bind(rack, node, dg_id)` (existing)
   - Remove `DiskGroupUsageKey` for `dg_id` (need to add a method or
     use raw delete on `/hw/dg_usage/{dg_id}`)
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
  listed. Partial cleanup is better than no cleanup; the remaining
  records are stale but harmless (no reader references a removed
  entity).
- Concurrent add during cascade → the new entity's records survive
  the cascade (the cascade lists before deleting; a concurrent add
  after the list is not in the list). Correct.

## 4. Console sysdata sync — rack/node

### 4.1 Why

The existing rack/node add/remove HTTP handlers
(`app/crow-web/src/lifecycle/rack_node.rs`) only mutate the console
config TOML. They never call `HardwareClient` to sync group 0
sysdata. Only `cluster_init` Phase 5 writes sysdata (one-time
bootstrap).

### 4.2 HardwareClient helper for handlers

Add a helper function in `app/crow-web/src/mgmt.rs`:

```rust
pub(crate) async fn build_hardware_client(
    state: &AppState,
) -> Option<HardwareClient> {
    // Find any node hosting group 0.
    let snap = state.monitor_cache.snapshot().await;
    for (node_id, _) in snap.iter() {
        if let Some(ep) = grpc_endpoint_for_node(state, *node_id, 0).await {
            let kv = crow_kv_client::CrowkvClient::new(
                crow_kv_client::ClientConfig::new(Vec::new()),
            );
            kv.seed_leader(0, 0, ep);
            return Some(HardwareClient::new(kv));
        }
    }
    None
}
```

### 4.3 Rack/node handler changes

`http_add_rack`: after `cfg.add_rack` + persist, call
`hw.add_rack(id, &RackValue { status: Up, node_ids: Vec::new() })`.
If `hw` is None (no group 0 endpoint) or the call fails, log `warn!`
and continue — the config TOML is the source of truth for the
console; group 0 sysdata is a derived view.

`http_remove_rack`: after `cfg.remove_rack` + persist, call
`hw.remove_rack_cascade(id)`.

`http_add_node`: after `cfg.add_node` + persist, call
`hw.add_node(rack_id, id, &NodeValue { ... })`.

`http_remove_node`: after `cfg.remove_node` + persist, call
`hw.remove_node_cascade(rack_id, id)`.

Edge cases:
- Group 0 not yet initialized (fresh console, no cluster_init) →
  `build_hardware_client` returns None → skip sysdata sync, log
  `warn!`. The config TOML is updated; when `cluster_init` runs, Phase
  5 writes the full hierarchy.
- Sysdata write fails → log `warn!`, return success. The console
  config is the operator's intent; sysdata is derived. A later
  `cluster_init` re-run or a manual sync can reconcile.

## 5. Console disk-group/disk handlers (new)

### 5.1 Why

Disk-group/disk console HTTP handlers do not exist (originally R77
scope; R81 absorbs handler/CLI creation, R77 reduced to
UI/visualization). Operators need add/remove/move flows for
disk-groups and disks via the console API.

### 5.2 ConsoleConfig extensions

Add to `lib/crow-console-shared/src/config.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskGroupEntry {
    pub id: DiskGroupId,
    pub node_id: NodeId,
    pub rack_id: RackId,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskEntry {
    pub disk_id: String,  // UUID hex string
    pub disk_group_id: DiskGroupId,
    pub node_id: NodeId,
    pub rack_id: RackId,
    pub disk_type: String,  // "Hdd" | "Ssd"
    pub capacity_bytes: u64,
    pub zone_size_bytes: u64,
    pub unit_size_bytes: u32,
}
```

Add `disk_groups: Vec<DiskGroupEntry>` and `disks: Vec<DiskEntry>` to
`ConsoleConfig`. Add `add_disk_group` / `remove_disk_group` /
`add_disk` / `remove_disk` methods following the existing rack/node
pattern (duplicate check, foreign key check, conflict-on-remove).

### 5.3 HTTP handlers

New file `app/crow-web/src/lifecycle/disk_group.rs`:

- `http_add_disk_group` — POST `/api/nodes/:node_id/disk-groups`
- `http_remove_disk_group` — DELETE `/api/nodes/:node_id/disk-groups/:dg_id`
- `http_list_node_disk_groups` — GET `/api/nodes/:node_id/disk-groups`
- `http_get_node_disk_group` — GET `/api/nodes/:node_id/disk-groups/:dg_id`

New file `app/crow-web/src/lifecycle/disk.rs`:

- `http_add_disk` — POST `/api/nodes/:node_id/disk-groups/:dg_id/disks`
- `http_remove_disk` — DELETE `/api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`
- `http_list_disks_in_group` — GET `/api/nodes/:node_id/disk-groups/:dg_id/disks`
- `http_get_disk` — GET `/api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`
- `http_move_disk` — POST `/api/disks/:disk_id/move`

Each add/remove handler follows the rack/node pattern: update
`ConsoleConfig` + persist + call `HardwareClient` method. The move
handler is covered in §7.

### 5.4 Router registration

Add routes to `app/crow-web/src/lib.rs` router following the existing
pattern.

### 5.5 ConsoleClient methods

Add to `lib/crow-console-shared/src/console_client.rs`:
`add_disk_group`, `remove_disk_group`, `list_disk_groups`,
`add_disk`, `remove_disk`, `list_disks`, `move_disk` — thin HTTP
wrappers following the existing `add_rack` / `remove_rack` pattern.

### 5.6 CLI commands

New file `app/crow-cli/src/commands/disk_group.rs`:
`DiskGroupVerb { Add, Remove, List }`.

New file `app/crow-cli/src/commands/disk.rs`:
`DiskVerb { Add, Remove, List, Move }`.

Both follow the existing `rack.rs` pattern.

## 6. diskdb Init-state zone load

### 6.1 Why

`disk_add_init` (`app/crow-diskdb/src/liveness/keepalive.rs:677`)
creates empty in-memory `DdbZone`s for every new disk, writes
baseline `ZoneValue` records (if no snapshots exist), then calls
`rebuild_allocating_disks` — adding the disk to the allocating set
with empty zones. If the disk has existing KV records (ownership
transfer, disk-group reassignment, disk move), the empty zones cause
allocations to overwrite used blocks — silent data corruption.

### 6.2 Replace disk_add_init with Init-state load

a. `DdbDisk::new` (`app/crow-diskdb/src/model/disk.rs:45`): change
   default `effective_status` from `HwStatus::Up` to `HwStatus::Init`.
b. Delete `disk_add_init` and `zone_snapshots_exist`.
c. New `reconcile_disks` new-disk path: create `DdbDisk` with
   `effective_status = Init` and `zones: Vec::new()` (no empty
   skeletons). Attach metrics. Add to `dg.disks`. Spawn a background
   zone load task.
d. Background zone load task: for each zone index, call
   `load_zone_inner` (strategy 2 + strategy 1 fallback — same as
   `ZoneLoader::load_disk_group`). When all zones loaded:
   - Transition `Init → disk_value.status` via
     `HwStateMachine::transition_disk`. The target status comes from
     the `DiskValue` in group 0: `Up` for a fresh disk, `Maintenance`
     for a moved disk, `Offline` for an operator-set offline disk.
   - If `disk_value.status` is not a legal `Init → *` transition
     (e.g. `Suspect`/`Missing`/`Bad` — a new disk should never carry
     these), fall back to `Offline` and log `warn!`.
   - Call `rebuild_active_zones` + `rebuild_allocating_disks`.
e. On load failure (strategy 2 + strategy 1 both fail for any zone):
   transition `Init → Offline`, log `error!`. No recovery scan
   (disk was never `Up`).

### 6.3 Startup path

`run_zone_load` (`app/crow-diskdb/src/main.rs:305`) currently uses
`ZoneLoader::load_disk_group` which creates disks with
`DdbDisk::new` (which will now default to `Init`). The startup path
keeps its deferred-background design: the initial keepalive tick
creates Init disks (no IO, no zones — fast), `run_zone_load` loads
zones in the background, and the server transitions to `Up` when all
disks are `Up`. `ZoneLoader::load_disk_group` already loads zones
via `load_zone_inner` and adds them to the disk — after loading, it
needs to transition `Init → Up` (currently it doesn't transition
because `DdbDisk::new` defaulted to `Up`). Add the transition after
all zones are loaded in `load_disk_group`.

### 6.4 reconcile_absent_disk skip logic

`reconcile_absent_disk` (`keepalive.rs:522`) currently skips only
`Bad`. New behavior:

a. `Bad` → skip (keep in memory, recovery scan running). Existing.
b. `Offline`, `Maintenance` → remove from `dg.disks` directly. The
   disk's `DiskKey` was deleted from group 0 (moved or removed);
   absence means the disk is gone. Remove from `disk_index` too.
c. `Init` → remove from `dg.disks` directly (never loaded, nothing
   to preserve). Handled in `reconcile_disks`'s removed-disk
   detection, not in `reconcile_absent_disk` — but
   `reconcile_absent_disk` is called for all absent disks, so it
   must handle `Init` too (remove from memory).
d. `Up`, `Suspect`, `Missing` → miss-count → `Missing` → `Bad` +
   recovery scan. Existing behavior.

Add a `remove_disk_from_memory` helper on `DdbDiskGroup` that removes
from both `disks` vec and `disk_index`, then calls
`rebuild_allocating_disks`.

Edge cases:
- Disk in `Maintenance` absent from sync but not actually moved (sync
  glitch) → removed from memory, reappears on next sync as a new Init
  disk, reloads zones. Correct — the disk's records are on the bind,
  reload is safe.
- Disk in `Offline` absent from sync → same as Maintenance. Correct.

## 7. Disk move

### 7.1 Why

A physical disk needs to move from
`(old_rack, old_node, old_dg)` to `(new_rack, new_node, new_dg)`,
keeping `DiskId` unchanged, without a full recovery scan. The disk's
records (zone/busy/free) are keyed by `DiskId` only — a literal
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
a. Resolve the disk's current placement from `ConsoleConfig` (find
   the `DiskEntry` with matching `disk_id`).
b. Set the disk to `Maintenance` in group 0:
   `hw.set_disk_status(old_rack, old_node, old_dg, &disk_id, Maintenance)`.
c. Copy records from old bind to new bind:
   - Resolve old bind: `hw.get_bind(old_rack, old_node, old_dg)` →
     `(old_store, old_group)`.
   - Resolve new bind: `hw.get_bind(new_rack, new_node, new_dg)` →
     `(new_store, new_group)`. If no bind exists, create one
     (`hw.set_bind(...)` to an available paxos data group).
   - Scan the old bind by `DiskId` prefix: `kv.scan(old_store,
     old_group, DiskKey::prefix_for_disk(disk_id), ...)`.
   - For each record: `kv.batch_write(new_store, new_group, &[...])`.
     Literal key-value copy — same keys, same values.
d. Update group 0 placement:
   - `hw.remove_disk(old_rack, old_node, old_dg, &disk_id)`
   - `hw.add_disk(new_rack, new_node, new_dg, &disk_id, &disk_value)`
     (same `DiskValue`, status = `Maintenance`).
e. Update `ConsoleConfig`: move the `DiskEntry` to the new
   disk-group. Persist.
f. The old placement's diskdb instance sees the disk disappear from
   sync → `reconcile_absent_disk` removes the Maintenance disk from
   memory (§6.4).
g. The new placement's diskdb instance sees the disk appear → Init
   load path → loads zones from the new bind (records just copied) →
   `Init → Maintenance` (via `disk_value.status`).
h. Operator brings the disk back: `hw.set_disk_status(new_rack,
   new_node, new_dg, &disk_id, Offline)` then `Up` (or directly
   `Offline → Up`).

Edge cases:
- Copy fails mid-way → the disk is in Maintenance at the old
  placement, records partially copied to the new bind. The old
  placement still has the disk in memory (Maintenance, not absent
  from sync yet because step d hasn't happened). Operator can retry
  the move or abort (set disk back to Up at the old placement). The
  partial copy on the new bind is harmless — it's orphaned records
  that will be overwritten on a successful retry or cleaned up if the
  new bind's disk-group is removed.
- No bind exists for the new disk-group → the handler creates one.
  This requires knowing which paxos data groups are available. For
  v1, require the new disk-group to already have a bind (operator
  creates the bind via a separate API before the move). Simplifies
  the handler.

### 7.3 Record copy implementation

Add a method to `DdbKvClient` (`app/crow-diskdb/src/kv_client.rs` or
wherever the KV client lives) or to `HardwareClient`:

```rust
pub async fn copy_disk_records(
    &self,
    old_bind: (u64, u64),
    new_bind: (u64, u64),
    disk_id: &DiskId,
) -> Result<u64>  // returns count of records copied
```

a. Scan old bind with `DiskId` prefix (zone, busy, free, recovery
   scan progress keys — all keyed by `DiskId`).
b. Batch-write to new bind. Batch size bounded (e.g. 100 records per
   `batch_write`).
c. Return count for logging.

The scan uses the binary key prefix
(`ZoneKey::prefix_for_disk`, `BusyBlockKey::prefix_for_disk`, etc.)
or a broader prefix that covers all disk-keyed records. Since all
disk-level records share the `DiskId` in their key, a single prefix
scan per record type suffices.

## 8. Cluster reset

### 8.1 Why

`http_internal_reset` (`app/crow-web/src/lifecycle.rs:986`) tears
down the cluster but does not clean group 0 sysdata (hardware
hierarchy). After reset, stale sysdata records remain.

### 8.2 Reset changes

After the existing reset flow (groups → stores → servers → nodes →
racks removed from config), add:
a. Build `HardwareClient` (same helper as §4.2).
b. `hw.remove_rack_cascade(rack_id)` for each rack that was in the
   config before reset.
c. Also clean KV-cluster topology records (stores, groups, replicas)
   via `KVClusterMetaClient` — the existing reset removes them from
   config but not from group 0.

Edge cases:
- Group 0 already gone (servers stopped) → `build_hardware_client`
  returns None → skip sysdata cleanup, log `warn!`. The servers are
  stopped, so the sysdata is inaccessible anyway; a fresh
  `cluster_init` will overwrite it.

## Scope

- `app/crow-kv-server/src/mgmt/group_ops.rs` — add dir deletion +
  node_config update to `remove_group` handler
- `app/crow-kv-server/src/mgmt/store_ops.rs` — add dir deletion +
  node_config update to `remove_store` handler
- `lib/crow-kv/src/cluster/node_config.rs` — remove
  `#[allow(dead_code)]` from `NodeConfigStore::remove_store`
- `lib/crow-kv-client/src/topology.rs` — add eviction to `merge` +
  eviction callback hook
- `lib/crow-kv-client/src/client.rs` — rename `write_watermark` →
  `write_slot_highwater` + wire eviction callback
- `lib/crow-kv-client/src/hardware.rs` — add `remove_*_cascade`
  methods + `copy_disk_records` method
- `lib/crow-console-shared/src/config.rs` — add `DiskGroupEntry`,
  `DiskEntry`, `add_*` / `remove_*` methods
- `lib/crow-console-shared/src/console_client.rs` — add disk-group/
  disk/move methods
- `app/crow-web/src/mgmt.rs` — add `build_hardware_client` helper
- `app/crow-web/src/lifecycle/rack_node.rs` — wire sysdata sync to
  add/remove handlers
- `app/crow-web/src/lifecycle/disk_group.rs` — new: disk-group HTTP
  handlers
- `app/crow-web/src/lifecycle/disk.rs` — new: disk HTTP handlers +
  move handler
- `app/crow-web/src/lifecycle.rs` — add sysdata cleanup to
  `http_internal_reset`
- `app/crow-web/src/lib.rs` — register new routes
- `app/crow-diskdb/src/model/disk.rs` — `DdbDisk::new` default
  `Init`
- `app/crow-diskdb/src/model/disk_group.rs` — add
  `remove_disk_from_memory` helper
- `app/crow-diskdb/src/liveness/keepalive.rs` — replace
  `disk_add_init` with Init-state load; update
  `reconcile_absent_disk` skip logic; add background zone load task
- `app/crow-diskdb/src/recovery.rs` — `load_disk_group` transition
  `Init → Up` after load
- `app/crow-diskdb/src/recovery/journal_replay.rs` — delete
  `zone_snapshots_exist`
- `app/crow-cli/src/commands/disk_group.rs` — new: CLI commands
- `app/crow-cli/src/commands/disk.rs` — new: CLI commands
- `app/crow-cli/src/commands/mod.rs` — register new command modules
- `app/crow-cli/src/main.rs` — add `DiskGroup` and `Disk` to
  `Group` enum

## Complexity

**High.** Spans 5 crates (crow-kv, crow-kv-client, crow-web,
crow-diskdb, crow-cli) + crow-console-shared. The diskdb Init-state
load path is the hardest: replacing `disk_add_init`'s eager
zone-creation with a deferred background load touches the keepalive
reconcile loop, the startup zone load path, and the state machine.
The disk move record copy is new logic (prefix scan + batch_write
across binds). The console disk-group/disk handlers are new surface
area but follow existing patterns (rack/node). The client cache
eviction is small but subtle (the callback wiring between
`TopologyCache` and `CrowkvClient`).

## Test Design

### Unit tests (UT)

- **TopologyCache eviction**: build a cache with 3 groups via
  `merge`; merge a body with only 2 groups; assert the 3rd group's
  `leaders` + `replicas` entries are evicted.
- **write_slot_highwater eviction**: build a `CrowkvClient`, write to
  group (1,1) to populate `write_slot_highwater`; trigger topology
  refresh with a body missing group (1,1); assert
  `write_slot_highwater` no longer contains (1,1).
- **Rename**: all existing `write_watermark` references compile as
  `write_slot_highwater`; `read_your_writes_slot` still returns the
  correct value.
- **HardwareClient cascading remove**: mock group 0 with a rack →
  node → disk-group → disk hierarchy; call `remove_rack_cascade`;
  assert all `RackKey`, `NodeKey`, `DiskGroupKey`, `DiskKey`,
  `OwnerMapKey`, `BindMapKey` records deleted.
- **ConsoleConfig disk-group/disk add/remove**: add disk-group to
  unknown node → `Validation` error; add duplicate disk → `Conflict`;
  remove disk-group with disks → `Conflict`.
- **DdbDisk::new default Init**: assert
  `DdbDisk::new(...).effective_status == HwStatus::Init`.
- **reconcile_absent_disk skip logic**:
  - Bad disk absent → kept in memory (skip).
  - Offline disk absent → removed from `dg.disks`.
  - Maintenance disk absent → removed from `dg.disks`.
  - Init disk absent → removed from `dg.disks`.
  - Up disk absent → miss-counted → Missing.
- **Init-state load failure**: disk whose zone load fails (strategy
  2 + strategy 1 both fail) → `Init → Offline`, no recovery scan.
- **Init → disk_value.status**: load completes, `disk_value.status =
  Up` → `Init → Up`; `disk_value.status = Maintenance` →
  `Init → Maintenance`; `disk_value.status = Suspect` (illegal for
  new disk) → falls back to `Offline` + `warn!` log.

### End-to-end tests (E2E)

- **Server-side group cleanup**: start server → create group →
  remove group → restart server → assert group not recreated (WAL
  dir, engine dir, node_config all clean).
- **Server-side store cleanup**: same for store.
- **ID reuse end-to-end**: remove group → re-create at same
  `(store_id, group_id)` → write + read → assert correct data, no
  stale state.
- **Console sysdata sync — add rack**: add rack via console API →
  assert `RackKey` record exists in group 0.
- **Console sysdata sync — remove rack**: remove rack via console →
  assert `RackKey` + cascaded records deleted from group 0.
- **Console disk-group/disk add/remove**: add disk-group via console
  → assert `DiskGroupKey` in group 0; remove → assert deleted +
  cascaded.
- **Disk move**: move a disk to a new disk-group bound to a different
  paxos group → assert records copied to new bind during Maintenance,
  disk available at new placement with correct zone usage,
  multi-block allocate works with new peers, no full recovery scan.
- **Cluster reset**: `POST /internal/reset` → assert group 0 sysdata
  clean (hardware + KV topology) + config TOML clean → re-init →
  assert clean state.

## Module Structure

```
app/crow-web/src/lifecycle/disk_group.rs   — new: disk-group HTTP handlers
app/crow-web/src/lifecycle/disk.rs         — new: disk HTTP handlers + move
app/crow-cli/src/commands/disk_group.rs    — new: disk-group CLI commands
app/crow-cli/src/commands/disk.rs          — new: disk CLI commands
```

Modified files listed in Scope above.

## Config Extensions

No new config fields. `DiskGroupEntry` and `DiskEntry` are new
`ConsoleConfig` entries, not config fields. No `validate()` changes.

## Server Wiring

diskdb startup (`app/crow-diskdb/src/main.rs`):
1. Initial keepalive tick → `reconcile_disks` creates Init disks
   (no IO, no zones — fast).
2. `run_zone_load` → `ZoneLoader::load_disk_group` loads zones via
   `load_zone_inner`, transitions `Init → Up` after load.
3. Server transitions to `Up` when all disks are `Up`.

Mid-running disk appearance (keepalive tick after startup):
1. `reconcile_disks` sees new `DiskId` → creates Init disk → spawns
   background zone load task.
2. Background task loads zones → transitions `Init → disk_value.status`.
3. Disk-group is already serving; Init disk excluded from allocation
   until loaded.

## Open Questions

None — all decisions resolved in the backlog doc's Resolved Questions
section.
