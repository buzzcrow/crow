<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R81: sysdata — ID Reuse Safety + Disk Move

**Problem**: Two distinct problems, both solvable without an
epoch/generation field:

1. **Reusable integer ID reuse** — the cluster-topology integer IDs
   (`RackId`, `NodeId`, `DiskGroupId`, paxos `store_id`/`group_id`/
   `replica_id`, all u64) are reusable. They are not globally unique
   like `DiskId` (128-bit) or `ChunkId` (192-bit). When an entity is
   removed and a new entity is later created with the same integer ID,
   stale on-disk state (WAL, engine), stale node config, stale group 0
   sysdata, and stale client caches can be incorrectly associated with
   the new entity. The goal is to make ID reuse safe so operators can
   keep the ID space small (e.g. re-add node 1 after removing the old
   node 1).

2. **Disk move with stable UUID** — a physical disk needs to be moved
   from one node/disk-group to another, keeping its globally-unique
   `DiskId` (UUID) unchanged, without triggering a full recovery scan
   (strategy 1 — physical disk block scan). The disk's zone/busy/free
   records (keyed by `DiskId`) should remain accessible at the new
   placement. The disk's data is intact on the physical medium; only
   its placement (which `rack_id`/`node_id`/`disk_group_id` it lives
   under, and which paxos group its records are bound to) changes.

**Analysis — why no epoch/generation is needed**:

An epoch earns its keep only when a consumer holds a long-lived cache
keyed by the bare integer ID across the remove+re-add window. If every
consumer re-reads state each sync cycle or each operation, cleanup
alone makes reuse safe. Codebase-verified per-ID analysis:

- **`rack_id`** — `RackValue` and `RackValue.node_ids` have no
  production reader. The console reads racks from its own config TOML
  (`cfg.racks`), not from group 0 sysdata. diskdb derives its owned
  set from the owner map (filtered by `instance_id`), not from
  `RackValue`. No consumer caches by bare `rack_id`.
  **Cleanup is sufficient; no epoch.**
- **`node_id`** — `NodeValue.disk_group_ids` and
  `NodeValue.last_used_dg_id` have no production reader. diskdb's
  `DdbDiskGroupContainer` is keyed by `disk_group_id`, not `node_id`,
  and is rebuilt each sync from the owner map. No consumer caches by
  bare `node_id`.
  **Cleanup is sufficient; no epoch.**
- **`disk_group_id`** — diskdb's container is a
  `HashMap<DiskGroupId, Arc<DdbDiskGroup>>` reconciled each sync from
  the owner map. The reconcile loop already drops disk-groups that
  disappear from the owner map. The bind map is re-read each sync
  (`list_binds()` → fresh `HashMap`). No long-lived cache by bare
  `dg_id`.
  **Cleanup (delete owner + bind + disk records) is sufficient; the
  container self-heals next sync. No epoch.**
- **`disk_id`** (128-bit UUID) — globally unique by construction; no
  identity-reuse risk. The disk move case (Part 2) is a placement
  change, not an identity change. No consumer caches
  `DiskId → placement` across the move window (diskdb re-reads disk
  lists each sync).
  **No epoch needed for identity or placement.**
- **`store_id` / `group_id`** — the one case with long-lived client
  caches keyed by bare `(store_id, group_id)`:
  - `TopologyCache`: `DashMap<(u64, u64), String>` for leader
    endpoints, `DashMap<(u64, u64), Vec<String>>` for replica
    endpoints. `merge` only inserts, never evicts.
  - `write_watermark`: `DashMap<(u64, u64), u64>` tracking the highest
    `revision` from this client's writes, used as `min_slot` for
    `MinSlot` reads. Updated by monotonic max, never decreases, never
    evicts.
  - Server-side: `remove_group` only removes from the in-memory
    `DashMap` + cancels the tenure token. It does NOT delete the WAL
    dir, engine dir, or update `node_config` — so the group's data
    survives on disk and the group resurrects on server restart.
  **Needs cleanup + cache eviction. No epoch needed — eviction is
  sufficient.**
- **`replica_id`** — scoped within a `(store_id, group_id)`. Always
  paired with its group in `NotLeaderHint` and `/topology` responses,
  never used as a standalone cache key. Auto-assigned as "max existing
  + 1" within a group's current view.
  **Solving the `(store_id, group_id)` identity problem covers
  replicas transitively. No epoch.**

**Solution — Part 1: ID Reuse Safety**:

No epoch or generation field. The fix is thorough cleanup on removal
+ client cache eviction.

*Server-side `remove_group` (per node)*:
- Delete WAL dir: `{wal_root}/store{store_id}/group{group_id}/`
- Delete engine dir: `{data_root}/store{store_id}/group{group_id}/`
- Call `node_config.remove_group(store_id, group_id)` — method exists
  (`lib/crow-kv/src/cluster/node_config.rs`) but is not wired into the
  management API handler
- Ensure tenure cancellation completes before dir deletion (running
  tasks hold `Arc<PxGroup>` until they notice the cancel token —
  `PxKvStore::remove_group` already cancels the tenure; the dir
  deletion must wait for shutdown confirmation or run after a short
  grace period)

*Server-side `remove_store` (per node)*:
- Delete `{wal_root}/store{store_id}/` (cascades all group subdirs)
- Delete `{data_root}/store{store_id}/`
- Call `node_config.remove_store(store_id)`

*Group 0 sysdata cleanup*:
- On `remove_group`: delete the group's metadata record in group 0
- On `remove_store`: delete the store's metadata record + all group
  records under it
- On `remove_node` / `remove_disk_group` / `remove_disk`: cascade-delete
  derived records (`OwnerMapKey`, `BindMapKey`, `DiskGroupUsageKey`,
  `DiskKey` records under the removed entity)

*Console layer wiring* (part of R81, not a prerequisite):
- `http_remove_group` already fans out to all replica nodes
  (`app/crow-web/src/mgmt/group_ops.rs`) — the distributed cleanup
  orchestration exists; each node just needs to do its local dir +
  node_config cleanup
- `http_remove_store` similarly fans out to all hosting nodes
- **Console add/remove flows for rack/node/disk-group/disk must be
  wired to actually sync group 0 sysdata.** Today only `cluster_init`
  Phase 5 writes sysdata; the add/remove HTTP handlers
  (`http_add_rack`, `http_remove_rack`, `http_add_node`,
  `http_remove_node`, etc. in `app/crow-web/src/lifecycle/`) only
  mutate the console config TOML — `HardwareClient::add_*` /
  `remove_*` is never called in production. This wiring is part of
  R81: each add/remove handler calls the corresponding
  `HardwareClient` method (with cascading cleanup on remove) in
  addition to updating the config TOML. The CLI commands
  (`crow-cli/src/commands/{rack,node,...}.rs`) are thin wrappers over
  the same HTTP API, so they need no separate change — they already
  call the console endpoints.
- **Test coverage**: enhance existing unit/integration tests to verify
  the console add/remove flows update group 0 sysdata, not just the
  config TOML. Specifically:
  - Add rack via console → assert `RackKey` record exists in group 0
  - Remove rack via console → assert `RackKey` record deleted from
    group 0 + cascaded derived records deleted
  - Same for node, disk-group, disk
  - Reuse existing `crow-web` test infrastructure
    (`app/crow-web/tests/`) where possible; add new tests where the
    existing coverage doesn't exercise the sysdata sync path
- **Cluster reset** (`POST /internal/reset`,
  `app/crow-web/src/lifecycle.rs:http_internal_reset`): the existing
  reset flow already fans out `remove_group` / `remove_store` to all
  nodes, then removes nodes and racks from config. Once R81 makes
  `remove_group` / `remove_store` delete WAL + engine dirs +
  `node_config` on the server, and makes the console remove handlers
  sync group 0 sysdata, the reset flow inherits these fixes
  automatically (it calls the same RPCs). One additional change:
  reset should also call `HardwareClient::remove_*` for the hardware
  hierarchy (rack/node/disk-group/disk records in group 0) — today it
  only removes them from the console config TOML, leaving group 0
  sysdata stale. After R81, reset cleans both config TOML and group 0
  sysdata, and the server-side dir/node_config cleanup happens via
  the `remove_group` / `remove_store` RPCs. The reset flow should work
  as before from the operator's perspective — same API, same
  behavior, just with complete cleanup.

*Client-side cache eviction*:
- `TopologyCache::merge`: when a group disappears from the fresh
  `/topology` body, remove its `leaders` and `replicas` entries.
  Availability optimization — the stale leader endpoint already
  self-heals via `NotLeaderHint` on the next failed request.
- `write_slot_highwater` (renamed from `write_watermark`): evict the
  entry for a `(store_id, group_id)` when the group disappears from
  `/topology`. **Critical fix** — a stale `min_slot` high-watermark
  does NOT self-heal: a `MinSlot` read against a reused group ID with
  a stale high-watermark silently returns empty results forever (new
  group's revisions start from 0, but the high-watermark is stuck at
  the old group's max revision).

  **Chosen approach: evict in `merge`** — when `merge` detects a
  group absent from the fresh `/topology` body, also remove its
  `write_slot_highwater` entry. Reuses the existing refresh
  mechanism; no proto change, no epoch on the kv group. Residual
  window: if the client refreshes after the group is already
  re-created at the same ID, `merge` sees the group present and does
  not evict — the stale high-watermark persists. In that window, a
  `MinSlot` read returns empty (not wrong data) — the caller's retry
  or the next `NotLeaderHint`-triggered refresh eventually lands
  during a remove-recreate gap and evicts. Acceptable: the residual
  window produces visible errors (empty reads), not silent data
  corruption, and retry resolves it.

*Rename*: `write_watermark` → `write_slot_highwater` in
`crow-kv-client` (`CrowkvClient`). The field tracks the highest paxos
slot from this client's writes — "slot" is the accurate term (it's a
paxos slot number, not a generic watermark), and "highwater" conveys
the monotonic-max semantics. Part of R81 since the eviction logic
touches the same field.

**Solution — Part 2: Disk Move (no full recovery scan)**:

A disk move relocates a physical disk from
`(old_rack, old_node, old_dg)` to `(new_rack, new_node, new_dg)`,
keeping `DiskId` unchanged. The goal is to make the disk available at
the new placement without a full recovery scan (strategy 1 — physical
disk block scan).

*Key architectural insight — records below disk belong to the disk,
not to the disk-group*:

The bind (`disk_group_id → (store_id, group_id)`) is a disk-group-layer
concept — it routes disk-group operations to one paxos data group so
multi-block allocation can use a single `batch_write`
(`design-crow-diskdb.md` §3.2). But the layer **below** the disk —
`ZoneKey`, `BusyBlockKey`, `FreeBlockKey` — is keyed only by `DiskId`
(globally unique) + `zone_index` (+ `unit_offset`). No
`disk_group_id`, no `node_id`, no `rack_id`, no `(store_id, group_id)`
in the key (`lib/crow-protocol/src/key/diskdb.rs`). The records belong
to the disk, not to the disk-group. They don't reference their
placement or their paxos group.

This means: copying a disk's records from one paxos group to another
is a **literal key-value copy** — same keys, same values, no
transformation. The records don't care which paxos group they're
stored on.

*No per-disk bind tracking*: after the move, the disk belongs to the
new disk-group. All reads and writes — including reads of old data
(zone snapshots, existing busy/free blocks) — go through the new
disk-group's bind. To make this work, the disk's records are copied
from the old disk-group's bind to the new disk-group's bind during the
move (while the disk is in Maintenance, no concurrent writes). After
the copy, the disk fully belongs to the new disk-group — no split
records, no per-disk bind override, no special-casing on restart.

*API surface*: console HTTP endpoint (`POST /api/disks/:disk_id/move`)
+ CLI command (`crow disk move`). Both call the same console handler.

*Move flow* (uses existing `HwStatus` states — no new status needed):

1. **`Up → Maintenance`**: operator triggers move; disk taken out of
   allocation rotation. `Op::Allocate` denied on `Maintenance`;
   `Op::Free` still allowed (existing blocks can be freed during the
   move). See `HwStateMachine::permits`
   (`app/crow-diskdb/src/liveness/state_machine.rs`).
2. **Copy records**: batch-copy the disk's zone/busy/free records from
   the old disk-group's bind `(old_store, old_group)` to the new
   disk-group's bind `(new_store, new_group)`. Scan by `DiskId` prefix
   on the old bind, `batch_write` to the new bind. Literal key-value
   copy — same keys, same values. Safe because Maintenance blocks
   concurrent writes. This is a fast KV-to-KV operation, not a
   physical disk block scan (strategy 1).
3. **Update group 0 placement**: delete old `DiskKey` at
   `/hw/disk/<old_rack>/<old_node>/<old_dg>/<DiskId>`, write new
   `DiskKey` at `/hw/disk/<new_rack>/<new_node>/<new_dg>/<DiskId>`
   (same `DiskId`, new path). No per-disk bind field in `DiskValue` —
   the disk uses the new disk-group's bind like every other disk.
4. The diskdb instance owning the new dg picks up the disk via
   keepalive reconcile (`reconcile_disks` sees a new `DiskId` in the
   new dg's disk list).
5. `disk_add_init` checks `zone_snapshots_exist` against the new
   disk-group's bind → snapshots exist (just copied) → skips baseline
   write → inline zone load (see below) loads zone usage → disk is
   ready.
6. **`Maintenance → Offline → Up`**: disk brought back to `Up` at the
   new placement. Direct `Maintenance → Up` is not a legal transition
   in `is_legal_transition`; the path goes through `Offline`.
7. **No full scan.** After the copy, the disk fully belongs to the new
   disk-group — all reads/writes go through the new bind, multi-block
   allocate works normally with new peers, restart recovery is normal
   (single bind, single scan per zone).

*Key change to `disk_add_init`*: today, when `zone_snapshots_exist`
returns true, `disk_add_init` skips the baseline zone write but leaves
the in-memory zone usage at zero (empty zones). Zone usage loading
(journal replay) is done by `RecoveryEngine::recover_disk_group`,
which only runs at startup — not in the keepalive path. This means a
disk that appears mid-running (via keepalive reconcile) with existing
snapshots gets empty zones, causing the disk to appear empty and
allocations to overwrite existing data. This is an existing gap, not
specific to disk move.

The fix: when `zone_snapshots_exist` is true, `disk_add_init` should
inline-load the zone usage via journal replay (strategy 2) per-disk,
not just skip the baseline write. This makes the disk immediately
available with its correct usage state. This also fixes the existing
gap for mid-running disk-group reassignment (a disk-group reassigned
to a different instance without restart).

*`RecoveryScanProgressKey { disk_id }`*: keyed by `DiskId`, copied to
the new bind along with the other records. No orphan, no cleanup
needed.

*No epoch on `DiskValue`*: the original R81 proposed a placement epoch
on `DiskValue` bumped on move. This is not needed — no consumer caches
`DiskId → placement` across the move window. diskdb re-reads disk
lists each sync cycle and reconciles. The move is detected by the
keepalive reconcile (disk appears in new dg, disappears from old dg),
not by comparing epochs.

**One-line summary**: make reusable integer IDs reuse-safe via
thorough cleanup on removal + client cache eviction (no epoch), and
support disk move with stable UUID via record copy during Maintenance
+ group 0 placement update + inline zone load in `disk_add_init` (no
full recovery scan, no per-disk bind, no placement epoch).

**Edge cases at a glance**:
- Re-add with same ID before cleanup propagates → the cleanup must be
  synchronous (or the re-add must wait for cleanup confirmation) to
  prevent collision under concurrent re-add + cleanup.
- Re-add with a fresh ID → no collision; the mechanism must not break
  this existing safe path.
- `write_slot_highwater` stale after group reuse → does not
  self-heal; must be evicted in `merge` when the group disappears
  from `/topology`. Residual window (refresh lands after re-create)
  produces visible errors (empty reads), not silent corruption —
  retry resolves it.
- Server restart after `remove_group` without node_config update →
  group resurrects from persisted config; must call
  `node_config.remove_group` as part of cleanup.
- Disk move → records stay on the original paxos group (per-disk
  bind); just update group 0 placement + inline zone load. No record
  copy regardless of same-bind or cross-bind.
- Disk move while disk has active allocations → set disk to
  `HwStatus::Maintenance` before the move (`Op::Allocate` denied,
  `Op::Free` still allowed); bring back to `Up` via `Offline` after
  the move. No new status needed — existing `HwStateMachine`
  transitions cover this.
- Moved disk in multi-block allocate → no issue; after the record
  copy during Maintenance, the disk's records are on the new
  disk-group's bind, same as all other disks. Multi-block allocate
  works normally.
- `ChunkId` already globally unique and never moved → out of scope.

**Dependencies**: none on unlanded extensions. Touches:
- `crow-kv-server` (`remove_group` / `remove_store` handlers — add
  dir deletion + `node_config` update)
- `crow-kv` (`node_config` — wire existing `remove_group` /
  `remove_store` methods)
- `crow-kv-client` (`TopologyCache::merge` — add eviction;
  `write_watermark` → rename to `write_slot_highwater` + add eviction)
- `crow-kv-client` (`HardwareClient::remove_*` — add cascading
  cleanup of derived records)
- `crow-web` (console add/remove flows — wire to `HardwareClient`
  sysdata sync; disk move API)
- `crow-diskdb` (`disk_add_init` — inline zone load when snapshots
  exist; disk move record copy during Maintenance; disk move
  reconcile)
- `crow-cli` (disk move command, if exposed via CLI)

**Acceptance**:
- **Server-side group cleanup**: after `remove_group`, the WAL dir
  and engine dir for `(store_id, group_id)` are deleted, and
  `node_config` no longer contains the group. Setup → create group →
  remove group → restart server → assert group is not recreated.
  Integration test.
- **Server-side store cleanup**: after `remove_store`, the store dir
  and all group subdirs are deleted, and `node_config` no longer
  contains the store. Integration test.
- **Group 0 sysdata cleanup**: after `remove_node` /
  `remove_disk_group`, no `OwnerMapKey` / `BindMapKey` /
  `DiskGroupUsageKey` / `DiskKey` records remain for the removed
  entity. Integration test.
- **Console sysdata sync — add**: adding rack/node/disk-group/disk
  via the console API writes the corresponding group 0 sysdata record
  (`RackKey` / `NodeKey` / `DiskGroupKey` / `DiskKey`), not just the
  console config TOML. Enhance existing `crow-web` tests or add new
  ones. Integration test.
- **Console sysdata sync — remove**: removing rack/node/disk-group/
  disk via the console API deletes the group 0 sysdata record AND
  cascades to derived records (`OwnerMapKey` / `BindMapKey` /
  `DiskGroupUsageKey` / `DiskKey`). Enhance existing `crow-web` tests
  or add new ones. Integration test.
- **Cluster reset**: `POST /internal/reset` cleans group 0 sysdata
  (hardware hierarchy + KV-cluster topology) in addition to the
  console config TOML, and the server-side `remove_group` /
  `remove_store` RPCs delete WAL + engine dirs + `node_config`. After
  reset, re-init the cluster → assert clean state, no stale records.
  Enhance existing `crow-web` reset tests. Integration test.
- **Client cache eviction — TopologyCache**: after a group is removed
  and the client refreshes, the `TopologyCache` no longer contains
  the removed group's leader/replica entries. Unit test.
- **Client cache eviction — `write_slot_highwater`**: after a group
  is removed and the client refreshes, `merge` evicts the removed
  group's `write_slot_highwater` entry. A subsequent `MinSlot` read
  against a re-created group at the same ID returns correct results
  (not empty). Unit + integration test.
- **Rename `write_watermark` → `write_slot_highwater`**: the field is
  renamed in `CrowkvClient` and all references updated. No behavior
  change beyond the eviction logic. Unit test.
- **ID reuse end-to-end**: remove a group → re-create at the same
  `(store_id, group_id)` → write + read → assert correct data, no
  stale state from the old group. Integration test.
- **Disk move (record copy during Maintenance)**: move a disk to a
  new disk-group
  bound to a different paxos group → assert records copied to the new
  bind during Maintenance, disk is available at the new placement with
  correct zone usage, multi-block allocate works with new peers, no
  full recovery scan. Integration test.
- **`disk_add_init` inline zone load**: a disk with existing zone
  snapshots appearing via keepalive reconcile (not startup recovery)
  gets its zone usage loaded inline, not left empty. Unit test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run test-diskdb` (relevant integration tests pass) + any
  `test-kv-client` / `test-kv-server` tests touched by the changes.

**Open Questions**:
- **Should `disk_add_init` inline zone load replace the startup
  recovery path, or coexist?** The startup `run_recovery` path
  (`RecoveryEngine::recover_disk_group`) handles all owned
  disk-groups at boot. The inline zone load in `disk_add_init` would
  handle mid-running disk appearance. They could coexist (startup
  uses `run_recovery`, keepalive uses inline load) or the inline load
  could subsume both. Design detail for the design draft.
