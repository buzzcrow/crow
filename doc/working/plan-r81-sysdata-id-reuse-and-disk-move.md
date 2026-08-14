<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R81 sysdata ID Reuse Safety + Disk Move Plan

Design: [`design-r81-sysdata-id-reuse-and-disk-move.md`](design-r81-sysdata-id-reuse-and-disk-move.md).
Backlog: [`R81-sysdata-id-reuse-safety-and-disk-move.md`](../backlog/R81-sysdata-id-reuse-safety-and-disk-move.md).
Goal: make reusable integer IDs reuse-safe via cleanup + eviction, and
support disk move with stable UUID via record copy + Init-state load.

## Phase 1: Client cache eviction + rename (crow-kv-client)

- [x] **Rename `write_watermark` → `write_slot_highwater`**: rename
  the field in `CrowkvClient`, update `record_write`,
  `read_your_writes_slot`, `new()`. Method name `read_your_writes_slot`
  stays (public API). Files: `lib/crow-kv-client/src/client.rs`.
- [x] **TopologyCache eviction callback hook**: add
  `Option<Arc<dyn Fn(&HashSet<(u64, u64)>) + Send + Sync>>` to
  `TopologyCache`, set via `new()`. Files:
  `lib/crow-kv-client/src/topology.rs`.
- [x] **TopologyCache::merge eviction**: collect fresh `(store_id,
  group_id)` set from body; after insert loop, evict stale entries
  from `leaders` + `replicas`; call eviction hook with evicted keys.
  Files: `lib/crow-kv-client/src/topology.rs`.
- [x] **Wire eviction callback in CrowkvClient::new**: create the
  callback that removes evicted keys from `write_slot_highwater`;
  pass to `TopologyCache::new`. Files:
  `lib/crow-kv-client/src/client.rs`.
- [x] **UT: TopologyCache eviction**: build cache with 3 groups,
  merge body with 2, assert 3rd evicted from `leaders` + `replicas`.
  Files: `lib/crow-kv-client/src/topology.rs` (test module).
- [x] **UT: write_slot_highwater eviction**: covered by
  `merge_eviction_hook_fires_for_evicted_groups` test (hook fires
  with correct evicted keys). Files:
  `lib/crow-kv-client/src/topology.rs` (test module).

## Phase 2: HardwareClient cascading cleanup (crow-kv-client)

- [ ] **Add `remove_*_cascade` methods**: `remove_disk_cascade`,
  `remove_disk_group_cascade` (list disks → remove each → remove
  owner + bind + usage → remove dg), `remove_node_cascade` (list dgs
  → remove each cascade → remove node), `remove_rack_cascade` (list
  nodes → remove each cascade → remove rack). Files:
  `lib/crow-kv-client/src/hardware.rs`.
- [ ] **Add `DiskGroupUsageKey` remove method**: add
  `remove_disk_group_usage(dg_id)` to `HardwareClient` (raw delete on
  `/hw/dg_usage/{dg_id}`). Files: `lib/crow-kv-client/src/hardware.rs`.
- [ ] **UT: cascading remove**: mock group 0 with full hierarchy,
  call `remove_rack_cascade`, assert all records deleted. Files:
  `lib/crow-kv-client/src/hardware.rs` (test module) or integration
  test.

## Phase 3: Server-side group/store cleanup (crow-kv-server)

- [ ] **Wire `NodeConfigStore::remove_store`**: remove
  `#[allow(dead_code)]`. Files:
  `lib/crow-kv/src/cluster/node_config.rs`.
- [ ] **`remove_group` handler cleanup**: after `store.remove_group`,
  delete engine dir (`store_crow_tree_path`), delete WAL group
  segments, call `node_config_store.remove_group`. Files:
  `app/crow-kv-server/src/mgmt/group_ops.rs`.
- [ ] **`remove_store` handler cleanup**: after `state.remove_store`
  + shutdown, delete engine store dir, delete WAL store dir, call
  `node_config_store.remove_store`. Files:
  `app/crow-kv-server/src/mgmt/store_ops.rs`.
- [ ] **E2E: group cleanup**: create group → remove → restart →
  assert not recreated. Files: `app/crow-kv-server/tests/` (new or
  existing).
- [ ] **E2E: store cleanup**: same for store.
- [ ] **E2E: ID reuse**: remove group → re-create at same ID →
  write + read → assert correct. Files: `app/crow-kv-server/tests/`.

## Phase 4: Console sysdata sync — rack/node (crow-web)

- [ ] **`build_hardware_client` helper**: add to
  `app/crow-web/src/mgmt.rs`. Finds group 0 endpoint via monitor
  cache, builds `HardwareClient`. Files: `app/crow-web/src/mgmt.rs`.
- [ ] **Wire rack/node add/remove to HardwareClient**: `http_add_rack`
  → `hw.add_rack`; `http_remove_rack` → `hw.remove_rack_cascade`;
  `http_add_node` → `hw.add_node`; `http_remove_node` →
  `hw.remove_node_cascade`. Log `warn!` on failure, continue. Files:
  `app/crow-web/src/lifecycle/rack_node.rs`.
- [ ] **E2E: add rack sysdata sync**: add rack via console → assert
  `RackKey` in group 0. Files: `app/crow-web/tests/`.
- [ ] **E2E: remove rack sysdata sync**: remove rack via console →
  assert `RackKey` + cascaded records deleted. Files:
  `app/crow-web/tests/`.

## Phase 5: Console disk-group/disk handlers (crow-web + console-shared + CLI)

- [ ] **ConsoleConfig disk-group/disk entries**: add
  `DiskGroupEntry`, `DiskEntry` structs; add `disk_groups`,
  `disks` fields to `ConsoleConfig`; add `add_disk_group`,
  `remove_disk_group`, `add_disk`, `remove_disk` methods. Files:
  `lib/crow-console-shared/src/config.rs`.
- [ ] **ConsoleClient disk-group/disk methods**: add
  `add_disk_group`, `remove_disk_group`, `list_disk_groups`,
  `add_disk`, `remove_disk`, `list_disks`, `move_disk` HTTP wrapper
  methods. Files:
  `lib/crow-console-shared/src/console_client.rs`.
- [ ] **Disk-group HTTP handlers**: new file
  `app/crow-web/src/lifecycle/disk_group.rs` —
  `http_add_disk_group`, `http_remove_disk_group`,
  `http_list_node_disk_groups`, `http_get_node_disk_group`. Each
  updates config + calls `HardwareClient`. Files:
  `app/crow-web/src/lifecycle/disk_group.rs`.
- [ ] **Disk HTTP handlers**: new file
  `app/crow-web/src/lifecycle/disk.rs` — `http_add_disk`,
  `http_remove_disk`, `http_list_disks_in_group`, `http_get_disk`,
  `http_move_disk` (stub for now, full impl in Phase 7). Files:
  `app/crow-web/src/lifecycle/disk.rs`.
- [ ] **Register routes**: add disk-group/disk routes to router.
  Files: `app/crow-web/src/lib.rs`,
  `app/crow-web/src/lifecycle.rs` (re-export).
- [ ] **CLI disk-group commands**: new file
  `app/crow-cli/src/commands/disk_group.rs` — `DiskGroupVerb { Add,
  Remove, List }`. Files: `app/crow-cli/src/commands/disk_group.rs`,
  `app/crow-cli/src/commands/mod.rs`, `app/crow-cli/src/main.rs`.
- [ ] **CLI disk commands**: new file
  `app/crow-cli/src/commands/disk.rs` — `DiskVerb { Add, Remove,
  List, Move }` (Move stub). Files:
  `app/crow-cli/src/commands/disk.rs`,
  `app/crow-cli/src/commands/mod.rs`, `app/crow-cli/src/main.rs`.
- [ ] **E2E: disk-group/disk add/remove sysdata sync**: add via
  console → assert group 0 record; remove → assert deleted +
  cascaded. Files: `app/crow-web/tests/`.

## Phase 6: diskdb Init-state zone load (crow-diskdb)

- [ ] **`DdbDisk::new` default `Init`**: change
  `effective_status` default from `Up` to `Init`. Files:
  `app/crow-diskdb/src/model/disk.rs`.
- [ ] **`DdbDiskGroup::remove_disk_from_memory`**: add helper to
  remove from `disks` vec + `disk_index` + rebuild allocating. Files:
  `app/crow-diskdb/src/model/disk_group.rs`.
- [ ] **Delete `zone_snapshots_exist`**: remove the function. Files:
  `app/crow-diskdb/src/recovery/journal_replay.rs`,
  `app/crow-diskdb/src/recovery.rs` (re-export).
- [ ] **Replace `disk_add_init` with Init-state create**: new-disk
  path in `reconcile_disks` creates `DdbDisk` with `Init` + no zones,
  attaches metrics, adds to `dg.disks`, spawns background zone load
  task. Files: `app/crow-diskdb/src/liveness/keepalive.rs`.
- [ ] **Background zone load task**: loads each zone via
  `load_zone_inner`, transitions `Init → disk_value.status` (with
  Offline fallback for illegal transitions), calls
  `rebuild_active_zones` + `rebuild_allocating_disks`. On failure →
  `Init → Offline`. Files:
  `app/crow-diskdb/src/liveness/keepalive.rs`.
- [ ] **`load_disk_group` Init → Up transition**: after loading all
  zones, transition `Init → Up` (startup path). Files:
  `app/crow-diskdb/src/recovery.rs`.
- [ ] **`reconcile_absent_disk` skip logic**: Bad → skip; Offline,
  Maintenance, Init → `remove_disk_from_memory`; Up, Suspect, Missing
  → miss-count. Files:
  `app/crow-diskdb/src/liveness/keepalive.rs`.
- [ ] **UT: DdbDisk::new default Init**: assert default status.
  Files: `app/crow-diskdb/tests/state_machine_test.rs` or new.
- [ ] **UT: reconcile_absent_disk skip logic**: Bad skip, Offline
  remove, Maintenance remove, Init remove, Up miss-count. Files:
  `app/crow-diskdb/tests/` (new or existing).
- [ ] **UT: Init → disk_value.status**: Up → Up, Maintenance →
  Maintenance, Suspect → Offline fallback. Files:
  `app/crow-diskdb/tests/`.
- [ ] **UT: Init load failure → Offline**: strategy 2 + 1 both fail
  → `Init → Offline`, no recovery. Files: `app/crow-diskdb/tests/`.
- [ ] **E2E: Init-state zone load (mid-running)**: disk with
  snapshots appears via keepalive → Init → background load → Up.
  Files: `app/crow-diskdb/tests/`.

## Phase 7: Disk move (crow-web + crow-kv-client + crow-diskdb)

- [ ] **`copy_disk_records` method**: scan old bind by `DiskId`
  prefix, batch-write to new bind. Files:
  `lib/crow-kv-client/src/hardware.rs` or new method on
  `DdbKvClient`.
- [ ] **`http_move_disk` handler**: resolve placement, set
  Maintenance, copy records, update group 0 placement, update config.
  Files: `app/crow-web/src/lifecycle/disk.rs`.
- [ ] **CLI `disk move` command**: wire `DiskVerb::Move` to the
  console API. Files: `app/crow-cli/src/commands/disk.rs`.
- [ ] **E2E: disk move**: move disk to new dg on different paxos
  group → assert records copied, disk available at new placement,
  no full scan. Files: `app/crow-diskdb/tests/` or
  `app/crow-web/tests/`.

## Phase 8: Cluster reset (crow-web)

- [ ] **Reset sysdata cleanup**: after existing reset flow, build
  `HardwareClient`, call `remove_rack_cascade` for each rack; clean
  KV topology via `KVClusterMetaClient`. Files:
  `app/crow-web/src/lifecycle.rs`.
- [ ] **E2E: reset cleans sysdata**: reset → assert group 0 clean →
  re-init → assert clean state. Files: `app/crow-web/tests/`.

## File list

- `lib/crow-kv-client/src/client.rs` — rename field, wire eviction
- `lib/crow-kv-client/src/topology.rs` — eviction in merge + hook
- `lib/crow-kv-client/src/hardware.rs` — cascade methods, copy_disk_records
- `lib/crow-kv/src/cluster/node_config.rs` — remove dead_code allow
- `app/crow-kv-server/src/mgmt/group_ops.rs` — dir + node_config cleanup
- `app/crow-kv-server/src/mgmt/store_ops.rs` — dir + node_config cleanup
- `lib/crow-console-shared/src/config.rs` — disk-group/disk entries
- `lib/crow-console-shared/src/console_client.rs` — disk-group/disk methods
- `app/crow-web/src/mgmt.rs` — build_hardware_client helper
- `app/crow-web/src/lifecycle/rack_node.rs` — wire sysdata sync
- `app/crow-web/src/lifecycle/disk_group.rs` — new: disk-group handlers
- `app/crow-web/src/lifecycle/disk.rs` — new: disk handlers + move
- `app/crow-web/src/lifecycle.rs` — reset sysdata cleanup + re-export
- `app/crow-web/src/lib.rs` — register new routes
- `app/crow-diskdb/src/model/disk.rs` — default Init
- `app/crow-diskdb/src/model/disk_group.rs` — remove_disk_from_memory
- `app/crow-diskdb/src/liveness/keepalive.rs` — Init load, skip logic
- `app/crow-diskdb/src/recovery.rs` — Init → Up transition
- `app/crow-diskdb/src/recovery/journal_replay.rs` — delete zone_snapshots_exist
- `app/crow-cli/src/commands/disk_group.rs` — new: CLI commands
- `app/crow-cli/src/commands/disk.rs` — new: CLI commands
- `app/crow-cli/src/commands/mod.rs` — register modules
- `app/crow-cli/src/main.rs` — add to Group enum

## Test checklist

### Unit (no clean-env)

- [ ] TopologyCache eviction (3→2 groups, assert 3rd evicted)
- [ ] write_slot_highwater eviction (group missing → evicted)
- [ ] HardwareClient cascading remove (full hierarchy → all deleted)
- [ ] ConsoleConfig disk-group/disk add/remove (validation + conflict)
- [ ] DdbDisk::new default Init
- [ ] reconcile_absent_disk skip logic (Bad skip, Offline/Maintenance/Init remove, Up miss-count)
- [ ] Init → disk_value.status (Up, Maintenance, Suspect→Offline fallback)
- [ ] Init load failure → Offline (no recovery)

### Integration (clean-env prefix)

- [ ] Server-side group cleanup (remove → restart → not recreated)
- [ ] Server-side store cleanup
- [ ] ID reuse end-to-end (remove → re-create → write + read)
- [ ] Console add rack sysdata sync
- [ ] Console remove rack sysdata sync (cascaded)
- [ ] Console disk-group/disk add/remove sysdata sync
- [ ] Init-state zone load (mid-running disk appearance)
- [ ] Disk move (record copy, new placement, no full scan)
- [ ] Cluster reset cleans sysdata

### Quality gate

- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run test-kv-client` (affected tests)
- [ ] `pixi run test-kv-server` (affected tests)
- [ ] `pixi run test-diskdb` (affected tests)
- [ ] `pixi run test-console-shared` (affected tests)
- [ ] `pixi run test-console-cli` (affected tests)
