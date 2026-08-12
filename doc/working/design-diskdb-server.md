<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Server Implementation Design (R71 + R72)

Working design draft for the diskdb server implementation. diskdb has
two major design areas:

1. **Status management** (§3) — how diskdb handles disk-group/disk
   status changes detected via group-0 sync: new disks (disk-add init),
   missing disks (absent from sync → `Missing` → `Bad`), and status
   transitions (`Up`/`Suspect`/`Offline`/`Maintenance`/`Bad`). When a
   disk goes `Bad`, diskdb scans all impacted busy blocks on that disk
   and hands them to a future recovery/relocation path (§3.7).
2. **Allocate / free** (§4) — the zone bitmap-scan allocator, rotating
   active-zone-set, round-robin within the named disk-group, two-phase
   async allocation, and immediate free. The hard part is the **zone
   record usage bitmap** crash/error safety: the bitmap is derived from
   records (not a source of truth), so R72's allocate/free must
   preserve invariants that let R73 recover (replay + compaction) from
   any crash point (§4.8).

Folded into `doc/design/diskdb/design-crow-diskdb.md` and deleted after
merge.

Root design: `doc/design/diskdb/design-crow-diskdb.md` (§1–§18). This
doc covers implementation details — module structure, protocol
enhancements, group-0 test cluster, data structures, flows, tests.
Architecture decisions and rationale are in the root design; this doc
does not repeat them.

**R71 is implemented** (partial — see §0). R72 is the remaining work
this doc primarily targets.

---

## 0. Current State (R71, as implemented)

The R71 build diverged from the earlier draft of this doc in module
layout and the group-0 client. The implemented code is the source of
truth for those parts; the sections below describe it as built and
mark the R72 additions.

Module layout (actual — flat files, not `mod.rs` subdirs):

```
app/crow-diskdb/src/
├── lib.rs             — module declarations (config, grpc, node, status, sync)
├── main.rs            — entry point: config, clients, sync loop, gRPC server
├── config.rs          — DiskdbConfig + validate (inline unit tests)
├── grpc.rs            — DiskdbService (gRPC handlers; allocate/free stubbed)
├── node.rs            — Node (disk-group manager) + module decls
├── node/
│   ├── container.rs   — NodeContainer (owned disk-groups, degraded flag)
│   └── disk.rs        — ZoneDisk + ZoneRef (stubs; allocation is R72)
├── status.rs          — StatusManager (pure transitions; inline unit tests)
└── sync.rs            — SyncLoop (keepalive + owner-map sync)
```

No `sysdata/`, `zone/`, or `persistence/` modules exist yet. R72 adds
`zone.rs` and `persistence.rs` (or `persistence/` — see §4.0).

Group-0 client — **`SysdataClient` was not built**. The
`crow-kv-client` service classes already cover group-0 sysdata:

- `HardwareClient` (wraps `CrowkvClient`) — `list_owners() ->
  Vec<DiskdbOwnerEntry>`, `get_owner`, `set_owner`, `remove_owner`,
  `list_binds() -> Vec<KVGroupBindEntry>`, `get_bind`, `set_bind`,
  `remove_bind`, `list_disks_in_group(rack, node, dg) ->
  Vec<(DiskId, DiskValue)>`, `set_disk_status`, etc. Uses text-path
  keys + JSON values (group 0's sysdata encoding).
- `ServiceRegistryClient` — `heartbeat_diskdb(instance_id, endpoint,
  owned_dg_ids)` for keepalive. **No usage-summary piggyback param
  yet** (§1.3 / §11 want `DiskGroupUsageSummary`; the client API must
  be extended for that).

`DiskdbOwnerEntry { rack_id, node_id, dg_id, instance_id,
lease_expiry_ms }`. `KVGroupBindEntry { rack_id, node_id, dg_id,
store_id, group_id }`.

`SyncLoop` (implemented, simplified) — per tick: `heartbeat_diskdb`
→ `list_owners()` → filter to `instance_id == self` → add new `Node`s
/ remove gone `Node`s → reset `missed_count` / degraded mode. It does
**not** yet: read the bind map, read member disks, run disk-add init,
apply status transitions, or piggyback usage. `Node.bind` stays
`(0, 0)`; `Node.disks` stays empty.

`StatusManager` — implemented as a **pure, standalone** helper
(`is_legal_transition`, `apply_transition`, `effective_status`,
`allows_allocate`, `allows_free`, `check_suspect_timeout`) with inline
unit tests. **Not wired into `SyncLoop`** and does not write to group
0. Wiring + `apply_disk_status` (write-back) are R71 follow-ups
folded into the R72 sync-completion work (§3.3, §3.4).

`Node` / `ZoneDisk` — stubs. `AllocateDiskContext = ()`,
`ActiveZoneContext = ()`, `ZoneRef { zone_index: u32 }`. `disks` is
`Vec<ZoneDisk>` (not `HashMap<DiskId, Arc<ZoneDisk>>`). No
`allocating_disks`, `pos_v_disk_ctx`, `active_zone_context`, or
`pos_v_zone_ctx` fields yet — R72 adds them (§3.6, §4.2).

`UsageBitmap` (R70, `crow-protocol/src/bitmap.rs`) — `Vec<AtomicU64>`
with `range_set(offset, count) -> bool` (`fetch_or`, rollback on
double-set) and `range_clear(offset, count) -> bool` (`fetch_and`,
rollback on double-clear), plus `snapshot`/`restore`/`count_set`.
**It does not expose word-level `compare_exchange`, a rotating
`last_pos_64` cursor, `countr_one` scan, or a CAS retry bound.** R72
extends it (or adds the scan/CAS layer in `Zone`) — see §4.1.

`main.rs` — config load + validate, generate `instance_id`, build
`NodeContainer`, build `CrowkvClient` + `HardwareClient` +
`ServiceRegistryClient`, spawn `SyncLoop`, serve gRPC with shutdown.
**No `DataGroupClient`, no `StatusManager` instance, no blocking
initial sync, no HTTP management server.** R72 adds `DataGroupClient`
wiring (§5); the HTTP server is R77.

Tests — only inline unit tests in `status.rs` and `config.rs`. No
`tests/` directory, no integration tests. R72 adds the test harness
(§7) against a real group-0 cluster (§2).

Protos — still stale vs. root design (§1 below). `BusyBlockValue`
has `allocate_count` (field 3), `FreeBlockValue` has `free_count`
(field 3), `ZoneValue` carries metadata fields, no `BlockState` enum,
no `BlockState` enum. §1 is still accurate and is the first R72 step.

---

## 1. Protocol Enhancements (do first)

The protos in `lib/crow-protocol/src/proto/` are stale vs. the root
design. Fix before implementing:

### 1.1 `diskdb_type.proto`

- **`BusyBlockValue`** — current has `allocate_count` (field 3).
  Replace with:
  - `unit_count: u32` (field 1, keep)
  - `owner_chunk: ChunkId` (field 2, keep)
  - `unit_size: u32` (field 3, **replace** `allocate_count`) — size of
    one unit in bytes, carried per-block for the data-IO layer.
  - `state: BlockState` (field 4, **add**) — per-block I/O-behavior
    state. Default `BLOCK_STATE_OK` on allocate.
- **`FreeBlockValue`** — current has `free_count` (field 3). Replace
  with:
  - `unit_count: u32` (field 1, keep)
  - `previous_owner: ChunkId` (field 2, keep)
  - Drop `free_count` (field 3, **remove**) — no refcount, no version
    (§3.4).
- **`ZoneValue`** — current has `disk_offset_units`, `zone_size_units`,
  `alloc_state`, `used_units`, `usage_bitmap`. The snapshot needs:
  - `usage_bitmap: bytes` (keep) — the full zone bitmap.
  - `snapshot_slot: u64` (**add**) — the slot at which this snapshot
    was written; strategy 2 replays after this slot.
  - `crc32: u32` (**add**) — CRC32 over `usage_bitmap` for integrity.
  - `disk_offset_units`, `zone_size_units`, `alloc_state`, `used_units`
    — **drop** from `ZoneValue`; these are zone metadata / derived
    fields, not snapshot fields. `alloc_state` is derived from
    `used_count` (§9); `used_units` is derivable from the bitmap
    popcount; `disk_offset_units`/`zone_size_units` are static zone
    properties (live in the in-memory `Zone` struct, not the snapshot).
- **`BlockState` enum`** (**add**) — per-block I/O-behavior state:
  - `BLOCK_STATE_OK = 0` — normal; default on allocate.
  - `BLOCK_STATE_SUSPECT = 1` — data may be unreadable; data-IO layer
    tries with timeout, falls back to mirror/EC rebuild.
  - `BLOCK_STATE_CORRUPT = 2` — data confirmed unreadable; data-IO
    layer skips read, rebuilds from EC/mirror.
  Updated by background paths (sync, health probe, scanner) or by
  re-putting the `BusyBlockValue` with a new `state` — no dedicated
  admin RPC (a status change is a plain `put` of the busy record with
  a different `state` field). Not updated by the allocate hot path.

### 1.2 `diskdb_service.proto`

No new RPCs. `RebuildZoneBitmap` is **not** an RPC — diskdb rebuilds
zone bitmaps itself by scanning its own `BusyBlockKey` / `FreeBlockKey`
records (R73's strategy-1 full scan, run internally). Block status
transitions (suspect/corrupt) are **not** RPCs either — the caller
re-puts the `BusyBlockValue` with a new `state` field. The service
surface stays as the five existing RPCs (`AllocateBlocks`,
`FreeBlocks`, `QueryCapacityStats`, `GetDiskGroupInfo`, `GetDiskInfo`).

### 1.3 Keepalive usage piggyback

- `ServiceRegistryClient::heartbeat_diskdb` currently takes
  `(instance_id, endpoint, owned_dg_ids)`. Add a per-disk-group usage
  summary (§11): `group_usages: &[DiskGroupUsageSummary]` piggybacked
  on each keepalive tick. Each entry: `{disk_group_id,
  capacity_bytes, used_bytes, free_bytes, disk_count,
  allocatable_disk_count}`. Group 0 stores this at the disk-group
  level (`DiskGroupUsageKey { disk_group_id }`).
- This requires a new proto message (`DiskGroupUsageSummary`) and a
  `diskdb_sys_op` / keepalive request extension. The current proto
  set has no `diskdb_sys_op.proto`; the keepalive request lives in the
  service-registry proto — extend it there.

---

## 2. Test Cluster (real group 0)

A real crow-kv group 0 is available after cluster setup and init —
no mock is needed. The group-0 sysdata schema, service classes, and
bootstrap flow are fully designed in
`doc/design/kv/design-crow-kv-group0.md` (§1–§6). This section
describes how diskdb integration tests stand up a real group-0
cluster and seed the hardware/owner/bind entries diskdb depends on.

### 2.1 Group-0 background (see `design-crow-kv-group0.md`)

- Group 0 is a designated Paxos group (store 0, group 0) storing
  cluster-wide sysdata as regular KV entries — replicated, consistent,
  highly available by the same Paxos mechanism as user data
  (`design-crow-kv-group0.md` §1).
- `crow-kv-client` is the single sysdata API surface
  (`design-crow-kv-group0.md` §2.1): `HardwareClient` (hardware
  hierarchy + ownership/binding maps), `ServiceRegistryClient`
  (service registry + keep-alive), `KVClusterMetaClient` (KV-cluster
  topology). All writes are blind puts; group-0 values are
  JSON-encoded; keys are text-path (`/hw/...`, `/srv/...`, `/kv/...`).
- Hardware admin is via `HardwareClient` — there is **no**
  `DiskdbAdminService` gRPC surface (`design-crow-kv-group0.md` §2.8).
  The diskdb server serves only `DiskdbService` (allocate/free) and
  reads hardware state from group 0 via `HardwareClient` in its sync
  loop.
- Bootstrap: `http_cluster_init` (`app/crow-web/src/mgmt/cluster_init.rs`)
  calls `system_init` on each node to create group 0, wires remotes
  for multi-node, then writes the hardware hierarchy + KV-cluster
  topology into group 0 via `HardwareClient` +
  `KVClusterMetaClient` (`design-crow-kv-group0.md` §5.1). After
  init, group 0 is authoritative and `HardwareClient` reads/writes
  work against the real Paxos group.

### 2.2 Existing test harnesses (reuse)

Two patterns already exist for standing up a real crow-kv cluster in
tests:

- **In-process `PxKvStore`** — `lib/crow-kv/tests/common/cluster.rs`
  (`start_cluster`, `start_cluster_no_leader`) builds `PxKvStore` +
  `PxGroup` + election driver in-process; `TestCluster` exposes
  `kv_client(node)` returning a `KvServiceClient<Channel>` connected
  to a real gRPC endpoint. Used by crow-kv's own integration tests.
- **Spawned `crow-kv-server` binary** —
  `app/crow-kv-server/tests/common/process.rs`
  (`start_test_server_with_ports`) spawns the real `crow-kv-server`
  binary with a temp WAL dir and OS-assigned ports; `ServerHandle`
  exposes `base_url()` + `wait_for_ready()`. Used by server
  integration / startup tests.

diskdb integration tests use one of these (prefer the **spawned
binary** for end-to-end fidelity, or the **in-process `PxKvStore`**
for faster, hermetic tests that don't need the full server binary).
No new mock server is built.

### 2.3 diskdb test harness

`diskdb_test_harness` helper (in `app/crow-diskdb/tests/common/`):

- Start a real group-0 cluster (single-node self-elect is sufficient
  for most tests; multi-node for failover/degraded-mode tests):
  - Spawned-binary path: `start_test_server_with_ports(...)` →
    `ServerHandle`; call `system_init` (or the console
    `http_cluster_init` equivalent) to create group 0; wait for
    leader.
  - In-process path: `start_cluster(&[1], 1)` → `TestCluster` with a
    single-node group 0 that self-elects.
- Seed group 0 with test topology via `HardwareClient` (the same
  writes `http_cluster_init` Phase 5 does, scoped to the test's
  needs): one rack, one node, one disk-group, 2–4 disks, an owner
  entry (`set_owner`) pointing at the test diskdb instance, and a
  bind entry (`set_bind`) pointing at a data group on the same
  cluster (or a second group created via `add_group`).
- Build `CrowkvClient` seeded with the group-0 leader endpoint
  (`seed_leader(0, 0, addr)`); wrap in `HardwareClient` +
  `ServiceRegistryClient` + `DataGroupClient` (R72).
- Return a struct with all clients + the cluster/server handle for
  teardown.

The bound data group for zone records is a **real** paxos group on
the same cluster (created via the kv-server `add_group` mgmt endpoint
or `KVClusterAdmin`), not a mock. `DataGroupClient` puts/deletes/scans
go through the real Paxos path — durability, ordering, and replay
(§7, R73) are exercised exactly as in production.

---

## 3. Part 1: Status Management (R71 — complete the implementation)

R71 shipped a partial `SyncLoop` and a standalone `StatusManager`.
R72 completes the status-management wiring (bind-map read, disk reads,
disk-add init, status transitions, usage piggyback) because allocate/
free depend on disks and binds being populated. This section describes
the **target** state after R72's sync-completion work.

### 3.1 Module structure (target)

R72 keeps the flat layout and adds `zone.rs` + `persistence.rs`:

```
app/crow-diskdb/src/
├── lib.rs             — add: zone, persistence modules
├── main.rs            — add: DataGroupClient wiring
├── config.rs          — extend (§6)
├── grpc.rs            — allocate/free handlers + stubs
├── node.rs            — Node: add allocating_disks, pos_v_disk_ctx
├── node/
│   ├── container.rs   — NodeContainer (unchanged shape)
│   └── disk.rs        — ZoneDisk: add active_zone_context, cursors
├── status.rs          — StatusManager: add apply_disk_status write-back
├── sync.rs            — SyncLoop: complete (bind, disks, init, status)
├── zone.rs            — Zone bitmap-scan allocator (R72)
└── persistence.rs     — DataGroupClient + alloc/free (R72)
```

(If `persistence.rs` grows large, split into `persistence/{mod,alloc,
free}.rs` following the `node/` pattern. Start as a single file; split
only if it exceeds ~300 lines.)

### 3.2 Group-0 client — use `HardwareClient` + `ServiceRegistryClient`

No `SysdataClient` is introduced. `SyncLoop` already holds
`HardwareClient` + `ServiceRegistryClient`. The R72 sync-completion
work calls the existing methods:

- `hw.list_owners()` — owned disk-groups (already used).
- `hw.list_binds()` — bind map; for each owned disk-group, look up its
  `(store_id, group_id)` and store on `Node.bind`.
- `hw.list_disks_in_group(rack_id, node_id, dg_id)` — member disks +
  their `DiskValue` (capacity, zone size, unit size, status).
- `hw.set_disk_status(...)` — write back a disk status transition
  (used by `StatusManager::apply_disk_status`, §3.4).
- `svc.heartbeat_diskdb(instance_id, endpoint, owned_dg_ids,
  group_usages)` — after §1.3 extends the signature with usage
  summaries.

All group-0 writes are blind puts (no CAS, §3.3). Values are small
(< 1 KB).

### 3.3 SyncLoop (complete)

`app/crow-diskdb/src/sync.rs` — extend the implemented `sync_once()`:

- a. Keep-alive heartbeat (already implemented) — add the usage-summary
  piggyback once §1.3 lands.
- b. Read the ownership map via `list_owners()`; filter to
  `instance_id == self` (already implemented).
- c. **Read the bind map** via `list_binds()`; for each owned
  disk-group, store `(store_id, group_id)` on `Node.bind` (new).
- d. **For each owned disk-group, read member disks** via
  `list_disks_in_group(rack, node, dg)`; build/update the in-memory
  `Node.disks` (new). Detect:
  - New disk-group assigned → add `Node`, trigger disk-add init (§3.5).
  - Disk-group removed → remove `Node`.
  - Disk added → `disk_add_init_flow` (§3.5).
  - Disk removed → remove from `Node.disks`, `refresh_disk_context()`.
  - Status changed → apply transition via `StatusManager` (§3.4).
  - Disk absent from sync response → transition to `Missing` (§9, §10).
- e. Write instance heartbeat with piggybacked usage summary (§1.3).
- f. Return `SyncOutcome { groups_added, groups_removed, disks_added,
  disks_removed, status_changes, sync_duration_ms }` (struct already
  exists; populate the currently-unused fields).
- **Degraded mode** — already implemented (`missed_count` ≥
  `miss_threshold` → `enter_degraded_mode`; success →
  `exit_degraded_mode`). Keep.
- **Epoch/revision guard** — `FetchHardware` is epoch-based in the
  root design (§10). The current `HardwareClient` does not expose an
  epoch guard; defer until `FetchHardware` lands (R71 follow-up / R74).
  For R72, the per-tick `list_owners` + `list_binds` +
  `list_disks_in_group` reads are sufficient.

### 3.4 StatusManager (wire + write-back)

`app/crow-diskdb/src/status.rs` — the pure helpers stay. Add:

- `apply_disk_status(disk_id, new_status)` — validates transition
  legality (`is_legal_transition`, already implemented), writes the
  updated `DiskValue` to group 0 via `hw.set_disk_status(...)`, and
  updates the in-memory `ZoneDisk.disk_value`. Called from `SyncLoop`
  on detected status changes.
- `check_suspect_timeouts()` — already implemented as
  `check_suspect_timeout(suspect_since, now)`; wire it into each sync
  tick to transition `Suspect` older than `temp_failure_timeout_secs`
  (default 900 s) to `Offline`.
- `effective_status(node, group, disk)` (already implemented) — used
  by `disk_allocate` and `refresh_disk_context` to decide
  allocatability.

Transition rules (§9) — already encoded in `is_legal_transition`;
no change.

### 3.5 Disk-add initialization flow

The sync loop (§3.3) runs on a fixed 10 s interval. Each tick: diskdb
sends a keepalive heartbeat to group 0 (registering itself as a live
instance via `ServiceRegistryClient`), then reads the hardware info
for its owned disk-groups from group 0 via `HardwareClient`. It
compares the returned hardware info against its current in-memory
state and reconciles differences: `StatusManager` handles status
changes (§3.4), new disks trigger the disk-add init flow below, and
missing disks (absent from the sync response) transition to `Missing`
(§9, §10).

When `SyncLoop` detects a disk in group 0 that is not yet in
`Node.disks` (§10):

a. Read the `DiskValue` from group 0 (capacity, zone size, unit size)
   — via `list_disks_in_group`.
b. Create the in-memory `ZoneDisk` with one `Zone` per zone:
   - zone count = `capacity_units / zone_size_units` (last zone may be
     smaller, §3.5 word-alignment rule).
   - Each zone's `unit_capacity` = `zone_size_units` (last zone:
     `remaining_units` rounded down to a multiple of 64).
   - Each zone starts with an empty `usage_bits` bitmap and
     `used_count = 0`.
c. Write baseline `ZoneValue` records (empty bitmap,
   `snapshot_slot = 0`, `crc32 = compute_checksum(empty_bitmap)`) to
   the bound data group at `ZoneKey { disk_id, zone_index }` in one
   `batch_write` via `DataGroupClient`. These are the replay baselines
   (§7); subsequent allocates write `BusyBlockValue` records on top.
d. Call `disk.rebuild_active_zones()` — build the initial
   `ActiveZoneContext` with the first `zone_rotate_count` allocatable
   zones.
e. Add the disk to `Node.disks`; call `node.refresh_disk_context()`.

**Note:** R72 ships disk-add init for the *empty-disk* case (no prior
records). Recovery of a disk that already has records (restart,
re-assign) is R73's strategy-1/2 replay. For R72, the sync path
assumes a newly-added disk; R73 wires the replay-then-init path.

### 3.6 NodeContainer / Node / ZoneDisk (target fields)

`NodeContainer` (`node/container.rs`) — shape unchanged: `nodes:
RwLock<HashMap<DiskGroupId, Arc<Node>>>`, `instance_id`, `degraded`.
Methods `add_node` / `remove_node` / `get_node` / `node_ids` /
`enter_degraded_mode` / `exit_degraded_mode` / `is_degraded` already
exist.

**Naming note:** diskdb does not have a "node" concept — its
ownership unit is the **disk-group** (root design §3.3: "each
disk-group is owned by exactly one diskdb instance"). The `Node`
struct is really a **disk-group manager**; the name is inherited from
the hardware hierarchy. `node_id` / `rack_id` on `Node` and `ZoneDisk`
are carried only for group-0 key routing (`list_disks_in_group`,
`set_disk_status` — the group-0 key path is
`/hw/disk/<rack_id>/<node_id>/<dg_id>/<disk_id_hex>`), not because
diskdb "owns" a node. Allocate/free requests carry `disk_group_id`;
diskdb selects the disk and zone within that one disk-group and
performs the allocate/free operation (§4.3). No rename is planned for
R72 (the existing code uses `Node`/`NodeContainer`); a future cleanup
could rename to `DiskGroupManager`/`DiskGroupContainer`.

`Node` (`node.rs`) — add the R72 allocation fields:
- `disk_group_id`, `node_id`, `rack_id` (exist; `node_id`/`rack_id`
  for group-0 key routing — see naming note above), `status:
  RwLock<HwStatus>` (exists), `bind: RwLock<(u64, u64)>` (exists,
  populated by §3.3).
- `disks: RwLock<Vec<ZoneDisk>>` (exists; consider `HashMap<DiskId,
  Arc<ZoneDisk>>` for O(1) free-by-disk-id lookup, §14 — switch if
  free-path lookups warrant it).
- `allocating_disks: RwLock<Arc<AllocateDiskContext>>` (**add**, R72)
  — RCU context of allocatable disks within this disk-group.
- `pos_v_disk_ctx: AtomicU64` (**add**, R72) — round-robin cursor.
- Replace `type AllocateDiskContext = ()` with the real struct (§4.3).

`ZoneDisk` (`node/disk.rs`) — add the R72 zone-rotation fields:
- `disk_id`, `disk_group_id`, `node_id`, `rack_id`, `disk_value:
  RwLock<DiskValue>` (exist), `zones: RwLock<Vec<ZoneRef>>` (exists;
  `ZoneRef` becomes a real `Arc<Zone>`), `pos_v_zone: AtomicU64`
  (exists).
- `active_zone_context: RwLock<Arc<ActiveZoneContext>>` (**add**, R72).
- `pos_v_zone_ctx: AtomicU64` (**add**, R72) — round-robin over the
  active set.
- Replace `type ActiveZoneContext = ()` and `ZoneRef { zone_index }`
  with the real types (§4.2).

### 3.7 Bad-disk handling — scan impacted busy blocks

When a disk transitions to `Bad` (via `Missing → Bad` after
confirmation, root design §9), its busy blocks are no longer
readable. diskdb does **not** rebuild or relocate them inline on the
sync path — that is a future recovery requirement. R72's
responsibility is to **detect and enumerate** the impacted blocks so
a later path can handle them:

- On `Bad` transition, mark the `ZoneDisk` and all its `Zone`s as
  `Bad` (`zone_state = Bad`; `allocatable()` returns `false`).
- Scan the zone records for the bad disk (`read_zone_records` per
  zone, §4.4) and collect all live `BusyBlockValue`s — these are the
  impacted blocks. Each carries `owner_chunk` (the chunk that owns the
  allocation) so the caller/data-IO layer can be notified.
- The collected list is handed to a future recovery/relocation path
  (not in R72): the data-IO layer rebuilds from EC/mirror, or the
  owner is notified to re-allocate elsewhere. R72 stubs the hand-off
  (log + metric `disk.bad.impacted_blocks`); the actual relocation is
  a follow-up requirement.
- The disk stays `Bad` — no new allocates, no bitmap changes. Free is
  still allowed on `Bad` disks? No — `allows_free(Bad)` is `false`
  (§3.4: free allows `Up`/`Maintenance`/`Suspect` only). A `Bad`
  disk's records are read-only until an operator removes the disk or
  marks it `Up` after repair (which triggers R73 recovery).

**R72 scope:** detect `Bad`, mark zones, enumerate impacted busy
blocks, emit metrics. **Future scope:** notify owners, trigger
rebuild, relocate allocations.

---

## 4. Part 2: Allocate / Free (R72)

### 4.0 Module placement

`zone.rs` (single file) holds `Zone` + `AllocatedRange`. `persistence.rs`
holds `DataGroupClient` + the two-phase alloc/free functions; split into
`persistence/{mod,alloc,free}.rs` only if it exceeds ~300 lines. This
matches the existing flat-file convention (`node.rs` + `node/`).

### 4.1 Zone bitmap-scan allocator

`app/crow-diskdb/src/zone.rs`:

- `Zone` — per-zone allocation state:
  - `disk_id: DiskId`, `zone_index: u32`, `disk_group_id: DiskGroupId`.
  - `zone_state: RwLock<ZoneHealth>` — `Healthy` / `Missing` / `Bad`.
    Not atomic — updated by sync loop (R71) and health probe (R76), not
    the hot path. Zones inherit the disk's `HwStatus`; no separate
    zone-level CAS state machine (§9).
  - `unit_capacity: u32` — total block units, word-aligned (§3.5).
  - `usage_bits: UsageBitmap` — the R70 bitmap (lock-free atomic bit
    ops).
  - `last_pos_64: AtomicU64` — rotating cursor over 64-bit words.
  - `used_count: AtomicU32` — count of set bits.
  - `snapshot_slot: AtomicU64` — last compacted snapshot slot (R73).
  - `uncompacted_free_record_count: AtomicU32` — compaction backlog
    gauge (§11).
- `allocate(unit_count: u32) -> Option<AllocatedRange>` — Phase 1
  (sync), per-bit CAS (§8):
  a. Check `zone_state == Healthy` and `used_count < unit_capacity`.
  b. Scan bitmap from `last_pos_64` (rotating, wrapping). For each
     64-bit word, `countr_one` to find the first zero bit.
  c. CAS-set the bit via `compare_exchange`. On CAS failure, re-scan
     the same word. **CAS retry bound:** capped at `cas_retry_limit`
     (default 100); on exhaustion, fall through to the next bit / word
     / zone. Increment `zone.allocate.retry.cms.bit` counter on each
     retry.
  d. For `unit_count > 1`: find `unit_count` consecutive zero bits
     (may span words), CAS-set each; on any failure, clear bits already
     set and continue scanning.
  e. On success: update `last_pos_64`, increment `used_count`, return
     `AllocatedRange { unit_offset, unit_count }`.
- `free(unit_offset: u64, unit_count: u32) -> bool` — clear bits via
  CAS. Decrement `used_count`. Return `false` if any bit was already
  clear (double-free detection).
- `allocatable() -> bool` — `Healthy && used_count < unit_capacity`.
- `derived_alloc_state() -> ZoneAllocationState` — `Active` (0),
  `Available` (0 < used < cap), `Full` (used == cap). Reporting only.

**Bitmap API gap (important):** the existing `UsageBitmap` exposes
`range_set` / `range_clear` (whole-range `fetch_or` / `fetch_and` with
rollback) but **not** word-level `compare_exchange`, a scan-from-cursor
helper, or `countr_one`-based free-bit search. R72 must add to
`UsageBitmap` (in `crow-protocol/src/bitmap.rs`):

- `load_word(index: usize) -> u64` — `Acquire` load of one 64-bit word.
- `cas_word(index: usize, expected: u64, new: u64) -> Result<u64, u64>`
  — `compare_exchange` on one word (`AcqRel`/`Acquire`).
- `cas_bit(bit_index: u32, set: bool) -> bool` — helper wrapping
  `cas_word` with the right mask (used by `allocate`/`free` per-bit).
- Keep `range_set` / `range_clear` for R73's bulk replay; the hot path
  uses the CAS helpers + `last_pos_64` scan implemented in `Zone`.

The scan loop (`countr_one`, rotating cursor, retry bound) lives in
`Zone::allocate`, not in `UsageBitmap` — `UsageBitmap` stays a
low-level bit-vector; `Zone` owns the allocation strategy.

### 4.2 Rotating active-zone-set + disk-level allocate

`app/crow-diskdb/src/node/disk.rs` (§8):

- `ActiveZoneContext` — `Vec<Arc<Zone>>` holding `zone_rotate_count`
  zones. Replaced via RCU publish. (Replaces `type ActiveZoneContext =
  ()`.)
- `ZoneRef` → `Arc<Zone>` (replaces the `{ zone_index }` stub).
- `disk_allocate(unit_count: u32) -> Option<(Arc<Zone>,
  AllocatedRange)>`:
  a. Check disk effective `HwStatus == Up`.
  b. `max_loop = zone_num / zone_rotate_count + 2`.
  c. Loop: load `active_zone_context` (Arc clone, RCU read) →
     `pos_v_zone_ctx.fetch_add(1)` → round-robin over the active set →
     `zone.allocate(unit_count)`. On success, return. If all fail,
     `rotate_active_zones()`. If rotation returns `false` (no
     allocatable zones), break.
- `rotate_active_zones(old_ctx) -> bool`:
  a. Take write lock. RCU check: if current != `old_ctx`, another
     thread rotated — return `true` (caller retries).
  b. Scan all zones from `pos_v_zone` (rotating start), pick the first
     `zone_rotate_count` allocatable zones. Advance `pos_v_zone`.
  c. If none found: store empty context, return `false`.
  d. Build new `ActiveZoneContext`, swap in (RCU publish), return
     `true`.
- `free(zone_index, unit_offset, unit_count) -> bool` — look up zone
  by index, call `zone.free()`.
- `rebuild_active_zones()` — build initial `ActiveZoneContext` with
  the first `zone_rotate_count` allocatable zones. Called by disk-add
  init (§3.5) and recovery (R73).

### 4.3 Round-robin across disks within the named disk-group

`app/crow-diskdb/src/node.rs` (§8):

The `AllocateBlocks` request carries `disk_group_id` (§3.2). diskdb
never round-robins across disk-groups; it round-robins across the
disks **within that one named disk-group**.

- `AllocateDiskContext` — `Vec<Arc<ZoneDisk>>` holding all allocatable
  disks within the named disk-group. Replaced via RCU publish on
  add/remove/status-change. (Replaces `type AllocateDiskContext = ()`.)
- `allocate_block(disk_group_id, unit_count, exclude_disks) ->
  Result<(Arc<ZoneDisk>, Arc<Zone>, AllocatedRange)>`:
  a. Get the `Node` for `disk_group_id` from `NodeContainer`.
  b. Read-lock `allocating_disks` (Arc clone, drop lock); check
     non-empty (else `NoSpace`).
  c. `pos_v_disk_ctx.fetch_add(1)` → round-robin. Skip disks in
     `exclude_disks` (anti-affinity, per-disk — within the named
     disk-group). Call `disk_allocate(unit_count)`. On success, return.
  d. If no disk succeeded, return `NoSpace`.
- `allocate_blocks(disk_group_id, unit_count, count, exclude_disks)
  -> Result<Vec<(Arc<ZoneDisk>, Arc<Zone>, AllocatedRange)>>`:
  a. For each of `count` allocations: round-robin select disk, skip
     excluded, call `disk_allocate(unit_count)`.
  b. Collect claims. If not all `count` claimed, retry remaining with
     a full scan (random start, skip excluded and already-used disks).
  c. If still not all claimed, return `NoSpace`.
- `free_block(segment: &Segment) -> Result<bool>` — look up disk by
  `segment.disk_id` via the disk-id → disk map (§14; O(1) — if
  `disks` is a `Vec`, build a side `HashMap<DiskId, Arc<ZoneDisk>>`
  index alongside `allocating_disks`), call `disk.free(...)`.
- `refresh_disk_context()` — scan all disks, build new
  `AllocateDiskContext` with disks where effective `HwStatus == Up`.
  Swap in (RCU publish). Called on add/remove disk and on status
  change from the sync loop.

### 4.4 Record persistence (KV operations)

`app/crow-diskdb/src/persistence.rs`:

diskdb has no "journal" abstraction — it performs plain KV put/delete
operations on the bound data group via `CrowkvClient`; crow-kv's paxos
journal is the durability mechanism (§1). The "journal" framing (a
sequence of puts/deletes replayable in slot order) is how diskdb *uses*
crow-kv's slot-ordered KV, not a concept in diskdb's code.

- `DataGroupClient` — wraps `CrowkvClient` for put/delete/scan on the
  disk-group's bound paxos data group (parallels `HardwareClient` for
  group 0). Uses `(store_id, group_id)` from `Node.bind` (set by §3.3).
  Keys are the binary keys from `lib/crow-protocol/src/key/diskdb.rs`
  (`BusyBlockKey` / `FreeBlockKey` / `ZoneKey`), encoded via
  `BinaryKey::encode`.
- `persist_busy(dg_id, bind, disk_id, zone_idx, unit_offset, value:
  &BusyBlockValue) -> Result<()>` — `put` to `BusyBlockKey`.
- `persist_busy_batch(dg_id, bind, records: &[(disk_id, zone_idx,
  unit_offset, BusyBlockValue)]) -> Result<()>` — `batch_write` all
  records in one async round-trip (multi-block allocate; one
  `batch_write` per data group, atomic within the group).
- `persist_free(dg_id, bind, disk_id, zone_idx, unit_offset, value:
  &FreeBlockValue) -> Result<()>` — one `batch_write` that **deletes
  the `BusyBlockKey`** and **puts the `FreeBlockValue`** at
  `FreeBlockKey` (per §3.4/§7 record model).
- `persist_free_batch(dg_id, bind, records: &[(disk_id, zone_idx,
  unit_offset, FreeBlockValue)]) -> Result<()>` — `batch_write` that,
  for each record, deletes `BusyBlockKey` + puts `FreeBlockKey` (one
  round-trip per data group). Reused by R79's size-threshold batch.
- `read_zone_records(dg_id, bind, disk_id, zone_idx) ->
  Result<ZoneRecords>` — prefix scan `BusyBlockKey` +
  `FreeBlockKey` + `ZoneKey` for one zone. Used by R73 recovery.
- `delete_free_records_batch(dg_id, bind, keys: &[Vec<u8>]) ->
  Result<()>` — `batch_write` with `Delete` ops for free records only.
  Used by R73 compaction.

### 4.5 Two-phase async allocation

`app/crow-diskdb/src/persistence.rs` (or `persistence/alloc.rs`):

- `allocate_block(node: &Arc<Node>, disk_group_id, unit_count,
  owner_chunk: &ChunkId, unit_size: u32, kv: &DataGroupClient) ->
  Result<Segment>`:
  a. **Phase 1 (sync)**: `node.allocate_block(disk_group_id,
     unit_count, &[])` → `(disk, zone, range)`. Bits set via per-bit
     CAS. No zone-level lock.
  b. **Phase 2 (async)**: Build `BusyBlockValue { unit_count,
     unit_size, owner_chunk, state: BlockState::Ok }`. `await
     kv.persist_busy(dg_id, bind, disk_id, zone_idx,
     range.unit_offset, &value)`.
  c. On success: return `Segment { disk_id, zone_index, unit_offset,
     unit_count, owner_chunk }` (no `node_id`/`disk_group_id` in
     `Segment`, §3.9).
  d. On failure: `zone.free(range.unit_offset, range.unit_count)`
     (rollback — clear the bits set in Phase 1), return error.
- `allocate_blocks(node, disk_group_id, unit_count, count,
  exclude_disks, owner_chunk, unit_size, kv) -> Result<Vec<Segment>>`:
  a. Phase 1: `node.allocate_blocks(...)` → `Vec<(disk, zone, range)>`.
  b. Phase 2: Build `BusyBlockValue` for each claim. `await
     kv.persist_busy_batch(dg_id, bind, &records)` (one `batch_write`
     per data group).
  c. On success: return `Vec<Segment>`.
  d. On failure: `zone.free()` ALL claims (rollback every bit), return
     error.
- `dg_id` and `bind` come from the `Node` struct (set by §3.3's sync
  loop from the binding map).

### 4.6 Immediate free

`app/crow-diskdb/src/persistence.rs` (or `persistence/free.rs`) (v1:
no `FreeBatch`, no timer, no background flush loop — §8):

- `free_block(node: &Arc<Node>, segment: &Segment, kv:
  &DataGroupClient) -> Result<()>`:
  a. `node.free_block(segment)` — clear bitmap locally (per-bit CAS)
     via disk-id → disk map + zone-index → zone vec (§14; O(1) lookups).
  b. Build `FreeBlockValue { unit_count, previous_owner:
     segment.owner_chunk }` (`previous_owner` from the `Segment` — no
     KV read needed).
  c. `await kv.persist_free(dg_id, bind, disk_id, zone_idx,
     unit_offset, &value)` — one `batch_write`: delete `BusyBlockKey`
     + put `FreeBlockValue` at `FreeBlockKey`.
  d. Return the persist result. Free is synchronous in v1 — the
     caller's `FreeBlocks` RPC returns only after the `FreeBlockValue`
     is durable and the `BusyBlockKey` is gone.
- `free_blocks(node, segments: &[Segment], kv) -> Result<()>`:
  a. For each segment: `node.free_block(segment)` (clear bitmap
     locally).
  b. Group by `dg_id`, build `FreeBlockValue` list per data group.
  c. For each affected data group: `await kv.persist_free_batch(...)`
     (one `batch_write`: delete each `BusyBlockKey` + put each
     `FreeBlockValue`).
  d. On failure: bitmap clears already happened locally — return error;
     the §12 ghost-allocation scanner reconciles on restart.
- **No KV read on free in v1** (§14): `owner_chunk` is in the
  `Segment` → becomes `FreeBlockValue.previous_owner`. The free is one
  `batch_write` (Delete `BusyBlockKey` + Put `FreeBlockKey`) with no
  prior read. Ownership validation deferred to the §12 scanner. Config
  toggle `validate_owner_on_free` (default false) enables a KV read of
  `BusyBlockValue` first (one paxos round-trip, doubles free latency).
- **Free batching (R79):** when `free_batch_enabled` is true (default
  false), the free path groups frees and flushes via one `batch_write`
  when the batch reaches `free_flush_max_batch` (default 256). No
  timer. R72 ships with the toggle off — immediate free only.

### 4.7 gRPC handlers

`app/crow-diskdb/src/grpc.rs`:

- `allocate_blocks` — validate `unit_count` (non-zero, aligned to block
  size) and `count` (1–1024), check not degraded, get node, call
  `persistence::allocate_blocks()`, return `Vec<Segment>`.
- `free_blocks` — parse `Vec<Segment>`, get node, call
  `persistence::free_blocks()` (immediate free in v1).
- `query_capacity_stats` — stub (returns empty); R74 fills it in.
- `get_disk_group_info` / `get_disk_info` — already implemented (read
  from synced cache).
- Error mapping: `NoSpace` → `ResourceExhausted`, `NotLeaderHint`/not
  owner → `PermissionDenied`, `InvalidSize`/`InvalidCount` →
  `InvalidArgument`, `Degraded` → `Unavailable`.

### 4.8 Crash-safety invariants + compaction relationship

The zone usage bitmap is **derived from records** (root design §3.4:
"records are the source of truth; bitmap is derived"). The in-memory
bitmap is a performance cache; the durable state is the set of
`BusyBlockKey` / `FreeBlockKey` / `ZoneValue` records on the bound
data group. R72's allocate/free must preserve invariants that let R73
recover correctly from **any** crash point. This is the hard part of
the design.

**Record-model invariants (R72 must preserve):**

- **Allocate ordering** — Phase 1 (bitmap CAS) happens **before**
  Phase 2 (`BusyBlockValue` persist). If diskdb crashes between Phase
  1 and Phase 2, the bit is set in memory but no `BusyBlockKey`
  exists on disk. On restart, R73's strategy-1 full scan rebuilds the
  bitmap from records — the bit is **clear** (no busy record), so the
  block is correctly free. This is a **ghost allocation** (bit set
  in-memory, no record) that is self-correcting on restart. The §12
  scanner also detects this drift during live operation.
- **Free atomicity** — the free is one `batch_write` (Delete
  `BusyBlockKey` + Put `FreeBlockValue` at `FreeBlockKey`), atomic
  within the data group (crow-kv paxos). If diskdb crashes after the
  bitmap clear but before the `batch_write` persists, the bit is clear
  in memory but the `BusyBlockKey` still exists on disk. On restart,
  R73's full scan sees the `BusyBlockKey` and re-sets the bit — the
  block is correctly busy. The §12 scanner reconciles this drift
  during live operation (ghost free).
- **Current-state rule** (root design §7): a block is **busy** iff its
  `BusyBlockKey` exists; otherwise it is **free**. A `FreeBlockKey`
  may exist for a not-yet-compacted free (carrying `previous_owner`);
  after compaction, neither key exists for that offset. This rule
  holds at every crash point because allocate and free are each a
  single durable operation (one `put` or one `batch_write`).
- **Re-allocate clears the free marker** — on re-allocate (after a
  free), the `FreeBlockKey` is deleted and a new `BusyBlockValue` is
  written at `BusyBlockKey`. R72's `persist_busy` path must delete any
  prior `FreeBlockKey` for the same offset (in the same `batch_write`
  as the `BusyBlockValue` put) so the record set stays consistent.

**Bitmap crash safety:**

- The in-memory bitmap is **never the source of truth**. On restart,
  R73 rebuilds it from records (strategy 2 journal-scan replay, or
  strategy 1 full scan as fallback). R72 does not persist the bitmap
  on the allocate/free hot path — only the `ZoneValue` snapshot
  (written by R73 compaction) carries a serialized bitmap, with
  `snapshot_slot` (replay start point) + `crc32` (integrity check).
- R72 writes the **baseline** `ZoneValue` (empty bitmap,
  `snapshot_slot = 0`) during disk-add init (§3.5). All subsequent
  state changes are `BusyBlockValue` / `FreeBlockValue` records; R73
  compaction periodically merges free records into a fresh `ZoneValue`
  snapshot and deletes the free records.

**Compaction relationship (R73 — R72 must not block it):**

- Compaction (strategy 3, root design §7) periodically scans free
  records for a zone, merges them into the `ZoneValue` bitmap (clears
  the freed bits), writes a new `ZoneValue` snapshot with
  `snapshot_slot = current_max_slot` + `crc32`, and deletes the free
  records in one `batch_write`.
- R72's `persist_free` writes `FreeBlockValue` records that compaction
  will later merge. R72's `delete_free_records_batch` (§4.4) is the
  method R73 compaction calls to delete the merged free records.
- R72's `uncompacted_free_record_count` gauge (§4.1) tracks the
  compaction backlog — when it exceeds `snapshot_journal_threshold`
  (config, default 4096), R73 compaction triggers. R72 maintains the
  gauge (increment on free, decrement on compaction — the decrement
  is R73's call).
- **R72 must not delete free records itself** — only compaction
  (R73) deletes free records, after merging them into the snapshot.
  R72's allocate path deletes a `FreeBlockKey` only on re-allocate
  (when the offset is reused), which is correct: the free record is
  superseded by a new busy record.

**Error handling on the hot path:**

- **Allocate persist failure** (Phase 2 `batch_write` fails): rollback
  the bitmap (clear the bits set in Phase 1) and return error. The
  caller retries or reports failure. No record was written, so the
  record set is consistent (no ghost).
- **Free persist failure** (`batch_write` fails): the bitmap was
  already cleared locally. Return error; the §12 scanner reconciles
  on restart (the `BusyBlockKey` still exists on disk → block is
  busy → scanner detects the bitmap/record drift and re-sets the
  bit). The caller's `FreeBlocks` RPC returns failure; the caller may
  retry.
- **Degraded mode** (group-0 / data-group unreachable): allocate/free
  RPCs return `Unavailable` before touching the bitmap. No partial
  state.

---

## 5. Server Wiring

`app/crow-diskdb/src/main.rs` — extend the implemented wiring:

1. Parse CLI, load + validate config (exists).
2. Generate `instance_id` (exists).
3. Build `CrowkvClient` + `HardwareClient` + `ServiceRegistryClient`
   (exist).
4. **Create `DataGroupClient` wrapping `CrowkvClient`** (new — for
   bound data groups).
5. Create `NodeContainer` (exists).
6. **Create `StatusManager`** (new — wire into `SyncLoop`).
7. `SyncLoop` — spawn as background task (exists); pass
   `DataGroupClient` + `StatusManager` so sync can run disk-add init
   and status write-backs.
8. **Run initial sync (blocking)** — server must not serve RPCs until
   the first sync completes and disk-add init flows finish (new; the
   current loop is spawn-and-go).
9. gRPC service (`DiskdbService`) — wire with `NodeContainer`,
   `DataGroupClient`, config (add `DataGroupClient` to the service
   struct; allocate/free handlers use it).
10. Start gRPC server (exists).
11. HTTP management server (axum) — **R77**, not R72. Leave the
    `http_listen_addr` config field in place; do not start the server.
12. No `FreeBatch`, no `FreeFlushLoop` in v1 (R79 adds the
    size-threshold batch when `free_batch_enabled` is true).

---

## 6. Config Extensions

`app/crow-diskdb/src/config.rs` — current state vs. target:

- `StorageDefaults` currently has `zone_size_bytes`, `block_size_bytes`,
  `allocate_granularity`, `zone_rotate_count`. **Add**:
  - `cas_retry_limit: u32` (default 100) — per-bit CAS retry cap (§8).
  - `validate_owner_on_free: bool` (default false) — strict ownership
    validation before free (§14, §16).
- `PersistenceConfig` currently has `free_flush_interval_ms`,
  `free_flush_max_batch`, `snapshot_interval_secs`,
  `snapshot_journal_threshold`. **Add** (reserve for R79, default off):
  - `free_batch_enabled: bool` (default false).
  - `free_flush_max_batch` (default 256, already exists).
  - **Remove `free_flush_interval_ms`** — no timer in v1; R79 is
    size-threshold, no timer.
- `HeartbeatConfig.interval_secs` — already 10 (aligned to design §10).
  Keep.
- `SyncConfig` — `group0_store_id`, `group0_group_id`,
  `sync_interval_secs` (all exist, 10 s). Keep.
- Add validation for the new fields (`cas_retry_limit > 0`).

---

## 7. Test Strategy

### 7.1 Unit tests (no external deps)

- `Zone::allocate()` — concurrent allocations on the same zone
  serialize via per-bit CAS (no double-alloc, all bits unique).
- `Zone::free()` — double-free detection (free an allocated bit, then
  free again → `false`).
- `Zone::allocate(unit_count > 1)` — multi-unit range, bits contiguous.
- `Zone::allocate()` CAS retry bound — force contention, verify
  `zone.allocate.retry.cms.bit` counter increments and the allocate
  falls through after `cas_retry_limit` retries.
- `Zone::derived_alloc_state()` — Active / Available / Full transitions.
- `UsageBitmap` — new `load_word` / `cas_word` / `cas_bit` helpers
  (round-trip, CAS success/failure).
- `ZoneDisk::disk_allocate()` — round-robin over active set, rotation
  when exhausted.
- `StatusManager` — already has unit tests (transitions, effective
  status, allows_allocate/free). Add `apply_disk_status` write-back
  test once wired.
- Config validation — new fields (`cas_retry_limit`,
  `validate_owner_on_free`).
- **Crash-safety invariants** (§4.8) — verify the record-model
  invariants hold: allocate-then-crash-before-persist → no
  `BusyBlockKey` (block is free by the current-state rule);
  free-then-crash-before-persist → `BusyBlockKey` still exists (block
  is busy); re-allocate deletes the prior `FreeBlockKey` in the same
  `batch_write` as the new `BusyBlockValue` put.
- **Bad-disk handling** (§3.7) — mark a disk `Bad`, verify all its
  zones transition to `Bad`, `allocatable()` returns `false`, and the
  impacted busy blocks are enumerable (scan returns the live
  `BusyBlockValue`s with `owner_chunk`).

### 7.2 Integration tests (real group-0 cluster)

Using the `diskdb_test_harness` (§2.3) against a real group-0
cluster:

- **Sync** — seed group 0 with topology via `HardwareClient`, start
  `SyncLoop`, verify `NodeContainer` has the correct
  disk-groups/disks/zones and `Node.bind` is populated.
- **Disk-add init** — add a disk to group 0 via `HardwareClient`,
  trigger sync, verify `ZoneValue` baselines written to the bound
  data group.
- **Status transition** — `set_disk_status` on group 0, trigger sync,
  verify in-memory status updated, `allocatable` reflects it.
- **Degraded mode** — stop the group-0 leader (or kill the
  group-0 server), wait 3 sync cycles, verify degraded mode; restart,
  verify recovery.
- **Bad disk** — allocate blocks on a disk, set its status to `Bad`
  in group 0, trigger sync, verify the disk's zones go `Bad`,
  allocates stop serving that disk, and the impacted busy blocks are
  enumerable via `read_zone_records`.
- **Allocate (single)** — allocate one block, verify `BusyBlockValue`
  in the data group at the expected `BusyBlockKey` with fields
  `{ unit_count, unit_size, owner_chunk, state: Ok }`.
- **Allocate (multi)** — allocate N blocks, verify all
  `BusyBlockValue`s in one `batch_write`.
- **Allocate rollback** — force KV persist failure, verify bitmap
  bits cleared (rollback).
- **Free (single)** — free a block, verify `FreeBlockValue` in the
  data group (carrying `previous_owner`) and `BusyBlockKey` **deleted**.
- **Free (multi)** — free N blocks, verify all in one `batch_write`
  per data group, `BusyBlockKey`s deleted.
- **Allocate after free** — allocate → free → allocate same offset,
  verify `FreeBlockKey` deleted, new `BusyBlockValue` written.
- **gRPC end-to-end** — `AllocateBlocks` / `FreeBlocks` via gRPC,
  verify error mapping (`NoSpace` → `ResourceExhausted`, etc.).
- **Round-robin** — allocate many blocks, verify distribution across
  disks within the named disk-group (not across disk-groups).
- **`exclude_disks`** — allocate with excluded disks, verify they are
  skipped.

### 7.3 Verification commands

- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
- Relevant tests pass (`pixi run cargo test -p crow-diskdb` and
  `pixi run cargo test -p crow-protocol` for the bitmap additions).
