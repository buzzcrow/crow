<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Server Implementation Design (R71 + R72)

Working design draft for the diskdb server implementation. Covers both
major components: **status management** (R71 — group-0 sync, keepalive,
disk status) and **allocate/free** (R72 — zone allocator, record
persistence). Folded into `doc/design/diskdb/design-crow-diskdb.md` and
deleted after merge.

Root design: `doc/design/diskdb/design-crow-diskdb.md` (§1–§17). This
doc covers implementation details — module structure, protocol
enhancements, group-0 simulation, data structures, flows, tests.
Architecture decisions and rationale are in the root design; this doc
does not repeat them.

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
  Updated by background paths (sync, health probe, scanner) or via
  `MarkBlockSuspect` / `MarkBlockCorrupt` admin RPCs (R75) — not by
  the allocate hot path.

### 1.2 `diskdb_service.proto`

- **Add** `rpc RebuildZoneBitmap(RebuildZoneBitmapRequest) returns
  (RebuildZoneBitmapResponse)` — strategy 1 full scan rebuild (R73
  implements; stub in R72).
- **Add** `rpc MarkBlockSuspect(MarkBlockSuspectRequest) returns
  (AdminResponse)` — per-block state transition (R75 implements; stub
  in R72).
- **Add** `rpc MarkBlockCorrupt(MarkBlockCorruptRequest) returns
  (AdminResponse)` — per-block state transition (R75 implements; stub
  in R72).
- Request/response messages go in `diskdb_op.proto`.

### 1.3 `diskdb_sys_op.proto` (keepalive)

- **`KeepaliveRequest`** — add per-disk-group usage summary (§11):
  - `repeated DiskGroupUsageSummary group_usages = 4` — piggybacked on
    each keepalive tick. Each entry: `{disk_group_id, capacity_bytes,
    used_bytes, free_bytes, disk_count, allocatable_disk_count}`.
  - Group 0 maintains this at the disk-group level
    (`DiskGroupUsageKey { disk_group_id }`).

### 1.4 `diskdb_op.proto` (new messages)

- **`RebuildZoneBitmapRequest`** — `{disk_id: DiskId, zone_index: u32}`
  (one zone) or `{disk_id: DiskId, zone_index: u32 = MAX}` (all zones
  on a disk).
- **`RebuildZoneBitmapResponse`** — `{rebuilt_zone_count: u32,
  total_busy_units: u64, total_free_units: u64}`.
- **`MarkBlockSuspectRequest`** / **`MarkBlockCorruptRequest`** —
  `{segment: Segment}` (the block to mark).

---

## 2. Group-0 Simulation

The real crow-kv group-0 may not be available during R71+R72
development. Simulate it with an **in-memory mock** that serves
`DiskdbAdminService` and stores sysdata in a `HashMap<Vec<u8>,
Vec<u8>>` (binary keys → serialized values). This lets the diskdb
server's `SysdataClient` talk to a real gRPC endpoint without a full
crow-kv cluster.

### 2.1 Mock group-0 server

Create `app/crow-diskdb/tests/mock_group0.rs` (test-only, not shipped
in the binary):

- `MockGroup0Server` — an in-process tonic server implementing
  `DiskdbAdminService`:
  - Backed by a `RwLock<HashMap<Vec<u8>, Vec<u8>>>` — binary keys (from
    `lib/crow-protocol/src/key/`) → protobuf-encoded values.
  - `AddRack` / `AddNode` / `AddDiskGroup` / `AddDisk` / `RemoveDisk` /
    `SetDiskStatus` / `SetDiskGroupStatus` / `SetNodeStatus` — write
    the corresponding `*Value` / `*Meta` to the map.
  - `FetchHardware` — epoch-based: scan all `NodeKey` / `DiskKey` /
    `RackKey` entries, build `FetchHardwareResponse`, return with
    current epoch. If request epoch == current epoch, return empty
    response (no change).
  - `Keepalive` — update `InstanceMeta` heartbeat + store the
    piggybacked `DiskGroupUsageSummary` at `DiskGroupUsageKey`.
- Listens on a random port (`127.0.0.1:0`); the test harness gets the
  actual address from the server handle.
- The mock does **not** implement paxos consensus — it's a simple
  key-value store. This is sufficient for R71+R72 testing because
  diskdb's group-0 interaction is blind puts + prefix scans (§3.3).

### 2.2 SysdataClient against the mock

`SysdataClient` wraps `CrowkvClient` for group-0 operations. For
testing, `CrowkvClient` is configured to point at the mock group-0
server's address. The mock translates `get`/`scan`/`put`/`batch_write`
into HashMap lookups.

Two options for wiring:
- **(a) Mock implements the crow-kv scan/get/put API directly** — the
  mock acts as a crow-kv-compatible backend (same gRPC service as
  crow-kv). `CrowkvClient` talks to it transparently. This is
  preferred — no special test mode in `SysdataClient`.
- **(b) Mock implements `DiskdbAdminService` only** — `SysdataClient`
  has a test mode that uses the admin RPCs instead of raw KV. More
  code, less realistic.

**Choose (a)** — the mock implements the crow-kv store gRPC service
(`KvService` from `crow-kv`'s protos) for store 0 / group 0 only. This
means the mock is a minimal crow-kv-compatible single-group server.
`SysdataClient` is unchanged between test and production.

### 2.3 Test harness

- `diskdb_test_harness` helper (in `app/crow-diskdb/tests/common/`):
  - Start `MockGroup0Server` on a random port.
  - Seed it with test topology: one node, one disk-group, 2–4 disks.
  - Create `CrowkvClient` pointing at the mock.
  - Create `SysdataClient` wrapping that `CrowkvClient`.
  - Create `DataGroupClient` for zone records — also pointing at the
    mock (simulating the bound data group on the same mock server, or
    a second mock instance for the data group).
  - Return a struct with all clients + the mock server handle for
  teardown.

---

## 3. Part 1: Status Management (R71)

### 3.1 Module structure

```
app/crow-diskdb/src/
├── sysdata/
│   ├── mod.rs          — SysdataClient (group-0 read/write)
│   └── types.rs        — Rust structs for *Meta / *Value (serde + proto)
├── sync/
│   └── mod.rs          — SyncLoop (periodic sync, change detection)
├── status/
│   └── mod.rs          — StatusManager (transitions, effective status)
├── node/
│   ├── mod.rs          — NodeContainer, Node
│   └── disk.rs         — ZoneDisk (zone list, active zone set)
├── zone/
│   └── mod.rs          — Zone (bitmap, allocator — R72)
├── config.rs           — (exists, extend)
└── main.rs             — (exists, wire up)
```

### 3.2 SysdataClient

`app/crow-diskdb/src/sysdata/mod.rs` — wraps `CrowkvClient` for group-0
(store 0, group 0) read/write. Uses binary keys from
`lib/crow-protocol/src/key/`. Values are protobuf-encoded (`*Value`
messages from `diskdb_type.proto` / `common_type.proto`).

Read methods (all via `get` or `scan` with prefix constructors):
- `read_node(node_id) -> Result<Option<NodeValue>>`
- `read_all_nodes() -> Result<Vec<NodeValue>>` — prefix scan
  `NodeKey::prefix_all()`.
- `read_disk_group(node_id, dg_id) -> Result<Option<DiskGroupValue>>`
- `read_all_disk_groups() -> Result<Vec<(DiskGroupKey, DiskGroupValue)>>`
  — scan all `DiskGroupKey` entries.
- `read_disk(node_id, dg_id, disk_id) -> Result<Option<DiskValue>>`
- `read_disks_for_disk_group(node_id, dg_id) -> Result<Vec<DiskValue>>`
  — prefix scan `DiskKey::prefix_for_disk_group(node_id, dg_id)`.
- `read_owner_map() -> Result<Vec<(OwnerMapKey, OwnerEntry)>>` — scan
  all `OwnerMapKey` entries. `OwnerEntry = { instance_id: u64,
  lease_expiry_ms: u64 }`.
- `read_bind_map() -> Result<Vec<(BindMapKey, BindEntry)>>` — scan all
  `BindMapKey` entries. `BindEntry = { store_id: u64, group_id: u64 }`.
- `read_instance(instance_id) -> Result<Option<InstanceMeta>>`.

Write methods (all via `put` or `batch_write`):
- `write_node_meta(node_id, value: &NodeValue)`
- `write_disk_group_meta(node_id, dg_id, value: &DiskGroupValue)`
- `write_disk_meta(node_id, dg_id, disk_id, value: &DiskValue)`
- `write_instance_heartbeat(instance_id, endpoint: &str,
  owned_dg_ids: &[u32], group_usages: &[DiskGroupUsageSummary])` —
  updates `InstanceMeta` + writes `DiskGroupUsageKey` entries.
- `write_owner_entry(node_id, dg_id, entry: &OwnerEntry)`
- `write_bind_entry(node_id, dg_id, entry: &BindEntry)`

All writes are blind puts (no CAS, §3.3). Values are small (< 1 KB).

### 3.3 SyncLoop

`app/crow-diskdb/src/sync/mod.rs`:

- `SyncLoop` — owns `SysdataClient`, `Arc<NodeContainer>`,
  `SyncConfig`. Runs as `tokio::spawn` background task.
- `run()` — loop: `sleep(sync_interval)` → `sync_once()` → repeat.
  Fixed 10 s interval (same on success and failure — no back-off in
  v1, §10).
- `sync_once() -> Result<SyncOutcome>`:
  a. Read the ownership map from group 0. Filter to entries where
     `instance_id == self.instance_id`. These are the disk-groups this
     instance owns.
  b. Read the binding map. For each owned disk-group, look up its
     `(store_id, group_id)` — the paxos data group for zone records.
  c. For each owned disk-group, read its `DiskGroupValue` and its
     member disks' `DiskValue` (prefix scan). Build/update the
     in-memory `NodeContainer` state.
  d. **Detect changes**:
     - New disk-group assigned → add to container, trigger recovery
       (R73, stubbed in R71).
     - Disk-group removed → remove from container.
     - Disk added → `disk_add_init_flow` (§3.5 below).
     - Disk removed → remove from container.
     - Status changed → apply transition via `StatusManager`.
     - Disk/node absent from sync response → transition to `Missing`
       (§9, §10).
  e. Write instance heartbeat to group 0 (with piggybacked usage
     summary).
  f. Return `SyncOutcome { groups_added, groups_removed, disks_added,
     disks_removed, status_changes, sync_duration_ms }`.
- **Epoch/revision guard** — skip a sync response whose epoch ≤
  current (prevents stale overwrites, §10). The `FetchHardware` RPC
  is epoch-based; `SyncLoop` uses it for the bulk read, then
  reconciles with the ownership/binding maps.
- **Degraded mode** — track `missed_count` of consecutive sync
  failures. After `miss_threshold` (default 3), enter degraded mode
  (`NodeContainer.enter_degraded_mode()`). In degraded mode,
  allocate/free RPCs return `Unavailable`. On first successful sync,
  exit degraded mode.

### 3.4 StatusManager

`app/crow-diskdb/src/status/mod.rs`:

- `StatusManager` — applies status transitions and computes effective
  status. Integrated with the sync loop.
- `effective_status(node: HwStatus, group: HwStatus, disk: HwStatus)
  -> HwStatus` — `max(node, group, disk)` (three-level check, §9).
  `HwStatus` is `Ord` (ordered by severity).
- `apply_disk_status(disk_id, new_status)` — validates transition
  legality (§9), writes updated `DiskValue` to group 0, updates
  in-memory state.
- `check_temp_failure_timeouts()` — called on each sync tick;
  transitions any disk/disk-group/node in `Suspect` longer than
  `temp_failure_timeout_secs` (default 900 s) to `Offline`.
- `allows_allocate(effective) -> bool` — `effective == Up` only.
- `allows_free(effective) -> bool` — `Up`, `Maintenance`, or `Suspect`.

Transition rules (§9):
- `Init` → `{Up, Offline, Maintenance}` on startup (load from group 0).
- `Up` → `Suspect` (3 missed syncs).
- `Up` → `Offline` / `Maintenance` (operator).
- `Suspect` → `Up` (sync recovers) or → `Missing` (cannot probe) or →
  `Offline`.
- `Missing` → `Bad` (confirmed) or → `Up` (rediscovered). **Missing is
  detected by absence from a group-0 sync response.**
- `Offline` ↔ `Maintenance` (operator).
- `Offline` → `Up` (operator).

### 3.5 Disk-add initialization flow

When `SyncLoop` detects a disk in group 0 that is not yet in the
in-memory state (§10):

a. Read the `DiskValue` from group 0 (capacity, zone size, unit size).
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
   `batch_write`. These are the replay baselines (§7); subsequent
   allocates write `BusyBlockValue` records on top.
d. Call `disk.rebuild_active_zones()` — build the initial
   `ActiveZoneContext` with the first `zone_rotate_count` allocatable
   zones.
e. Add the disk to the `NodeContainer`.

### 3.6 NodeContainer

`app/crow-diskdb/src/node/mod.rs`:

- `NodeContainer` — per-instance singleton:
  - `nodes: RwLock<HashMap<DiskGroupId, Arc<Node>>>` — owned
    disk-groups.
  - `instance_id: u64`, `config: DiskdbConfig`.
  - `degraded: AtomicBool`.
- `add_node(node)`, `remove_node(dg_id)`, `get_node(dg_id) ->
  Option<Arc<Node>>`, `node_ids() -> Vec<DiskGroupId>`.
- `enter_degraded_mode()` / `exit_degraded_mode()` / `is_degraded()`.
- `Node` — disk-group manager:
  - `disk_group_id: DiskGroupId`, `node_id: u64`.
  - `bind: (u64, u64)` — `(store_id, group_id)` for the bound paxos
    data group (from the binding map).
  - `disks: RwLock<HashMap<DiskId, Arc<ZoneDisk>>>` — all disks.
  - `allocating_disks: RwLock<Arc<AllocateDiskContext>>` — RCU context
    of allocatable disks (R72).
  - `pos_v_disk_ctx: AtomicU64` — round-robin cursor (R72).
  - `status: RwLock<HwStatus>` — disk-group status.
- `ZoneDisk` — `app/crow-diskdb/src/node/disk.rs`:
  - `disk_id: DiskId`, `disk_group_id: DiskGroupId`, `node_id: u64`.
  - `disk_value: RwLock<DiskValue>` — capacity, zone size, unit size,
    status.
  - `zones: RwLock<Vec<ZoneRef>>` — all zones.
  - `active_zone_context: RwLock<Arc<ActiveZoneContext>>` — RCU active
    set (R72).
  - `pos_v_zone_ctx: AtomicU64` — round-robin cursor over active set.
  - `pos_v_zone: AtomicU64` — rotating cursor for zone rotation scan.
  - Zone management methods (`add_zone`, `rebuild_active_zones`) are
    defined here; zone **allocation** logic (CAS claim) is R72.

---

## 4. Part 2: Allocate / Free (R72)

### 4.1 Zone bitmap-scan allocator

`app/crow-diskdb/src/zone/mod.rs`:

- `Zone` — per-zone allocation state:
  - `disk_id: DiskId`, `zone_index: u32`, `disk_group_id: DiskGroupId`.
  - `zone_state: RwLock<ZoneHealth>` — `Healthy` / `Missing` / `Bad`.
    Not atomic — updated by sync loop (R71) and health probe (R76), not
    the hot path. Zones inherit the disk's `HwStatus`; no separate
    zone-level CAS state machine (§9).
  - `unit_capacity: u32` — total block units, word-aligned (§3.5).
  - `usage_bits: UsageBitmap` — lock-free atomic bit operations (from
    R70). Bit set = busy, bit clear = free.
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

### 4.2 Rotating active-zone-set + disk-level allocate

`app/crow-diskdb/src/node/disk.rs` (§8):

- `ActiveZoneContext` — `Vec<ZoneRef>` holding `zone_rotate_count`
  zones. Replaced via RCU publish.
- `disk_allocate(unit_count: u32) -> Option<(ZoneRef,
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
  init (R71) and recovery (R73).

### 4.3 Round-robin across disks within the named disk-group

`app/crow-diskdb/src/node/mod.rs` (§8):

The `AllocateBlocks` request carries `disk_group_id` (§3.2). diskdb
never round-robins across disk-groups; it round-robins across the
disks **within that one named disk-group**.

- `AllocateDiskContext` — `Vec<Arc<ZoneDisk>>` holding all allocatable
  disks within the named disk-group. Replaced via RCU publish on
  add/remove/status-change.
- `allocate_block(disk_group_id, unit_count, exclude_disks) ->
  Result<(Arc<ZoneDisk>, ZoneRef, AllocatedRange)>`:
  a. Get the `Node` for `disk_group_id` from `NodeContainer`.
  b. Read-lock `allocating_disks` (Arc clone, drop lock); check
     non-empty (else `NoSpace`).
  c. `pos_v_disk_ctx.fetch_add(1)` → round-robin. Skip disks in
     `exclude_disks` (anti-affinity, per-disk — within the named
     disk-group). Call `disk_allocate(unit_count)`. On success, return.
  d. If no disk succeeded, return `NoSpace`.
- `allocate_blocks(disk_group_id, unit_count, count, exclude_disks)
  -> Result<Vec<(Arc<ZoneDisk>, ZoneRef, AllocatedRange)>>`:
  a. For each of `count` allocations: round-robin select disk, skip
     excluded, call `disk_allocate(unit_count)`.
  b. Collect claims. If not all `count` claimed, retry remaining with
     a full scan (random start, skip excluded and already-used disks).
  c. If still not all claimed, return `NoSpace`.
- `free_block(segment: &Segment) -> Result<bool>` — look up disk by
  `segment.disk_id` via the disk-id → disk hash map (§14; O(1)),
  call `disk.free(segment.zone_index, segment.unit_offset,
  segment.unit_count)`.
- `refresh_disk_context()` — scan all disks, build new
  `AllocateDiskContext` with disks where effective `HwStatus == Up`.
  Swap in (RCU publish). Called on add/remove disk and on status
  change from the sync loop.

### 4.4 Record persistence (KV operations)

`app/crow-diskdb/src/persistence/mod.rs`:

diskdb has no "journal" abstraction — it performs plain KV put/delete
operations on the bound data group via `CrowkvClient`; crow-kv's paxos
journal is the durability mechanism (§1). The "journal" framing (a
sequence of puts/deletes replayable in slot order) is how diskdb *uses*
crow-kv's slot-ordered KV, not a concept in diskdb's code.

- `DataGroupClient` — wraps `CrowkvClient` for put/delete/scan on the
  disk-group's bound paxos data group (parallels `SysdataClient` for
  group 0). Uses `(store_id, group_id)` from the binding map (R71).
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

`app/crow-diskdb/src/persistence/alloc.rs`:

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
- `dg_id` and `bind` come from the `Node` struct (set by R71's sync
  loop from the binding map).

### 4.6 Immediate free

`app/crow-diskdb/src/persistence/free.rs` (v1: no `FreeBatch`, no
timer, no background flush loop — §8):

- `free_block(node: &Arc<Node>, segment: &Segment, kv:
  &DataGroupClient) -> Result<()>`:
  a. `node.free_block(segment)` — clear bitmap locally (per-bit CAS)
     via disk-id → disk hash map + zone-index → zone vec (§14; O(1)).
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

`app/crow-diskdb/src/grpc/service.rs`:

- `allocate_blocks` — validate `unit_count` (non-zero, aligned to block
  size) and `count` (1–1024), check not degraded, get node, call
  `persistence::allocate_blocks()`, return `Vec<Segment>`.
- `free_blocks` — parse `Vec<Segment>`, get node, call
  `persistence::free_blocks()` (immediate free in v1).
- `query_capacity_stats` — stub (returns empty); R74 fills it in.
- `get_disk_group_info` / `get_disk_info` — read from synced cache
  (R71).
- `rebuild_zone_bitmap` — stub (`Unimplemented`); R73 implements
  strategy 1.
- `mark_block_suspect` / `mark_block_corrupt` — stub
  (`Unimplemented`); R75 implements.
- Error mapping: `NoSpace` → `ResourceExhausted`, `NotOwner` →
  `PermissionDenied`, `InvalidSize`/`InvalidCount` →
  `InvalidArgument`, `Degraded` → `Unavailable`.

---

## 5. Server Wiring

`app/crow-diskdb/src/main.rs`:

1. Parse CLI, load + validate config.
2. Create `CrowkvClient` (points at crow-kv cluster; mock group-0 in
   tests).
3. Create `SysdataClient` wrapping `CrowkvClient` (for group 0).
4. Create `DataGroupClient` wrapping `CrowkvClient` (for bound data
   groups).
5. Create `NodeContainer` (shared state).
6. Create `StatusManager`.
7. Create `SyncLoop` — spawn as background task.
8. Run initial sync (blocking — server must not serve RPCs until
   first sync completes and disk-add init flows finish).
9. Create gRPC service (`DiskdbService`) wired with `NodeContainer`,
   `DataGroupClient`, config.
10. Start gRPC server (tonic) on `listen_addr`.
11. Start HTTP management server (axum) on `http_listen_addr` (minimal
    health + info endpoints; full console is R77).
12. No `FreeBatch`, no `FreeFlushLoop` in v1 (R79 adds the
    size-threshold batch when `free_batch_enabled` is true).

---

## 6. Config Extensions

`app/crow-diskdb/src/config.rs` — add to `StorageDefaults`:
- `cas_retry_limit: u32` (default 100) — per-bit CAS retry cap (§8).
- `validate_owner_on_free: bool` (default false) — strict ownership
  validation before free (§14, §16).

Add to `PersistenceConfig` (reserve for R79, default off):
- `free_batch_enabled: bool` (default false).
- `free_flush_max_batch: u32` (default 256, already exists).

Fix `PersistenceConfig` — remove `free_flush_interval_ms` (no timer in
  v1; R79 is size-threshold, no timer).

Fix `HeartbeatConfig` — rename `interval_secs` to match the 10 s sync
  interval (design §10 says 10 s, config says 13 s — align to design).

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
- `ZoneDisk::disk_allocate()` — round-robin over active set, rotation
  when exhausted.
- `StatusManager::effective_status()` — `max(node, group, disk)`.
- `StatusManager` transition rules — all legal transitions succeed,
  illegal ones rejected.
- Config validation — new fields (`cas_retry_limit`,
  `validate_owner_on_free`).

### 7.2 Integration tests (in-process mock group-0)

Using the `diskdb_test_harness` (§2.3):

- **Sync** — seed mock group-0 with topology, start `SyncLoop`,
  verify `NodeContainer` has the correct disk-groups/disks/zones.
- **Disk-add init** — add a disk to mock group-0, trigger sync,
  verify `ZoneValue` baselines written to the data group.
- **Status transition** — `SetDiskStatus` on mock group-0, trigger
  sync, verify in-memory status updated, `allocatable` reflects it.
- **Degraded mode** — stop mock group-0, wait 3 sync cycles, verify
  degraded mode; restart mock, verify recovery.
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
- Relevant tests pass (`pixi run cargo test -p crow-diskdb`).
