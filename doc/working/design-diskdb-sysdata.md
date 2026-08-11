<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Group-0 Sysdata + Sync Loop + Status Management Design (R71)

Working design draft for the diskdb server's first major component:
group-0 sysdata read/write, the sync loop, and disk status management.
Covers the `SysdataClient`, `SyncLoop`, `StatusManager`,
`NodeContainer`, disk-add initialization flow, and the admin gRPC
handlers. Folded into `doc/design/diskdb/design-crow-diskdb.md` and
deleted after merge.

Root design: `doc/design/diskdb/design-crow-diskdb.md` (§3.1, §3.3,
§5, §9, §10, §16). Server implementation design (R71 + R72):
`doc/working/design-diskdb-server.md` covers the combined view. This
doc covers R71-specific implementation details — the sysdata client,
sync loop, status manager, data structures, flows, tests. Architecture
decisions and rationale are in the root design; this doc does not
repeat them.

---

## 1. Protocol Review (do first)

The protos in `lib/crow-protocol/src/proto/` are mostly in place for
R71. Review and fix:

### 1.1 Missing value types

Three sysdata value types are referenced in the design doc §5 but not
yet defined as proto messages:

- **`InstanceValue`** — stored at `InstanceKey { instance_id }`. Fields:
  `instance_id: u64`, `grpc_endpoint: string`, `last_heartbeat_ms: u64`,
  `owned_dg_ids: repeated u32`. Add to `diskdb_type.proto`.
- **`OwnerEntry`** — stored at `OwnerMapKey { node_id,
  disk_group_id }`. Fields: `instance_id: u64`, `lease_expiry_ms:
  u64`. Add to `diskdb_type.proto`.
- **`BindEntry`** — stored at `BindMapKey { node_id, disk_group_id }`.
  Fields: `store_id: u64`, `group_id: u64`. Add to `diskdb_type.proto`.

These are internal sysdata types (not exposed in RPC responses), but
they are stored as KV values in group 0 and need a defined encoding.
Per §3.8, proto types are used directly — no Rust type duplication.

### 1.2 `KeepaliveRequest` enhancement

Add per-disk-group usage summary piggybacked on keepalive (§11):
- `repeated DiskGroupUsageSummary group_usages = 4` — each entry:
  `{disk_group_id, capacity_bytes, used_bytes, free_bytes, disk_count,
  allocatable_disk_count}`. Group 0 maintains this at the disk-group
  level.

### 1.3 `DiskdbAdminService` — served by mock group-0

The admin RPCs (`AddRack`, `AddNode`, `AddDiskGroup`, `AddDisk`,
`SetDiskStatus`, `FetchHardware`, `Keepalive`, etc.) target group 0.
In production, a future admin/console component serves them. For R71
testing, the mock group-0 server implements them (see §2 below).

The diskdb server itself does **not** serve `DiskdbAdminService` — it
serves `DiskdbService` (allocate/free/query/info RPCs). R71 implements
the `DiskdbService` handlers that are status-related
(`GetDiskGroupInfo`, `GetDiskInfo`, `QueryCapacityStats` stub); the
allocate/free handlers are stubs (`Unimplemented`) until R72.

---

## 2. Group-0 Simulation

The real crow-kv group-0 may not be available during R71 development.
Simulate it with an in-process mock that serves `DiskdbAdminService`
and stores sysdata in a `HashMap<Vec<u8>, Vec<u8>>` (binary keys →
protobuf-encoded values). This lets the diskdb server's `SysdataClient`
talk to a real gRPC endpoint without a full crow-kv cluster.

### 2.1 Mock group-0 server

Create `app/crow-diskdb/tests/mock_group0.rs` (test-only, not shipped
in the binary):

- `MockGroup0Server` — an in-process tonic server implementing the
  crow-kv `KvService` for store 0 / group 0 only:
  - Backed by a `RwLock<HashMap<Vec<u8>, Vec<u8>>>` — binary keys (from
    `lib/crow-protocol/src/key/`) → protobuf-encoded values.
  - `Put` / `Get` / `Delete` / `BatchWrite` / `Scan` — straightforward
    HashMap operations. `Scan` filters by prefix, sorts keys, paginates
    via `start_after` / `end_key` / `limit`.
  - The mock does **not** implement paxos consensus — it's a simple
    key-value store. This is sufficient for R71 testing because
    diskdb's group-0 interaction is blind puts + prefix scans (§3.3).
- Listens on a random port (`127.0.0.1:0`); the test harness gets the
  actual address from the server handle.

### 2.2 SysdataClient against the mock

`SysdataClient` wraps `CrowkvClient` for group-0 operations. For
testing, `CrowkvClient` is configured to point at the mock group-0
server's address. The mock acts as a crow-kv-compatible backend (same
`KvService` gRPC service), so `CrowkvClient` talks to it transparently
— no special test mode in `SysdataClient`.

### 2.3 Test harness

- `diskdb_test_harness` helper (in `app/crow-diskdb/tests/common/`):
  - Start `MockGroup0Server` on a random port.
  - Seed it with test topology: one node, one disk-group, 2–4 disks
    (via `Put` operations using the binary key types).
  - Create `CrowkvClient` pointing at the mock.
  - Create `SysdataClient` wrapping that `CrowkvClient`.
  - Return a struct with all clients + the mock server handle for
    teardown.

---

## 3. SysdataClient

`app/crow-diskdb/src/sysdata/mod.rs` — wraps `CrowkvClient` for
group-0 (store 0, group 0) read/write. Uses binary keys from
`lib/crow-protocol/src/key/`. Values are protobuf-encoded (`*Value`
messages from `common_type.proto` / `diskdb_type.proto`) per §3.8 — no
serde_json, no Rust type duplication.

### 3.1 Read methods (all via `get` or `scan` with prefix constructors)

- `read_node(node_id: u64) -> Result<Option<NodeValue>>` — `get` from
  `NodeKey { node_id }`.
- `read_all_nodes() -> Result<Vec<(NodeKey, NodeValue)>>` — prefix scan
  `NodeKey::prefix_all()` (tag `0x0001`).
- `read_disk_group(node_id: u64, dg_id: u32) ->
  Result<Option<DiskGroupValue>>` — `get` from `DiskGroupKey { node_id,
  disk_group_id }`.
- `read_all_disk_groups() -> Result<Vec<(DiskGroupKey,
  DiskGroupValue)>>` — prefix scan `DiskGroupKey::prefix_all()` (tag
  `0x0003`).
- `read_disk(node_id: u64, dg_id: u32, disk_id: DiskId) ->
  Result<Option<DiskValue>>` — `get` from `DiskKey`.
- `read_disks_for_disk_group(node_id: u64, dg_id: u32) ->
  Result<Vec<(DiskKey, DiskValue)>>` — prefix scan
  `DiskKey::prefix_for_disk_group(node_id, dg_id)` (tag `0x0004`).
- `read_owner_map() -> Result<Vec<(OwnerMapKey, OwnerEntry)>>` — prefix
  scan `OwnerMapKey::prefix_all()` (tag `0x0008`).
- `read_bind_map() -> Result<Vec<(BindMapKey, BindEntry)>>` — prefix
  scan `BindMapKey::prefix_all()` (tag `0x0009`).
- `read_instance(instance_id: u64) -> Result<Option<InstanceValue>>` —
  `get` from `InstanceKey { instance_id }` (tag `0x000A`).

### 3.2 Write methods (all via `put` or `batch_write`; blind puts, no CAS)

- `write_node_value(node_id: u64, value: &NodeValue) -> Result<()>`.
- `write_disk_group_value(node_id: u64, dg_id: u32, value:
  &DiskGroupValue) -> Result<()>`.
- `write_disk_value(node_id: u64, dg_id: u32, disk_id: DiskId, value:
  &DiskValue) -> Result<()>`.
- `write_instance_heartbeat(instance_id: u64, endpoint: &str,
  owned_dg_ids: &[u32], group_usages: &[DiskGroupUsageSummary]) ->
  Result<()>` — updates `InstanceValue` with `last_heartbeat_ms =
  now()`, writes `InstanceKey`, and stores the piggybacked usage
  summaries.
- `write_owner_entry(node_id: u64, dg_id: u32, entry: &OwnerEntry) ->
  Result<()>`.
- `write_bind_entry(node_id: u64, dg_id: u32, entry: &BindEntry) ->
  Result<()>`.

All writes are blind puts (no CAS, §3.3). Values are small (< 1 KB).

### 3.3 Key encoding

All keys use the binary encoding from `lib/crow-protocol/src/key/`:
`magic:1 | type_tag:2 | fields:big-endian-fixed-width`. Prefix
constructors (`prefix_all()`, `prefix_for_node()`,
`prefix_for_disk_group()`) truncate at field boundaries for prefix
scans. No string keys, no serde_json — the R71 backlog's old
`/diskdb/node/{uuid}/meta` string paths are replaced by binary keys.

---

## 4. SyncLoop

`app/crow-diskdb/src/sync/mod.rs`:

### 4.1 Structure

- `SyncLoop` — owns `SysdataClient`, `Arc<NodeContainer>`,
  `SyncConfig`. Runs as `tokio::spawn` background task.
- `run()` — loop: `sleep(sync_interval)` → `sync_once()` → repeat.
  Fixed 10 s interval (same on success and failure — no back-off in
  v1, §10).

### 4.2 `sync_once`

```rust
async fn sync_once(&self) -> Result<SyncOutcome>
```

a. Read the ownership map from group 0 (`read_owner_map`). Filter to
   entries where `instance_id == self.instance_id`. These are the
   disk-groups this instance owns.
b. Read the binding map (`read_bind_map`). For each owned disk-group,
   look up its `(store_id, group_id)` — the paxos data group for zone
   records.
c. For each owned disk-group, read its `DiskGroupValue` and its member
   disks' `DiskValue` (prefix scan
   `DiskKey::prefix_for_disk_group`). Build/update the in-memory
   `NodeContainer` state.
d. **Detect changes**:
   - New disk-group assigned → add to container, trigger disk-add init
     flow (§6 below) for each disk, then trigger recovery (R73,
     stubbed in R71).
   - Disk-group removed → remove from container.
   - Disk added → disk-add init flow (§6).
   - Disk removed → remove from container.
   - Status changed → apply transition via `StatusManager`.
   - Disk/node absent from sync response → transition to `Missing`
     (§9, §10).
e. Write instance heartbeat to group 0 (`write_instance_heartbeat`
   with piggybacked usage summary).
f. Return `SyncOutcome { groups_added, groups_removed, disks_added,
   disks_removed, status_changes, sync_duration_ms }`.

### 4.3 Epoch/revision guard

Skip a sync response whose epoch ≤ current (prevents stale overwrites,
§10). The `FetchHardware` RPC is epoch-based; `SyncLoop` uses it for
the bulk read, then reconciles with the ownership/binding maps.

### 4.4 Degraded mode

- Track `missed_count` of consecutive sync failures. After
  `miss_threshold` (default 3), enter degraded mode
  (`NodeContainer.enter_degraded_mode()`).
- In degraded mode, allocate/free RPCs return `Unavailable`.
- On first successful sync, exit degraded mode
  (`exit_degraded_mode()`).

### 4.5 Notify mechanism (deferred to R78)

v1 uses fixed-interval polling at 10 s. Group 0 reads are cheap prefix
scans. A watch/notify mechanism (group 0 pushes notifications to
registered diskdb endpoints) is tracked as R78. Polling stays as a
safety net even after R78.

---

## 5. StatusManager

`app/crow-diskdb/src/status/mod.rs`:

### 5.1 Structure

- `StatusManager` — applies status transitions and computes effective
  status. Integrated with the sync loop (called on each `sync_once`).

### 5.2 Effective status

```rust
fn effective_status(
    node: HwStatus,
    group: HwStatus,
    disk: HwStatus,
) -> HwStatus
```

`max(node, group, disk)` — three-level check (§9). `HwStatus` is
`Ord` (ordered by severity: `Init < Up < Maintenance < Suspect <
Missing < Bad < Offline`).

### 5.3 Transition methods

- `apply_node_status(node_id: u64, new_status: HwStatus)` — validates
  transition legality (§9), writes updated `NodeValue` to group 0,
  updates in-memory state.
- `apply_disk_group_status(node_id: u64, dg_id: u32, new_status)` —
  same pattern, writes `DiskGroupValue`.
- `apply_disk_status(node_id: u64, dg_id: u32, disk_id: DiskId,
  new_status)` — same pattern, writes `DiskValue`.

### 5.4 Transition rules (§9)

- `Init` → `{Up, Offline, Maintenance}` on startup (load from group 0).
- `Up` → `Suspect` (3 missed syncs).
- `Up` → `Offline` / `Maintenance` (operator).
- `Suspect` → `Up` (sync recovers) or → `Missing` (cannot probe) or →
  `Offline`.
- `Missing` → `Bad` (confirmed) or → `Up` (rediscovered). **Missing is
  detected by absence from a group-0 sync response** — a disk/node
  absent from the sync response is transitioned to `Missing`.
- `Offline` ↔ `Maintenance` (operator).
- `Offline` → `Up` (operator).

### 5.5 Timeout check

- `check_suspect_timeouts()` — called on each sync tick; transitions
  any disk/disk-group/node that has been in `Suspect` longer than
  `temp_failure_timeout_secs` (default 900 s / 15 min) to `Offline`.

### 5.6 Allocation gating

- `allows_allocate(effective: HwStatus) -> bool` — `Up` only.
- `allows_free(effective: HwStatus) -> bool` — `Up`, `Maintenance`, or
  `Suspect`.

---

## 6. Disk-Add Initialization Flow

When `SyncLoop` detects a disk in group 0 that is not yet in the
in-memory state (§10):

a. Read the `DiskValue` from group 0 (capacity, zone size, unit size,
   disk type, status).
b. Create the in-memory `ZoneDisk` with one `Zone` per zone:
   - Zone count = `capacity_units / zone_size_units` (last zone may be
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
f. Increment `disk.add.count` counter (§11).

**Note**: R71 creates the zones and writes the baseline `ZoneValue`
records, but the zone **allocation** logic (CAS claim, bitmap-scan) is
R72. R71's `ZoneDisk` has the zone management methods (`add_zone`,
`rebuild_active_zones`) but the `allocate`/`free` methods are stubbed.

---

## 7. NodeContainer

`app/crow-diskdb/src/node/mod.rs`:

### 7.1 NodeContainer

- Per-instance singleton managing all owned disk-groups.
- `nodes: RwLock<HashMap<DiskGroupId, Arc<Node>>>` — owned disk-groups.
- `instance_id: u64`, `config: DiskdbConfig`.
- `degraded: AtomicBool`.
- `add_node(node: Arc<Node>)`, `remove_node(dg_id: DiskGroupId)`,
  `get_node(dg_id) -> Option<Arc<Node>>`, `node_ids() ->
  Vec<DiskGroupId>`.
- `enter_degraded_mode()` / `exit_degraded_mode()` / `is_degraded() ->
  bool` — atomic flag.

### 7.2 Node

- Disk-group manager.
- `disk_group_id: DiskGroupId`, `node_id: u64`.
- `bind: (u64, u64)` — `(store_id, group_id)` for the bound paxos data
  group (from the binding map).
- `disks: RwLock<HashMap<DiskId, Arc<ZoneDisk>>>` — all disks.
- `allocating_disks: RwLock<Arc<AllocateDiskContext>>` — RCU context
  of allocatable disks (R72; initialized by `refresh_disk_context()`
  on add/remove/status-change).
- `pos_v_disk_ctx: AtomicU64` — round-robin cursor (R72).
- `status: RwLock<HwStatus>` — disk-group status.

### 7.3 ZoneDisk

`app/crow-diskdb/src/node/disk.rs`:

- `disk_id: DiskId`, `disk_group_id: DiskGroupId`, `node_id: u64`.
- `disk_value: RwLock<DiskValue>` — capacity, zone size, unit size,
  status, disk type.
- `zones: RwLock<Vec<ZoneRef>>` — all zones.
- `active_zone_context: RwLock<Arc<ActiveZoneContext>>` — RCU active
  zone set (R72).
- `pos_v_zone_ctx: AtomicU64` — round-robin cursor over active set
  (R72).
- `pos_v_zone: AtomicU64` — rotating cursor for zone rotation scan
  (R72).
- Zone management methods: `add_zone(zone: ZoneRef)`,
  `rebuild_active_zones()` — build `ActiveZoneContext` with the first
  `zone_rotate_count` allocatable zones.
- Zone **allocation** methods (`disk_allocate`, `rotate_active_zones`)
  are stubbed in R71 — R72 fills them in.
- v1: single implementation for all disk types (BlockHdd, BlockSsd).
  SMR/SSD trait variants are stubbed (non-goal per design doc §2).

### 7.4 Disk-id → disk-group lookup

For the free path (R72), the `FreeBlocks` request carries `Segment`s
with `disk_id` but no `disk_group_id`. diskdb needs to look up the
disk-group from the disk-id. Two options:
- **(a) In-memory hash map** — `NodeContainer` maintains a
  `disk_id → (DiskGroupId, Arc<Node>)` map. Updated on disk-add and
  disk-remove. O(1) lookup.
- **(b) Group-0 reverse lookup** — scan group 0 for the `DiskKey`
  matching the `disk_id`. Too slow for the free hot path.

**Choose (a)** — the in-memory hash map is built during sync and
disk-add init. The free path looks up `disk_id` in the map to find the
`Node` and `ZoneDisk`, then calls `disk.free()`.

---

## 8. Admin gRPC Handlers

`app/crow-diskdb/src/grpc/admin.rs`:

R71 implements the `DiskdbService` handlers that are status-related.
The allocate/free handlers are stubs (`Unimplemented`) until R72.

- `get_disk_group_info(node_id, dg_id)` — read from in-memory
  `NodeContainer`; return `DiskGroupInfo` (key + value fields
  flattened + member disks).
- `get_disk_info(node_id, dg_id, disk_id)` — read from in-memory
  `ZoneDisk`; return `DiskInfo` (key + value fields + zone breakdown
  fields, 0 if not probed).
- `query_capacity_stats(dg_id)` — stub (returns empty); R74 fills it
  in.
- `allocate_blocks` / `free_blocks` — stub (`Unimplemented`); R72.
- `rebuild_zone_bitmap` — stub (`Unimplemented`); R73.
- `mark_block_suspect` / `mark_block_corrupt` — stub
  (`Unimplemented`); R75.

**Note**: `DiskdbAdminService` (AddRack, AddNode, AddDisk,
SetDiskStatus, FetchHardware, Keepalive) is **not** served by the
diskdb server — it targets group 0 and is served by a future
admin/console component (or the mock group-0 in tests). The diskdb
server only serves `DiskdbService`.

---

## 9. Server Wiring

`app/crow-diskdb/src/main.rs` (extends the existing skeleton):

1. Parse CLI (clap), load + validate config.
2. Create `CrowkvClient` (points at crow-kv cluster; mock group-0 in
   tests).
3. Create `SysdataClient` wrapping `CrowkvClient` (for group 0).
4. Create `NodeContainer` (shared state).
5. Create `StatusManager`.
6. Create `SyncLoop` — spawn as background task.
7. Run initial sync (blocking — server must not serve RPCs until
   first sync completes and disk-add init flows finish).
8. Create gRPC service (`DiskdbService`) wired with `NodeContainer`,
   config.
9. Start gRPC server (tonic) on `listen_addr`.
10. Start HTTP management server (axum) on `http_listen_addr` (minimal
    health + info endpoints; full console is R77).

The server starts, loads config, connects to crow-kv, runs an initial
sync from group 0, starts the sync loop, and serves gRPC. Allocate/free
RPCs return `Unimplemented` until R72.

---

## 10. Config

`app/crow-diskdb/src/config.rs` — the existing config is mostly
correct. Verify/fix:

- `HeartbeatConfig.interval_secs` — 10 (was 13; fixed to match design
  doc §10/§16).
- `SyncConfig.sync_interval_secs` — 10 (was 13; fixed).
- `HeartbeatConfig.miss_threshold` — 3 (correct).
- `HeartbeatConfig.temp_failure_timeout_secs` — 900 (correct, 15 min).
- `SyncConfig.group0_store_id` / `group0_group_id` — 0 / 0 (correct).

No new config fields needed for R71. R72 adds `cas_retry_limit` and
`validate_owner_on_free`; R73 adds `CompactionConfig`.

---

## 11. Module Structure

```
app/crow-diskdb/src/
├── sysdata/
│   └── mod.rs          — SysdataClient (group-0 read/write)
├── sync/
│   └── mod.rs          — SyncLoop (periodic sync, change detection,
│                         degraded mode)
├── status/
│   └── mod.rs          — StatusManager (transitions, effective status,
│                         timeout check)
├── node/
│   ├── mod.rs          — NodeContainer, Node
│   └── disk.rs         — ZoneDisk (zone list, active zone set stubs)
├── zone/
│   └── mod.rs          — Zone struct (bitmap stubs — R72 fills in
│                         allocate/free)
├── grpc/
│   ├── mod.rs          — gRPC service struct
│   └── service.rs      — DiskdbService handlers (info/query; allocate/
│                         free stubs)
├── config.rs           — (exists, verified)
├── lib.rs              — module declarations and re-exports
└── main.rs             — (exists, wire up)
```

---

## 12. Test Strategy

### 12.1 Unit tests (no external deps)

- `SysdataClient` read/write round-trip — write a `NodeValue`, read it
  back; write a `DiskValue`, read it back; etc. Uses a mock
  `CrowkvClient` (in-memory HashMap).
- `StatusManager::effective_status()` — `max(node, group, disk)` for
  all combinations.
- `StatusManager` transition rules — all legal transitions succeed,
  illegal ones rejected. Cover each rule in §5.4.
- `StatusManager::check_suspect_timeouts()` — a disk in `Suspect` for
  > 900 s transitions to `Offline`.
- `StatusManager::allows_allocate()` / `allows_free()` — correct
  gating for each `HwStatus` value.
- `NodeContainer` — `add_node`/`remove_node`/`get_node` with `RwLock`
  concurrency; `enter_degraded_mode`/`exit_degraded_mode` via atomic
  flag.
- Config validation — existing tests (verify 10 s defaults).

### 12.2 Integration tests (in-process mock group-0)

Using the `diskdb_test_harness` (§2.3):

- **Sync** — seed mock group-0 with topology (one node, one
  disk-group, 2–4 disks), start `SyncLoop`, verify `NodeContainer` has
  the correct disk-groups/disks/zones.
- **Disk-add init** — add a disk to mock group-0 (via `Put` to
  `DiskKey`), trigger sync, verify `ZoneValue` baselines written to
  the data group, zone count correct, `ActiveZoneContext` built.
- **Status transition** — `SetDiskStatus` on mock group-0 (via `Put`
  to `DiskKey` with updated `DiskValue.status`), trigger sync, verify
  in-memory status updated, `allocatable` reflects it.
- **Missing detection** — remove a disk from mock group-0, trigger
  sync, verify disk transitions to `Missing`.
- **Degraded mode** — stop mock group-0, wait 3 sync cycles, verify
  degraded mode; restart mock, verify recovery.
- **Heartbeat** — verify `InstanceValue` written to group 0 on each
  sync tick with correct `last_heartbeat_ms` and `owned_dg_ids`.
- **gRPC info handlers** — `GetDiskGroupInfo` / `GetDiskInfo` return
  correct data from in-memory state.
- **Server startup** — server starts, runs initial sync, serves gRPC
  (info RPCs functional; allocate/free return `Unimplemented`).

### 12.3 Verification commands

- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
- Relevant tests pass (`pixi run cargo test -p crow-diskdb`).
