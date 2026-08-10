<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design — diskdb Protocol + Core Types + Config Validation

Working design draft for R70. Folds into
`doc/design/diskdb/design-crow-diskdb.md` (§5, §7, §13) after merge.

## Hierarchy + Core Concepts

diskdb manages physical disk space as a hierarchy. The largest unit
diskdb deals with is the **disk-group** — Node exists in group 0 as
metadata but is not exposed as a protocol concept in `DiskdbService`.

```
DiskGroup (node_id, dg_id)              unit of ownership + paxos binding
 └─ Disk (uuid_h, uuid_l)               physical device
     └─ Zone (index, variable size)     append-only allocation region
         └─ DiskBlock (1 MB aligned)    smallest allocatable unit
```

**Identity model:**
- `NodeId` — `u64`, integer node identifier (assigned by the cluster).
- `DiskGroupId` — `u32`, integer disk-group identifier, **unique within
  a node**. A disk-group is globally identified by the pair
  `(node_id, dg_id)`. The node tracks a `last_used_dg_id` counter; new
  disk-groups increment it and use the next value.
- `DiskUuid` — two `u64` values (`high`, `low`), representing a 128-bit
  disk UUID. Globally unique.
- `instance_id` — string (auto-generated UUID), identifies a diskdb
  process. Not tied to the node/disk hierarchy.

**Layers:**
- **Node** — a physical machine. Exists in group 0 (NodeMeta) with a
  `Status` and `last_used_dg_id`. Not exposed as a protocol message in
  `DiskdbService`; diskdb reads node metadata internally to compute
  effective status. `SetNodeStatus` is an admin RPC (group-0 write).
- **DiskGroup** — logical container of disks on one node. The **unit of
  ownership** (assigned to one diskdb instance via the ownership map in
  group 0) and the **unit of paxos binding** (all zone journals for a
  disk-group live on one paxos data group, per the binding map in group
  0). Has a `Status`. Identified by `(node_id, dg_id)`.
- **Disk** — a physical device. Has a `Status` (operator-set, synced
  from group 0) and a `DiskState` (health, probed locally — not stored
  in group 0). Has a `DiskType` (BlockHdd/BlockSsd/ZoneSsd/SmrHdd).
  Disk metadata lives in group 0.
- **Zone** — a logical append-only region on a disk. Has a `ZoneState`
  (health: Healthy/Missing/Bad) and a `ZoneAllocationState` (lifecycle:
  Active/Busy/Error/Full). Zones may vary in size (the last zone on a
  disk is usually smaller). Each zone records its absolute `disk_offset`
  and `zone_size_bytes`. Zone state lives on the disk-group's bound data
  group (journal + snapshot), not in group 0.
- **DiskBlock** — the smallest allocatable unit (default 1 MB, aligned).
  Has no status; it is either allocated (tracked by `Segment` + bitmap
  bit set) or free (bitmap bit clear).

**Status vs. DiskState vs. ZoneState:**
- `Status` — operator-facing operational status, **shared by Node,
  DiskGroup, and Disk**. Ordered by restrictiveness (Online < Init <
  Maintenance < TempFailure < Offline). The effective status for a disk
  = `max(node.status, group.status, disk.status)`. Allocations require
  effective `Online`; frees allow `Online`, `Maintenance`, or
  `TempFailure`.
- `DiskState` — disk health probed locally (Init/Active/Suspect/Missing/
  Bad). Distinct from `Status` (which is operator-set via group 0). Not
  stored in group 0; each diskdb instance probes its own disks.
- `ZoneState` — zone health probed locally (Healthy/Missing/Bad).
- `ZoneAllocationState` — zone allocation lifecycle (Active/Busy/Error/
  Full), driven by in-memory CAS within the diskdb instance.

## Usage Scenarios

The protocol serves five scenarios:

1. **Allocate** — a caller (future object store, chunk service) asks
   diskdb to allocate block(s) in a disk-group. The caller may exclude
   specific disks (replica-aware: "I already have data on disk X, don't
   put another replica there"). diskdb returns `Segment`(s) — opaque
   handles the caller uses to write data later. `AllocateBlocks` handles
   both single-block (`count=1`) and multi-block; no separate single-
   block RPC (one code path, simpler surface). `exclude_disk_uuids` is
   always available — even for `count=1`, the caller may be placing
   replica N and need to exclude disks holding replicas 1..N-1.

2. **Free** — a caller returns block(s) to diskdb when the data is no
   longer needed. `FreeBlocks` handles both single and batch (free is
   not hot-path; batched internally via FreeBatch anyway). No separate
   single-free RPC.

3. **Query capacity** — an operator or the console queries capacity,
   busy/free, and zone-level breakdown across owned disk-groups
   (`QueryCapacityStats`). Returns `repeated DiskGroupInfo` (diskdb owns
   disk-groups, not nodes). Optionally includes per-zone detail
   (`include_zones`) for the block-array visualization.

4. **Get info** — read-only queries (`GetDiskGroupInfo`, `GetDiskInfo`)
   let operators inspect a specific diskdb instance's synced view of
   what it owns. No `GetNodeInfo` — diskdb doesn't own nodes.

5. **Sync / keep-alive (diskdb → group 0)** — diskdb periodically syncs
   with group 0 (default 13s). On each sync:
   - **Read**: diskdb reads its owned disk-groups from the ownership map
     (`/diskdb/map/owner/{node_id}-{dg_id}`), then for each owned
     disk-group reads DiskGroupMeta, DiskMeta for each member disk,
     NodeMeta for the parent node (for effective status), and the
     binding map (`/diskdb/map/bind/{node_id}-{dg_id}`) to know which
     paxos data group holds the zone journals.
   - **Write (keep-alive)**: diskdb writes its InstanceMeta
     (`/diskdb/instance/{instance_id}`) with updated
     `last_heartbeat_ms` and endpoint info. Group 0 uses this to learn
     the diskdb instance is alive and can balance disk-groups across
     instances.
   - **Write (local changes)**: if diskdb detected a local disk status
     change (disk found, disk bad), it writes the updated DiskMeta to
     group 0 first, then reflects locally.

   This is **R71's implementation** (sync loop, batch disk-group sync,
   heartbeat-as-keep-alive). R70 defines the data types (InstanceMeta,
   DiskGroupMeta, DiskMeta, NodeMeta) and sysdata keys the sync uses.

6. **Admin (group 0)** — an operator adds/removes disk-groups and disks,
   and sets the status of nodes/disk-groups/disks. These are **group-0
   writes**, not diskdb server operations. The diskdb server syncs them.
   Served by a future admin/console component, not the diskdb server.

## Protocol — `diskdb.proto`

```proto
// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

syntax = "proto3";
package crow.diskdb.rpc;

// ── Id types ────────────────────────────────────────────────────

// 128-bit disk UUID, split into two uint64 for proto efficiency.
message DiskUuid {
  uint64 high = 1;
  uint64 low  = 2;
}

// ── Enums ───────────────────────────────────────────────────────

// Physical disk technology. Determines zone implementation.
enum DiskType {
  BLOCK_HDD = 0;
  BLOCK_SSD = 1;
  ZONE_SSD  = 2;
  SMR_HDD   = 3;
}

// Operational status shared by Node, DiskGroup, and Disk.
// Ordered by restrictiveness; OFFLINE is most restrictive.
// Effective status of a disk = max(node, group, disk).
enum Status {
  ONLINE        = 0;
  INIT          = 1;
  MAINTENANCE   = 2;
  TEMP_FAILURE  = 3;
  OFFLINE       = 4;
}

// Zone hardware health (probed locally).
enum ZoneState {
  HEALTHY = 0;
  MISSING = 1;
  BAD     = 2;
}

// Zone allocation lifecycle (in-memory CAS within one diskdb instance).
enum ZoneAllocationState {
  ACTIVE = 0;
  BUSY   = 1;
  ERROR  = 2;
  FULL   = 3;
}

// Disk health state (probed locally; distinct from operator-set Status).
enum DiskState {
  INIT    = 0;
  ACTIVE  = 1;
  SUSPECT = 2;
  MISSING = 3;
  BAD     = 4;
}

// ── Hierarchy data structures ───────────────────────────────────
// Nesting: DiskGroup → Disk → Zone.
// (Node exists in group 0 but is not a diskdb protocol level.)
// Returned by Get*Info and QueryCapacityStats.

message ZoneInfo {
  uint32 zone_index          = 1;
  uint64 disk_offset         = 2;  // absolute byte offset on disk
  uint64 zone_size_bytes     = 3;  // capacity (last zone may be smaller)
  ZoneState zone_state       = 4;
  ZoneAllocationState alloc_state = 5;
  uint64 used_bytes          = 6;
  uint64 free_bytes          = 7;
  uint32 block_count         = 8;
  uint32 busy_block_count    = 9;
}

message DiskInfo {
  DiskUuid disk_uuid       = 1;
  uint64 node_id           = 2;
  DiskType disk_type       = 3;
  uint64 capacity_bytes    = 4;
  uint64 zone_size_bytes   = 5;
  uint32 block_size_bytes  = 6;
  uint32 zone_count        = 7;
  Status status            = 8;
  DiskState disk_state     = 9;
  repeated ZoneInfo zones  = 10;
}

message DiskGroupInfo {
  uint64 node_id           = 1;
  uint32 disk_group_id     = 2;  // unique within node
  Status status            = 3;
  repeated DiskInfo disks  = 4;
}

// ── Segment (allocation handle) ─────────────────────────────────

message Segment {
  uint64 node_id        = 1;
  uint32 disk_group_id  = 2;
  DiskUuid disk_uuid    = 3;
  uint32 zone_index     = 4;
  uint64 zone_offset    = 5;  // byte offset within zone
  uint32 size           = 6;  // bytes, aligned to block granularity
  uint64 tag            = 7;  // nanosecond timestamp for tracking
}

// ── Allocate ────────────────────────────────────────────────────

message AllocateBlocksRequest {
  uint64 node_id                   = 1;
  uint32 disk_group_id             = 2;
  uint32 size                      = 3;  // bytes, aligned to block granularity
  uint32 count                     = 4;
  repeated DiskUuid exclude_disk_uuids = 5;  // negative disk hints
}

message AllocateResponse {
  bool   ok              = 1;
  string error           = 2;
  repeated Segment segments = 3;
}

// ── Free ────────────────────────────────────────────────────────

message FreeBlocksRequest {
  repeated Segment segments = 1;
}

message FreeResponse {
  bool   ok           = 1;
  string error        = 2;
  uint32 freed_count  = 3;  // how many succeeded
}

// ── Query capacity stats ────────────────────────────────────────

message QueryCapacityStatsRequest {
  // If node_id + disk_group_id set = specific group; empty = all owned.
  uint64 node_id       = 1;
  uint32 disk_group_id = 2;
  // If true, include per-zone breakdown in DiskInfo.zones.
  bool include_zones   = 3;
}

message QueryCapacityStatsResponse {
  bool   ok    = 1;
  string error = 2;
  repeated DiskGroupInfo disk_groups = 3;
}

// ── Get info (read-only, from diskdb synced cache) ──────────────

message GetDiskGroupInfoRequest {
  uint64 node_id       = 1;
  uint32 disk_group_id = 2;
}

message GetDiskGroupInfoResponse {
  bool         ok    = 1;
  string       error = 2;
  DiskGroupInfo group = 3;
}

message GetDiskInfoRequest {
  DiskUuid disk_uuid = 1;
}

message GetDiskInfoResponse {
  bool     ok    = 1;
  string   error = 2;
  DiskInfo disk  = 3;
}

// ── Admin (targets group 0; NOT served by the diskdb server) ────

message AddDiskGroupRequest {
  uint64 node_id = 1;
  // dg_id auto-assigned by incrementing node's last_used_dg_id.
}

message AddDiskGroupResponse {
  bool   ok            = 1;
  string error         = 2;
  uint32 disk_group_id = 3;  // assigned dg_id
}

message AddDiskRequest {
  uint64 node_id        = 1;
  uint32 disk_group_id  = 2;  // which disk-group this disk joins
  DiskUuid disk_uuid    = 3;
  DiskType disk_type    = 4;
  uint64 capacity_bytes = 5;
  uint64 zone_size_bytes  = 6;
  uint32 block_size_bytes = 7;
}

message RemoveDiskRequest {
  uint64 node_id    = 1;
  DiskUuid disk_uuid = 2;
}

message SetDiskStatusRequest {
  uint64 node_id    = 1;
  DiskUuid disk_uuid = 2;
  Status status     = 3;
}

message SetDiskGroupStatusRequest {
  uint64 node_id       = 1;
  uint32 disk_group_id = 2;
  Status status        = 3;
}

message SetNodeStatusRequest {
  uint64 node_id  = 1;
  Status status   = 2;
}

message AdminResponse {
  bool   ok    = 1;
  string error = 2;
}

// ── Services ────────────────────────────────────────────────────

// Served by the diskdb server.
service DiskdbService {
  rpc AllocateBlocks(AllocateBlocksRequest)     returns (AllocateResponse);
  rpc FreeBlocks(FreeBlocksRequest)             returns (FreeResponse);
  rpc QueryCapacityStats(QueryCapacityStatsRequest) returns (QueryCapacityStatsResponse);
  rpc GetDiskGroupInfo(GetDiskGroupInfoRequest) returns (GetDiskGroupInfoResponse);
  rpc GetDiskInfo(GetDiskInfoRequest)           returns (GetDiskInfoResponse);
}

// Targets group 0; served by a future admin/console component.
service DiskdbAdminService {
  rpc AddDiskGroup(AddDiskGroupRequest)         returns (AddDiskGroupResponse);
  rpc AddDisk(AddDiskRequest)                   returns (AdminResponse);
  rpc RemoveDisk(RemoveDiskRequest)             returns (AdminResponse);
  rpc SetDiskStatus(SetDiskStatusRequest)       returns (AdminResponse);
  rpc SetDiskGroupStatus(SetDiskGroupStatusRequest) returns (AdminResponse);
  rpc SetNodeStatus(SetNodeStatusRequest)       returns (AdminResponse);
}
```

**Key design points:**
- `AllocateBlocks` is the single allocate RPC — `count=1` is the
  single-block case. No separate `AllocateBlock` (one code path,
  simpler surface). `exclude_disk_uuids` always available.
- `FreeBlocks` is the single free RPC — `count=1` is the single-free
  case. No separate `FreeBlock` (free is not hot-path; batched
  internally via FreeBatch).
- `QueryCapacityStats` returns `repeated DiskGroupInfo` (diskdb owns
  disk-groups, not nodes). No `NodeInfo` message — Node is not a
  diskdb protocol level.
- `GetDiskGroupInfo` replaces `GetNodeInfo`. `GetDiskInfo` stays.
- Integer ids throughout: `node_id` (uint64), `disk_group_id` (uint32,
  unique within node), `DiskUuid` (two uint64). No string uuids in the
  protocol.
- `AddDiskGroup` auto-assigns `dg_id` by incrementing the node's
  `last_used_dg_id`. `AddDisk` requires `disk_group_id` — a disk joins
  a specific disk-group when added.
- `DiskdbAdminService` is a separate service targeting group 0. The
  diskdb server never implements it.

## Core Types (Rust) — `app/crow-diskdb/src/types/`

The Rust types mirror the proto hierarchy and add the journal/snapshot
types that are internal to diskdb (not exposed via gRPC).

### Module layout

- `types/mod.rs` — module declarations + `pub use` re-exports.
- `types/ids.rs` — `NodeId`, `DiskGroupId`, `DiskUuid`, `Segment`,
  `ClaimSnapshot`.
- `types/status.rs` — `Status` + `effective_status()` +
  `allows_allocate()` / `allows_free()`.
- `types/zone_state.rs` — `ZoneState`, `ZoneAllocationState`
  (`#[repr(u8)]`, `from_u8()`).
- `types/disk_state.rs` — `DiskState`, `DiskType`.
- `types/journal.rs` — `BusyRecord`, `FreeRecord`, `ZoneRecord`,
  key-layout helpers, CRC helpers.
- `types/disk.rs` — `DiskMeta`.
- `types/disk_group.rs` — `DiskGroupMeta`.
- `types/node.rs` — `NodeMeta`.
- `types/instance.rs` — `InstanceMeta`.

### Identity types

`NodeId` — type alias for `u64`. Integer node identifier.

`DiskGroupId` — type alias for `u32`. Integer disk-group identifier,
unique within a node. A disk-group is globally identified by the pair
`(NodeId, DiskGroupId)`.

`DiskUuid` — struct `{ high: u64, low: u64 }` representing a 128-bit
disk UUID. Derives `Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize,
Deserialize`. Implements `Display` (formats as
`"{high:016x}-{low:016x}"` for human-readable output) and a
`to_key_component()` method (formats as `"{high:016x}{low:016x}"` —
32 hex chars, no dash, for compact KV key paths).

### Allocation types

`Segment` — `{ node_id: NodeId, disk_group_id: DiskGroupId, disk_uuid:
DiskUuid, zone_index: u32, zone_offset: u64, size: u32, tag: u64 }`.
Derives `Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize`.
`tag` is a nanosecond timestamp for debugging/tracking.

`ClaimSnapshot` — `{ prev_pos: u32, count: u32 }`. `Clone, Copy, Debug`.
Used by R72 for rollback; defined here as a shared type.

### Journal types (internal, not in proto)

`ZoneRecord` — the **compacted snapshot** of a zone, written
periodically by R73 (not on every allocate). Stored directly at the
snapshot key (no wrapper):
```
ZoneRecord {
    disk_uuid: DiskUuid,
    zone_index: u32,
    disk_offset: u64,        // absolute byte offset of this zone on the disk
    zone_size_bytes: u64,    // capacity (last zone may be smaller)
    allocate_pos: u32,       // current position in block units
    usage_bitmap: Vec<u8>,   // compacted bitmap bytes
    zone_state: ZoneState,
    snapshot_slot: u64,      // max journal slot included in this compaction
    checksum: u32,           // CRC32 over the record with this field zeroed
}
```
`disk_offset` + `zone_size_bytes` support variable zone sizes. The
allocation ceiling is derived as `zone_size_bytes / block_size_bytes`.
Derives `Clone, Debug, Serialize, Deserialize`; bincode for compact
storage.

`BusyRecord` — `{ zone_offset: u64, size: u32, tag: u64 }`. Appended on
each allocate. ≤ 32 bytes bincode-serialized. `Clone, Copy, Debug,
Serialize, Deserialize`, bincode.

`FreeRecord` — `{ zone_offset: u64, size: u32, tag: u64 }`. Same shape
as `BusyRecord`. Appended on each free.

No `ZoneSnapshot` wrapper — `ZoneRecord` is stored directly at the
snapshot key. `ZoneRecord.snapshot_slot` is the single source of truth
for the max journal slot included in the compaction. (The previous
wrapper was redundant — `snapshot_slot` was in both the wrapper and the
record.)

### Status + state enums

`Status` — shared by Node, DiskGroup, Disk. `Online=0, Init=1,
Maintenance=2, TempFailure=3, Offline=4` (ordered by restrictiveness).
`effective_status(node, group, disk) -> Status` = `max(node, group,
disk)`. `allows_allocate()` (Online only), `allows_free()` (Online,
Maintenance, TempFailure).

`ZoneState` — `Healthy, Missing, Bad`.

`ZoneAllocationState` — `Active, Busy, Error, Full`, `#[repr(u8)]`,
`from_u8()` (unknown → `Error`).

`DiskState` — `Init, Active, Suspect, Missing, Bad`.

`DiskType` — `BlockHdd, BlockSsd, ZoneSsd, SmrHdd` (replaces the
skeleton's 3-variant `Hdd/Ssd/Smr`).

### Group-0 sysdata meta types

`NodeMeta` — `{ node_id: NodeId, dc_id: Option<String>,
rack_id: Option<String>, status: Status, last_used_dg_id: u32,
disk_group_ids: Vec<DiskGroupId>, status_changed_at_ms: u64,
temp_failure_since_ms: Option<u64> }`. `dc_id`/`rack_id` reserved (v1
flat). `last_used_dg_id` is the auto-increment counter for new
disk-group ids within this node. Matches design doc §5.

`DiskGroupMeta` — `{ node_id: NodeId, dg_id: DiskGroupId, status:
Status, disk_uuids: Vec<DiskUuid> }`. The disk-group's global identity
is `(node_id, dg_id)`. `disk_uuids` is the source of truth for
membership (DiskMeta does not carry `disk_group_id` — no duplication).

`DiskMeta` — `{ disk_uuid: DiskUuid, node_id: NodeId, disk_type:
DiskType, capacity_bytes: u64, zone_size_bytes: u64, block_size_bytes:
u32, zone_count: u32, status: Status }`. No `disk_state` — DiskState is
purely local (probed, never stored in group 0). No `disk_group_id` —
membership is tracked in `DiskGroupMeta.disk_uuids`.

`InstanceMeta` — `{ instance_id: String, grpc_endpoint: String,
http_endpoint: String, owned_dg_ids: Vec<(NodeId, DiskGroupId)>,
last_heartbeat_ms: u64 }`. Written by diskdb on each sync as keep-alive.
Group 0 uses this to learn diskdb liveness and balance disk-groups.

All meta types derive `Clone, Debug, Serialize, Deserialize` (serde
JSON for group-0 storage).

## Key Layout — `types/journal.rs`

Key-layout helpers take integer params and format to string at the KV
boundary.

### Journal keys (data group, prefix-scan replay by `slot`)

- `journal_key_busy(node_id, dg_id, disk_uuid, zone_idx, slot)` →
  `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/busy/{slot}`
- `journal_key_free(node_id, dg_id, disk_uuid, zone_idx, slot)` →
  `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/free/{slot}`
- `journal_key_snapshot(node_id, dg_id, disk_uuid, zone_idx)` →
  `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/snapshot`
- `journal_prefix_zone(node_id, dg_id, disk_uuid, zone_idx)` →
  `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/`
- `journal_prefix_disk(node_id, dg_id, disk_uuid)` →
  `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/`
- `journal_prefix_dg(node_id, dg_id)` →
  `/diskdb/journal/{node_id}-{dg_id}/`

`{disk_uuid}` in keys = `disk_uuid.to_key_component()` (32 hex chars,
no dash). `slot` is a `u64`, zero-padded to 20 digits (`{:020}`) so
lexicographic prefix-scan returns records in ascending slot order. It
is a diskdb-assigned per-zone counter — **not** the crow-kv paxos slot.
Resolves the D4 open question in favor of prefix-scan replay (no
crow-kv slot feedback needed).

### Group-0 sysdata keys (matching design doc §5)

- `sysdata_key_node(node_id)` →
  `/diskdb/node/{node_id}/meta`
- `sysdata_key_disk_group(node_id, dg_id)` →
  `/diskdb/node/{node_id}/dg/{dg_id}/meta`
- `sysdata_key_disk(node_id, disk_uuid)` →
  `/diskdb/node/{node_id}/disk/{disk_uuid}/meta`
- `sysdata_key_owner(node_id, dg_id)` →
  `/diskdb/map/owner/{node_id}-{dg_id}`
- `sysdata_key_bind(node_id, dg_id)` →
  `/diskdb/map/bind/{node_id}-{dg_id}`
- `sysdata_key_instance(instance_id)` →
  `/diskdb/instance/{instance_id}`

## Config + Validation — `app/crow-diskdb/src/config/`

`config/` module directory: `config/mod.rs` (structs + re-exports),
`config/validation.rs` (`validate()`).

`DiskdbConfig` top-level: `{ server, storage, heartbeat, persistence,
scanner, sync }`.

- `ServerConfig` — `listen_addr` (default `0.0.0.0:9941`),
  `http_listen_addr` (default `0.0.0.0:9942`), `instance_id:
  Option<String>` (auto-generated UUID if absent).
- `StorageDefaults` — `zone_size_bytes` (default 16 GB),
  `block_size_bytes` (default 1 MB, configurable 512 KB–2 MB),
  `allocate_granularity` (default 1 MB, must be power of 2). Both
  `block_size_bytes` and `allocate_granularity` are kept — v1 enforces
  equality (validate checks `allocate_granularity == block_size_bytes`);
  the separate field is forward-compatible when granularity diverges
  from block size in a future version.
- `HeartbeatConfig` — `interval_secs` (13), `miss_threshold` (3),
  `temp_failure_timeout_secs` (900).
- `SyncConfig` — `group0_store_id` (u64, default 0), `group0_group_id`
  (u64, default 0), `sync_interval_secs` (13).
- `PersistenceConfig` — `free_flush_interval_ms` (500),
  `free_flush_max_batch` (256), `snapshot_interval_secs` (300),
  `snapshot_journal_threshold` (4096).
- `ScannerConfig` — `scan_interval_secs` (600),
  `detect_ghost_allocations` (true), `verify_record_integrity` (true).

The skeleton's `KvConfig { mgmt_seeds }` is dropped — topology
discovery is R71's concern (the sync layer discovers the cluster via
the management API). R71 will define its own discovery config fields
based on the sync design, not inherit a placeholder.

`validate(config) -> Result<(), String>` checks:
- `block_size_bytes` is a power of 2 and in `[512 KB, 2 MB]`.
- `zone_size_bytes` > 0 and a multiple of `block_size_bytes`.
- `allocate_granularity` == `block_size_bytes` (v1: granularity = block
  size).
- `free_flush_max_batch` > 0.
- `snapshot_interval_secs` > 0.
- `listen_addr` parses as `SocketAddr`.
- `http_listen_addr` parses as `SocketAddr`.
- `sync_interval_secs` > 0; `heartbeat.interval_secs` > 0;
  `heartbeat.miss_threshold` > 0.

## Bitmap Utilities — `app/crow-diskdb/src/zone/bitmap.rs`

`zone/` module directory: `zone/mod.rs`, `zone/bitmap.rs`.

`UsageBitmap` wraps `Vec<AtomicU64>` for lock-free bit operations:
- `new(block_count: u32) -> Self`
- `range_set(offset: u32, count: u32) -> bool` — atomic `fetch_or` per
  bit, rollback on collision (double-set → `false`).
- `range_clear(offset: u32, count: u32) -> bool` — atomic `fetch_and`
  per bit, rollback on collision (double-clear → `false`).
- `snapshot() -> Vec<u8>` — load all words (Acquire), little-endian
  bytes.
- `restore(bytes: &[u8]) -> Self` — little-endian bytes → `AtomicU64`.
- `count_set() -> u64` — popcount over all words.
- `block_count() -> u32`, `word_count() -> usize`.

`AtomicU64` words, `Ordering::AcqRel` for `fetch_or`/`fetch_and`,
`Ordering::Acquire` for loads. Per-bit atomic ops for v1 (correctness-
first; word-level CAS for aligned ranges is a future optimization if
profiled as hot). Pattern follows the aioss reference, wrapped in a
struct for ownership.

## CRC Integrity — `types/journal.rs`

`ZoneRecord` gets:
- `compute_checksum(&mut self)` — zero the `checksum` field, bincode-
  serialize, CRC32 (`crc32fast::hash`), store.
- `verify_checksum(&self) -> bool` — clone, zero checksum, bincode-
  serialize, compare CRC32.

No `ZoneSnapshot` wrapper — CRC lives only on `ZoneRecord`. Used by R73
(snapshot write) and R75 (integrity scanner).

## Files

- `lib/protocol/src/proto/diskdb.proto` — full rewrite (proto above).
- `lib/protocol/src/lib.rs` — no change (auto-generated module covers
  new types).
- `app/crow-diskdb/src/lib.rs` — add `pub mod zone;`, re-export
  `types::*` and `config::validate`.
- `app/crow-diskdb/src/types.rs` → `types/` — `mod.rs`, `ids.rs`,
  `status.rs`, `zone_state.rs`, `disk_state.rs`, `journal.rs`,
  `disk.rs`, `disk_group.rs`, `node.rs`, `instance.rs`.
- `app/crow-diskdb/src/config.rs` → `config/` — `mod.rs`,
  `validation.rs`.
- `app/crow-diskdb/src/zone/` — `mod.rs`, `bitmap.rs`.
- `app/crow-diskdb/src/main.rs` — rename CLI flags (`--grpc_addr` →
  `--listen-addr`, `--http_addr` → `--http-addr`), update field names.
- `app/crow-diskdb/Cargo.toml` — add `crc32fast`, `bincode`. (`uuid`
  only needed if `instance_id` auto-generation uses it — keep for
  `instance_id` default. `serde`, `serde_json` already present.)
- `app/crow-diskdb/tests/types.rs` — integration test: serde
  round-trips, key-format strings, CRC compute/verify, bitmap
  double-set/double-clear/snapshot-restore, config validate
  accept/reject.

## Acceptance Criteria

- `diskdb.proto` defines `DiskdbService` with `AllocateBlocks`,
  `FreeBlocks`, `QueryCapacityStats`, `GetDiskGroupInfo`,
  `GetDiskInfo`; and `DiskdbAdminService` with `AddDiskGroup`,
  `AddDisk`, `RemoveDisk`, `SetDiskStatus`, `SetDiskGroupStatus`,
  `SetNodeStatus`. All five enums present. `DiskUuid` message present.
  Hierarchy messages (`DiskGroupInfo`/`DiskInfo`/`ZoneInfo`) present.
  No `NodeInfo`, no `AllocateBlock`, no `FreeBlock`, no `ActiveZone`.
- Core types (`NodeId`, `DiskGroupId`, `DiskUuid`, `Segment`,
  `ClaimSnapshot`, `ZoneRecord`, `BusyRecord`, `FreeRecord`, `Status`,
  `ZoneState`, `ZoneAllocationState`, `DiskState`, `DiskType`,
  `NodeMeta`, `DiskGroupMeta`, `DiskMeta`, `InstanceMeta`) defined in
  `app/crow-diskdb/src/types/` with serde + bincode serialization.
- `ZoneRecord` carries `disk_offset` + `zone_size_bytes` (no
  `max_allocate_pos`). No `ZoneSnapshot` wrapper.
- `DiskMeta` has no `disk_state` (local-only) and no `disk_group_id`
  (membership in `DiskGroupMeta`).
- `NodeMeta` has `last_used_dg_id` for disk-group id auto-increment.
- Journal + sysdata key-layout helpers produce the exact key formats
  above; `slot` is zero-padded to 20 digits; integer ids formatted to
  string at the KV boundary.
- `DiskdbConfig` with all sub-configs defined with production defaults;
  `validate()` accepts a valid default config and rejects: non-power-of-
  2 block size, block size out of `[512 KB, 2 MB]`, zone not a multiple
  of block, granularity != block size, unparseable listen addr.
- `UsageBitmap` passes: range_set/range_clear, double-set detection,
  double-clear detection, snapshot/restore round-trip, cross-word-
  boundary, count_set.
- `ZoneRecord` CRC32 compute/verify round-trip; tampering flips
  `verify_checksum` to `false`.
- `pixi run cargo fmt --all -- --check` clean.
- `pixi run cargo clippy --all-targets -- -D warnings` clean.
- New tests in `app/crow-diskdb` pass.

## Notes for Follow-up Requirements

- **R71** owns the sync mechanism: batch disk-group sync, heartbeat-as-
  keep-alive (group 0 learns diskdb liveness → balance disk-groups),
  ownership/binding map read/write, instance heartbeat registration,
  topology discovery (seed endpoints). R70 only defines the meta types
  and sysdata key layout R71 uses. R71 will define its own discovery
  config fields (not inheriting a placeholder from R70).
- **R72** owns the zone allocator: CAS claim, active deque, two-phase
  async allocation, free batch. R70 defines `ZoneAllocationState`,
  `ClaimSnapshot`, `UsageBitmap`, `BusyRecord`/`FreeRecord` as shared
  types.
- **R73** owns crash recovery + snapshot compaction: uses `ZoneRecord`,
  journal key layout, CRC helpers from R70.
- **R74** owns space metrics + `QueryCapacityStats`: uses the hierarchy
  messages from R70.
- The **diskgroup↔diskdb-instance management layer** (serving
  `DiskdbAdminService`, ownership/binding write policies, instance
  registry write path) is a separate follow-up; R70 only defines its
  proto surface and sysdata types/keys.
