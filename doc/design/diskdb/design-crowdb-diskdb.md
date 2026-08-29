<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: diskdb (Overview)

This is the root design document for the diskdb component area. It
defines **what diskdb is**, **why key choices were made**, and **how the
component is structured**. Field-level details live in the schema files
and Rust source; this doc covers decisions and architecture only.

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Non-Goals (Design Envelope)](#2-non-goals-design-envelope)
- [3. Key Design Decisions](#3-key-design-decisions)
  - [3.1 Group 0 is the centralized sysdata store](#31-group-0-is-the-centralized-sysdata-store)
  - [3.2 disk-group → paxos group binding via a table (not hash)](#32-disk-group--paxos-group-binding-via-a-table-not-hash)
  - [3.3 No CAS needed; exclusive ownership](#33-no-cas-needed-exclusive-ownership)
  - [3.4 Records are the source of truth; bitmap is derived](#34-records-are-the-source-of-truth-bitmap-is-derived)
  - [3.5 Zone is a logical concept; sizes may vary](#35-zone-is-a-logical-concept-sizes-may-vary)
  - [3.6 Common protocol crate; crowdb-rpc transport](#36-common-protocol-crate-crowdb-rpc-transport)
  - [3.7 Reuse crowdb-common metrics](#37-reuse-crowdb-common-metrics)
  - [3.8 Schema types used directly; no Rust type duplication](#38-schema-types-used-directly-no-rust-type-duplication)
  - [3.9 Unit-based sizes; disk-id key routing](#39-unit-based-sizes-disk-id-key-routing)
- [4. Architecture Overview](#4-architecture-overview)
- [5. Group-0 Sysdata Schema](#5-group-0-sysdata-schema)
- [6. Hierarchy](#6-hierarchy)
- [7. Zone Management](#7-zone-management) — detailed design in [`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md)
- [8. Disk Status Management](#8-disk-status-management)
- [9. Space Metrics](#9-space-metrics) — detailed design in [`design-crowdb-diskdb-space-metrics.md`](design-crowdb-diskdb-space-metrics.md)
- [10. Background Scanner](#10-background-scanner)
- [11. Crate Layout](#11-crate-layout)
- [12. Concurrency Model](#12-concurrency-model)
- [13. Non-Gaps (Good Fits with CROWDB)](#13-non-gaps-good-fits-with-crow)
- [14. Configuration](#14-configuration)
- [15. Implementation Scope](#15-implementation-scope)
- [16. References](#16-references)
- [17. DiskdbClient Scanner / Rebuild Wrappers](#17-diskdbclient-scanner--rebuild-wrappers)
- [18. Group-0 Status Write-Back: Init → Offline](#18-group-0-status-write-back-init--offline)

## 1. Overview

diskdb is a **distributed disk-block allocator** that runs on top of
CROWDB's KV cluster. It is a **lightweight, stateless server**: a diskdb
instance takes ownership of some disk-groups, manages the disks and
space inside them, and persists all durable state to CROWDB KV. It holds
no state that cannot be reconstructed from KV. On crash or restart it
rebuids in-memory structures from the KV records and group-0 metadata.

Multiple diskdb instances run across a cluster, each managing a subset
of disk-groups. diskdb provides fast block allocation/deallocation on
physical disks using per-zone bitmap-scan allocators with per-bit CAS
and a rotating cursor for O(1) block tracking. The free path is
persist-only (the bitmap is a conservative over-estimate; compaction
reclaims freed space). All state changes are durably persisted to CROWDB
KV before being acknowledged to callers.

diskdb **allocates** blocks; it does **not** perform data I/O. Callers
(a future object store, chunk service) write to the allocated
blocks themselves and tell diskdb when they are done (`active_zone`).

**Language:** Rust. **Runtime:** tokio (async everywhere).

**Core goals:**
- **Fast allocation** — per-zone bitmap-scan allocators with per-bit
  CAS (no zone-level lock); compaction reclaims freed space. No
  KV-level CAS on the hot path.
- **Durability via CROWDB KV** — the paxos journal is the sole durable
  store; diskdb has no local WAL and is stateless on disk.
- **Crash safety via record replay** — busy/free records in the paxos
  slots are the source of truth; the bitmap is derived and rebuilt on
  restart.
- **Accurate space metrics** — per-disk / per-disk-group / per-zone
  capacity and busy/free, with a recalculation path to verify
  correctness.

**Design philosophy:** "diskdb is a thin, stateless client of crowdb-kv."
All consensus, replication, and durability are delegated to crowdb-kv.
diskdb's job is allocation policy, disk health, and space accounting —
nothing more. One crowdb-kv extension is required: a `JournalScan` RPC
(slot-range + key-prefix filter, returns ops in slot order) for fast
crash recovery (zone-management §6, strategy 2). This is the sole extension; all other
diskdb ↔ crowdb-kv interaction uses the existing scan / get / put /
batch_write API.

## 2. Non-Goals (Design Envelope)

- **No data I/O.** diskdb allocates blocks; it does not read/write
  block contents. A future diskio-like component does data I/O.
- **No local WAL.** CROWDB KV's WAL is the sole durability mechanism.
- **No consensus code.** diskdb is a client of crowdb-kv, with one
  extension: a `JournalScan` RPC for fast crash recovery (zone-management §6). All
  other interaction uses the existing crowdb-kv API.
- **No native SMR / zoned-namespace SSD support (v1).** Start with
  conventional HDD/SSD (`ZoneBlockDisk`); the zone is a logical concept
  that can later adopt to native zone APIs.
- **No leak-detection scanner (v1).** Leak detection needs caller
  registries that do not exist yet; ship ghost/drift/integrity only.
- **No rack/data-center aggregated dashboards (v1).** Per-disk and
  per-node metrics first; higher-level aggregation later.
- **No console UI (v1).** Minimal HTTP management API first; full
  console integration (web + CLI) as a follow-up.

## 3. Key Design Decisions

### 3.1 Group 0 is the centralized sysdata store

Group 0 (store 0, group 0) holds the basic metadata for the diskdb
subsystem: nodes, disks, disk-groups, and the two maps (disk-group →
diskdb instance, disk-group → paxos data group). diskdb fetches all
metadata from group 0 on startup and on sync. Any local status change
(disk found, disk bad, disk added/removed) is written to group 0 first,
then reflected locally.

**Zones are NOT maintained in group 0** — a zone belongs to a disk, is
created when the disk is added, and is maintained as a separate zone
record on the disk-group's bound paxos data group.

### 3.2 disk-group → paxos group binding via a table (not hash)

A disk-group's zone records all live on **one paxos data group** so
multi-block allocation can use a single `batch_write` (atomic within a
group). The disk-group → paxos-group mapping is a **bind table stored in
group 0**, not a hash. A table (vs hash) enables dynamic scaling: new
paxos data groups can be added and disk-groups rebound without
rehashing the whole keyspace.

**Allocation routing:** the `AllocateBlocks` request carries
`disk_group_id` (not `node_id`); CROWDB uses disk-groups instead of nodes
as the allocation routing unit. Allocate is scoped to one disk-group;
multi-block uses one `batch_write` on that group's bound paxos data
group; atomic within the group. No cross-group multi-block allocate in
v1. The caller issues separate `AllocateBlocks` calls per group if
needed. The caller (or a future placement service) picks the
disk-group.

### 3.3 No CAS needed; exclusive ownership

Each disk-group is owned by exactly one diskdb instance at a time (map
in group 0). A zone in a disk-group therefore has a single writer, so no
KV-level CAS is required. A blind `Put` of the zone record is enough.
In-memory concurrency within one instance is handled by **per-bit CAS**
on the usage bitmap (`compare_exchange` on 64-bit words), not a
zone-level lock. Multiple threads can allocate from the same zone
concurrently. The `ZoneAllocationState` enum is a **derived** field
for reporting (Active/Available/Full based on `used_count`), not a CAS
state machine.

### 3.4 Records are the source of truth; bitmap is derived

For each allocate or free, diskdb **cannot** update the full zone
bitmap in KV, as that would be a large write for the paxos group on every
block. Instead, each allocate writes a small **`BusyBlockValue`** at
the `BusyBlockKey`, and each free writes a **`FreeBlockValue`** at the
`FreeBlockKey` (and deletes the `BusyBlockKey` in the same
`batch_write`). The bitmap is **derived** from the records, never
written directly as a full bitmap on the hot path. The free path is
**persist-only**: the bitmap is not touched on free (the bit stays set,
`used_count` is not decremented); compaction is the sole mechanism for
clearing freed bits. This makes the bitmap a conservative over-estimate
that never shows freed space as available until compaction reconciles
it from records.

Record key layout, value schemas, the record model, current-state
determination, and scan-ordering notes are in
[`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md) §3.

### 3.5 Zone is a logical concept; sizes may vary

Zone is a **logical concept** — a rough mapping that can later adopt to
native zoned-namespace SSD or SMR HDD zone APIs. The last zone on a disk
may be smaller; the word-alignment rule (each zone's `unit_capacity` is
a multiple of 64) ensures no bitmap masking or padding. Details in
[`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md) §2.

### 3.6 Common protocol crate; crowdb-rpc transport

A new `lib/crowdb-protocol` crate holds flatbuffers definitions for all CROWDB
components. diskdb uses it first; crowdb-kv's existing schemas stay where
they are (unchanged). The flatbuffers messages are reused across the
crowdb-rpc transport.

### 3.7 Reuse crowdb-common metrics

diskdb reuses `crowdb-common`'s metrics module (no parallel metrics
system). Per-disk atomic counters stay as hot-path counters that flush
into the crowdb-common registry at reporting intervals.

### 3.8 Schema types used directly; no Rust type duplication

Schema types (`DiskId`, `ChunkId`, `Segment`, `HwStatus`, `DiskType`,
`ZoneAllocationState`, value types) are defined in `crowdb_protocol`
and used directly in Rust, with no field duplication. Extension traits add
domain methods. Internal types (metas, bitmap, CRC logic) are Rust
structs not exposed via rpc.

**Keys are not flatbuffers.** KV keys use the cross-component binary
encoding defined in `doc/design/protocol/design-crowdb-protocol-key.md` (flat
per-kind Rust structs, two-byte `magic | type_tag` header, big-endian
fixed-width fields). Flatbuffers-serialized `*Key` messages are never
used as KV key bytes. The tag bytes break prefix scans. RPC
responses/requests use `**Info` schema messages that flatten key and
value fields into one message; see §5 and zone-management §6.

### 3.9 Unit-based sizes; disk-id key routing

All sizes are in units (block unit size defined per-disk, default 1M).
Record keys are disk-id-based (globally unique → reverse-lookup to the
data group). No `node_id`/`disk_group_id` in record keys or in
`Segment`. The `AllocateBlocks` request carries `disk_group_id` for
routing (§3.2); it is not stored in record keys or `Segment`.

## 4. Architecture Overview

```
                          CROWDB Cluster
   ┌────────────────────────────────────────────────────────────────┐
   │  diskdb-A              diskdb-B              diskdb-C          │
   │  ┌──────────┐          ┌──────────┐          ┌──────────┐      │
   │  │ disk-grp1│          │ disk-grp2│          │ disk-grp3│      │
   │  │ disk-grp4│          │ disk-grp5│          │ disk-grp6│      │
   │  └────┬─────┘          └────┬─────┘          └────┬─────┘      │
   └───────┼─────────────────────┼─────────────────────┼────────────┘
           │  crowdb-kv-client     │                     │
           v                     v                     v
   ┌────────────────────────────────────────────────────────────────┐
   │  CROWDB KV (Multi-Paxos)                                         │
   │  group 0 (sysdata)    group-1..N (diskdb data groups)          │
   │  ┌──────────────┐     ┌──────────────────────────────────┐     │
   │  │ nodes        │     │ zone records (busy/free/snapshot)│     │
   │  │ disk-groups  │     │                                  │     │
   │  │ ownership map│     │                                  │     │
   │  │ binding map  │     │                                  │     │
   │  └──────────────┘     └──────────────────────────────────┘     │
   └────────────────────────────────────────────────────────────────┘
           ^
           │  HTTP mgmt API (axum)
   ┌───────┴────────┐
   │  Console/CLI   │  (follow-up: disk/disk-group management)
   └────────────────┘
```

- **diskdb instance** — one process per node (or per disk-group set).
  Owns disk-groups assigned via group 0. Stateless on disk.
- **disk-group** — logical container of disks on one node; unit of
  ownership and paxos-group binding. A disk-group belongs to exactly
  one node; a node can have multiple disk-groups.
- **group 0** — centralized sysdata: node/disk/disk-group metadata +
  ownership map + binding map.
- **data groups (group-1..N)** — paxos groups holding zone records and
  snapshots. Each disk-group is bound to one data group.

### Protocol

Schema definitions are split across multiple files in
`lib/crowdb-protocol/src/fbs/` (`error_code`, `common_type`,
`diskdb_type`, `diskdb_op`, `diskdb_sys_op`, `diskdb_service`,
`diskdb_sys_service`, `chunkdb_type`, `chunkdb_op`, `chunkdb_service`,
`diskio_op`, `diskio_service`). See the schema files for field-level
detail.

Three rpc services (diskdb now; chunkdb and diskio are future
components with protocol surfaces reserved):
- **`DiskdbService`** (served by the diskdb server): `AllocateBlocks`
  (carries `disk_group_id`, not `node_id`; carries `exclude_disks` for
  anti-affinity), `FreeBlocks`, `QueryCapacityStats`,
  `GetDiskGroupInfo`, `GetDiskInfo`, `RebuildZoneBitmap` (on-demand
  full-scan rebuild, strategy 1, zone-management §6), `MarkBlockSuspect`,
  `MarkBlockCorrupt` (per-block state transitions, zone-management §6). The service ships
  with allocate/free returning `Unimplemented`; the rest are later
  requirements.
- **Hardware admin** (no rpc surface): rack/node/disk-group/disk
  add/remove and `set_*_status` are writes to group-0 sysdata
  performed through `HardwareClient` in `crowdb-kv-client`, invoked by
  the console (`crowdb-web` / `crowdb-cli`). The previous
  `DiskdbAdminService` schema (`diskdb_sys_service.fbs` /
  `diskdb_sys_op.fbs`) is removed — `FetchHardware` is replaced by
  `HardwareClient` prefix scans, `Keepalive` by
  `ServiceRegistryClient.heartbeat`, and the add/remove/status ops by
  `HardwareClient` methods. The diskdb server reads hardware state
  from group 0 via `HardwareClient` in its sync loop; it does not
  serve hardware admin. See `doc/design/kv/design-crowdb-kv-group0.md`
  §2.8.
- **`ChunkdbService`** (future chunkdb server): `AllocateChunk`,
  `AppendChunk`, `QueryChunk`, `SealChunk`, `DeleteChunk`,
  `DeleteChunkRange`, `UpdateChunkStrip`, `ListChunks`.
- **`DiskioService`** (future diskio server): `DiskWrite`, `DiskRead`.

Key protocol decisions: integer IDs throughout (no string UUIDs);
`DiskId` is globally unique (no `node_id`/`disk_group_id` in `Segment`
or record keys. `disk_group_id` is in the `AllocateBlocks` request for
routing only); `Segment.owner_chunk` (192-bit `ChunkId`) replaces the
former `tag`; all sizes are unit-based; errors are returned via rpc
status codes with `ErrorInfo` details (`error_code.fbs`), not
`bool ok + string error` in response bodies.

## 5. Group-0 Sysdata Schema

Group 0 stores the diskdb subsystem metadata as KV entries. The
authoritative schema is defined in
[`doc/design/kv/design-crowdb-kv-group0.md`](../kv/design-crowdb-kv-group0.md)
(text-path keys + JSON values, owned by `crowdb-kv-client`'s
`HardwareClient` / `ServiceRegistryClient` / `KVClusterMetaClient`).
This section summarizes the diskdb-relevant parts; see the group-0
design doc for the full key/value layout and scan patterns.

### Key layout (text-path encoding in group 0)

Group-0 keys use the `TextKey` encoding (`/magic/type/<fields>…`),
JSON-encoded values. The hardware hierarchy embeds the full path in
each key for scan narrowing:

```
/hw/rack/<rack_id>                                 -> RackValue
/hw/node/<rack_id>/<node_id>                       -> NodeValue
/hw/dg/<rack_id>/<node_id>/<dg_id>                 -> DiskGroupValue
/hw/disk/<rack_id>/<node_id>/<dg_id>/<disk_id_hex> -> DiskValue
/hw/dg_owner/<rack_id>/<node_id>/<dg_id>           -> OwnerMapValue
/hw/dg_bind/<rack_id>/<node_id>/<dg_id>            -> BindMapValue
/srv/diskdb/<instance_id>                          -> InstanceValue
/srv/kv-server/<instance_id>                       -> InstanceValue
```

`rack_id`, `node_id`, `dg_id` (`DiskGroupId`), and `instance_id` are
`u64`; `disk_id_hex` is the 32-char hex of the 128-bit `DiskId`
(`high:u64 BE | low:u64 BE`, lowercase). v1 ships flat (no DC layer);
`dc_id` is dropped from `RackKey` and the Info types. The key concept
structs live in `lib/crowdb-protocol/src/key/` and implement both
`BinaryKey` (kept for data-group records) and `TextKey` (group 0). See
`doc/design/protocol/design-crowdb-protocol-key.md` §5 and
`doc/design/kv/design-crowdb-kv-group0.md` §3.

diskdb's own data groups (zone records) keep the `BinaryKey` encoding:
`ZoneKey`, `BusyBlockKey`, `FreeBlockKey` (binary + flatbuffers, unchanged).

Value types live in `crowdb-protocol` (flatbuffers `*Value` types used
directly; new sysdata values `OwnerMapValue`, `BindMapValue`,
`InstanceValue` added as schema messages; Entry return types
`DiskGroupEntry`/`DiskdbOwnerEntry`/`KVGroupBindEntry` are plain serde
structs). See `design-crowdb-kv-group0.md` §3.3.

**Note**: zones are NOT stored in group 0. A zone is created when its
disk is added, and its state (records + snapshot) lives on the
disk-group's bound data group.

### Map semantics

- **Ownership map** (`/hw/dg_owner/...`) — written by the operator (via
  `HardwareClient` through the console) to assign a disk-group to a
  diskdb instance. Read by diskdb on sync.
- **Binding map** (`/hw/dg_bind/...`) — written by the operator to bind
  a disk-group to a paxos data group. Read by diskdb to route zone
  record writes.
- **Service registry** (`/srv/<service>/<instance_id>`) — written by
  each service instance on heartbeat (diskdb, kv-server); used by the
  console and other components for discovery.

## 6. Hierarchy

CROWDB v1 has rack and node (no data-center layer). The physical
hierarchy and the logical disk-group layer:

- **Physical**: rack → node → disk. These are real hardware
  containers. v1 ships with rack and node; a data-center layer is not
  in v1 (the schema drops `dc_id`).
- **Logical**: disk-group sits between node and disk. A disk-group
  belongs to exactly one node; a node can have multiple disk-groups. A
  disk-group is the unit of ownership (assigned to one diskdb instance)
  and the unit of paxos-group binding (a disk-group's zone records all
  live on one paxos data group).

```
Rack (rack_id, u64) → Node (node_id, u64) → Disk-Group (dg_id, u64)  [logical]
  → Disk (DiskId, 128-bit) → Zone (index, variable size) → Disk-Block (1 MB aligned)
```

## 7. Zone Management

diskdb's second major component. The zone is the unit of allocation:
each disk is divided into zones, and each zone has a bitmap-scan
allocator with per-bit CAS (lock-free allocate). The free path is
**persist-only**: the bitmap is not touched on free; compaction is the
sole bit-clearer. Zone rotation (compaction-before-rotation with a
preparatory thread) ensures the allocator always sees accurate free
space in active zones. Crash recovery uses three strategies: full scan
rebuild, journal scan replay, and compaction.

Detailed design (record model, allocation algorithm, free path,
compaction, rotation, crash recovery, zone-level concurrency, and
invariants) lives in
[`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md).

## 8. Disk Status Management

### Node / disk-group / disk HwStatus

Shared enum `HwStatus`: `Init`, `Up`, `Maintenance`, `Suspect`,
`Missing`, `Bad`, `Offline` (ordered by severity). Effective status =
`max(node, group, disk)`, a three-level check (node + disk-group +
disk). The reference impl checks two levels (node + disk); CROWDB adds
the disk-group layer, which is new. Allocations require effective `Up`;
frees allow `Up`, `Maintenance`, or `Suspect`.

Transitions:
- Init → {Up, Offline, Maintenance} on startup (load from group 0).
- Up → Suspect (3 missed syncs).
- Up → Offline / Maintenance (operator).
- Suspect → Up (sync recovers) or → Missing (cannot probe) or → Offline.
- Missing → Bad (confirmed) or → Up (rediscovered). **Missing is
  detected by absence from a group-0 sync response** — a disk/node
  absent from the sync response is transitioned to `Missing` (then to
  `Bad` after confirmation or `Up` if rediscovered).
- Offline ↔ Maintenance (operator).
- Offline → Up (operator).

diskdb's first major component:
- **Sync with group 0** (fixed 10 s interval, same on success and
  failure — no error back-off in v1): fetch latest metadata; update
  group 0 first when local status changes (disk found, disk bad, disk
  added/removed). A disk/node
  **absent from a sync response** is transitioned to `Missing` (then to
  `Bad` after confirmation or `Up` if rediscovered) — this is how
  `Missing` is detected (§8).
- **Disk discovery + health probing**: discover local disks (config-
  driven for v1; `/dev` scan later); probe health (existence, size,
  basic read/write test). Source of truth for disk identity/capacity is
  group 0; live health is probed locally.
- **Disk failure detection + bad-disk handling**: on disk failure,
  transition to `Suspect`, update group 0. When a disk transitions to
  `Bad` (via `Missing → Bad` after confirmation, §8), its busy blocks
  are no longer readable. diskdb does **not** rebuild or relocate them
  inline on the sync path — relocation/rebuild is a follow-up
  requirement. The bad-disk handling on the sync path:
  - Mark the `ZoneDisk` and all its `Zone`s as `Bad`
    (`zone_state = Bad`; `allocatable()` returns `false`). No new
    allocates touch the disk. Free is also blocked —
    `allows_free(Bad)` is `false` (free allows `Up`/`Maintenance`/
    `Suspect` only, §8).
  - Scan the zone records for the bad disk (`read_zone_records` per
    zone, zone-management §6) and collect all live `BusyBlockValue`s — these are the
    impacted blocks. Each carries `owner_chunk` (the chunk that owns
    the allocation) so the caller / data-IO layer can be notified.
  - Emit the `disk.bad.impacted_blocks` gauge (§9) and log the
    hand-off. The collected list is handed to a future
    recovery/relocation path: the data-IO layer rebuilds from
    EC/mirror, or the owner is notified to re-allocate elsewhere.
  - The disk stays `Bad` — its records are read-only until an operator
    removes the disk or marks it `Up` after repair (which triggers
    strategy-1/2 recovery, zone-management §6).
- **Disk-add initialization flow**: the operator adds a disk in group 0
  (via `HardwareClient.add_disk` through the console), which writes
  `DiskValue` to group 0. On the next sync tick, diskdb fetches the
  updated `DiskValue` and sees a disk not yet in its in-memory state.
  diskdb then initializes the disk: creates the in-memory `ZoneDisk`
  with one `Zone` per zone (zone count = `capacity / zone_size`, last
  zone sized per
  zone-management §2's word-alignment rule), and writes baseline `ZoneValue` records
  (empty bitmap, `snapshot_slot = 0`) to the bound data group at
  `ZoneKey { disk_id, zone_index }` in one `batch_write`. These are the
  replay baselines (zone-management §6); subsequent allocates write `BusyBlockValue`
  records on top. Group 0 holds disk metadata only; zone records live
  on the bound data group.

**Follow-up — group-0 notify/watch:** the watch/notify mechanism is a
**client-pulled `WatchNotify` bidi stream**: diskdb opens the stream to
the group-0 leader and subscribes to the prefixes it cares about
(`/hw/dg_owner/`, `/hw/dg_bind/`, `/hw/disk/`); the leader pushes
hw-status-change and ownership-change notifications over that stream.
No separate notify-endpoint registration is needed for notify delivery.
The `rpc_endpoint` arg of `heartbeat_diskdb` is the diskdb rpc
service address for service-registry discovery (so clients can route
`allocate_blocks`), already populated from `config.server.listen_addr`.
This replaces polling for status changes as the primary
change-detection mechanism; polling stays as a safety net with an
increased interval. The detailed design (schema, registry, apply-path
trigger, client, diskdb handler, configuration) lives in
[`design-crowdb-kv-watch-notify.md`](../kv/design-crowdb-kv-watch-notify.md).

## 9. Space Metrics

diskdb's third major component. Detailed design (usage accessors,
`QueryCapacityStats` handler, per-disk counters, keepalive piggyback,
recalc verifier, reporting loop, schema, kv-client aggregation, and the
full `crowdb-diskdb-client` library) lives in
[`design-crowdb-diskdb-space-metrics.md`](design-crowdb-diskdb-space-metrics.md).
This section carries the architecture and the metric categories.

Metrics must show **internal status**
(gauges reflecting current state) and a **latency hierarchy** (where
time is spent, broken down by layer) so operators can diagnose both
capacity problems and performance bottlenecks.

**Space accounting:**
- **Per-disk** — capacity, busy, free, active zone count.
- **Per-disk-group** — aggregated from disks.
- **Per-zone** — dive into busy/free blocks (for the console zone
  visualization).
- **Accurate + recalculation** — statistics are derived from the
  in-memory bitmap (which is derived from the records). A recalculation
  path replays the records to verify the bitmap matches the derived
  statistics, detecting drift.

**Disk-group-level usage summary on keepalive:** diskdb piggybacks a
per-disk-group usage summary (`capacity_bytes`, `used_bytes`,
`free_bytes`, `disk_count`, `allocatable_disk_count`) on the keepalive
message sent to group 0 on each sync tick. Group 0 maintains this at
the disk-group level (`DiskGroupUsageKey { disk_group_id }`). The
console reads this for cluster-wide overview; per-disk/per-zone
drill-down is via the `QueryCapacityStats` API. The summary is
derived (recomputed from the in-memory bitmap on each tick), not a
source of truth.

**Metrics categories:**

Metrics reuse `crowdb-common`'s metrics module. Per-disk hot-path
counters (atomics) flush into the crowdb-common registry at reporting
intervals.

**1. Counters (events, monotonically increasing):**
- per-zone: `allocate.count`, `free.count`,
  `allocate.retry.cms.bit.count` (contention signal — ties to §8 CAS
  retry bound)
- per-disk: `allocate.count`, `free.count`
- per-disk-group: `allocate.count`, `free.count`
- per-instance: `sync.count`, `sync.error.count`,
  `compaction.count`, `compaction.error.count`,
  `free_batch.flush.count` (when batching enabled)

**2. Gauges (internal status, current state snapshot):**
- per-disk: `capacity_bytes`, `used_bytes`, `free_bytes`, `used_pct`,
  `zone_count`, `active_zone_count`
- per-zone: `unit_capacity`, `used_count`, `free_count`,
  `alloc_state` (derived: Active/Available/Full), `hw_status`
  (inherited from disk)
- per-disk-group: `disk_count`, `allocatable_disk_count`,
  `capacity_bytes`, `used_bytes`, `free_bytes`
- per-instance: `owned_disk_group_count`, `degraded` (0/1),
  `free_batch_len` (current pending frees),
  `uncompacted_free_record_count` (per zone — compaction backlog),
  `last_sync_slot` (group-0 sync frontier),
  `last_sync_age_secs` (time since last successful sync)

**3. Latency hierarchy (where time is spent, per layer):**

The allocate/free paths are two-phase (sync bitmap claim + async KV
persist). The latency hierarchy breaks down each phase so operators
can see whether the bottleneck is the in-memory allocator or the KV
persist round-trip.

- **Allocate path:**
  - `allocate.rpc.latency_us` — total RPC latency (handler entry →
    response). Top of the hierarchy.
  - `allocate.bitmap_scan.latency_us` — Phase 1 sync: time in the zone
    bitmap-scan + per-bit CAS (includes CAS retries). Nanoseconds in
    the common case; spikes indicate contention.
  - `allocate.kv_persist.latency_us` — Phase 2 async: time awaiting
    the `batch_write` of `BusyBlockValue` to the data group. Dominant
    latency component (one paxos round-trip).
  - `allocate.zone_rotate.latency_us` — time in `rotate_active_zones`
    when the active set is exhausted.
- **Free path:**
  - `free.rpc.latency_us` — total RPC latency.
  - `free.bitmap_clear.latency_us` — time in the per-bit CAS clear.
  - `free.kv_persist.latency_us` — time awaiting the `batch_write` of
    `FreeBlockValue` (immediate in v1; batch flush when batching enabled).
- **Sync path:**
  - `sync.latency_us` — total `sync_once` latency.
  - `sync.read_group0.latency_us` — time reading from group 0.
  - `sync.apply_changes.latency_us` — time applying changes to
    in-memory state.
- **Compaction path:**
  - `compaction.latency_us` — total compaction latency per zone.
  - `compaction.scan_free.latency_us` — time scanning free records.
  - `compaction.merge_bitmap.latency_us` — time merging free records
    into the `ZoneValue` bitmap (in-memory).
  - `compaction.kv_persist.latency_us` — time awaiting the
    `batch_write` (new `ZoneValue` + delete free records).

Latency metrics use `LatencyHistogram` (percentile precision) for hot
paths (allocate/free bitmap scan, KV persist) and `LatencySummary`
(count + sum + max + avg) for cold paths (sync, compaction, zone
rotate), matching crowdb-kv's convention. Gauges are derived snapshots
updated on the reporting interval, not hot-path writes. `degraded` and
`last_sync_age_secs` are the key health indicators for alerting.

## 10. Background Scanner

The scanner is a periodic consistency check. It detects and reports
live-state drift, catches record corruption early, and gives operators
visibility into cluster health during uptime. It is **not** a safety
mechanism (the free path is persist-only, zone-management §6, and the bitmap is a
conservative over-estimate; freed blocks stay busy until compaction);
it is an operational health mechanism for early corruption detection,
defense-in-depth against unknown bugs or hardware errors, and operator
visibility.

- **Data-safety principle** — a busy block may have data written to
  it. The scanner's first priority is to never free a block that might
  have data. When drift is detected and the true state is uncertain
  (corrupt records, conflicting signals), the scanner defaults to
  **busy** (keep the bit set, keep the block allocated). Wasting space
  is always preferable to freeing a block with data. The scanner only
  clears a bit when records confidently say "free" (no `BusyBlockKey`,
  no `FreeBlockKey`, records intact and readable).
- **Drift detection in the persist-only model** — the free path does
  not touch the bitmap (zone-management §6), so "bit set, no `BusyBlockKey`" is
  **normal** for freed-but-not-compacted blocks (a `FreeBlockKey`
  exists). The scanner distinguishes:
  - **Real ghost-busy** (drift): bit set, no `BusyBlockKey`, no
    `FreeBlockKey` — the block was never freed and never allocated
    (crash between allocate Phase 1 and Phase 2, or a bug). Records
    are authoritative → block is free → safe to clear the bit.
  - **Normal uncompacted**: bit set, no `BusyBlockKey`, `FreeBlockKey`
    exists — the block was freed (persist-only) but compaction hasn't
    cleared the bit yet. This is **not drift** — it's the expected
    state. The scanner counts it as `uncompacted_lag` (not drift) so
    operators can see compaction lag, but does not auto-correct it.
  - **Ghost-free** (drift): bit clear, `BusyBlockKey` exists — should
    not happen in the persist-only model (free never clears bits, and
    only allocate/compaction touch the bitmap). If detected, it
    indicates a bug or hardware error. Records are authoritative →
    block is busy → set the bit back. Data may be written.
- **Ghost scan implementation** (`scanner/ghost.rs`) — `scan_ghosts`
  iterates owned disk-groups → disks → zones. For each non-active,
  unlocked zone, it replays the journal into a throwaway `DdbZone`
  via `recover_zone_inner` (strategy 2) with
  `rebuild_zone_bitmap_full_scan` (strategy 1) fallback — same logic
  as `RecalcEngine::recalc_zone`. It then does a bit-by-bit diff
  (`diff_bitmaps`) between the live and replayed bitmaps, classifying
  each differing bit by checking `ZoneRecords` busy/free offset sets
  (built into `HashSet<u64>` for O(1) lookup). The zone lock is not
  held across awaits (I9); it is re-acquired (try_write) only for the
  synchronous classify + auto-correct step.
- **Re-verify step** — if real drift is detected and
  `reverify_delay_ms > 0`, the scanner sleeps, re-snapshots the live
  bitmap, and re-checks each drifted bit. If the drift disappeared
  during the delay (a zone rotated out with an in-flight allocate
  whose Phase 2 completed), it was transient and is skipped. If
  persistent, it is reported. Setting `reverify_delay_ms = 0`
  disables re-verify (report immediately, may include false
  positives from in-flight operations).
- **Auto-correct** — when `ghost.auto_correct` is enabled and
  re-verify confirms persistent drift, the live bitmap is corrected:
  real ghost-busy → `cas_bit(off, false)` + `used_count` decrement;
  ghost-free → `cas_bit(off, true)` + `used_count` increment. Normal
  uncompacted bits are never auto-corrected (compaction's job).
  Fallback (CRC fail / GC gap) suppresses auto-correct (corruption
  signal — the scanner reports but does not correct when the
  record-derived bitmap itself may be unreliable).
- **Record integrity** (`scanner/integrity.rs`) — `scan_integrity`
  re-checks CRC on the `ZoneValue` snapshot via
  `ZoneValueExt::verify_checksum`, detects records that
  `read_zone_records` silently skips (undecodable keys/values) by
  doing its own prefix scans with `BusyBlockKey::from_bytes` +
  `bincode::deserialize` on each item, and optionally validates
  `owner_chunk` well-formedness (non-zero) on each `BusyBlockValue`.
  Corrupt records are reported; the block is kept busy (key exists,
  data may be written); no auto-correction frees a block with a
  corrupt record.
- **Leak detection** (`scanner/leak.rs`) — deferred (needs caller
  registries). The scaffold returns `LeakScanResult { status:
  "deferred" }` so the scanner loop calls it unconditionally.
- **Scanner task** (`scanner/task.rs`) — `ScannerTask` implements
  `BackgroundTask` and runs on `BgRunner` with a `TimerFn` trigger
  reading `scanner.scan_interval_secs` from the shared config handle.
  `ScanState` holds the last `ScanSummary` (shared between the task
  and the rpc service handlers) + an `AtomicBool` scan-requested
  flag (set by `TriggerScan`, consumed at the start of the next
  `run_cycle`) + an `AtomicBool` in-progress flag (prevents overlap).
- **Admin RPCs** — `TriggerScan` sets the scan-requested flag and
  returns the current summary + `scan_in_progress` flag;
  `GetScanStatus` returns the last summary + `has_run` flag. The
  `ScanSummary` schema message carries all scan counts (ghost_busy,
  ghost_free, uncompacted_lag, corrupt_snapshots, corrupt_records,
  owner_mismatches, leak_status) + timing.
- **Scanner metrics** — `scanner_runs_total` (counter),
  `scanner_duration_ms` (latency summary), `scanner_ghosts_found`
  (gauge), `scanner_drift_found` (gauge = ghost_busy + ghost_free),
  `scanner_corrupt_records` (gauge = corrupt_snapshots +
  corrupt_records).
- **Zone coordination** — the scanner skips active zones, acquires the
  zone-level lock for non-active zones, and coordinates with compaction
  via the shared lock. Details in
  [`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md) §9.

## 11. Crate Layout

```
lib/crowdb-protocol         (common flatbuffers for all CROWDB components; diskdb first)
app/crowdb-diskdb           (server: lib + binary — types, allocator, records, sync, rpc + HTTP, CLI)
lib/crowdb-diskdb-client    (client library for callers: allocate/free/query; crowdb-rpc transport)
```

- **`lib/crowdb-protocol`** — flatbuffers definitions for diskdb rpc services
  (allocate/free/query) + extension traits for schema types
  (`diskdb_type_util.rs`: `DiskIdExt`, `HwStatusExt`,
  `ZoneAllocationStateExt`, `ZoneValueExt`, `effective_status`).
  crowdb-kv's existing schemas stay in `crowdb-kv` (unchanged). The
  flatbuffers messages are reused across the crowdb-rpc transport.
- **`app/crowdb-diskdb`** — server crate (package name `crowdb-diskdb`).
  Combined library + binary: contains all diskdb logic (types, zone
  allocator, record persistence, scanner, ownership/sync, rpc +
  HTTP handlers, CLI, config). The library target enables integration
  tests without spawning a separate process; the binary target
  (`crowdb-diskdb`) is the server executable. Proto types and their
  extension traits are re-exported from `crowdb_protocol`; internal types
  (metas, key layout, `AllocatedRange`, `ActiveZoneContext`, bitmap)
  are Rust structs.
- **`lib/crowdb-diskdb-client`** — client library for easy access to the server
  (allocate/free/query), mirroring `crowdb-kv-client`'s retry +
  topology-cache pattern. crowdb-rpc transport; flatbuffers framing.

### Dependencies

```
crowdb-common (metrics, logging, time)
  ↑
crowdb-kv-client (sole durable store — group 0 + data groups)
  ↑
crowdb-diskdb (server) ──depends──> crowdb-kv-client, crowdb-common, protocol
crowdb-diskdb-client  ──depends──> protocol, crowdb-kv-client (for group-0 discovery)

protocol ──> (no internal deps beyond flatbuffers/crowdb-rpc)
```

## 12. Concurrency Model

All public and inter-module APIs are `async`. Runtime is `tokio`
(multi-threaded for production).

- No blocking calls in business-logic paths.
- **Allocate** — lock-free per-bit CAS on the usage bitmap; no
  zone-level lock. Allocate runs only on active zones. Details in
  [`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md) §4, §8.
- **Free** — persist-only: one `batch_write`, no bitmap touch, no
  zone-level lock. Free can run on any zone without coordination.
  Details in [`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md) §4, §8.
- **Zone-level lock for non-allocate operations** — compaction,
  scanner, and health checks acquire a zone-level lock on non-active
  zones only. Common methods on `DdbZone` encapsulate the lock +
  operation. Details in
  [`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md) §8.
- Disk-level active zone set uses RCU publish (`Arc` swap) for
  lock-free reads; rotation takes a brief write lock.
- **Free-side lookup structures** — disk-id → disk hash map and
  zone-index → zone vec for O(1) lookups on the free path. Details in
  [`design-crowdb-diskdb-zone-management.md`](design-crowdb-diskdb-zone-management.md) §8.
- v1 free is immediate (no `FreeBatch`, no background flush loop). When
  free batching ships, `FreeBatch` will be protected by a `Mutex` held
  only for the append, not for the KV flush; the flush is triggered by
  batch size (no timer).
- Node-level `add_disk` / `remove_disk` acquire a write lock on the
  disk list; allocation/free acquire a read lock (concurrent with each
  other, exclusive with add/remove).

## 13. Non-Gaps (Good Fits with CROWDB)

These design assumptions map cleanly onto CROWDB and need no design
work, just implementation:

- **Durability model**: crowdb-kv's WAL is the sole durable log. diskdb's
  blind writes become durable via crowdb-kv's WAL flush.
- **Consensus semantics**: Multi-Paxos with parallel slots. For diskdb's
  usage (blind writes of zone records), parallel slots may even improve
  allocation throughput. No change needed.
- **Blind-ops persistence**: diskdb persists via blind Puts (no
  read-modify-write) — matches crowdb's blind-ops-only model exactly
  (§3.3).
- **Async runtime**: tokio multi-threaded; diskdb's two-phase
  async allocation (sync bitmap-scan claim + async KV persist) maps
  directly.

## 14. Configuration

All settings that control flow behavior live in a config class (no
hardcoded tunables in business logic). Defaults:

- **Sync** — sync interval (10 s, fixed — same on success and failure),
  degraded miss threshold (3), temp-failure timeout (900 s)
- **Allocator** — `zone_rotate_count`, CAS retry limit (100)
- **Free** — `free_batch_enabled` (default false — v1 immediate free;
  size-threshold batching when true), `free_flush_max_batch` (256,
  used when batching is enabled)
- **Compaction** — snapshot compaction threshold (record count or
  time), compaction cadence (periodic interval for strategy 3)
- **Disk** — block / unit size (default 1 MB), zone size
- **Free validation** — `validate_owner_on_free` (default false — no
  KV read on free; enable for strict ownership validation)
- **Scanner** — `scan_interval_secs` (600), `ghost.detect` (true),
  `ghost.auto_correct` (false — manual review first; enable for
  self-healing), `integrity.verify` (true),
  `integrity.detect_owner_mismatch` (false — piggybacks on
  `read_zone_records`), `reverify_delay_ms` (1000 — set 0 to disable
  re-verify)

## 15. Implementation Scope

The diskdb implementation is organized by functional scope. Each area
below covers a coherent slice of the component; together they make up
the full diskdb server.

- **Protocol + core types** — flatbuffers services, core types, record key
  layout, config validation, bitmap utilities, CRC integrity.
- **Group-0 sysdata schema + sync** — disk status management, group-0
  read/write, ownership/binding maps, heartbeat, disk-add initialization
  flow, keepalive usage summary.
- **Zone allocator + record persistence** — bitmap-scan allocator,
  rotating active-zone-set, busy/free records, two-phase async
  allocation, persist-only free (no `FreeBatch`, no timer),
  `disk_group_id` routing, `exclude_disks`, CAS retry bound.
- **Crash recovery + snapshot compaction** — three strategies: full
  scan rebuild, journal scan replay via `JournalScan` RPC, compaction
  that deletes only free records.
- **Space metrics + query API** — per-disk/group/zone metrics,
  recalculation path, `query_disk_usage`, three-category metrics:
  counters, gauges, latency hierarchy.
- **Background scanner** — ghost/drift/integrity detection, per-block
  state validation, uses strategy 1 full scan rebuild.
- **Disk discovery + health probing** — config-driven disk list, health
  probe, disk failure detection.
- **Console + CLI integration** — disk/disk-group management UI, zone
  busy/free visualization, CLI command design.
- **Group-0 notify/watch** — replace polling with watch/notify; see
  [`design-crowdb-kv-watch-notify.md`](../kv/design-crowdb-kv-watch-notify.md).
  Polling stays as safety net.
- **Free batch** — size-threshold batching, no timer; graceful-shutdown
  drain + flush.

The design doc (this file and future sub-designs under
`doc/design/diskdb/`) is kept permanently — it is the root design for
the diskdb component area.

## 16. References

- CROWDB root KV design: `doc/design/kv/design-crowdb-kv.md`
- CROWDB WAL design: `doc/design/kv/design-crowdb-kv-wal.md`
- CROWDB metrics design: `doc/design/kv/design-crowdb-kv-observability.md`
- CROWDB console design: `doc/design/console/design-crowdb-console.md`

---

## 17. DiskdbClient Scanner / Rebuild Wrappers

`DiskdbClient` wraps allocate/free/query/recalc/compact and also the
scanner RPCs (`TriggerScan`, `GetScanStatus`) and
`RebuildZoneBitmap`. The schema + server handlers exist; the client
wrappers make them reachable from the REST proxy and CLI.

```rust
use crowdb_protocol::diskdb::rpc::{
    TriggerScanRequest, TriggerScanResponse,
    GetScanStatusRequest, GetScanStatusResponse,
    RebuildZoneBitmapRequest, RebuildZoneBitmapResponse,
};

impl DiskdbClient {
    /// Trigger a scan on all owned groups (or one group if `dg_id`
    /// is set). Returns the last `ScanSummary` + `scan_in_progress`.
    pub async fn trigger_scan(&self, dg_id: Option<DiskGroupId>)
        -> Result<TriggerScanResponse>

    /// Get the last scan summary + `has_run` flag.
    pub async fn get_scan_status(&self, dg_id: Option<DiskGroupId>)
        -> Result<GetScanStatusResponse>

    /// Rebuild one zone's bitmap on a disk. Routes via `dg_for_disk`.
    pub async fn rebuild_zone_bitmap(
        &self, disk_id: DiskId, zone_index: u32,
    ) -> Result<RebuildZoneBitmapResponse>
}
```

- `trigger_scan` / `get_scan_status` route to `first_cached_dg()`
  when `dg_id` is `None` (mirrors `recalc_disk_usage`); otherwise
  route to the specified dg. Both go through `with_retry`.
- `rebuild_zone_bitmap` routes via `dg_for_disk(disk_id)` (mirrors
  `compact_zone`), then `with_retry`.
- All three are admin/debug calls; transient `Unavailable` is retried
  per `RetryConfig`; `NotFound` (unknown disk/zone) is returned
  immediately.

Edge cases:
- `trigger_scan` while a scan is running → server returns
  `scan_in_progress: true`; client returns the response as-is (no
  error, no stacking).
- `rebuild_zone_bitmap` on unknown disk → `dg_for_disk` returns
  `Unreachable`; surfaced to the caller.
- Empty endpoint cache → `first_cached_dg()` returns `Unreachable`;
  caller sees "no cached endpoints; call refresh_endpoints".

## 18. Group-0 Status Write-Back: Init → Offline

`background_zone_load` transitions `Init → Offline` when zone loading
fails for all strategies (`all_ok = false`) without writing back to
group 0. The function is a `tokio::spawn`'d background task that does
not receive `HardwareClient`. Consequence: group 0 still says `Up`,
the next sync tick reads `Up` → `recover_disk_to_up` → the disk
becomes `Up` with broken zones.

The fix:
- `HardwareClient` is `Clone` (it wraps `Arc<CrowdbClient>`).
- `hw` is passed into `background_zone_load`; on `all_ok = false`,
  `write_back_disk_status(rack_id, node_id, dg_id, &disk_id, Offline)`
  is called before the `Init → Offline` transition.
- A disk whose zone load fails ends up `Offline` in both group 0 and
  the runtime state machine, and stays `Offline` across sync ticks
  (no flip-flop to `Up`).
