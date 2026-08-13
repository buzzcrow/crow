<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: diskdb (Overview)

This is the root design document for the diskdb component area. It
defines **what diskdb is**, **why key choices were made**, and **how the
component is structured**. Field-level details live in the proto files
and Rust source; this doc covers decisions and architecture only.

References (other projects, not ports):
- `/cjdata/cpp/aioss/server/diskdb/doc/design.md` — overall design.
- `/cpp/buzz-cpp/src/app/buzz-disk-db` — zone allocator algorithm
  (bitmap-scan, per-bit CAS, zone rotation).

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
  - [3.6 Common protocol crate; gRPC now, custom RPC later](#36-common-protocol-crate-grpc-now-custom-rpc-later)
  - [3.7 Reuse crow-common metrics](#37-reuse-crow-common-metrics)
  - [3.8 Proto types used directly; no Rust type duplication](#38-proto-types-used-directly-no-rust-type-duplication)
  - [3.9 Unit-based sizes; disk-id key routing](#39-unit-based-sizes-disk-id-key-routing)
- [4. Architecture Overview](#4-architecture-overview)
- [5. Group-0 Sysdata Schema](#5-group-0-sysdata-schema)
- [6. Hierarchy](#6-hierarchy)
- [7. Zone Records and Crash Recovery](#7-zone-records-and-crash-recovery)
  - [Record key layout](#record-key-layout)
  - [Value schemas](#value-schemas)
  - [Record model](#record-model)
  - [Three recovery strategies](#three-recovery-strategies)
  - [How the strategies work together](#how-the-strategies-work-together)
  - [Crash-safety invariants](#crash-safety-invariants)
  - [Recovery and compaction engines](#recovery-and-compaction-engines)
  - [Recovery triggers](#recovery-triggers)
- [8. Allocation Algorithm](#8-allocation-algorithm)
  - [Zone-level allocate (sync, in-memory)](#zone-level-allocate-sync-in-memory)
  - [Disk-level allocate (sync) — rotating active-zone-set](#disk-level-allocate-sync--rotating-active-zone-set)
  - [Round-robin across disks within the named disk-group (sync)](#round-robin-across-disks-within-the-named-disk-group-sync)
  - [Two-phase async allocation](#two-phase-async-allocation)
  - [Free (immediate in v1)](#free-immediate-in-v1)
  - [Zone rotation (internal)](#zone-rotation-internal)
- [9. State Machines](#9-state-machines)
  - [Node / disk-group / disk HwStatus](#node--disk-group--disk-hwstatus)
  - [Zone allocation state (derived, not a CAS state machine)](#zone-allocation-state-derived-not-a-cas-state-machine)
- [10. Disk Status Management](#10-disk-status-management)
- [11. Space Metrics](#11-space-metrics) — detailed design in [`design-crow-diskdb-space-metrics.md`](design-crow-diskdb-space-metrics.md)
- [12. Background Scanner](#12-background-scanner)
- [13. Crate Layout](#13-crate-layout)
- [14. Concurrency Model](#14-concurrency-model)
- [15. Non-Gaps (Good Fits with CROW)](#15-non-gaps-good-fits-with-crow)
- [16. Configuration](#16-configuration)
- [17. Implementation Scope](#17-implementation-scope)
- [18. References](#18-references)

## 1. Overview

diskdb is a **distributed disk-block allocator** that runs on top of
CROW's KV cluster. It is a **lightweight, stateless server**: a diskdb
instance takes ownership of some disk-groups, manages the disks and
space inside them, and persists all durable state to CROW KV. It holds
no state that cannot be reconstructed from KV — on crash or restart it
rebuids in-memory structures from the KV records and group-0 metadata.

Multiple diskdb instances run across a cluster, each managing a subset
of disk-groups. diskdb provides fast block allocation/deallocation on
physical disks using per-zone bitmap-scan allocators with per-bit CAS
and a rotating cursor for O(1) block tracking. Freed space is reused
immediately (no append-only position). All state changes are durably
persisted to CROW KV before being acknowledged to callers.

diskdb **allocates** blocks; it does **not** perform data I/O. Callers
(a future object store, chunk service, etc.) write to the allocated
blocks themselves and tell diskdb when they are done (`active_zone`).

**Language:** Rust. **Runtime:** tokio (async everywhere).

**Core goals:**
- **Fast allocation** — per-zone bitmap-scan allocators with per-bit
  CAS (no zone-level lock); freed space is reused immediately via
  bitmap scan. No KV-level CAS on the hot path.
- **Durability via CROW KV** — the paxos journal is the sole durable
  store; diskdb has no local WAL and is stateless on disk.
- **Crash safety via record replay** — busy/free records in the paxos
  slots are the source of truth; the bitmap is derived and rebuilt on
  restart.
- **Accurate space metrics** — per-disk / per-disk-group / per-zone
  capacity and busy/free, with a recalculation path to verify
  correctness.

**Design philosophy:** "diskdb is a thin, stateless client of crow-kv."
All consensus, replication, and durability are delegated to crow-kv.
diskdb's job is allocation policy, disk health, and space accounting —
nothing more. One crow-kv extension is required: a `JournalScan` RPC
(slot-range + key-prefix filter, returns ops in slot order) for fast
crash recovery (§7, strategy 2). This is the sole extension; all other
diskdb ↔ crow-kv interaction uses the existing scan / get / put /
batch_write API.

## 2. Non-Goals (Design Envelope)

- **No data I/O.** diskdb allocates blocks; it does not read/write
  block contents. A future diskio-like component does data I/O.
- **No local WAL.** CROW KV's WAL is the sole durability mechanism.
- **No consensus code.** diskdb is a client of crow-kv, with one
  extension: a `JournalScan` RPC for fast crash recovery (§7). All
  other interaction uses the existing crow-kv API.
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
`disk_group_id` (not `node_id`); CROW uses disk-groups instead of nodes
as the allocation routing unit. Allocate is scoped to one disk-group;
multi-block uses one `batch_write` on that group's bound paxos data
group; atomic within the group. No cross-group multi-block allocate in
v1 — the caller issues separate `AllocateBlocks` calls per group if
needed. The caller (or a future placement service) picks the
disk-group.

### 3.3 No CAS needed; exclusive ownership

Each disk-group is owned by exactly one diskdb instance at a time (map
in group 0). A zone in a disk-group therefore has a single writer — no
KV-level CAS is required. A blind `Put` of the zone record is enough.
In-memory concurrency within one instance is handled by **per-bit CAS**
on the usage bitmap (`compare_exchange` on 64-bit words), not a
zone-level lock — multiple threads can allocate from the same zone
concurrently. The `ZoneAllocationState` enum is a **derived** field
for reporting (Active/Available/Full based on `used_count`), not a CAS
state machine.

### 3.4 Records are the source of truth; bitmap is derived

For each allocate or free, diskdb **cannot** update the full zone
bitmap in KV — that would be a large write for the paxos group on every
block. Instead, each allocate writes a small **`BusyBlockValue`** at
the `BusyBlockKey` (keyed by `unit_offset`), and each free writes a
**`FreeBlockValue`** at the `FreeBlockKey`. The bitmap is **derived**
from the records — never written directly as a full bitmap on the hot
path. Freed space is reused immediately by the in-memory bitmap-scan
allocator (no append-only `allocate_pos`).

**Record model:**

- **`BusyBlockValue`** — written on allocate. Carries `owner_chunk`
  (the reverse reference to the block's owner), `unit_size`, and
  `state`. **Deleted on free** (in the same `batch_write` that writes
  the `FreeBlockValue`). On re-allocate (after a free), a new
  `BusyBlockValue` is written at the same key (new owner,
  `state = Ok`). Bounded by the number of currently-busy blocks (≤
  disk capacity).
- **`FreeBlockValue`** — written on free, in the same `batch_write`
  that deletes the `BusyBlockKey`. Carries `previous_owner` (the
  `owner_chunk` from the freed `BusyBlockValue`) for audit / scanner
  cross-check. On re-allocate, the `FreeBlockKey` is deleted (the
  block is busy again). **Transient** — deleted by compaction after
  being merged into the `ZoneValue` bitmap. Bounded by the number of
  frees since the last compaction.
- **`ZoneValue`** — the compacted snapshot bitmap. Updated periodically
  by compaction.

**Current state determination** (no slot ordering needed):
- A block is **busy** iff its `BusyBlockKey` exists.
- A block is **free** otherwise. A `FreeBlockKey` may exist for a
  not-yet-compacted free (carrying `previous_owner`); after compaction
  merges the free into `ZoneValue` and deletes the `FreeBlockKey`,
  neither key exists for that offset.

The full `ZoneValue` is a **compacted snapshot** written periodically.
The ideal approach on free would be to update the bitmap in the
`ZoneValue` and write the whole `ZoneValue` to KV. But `ZoneValue` is
large (full bitmap), and frees are random across all zones and disks,
so a per-free `ZoneValue` write is too expensive. Instead, each free
writes a small `FreeBlockValue` and deletes the `BusyBlockKey` in one
`batch_write`; later, compaction lists the free records for a zone (a
prefix scan — the free records of one zone are contiguous in the
crow-tree page, so this is efficient), merges them into the `ZoneValue`
bitmap (clear the freed bits), writes the updated `ZoneValue` once, and
deletes the free records in one `batch_write`.

On crash/restart, diskdb reconstructs the in-memory zone state using
three complementary strategies (§7):

- **Strategy 1 (full scan rebuild)** — on-demand, via RPC/API; scans
  all live busy/free records and rebuilds the bitmap from scratch. Used
  for consistency checks or full rebuilds; not in the common code flow.
- **Strategy 2 (journal scan replay)** — the primary restart path.
  Loads the `ZoneValue` snapshot, then replays only the operations
  written after `snapshot_slot`, in slot order, via a `JournalScan` RPC
  (a crow-kv extension: slot-range + key-prefix filter, returns ops in
  slot order). Fast because compaction keeps the uncompacted record set
  small.
- **Strategy 3 (compaction)** — ongoing maintenance. Merges free
  records into `ZoneValue` and deletes only the free records. Keeps
  strategy 2's replay fast.

**Note on scan ordering:** a key prefix scan returns records in
**lexicographic key order** (= `unit_offset` order), not slot order.
`min_slot` on the crow-kv scan request is a read-freshness floor, not a
record-slot filter. Strategy 1 works without slot ordering because the
busy record's existence is the indicator — a block is busy iff its
`BusyBlockKey` exists (not the write order). Strategy 2 requires the
`JournalScan` extension to get slot-ordered replay.

### 3.5 Zone is a logical concept; sizes may vary

Not all zones on a disk must be the same size — the last zone may be
smaller (disk capacity is rarely an exact multiple of the zone size).
Zone is a **logical concept** defined for easier implementation; it can
later adopt to native zoned-namespace SSD or SMR HDD zone APIs, but
currently no such devices are targeted — the zone is a rough mapping.

**Word alignment rule:** each zone's `unit_capacity` must be a multiple
of 64 (the 64-bit bitmap word size). All zones except the last have
`unit_capacity = zone_size / unit_size`. The last zone has
`unit_capacity = remaining_capacity / unit_size`, rounded down to a
multiple of 64; the sub-64-unit tail (at most 63 units) is unallocated.
Only the last zone may have a different size; all other zones on a disk
are uniform. No bitmap masking, no padding bits — each zone's bitmap is
sized to its own `unit_capacity`, which is word-aligned.

### 3.6 Common protocol crate; gRPC now, custom RPC later

A new `lib/crow-protocol` crate holds protobuf definitions for all CROW
components. diskdb uses it first; crow-kv's existing protos stay where
they are (unchanged). Later, when CROW adds its own RPC transport, the
protobuf messages are reused and only the transport changes (custom RPC
+ flatbuffer is a future direction).

### 3.7 Reuse crow-common metrics

diskdb reuses `crow-common`'s metrics module (no parallel metrics
system). Per-disk atomic counters stay as hot-path counters that flush
into the crow-common registry at reporting intervals.

### 3.8 Proto types used directly; no Rust type duplication

Proto types (`DiskId`, `ChunkId`, `Segment`, `HwStatus`, `DiskType`,
`ZoneAllocationState`, value types) are defined in `crow_protocol`
and used directly in Rust — no field duplication. Extension traits add
domain methods. Internal types (metas, bitmap, CRC logic) are Rust
structs not exposed via gRPC.

**Keys are not protobuf.** KV keys use the cross-component binary
encoding defined in `doc/design/protocol/design-crow-protocol-key.md` (flat
per-kind Rust structs, two-byte `magic | type_tag` header, big-endian
fixed-width fields). Protobuf-serialized `*Key` messages are never
used as KV key bytes — the tag bytes break prefix scans. RPC
responses/requests use `**Info` proto messages that flatten key and
value fields into one message; see §5 and §7.

### 3.9 Unit-based sizes; disk-id key routing

All sizes are in units (block unit size defined per-disk, default 1M).
Record keys are disk-id-based (globally unique → reverse-lookup to the
data group). No `node_id`/`disk_group_id` in record keys or in
`Segment`. The `AllocateBlocks` request carries `disk_group_id` for
routing (§3.2); it is not stored in record keys or `Segment`.

## 4. Architecture Overview

```
                          CROW Cluster
   ┌────────────────────────────────────────────────────────────────┐
   │  diskdb-A              diskdb-B              diskdb-C          │
   │  ┌──────────┐          ┌──────────┐          ┌──────────┐      │
   │  │ disk-grp1│          │ disk-grp2│          │ disk-grp3│      │
   │  │ disk-grp4│          │ disk-grp5│          │ disk-grp6│      │
   │  └────┬─────┘          └────┬─────┘          └────┬─────┘      │
   └───────┼─────────────────────┼─────────────────────┼────────────┘
           │  crow-kv-client     │                     │
           v                     v                     v
   ┌────────────────────────────────────────────────────────────────┐
   │  CROW KV (Multi-Paxos)                                         │
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

Proto definitions are split across multiple files in
`lib/crow-protocol/src/proto/` (`error_code`, `common_type`,
`diskdb_type`, `diskdb_op`, `diskdb_sys_op`, `diskdb_service`,
`diskdb_sys_service`, `chunkdb_type`, `chunkdb_op`, `chunkdb_service`,
`diskio_op`, `diskio_service`). See the proto files for field-level
detail.

Three gRPC services (diskdb now; chunkdb and diskio are future
components with protocol surfaces reserved):
- **`DiskdbService`** (served by the diskdb server): `AllocateBlocks`
  (carries `disk_group_id`, not `node_id`; carries `exclude_disks` for
  anti-affinity), `FreeBlocks`, `QueryCapacityStats`,
  `GetDiskGroupInfo`, `GetDiskInfo`, `RebuildZoneBitmap` (on-demand
  full-scan rebuild, strategy 1, §7), `MarkBlockSuspect`,
  `MarkBlockCorrupt` (per-block state transitions, §7). The service ships
  with allocate/free returning `Unimplemented`; the rest are later
  requirements.
- **Hardware admin** (no gRPC surface): rack/node/disk-group/disk
  add/remove and `set_*_status` are writes to group-0 sysdata
  performed through `HardwareClient` in `crow-kv-client`, invoked by
  the console (`crow-web` / `crow-cli`). The previous
  `DiskdbAdminService` proto (`diskdb_sys_service.proto` /
  `diskdb_sys_op.proto`) is removed — `FetchHardware` is replaced by
  `HardwareClient` prefix scans, `Keepalive` by
  `ServiceRegistryClient.heartbeat`, and the add/remove/status ops by
  `HardwareClient` methods. The diskdb server reads hardware state
  from group 0 via `HardwareClient` in its sync loop; it does not
  serve hardware admin. See `doc/design/kv/design-crow-kv-group0.md`
  §2.8.
- **`ChunkdbService`** (future chunkdb server): `AllocateChunk`,
  `AppendChunk`, `QueryChunk`, `SealChunk`, `DeleteChunk`,
  `DeleteChunkRange`, `UpdateChunkStrip`, `ListChunks`.
- **`DiskioService`** (future diskio server): `DiskWrite`, `DiskRead`.

Key protocol decisions: integer IDs throughout (no string UUIDs);
`DiskId` is globally unique (no `node_id`/`disk_group_id` in `Segment`
or record keys — `disk_group_id` is in the `AllocateBlocks` request for
routing only); `Segment.owner_chunk` (192-bit `ChunkId`) replaces the
former `tag`; all sizes are unit-based; errors are returned via gRPC
status codes with `ErrorInfo` details (`error_code.proto`), not
`bool ok + string error` in response bodies.

## 5. Group-0 Sysdata Schema

Group 0 stores the diskdb subsystem metadata as KV entries. The
authoritative schema is defined in
[`doc/design/kv/design-crow-kv-group0.md`](../kv/design-crow-kv-group0.md)
(text-path keys + JSON values, owned by `crow-kv-client`'s
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
structs live in `lib/crow-protocol/src/key/` and implement both
`BinaryKey` (kept for data-group records) and `TextKey` (group 0). See
`doc/design/protocol/design-crow-protocol-key.md` §5 and
`doc/design/kv/design-crow-kv-group0.md` §3.

diskdb's own data groups (zone records) keep the `BinaryKey` encoding:
`ZoneKey`, `BusyBlockKey`, `FreeBlockKey` (binary + prost, unchanged).

Value types live in `crow-protocol` (proto `*Value` types used
directly; new sysdata values `OwnerMapValue`, `BindMapValue`,
`InstanceValue` added as proto messages; Entry return types
`DiskGroupEntry`/`DiskdbOwnerEntry`/`KVGroupBindEntry` are plain serde
structs). See `design-crow-kv-group0.md` §3.3.

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

CROW v1 has rack and node (no data-center layer). The physical
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

## 7. Zone Records and Crash Recovery

### Record key layout

Three types of KV entries on the disk-group's bound data group. Keys
use the cross-component binary encoding
(`doc/design/protocol/design-crow-protocol-key.md`): each is a flat struct with
a `magic | type_tag` header and big-endian fixed-width fields. Keys
are disk-id-based (globally unique → reverse-lookup to the data group).
No `node_id`/`disk_group_id` in record keys.

```
# Busy block (on allocate; deleted on free)
BusyBlockKey { disk_id, zone_index, unit_offset }  -> BusyBlockValue

# Free block (on free, in the same batch_write that deletes the BusyBlockKey;
# transient — deleted by compaction)
FreeBlockKey  { disk_id, zone_index, unit_offset }  -> FreeBlockValue

# Zone (compacted snapshot, periodic)
ZoneKey       { disk_id, zone_index }               -> ZoneValue
```

`disk_id` is 16 bytes (`high:u64 BE | low:u64 BE`); `zone_index` is
`u32 BE`; `unit_offset` is `u64 BE`. Big-endian fixed width gives
lexicographic byte order == numeric order, so a prefix scan of one
zone's busy (or free) keys returns blocks in `unit_offset` order
without deserialization. Exact byte layouts and prefix constructors
(e.g. `BusyBlockKey::prefix_for_zone(disk_id, zone_index)`) are in
`doc/design/protocol/design-crow-protocol-key.md`; value field details are in
`diskdb_type.proto`.

### Value schemas

- **`BusyBlockValue`**:
  - `unit_count: u32` — number of units in this allocation (≥ 1;
    multi-unit allocations span consecutive offsets).
  - `unit_size: u32` — size of one unit in bytes (e.g. 1 MB). Carried
    per-block so the data-IO layer knows the block's granularity
    without a separate lookup.
  - `owner_chunk: ChunkId` (192-bit) — reverse reference to the
    block's owner (the chunk that holds this block's data). Needed for
    recovery and other logic.
  - `state: BlockState` — per-block I/O-behavior state enum:
    `Ok` (normal; default on allocate), `Suspect` (data may be
    unreadable; data-IO layer tries with a timeout, falls back to
    mirror/EC rebuild), `Corrupt` (data confirmed unreadable; data-IO
    layer skips read, rebuilds from EC/mirror). Updated by background
    paths (sync, health probe, scanner) or via `MarkBlockSuspect` /
    `MarkBlockCorrupt` admin RPCs — **not** by the allocate hot path.
- **`FreeBlockValue`**:
  - `unit_count: u32` — number of units freed (matches the
    corresponding `BusyBlockValue.unit_count`).
  - `previous_owner: ChunkId` (192-bit) — the `owner_chunk` from the
    `BusyBlockValue` that was freed (carried in the `Segment` on the
    free request; no KV read needed in v1). Carried for audit /
    scanner cross-check. No `state` field — a free block has no data.
- **`ZoneValue`**:
  - `usage_bitmap: bytes` — the full zone bitmap (one bit per unit;
    bit set = busy, bit clear = free). Sized to the zone's
    `unit_capacity` (multiple of 64 bits per §3.5).
  - `snapshot_slot: u64` — the slot at which this snapshot was written.
    Strategy 2 (journal scan replay) replays operations after this slot.
  - `crc32: u32` — CRC32 checksum over `usage_bitmap` for integrity
    verification (§12 scanner).

### Record model

- A busy/free entry can span multiple units (`unit_count` ≥ 1).
- **On free, the `BusyBlockKey` is deleted** and a `FreeBlockValue` is
  written at `FreeBlockKey` in the same `batch_write`. The
  `FreeBlockValue` carries `previous_owner` (the `owner_chunk` from the
  freed `BusyBlockValue`) for audit.
- On re-allocate, the `FreeBlockKey` is deleted and a new
  `BusyBlockValue` is written at the `BusyBlockKey` (new owner,
  `state = Ok`).
- **Current state determination** (no slot ordering needed): a block is
  **busy** iff its `BusyBlockKey` exists; otherwise it is **free**. A
  `FreeBlockKey` may exist for a not-yet-compacted free (carrying
  `previous_owner`); after compaction, neither key exists for that
  offset.
- `ZoneValue` carries a CRC32 checksum for integrity verification.

### Three recovery strategies

diskdb uses three complementary strategies for crash recovery and
maintenance. All three belong in the design, each in its role.

**Strategy 1 — full scan rebuild (on-demand, via RPC/API).**

Scan all live `BusyBlockKey`s for a zone and rebuild the bitmap from
scratch. No snapshot needed. For each offset: if a `BusyBlockKey`
exists, the bit is set (busy); otherwise the bit is clear (free). No
slot ordering needed — the busy record's existence is the indicator.
(`FreeBlockKey`s carry `previous_owner` audit info but are not needed
for state determination.) **Not in the common code flow** — provided
as an on-demand operation via the `RebuildZoneBitmap` RPC and API, used
for consistency checks (verify the in-memory bitmap matches the
records) or a full rebuild (e.g. after corruption, or when no
`ZoneValue` snapshot exists). Works with the existing `scan` API. Cost
= O(all live busy records per zone). Too slow for regular restart with
many zones, but correct and always available.

**Strategy 2 — journal scan replay (fast restart, primary path).**

Load the `ZoneValue` snapshot, then replay only the operations (Put /
Delete) written after `snapshot_slot`, in slot order, and apply them to
the snapshot bitmap. One scan per disk-group (or per disk) covers all
its zones — batch recovery, low overhead. **Requires a `JournalScan`
crow-kv RPC** (slot-range + key-prefix filter, returns ops in slot
order). This is the sole crow-kv extension diskdb needs. Fast because
compaction (strategy 3) keeps the uncompacted record set small.

**Strategy 3 — compaction (ongoing maintenance).**

Periodically (or when the free-record count for a zone exceeds a
threshold), merge free records into the `ZoneValue` bitmap and delete
the free records in one `batch_write`. This keeps the uncompacted
record set small, so strategy 2's replay is fast. Uses the existing
`scan` + `batch_write` API — no crow-kv extension needed. Batch by
`disk_id` prefix: one scan covers all zones on a disk (free records of
one zone are contiguous in the tree page). Only free records are
deleted; busy records for live blocks are untouched (busy records for
freed blocks were already deleted on free).

Compaction steps:
1. Scan free records for a zone by key prefix (the free records of one
   zone are contiguous in the crow-tree page, so this is efficient).
2. Merge the free records into the in-memory `ZoneValue` bitmap (clear
   the freed bits).
3. Write a new `ZoneValue` snapshot with `snapshot_slot =
   current_max_slot` (CRC32 checksum computed).
4. Delete the free records in one `batch_write`.

### How the strategies work together

- **Steady state**: allocate writes `BusyBlockValue` (and deletes any
  prior `FreeBlockKey` for that offset — re-allocate clears the free
  marker). Free deletes the `BusyBlockKey` and writes `FreeBlockValue`
  at `FreeBlockKey` in one `batch_write` (carries `previous_owner` for
  audit). Compaction (strategy 3) runs periodically, merging free
  records into `ZoneValue` and deleting the free records.
- **Restart**: load `ZoneValue` snapshot → journal scan (strategy 2)
  replays post-`snapshot_slot` operations in slot order → apply to
  bitmap. Fast because compaction kept the record set small.
- **On-demand (RPC/API)**: full scan (strategy 1) rebuilds the bitmap
  from all live records. Triggered by an operator or the §12 scanner
  for a consistency check or full rebuild — not in the common code flow.

### Crash-safety invariants

The in-memory bitmap is a **conservative over-estimate** of busy
blocks — it is never cleared on the free path. Free is persist-only
(delete `BusyBlockKey` + put `FreeBlockValue`); the bitmap bit stays
set until compaction clears it (§8). This means the bitmap may show a
block as busy when it is actually freed on disk — this is intentional
(data-safety principle: never show a block as free until compaction
has confirmed it from records). The durable state is the set of
`BusyBlockKey` / `FreeBlockKey` / `ZoneValue` records on the bound
data group. The **current-state rule** (a block is busy iff its
`BusyBlockKey` exists) holds at every crash point. The invariants:

- **Allocate ordering** — Phase 1 (bitmap CAS, set bit) happens before
  Phase 2 (`BusyBlockValue` persist). If diskdb crashes between Phase 1
  and Phase 2, the bit is set in memory but no `BusyBlockKey` exists on
  disk. On restart, strategy 1 full scan rebuilds the bitmap from
  records — the bit is clear (no busy record), so the block is
  correctly free. This is a **ghost-busy** (bit set in-memory, no
  record) that is self-correcting on restart; the §12 scanner also
  detects this drift during live operation.
- **Free = persist only** — the free is one `batch_write` (Delete
  `BusyBlockKey` + Put `FreeBlockValue` at `FreeBlockKey`), atomic
  within the data group via crow-kv paxos. The bitmap is **not**
  touched — the bit stays set, `used_count` is not decremented. The
  block is freed on disk but still shows busy in memory until
  compaction clears the bit and recomputes `used_count`. This is
  intentional: the bitmap is a conservative over-estimate that never
  shows freed space as available until compaction reconciles it. If
  diskdb crashes after the free persist, the bit is set in memory but
  no `BusyBlockKey` exists on disk. On restart, full scan sees no
  `BusyBlockKey` and clears the bit — the block is correctly free.
  Self-correcting; no drift.
- **Compaction reconciles the bitmap** — compaction (§8) is the sole
  mechanism for clearing freed bits in the bitmap. It reads free
  records, `range_clear`s the corresponding bits, recomputes
  `used_count = popcount`, writes a new `ZoneValue` snapshot, then
  deletes the free records. After compaction, the bitmap accurately
  reflects the durable state. Compaction runs on non-active zones
  (before they enter the active set via the preparatory thread, or
  periodically as a fallback) — no concurrent allocate.
- **Re-allocate after compaction** — once compaction has cleared the
  bit and deleted the `FreeBlockKey`, the block is available for
  re-allocation. The re-allocate is a normal allocate (CAS-set bit +
  persist `BusyBlockValue`). No stale free marker exists (compaction
  deleted it). The `FreeBlockKey` is cleared by compaction, not by
  the re-allocate.

The bitmap is never persisted on the allocate/free hot path — only the
`ZoneValue` snapshot (written by compaction) carries a serialized
bitmap, with `snapshot_slot` (replay start point) + `crc32` (integrity
check). The baseline `ZoneValue` (empty bitmap, `snapshot_slot = 0`)
is written during disk-add init (§10); all subsequent state changes
are `BusyBlockValue` / `FreeBlockValue` records until compaction
merges them into a fresh snapshot.

**Hot-path error handling:**

- **Allocate persist failure** (Phase 2 `batch_write` fails): rollback
  the bitmap (clear the bits set in Phase 1 via `zone.free`) and
  return error. No record was written, so the record set is consistent
  (no ghost). `zone.free` is the bitmap CAS-clear, used only by
  allocate rollback — not by the free path.
- **Free persist failure** (`batch_write` fails): the bitmap was
  never touched. Return error; the block is still busy on both disk
  and memory. The caller can retry safely.
- **Degraded mode** (group-0 / data-group unreachable): allocate/free
  RPCs return `Unavailable` before touching the bitmap. No partial
  state.

### Recovery and compaction engines

The recovery and compaction logic is implemented by two engines owned
by the diskdb server:

- **`RecoveryEngine`** — owns a `DataGroupClient`. Runs during startup
  and on ownership transfer. For each owned disk-group it builds an
  empty `Node` and calls `RecoveryEngine::recover_node`, which
  reconstructs each zone via `recover_zone`.
- **`CompactionEngine`** — owns a `DataGroupClient`. Runs as a
  background `compaction_loop` over the owned nodes in `NodeContainer`.

`recover_node` creates one `ZoneDisk` per disk, recovers each zone in
parallel (bounded by `recovery_concurrency`), adds the zones to the
disk, rebuilds the active zone set, and returns the reconstructed
`Node`.

`recover_zone` (strategy 2) is the primary restart path:

1. Load the latest `ZoneValue` for the zone with a single point get. If
   the CRC32 fails or no snapshot exists, proceed from an empty bitmap
   or fall back to strategy 1.
2. Initialize the bitmap from the snapshot, or from empty.
3. Issue two narrow `JournalScan`s from `snapshot_slot + 1` to the
   applied frontier: one over `BusyBlockKey` and one over `FreeBlockKey`
   prefixes. Merge the two op lists by slot. `JournalScan` supports
   slot-range, key-prefix, and transparent pagination; a
   `KV_ERROR_JOURNAL_SCAN_GC_GAP` response falls back to strategy 1.
4. Apply each op in slot order: `Put BusyBlockKey` sets the bit range
   using `BusyBlockValue.unit_count`; `Delete BusyBlockKey` clears the
   bit range, reading `unit_count` from the matching `FreeBlockValue`
   in the same batch when available; `Put`/`Delete FreeBlockKey` are
   no-ops for the bitmap; a newer `ZoneValue` written during replay
   restarts from that snapshot.
5. Return the rebuilt `Zone`, resetting the rotating cursor.

`rebuild_zone_bitmap_full_scan` (strategy 1) is the on-demand fallback:

1. Read all `BusyBlockKey`s for the zone via `read_zone_records` (key
   order equals `unit_offset` order). `FreeBlockKey`s are ignored for
   state determination.
2. Set the bit range for each busy record using `unit_count` and build
   the `Zone`.
3. Optionally write a fresh `ZoneValue` snapshot so the next restart can
   use strategy 2.
4. Return the rebuilt `Zone` plus derived stats.

`compact_zone` (strategy 3) is the background maintenance task:

1. Scan free records for the zone by key prefix.
2. Clear the freed bits in the in-memory bitmap.
3. Determine `snapshot_slot` from the data group's applied frontier.
4. Build a new `ZoneValue` from the merged bitmap and a CRC32 checksum.
5. Write the new `ZoneValue` snapshot before deleting the free records.
   The snapshot always reflects the freed state, so any crash point is
   safe.
6. Delete the free records in one `batch_write` and update the zone's
   `snapshot_slot` and `uncompacted_free_record_count`.

### Recovery triggers

- **Startup**: after the blocking `sync_once` fetches owned
  disk-groups, `RecoveryEngine::recover_node` runs for each one. The
  gRPC server does not accept RPCs until recovery completes.
- **Ownership transfer**: when `SyncLoop` detects a disk-group newly
  assigned to this instance, it checks whether `ZoneValue` snapshots
  already exist. If they do, the new owner runs
  `RecoveryEngine::recover_node` and discards any stale in-memory state;
  otherwise it initializes the disk-group via `disk_add_init`.

## 8. Allocation Algorithm

The algorithm follows the buzz-cpp reference
(`/cpp/buzz-cpp/src/app/buzz-disk-db`): bitmap-scan with a rotating
cursor, per-bit CAS, and disk-level zone rotation. The only
difference from buzz-cpp is persistence (CROW writes
`BusyBlockValue`/`FreeBlockValue` to the KV journal per-allocation
instead of saving the full bitmap to a local file).

### Zone-level allocate (sync, in-memory)

Bitmap-scan with per-bit CAS (no zone-level lock):
1. Check zone health is Healthy and `used_count < unit_capacity`.
2. Scan the bitmap from `last_pos_64` (rotating cursor), wrapping
   around. For each 64-bit word, use `countr_one` to find the first
   zero bit (hardware-optimized).
3. CAS-set the bit via `compare_exchange` on the 64-bit word. On CAS
   failure (another thread set the same bit), re-scan the same word.
   **CAS retry bound:** per-bit CAS is capped at 100 retries (config
   `cas_retry_limit`, default 100); on exhaustion, fall through to the
   next bit / word / zone. This prevents indefinite spinning under heavy
   contention. The `zone.allocate.retry.cms.bit` counter (§11) is
   incremented on each retry as the key operational signal for
   lock-free allocator contention.
4. For `unit_count > 1`: find `unit_count` consecutive zero bits
   (may span words), CAS-set each; on any CAS failure, clear bits
   already set in this attempt and continue scanning.
5. On success: update `last_pos_64`, increment `used_count`, return
   `AllocatedRange { unit_offset, unit_count }`. No zone-level state
   change — other threads can allocate concurrently.

### Disk-level allocate (sync) — rotating active-zone-set

1. Load the current `ActiveZoneContext` (RCU read — `Arc` clone, no
   lock held). This is a small set of `zone_rotate_count` zones.
2. Round-robin over the active set via `pos_v_zone_ctx.fetch_add(1)`.
3. Call `zone.allocate(unit_count)` on each zone. On success, return.
4. If all zones in the active set fail, call `rotate_active_zones()`:
   under a write lock, scan all zones from a rotating `pos_v_zone`
   start, pick the next `zone_rotate_count` allocatable zones, swap
   in a new `ActiveZoneContext` (RCU publish).
5. Retry with the new context (up to `zone_num / zone_rotate_count
   + 2` loops).

### Round-robin across disks within the named disk-group (sync)

The `AllocateBlocks` request carries `disk_group_id` (§3.2); allocate
is scoped to one disk-group — diskdb never round-robins across
disk-groups. Within that named disk-group, an `AtomicU64` iterator
(`pos_v_disk_ctx`) round-robins across disks: each allocation
increments and selects `iterator % num_disks` from the
`AllocateDiskContext` (RCU context of allocatable disks within the
named disk-group). Multi-block: `fetch_add(count)` distributes across
disks within the group. `exclude_disks` (anti-affinity, per-disk — skip
a disk that just failed) is applied within the named disk-group.
Multi-block uses one `batch_write` on the disk-group's bound paxos data
group; atomic within the group. No cross-group multi-block allocate in
v1.

### Two-phase async allocation

1. **Phase 1 (sync)**: bitmap-scan allocate (nanoseconds). Bits are
   set in `usage_bits` via per-bit CAS. No zone-level lock — other
   threads can allocate concurrently.
2. **Phase 2 (async)**: `.await` on crow-kv `Put` of the
   `BusyBlockValue` to the data group at `BusyBlockKey { disk_id,
   zone_idx, unit_offset }`. On success: return `Segment`. On
   failure: `zone.free(unit_offset, unit_count)` (clear the bits
   that were set in Phase 1), return error.

For multi-block: Phase 1 allocates all blocks (sync, may span
multiple zones/disks within one disk-group), Phase 2 uses one
`batch_write` per data group (one async round-trip per group).

### Free (persist-only, no bitmap mutation)

The free path is **persist-only**: delete `BusyBlockKey` + put
`FreeBlockValue` in one `batch_write`. The in-memory bitmap is **not**
touched — the bit stays set, `used_count` is not decremented. The
block is freed on disk but still shows busy in memory until compaction
clears the bit (§8 Compaction). This is the data-safety principle: the
bitmap is a conservative over-estimate that never shows freed space as
available until compaction reconciles it from records.

The free path increments `uncompacted_free_record_count` (an atomic
counter per zone) so compaction knows there is work to do. No `FreeBatch`,
no timer, no background flush loop — the free is a single durable
operation, and the bitmap reconciliation is deferred to compaction.

Free batching (grouping many frees into one `batch_write` per flush,
triggered by batch size — no timer) is an optimization for
high-free-throughput workloads, tracked as a future optimization (R79).
With persist-only free, batching does not change the bitmap contract —
the bitmap is never touched on free, regardless of batching.

### Zone rotation (internal) — compaction-on-rotation

The disk maintains a small active zone set (`zone_rotate_count`
zones, default 4). When all zones in the set are exhausted (no free
bits), the set rotates: a new set of allocatable zones is selected
from a rotating scan position and published via RCU. This spreads wear
across zones and avoids bias toward low-index zones.

**Compaction before rotation**: before a zone enters the active set,
it must be compacted — freed bits cleared, `used_count` recomputed,
snapshot written, free records deleted. This ensures the allocator
sees accurate free space in active zones. Without compaction, a
rotated-in zone would have stale set bits for freed-but-not-compacted
blocks, and the allocator would skip them (appearing fuller than it
is).

**Preparatory thread**: a separate background thread pre-compacts the
next batch of `zone_rotate_count` zones in advance, so rotation is
instant — the next active set is already compacted and ready to
publish via RCU. The preparatory thread runs continuously: it
identifies the next zones in the rotation order, compacts them, and
marks them as "ready." When rotation is triggered, the ready zones are
published immediately.

**Periodic compaction fallback**: a background compaction task
(`CompactionEngine`, 300s cadence or `uncompacted_free_record_count`
threshold) handles zones that don't rotate (low-churn zones, or
all-zones-active scenarios). This ensures freed space is eventually
reclaimed even without rotation.

Compaction runs on non-active zones only — no concurrent allocate.
The zone-level lock (§14) ensures compaction, scanner, and health
checks do not conflict with each other on the same zone.

## 9. State Machines

### Node / disk-group / disk HwStatus

Shared enum `HwStatus`: `Init`, `Up`, `Maintenance`, `Suspect`,
`Missing`, `Bad`, `Offline` (ordered by severity). Effective status =
`max(node, group, disk)` — a three-level check (node + disk-group +
disk). The reference impl checks two levels (node + disk); CROW adds
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

### Zone allocation state (derived, not a CAS state machine)

- **ZoneAllocationState** is a **derived** enum for reporting only:
  `Active` (`used_count == 0`), `Available` (0 < `used_count` <
  `unit_capacity`), `Full` (`used_count == unit_capacity`). There is
  no CAS state machine — allocation concurrency is handled by per-bit
  CAS on the usage bitmap. A zone is allocatable when it is Healthy
  (inheriting the disk's `HwStatus`) and `used_count < unit_capacity`.
  The `Full` state is transient: a free clears bits, making the zone
  `Available` again immediately (freed space is reused by the
  bitmap-scan allocator).

## 10. Disk Status Management

diskdb's first major component:
- **Sync with group 0** (fixed 10 s interval, same on success and
  failure — no error back-off in v1): fetch latest metadata; update
  group 0 first when local status changes (disk found, disk bad, disk
  added/removed). An **epoch/revision guard** skips a sync response
  whose epoch ≤ current (prevents stale overwrites). A disk/node
  **absent from a sync response** is transitioned to `Missing` (then to
  `Bad` after confirmation or `Up` if rediscovered) — this is how
  `Missing` is detected (§9).
- **Disk discovery + health probing**: discover local disks (config-
  driven for v1; `/dev` scan later); probe health (existence, size,
  basic read/write test). Source of truth for disk identity/capacity is
  group 0; live health is probed locally.
- **Disk failure detection + bad-disk handling**: on disk failure,
  transition to `Suspect`, update group 0. When a disk transitions to
  `Bad` (via `Missing → Bad` after confirmation, §9), its busy blocks
  are no longer readable. diskdb does **not** rebuild or relocate them
  inline on the sync path — relocation/rebuild is a follow-up
  requirement. The bad-disk handling on the sync path:
  - Mark the `ZoneDisk` and all its `Zone`s as `Bad`
    (`zone_state = Bad`; `allocatable()` returns `false`). No new
    allocates touch the disk. Free is also blocked —
    `allows_free(Bad)` is `false` (free allows `Up`/`Maintenance`/
    `Suspect` only, §9).
  - Scan the zone records for the bad disk (`read_zone_records` per
    zone, §7) and collect all live `BusyBlockValue`s — these are the
    impacted blocks. Each carries `owner_chunk` (the chunk that owns
    the allocation) so the caller / data-IO layer can be notified.
  - Emit the `disk.bad.impacted_blocks` gauge (§11) and log the
    hand-off. The collected list is handed to a future
    recovery/relocation path: the data-IO layer rebuilds from
    EC/mirror, or the owner is notified to re-allocate elsewhere.
  - The disk stays `Bad` — its records are read-only until an operator
    removes the disk or marks it `Up` after repair (which triggers
    strategy-1/2 recovery, §7).
- **Disk-add initialization flow**: the operator adds a disk in group 0
  (via `HardwareClient.add_disk` through the console), which writes
  `DiskValue` to group 0. On the next sync tick, diskdb fetches the
  updated `DiskValue` and sees a disk not yet in its in-memory state.
  diskdb then initializes the disk: creates the in-memory `ZoneDisk`
  with one `Zone` per zone (zone count = `capacity / zone_size`, last
  zone sized per
  §3.5's word-alignment rule), and writes baseline `ZoneValue` records
  (empty bitmap, `snapshot_slot = 0`) to the bound data group at
  `ZoneKey { disk_id, zone_index }` in one `batch_write`. These are the
  replay baselines (§7); subsequent allocates write `BusyBlockValue`
  records on top. Group 0 holds disk metadata only; zone records live
  on the bound data group.

**Follow-up — group-0 notify/watch:** a future follow-up adds a
watch/notify mechanism where group 0 pushes hw-status-change and
ownership-change notifications to registered diskdb endpoints (each
diskdb registers its endpoint on sync). This replaces polling for
status changes; polling stays as a safety net with an increased
interval. v1 ships with fixed-interval polling.

## 11. Space Metrics

diskdb's third major component. Detailed design — usage accessors,
`QueryCapacityStats` handler, per-disk counters, keepalive piggyback,
recalc verifier, reporting loop, proto, kv-client aggregation, and the
full `crow-diskdb-client` library — lives in
[`design-crow-diskdb-space-metrics.md`](design-crow-diskdb-space-metrics.md).
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

Metrics reuse `crow-common`'s metrics module. Per-disk hot-path
counters (atomics) flush into the crow-common registry at reporting
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
rotate) — matching crow-kv's convention. Gauges are derived snapshots
updated on the reporting interval, not hot-path writes. `degraded` and
`last_sync_age_secs` are the key health indicators for alerting.

## 12. Background Scanner

The scanner is a periodic consistency check — it detects and reports
live-state drift, catches record corruption early, and gives operators
visibility into cluster health during uptime. It is **not** a safety
mechanism (the free path is persist-only, §7, and the bitmap is a
conservative over-estimate — freed blocks stay busy until compaction);
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
  not touch the bitmap (§7), so "bit set, no `BusyBlockKey`" is
  **normal** for freed-but-not-compacted blocks (a `FreeBlockKey`
  exists). The scanner distinguishes:
  - **Real ghost-busy** (drift): bit set, no `BusyBlockKey`, no
    `FreeBlockKey` — the block was never freed and never allocated
    (crash between allocate Phase 1 and Phase 2, or a bug). Records
    are authoritative → block is free → safe to clear the bit.
  - **Normal uncompacted**: bit set, no `BusyBlockKey`, `FreeBlockKey`
    exists — the block was freed (persist-only) but compaction hasn't
    cleared the bit yet. This is **not drift** — it's the expected
    state. The scanner does not report it. (If the zone is not active
    and has a high `uncompacted_free_record_count`, the scanner may
    log a hint that compaction is lagging, but it's not a drift
    finding.)
  - **Ghost-free** (drift): bit clear, `BusyBlockKey` exists — should
    not happen in the persist-only model (free never clears bits, and
    only allocate/compaction touch the bitmap). If detected, it
    indicates a bug or hardware error. Records are authoritative →
    block is busy → set the bit back. Data may be written.
- **Bitmap drift detection** — in-memory `usage_bits` drifts from the
  record-derived bitmap; reload from records. The scanner uses
  strategy 1 (full scan rebuild, §7) as its rebuild mechanism — the
  same `RebuildZoneBitmap` RPC/API an operator would use for a
  consistency check or full rebuild.
- **Record integrity** — CRC check on zone snapshots and
  `BusyBlockValue` / `FreeBlockValue` records. Corrupt records are
  reported; the block is kept busy (key exists, data may be written);
  no auto-correction frees a block with a corrupt record.
- **Per-block state validation** — for busy blocks, cross-check
  `Segment.owner_chunk` against the `BusyBlockValue` in KV (ownership
  validation deferred from the free path, §14); for freed blocks, the
  `FreeBlockValue.previous_owner` is the audit trail.
- **Leak detection** — deferred (needs caller registries).
- **Zone skipping and locking** — the scanner skips zones in the
  disk's `active_zone_context` (the allocator is actively handing
  blocks from them — transient drift from the allocate Phase 1→2
  window is expected). For non-active zones, the scanner acquires the
  zone-level lock (§14) to coordinate with compaction and health
  checks. Skipped zones are checked on a later cycle when they rotate
  out of the active set (and have been compacted).
- **Compaction coordination** — compaction and the scanner share the
  zone-level lock (§14). They run on non-active zones only. The
  scanner skips zones locked by compaction (or waits briefly, since
  compaction is fast). Common methods on `DdbZone` (§14) encapsulate
  the lock + operation, used by both compaction and the scanner.

## 13. Crate Layout

```
lib/crow-protocol         (common protobuf for all CROW components; diskdb first)
app/crow-diskdb           (server: lib + binary — types, allocator, records, sync, gRPC + HTTP, CLI)
lib/crow-diskdb-client    (client library for callers: allocate/free/query; gRPC now, custom RPC later)
```

- **`lib/crow-protocol`** — protobuf definitions for diskdb gRPC services
  (allocate/free/query) + extension traits for proto types
  (`diskdb_type_util.rs`: `DiskIdExt`, `HwStatusExt`,
  `ZoneAllocationStateExt`, `ZoneValueExt`, `effective_status`).
  crow-kv's existing protos stay in `crow-kv` (unchanged). Later, when
  CROW adds its own RPC transport, the protobuf messages are reused.
- **`app/crow-diskdb`** — server crate (package name `crow-diskdb`).
  Combined library + binary: contains all diskdb logic (types, zone
  allocator, record persistence, scanner, ownership/sync, gRPC +
  HTTP handlers, CLI, config). The library target enables integration
  tests without spawning a separate process; the binary target
  (`crow-diskdb`) is the server executable. Proto types and their
  extension traits are re-exported from `crow_protocol`; internal types
  (metas, key layout, `AllocatedRange`, `ActiveZoneContext`, bitmap)
  are Rust structs.
- **`lib/crow-diskdb-client`** — client library for easy access to the server
  (allocate/free/query), mirroring `crow-kv-client`'s retry +
  topology-cache pattern. gRPC now; custom RPC + flatbuffer later.

### Dependencies

```
crow-common (metrics, logging, time)
  ↑
crow-kv-client (sole durable store — group 0 + data groups)
  ↑
crow-diskdb (server) ──depends──> crow-kv-client, crow-common, protocol
crow-diskdb-client  ──depends──> protocol, crow-kv-client (for group-0 discovery)

protocol ──> (no internal deps beyond prost/tonic)
```

## 14. Concurrency Model

All public and inter-module APIs are `async`. Runtime is `tokio`
(multi-threaded for production).

- No blocking calls in business-logic paths.
- **Allocate** — per-bit CAS on the usage bitmap
  (`compare_exchange` on 64-bit words). Multiple threads allocate from
  the same zone concurrently; no zone-level lock held across `.await`.
  CAS losers re-scan the same word or try the next zone; no thread
  blocking. Per-bit CAS is capped at 100 retries (§8), then falls
  through to the next bit / word / zone. Allocate runs only on active
  zones (in the `active_zone_context`).
- **Free** — persist-only (§7): one `batch_write` (Delete
  `BusyBlockKey` + Put `FreeBlockValue`). No bitmap touch, no
  `used_count` decrement, no zone-level lock. Free can run on any zone
  (active or not) without coordination — it only writes to the KV
  store. The bitmap is reconciled later by compaction.
- **Zone-level lock for non-allocate operations** — compaction,
  scanner, and health checks acquire a zone-level lock
  (`RwLock<()>` or `Mutex` on `DdbZone`) to coordinate with each other.
  These operations run on non-active zones only (no concurrent
  allocate), so the lock does not contend with the allocate hot path.
  Common methods on `DdbZone` encapsulate the lock + operation:
  - `compact_zone_inner()` — read free records, clear bits, recompute
    `used_count`, write snapshot, delete free records. Used by
    `CompactionEngine` (preparatory thread + periodic fallback).
  - `scan_zone_inner()` — replay journal, compare bitmap, verify
    records. Used by the scanner.
  - `health_check_zone_inner()` — verify zone records, CRC, snapshot
    integrity. Used by the health probe (R76).
  - Each method acquires the zone lock, performs the operation, and
    releases the lock. The lock is not held across `.await` on the KV
    client — the KV read is done before acquiring the lock, and the KV
    write is done after releasing the lock (the lock protects only the
    in-memory bitmap mutation).
- Disk-level active zone set uses RCU publish (`Arc` swap) for
  lock-free reads; rotation takes a brief write lock.
- **Free-side lookup structures** (RCU-published alongside the allocate
  context on add/remove/status-change):
  - disk-id → disk: a hash map (O(1) average). Used by free to find the
    in-memory disk from the `disk_id` in the `Segment`.
  - zone-index → zone: a vec indexed by zone-index (O(1) direct index).
    Used by free to find the in-memory zone from the `zone_index` in the
    `Segment`. (Lookup is for `uncompacted_free_record_count`
    increment; no bitmap mutation.)
  - **KV free path:** the `FreeBlockKey` is constructed directly from
    the `Segment` (no lookup needed). `owner_chunk` is carried in the
    `Segment` and becomes `FreeBlockValue.previous_owner` — no KV read
    on free in v1; the free is one `batch_write` (Delete `BusyBlockKey`
    + Put `FreeBlockKey`). Ownership validation is deferred to the §12
    scanner. If strict ownership validation is needed before free, a
    config toggle (`validate_owner_on_free`, default false) enables a
    KV read of the `BusyBlockValue` first (one paxos round-trip,
    doubles free latency).
- v1 free is immediate (no `FreeBatch`, no background flush loop). When
  free batching ships, `FreeBatch` will be protected by a `Mutex` held
  only for the append, not for the KV flush; the flush is triggered by
  batch size (no timer).
- Node-level `add_disk` / `remove_disk` acquire a write lock on the
  disk list; allocation/free acquire a read lock (concurrent with each
  other, exclusive with add/remove).

## 15. Non-Gaps (Good Fits with CROW)

These reference assumptions map cleanly onto CROW and need no design
work, just implementation:

- **Durability model**: aioss "metadb is sole durable store, no local
  WAL" → CROW "crow-kv WAL is sole durable log." diskdb's blind writes
  become durable via crow-kv's WAL flush. Identical model.
- **Consensus semantics**: aioss metadb = Raft; CROW = Multi-Paxos with
  parallel slots. For diskdb's usage (blind writes of zone records),
  both behave identically; parallel slots may even improve allocation
  throughput. No change needed.
- **Blind-ops persistence**: diskdb persists via blind Puts (no
  read-modify-write) — matches crow's blind-ops-only model exactly
  (§3.3).
- **Async runtime**: both use tokio multi-threaded; diskdb's two-phase
  async allocation (sync bitmap-scan claim + async KV persist) maps
  directly.

## 16. Configuration

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

## 17. Implementation Scope

The diskdb implementation is organized by functional scope. Each area
below covers a coherent slice of the component; together they make up
the full diskdb server.

- **Protocol + core types** — protobuf services, core types, record key
  layout, config validation, bitmap utilities, CRC integrity.
- **Group-0 sysdata schema + sync** — disk status management, group-0
  read/write, ownership/binding maps, heartbeat, disk-add initialization
  flow, keepalive usage summary.
- **Zone allocator + record persistence** — bitmap-scan allocator,
  rotating active-zone-set, busy/free records, two-phase async
  allocation, immediate free (no `FreeBatch`, no timer),
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
- **Group-0 notify/watch** — replace polling with watch/notify; requires
  crow-kv extension; polling stays as safety net.
- **Free batch** — size-threshold batching, no timer; graceful-shutdown
  drain + flush.

The design doc (this file and future sub-designs under
`doc/design/diskdb/`) is kept permanently — it is the root design for
the diskdb component area.

## 18. References

- CROW root KV design: `doc/design/kv/design-crow-kv.md`
- CROW WAL design: `doc/design/kv/design-crow-kv-wal.md`
- CROW metrics design: `doc/design/kv/design-crow-kv-observability.md`
- CROW console design: `doc/design/console/design-crow-console.md`
- Reference (another project): `/cjdata/cpp/aioss/server/diskdb/doc/design.md`
- Zone allocator reference: `/cpp/buzz-cpp/src/app/buzz-disk-db`
