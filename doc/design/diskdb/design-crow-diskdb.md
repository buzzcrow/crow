<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW diskdb — Design

This is the root design document for the diskdb component area. It
defines **what diskdb is**, **why key choices were made**, and **how the
component is structured**. Implementation-level detail lives in
sub-design docs (`design-crow-diskdb-*.md`); this doc covers decisions
and architecture only.

Scoping requirement: `doc/backlog/R69-diskdb-disk-block-allocator.md`.
Reference (another project, not a port):
`/cjdata/cpp/aioss/server/diskdb/doc/design.md`.

---

## 1. Overview

diskdb is a **distributed disk-block allocator** that runs on top of
CROW's KV cluster. It is a **lightweight, stateless server**: a diskdb
instance takes ownership of some disk-groups, manages the disks and
space inside them, and persists all durable state to CROW KV. It holds
no state that cannot be reconstructed from KV — on crash or restart it
rebuilds in-memory structures from the KV journal and group-0 metadata.

Multiple diskdb instances run across a cluster, each managing a subset
of disk-groups. diskdb provides fast block allocation/deallocation on
physical disks using per-zone append-only allocators with usage bitmaps
for O(1) block tracking. All state changes are durably persisted to
CROW KV before being acknowledged to callers.

diskdb **allocates** blocks; it does **not** perform data I/O. Callers
(a future object store, chunk service, etc.) write to the allocated
blocks themselves and tell diskdb when they are done (`active_zone`).

**Language:** Rust. **Runtime:** tokio (async everywhere).

**Core goals:**
- **Fast allocation** — per-zone append-only allocators with in-memory
  CAS serialization; no KV-level CAS on the hot path.
- **Durability via CROW KV** — the paxos journal is the sole durable
  store; diskdb has no local WAL and is stateless on disk.
- **Crash safety via journal replay** — busy/free records in the paxos
  slots are the source of truth; the bitmap is derived and rebuilt on
  restart.
- **Accurate space metrics** — per-disk / per-disk-group / per-zone
  capacity and busy/free, with a recalculation path to verify
  correctness.

**Design philosophy:** "diskdb is a thin, stateless client of crow-kv."
All consensus, replication, and durability are delegated to crow-kv.
diskdb's job is allocation policy, disk health, and space accounting —
nothing more.

## 2. Non-Goals (Design Envelope)

- **No data I/O.** diskdb allocates blocks; it does not read/write
  block contents. A future diskio-like component does data I/O.
- **No local WAL.** CROW KV's WAL is the sole durability mechanism.
- **No consensus code.** diskdb is a pure client of crow-kv.
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

The exact group-0 sysdata schema is designed in §5.

### 3.2 disk-group → paxos group binding via a table (not hash)

A disk-group's zone records all live on **one paxos data group** so
multi-block allocation can use a single `batch_write` (atomic within a
group). The disk-group → paxos-group mapping is a **bind table stored in
group 0**, not a hash. A table (vs hash) enables dynamic scaling: new
paxos data groups can be added and disk-groups rebound without
rehashing the whole keyspace.

### 3.3 No CAS needed; exclusive ownership

Each disk-group is owned by exactly one diskdb instance at a time (map
in group 0). A zone in a disk-group therefore has a single writer — no
KV-level CAS is required. A blind `Put` of the zone record is enough.
The in-memory `ZoneAllocationState` CAS stays in diskdb (it serializes
concurrent threads within one instance, not across instances).

### 3.4 Journal is the source of truth; bitmap is derived

For each allocate or free, diskdb **cannot** update the full zone
bitmap in KV — that would be a large journal write for the paxos group
on every block. Instead, each allocate/free appends a small **busy/free
record** to the paxos slots. The bitmap and `allocate_pos` are
**derived** from the journal — never written directly as a full bitmap
on the hot path.

The full `ZoneRecord` is a **compacted snapshot** written periodically.
Free records are eventually **deleted and merged** into the snapshot;
since deletes span any disk and any zone, the merge is done by
**batch** (collect expired free records across zones, write a new
snapshot, delete the old free records in one `batch_write`).

On crash/restart, diskdb reconstructs the in-memory zone state by
replaying the busy/free journal from the last snapshot forward. This
mirrors how crow-kv's own WAL replays into the engine.

**Open question (design time):** whether the diskdb client needs KV
slot info (which slot its write landed in) for correct replay, or
whether the replay can scan the journal by key prefix without slot
info. The latter is simpler (no slot feedback from crow-kv) but
requires a scan; the former is faster but needs a crow-kv extension.

### 3.5 Zone is a logical concept; sizes may vary

Not all zones on a disk must be the same size — the last zone may be
smaller (disk capacity is rarely an exact multiple of the zone size).
Zone is a **logical concept** defined for easier implementation; it can
later adopt to native zoned-namespace SSD or SMR HDD zone APIs, but
currently no such devices are targeted — the zone is a rough mapping.

### 3.6 Common protocol crate; gRPC now, custom RPC later

A new `lib/protocol` crate holds protobuf definitions for all CROW
components. diskdb uses it first; crow-kv's existing protos stay where
they are (unchanged). Later, when CROW adds its own RPC transport, the
protobuf messages are reused and only the transport changes (custom RPC
+ flatbuffer is a future direction).

### 3.7 Reuse crow-common metrics

diskdb reuses `crow-common`'s metrics module (no parallel metrics
system). Per-disk atomic counters stay as hot-path counters that flush
into the crow-common registry at reporting intervals.

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
   │  │ nodes        │     │ zone journals (busy/free records)│     │
   │  │ disks        │     │ zone snapshots                   │     │
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
- **data groups (group-1..N)** — paxos groups holding zone journals and
  snapshots. Each disk-group is bound to one data group.

## 5. Group-0 Sysdata Schema

Group 0 stores the diskdb subsystem metadata as KV entries. The schema
reserves room for the physical hierarchy (data-center, rack) even
though v1 ships flat (node list only).

### Key layout

```
# Physical hierarchy (v1: flat; DC/rack reserved)
/diskdb/dc/{dc_id}/meta                         -> DataCenterMeta
/diskdb/dc/{dc_id}/rack/{rack_id}/meta          -> RackMeta

# Node metadata
/diskdb/node/{node_uuid}/meta                   -> NodeMeta

# Disk-group metadata (logical, belongs to one node)
/diskdb/node/{node_uuid}/dg/{dg_index}/meta     -> DiskGroupMeta

# Disk metadata (physical, belongs to one node)
/diskdb/node/{node_uuid}/disk/{disk_uuid}/meta  -> DiskMeta

# Maps (the two core tables)
/diskdb/map/owner/{dg_id}                       -> {instance_id, lease_expiry}
/diskdb/map/bind/{dg_id}                        -> {store_id, group_id}

# diskdb instance registry
/diskdb/instance/{instance_id}                  -> InstanceMeta
```

### Core types (summary)

- **NodeMeta** — `node_uuid`, `dc_id` (reserved), `rack_id` (reserved),
  `status` (Init/Online/Offline/Maintenance/TempFailure).
- **DiskGroupMeta** — `dg_id` (`"{node_uuid}-{index}"`), `node_uuid`,
  `index`, `status`, `disk_uuids` (list of member disks).
- **DiskMeta** — `disk_uuid`, `node_uuid`, `disk_type` (HDD/SSD/SMR),
  `capacity_bytes`, `zone_size_bytes`, `block_size_bytes`,
  `zone_count`, `status`.
- **InstanceMeta** — `instance_id`, `endpoint` (gRPC + HTTP),
  `owned_dg_ids` (list), `last_heartbeat_ms`.

**Note**: zones are NOT stored in group 0. A zone is created when its
disk is added, and its state (journal + snapshot) lives on the
disk-group's bound data group.

### Map updates

- **Ownership map** — written by the operator (or a future coordinator)
  to assign a disk-group to a diskdb instance. Read by diskdb on sync.
- **Binding map** — written by the operator to bind a disk-group to a
  paxos data group. Read by diskdb to route zone-journal writes.
- **Instance registry** — written by each diskdb instance on heartbeat;
  used by the console and other components to discover diskdb
  endpoints.

## 6. Hierarchy

CROW today has no data-center or rack concept. The physical hierarchy
and the logical disk-group layer:

- **Physical**: data-center → rack → node → disk. These are real
  hardware containers. CROW currently lacks data-center and rack; v1
  ships flat (node list only), but the group-0 schema reserves room.
  Adding DC/rack is a follow-up.
- **Logical**: disk-group sits between node and disk. A disk-group
  belongs to exactly one node; a node can have multiple disk-groups. A
  disk-group is the unit of ownership (assigned to one diskdb instance)
  and the unit of paxos-group binding (a disk-group's zone records all
  live on one paxos data group).

```
Data Center → Rack → Node (uuid) → Disk-Group ("{node_uuid}-{index}")  [logical]
  → Disk (uuid, HDD/SSD/SMR) → Zone (index, ~16 GB) → Disk-Block (1 MB aligned)
```

## 7. Zone Journal and Crash Recovery

### Journal record format

Each allocate/free appends a small record to the paxos data group's
journal (via blind `Put`):

```
# Busy record (allocate)
/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/busy/{slot}  -> BusyRecord

# Free record (free)
/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/free/{slot}  -> FreeRecord

# Zone snapshot (compacted, periodic)
/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/snapshot     -> ZoneSnapshot
```

- **BusyRecord** — `{zone_offset, size, tag}`. Small (≤ 32 bytes
  serialized).
- **FreeRecord** — `{zone_offset, size, tag}`. Same shape as BusyRecord.
- **ZoneSnapshot** — `{allocate_pos, usage_bitmap, zone_state,
  snapshot_slot}`. The compacted full-zone state at a point in time.

### Replay algorithm

On startup / ownership transfer, for each zone:
1. Load the latest `ZoneSnapshot` (if any).
2. Scan the journal by key prefix
   `/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/` for all
   busy/free records with slot > `snapshot_slot`.
3. Apply busy records (set bits, advance `allocate_pos`) and free
   records (clear bits) to the snapshot's bitmap.
4. Rebuild the in-memory zone: `allocate_pos`, `usage_bits`,
  `zone_state`, active deque membership.

**Open question (design time):** whether the replay scans by key prefix
(simple, no crow-kv change) or uses slot info from the client (faster,
needs a crow-kv extension). The prefix-scan approach is the default;
slot-info is a future optimization.

### Snapshot compaction

Periodically (or when the journal for a zone exceeds a threshold),
diskdb compacts:
1. Compute the current bitmap + `allocate_pos` from the in-memory state.
2. Write a new `ZoneSnapshot` with `snapshot_slot = current_max_slot`.
3. Delete all busy/free records with slot ≤ `snapshot_slot` in one
   `batch_write` (the batch merge — deletes span any disk/zone, so
   multiple zones' expired records are collected and deleted together).

## 8. Allocation Algorithm

### Zone-level claim (sync, in-memory)

Append-only with CAS serialization:
1. CAS `ZoneAllocationState` Active → Busy (only one thread wins).
2. Validate size alignment; check `allocate_pos + count ≤ max`.
3. Set bits in `usage_bits`; advance `allocate_pos`.
4. Return `Segment` + `ClaimSnapshot`. Zone stays Busy until
   `release()` or `rollback()`.

### Disk-level claim (sync)

1. Poll a zone from the disk's `active_zones` deque.
2. If empty, scan for an allocatable zone (random start).
3. Claim from the zone; on success return `(ZoneRef, Segment,
   ClaimSnapshot)`.

### Node-level round-robin (sync)

`AtomicU32` iterator; each allocation increments and selects
`iterator % num_disks`. Multi-block: `fetch_add(count)` distributes
across disks.

### Two-phase async allocation

1. **Phase 1 (sync)**: CAS claim (nanoseconds). Zone is Busy.
2. **Phase 2 (async)**: `.await` on crow-kv `Put` of the BusyRecord to
   the data group. On success: `release()` (Busy → Active), return
   `Segment`. On failure: `rollback()` (undo + Busy → Active), return
   error.

For multi-block: Phase 1 claims all zones (sync), Phase 2 uses one
`batch_write` (one async round-trip).

### Free (batched)

1. Clear bits in local `usage_bits`; append to `FreeBatch`; return
   immediately.
2. Background loop flushes `FreeBatch` to the data group periodically
   (default 500 ms / 256 entries) as FreeRecords.

### active_zone

Re-adds a zone to the disk's active deque after the caller finishes
writing. Prevents over-committing write-once zones.

## 9. State Machines

### Node / disk-group / disk Status

Shared enum: `Online`, `Init`, `Maintenance`, `TempFailure`, `Offline`
(ordered by restrictiveness). Effective status = `max(node, group,
disk)`. Allocations require `Online`; frees allow `Online` or
`Maintenance` or `TempFailure`.

Transitions:
- Init → {Online, Offline, Maintenance} on startup (load from group 0).
- Online → TempFailure (3 missed syncs).
- Online → Offline / Maintenance (operator).
- TempFailure → Online (sync recovers) or → Offline (15 min elapsed).
- Offline ↔ Maintenance (operator).
- Offline → Online (operator).

### Zone dual state machine

- **ZoneState** (health): Healthy, Missing, Bad. Set by the local
  health probe.
- **ZoneAllocationState** (lifecycle): Active, Busy, Error, Full.
  Transitions via CAS. A zone is allocatable only when Healthy AND
  Active.

## 10. Disk Status Management

diskdb's first major component:
- **Sync with group 0** (13 s default): fetch latest metadata; update
  group 0 first when local status changes (disk found, disk bad, disk
  added/removed).
- **Disk discovery + health probing**: discover local disks (config-
  driven for v1; `/dev` scan later); probe health (existence, size,
  basic read/write test). Source of truth for disk identity/capacity is
  group 0; live health is probed locally.
- **Disk failure detection + recovery flow** (new — does not exist
  yet): on disk failure, transition to TempFailure, update group 0,
  trigger recovery. The recovery flow is designed in a follow-up
  requirement.

**Open question (design time):** a zookeeper-like notify/watch where
group 0 pushes refresh notifications to registered diskdb endpoints.
Each diskdb registers its endpoint on sync; group 0 notifies on change.
This needs a design review of how a paxos group can support watch/notify
(not a native crow-kv feature today) — may defer to a follow-up if the
polling cost is acceptable.

## 11. Space Metrics

diskdb's third major component:
- **Per-disk** — capacity, busy, free, active zone count.
- **Per-disk-group** — aggregated from disks.
- **Per-zone** — dive into busy/free blocks (for the console zone
  visualization).
- **Accurate + recalculation** — statistics are derived from the
  in-memory bitmap (which is derived from the journal). A recalculation
  path replays the journal to verify the bitmap matches the derived
  statistics, detecting drift.

Metrics reuse `crow-common`'s metrics module. Per-disk hot-path
counters (atomics) flush into the crow-common registry at reporting
intervals. Gauge counters cover capacity and busy/free for later UI
display.

## 12. Background Scanner

- **Ghost-allocation detection** — allocated in KV journal but freed
  locally (crash before free batch flush). Detected by comparing
  in-memory state against the journal.
- **allocate_pos drift detection** — in-memory position drifts from
  the journal-derived position; reload from journal.
- **Record integrity** — CRC check on zone snapshots.
- **Leak detection** — deferred (needs caller registries).

## 13. Crate Layout

```
lib/protocol              (common protobuf for all CROW components; diskdb first)
lib/crow-diskdb-client    (client library for callers: allocate/free/query; gRPC now, custom RPC later)
app/crow-diskdb           (server binary: CLI, config, gRPC + HTTP, background loops)
```

- **`lib/protocol`** — protobuf definitions for diskdb gRPC services
  (allocate/free/query). crow-kv's existing protos stay in `crow-kv`
  (unchanged). Later, when CROW adds its own RPC transport, the
  protobuf messages are reused.
- **`lib/crow-diskdb-client`** — client library for easy access to the server
  (allocate/free/query), mirroring `crow-kv-client`'s retry +
  topology-cache pattern. gRPC now; custom RPC + flatbuffer later.
- **`app/crow-diskdb`** — server binary (package name `crow-diskdb`).
  Contains all diskdb logic: types, zone allocator, journal persistence,
  scanner, ownership/sync, gRPC + HTTP handlers, CLI, config.

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
- Per-zone CAS serialization (`AtomicU8`) — not a Mutex held across
  `.await`. Losers try another zone; no thread blocking.
- `FreeBatch` protected by a `Mutex`; held only for the append, not for
  the KV flush.
- Node-level `add_disk` / `remove_disk` acquire a write lock on the
  disk list; allocation/free acquire a read lock (concurrent with each
  other, exclusive with add/remove).

## 15. Implementation Split

The diskdb implementation is split into follow-up requirements by
functional scope:

- **R70** — Project skeleton + protocol + types (this R69 sets up the
  skeleton; R70 fills in the protobuf services, core types, config).
- **R71** — Group-0 sysdata schema + sync (disk status management,
  group-0 read/write, ownership/binding maps, heartbeat).
- **R72** — Zone allocator + journal persistence (zone CAS, active
  deque, busy/free journal, two-phase async allocation, free batch).
- **R73** — Crash recovery + snapshot compaction (journal replay,
  snapshot write, batch merge of expired records).
- **R74** — Space metrics + query API (per-disk/group/zone metrics,
  recalculation path, `query_disk_usage`).
- **R75** — Background scanner (ghost/drift/integrity detection).
- **R76** — Disk discovery + health probing (config-driven disk list,
  health probe, disk failure detection).
- **R77** — Console + CLI integration (follow-up: disk/disk-group
  management UI, zone busy/free visualization, CLI command design).

Each requirement follows the `/implement-requirement` workflow. The
design doc (this file and future sub-designs under
`doc/design/diskdb/`) is kept permanently — it is the root design for
the diskdb component area.

## 16. References

- CROW root KV design: `doc/design/kv/design-crow-kv.md`
- CROW WAL design: `doc/design/kv/design-crow-kv-wal.md`
- CROW metrics design: `doc/design/kv/design-crow-kv-observability.md`
- CROW console design: `doc/design/console/design-crow-console.md`
- Scoping requirement: `doc/backlog/R69-diskdb-disk-block-allocator.md`
- Reference (another project): `/cjdata/cpp/aioss/server/diskdb/doc/design.md`
