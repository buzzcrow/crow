<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R69: diskdb — Distributed Disk-Block Allocator on CROW (Scope)

Redesign and implement a distributed disk-block allocator as a new
CROW component, referencing the original design in another project
(`/cjdata/cpp/aioss/server/diskdb/doc/design.md`). This is **not a
port** — the original persists to aioss's metadb (multi-raft, 256
partitions, partition-0 control plane, a separate `diskio` service);
diskdb-on-crow is a fresh design that runs on top of CROW's KV cluster
(Multi-Paxos, parallel slots, system group for topology) so the whole
storage stack lives in one codebase.

This is a **scoping + foundation requirement**: it defines the full
scope of diskdb, its functions, its high-level dependencies, and the
design decisions. R69 produces the high-level design doc
(`doc/design/diskdb/`), splits the implementation into follow-up
R-numbers by functional scope, and sets up the project skeleton
(`lib/protocol`, `app/crow-diskdb`, client) with correct
dependencies and a clean build. The follow-up R-numbers fill in the
actual functionality one by one.

**Reference**: `/cjdata/cpp/aioss/server/diskdb/doc/design.md`.

---

## What diskdb Is

diskdb is a **distributed disk-block allocator**. It is a **lightweight,
stateless server**: a diskdb instance takes ownership of some
disk-groups, manages the disks and space inside them, and persists all
durable state to CROW KV. It holds no state that cannot be reconstructed
from KV — on crash or restart it rebuilds in-memory structures from the
KV journal and group-0 metadata.

Multiple diskdb instances run across a cluster, each managing a subset
of **disk-groups**. It provides fast block allocation/deallocation on
physical disks using per-zone append-only allocators with usage bitmaps
for O(1) block tracking. All state changes are durably persisted to
CROW KV before being acknowledged to callers.

ai-todo: one design challedge is for a allocate or free, we can not update bitmap to KV, because their will be a journal write for the group.But for each allocate or free, we will add busy block or free block to the journal. We can use these info to track the bitmap and restore the bitmap status when restart. It require the client get kv slot info. Not sure if it's a right design.  We can delay to design time to discuss it.  The free block will finaly be deleted and merge to zonerecord. Because the delete can be happend on any disk and any zone, so we can merge them by batch. It's a more complex design. 

A core design challenge: for each allocate or free, we **cannot** update
the full zone bitmap in KV on every operation — that would be a large
journal write for the paxos group on every block. Instead, each
allocate/free appends a small **busy/free record** to the KV journal
(the paxos slots). The bitmap is **derived** from the journal: on
restart, diskdb replays the busy/free records to reconstruct the
in-memory bitmap and `allocate_pos`. This requires the diskdb client to
obtain KV slot info (which slot its write landed in) for correct
replay — whether this is the right design is an open question for
design time. Free records are eventually **deleted and merged** into a
compacted `ZoneRecord` snapshot; since deletes can happen on any disk
and any zone, the merge is done by **batch**. This is a more complex
design than the reference's "Put the full ZoneRecord on every
allocate" — see D4.

diskdb **allocates** blocks; it does **not** perform data I/O. Callers
(a future object store, chunk service, etc.) write to the allocated
blocks themselves and tell diskdb when they are done (`active_zone`).

### Hierarchy

CROW today has no data-center or rack concept. The physical hierarchy
and the logical disk-group layer need to be reviewed and added:

- **Physical**: data-center → rack → node → disk. These are real
  hardware containers. CROW currently lacks data-center and rack; adding
  them is part of this scope (or a prerequisite requirement to be
  decided during alignment).
- **Logical**: disk-group sits between node and disk. A disk-group
  belongs to exactly one node; a node can have multiple disk-groups. A
  disk-group is the unit of ownership (assigned to one diskdb instance)
  and the unit of paxos-group binding (a disk-group's zone records all
  live on one paxos data group).

```
Data Center → Rack → Node (uuid) → Disk-Group ("{node_uuid}-{index}")  [logical]
  → Disk (uuid, HDD/SSD/SMR) → Zone (index, 16 GB) → Disk-Block (1 MB aligned)
```

### Key properties

- Block size: 1 MB default (configurable 512 KB – 2 MB).
- Zone size: 16 GB default (configurable). Not all zones on a disk must
  be the same size — the last zone may be smaller (disk capacity is
  rarely an exact multiple of the zone size). Zone is a **logical
  concept** defined for easier implementation; it can later adopt to
  native zoned-namespace SSD or SMR HDD zone APIs, but currently no
  such devices are targeted — the zone is a rough mapping.
ai-todo: we are not force all zone has same size, depends on complecity, we allow last zone has smaller size.  Zone is also a logical concept. We can define it for easier implementation. The zone can adopt to SSD open their zone API or SMR HDD. but currently we do not have such device, just map a rough concept to it. 
- Zone allocation model: append-only with monotonic `allocate_pos`.
- Zone serialization: CAS on `ZoneAllocationState` (Active → Busy →
  Active) — in-memory `AtomicU8`, **not** a KV CAS.
- Active zone pool: lock-free `ConcurrentDeque`; zones polled for alloc,
  re-added via `active_zone()` by the caller after write completes.
- Disk selection: round-robin across Active disks with `AtomicU32`
  counter; multi-block distributes round-robin across disks. 
- Allocation persist: synchronous — zone state written to CROW KV
  before returning (two-phase: sync CAS claim, async KV persist).
- Free persist: batched — accumulated locally, flushed to CROW KV
  periodically (default 500 ms / 256 entries).
- Durability: CROW KV (Paxos-replicated, WAL) is the sole durable
  store; diskdb has **no local WAL** and is stateless on disk.
- Disk states: Init, Online, Offline, Maintenance, TempFailure.
- Zone health states: Healthy, Missing, Bad.
- Zone allocation states: Active, Busy, Error, Full.
- Disk types: ZoneBlockDisk (HDD), ZoneSSDDisk (SSD), ZoneSMRDisk (SMR)
  via trait.
- Heartbeat/sync interval: 13 s default.
- Ownership unit: Disk-group (map persisted in group 0).
- Negative disk hints: callers can exclude specific disks from
  multi-block allocation (spread replicas across failure domains).

---

## Major Components

diskdb has three major components:

1. **Disk status management** — talks with group 0 to sync status;
   handles status changes (new disk found, disk becomes bad); triggers
   disk recovery on failure. The disk-recovery flow does not exist yet
   and must be designed as part of this scope.
2. **Block allocate/free** — provides allocate/free disk-block APIs;
   writes durable state to the disk-group's bound paxos data group
   (group-x).
3. **Space metrics** — per-disk and per-disk-group space metrics, with
   the ability to dive into per-zone detail (busy/free blocks). Space
   statistics are important: we prefer **accurate** statistics with a
   way to **recalculate** the value (rebuild from the journal) to verify
   correctness and detect drift.

   ai-todo: the space statistics is important. We prefer a accurate statistics and have way can recaculate the value. 

---

## Functions (Scope)

### In scope for diskdb (split across follow-up requirements)

- **Allocation APIs**
  - `allocate_block` — single block from a disk-group; round-robin disk
    selection, poll active zone, advance `allocate_pos`; returns
    `Segment`.
  - `allocate_blocks` — multiple blocks; round-robin across disks;
    returns `Vec<Segment>`.
  - `condition_allocate_blocks` — multi-block with negative disk hint
    (exclude specific disk UUIDs).
- **Free API**
  - `free_block` — clear usage bit, append to `FreeBatch`, return
    immediately; background loop flushes to CROW KV.
- **Zone lifecycle API**
  - `active_zone` — re-add a zone to the disk's active deque after the
    caller finishes writing (prevents over-committing write-once zones).
- **Query API**
  - `query_disk_usage` — capacity/usage metrics for all owned nodes or a
    specific node; dive into per-zone busy/free.
- **Persistence to CROW KV**
  - Zone state persisted to the disk-group's bound paxos data group;
    allocation = synchronous write, free = batched flush. Zone records
    are large and high-frequency → see "Zone record journal" below.
- **Disk status management + group-0 sync**
  - Periodic sync (13 s) to fetch the latest metadata from group 0;
    update group 0 first when any local status changes (disk found, disk
    bad, disk added/removed). Consider a notify/watch mechanism
    (zookeeper-like) so group 0 can push refresh notifications to
    registered diskdb endpoints instead of polling only — to be
    reviewed during design.
  - Disk failure detection and recovery flow (new — does not exist
    yet).
- **State machines**
  - Node/disk-group/disk `Status` (Init/Online/Offline/Maintenance/
    TempFailure) with effective-status = `max(node, group, disk)`.
  - Zone dual state machine (`ZoneState` health × `ZoneAllocationState`
    lifecycle).
- **Disk discovery + health probing**
  - Discover local disks and probe health. CROW has no diskio service;
    diskdb does its own minimal discovery + probe. Source of truth for
    disk identity/capacity is group 0; live health is probed locally.
- **Background scanner**
  - Ghost-allocation detection (allocated in KV but freed locally),
    allocate_pos drift detection, record integrity (CRC).
  - Leak detection requires a caller-registry plugin interface —
    **deferred** (no callers exist yet).
- **Metrics**
  - Reuse `crow-common` metrics. Design diskdb-specific metrics covering
    the important flows (allocate, free, sync, recovery) plus gauge
    counters for capacity/busy-free so the internal status and perf
    issues are visible and the data can later be displayed in the UI.
- **Recovery (crash safety)**
  - On startup / ownership transfer: rebuild in-memory zone state from
    the KV journal (see "Zone record journal"). diskdb is stateless on
    disk — all reconstructable from KV.

### Out of scope (future requirements)

- **Data I/O service** (aioss `diskio` equivalent) — reading/writing
  block contents. diskdb only allocates; a future diskio-like component
  does data I/O.
- **Object store / chunk service / metadata service** — future
  components that consume diskdb allocations.
- **Native SMR / zoned-namespace SSD support** — start with conventional
  HDD/SSD (`ZoneBlockDisk`); SMR/SSD trait variants are stubbed.
- **Leak-detection scanner** — needs caller registries that do not
  exist yet; ship the scanner scaffolding + ghost/drift/integrity only.
- **Rack/data-center aggregated capacity dashboards** — per-disk and
  per-node metrics first; higher-level aggregation later.
- **Console UI for diskdb** — minimal HTTP management API first; full
  console integration (web + CLI) as a follow-up.

---

## High-Level Dependencies

### What diskdb depends on from CROW

- **`crow-kv-client`** — sole durable store. diskdb persists zone state
  to paxos data groups and metadata to group 0 via `put` /
  `batch_write` / `get` / `scan`. Replaces aioss's `metadb-client` +
  `DiskdbServiceClient`.
- **CROW system group (store 0, group 0)** — centralized **sysdata**
  home. Group 0 stores the basic info: nodes, disks, disk-groups, and
  the two maps (disk-group → diskdb instance, disk-group → paxos data
  group). diskdb fetches all metadata from group 0; other daemons
  (and diskdb itself for its own managed state) update group 0. **Zones
  are NOT maintained in group 0** — a zone belongs to a disk, is
  created when the disk is added, and is maintained as a separate zone
  record on the disk-group's bound paxos data group.
- **`crow-common`** — metrics registry, logging, time helpers. diskdb
  reuses crow-common's metrics module (no parallel metrics system).
- **CROW HTTP management API pattern (axum)** — for diskdb's management
  endpoints (add/remove disk, status, capacity). All managed info is
  also written to group 0.

### What diskdb does NOT depend on

- **No `diskio`** — CROW has no node-local disk I/O service. diskdb does
  its own minimal disk discovery + health probing. Data I/O is a future,
  separate component.
- **No local WAL** — CROW KV's WAL is the sole durability mechanism.
  diskdb's blind writes become durable via crow-kv's WAL flush. Crash
  safety is analyzed below ("Zone record journal").
- **No consensus code** — diskdb is a pure client of crow-kv; it does
  not implement Paxos/Raft. It is lightweight and stateless.

### New crates (proposed layout)

- **`lib/protocol`** — a new common protocol crate for all CROW
  components, protobuf first. Used by diskdb first; **crow-kv's existing
  protos stay where they are (unchanged)**. Later, when CROW adds its
  own RPC transport, the protobuf messages are reused and only the
  transport changes (custom RPC + flatbuffer is a future direction).
- **`app/crow-diskdb`** — the server binary (package name `crow-diskdb`;
  CLI, config, wiring, gRPC + HTTP servers, background loops).
  Self-contained — no separate server library crate.
- **`lib/crow-diskdb-client`** — a client library for easy access to the
  server (allocate/free/query), mirroring `crow-kv-client`'s retry +
  topology-cache pattern. gRPC now; custom RPC later.

---

## Design Decisions to Align

These are the decisions that must be agreed before the design doc is
written and the work is split. Each is stated as a position with
rationale; items still open for review are marked.

### D1 — Group 0 is the centralized sysdata store

Group 0 holds the basic metadata for the diskdb subsystem: nodes,
disks, disk-groups, and the two maps (disk-group → diskdb instance,
disk-group → paxos data group). diskdb fetches all metadata from
group 0 on startup and on sync. Any local status change (disk found,
disk bad, disk added/removed) is written to group 0 first, then
reflected locally.

**Open for review**: the exact info schema for group 0 must be
designed first — enumerate every field diskdb and other components
need (node identity, disk identity/capacity/type, disk-group
membership, ownership map, binding map, status) and lay out the KV
keys. This is the first design task after alignment.

**Open for review (notify mechanism)**: today diskdb would poll group
0 every 13 s. Consider a zookeeper-like notify/watch where group 0
pushes refresh notifications to registered diskdb endpoints. Each
diskdb registers its endpoint on sync; group 0 notifies on change.
This needs a design review of how a paxos group can support
watch/notify (not a native crow-kv feature today) — may defer to a
follow-up if the polling cost is acceptable.

### D2 — disk-group → paxos group binding via a table (not hash)

A disk-group's zone records all live on **one paxos data group** so
multi-block allocation can use a single `batch_write` (atomic within a
group). The disk-group → paxos-group mapping is a **bind table stored
in group 0**, not a hash. A table (vs hash) enables dynamic scaling:
new paxos data groups can be added and disk-groups rebound without
rehashing the whole keyspace.

**Open for review**: enumerate any case this approach cannot handle
(e.g. a disk-group growing beyond one group's capacity — unlikely
given zone-record sizing, but confirm). Decide how many data groups to
start with and whether they are dedicated to diskdb or shared with
user-data groups.

### D3 — No CAS needed; exclusive ownership

Each disk-group is owned by exactly one diskdb instance at a time (map
in group 0). A zone in a disk-group therefore has a single writer — no
KV-level CAS is required. A blind `Put` of the zone record is enough.
The in-memory `ZoneAllocationState` CAS stays in diskdb (it serializes
concurrent threads within one instance, not across instances).

### D4 — Zone record journal (crash safety + high-frequency writes)

Zone records are large (usage bitmap + allocate_pos + state) and
allocation is high-frequency, so writing the full zone record to KV on
every allocate is too expensive. Instead, the **journal is the source
of truth**: each allocate appends a small "busy" record (zone, offset,
size, tag) to the paxos slots; each free appends a "free" record. The
bitmap and `allocate_pos` are **derived** from the journal — never
written directly as a full bitmap on the hot path.

The full `ZoneRecord` is a **compacted snapshot** written periodically.
Free records are eventually **deleted and merged** into the snapshot;
since deletes span any disk and any zone, the merge is done by
**batch** (collect expired free records across zones, write a new
snapshot, delete the old free records in one `batch_write`).

On crash/restart, diskdb reconstructs the in-memory zone state by
replaying the busy/free journal from the last snapshot forward. This
mirrors how crow-kv's own WAL replays into the engine. A free is
durable once its record is in the paxos log (the free batch flushes
free records to the journal, not full zone records).

**Open for review**: design the journal record format, the snapshot
frequency, and the replay algorithm. Confirm the busy/free records fit
crow-kv's blind-ops model (they are blind Puts keyed by zone + slot).
Decide whether the diskdb client needs KV slot info (which slot its
write landed in) for correct replay, or whether the replay can scan
the journal by key prefix without slot info. Decide whether the
snapshot is a single large value or a batch of per-block entries.

### D5 — Crate layout

- New `lib/protocol` (common protobuf, diskdb first, KV protos
  unchanged).
- `app/crow-diskdb` (self-contained server, no separate lib).
- A client for the server (gRPC now, custom RPC + flatbuffer later).

### D6 — Physical hierarchy (data-center, rack)

CROW has no data-center or rack concept today. data-center → rack →
node → disk are physical concepts; disk-group is a logical layer
between node and disk. Adding DC/rack is part of this scope or a
prerequisite requirement — to be decided during alignment. v1 can ship
flat (node list only) and add the physical hierarchy later, but the
group-0 schema should reserve room for it.

### D7 — Console + CLI integration (follow-up)

Console support is needed: manage disks in nodes and disk-groups (a
disk-group belongs to one node; a node can have multiple disk-groups);
show disk properties and basic zone capacity; special zone view shows
a busy/free chart (an array of blocks — green = free, blue = busy —
rendered as an image generated from usage data).

CLI tooling needs a design: crow-cli currently has KV commands; new
diskdb commands are needed. Decide whether to add a command layer
(e.g. `crow kv ...` / `crow diskdb ...` subcommands) or ship sub-wrapper
binaries (`crow-kv-cli`, `crow-diskdb-cli`) that internally share
`crow-cli`. This is a follow-up requirement; v1 ships the HTTP mgmt
API only.

### D8 — Metrics

Reuse `crow-common` metrics. Design diskdb metrics covering the
important flows (allocate, free, sync, recovery) plus gauge counters
for per-disk / per-disk-group / per-zone capacity and busy/free, so
internal status and perf issues are visible and the data can later be
displayed in the console UI.

---

## Non-Gaps (good fits with CROW)

These reference assumptions map cleanly onto CROW and need no design
work, just implementation:

- **Durability model**: aioss "metadb is sole durable store, no local
  WAL" → CROW "crow-kv WAL is sole durable log." diskdb's blind writes
  become durable via crow-kv's WAL flush. Identical model.
- **Consensus semantics**: aioss metadb = Raft; CROW = Multi-Paxos with
  parallel slots. For diskdb's usage (blind writes of zone journal
  records), both behave identically; parallel slots may even improve
  allocation throughput. No change needed.
- **Blind-ops persistence**: diskdb persists via blind Puts (no
  read-modify-write) — matches crow's blind-ops-only model exactly
  (D3).
- **Async runtime**: both use tokio multi-threaded; diskdb's two-phase
  async allocation (sync CAS claim + async KV persist) maps directly.

---

## Complexity

**Medium-High.** The allocation engine (zones, CAS, deques, free batch,
scanner) is well-specified in the reference and redesigns cleanly. The
hard part is the CROW integration layer: the group-0 sysdata schema
(D1), the disk-group → paxos-group binding (D2), the zone-record
journal + crash recovery (D4), and the physical hierarchy (D6). The
decisions above must be aligned before the design doc is written and
the work is split.

## Acceptance

This requirement is complete when:
- The design decisions D1–D8 are reflected in a high-level design doc
  `doc/design/diskdb/design-crow-diskdb.md` (kept permanently under
  `doc/design/diskdb/`, not deleted — it is the root design for the
  diskdb component area, like `doc/design/kv/design-crow-kv.md` is for
  KV). The group-0 sysdata schema (D1) is a key section.
- `doc/doc_index.md` is updated with the new diskdb design area.
- The implementation work is split into follow-up R-numbers (one per
  functional scope), listed in `doc/backlog/backlog.md`.
- The project skeleton is set up: `lib/protocol` (common protobuf),
  `app/crow-diskdb` (server binary), and a client — with correct
  dependencies and a clean build. The skeleton compiles; follow-up
  R-numbers fill in the functionality.
