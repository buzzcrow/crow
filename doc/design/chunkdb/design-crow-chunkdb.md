<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: chunkdb (Overview)

This is the root design document for the chunkdb component area. It
defines **what chunkdb is**, **why key choices were made**, and **how the
component is structured**. Field-level details live in the proto files
and Rust source; this doc covers decisions and architecture only.

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Non-Goals (Design Envelope)](#2-non-goals-design-envelope)
- [3. Key Design Decisions](#3-key-design-decisions)
  - [3.1 Chunk as logical abstraction over disk blocks](#31-chunk-as-logical-abstraction-over-disk-blocks)
  - [3.2 Strip as atomic redundancy unit](#32-strip-as-atomic-redundancy-unit)
  - [3.3 Rack/node-aware placement for fault tolerance](#33-racknode-aware-placement-for-fault-tolerance)
  - [3.4 Topology cache with group-0 integration](#34-topology-cache-with-group-0-integration)
- [3.4a Watch/notify for real-time updates](#34a-watchnotify-for-real-time-updates)
  - [3.5 EC at strip level via isa-l (crow-common module)](#35-ec-at-strip-level-via-isa-l-crow-common-module)
  - [3.6 Stateless with KV persistence](#36-stateless-with-kv-persistence)
  - [3.7 Common protocol crate; gRPC now](#37-common-protocol-crate-grpc-now)
  - [3.8 Proto types used directly; no Rust type duplication](#38-proto-types-used-directly-no-rust-type-duplication)
  - [3.9 Chunk types for different use cases](#39-chunk-types-for-different-use-cases)
- [4. Architecture Overview](#4-architecture-overview)
- [5. Data Model](#5-data-model)
  - [5.1 Disk block](#51-disk-block)
  - [5.2 Strip](#52-strip)
  - [5.3 Chunk](#53-chunk)
  - [5.4 Chunk ID structure](#54-chunk-id-structure)
  - [5.4a Logical hash bucket system](#54a-logical-hash-bucket-system)
  - [5.4b Request handling during migration](#54b-request-handling-during-migration)
  - [5.5 Chunk types](#55-chunk-types)
- [6. Topology Management](#6-topology-management)
- [7. Placement Strategy](#7-placement-strategy)
  - [7.1 Mirror placement](#71-mirror-placement)
  - [7.2 EC placement](#72-ec-placement)
- [8. Allocation Flow](#8-allocation-flow)
- [9. Chunk Lifecycle](#9-chunk-lifecycle)
- [10. EC Encoding/Decoding](#10-ec-encodingdecoding)
- [11. Crate Layout](#11-crate-layout)
- [12. Concurrency Model](#12-concurrency-model)
- [13. Configuration](#13-configuration)
- [14. Implementation Scope](#14-implementation-scope)
- [15. References](#15-references)

## 1. Overview

chunkdb is a **distributed chunk metadata service** that runs on top of
CROW's KV cluster and diskdb. It is a **lightweight, stateless server**:
a chunkdb instance manages chunk metadata, orchestrates block allocation
from diskdb, and persists all durable state to CROW KV. It holds no
state that cannot be reconstructed from KV — on crash or restart it
rebuilds in-memory structures from the KV records and group-0 metadata.

Multiple chunkdb instances run across a cluster, providing fast chunk
allocation and lifecycle management. chunkdb provides rack/node-aware
placement for fault tolerance, supports both mirror and EC strip types,
and manages chunk state transitions (allocate → seal → delete). All
state changes are durably persisted to CROW KV before being acknowledged
to callers.

chunkdb **manages chunk metadata**; it does **not** perform data I/O.
Callers (a future object store, chunkio service, etc.) write to the
allocated disk blocks themselves and tell chunkdb when chunks are sealed
or deleted.

**Language:** Rust. **Runtime:** tokio (async everywhere).

**Core goals:**
- **Fast allocation** — rack/node-aware placement with parallel block
  allocation via diskdb, rollback on partial failure.
- **Fault tolerance** — mirror strips across distinct racks, EC strips
  with rack-aware placement, configurable safe/unsafe modes.
- **Durability via CROW KV** — the paxos journal is the sole durable
  store; chunkdb has no local WAL and is stateless on disk.
- **Crash safety via KV replay** — chunk metadata in KV is the source of
  truth; in-memory structures (topology cache, allocator state) are rebuilt
  on restart.
- **Flexible EC** — support multiple EC schemes simultaneously, selected
  per-strip based on cluster size and fault tolerance requirements.

**Design philosophy:** "chunkdb is a thin, stateless client of crow-kv and
diskdb." All consensus, replication, and durability are delegated to
crow-kv; block allocation is delegated to diskdb. chunkdb's job is chunk
lifecycle management, placement policy, and EC coordination — nothing more.

## 2. Non-Goals (Design Envelope)

- **No data I/O.** chunkdb allocates blocks and manages chunk metadata; it
  does not read/write block contents. A future diskio-like component does
  data I/O.
- **No local WAL.** CROW KV's WAL is the sole durability mechanism.
- **No consensus code.** chunkdb is a client of crow-kv; all interaction
  uses the existing crow-kv API.
- **No GC operations in v1.** Garbage collection (reclaim, collapse, merge)
  is deferred to a future requirement. Chunks are allocated and deleted as
  whole units in v1.
- **No mirror-to-EC conversion in v1.** Background conversion of mirror
  strips to EC strips (for shared chunks) is deferred to a future requirement.
- **No EC strip type restrictions.** Chunk type (repo/WAL/btree-page/page-index)
  is independent of strip type (mirror/EC). Any chunk can use either strip type
  based on configuration. v1 supports both mirror and EC strips for all chunk types.
- **No native SMR / zoned-namespace SSD support (v1).** Start with
  conventional HDD/SSD via diskdb; advanced disk features are deferred.

## 3. Key Design Decisions

### 3.1 Chunk as logical abstraction over disk blocks

A **chunk** is a logical container for strips, which in turn contain
physical disk blocks. This abstraction layer allows upper-layer services
to work with chunks without worrying about physical placement details.
chunkdb handles the mapping from logical chunk offsets to physical disk
blocks across multiple nodes and racks.

**Rationale:** Separates logical data management from physical storage
layout. Upper layers (object store, file system) work with chunks; chunkdb
handles the complexity of block allocation, placement, and fault tolerance.

### 3.2 Strip as atomic redundancy unit

A **strip** is the atomic unit of redundancy. Every strip is either a
**mirror strip** (replicated across multiple nodes) or an **EC strip**
(erasure-coded with data + parity blocks). Strip-level operations
(allocation, EC encoding, recovery) are atomic — a strip is either fully
allocated or not at all.

**Rationale:** Simplifies recovery and placement logic. When a disk fails,
recovery operates at strip level (rebuild lost replica or EC parity),
not at individual block level.

### 3.3 Rack/node-aware placement for fault tolerance

Placement decisions are rack-aware and node-aware to survive rack failures
and node failures. Mirror strips place replicas on distinct racks when
possible. EC strips distribute data+parity blocks across multiple racks,
with per-node block limits to ensure no single node failure exceeds the
code_num threshold.

**Rationale:** Rack failures are more common than individual disk failures.
Rack-aware placement ensures the system can survive rack-level failures
without data loss.

### 3.4 Topology cache with group-0 integration

chunkdb maintains a **TopologyCache** with site/rack/node/disk-group
hierarchy, refreshed periodically from group-0 via `HardwareClient`. This
cache is used for placement decisions and is point-in-time consistent for
each allocation operation.

**Rationale:** Placement decisions need up-to-date cluster topology.
Querying group-0 for every allocation would be too slow. Caching with
periodic refresh provides a good balance between freshness and performance.

### 3.4a Watch/notify for real-time updates

chunkdb uses crow-kv's watch/notify mechanism for real-time topology updates
in addition to periodic refresh:

- **Watch registration**: chunkdb registers for group-0 watch on disk-group
  and node status keys via the crow-kv watch/notify client.
- **Immediate updates**: On notification (disk failure, maintenance mode change,
  capacity update), affected entries in TopologyCache are updated immediately,
  without waiting for the periodic refresh interval.
- **Fallback mechanism**: Periodic refresh (30s default) continues as a fallback
  for missed notifications and consistency verification.
- **Placement impact**: Real-time updates enable placement decisions to reflect
  current cluster state, avoiding placement on failed/degraded resources.

**Rationale**: Watch/notify provides sub-second responsiveness to critical
cluster state changes (disk failures, maintenance) while periodic refresh
ensures eventual consistency and recovery from missed notifications.

### 3.5 EC at strip level via isa-l (crow-common module)

Erasure coding is performed at the strip level using the **isa-l** library
(Intel Storage Acceleration Library). A Rust FFI wrapper in `crow-common`
provides safe interfaces to isa-l's encode/decode functions. EC encoding
happens when strips are allocated; EC decoding happens during recovery.

**Rationale:** isa-l is the industry standard for high-performance EC.
Strip-level EC aligns with the strip-as-atomic-redundancy-unit design.
The FFI wrapper isolates unsafe C code from the rest of the Rust codebase.

### 3.6 Stateless with KV persistence

chunkdb is stateless on disk. All chunk metadata is persisted to CROW KV.
On restart, chunkdb rebuilds in-memory structures (topology cache,
allocator state) by replaying KV records and fetching fresh topology from
group-0.

**Rationale:** Simplifies operations (no local data to manage), enables
crash recovery without local WAL, and leverages CROW KV's durability
guarantees. Stateless design also makes scaling out easier — new instances
can start without data migration.

### 3.7 Common protocol crate; gRPC now

Protocol definitions live in `lib/crow-protocol/src/proto/chunkdb_*.proto`
and are shared between chunkdb server and chunkdb client. Communication
uses gRPC in v1. Future work may replace gRPC with a custom RPC library
for performance (similar to the crow-kv custom RPC plan).

**Rationale:** Single source of truth for protocol definitions. gRPC is
well-understood and sufficient for v1; custom RPC is an optimization that
can be added later if needed.

### 3.8 Proto types used directly; no Rust type duplication

Rust code uses the proto-generated types directly from `crow-protocol`.
No separate Rust struct definitions mirror the proto types. Conversion
between proto and domain types happens only at boundaries (e.g., KV
persistence).

**Rationale:** Reduces duplication and drift. Proto is the canonical
definition; Rust code works with proto types directly.

### 3.9 Chunk types for different use cases

Four chunk types are defined for CROW's storage hierarchy:
- **Repo chunk**: User data storage.
- **WAL chunk**: Write-ahead log entries.
- **BTree page chunk**: B-tree page storage for the crow-tree engine.
- **Page index chunk**: Page index metadata.

**Rationale:** Different storage components have different redundancy and
performance requirements. Chunk types allow optimization for each component's
needs. Chunk type is independent of strip type — any chunk type can use
either mirror or EC strips based on configuration and access patterns.

## 4. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         chunkdb Server                          │
├─────────────────────────────────────────────────────────────────┤
│  gRPC Service (ChunkdbService)                                  │
│  ├── AllocateChunk                                              │
│  ├── SealChunk                                                   │
│  ├── DeleteChunk                                                 │
│  ├── QueryChunk                                                   │
│  └── ListChunk                                                   │
├─────────────────────────────────────────────────────────────────┤
│  Lifecycle Layer                                                 │
│  ├── allocate_chunk()                                            │
│  ├── seal_chunk()                                                │
│  └── delete_chunk()                                              │
├─────────────────────────────────────────────────────────────────┤
│  Allocation Layer                                                │
│  ├── ChunkAllocator (orchestrate strip allocation)              │
│  ├── MirrorStripAllocator                                        │
│  └── ECStripAllocator                                            │
├─────────────────────────────────────────────────────────────────┤
│  Placement Layer                                                 │
│  ├── RackNodeSelector                                            │
│  ├── MirrorPlacement (distinct racks)                            │
│  └── ECPlacement (rack-aware, safe/unsafe)                       │
├─────────────────────────────────────────────────────────────────┤
│  Topology Layer                                                  │
│  ├── TopologyCache (site/rack/node/disk-group)                  │
│  ├── TopologyRefresh (periodic from group-0)                     │
│  └── WatchNotify (real-time updates from group-0)                │
├─────────────────────────────────────────────────────────────────┤
│  Storage Layer                                                   │
│  ├── KV Persistence (chunk metadata)                            │
│  └── DiskdbClient Pool (block allocation/free)                   │
├─────────────────────────────────────────────────────────────────┤
│  EC Layer                                                        │
│  └── crow-common EC (isa-l FFI wrapper)                        │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │ crow-kv │         │ diskdb  │         │ group-0 │
    │ (KV)    │         │         │         │Hardware │
    └─────────┘         └─────────┘         └─────────┘
```

**Key components:**

- **gRPC Service**: External API for chunk operations.
- **Lifecycle Layer**: Implements chunk state machine transitions.
- **Allocation Layer**: Orchestrates strip allocation with rollback on failure.
- **Placement Layer**: Rack/node-aware placement algorithms.
- **Topology Layer**: Caches and refreshes cluster topology from group-0.
- **Storage Layer**: KV persistence for chunk metadata, diskdb client pool.
- **EC Layer**: isa-l wrapper for strip-level EC operations.

## 5. Data Model

### 5.1 Disk block

A **disk block** is the smallest fixed-size physical storage unit managed
by diskdb. The default size is **1 MB** (configurable, range 1 MB – 2 MB).
Disk blocks are identified by `Segment { node_id, disk_id, zone_index,
zone_offset, size, tag }` (from diskdb proto).

### 5.2 Strip

A **strip** is the atomic redundancy unit. Two strip types:

**Mirror Strip**: One disk block capacity, replicated across N nodes
(configurable copy count, default 3). Each replica is a full copy on a
different node. Data capacity = 1 × disk_block_size.

**EC Strip**: `data_num` data blocks + `code_num` parity blocks,
distributed across different nodes. Data capacity = `data_num ×
disk_block_size`. For example:
- 6+3 EC with 1 MB blocks → 6 MB data capacity, 9 MB total.
- 8+4 EC with 1 MB blocks → 8 MB data capacity, 12 MB total.

Each strip tracks:
- **Usage bitmap**: Per-strip bitmap tracking which sub-block ranges are
  written (1 bit per granularity, e.g., 16 KB).
- **EC state** (EC strips only): `NotStarted` → `Complete`.
- **Block state**: Per-block health (Good, Suspicious, Bad).

### 5.3 Chunk

A **chunk** is a container for strips. Chunk properties:
- **128-bit ID**: Chunk type (8 bits) + Timestamp (48 bits) + Randomness (72 bits).
- **State**: `Init` → `Active` → `Sealed` → `Deleted`.
- **Type**: Metadata, Shared, Specific.
- **Capacity**: Total data capacity across all strips.
- **Write granularity**: Minimum write alignment (e.g., 4 KB).
- **Strips**: Ordered list of strips (mirror or EC).
- **Logical-to-physical mapping**: Encoded offset arrays for GC (deferred).

Chunk size is variable, determined by the total size of its constituent
strips (1 MB – 4 GB range).

### 5.4 Chunk ID structure

The 128-bit chunk ID uses a custom format optimized for chunkdb:

| Bits    | Width  | Field    | Description                          |
|---------|--------|----------|--------------------------------------|
| 0–7     | 8 bits | Chunk Type | Chunk type identifier (0-255)       |
| 8–55    | 48 bits| Timestamp | Milliseconds since Unix epoch        |
| 56–127  | 72 bits| Randomness | Cryptographically secure random      |

**Layout benefits:**
- **Chunk type prefix**: First byte enables efficient filtering and scanning by chunk type
- **Time ordering**: 48-bit timestamp provides natural ordering for sequential operations
- **Global uniqueness**: 48-bit timestamp + 72-bit randomness provides sufficient uniqueness
- **Compact size**: 128 bits total, same as standard UUID

**Generation**: Chunk IDs are generated using `getrandom` for cryptographically secure randomness combined with system timestamp. The generation function will be added to `crow-common` for reuse across components.

**Chunk type values (8 bits, 0-255):**
- 0: Repo chunk (user data)
- 1: WAL chunk
- 2: BTree page chunk
- 3: Page index chunk
- 4-255: Reserved for future use

The chunk ID is hashed to a **logical hash bucket** (16-bit bucket ID, 0-65535),
which is then mapped to a physical KV group via a binding table stored in group-0.
This two-level approach allows KV group topology changes without chunk data migration.

### 5.4a Logical Hash Bucket System

**Two-level routing design:**
1. **Chunk ID → Logical Bucket**: Hash chunk ID (128-bit) to a 16-bit logical bucket ID (0-65535)
2. **Logical Bucket → Physical KV Group**: Consult group-0 binding table to map bucket to KV group

**Benefits:**
- **Predictable migration**: Only chunks in affected bucket ranges need migration when KV groups change
- **Deterministic queries**: Chunk ID always hashes to same logical bucket, then consults current mapping
- **Gradual rebalancing**: Update mappings incrementally to move load between KV groups
- **Independent scaling**: Logical bucket space (65536 buckets) can map to any number of physical KV groups

**Hash function:**
```
logical_bucket = hash(chunk_id) % 65536
```
Using a fast, uniform hash function (e.g., xxHash, FarmHash) on the 128-bit chunk ID.

**Group-0 binding table schema:**
```
LogicalBucketBindingValue:
  bucket_range_start: uint16  # Start of bucket range (inclusive)
  bucket_range_end: uint16    # End of bucket range (exclusive)  
  kv_group_id: uint16         # Target physical KV group
  version: uint64             # Binding version for cache invalidation
```

Stored in group-0 with key pattern: `chunkdb_bucket_binding:<range_start>`

Example mapping:
- Buckets 0-16383 → KV group 1
- Buckets 16384-32767 → KV group 2
- Buckets 32768-49151 → KV group 3
- Buckets 49152-65535 → KV group 4

**Query flow:**
1. Hash chunk ID → logical bucket (e.g., 25000)
2. Consult group-0 binding table (cached locally) → KV group 2
3. Query chunk metadata from KV group 2 using chunk ID as key

**Adding/removing KV groups:**
- To add KV group: Split an existing bucket range, update binding table in group-0
- To remove KV group: Merge its bucket range into neighboring ranges, update binding table
- **Chunk record migration required**: When bucket mappings change, affected chunk
  records must be physically moved from old KV group to new KV group
- Migration strategy: Background task copies chunk records to new KV group,
  then deletes from old KV group after successful copy

**Load rebalancing:**
- Gradually shift bucket range boundaries to balance load across KV groups
- Example: If KV group 2 is overloaded, move some buckets to KV group 3 by updating ranges
- Changes take effect immediately as chunkdb servers refresh their binding cache

**Instance sharding:** The bucket space is also used to shard chunkdb
instances — each instance owns a range of buckets and rejects requests
for chunks outside its range. See sub-design
[`design-crow-chunkdb-range-binding.md`](design-crow-chunkdb-range-binding.md).

### 5.4b Request Handling During Migration

When chunk records are being migrated from old KV group to new KV group, requests
must be handled correctly to avoid data loss or inconsistencies.

**Migration phases:**
1. **Pre-migration**: Chunk exists only in old KV group
2. **Copying**: Chunk being copied from old KV group to new KV group
3. **Cutover**: Chunk exists in both KV groups (new location authoritative)
4. **Cleanup**: Old copy deleted from old KV group

**Request handling strategies:**

**Option 1: Dual-write during migration (Recommended)**
- During migration: Write to both old and new KV groups
- Reads: Try new KV group first, fall back to old KV group if not found
- After migration complete: Delete from old KV group, switch to single-location reads
- Pros: No data loss, minimal read latency impact
- Cons: Temporary write amplification during migration

**Option 2: Version-based routing**
- Add migration state to binding table: `in_migration`, `migration_complete`
- During migration: Route all requests to old KV group
- After copy complete: Update binding table to route to new KV group
- Pros: Simple routing logic, no dual writes
- Cons: Longer migration window, old KV group handles all traffic

**Option 3: Background copy with read repair**
- Background task copies to new KV group
- Reads: Always try new KV group first, fall back to old if not found
- When chunk found in old location during migration: trigger copy to new location
- Pros: Eventual consistency, can start serving from new location immediately
- Cons: Read latency spikes during migration, complex coordination

**Recommended approach (Option 1 - Dual-write):**
```rust
fn read_chunk(chunk_id: ChunkId) -> Result<Chunk> {
    let bucket = hash_to_bucket(chunk_id);
    let kv_group = get_binding(bucket);
    
    // Try new location first
    match kv_read(kv_group, chunk_id) {
        Ok(chunk) => return Ok(chunk),
        Err(_) => {
            // Fall back to old location if in migration
            if is_bucket_in_migration(bucket) {
                let old_kv_group = get_old_binding(bucket);
                return kv_read(old_kv_group, chunk_id);
            }
            return Err(NotFound);
        }
    }
}

fn write_chunk(chunk: Chunk) -> Result<()> {
    let bucket = hash_to_bucket(chunk.id);
    let kv_group = get_binding(bucket);
    
    // Write to new location
    kv_write(kv_group, &chunk)?;
    
    // Dual-write during migration
    if is_bucket_in_migration(bucket) {
        let old_kv_group = get_old_binding(bucket);
        kv_write(old_kv_group, &chunk)?;
    }
    
    Ok(())
}
```

**Migration state tracking in group-0:**
```
BucketMigrationState:
  bucket_range_start: uint16
  bucket_range_end: uint16
  old_kv_group: uint16
  new_kv_group: uint16
  state: enum { NotMigrating, Copying, Cutover, Cleanup, Complete }
  version: uint64
```

### 5.5 Chunk types

| Type          | Chunk Type Value | Description                          |
|---------------|------------------|--------------------------------------|
| Repo          | 0                | User data storage                    |
| WAL           | 1                | Write-ahead log entries              |
| BTree page    | 2                | B-tree page storage                  |
| Page index    | 3                | Page index metadata                  |
| Reserved      | 4-255            | Reserved for future use              |

**Note:** Chunk type is independent of strip type. Any chunk type can use
either mirror or EC strips based on configuration and requirements.

**Metadata chunks**: Contain only mirror strips for maximum redundancy and
fast access. Used for critical metadata.

**Repo chunks**: Used for user data storage. Can use either mirror or EC strips
based on configuration and access patterns.

**WAL chunks**: Used for write-ahead log entries. Typically use mirror strips
for durability and fast recovery, but EC is possible for space optimization.

**BTree page chunks**: Used for B-tree page storage in the crow-tree engine.
Can use mirror or EC strips based on performance vs space trade-offs.

**Page index chunks**: Used for page index metadata. Typically use mirror strips
for fast lookup, but EC is possible for large indexes.

## 6. Topology Management

chunkdb maintains a **TopologyCache** with the cluster hierarchy:

```
Site (data center)
  └── Rack (physical rack)
      └── Node (server)
          └── Disk-Group (logical disk group)
              └── Disk (physical disk)
```

**TopologySnapshot**: Point-in-time immutable snapshot containing:
- `sites`: Map<site_id, SiteInfo>
- `racks`: Map<rack_id, RackInfo>
- `nodes`: Map<node_uuid, NodeInfo>
- `disk_groups`: Map<disk_group_id, DiskGroupInfo>

**TopologyRefresh**: Hybrid approach combining periodic refresh with watch/notify:
- **Periodic refresh**: Background task that periodically (configurable,
  default 30s) fetches full topology from group-0 via `HardwareClient` for
  consistency and missed notification recovery.
- **Watch/notify integration**: Register for group-0 watch on disk-group and
  node status keys via crow-kv watch/notify client. On notification, update
  affected entries in TopologyCache immediately for real-time responsiveness
  to disk failures, maintenance changes, and capacity updates.
- Fetch all nodes with status and rack assignment
- Fetch all disk-groups with capacity and node assignment
- Fetch all racks with site assignment
- Atomically update the cache

**Rationale**: Placement decisions need consistent topology. Point-in-time
snapshots ensure a single allocation operation sees a consistent view.
Periodic refresh balances freshness and performance.

## 7. Placement Strategy

### 7.1 Mirror placement

Mirror placement aims to place replicas on **distinct racks** to survive
rack failures:

1. Select N nodes (copy count) from online nodes
2. Prefer nodes on different racks
3. If not enough distinct racks, place multiple copies on same rack
4. For each node, select the disk-group with most free space
5. Return placement plan with node/disk-group assignments

**Negative hints**: Nodes can be excluded from placement (e.g., during
recovery to avoid re-using failed nodes).

**Example**: 3-copy mirror on 3-rack cluster → 3 replicas on 3 distinct racks.
On 2-rack cluster → 2 replicas on distinct racks, 1 replica on either rack.

### 7.2 EC placement

EC placement distributes data+parity blocks **across multiple racks**
with per-node block limits:

1. Calculate total blocks = data_num + code_num
2. Select nodes such that:
   - No node has > code_num blocks (safe mode)
   - Blocks are distributed across ≥3 racks when possible
3. For each node, select disk-group with most free space
4. Return placement plan with block assignments

**Safe mode**: Ensures no single node failure exceeds code_num, guaranteeing
recoverability. Requires enough nodes to satisfy constraints.

**Unsafe mode**: Relaxes per-node limits when cluster is too small. Used
only in exceptional circumstances (e.g., degraded cluster).

**Example**: 8+4 EC on 12-node cluster → 12 blocks across ≥3 racks, max 4
blocks per node. On 3-node cluster (unsafe mode) → 12 blocks, 4 per node.

## 8. Allocation Flow

**AllocateChunk** operation:

1. Generate 192-bit chunk ID (metadata + UUID + reserved)
2. Fetch topology snapshot from TopologyCache
3. Calculate strip layout based on requested capacity and strip type
4. For each strip:
   - Call placement selector to get node/disk-group assignments
   - Call diskdb to allocate blocks in parallel
   - On failure: rollback (free all allocated blocks), return error
5. Build chunk metadata with strips and state = Active
6. Persist chunk metadata to KV
7. Return chunk to caller

**Parallel allocation**: Strips are allocated in parallel to minimize
latency. Diskdb calls within a strip are also parallel (all blocks in a
mirror strip, all data+parity blocks in an EC strip).

**Rollback**: If any strip allocation fails, all successfully allocated
blocks are freed via diskdb before returning an error. This ensures no
leaked blocks on partial failure.

## 9. Chunk Lifecycle

```
Init ──> Active ──> Sealed ──> Deleted
```

| State   | Description                                                  |
|---------|--------------------------------------------------------------|
| Init    | Internal transient state during allocation. Not visible to callers. |
| Active  | Chunk is open for writes. Strips can be appended. Returned by AllocateChunk. |
| Sealed  | Chunk is read-only. Records final length and seal timestamp. |
| Deleted | Chunk marked for deletion. All disk blocks freed via diskdb.  |

**State transitions**:
- `Init → Active`: After successful block allocation and KV persistence.
- `Active → Sealed`: Via `SealChunk` RPC. Validates state, updates sealed_length.
- `Active → Deleted`: Via `DeleteChunk` RPC. Frees disk blocks.
- `Sealed → Deleted`: Via `DeleteChunk` RPC. Frees disk blocks.
- Invalid transitions (e.g., `Sealed → Active`) return errors.

**Concurrency**: KV CAS or state machine guards prevent conflicting
transitions. Last writer wins with validation.

## 10. EC Encoding/Decoding

**EC encoding** (strip allocation):
1. Allocate data_num + code_num blocks via diskdb
2. Fill data blocks with actual data (by caller, not chunkdb)
3. Call isa-l encode to generate parity blocks
4. Mark EC state = Complete

**EC decoding** (recovery, deferred):
1. Identify failed blocks (via disk failure notification)
2. Read surviving data+parity blocks
3. Call isa-l decode to reconstruct lost data
4. Write reconstructed data to new blocks
5. Update strip with new block locations

**crow-common EC module**: Provides safe Rust interface to isa-l:
- `make_gf_table(data_num, code_num)`: Generate Galois Field table
- `encode(data_num, code_num, segment_len, gf_table, data_ptr, code_ptr)`: Encode
- `decode(...)`: Decode with error recovery
- Buffer management: `make_buffer`, `destroy_buffer`

**Rationale**: isa-l is highly optimized for AVX2/AVX512. Strip-level EC
aligns with redundancy unit design.

## 11. Crate Layout

```
app/crow-chunkdb/              # chunkdb server binary
├── Cargo.toml
├── src/
│   ├── main.rs               # CLI entrypoint
│   ├── server.rs             # gRPC server
│   ├── lifecycle.rs          # Chunk lifecycle handlers
│   ├── allocator/
│   │   ├── mod.rs            # ChunkAllocator
│   │   ├── mirror.rs         # MirrorStripAllocator
│   │   └── ec.rs             # ECStripAllocator
│   ├── selector/
│   │   ├── mod.rs            # RackNodeSelector
│   │   ├── mirror.rs         # Mirror placement
│   │   └── ec.rs             # EC placement
│   ├── topology/
│   │   ├── mod.rs            # TopologyCache, TopologySnapshot
│   │   └── refresh.rs        # TopologyRefresh task
│   ├── storage.rs            # KV persistence
│   ├── ec.rs                 # EC encoding/decoding orchestration
│   └── types/
│       ├── mod.rs            # Common types
│       ├── chunk.rs          # Chunk types
│       ├── strip.rs          # Strip types
│       ├── chunk_id.rs       # Chunk ID generation (128-bit format)
│       └── placement.rs      # Placement types

lib/crow-chunkdb-client/       # chunkdb client library
├── Cargo.toml
└── src/
    ├── client.rs             # ChunkdbClient
    └── types.rs              # Client-side types

lib/crow-common/               # Shared library (EC module added)
├── src/
│   └── ec.rs                 # isa-l FFI wrapper (encode/decode)

lib/crow-protocol/             # Protocol definitions
└── src/proto/
    ├── chunkdb_service.proto # gRPC service
    └── chunkdb_types.proto   # Data types
```

## 12. Concurrency Model

- **Async everywhere**: All public APIs are async (`async fn`).
- **Shared state**: `Arc<RwLock<T>>` for topology cache, allocator state.
- **Lock scoping**: Acquire locks in `{}` blocks, drop before `.await`.
- **Parallel allocation**: Use `futures::join_all` for parallel strip/block
  allocation.
- **Background tasks**: `tokio::spawn` for topology refresh, rollback cleanup.

**Rationale**: tokio provides efficient async I/O. Lock scoping prevents
deadlocks with `.await`. Parallel allocation minimizes latency.

## 13. Configuration

Key configuration parameters:

| Parameter                  | Default | Description                          |
|----------------------------|---------|--------------------------------------|
| disk_block_size            | 1 MB    | Size of disk blocks from diskdb       |
| mirror_copy_count          | 3       | Number of replicas for mirror strips  |
| default_ec_scheme          | 6+3     | Default EC scheme (data+parity)       |
| topology_refresh_interval  | 30 s    | Topology cache refresh interval       |
| placement_safe_mode        | true    | Enforce safe EC placement constraints |
| max_allocation_parallelism | 10      | Max parallel strip allocations        |

Configuration is loaded from CLI args or config file at startup.

## 14. Implementation Scope

**v1 (R85)**:
- Basic chunkdb server and client
- Mirror and EC strip allocation
- Rack/node-aware placement
- Topology cache with group-0 integration and watch/notify for real-time updates
- Basic chunk lifecycle (allocate/seal/delete)
- EC wrapper (crow-common module)
- Chunk ID generation (crow-common): 128-bit format with getrandom
- Repo, WAL, BTree page, Page index chunk types (mirror and EC strips supported)
- E2E tests

**Future work** (separate requirements):
- In-chunk GC operations (reclaim, collapse, merge)
- Mirror-to-EC conversion for shared chunks
- Specific chunk type (direct EC write for large objects)
- Recovery flow (disk failure handling, EC rebuild)
- Metrics and observability
- Console/CLI integration
- Custom RPC for performance

## 15. References

References (other projects, not ports):

- aioss chunkdb design: `/cpp/aioss/server/chunkdb/doc/design.md`
- aioss chunkdb proto: `/cpp/aioss/libs/protocol/proto/chunkdb/chunkdb.proto`
- Java reference: `/cpp/buzz-java/buzz-pods/buzz-chunk-db`
- Original strip chunk: `/cpp/buzz-java/buzz-libs/buzz-proto/src/main/proto/strip_chunk.fbs`
- isa-l EC wrapper: `/cpp/buzz-java/buzz-libs/buzz-ni/src/main/java/com/buzz/ni/EC.java`
- CROW diskdb design: `doc/design/diskdb/design-crow-diskdb.md`