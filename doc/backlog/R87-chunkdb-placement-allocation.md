<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R87: chunkdb — Placement and Allocation

**Problem**:

- **Current behavior + impact** — chunkdb must place strip blocks across
  racks and nodes for fault tolerance and orchestrate disk-block
  allocation via diskdb. There is no placement selector or chunk
  allocator in the chunkdb server yet (R85 lands only the skeleton, R86
  lands the topology cache). Without rack/node-aware placement, mirror
  replicas could land on the same rack (a single rack failure loses all
  replicas) and EC blocks could concentrate on one node (a single node
  failure exceeds `code_num` and loses data). Without a chunk allocator
  orchestrating parallel diskdb calls with rollback, a partial
  allocation failure (one diskdb instance down mid-strip) would leak
  allocated blocks with no owner. This is the core fault-tolerance
  mechanism of chunkdb; every `AllocateChunk` / `AppendChunk` call
  (R89) depends on it.
- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §3.2 (strip as atomic redundancy unit), §3.3 (rack/node-aware
  placement), §7 (placement strategy — §7.1 mirror distinct racks,
  §7.2 EC rack-aware safe/unsafe modes), §8 (allocation flow —
  parallel + rollback), §12 (concurrency — `futures::join_all` for
  parallel allocation),
  [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
  §3.2 (disk-group → paxos group binding), §3.4 (`Segment` record is
  the allocation handle, `BusyBlockValue` carries `owner_chunk`),
  `diskdb_service.proto` (`AllocateBlocks` / `FreeBlocks` RPCs).
  aioss analog: aioss chunkdb `RackNodeSelector` — mirror selects N
  distinct nodes across racks; EC distributes `data_num + code_num`
  blocks across racks with per-node `code_num` limit (safe mode);
  CROW follows the same algorithm with CROW's `HardwareClient` +
  `DiskdbClient` (design §7 — direct port of the algorithm).
- **Use scenarios** —
  - **Mirror strip placement across distinct racks** — an
    `AllocateChunk` request for a mirror strip with 3 copies on a
    3-rack cluster → the selector picks one node per rack; the
    allocator calls diskdb on each node's disk-group in parallel; all
    3 `Segment`s are allocated; the strip is whole. A single rack
    failure later does not lose the strip.
  - **EC strip safe-mode placement** — an 8+4 EC strip on a 12-node,
    3-rack cluster → the selector distributes 12 blocks across ≥3
    racks, max 4 blocks per node (safe mode); any 4-node failure is
    recoverable. The allocator calls diskdb per node in parallel.
  - **EC strip unsafe-mode fallback** — an 8+4 EC strip on a 3-node
    cluster (not enough nodes for safe mode) → the selector falls back
    to unsafe mode, places 4 blocks per node; logs a warning that
    fault tolerance is reduced. Allocation still succeeds.
  - **Partial failure with rollback** — a 3-copy mirror strip
    allocation: 2 of 3 diskdb calls succeed, the 3rd fails (disk-group
    `Bad`) → the allocator frees the 2 successfully-allocated `Segment`s
    via `FreeBlocks` and returns an error; no leaked blocks, no
    orphan `owner_chunk` records.
  - **Negative hints during recovery** — a disk is `Bad`; the selector
    receives a negative hint (exclude that node/disk-group); placement
    skips it and picks a healthy alternative. Used by R83 recovery to
    avoid re-placing on the failed disk.
  - **Parallel allocation minimizes latency** — a multi-strip chunk
    allocation: strips are allocated in parallel (`futures::join_all`),
    and within each strip the diskdb calls are parallel; total
    allocation latency ≈ the slowest single diskdb call, not the sum.

**Solution**:

**One-line summary**: add a `RackNodeSelector` (mirror: distinct racks;
EC: rack-aware safe/unsafe modes + negative hints) and a
`ChunkAllocator` that orchestrates strip layout → selector → parallel
diskdb `AllocateBlocks` with rollback on partial failure.

1. **Placement selector** —
   `app/crow-chunkdb/src/selector/` (new module):
   - `mod.rs`: `RackNodeSelector` trait + factory; takes a
     `TopologySnapshot` (R86) + placement constraints; returns a
     `PlacementPlan` (list of `(rack_id, node_id, disk_group_id,
     block_count)` tuples).
   - `mirror.rs`: `MirrorPlacement` — select N distinct nodes across
     distinct racks (one copy per rack when possible); random-start
     scan to avoid hot spots; negative hints (exclude failed
     nodes/racks). Design §7.1.
   - `ec.rs`: `EcPlacement` — distribute `data_num + code_num` blocks
     across racks first, then nodes within racks; safe mode (max
     `code_num` blocks per node — guarantees recoverability) vs unsafe
     mode (relax per-node limit when cluster too small). Design §7.2.
   - Negative hints: `PlacementConstraints { exclude_nodes,
     exclude_racks, exclude_disk_groups }` — used by R83 recovery and
     by re-allocation after a mid-allocation failure.

2. **Chunk allocator** —
   `app/crow-chunkdb/src/allocator/` (new module):
   - `mod.rs`: `ChunkAllocator` — orchestrates strip layout calculation
     → `RackNodeSelector` → parallel diskdb `AllocateBlocks` →
     `Segment` collection → strip assembly. Design §8.
   - `mirror.rs`: `MirrorStripAllocator` — N copies, one `Segment` per
     replica via diskdb; `futures::join_all` for parallel allocation.
   - `ec.rs`: `EcStripAllocator` — `data_num + code_num` `Segment`s
     via diskdb; parallel allocation; EC parity computation deferred
     (design §10 — parity is computed by the caller or a later step,
     not at allocation time in v1).
   - `pool.rs`: `DiskdbClientPool` — `DashMap<DiskGroupId, Channel>`
     cache of gRPC connections to diskdb instances; reuses
     `DiskdbClient` (R74) endpoint discovery pattern. Routes
     `AllocateBlocks` to the correct diskdb instance per disk-group.

3. **Rollback on partial failure** —
   `app/crow-chunkdb/src/allocator/rollback.rs` (new):
   - If any diskdb `AllocateBlocks` call fails mid-strip, free all
     successfully-allocated `Segment`s via `FreeBlocks` (best-effort;
     log failures for the orphan-block scanner to catch later).
   - Rollback is synchronous within the `AllocateChunk` call — the
     caller sees either a fully-allocated strip or an error with no
     leaked blocks. Design §8 "Rollback".

**Flow diagram**:

```
  AllocateChunk request (strip_type, data_num, code_num, strip_count)
       │
       ▼
  ChunkAllocator (item 2)
       │  calculate strip layout
       ▼
  RackNodeSelector (item 1) ◄── TopologySnapshot (R86) + constraints
       │  mirror: N distinct racks    EC: rack-aware safe/unsafe
       ▼
  PlacementPlan [(rack, node, dg, block_count), ...]
       │
       ▼
  ┌──────────────────────────────────────────────┐
  │ parallel diskdb AllocateBlocks (join_all)    │
  │  per node/disk-group via DiskdbClientPool    │
  └──────────────┬───────────────────────────────┘
                 │
       ┌─────────┴─────────┐
       │ all succeed       │ any fail
       ▼                   ▼
  collect Segments    rollback (item 3)
       │               free successful Segments via FreeBlocks
       ▼               return error (no leak)
  assemble strip
       │
       ▼
  return to lifecycle (R89) for KV persistence
```

- **Edge cases at a glance**:
  - Not enough racks for mirror copy count (e.g. 3 copies, 2 racks) →
    place 2 copies on distinct racks, 3rd copy on a distinct node in
    the first rack; log a warning that rack-fault tolerance is
    reduced. No error.
  - Not enough nodes for EC safe mode → fall back to unsafe mode;
    log a warning; allocation succeeds with reduced fault tolerance.
  - Not enough nodes even for unsafe mode (fewer nodes than
    `data_num + code_num`) → return a `InsufficientNodes` error; do
    not over-allocate.
  - diskdb `AllocateBlocks` returns partial segments (fewer than
    requested) → treat as failure for that node; rollback the whole
    strip (atomic strip semantics, design §3.2).
  - Rollback `FreeBlocks` fails → log the orphan segments (with
    `owner_chunk`) for the diskdb orphan scanner to reclaim later;
    do not block the error return to the caller.
  - Negative hint excludes all nodes in a rack → selector skips that
    rack; if no rack has enough capacity, return
    `InsufficientCapacity`.
  - Topology snapshot is stale (disk went `Bad` after snapshot was
    taken) → diskdb call fails; rollback; the next attempt uses a
    fresher snapshot (R86 watch/notify).

**Dependencies**:

- **R85** (foundation) — chunkdb server crate must exist.
- **R86** (topology) — `TopologyCache` / `TopologySnapshot` is the
  input to the selector; must be landed first.
- **R72** (diskdb core) — `AllocateBlocks` / `FreeBlocks` RPCs and
  `DiskdbClient` (R74) must exist; R87 uses `DiskdbClient` via the
  `DiskdbClientPool`.
- **R83** (recovery) depends on R87 — recovery re-allocation uses the
  selector with negative hints (exclude the `Bad` disk).
- **R89** (lifecycle) depends on R87 — `allocate_chunk` calls the
  `ChunkAllocator`.

**Acceptance**:

**Mirror placement**:
- `MirrorPlacement::select(3 copies, 3-rack topology)` returns 3
  nodes, one per rack → verify each node is in a distinct rack.
  Unit test.
- `MirrorPlacement::select(3 copies, 2-rack topology)` returns 3
  nodes, 2 on distinct racks + 1 on a distinct node in the first
  rack; a warning is logged → verify rack spread is maximized. Unit
  test.
- `MirrorPlacement::select` with a negative hint excluding rack 1 →
  no selected node is in rack 1. Unit test.

**EC placement**:
- `EcPlacement::select(8+4, 12-node/3-rack topology, safe mode)`
  returns 12 blocks across ≥3 racks, max 4 per node → verify the
  per-node limit and rack spread. Unit test.
- `EcPlacement::select(8+4, 3-node topology)` falls back to unsafe
  mode, 4 blocks per node; logs a warning → verify unsafe fallback
  and warning. Unit test.
- `EcPlacement::select(8+4, 2-node topology)` returns
  `InsufficientNodes` (2 < 12 even in unsafe mode is not enough when
  blocks > nodes × max_per_node_unsafe) → verify the error. Unit
  test.

**Parallel allocation + rollback**:
- `ChunkAllocator::allocate_mirror_strip(3 copies)` on a 3-disk-group
  cluster → 3 `Segment`s allocated in parallel; all succeed; strip
  assembled. Integration test (with diskdb instances).
- `ChunkAllocator::allocate_mirror_strip(3 copies)` where the 2nd
  diskdb call fails → the 1st `Segment` is freed via `FreeBlocks`;
  error returned; no leaked `Segment` (verify via diskdb
  `QueryCapacityStats` — busy count unchanged). Integration test.
- `ChunkAllocator::allocate_ec_strip(8+4)` → 12 `Segment`s allocated
  in parallel; all succeed; strip assembled with `EC_STATE_NO_PARITY`
  (parity deferred). Integration test.
- Multi-strip chunk allocation → strips allocated in parallel
  (`join_all`); total latency ≈ slowest single strip, not the sum
  (verify via timing). Integration test.

**Edge cases**:
- diskdb returns partial segments (requested 4, got 2) → treated as
  failure; rollback; error returned. Unit test.
- Rollback `FreeBlocks` fails → orphan segments logged with
  `owner_chunk`; error still returned to caller (rollback failure does
  not mask the original error). Unit test.
- Negative hint excludes all nodes → `InsufficientCapacity` error.
  Unit test.

**Lint + test commands**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` (selector unit tests + allocator
  integration tests with diskdb pass).
