<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R72: diskdb — Zone Allocator + Record Persistence (Block Allocate/Free)

**Problem**: R71 gives diskdb a running server with group-0 sync and
disk status management, but the core allocation engine does not exist.
The server can discover its disk-groups and disks but cannot allocate
or free blocks. This is diskdb's primary function — the allocate/free
RPCs are stubbed (`Unimplemented`) and there is no zone allocator,
no active zone rotation, and no record persistence.

The design doc (§8) specifies the allocation algorithm: per-zone
bitmap-scan allocators with per-bit CAS, disk-level rotating
active-zone-set, round-robin across disks within the named
disk-group, two-phase async
allocation (sync bitmap claim + async KV persist), and immediate free.
The key CROW-specific design (§3.4) is that each allocate writes a
small `BusyBlockValue` to the disk-group's bound paxos data group at
the `BusyBlockKey` (keyed by `unit_offset`, not a full `ZoneValue`),
and each free deletes the `BusyBlockKey` and writes a `FreeBlockValue`
at the `FreeBlockKey` in one `batch_write`. The bitmap is derived from
the records — never written as a full bitmap on the hot path.

**Record model (§3.4, §7):**

- `BusyBlockValue` — written on allocate. Carries `owner_chunk`,
  `unit_size`, `state`. **Deleted on free** (in the same `batch_write`
  that writes the `FreeBlockValue`). On re-allocate (after a free), a
  new `BusyBlockValue` is written at the same key (new owner,
  `state = Ok`).
- `FreeBlockValue` — written on free, in the same `batch_write` that
  deletes the `BusyBlockKey`. Carries `previous_owner` (the
  `owner_chunk` from the freed `BusyBlockValue`) for audit / scanner
  cross-check. On re-allocate, the `FreeBlockKey` is deleted. Transient
  — deleted by compaction (R73) after being merged into the `ZoneValue`
  bitmap.
- Current state determination (no slot ordering needed): a block is
  **busy** iff its `BusyBlockKey` exists; otherwise it is **free**. A
  `FreeBlockKey` may exist for a not-yet-compacted free (carrying
  `previous_owner`); after compaction, neither key exists for that
  offset.

**Free is immediate in v1 (§8):** no `FreeBatch`, no timer, no
background flush loop. Each free writes its `FreeBlockValue` to the
bound data group immediately via one `batch_write` (the `BusyBlockKey`
stays). This is simpler, avoids the ghost-allocation-on-crash window,
and matches the "records are the source of truth" model directly. Free
batching (size-threshold grouping, no timer) is an optimization for
high-free-throughput workloads, tracked as R79.

The buzz-cpp reference (`/cpp/buzz-cpp/src/app/buzz-disk-db`) has a
working zone allocator with the same algorithm concept: bitmap-scan
with a rotating cursor (`last_pos_64`), per-bit CAS via
`compare_exchange` on 64-bit words, and disk-level zone rotation
(rotating active-zone-set that rotates when exhausted). The only
difference is persistence: buzz-cpp saves the full bitmap to a local
file via a background scheduled task; CROW replaces this with
per-allocation `BusyBlockValue`/`FreeBlockValue` writes to the
bound data group via KV put/delete (§3.4). The bitmap-scan, per-bit
round-robin patterns are directly reusable; the persistence layer is
new.

**Solution**: Implement the second major component — block
allocate/free — with the bitmap-scan allocator from buzz-cpp and the
record-based persistence model from §3.4.

1. **Zone bitmap-scan allocator** — create
   `app/crow-diskdb/src/zone/mod.rs`:
   - `Zone` — per-zone allocation state (extends R70's types):
     - `disk_group_id: DiskGroupId`, `disk_id: DiskId`, `zone_index:
       u32`.
     - `zone_state: RwLock<ZoneState>` — health
       (Healthy/Missing/Bad). Not atomic — updated by the sync loop
       (R71) and health probe (R76), not the hot path. Zones inherit
       the disk's `HwStatus`; there is no separate zone-level CAS
       state machine (§9).
     - `unit_capacity: u32` — total block units in the zone
       (`zone_size / block_size`), word-aligned per §3.5.
     - `usage_bits: UsageBitmap` — from R70, lock-free atomic bit
       operations. Each bit = one block unit; bit set = busy, bit
       clear = free.
     - `last_pos_64: AtomicU64` — rotating cursor over 64-bit words
       in the bitmap. Scans start from this position and wrap around,
       spreading allocation load across the zone. Updated on each
       successful allocation to the word where the bit was found.
     - `used_count: AtomicU32` — count of set bits (for fast
       `allocatable()` check and metrics). Incremented on allocate,
       decremented on free.
   - `allocate(unit_count: u32) -> Option<AllocatedRange>` — Phase 1
     (sync), per-bit CAS (buzz-cpp `ddb_disk_zone::allocate_block`
     pattern, §8):
     a. Check `zone_state` is Healthy; return `None` if not.
     b. Check `used_count < unit_capacity`; if not, return `None`
        (zone full).
     c. Scan bitmap from `last_pos_64` (rotating cursor), wrapping
        around:
       - For each 64-bit word at index `i` (starting from
         `last_pos_64`, wrapping): load the word, use `countr_one` to
         find the first zero bit (hardware-optimized on x86/ARM).
       - For `unit_count == 1` (common case, since v1 enforces
         `allocate_granularity == block_size_bytes`): CAS-set the bit
         via `compare_exchange` on the 64-bit word. On success:
         increment `used_count`, store `last_pos_64 = i`, return
         `AllocatedRange { unit_offset: bit_index, unit_count: 1 }`.
         On CAS failure (another thread set the same bit): retry the
         same word (the bit may now be set; re-scan from
         `countr_one`).
       - For `unit_count > 1`: find a run of `unit_count` consecutive
         zero bits (may span word boundaries). CAS-set each bit in
         the run; if any CAS fails, clear the bits already set in
         this attempt and continue scanning. On success: increment
         `used_count` by `unit_count`, store `last_pos_64`, return
         `AllocatedRange`.
     d. **CAS retry bound (§8):** per-bit CAS is capped at
        `cas_retry_limit` retries (config, default 100); on
        exhaustion, fall through to the next bit / word / zone. This
        prevents indefinite spinning under heavy contention. The
        `zone.allocate.retry.cms.bit` counter (§11) is incremented on
        each retry as the key operational signal for lock-free
        allocator contention.
     e. If no free bits found after a full wrap, return `None`.
   - `free(unit_offset: u64, unit_count: u32) -> bool` — clear bits
     via CAS on each 64-bit word. Decrement `used_count`. Return
     `false` if any bit was already clear (double-free detection).
   - `allocatable() -> bool` — `zone_state == Healthy && used_count
     < unit_capacity`.
   - `derived_alloc_state() -> ZoneAllocationState` — returns
     `Active` (used_count == 0), `Available` (0 < used_count <
     unit_capacity), or `Full` (used_count == unit_capacity). Used
     for `ZoneValue` snapshots and reporting only — no CAS, no state
     machine (§9).

2. **Rotating active-zone-set + disk-level allocate** — implement
   in `app/crow-diskdb/src/node/disk.rs` (extends R71's `ZoneDisk`),
   following the buzz-cpp `ddb_disk` pattern (§8):
   - `ActiveZoneContext` — `Vec<ZoneRef>` holding
     `zone_rotate_count` zones (the "active set"). Replaced as a
     whole via RCU-style publish (swap + atomic pointer store).
   - `ZoneDisk` fields:
     - `zones: RwLock<Vec<ZoneRef>>` — all zones on the disk.
     - `active_zone_context: RwLock<Arc<ActiveZoneContext>>` — the
       current active set (RCU read: clone Arc, no lock held during
       allocation).
     - `pos_v_zone_ctx: AtomicU64` — rotating cursor over the active
       set (round-robin).
     - `pos_v_zone: AtomicU64` — rotating cursor for zone rotation
       scan (advances each rotation to spread wear).
   - `disk_allocate(unit_count: u32) -> Option<(ZoneRef,
     AllocatedRange)>`:
     a. Check disk `HwStatus` is `Up`; return `None` if not.
     b. `max_loop = zone_num / zone_rotate_count + 2`.
     c. Loop while `max_loop > 0`:
       - Load current `active_zone_context` (Arc clone, RCU read).
       - `start = pos_v_zone_ctx.fetch_add(1, Relaxed)`.
       - For `i` in `start .. start + ctx.len()`: select
         `ctx[i % ctx.len()]`, call `zone.allocate(unit_count)`. On
         success: record metrics, return `(zone, range)`.
       - All zones in the active set failed → call
         `rotate_active_zones(&ctx)`. If rotation returns `false`
         (no allocatable zones), break. Otherwise continue loop with
         new context.
     d. Return `None`.
   - `rotate_active_zones(old_ctx: &ActiveZoneContext) -> bool`:
     a. Take write lock on `active_zone_context`.
     b. RCU check: if the current context is no longer `old_ctx`,
        another thread already rotated — return `true` (caller
        retries with the new context).
     c. Scan all zones from `pos_v_zone` (rotating start), wrapping:
       pick the first `zone_rotate_count` zones where
       `allocatable()` is true. Advance `pos_v_zone` by the number
       of zones scanned.
     d. If no allocatable zones found: store empty context, return
        `false`.
     e. Build new `ActiveZoneContext`, swap into
        `active_zone_context` (RCU publish), return `true`.
   - `free(zone_index, unit_offset, unit_count) -> bool` — look up
     zone by index, call `zone.free()`.
   - `rebuild_active_zones()` — scan all zones, build initial
     `ActiveZoneContext` with the first `zone_rotate_count`
     allocatable zones. Called by R73's recovery on startup.

3. **Round-robin across disks within the named disk-group** —
   implement in `app/crow-diskdb/src/node/mod.rs` (extends R71's
   `Node`), following the buzz-cpp `ddb_node` pattern and design §8.
   The `AllocateBlocks` request specifies the target `disk_group_id`
   (§3.2) — diskdb never round-robins across disk-groups; it
   round-robins across the disks **within that one named disk-group**:
   - `AllocateDiskContext` — `Vec<Arc<ZoneDisk>>` holding all
     allocatable disks **within one named disk-group**. Replaced via
     RCU-style publish on add/remove/status-change.
   - `Node` fields:
     - `disks: RwLock<Vec<Arc<ZoneDisk>>>` — all disks.
     - `allocating_disks: RwLock<Arc<AllocateDiskContext>>` — RCU
       context of allocatable disks (scoped to the request's
       `disk_group_id`).
     - `pos_v_disk_ctx: AtomicU64` — rotating cursor over the
       allocatable disk context.
   - `allocate_block(disk_group_id, unit_count, exclude_disks:
     &[DiskId]) -> Result<(Arc<ZoneDisk>, ZoneRef,
     AllocatedRange)>`:
     a. Read-lock `allocating_disks` (Arc clone, then drop lock);
        check non-empty (else `NoSpace`).
     b. `start = pos_v_disk_ctx.fetch_add(1, Relaxed)`.
     c. For `i` in `start .. start + ctx.len()`: select
        `ctx[i % ctx.len()]`, skip if `disk_id` in `exclude_disks`
        (anti-affinity, per-disk — skip a disk that just failed,
        applied within the named disk-group), call
        `disk_allocate(unit_count)`. On success, return.
     d. If no disk succeeded, return `NoSpace`.
   - `allocate_blocks(disk_group_id, unit_count, count,
     exclude_disks) -> Result<Vec<(Arc<ZoneDisk>, ZoneRef,
     AllocatedRange)>>`:
     a. For each of `count` allocations: round-robin select disk
        via `pos_v_disk_ctx.fetch_add(1)`, skip excluded, call
        `disk_allocate(unit_count)`.
     b. Collect successful claims. If not all `count` claimed,
        retry remaining with a full scan (random start, skip
        excluded and already-used disks).
     c. If still not all claimed, return `NoSpace` (caller decides
        whether to partial-commit).
   - `free_block(segment: &Segment) -> Result<bool>` — look up disk
     by `segment.disk_id` via the disk-id → disk hash map (§14),
     call `disk.free(segment.zone_index, segment.unit_offset,
     segment.unit_count)`.
   - `refresh_disk_context()` — scan all disks, build new
     `AllocateDiskContext` with disks where `HwStatus == Up`. Swap
     in (RCU publish). Called on add/remove disk and on status
     change from the sync loop (R71).

4. **Record persistence (KV operations)** — create
   `app/crow-diskdb/src/persistence/` module. diskdb has no "journal"
   abstraction of its own — it performs plain KV put/delete operations
   on the bound data group via `CrowkvClient`; crow-kv's paxos journal
   is the durability mechanism (§1). The "journal" framing (a sequence
   of puts/deletes that can be replayed in slot order) is how diskdb
   *uses* crow-kv's slot-ordered KV, not a concept exposed in diskdb's
   code.
   - `DataGroupClient` — wraps `CrowkvClient` for put/delete/scan on
     the disk-group's bound paxos data group (parallels R71's
     `SysdataClient` for group 0). Uses the `(store_id, group_id)`
     from the binding map (R71).
   - `persist_busy(dg_id, bind, disk_id, zone_idx, unit_offset,
     value: &BusyBlockValue) -> Result<()>` — `put` to
     `BusyBlockKey { disk_id, zone_idx, unit_offset }` on the bound
     data group. The key is the existing binary key from R70
     (`lib/crow-protocol/src/key/diskdb.rs`), not a slot-based key.
     The `unit_offset` uniquely identifies the block range within
     the zone; crow-kv's slot mechanism provides write ordering for
     replay (R73).
   - `persist_busy_batch(dg_id, bind, records: &[(disk_id, zone_idx,
     unit_offset, BusyBlockValue)]) -> Result<()>` — `batch_write`
     of all records in one async round-trip. Used for multi-block
     allocation (one `batch_write` per data group; atomic within the
     group, §3.2).
   - `persist_free(dg_id, bind, disk_id, zone_idx, unit_offset,
     value: &FreeBlockValue) -> Result<()>` — one `batch_write` that
     **deletes the `BusyBlockKey`** and **puts the `FreeBlockValue`**
     at `FreeBlockKey { disk_id, zone_idx, unit_offset }` on the bound
     data group (per the record model in §3.4/§7). Used for immediate
     free in v1.
   - `persist_free_batch(dg_id, bind, records: &[(disk_id, zone_idx,
     unit_offset, FreeBlockValue)]) -> Result<()>` — `batch_write`
     that, for each record, deletes the `BusyBlockKey` and puts the
     `FreeBlockKey` value in one async round-trip. Used for multi-block
     free (one `batch_write` per data group). When R79 ships, this is
     reused by the size-threshold free batch flush.
   - `read_zone_records(dg_id, bind, disk_id, zone_idx) ->
     Result<ZoneRecords>` — prefix scan
     `BusyBlockKey::prefix_for_zone(disk_id, zone_idx)` and
     `FreeBlockKey::prefix_for_zone(disk_id, zone_idx)` to fetch all
     busy/free records for one zone, plus the latest `ZoneValue`
     snapshot at `ZoneKey { disk_id, zone_idx }`. Used by R73's
     recovery. (This requirement defines the method; R73 implements
     the replay logic.)
   - `delete_free_records_batch(dg_id, bind, keys: &[Vec<u8>]) ->
     Result<()>` — `batch_write` with `Delete` ops for free records
     only. Used by R73's snapshot compaction (compaction deletes only
     free records; busy records for freed blocks were already deleted
     on free, §7).

5. **Two-phase async allocation** — implement in
   `app/crow-diskdb/src/persistence/alloc.rs`:
   - `allocate_block(node: &Arc<Node>, disk_group_id, unit_count:
     u32, owner_chunk: &ChunkId, unit_size: u32, kv:
     &DataGroupClient) -> Result<Segment>`:
     a. **Phase 1 (sync)**: `node.allocate_block(disk_group_id,
        unit_count, &[])` → `(disk, zone, range)`. Bits are set in
        the zone's `usage_bits` via per-bit CAS. No zone-level lock
        — other threads can allocate concurrently from the same
        zone.
     b. **Phase 2 (async)**: Build `BusyBlockValue { unit_count,
        unit_size, owner_chunk, state: BlockState::Ok }` (§7). `await
        kv.persist_busy(dg_id, bind, disk_id, zone_idx,
        range.unit_offset, &value)`.
     c. On success: return `Segment { disk_id, zone_index,
        unit_offset: range.unit_offset, unit_count: range.unit_count,
        owner_chunk }` (no `node_id`/`disk_group_id` in `Segment`,
        §3.9).
     d. On failure: `zone.free(range.unit_offset, range.unit_count)`
        (rollback — clear the bits that were set in Phase 1), return
        error.
   - `allocate_blocks(node, disk_group_id, unit_count, count,
     exclude_disks, owner_chunk, unit_size, kv) ->
     Result<Vec<Segment>>`:
     a. Phase 1: `node.allocate_blocks(disk_group_id, unit_count,
        count, exclude_disks)` → `Vec<(disk, zone, range)>`.
     b. Phase 2: Build `BusyBlockValue` for each claim. Collect all
        `(disk_id, zone_idx, unit_offset, value)` tuples. `await
        kv.persist_busy_batch(dg_id, bind, &records)` (one
        `batch_write` per data group; atomic within the group).
     c. On success: return `Vec<Segment>`.
     d. On failure: `zone.free()` ALL claims (rollback every bit
        that was set), return error.
   - The `dg_id` and `bind` come from the `Node` struct (set by
     R71's sync loop from the binding map).

6. **Immediate free** — implement in
   `app/crow-diskdb/src/persistence/free.rs` (v1: no `FreeBatch`, no
   timer, no background flush loop — §8):
   - `free_block(node: &Arc<Node>, segment: &Segment, kv:
     &DataGroupClient) -> Result<()>`:
     a. `node.free_block(segment)` — clear bitmap locally (per-bit
        CAS clear) via the disk-id → disk hash map and zone-index →
        zone vec (§14; O(1) lookups).
     b. Build `FreeBlockValue { unit_count, previous_owner:
        segment.owner_chunk }` (§7; `previous_owner` comes from the
        `Segment` — no KV read needed; no `state` field — a free
        block has no data).
     c. `await kv.persist_free(dg_id, bind, disk_id, zone_idx,
        unit_offset, &value)` — one `batch_write` that **deletes the
        `BusyBlockKey`** and **writes the `FreeBlockValue`** at
        `FreeBlockKey` (per §3.4/§7).
     d. Return the persist result. Free is synchronous in v1 — the
        caller's `FreeBlocks` RPC returns only after the
        `FreeBlockValue` is durable and the `BusyBlockKey` is gone.
   - `free_blocks(node, segments: &[Segment], kv) ->
     Result<()>`:
     a. For each segment: `node.free_block(segment)` (clear bitmap
        locally).
     b. Group by `dg_id` and build `FreeBlockValue` list per data
        group.
     c. For each affected data group, `await
        kv.persist_free_batch(...)` (one `batch_write` that
        deletes each `BusyBlockKey` and writes each `FreeBlockValue`).
     d. On failure: the bitmap clears already happened locally —
        return error; the §12 ghost-allocation scanner reconciles
        any in-memory/KV mismatch on restart.
   - **No KV read on free in v1 (§14):** `owner_chunk` is carried in
     the `Segment` and becomes `FreeBlockValue.previous_owner`, so the
     free is one `batch_write` (Delete `BusyBlockKey` + Put
     `FreeBlockKey`) with no prior read. Ownership validation is
     deferred to the §12 scanner. If strict ownership validation is
     needed before free, a config toggle (`validate_owner_on_free`,
     default false) enables a KV read of the `BusyBlockValue` first
     (one paxos round-trip, doubles free latency).
   - **Free batching (R79):** when `free_batch_enabled` is true
     (default false), the free path groups frees into a batch and
     flushes via one `batch_write` when the batch reaches
     `free_flush_max_batch` (default 256). No timer. R72 ships with
     the toggle off — immediate free only.

7. **gRPC handlers** — implement in
   `app/crow-diskdb/src/grpc/service.rs`:
   - `allocate_blocks` — validate `unit_count` (non-zero, aligned to
     block size) and `count` (1–1024), check not degraded, get node,
     call `persistence::allocate_blocks()`, return `Vec<Segment>`.
   - `free_blocks` — parse `Vec<Segment>`, get node, call
     `persistence::free_blocks()` (immediate free in v1).
   - `query_capacity_stats` — stub (returns empty); R74 fills it in.
   - `get_disk_group_info` / `get_disk_info` — read from synced
     cache (R71).
   - `rebuild_zone_bitmap` — stub (`Unimplemented`); R73 implements
     strategy 1 (full scan rebuild, §7).
   - `mark_block_suspect` / `mark_block_corrupt` — stub
     (`Unimplemented`); R75 implements per-block state transitions
     (§7, §12).
   - Error mapping: `NoSpace` → `ResourceExhausted`, `NotOwner` →
     `PermissionDenied`, `InvalidSize`/`InvalidCount` →
     `InvalidArgument`, `Degraded` → `Unavailable`.

8. **Server wiring** — update `app/crow-diskdb/src/main.rs`:
   - Create `DataGroupClient` from `CrowkvClient`.
   - Wire gRPC service with `NodeContainer`, `DataGroupClient`,
     config.
   - Allocate/free RPCs now functional.
   - No `FreeBatch`, no `FreeFlushLoop` in v1 (R79 adds the
     size-threshold batch when `free_batch_enabled` is true).

**Scope** (expected changed files):
- `app/crow-diskdb/src/zone/mod.rs` — `Zone` struct with
  bitmap-scan `allocate`, `free`, `allocatable`, `derived_alloc_state`.
- `app/crow-diskdb/src/node/disk.rs` — `ZoneDisk` with
  `ActiveZoneContext`, `disk_allocate`, `rotate_active_zones`,
  `rebuild_active_zones`.
- `app/crow-diskdb/src/node/mod.rs` — `Node` with
  `AllocateDiskContext`, `allocate_block`, `allocate_blocks`,
  `free_block`, `refresh_disk_context`.
- `app/crow-diskdb/src/persistence/mod.rs` — `DataGroupClient`.
- `app/crow-diskdb/src/persistence/alloc.rs` — two-phase async
  allocation.
- `app/crow-diskdb/src/persistence/free.rs` — immediate free
  (single + multi-block).
- `app/crow-diskdb/src/grpc/service.rs` — allocate/free handlers +
  stubs for R73/R75 RPCs.
- `app/crow-diskdb/src/grpc/mod.rs` — wire service struct.
- `app/crow-diskdb/src/lib.rs` — module declarations.
- `app/crow-diskdb/src/config.rs` — add `zone_rotate_count`,
  `cas_retry_limit` (default 100), `validate_owner_on_free`
  (default false) to `StorageDefaults`. Reserve
  `free_batch_enabled` (default false) and `free_flush_max_batch`
  (default 256) for R79.
- `app/crow-diskdb/Cargo.toml` — add `rand` (for retry random
  start).
- `app/crow-diskdb/src/main.rs` — wire `DataGroupClient`.

**Complexity**: High. The bitmap-scan allocator, per-bit CAS, and
zone rotation patterns are well-proven in the buzz-cpp reference
(`ddb_disk_zone::allocate_block`, `ddb_disk::rotate_active_zones`).
The new work is the record-based persistence (§3.4): instead of
saving a full bitmap to a local file on a schedule, diskdb writes a
small `BusyBlockValue` to the paxos data group on each allocate and,
on each free, deletes the `BusyBlockKey` and writes a `FreeBlockValue`
(carries `previous_owner` for audit) in one `batch_write`. The
`unit_offset`-based key layout (from R70) enables prefix-scan replay
(R73) via crow-kv's slot ordering. The two-phase async pattern (sync
bitmap claim + async KV persist) requires rollback on failure (clear
the claimed bits). The CAS retry bound (§8) prevents indefinite
spinning under heavy contention.

**Dependencies**: R70 (core types, bitmap, config, key layout), R71
(NodeContainer, sync loop, server binary, SysdataClient). No
dependency on R73–R77. R79 (free batch) depends on this requirement's
free path and `persist_free_batch`.

**Acceptance**:
- `Zone::allocate()` scans the bitmap from `last_pos_64`, finds a
  free bit via `countr_one`, CAS-sets it, returns `AllocatedRange`.
  Unit test: concurrent allocations on the same zone serialize via
  per-bit CAS (no double-alloc, all bits unique).
- `Zone::free()` clears bits and detects double-free. Unit test:
  free an allocated bit, then free again → `false`.
- `Zone::allocate(unit_count > 1)` finds consecutive free bits.
  Unit test: allocate a multi-unit range, verify bits are contiguous.
- `Zone::allocate()` respects the CAS retry bound: after
  `cas_retry_limit` retries on one bit, falls through to the next
  bit / word / zone. Unit test: force CAS contention, verify the
  `zone.allocate.retry.cms.bit` counter increments and the allocate
  does not spin indefinitely.
- `ZoneDisk::disk_allocate()` round-robins over the active zone
  set, rotates when exhausted via `rotate_active_zones()`. Unit
  test: fill all zones in the active set, verify rotation picks new
  zones.
- `Node::allocate_block()` round-robins across disks within the
  named disk-group. `allocate_blocks()` distributes across disks,
  respects `exclude_disks` (anti-affinity within the disk-group).
- `allocate_block()` two-phase: Phase 1 sync bitmap claim, Phase 2
  async `BusyBlockValue` persist. On persist failure, rollback
  (clear bits). Integration test with in-process crow-kv: allocate
  a block, verify `BusyBlockValue` appears in the data group at the
  expected `BusyBlockKey` with fields `{ unit_count, unit_size,
  owner_chunk, state: Ok }`.
- `allocate_blocks()` multi-block: one `batch_write` for all
  `BusyBlockValue`s. Integration test: allocate N blocks, verify all
  records in one batch.
- `free_block()` clears bitmap locally, then persists
  `FreeBlockValue` at `FreeBlockKey` and **deletes the
  `BusyBlockKey`** in one `batch_write`. Integration test: free a
  block, verify the `FreeBlockValue` appears in the data group
  (carrying `previous_owner`) and the corresponding `BusyBlockKey` is
  **deleted**.
- `free_blocks()` multi-block: one `batch_write` per data group that
  deletes each `BusyBlockKey` and writes each `FreeBlockValue`.
  Integration test: free N blocks, verify all `FreeBlockValue`s in
  one batch per group and `BusyBlockKey`s are deleted.
- gRPC handlers: `allocate_blocks`, `free_blocks` functional via
  gRPC. Error mapping correct (`NoSpace` → `ResourceExhausted`,
  etc.). `rebuild_zone_bitmap`, `mark_block_suspect`,
  `mark_block_corrupt` return `Unimplemented` (R73/R75).
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.
