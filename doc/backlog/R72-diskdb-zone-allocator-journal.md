<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R72: diskdb — Zone Allocator + Record Persistence (Block Allocate/Free)

**Status**: Implemented (commit `539f596` + `248989b` e2e test). This
doc is refined to reflect the actual implementation; remaining gaps are
listed in the final section.

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

1. **Zone bitmap-scan allocator** — `app/crow-diskdb/src/zone.rs`
   (single file, per the working design §4.0 flat-file convention):
   - `Zone` — per-zone allocation state (extends R70's types):
     - `disk_id: DiskId`, `zone_index: u32`, `disk_group_id:
       DiskGroupId`.
     - `zone_state: RwLock<ZoneHealth>` — health
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
     - `snapshot_slot: AtomicU64` — last compacted snapshot slot
       (R73; maintained by compaction, not the hot path).
     - `uncompacted_free_record_count: AtomicU32` — compaction
       backlog gauge (§11). Incremented on free; decremented by R73
       compaction.
     - `cas_retry_count: AtomicU64` — per-zone CAS retry counter
       (§11: `zone.allocate.retry.cms.bit`). Incremented on each
       failed `cas_bit` in the allocate path.
     - `metrics_cas_retry: Option<Arc<Counter>>` — optional
       crow-common metrics handle for CAS retries. When `Some`, the
       allocate path increments this counter on each CAS retry.
   - `allocate(unit_count: u32, cas_retry_limit: u32) ->
     Option<AllocatedRange>` — Phase 1 (sync), per-bit CAS (buzz-cpp
     `ddb_disk_zone::allocate_block` pattern, §8):
     a. Check `zone_state` is Healthy and `used_count < unit_capacity`;
        return `None` if not. Also reject `unit_count == 0` or
        `unit_count > unit_capacity`.
     b. Scan bitmap from `last_pos_64` (rotating cursor), wrapping
        around:
       - For each 64-bit word at index `i` (starting from
         `last_pos_64`, wrapping): load the word, use `trailing_ones`
         (Rust's `countr_one` equivalent) to find the first zero bit.
       - For `unit_count == 1` (common case, since v1 enforces
         `allocate_granularity == block_size_bytes`): CAS-set the bit
         via `cas_bit`. On success: increment `used_count`, store
         `last_pos_64 = i`, return `AllocatedRange`. On CAS failure
         (another thread set the same bit): retry the same word (the
         bit may now be set; re-scan from `trailing_ones`).
       - For `unit_count > 1`: find a run of `unit_count` consecutive
         zero bits (may span word boundaries). CAS-set each bit in
         the run via `try_claim_range`; if any CAS fails, clear the
         bits already set in this attempt and continue scanning. On
         success: increment `used_count` by `unit_count`, store
         `last_pos_64`, return `AllocatedRange`.
     c. **CAS retry bound (§8):** per-bit CAS is capped at
        `cas_retry_limit` retries (config, default 100); on
        exhaustion, fall through to the next bit / word / zone. This
        prevents indefinite spinning under heavy contention. The
        `cas_retry_count` atomic and the optional
        `metrics_cas_retry` counter (§11) are incremented on each
        retry as the key operational signal for lock-free allocator
        contention.
     d. If no free bits found after a full wrap, return `None`.
   - `free(unit_offset: u64, unit_count: u32) -> bool` — clear bits
     via CAS on each 64-bit word. Decrement `used_count`. Increment
     `uncompacted_free_record_count`. Return `false` if any bit was
     already clear (double-free detection).
   - `allocatable() -> bool` — `zone_state == Healthy && used_count
     < unit_capacity`.
   - `derived_alloc_state() -> ZoneAllocationState` — returns
     `ZoneAllocActive` (used_count == 0), `ZoneAllocAvailable` (0 <
     used_count < unit_capacity), or `ZoneAllocFull` (used_count ==
     unit_capacity). Used for `ZoneValue` snapshots and reporting
     only — no CAS, no state machine (§9).
   - `set_health(health: ZoneHealth)` — called by the sync loop /
     health probe to update zone health.

2. **Rotating active-zone-set + disk-level allocate** —
   `app/crow-diskdb/src/node/disk.rs` (extends R71's `ZoneDisk`),
   following the buzz-cpp `ddb_disk` pattern (§8):
   - `ActiveZoneContext` — `Vec<Arc<Zone>>` holding
     `zone_rotate_count` zones (the "active set"). Replaced as a
     whole via RCU-style publish (Arc swap).
   - `ZoneDisk` fields:
     - `disk_id`, `disk_group_id`, `node_id`, `rack_id` — identity.
     - `disk_value: RwLock<DiskValue>` — the disk's metadata.
     - `zones: RwLock<Vec<Arc<Zone>>>` — all zones on the disk,
       indexed by `zone_index`.
     - `active_zone_context: RwLock<Arc<ActiveZoneContext>>` — the
       current active set (RCU read: clone Arc, no lock held during
       allocation).
     - `pos_v_zone_ctx: AtomicU64` — rotating cursor over the active
       set (round-robin).
     - `pos_v_zone: AtomicU64` — rotating cursor for zone rotation
       scan (advances each rotation to spread wear).
     - `effective_status: RwLock<HwStatus>` — effective status for
       this disk (node/group/disk combined).
   - `disk_allocate(unit_count: u32, cas_retry_limit: u32,
     zone_rotate_count: u32) -> Option<(Arc<Zone>,
     AllocatedRange)>`:
     a. Check `effective_status` is `Up`; return `None` if not.
     b. `max_loop = zone_num / zone_rotate_count + 2`.
     c. Loop while `max_loop > 0`:
       - Load current `active_zone_context` (Arc clone, RCU read).
         If empty, call `rotate_active_zones(&ctx, zone_rotate_count)`;
         if it returns `false`, return `None`; otherwise continue.
       - `start = pos_v_zone_ctx.fetch_add(1, Relaxed)`.
       - For `i` in `start .. start + ctx.len()`: select
         `ctx[i % ctx.len()]`, call
         `zone.allocate(unit_count, cas_retry_limit)`. On success:
         return `(zone, range)`.
       - All zones in the active set failed → call
         `rotate_active_zones(&ctx, zone_rotate_count)`. If rotation
         returns `false` (no allocatable zones), return `None`.
         Otherwise continue loop with new context.
     d. Return `None`.
   - `rotate_active_zones(old_ctx: &Arc<ActiveZoneContext>,
     zone_rotate_count: u32) -> bool`:
     a. RCU check (double-checked locking): read-lock
        `active_zone_context`; if the current context is no longer
        `old_ctx` (via `Arc::ptr_eq`), another thread already
        rotated — return `true` (caller retries with the new
        context). Then take write lock and re-check.
     b. Scan all zones from `pos_v_zone` (rotating start), wrapping:
       pick the first `zone_rotate_count` zones where
       `allocatable()` is true. Advance `pos_v_zone` by `zone_num`.
     c. If no allocatable zones found: store empty context, return
        `false`.
     d. Build new `ActiveZoneContext`, swap into
        `active_zone_context` (RCU publish), return `true`.
   - `free(zone_index, unit_offset, unit_count) -> bool` — look up
     zone by index, call `zone.free()`.
   - `rebuild_active_zones(zone_rotate_count: u32)` — scan all
     zones, build initial `ActiveZoneContext` with the first
     `zone_rotate_count` allocatable zones. Called by disk-add init
     (§3.5) and R73's recovery on startup.
   - `set_effective_status(status: HwStatus)` — called by
     `StatusManager`. When `Bad`, marks all zones `Bad`.
   - `add_zone(zone: Arc<Zone>)` — add a zone to this disk.

3. **Round-robin across disks within the named disk-group** —
   `app/crow-diskdb/src/node.rs` (extends R71's `Node`), following
   the buzz-cpp `ddb_node` pattern and design §8. The
   `AllocateBlocks` request specifies the target `disk_group_id`
   (§3.2) — the gRPC handler looks up the `Node` by `disk_group_id`
   first, then calls `node.allocate_block`. diskdb never round-robins
   across disk-groups; it round-robins across the disks **within that
   one named disk-group** (the `Node` IS one disk-group):
   - `AllocateDiskContext` — `Vec<Arc<ZoneDisk>>` holding all
     allocatable disks **within one named disk-group**. Replaced via
     RCU-style publish on add/remove.
   - `AllocClaim` — `(Arc<ZoneDisk>, Arc<Zone>, AllocatedRange)`,
     the result of a successful allocation.
   - `Node` fields:
     - `disk_group_id: DiskGroupId`, `node_id: u64`, `rack_id: u64`
       — identity.
     - `status: RwLock<HwStatus>` — node-level status.
     - `bind: RwLock<(u64, u64)>` — `(store_id, group_id)` for the
       bound paxos data group (set by the sync loop from the binding
       map).
     - `disks: RwLock<Vec<Arc<ZoneDisk>>>` — all disks.
     - `disk_index: RwLock<HashMap<DiskId, Arc<ZoneDisk>>>` — O(1)
       disk-id → disk lookup for the free path (§14).
     - `allocating_disks: RwLock<Arc<AllocateDiskContext>>` — RCU
       context of allocatable disks within this disk-group.
     - `pos_v_disk_ctx: AtomicU64` — rotating cursor over the
       allocatable disk context.
   - `allocate_block(unit_count: u32, exclude_disks: &[DiskId],
     cas_retry_limit: u32, zone_rotate_count: u32) ->
     Result<AllocClaim, AllocError>`:
     a. Check `status == Up`; return `NoSpace` if not.
     b. Read-lock `allocating_disks` (Arc clone, then drop lock);
        check non-empty (else `NoSpace`).
     c. `start = pos_v_disk_ctx.fetch_add(1, Relaxed)`.
     d. For `i` in `start .. start + ctx.len()`: select
        `ctx[i % ctx.len()]`, skip if `disk_id` in `exclude_disks`
        (anti-affinity, per-disk — skip a disk that just failed,
        applied within the named disk-group), call
        `disk_allocate(unit_count, cas_retry_limit,
        zone_rotate_count)`. On success, return `(disk, zone,
        range)`.
     e. If no disk succeeded, return `NoSpace`.
   - `allocate_blocks(unit_count: u32, count: u32, exclude_disks:
     &[DiskId], cas_retry_limit: u32, zone_rotate_count: u32) ->
     Result<Vec<AllocClaim>, AllocError>`:
     a. First pass: round-robin. For each of `count` allocations:
        call `allocate_block(unit_count, &used_disks, ...)`. On
        success, add the disk to `used_disks` (anti-affinity — no
        disk reused within one `allocate_blocks` call).
     b. If not all `count` claimed, second pass: full scan (random
        start via `rand::random_range`, skip excluded + used disks).
        Repeat until all claimed or no progress.
     c. If still not all claimed, return `NoSpace` (caller decides
        whether to partial-commit).
   - `free_block(disk_id: &DiskId, zone_index: u32, unit_offset:
     u64, unit_count: u32) -> bool` — look up disk by `disk_id` via
     the `disk_index` hash map (§14; O(1) lookup), call
     `disk.free(zone_index, unit_offset, unit_count)`.
   - `add_disk(disk: Arc<ZoneDisk>)` — add a disk, rebuild the
     allocatable disk set.
   - `rebuild_allocating_disks()` — scan all disks, build new
     `AllocateDiskContext` with disks where `allocatable()` is true.
     Swap in (RCU publish). Called on add/remove disk. (Spec name
     was `refresh_disk_context`; impl name is
     `rebuild_allocating_disks`.)
   - `AllocError` — `NoSpace` (no disk/zone can satisfy the
     request). The gRPC handler maps this to `ResourceExhausted`.

4. **Record persistence (KV operations)** —
   `app/crow-diskdb/src/persistence.rs` (single file, per the
   working design §4.0). diskdb has no "journal" abstraction of its
   own — it performs plain KV put/delete operations on the bound data
   group via `CrowkvClient`; crow-kv's paxos journal is the durability
   mechanism (§1). The "journal" framing (a sequence of puts/deletes
   that can be replayed in slot order) is how diskdb *uses* crow-kv's
   slot-ordered KV, not a concept exposed in diskdb's code.
   - `Bind` — type alias for `(u64, u64)` = `(store_id, group_id)`.
   - `BusyRecord` / `FreeRecord` / `ZoneRecords` — decoded record
     structs returned by `read_zone_records` (used by R73).
   - `DataGroupClient` — wraps `Arc<CrowkvClient>` for put/delete/
     scan on the disk-group's bound paxos data group (parallels R71's
     `SysdataClient` for group 0). Uses the `(store_id, group_id)`
     from `Node.bind` (set by the sync loop from the binding map).
   - `persist_busy(bind, disk_id, zone_idx, unit_offset, value:
     &BusyBlockValue) -> Result<()>` — one `batch_write` that **puts
     the `BusyBlockValue`** at `BusyBlockKey { disk_id, zone_idx,
     unit_offset }` and **deletes any prior `FreeBlockKey`** for the
     same offset (re-allocation of a freed block, per §3.4). The key
     is the existing binary key from R70
     (`lib/crow-protocol/src/key/diskdb.rs`), encoded via
     `to_bytes()`. Values are bincode-serialized.
   - `persist_busy_batch(bind, records: &[(DiskId, u32, u64,
     BusyBlockValue)]) -> Result<()>` — `batch_write` of all records
     in one async round-trip (each record = Put `BusyBlockKey` +
     Delete `FreeBlockKey`). Used for multi-block allocation (one
     `batch_write` per data group; atomic within the group, §3.2).
   - `persist_free(bind, disk_id, zone_idx, unit_offset, value:
     &FreeBlockValue) -> Result<()>` — one `batch_write` that
     **deletes the `BusyBlockKey`** and **puts the `FreeBlockValue`**
     at `FreeBlockKey { disk_id, zone_idx, unit_offset }` on the
     bound data group (per the record model in §3.4/§7). Used for
     immediate free in v1.
   - `persist_free_batch(bind, records: &[(DiskId, u32, u64,
     FreeBlockValue)]) -> Result<()>` — `batch_write` that, for each
     record, deletes the `BusyBlockKey` and puts the `FreeBlockKey`
     value in one async round-trip. Used for multi-block free (one
     `batch_write` per data group). When R79 ships, this is reused
     by the size-threshold free batch flush.
   - `put_zone(bind, disk_id, zone_idx, value: &ZoneValue) ->
     Result<()>` — `put` a `ZoneValue` snapshot at `ZoneKey`. Used
     by the sync loop's disk-add init to write baseline `ZoneValue`
     records (empty bitmap, `snapshot_slot = 0`).
   - `read_zone_records(bind, disk_id, zone_idx) ->
     Result<ZoneRecords>` — point-lookup `ZoneValue` via `get`, then
     prefix scan `BusyBlockKey::prefix_for_zone(disk_id, zone_idx)`
     and `FreeBlockKey::prefix_for_zone(disk_id, zone_idx)` to fetch
     all busy/free records for one zone. Used by R73's recovery.
     (This requirement defines the method; R73 implements the replay
     logic.)
   - `delete_free_records_batch(bind, keys: &[Vec<u8>]) ->
     Result<()>` — `batch_write` with `Delete` ops for free records
     only. Used by R73's snapshot compaction (compaction deletes only
     free records; busy records for freed blocks were already deleted
     on free, §7).

5. **Two-phase async allocation** —
   `app/crow-diskdb/src/persistence.rs` (free functions, not a
   separate `alloc.rs` module):
   - `allocate_block(node: &Arc<Node>, unit_count: u32, owner_chunk:
     &ChunkId, unit_size: u32, kv: &DataGroupClient, cas_retry_limit:
     u32, zone_rotate_count: u32) -> Result<Segment, AllocError>`:
     a. **Phase 1 (sync)**: `node.allocate_block(unit_count, &[],
        cas_retry_limit, zone_rotate_count)` → `(disk, zone, range)`.
        Bits are set in the zone's `usage_bits` via per-bit CAS. No
        zone-level lock — other threads can allocate concurrently
        from the same zone.
     b. **Phase 2 (async)**: Build `BusyBlockValue { unit_count,
        unit_size, owner_chunk: Some(*owner_chunk), state:
        BlockState::Ok }` (§7). `bind = *node.bind.read()`. `await
        kv.persist_busy(bind, &disk.disk_id, zone.zone_index,
        range.unit_offset, &value)`.
     c. On success: return `Segment { disk_id: Some(disk.disk_id),
        zone_index, unit_offset, unit_count, owner_chunk:
        Some(*owner_chunk) }` (no `node_id`/`disk_group_id` in
        `Segment`, §3.9).
     d. On failure: `zone.free(range.unit_offset, range.unit_count)`
        (rollback — clear the bits that were set in Phase 1), return
        `AllocError::NoSpace`.
   - `allocate_blocks(node, unit_count, count, exclude_disks,
     owner_chunk, unit_size, kv, cas_retry_limit, zone_rotate_count)
     -> Result<Vec<Segment>, AllocError>`:
     a. Phase 1: `node.allocate_blocks(unit_count, count,
        exclude_disks, cas_retry_limit, zone_rotate_count)` →
        `Vec<AllocClaim>`.
     b. Phase 2: Build `BusyBlockValue` for each claim. Collect all
        `(disk_id, zone_idx, unit_offset, value)` tuples. `bind =
        *node.bind.read()`. `await kv.persist_busy_batch(bind,
        &records)` (one `batch_write` per data group; atomic within
        the group).
     c. On success: return `Vec<Segment>`.
     d. On failure: `zone.free()` ALL claims (rollback every bit
        that was set), return `AllocError::NoSpace`.
   - The `bind` comes from `Node.bind` (set by R71's sync loop from
     the binding map).

6. **Immediate free** — `app/crow-diskdb/src/persistence.rs` (free
   functions; v1: no `FreeBatch`, no timer, no background flush loop
   — §8):
   - `free_block(node: &Arc<Node>, segment: &Segment, kv:
     &DataGroupClient) -> Result<()>`:
     a. Extract `disk_id` from `segment.disk_id` (error if missing).
     b. `node.free_block(&disk_id, segment.zone_index,
        segment.unit_offset, segment.unit_count)` — clear bitmap
        locally (per-bit CAS clear) via the `disk_index` hash map
        and zone-index → zone vec (§14; O(1) lookups). Returns
        `false` on double-free (bitmap clear failed).
     c. Build `FreeBlockValue { unit_count, previous_owner:
        segment.owner_chunk }` (§7; `previous_owner` comes from the
        `Segment` — no KV read needed; no `state` field — a free
        block has no data).
     d. `bind = *node.bind.read()`. `await kv.persist_free(bind,
        &disk_id, segment.zone_index, segment.unit_offset,
        &value)` — one `batch_write` that **deletes the
        `BusyBlockKey`** and **writes the `FreeBlockValue`** at
        `FreeBlockKey` (per §3.4/§7).
     e. Return the persist result. Free is synchronous in v1 — the
        caller's `FreeBlocks` RPC returns only after the
        `FreeBlockValue` is durable and the `BusyBlockKey` is gone.
   - `free_blocks(node, segments: &[Segment], kv) -> Result<()>`:
     a. For each segment: `node.free_block(...)` (clear bitmap
        locally). On bitmap clear failure, log a warning (ghost
        scanner will reconcile) but continue.
     b. Build `FreeBlockValue` list from all segments.
     c. `bind = *node.bind.read()`. `await
        kv.persist_free_batch(bind, &records)` (one `batch_write`
        that deletes each `BusyBlockKey` and writes each
        `FreeBlockValue`).
     d. On failure: the bitmap clears already happened locally —
        return error; the §12 ghost-allocation scanner reconciles
        any in-memory/KV mismatch on restart.
   - **No KV read on free in v1 (§14):** `owner_chunk` is carried in
     the `Segment` and becomes `FreeBlockValue.previous_owner`, so the
     free is one `batch_write` (Delete `BusyBlockKey` + Put
     `FreeBlockKey`) with no prior read. Ownership validation is
     deferred to the §12 scanner. The config toggle
     `validate_owner_on_free` (default false) is reserved for a
     future KV-read-first path (one paxos round-trip, doubles free
     latency) — not yet wired into the free path (see Gaps).
   - **Free batching (R79):** when `free_batch_enabled` is true
     (default false), the free path groups frees into a batch and
     flushes via one `batch_write` when the batch reaches
     `free_flush_max_batch` (default 256). No timer. R72 ships with
     the toggle off — immediate free only. The toggle is not yet
     checked in the free path (see Gaps).

7. **gRPC handlers** — `app/crow-diskdb/src/grpc.rs` (single file,
   not a `grpc/` module):
   - `allocate_blocks` — validate `unit_count` (non-zero, aligned to
     block size) and `count` (1–1024, capped by `MAX_ALLOCATE_COUNT`),
     check not degraded, get node by `disk_group_id`, extract
     `owner_chunk`, call `persistence::allocate_blocks()`, return
     `Vec<Segment>`.
   - `free_blocks` — parse `Vec<Segment>`, check not degraded, find
     the node that owns the first segment's disk (scan all nodes),
     call `persistence::free_blocks()` (immediate free in v1).
   - `query_capacity_stats` — stub (`Unimplemented`); R74 fills it
     in.
   - `get_disk_group_info` / `get_disk_info` — read from synced
     in-memory state (R71).
   - `rebuild_zone_bitmap` / `mark_block_suspect` /
     `mark_block_corrupt` — **not yet defined in the proto**
     (`diskdb_service.proto` has only 5 RPCs). These stubs are
     deferred until R73/R75 add the RPC definitions to the proto
     (see Gaps).
   - Error mapping: `NoSpace` → `ResourceExhausted`, not-owner →
     `PermissionDenied`, invalid `unit_count`/`count` →
     `InvalidArgument`, degraded → `Unavailable`.

8. **Server wiring** — `app/crow-diskdb/src/main.rs`:
   - Create `DataGroupClient` wrapping `CrowkvClient` (one for the
     gRPC service, one for the sync loop's disk-add init baseline
     writes).
   - Wire gRPC service (`DiskdbService`) with `NodeContainer`,
     `DataGroupClient`, `StorageDefaults`.
   - Run a blocking initial sync (`sync_once`) before serving gRPC
     to populate in-memory node/disk/zone state.
   - Allocate/free RPCs now functional.
   - No `FreeBatch`, no `FreeFlushLoop` in v1 (R79 adds the
     size-threshold batch when `free_batch_enabled` is true).

**Scope** (actual changed files):
- `app/crow-diskdb/src/zone.rs` — `Zone` struct with bitmap-scan
  `allocate`, `free`, `allocatable`, `derived_alloc_state`,
  `set_health`; `AllocatedRange`, `ZoneHealth` types.
- `app/crow-diskdb/src/node/disk.rs` — `ZoneDisk` with
  `ActiveZoneContext`, `disk_allocate`, `rotate_active_zones`,
  `rebuild_active_zones`, `set_effective_status`, `add_zone`.
- `app/crow-diskdb/src/node.rs` — `Node` with
  `AllocateDiskContext`, `AllocClaim`, `allocate_block`,
  `allocate_blocks`, `free_block`, `add_disk`,
  `rebuild_allocating_disks`; `AllocError` enum.
- `app/crow-diskdb/src/node/container.rs` — `NodeContainer`
  (degraded mode, node add/remove/get).
- `app/crow-diskdb/src/persistence.rs` — `DataGroupClient`,
  `Bind`, `BusyRecord`/`FreeRecord`/`ZoneRecords`, `persist_busy`,
  `persist_busy_batch`, `persist_free`, `persist_free_batch`,
  `put_zone`, `read_zone_records`, `delete_free_records_batch`;
  two-phase `allocate_block`/`allocate_blocks`; immediate
  `free_block`/`free_blocks`.
- `app/crow-diskdb/src/grpc.rs` — `DiskdbService` with
  `allocate_blocks`, `free_blocks`, `query_capacity_stats` (stub),
  `get_disk_group_info`, `get_disk_info` handlers.
- `app/crow-diskdb/src/metrics.rs` — `DiskdbMetrics` with
  `allocate_retry_cas_bit` + `disk_bad_impacted_blocks` counter
  handles.
- `app/crow-diskdb/src/lib.rs` — module declarations.
- `app/crow-diskdb/src/config.rs` — `StorageDefaults` with
  `zone_rotate_count`, `cas_retry_limit` (default 100),
  `validate_owner_on_free` (default false);
  `PersistenceConfig` with `free_batch_enabled` (default false),
  `free_flush_max_batch` (default 256), `snapshot_interval_secs`,
  `snapshot_journal_threshold` (reserved for R73/R79).
- `app/crow-diskdb/Cargo.toml` — `rand = "0.9.5"` (retry random
  start); `[[test]]` entries for `zone_alloc_test`,
  `disk_alloc_test`, `diskdb_e2e_test`.
- `app/crow-diskdb/src/main.rs` — wire `DataGroupClient`,
  `DiskdbService`, blocking initial sync.
- `app/crow-diskdb/tests/zone_alloc_test.rs` — unit tests
  (concurrent alloc, double-free, multi-unit, CAS retry, derived
  state, health).
- `app/crow-diskdb/tests/disk_alloc_test.rs` — unit tests
  (disk round-robin, zone rotation, multi-disk spread,
  exclude_disks, free-by-disk-id).
- `app/crow-diskdb/tests/diskdb_e2e_test.rs` — integration test
  (real kv-server cluster, allocate/free, verify records
  persisted).
- `app/crow-diskdb/tests/common/cluster.rs` — shared 3-node
  kv-server cluster harness.

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

**Acceptance** (verified by existing tests):
- `Zone::allocate()` scans the bitmap from `last_pos_64`, finds a
  free bit via `trailing_ones`, CAS-sets it, returns
  `AllocatedRange`. Unit test (`zone_alloc_test.rs::
  zone_concurrent_allocate_no_double_alloc`): 8 threads allocate
  concurrently on a 256-unit zone; all 256 bits allocated exactly
  once (no double-alloc, all offsets unique).
- `Zone::free()` clears bits and detects double-free. Unit test
  (`zone_alloc_test.rs::zone_double_free_detected`): free an
  allocated bit, then free again → `false`.
- `Zone::allocate(unit_count > 1)` finds consecutive free bits.
  Unit test (`zone_alloc_test.rs::zone_allocate_multi_unit_contiguous`):
  allocate a 4-unit range, verify `unit_offset == 0` and next
  allocate starts at offset 4.
- `Zone::allocate()` respects the CAS retry bound. Unit test
  (`zone_alloc_test.rs::zone_cas_retry_counter_increments_under_contention`):
  16 threads with a barrier force CAS contention on a 64-unit zone;
  verify `cas_retry_count > 0` and the allocate does not spin
  indefinitely.
- `Zone::derived_alloc_state()` transitions Active → Available →
  Full. Unit test (`zone_alloc_test.rs::
  zone_derived_alloc_state_transitions`).
- `Zone::set_health(Bad/Missing)` blocks allocate. Unit tests
  (`zone_alloc_test.rs::zone_bad_health_blocks_allocate`,
  `zone_missing_health_blocks_allocate`).
- `ZoneDisk::disk_allocate()` round-robins over the active zone
  set, rotates when exhausted. Unit test
  (`disk_alloc_test.rs::disk_allocate_rotates_across_active_zones`):
  4 allocations land on ≥ 2 different zones.
  `disk_alloc_test.rs::disk_allocate_returns_none_when_all_zones_full`.
- `Node::allocate_block()` round-robins across disks within the
  named disk-group. Unit test
  (`disk_alloc_test.rs::node_allocate_block_round_robins_across_disks`):
  3 allocations across 3 disks use ≥ 2 different disks.
  `node_allocate_block_respects_exclude_disks`,
  `node_allocate_block_no_space_when_all_excluded`.
- `Node::allocate_blocks()` distributes across disks with
  anti-affinity. Unit test
  (`disk_alloc_test.rs::node_allocate_blocks_spreads_across_disks`):
  3 blocks on 3 different disks.
  `node_allocate_blocks_no_space_when_count_exceeds_disks`.
- `Node::free_block()` by disk-id. Unit test
  (`disk_alloc_test.rs::node_free_block_by_disk_id`,
  `node_free_block_unknown_disk_id_fails`).
- `allocate_block()` two-phase: Phase 1 sync bitmap claim, Phase 2
  async `BusyBlockValue` persist. Integration test
  (`diskdb_e2e_test.rs::diskdb_e2e_allocate_free`): allocate a
  block against a real 3-node kv-server cluster, verify
  `BusyBlockValue` appears in the data group at the expected
  `BusyBlockKey` with fields `{ unit_count, owner_chunk, state:
  Ok }`.
- `allocate_blocks()` multi-block: one `batch_write` for all
  `BusyBlockValue`s. Integration test: allocate 3 blocks, verify
  all records persisted.
- `free_block()` clears bitmap locally, then persists
  `FreeBlockValue` at `FreeBlockKey` and **deletes the
  `BusyBlockKey`** in one `batch_write`. Integration test: free a
  block, verify the `FreeBlockValue` appears in the data group
  (carrying `previous_owner`) and the corresponding `BusyBlockKey`
  is **deleted**.
- `free_blocks()` multi-block: one `batch_write` per data group
  that deletes each `BusyBlockKey` and writes each
  `FreeBlockValue`. Integration test: free 3 blocks, verify all
  `FreeBlockValue`s exist and `BusyBlockKey`s are deleted.
- Baseline `ZoneValue` records written during disk-add init.
  Integration test: verify `ZoneValue` exists for all 3 disks with
  valid checksum and `snapshot_slot == 0`.
- gRPC handlers: `allocate_blocks`, `free_blocks` functional.
  Error mapping: `NoSpace` → `ResourceExhausted`, not-owner →
  `PermissionDenied`, invalid args → `InvalidArgument`, degraded
  → `Unavailable`. `query_capacity_stats` returns `Unimplemented`
  (R74).
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.

**Gaps** (items not yet implemented or diverging from the spec;
see `doc/working/plan-diskdb-r72-cleanup.md` for the cleanup plan):

**R72 cleanup (planned in `plan-diskdb-r72-cleanup.md`):**

- **Metrics not wired into the running server** — `DiskdbMetrics`
  is defined in `metrics.rs` with `allocate_retry_cas_bit` and
  `disk_bad_impacted_blocks` counter handles, but `main.rs` never
  creates a `MetricsRegistry` or `DiskdbMetrics`. `Zone::
  metrics_cas_retry` is always `None` in production — only the
  internal `cas_retry_count` atomic on `Zone` works. The
  `zone.allocate.retry.cms.bit` crow-common counter is never
  registered or published. Fix: create a `MetricsRegistry` in
  `main.rs`, register `DiskdbMetrics`, attach the
  `allocate_retry_cas_bit` counter to each `Zone` via
  `with_cas_retry_metric` during disk-add init.

- **`validate_owner_on_free` config toggle not wired** — The config
  field exists (default `false`) but the free path in
  `persistence.rs` never checks it. When `true`, the free path
  should do a KV read of `BusyBlockValue` first and validate
  `owner_chunk` (one paxos round-trip). When `false` (default), the
  free is no-read (owner from `Segment`) — current behavior. Plan:
  wire it now, config-gated, with unit tests.

- **`rebuild_allocating_disks()` not called on status change** —
  The sync loop calls `rebuild_allocating_disks()` only in
  `disk_add_init` (new disk). When a disk's `effective_status`
  changes (e.g. Up → Missing), the RCU `allocating_disks` context
  is **not refreshed** — it still includes the now-non-allocatable
  disk. The `disk.allocatable()` check in `disk_allocate` still
  rejects it, so correctness is preserved, but round-robin wastes
  cycles probing dead disks. Fix: call
  `node.rebuild_allocating_disks()` after every
  `set_effective_status` in `reconcile_disks`.

**Reassigned to later requirements:**

- **`disk.bad.impacted_blocks` counter → R76** — The design doc
  (§10, updated) specifies that on a disk transitioning to `Bad`,
  the sync path scans zone records for live `BusyBlockValue`s (the
  impacted blocks), emits the `disk.bad.impacted_blocks` gauge, and
  logs the hand-off. This is R76's bad-disk handling scope. The
  metric handle already exists in `DiskdbMetrics`; R76 adds the
  producer.

- **R73/R75 gRPC stubs → R73/R75** — `rebuild_zone_bitmap` (R73),
  `mark_block_suspect`, `mark_block_corrupt` (R75) are not defined
  in `diskdb_service.proto` (only 5 RPCs exist). The proto must be
  extended first, then the stubs implemented.

- **`free_batch_enabled` / `free_flush_max_batch` → R79** — Config
  fields exist in `PersistenceConfig` but the free path does not
  check them. R79 adds the size-threshold batch flush logic.

- **Keepalive usage summary (§11) → R74** — The sync loop's
  `heartbeat_diskdb` passes empty arrays. §11 specifies a per-
  disk-group usage summary piggybacked on keepalive. R74 (space
  metrics) computes these from the in-memory bitmap.

- **Latency hierarchy metrics (§11) → R74** — §11 specifies a
  detailed latency breakdown (`allocate.rpc.latency_us`,
  `allocate.bitmap_scan.latency_us`, `allocate.kv_persist.
  latency_us`, `free.*`, `sync.*`, `compaction.*`). None are
  implemented. R74 (space metrics + query API) adds these.
