<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R72: diskdb — Zone Allocator + Journal Persistence (Block Allocate/Free)

**Problem**: R71 gives diskdb a running server with group-0 sync and
disk status management, but the core allocation engine does not exist.
The server can discover its disk-groups and disks but cannot allocate
or free blocks. This is diskdb's primary function — the allocate/free
RPCs are stubbed (`Unimplemented`) and there is no zone CAS allocator,
no active zone deque, no journal persistence, and no free batch.

The design doc (§8) specifies the allocation algorithm in detail:
per-zone append-only allocators with CAS serialization, disk-level
round-robin, two-phase async allocation (sync CAS claim + async KV
persist), and batched free flush. The key CROW-specific design (D4) is
that each allocate appends a small `BusyRecord` to the paxos data
group's journal (not a full `ZoneRecord`), and each free appends a
`FreeRecord`. The bitmap is derived from the journal — never written
as a full bitmap on the hot path.

The aioss reference has a working zone allocator (`zone/mod.rs`) and
persistence layer (`persistence/mod.rs`), but it writes the **full
`ZoneRecord`** on every allocate — the CROW design deliberately
replaces this with journal records (D4) for efficiency. The CAS logic,
bitmap operations, and round-robin patterns are directly reusable; the
persistence layer is new.

**Solution**: Implement the second major component — block
allocate/free — with the journal-based persistence model from D4.

1. **Zone CAS allocator** — create `lib/crow-diskdb/src/zone/mod.rs`:
   - `Zone` — per-zone allocation state (extends R70's types):
     - `disk_group_id: DiskGroupId`, `disk_uuid: Uuid`, `zone_index:
       u32`.
     - `allocation_state: AtomicU8` — CAS-based state machine
       (Active/Busy/Error/Full), `#[repr(u8)]` from R70.
     - `zone_state: RwLock<ZoneState>` — health (Healthy/Missing/Bad).
       Not atomic — updated by the sync loop (R71) and health probe
       (R76), not the hot path.
     - `max_allocate_pos: u32` — capacity in block units
       (`zone_size / block_size`).
     - `allocate_pos: AtomicU32` — current monotonically increasing
       position (block units).
     - `granularity_shift: u32` — log2(block_size) for bit shifts.
     - `usage_bits: UsageBitmap` — from R70, lock-free atomic bit
       operations.
     - `next_journal_slot: AtomicU64` — per-zone monotonic counter for
       journal key slots. Each BusyRecord/FreeRecord gets a unique
       slot number embedded in its key.
   - `claim(size: u32) -> Option<(Segment, ClaimSnapshot)>` — Phase 1
     (sync):
     a. Check `zone_state` is Healthy; return `None` if not.
     b. CAS `allocation_state` Active → Busy (only one thread wins).
        Use `compare_exchange` on `AtomicU8`.
     c. Compute `count = size >> granularity_shift` (block units).
     d. Check `allocate_pos + count <= max_allocate_pos`; if not,
        transition to Full, return `None`.
     e. `range_set(allocate_pos, count)` on `usage_bits`; if fails
        (double-alloc, should not happen), transition to Error,
        return `None`.
     f. `prev_pos = allocate_pos.fetch_add(count, AcqRel)`.
     g. Build `Segment { zone_offset: prev_pos << granularity_shift,
        size, tag: now_ns(), ... }`.
     h. Build `ClaimSnapshot { prev_pos, count }`.
     i. Return `Some((segment, snapshot))`. Zone stays Busy until
        `release()` or `rollback()`.
   - `release()` — Phase 2 success: store `Active` with `Release`
     ordering. Does NOT re-add to deque — caller does that via
     `active_zone()`.
   - `rollback(snapshot)` — Phase 2 failure: restore `allocate_pos =
     snapshot.prev_pos`, `range_clear(prev_pos, count)`, transition to
     Active.
   - `active()` — CAS Busy → Active for external re-activation (the
     `active_zone` API). Returns `true` if CAS succeeded.
   - `free(zone_offset, size) -> bool` — convert offset to block
     units, `range_clear`, return `false` on double-free detection.
   - `allocatable() -> bool` — `zone_state == Healthy &&
     allocation_state == Active`.
   - `next_slot() -> u64` — `next_journal_slot.fetch_add(1, Relaxed)`.
     Used to generate unique journal keys for BusyRecord/FreeRecord.

2. **Active zone deque + disk-level claim** — implement in
   `lib/crow-diskdb/src/node/disk.rs` (extends R71's `ZoneDisk`):
   - `active_zones: SegQueue<ZoneRef>` — lock-free MPMC deque
     (`crossbeam-queue::SegQueue`).
   - `disk_claim(size: u32) -> Option<(ZoneRef, Segment,
     ClaimSnapshot)>`:
     a. Check `disk_state` is Active; return `None` if not.
     b. Loop: pop zone from `active_zones`. If empty, call
        `find_active_zone()`. If none, return `None`.
     c. Call `zone.claim(size)`. On success, record metrics, return.
        On failure (Full/Error), loop to try next zone.
   - `find_active_zone() -> Option<ZoneRef>` — random-start scan of
     all zones; return first `allocatable()` one. Avoids bias toward
     low-index zones.
   - `free(zone_index, offset, size) -> bool` — look up zone, call
     `zone.free()`.
   - `active_zone(zone_index) -> bool` — look up zone, push to
     `active_zones` deque if allocatable, CAS Busy → Active.
   - `rebuild_active_zones()` — scan all zones, push allocatable ones
     to deque. Called by R73's recovery.

3. **Node-level round-robin** — implement in
   `lib/crow-diskdb/src/node/mod.rs` (extends R71's `Node`):
   - `claim_block(size) -> Result<(ZoneRef, Segment, ClaimSnapshot)>`:
     a. Read-lock `allocating_disks`; check non-empty (else
        `NoSpace`).
     b. `start = disk_allocate_iterator.fetch_add(1, Relaxed)`.
     c. Select `disks[start % num_disks]`; call `disk_claim(size)`.
     d. On success, return. On failure, `retry_claim(size, &[])`.
   - `claim_blocks(size, count, exclude_disks) ->
     Result<Vec<(ZoneRef, Segment, ClaimSnapshot)>>`:
     a. `start = iterator.fetch_add(count, Relaxed)`.
     b. For each block: select `disks[(start + i) % num_disks]`,
        skip if in `exclude_disks`, call `disk_claim(size)`.
     c. If not all claimed, `retry_allocate()` with random start,
        skipping excluded and already-used disks.
   - `retry_claim(size, exclude) -> Result<...>` — random start,
     scan all disks from start, skip excluded, return first success.
   - `retry_allocate(size, remaining, claims, exclude)` — random
     start, scan, skip excluded and used, claim remaining blocks.
   - `free_block(segment) -> Result<bool>` — look up disk by
     `segment.disk_uuid`, call `disk.free()`.
   - `active_zone(disk_uuid, zone_index) -> Result<bool>` — look up
     disk, call `disk.active_zone()`.

4. **Journal persistence** — create
   `lib/crow-diskdb/src/persistence/` module:
   - `JournalClient` — wraps `CrowkvClient` for writing to the
     disk-group's bound paxos data group. Uses the `(store_id,
     group_id)` from the binding map (R71).
   - `persist_busy_record(dg_id, bind, disk_uuid, zone_idx, slot,
     record: &BusyRecord) -> Result<()>` — `put` to
     `journal_key_busy(dg_id, disk_uuid, zone_idx, slot)` on the
     bound data group. Serializes `BusyRecord` via bincode (compact,
     ≤ 32 bytes).
   - `persist_busy_records_batch(dg_id, bind, records: &[(disk_uuid,
     zone_idx, slot, BusyRecord)]) -> Result<()>` — `batch_write` of
     all records in one async round-trip. Used for multi-block
     allocation (one `batch_write` per data group).
   - `persist_free_records_batch(dg_id, bind, records: &[(disk_uuid,
     zone_idx, slot, FreeRecord)]) -> Result<()>` — `batch_write` for
     free batch flush.
   - `read_journal_zone(dg_id, bind, disk_uuid, zone_idx) ->
     Result<JournalReplayData>` — prefix scan
     `journal_prefix_zone(dg_id, disk_uuid, zone_idx)` to fetch all
     BusyRecord/FreeRecord/ZoneSnapshot for one zone. Used by R73's
     recovery. (This requirement defines the method; R73 implements
     the replay logic.)
   - `delete_journal_records_batch(dg_id, bind, keys: &[String]) ->
     Result<()>` — `batch_write` with `Delete` ops. Used by R73's
     snapshot compaction.

5. **Two-phase async allocation** — implement in
   `lib/crow-diskdb/src/persistence/alloc.rs`:
   - `allocate_block(node: &Arc<Node>, size: u32, journal: &
     JournalClient) -> Result<Segment>`:
     a. **Phase 1 (sync)**: `node.claim_block(size)` →
        `(zone, segment, snapshot)`. Zone is now Busy.
     b. **Phase 2 (async)**: Get `next_slot()` from zone. Build
        `BusyRecord { zone_offset: segment.zone_offset, size:
        segment.size, tag: segment.tag }`. `await
        journal.persist_busy_record(dg_id, bind, disk_uuid,
        zone_idx, slot, &record)`.
     c. On success: `zone.release()`, return `segment`.
     d. On failure: `zone.rollback(snapshot)`, return error.
   - `allocate_blocks(node, size, count, exclude_disks, journal) ->
     Result<Vec<Segment>>`:
     a. Phase 1: `node.claim_blocks(size, count, exclude_disks)` →
        `Vec<(zone, segment, snapshot)>`.
     b. Phase 2: For each claim, get `next_slot()`, build
        `BusyRecord`. Collect all records. `await
        journal.persist_busy_records_batch(dg_id, bind, &records)`.
     c. On success: `release()` all zones, return segments.
     d. On failure: `rollback()` ALL claims, return error.
   - The `dg_id` and `bind` come from the `Node` struct (set by R71's
     sync loop from the binding map).

6. **Free batch flush** — create
   `lib/crow-diskdb/src/persistence/free_batch.rs`:
   - `FreeBatch` — `inner: Mutex<Vec<(Segment, u64)>>` where the `u64`
     is the pre-assigned journal slot. `append(segment, slot)`,
     `drain() -> Vec<(Segment, u64)>`, `re_enqueue(items)`,
     `len()`, `is_empty()`.
   - `free_block(node, segment, free_batch) -> Result<()>`:
     a. `node.free_block(segment)` — clear bitmap locally.
     b. Pre-assign a journal slot: get the zone from the node, call
        `zone.next_slot()`.
     c. `free_batch.append((segment, slot))`.
     d. Return `Ok(())` immediately — free is async.
   - `FreeFlushLoop` — background task:
     a. `sleep(free_flush_interval_ms)` (default 500 ms).
     b. `drain()` the batch. If empty, continue.
     c. Group by `(dg_id, disk_uuid, zone_idx)` and build
        `FreeRecord` list.
     d. For each affected data group, `await
        journal.persist_free_records_batch(...)`.
     e. On failure: `re_enqueue(items)` for retry on next tick.
     f. Also flush when `free_batch.len() >= free_flush_max_batch`
        (default 256) — check before sleeping.

7. **gRPC handlers** — implement in
   `lib/crow-diskdb/src/grpc/service.rs`:
   - `allocate_block` — validate size (non-zero, aligned to block
     size), check not degraded, get node, call
     `persistence::allocate_block()`, return `Segment`.
   - `allocate_blocks` — validate size + count (1–1024), check not
     degraded, get node, call `persistence::allocate_blocks()`.
   - `condition_allocate_blocks` — parse `exclude_disk_uuids`, call
     `allocate_blocks()` with hints.
   - `free_block` — parse `Segment`, get node, call
     `persistence::free_block()`.
   - `active_zone` — parse disk_uuid + zone_index, get node, call
     `node.active_zone()`.
   - `query_disk_usage` — stub (returns empty); R74 fills it in.
   - Error mapping: `NoSpace` → `ResourceExhausted`, `NotOwner` →
     `PermissionDenied`, `InvalidSize`/`InvalidCount` →
     `InvalidArgument`, `Degraded` → `Unavailable`.

8. **Server wiring** — update `app/crow-diskdb-server/src/main.rs`:
   - Create `JournalClient` from `CrowkvClient`.
   - Create `FreeBatch` (shared `Arc<FreeBatch>`).
   - Spawn `FreeFlushLoop` as background task.
   - Wire gRPC service with `NodeContainer`, `JournalClient`,
     `FreeBatch`, config.
   - Allocate/free/active_zone RPCs now functional.

**Scope** (expected changed files):
- `lib/crow-diskdb/src/zone/mod.rs` — `Zone` struct with CAS claim,
  release, rollback, free, active.
- `lib/crow-diskdb/src/node/disk.rs` — `ZoneDisk` with `disk_claim`,
  `find_active_zone`, `active_zone`, `rebuild_active_zones`.
- `lib/crow-diskdb/src/node/mod.rs` — `Node` with `claim_block`,
  `claim_blocks`, `retry_claim`, `retry_allocate`, `free_block`,
  `active_zone`.
- `lib/crow-diskdb/src/persistence/mod.rs` — `JournalClient`.
- `lib/crow-diskdb/src/persistence/alloc.rs` — two-phase async
  allocation.
- `lib/crow-diskdb/src/persistence/free_batch.rs` — `FreeBatch` +
  `FreeFlushLoop`.
- `lib/crow-diskdb/src/grpc/service.rs` — allocate/free/active_zone
  handlers.
- `lib/crow-diskdb/src/grpc/mod.rs` — wire service struct.
- `lib/crow-diskdb/src/lib.rs` — module declarations.
- `lib/crow-diskdb/Cargo.toml` — add `crossbeam-queue`, `bincode`,
  `rand`.
- `app/crow-diskdb-server/src/main.rs` — wire `JournalClient`,
  `FreeBatch`, `FreeFlushLoop`.

**Complexity**: High. The CAS allocator and round-robin patterns are
well-proven in the aioss reference. The new work is the journal-based
persistence (D4): instead of writing a full `ZoneRecord` on every
allocate, diskdb appends a small `BusyRecord` to the paxos data group.
The slot-based key layout (from R70) enables prefix-scan replay (R73)
without crow-kv slot feedback. The two-phase async pattern (sync CAS
claim + async KV persist) requires careful rollback on failure.

**Dependencies**: R70 (core types, bitmap, config), R71 (NodeContainer,
sync loop, server binary, SysdataClient). No dependency on R73–R77.

**Acceptance**:
- `Zone::claim()` performs CAS Active → Busy, advances `allocate_pos`,
  sets usage bits, returns `(Segment, ClaimSnapshot)`. Unit test:
  concurrent claims on the same zone serialize via CAS (only one
  wins, others return `None` or retry).
- `Zone::rollback()` restores `allocate_pos` and clears bits. Unit
  test: rollback after a failed persist leaves the zone in its
  pre-claim state.
- `Zone::free()` clears bits and detects double-free. Unit test.
- `ZoneDisk::disk_claim()` polls from active deque, falls back to
  random scan, tries multiple zones on Full/Error.
- `Node::claim_block()` round-robins across disks via `AtomicU32`.
  `claim_blocks()` distributes across disks, respects
  `exclude_disks`.
- `allocate_block()` two-phase: Phase 1 sync claim, Phase 2 async
  `BusyRecord` persist. On persist failure, rollback. Integration
  test with in-process crow-kv: allocate a block, verify
  `BusyRecord` appears in the data group at the expected journal key.
- `allocate_blocks()` multi-block: one `batch_write` for all
  `BusyRecord`s. Integration test: allocate N blocks, verify all
  records in one batch.
- `free_block()` clears bitmap locally, appends to `FreeBatch`,
  returns immediately. `FreeFlushLoop` flushes every 500 ms or 256
  entries. Integration test: free blocks, wait for flush, verify
  `FreeRecord`s in the data group.
- `active_zone()` re-adds a zone to the active deque. Unit test:
  after allocate (zone removed from deque), `active_zone()` makes it
  allocatable again.
- gRPC handlers: `allocate_block`, `allocate_blocks`,
  `condition_allocate_blocks`, `free_block`, `active_zone` functional
  via gRPC. Error mapping correct (`NoSpace` → `ResourceExhausted`,
  etc.).
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.
