<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: diskdb Zone Management

Depends on: [`design-crowdb-diskdb.md`](design-crowdb-diskdb.md) §3.3 (no CAS,
exclusive ownership), §3.9 (unit-based sizes, disk-id key routing), §6
(hierarchy);
[`design-crowdb-protocol-key.md`](../protocol/design-crowdb-protocol-key.md)
(binary key encoding);
[`design-crowdb-kv-state-machine.md`](../kv/design-crowdb-kv-state-machine.md)
(slot tracking, apply semantics).
Satisfies: `design-crowdb-diskdb.md` §7 (Zone Records and Crash Recovery),
§8 (Allocation Algorithm), §9 zone allocation state, §14 zone-level
concurrency.

Detailed design for diskdb's zone-management component — the zone
allocator, record model, free path, compaction, rotation, crash
recovery, and zone-level concurrency. Architecture decisions and
rationale live in the root design doc; this doc carries the structs,
algorithms, record layouts, and coordination rules. Reused surfaces
(named, not re-built): `DdbZone`, `DdbDisk`, `DdbDiskGroup`,
`DdbDiskGroupContainer`, `DdbKvClient`, `CompactionEngine`,
`RecoveryEngine`, `UsageBitmap`, `ZoneValueExt`.

## Table of Contents

- [1. Scope](#1-scope)
- [2. Zone as a Logical Concept](#2-zone-as-a-logical-concept)
- [3. Record Model](#3-record-model)
  - [Record key layout](#record-key-layout)
  - [Value schemas](#value-schemas)
  - [Record model](#record-model)
  - [Current state determination](#current-state-determination)
- [4. Allocation Algorithm](#4-allocation-algorithm)
  - [Zone-level allocate (sync, in-memory)](#zone-level-allocate-sync-in-memory)
  - [Disk-level allocate (sync) — rotating active-zone-set](#disk-level-allocate-sync--rotating-active-zone-set)
  - [Round-robin across disks within the named disk-group (sync)](#round-robin-across-disks-within-the-named-disk-group-sync)
  - [Two-phase async allocation](#two-phase-async-allocation)
  - [Free (persist-only)](#free-persist-only)
- [5. Zone Rotation and Compaction](#5-zone-rotation-and-compaction)
  - [Compaction-on-rotation](#compaction-on-rotation)
  - [Preparatory thread](#preparatory-thread)
  - [Periodic compaction fallback](#periodic-compaction-fallback)
  - [Compaction algorithm](#compaction-algorithm)
  - [Ready-zone tracking](#ready-zone-tracking)
- [6. Crash Recovery](#6-crash-recovery)
  - [Three recovery strategies](#three-recovery-strategies)
  - [How the strategies work together](#how-the-strategies-work-together)
  - [Crash-safety invariants](#crash-safety-invariants)
  - [Recovery and compaction engines](#recovery-and-compaction-engines)
  - [Recovery triggers](#recovery-triggers)
- [7. Zone Allocation State (derived)](#7-zone-allocation-state-derived)
- [8. Concurrency Model](#8-concurrency-model)
  - [Allocate (lock-free)](#allocate-lock-free)
  - [Free (persist-only, no lock)](#free-persist-only-no-lock)
  - [Zone-level lock for non-allocate operations](#zone-level-lock-for-non-allocate-operations)
  - [Common methods on DdbZone](#common-methods-on-ddbzone)
  - [Free-side lookup structures](#free-side-lookup-structures)
  - [Monotonic timestamp source](#monotonic-timestamp-source)
- [9. Background Scanner Coordination](#9-background-scanner-coordination)
- [10. Invariants](#10-invariants)
- [11. Tunables and Defaults](#11-tunables-and-defaults)

## 1. Scope

This doc covers everything zone-level: the in-memory zone allocator
(bitmap-scan with per-bit CAS), the durable record model (busy/free/
snapshot records), the free path (persist-only, bitmap untouched on
free), compaction (the sole bit-clearer), zone rotation (compaction-
before-rotation with a preparatory thread), crash recovery (three
strategies), and the zone-level concurrency model (lock-free allocate,
zone-level lock for non-allocate operations).

What is **not** here: disk-level status management (root §10), space
metrics (root §11 / space-metrics sub-design), background scanner design
(root §12 / scanner sub-design), group-0 sysdata schema (root §5). Those
docs reference this one for zone-level behavior.

## 2. Zone as a Logical Concept

Not all zones on a disk must be the same size. The last zone may be
smaller (disk capacity is rarely an exact multiple of the zone size).
Zone is a **logical concept** defined for easier implementation; it can
later adopt to native zoned-namespace SSD or SMR HDD zone APIs, but
currently no such devices are targeted. The zone is a rough mapping.

**Word alignment rule:** each zone's `unit_capacity` must be a multiple
of 64 (the 64-bit bitmap word size). All zones except the last have
`unit_capacity = zone_size / unit_size`. The last zone has
`unit_capacity = remaining_capacity / unit_size`, rounded down to a
multiple of 64; the sub-64-unit tail (at most 63 units) is unallocated.
Only the last zone may have a different size; all other zones on a disk
are uniform. No bitmap masking, no padding bits. Each zone's bitmap is
sized to its own `unit_capacity`, which is word-aligned.

## 3. Record Model

### Record key layout

Three types of KV entries on the disk-group's bound data group. Keys
use the cross-component binary encoding
(`doc/design/protocol/design-crowdb-protocol-key.md`): each is a flat
struct with a `magic | type_tag` header and big-endian fixed-width
fields. Keys are disk-id-based (globally unique → reverse-lookup to the
data group). No `node_id`/`disk_group_id` in record keys.

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
`doc/design/protocol/design-crowdb-protocol-key.md`; value field details
are in `diskdb_type.fbs`.

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
    free request; no KV read needed). Carried for audit / scanner
    cross-check. No `state` field — a free block has no data.
  - `freed_ts: u64` — wall-clock nanoseconds at free time, generated by
    a per-disk-group monotonic timestamp source (§8). Used by compaction
    as a watermark to distinguish already-merged free records (stale,
    `freed_ts <= compact_ts`) from new free records (`freed_ts >
    compact_ts`). This prevents double-free when orphaned free records
    survive a crashed compaction and the block is re-allocated before
    the next compaction (§5 Compaction algorithm).
- **`ZoneValue`**:
  - `usage_bitmap: bytes` — the full zone bitmap (one bit per unit;
    bit set = busy, bit clear = free). Sized to the zone's
    `unit_capacity` (multiple of 64 bits per §2).
  - `snapshot_slot: u64` — the slot at which this snapshot was written.
    Strategy 2 (journal scan replay) replays operations after this slot.
  - `compact_ts: u64` — the max `freed_ts` of all free records merged
    into this snapshot's bitmap. Free records with `freed_ts <=
    compact_ts` have already been merged; they are stale and can be
    safely dropped by compaction without touching the bitmap. Free
    records with `freed_ts > compact_ts` are new and need to be merged.
    Initialized to 0 in the baseline snapshot (disk-add init).
  - `crc32: u32` — CRC32 checksum over `usage_bitmap` + `compact_ts`
    for integrity verification (§9 scanner coordination).

### Record model

- A busy/free entry can span multiple units (`unit_count` ≥ 1).
- **On free, the `BusyBlockKey` is deleted** and a `FreeBlockValue` is
  written at `FreeBlockKey` in the same `batch_write`. The
  `FreeBlockValue` carries `previous_owner` (the `owner_chunk` from the
  freed `BusyBlockValue`) for audit.
- On re-allocate, the `FreeBlockKey` is deleted and a new
  `BusyBlockValue` is written at the `BusyBlockKey` (new owner,
  `state = Ok`). The `FreeBlockKey` is cleared by compaction, not by
  the re-allocate — compaction deletes it when merging the free into
  `ZoneValue`. Before compaction deletes it, the re-allocate's
  `batch_write` also deletes the `FreeBlockKey` (the block is busy
  again, so the free marker is stale). If the re-allocate happens after
  compaction already merged the free (cleared the bit + set
  `compact_ts >= freed_ts`), the orphaned `FreeBlockKey` (if the
  compaction's delete didn't succeed) has `freed_ts <= compact_ts` —
  the next compaction drops it without touching the bitmap (§5).
- **Current state determination** (no slot ordering needed): a block is
  **busy** iff its `BusyBlockKey` exists; otherwise it is **free**. A
  `FreeBlockKey` may exist for a not-yet-compacted free (carrying
  `previous_owner`); after compaction, neither key exists for that
  offset.
- `ZoneValue` carries a CRC32 checksum for integrity verification.

### Current state determination

The full `ZoneValue` is a **compacted snapshot** written periodically.
The ideal approach on free would be to update the bitmap in the
`ZoneValue` and write the whole `ZoneValue` to KV. But `ZoneValue` is
large (full bitmap), and frees are random across all zones and disks,
so a per-free `ZoneValue` write is too expensive. Instead, each free
writes a small `FreeBlockValue` and deletes the `BusyBlockKey` in one
`batch_write`; later, compaction lists the free records for a zone (a
prefix scan (the free records of one zone are contiguous in the
crowdb-tree page, so this is efficient), merges them into the `ZoneValue`
bitmap (clear the freed bits), writes the updated `ZoneValue` once, and
deletes the free records in one `batch_write`.

On crash/restart, diskdb reconstructs the in-memory zone state using
three complementary strategies (§6).

**Note on scan ordering:** a key prefix scan returns records in
**lexicographic key order** (= `unit_offset` order), not slot order.
`min_slot` on the crowdb-kv scan request is a read-freshness floor, not a
record-slot filter. Strategy 1 works without slot ordering because the
busy record's existence is the indicator: a block is busy iff its
`BusyBlockKey` exists (not the write order). Strategy 2 requires the
`JournalScan` extension to get slot-ordered replay.

## 4. Allocation Algorithm

The algorithm uses bitmap-scan with a rotating cursor, per-bit CAS,
and disk-level zone rotation. The only difference from a local-file
allocator is persistence (CROWDB writes `BusyBlockValue`/
`FreeBlockValue` to the KV journal per-allocation instead of saving the
full bitmap to a local file).

### Zone-level allocate (sync, in-memory)

Bitmap-scan with per-bit CAS (no zone-level lock):
1. Check zone health is Healthy and `used_count < unit_capacity`.
2. Scan the bitmap from `last_pos_64` (rotating cursor), wrapping
   around. For each 64-bit word, use `countr_one` to find the first
   zero bit (hardware-optimized).
3. CAS-set the bit via `compare_exchange` on the 64-bit word. On CAS
   failure (another thread set the same bit), re-scan the same word.
   **CAS retry bound:** per-bit CAS is capped at `cas_retry_limit`
   retries (default 100); on exhaustion, fall through to the next bit /
   word / zone. This prevents indefinite spinning under heavy
   contention. The `cas_retry_count` atomic (and the wired crowdb-common
   counter) is incremented on each retry as the key operational signal
   for lock-free allocator contention.
4. For `unit_count > 1`: find `unit_count` consecutive zero bits (may
   span words), CAS-set each; on any CAS failure, clear bits already
   set in this attempt and continue scanning.
5. On success: update `last_pos_64`, increment `used_count`, return
   `AllocatedRange { unit_offset, unit_count }`. No zone-level state
   change. Other threads can allocate concurrently.

### Disk-level allocate (sync) — rotating active-zone-set

1. Load the current `ActiveZoneContext` (RCU read, `Arc` clone, no
   lock held). This is a small set of `zone_rotate_count` zones.
2. Round-robin over the active set via `pos_v_zone_ctx.fetch_add(1)`.
3. Call `zone.allocate(unit_count)` on each zone. On success, return.
4. If all zones in the active set fail, call `rotate_active_zones()`:
   under a write lock, publish the next set of **ready** (pre-compacted)
   zones as the new `ActiveZoneContext` (RCU publish). If no ready zones
   are available, fall back to synchronous compaction of the next batch
   (§5 Compaction-on-rotation), then publish.
5. Retry with the new context (up to `zone_num / zone_rotate_count + 2`
   loops).

### Round-robin across disks within the named disk-group (sync)

The `AllocateBlocks` request carries `disk_group_id` (root §3.2);
allocate is scoped to one disk-group. diskdb never round-robins across
disk-groups. Within that named disk-group, an `AtomicU64` iterator
(`pos_v_disk_ctx`) round-robins across disks: each allocation increments
and selects `iterator % num_disks` from the `AllocateDiskContext` (RCU
context of allocatable disks within the named disk-group). Multi-block:
`fetch_add(count)` distributes across disks within the group.
`exclude_disks` (anti-affinity, per-disk, skip a disk that just failed)
is applied within the named disk-group. Multi-block uses one
`batch_write` on the disk-group's bound paxos data group; atomic within
the group. No cross-group multi-block allocate in v1.

### Two-phase async allocation

1. **Phase 1 (sync)**: bitmap-scan allocate (nanoseconds). Bits are set
   in `usage_bits` via per-bit CAS. No zone-level lock. Other threads
   can allocate concurrently.
2. **Phase 2 (async)**: `.await` on crowdb-kv `batch_write` that Puts
   the `BusyBlockValue` at `BusyBlockKey { disk_id, zone_idx,
   unit_offset }` **and** Deletes any prior `FreeBlockKey` for the same
   offset (re-allocate clears the stale free marker, §3 Record model).
   On success: return `Segment`. On failure:
   `zone.rollback_allocate(unit_offset, unit_count)` (clear the bits
   that were set in Phase 1), return error.

For multi-block: Phase 1 allocates all blocks (sync, may span multiple
zones/disks within one disk-group), Phase 2 uses one `batch_write` per
data group (one async round-trip per group).

### Free (persist-only)

The free path is **persist-only**: delete `BusyBlockKey` + put
`FreeBlockValue` in one `batch_write`. The in-memory bitmap is **not**
touched. The bit stays set, `used_count` is not decremented. The block
is freed on disk but still shows busy in memory until compaction clears
the bit (§5). This is the data-safety principle: the bitmap is a
conservative over-estimate that never shows freed space as available
until compaction reconciles it from records.

The free path increments `uncompacted_free_record_count` (an atomic
counter per zone) so compaction knows there is work to do. No `FreeBatch`,
no timer, no background flush loop. The free is a single durable
operation, and the bitmap reconciliation is deferred to compaction.

Free steps:
1. **Phase 0 (optional)**: when `validate_owner_on_free` is `true`, read
   the `BusyBlockValue` from the data group and validate `owner_chunk`
   before persisting. Rejects on `NotBusy` or `OwnerMismatch`. When
   `false` (default), no KV read. `owner_chunk` comes from the
   `Segment` (§8 Free-side lookup structures).
2. **Phase 1 (persist)**: one `batch_write` (Delete `BusyBlockKey` + Put
   `FreeBlockValue` at `FreeBlockKey`). The `FreeBlockValue` carries
   `freed_ts` from the per-disk-group monotonic timestamp source (§8).
   Durable free first.
3. **Post-persist (in-memory)**: increment
   `uncompacted_free_record_count` on the zone (lookup via zone-index →
   zone vec). No bitmap mutation, no `used_count` decrement.
4. Record per-disk event counter after the durable free.

Persist-before-bitmap-clear is not needed here because the bitmap is
never cleared on free. The persist is the entire free operation. If the
persist fails, the block is still busy on both disk and memory. The
caller can retry safely. If the persist succeeds, the block is free on
disk; the in-memory bitmap still shows it busy (conservative over-
estimate) until compaction reconciles.

Free batching (grouping many frees into one `batch_write` per flush,
triggered by batch size, no timer) is an optimization for high-free-
throughput workloads, tracked as a future optimization. With persist-only
free, batching does not change the bitmap contract. The bitmap is never
touched on free, regardless of batching.

**`rollback_allocate` — allocate-only bitmap clear:** the
`DdbZone::rollback_allocate` method (CAS-clear bits, decrement
`used_count`) is used **only** by the allocate Phase 2 failure path to
undo Phase 1's bitmap claim. It is **not** called by the free path. This
method replaces the former `DdbZone::free` that was used by both paths.

## 5. Zone Rotation and Compaction

The disk maintains a small active zone set (`zone_rotate_count` zones,
default 4). When all zones in the set are exhausted (no free bits), the
set rotates: a new set of allocatable zones is selected from a rotating
scan position and published via RCU. This spreads wear across zones and
avoids bias toward low-index zones.

### Compaction-on-rotation

Before a zone enters the active set, it **must be compacted**: freed
bits cleared, `used_count` recomputed, snapshot written, free records
deleted. This ensures the allocator sees accurate free space in active
zones. Without compaction, a rotated-in zone would have stale set bits
for freed-but-not-compacted blocks, and the allocator would skip them
(appearing fuller than it is).

Compaction runs on **non-active zones only**, no concurrent allocate.
The zone-level lock (§8) ensures compaction, scanner, and health checks
do not conflict with each other on the same zone.

### Preparatory thread

A separate background thread pre-compacts the next batch of
`zone_rotate_count` zones in advance, so rotation is instant. The next
active set is already compacted and ready to publish via RCU. The
preparatory thread runs continuously:

1. Identify the next `zone_rotate_count` zones in the rotation order
   (starting from `pos_v_zone + zone_rotate_count`, wrapping around).
2. For each zone that is not already ready and not in the current active
   set: acquire the zone-level lock, compact it (§5 Compaction
   algorithm), mark it as ready.
3. Sleep briefly, then re-check (new zones may need compaction after
   frees, or the rotation position may have advanced).

When rotation is triggered, the ready zones are published immediately
as the new active set. Their ready flag is cleared. The preparatory
thread then starts building the next ready set.

**Fallback when no ready zones exist:** if the preparatory thread has
not finished pre-compacting (e.g., on startup, or under heavy churn
where zones exhaust faster than compaction can keep up), rotation falls
back to **synchronous compaction**: compact the next batch inline, then
publish. This is slower but correct. The allocator waits for
compaction to finish before allocating from the new zones.

### Periodic compaction fallback

A background compaction task (`CompactionEngine`, 300s cadence or
`uncompacted_free_record_count` threshold) handles zones that don't
rotate (low-churn zones, or all-zones-active scenarios). This ensures
freed space is eventually reclaimed even without rotation. The periodic
compaction also skips active zones and uses the zone-level lock.

### Compaction algorithm

`compact_zone` (strategy 3) is the background maintenance task. It uses
a **timestamp watermark** (`compact_ts` on `ZoneValue` vs `freed_ts` on
`FreeBlockValue`) to distinguish already-merged free records from new
ones, and writes the new snapshot + deletes the free records in **one
atomic `batch_write`**. This prevents double-free when orphaned free
records survive a crashed compaction and the block is re-allocated
before the next compaction.

1. **Scan free records** for the zone by key prefix (the free records
   of one zone are contiguous in the crowdb-tree page). This KV read is
   done **before** acquiring the zone-level lock. Also read the current
   `ZoneValue` to get `compact_ts` (or use the in-memory
   `zone.compact_ts` if the zone is already loaded).
2. **Partition free records by watermark:**
   - **Stale** (`freed_ts <= compact_ts`): already merged into the
     bitmap by a prior compaction (or a crashed compaction that wrote
     the snapshot but didn't finish the delete). These are dropped.
     Their bits are already clear in the bitmap. Do NOT `range_clear`
     them again (the block may have been re-allocated since; clearing
     would corrupt a live allocation).
   - **New** (`freed_ts > compact_ts`): not yet merged. These need
     `range_clear` to clear their bits.
3. **Acquire zone-level lock**, merge only the **new** free records
   into the in-memory bitmap (`range_clear` per new free record, clear
   the freed bits), recompute `used_count = popcount`. Re-read
   `zone.compact_ts` under the lock (the step-1 read was unlocked and
   may be stale if another compaction ran in between). Compute
   `new_compact_ts = max(zone.compact_ts, max(freed_ts of all new free
   records))`. The max with the current `compact_ts` prevents
   regression when the step-1 read was stale (range_clear on
   already-cleared bits is a no-op, but a regressed `compact_ts` would
   violate the watermark invariant). If there are no new free records,
   keep `zone.compact_ts`. **Release lock.** The lock protects only the
   in-memory bitmap mutation.
4. Determine `snapshot_slot` from the data group's applied frontier.
5. Build a new `ZoneValue` from the merged bitmap + `new_compact_ts` +
   CRC32 checksum. Update `snapshot_slot` and `compact_ts` on the zone.
6. **One atomic `batch_write`**: Put the new `ZoneValue` + Delete all
   scanned free records (both stale and new). This is a single
   `batch_write` on the bound data group, atomic via crowdb-kv paxos.
   The snapshot and the free-record deletion succeed or fail together;
   there is no window where the snapshot is written but the free
   records survive (the race that caused double-free in the two-op
   design).
7. Decrement `uncompacted_free_record_count` by the number of free
   records deleted (both stale and new).

Only free records are deleted; busy records for live blocks are
untouched (busy records for freed blocks were already deleted on free).

**Why the timestamp watermark prevents double-free:**

Consider the failure scenario with the old two-op design (write
snapshot, then delete free records):
- Compaction scans [F1 (`freed_ts=100`)], clears F1's bit, writes
  `ZoneValue` (`compact_ts=100`), then crashes before deleting F1.
- F1's `FreeBlockKey` is orphaned. The bitmap has F1's bit clear.
- F1's block is re-allocated (new `BusyBlockValue`, bit set again).
- Next compaction scans [F1 (`freed_ts=100`)]. Without the watermark,
  it would `range_clear` F1's bit, **corrupting the new allocation**
  (double-free).

With the watermark:
- Next compaction scans [F1 (`freed_ts=100`)], reads `compact_ts=100`.
- `100 <= 100` → F1 is **stale** (already merged). Drop it, do NOT
  `range_clear`. The new allocation's bit is safe.
- F1's orphaned `FreeBlockKey` is deleted in the batch_write (cleanup).

**Worst case:** if the watermark logic incorrectly classifies a new
free as stale (e.g., clock skew causes `freed_ts <= compact_ts` for a
genuinely new free), the free record is dropped without clearing the
bit. The block stays busy in the bitmap, **wasted space, no data loss**
(conservative over-estimate, I1). The background scanner (§9) detects
this drift (bit set, no `BusyBlockKey`, no `FreeBlockKey`, real
ghost-busy) and can correct it. This is the data-safety principle: fail
conservative (keep busy), never free a block that might have data.

### Ready-zone tracking

Each `DdbZone` carries:
- `compacted_ready: AtomicBool` — `false` when the zone needs
  compaction before it can enter the active set (or it is currently
  active, or has been modified since last compaction); `true` when the
  zone has been compacted and its bitmap is accurate (eligible for
  rotation into the active set).
- `compact_ts: AtomicU64` — the in-memory mirror of
  `ZoneValue.compact_ts`. Used by compaction to partition free records
  by watermark without a separate KV read. Updated when compaction
  writes a new `ZoneValue`; loaded from the snapshot during recovery.

The `compacted_ready` flag is set to `true` by the preparatory thread
after compaction completes. It is set to `false` when the zone is
published into the active set (it will need re-compaction after being
allocated from and freed). `rotate_active_zones` picks zones where
`compacted_ready == true` and not in the current active set. If fewer
than `zone_rotate_count` ready zones exist, it falls back to
synchronous compaction for the remainder.

## 6. Crash Recovery

### Three recovery strategies

diskdb uses three complementary strategies for crash recovery and
maintenance. All three belong in the design, each in its role.

**Strategy 1 — full scan rebuild (on-demand, via RPC/API).**

Scan all live `BusyBlockKey`s for a zone and rebuild the bitmap from
scratch. No snapshot needed. For each offset: if a `BusyBlockKey`
exists, the bit is set (busy); otherwise the bit is clear (free). No
slot ordering needed. The busy record's existence is the indicator.
(`FreeBlockKey`s carry `previous_owner` audit info but are not needed
for state determination.) **Not in the common code flow**, provided
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
its zones, batch recovery, low overhead. **Requires a `JournalScan`
crowdb-kv RPC** (slot-range + key-prefix filter, returns ops in slot
order). This is the sole crowdb-kv extension diskdb needs. Fast because
compaction (strategy 3) keeps the uncompacted record set small.

**Strategy 3 — compaction (ongoing maintenance).**

Periodically (or when the free-record count for a zone exceeds a
threshold), merge free records into the `ZoneValue` bitmap and delete
the free records in one `batch_write`. This keeps the uncompacted record
set small, so strategy 2's replay is fast. Uses the existing `scan` +
`batch_write` API, no crowdb-kv extension needed. Batch by `disk_id`
prefix: one scan covers all zones on a disk (free records of one zone
are contiguous in the tree page). Only free records are deleted; busy
records for live blocks are untouched (busy records for freed blocks
were already deleted on free).

### How the strategies work together

- **Steady state**: allocate writes `BusyBlockValue` (and deletes any
  prior `FreeBlockKey` for that offset — re-allocate clears the free
  marker). Free deletes the `BusyBlockKey` and writes `FreeBlockValue`
  at `FreeBlockKey` in one `batch_write` (carries `previous_owner` for
  audit). The bitmap is **not touched on free** — the bit stays set,
  `used_count` is not decremented. Compaction (strategy 3) runs
  periodically (and before rotation), merging free records into
  `ZoneValue`, clearing the freed bits, recomputing `used_count`, and
  deleting the free records.
- **Restart**: load `ZoneValue` snapshot → journal scan (strategy 2)
  replays post-`snapshot_slot` operations in slot order → apply to
  bitmap. Fast because compaction kept the record set small.
- **On-demand (RPC/API)**: full scan (strategy 1) rebuilds the bitmap
  from all live records. Triggered by an operator or the scanner for a
  consistency check or full rebuild — not in the common code flow.

### Crash-safety invariants

The in-memory bitmap is a **conservative over-estimate** of busy
blocks. It is never cleared on the free path. Free is persist-only
(delete `BusyBlockKey` + put `FreeBlockValue`); the bitmap bit stays
set until compaction clears it (§5). This means the bitmap may show a
block as busy when it is actually freed on disk. This is intentional
(data-safety principle: never show a block as free until compaction has
confirmed it from records). The durable state is the set of
`BusyBlockKey` / `FreeBlockKey` / `ZoneValue` records on the bound data
group. The **current-state rule** (a block is busy iff its
`BusyBlockKey` exists) holds at every crash point. The invariants:

- **Allocate ordering** — Phase 1 (bitmap CAS, set bit) happens before
  Phase 2 (`BusyBlockValue` persist). If diskdb crashes between Phase 1
  and Phase 2, the bit is set in memory but no `BusyBlockKey` exists on
  disk. On restart, strategy 1 full scan rebuilds the bitmap from
  records — the bit is clear (no busy record), so the block is
  correctly free. This is a **ghost-busy** (bit set in-memory, no
  record) that is self-correcting on restart; the scanner also detects
  this drift during live operation.
- **Free = persist only** — the free is one `batch_write` (Delete
  `BusyBlockKey` + Put `FreeBlockValue` at `FreeBlockKey`), atomic
  within the data group via crowdb-kv paxos. The bitmap is **not**
  touched — the bit stays set, `used_count` is not decremented. The
  block is freed on disk but still shows busy in memory until
  compaction clears the bit and recomputes `used_count`. This is
  intentional: the bitmap is a conservative over-estimate that never
  shows freed space as available until compaction reconciles it. If
  diskdb crashes after the free persist, the bit is set in memory but
  no `BusyBlockKey` exists on disk. On restart, full scan sees no
  `BusyBlockKey` and clears the bit — the block is correctly free.
  Self-correcting; no drift.
- **Compaction reconciles the bitmap** — compaction (§5) is the sole
  mechanism for clearing freed bits in the bitmap. It partitions free
  records by the `compact_ts` watermark (stale = already merged, new =
  not yet merged), `range_clear`s only the new free records,
  recomputes `used_count = popcount`, and writes the new `ZoneValue`
  (with updated `compact_ts`) + deletes all scanned free records in
  **one atomic `batch_write`**. After compaction, the bitmap
  accurately reflects the durable state. Compaction runs on non-active
  zones (before they enter the active set via the preparatory thread,
  or periodically as a fallback) — no concurrent allocate.
- **Compaction crash safety** — the snapshot write and free-record
  deletion are one atomic `batch_write`; they succeed or fail together.
  If diskdb crashes during compaction, either both the new `ZoneValue`
  (with `compact_ts` advanced) and the free-record deletions are
  durable, or neither is. There is no window where the snapshot is
  written but the free records survive (the race that caused
  double-free in the two-op design). If a prior compaction crashed
  after writing the snapshot but before deleting free records (only
  possible with the legacy two-op design; the atomic batch eliminates
  this), the orphaned free records have `freed_ts <= compact_ts` and
  are dropped by the next compaction without touching the bitmap.
- **Re-allocate after compaction** — once compaction has cleared the
  bit and deleted the `FreeBlockKey`, the block is available for
  re-allocation. The re-allocate is a normal allocate (CAS-set bit +
  persist `BusyBlockValue`). No stale free marker exists (compaction
  deleted it). The `FreeBlockKey` is cleared by compaction, not by
  the re-allocate. (If a re-allocate happens before compaction deletes
  the `FreeBlockKey`, the re-allocate's `batch_write` also deletes the
  `FreeBlockKey` — the block is busy again, so the free marker is
  stale. If the re-allocate happens after compaction merged the free
  but an orphaned `FreeBlockKey` survived a crashed compaction, the
  next compaction drops it via the watermark — no double-free.)

The bitmap is never persisted on the allocate/free hot path. Only the
`ZoneValue` snapshot (written by compaction) carries a serialized
bitmap, with `snapshot_slot` (replay start point), `compact_ts`
(compaction watermark), and `crc32` (integrity check over
`usage_bitmap` + `compact_ts`). The baseline `ZoneValue` (empty bitmap,
`snapshot_slot = 0`, `compact_ts = 0`) is written during disk-add init
(root §8); all subsequent state changes are `BusyBlockValue` /
`FreeBlockValue` records until compaction merges them into a fresh
snapshot.

**Hot-path error handling:**

- **Allocate persist failure** (Phase 2 `batch_write` fails): rollback
  the bitmap (clear the bits set in Phase 1 via
  `zone.rollback_allocate`) and return error. No record was written, so
  the record set is consistent (no ghost). `rollback_allocate` is the
  bitmap CAS-clear, used only by allocate rollback, not by the free
  path.
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
  background `compaction_loop` over the owned nodes in
  `NodeContainer`. Also drives the preparatory thread for
  compaction-on-rotation.

`recover_node` creates one `ZoneDisk` per disk, recovers each zone in
parallel (bounded by `recovery_concurrency`), adds the zones to the
disk, rebuilds the active zone set (picking ready / compacted zones),
and returns the reconstructed `Node`.

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
4. Apply only `Put BusyBlockKey` ops in slot order: each sets the bit
   range using `BusyBlockValue.unit_count`. `Delete BusyBlockKey` ops
   (frees) are **ignored**. The bitmap stays as a conservative over-
   estimate (persist-only recovery, consistent with the free path
   during normal operation). `Put`/`Delete FreeBlockKey` are no-ops for
   the bitmap. The free records on disk are the source of truth for
   what's freed; the common compaction flow (§5) will `range_clear`
   their bits naturally. This eliminates the double-free risk that
   would arise if recovery cleared bits for frees while the free
   records still exist on disk.
5. Return the rebuilt `Zone`, resetting the rotating cursor. Mark the
   zone as `compacted_ready = true` (it was just recovered from
   records). `compact_ts` stays at `snapshot.compact_ts`, no
   advancement needed. The free records on disk have `freed_ts >
   snapshot.compact_ts` (they were written after the snapshot), so the
   next compaction will classify them as "new" and `range_clear` their
   bits, correct, since those blocks ARE free (no `BusyBlockKey` on
   disk). Count the net free records (Put FreeBlockKey = +1, Delete
   FreeBlockKey = -1 from re-allocate) to initialize
   `uncompacted_free_record_count` so the compaction engine knows the
   backlog. The per-disk-group monotonic timestamp source is
   initialized to `max(now(), max(freed_ts of all scanned free
   records) + 1)` after all zones in the disk-group are recovered
   (§8 Monotonic timestamp source).

`rebuild_zone_bitmap_full_scan` (strategy 1) is the on-demand fallback:

1. Read all `BusyBlockKey`s for the zone via `read_zone_records` (key
   order equals `unit_offset` order). `FreeBlockKey`s are ignored for
   state determination.
2. Set the bit range for each busy record using `unit_count` and build
   the `Zone`.
3. Optionally write a fresh `ZoneValue` snapshot so the next restart can
   use strategy 2.
4. Return the rebuilt `Zone` plus derived stats. Mark the zone as
   `compacted_ready = true`.

`compact_zone` (strategy 3) is the background maintenance task. See
§5 Compaction algorithm.

### Recovery triggers

- **Startup**: after the blocking `sync_once` fetches owned
  disk-groups, `RecoveryEngine::recover_node` runs for each one. The
  rpc server does not accept RPCs until recovery completes.
- **Ownership transfer**: when `SyncLoop` detects a disk-group newly
  assigned to this instance, it checks whether `ZoneValue` snapshots
  already exist. If they do, the new owner runs
  `RecoveryEngine::recover_node` and discards any stale in-memory state;
  otherwise it initializes the disk-group via `disk_add_init`.

## 7. Zone Allocation State (derived)

`ZoneAllocationState` is a **derived** enum for reporting only:
`Active` (`used_count == 0`), `Available` (0 < `used_count` <
`unit_capacity`), `Full` (`used_count == unit_capacity`). There is no
CAS state machine. Allocation concurrency is handled by per-bit CAS on
the usage bitmap. A zone is allocatable when it is Healthy (inheriting
the disk's `HwStatus`) and `used_count < unit_capacity`.

In the persist-only model, `used_count` is only decremented by
compaction (never by free). So after a free, `used_count` still reflects
the pre-free count. The zone may show `Full` even though it has freed
blocks on disk. This is correct: the bitmap is a conservative over-
estimate, and the derived state reflects the bitmap, not the durable
records. After compaction clears the freed bits and recomputes
`used_count`, the derived state accurately reflects the durable state.
A zone that was `Full` before compaction may become `Available` after
compaction, and then it is eligible for rotation back into the active
set.

## 8. Concurrency Model

All public and inter-module APIs are `async`. Runtime is `tokio`
(multi-threaded for production).

### Allocate (lock-free)

Per-bit CAS on the usage bitmap (`compare_exchange` on 64-bit words).
Multiple threads allocate from the same zone concurrently; no zone-level
lock held across `.await`. CAS losers re-scan the same word or try the
next zone; no thread blocking. Per-bit CAS is capped at `cas_retry_limit`
retries (default 100), then falls through to the next bit / word / zone.
Allocate runs only on active zones (in the `active_zone_context`).

### Free (persist-only, no lock)

One `batch_write` (Delete `BusyBlockKey` + Put `FreeBlockValue`). No
bitmap touch, no `used_count` decrement, no zone-level lock. Free can
run on any zone (active or not) without coordination. It only writes
to the KV store and increments `uncompacted_free_record_count`. The
bitmap is reconciled later by compaction.

### Zone-level lock for non-allocate operations

Compaction, scanner, and health checks acquire a zone-level lock
(`RwLock<()>` on `DdbZone`) to coordinate with each other. These
operations run on **non-active zones only** (no concurrent allocate), so
the lock does not contend with the allocate hot path. The lock protects
only the in-memory bitmap mutation. It is **not** held across `.await`
on the KV client. The KV read is done before acquiring the lock, and the
KV write is done after releasing the lock.

### Common methods on DdbZone

Common methods on `DdbZone` encapsulate the lock + operation:

- `compact_zone_inner()` — read free records + current `ZoneValue`
  (KV read, no lock), partition by `compact_ts` watermark (stale vs
  new), acquire zone lock, `range_clear` only new free records +
  recompute `used_count` + advance `compact_ts` (in-memory), release
  lock, write new `ZoneValue` + delete all free records in one atomic
  `batch_write` (KV write, no lock). Used by `CompactionEngine`
  (preparatory thread + periodic fallback).
- `scan_zone_inner()` — replay journal (KV read, no lock), acquire zone
  lock, compare bitmap (in-memory), release lock. Used by the scanner.
- `health_check_zone_inner()` — verify zone records (KV read, no lock),
  acquire zone lock, verify CRC + snapshot integrity (in-memory),
  release lock. Used by the health probe.
- Each method acquires the zone lock only for the in-memory bitmap
  mutation/verification, and releases it before any KV write.
- Allocate does **not** acquire this lock — it uses per-bit CAS
  (lock-free). The lock only coordinates non-allocate operations on
  non-active zones (no concurrent allocate).

Disk-level active zone set uses RCU publish (`Arc` swap) for lock-free
reads; rotation takes a brief write lock.

### Free-side lookup structures

RCU-published alongside the allocate context on add/remove/status-change:

- **disk-id → disk**: a hash map (O(1) average). Used by free to find
  the in-memory disk from the `disk_id` in the `Segment`.
- **zone-index → zone**: a vec indexed by zone-index (O(1) direct
  index). Used by free to find the in-memory zone from the
  `zone_index` in the `Segment`. (Lookup is for
  `uncompacted_free_record_count` increment; no bitmap mutation.)
- **KV free path:** the `FreeBlockKey` is constructed directly from the
  `Segment` (no lookup needed). `owner_chunk` is carried in the
  `Segment` and becomes `FreeBlockValue.previous_owner` — no KV read on
  free in v1; the free is one `batch_write` (Delete `BusyBlockKey` +
  Put `FreeBlockKey`). Ownership validation is deferred to the scanner.
  If strict ownership validation is needed before free, a config toggle
  (`validate_owner_on_free`, default false) enables a KV read of the
  `BusyBlockValue` first (one paxos round-trip, doubles free latency).

Node-level `add_disk` / `remove_disk` acquire a write lock on the disk
list; allocation/free acquire a read lock (concurrent with each other,
exclusive with add/remove).

### Monotonic timestamp source

`FreeBlockValue.freed_ts` and `ZoneValue.compact_ts` use a per-disk-
group monotonic timestamp source: wall-clock nanoseconds with a
monotonic guard. The source is an `AtomicU64` on `DdbDiskGroup`,
advanced by `max(now(), last + 1)` on each free. This ensures
monotonicity within a disk-group even under NTP adjustments, and is
human-readable for debugging (wall-clock nanoseconds, not an opaque
counter).

**Ownership transfer / recovery:** a new owner initializes the
timestamp source to `max(now(), max(freed_ts of all scanned free
records) + 1)` during `recover_node` (strategy 2/1 scans all free
records). This guarantees the new owner generates timestamps higher
than any existing free record, so the `compact_ts` watermark works
correctly across ownership transfers.

**Worst case (clock skew across owners):** if a new owner's wall clock
is behind the previous owner's, the monotonic guard (init from existing
records' `freed_ts`) keeps timestamps increasing. If the guard is
somehow bypassed (bug), a new free could get `freed_ts <= compact_ts`
and be misclassified as stale by compaction. The block stays busy
(wasted space, no data loss, I1/I7). The scanner (§9) detects this as
real ghost-busy drift and can correct it. This is the data-safety
principle: fail conservative (keep busy), never free a block that
might have data.

## 9. Background Scanner Coordination

The background scanner (root §12) is a periodic consistency check that
detects live-state drift, catches record corruption early, and gives
operators visibility into cluster health during uptime. It coordinates
with compaction via the zone-level lock (§8):

- **Zone skipping and locking** — the scanner skips zones in the disk's
  `active_zone_context` (the allocator is actively handing blocks from
  them — transient drift from the allocate Phase 1→2 window is
  expected). For non-active zones, the scanner acquires the zone-level
  lock (§8) to coordinate with compaction and health checks. Skipped
  zones are checked on a later cycle when they rotate out of the active
  set (and have been compacted).
- **Compaction coordination** — compaction and the scanner share the
  zone-level lock (§8). They run on non-active zones only. The scanner
  skips zones locked by compaction (or waits briefly, since compaction
  is fast). Common methods on `DdbZone` (§8) encapsulate the lock +
  operation, used by both compaction and the scanner.
- **Drift detection in the persist-only model** — the free path does
  not touch the bitmap (§4 Free), so "bit set, no `BusyBlockKey`" is
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

## 10. Invariants

- **I1 — Bitmap is a conservative over-estimate**: the in-memory
  `usage_bits` never shows a freed block as free until compaction clears
  the bit. Free is persist-only (no bitmap touch); compaction is the
  sole bit-clearer. `used_count` is only decremented by compaction
  (never by free).
- **I2 — Records are the source of truth**: a block is busy iff its
  `BusyBlockKey` exists on the bound data group. The bitmap is derived
  from records (via recovery or compaction) and may lag the durable
  state (conservatively). The current-state rule holds at every crash
  point.
- **I3 — Compaction is the sole bit-clearer**: no code path except
  compaction (and allocate rollback, which only clears Phase 1 claims
  that were never persisted) clears bits in `usage_bits`. The free path
  never touches the bitmap.
- **I4 — Compaction runs on non-active zones only**: no concurrent
  allocate. The zone-level lock coordinates compaction, scanner, and
  health checks on the same zone.
- **I5 — Compaction-before-rotation**: a zone must be compacted (freed
  bits cleared, `used_count` recomputed, snapshot written) before it
  enters the active set. The preparatory thread pre-compacts the next
  batch; rotation publishes ready (pre-compacted) zones.
- **I6 — Atomic snapshot + delete**: compaction writes the new
  `ZoneValue` (with advanced `compact_ts`) and deletes the free records
  in **one atomic `batch_write`**. They succeed or fail together — no
  window where the snapshot is durable but the free records survive.
- **I7 — Timestamp watermark prevents double-free**: free records with
  `freed_ts <= compact_ts` are already merged into the bitmap; compaction
  drops them without `range_clear` (the block may have been re-allocated).
  Only free records with `freed_ts > compact_ts` are merged. Worst case
  (watermark misclassifies a new free as stale): the block stays busy
  (wasted space, no data loss); the scanner reconciles later.
- **I8 — `rollback_allocate` is allocate-only**: the bitmap CAS-clear
  method is used only by the allocate Phase 2 failure path to undo
  Phase 1's claim. It is never called by the free path.
- **I9 — Zone lock not held across `.await`**: the zone-level lock
  protects only in-memory bitmap mutation. KV reads happen before
  acquiring the lock; KV writes happen after releasing it.
- **I10 — Persist-only recovery**: recovery (strategy 2) only applies
  `Put BusyBlockKey` ops to the bitmap (sets bits for allocations).
  `Delete BusyBlockKey` ops (frees) are ignored — the bitmap is a
  conservative over-estimate after recovery, same as during normal
  operation. The free records on disk are the source of truth; the
  common compaction flow clears the freed bits. This eliminates the
  double-free risk that would arise if recovery cleared bits for frees
  while the free records still exist on disk (no `compact_ts`
  advancement needed).

## 11. Tunables and Defaults

- `zone_rotate_count` — number of zones in the disk-level active zone
  set (default 4). The disk round-robins over this many zones at a time;
  when all are exhausted, the set rotates to a new batch of pre-compacted
  zones.
- `cas_retry_limit` — per-bit CAS retry cap in the zone bitmap-scan
  allocator (default 100). On exhaustion, the allocator falls through to
  the next bit / word / zone.
- `zone_size_bytes` — default zone size in bytes (default 16 GB).
- `block_size_bytes` / `allocate_granularity` — block unit size (default
  1 MB; configurable 512 KB–2 MB; must be power of 2). v1 enforces
  `allocate_granularity == block_size_bytes`.
- `compaction_cadence_secs` — periodic compaction interval (default
  300). The compaction loop sleeps this long between cycles.
- `snapshot_compaction_threshold` — compact a zone when its
  `uncompacted_free_record_count` exceeds this (default 4096). Cadence
  OR threshold — whichever fires first for a given zone.
- `validate_owner_on_free` — strict ownership validation before free
  (default false). When true, the free path reads the `BusyBlockValue`
  from the data group first and validates `owner_chunk` (one extra paxos
  round-trip, doubles free latency).
- `free_batch_enabled` — free batching toggle (default false). When
  false, frees are immediate (one `batch_write` per free). When true,
  frees are grouped and flushed via one `batch_write` when the batch
  reaches `free_flush_max_batch` (no timer).
- `free_flush_max_batch` — free batch max size before forced flush
  (default 256). Used when batching is enabled.
- `recovery_concurrency` — max concurrent zone recoveries in
  `recover_node` (default 16).
