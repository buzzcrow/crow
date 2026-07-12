# CrowKV - Design: crowtree Mapping Table

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-crowtree-core.md`](design-crowtree-core.md), [`design-crowtree-persistence.md`](design-crowtree-persistence.md)
Tracked by: [`plan-tree.md`](../plan-tree.md) #14

The mapping table is the indirection layer of the B+tree: every structural
reference (root, sibling, inner child) is a logical `PID`, and the table
translates a `PID` to the page's current location — a resident in-memory page or
an unloaded durable address. This doc specifies the workable in-memory structure,
the on-disk format, snapshot, recovery, and segment recycling.

## Table of Contents

- [1. Role and Bw-Tree Reference](#1-role-and-bw-tree-reference)
- [2. Problems with the v1 Table](#2-problems-with-the-v1-table)
- [3. Design Decisions](#3-design-decisions)
- [4. In-Memory Structure](#4-in-memory-structure)
- [5. PID Allocation](#5-pid-allocation)
- [6. Segment Recycling](#6-segment-recycling)
- [7. On-Disk Format](#7-on-disk-format)
- [8. Snapshot](#8-snapshot)
- [9. Recovery](#9-recovery)
- [10. Old Image Cleanup](#10-old-image-cleanup)
- [11. Concurrency Invariants](#11-concurrency-invariants)
- [12. Test Plan](#12-test-plan)
- [13. Open Questions](#13-open-questions)

---

## 1. Role and Bw-Tree Reference

The original Bw-Tree / LLAMA design uses the mapping table as both the
concurrency indirection point and the cache/storage indirection point:

- Tree links are logical PIDs, not physical pointers. A mapping entry translates
  a PID to either a resident memory pointer or a stable-storage address. Moving a
  page between memory and disk changes only that one entry.
- Page state is installed by replacing one mapping entry. In Bw-Tree this is a
  CAS (multi-writer); in crowtree it is a **single-writer atomic store** (the
  Flusher/reclaimer is the only writer; readers only load).
- Snapshoting persists page contents first, then persists enough mapping state
  to translate every live PID to a durable address. Recovery restores the mapping
  table, then resumes from the durable frontier.
- Unused entries persist as empty; resident entries are converted to their
  durable address before the commit point.

Crowtree keeps these ideas but simplifies for its constraints: one writer, COW
root versions, no internal data WAL (recovery = snapshot + consensus replay,
persistence §6), and `PageStore` backends (file/block/RDMA). The durable mapping
table is therefore a **snapshoted metadata image of `PID -> PageAddr`**, not a
second redo log.

---

## 2. Problems with the v1 Table

- **P1 — PID leak.** `next_page_id_` grows monotonically; merged-away PIDs are
  never cleared (`crowtree.cc:570`), so segment count grows with total split/merge
  count, not live tree size.
- **P2 — full manifest per snapshot.** Every snapshot DFS-walks the tree and
  writes `(pid, addr, len)` for all reachable pages — O(N) I/O even if few pages
  changed (~120 MB for a 100 GB tree).
- **P3 — snapshot holds `write_mutex_`** for its full duration (addressed by
  #3/#8/#11; the manifest size problem remains).
- **P4 — file-specific layout.** The superblock A/B + manifest region assumes the
  file backend; block/RDMA need a backend-neutral format.

---

## 3. Design Decisions

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | **No PID recycling.** | A reused PID could be seen by a stale reader as the new page → silent wrong data. Data integrity outweighs the memory saved. `next_page_id_` is monotonic. |
| D2 | **Segment recycling — YES.** | When all slots in a segment are empty, free its memory. Bounds growth to live segments. Safe because PIDs are never reused, so a freed interior segment is never re-created (§6). |
| D3 | **Sparse segments acceptable.** | One live slot pins an 8 KB segment. Migrating the slot would change the PID (referenced everywhere) — far worse. Segment size is tunable. |
| D4 | **Segment-level persistence.** | Persist only *dirty* segments as self-describing images; the mapping table itself is the durable structure. No base+delta metadata WAL. |
| D5 | **Backend-neutral format.** | All I/O via `PageStore` (`allocate`/`write_page`/`read_page`/`free`/`flush`). No fixed superblock/manifest region. |
| D6 | **Tiny fixed commit anchor + separate segment directory.** | The anchor is a small fixed A/B record (atomic commit point); it points to a segment-directory image, so the commit stays atomic even for thousands of segments. |

---

## 4. In-Memory Structure

```cpp
constexpr uint32_t kSegSlots = 1024;   // Options.mapping_segment_slots, fixed per tree

struct Segment {
    std::atomic<uint64_t> slots[kSegSlots];  // packed slot words (see below)
    std::atomic<uint32_t> live_count;        // non-empty slots; 0 => recyclable
    std::atomic<uint32_t> generation;        // bumped when persisted (image version)
    std::atomic<bool>     dirty;             // any slot changed since last persist
};

class MappingTable {
    std::atomic<Segment*> segments_[kMaxSegments];
    std::atomic<uint64_t> next_page_id_;
    // writer-owned dirty-set of segment indices (drained at snapshot)
};
```

**Packed slot word (64-bit)** — the same encoding in memory and on disk:

```
word == 0                 -> empty (dead or never-allocated PID)
(word & 1) == 0, != 0     -> resident: PageBase* (pointer, 8-byte aligned) [in-memory only]
(word & 1) == 1           -> unloaded descriptor:
                               bits [63:24] iu_index  (durable PageAddr, in IUs)
                               bits [23:1]  iu_count   (page length, in IUs)
                               bit  [0]     = 1 (tag)
```

Resident pointers are **never** persisted. On disk a slot is only `0` (empty) or a
tagged unloaded descriptor, so a segment image is literally the array of packed
words — recovery installs them into `slots[]` with **zero decode** (Bw-Tree's
"the mapping table IS the persistent structure").

`Get(pid)`: load segment ptr (nullptr => empty), load slot word. Resident =>
return `PageBase*`. Unloaded => demand-load via buffer pool (persistence §4.4),
publish the resident pointer, return. Empty => page is gone; caller retries from
root. All loads are lock-free under an epoch guard.

---

## 5. PID Allocation

`next_page_id_ = fetch_add(1)` (writer only). `seg_idx = pid / kSegSlots`,
`slot = pid % kSegSlots`. First use of a segment allocates the `Segment` and
installs it with an atomic store. PIDs are never returned to a free list (D1).

---

## 6. Segment Recycling

Slot clearing runs in the **epoch deleter** (after all guards that could see the
page have drained; core §10, persistence §4.6):

```
on page retire (merge / COW-evict that unlinks the PID):
    seg->slots[i].store(0, release)          // empty
    if seg->live_count.fetch_sub(1) == 1:    // reached 0
        if segments_[seg_idx].CAS(seg, nullptr):
            epoch.retire(seg)                // freed after guards drain
    mark_dirty(seg_idx)                       // slot changed -> persist as empty
```

Safe and final because PIDs are monotonic: a freed interior segment's PID range
is dead forever, so allocation never re-creates it. A reader that loads a
`nullptr` segment (or an empty slot) treats the PID as gone and retries from the
root — never wrong data (D1).

---

## 7. On-Disk Format

All records are `PageStore` allocations, self-describing, CRC-protected.

### 7.1 Segment image (one per dirty segment per snapshot)

```
SegmentImageHeader (fixed):
  magic u32='CTMS', format_version u16, flags u16
  seg_idx u32, generation u64
  slot_count u32 (= kSegSlots), live_count u32
  header_crc u32
Body: uint64_t packed_word[slot_count]      // empty(0) or unloaded descriptor
Trailer: body_crc u32                        // over header+body
```

Size = header + `kSegSlots*8` (≈ 8 KB for 1024 slots). Only dirty segments are
written per snapshot.

### 7.2 Segment directory image (rewritten when any segment generation changes)

```
DirHeader: magic='CTSD', format_version, entry_count u32, header_crc u32
Body: DirEntry[entry_count]
  DirEntry { seg_idx u32; pad u32; generation u64; image_addr u64; image_len u32; image_crc u32 }
Trailer: body_crc u32
```

Cost is O(live segments) (~195 KB for a 100 GB tree) — negligible vs the old
120 MB full manifest. The directory decouples the (possibly large) segment list
from the tiny atomic anchor.

### 7.3 Commit anchor (tiny, fixed size, A/B double-buffered)

```
CommitAnchor (fixed, written to reserved slot A or B):
  magic u32='CTCA', format_version u16, flags u16
  snapshot_seq u64            // monotone; recovery picks highest valid
  root_pid u64, leftmost_leaf_pid u64
  last_applied_slot u64, next_page_id u64
  segment_slots u32, pad u32
  segdir_addr u64, segdir_len u32, segdir_crc u32
  page_alloc_root u64          // space-allocator snapshot state
  anchor_crc u32
```

The anchor is the **commit point**. Its small fixed size makes the A/B swap
atomic on every backend (file `fsync`, block sector write, RDMA IU-aligned write).
**IU 0 (slot A) and IU 1 (slot B)** are reserved for the anchor at store-create;
all other addresses are normal allocation. A snapshot writes the anchor to the
slot *not* named by the current highest-seq anchor, so a torn write never destroys
the last committed anchor.

---

## 8. Snapshot

Integrates with the persistence pipeline (persistence §5A). Runs without
`write_mutex_` on the I/O phase (async PageStore, #11).

```
snapshot(seq):
  1. Flusher writes dirty page frames; buffer pool assigns each a durable
     PageAddr (append cursor) and page_len.                       [persistence §4.5]
  2. Serialize each dirty segment: for each slot ->
       empty -> 0; unloaded -> its word; resident -> pack(frame.durable_addr, len).
     Bump generation, allocate image addr, write_page.
  3. If any generation changed, rewrite the segment directory image, write_page.
  4. PageStore.flush()                       // frames + images + dir DURABLE
  5. Write commit anchor to the next A/B slot (seq = prev+1).
  6. PageStore.flush()                       // anchor DURABLE == commit point
  7. Clear dirty bits, drain dirty-set, schedule old-image cleanup (§10).
```

**Ordering rule:** frames + segment images + directory are durable *before* the
anchor. A crash before step 6 leaves the previous anchor (and its images) intact;
after step 6 the new snapshot is live.

---

## 9. Recovery

```
recover():
  1. Read anchor slots A and B; pick the highest snapshot_seq with valid crc.
     none valid -> empty tree (last_applied_slot=0).
  2. Read the segment directory image at segdir_addr (crc-checked).
  3. For each DirEntry: read the segment image (crc-checked); allocate the
     Segment; memcpy packed words into slots[]; set live_count, generation.
  4. Set root_pid, leftmost_leaf_pid, next_page_id, last_applied_slot,
     segment_slots, page_alloc_root from the anchor.
  5. Pages are demand-loaded lazily on first Get (slots are unloaded).  [persistence §7]
```

No page bytes are read eagerly — restart is fast even for large trees. A segment
or directory image failing CRC while its anchor was committed indicates media
corruption → fail the node out; it rejoins via snapshot install (state-machine §4.5).

---

## 10. Old Image Cleanup

Segment/directory images accumulate (a new addr per generation). Reclaim with a
**two-generation rule**: after anchor `N` commits, `free()` images referenced
only by anchors `< N-1`. Track superseded `(addr, len)` in a pending-free list
keyed by the superseding seq; release entries whose key `< N-1`. This tolerates a
crash during cleanup (the still-referenced generation is always intact).

---

## 11. Concurrency Invariants

- **Single writer of slots.** The Flusher (and the epoch reclaimer it drives)
  are the only slot writers; no CAS needed for slot stores. Readers only
  atomic-load. Segment install/retire is an atomic store/CAS on `segments_[i]`.
- **Epoch safety.** Slot clearing and segment retire happen in the epoch deleter,
  after all overlapping reader guards drain — no use-after-free (persistence §4.6).
- **Demand load.** Resident publication (unloaded -> resident) is a single atomic
  store on the writer side during a miss install; readers re-load.

---

## 12. Test Plan

- **Unit:** packed-word pack/unpack round-trip; segment image + directory + anchor
  serialize/parse with CRC; empty/resident/unloaded slot transitions.
- **Recovery:** crash before vs after anchor commit; torn segment image (bad CRC);
  torn anchor (A valid / B invalid, and vice-versa); highest-seq selection.
- **Recycling:** split/merge churn drives segments to empty; assert memory freed;
  TSan/ASan clean; stale-reader-sees-empty (never wrong value).
- **Incremental cost:** modify K pages in M segments; assert only M images + dir
  written (not full tree).
- **Backend abstraction:** run the suite on mem `BlockPageStore` and `FilePageStore`.
- **Demand load:** reopen; first Get loads the page; values equal pre-crash.

---

## 13. Settled Decisions & Open Questions

**Settled (2026-07-01):**

1. **On-disk format = clean break.** Nothing is released, so no compatibility is
   required. Segment images + directory + A/B anchor replace the v1
   superblock/manifest layout; a `format_version` guard in the anchor refuses to
   open an older/foreign format. No converter.
2. **Anchor region = fixed A/B at IU 0 and IU 1** (§7.3), reserved at
   store-create; recovery picks the highest valid `snapshot_seq`.
3. **Terminology = "snapshot"** (no "checkpoint"); the durable persist is the
   persist phase of `create_snapshot` (plan-tree #8, code rename #19).

**Open:**

1. **Native snapshot sharing (#16).** The native frame snapshot format should
   reuse the segment-image + directory encoding; confirm one shared serializer.
2. **Fault-injection test design (#14e).** `FaultyPageStore` fault points
   (which writes to drop/tear/reorder, and the assertion matrix) to be specified
   in `design-crowtree-test.md`.
