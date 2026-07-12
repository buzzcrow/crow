# CrowKV - Design: crowtree Core Data Structure

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-state-machine.md`](design-state-machine.md) (apply semantics, slot rules)

This document specifies crowtree's in-memory data structure and the read/write
algorithms: the MemTable (L0), pages, the mapping table, delta records, the
slot-aware value cell, the two-level write path (apply→MemTable, then
flush→delta→consolidate→split/merge), the versioned root for consistent snapshots,
and epoch-based reclamation.

On-disk format, backends, checkpoint, and recovery are in
[`design-crowtree-persistence.md`](design-crowtree-persistence.md).

## Table of Contents

- [1. Logical Model](#1-logical-model)
- [2. Slot-Aware Value Cell](#2-slot-aware-value-cell)
- [3. Pages](#3-pages)
- [4. Mapping Table](#4-mapping-table)
- [5. Delta Records](#5-delta-records)
- [6. Write Path: MemTable Ingest + Flush](#6-write-path-memtable-ingest--flush)
- [7. Consolidation](#7-consolidation)
- [8. Page Split and Merge](#8-page-split-and-merge)
- [9. Versioned Root (Consistent Snapshots)](#9-versioned-root-consistent-snapshots)
- [10. Epoch-Based Reclamation](#10-epoch-based-reclamation)
- [11. Read Path](#11-read-path)
- [12. Concurrency Summary](#12-concurrency-summary)

---

## 1. Logical Model

crowtree is an ordered map `key → (slot, cell)` where `key` is an opaque byte
string compared lexicographically, `slot` is the resolved consensus slot, and
`cell` is a live value or a tombstone.

It is a **bounded 2-level** structure (`design-crowtree.md` D3):

- **L0 — MemTable.** A concurrent in-memory ordered map that absorbs `apply`
  (concurrent, possibly out-of-order by slot; §6.1). Keeps one (highest-slot)
  cell per key.
- **L1 — B+tree.** A **single-writer / multi-reader** B+tree with copy-on-write
  base pages and a per-leaf delta chain. Inner pages hold separator keys + child
  PIDs; leaf pages hold sorted `(key, slot, cell)` entries and a `right_sibling`
  link for range scans. Reachable from a `root_pid` through the mapping table.

The single **Flusher** thread merges the MemTable's contiguous-applied prefix into
L1 ("flush = the persistent write"; §6.2). Reads overlay L0 on L1 (§11), so read
amplification is bounded at 2. The B+tree has exactly one writer (the Flusher).

---

## 2. Slot-Aware Value Cell

Every value stored in a leaf or a delta carries the slot and a kind flag:

```
cell_payload := [slot: u64 LE][flags: u8][value bytes...]
flags bit0 = tombstone (1 → value bytes empty)
flags bit1..7 reserved
```

- **Highest-slot-wins:** on write, if the incoming `slot <= existing slot` for a
  key, the write is skipped (idempotent; `design-state-machine.md §3.3, §4.2`).
- **Tombstone:** a delete is a cell with the tombstone flag; it occupies space
  until GC removes it below the watermark (`design-crowtree-snapshot-gc.md`).
- `get` on a tombstone returns `None`; `iter_all` returns the tombstone (for
  `compare`).

This inlining is the single difference from a generic ordered KV engine; it is
why a stock bw-tree cannot be used unmodified.

---

## 3. Pages

A page is the unit of indexing, consolidation, and (when flushed) I/O. All pages
begin with a common header.

```cpp
enum class PageType : uint8_t {
    kLeafBase    = 0x02,
    kInnerBase   = 0x01,
    kBatchDelta  = 0x12,   // batch of (key, slot, cell) upserts/deletes
    kSplitDelta  = 0x20,   // leaf split, phase 1 (writer-exclusive)
    kIndexDelta  = 0x21,   // inner index-term insert
    kMergeDelta  = 0x30,   // sibling absorb
    kRemoveDelta = 0x31,   // node removal
};

struct PageBase {           // 32 bytes
    PageType  type;
    uint8_t   flags;        // bit0 compressed
    uint16_t  delta_len;    // delta chain length above this node
    uint32_t  chain_bytes;  // approximate chain byte size (consolidation trigger)
    uint64_t  self_pid;
    PageBase* next;         // nullptr for base pages
};
```

### Leaf base page

In-memory leaf base pages mirror the pagetree layout: header, optional bloom
filter, four `uint32_t` metadata arrays (key offsets, cell offsets, key sizes,
cell sizes), then concatenated key bytes and cell payloads. Entries are sorted by
key. The cell payload is the `[slot][flags][value]` from §2.

Leaf pages are sized to a target (default 64 KiB) and, when flushed, padded to
the backend's IU (default 16 KiB) — see persistence doc. In memory before a
checkpoint, a leaf base page is a plain heap allocation (no IU padding) to keep
idle overhead low.

```cpp
struct LeafHeader {
    PageBase base;            // type = kLeafBase
    uint32_t count;
    uint32_t data_bytes;
    uint64_t right_sibling;   // PID, kInvalidPID if rightmost
    uint32_t low_key_off, low_key_size;   // 0 size = -inf (leftmost)
    uint32_t high_key_off, high_key_size; // 0 size = +inf (rightmost)
    uint32_t bloom_bytes;
    uint8_t  bloom_k;
    uint8_t  compression;     // 0 none, 1 LZ4, 2 zstd
};
```

### Inner base page

Inner pages store separator keys + child PIDs only (no values), so they stay
small and live in memory; they are written only at checkpoint. Same metadata-array
style, with child PIDs instead of cells.

---

## 4. Mapping Table

A logical page id (`PID`, `uint64_t`) indexes a two-level array of
`std::atomic<PageBase*>`. All structural references (root, sibling links, inner
children) are PIDs, not raw pointers, so a page can be replaced (consolidate,
split, flush-in/out) by swapping one slot.

```cpp
class MappingTable {
    static constexpr uint64_t kInvalidPID = UINT64_MAX;
    static constexpr size_t   kSegmentSize = 1 << 10;   // 1K slots = 8 KiB
    PageBase* Get(uint64_t pid) const;                  // atomic load (readers)
    void      Store(uint64_t pid, PageBase* page);      // writer only, no CAS
    uint64_t  AllocatePID();
    void      FreePID(uint64_t pid);                    // recycled via free list
};
```

**Single-writer simplification (D2).** The pagetree `Install(expected, desired)`
CAS-with-retry is replaced by a plain `Store`. Only the **Flusher** thread mutates
the tree, under the tree writer lock, so there is no install race. (`apply` itself
never touches the tree — it writes the MemTable.) Readers still do an atomic load,
so reads remain lock-free.

Segments are allocated on demand; an idle tree costs one 8 KiB segment.

---

## 5. Delta Records

A **flush** prepends an immutable delta to each affected leaf's chain instead of
rewriting the whole leaf. crowtree needs only one data delta type — the batch
delta — one per leaf per flushed slot.

```cpp
// One slot's batch worth of mutations targeting a single leaf page.
// Entries sorted by key; each carries its own cell (slot + kind + value).
struct BatchDelta {            // type = kBatchDelta
    PageBase base;
    uint64_t slot;             // the consensus slot of this batch
    uint32_t count;
    uint32_t data_bytes;
    // followed by: key_off[count], cell_off[count], key_sz[count], cell_sz[count],
    //              then key bytes, then cell payloads
    uint32_t FindKey(Slice key) const;   // binary search within the delta
};
```

A single flushed slot may touch several leaves; the Flusher produces one
`BatchDelta` per affected leaf (all stamped with the same `slot`) — the slot
fan-out noted in `design-crowtree.md` D5. Put and Delete are not separate delta
types — a delete is a tombstone cell inside the batch.

SMO deltas (`kSplitDelta`, `kIndexDelta`, `kMergeDelta`, `kRemoveDelta`) appear
only transiently during a writer-exclusive split/merge (§8); there is no abort
delta because there is no competing writer.

---

## 6. Write Path: MemTable Ingest + Flush

### 6.1 MemTable ingest (`apply`)

`apply(slot, batch, contiguous_slot)` does **not** touch the B+tree. It folds the
batch into the concurrent MemTable and records the learner's contiguous frontier:

```
apply(slot, batch, contiguous_slot):
    if !batch.empty():
        for (key, op, value) in batch:            // intra-batch: last occurrence wins
            if slot <= last_applied_slot: continue // already durable in L1; drop
            memtable.upsert_if_higher(key, {slot, kind(op), value})
    contiguous.store_max(contiguous_slot)          // frontier supplied by the learner
    maybe_signal_flush()                           // size/entry/time threshold crossed
    return Ok
```

- The MemTable keeps **one cell per key** (highest slot wins), so repeated writes
  to a hot key collapse in memory before ever reaching the tree.
- `apply` drops cells already durable in L1 (`slot <= last_applied_slot`) and keeps
  the highest slot per key, so the MemTable always holds cells **strictly newer**
  than L1 for any shared key — this is what makes the L0-first read (§11) correct.
- `apply` may run concurrently from the FFI ingest task(s); the MemTable is the
  only concurrently-mutated structure (skiplist or sharded ordered map).
- A NoOp / empty batch still advances `contiguous_slot` (the learner counts it),
  so the Flusher never blocks on the gap a NoOp leaves (`design-crowtree.md §4.1`).

### 6.2 Flush (MemTable → B+tree)

The single Flusher thread, when a size/entry or time threshold trips, drains the
**contiguous-applied prefix** of the MemTable into L1:

```
flush():
    cs = contiguous.load()
    drained = memtable.take_while(entry.slot <= cs)     // ordered by key
    if drained.empty(): return
    guard = env.epoch.enter()
    groups = group_by_leaf(drained)                     // find_leaf(key) per run
    for (pid, group) in groups:
        head  = mapping.Get(pid)
        delta = build_batch_delta(flushed_slot = cs, group)
        delta.base.next        = head
        delta.base.delta_len   = head.delta_len + 1
        delta.base.chain_bytes = head.chain_bytes + delta.bytes()
        mapping.Store(pid, delta)                        // sole writer, plain store
        dirty.mark(pid)
        if delta.delta_len > policy.max_delta_len
           or delta.chain_bytes > policy.max_delta_bytes:
            consolidate(pid)                            // may split/merge (§7, §8)
    publish_root_version(version++, root_pid, last_applied_slot = cs)   // §9 snapshot
```

- Every cell in a flush has `slot <= cs <= last_applied_slot`, so the published
  `RootVersion` is an **exact point-in-time** state (§9) — no >frontier slots ever
  reach the tree.
- The Flusher is the sole tree mutator, so mapping stores need no CAS; a multi-leaf
  flush is atomic to readers per leaf (each head swap is one atomic store), and the
  L0 overlay (§11) covers any cross-leaf read consistency during the flush.

**Highest-slot-wins placement.** Resolved consistently in three places: MemTable
upsert (memory), consolidation (chain fold), and read (L0 overlay + chain scan). A
higher-slot cell always shadows a lower-slot one (`design-state-machine.md §4.2`).

**Batching benefit.** Hot keys collapse in the MemTable; each flush does one atomic
store + at most one consolidation per affected leaf per flushed slot — not per key.
This is the write batching of D3/D4 without global compaction.

---

## 7. Consolidation

When a leaf's delta chain exceeds `max_delta_len` (default 8) or
`max_delta_bytes` (default 256 KiB), the chain is folded into a fresh leaf base
page.

```
consolidate(pid):
    head = mapping.Get(pid)
    merged = replay_chain(head)        // walk head→base, newest wins per key,
                                       // keep highest slot per key, drop nothing yet
    if merged.data_bytes > split_threshold:  return split(pid, merged)
    if merged.data_bytes < merge_threshold and pid != root: return try_merge(pid, merged)
    new_base = build_leaf(pid, merged) // sorted entries + bloom + CRC (+ optional compress)
    mapping.Store(pid, new_base)
    guard.retire_chain(head)           // epoch-deferred free of old deltas+base
```

`replay_chain` resolves duplicates by **highest slot** (not merely newest chain
position), which is the authoritative point where the slot rule is enforced.
Tombstones are preserved here; they are removed only by GC below the watermark
(`design-crowtree-snapshot-gc.md`), never by plain consolidation.

Consolidation may run eagerly on the apply thread (default) or be deferred to the
shared `CrowtreeEnv` consolidation pool.

---

## 8. Page Split and Merge

Page-level SMOs keep leaf pages within size bounds. Because there is exactly one
writer, the multi-phase cooperative protocol of the pagetree reference is
**dropped**; a split/merge is a writer-exclusive in-place restructure followed by
atomic mapping-table stores.

**Split** (leaf exceeded `split_threshold`, default = target page bytes):

```
split(pid, entries):
    i = split_point_by_bytes(entries)          // cumulative bytes cross threshold/2
    left  = build_leaf(pid,        entries[0..i])
    right = build_leaf(new_pid=alloc, entries[i..])
    split_key = entries[i].key
    right.right_sibling = left.right_sibling
    left.right_sibling  = right.self_pid
    mapping.Store(right.self_pid, right)
    mapping.Store(pid, left)                    // left replaces old leaf
    insert_index_term(parent_of(pid), split_key, right.self_pid)  // may recurse/split inner
```

**Merge** (leaf below `merge_threshold`, default = target/4, non-root):

```
try_merge(pid, entries):
    sib = left_sibling(pid)                     // prefer absorbing into left
    if sib is None or combined_bytes(sib, pid) > target: return            // skip
    merged = merge_entries(sib, entries)
    new_base = build_leaf(sib.pid, merged)
    new_base.right_sibling = right_sibling(pid)
    mapping.Store(sib.pid, new_base)
    remove_index_term(parent, separator_for(pid))
    mapping.FreePID(pid)
    guard.retire(old sib chain, old pid chain)
```

Inner pages split/merge by the same logic (separator keys + child PIDs, no
values). Root collapses when it has a single child; root grows a new level when
an inner split propagates past the current root. Thresholds use a hysteresis gap
(split at target, merge at target/4) to avoid split-merge oscillation.

---

## 9. Versioned Root (Consistent Snapshots)

Steady-state reads use epoch pinning (§10) on the live chain. For **long-lived
consistent views** — `scan` with a large result, `compare`, `snapshot_export`,
and recovery anchoring — crowtree maintains an immutable **versioned root**.

```cpp
struct RootVersion {
    uint64_t version;            // monotonically increasing
    uint64_t root_pid;           // root of an immutable, fully-consolidated tree
    uint64_t last_applied_slot;  // engine state == applying all slots <= this
    uint32_t refcount;           // live readers + (export/recovery holds)
};
class VersionTable { /* keeps the newest plus any pinned older versions */ };
```

- At `persist_checkpoint` (and optionally on a cadence), the writer **freezes**
  the current tree: consolidates dirty leaves into immutable base pages, records
  a new `RootVersion{version, root_pid, last_applied_slot}`, and makes it the
  current version. This is the copy-on-write boundary of D4.
- A reader takes `snapshot_view()` → pins the current `RootVersion` (refcount++).
  All `EngineView` methods read that fixed tree; writes after the pin allocate new
  pages and never mutate the pinned version's pages.
- A `RootVersion` (and the pages reachable only from it) is reclaimable when
  `refcount == 0` **and** `last_applied_slot < gc watermark`
  (`design-crowtree-snapshot-gc.md`).

This gives true MVCC snapshots for readers/export without the multi-version
per-key storage that `design-state-machine.md §3.2` rules out: only whole *tree
versions* are retained briefly, not multiple versions per key.

Steady state keeps just one live version; older versions exist only while a long
reader or an in-flight snapshot export holds them.

---

## 10. Epoch-Based Reclamation

Page lifetime (deletion / references / safe concurrent access) is governed by an
epoch manager shared in `CrowtreeEnv`, reusing the pagetree `EpochManager` idea.

```cpp
class EpochManager {
    EpochGuard Enter();        // reader/writer: publish local epoch (one atomic store)
    void Retire(void* p, deleter); // defer free until all threads leave current epoch
    void AdvanceEpoch();       // background
    size_t TryReclaim();       // GC pool: free garbage from drained epochs
};
```

- A reader does `Enter()` (nanosecond-scale) before touching any page and exits
  when the guard drops.
- The writer `Retire()`s pages it replaces (old delta chains, old base pages,
  freed leaf/inner pages from split/merge, unreferenced `RootVersion` pages).
- A retired page is freed only after every thread that could hold a pointer has
  left the epoch in which it was retired.

Because there is a single writer, `Retire` is uncontended; readers pay only the
enter/exit. This is the one mechanism that answers page deletion, page
references, and concurrent-read performance together (D6).

---

## 11. Read Path

```
get(key):
    guard = env.epoch.enter()
    if cell = memtable.get(key):          // L0 first: holds the newest cell if present
        return cell.tombstone ? None : (cell.slot, cell.value)
    pid  = find_leaf(key)                  // L1: descend inner pages from root_pid
    for node in chain(pid):                // head → base
        switch node.type:
          kBatchDelta:  i = node.FindKey(key); if found return cell_of(i) (tombstone→None)
          kLeafBase:    i = bsearch(node, key); return found ? cell : None
          kSplit/Merge: follow redirect
    return None
```

- **L0 overlay.** Because `apply` drops cells `<= last_applied_slot` and keeps the
  highest slot per key (§6.1), any key present in the MemTable has a slot strictly
  newer than L1, so checking L0 first is correct (read amplification 2).
- `scan(prefix, limit)` / range reads use a **merge cursor** over L0 and the L1
  leaf chain: at each step take the smaller key, and on a key tie take the L0 cell
  (newer). Within L1, materialize the current leaf's live entries (resolving the
  delta chain by highest slot), then follow `right_sibling`. For bounded `limit`
  the live overlay is fine; for large scans the cursor runs on a pinned
  `RootVersion` (§9) merged with an L0 snapshot, refreshing the epoch per leaf.
- `multi_get` is a batched `get`.
- `iter_all` (for `compare`) always runs on a pinned `RootVersion` (merged with an
  L0 snapshot) and includes tombstones.

Reads never block apply and apply never blocks reads: readers see immutable
pages; the writer only ever publishes new pages via atomic stores and retires old
ones via the epoch manager.

---

## 12. Concurrency Summary

| Actor | Mechanism |
| --- | --- |
| MemTable ingest (`apply`, ≥ 1) | Concurrent ordered-map upsert; no tree access |
| Flusher (1 per tree) | Sole tree writer: drain prefix → `BatchDelta` → plain `Store`; epoch `Retire` |
| Point/range readers (N) | Epoch `Enter`/exit; L0 overlay + lock-free atomic loads of immutable pages |
| Long readers / export | Pin a `RootVersion` (refcount) + L0 snapshot for a stable MVCC view |
| Consolidation / GC workers | `CrowtreeEnv` shared pools; epoch-gated frees |

Invariants:

- **I1** A page is freed only after no reader epoch can reference it.
- **I2** A pinned `RootVersion`'s pages are immutable until its refcount hits 0.
- **I3** Per-key resolved slot is monotone (highest-slot-wins at upsert, read & consolidate).
- **I4** A reader observes a linearizable point-in-time state (L0 overlay on the live
  tree, or a pinned version), never a partial multi-leaf flush.
- **I5** For any key in both, the MemTable cell's slot is strictly newer than L1's
  (apply drops `slot <= last_applied_slot`), so L0-first reads are correct.
- **I6** The B+tree only ever holds slots `<= last_applied_slot` (flush drains the
  contiguous prefix only), so every `RootVersion` is an exact point-in-time state.
