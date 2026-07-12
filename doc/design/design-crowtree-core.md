# CrowKV - Design: crowtree Core Data Structure

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-state-machine.md`](design-state-machine.md) (apply semantics, slot rules)

This document specifies crowtree's in-memory data structure and the read/write
algorithms: the MemTable (L0), pages, the mapping table, delta records, the
slot-aware value cell, the two-level write path (apply→MemTable, then
flush→delta→consolidate→split/merge), the versioned root for consistent snapshots,
and epoch-based reclamation.

On-disk format, backends, snapshot, and recovery are in
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
  cell per key. The ordered map is an **`absl::btree_map`** (D-Q10): B-tree fanout
  gives cache-friendly point lookups and preserves ordered iteration for
  drain/snapshot. Key/value bytes are stored as move-only `buffer`s
  (`design-crowtree-memory.md`), not `std::string`, so the write path is
  single-allocation and zero-copy down to the frame build. A future double-buffer
  (active + flushing) allows writes to continue during flush (plan #3).
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
snapshot, a leaf base page is a plain heap allocation (no IU padding) to keep
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
small and live in memory; they are written only at snapshot. Same metadata-array
style, with child PIDs instead of cells.

---

## 4. Mapping Table

> **Full design:** [`design-crowtree-mappingtable.md`](design-crowtree-mappingtable.md)
> (task #14). Summary below; that doc has the workable spec.
>
> The mapping table maps `PID (uint64_t) → atomic<uint64_t>` **packed slot word**:
> `0`=empty, `bit0=0`=resident `PageBase*`, `bit0=1`=unloaded `(iu_index,
> iu_count)`. All structural references (root, sibling links, inner children) are
> PIDs, not raw pointers, so a page is replaced (consolidate, split, flush-in/out)
> by storing one slot. Readers do a lock-free atomic load under an epoch guard;
> only the Flusher/reclaimer stores (single-writer, no CAS).
>
> What is settled:
> - PID recycling: **NO** — race condition risk too high; `next_page_id_` monotonic
> - Segment recycling: **YES** — epoch deleter frees empty segments (safe: PIDs
>   never reused, so a freed segment is never re-created)
> - Sparse segments: **acceptable** — 8 KB waste per segment
> - Persistence: **segment images + directory + tiny A/B commit anchor** (replaces
>   the full manifest); recovery loads packed words directly, pages demand-loaded
> - Backend abstraction: **YES** — all I/O via `PageStore` interface

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

`apply(slot, batch)` does **not** touch the B+tree. It folds the
batch into the concurrent MemTable (`absl::btree_map<buffer, buffer>`, D-Q10):

```
apply(slot, batch):
    if !batch.empty():
        for (key, op, value) in batch:            // intra-batch: last occurrence wins
            if slot <= last_applied_slot: continue // already durable in L1; drop
            memtable.try_emplace(std::move(key_buf), std::move(cell_buf))  // move-only
    maybe_signal_flush()                           // size/entry/time threshold crossed
    return Ok
```

The learner's contiguous frontier is advanced separately via `force_advance_slot`
(§4.1); `apply` no longer takes a `contiguous_slot` parameter.

- The MemTable keeps **one cell per key** (highest slot wins), so repeated writes
  to a hot key collapse in memory before ever reaching the tree.
- `apply` drops cells already durable in L1 (`slot <= last_applied_slot`) and keeps
  the highest slot per key, so the MemTable always holds cells **strictly newer**
  than L1 for any shared key — this is what makes the L0-first read (§11) correct.
- `apply` may run concurrently from the FFI ingest task(s); the MemTable is the
  only concurrently-mutated structure (`absl::btree_map` under a mutex, or
  double-buffered active/flushing pair — plan #3).
- A NoOp / empty batch still advances the contiguous frontier via
  `force_advance_slot` (the learner counts it), so the Flusher never blocks on
  the gap a NoOp leaves (`design-crowtree.md §4.1`).

### 6.2 Flush (MemTable → B+tree)

The single Flusher thread, when a size/entry or time threshold trips, drains the
**contiguous-applied prefix** of the MemTable into L1:

```
flush():
    cs = contiguous.load()
    drained = memtable.take_while(entry.slot <= cs)     // ordered by key
    if drained.empty(): return
    guard = epoch_.enter()                          // tree-owned epoch (D-Q9, §10)
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
  reach the tree. **`flush` *is* snapshot creation *is* snapshot** (R5, OQ5):
  the public API unifies under snapshot terminology (`create_snapshot` = this
  drain + publish + **persist to disk**), and `snapshot_view` returns the latest
  pinned root rather than materializing a copy. Every snapshot is durable — there
  is no "in-memory-only flush." Recovery uses the latest durable snapshot's slot
  as the replay starting point.
- **Dual trigger (D-Q11).** Flush fires on a MemTable size/entry limit (primary)
  **or** a long time interval since the last flush (secondary safety net,
  default ~2 h) so a slow-write workload cannot leave L0 un-flushed and make crash
  recovery replay unbounded (`design-crowtree.md §4.1`).
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

Consolidation may run eagerly on the apply thread (default) or be deferred to a
worker pool injected via `Options` (future; `CrowtreeEnv` has been removed — OQ7).

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

- At `persist_snapshot` (and optionally on a cadence), the writer **freezes**
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
reader holds them. Snapshot export always pins the **current** version —
historical snapshot export (exporting an arbitrary past slot) is not supported;
Raft InstallSnapshot always installs the latest durable state.

---

## 10. Epoch-Based Reclamation

Page lifetime (deletion / references / safe concurrent access) is governed by an
epoch manager **owned by the `Crowtree` instance** (D6, revised in
`design-crowtree.md` D-Q9), reusing the pagetree `EpochManager` idea. It was
originally shared in `CrowtreeEnv`; because the buffer pool and all retired pages
are tree-private there is no cross-tree page sharing, so a per-tree epoch is
simpler and lets `get`/`scan` hand back **borrowed zero-copy views** into resident
frames that stay valid for the read guard's lifetime
(`design-crowtree-memory.md §4`).

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

### 10.1 Lock-Free EBR (future optimization)

**Current implementation** uses a single `std::mutex` in `EpochManager` — every
`enter()` and `exit()` (i.e. every `get()`/`scan()`) takes the mutex. The
critical section is short (μs), but on high-core machines (32+) with high QPS,
mutex contention on the reader path becomes a measurable bottleneck. The
`exit()` path is worse: it also runs `ReclaimLocked()`, which iterates the entire
`retired_` list under the lock.

**Goal: make `enter()`/`exit()` lock-free** — each becomes a single atomic store.
The writer path (`retire`/`reclaim`) can keep a mutex since there is only one
writer (the Flusher).

#### Background

Epoch-based reclamation (EBR) originates from Keir Fraser's PhD thesis (2004),
which formalized the idea of deferring reclamation until all threads that could
hold a reference have left the current epoch. The same concept underlies Linux
kernel **RCU (Read-Copy-Update)** — first implemented by McKenney and Slingwine
in DYNIX/ptx (1995) and later adopted in Linux (2001). RCU's key property: **readers
need no locks, no atomic instructions, and (on non-Alpha) no memory barriers**.
The Linux kernel uses RCU pervasively for read-mostly data structures (dcache,
VFS inode path lookup, etc.).

In the Rust ecosystem, **`crossbeam-epoch`** (part of the Crossbeam concurrency
library, designed by Aaron Turon) provides a mature, production-grade EBR
implementation. Its API (`pin()` → `Guard`, `Atomic::load`/`store`/`compare_and_swap`,
`Guard::defer_unchecked`) is the Rust analog of what crowtree needs. The
implementation uses a 3-epoch rotation (global epoch ∈ {0, 1, 2}), per-thread
local epoch counters, and a periodic garbage collection step that advances the
global epoch only when all active threads have observed the current one. The
`pin()` operation (equivalent to our `enter()`) is a single atomic store of the
thread's local epoch — exactly the design we adopt.

References:
- Fraser, K. *Practical Lock-Freedom* (PhD thesis, Cambridge, 2004).
- McKenney, P. *RCU Usage in the Linux Kernel* (kernel.org documentation).
- Turon, A. *Lock-freedom without garbage collection* (aturon.github.io, 2015).
- `crossbeam-epoch` crate: docs.rs/crossbeam-epoch

#### Design

```
Lock-free EBR data layout:

  global_epoch: atomic<uint64_t>           // monotonic, advanced by writer
  participants: dynamic array of per-thread slots, each:
    local_epoch: atomic<uint64_t>          // 0 = inactive, >0 = active at that epoch
    (cache-padded to avoid false sharing)

enter():
    e = global_epoch.load(memory_order_acquire)
    slot[tid].local_epoch.store(e, memory_order_release)   // ONE atomic store
    return Guard(e, slot)

exit():   // Guard destructor
    slot[tid].local_epoch.store(0, memory_order_release)   // ONE atomic store

retire(ptr, deleter):                       // writer only, mutex OK
    lock(mu_)
    retired_.push({global_epoch.load(), ptr, deleter})
    unlock(mu_)
    try_reclaim()

try_reclaim():                              // writer or any thread
    // Compute min epoch among all active participants
    min_active = UINT64_MAX
    for each slot:
        e = slot.local_epoch.load(acquire)
        if e != 0 and e < min_active: min_active = e
    if min_active == UINT64_MAX: min_active = global_epoch.load()
    // Free all retired entries with epoch < min_active
    lock(mu_)
    free retired_ entries where epoch < min_active
    unlock(mu_)
```

Key properties:

- **`enter()` = one atomic store** (acquire-load global + release-store local).
  No mutex, no CAS, no contention between readers on different cores.
- **`exit()` = one atomic store** (release-store 0). No `ReclaimLocked()` on the
  reader path — reclamation is deferred to `try_reclaim()` called by the writer
  (Flusher) or a periodic background tick.
- **3-epoch rotation** (as in crossbeam-epoch) is optional; a monotonic epoch
  counter is simpler and correct. The trade-off: monotonic epoch means `retired_`
  entries are filtered by `< min_active` rather than by epoch-slot recycling, but
  the cost is identical (linear scan of a small vector).
- **Per-thread slot management**: threads register on first use (allocate a free
  slot index from a pool) and unregister on thread exit (release the slot). A
  fixed-capacity array (e.g. 128 slots, cache-padded at 128 B each = 16 KiB total)
  covers practical thread counts. Slot allocation uses a simple atomic counter +
  bitmask; no dynamic allocation on the hot path.
- **Writer path keeps mutex**: `retire()` and the free-phase of `try_reclaim()`
  are writer-only (single Flusher thread), so a mutex is fine — no contention.

#### Why not sharding?

Sharding (multiple `EpochManager` instances, reader hashed by thread ID) reduces
contention but does not eliminate it — each shard still has a mutex, and
`retire`/`reclaim` must coordinate across all shards (take min epoch across all
shards). Lock-free EBR eliminates the mutex entirely on the reader path, which
is strictly better. Sharding adds complexity for no benefit over lock-free EBR.

#### When to implement

After **#5 B3** (zero-copy read) lands. Zero-copy read makes `enter()`/`exit()`
frequency higher (every `get()` needs a guard), so the lock-free optimization
payoff is maximized. Before that, the mutex-based implementation is sufficient
for correctness and development.

---

## 11. Read Path

```
get(key):
    guard = epoch.enter()                 // tree-owned epoch (§10); keeps frames resident
    if cell = memtable.get(key):          // L0 first: holds the newest cell if present
        return cell.tombstone ? None : (cell.slot, cell.value.clone())  // L0 must COPY (mutex)
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
- **Zero-copy value returns (`design-crowtree-memory.md §4`).** An **L1** hit
  returns a *borrowed* `buffer` pointing into the resident leaf frame (the frame
  stays resident under the epoch guard + buffer-pool pin), valid only for the
  guard's lifetime. An **L0** hit must return an *owned* copy because the MemTable
  mutex is released on return. Owning `get`/`scan` that outlive the guard clone the
  borrowed view and release the guard.
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
| Consolidation / GC workers | `Options`-injected pool (future); epoch-gated frees |

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
