<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Design: crowtree In-Memory Engine

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-state-machine.md`](design-state-machine.md) (apply semantics, slot rules)

This document specifies crowtree's in-memory engine: the bounded 2-level
structure (concurrent MemTable over a COW B+tree), the slot-aware value cell,
the write path (apply → delta → consolidate → split/merge), the versioned root
for consistent snapshots, epoch-based reclamation, and the read path — then the
two supporting layers that make that write/read path zero-copy: the `buffer`
memory-ownership model, and the io_uring async FFI bridge that exposes it to
Rust without blocking.

On-disk format, backends, snapshot persistence, and recovery are in
[`design-crowtree-storage.md`](design-crowtree-storage.md).

## Table of Contents

- [1. The Tree Engine](#1-the-tree-engine)
  - [1.1 Logical Model and the Slot-Aware Cell](#11-logical-model-and-the-slot-aware-cell)
  - [1.2 Pages and Delta Records](#12-pages-and-delta-records)
  - [1.3 Write Path: MemTable Ingest + Flush](#13-write-path-memtable-ingest--flush)
  - [1.4 Consolidation, Split, and Merge](#14-consolidation-split-and-merge)
  - [1.5 Versioned Root (Consistent Snapshots)](#15-versioned-root-consistent-snapshots)
  - [1.6 Epoch-Based Reclamation](#16-epoch-based-reclamation)
  - [1.7 Read Path](#17-read-path)
  - [1.8 Concurrency Summary](#18-concurrency-summary)
- [2. Memory and Buffer Management](#2-memory-and-buffer-management)
  - [2.1 The `buffer` Abstraction](#21-the-buffer-abstraction)
  - [2.2 Write and Read Pipelines](#22-write-and-read-pipelines)
  - [2.3 Rust FFI Ownership](#23-rust-ffi-ownership)
- [3. Async FFI Bridge](#3-async-ffi-bridge)
  - [3.1 Problem and Design Principle](#31-problem-and-design-principle)
  - [3.2 Architecture](#32-architecture)
  - [3.3 Fast Path vs Slow Path](#33-fast-path-vs-slow-path)
  - [3.4 Zero-Copy Value Across FFI](#34-zero-copy-value-across-ffi)
  - [3.5 Decision Log](#35-decision-log)

---

## 1. The Tree Engine

### 1.1 Logical Model and the Slot-Aware Cell

crowtree is an ordered map `key → (slot, cell)`, where `key` is an opaque byte
string compared lexicographically, `slot` is the resolved consensus slot, and
`cell` is a live value or a tombstone. It is a **bounded 2-level** structure
(`design-crowtree.md` D3):

- **L0 — MemTable.** A concurrent in-memory ordered map (**`absl::btree_map`**,
  D9) that absorbs `apply` (concurrent, possibly out-of-order by slot; §1.3).
  Keeps one (highest-slot) cell per key. Key/value bytes are stored as
  move-only `buffer`s (§2), not `std::string`, so the write path is
  single-allocation and zero-copy down to the frame build.
- **L1 — B+tree.** A **single-writer / multi-reader** B+tree with
  copy-on-write base pages and a per-leaf delta chain. Inner pages hold
  separator keys + child PIDs; leaf pages hold sorted `(key, slot, cell)`
  entries and a `right_sibling` link for range scans. Reachable from a
  `root_pid` through the mapping table
  ([`design-crowtree-storage.md §8`](design-crowtree-storage.md#8-mapping-table)).

The single **Flusher** thread merges the MemTable's contiguous-applied prefix
into L1 ("flush = the persistent write"; §1.3). Reads overlay L0 on L1 (§1.7),
so read amplification is bounded at 2. The B+tree has exactly one writer.

Every value carries the slot and a kind flag:

```
cell_payload := [slot: u64 LE][flags: u8][value bytes...]
flags bit0 = tombstone (1 → value bytes empty)
```

- **Highest-slot-wins:** on write, if the incoming `slot <= existing slot` for
  a key, the write is skipped (idempotent).
- **Tombstone:** a delete is a cell with the tombstone flag; it occupies space
  until GC removes it below the watermark
  ([`design-crowtree-storage.md §9`](design-crowtree-storage.md#9-garbage-collection)).
  `get` on a tombstone returns `None`; `iter_all` returns the tombstone (for
  `compare`).

This inlining is the single difference from a generic ordered KV engine; it is
why a stock bw-tree cannot be used unmodified.

### 1.2 Pages and Delta Records

A page is the unit of indexing, consolidation, and (when flushed) I/O. Leaf
pages hold sorted key/cell entries plus an optional bloom filter and
`right_sibling` link; inner pages hold separator keys + child PIDs only (no
values), so they stay small and are written only at snapshot. The concrete
on-disk/in-memory frame layout (identical representation, zero-copy) is
specified in
[`design-crowtree-storage.md §3`](design-crowtree-storage.md#3-on-disk-page-format-zero-copy-frame);
this doc only needs the logical shape above.

A **flush** prepends an immutable delta to each affected leaf's chain instead
of rewriting the whole leaf. crowtree needs only one data delta type — the
**batch delta**: one slot's worth of mutations targeting a single leaf,
entries sorted by key, each carrying its own cell. A single flushed slot may
touch several leaves; the Flusher produces one batch delta per affected leaf
(all stamped with the same `slot` — the slot fan-out is the *common* case, not
an edge case). Put and Delete are not separate delta types — a delete is a
tombstone cell inside the batch. SMO deltas (split/merge/index-insert) appear
only transiently during a writer-exclusive split/merge (§1.4); there is no
abort delta because there is no competing writer.

### 1.3 Write Path: MemTable Ingest + Flush

`apply(slot, batch)` does **not** touch the B+tree. It folds the batch into
the concurrent MemTable:

```
apply(slot, batch):
    for (key, op, value) in batch:              // intra-batch: last occurrence wins
        if slot <= last_applied_slot: continue  // already durable in L1; drop
        memtable.try_emplace(std::move(key_buf), std::move(cell_buf))  // move-only
    maybe_signal_flush()                         // size/entry/time threshold crossed
```

- The consensus apply path stores **split cells** — the value is
  borrowed from the payload `Bytes` via a `kExternal` buffer (no value
  memcpy), and the 9-byte cell header is stored as `slot`/`flags` fields
  in `cell_entry`. The contiguous cell is materialized at flush / L0-read.
  See §2.4 for the full design. The pseudocode above shows the
  contiguous-cell path (used by snapshot import and the direct C API);
  both paths coexist in the same MemTable.
- The MemTable keeps **one cell per key** (highest slot wins), so repeated
  writes to a hot key collapse in memory before ever reaching the tree.
- `apply` drops cells already durable in L1 (`slot <= last_applied_slot`) and
  keeps the highest slot per key, so the MemTable always holds cells
  **strictly newer** than L1 for any shared key — this is what makes the
  L0-first read (§1.7) correct.
- A NoOp / empty batch still advances the contiguous frontier via
  `force_advance_slot` (the learner counts it), so the Flusher never blocks on
  the gap a NoOp leaves (`design-crowtree.md §3.1`).

The single Flusher thread, when a size/entry or time threshold trips, drains
the **contiguous-applied prefix** of the MemTable into L1:

```
flush():
    cs = contiguous.load()
    drained = memtable.take_while(entry.slot <= cs)     // ordered by key
    guard = epoch.enter()
    for (pid, group) in group_by_leaf(drained):          // find_leaf(key) per run
        delta = build_batch_delta(flushed_slot = cs, group)
        prepend delta onto pid's chain; mapping.Store(pid, delta)  // sole writer, plain store
        if chain exceeds max_delta_len / max_delta_bytes: consolidate(pid)  // may split/merge
    publish_root_version(version++, root_pid, last_applied_slot = cs)   // §1.5
```

- Every cell in a flush has `slot <= cs <= last_applied_slot`, so the
  published `RootVersion` is an **exact point-in-time** state (§1.5) — no
  slot beyond the frontier ever reaches the tree. `flush` *is* snapshot
  creation *is* snapshot: the public API is unified under snapshot
  terminology (`create_snapshot` = drain + publish + **persist to disk**),
  and `snapshot_view` returns the latest pinned root rather than
  materializing a copy. Every snapshot is durable — there is no
  "in-memory-only flush." Recovery uses the latest durable snapshot's slot as
  the replay starting point.
- The Flusher is the sole tree mutator, so mapping stores need no CAS; a
  multi-leaf flush is atomic to readers per leaf (each head swap is one
  atomic store), and the L0 overlay (§1.7) covers any cross-leaf read
  consistency during the flush.
- **Highest-slot-wins placement** is resolved consistently in three places:
  MemTable upsert (memory), consolidation (chain fold), and read (L0 overlay
  + chain scan). A higher-slot cell always shadows a lower-slot one.
- **Batching benefit:** hot keys collapse in the MemTable; each flush does
  one atomic store + at most one consolidation per affected leaf per flushed
  slot — not per key. This is LSM-style write batching without global
  compaction.

### 1.4 Consolidation, Split, and Merge

When a leaf's delta chain exceeds `max_delta_len` (default 8) or
`max_delta_bytes` (default 256 KiB), the chain is folded into a fresh leaf
base page: replay the chain (highest slot wins per key — the authoritative
point where the slot rule is enforced; tombstones are preserved, removed only
by GC), then either build a new base page, or split/merge if the result
crosses a size threshold.

Because there is exactly one writer, the multi-phase cooperative SMO protocol
of the pagetree reference is **dropped**: split/merge is a writer-exclusive
in-place restructure followed by atomic mapping-table stores.

- **Split** (leaf exceeds the target page size): partition entries at a
  cumulative-byte midpoint, build left/right leaves, splice `right` into the
  sibling chain, insert a new separator into the parent (may recurse/split
  the parent).
- **Merge** (leaf falls below `merge_threshold`, default target/4,
  non-root): absorb into the left sibling if the combined size still fits,
  rebuild, remove the parent's separator, free the old PID.

Inner pages split/merge by the same logic (separator keys + child PIDs, no
values). Root collapses when it has a single child; root grows a new level
when an inner split propagates past the current root. Thresholds use a
hysteresis gap (split at target, merge at target/4) to avoid split-merge
oscillation.

### 1.5 Versioned Root (Consistent Snapshots)

Steady-state reads use epoch pinning (§1.6) on the live chain. For
**long-lived consistent views** — `scan` with a large result, `compare`,
`snapshot_export`, and recovery anchoring — crowtree maintains an immutable
**versioned root**: `{version, root_pid, last_applied_slot, refcount}`.

- At `persist_snapshot` (and optionally on a cadence), the writer **freezes**
  the current tree: consolidates dirty leaves into immutable base pages,
  records a new `RootVersion`, and makes it current. This is the
  copy-on-write boundary.
- A reader takes `snapshot_view()` → pins the current `RootVersion`
  (refcount++). All `EngineView` methods read that fixed tree; writes after
  the pin allocate new pages and never mutate the pinned version's pages.
- A `RootVersion` (and pages reachable only from it) is reclaimable when
  `refcount == 0` **and** `last_applied_slot < gc watermark`
  ([`design-crowtree-storage.md §9`](design-crowtree-storage.md#9-garbage-collection)).

This gives true MVCC snapshots for readers/export without multi-version
per-key storage: only whole *tree versions* are retained briefly, not
multiple versions per key. Steady state keeps just one live version; older
versions exist only while a long reader holds them. Snapshot export always
pins the **current** version — exporting an arbitrary past slot is not
supported; install-snapshot always installs the latest durable state.

### 1.6 Epoch-Based Reclamation

Page lifetime (deletion / references / safe concurrent access) is governed by
an epoch manager **owned by each `Crowtree` instance** (not shared): because
the buffer pool and all retired pages are already tree-private, a per-tree
epoch is simpler and lets `get`/`scan` hand back **borrowed zero-copy views**
into resident frames that stay valid for the read guard's lifetime (§2.2).

- A reader does `Enter()` (a single atomic store) before touching any page and
  exits when its guard drops.
- The writer `Retire()`s pages it replaces (old delta chains, old base pages,
  freed leaf/inner pages from split/merge, unreferenced `RootVersion` pages).
- A retired page is freed only after every thread that could hold a pointer
  has left the epoch in which it was retired.

Because there is a single writer, `Retire` is uncontended; readers pay only
the enter/exit. This is the one mechanism that answers page deletion, page
references, and concurrent-read performance together.

**`enter()`/`exit()` are lock-free** (a single atomic store each; no mutex,
no CAS). The writer path (`retire`/`reclaim`) keeps a mutex since there is
only one writer (the Flusher) — no contention there. This follows the classic
epoch-based reclamation (EBR) design (Fraser, *Practical Lock-Freedom*, 2004;
the same idea underlies Linux kernel RCU and Rust's `crossbeam-epoch`): a
monotonic global epoch, per-thread local-epoch slots (cache-padded,
fixed-capacity pool, no dynamic allocation on the hot path), and reclamation
deferred to whichever thread calls `try_reclaim()` (the writer or a periodic
tick), which frees any retired entry whose epoch is below the minimum active
participant's epoch.

**Why not sharding instead?** Sharding (multiple `EpochManager` instances,
reader hashed by thread) reduces contention but does not eliminate it — each
shard still needs a mutex, and retire/reclaim must coordinate across shards
(take the min epoch across all of them). Lock-free EBR eliminates the mutex
entirely on the reader path, which is strictly better, for less complexity.

**Why epoch-based reclamation and not pin counts or hazard pointers?** The
hard part of eviction/reclamation is not picking a victim; it is answering
*"when is it safe to actually reuse a page's frame bytes?"* — readers are
lock-free and read `Slice`s that point **directly into a frame**, so freeing
or reusing memory a reader still holds a pointer into is a use-after-free.
Three disciplines solve this:

1. **Pin counts / refcounting** (textbook buffer pool): `pin++` before
   touching a page, `pin--` after; evict only at `pin == 0`. Correct, but it
   puts an atomic RMW on a shared cacheline on **every page touch on the read
   hot path** — a real regression versus lock-free reads.
2. **Hazard pointers:** a reader publishes each pointer it dereferences into
   a per-thread slot before touching it; the reclaimer scans all threads'
   slots before freeing. Lock-free and tightly memory-bounded, but the reader
   pays a publish (store + fence + re-validation) per pointer, and a
   root→…→leaf descent needs one hazard per level.
3. **Epoch-based reclamation (EBR)** — crowtree's choice for the sync read
   hot path (`get`/`scan`/`get_view`). A reader brackets its **whole**
   operation in one `Guard`; it never publishes which pointers it holds.
   Reader cost is one enter/exit *per operation*, not per pointer. Trade-off
   vs. hazard pointers: a single stalled guard blocks *all* reclamation
   (worst-case unbounded retained memory) — accepted because reads are short
   (one `get`/`scan`) and there is a single writer, so the active-epoch
   window is tiny.
4. **Per-page refcount on handoff paths** — composes with EBR as an
   orthogonal cross-thread lifetime mechanism. EBR protects the same-thread
   walk (the `Guard` is thread-bound); refcount extends page lifetime across
   threads after the walk hands off a borrowed `Slice` (`get_async` slow
   path) or a pinned snapshot (`snapshot_view` → `PinnedSnapshot`). The
   per-page `pin_state_` atomic on `PageBase` is only touched on the
   handoff paths (one `fetch_add` on pin, one `fetch_sub` on unpin), NOT on
   the sync `get`/`scan` hot path — the §1.6 rejection of refcount ("atomic
   RMW on every page touch on the read hot path") stands for the hot path.
   The epoch deleter sets a `kRetiredBit` via `fetch_or`; if pins are
   outstanding (count > 0), the deleter defers and the last `unpin` frees.
   This decouples page lifetime from the entering thread's participant slot,
   eliminating the copy in `materialize_owned` and the stale-root GC delay
   when a snapshot is handed to another thread.

### 1.7 Read Path

```
get(key):
    guard = epoch.enter()                 // keeps frames resident
    if cell = memtable.get(key):          // L0 first: holds the newest cell if present
        return cell.tombstone ? None : (cell.slot, cell.value.clone())  // L0 must COPY (mutex)
    pid  = find_leaf(key)                  // L1: descend inner pages from root_pid
    for node in chain(pid):                // head → base
        if node has key: return cell_of(node) (tombstone → None)
    return None
```

- **L0 overlay.** Because `apply` drops cells `<= last_applied_slot` and keeps
  the highest slot per key (§1.3), any key present in the MemTable has a slot
  strictly newer than L1, so checking L0 first is correct (read amplification
  2).
- `scan(prefix, limit)` uses a **merge cursor** over L0 and the L1 leaf chain:
  at each step take the smaller key, and on a key tie take the L0 cell
  (newer). Within L1, materialize the current leaf's live entries (resolving
  the delta chain by highest slot), then follow `right_sibling`. For bounded
  `limit` the live overlay is fine; for large scans the cursor runs on a
  pinned `RootVersion` merged with an L0 snapshot.
- **Zero-copy value returns.** An **L1** hit returns a *borrowed* `buffer`
  pointing into the resident leaf frame (valid only for the guard's
  lifetime, §2.2). An **L0** hit must return an *owned* copy because the
  MemTable mutex is released on return.
- `iter_all` (for `compare`) always runs on a pinned `RootVersion` (merged
  with an L0 snapshot) and includes tombstones.

Reads never block apply and apply never blocks reads: readers see immutable
pages; the writer only ever publishes new pages via atomic stores and retires
old ones via the epoch manager.

### 1.8 Concurrency Summary

| Actor | Mechanism |
| --- | --- |
| MemTable ingest (`apply`, ≥ 1) | Concurrent ordered-map upsert; no tree access |
| Flusher (1 per tree) | Sole tree writer: drain prefix → batch delta → plain store; epoch retire |
| Point/range readers (N) | Epoch enter/exit; L0 overlay + lock-free atomic loads of immutable pages |
| Long readers / export | Pin a `RootVersion` (refcount) + L0 snapshot for a stable MVCC view |

Invariants:

- **I1** A page is freed only after no reader epoch can reference it.
- **I2** A pinned `RootVersion`'s pages are immutable until its refcount hits 0.
- **I3** Per-key resolved slot is monotone (highest-slot-wins at upsert, read
  & consolidate).
- **I4** A reader observes a linearizable point-in-time state (L0 overlay on
  the live tree, or a pinned version), never a partial multi-leaf flush.
- **I5** For any key in both, the MemTable cell's slot is strictly newer than
  L1's (apply drops `slot <= last_applied_slot`), so L0-first reads are
  correct.
- **I6** The B+tree only ever holds slots `<= last_applied_slot` (flush
  drains the contiguous prefix only), so every `RootVersion` is an exact
  point-in-time state.

---

## 2. Memory and Buffer Management

The write path used to copy key/value bytes **three times**: `encode_cell()`
into a `std::string`, `MemTable::upsert` storing the string, `flush()`
draining into a vector, and the frame builder writing into the frame. **Goal:
allocate key/value memory once, at the earliest point (the Rust/C API
boundary), then move it down to the MemTable and into the B+tree without
copying.** The only unavoidable copy is the final placement into the slotted
frame layout — that copy *is* page construction, not a redundant one. On the
read side, `get`/`scan` hand back a **borrowed** view into the resident frame
instead of a freshly-allocated `std::string`; the caller copies only if it
needs to outlive the read guard.

### 2.1 The `buffer` Abstraction

`buffer` (`crowtree/include/crowtree/buffer.h`) is a move-only byte container
that is *either* owned (frees on destruction) *or* borrowed (a non-owning view
whose lifetime is guaranteed elsewhere — a resident frame under an epoch
guard). Design rules:

- **Move-only.** A `buffer` is never implicitly copied; passing it down the
  write path is a `std::move`. Deep copies are explicit (`clone()`).
- **Two modes, one type.** The same type serves an owned write-path value and
  a borrowed read-path view. A borrowed `buffer` never frees.
- **Header reserve.** `alloc(cap, header_reserve)` over-allocates by
  `header_reserve` so the cell header (`[slot][flags]`, §1.1) is written in
  the reserved prefix and the value bytes follow contiguously — the encoded
  cell is one contiguous buffer with no second allocation or copy.
- **Small-buffer optimization (SBO) — required, not optional.** An owned
  `buffer` whose total length fits `kInlineCap` (24 B) stores its bytes
  **inline**, with no heap allocation — mirroring `std::string`'s SSO. This is
  a *correctness-for-performance* rule: without it, replacing `std::string`
  (which inlines ~15 B) with `buffer` on the write path would *regress* the
  common small-key/small-value case by forcing a `malloc` where there was
  none. A 9-byte cell header + up to 15 B of value stays inline.
- **Allocator seam.** `alloc()` routes owned allocations larger than
  `kInlineCap` through a single internal allocator hook (today: glibc
  `malloc`); a size-classed pool or RDMA-pinned allocator could slot in here
  later with no call-site changes — see [`todo_code.md`](../todo_code.md) for
  why that hasn't been done speculatively.
- **MemTable = `absl::btree_map<std::string, cell_entry>`.** The KEY stays
  `std::string`, the VALUE is a `cell_entry{slot, flags, cell}`. The
  `cell` buffer is either **contiguous** (`kOwned`, full `[header][value]`
  — used by snapshot import and the direct C API) or **split** (`kExternal`,
  value-only borrowed from a Rust `Bytes` — the zero-copy consensus apply
  path). The contiguous form is materialized at the API boundary
  (`get`/`drain`/`snapshot`). **Why the key is not a `buffer`:** a B-tree stores
  `pair<const Key, Value>` and *relocates* slots on node split/merge, which
  requires moving the `const` key — a move-only `buffer` key falls back to
  its deleted copy ctor and fails to compile. `std::string`'s SSO already
  inlines small keys, so it is the correct key type; `buffer`'s SBO gives
  the same inline benefit on the value side plus the borrowed read path.
  `std::less<>` is transparent → heterogeneous `Slice`/`string_view`
  lookup with no temporary key.

### 2.2 Write and Read Pipelines

```
Write (consensus apply path — zero-copy):
  Rust: Batch::decode produces Bytes slices (O(1) Arc ref-bump)
  → CrowtreeEngine::apply: per-op Bytes::clone → BytesRef handle (Arc bump)
  → ct_apply_batch_external: C++ wraps value as kExternal buffer (NO memcpy)
  → apply_external → MemTable::upsert_external: cell_entry{slot, flags, value:kExternal}
  → [payload Bytes stays alive, pinned by external buffers' Arc clones]

Write (contiguous-cell path — snapshot import / direct C API):
  → allocate one buffer (value + reserved cell header)          ← the ONE allocation
  → C FFI: ct_apply_put(tree, slot, key_ptr,klen, val_ptr,vlen)
  → C++ wraps as buffer (copy at the boundary; §2.3)
  → encode cell: write slot+flags into the reserved header       (no alloc, no copy)
  → MemTable::upsert(key_buf, cell_buf): std::move into the map   (no copy)

Flush (off the apply critical path):
  → drain_up_to: materialize split cells → contiguous [header][value] (value memcpy HERE)
  → frame builder copies bytes into the slotted frame layout     ← unavoidable (page construction)

Read: get(key):
  guard = epoch.enter()                       // keeps resident frames alive (§1.6)
  L0 (MemTable) hit: must COPY (materializes split cell into [header][value]; one copy)
  L1 (B+tree) hit:   return buffer::wrap(cell_value_ptr_in_frame, len)  // BORROWED, zero copy
                      valid only while `guard` is held AND the frame stays resident
```

- **Apply critical path: zero value memcpy.** The value bytes are borrowed
  from the Paxos payload `Bytes` via `kExternal` buffers; the `memcpy` is
  deferred to flush (off the critical path). The total value-copy count is
  2 (materialization at flush + frame construction), both on the Flusher
  thread — none on the apply thread.
- **L1 reads are zero-copy:** the returned `buffer` borrows the cell's value
  bytes directly from the resident frame. **The epoch guard alone keeps the
  frame resident** — eviction does not free a page's frame directly, it
  re-tags the mapping slot unloaded and epoch-retires the page, whose
  destructor (which releases the frame) only runs after the epoch reclaims
  it. A reader holding a guard therefore keeps the page alive, so the frame
  stays pinned and the buffer pool's CLOCK sweep skips it — no separate pin
  needs to be bundled into the returned handle.
- **Caller contract:** a borrowed `buffer` is valid only while the caller
  holds the read guard. To retain a value past the guard, the caller calls
  `clone()` (owned copy) and releases the guard.

### 2.3 Rust FFI Ownership

Three options for how Rust hands value memory to C++:

- **Option A — copy at the boundary.** Rust passes `*const u8` + len; C++
  `buffer::alloc()`s and copies once at the FFI boundary. Simpler; one
  copy remains at the boundary but the *internal* pipeline is zero-copy
  (§2.2). Used by the simple single-op C API (`ct_apply_put`,
  `ct_apply_batch`, `ct_apply_batch_slices`).
- **Option B — handle-based ownership transfer.** The caller
  allocates crowtree-owned memory via `ct_alloc(key_len, val_len)`, which
  returns an opaque `ct_write_handle` plus two writable pointers (key +
  value). The caller writes key/value bytes directly into that memory,
  then `ct_apply_put_owned(tree, slot, handle)` writes the cell header
  (slot + flags) into the pre-allocated cell and moves key+cell into
  `apply_encoded` — zero value memcpy. `ct_free_handle(handle)` is the
  error/cancel path. The cell header layout (`kCellHeaderSize`) is
  internal — the C API never exposes it, sidestepping the ABI-stability
  concern that originally blocked this option. The Rust adapter wraps the
  handle in a `WriteHandle` struct with RAII `Drop` (frees if not
  consumed) and `apply(self, slot)` (consumes). `!Send + !Sync`. SBO is
  preserved: small values (≤ 15 B) use inline storage, same as Option A.
  Intended for direct API callers that want zero-copy single-op apply.
- **Option C — external borrow from Rust `Bytes`.** The consensus
  apply path (`CrowtreeEngine::apply`) uses `ct_apply_batch_external`:
  each Put op's value `Bytes` is cloned (O(1) `Arc` bump) into a
  `BytesRef` handle handed to C++. C++ wraps the value bytes as a
  `kExternal` `buffer` (borrowed, no memcpy) and stores a **split cell**
  in the MemTable — `cell_entry{slot, flags, value:kExternal}` — where
  the 9-byte cell header is stored as fields, not as adjacent bytes. The
  contiguous `[header][value]` cell is materialized at the MemTable API
  boundary (`get`/`drain_up_to`/`snapshot`), where a copy already exists,
  all off the apply critical path. When the `kExternal` buffer is freed
  (drain/overwrite), C++ calls back into Rust (`ct_release_bytes`) to
  drop the `Arc<Bytes>` clone, keeping the payload allocation alive until
  every borrowing buffer is freed. The `kExternal` mode adds zero size
  overhead to `buffer` (the drop-control fields overlap the SBO `inbuf_`
  array via a union). See §2.4.

Ownership rule (extends `design-crowtree.md §4`): a buffer *yielded* to
`ct_apply_*` is owned by the tree and freed by it once flushed into a frame;
a buffer *borrowed* into a call (not yielded) follows the existing "borrowed
for the call's duration" rule. An **external** buffer (Option C) is
borrowed from Rust and kept alive by an `Arc<Bytes>` refcount — C++ calls
back into Rust to drop the ref when the buffer is freed (drain/overwrite),
so the payload allocation survives until every borrowing buffer is released.

### 2.4 Zero-Copy Apply: Split-Cell External Buffers

**Design challenge.** The consensus apply path receives value bytes as
`Bytes` slices into the packed Paxos payload — a single refcounted
allocation shared by all ops in the batch. The contiguous-cell apply path
(Option A/B) calls `encode_cell_buf`, which performs a value `memcpy`
(64 KiB for a large value) on the apply thread — the dominant per-op cost
for large values. The goal is to eliminate this copy for the consensus
apply path without changing the contiguous cell representation downstream.

**Why a naive external buffer doesn't work.** The cell is stored as one
contiguous `buffer` `[9-byte header][value]` everywhere (`mem_entry.cell`,
`leaf_entry.cell`, B+tree frames, snapshot I/O). `CellView` reads
`slot()`/`flags()`/`value()` by slicing this contiguous memory. A
`Bytes`-backed external buffer cannot satisfy this contiguity: the value
bytes live in the packed payload `Bytes`, and the 9-byte header (known only
at apply time) is not adjacent to them.

**Design.** Split the cell **only while it lives in the MemTable**;
materialize the contiguous form at the memtable API boundary
(`get`/`drain_up_to`/`snapshot`) — points where a copy already exists, all
off the apply critical path. Everything downstream (flush, delta, frame,
`leaf_entry`, `CellView`, snapshot I/O) sees contiguous cells unchanged.

- **`buffer::mode::kExternal`** — a buffer mode that borrows value bytes
  from a Rust `Bytes` and calls `drop_fn(owner)` on destruction to
  decrement the Rust refcount. The drop-control fields (`drop_fn`, `owner`)
  overlap the SBO `inbuf_` array via a union — zero size overhead.
  `clone()` deep-copies the borrowed bytes into an owned buffer; move
  transfers `ext_` and releases the source without calling `drop_fn`.
- **`cell_entry`** — the MemTable's internal map value: `{slot, flags, cell}`.
  Contiguous: `cell` is `kOwned` (full `[header][value]`). Split: `cell` is
  `kExternal` (value-only, borrowed). Tag: `cell.ownership() == kExternal`.
  `materialize()` builds a contiguous `[header][value]` buffer from the
  fields + borrowed value (one value `memcpy`, off the apply hot path).
- **`apply_external(slot, vector<external_op>)`** — same semantics as
  `apply_encoded` (oversized-key rejection, slot bookkeeping,
  `maybe_swap_active`, intra-batch last-key-wins) but builds `cell_entry`
  directly — no `encode_cell_buf`, no value `memcpy`.
- **`ct_apply_batch_external`** — C API: `ct_ext_op{key, value, kind,
  bytes_ref, drop_fn}`. Rust hands a `BytesRef` (boxed `Arc<Bytes>`) per Put
  op; C++ wraps the value as `kExternal` and calls `drop_fn(bytes_ref)` when
  the buffer is freed.
- **`CrowtreeEngine::apply`** — unchanged `KVEngine::apply` trait; builds
  `ExtOp`s from the `Batch`'s `Bytes` slices (O(1) `Arc::clone` per op) and
  calls `apply_batch_external`. WAL replay goes through the same
  `learner.learn` → `apply_entry` path, so it benefits automatically.

---

## 3. Async FFI Bridge

### 3.1 Problem and Design Principle

Wrapping every synchronous C++ engine call in `tokio::task::spawn_blocking`
has two problems: (1) every `get`/`apply`/`scan` hops through Tokio's blocking
thread pool, and the ~5–10 μs scheduling overhead is significant relative to
a μs-level in-memory operation; (2) a high-performance engine must not block
async workers or rely on large thread pools for I/O — `spawn_blocking` is a
workaround for synchronous I/O, not a long-term architecture.

**Design principle: completion-based async I/O via io_uring. No blocking, no
thread pools, no C++ coroutine dependency.**

- **C++ coroutine (`co_await`) is not used.** It would add compiler/runtime
  surface and lifetime complexity at the C ABI without improving the kernel
  I/O path — the boundary still needs an opaque handle Rust can poll, so
  exposing coroutine state machines across FFI is the wrong abstraction.
- **Folly futures are not used.** They bring a large dependency stack and an
  executor model that duplicates Tokio on the Rust side; crowtree only needs
  a small completion object plus a notification fd.
- **epoll is only the readiness fallback.** io_uring handles disk/block I/O;
  `eventfd` handles C++→Rust wakeup, and Tokio already integrates that fd via
  `AsyncFd` (epoll internally) — no separate epoll reactor for storage I/O.

### 3.2 Architecture

```
  Tokio async runtime                         C++ engine
  ┌──────────────────┐                ┌──────────────────────────┐
  │  async fn get()  │                │  ct_get_async()          │
  │  CtGetFuture     │──poll()───────►│  fast path? ──yes──► done│
  │  .poll()         │                │     │ no                 │
  │     │ pending    │◄──done=0───────│  submit io_uring SQE     │
  │     │ register   │                │  return pending          │
  │     │ waker on   │                │                          │
  │     │ AsyncFd    │                │  Reactor thread (1)      │
  │     │ (eventfd)  │◄──eventfd──────│  io_uring_enter loop     │
  │     │ wake       │                │  CQE → callback          │
  │  .poll() again   │──poll()───────►│  ct_future_poll → done=1 │
  │  → Ready(result) │                │  return ct_buf           │
  └──────────────────┘                └──────────────────────────┘
```

One reactor per `Crowtree` instance, on a dedicated C++ thread: it calls
`io_uring_enter` (blocking until a CQE arrives or a timeout fires), peeks
CQEs, dispatches each completion callback, then writes to its `eventfd` to
wake the Rust side. The Rust side wraps that `eventfd` in Tokio's `AsyncFd`;
a `ct_future` handle carries the pending state, result, and — for the fast
path — an `EpochManager::Guard` keeping the frame resident (§3.4).

### 3.3 Fast Path vs Slow Path

| Operation | Fast path (sync) | Slow path (io_uring) |
|-----------|-----------------|---------------------|
| `get` | L0 MemTable hit, L1 resident page hit | L1 cache miss → demand-load from disk |
| `scan` | All pages resident | Any page needs demand-load |
| `apply_put` / `apply_delete` | MemTable insert (no flush triggered) | Triggers flush → write to disk |
| `flush` / `snapshot` | — | Always: write dirty pages to disk |
| `snapshot_view` | All pages resident | Any page needs demand-load |

The C++ engine determines the path internally: if the operation can complete
without I/O, it fills the future synchronously (`state = kDone`) and returns;
otherwise it submits SQE(s) to the reactor (`state = kPending`) and the
reactor completes the future when I/O finishes. A `CtFuture`'s first
`poll()` on the fast path resolves immediately — zero scheduling overhead,
no thread switch; on the slow path it registers a waker on `AsyncFd` and
resolves on the next reactor-driven wakeup.

### 3.4 Zero-Copy Value Across FFI

- **Fast path (PinnedValue):** the `ct_future` holds an
  `EpochManager::Guard` that keeps the frame resident;
  `ct_future_poll` returns a `ct_buf` pointing directly into the frame
  bytes (borrowed, not owned). `AsyncCrowtree::try_get_pinned` wraps
  this in a `PinnedValue` — a `!Send` RAII type that holds the
  `ct_future` handle so the epoch guard stays alive until `PinnedValue`
  is dropped. `PinnedValue::as_bytes()` borrows directly from the C++
  frame with no `copy_buf` allocation. The `KVEngine::get_bytes` trait
  method (default impl delegates to `get` + `Bytes::from(vec)`;
  `CrowtreeEngine` overrides to use `try_get_pinned`) copies from
  `PinnedValue` into `Bytes` before dropping the pin, eliminating the
  intermediate `Vec<u8>` allocation. Total: one copy (frame → `Bytes`),
  one allocation (`Bytes`), no `Vec<u8>`.
- **Slow path:** the I/O read fills a C++-owned buffer; on completion
  the future returns it as an owned `ct_buf` (C++ allocates, Rust
  frees). For frame-borrowed values, the zero-copy path replaces the copy with a per-page
  refcount pin: the reactor thread pins the leaf base page(s) under its
  epoch guard, releases the guard, and hands the `GetView` (still borrowing
  the frame) across threads; `ct_future_free` unpins from the dropping
  thread. `materialize_owned` remains only for overflow-chain values
  (assembled from multiple pages, no single frame to borrow). The Rust
  side then copies from the borrowed/owned buffer into `Vec<u8>` / `Bytes`.
- **Key copy elimination:** `AsyncCrowtree::try_get` /
  `try_get_pinned` accept `&[u8]` instead of `Vec<u8>`, since
  `ct_get_async` copies the key internally into a
  `std::shared_ptr<std::string>`. The Rust-side `key.to_vec()` is
  eliminated — no C++ changes required.
- **True zero-copy:** `PinnedValue::into_bytes()` creates a `Bytes`
  via `Bytes::from_owner` backed by the C++ frame — no copy. The
  `ct_future` handle (and its page refcount pins) is held by the `Bytes`
  owner; when the last `Bytes` ref clone is dropped on any thread, the
  owner's `Drop` runs, which drops the `PinnedValue`, which calls
  `ct_future_free` to unpin. `PinnedValue` is `Send` (the per-page
  refcount is thread-independent), enabling this cross-thread zero-copy
  handoff. `CrowtreeEngine::get_bytes` uses this path directly.

### 3.5 Decision Log

| ID | Decision | Rationale |
|----|----------|-----------|
| D-Async1 | **Async FFI via io_uring + completion-based futures.** No `spawn_blocking`, no large thread pools, no C++ coroutine/Folly dependency. | A `ct_future` handle is the correct FFI abstraction for Rust polling; C++ coroutine and Folly executor state must not cross the C ABI. Fast path completes synchronously with zero scheduling overhead. |
| D-Async2 | **One reactor thread per `Crowtree`.** | Each tree has its own buffer pool and I/O; a per-tree reactor keeps I/O completions local and avoids cross-tree coordination. The reactor does no application logic — only CQE dispatch. |
| D-Async3 | **`eventfd` for Rust↔C++ notification.** | Simplest kernel-level notification: one `write(8 bytes)` wakes Tokio's `AsyncFd`. Level-triggered, no missed events. Falls back to a pipe + `kqueue` on macOS. |
| D-Async4 | **macOS dev path: in-memory store, no io_uring.** | io_uring is Linux-only. For dev/testing on macOS, the in-memory store completes synchronously (fast path only). Production runs on Linux with real io_uring. |

---

## 4. Rust-Side `KVEngine` Async Trait Shape

> **Merged from `design-crowkv-async-kvengine.md` (2026-07).**
> **Status:** implemented (landed 2026-07-09).
> This section records the design of the Rust-side `KVEngine` trait's async
> shape (`KVFuture<T>`) and why it looks the way it does — kept as the
> rationale record for a decision that is easy to get wrong (the naive
> `async-trait` conversion), not as a live plan.

### 4.1 The Problem

`KVEngine` is consumed as `Box<dyn KVEngine>` (`PxLearner::engine`), chosen at
runtime via a CLI flag (`--kv-engine {memory,crowtree}`). Its `get`/`scan`/
`apply` need an async-capable return type so that a genuine I/O path
(crowtree demand-load miss, served by the io_uring reactor, §3 "Async FFI
Bridge") can suspend instead of blocking a Tokio worker thread on a
synchronous `pread` — the exact anti-pattern the reactor exists to avoid at
the C++/FFI layer, which would otherwise resurface immediately one layer up
in Rust.

### 4.2 The Central Tension: `dyn KVEngine` vs. `async fn` in Traits

Native `async fn` in traits is **not `dyn`-compatible**. Three ways to square
that with runtime engine selection:

| Option | Cost |
| --- | --- |
| **(a) `async-trait` crate** | Boxes every async call into a `Pin<Box<dyn Future>>` via macro — one heap allocation **per call, including the fast in-memory path with no I/O**. Undoes the reactor's "fast path costs nothing" property one layer up. |
| **(b) Generic `PxLearner<E: KVEngine>`** | Zero overhead, but `E` would need to propagate through `PxLocalReplica`, `PxGroup`, `PxKvStore`, and `DashMap<GroupId, PxGroup>` — too invasive for an engine chosen at runtime. |
| **(c) Hybrid fast-path/slow-path future (chosen)** | Plain (non-`async`) `fn`s return a small custom future enum that resolves immediately (no allocation) for the fast path and only boxes a real future for the rare slow (I/O) path. Fully `dyn`-compatible; mirrors the exact fast/slow split the C++ layer already makes. |

### 4.3 `KVFuture<T>` and the Trait Shape

```rust
pub enum KVFuture<T> {
    Ready(Option<T>),                              // taken on first poll; re-polling panics
    Pending(Pin<Box<dyn Future<Output = T> + Send>>),
}

impl<T> KVFuture<T> {
    pub fn ready(v: T) -> Self { KVFuture::Ready(Some(v)) }
}

pub trait KVEngine: Send + Sync {
    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<()>;
    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>>;
    fn scan(&self, prefix: &[u8], limit: usize) -> KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)>;
    // iter_all / live_key_count / clear / compare / resume_from_slot /
    // persist_snapshot / set_gc_watermark / collect_garbage: plain sync fns.
    // Diagnostic/maintenance-path only (compare/iter_all: tests + snapshot
    // export; the rest: restore path + the periodic group-maintenance
    // task), never on the hot Paxos-accept / gRPC-read path, so a brief
    // blocking call there is an acceptable trade-off.
}
```

`Ready` costs nothing beyond the enum tag + inline value — no allocation, no
`Pin<Box<..>>`. `InMemKV` always returns `Ready`. `CrowtreeEngine::get`
(`crowkv/src/kv/crowtree_engine.rs`) does the same fast-path check the C++
layer does first, via `crowtree_ffi::AsyncCrowtree::try_get`; on a resident
hit/miss it returns `Ready` at zero extra cost, and only on a genuine
demand-load miss does it construct `Pending`, wrapping the reactor-driven
future `try_get` already builds. `CrowtreeEngine::scan`/`apply` always
resolve `Ready` today (no async `scan`/`apply` C API exists yet — an honest
gap, not an oversight; see `CrowtreeEngine`'s doc comment).

### 4.4 Caller-Side Wiring

`Learner::learn` is a native `async fn` (mirroring `Acceptor`'s existing
`async fn accept`/`prepare` convention — neither trait is ever used as
`dyn`, so native `async fn` is safe for both). `PxLearner::apply_entry`/
`engine_get`/`engine_scan` are `async fn` and `.await` the `KVFuture`
directly. `PxLocalReplica::learn_chosen`/`apply_committed_up_to` and
`PxKvStore::kv_get`/`kv_scan` were already `async fn` (already awaited by
every caller) and now genuinely `.await` a future that can suspend, instead
of always resolving on the first poll.

`engine_scan`/`apply_entry` stay `async fn` for signature uniformity with
`engine_get`, but never actually suspend today, matching §4.3's note that
`scan`/`apply` have no async C API yet.

### 4.5 Testing

- `kv/kv_future_test.rs`: `KVFuture::ready(v)` polls to `Ready(v)` on the
  first poll without registering a waker; a `Pending` variant polls through
  to the wrapped future's result; polling a completed `Ready` again panics.
- `InMemKV` regression guard (`mem_kv_test.rs`): every call returns `Ready`
  (asserted via `matches!`, not just the unwrapped value, so an accidental
  switch to `Pending` fails loudly here first).
- `CrowtreeEngine` regression tests (`crowtree_engine_test.rs`): a resident
  hit/miss returns `Ready`; `get_constructs_pending_for_genuine_demand_load_miss`
  evicts a durable engine's resident leaf and asserts `get` returns `Pending`
  and resolves to the right value.
- `paxos/learner_async_test.rs`: the same fast/slow property one layer up,
  through `PxLearner::engine_get`.
