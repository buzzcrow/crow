<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R50 Design: Epoch-Protected Lock-Free MemTable

## Problem

`MemTable::snapshot()` (`memtable.cpp:195`) deep-copies every live L0
entry — key + full `[header][value]` cell payload — into a
`std::vector<mem_entry>` on every scan call, regardless of the scan's
`limit` or key range. The copy is the safety mechanism: `scan()` runs
under an epoch guard only (no `write_mutex_`), while `flush()` →
`drain_up_to()` runs under `write_mutex_` and calls `map_.erase(it)`
(`memtable.cpp:182`). A live `absl::btree_map` cursor positioned on a
key the Flusher just drained is a use-after-free, so the snapshot
copies everything up front under `mu_`.

`get()` has the same root cause plus an extra copy:
`MemTable::get()` (`memtable.cpp:143`) takes `mu_` and copies the cell
into a `std::string`; `get_view()` (`crow-tree.cpp:1470`) then copies
the value *again* into `result.owned_`. Two copies per L0 hit, on both
`get_view()` and `try_get_view_no_load()`.

**Gate 2 measurement** (cleared): under sustained concurrent write+scan
with a production-like 3s flush tick, `l0_snapshot` is 126us/64B and
1122us/1KiB — 82–94% of scan time. Post-R48 (`l1_resolve` → 0us),
`l0_snapshot` is the dominant remaining scan cost when L0 is non-empty.

## Current Behavior

- **L0 structure**: `absl::btree_map<std::string, cell_entry>` under
  `std::mutex mu_` (`memtable.h:164-168`). One map per MemTable; a
  Crowtree has one `active_` + a `frozen_` deque (max 1 frozen by
  default).
- **Write path**: `upsert`/`upsert_external` take `mu_`, find the key,
  highest-slot-wins replace or insert. Overwrite frees the old cell
  in place (the `cell_entry` assignment drops the old `buffer`,
  firing the R30 `drop_fn` if split).
- **Read path (scan)**: `all_memtables()` (`crow-tree.cpp:826`) takes
  a shared_lock on `memtable_mutex_`, returns `shared_ptr` copies of
  `frozen_` + `active_`. Each table's `snapshot()` copies the whole
  map under `mu_` into a `vector<mem_entry>`. The scan merge loop
  (`crow-tree.cpp:1885-1941`) walks these vectors by index.
- **Read path (get)**: `all_memtables()`, then each table's `get()`
  copies the cell into a `std::string` under `mu_`. The caller
  (`get_view`) copies the value again into `owned_`.
- **Drain path**: `drain_up_to(cs)` (`memtable.cpp:171`) takes `mu_`,
  iterates the map, materializes+moves entries with slot <= cs into a
  vector, erases them from the map.
- **Relocate path**: `flush()` (`crow-tree.cpp:1003`) drains a frozen
  table's remaining entries (slot > cs) and re-upserts them into the
  live `active_` — a concurrent scan may be iterating `active_` at
  the same time.

## Root Cause

L0 is the only reader-visible structure outside the engine's EBR
scheme. L1 pages are epoch-protected: `retire_page()`
(`crow-tree.cpp:126`) defers freeing a replaced page until no in-flight
guard could still reference it, so lock-free readers walk L1 with zero
copy. L0 cells live in a `btree_map` under a mutex and are freed
immediately on erase *and overwrite*, so readers must snapshot-copy.

## Proposed Approach

Replace `absl::btree_map` under `mu_` with a concurrent skip list
whose nodes and cell buffers are epoch-retired. Readers iterate L0
lock-free under their existing epoch guard with zero copy; every
writer-side free (erase, overwrite, reset) is epoch-deferred through
the engine's existing `epoch_` (`crow-tree.h:1203`).

### 1. ConcurrentSkipList — node layout and allocation

- **Node** holds: `next[]` tower of `std::atomic<Node*>`, an atomic
  cell-version pointer, a logical-deleted flag, key length, and the
  key bytes **inline in the node's tail allocation** (RocksDB
  `InlineSkipList` style) — one allocation, no pointer chase, no
  `std::string` header.
- Height drawn at insert (p=0.25, max 12) so the tower is sized
  exactly.
- Keys are immutable for a node's lifetime; only the cell version
  pointer is mutable. This makes the read path a pure atomic load.
- Nodes come from a per-MemTable arena (simple bump allocator; freed
  in bulk when the MemTable is destroyed, or per-node via epoch
  retirement).

### 2. Cell version — the overwrite safety mechanism

A node carries a **versioned cell**: `std::atomic<CellVersion*>`
where `CellVersion` holds the `buffer` (contiguous or split) + slot +
flags. Overwrite publishes a new `CellVersion*` with a release store,
then `epoch_.retire(old_version)`. A reader that loaded the old
pointer under its guard keeps it alive — no use-after-free. This is
the key difference from the current in-place overwrite.

### 3. Write path — single spinlock, every free epoch-deferred

Writers serialized by a write spinlock replacing `mu_`. `apply()`
already serializes on `mu_` today and is not the scan bottleneck;
CAS-based concurrent insert is out of scope.

- **Insert** (`upsert`/`upsert_external`): splice the node in
  bottom-up with release stores. Highest-slot-wins,
  `durable_floor_`, and `allow_old_slots` carry over unchanged.
- **Overwrite**: publish new `CellVersion*` (release store), then
  `epoch_.retire(old_version)`. The deleter fires the R30 `drop_fn`
  for split cells (Rust refcount release).
- **Erase** (`drain_up_to`): set `deleted`, unlink the tower, then
  `epoch_.retire(node)` + `epoch_.retire(cell_version)`. The deleter
  frees the node arena block and fires the `kExternal` buffer's
  `drop_fn`.
- **`reset()`**: epoch-retire every node rather than clearing.

### 4. Read path — cursor and borrow, no mutex

- `MemTable::cursor(start_after)` returns a cursor seeded by an
  O(log N) `lower_bound`, exposing `key()`, `cell()`, `slot()`,
  `flags()`, `advance()`. Traversal is atomic acquire loads on
  `next[]`; logically-deleted nodes are skipped.
- The scan's `L0Cursor` (`crow-tree.cpp:1758`) changes from
  `{vector<mem_entry> entries; size_t idx}` to `{SkipListCursor cur}`.
  The merge loop is otherwise unchanged — min-key select,
  highest-slot-wins on collision, early stop past prefix.
  `materialize()` runs only for entries that reach the output: O(limit).
- The `upper_bound` skip pass (`crow-tree.cpp:1779-1787`) and its
  `scan_l0_skip_l` metric are deleted — the cursor seeks directly.
- **Get borrows, both copies gone.** `get_view()` and
  `try_get_view_no_load()` drop the `std::string best_cell` staging
  and compare slot/flags read directly off the node for
  highest-slot-wins, then borrow the winner's value into
  `GetView::value_` — the guard already held keeps it alive, exactly
  as an L1 frame hit does today. Split cells (R30) borrow the
  `kExternal` value directly; only the 9-byte header is synthesized
  for callers that need a full cell.

### 5. Epoch integration and bounded retirement

- Nodes and cell versions retire through the engine's existing
  `epoch_` (`crow-tree.h:1203`). No second EBR instance — one
  manager, one sweep, covering L0 nodes and L1 pages.
- **Bounded retirement.** A retire-queue high-water mark forces a
  `try_reclaim()` sweep, plus a metric for queue depth and
  oldest-guard age. Without this, a long-lived `GetView` guard
  (held across FFI) pins every retired L0 node and its borrowed Rust
  `Bytes` across a whole flush cycle — trading a CPU cost for an
  unbounded memory cost.

### 6. Bookkeeping counters become atomics

`bytes_`, `min_slot_`, `max_slot_`, `count()`, and `empty()` become
relaxed atomics maintained by the writer (read by
`maybe_freeze_active`'s thresholds and diagnostics).

### 7. `snapshot()` retained for full-set paths

`iter_all`, `compare`, and `snapshot_export` need every entry — O(N)
is correct there. `snapshot()` is reimplemented as a cursor walk. The
point of R50 is that it is no longer on the scan or get path.

## Alternatives Considered

- **Range- or chunk-bounded snapshot** (`snapshot_range(start_after,
  prefix, max_entries)` with cursor refill): reaches the same
  O(log N + limit) asymptotic in ~150 lines with no concurrency
  reasoning. Rejected as the end state because it keeps `mu_` on the
  read path, still copies, does nothing for `get()`, and leaves the
  `crow-tree.h:81` gap open. Fallback if Gate 2 had shown a modest
  but non-zero L0 cost.
- **Sealed immutable frozen tables** (write-closed `btree_map` +
  non-destructive drain, lifetime via `shared_ptr`): sound and
  EBR-free, but only covers `frozen_`. `active_` is where the
  reader/writer race is.
- **COW frozen + small active snapshot**: frozen becomes zero-copy
  but `active_` still copies, memory doubles briefly on freeze, and
  `get()` is untouched.
- **Hybrid (sealed btree for frozen + skip list for active)**:
  lower-risk to build incrementally but a permanently worse end state
  (two cursor types, two drain paths, a sealing state machine), and
  leaves the gap open for `active_`.

## Acceptance Criteria

- `scan_l0_snapshot_l` is independent of N_l0 for a bounded scan
  (benchmark at 1K / 10K / 100K live L0 entries, `limit = 100`).
- No mutex on the L0 read path (profiled or asserted).
- L0 `get_view` performs zero value copies on a hit.
- Overwrite-while-reading is safe: a targeted stress test with a
  reader holding a borrowed L0 value across a concurrent higher-slot
  upsert to the same key.
- Retire-queue depth stays bounded with a `GetView` deliberately held
  across several flush cycles.
- R30 correctness holds: split cells, `kExternal` buffers, and the
  Rust refcount `drop_fn` release exactly once, at reclamation rather
  than at erase.
- Existing `test-tree-ct` passes: ReadPath.*, AsyncScan.*, overflow,
  snapshot export/import, install_snapshot, iter_all / compare.
- `tree-tsan` and `tree-asan` green. The stress test under TSAN is
  the real gate for memory-ordering correctness.

## Files (expected)

- New `include/crow-tree/skip_list.h`, `src/skip_list.cpp` —
  concurrent skip list, inline keys, arena, epoch-deferred reclamation.
- New `tests/unit/skip_list_test.cpp` — concurrent insert / overwrite
  / drain / iterate stress, epoch reclamation safety.
- `memtable.h` / `memtable.cpp` — rewritten on the skip list:
  versioned cell, cursor API, borrow-returning get, atomic counters.
- `crow-tree.cpp` — `L0Cursor` becomes a skip-list cursor in `scan()`
  and `try_scan_no_load()`; `get_view()` / `try_get_view_no_load()`
  borrow; `upper_bound` skip pass removed.
- `crow-tree.h` — the `:81` gap comment is removed (gap closed).

## Complexity

High — ~800–1300 lines. The hard parts are the memory ordering on the
`next[]` tower under concurrent insert / unlink / traverse, the
versioned-cell overwrite protocol, and proving reclamation defers
past every in-flight guard. Reference: RocksDB's `InlineSkipList`.
