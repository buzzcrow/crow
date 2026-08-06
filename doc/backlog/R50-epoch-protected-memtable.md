<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R50: Epoch-protected lock-free MemTable — eliminate scan/get L0 copy

**Problem**: `MemTable::snapshot()` (`memtable.cpp:195`) deep-copies
every live L0 entry — key + full `[header][value]` cell payload — into a
`std::vector<mem_entry>` on every scan call, regardless of the scan's
`limit` or key range. The copy is the safety mechanism, not laziness:
`scan()` runs under an epoch guard only (no `write_mutex_`, plan-tree #5
B3), while `flush()` → `drain_up_to()` runs under `write_mutex_` and
calls `map_.erase(it)` (`memtable.cpp:182`). A live `absl::btree_map`
cursor positioned on a key the Flusher just drained is a use-after-free,
so the snapshot copies everything up front under `mu_` and lets the
Flusher erase freely afterward.

The cost is O(N_l0 × (key + 9B header + value bytes)) per scan, paid
even for entries the scan never emits (everything before `start_after`
is copied then discarded by the `upper_bound` skip at
`crow-tree.cpp:1820-1826`; everything past the prefix range or the
`limit` is copied then never read). `get()` (`memtable.cpp:143`) has the
same root issue in miniature: it takes `mu_` and copies the cell into a
`std::string` on every L0 hit, even though the caller already holds an
epoch guard that could keep the cell alive without a copy.

The header at `crow-tree.h:81` already flags this as a known gap:
"MemTable cell isn't epoch-protected the same way — a later refinement."

**Root cause**: L0 (MemTable) is the only reader-visible structure not
integrated into the engine's epoch-based reclamation (EBR) scheme. L1
pages are epoch-protected — `retire_page()` (`crow-tree.cpp:166`) defers
freeing a replaced page until no in-flight epoch guard could still
reference it, so lock-free readers walk L1 safely with zero copy. L0
cells live in a `btree_map` under a mutex and are freed immediately on
erase, so readers must take a snapshot copy to be safe.

**Target**: Bring L0 into the same epoch-protection scheme as L1.
Replace the `absl::btree_map<std::string, cell_entry>` under `mu_` with
a concurrent skip list whose nodes are epoch-protected, so readers
(get/scan) iterate L0 lock-free under their existing epoch guard with
zero copy, and the Flusher erases entries via epoch-deferred reclamation
(same mechanism as `retire_page()`).

**Why a skip list** (not another data structure):
- **Concurrent ordered map with lock-free reads** is the core
  requirement. A skip list is the industry-standard choice — RocksDB's
  and LevelDB's MemTables both use skip lists for exactly this property.
- **Erase-safe for concurrent readers**: a node can be logically deleted
  (tombstone flag) then physically unlinked, with reclamation deferred
  to epoch advance. A reader whose cursor is positioned on a
  logically-deleted node sees the tombstone and advances; the node's
  memory stays alive until the reader's guard closes. This is the same
  pattern L1 uses (a retired page stays readable under an in-flight
  guard).
- **Cache-friendliness**: a skip list is less cache-friendly than a
  B+tree for long range scans, but L0 is bounded by
  `memtable_flush_entries` (100K keys, typically far less under the 3s
  maintenance tick). The L1 B+tree handles the heavy range-scan workload;
  L0's job is point-get + short-prefix overlay, where skip-list
  performance is competitive.
- **Simpler than a concurrent B+tree**: a latch-free B+tree (Bw-Tree,
  OLFIT, EpTree) is 3–5× more code and far harder to get right under
  concurrent split/merge. L0 doesn't need split/merge — it's a flat
  sorted map that gets drained wholesale. A skip list avoids all that
  complexity.
- **Single-writer, many-reader suffices for v1**: the apply path
  (`upsert`) can keep a write lock (or use CAS for concurrent writers as
  a later refinement). The win is on the read side — readers never
  block, never copy. Apply is not the bottleneck; scan/get latency is.

**Design**:

1. **New `ConcurrentSkipList`** (replaces `absl::btree_map` in MemTable):
   - Nodes: `key` (std::string, SSO), `cell_entry` (slot/flags/cell, same
     as today), `next[]` tower of `std::atomic<Node*>`, a `deleted` flag
     (logical delete for drain).
   - Node allocation: from a dedicated arena (or the epoch-aware
     allocator). Nodes are epoch-retired, not immediately freed, so a
     reader under a guard can safely traverse a node the Flusher just
     unlinked.
   - Insert (`upsert` / `upsert_external`): under a write spinlock or
     CAS-based (v1: spinlock is fine — apply is not the bottleneck).
     Highest-slot-wins logic unchanged. The `durable_floor` /
     `allow_old_slots` semantics carry over.
   - Erase (`drain_up_to`): mark node `deleted = true` (logical), unlink
     from the tower, then `epoch_.retire(node, deleter)`. The deleter
     frees the node and fires the `kExternal` buffer's drop_fn (R30 Rust
     refcount decrement) — same as `map_.erase(it)` does today.
   - The `kExternal` split-cell path (R30) is unchanged: the node's
     `cell_entry` holds the `kExternal` buffer borrowing Rust `Bytes`,
     and the drop_fn fires when the node is epoch-reclaimed (deferred,
     not immediate — the Rust ref stays alive a bit longer, which is
     safe and matches how L1 pages hold `kExternal` refs).

2. **`MemTable::get()` — lock-free, zero-copy on hit**:
   - Traverse the skip list with atomic loads on `next[]` (no `mu_`).
   - On hit: if the caller holds an epoch guard (the engine's `get_view`
     / `scan` always do), the node is alive for the guard's duration —
     return a `kBorrowed` view into the cell, no copy. The existing
     `GetView::owned_` → `GetView::value_` borrow path (used for L1
     hits) extends naturally to L0 hits.
   - On `deleted` flag: skip (the node is logically drained).
   - The `get_view` L0 loop (`crow-tree.cpp:1444-1460`) no longer
     materializes each hit into `best_cell` then re-parses — it reads
     the `cell_entry`'s slot/flags directly from the node for the
     highest-slot-wins comparison, and only materializes the winner (one
     copy for the output, same as L1's `assemble_overflow_value` path).

3. **`MemTable::scan_cursor()` — replaces `snapshot()` in the scan path**:
   - Returns a cursor that holds `lower_bound(start_after)` on the skip
     list (O(log N), no copy). The cursor is a thin wrapper over the
     skip-list iterator — `key()`, `cell_slice()`, `advance()`.
   - The scan's merge loop (`crow-tree.cpp:1898-1947`) uses the cursor
     directly instead of indexing into a pre-built `vector<mem_entry>`.
     The `L0Cursor` struct changes from `{vector<mem_entry> entries;
     size_t idx}` to `{SkipListCursor cur}` — the merge logic (min-key
     select, highest-slot-wins on collision, early-stop past prefix) is
     unchanged.
   - `materialize()` is called only for entries that pass the
     `consider` lambda and make it into the output — O(limit)
     materializations instead of O(N_l0). For split cells, this means
     only `limit` value memcpys; for contiguous cells, only `limit`
     buffer clones.
   - The `upper_bound` skip pass (`crow-tree.cpp:1820-1826`) is
     eliminated — the cursor starts at `lower_bound(start_after)`
     directly.
   - `scan()`, `try_scan_no_load()`, and `scan_async()` (via
     `try_scan_no_load`) all get the same treatment — they share the
     `L0Cursor` pattern.

4. **`MemTable::snapshot()` — kept for `iter_all` / `compare` /
   `snapshot_export`**:
   - These paths need the full entry set (O(N) is correct there — they
     walk everything). `snapshot()` can be reimplemented on top of the
     skip list (traverse + materialize all) or kept as-is if those paths
     are off the hot path. The key point: `snapshot()` is no longer on
   the scan/get path.

5. **Epoch integration**:
   - The MemTable's nodes are retired through the same `epoch_` the
     engine already owns (`crow-tree.h:1197`). No new EBR instance —
     one `EpochManager`, one reclamation sweep, covering both L0 nodes
     and L1 pages.
   - `drain_up_to` calls `epoch_.retire(node, deleter)` instead of
     `map_.erase(it)`. The deleter frees the node and fires the
     `kExternal` drop_fn.
   - `reset()` (snapshot import) epoch-retires every node instead of
     clearing the map — same pattern as `free_all_resident_pages(retire
     = true)`.

**Why this is better than the alternatives**:
- **Range-bounded snapshot** (the old R48 L0-cursor scope, now dropped):
  still copies O(range) entries under `mu_`, still holds the mutex
  during the copy, doesn't help `get()`. A partial fix that leaves the
  root cause (no epoch protection for L0) in place. R48 is now scoped
  to the L1 leaf resolver (the actual 1KiB anomaly cause), not L0.
- **COW frozen_ + small active_ snapshot**: frozen tables become
  zero-copy (publish `shared_ptr<const btree_map>`), but `active_`
  still copies (bounded by range, but still a copy under `mu_`). Doubles
  memory briefly on freeze. Doesn't help `get()`. Doesn't address the
  `crow-tree.h:81` known gap. A workaround, not a fix.
- **Epoch-protected lock-free MemTable** (this item): eliminates the
  copy entirely on both scan and get, removes the `mu_` from the read
  path, closes the `crow-tree.h:81` gap, and aligns L0 with L1's
  reclamation model. The MemTable gets a more capable data structure
  (concurrent skip list) that is purpose-built for this workload.

**Expected gain**:
- Scan L0 cost: O(N_l0) copy → O(log N + limit) traversal. For a
  prefix scan with `limit = 100` over a 60K-entry memtable, this is
  ~600× fewer entries touched (60K → 100) and zero wasted copies.
- `get()` L0 hit: one mutex acquire + one cell copy → lock-free lookup
  + zero-copy borrow (under the existing epoch guard). Removes `mu_`
  contention between concurrent readers and the apply path.
- Scan tail latency: the `l0_snapshot` step (currently timed separately
  at `crow-tree.cpp:1957`) drops to near-zero for bounded scans.

**Files** (expected):
- New: `lib/crow-tree/include/crow-tree/skip_list.h` — concurrent skip
  list with epoch-deferred node reclamation.
- New: `lib/crow-tree/src/skip_list.cpp` — implementation + tests.
- Modified: `lib/crow-tree/include/crow-tree/memtable.h` — replace
  `absl::btree_map` with `ConcurrentSkipList`; add `scan_cursor()`;
  update `get()` to return a borrow; keep `snapshot()` for full-set
  paths.
- Modified: `lib/crow-tree/src/memtable.cpp` — rewrite on top of the
  skip list.
- Modified: `lib/crow-tree/src/crow-tree.cpp` — `scan()` /
  `try_scan_no_load()` / `get_view()` use the cursor / borrow; remove
  the `upper_bound` skip pass.
- Modified: `lib/crow-tree/include/crow-tree/crow-tree.h` — update the
  `crow-tree.h:81` comment (the gap is closed).
- New: `lib/crow-tree/tests/unit/skip_list_test.cpp` — concurrency
  correctness (insert/erase/iterate race), epoch reclamation safety.

**Acceptance**:
- Scan L0 cost is O(log N + limit), verified by a benchmark showing
  `l0_snapshot_ns` drops to near-zero for bounded scans and is
  independent of N_l0.
- `get()` L0 hit path takes no mutex (verified by profiling or a
  targeted stress test with concurrent readers + writers).
- All existing `test-tree-ct` tests pass (ReadPath.*, AsyncScan.*,
  overflow, snapshot export/import, install_snapshot, iter_all /
  compare via `snapshot()`).
- The R30 zero-copy apply path (split cells, `kExternal` buffers, Rust
  refcount drop_fn) remains correct — the drop_fn fires at epoch
  reclamation time instead of `map_.erase` time, which is safe (the
  Rust `Bytes` ref stays alive until the node is reclaimed, same as L1
  pages holding `kExternal` refs).
- No new mutex on the read path. No deadlock between the epoch guard
  and the write lock (the write lock is only on the upsert/drain path;
  readers never take it).

**Complexity**: High — a correct concurrent skip list with
epoch-deferred reclamation is ~500–800 lines (the data structure itself
+ tests). The MemTable rewrite and scan/get path changes are another
~300–500 lines. The hard part is getting the memory ordering right on
the skip list's `next[]` tower under concurrent insert/erase/iterate,
and ensuring the epoch reclamation correctly defers node freeing (and
the R30 drop_fn) until all in-flight readers have closed their guards.
Reference: RocksDB's `InlineSkipList` is the proven design to study
before building.

**Dependencies**: None — this is independent of R48 (now scoped to
the L1 leaf resolver, the actual 1KiB anomaly cause). R50 covers the
L0 copy cost; R48 covers the L1 resolve cost. Both are needed — they
address different parts of the scan path.

**Note**: The analysis of the current `snapshot()` copy cost and the
concurrency constraint (scan under epoch guard vs. drain under
`write_mutex_`) lives in `doc/working/kv-scan-flow-analysis.md` ≈L67-73.
This backlog item is the trackable stub; the working doc is the
rationale.
