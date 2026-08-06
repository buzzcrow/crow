<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R50: Epoch-protected lock-free MemTable — zero-copy L0 reads

**Status — unblocked. Both gates cleared:**

- **Gate 1 — R48 landed.** The lazy `LeafChainCursor` (commit `9ae2e72`)
  dropped 64B limit=1000 from 1183us to 19.9us total, `l1_resolve` to 0.0us.
  Post-R48, `l0_snapshot` is the dominant remaining scan cost when L0 is
  non-empty — the opposite of the pre-R48 picture.
- **Gate 2 — measured.** A concurrent write+scan microbench
  (`scan_step_bench.cpp` `run_concurrent`) with a production-like 3s flush
  tick (mimicking the maintenance loop) and a sustained writer shows
  `l0_snapshot` is non-trivial and dominates scan time:

  | Scenario | Write rate | Flush | l0_snapshot avg | % of scan |
  |----------|-----------:|------|----------------:|----------:|
  | 64B flush 3s | 1261k/s | 3s | 126.2us | 81.8% |
  | 1KiB flush 3s | 888k/s | 3s | 1122us | 93.5% |
  | 64B no flush | 1294k/s | none | 128.4us | 82.8% |
  | 1KiB no flush | 1224k/s | none | 5816us | 97.8% |

  The production bench showed 0us because it is pre-populate-then-scan
  with no concurrent writes — the 3s tick drains L0 during pre-populate.
  Under sustained concurrent write+scan, L0 fills between flush ticks
  (1 frozen + 1 overgrown active_, bounded by the keyspace), and the
  snapshot copy dominates. Per the gate rule, `l0_snapshot` does **not**
  stay near zero → R50 proceeds.

Scope note: the double copy on the L0 `get` path (below) is real,
unconditional, and fixable in ~20 lines without touching concurrency.
It is folded into R50's `get_view` borrow change (Design §3).

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
even for entries the scan never emits: everything before `start_after`
is copied then discarded by the `upper_bound` skip
(`crow-tree.cpp:1820-1826`), and everything past the prefix range or the
`limit` is copied then never read.

`get()` has the same root cause plus an extra copy. `MemTable::get()`
(`memtable.cpp:143`) takes `mu_` and copies the cell into a
`std::string`; `get_view()` (`crow-tree.cpp:1470-1473`) then copies the
value *again* into `result.owned_`. Two copies per L0 hit, on both
`get_view()` and `try_get_view_no_load()`.

The header at `crow-tree.h:81` flags this as a known gap: "MemTable cell
isn't epoch-protected the same way — a later refinement."

**Root cause**: L0 is the only reader-visible structure not integrated
into the engine's epoch-based reclamation (EBR) scheme. L1 pages are
epoch-protected — `retire_page()` (`crow-tree.cpp:166`) defers freeing a
replaced page until no in-flight epoch guard could still reference it,
so lock-free readers walk L1 with zero copy. L0 cells live in a
`btree_map` under a mutex and are freed immediately on erase *and on
overwrite*, so readers must snapshot-copy to be safe.

**Target**: bring L0 into the same epoch-protection scheme as L1.
Replace `absl::btree_map<std::string, cell_entry>` under `mu_` with a
concurrent skip list whose nodes and cell buffers are epoch-retired, so
readers iterate L0 lock-free under their existing guard with zero copy,
and every writer-side free (erase, overwrite, reset) is epoch-deferred.

One structure for both `active_` and `frozen_` tables. A hybrid — a
sealed immutable `btree_map` for frozen tables plus a skip list for
`active_` — was considered and rejected: it is lower-risk to build
incrementally but a permanently worse end state (two cursor types, two
drain paths, a sealing state machine), and it leaves the
`crow-tree.h:81` gap open for `active_`, which is where the reader/writer
race actually lives.

---

**Design**

*1. `ConcurrentSkipList` — node layout and allocation*

- Node holds: `next[]` tower of `std::atomic<Node*>`, `std::atomic<cell
  version*>`, a logical-`deleted` flag, key length, and the key bytes
  **inline in the node's tail allocation** (RocksDB `InlineSkipList`
  style) — one allocation, no pointer chase, no `std::string` header.
  Retrofitting this later means re-touching every node path, so it is
  in scope from the start.
- Nodes come from a per-MemTable arena. Height is drawn at insert
  (p=0.25, max 12) so the tower is sized exactly, no over-allocation.
- Keys are immutable for a node's lifetime; only the cell version
  pointer is mutable. This is what makes the read path a pure atomic
  load.

*2. Write path — single spinlock, every free epoch-deferred*

Writers are serialized by a write spinlock replacing `mu_`. `apply()`
already serializes on `mu_` today and is not the scan bottleneck, so
CAS-based concurrent insert is explicitly out of scope; it can be added
later without changing the read path.

- **Insert** (`upsert` / `upsert_external`): splice the node in
  bottom-up with release stores. Highest-slot-wins, `durable_floor_`,
  and `allow_old_slots` semantics carry over unchanged.
- **Overwrite** — the correctness hole in the naive design. Today
  highest-slot-wins frees the old cell in place (`memtable.cpp:51`,
  `:91`). Once readers borrow cell bytes under a guard, an out-of-order
  higher-slot upsert to a key a reader is mid-borrow on is a
  use-after-free. The node therefore carries a **versioned cell**:
  publish the new version with a release store, then
  `epoch_.retire(old_version)`. The `flush()` relocate path
  (`crow-tree.cpp:1043-1045`) upserts into a live, concurrently-scanned
  `active_`, so this is a routine occurrence, not an edge case.
- **Erase** (`drain_up_to`): set `deleted`, unlink the tower, then
  `epoch_.retire(node)`. The deleter frees the node arena block and
  fires the `kExternal` buffer's drop_fn (R30 Rust refcount release).
  Drain no longer needs `materialize_move()`'s move optimization to be
  destructive-safe, but keeps it — the node is unlinked before the
  buffer moves out, and reclamation is deferred, so the move is legal.
- **`reset()`** (snapshot import): epoch-retires every node rather than
  clearing — same pattern as `free_all_resident_pages(retire = true)`.

*3. Read path — cursor and borrow, no mutex*

- `MemTable::cursor(start_after)` returns a cursor seeded by an O(log N)
  `lower_bound`, exposing `key()`, `cell()`, `slot()`, `flags()`,
  `advance()`. Traversal is atomic acquire loads on `next[]`;
  logically-deleted nodes are skipped.
- The scan's `L0Cursor` (`crow-tree.cpp:1798-1802`) changes from
  `{vector<mem_entry> entries; size_t idx}` to `{SkipListCursor cur}`.
  The merge loop (`crow-tree.cpp:1898-1947`) is otherwise unchanged —
  min-key select, highest-slot-wins on collision, early stop past
  prefix. `materialize()` runs only for entries that pass `consider`
  and reach the output: O(limit), not O(N_l0).
- The `upper_bound` skip pass (`crow-tree.cpp:1820-1826`) and its
  `scan_l0_skip_l` metric are deleted — the cursor seeks directly.
- `scan()`, `try_scan_no_load()`, and `scan_async()` share the change
  via `L0Cursor`.
- **Get borrows, both copies gone.** `get_view()` and
  `try_get_view_no_load()` drop the `std::string best_cell` staging and
  compare slot/flags read directly off the node for highest-slot-wins,
  then borrow the winner's value into `GetView::value_` — the guard
  already held keeps it alive, exactly as an L1 frame hit does today.
  Split cells (R30) borrow the `kExternal` value directly; only the
  9-byte header is synthesized, and only for callers that need a full
  cell.

*4. Epoch integration and bounded retirement*

- Nodes and cell versions retire through the engine's existing `epoch_`
  (`crow-tree.h:1197`). No second EBR instance — one manager, one sweep,
  covering L0 nodes and L1 pages.
- **Retirement must be bounded.** Today `drain_up_to` frees cells and
  fires the R30 drop_fn promptly. Under EBR a single long-lived guard
  pins every retired L0 node and its borrowed Rust `Bytes`, and
  `GetView` holds a guard for the caller's lifetime (`crow-tree.h:171`)
  across the FFI boundary. One slow Rust consumer can then hold drained
  entries resident in both L1 and retired-pending L0 across a whole
  flush cycle. In scope: a retire-queue high-water mark that forces a
  sweep, and a metric for queue depth and oldest-guard age. Without
  this, R50 trades a CPU cost for an unbounded memory cost.

*5. Bookkeeping counters become atomics*

`bytes_`, `min_slot_`, `max_slot_`, `count()`, and `empty()` are read by
`maybe_freeze_active`'s thresholds and by diagnostics, and currently sit
under `mu_`. They become relaxed atomics maintained by the writer.

*6. `snapshot()` retained for full-set paths*

`iter_all`, `compare`, and `snapshot_export` need every entry — O(N) is
correct there. `snapshot()` is reimplemented as a cursor walk. The point
of R50 is that it is no longer on the scan or get path.

---

**Why a skip list**

- A concurrent ordered map with lock-free reads is the requirement, and
  a skip list is the industry-standard answer (RocksDB, LevelDB).
- Erase is safe for concurrent readers: logical tombstone, unlink,
  epoch-deferred reclamation — the same pattern L1's `retire_page()`
  already uses, so there is one reclamation model in the engine, not
  two.
- A latch-free B+tree (Bw-Tree, OLFIT, EpTree) is several times more
  code and far harder to get right under concurrent split/merge, which
  L0 does not need — it is a flat sorted map drained wholesale.
- Cache-friendliness is the one regression: a skip list is worse than
  `absl::btree_map` for long ordered walks, and node-per-entry
  allocation costs RSS versus btree's packed nodes. L0 is bounded by
  `memtable_flush_entries` and its job is point-get plus short-prefix
  overlay; the L1 B+tree carries the heavy range scans. Accepted, but it
  is a real cost and belongs in the Gate 2 measurement.

**Rejected alternatives**

- *Range- or chunk-bounded snapshot* (`snapshot_range(start_after,
  prefix, max_entries)` with cursor refill): reaches the same O(log N +
  limit) asymptotic in ~150 lines with no concurrency reasoning, and is
  the right answer if only scan matters. Rejected as the end state
  because it keeps `mu_` on the read path, still copies, does nothing
  for `get()`, and leaves `crow-tree.h:81` open. Worth remembering as
  the fallback if Gate 2 shows a modest but non-zero L0 cost.
- *Sealed immutable frozen tables* (write-closed `btree_map` +
  non-destructive drain, lifetime via the existing `shared_ptr`): sound
  and EBR-free, but only covers `frozen_`. `active_` is where the
  reader/writer race is. See the Target section for why the hybrid is
  rejected.
- *COW frozen + small active snapshot*: frozen becomes zero-copy but
  `active_` still copies, memory doubles briefly on freeze, and `get()`
  is untouched.

**Expected gain** (to be confirmed at Gate 2, not assumed)

- Scan L0: O(N_l0) copy → O(log N + limit) traversal.
- Get L0 hit: mutex + two copies → lock-free lookup + zero-copy borrow.
- `mu_` removed from the read path entirely.

**Files** (expected)

- New `include/crow-tree/skip_list.h`, `src/skip_list.cpp` — concurrent
  skip list, inline keys, arena, epoch-deferred reclamation.
- New `tests/unit/skip_list_test.cpp` — concurrent insert / overwrite /
  drain / iterate stress, epoch reclamation safety.
- `memtable.h` / `memtable.cpp` — rewritten on the skip list: versioned
  cell, cursor API, borrow-returning get, atomic counters.
- `crow-tree.cpp` — `L0Cursor` becomes a skip-list cursor in `scan()`
  and `try_scan_no_load()`; `get_view()` / `try_get_view_no_load()`
  borrow; `upper_bound` skip pass removed.
- `crow-tree.h` — the `:81` gap comment is removed (gap closed).

**Acceptance**

- `scan_l0_snapshot_l` is independent of N_l0 for a bounded scan
  (benchmark at 1K / 10K / 100K live L0 entries, `limit = 100`).
- No mutex on the L0 read path (profiled or asserted).
- L0 `get_view` performs zero value copies on a hit.
- Overwrite-while-reading is safe: a targeted stress test with a reader
  holding a borrowed L0 value across a concurrent higher-slot upsert to
  the same key.
- Retire-queue depth stays bounded with a `GetView` deliberately held
  across several flush cycles.
- R30 correctness holds: split cells, `kExternal` buffers, and the Rust
  refcount drop_fn release exactly once, at reclamation rather than at
  erase.
- Existing `test-tree-ct` passes: ReadPath.*, AsyncScan.*, overflow,
  snapshot export/import, install_snapshot, iter_all / compare.
- `tree-tsan` and `tree-asan` green. Existing tests passing will not
  catch a memory-ordering bug in a hand-rolled skip list; the stress
  test under TSAN is the real gate.

**Complexity**: High — ~800–1300 lines. The hard parts are the memory
ordering on the `next[]` tower under concurrent insert / unlink /
traverse, the versioned-cell overwrite protocol, and proving reclamation
defers past every in-flight guard. Reference: RocksDB's
`InlineSkipList`. This is the highest-risk change in the engine, which
is why both gates are hard blockers rather than advisory.

**Dependencies**: **R48 is a blocker** (Gate 1) — it fixes the
measured 99.5% of scan cost and must land before L0 work is
prioritized. The two items are otherwise independent: R48 covers the L1
resolve cost, R50 the L0 copy cost.

**Note**: the analysis of the current `snapshot()` copy cost and the
concurrency constraint (scan under epoch guard vs. drain under
`write_mutex_`) lives in `doc/working/kv-scan-flow-analysis.md` ≈L67-73,
with the per-step scan profile that produced the 0us `l0_snapshot`
reading. This backlog item is the trackable stub; the working doc is the
rationale.
