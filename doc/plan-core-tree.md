# CrowKV — Plan: crowtree Core (libcrowtree)

Implementation task backlog for the **core data structure** of crowtree, i.e. the
scope of [`design/design-crowtree-core.md`](design/design-crowtree-core.md). This
is the **C++** in-memory engine in a **separate library `crowtree`** (no Rust, no
FFI, no disk yet — those are later plans).

- Parent design: [`design/design-crowtree.md`](design/design-crowtree.md)
- Test strategy: [`design/design-crowtree-test.md`](design/design-crowtree-test.md)
- Out of scope here (separate plans): `PageStore` / disk / checkpoint / recovery
  (`design-crowtree-persistence.md`), the C ABI + Rust `CrowtreeEngine`
  (FFI/`KVEngine` migration, `plan.md` P3 M1/M2), snapshot transfer + GC flow
  wiring (`design-crowtree-snapshot-gc.md`).

This plan maps to **`plan.md` P3 M2 (libcrowtree core)**, the in-memory part.

## Scope boundary

- **In:** MemTable (L0), slot cell, leaf/inner pages (in-memory layout), mapping
  table, delta records, write path (`apply` ingest + `flush`), consolidation,
  page split/merge, versioned root, epoch GC, read path (L0 overlay + scan), and
  an in-memory-only `compare`/`iter_all` oracle.
- **Out:** anything that writes bytes to a device, the on-disk page format / IU
  padding / CRC trailer, `PageStore`, the C ABI, async, RDMA. Pages here are plain
  heap allocations (core doc §3); persistence reuses the same layouts later.

## Library layout (proposed)

```
crowtree/                         # C++ lib, sibling of the crowkv/ Rust crate
  CMakeLists.txt
  include/crowtree/               # public C++ headers (NOT the C ABI)
    slice.h  status.h  options.h
    cell.h   page.h    mapping_table.h
    delta.h  memtable.h
    epoch.h  version.h
    crowtree.h  env.h
  src/                            # implementation .cc
  tests/
    unit/                         # GoogleTest, one file per component
    integration/                  # multi-component flows + randomized oracle
  bench/                          # Google Benchmark (optional, later)
```

**DECIDED:** the lib lives at **`crowtree/` in this repo** (may move out later).
Build integration is via the top-level **`Makefile`** (CMake under the hood), not a
cargo build script, while crowtree has no FFI yet:

- `make crowtree` — configure + build `libcrowtree` (CMake, `crowtree/build/`).
- `make crowtree-test` — build + run the GoogleTest suite (ctest).
- `make crowtree-asan` / `crowtree-tsan` — sanitizer builds.

When the C ABI lands (separate FFI plan), a cargo `build.rs` will invoke the same
CMake targets so `cargo build` transitively builds `libcrowtree`. Until then the
Makefile is the single entry point.

## Conventions

- C++20, `-Wall -Wextra -Werror`; CI matrix adds **ASan / TSan / UBSan** jobs.
- Tests: **GoogleTest**, integration-style under `tests/`, one file per component
  (mirrors `/coding` test layout intent for the Rust side).
- The correctness **oracle** is a `std::map<std::string,Cell>` reference model;
  `compare()`-style equivalence after every op (design-crowtree-test.md §1, §6).
- No exceptions across public API surfaces that the C ABI will later wrap;
  return `Status`. Internal code may use exceptions sparingly.

---

## Milestone CT-A — Scaffolding & primitives

- [x] **CT1 — Lib scaffold.** `CMakeLists.txt`, GoogleTest fetch/integration,
  sanitizer build options, `crowtree/` tree, `CrowtreeEnv` skeleton (owns the
  epoch manager + worker-pool placeholders), `Slice`, `Status`, `Options`
  (consolidation/flush/split tunables with defaults from core doc).
  - Tests: build smoke test; `Slice`/`Status` round-trip; `Options` defaults.
  - Deps: none.
- [x] **CT2 — Slot-aware value cell** (core doc §2). Encode/decode
  `[slot u64 LE][flags u8][value]`; tombstone flag; empty value; `kind`
  accessors; highest-slot-wins comparator helper.
  - Tests (`unit/cell_test.cc`): round-trip; tombstone; empty value; slot compare
    boundaries; reserved-bits ignored.
  - Deps: CT1.

## Milestone CT-B — Pages & indexing

- [x] **CT3 — Leaf base page** (core doc §3). In-memory leaf layout: header,
  four `uint32_t` metadata arrays (key/cell offsets + sizes), key bytes, cell
  payloads; build-from-sorted-entries; binary search; bloom filter
  (build + query); `low/high_key`, `right_sibling`. (No IU padding / on-disk CRC
  — that is persistence.)
  - Tests (`unit/leaf_page_test.cc`): build + search hit/miss; ordered iteration;
    bloom true-negative + measured false-positive rate; boundary keys (`-inf`/`+inf`).
  - Deps: CT2.
- [x] **CT4 — Inner base page + descent** (core doc §3). Separator keys + child
  PIDs; `find_leaf(key)` descent from `root_pid`.
  - Tests (`unit/inner_page_test.cc`): separator search; multi-level descent to
    the correct leaf; leftmost/rightmost.
  - Deps: CT3, CT5 (PID lookups via mapping table).
- [x] **CT5 — Mapping table** (core doc §4). Two-level segment array of
  `atomic<PageBase*>`; `Get` (atomic load), `Store` (plain, single-writer),
  `AllocatePID`/`FreePID` with free-list recycle; on-demand segment growth.
  - Tests (`unit/mapping_table_test.cc`): alloc/free/recycle; segment growth;
    `kInvalidPID`; concurrent readers + single writer store (TSan).
  - Deps: CT1.

## Milestone CT-C — Concurrency primitives

- [x] **CT6 — Epoch manager** (core doc §10). `Enter`/guard, `Retire(ptr,
  deleter)`, `AdvanceEpoch`, `TryReclaim`; reader-light enter/exit.
  - Tests (`unit/epoch_test.cc`): retired object not freed while a guard is open;
    freed after all guards in the epoch drop; reclaim accounting; ASan/TSan
    stress (many readers + retiring writer).
  - Deps: CT1.
- [x] **CT7 — MemTable (L0)** (core doc §1, §6.1). Concurrent ordered map
  (skiplist or sharded ordered map) of `key → cell`; `upsert_if_higher` (keeps
  highest slot, drops `slot ≤ last_applied_slot`); ordered `take_while(slot ≤ cs)`
  prefix drain; immutable ordered snapshot for reads; size/entry accounting for
  flush triggers.
  - Tests (`unit/memtable_test.cc`): highest-slot-wins upsert; drop-below-durable;
    ordered drain prefix vs retained tail; snapshot isolation; concurrent
    upsert + read + drain (TSan); hot-key collapse.
  - Deps: CT2, CT6.

## Milestone CT-D — Delta chain & write path

- [x] **CT8 — Delta records** (core doc §5). `BatchDelta` build from a sorted
  per-leaf group (metadata arrays + key/cell bytes), `FindKey` binary search,
  chain linkage (`next`, `delta_len`, `chain_bytes`); chain replay/resolve by
  highest slot.
  - Tests (`unit/delta_test.cc`): build + `FindKey`; chain of N deltas over a
    base resolves newest/highest-slot; tombstone shadows value.
  - Deps: CT3.
- [x] **CT9 — Write path: apply + flush** (core doc §6). `apply(slot, batch,
  contiguous_slot)` → MemTable ingest + frontier update + flush signal;
  `flush()` → drain contiguous prefix → `group_by_leaf` → one `BatchDelta` per
  leaf → `mapping.Store` → mark dirty → maybe consolidate → `publish_root_version`.
  - Tests (`integration/write_path_test.cc`): single & multi-leaf flush;
    intra-batch last-wins; out-of-order `apply` converges (idempotent); NoOp slot
    advances frontier without blocking flush; `last_applied_slot` after flush.
  - Deps: CT7, CT8, CT5, CT11 (version publish), CT10 (consolidate trigger).

## Milestone CT-E — Maintenance SMOs

- [x] **CT10 — Consolidation** (core doc §7). Fold a leaf's delta chain into a
  fresh base by highest slot; preserve tombstones (drop only with a GC hint, which
  is a no-op stub here); triggers on `max_delta_len` / `max_delta_bytes`; epoch-
  retire the old chain.
  - Tests (`unit/consolidation_test.cc`): fold correctness vs oracle; trigger at
    both thresholds; tombstones preserved; old chain retired (epoch counters).
  - Deps: CT8, CT6.
- [x] **CT11 — Versioned root / VersionTable** (core doc §9). `publish_root_version`
  (immutable root + `last_applied_slot` tag, version++); `snapshot_view()` pins
  current version (refcount); release; eligibility check (refcount 0 + below a
  watermark stub).
  - Tests (`unit/version_test.cc`): pin sees a stable tree while writer churns;
    refcount lifecycle; exact-point-in-time tag equals flushed slot.
  - Deps: CT5, CT6.
- [x] **CT12 — Page split & merge** (core doc §8). Writer-exclusive split (by-bytes
  split point, sibling relink, index-term insert, inner split propagate, root
  grow); merge (below threshold, absorb into left sibling, index-term remove,
  free PID, root collapse); hysteresis (split at target, merge at target/4).
  - Tests (`integration/split_merge_test.cc`): drive enough data to split leaves &
    grow an inner level; delete to trigger merges & root collapse; tree stays
    ordered + searchable throughout; no split/merge oscillation.
  - Deps: CT9, CT10, CT4.

## Milestone CT-F — Reads & end-to-end

- [ ] **CT13 — Read path** (core doc §11). `get` (L0 overlay then L1 chain),
  `multi_get`, `scan(prefix, limit)` merge cursor over L0 + L1 leaf chain (key
  merge, L0 wins ties), `iter_all` on a pinned version (includes tombstones),
  `compare(other)`.
  - Tests (`integration/read_path_test.cc`): get after put/delete; L0-overrides-L1;
    scan order + `limit`/`truncated` across `right_sibling`; scan excludes
    tombstones; `iter_all` includes them; `compare` empty/non-empty diffs.
  - Deps: CT9, CT11.
- [ ] **CT14 — Randomized parity + concurrency** (design-crowtree-test.md §6, §7).
  Seeded op-stream generator (puts/deletes/batches, increasing + out-of-order
  slots, duplicate + prefix keys); apply to crowtree and to the `std::map` oracle;
  `compare`-equal after every K ops, including across flush/consolidate/split.
  Reader+writer stress under **TSan/ASan/UBSan**.
  - Tests (`integration/parity_test.cc`, `integration/stress_test.cc`): parity
    invariant; epoch reclaim-under-load (no UAF); version-pin GC under churn.
  - Deps: all above.

---

## Dependency order (critical path)

```
CT1 ─┬─ CT2 ─ CT3 ─┬─ CT4
     │             └─ CT8 ─┐
     ├─ CT5 ───────────────┤
     ├─ CT6 ─┬─ CT7 ───────┼─ CT9 ─┬─ CT10 ─┐
     │       └─ CT11 ──────┘       ├─ CT12  ├─ CT13 ─ CT14
     └───────────────────────────-┘        ┘
```

## Acceptance for this plan (P3 M2 in-memory part)

- All CT unit + integration tests green, including the `std::map` parity oracle.
- Clean under ASan / TSan / UBSan.
- Public C++ API (`Crowtree`, `CrowtreeEnv`, `EngineView`-equivalent view) stable
  enough for the persistence + C-ABI plans to build on without core redesign.

## Open items to confirm before/while coding

- ~~Library path~~ — DECIDED: `crowtree/` in-repo (see layout note).
- MemTable structure: lock-free skiplist vs sharded `std::map` under a striped
  lock (start with the simpler sharded map; revisit under the CT14 stress bench).
- Whether inner pages keep a delta chain too, or are always rebuilt on change
  (core doc currently flushes inner pages only at checkpoint; in-memory we can
  rebuild eagerly — confirm in CT4/CT12).

---

## Implementation Log & Issues (for user review)

Autonomous implementation notes. Decisions I made without blocking, and questions
for the user to answer in one pass.

### Decisions taken during impl

- **Build:** CMake (`crowtree/CMakeLists.txt`) with `GLOB_RECURSE CONFIGURE_DEPENDS`
  for sources + tests, a single `crowtree_tests` binary, `gtest_discover_tests`.
  System GoogleTest (`find_package(GTest)`). Driven from the top `Makefile`
  (`make crowtree`, `crowtree-test`, `crowtree-asan`, `crowtree-tsan`).
- **In-memory pages, not byte-packed (big one — see Q1 below).** For the core
  in-memory engine, leaf/inner pages are backed by C++ containers, not the exact
  on-disk offset-array byte layout from core doc §3. Slot/cell encoding *is* real
  (the byte format that matters for semantics). The packed on-disk page layout is
  implemented in the persistence plan where it is actually needed.
- **Epoch reclamation (CT6) is mutex-based**, not the lock-free nanosecond
  enter/exit from the design — correct + TSan-clean, optimize later.
- **MemTable (CT7)** starts as `std::map` under a mutex (sharded/skiplist later).
- **Sanitizers:** `gtest_discover_tests(DISCOVERY_MODE PRE_TEST)` + `setarch -R`
  (disable ASLR) are required for TSan/ASan on this kernel (otherwise TSan aborts
  with "unexpected memory mapping"). `make crowtree-tsan` is green.
- **MappingTable:** fixed top-level array of `kMaxSegments=65536` atomic segment
  pointers (64M PIDs), segments allocated on demand via CAS; lock-free `Get`/
  `Store`, mutex only for PID alloc/free. Does not own pages (epoch frees them).
- **Write path (CT9):** `Apply` dedups intra-batch (last-wins) then upserts L0.
  `Flush` sets the MemTable durable floor to `cs` *before* draining `<= cs`, so L0
  is always strictly newer than L1 (no read race). Flush groups the key-sorted
  drained entries by leaf, prepends one `BatchDelta` per leaf, and consolidates a
  leaf whose chain exceeds `max_delta_len`/`max_delta_bytes`. A `BatchDelta`'s
  `slot_` is the flushed `cs` (chain tag); each cell keeps its own real slot, so
  resolve/consolidate stay highest-slot-wins even with mixed slots in one flush.
  `~Crowtree` frees live pages by walking from the root; the (shared) Env's epoch
  manager frees retired pages. Tests use a local `CrowtreeEnv` per test.

### More decisions (CT12)

- **Split is reader-safe without an SMO/split-delta protocol:** the right sibling
  is published and parents repointed *before* the original leaf is shrunk, and
  merges publish the combined leaf + repoint the parent before retiring the old
  leaf. So concurrent latest-`Get` never misses a key mid-SMO. Old pages are
  epoch-retired.
- **Minimal retention GC added (`SetGcWatermark`)**: consolidation/merge drop
  tombstones with `slot <= watermark`. This was needed so deletes actually shrink
  leaves (else merge/root-collapse can never fire). It also implements part of the
  design's logical retention GC. Inner-node underflow merge is deferred (only leaf
  merge + root collapse); correctness holds, tree may keep underfull inner nodes.
- **Merge leaks the merged-away PID** (no `FreePID`) to avoid a nullptr race
  window for stragglers; acceptable in v1 (64M PID space). `PathToPidLocked` is an
  O(tree) DFS per SMO; a parent-pointer optimization is deferred.

### Questions / issues for the user

- **Q1 (most important).** I implemented pages as in-memory C++ containers rather
  than the byte-packed offset-array layout in `design-crowtree-core.md §3`. The
  byte-packed layout only earns its keep on disk, so I deferred it to the
  persistence plan and kept the core engine semantics-accurate but representation-
  simple. **OK?** If you want the exact packed layout in the core too, say so and
  I'll add a packed `LeafBase`/`InnerBase` codec.
- **Q2 (snapshots).** `SnapshotView()` materializes the L1 keyspace into an
  independent immutable `Snapshot` (key-sorted, includes tombstones), under a
  brief write lock, instead of pinning a zero-copy COW root. Rationale: the live
  tree uses in-place mapping-slot replacement (fast writes, lock-free *latest*
  reads); true path-copy COW (new PID per modified node up to a new root) is the
  design's zero-copy model but a much larger change, best done with persistence.
  Materialized snapshots are O(N) and take a short lock — fine for tests, scan-at,
  compare and the parity oracle. **OK to keep materialized snapshots for the core,
  and add path-copy COW later (likely with the persistence plan)?**
- (more added as tasks complete)
