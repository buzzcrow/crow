<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R50 Plan: Epoch-Protected Lock-Free MemTable

Reference: `doc/working/design-epoch-protected-memtable.md`

## Task Breakdown

- [ ] **T1: ConcurrentSkipList core** (`skip_list.h`, `skip_list.cpp`)
  - Node layout: inline key, `next[]` tower, atomic cell-version ptr,
    deleted flag
  - Arena allocator (bump alloc, per-MemTable)
  - Height randomization (p=0.25, max 12)
  - Insert (spinlock-serialized, bottom-up splice, release stores)
  - Find / lower_bound (acquire-load traversal)
  - Logical delete + unlink
  - Build + clang-format
- [ ] **T2: SkipList unit tests** (`skip_list_test.cpp`)
  - Basic insert/find/ordered-iteration
  - Overwrite (versioned cell publish)
  - Drain (delete + unlink)
  - Concurrent insert + iterate stress
  - Concurrent overwrite + iterate stress (TSAN)
  - Epoch reclamation safety (reader holds node across retire)
- [ ] **T3: MemTable rewrite** (`memtable.h`, `memtable.cpp`)
  - Replace `btree_map` + `mu_` with `ConcurrentSkipList`
  - CellVersion struct (buffer + slot + flags)
  - `upsert` / `upsert_external` on skip list (versioned overwrite)
  - `cursor(start_after)` — SkipListCursor wrapper
  - `get` → borrow-returning `get_view` (no std::string copy)
  - `drain_up_to` — unlink + epoch retire
  - `reset` — epoch retire all nodes
  - `snapshot` — cursor walk (full-set paths)
  - Atomic counters: bytes_, min_slot_, max_slot_, count, empty
  - EpochManager reference (passed in from Crowtree)
- [ ] **T4: Crowtree scan path** (`crow-tree.cpp`)
  - `L0Cursor` → `{SkipListCursor cur}` in `scan()` and
    `try_scan_no_load()`
  - Merge loop: read key/cell/slot off cursor, advance
  - Remove `upper_bound` skip pass + `scan_l0_skip_l` metric
  - Remove `l0_snapshot` timing (now O(log N), not a copy)
- [ ] **T5: Crowtree get path** (`crow-tree.cpp`)
  - `get_view()` / `try_get_view_no_load()`: borrow L0 value
    directly into `GetView::value_` (guard keeps it alive)
  - Split cells: borrow kExternal value, synthesize 9B header only
    if caller needs full cell
  - Remove `std::string best_cell` staging
- [ ] **T6: Epoch integration + bounded retirement**
  - Pass `epoch_` to MemTable (constructor or setter)
  - Retire nodes + cell versions through `epoch_.retire()`
  - Retire-queue high-water mark → force `try_reclaim()`
  - Metric: retire queue depth, oldest-guard age
- [ ] **T7: crow-tree.h gap comment removal**
  - Remove the `:81` "MemTable cell isn't epoch-protected" comment
  - Update `design-crow-tree-engine.md` §1.7 read path (L0 no longer
    copies)
- [ ] **T8: Update existing memtable tests**
  - `memtable_test.cpp`: adapt to new API (cursor, borrow get)
  - `zero_copy_apply_test.cpp`: verify split-cell borrow still works
  - Run `test-tree-ct` — fix regressions
- [ ] **T9: TSAN + ASAN validation**
  - `tree-tsan` on skip_list_test + memtable_test
  - `tree-asan` on skip_list_test + memtable_test
  - Overwrite-while-reading stress under TSAN

## File List

- **New**: `lib/crow-tree/include/crow-tree/skip_list.h`
- **New**: `lib/crow-tree/src/skip_list.cpp`
- **New**: `lib/crow-tree/tests/unit/skip_list_test.cpp`
- **Modify**: `lib/crow-tree/include/crow-tree/memtable.h`
- **Modify**: `lib/crow-tree/src/memtable.cpp`
- **Modify**: `lib/crow-tree/src/crow-tree.cpp` (scan + get paths)
- **Modify**: `lib/crow-tree/include/crow-tree/crow-tree.h` (gap comment)
- **Modify**: `lib/crow-tree/tests/unit/memtable_test.cpp`
- **Modify**: `lib/crow-tree/CMakeLists.txt` (new sources)
- **Modify**: `doc/design/tree/design-crow-tree-engine.md` (§1.7)

## Test Checklist

- [ ] `pixi run test-ct` (all crow-tree C++ tests)
- [ ] `tree-tsan` on new + existing memtable tests
- [ ] `tree-asan` on new + existing memtable tests
- [ ] `scan_step_profile` bench: l0_snapshot independent of N_l0
- [ ] R30 split-cell drop_fn fires exactly once (zero_copy_apply_test)
- [ ] Concurrent overwrite-while-reading stress (TSAN clean)

## Dependency Ordering

T1 → T2 → T3 → T4 + T5 (parallel) → T6 → T7 → T8 → T9

T1-T2 are self-contained (skip list + its own tests). T3 depends on
T1. T4/T5 depend on T3. T6 ties T3 into the engine's epoch. T7-T9 are
cleanup + validation.
