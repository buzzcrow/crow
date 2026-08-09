<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R58 Plan — Merge Loop 2-Source Fast Path + Loser Tree

Track progress here. One task in progress at a time.

## Tasks

- [x] 1. Add `Cursor::prefetch_next()` to `skip_list.h` (1-line method:
      `__builtin_prefetch(cur_->next(0))`).
- [x] 2. Add `LoserTree` internal helper class in `crow-tree.cpp`
      (MergeSource view, match function, build, winner, sift-up, collision
      drain, rebuild-on-exhaust).
- [x] 3. Rewrite `scan()` merge loop: track `n_valid_l0` incrementally;
      dispatch to single-source / 2-source fast path / loser tree based on
      `n_sources`; add `__builtin_prefetch` at cursor-advance and `refill_l1`
      sites.
- [x] 4. Rewrite `try_scan_no_load()` merge loop: same changes as `scan()`.
- [x] 5. Add C++ test: multi-frozen-memtable scan (3+ frozen, k > 2 path)
      asserting correct merge order + highest-slot-wins + no duplicates.
- [x] 6. Lint + affected tests pass; commit.

## Files

- `lib/crow-tree/include/crow-tree/skip_list.h` — `Cursor::prefetch_next()`
- `lib/crow-tree/src/crow-tree.cpp` — `LoserTree` helper; `scan()` +
  `try_scan_no_load()` merge loop rewrite; prefetch calls
- `lib/crow-tree/tests/integration/read_path_test.cpp` (or
  `double_buffer_test.cpp`) — multi-frozen-memtable scan test

## Test Checklist

- [ ] `pixi run clean-env && pixi run test-tree-ct` (C++ scan tests +
  new multi-frozen test)
- [ ] `pixi run clean-env && pixi run test-tree-ffi` (FFI scan, exercises
  `scan()` via C API)
- [ ] `pixi run clean-env && pixi run test-kv-core` (conformance scan tests
  exercise `try_scan_no_load` via the engine)
- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run clang-format --dry-run --Werror` (changed .cpp/.h)
- [ ] `pixi run tree-lint` (changed C++)
