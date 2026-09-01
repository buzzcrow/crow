<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Performance Observer Mechanism Plan

Refines: `doc/working/todo_perf.md` (P2 + P3)
Goal: implement P2 maintenance-loop logs and P3 tree-engine metrics
across all three layers (tree ops, mapping table, backend I/O).

## Inconsistencies Found (code vs. todo_perf.md)

- P3 "Done" claims `ct.snapshot.l` was brought back as a logical
  metric — it is NOT in code. Fix: add it.
- `ct.snapshot.pages.c` is registered but never incremented — bug.
- All P3 renames are NOT done (code uses old names).
- All P3 new metrics are MISSING (no handles, no registration).
- P2 maintenance loop: only snapshot has partial start/stop logs.
  Flush, WAL flush, GC phases have no timing logs. No snapshot
  trigger reason log.
- Frozen queue full log already exists on C++ side
  (`maybe_freeze_active`, `crowdb-tree.cpp:1028`). P2 requirement
  satisfied — no Rust-side log needed.

## Tasks

### Phase 1 — P2: Maintenance loop logs

- [x] **P2.1**: Add flush phase start/stop log with elapsed_ms.
  Files: `lib/crowdb-kv/src/cluster/group_maintenance.rs` (run_pass,
  around the `engine_arc.flush()` spawn_blocking call ~line 196).
- [x] **P2.2**: Add snapshot trigger reason log (which threshold
  fired: slot/time/flush_count). Files: `group_maintenance.rs`
  (run_pass, around `should_snapshot` check ~line 227).
- [x] **P2.3**: Add WAL flush phase start/stop log with elapsed_ms
  (currently only has completion debug log). Files:
  `group_maintenance.rs` (~line 254).
- [x] **P2.4**: Add GC phase start/stop log with elapsed_ms. Files:
  `group_maintenance.rs` (~line 287).

### Phase 2 — P3 Layer 1: Tree operations

- [x] **L1.1**: Rename `mt_upsert_c` → `mt_apply_c` (handle,
  registration string `.mt.upsert.c` → `.mt.apply.c`, 3 observe
  sites). Files: `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L1.2**: Add `ct.mt.apply.l` — memtable upsert batch latency.
  Time the `apply_one_locked` upsert loop. Files: `crowdb-tree.h`,
  `crowdb-tree.cpp`.
- [x] **L1.3**: Add `ct.mt.get.l` — memtable get latency. Time the
  L0 lookup loop in `try_get_view`. Files: `crowdb-tree.h`,
  `crowdb-tree.cpp`.
- [x] **L1.4**: Add `ct.mt.frozen.g` + `ct.mt.records.g` — gauges
  for frozen count and total live records. Update in flush or on a
  periodic tick. Files: `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L1.5**: Add `ct.mt.freeze.c` — memtable freeze counter.
  Increment in `maybe_freeze_active` on successful freeze. Files:
  `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L1.6**: Add `ct.l1.get.l` — L1 get latency. Time the
  descent + resolve_chain in `try_get_view`. Files: `crowdb-tree.h`,
  `crowdb-tree.cpp`.
- [x] **L1.7**: Add `ct.page.split.c` + `ct.page.merge.c` — SMO
  counters. Increment in `split_leaf_locked` and
  `try_merge_leaf_locked`. Files: `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L1.8**: Add `ct.page.consolidate.c` — consolidation counter.
  Increment in `consolidate_locked`. Files: `crowdb-tree.h`,
  `crowdb-tree.cpp`.
- [x] **L1.9**: Add `ct.tree.height.g` — tree height gauge. Update
  on root page id change (compute via `height()`). Files:
  `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L1.10**: Add `ct.scan.retry.c` — scan async retry counter.
  Increment in `scan_async_attempt` on cold-page recursion. Files:
  `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L1.11**: Add `ct.gc.tombstones.c` + `ct.gc.pages.c` — GC
  counters. Increment from `GcStats` return of `collect_garbage`.
  Files: `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L1.12**: Add `ct.snapshot.l` — logical snapshot latency
  (full `Crowdbtree::snapshot` wall time). Files: `crowdb-tree.h`,
  `crowdb-tree.cpp`, `persist.cpp`.

### Phase 3 — P3 Layer 2: Page mapping table

- [x] **L2.1**: Rename `page_map_lookup_c` → `page_find_c`
  (handle, registration `.page.map.lookup.c` → `.page.find.c`,
  observe site in `resident()`). Files: `crowdb-tree.h`,
  `crowdb-tree.cpp`.
- [x] **L2.2**: Merge `buf.hits.c` + `buf.misses.c` →
  `page.find.c` (total) + `page.find.hit.c` (hits). Update
  `BufferPool::set_metrics` signature, `pin()`, `init_metrics`.
  Files: `buffer_pool.h`, `buffer_pool.cpp`, `crowdb-tree.h`,
  `crowdb-tree.cpp`.
- [x] **L2.3**: Add `ct.page.map.alloc.c` — page ID allocation
  counter. Increment in `MappingTable::allocate_page_id`. Files:
  `crowdb-tree.h`, `mapping_table.h`, `mapping_table.cpp`,
  `crowdb-tree.cpp`.
- [x] **L2.4**: Add `ct.page.map.total_pids.g` + `ct.page.map.segments.g`
  — gauges for `next_page_id_` and `segments_allocated()`. Update
  periodically (in flush or a tick). Files: `crowdb-tree.h`,
  `crowdb-tree.cpp`.

### Phase 4 — P3 Layer 3: Backend I/O

- [x] **L3.1**: Rename `demand_load_l` → `page_load_l` (handle,
  registration `.demand.load.l` → `.page.load.l`, 2 observe sites).
  Files: `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L3.2**: Add `ct.{backend}.page.writeback.l` — eviction
  writeback latency. Time `BufferPool::write_back`. Files:
  `buffer_pool.h`, `buffer_pool.cpp`, `crowdb-tree.h`,
  `crowdb-tree.cpp`.
- [x] **L3.3**: Add `ct.{backend}.page.write.bw` — page write
  bandwidth (flush drain, non-snapshot). Observe in
  `store_preserving_parent_locked` or consolidate page writes.
  Files: `crowdb-tree.h`, `crowdb-tree.cpp`.
- [x] **L3.4**: Add `ct.{backend}.fsync.l` — fsync latency. Time
  `page_store->sync()` calls in `snapshot()`. Files: `crowdb-tree.h`,
  `persist.cpp`.
- [x] **L3.5**: Fix `ct.snapshot.pages.c` — wire to
  `prepare_snapshot_locked` where `pages_written` is computed. Move
  from `io` prefix to `prefix` (logical, per plan). Files:
  `crowdb-tree.cpp`, `persist.cpp`.

### Phase 5 — Tests + lint

- [x] **T1**: Update `lib/crowdb-tree/tests/unit/metrics_test.cpp`
  for renamed/new metrics.
- [x] **T2**: Update
  `lib/crowdb-tree/tests/integration/incremental_checkpoint_test.cpp`
  for init_metrics signature changes.
- [x] **T3**: Update `lib/crowdb-tree/bench/scan_step_bench.cpp`
  for renamed metrics prints.
- [x] **T4**: Run `pixi run -- cargo clippy -- -D warnings` and
  `pixi run -- cargo fmt --check`.
- [x] **T5**: Run `pixi run -- cargo test -p crowdb-kv` relevant
  tests.
- [x] **T6**: Run tree tests: `pixi run -- test-tree-ct` or
  equivalent.
- [x] **T7**: Run `clang-format --dry-run --Werror` on changed
  `.cpp`/`.h` files.

## File List

- `lib/crowdb-kv/src/cluster/group_maintenance.rs` — P2 logs
- `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` —
  MetricsHandles struct (new fields, renames)
- `lib/crowdb-tree/include/crowdb-tree/buffer_pool.h` —
  set_metrics signature change (L2.2), writeback latency (L3.2)
- `lib/crowdb-tree/include/crowdb-tree/mapping_table.h` —
  alloc counter accessor (L2.3)
- `lib/crowdb-tree/src/crowdb-tree.cpp` — init_metrics, observe
  sites, renames, new counters/gauges/summaries
- `lib/crowdb-tree/src/buffer_pool.cpp` — pin/writeback changes
- `lib/crowdb-tree/src/mapping_table.cpp` — alloc counter
- `lib/crowdb-tree/src/persist.cpp` — snapshot.pages.c fix,
  fsync.l, snapshot.l
- `lib/crowdb-tree/bench/scan_step_bench.cpp` — print updates
- `lib/crowdb-tree/tests/unit/metrics_test.cpp` — test fixes
- `lib/crowdb-tree/tests/integration/incremental_checkpoint_test.cpp`
  — init_metrics fixes
- `doc/working/todo_perf.md` — update status after impl

## Open Issues

All issues resolved in follow-up commit.

1. **`page.write.bw` → `page.writeback.bw`** (resolved):
   Renamed to `page.writeback.bw` for clarity — it pairs with
   `page.writeback.l` and accurately describes eviction I/O bandwidth.
   In-memory page rebuilds are tracked by separate counters:
   `page.split.c`, `page.merge.c`, `page.consolidate.c` — all three
   SMO (structure modification operation) counters are implemented and
   incremented in `split_leaf_locked`, `try_merge_leaf_locked`, and
   `consolidate_locked` respectively.

2. **`page.find.hit.c` removed** (resolved):
   Removed `page.find.hit.c` — it was misleading because it only
   counted buffer pool hits during demand loads (cold-page path), not
   all page lookups. The hit/miss breakdown is now derivable from:
   - `page.find.c` = total page lookups (mapping table resolution in
     `resident()`)
   - `page.load.l` count = demand loads (misses that went to disk)
   - hits = `page.find.c` - `page.load.l.count`
   The `page.load.l` LatencySummary already provides both count and
   latency for the I/O path, so no separate hit counter is needed.

3. **Structural gauges → CallbackGauge** (resolved):
   Converted `tree.height.g`, `mt.frozen.g`, `mt.records.g`,
   `page.map.total_pids.g`, and `page.map.segments.g` from plain
   `Gauge` to `CallbackGauge`. The callbacks invoke `height()`,
   `frozen_table_count()`, `all_memtables()` count sum,
   `mapping_.next_page_id()`, and `mapping_.segments_allocated()`
   respectively at flush time only (every ~5s on the metrics flush
   thread). Overhead is negligible — zero on hot paths, one callback
   invocation per metric per flush cycle. Removed the
   `update_structural_gauges()` method and its call from `flush()`.
   Gauges are now always current (no staleness between flushes).

4. **`scan_step_bench.cpp` code review** (resolved):
   The `init_metrics("s.0.g.0", "")` call is correct — empty backend
   label means metrics register as `s.0.g.0.*` (no backend suffix),
   which is appropriate for a standalone bench with `MemPageStore`.
   The bench doesn't read metrics output; it uses `scan_profile()` for
   profiling. The `init_metrics` call exists only to initialize the
   registry so metric observe sites don't crash on null handles. No
   other issues found.

5. **`ct.snapshot.l`** (resolved, no action needed):
   Added and observed in `Crowdbtree::snapshot()` (full wall time
   including prepare + I/O + fsync).