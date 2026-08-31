<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# crowdb-tree Drain & Snapshot Lock Optimization Plan

Goal: eliminate `write_mutex_` as a multi-second bottleneck on the
maintenance path so that `flush()` and `snapshot()` no longer serialize
against each other or block the B+tree for seconds at a time.

Context: bench `cluster-local-deploy-20260831-0935.17` showed all 3 nodes
running `persist_snapshot` for 6.5–7.7 seconds simultaneously during
steady-state. The root cause is `drain_memtable_into_l1_locked()` holding
`write_mutex_` for the entire B+tree drain (CPU-bound, 607K entries),
not the fsync. The freeze/swap mechanism (`maybe_freeze_active`) is
already outside `write_mutex_` (uses `memtable_mutex_`), so the apply
path is not directly blocked — but the drain's CPU saturation starves
the tokio/RPC reactor, causing heartbeat delays > election timeout,
leader churn, and a multi-second drain tail.

Related docs:
- `doc/design/tree/design-crowdb-tree-engine.md` §1.3 (write path),
  §1.8 (concurrency summary)
- `doc/design/tree/design-crowdb-tree-storage.md` (snapshot, persistence)

## Current Lock Topology

- `memtable_mutex_` (`std::shared_mutex`) — guards `active_` / `frozen_`
  queue swaps. Held briefly (microseconds) by `maybe_freeze_active()`
  on the apply path and by `flush()` to swap out the frozen queue. NOT
  on the hot path.
- `write_mutex_` (`std::mutex`) — the B+tree structural mutation lock.
  Held by:
  - `flush()` — for the ENTIRE drain (`drain_memtable_into_l1_locked`):
    per-entry leaf descent, batch delta build, `mapping_.store()`,
    `consolidate_locked()` (split/merge). CPU-bound, scales with
    memtable entry count.
  - `snapshot()` — only during `prepare_snapshot_locked()` (walk dirty
    segments, collect page writes). Released BEFORE page writes + fsync.
  - `collect_garbage()` — full GC pass.
  - `evict_clean_*()` — buffer pool eviction.
  - `install_snapshot*()` / `clear()` — snapshot install (rare).

## Problem Breakdown

### P1: `flush()` holds `write_mutex_` for the entire drain

`drain_memtable_into_l1_locked()` does two phases per entry group:

1. **Resolve** — `find_leaf_page_id()` descent + `resident()` lookup.
   Read-only on the tree structure. Safe under an epoch guard.
2. **Publish** — `mapping_.store(page_id, delta)` + possible
   `consolidate_locked()` (split/merge). This is the structural mutation
   that needs exclusion.

Phase 1 is the bulk of the CPU time (B+tree descent per entry group) and
does not need `write_mutex_`. Phase 2 is the actual mutation and needs
exclusion, but only for the store + consolidate, not the descent.

### P2: `flush()` + `snapshot()` serialized in `run_pass`

`group_maintenance.rs::run_pass()` calls `flush()` then
`persist_snapshot()` sequentially, both via `spawn_blocking`. The
snapshot internally calls `flush()` again (via
`crowdb_tree_engine.rs::persist_snapshot()`). So during one maintenance
pass: flush → snapshot's internal flush → snapshot's prepare. Three
`write_mutex_` acquisitions back-to-back.

### P3: All nodes snapshot simultaneously after cluster clean

After `wipe-user-data`, all 3 nodes start from slot 0 at the same time.
They hit `snapshot_slot_threshold` at the same slot, fire `flush()` +
`snapshot()` simultaneously, saturate all cores with B+tree drains,
starve the RPC reactor, and trigger leader election churn.

### P4: `max_memtable_count` was too low (fixed)

Default was 2 (1 active + 1 frozen). During a multi-second flush, the
single frozen slot fills, `maybe_freeze_active` returns false, and
`active_` grows unbounded — OOM risk under sustained write load. Now
bumped to 5 (1 active + 4 frozen) with an error log when the queue is
full.

## Tasks

### Phase 1: Stagger + headroom (low-risk, immediate)

- [x] **Bump `max_memtable_count` default to 5**: gives 4 frozen slots
  so active_ doesn't grow unbounded during a multi-second flush.
  Files: `lib/crowdb-tree/include/crowdb-tree/options.h`.
- [x] **Add error log when frozen queue is full**: `maybe_freeze_active`
  now logs at ERROR level with frozen count, active entries/bytes, and
  frozen total entries/bytes. Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [ ] **Stagger snapshot timing across replicas**: add per-replica jitter
  to `snapshot_slot_threshold` or `snapshot_time_threshold_ms` in the
  maintenance loop so 3 nodes don't fire snapshot at the same slot.
  Approach: seed jitter from `replica_id` (deterministic, no coordination
  needed). Files: `lib/crowdb-kv/src/cluster/group_maintenance.rs`,
  `lib/crowdb-kv/src/cluster/config.rs` (add
  `snapshot_threshold_jitter_pct`).

### Phase 2: Pipeline flush + snapshot in `run_pass`

- [ ] **Split `persist_snapshot` into flush + prepare + persist**: the
  Rust `persist_snapshot()` currently calls `flush()` then `snapshot()`
  as two separate blocking calls. Instead:
  1. `flush()` — drain L0 → L1 (holds `write_mutex_`).
  2. `snapshot_prepare()` — `prepare_snapshot_locked()` only (holds
     `write_mutex_` briefly).
  3. `snapshot_persist(prepared)` — page writes + fsync (NO
     `write_mutex_`).
  This lets the I/O phase overlap with the next maintenance tick's
  flush. Files: `lib/crowdb-tree/src/persist.cpp` (split `snapshot()`),
  `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` (new API),
  `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs` (call split API),
  `lib/crowdb-kv/src/cluster/group_maintenance.rs` (pipeline).

### Phase 3: `write_mutex_`-free drain (the big refactor)

- [ ] **Split `drain_memtable_into_l1_locked` into resolve + publish**:
  Phase 1 (resolve): for each entry group, descend the tree under an
  epoch guard to find the target `page_id`. No `write_mutex_`. Build the
  `BatchDelta` object (heap allocation, no tree mutation).
  Phase 2 (publish): acquire `write_mutex_` only for the
  `mapping_.store()` + `consolidate_locked()` call. Release immediately
  after. The store is a single atomic; consolidate may split/merge and
  needs the lock for the duration of the SMO only.
  Files: `lib/crowdb-tree/src/crowdb-tree.cpp`
  (`drain_memtable_into_l1_locked` → `drain_memtable_resolve` +
  `drain_memtable_publish`).
- [ ] **Re-evaluate `consolidate_locked` lock scope**: consolidation
  folds a delta chain into a fresh base and may split/merge. The fold
  itself is on a single page_id and could use a per-page lock or
  CAS-based publish instead of the global `write_mutex_`. The split/merge
  SMO touches parent pages and needs coordination — this is the hard
  part. Files: `lib/crowdb-tree/src/crowdb-tree.cpp`
  (`consolidate_locked`, `split_leaf_locked`).
- [ ] **Consider lock-free `mapping_.store` for delta publishes**: the
  design doc says "The Flusher is the sole tree writer, so mapping stores
  need no CAS." If we break the single-writer assumption (Phase 3), delta
  publishes need CAS or a per-page sequence lock. Evaluate whether the
  contention is low enough for CAS (single flusher still, just not
  holding the global lock during resolve). Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`,
  `lib/crowdb-tree/include/crowdb-tree/mapping_table.h`.

### Phase 4: Overlap snapshot I/O with drain

- [ ] **Async snapshot persist**: once `snapshot()` is split into
  prepare + persist (Phase 2), the persist phase (page writes + fsync)
  can run on a separate blocking thread concurrently with the next
  flush. The `snapshot_inflight_` spin-gate already prevents concurrent
  snapshots; we just need to not hold `write_mutex_` during persist.
  Files: `lib/crowdb-tree/src/persist.cpp`,
  `lib/crowdb-kv/src/cluster/group_maintenance.rs`.

## File List

- `lib/crowdb-tree/include/crowdb-tree/options.h` —
  `max_memtable_count` default (done).
- `lib/crowdb-tree/src/crowdb-tree.cpp` —
  `maybe_freeze_active` error log (done); `drain_memtable_into_l1_locked`
  split; `consolidate_locked` lock scope; `flush()` lock release between
  resolve and publish.
- `lib/crowdb-tree/src/persist.cpp` —
  `snapshot()` split into prepare + persist; async persist.
- `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` —
  new split snapshot API declarations.
- `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs` —
  call split snapshot API; pipeline flush + snapshot.
- `lib/crowdb-kv/src/cluster/group_maintenance.rs` —
  pipeline `run_pass`; per-replica snapshot jitter.
- `lib/crowdb-kv/src/cluster/config.rs` —
  `snapshot_threshold_jitter_pct` config field.
- `doc/design/tree/design-crowdb-tree-engine.md` —
  update §1.3 and §1.8 to reflect the split drain + multi-writer model.

## Test Checklist

### Unit (crowdb-tree)

- [ ] `double_buffer_test.cpp` — existing tests pass with
  `max_memtable_count = 5` default; add a test that verifies the error
  log fires when frozen queue is full.
- [ ] New test: `drain_memtable_resolve` produces the same page_id
  grouping as the current `drain_memtable_into_l1_locked` for a given
  memtable.
- [ ] New test: concurrent `flush()` + `apply()` (multiple writer
  threads) with the split drain — verify no lost writes, no partial
  delta chains.
- [ ] New test: `snapshot_prepare` + `snapshot_persist` split — verify
  the persisted snapshot is identical to the monolithic `snapshot()`.

### Integration (crowdb-kv)

- [ ] `bench kv` with 3-node cluster — verify no simultaneous
  `persist_snapshot` across all nodes after cluster clean (stagger
  check).
- [ ] `bench kv` — verify steady-state TPS improves and the drain tail
  shrinks from seconds to < 1s.
- [ ] Sustained write load test — verify `active_` does not grow
  unbounded when frozen queue is full (the error log fires, but memory
  stays bounded by `max_memtable_count * memtable_flush_bytes`).

### Regression

- [ ] `tools/bench-kv-scan-regression.sh` — scan throughput unaffected.
- [ ] `tools/bench-kv-read-regression.sh` — read latency unaffected.
- [ ] `tools/bench-rpc-regression.sh` — RPC throughput unaffected (no
  tree changes on the RPC path, but verify no reactor starvation).
