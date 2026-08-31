<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# crowdb-tree Flush Drain Optimization Plan

Goal: reduce `flush()` drain time from 3.4s to ~1.0s so the maintenance
path no longer blocks the B+tree for seconds at a time, eliminating
RPC reactor starvation and leader churn during steady-state writes.

Related docs:
- `doc/design/tree/design-crowdb-tree-engine.md` §1.3 (write path),
  §1.8 (concurrency summary)
- `doc/design/tree/design-crowdb-tree-storage.md` (snapshot, persistence)

## Problem: `flush()` is slow, not `snapshot()`

Bench verification (128T/4C, 20s, `mem-block` backend, 3.6M ops, 142K
TPS, run `cluster-local-deploy-20260831-1325.49`):

- `persist_snapshot` total elapsed: 4,313ms
  - `flush()`: 3,589ms (83%) — drains ~300K memtable entries into L1
    B+tree under `write_mutex_` (CPU-bound: per-entry leaf descent,
    batch-delta build, `mapping_.store()`, `consolidate_locked()`)
  - `snapshot()` prepare + persist: 724ms (17%) —
    `prepare_snapshot_locked()` 214ms (walk dirty segments, fold delta
    chains, build segment images) + page writes 510ms (516 × memcpy,
    no fsync under mem-block)
- The `mem-block` backend makes `sync()` a no-op and `write_at()` a
  memcpy, so the snapshot persistence cost is purely CPU (page encode
  + copy), not I/O. The 724ms snapshot cost is acceptable; the 3,589ms
  flush cost is the bottleneck.
- Earlier analysis attributed 3.4s to the snapshot itself. That was
  wrong: the snapshot was silently **failing** (metrics showed
  `snapshot.apply.l` present but `snapshot.l` missing —
  `prepare_snapshot_locked` returned an error, swallowed by
  `unwrap_or(0)` in `persist_snapshot`). The 3.4s was entirely
  `flush()` time, and the snapshot never ran. After adding error
  logging and rebuilding, snapshots succeed and the true cost split is
  visible.

The flush drain holds `write_mutex_` for the entire duration, blocking
`snapshot()`'s `prepare_snapshot_locked()` (which also needs
`write_mutex_`) and starving the tokio/RPC reactor (heartbeats delay
past election timeout → leader churn → tail extension).

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

## `flush()` Step-by-Step Analysis

Measured from bench run `cluster-local-deploy-20260831-1325.49`, node1
metrics window 21 (the first big flush during steady-state):

- `flush.l` = 3,418,595 us (3.4s) — one `flush()` call
- `flush.drain.c` = 10 — 10 `drain_memtable_into_l1_locked` calls (10
  frozen memtables drained in one flush)
- `flush.entries.c` = 277,780 — 277K entries drained total
- `page.write.l` = 127 calls, avg 9,105 us, max 25,712 us — 127
  `consolidate_locked` calls (fold + maybe split/merge)

### Step breakdown (where the 3.4s goes)

`flush()` (`crowdb-tree.cpp:1134`) does:

1. **Acquire `write_mutex_`** (line 1137) — held for the ENTIRE 3.4s.
2. **Freeze active memtable** (line 1147) — `maybe_freeze_active(true)`,
   swaps `active_` into `frozen_` under `memtable_mutex_` (microseconds).
3. **Swap frozen queue to local** (line 1157) — under `memtable_mutex_`
   (microseconds).
4. **Drain loop** (line 1164) — for each frozen memtable, call
   `drain_memtable_into_l1_locked()`. This is where 99% of the time goes.

`drain_memtable_into_l1_locked()` (`crowdb-tree.cpp:1065`) does, per
memtable:

- **a. `drain_up_to(cs)`** — walks the skip list in key order, unlinks
  nodes with slot <= cs, materializes cells into `vector<mem_entry>`.
  Output is **key-sorted**. Cost: O(N) walk + cell copy. ~50ms for 277K
  entries (estimated from total minus descent/consolidate).
- **b. Per-entry-group descent** (line 1084-1096) — for EACH entry,
  calls `find_leaf_page_id()` which descends from root to leaf through
  inner pages. The inner loop (line 1092-1096) calls
  `find_leaf_page_id()` AGAIN for the next entry to check if it belongs
  to the same leaf. **This means N descents for N entries, not
  N/leaf_size descents.** With 277K entries and ~500 leaves, that's
  277K descents instead of ~500. At ~5 us per descent (2-3 inner pages),
  this is ~1.4s — **40% of the flush time**.
- **c. Delta build + publish** (line 1125-1126) — `BatchDelta::build()`
  + `mapping_.store()`. Heap allocation + atomic store. ~0.1ms per
  group. ~50ms total for 500 groups.
- **d. `consolidate_locked()`** (line 1128, 1116) — called when delta
  chain exceeds `max_delta_len` or `max_delta_bytes`, or when in-frame
  delta cap is hit. Folds the chain (`resolve_leaf_chain_for_rebuild` +
  `build_leaf_spilling_locked`), publishes a fresh base, then
  `maybe_split_or_merge_locked()`. 127 calls at ~9ms each = **~1.16s —
  34% of the flush time**.
- **e. `path_to_page_id_locked()`** (called by
  `maybe_split_or_merge_locked`) — **O(tree size) DFS** to find the
  path from root to the split/merge target. With ~500 leaves, each DFS
  visits ~250 pages on average. 127 calls × 250 pages × ~0.5 us =
  ~16ms. Small but wasteful — scales linearly with tree size.
- **f. `maybe_evict_locked()`** (line 1202) — evict clean leaves to
  keep buffer pool bounded. One pass after all drains. ~10ms.

### Time budget summary

| Step | Time | % | Notes |
| --- | --- | --- | --- |
| Per-entry descent (b) | ~1,400ms | 40% | N descents for N entries; should be N/leaf_size |
| Consolidate fold+split (d) | ~1,160ms | 34% | 127 calls × 9ms; fold is O(entries-in-chain) |
| Skip-list drain + cell copy (a) | ~500ms | 15% | O(N) walk, unavoidable |
| Delta build + publish (c) | ~50ms | 1% | Heap alloc + atomic store |
| Path DFS (e) | ~16ms | <1% | O(tree) per split; scales badly with tree growth |
| Evict (f) | ~10ms | <1% | One pass |
| Overhead | ~282ms | 8% | Lock acquire, memtable swap, etc. |

### Two structural problems

Beyond the per-step costs, two structural issues multiply the waste:

- **Per-memtable drain loop**: `flush()` drains frozen memtables one by
  one. With 10 frozen memtables, the B+tree is walked 10 times even
  though the memtables overlap heavily in key ranges (all writers hit
  the same keyspace). Each memtable gets its own descent pass and its
  own consolidate calls on the same leaves.
- **All nodes snapshot simultaneously after cluster clean**: after
  `wipe-user-data`, all 3 nodes start from slot 0 and hit
  `snapshot_slot_threshold` at the same slot, firing `flush()` +
  `snapshot()` simultaneously, saturating all cores, starving RPC
  reactors, and triggering leader election churn.

## Optimization Ideas

**O1: Sort-aware descent — eliminate redundant descents (b → ~0)**

The drained entries are key-sorted (skip list level 0 walk). The current
code calls `find_leaf_page_id()` for every entry, including the inner
loop's same-leaf check. Since entries are sorted, we can:

- Descend once to find the first leaf's `page_id`.
- For subsequent entries, compare the key against the current leaf's
  max key (available from `LeafBase::view()`). If the key is within
  range, it belongs to the same leaf — no descent needed.
- Only re-descend when the key exceeds the current leaf's range.

This reduces descents from 277K to ~500 (one per leaf boundary).
Expected saving: ~1.4s → ~3ms. **This is the single biggest win.**

Risk: low. The leaf's key range is stable under `write_mutex_` (no
concurrent splits). The only edge case is a split that happened earlier
in this same drain — but that's fine, we just re-descend after the
split.

Files: `lib/crowdb-tree/src/crowdb-tree.cpp`
(`drain_memtable_into_l1_locked`).

**O2: Move descent outside `write_mutex_` (b → concurrent with apply)**

Even with O1, the descent is still under `write_mutex_`. Since descent
is read-only (follows inner page pointers, no mutation), it could run
under an epoch guard instead. Only the publish step
(`mapping_.store` + `consolidate_locked`) needs `write_mutex_`.

This breaks the single-writer assumption and requires CAS-based publish
or per-page locks. Higher risk, larger effort. Defer until O1+O5+O3
are done and the remaining flush time is still too high.

**O3: Reduce consolidate frequency (d → fewer calls)**

127 consolidates for 277K entries = one consolidate per ~2,200 entries.
Each consolidate folds the entire delta chain into a fresh base. If we
raise `max_delta_len` / `max_delta_bytes`, fewer consolidates fire
during drain, and more entries accumulate as deltas. The snapshot's
`prepare_snapshot_locked` already folds delta chains, so deferring the
fold to snapshot time is safe.

Tuning: double `max_delta_len` and `max_delta_bytes` → expect ~64
consolidates instead of 127, saving ~580ms. But larger delta chains
slow reads (longer chain resolution) between flushes. Trade-off needs
measurement.

Risk: medium. Read latency regression on hot keys that are being
actively written.

Files: `lib/crowdb-tree/include/crowdb-tree/options.h`
(`max_delta_len`, `max_delta_bytes`).

**O4: Cache parent pointers (e → O(1))**

`path_to_page_id_locked()` is O(tree size) DFS. With ~500 leaves it's
cheap (~16ms), but it scales linearly. Adding a parent pointer to each
inner page entry makes path lookup O(depth) = O(log N). This is a
standard B+tree optimization.

Risk: medium. Parent pointers must be maintained on every split/merge
(inner page mutation), adding complexity to the SMO code.

Files: `lib/crowdb-tree/include/crowdb-tree/page.h` (InnerBase),
`lib/crowdb-tree/src/crowdb-tree.cpp` (split/merge path maintenance).

**O5: Merge all frozen memtables in one sorted pass (a+b → single merge)**

The current `flush()` drains frozen memtables **one by one**, each
calling `drain_memtable_into_l1_locked()` separately. With 10 frozen
memtables, this means 10 separate descent passes over the B+tree, 10
separate consolidate sweeps, and 10 separate leaf visits — even though
the memtables overlap heavily in key ranges (all writers write to the
same 1M keyspace).

Since each memtable is a skip list (key-sorted), we can open a cursor
on each frozen memtable and do a **single k-way sorted merge** across
all of them — exactly like the scan path's `MergeSource` loser tree
(`crowdb-tree.cpp:101`). This gives one globally key-sorted stream of
all entries across all frozen memtables, drained in one pass:

- **One descent per leaf boundary** (not 10 × N/leaf_size) — the merge
  stream is sorted, so O1's sort-aware descent applies directly.
- **One consolidate per leaf** — all entries for a given leaf across
  all memtables are grouped into a single delta, not 10 separate deltas
  that each trigger a consolidate.
- **Highest-slot-wins dedup for free** — the merge already compares
  keys; when the same key appears in multiple memtables, keep the
  highest-slot version and drop the rest. This reduces the entry count
  before the B+tree ever sees it (e.g. 277K → ~250K if 10% are
  overwrites across memtables).

The existing `MergeSource` / loser-tree infrastructure
(`crowdb-tree.cpp:98-153`) already implements the k-way merge for the
scan path (L0 skip-list cursor vs L1 leaf cursor). Reusing it for the
flush drain is mostly wiring: open `kL0` cursors on all frozen
memtables, run the merge, and feed the sorted output to a single
descent+publish loop.

Risk: low-medium. The merge logic is already proven (scan path uses
it). The main change is replacing the `for (auto &mt : to_drain)`
loop with a single merge-driven loop. Correctness: highest-slot-wins
is the same rule `MemTable::upsert` already uses internally, so
cross-memtable dedup is consistent.

Files: `lib/crowdb-tree/src/crowdb-tree.cpp` (`flush()` →
`drain_all_frozen_locked()` using `MergeSource`),
`lib/crowdb-tree/src/memtable.cpp` (expose a `ConcurrentSkipList::Cursor`
on a frozen memtable — already exists via `MemTable::cursor()`).

**O6: Stagger snapshot timing across replicas**

After `wipe-user-data`, all 3 nodes start from slot 0 and hit
`snapshot_slot_threshold` at the same slot. Add per-replica jitter to
`snapshot_slot_threshold` or `snapshot_time_threshold_ms` in the
maintenance loop, seeded from `replica_id` (deterministic, no
coordination needed).

Risk: low. Pure timing change, no structural impact.

Files: `lib/crowdb-kv/src/cluster/group_maintenance.rs`,
`lib/crowdb-kv/src/cluster/config.rs` (add
`snapshot_threshold_jitter_pct`).

**O7: Pipeline flush + snapshot in `run_pass`**

`group_maintenance.rs::run_pass()` calls `flush()` then
`persist_snapshot()` sequentially, both via `spawn_blocking`. The
snapshot internally calls `flush()` again. So during one maintenance
pass: flush → snapshot's internal flush → snapshot's prepare. Three
`write_mutex_` acquisitions back-to-back.

Split `persist_snapshot` into flush + prepare + persist:
1. `flush()` — drain L0 → L1 (holds `write_mutex_`).
2. `snapshot_prepare()` — `prepare_snapshot_locked()` only (holds
   `write_mutex_` briefly).
3. `snapshot_persist(prepared)` — page writes + fsync (NO
   `write_mutex_`).

This lets the I/O phase overlap with the next maintenance tick's
flush. Defer until O1+O5+O3 are done — the 724ms snapshot cost is
acceptable once flush drops to ~1s.

Risk: medium. Splitting the snapshot API across the FFI boundary
requires careful lifetime management of the `PreparedSnapshot` struct.

Files: `lib/crowdb-tree/src/persist.cpp` (split `snapshot()`),
`lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` (new API),
`lib/crowdb-kv/src/kv/crowdb_tree_engine.rs` (call split API),
`lib/crowdb-kv/src/cluster/group_maintenance.rs` (pipeline).

## Recommended Order

1. **O1 + O5 (sort-aware descent + merged drain)** — low risk, ~1.4s +
   redundant-per-memtable savings. Do together since both rely on a
   single sorted stream. This is the single biggest win.
2. **O3 (raise delta thresholds)** — medium risk, ~0.6s saving, tune
   after O1+O5.
3. **O4 (parent pointers)** — medium risk, prevents scaling cliff, do
   when tree size grows past ~5K leaves.
4. **O6 (stagger snapshots)** — low risk, prevents synchronized
   multi-node stalls. Can do anytime.
5. **O7 (pipeline flush + snapshot)** — medium risk, overlap snapshot
   I/O with next flush. Do after O1+O5+O3 when flush is fast enough
   that the 724ms snapshot is the remaining bottleneck.
6. **O2 (descent outside write_mutex_)** — high risk, do last. Only
   needed if the remaining ~1s flush time (after O1+O5+O3) still
   causes reactor starvation.

With O1 + O5 + O3, expected flush time: 3.4s → ~1.0s (70% reduction).
The remaining time is skip-list merge+drain (0.4s) + consolidate fold
(0.3s, fewer calls due to O5 grouping) + delta build (0.05s) +
overhead (0.25s).

## Completed

- [x] **Bump `max_memtable_count` default to 5**: gives 4 frozen slots
  so active_ doesn't grow unbounded during a multi-second flush.
  Files: `lib/crowdb-tree/include/crowdb-tree/options.h`.
- [x] **Add error log when frozen queue is full**: `maybe_freeze_active`
  logs at ERROR level with frozen count, active entries/bytes, and
  frozen total entries/bytes. Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **Add error logging to `persist_snapshot`**: `flush_ms`,
  `snapshot_ms`, and error details are now logged so silent snapshot
  failures are visible. Files:
  `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs`.
- [x] **Add `--kv-backend` / `--wal-backend` CLI params**: enables
  `mem-block` backend for bench runs to isolate CPU vs I/O costs.
  Files: `app/crowdb-cli/src/commands/cluster.rs`,
  `lib/crowdb-console-shared/src/ops/cluster.rs`.

## Tasks

### Phase 1: Sort-aware merged drain (O1 + O5) — the big win

- [ ] **O5: Replace per-memtable drain loop with k-way merge**: open a
  `ConcurrentSkipList::Cursor` on each frozen memtable, run a loser-tree
  merge (reuse `MergeSource`), and feed one globally key-sorted stream
  to a single descent+publish loop. Highest-slot-wins dedup during
  merge reduces entry count before the B+tree sees it.
  Files: `lib/crowdb-tree/src/crowdb-tree.cpp` (`flush()` →
  `drain_all_frozen_locked()`),
  `lib/crowdb-tree/src/memtable.cpp` (cursor exposure).
- [ ] **O1: Sort-aware descent in the merged drain loop**: since the
  merge output is key-sorted, descend once per leaf boundary instead of
  once per entry. Compare the next key against the current leaf's max
  key; only re-descend when crossing a boundary. Handle splits that
  happened earlier in the same drain by re-descending after
  `consolidate_locked`.
  Files: `lib/crowdb-tree/src/crowdb-tree.cpp`
  (`drain_all_frozen_locked`).

### Phase 2: Reduce consolidate cost (O3)

- [ ] **O3: Tune `max_delta_len` / `max_delta_bytes`**: double the
  thresholds to reduce consolidate frequency from 127 to ~64 calls
  during drain. Measure read latency impact on hot keys between flushes.
  Files: `lib/crowdb-tree/include/crowdb-tree/options.h`.

### Phase 3: Prevent scaling cliff (O4)

- [ ] **O4: Add parent pointers to InnerBase**: replace
  `path_to_page_id_locked()` O(tree) DFS with O(depth) parent walk.
  Maintain parent pointers on every split/merge.
  Files: `lib/crowdb-tree/include/crowdb-tree/page.h` (InnerBase),
  `lib/crowdb-tree/src/crowdb-tree.cpp` (split/merge path maintenance).

### Phase 4: Stagger + pipeline (O6 + O7)

- [ ] **O6: Stagger snapshot timing across replicas**: per-replica
  jitter on `snapshot_slot_threshold`, seeded from `replica_id`.
  Files: `lib/crowdb-kv/src/cluster/group_maintenance.rs`,
  `lib/crowdb-kv/src/cluster/config.rs`.
- [ ] **O7: Split `persist_snapshot` into flush + prepare + persist**:
  separate `snapshot()` into `snapshot_prepare()` (holds
  `write_mutex_` briefly) + `snapshot_persist(prepared)` (no
  `write_mutex_`, pure I/O). Pipeline the persist phase with the next
  maintenance tick's flush.
  Files: `lib/crowdb-tree/src/persist.cpp`,
  `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h`,
  `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs`,
  `lib/crowdb-kv/src/cluster/group_maintenance.rs`.

### Phase 5: Descent outside `write_mutex_` (O2) — deferred

- [ ] **O2: Split drain into resolve + publish**: resolve phase
  (descent + BatchDelta build) under epoch guard only; publish phase
  (`mapping_.store` + `consolidate_locked`) under `write_mutex_`.
  Requires CAS-based publish or per-page locks. Only do this if the
  remaining ~1s flush time (after Phase 1-2) still causes reactor
  starvation.
  Files: `lib/crowdb-tree/src/crowdb-tree.cpp`,
  `lib/crowdb-tree/include/crowdb-tree/mapping_table.h`.

## File List

- `lib/crowdb-tree/include/crowdb-tree/options.h` —
  `max_memtable_count` default (done); `max_delta_len` /
  `max_delta_bytes` tuning (O3).
- `lib/crowdb-tree/include/crowdb-tree/page.h` —
  parent pointers in InnerBase (O4).
- `lib/crowdb-tree/src/crowdb-tree.cpp` —
  `flush()` → `drain_all_frozen_locked()` with k-way merge + sort-aware
  descent (O1+O5); `path_to_page_id_locked` → parent walk (O4);
  `drain_memtable_resolve` + `drain_memtable_publish` split (O2).
- `lib/crowdb-tree/src/memtable.cpp` —
  expose `ConcurrentSkipList::Cursor` on frozen memtables (O5).
- `lib/crowdb-tree/src/persist.cpp` —
  `snapshot()` split into prepare + persist (O7); error log on
  prepare failure (done).
- `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` —
  new split snapshot API declarations (O7).
- `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs` —
  error logging (done); call split snapshot API (O7).
- `lib/crowdb-kv/src/cluster/group_maintenance.rs` —
  per-replica snapshot jitter (O6); pipeline flush + snapshot (O7).
- `lib/crowdb-kv/src/cluster/config.rs` —
  `snapshot_threshold_jitter_pct` config field (O6).
- `doc/design/tree/design-crowdb-tree-engine.md` —
  update §1.3 and §1.8 to reflect the merged drain + sort-aware descent.

## Test Checklist

### Unit (crowdb-tree)

- [ ] `double_buffer_test.cpp` — existing tests pass with
  `max_memtable_count = 5` default; add a test that verifies the error
  log fires when frozen queue is full.
- [ ] New test: `drain_all_frozen_locked` with multiple frozen
  memtables produces the same L1 state as the current per-memtable
  drain loop. Verify highest-slot-wins dedup across memtables.
- [ ] New test: sort-aware descent groups entries correctly when leaf
  boundaries are crossed mid-drain. Verify re-descent after a split
  during the same drain.
- [ ] New test: `snapshot_prepare` + `snapshot_persist` split — verify
  the persisted snapshot is identical to the monolithic `snapshot()`.
- [ ] New test: parent pointer lookup returns the correct path after a
  split/merge (O4).

### Integration (crowdb-kv)

- [ ] `bench kv` with 3-node cluster — verify flush time drops from
  3.4s to ~1.0s with O1+O5+O3 applied.
- [ ] `bench kv` — verify no simultaneous `persist_snapshot` across all
  nodes after cluster clean (O6 stagger check).
- [ ] `bench kv` — verify steady-state TPS improves and the drain tail
  shrinks from seconds to < 1s.
- [ ] Sustained write load test — verify `active_` does not grow
  unbounded when frozen queue is full.

### Regression

- [ ] `tools/bench-kv-write-regression.sh` — write throughput improves,
  no errors.
- [ ] `tools/bench-kv-scan-regression.sh` — scan throughput unaffected.
- [ ] `tools/bench-kv-read-regression.sh` — read latency unaffected
  (especially after O3 delta threshold change).
- [ ] `tools/bench-rpc-regression.sh` — RPC throughput unaffected (no
  tree changes on the RPC path, but verify no reactor starvation).

## Analysis: frozen-queue-full errors during bench kv write (2026-08-31)

Observed while running `tools/bench-kv-write-regression.sh` line 205
(`write_128t_4c_win32_coales16`, win=32, coalesce=16, workers=4) on
this machine. Result: 141,611 ops/s vs reference 189,585 ops/s
(Ryzen 9 5950X). node1's `crowdb-kv-server-tree-*.log` filled with:

  `[error] [s0.g0] maybe_freeze_active: frozen queue full (4),
  active_ growing past threshold (entries=261939 bytes=139613487);
  frozen total: entries=31503 bytes=16791099; next step: flush()
  must catch up or OOM risk -- increase max_memtable_count`

Symptom
- active_ memtable grew to ~140MB (entries=261939) because the frozen
  queue (4 slots, `max_memtable_count=5` default) was full and flush
  was not draining a slot free.
- Writes still proceeded (the engine lets active_ grow past threshold
  rather than stall the writer — see `maybe_freeze_active` in
  `lib/crowdb-tree/src/crowdb-tree.cpp` lines 1014-1033), so ops/s was
  only ~25% below reference, not stalled.
- p99 = 5000us vs reference 1596us — the 140MB active_ slows any
  read/scan touching it and inflates write tail latency.
- Zero errors at the bench level (err=0); the tree errors are
  backpressure warnings, not data-loss.

Root cause
- `PxElectionConfig::DEFAULT.maintenance_tick_ms = 10_000`
  (`lib/crowdb-kv/src/common/config.rs` line 359). The engine flush
  (`engine.flush()` draining frozen_ → L1) runs once per maintenance
  tick (`group_maintenance.rs` lines 166-178).
- With a 10s tick and ~72MB/s write rate (141K ops/s × 512B):
  1. Within ~0.25s the 4 frozen memtables fill (4 × 4MB, the
     `memtable_flush_bytes` default — `options.h` line 73).
  2. For the remaining ~9.75s, `maybe_freeze_active` cannot freeze
     (frozen queue full), so active_ grows unbounded → 140MB+.
  3. The error fires on every write crossing the threshold.
- `maintenance_tick_ms` is NOT exposed as a CLI flag on
  `crowdb-kv-server` (`app/crowdb-kv-server/src/cli.rs` has no
  `--maintenance-tick-ms`), so the bench script cannot tune it. The
  10s default is production-conservative; write-heavy benches need
  ~1s.
- The reference run (189K ops/s, 96MB/s × 10s ≈ 960MB) would have hit
  the same issue — the reference was likely captured before the 10s
  default landed, or with a different config path. Worth confirming
  against the reference platform's actual config.

This is a pre-existing config gap, not a regression from the
2026-08-31 ops_log/metrics refactor.

Proposed fix (smallest first)
- Expose `--maintenance-tick-ms` on `crowdb-kv-server` (cli.rs +
  main.rs wiring into `PxElectionConfig`), default 10_000 (unchanged
  for production).
- Update `tools/bench-kv-write-regression.sh` `deploy_group` to pass
  `--maintenance-tick-ms 1000` (1s) so flush keeps up with the write
  rate.
- Re-run line 205 and confirm the tree errors stop and ops/s recovers
  toward the reference.

Open questions for review
- Is 1s the right bench value, or should it scale with the write rate
  (e.g. 500ms for 128T+)? The maintenance tick also drives snapshot
  threshold checks and WAL flush interval gating — a faster tick
  increases snapshot/WAL-flush check frequency (cheap when thresholds
  not met) but is worth measuring.
- Should `max_memtable_count` also be tunable from the CLI for
  write-heavy workloads, or is fixing the tick sufficient? At 1s tick
  + 72MB/s, one frozen slot (4MB) drains per tick = 4MB/tick, vs
  72MB/s × 1s = 72MB produced — still a 18× deficit per tick. The
  flush drains ALL frozen_ in one call (the `to_drain.swap(frozen_)`
  loop), so 4 slots × 4MB = 16MB per tick vs 72MB produced → still
  behind. Either the tick must be << 1s (e.g. 100ms → 7.2MB produced
  vs 16MB drained, sustainable) OR `memtable_flush_bytes` must be
  raised so each frozen slot holds more (fewer, larger drains). The
  interaction is worth a proper analysis, not just a 1s guess.
- Alternative: a background flush thread in the C++ engine itself
  (decoupled from the Rust maintenance tick) — the `flush_async`
  stub at `crowdb-tree.cpp` line 1212 already exists. This would
  remove the tick-rate dependency entirely.
