<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# B-tree Page Count Metrics + Persistence Plan

Goal: add O(1) leaf/inner page count gauges and a retired-page count gauge
to the tree engine, persist leaf/inner counts in the commit anchor so they
survive restart, and expose the retired count as a GC trigger signal.

## Design decisions

### Gauge type: Gauge (atomic) vs CallbackGauge

- `leaf_count_` / `inner_count_` — maintained as `std::atomic<uint64_t>`,
  exposed via `Gauge*` (atomic `set()` at SMO sites, `get()` at flush time).
  No callback overhead on the hot path.
- `retired count` — exposed via `CallbackGauge*` calling
  `epoch_.pending_retired()` (already O(1): returns `retired_.size()` under
  `reclaim_mu_`). The callback runs only at metrics flush time, not on every
  retire. No duplicate atomic to maintain.

Each metric is a separate registered instance — same pattern as the existing
`tree.height.g`, `page.map.total_pids.g`, etc.

### Persistence location: CommitAnchor (not the root page)

The `CommitAnchor` (persist.cpp) is the persistent metadata record — it
already carries `root_page_id`, `next_page_id`, `last_applied_slot`. Adding
`leaf_count` / `inner_count` here:
- Read first during `open()` recovery, before any page is loaded — counts
  available immediately without walking the tree.
- Written every snapshot, regardless of whether the root changed.
- No change to the leaf/inner frame format (frames stay pure data).
- The anchor is effectively "the btree root's persistent record." Each
  snapshot writes a new anchor (A/B alternation by seq parity).

### Format version bump

Current `kFormatVersion = 2` (clean-break, no backward compat). Adding two
`u64` fields bumps it to 3. Old snapshots won't decode — consistent with the
existing clean-break policy.

### Retired count mutex concern

`epoch_.pending_retired()` takes `reclaim_mu_` (a `recursive_mutex`). The
metrics flush thread calls it periodically. This is acceptable because:
- The flush thread is off the hot path (periodic, not per-request).
- `reclaim_mu_` is writer-side only; readers never contend.
- `retired_.size()` is O(1) — the lock is held for a vector size read.

If contention becomes measurable, a lock-free approximate count (atomic
incremented at `retire()`, decremented in `reclaim_locked()`) can replace it
later. Not needed now.

## Tasks

### Phase 1: Anchor format + persistence

- [x] **Add leaf_count/inner_count to CommitAnchor**: add two `uint64_t`
  fields to the `CommitAnchor` struct. Update `kAnchorFixedFields` (+16
  bytes). Bump `kFormatVersion` from 2 to 3. Files:
  `lib/crowdb-tree/src/persist.cpp`.
- [x] **Update encode_anchor/decode_anchor**: `put_u64`/`get_u64` the two
  new fields after `segdir_crc`, before the anchor CRC. Files:
  `lib/crowdb-tree/src/persist.cpp`.
- [x] **Persist counts in prepare_snapshot_locked**: set
  `anchor.leaf_count = leaf_count_.load(relaxed)` and
  `anchor.inner_count = inner_count_.load(relaxed)`. Files:
  `lib/crowdb-tree/src/persist.cpp`.
- [x] **Restore counts on open()**: after recovery, store the anchor's
  leaf_count/inner_count into the atomic counters. Files:
  `lib/crowdb-tree/src/persist.cpp`.

### Phase 2: Atomic counters + SMO maintenance

- [x] **Add atomic counters to Crowdbtree**: add `std::atomic<uint64_t>
  leaf_count_{0}` and `std::atomic<uint64_t> inner_count_{0}` as private
  members. Files: `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h`.
- [x] **Constructor**: set `leaf_count_.store(1)` (initial empty root leaf).
  Files: `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **split_leaf_locked**: `leaf_count_.fetch_add(1, relaxed)` (new right
  sibling leaf). Files: `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **propagate_split_locked (path.empty: new root)**:
  `inner_count_.fetch_add(1, relaxed)`. Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **propagate_split_locked (inner overflow: new right inner)**:
  `inner_count_.fetch_add(1, relaxed)`. Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **try_merge_leaf_locked**: `leaf_count_.fetch_sub(1, relaxed)` (two
  leaves → one). Files: `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **try_merge_leaf_locked (root collapse: single child)**:
  `inner_count_.fetch_sub(1, relaxed)` (root inner retired). Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **try_merge_inner_locked (merge two inners)**:
  `inner_count_.fetch_sub(1, relaxed)`. Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **try_merge_inner_locked (root collapse)**:
  `inner_count_.fetch_sub(1, relaxed)`. Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **install_snapshot**: `leaf_count_.store(1)` and
  `inner_count_.store(0)` after `free_all_resident_pages` + new root.
  Files: `lib/crowdb-tree/src/crowdb-tree.cpp`.

### Phase 3: Gauge registration + wiring

- [x] **Add gauge handles to MetricsHandles**: add `Gauge
  *tree_leaf_count_g`, `Gauge *tree_inner_count_g`, `CallbackGauge
  *tree_retired_count_g` to the `MetricsHandles` struct. Files:
  `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h`.
- [x] **Register gauges in init_metrics**: register `tree.leaf.count.g`
  (Gauge), `tree.inner.count.g` (Gauge), `tree.retired.count.g`
  (CallbackGauge with `epoch_.pending_retired()` callback). Files:
  `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **Set gauge values at SMO sites**: after each atomic counter update,
  call `metrics_.tree_leaf_count_g->set(...)` / `tree_inner_count_g->set(...)`
  if the handle is non-null. Factor into a small helper to avoid repetition.
  Files: `lib/crowdb-tree/src/crowdb-tree.cpp`.

### Phase 4: Tests

- [x] **Unit test: counter parity with tree walk**: after a sequence of
  splits and merges, verify `leaf_count_` matches `leaf_count()` (tree walk)
  and `inner_count_` matches a similar inner walk. Files:
  `lib/crowdb-tree/tests/integration/split_merge_test.cpp`.
- [x] **Persistence test: snapshot → open → verify counts**: build a
  multi-level tree, snapshot, open a new instance, verify leaf/inner counts
  restored from the anchor. Files:
  `lib/crowdb-tree/tests/integration/persist_test.cpp` or
  `crash_recovery_test.cpp`.
- [x] **Retired count test**: after a consolidation that retires pages,
  verify `epoch_.pending_retired()` > 0; after `try_reclaim()`, verify it
  drops. Files: `lib/crowdb-tree/tests/unit/consolidation_test.cpp` (extend
  existing).
- [x] **Run full split_merge + persist + consolidation suites**: verify no
  regressions. Files: `lib/crowdb-tree/build/crowdb_tree_tests`.

## File list

- `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` — add atomic counters,
  gauge handles.
- `lib/crowdb-tree/src/crowdb-tree.cpp` — maintain counters at SMO sites,
  register gauges, set gauge values.
- `lib/crowdb-tree/src/persist.cpp` — anchor format (fields + version bump),
  encode/decode, persist in snapshot, restore on open.
- `lib/crowdb-tree/tests/integration/split_merge_test.cpp` — counter parity
  test.
- `lib/crowdb-tree/tests/integration/persist_test.cpp` — persistence test.
- `lib/crowdb-tree/tests/unit/consolidation_test.cpp` — retired count test.

## Test checklist

### Unit

- [x] `Consolidation.RetiredCountAfterFold` — after a consolidation that
  retires delta chain nodes, `pending_retired()` > 0; after guard drain +
  `try_reclaim()`, drops to 0.

### Integration

- [x] `SplitMerge.LeafInnerCountParityAfterSplits` — build a multi-level
  tree via splits; verify `leaf_count_` == `leaf_count()` and
  `inner_count_` == inner walk count.
- [x] `SplitMerge.LeafInnerCountParityAfterMerges` — delete-heavy workload
  that triggers merges + root collapse; verify counts stay consistent.
- [x] `SplitMerge.ConsolidationSplitsIterativelyToThreshold` — (existing)
  verify leaf count >= 10 after the big consolidation.
- [x] `Persist.LeafInnerCountSurvivesSnapshotOpen` — build tree, snapshot,
  open new instance, verify counts restored from anchor.
- [x] `Persist.OpenEmptyTreeCounts` — fresh empty tree (no anchor): leaf=1,
  inner=0.

### Full suite

- [x] `pixi run test-tree-ct` — all C++ tests pass.
- [x] `pixi run test-tree-ffi` — FFI tests pass.

---

# Flush Re-check Loop Plan

Goal: make `Crowdbtree::flush()` drain memtables that freeze *during* an
in-flight drain in the same call, instead of leaving them for the next
`flush()` invocation. Links to design draft: none yet (inline design below,
small change). Backlog doc: none (not a backlog requirement — perf
improvement from the 2026-09-01 bench analysis in
`design-crowdb-tree-engine-flush-flow.md`).

## Problem

Current `flush()` (`crowdb-tree.cpp:1278-1337`):

1. `lock(write_mutex_)`, read `cs = contiguous_slot_`.
2. `maybe_freeze_active(force=true)` — freeze active_ into frozen_.
3. `swap frozen_ -> to_drain` under `memtable_mutex_` (frozen_ is now empty).
4. `drain_all_frozen_locked(to_drain, active, cs)` — one k-way merge.
5. Release `write_mutex_`, return.

While step 4 runs (holding `write_mutex_`), `apply()` on other threads
continues landing writes in the *new* active_. If that active_ crosses the
threshold, `maybe_swap_active()` freezes it → pushes onto the now-empty
`frozen_`. When `flush()` releases `write_mutex_`, those frozen tables sit
in `frozen_` until the *next* maintenance-loop tick calls `flush()` again.

Under sustained write load (the 2026-09-01 bench: `tree.flush.l` avg=643ms),
multiple memtables can accumulate during one drain. The next `flush()` then
drains them all in one k-way merge — but the *gap* between flushes means
frozen_ depth grows, `maybe_freeze_active` hits the `max_memtable_count` cap
(20,026 "frozen queue full" errors in the bench), and active_ grows past its
threshold unbounded.

## Design

### Re-check loop in flush()

After `drain_all_frozen_locked` completes, re-check `frozen_` under
`memtable_mutex_`. If non-empty, swap it out and drain again — in the same
`flush()` call, still under `write_mutex_`. Repeat until `frozen_` is empty
or a cap is hit.

```
flush():
    lock(write_mutex_)
    maybe_freeze_active(force=true)
    loop:
        swap frozen_ -> to_drain      // under memtable_mutex_
        if to_drain.empty(): break
        cs = contiguous_slot_.load()  // re-read: apply() may have advanced it
        active = current_active()
        wrote_any = drain_all_frozen_locked(to_drain, active, cs)
        active->set_durable_floor(cs)
        if wrote_any:
            last_applied_slot_ = cs
            version_++
    maybe_evict_locked()               // once, after all drains
    observe flush_l
```

Key points:

- **Re-read `cs` each iteration.** `apply()` runs concurrently (it does not
  take `write_mutex_`), so `contiguous_slot_` may advance between drain
  passes. Re-reading lets the second pass drain entries that became
  contiguous during the first pass — fewer leftover relocations to active_.
- **`maybe_freeze_active(force=true)` only once, before the loop.** The
  loop's job is to catch tables that froze *during* a drain, not to
  force-freeze the current active_ repeatedly (that would create tiny
  single-entry memtables and defeat double buffering). The new active_
  installed by the first freeze stays active_ across loop iterations —
  writes keep landing in it, and if it crosses the threshold,
  `maybe_swap_active()` naturally freezes it mid-loop.
- **`maybe_evict_locked()` once, after the loop.** Running it after every
  drain pass would add O(resident set) scans per pass. One eviction at the
  end is sufficient — the buffer pool pressure from multiple drains is
  bounded by the total entries published, and eviction to 70% after all
  drains is the same end state.
- **`last_applied_slot_` / `version_` per pass that wrote.** Each drain
  pass that published entries advances the durable watermark and bumps the
  version, so readers see incremental progress. A pass that wrote nothing
  (all entries were non-contiguous leftovers) does not bump — same as the
  current single-pass behavior.
- **Termination cap.** Under sustained write pressure faster than drain
  throughput, the loop could run indefinitely (each drain pass takes
  hundreds of ms, new memtables freeze during it). Cap at
  `opt_.max_memtable_count` iterations (the same bound as the frozen queue
  depth — if we've drained that many tables in one `flush()` call, the
  backlog is larger than one call can clear and the next tick should pick
  it up). Log a `warn` if the cap is hit so it's visible.

### Why not re-freeze active_ at the end?

The user's description ("when first flush finished, it will check the frozen
queue, and trigger flush all memtable together") is about draining the
*frozen queue*, not force-freezing active_. Force-freezing active_ at the
end would drain whatever partial writes are in active_ right now — but that
is the *next* tick's job (or an explicit `flush()` from
snapshot/install_snapshot). The maintenance loop already calls `flush()`
every tick; force-freezing every call creates many tiny drains → many small
per-leaf deltas → excessive consolidation (the exact problem the
`do_flush` skip logic in `group_maintenance.rs:104` avoids). The re-check
loop only catches tables that *already* froze (threshold-triggered), which
is the right set.

### Concurrency safety

- `write_mutex_` is held throughout the loop — same as today. `apply()`
  does not take `write_mutex_` (it takes `memtable_mutex_` only for the
  pointer swap in `maybe_swap_active`), so writes proceed concurrently.
- `memtable_mutex_` is taken briefly per iteration for the `swap` — same
  as today, just repeated. `maybe_swap_active`'s push and `flush()`'s swap
  are the only two writers on `frozen_`; the swap empties it, so a
  concurrent push between iterations is exactly what the re-check catches.
- `drain_all_frozen_locked` already handles leftover (slot > cs)
  relocation to active_ — re-reading cs each pass means fewer leftovers,
  but the relocation path is unchanged and still correct.

## Tasks

### Phase 1: flush() re-check loop

- [x] **Refactor flush() into a drain loop**: replace the single
  `drain_all_frozen_locked` call with a loop that re-swaps `frozen_` and
  re-drains until empty or cap hit. Re-read `cs` and `active` each
  iteration. Move `maybe_evict_locked()` and `flush_l` observation outside
  the loop. Files: `lib/crowdb-tree/src/crowdb-tree.cpp`.
- [x] **Add iteration cap + warn log**: cap at `opt_.max_memtable_count`
  iterations; emit a `warn` with `name_`, iteration count, and remaining
  `frozen_.size()` if hit. Files: `lib/crowdb-tree/src/crowdb-tree.cpp`.

### Phase 2: Doc updates

- [x] **Update flush() doc comment in crowdb-tree.h**: note the re-check
  loop — flush() now drains all memtables that freeze during the call, not
  just those frozen at entry. Files:
  `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h`.
- [x] **Update frozen_ member comment**: note that flush() drains frozen_
  to empty (modulo the cap) in one call. Files:
  `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h`.
- [x] **Update design-crowdb-tree-engine-flush-flow.md**: update the
  "What flush() does" pseudocode and add a note in the bottleneck analysis
  that the re-check loop reduces frozen-queue-full errors. Files:
  `doc/design/tree/design-crowdb-tree-engine-flush-flow.md`.

### Phase 3: Tests

- [x] **Integration test: memtables frozen during drain are drained in
  same flush()**: use a `BlockingPageStore` or small `memtable_flush_entries`
  to stall a drain mid-flight, apply enough writes to freeze a second
  memtable, unblock, verify a single `flush()` call drains both (frozen_
  is empty after, no second flush needed). Files:
  `lib/crowdb-tree/tests/integration/double_buffer_test.cpp`.
- [x] **Integration test: iteration cap warn**: set `max_memtable_count`
  small, freeze more tables than the cap during a drain, verify the loop
  exits at the cap and remaining tables stay in frozen_ for the next
  flush(). Files:
  `lib/crowdb-tree/tests/integration/double_buffer_test.cpp`.
- [x] **Run existing double_buffer + background_flush suites**: verify no
  regressions. Files: `lib/crowdb-tree/build/crowdb_tree_tests`.

## File list

- `lib/crowdb-tree/src/crowdb-tree.cpp` — refactor `flush()` into re-check
  loop, add cap + warn.
- `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` — update `flush()`
  and `frozen_` doc comments.
- `doc/design/tree/design-crowdb-tree-engine-flush-flow.md` — update
  pseudocode + bottleneck note.
- `lib/crowdb-tree/tests/integration/double_buffer_test.cpp` — two new
  tests (during-drain freeze, iteration cap).

## Test checklist

### Integration

- [x] `DoubleBuffer.FlushDrainsMemtablesFrozenDuringDrain` — a second
  memtable freezes while the first is being drained; a single `flush()`
  call drains both; `frozen_table_count() == 0` after.
- [x] `DoubleBuffer.FlushIterationCapExitsCleanly` — more memtables freeze
  than the cap; `flush()` exits at the cap; remaining tables drained by
  the next `flush()`; no data loss.

### Full suite

- [x] `pixi run test-tree-ct` — all C++ tests pass (no regressions in
  double_buffer, background_flush, split_merge, persist).

---

# Open issue: 256T:8C consensus instability (write-regression bench)

Observed during the 2026-09-02 post-implementation regression run
(`tools/bench-kv-write-regression.sh`). The `write_256t_8c_win32_coales16`
sub-test (256 threads, 8 connections, win=32, coalesce=16, rpc-workers=4)
consistently fails with consensus-layer instability. Reproduced 3 times.

## Symptoms

- `accept rejected; next step: proposer should run prepare with a higher
  ballot` — hundreds of accept rejections from `px_rpc_service`.
- `prepare rejected; next step: proposer should retry with a higher ballot`
  — repeated prepare rejections.
- `precandidate failed to gather pre-vote quorum` — pre-vote failures.
- `become_candidate` → `become_leader` cycling — leader election churn.
- `topology refresh failed: request error: builder error` — client
  transport can't reach node3 after it loses leadership.
- Run 1: bench command returned 0 despite 256 errors (ops/s=22503,
  err=256, r2=0us/r3=0us — replicas stopped responding).
- Runs 2 and 3: bench command exited non-zero, `set -euo pipefail`
  killed the script mid-256T.

## Scope

- **Not storage-engine related.** All errors are in `px_rpc_service`
  (Rust consensus) and `client_retry` (topology discovery). The C++ tree
  engine logs show no errors.
- The 512T and 1000T configs (Group B, win=64, coalesce=64) — which
  stress the flush re-check loop more — run cleanly with 0 errors and
  264K/226K ops/s (better than the ~234K reference).
- All 4 sub-tests before 256T (1T, 16T, 64T, 128T) complete with 0
  errors in every run.
- The 256T:8C config sits at the boundary between Group A (win=32) and
  Group B (win=64). The win=32 inflight window appears too small for 256
  threads, causing accept-round contention and leader churn.

## Hypothesis

The `max_inflight=32` window at 256 threads with 8 connections creates
more in-flight proposals than the window can absorb, leading to accept
rejections, ballot escalation, and election churn. The Group B config
(win=64) handles 512T/1000T without this issue. This is likely a
pre-existing tuning problem exposed by the 256T:8C config, not a
regression from the page-count-metrics or flush-re-check-loop changes.

The 128T:4C config also shows symptoms in the regression script
(r2=0us/r3=0us, co=4.3/16, WAL/node=781K vs reference 242K). A
standalone `bench-compare-128t.sh` run (fresh deploy, no prior
sub-tests, same tunables) gets 198K ops/s with co=15.75/16 and 0
errors — confirming the storage changes are fine. The regression
script's `cluster clean` → immediate bench transition leaves the
cluster unstable for high-concurrency configs.

## Next steps

- [ ] Reproduce with the pre-change binary (git stash the C++ changes,
  rebuild, run the bench) to confirm this is pre-existing.
- [ ] If pre-existing: file as a separate consensus-tuning issue (not
  blocked on the tree-count / flush-re-check work).
- [ ] Consider raising `max_inflight` for the 256T:8C config or
  documenting it as a known-limit config in the bench script.
- [ ] Add a stabilization wait after `cluster clean` in the regression
  script (e.g. sleep 2-3s for lease propagation + follower catch-up
  before starting the bench).
