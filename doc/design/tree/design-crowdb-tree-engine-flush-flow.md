# Flush Flow Analysis

Flush drains frozen MemTables (L0) into the B+tree (L1) in memory. It does
not touch disk — that is snapshot's job. The `tree.flush.l` metric measures
the C++ `Crowdbtree::flush()` call only.

## Call chain

1. **Maintenance loop** (`group_maintenance.rs:194-211`) — every tick,
   `spawn_blocking(|| engine_arc.flush())`.
2. **Rust bridge** (`crowdb_tree_engine.rs:284-293`) — calls
   `handle().flush()` via FFI.
3. **C++ entry** (`crowdb-tree.cpp:1278-1337`) — `Crowdbtree::flush()`.

## What `flush()` does

```
flush():
    t0 = steady_clock::now()
    lock(write_mutex_)
    maybe_freeze_active(force=true)          // freeze active if past threshold
    for iter in 0..max_memtable_count:
        swap frozen_ -> to_drain             // under memtable_mutex_
        if to_drain.empty(): break
        cs = contiguous_slot_.load()         // re-read each pass
        active = current_active()
        wrote_any |= drain_all_frozen_locked(to_drain, active, cs)
        active->set_durable_floor(cs)
        last_applied_slot_ = cs
        version_++
    if frozen_ still non-empty: warn (iteration cap hit)
    if !wrote_any: advance last_applied_slot; observe flush_l; return
    maybe_evict_locked()                     // once, after all drains
    observe flush_l
```

The re-check loop drains memtables that freeze *during* an in-flight drain
in the same `flush()` call — `apply()` on other threads keeps writing and
may trip the freeze threshold mid-drain. Re-reading `cs` each pass lets the
next pass drain entries that became contiguous during the prior pass. Capped
at `max_memtable_count` iterations so a sustained write rate faster than
drain throughput exits cleanly (remaining tables drain on the next tick).

### `drain_all_frozen_locked` (`crowdb-tree.cpp:1137-1276`)

Three phases:

1. **Open cursors** on all frozen MemTables.
2. **K-way merge** via LoserTree. For each key run:
   - `find_leaf_page_id(root, key)` — B-tree descent.
   - `resident(page_id)` — may demand-load the leaf from disk if evicted
     (`crowdb-tree.cpp:314-378`, blocking `read_at` + decompress under
     `write_mutex_`).
   - `publish_group_to_leaf_locked(page_id, cs, group)` — append deltas
     or consolidate.
3. **Drain L0** — `mt->drain_up_to(cs)` on each frozen table.

### `publish_group_to_leaf_locked` (`crowdb-tree.cpp:1105-1135`)

Two paths:
- **In-frame delta** (fast): append deltas directly into the LeafBase
  frame if `delta_count <= max_inframe_delta`. Consolidate when the
  frame fills or `data_bytes > leaf_split_bytes`.
- **Heap delta**: build a `BatchDelta` node, prepend to the chain.
  Consolidate when `delta_len > max_delta_len` or
  `chain_bytes > max_delta_bytes`.

### `consolidate_locked` (`crowdb-tree.cpp:1347-1389`)

Folds the entire delta chain into a fresh LeafBase:
1. `resolve_leaf_chain_for_rebuild` — merge all base + delta entries into
   an `std::map` by highest-slot-wins, drop tombstones `<= gc_floor`.
2. `build_leaf_spilling_locked` — re-encode every entry, spill large
   values to overflow chains.
3. Retire old pages (delta nodes, old base, dead overflow chains).
4. `maybe_split_or_merge_locked` — may cascade splits/merges up the tree.

### `maybe_evict_locked` (`crowdb-tree.cpp:721-737`)

If buffer pool usage > 85% of frames, evict clean leaves to 70%.
`evict_clean_leaves_locked` (`crowdb-tree.cpp:555-630`) collects all
resident leaves, sorts by `last_touch_tick`, re-tags as unloaded.
This runs **inside `write_mutex_`**.

## Metric observation points

- **Start**: `crowdb-tree.cpp:1280` (`auto t0 = steady_clock::now()`)
- **End (no-op)**: `crowdb-tree.cpp:1319-1323`
- **End (normal)**: `crowdb-tree.cpp:1332-1335`
- **Registration**: `crowdb-tree.cpp:3655` (`prefix + ".flush.l"`)

## Bottleneck analysis (2026-09-01 bench)

Observed: `tree.flush.l` avg=643ms, max=1422ms. 20,026 frozen queue full
errors. The flush cannot keep up with the write rate.

### Primary causes (in order of likelihood)

1. **Synchronous demand-page loads under `write_mutex_`**. When the L1
   working set exceeds the buffer pool, `resident()` does a blocking
   `read_at` + decompress for each cold leaf. This serializes all writes
   behind the I/O. The buffer pool had `buf.resident.g=0` at the end of
   the run — everything was evicted, so every flush demand-loads.

2. **Large per-leaf consolidations**. A hot leaf receiving thousands of
   updates per flush triggers `consolidate_locked`, which builds an
   `std::map` of all entries, re-encodes them, and may split. The
   `flush.entries.c` shows 1,035,551 entries drained across 5 flushes
   (~207K entries/flush).

3. **`maybe_evict_locked` inside `write_mutex_`**. The eviction scan
   touches every resident leaf. With a large resident set this adds
   hundreds of ms to each flush.

4. **`write_mutex_` contention**. `flush()` holds the mutex for the
   entire duration. Concurrent `apply()` calls are blocked, creating
   a backlog that makes the next flush larger.

### Levers

- **Re-check loop in flush()** (implemented): drains memtables that freeze
  during an in-flight drain in the same `flush()` call, reducing the frozen-
  queue-full errors that occur when the backlog grows between flushes.
- **Increase buffer pool size** so the L1 working set stays resident.
  This eliminates demand-load stalls — the dominant cost.
- **Pre-warm leaves** before acquiring `write_mutex_`. The frozen
  MemTables know which keys they touch; pin those leaves first.
- **Move eviction out of `write_mutex_`**. Eviction can run after
  releasing the lock, or in a separate background pass.
- **Tune delta thresholds** (`max_inframe_delta`, `max_delta_len`,
  `max_delta_bytes`, `leaf_split_bytes`) to reduce consolidation
  frequency vs. cost per consolidation.
- **Parallelize the k-way merge** across leaves. Currently the merge
  is sequential; independent leaf writes could be parallelized.

## Sub-phase instrumentation gap

`tree.flush.l` is a single wall-clock summary. There are no per-phase
metrics for demand-load, merge, consolidation, split/merge, or eviction.
Adding local `steady_clock` probes and emitting per-phase times would
pinpoint which phase dominates the 643ms average.
