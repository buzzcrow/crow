<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R122: kv — Rust mutex/lock review fixes

A full review of all mutex/`RwLock`/`parking_lot`/`tokio::sync` lock
usage across the Rust crates (75 production files scanned: `Mutex`,
`RwLock`, `parking_lot::`, `tokio::sync::{Mutex, Semaphore, Notify}`,
`Condvar`, `Barrier`, plus `Atomic*`/`OnceLock`/`arc_swap` for lock-free
patterns) found 3 critical hot-path findings, 7 medium findings, and
~30 correct patterns. The codebase leans heavily on lock-free patterns
(`DashMap`, `Atomic*`, `OnceLock`, `arc_swap`, `tokio::sync::Notify`,
per-bit CAS) and reserves mutexes for rare or read-heavy paths, so the
findings are fewer and milder than the C++ review (R121). This item
tracks the fixes; the OK findings document notably good lock-free
design and need no action.

**Current behavior + impact:**
- The highest-impact finding is `PxLearner::out_of_order` — a
  `parking_lot::Mutex<BTreeMap<SlotIndex, PxTerm>>` taken on every
  accepted slot on a follower (via `handle_accept_inner` /
  `BatchChosen` / `handle_heartbeat`) for the chosen-frontier advance.
  Under out-of-order delivery (multi-pipeline WAL, batched accepts),
  concurrent frontier advances serialize on this single lock. The
  `last_chosen_term` fence also takes the lock purely to fence an
  atomic store, adding lock acquisitions even on the in-order fast
  path.
- `WalEngine::index` — `Arc<parking_lot::Mutex<SegmentIndex>>` held
  during every flush batch's index insert loop. With multiple pipelines
  (multi-disk WAL), flush completions from different disks serialize;
  GC and replay also take this lock, so a GC pass can stall pipeline
  flushes. Append path itself is lock-free (only flush throughput is
  impacted).
- `ClientMetrics::window_lat` — `std::sync::Mutex<WindowLatency>`
  taken on every client RPC completion (`record_put_latency` /
  `record_get_latency` / etc.). On a high-QPS multi-threaded client,
  all completion paths serialize on this single mutex, and the
  `PreciseHistogram::record` bucket scan makes the critical section
  non-trivial.
- `PxGroup::coalescer` — `Mutex<Option<PendingBatch>>` on every
  propose (acceptable — short critical section, fair `parking_lot`
  mutex).
- `PxGroup::peer_applied`/`peer_durable` — two `Mutex<HashMap>` updated
  on every heartbeat reply (low priority — small replica sets, bounded
  contention).
- `PxLocalReplica::gap_slots` — `Mutex<BTreeSet>` on the apply loop
  (low priority — gaps are bounded and drained continuously).
- `RangeBindingClient::bindings` — `RwLock<Vec>` read on every route
  (medium — could use `arc_swap` for lock-free reads, matching the
  `watch_registry` pattern).
- `DdbDiskGroup::allocating_disks` — `RwLock<Arc<AllocateDiskContext>>`
  read on every allocate (low priority — RCU pattern, critical section
  is one `Arc::clone`).
- `MetricsRunner` + `engine_collector` — up to 6 mutexes per flush,
  with the collector callback running inside `registry.lock()` so any
  new metric registration blocks for the whole collector run (medium —
  same root cause as C++ R121 finding #4).
- `PxLocalReplica::election_state` — `Mutex` taken for locked reads on
  the status path where atomic snapshots would suffice (low priority —
  atomic mirrors exist for the hot path).

**Design pointers:**
- `design/kv/design-crowdb-kv-consensus.md` §5 (Learner — chosen-frontier
  and apply-frontier advance, the `PxLearner::out_of_order` finding).
- `design/kv/design-crowdb-kv-wal.md` — WAL pipeline + index design (the
  `WalEngine::index` finding).
- `design/kv/design-crowdb-kv-observability.md` — metrics runner/collector
  design (the `MetricsRunner` finding, shared root cause with R121 #4).
- `design/chunkdb/design-crowdb-chunkdb-range-binding.md` §5 (the
  `RangeBindingClient` finding — `arc_swap` pattern reference).

**Use scenarios:**
- A follower receiving out-of-order accepts from a multi-pipeline WAL:
  concurrent `update_chosen_frontier` calls serialize on the
  `out_of_order` mutex, capping follower apply throughput. Expected
  after fix: out-of-order applies proceed concurrently; the
  `last_chosen_term` fence uses a CAS loop, not a mutex.
- A multi-disk WAL with N pipelines flushing concurrently: flush
  completions serialize on the `index` mutex; a GC pass stalls all
  pipeline flushes. Expected after fix: per-pipeline sharded index or
  lock-free concurrent map; GC does not stall flushes.
- A high-QPS multi-threaded client recording latency on every RPC:
  all completion paths serialize on `window_lat`. Expected after fix:
  per-thread histograms drained into the window on flush, or a
  lock-free histogram.
- A chunk-write client routing every write through
  `RangeBindingClient::route`: read lock on every route even though the
  table is rarely refreshed. Expected after fix: `arc_swap` for
  lock-free reads (matches `watch_registry` pattern).
- A metrics flush running the engine collector inside
  `registry.lock()`: a concurrent metric registration blocks for the
  whole collector run. Expected after fix: collector runs before
  taking `registry.lock()` (collect into a local buffer, then lock +
  apply).

## Solution

**One-line summary:** Move the learner frontier advance off the mutex
(CAS fence + sharded/deferred map), shard the WAL index, use per-thread
latency histograms, convert `RangeBindingClient` to `arc_swap`, and
collect metrics before taking the registry lock; lower-priority findings
are documented with recommended approaches for later.

1. **`PxLearner::out_of_order` — lock-free frontier** —
   `lib/crowdb-kv/src/paxos/learner.rs` lines 89, 344, 373, 404, 413.
   Replace the `last_chosen_term` mutex fence (lines 373/404) with a
   `compare_exchange` loop on a packing atomic. For the frontier drain,
   use a lock-free skip list or a sharded map (by `slot % N`), or defer
   the out-of-order map insert to a background drain since
   `contiguous_chosen` is the fast-path check. Medium-high priority if
   follower apply throughput is a bottleneck.

2. **`WalEngine::index` — sharded index** —
   `lib/crowdb-kv/src/wal/pipeline_writer.rs` lines 447, 655;
   `lib/crowdb-kv/src/wal/wal_engine.rs` line 82. Shard the index (one
   mutex per pipeline, merged on read) or use a lock-free concurrent
   map. Lower priority than #1 since append is lock-free; revisit if
   multi-disk flush throughput is bounded by index contention.

3. **`ClientMetrics::window_lat` — per-thread histograms** —
   `lib/crowdb-kv-client/src/metrics.rs` lines 161, 166–168. Use
   per-thread histograms drained into the window on flush (thread-local
   accumulation), or a lock-free histogram. Medium priority for
   high-QPS multi-threaded clients.

4. **`RangeBindingClient::bindings` — `arc_swap`** —
   `lib/crowdb-kv-client/src/range_binding.rs` line 98. Convert
   `RwLock<Vec<ChunkdbRangeBinding>>` to
   `arc_swap::ArcSwap<Vec<ChunkdbRangeBinding>>` for lock-free reads
   (refresh = swap to a new `Arc<Vec>`). Matches the `watch_registry`
   pattern already used in the learner.

5. **`MetricsRunner` — collect before lock** —
   `lib/crowdb-common/rust/src/metrics/runner.rs` lines 116–134;
   `app/crowdb-kv-server/src/engine_collector.rs` lines 135–220. Run the
   collector before taking `registry.lock()` (collect into a local
   buffer, then lock + apply). Same fix as C++ R121 finding #4.

6. **`PxGroup::coalescer` — acceptable, low priority** —
   `lib/crowdb-kv/src/cluster/group.rs` line 244. Acceptable as-is (short
   critical section, fair mutex). If profiling shows contention,
   consider an MPSC channel into the coalescer.

7. **`PxGroup::peer_applied`/`peer_durable` — low priority** —
   `lib/crowdb-kv/src/cluster/group.rs` lines 145, 157. A `DashMap` or
   per-peer atomic `SlotIndex` would eliminate the lock, but the gain
  is marginal at typical replica-set sizes (3–7).

8. **`PxLocalReplica::gap_slots` — low priority** —
   `lib/crowdb-kv/src/cluster/local_replica_apply.rs` lines 117, 147,
   167, 347. A `DashMap<SlotIndex, ()>` or lock-free bounded ring would
   reduce contention under heavy gap load, but the steady-state path
   is fine.

9. **`DdbDiskGroup::allocating_disks` — low priority** —
   `app/crowdb-diskdb/src/model/disk_group.rs` lines 144, 195. Convert
   to `arc_swap::ArcSwap<AllocateDiskContext>` for truly lock-free
   reads. Low priority since the critical section is one `Arc::clone`.

10. **`PxLocalReplica::election_state` — low priority** —
    `lib/crowdb-kv/src/cluster/local_replica.rs` lines 555, 561, 567,
    587, 659, 667, 671, 675. Audit callers of `current_term()` /
    `voted_for()` and switch to `current_term_snapshot()` where a stale
    snapshot is safe (observations, logging).

**Edge cases at a glance:**
- `PxLearner` CAS fence on `last_chosen_term` → must handle the
  ABA case where term + slot wrap; a packing atomic `(term, slot)` with
  CAS avoids it.
- Sharded WAL index merged on read → a read spanning pipelines must
  see a consistent snapshot (merge all shards under no lock, or accept
  eventual consistency for the index — replay already handles gaps).
- Per-thread latency histograms → the flush drain must handle a thread
  that exits between samples (drop its local buffer or drain on exit).
- `arc_swap` on `RangeBindingClient` → a route in flight during a
  refresh sees the old `Arc` (safe — `Arc` keeps it alive); no
  torn read.
- Metrics collector before lock → the local buffer must be bounded
  (don't allocate unbounded memory between collect and apply).

## Dependencies

- None — all findings are in landed code. The learner fix is
  self-contained within `crowdb-kv`; the WAL index fix is self-contained
  within `crowdb-kv`; the client metrics fix is self-contained within
  `crowdb-kv-client`; the `RangeBindingClient` fix is self-contained
  within `crowdb-kv-client`. No cross-component ordering.
- R121 (C++ lock review) shares the same `MetricsRunner`/`MetricsRegistry`
  collector pattern (Rust `MetricsRunner` holds `registry.lock()` during
  the collector callback — same root cause as C++ R121 finding #4); the
  two can be fixed independently.

## Acceptance

**PxLearner frontier (work item 1):**
- `last_chosen_term` fence uses a CAS loop, no mutex acquisition on the
  in-order fast path (verify via a test that counts mutex acquisitions
  on the in-order path → 0). Unit test.
- Concurrent out-of-order `update_chosen_frontier` calls do not
  serialize on a single lock (two threads, measure parallelism vs.
  serialized baseline). Unit test.
- Chosen-frontier correctness preserved: `contiguous_chosen` advances
  correctly under out-of-order delivery (existing learner tests pass).
  Integration test.

**WalEngine index (work item 2):**
- Multi-pipeline flush completions do not serialize on a single index
  mutex (N pipelines, measure parallel flush throughput vs. serialized
  baseline). Unit test.
- GC pass does not stall pipeline flushes (start a GC pass, verify
  flushes continue). Integration test.
- Index correctness preserved: replay finds all records after sharded
  index (existing WAL tests pass). Unit test.

**ClientMetrics (work item 3):**
- High-QPS multi-threaded client: latency recording does not serialize
  on `window_lat` (N threads, measure contention vs. baseline). Unit
  test.
- Window latency values are correct after flush drain (per-thread
  buffers merged correctly). Unit test.

**RangeBindingClient (work item 4):**
- `route()` takes no lock (verify via a test that routes N times
  concurrently with a contention probe → no lock contention). Unit
  test.
- Refresh during a route in flight → route sees a consistent
  (old or new) `Arc<Vec>`, no torn read. Unit test.

**MetricsRunner (work item 5):**
- Collector callback runs outside `registry.lock()` (verify via a test
  that registers a metric during a flush → no blocking on the
  collector). Unit test.
- Flush output unchanged (same metrics reported, same order tolerance).
  Unit test.

**Lower-priority items (work items 6–10):**
- Documented as deferred — no acceptance bullets required unless
  implemented. If implemented, add per-item unit tests as above.

**All items:**
- `pixi run test-kv-core` passes (existing tests + new tests).
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
