# todo_perf.md — Performance Observer Mechanism

Context: bench `write_128t_4c_win32_coales16` showed 124K ops/s (34% below
189K reference). Root cause: sequential maintenance loop (flush -> snapshot ->
WAL flush -> GC) blocks flushes during 1.6-2.6s snapshots, causing frozen
queue full errors (81K) and active memtable growth.

Before fixing the maintenance loop, we need better observability to
characterize the stall behavior. This file tracks metrics/log refinements.

## Pending — Complex (needs refinement)

### P0: Remove legacy duplicate metrics system

**Problem**: Two parallel metrics systems track the same data at the same
call sites. Every increment/observe hits both, adding unnecessary atomic
ops on hot paths and doubling the mental overhead when reading code.

**Legacy system** (hand-rolled `AtomicU64` in `common/metrics.rs`):
- `LayerMetrics` — `rpc_count`, `err_count`, `last_rtt_ns` per remote
  replica. Read via `MetricsSnapshot`, exposed through the management API
  `/status` endpoint. NOT flushed to metrics log files.
- `ElectionMetrics` — `election_count`, `step_downs_*` per local
  replica. Read via `ElectionMetricsSnapshot`, exposed through the
  management API. NOT flushed to metrics log files.

**New system** (`MetricsRegistry` + `Counter`/`LatencySummary`/`Gauge`):
- Registered handles on `PxGroup` / `PxLocalReplica`, flushed to metrics
  log files. Covers everything the legacy system tracks plus much more.
- Election: `paxos.elections.c`, `paxos.step_downs.*.c`
- RPC: `s.X.g.X.rpc.l@N` (per-peer latency summaries)

**Duplication map** (legacy -> new):
- `LayerMetrics.rpc_count` -> `s.X.g.X.rpc.l@N` count
- `LayerMetrics.err_count` -> (no direct equivalent yet — see note)
- `LayerMetrics.last_rtt_ns` -> `s.X.g.X.rpc.l@N` max/avg
- `ElectionMetrics.election_count` -> `paxos.elections.c`
- `ElectionMetrics.step_downs_higher_term` -> `paxos.step_downs.higher_term.c`
- `ElectionMetrics.step_downs_lease_unrenewable` -> `paxos.step_downs.lease.c`
- `ElectionMetrics.step_downs_admin` -> `paxos.step_downs.admin.c`

**Note**: `err_count` has no registry equivalent. Options:
1. Add `paxos.rpc.err.c` per remote replica to the registry
2. Keep `err_count` as the only legacy field (minimal, not worth removing)

**Plan**:
1. Add `paxos.rpc.err.c` to registry (if choosing option 1)
2. Make management API `/status` read from the registry instead of legacy
   structs — need `MetricsRegistry::get_counter(name)` or snapshot lookup
3. Remove `LayerMetrics`, `ElectionMetrics`, `ElectionMetricsSnapshot`,
   `ElectionMetricsCounters` from `common/metrics.rs`
4. Remove `election_metrics()` / `election_metrics_snapshot()` from
   `PxLocalReplica`, replace with registry reads
5. Remove `LayerMetrics` from `RemoteReplica`, replace with registry reads
6. Remove `record_ok` / `record_err` / `record_step_down_*` methods
7. Update `status.rs` to build `MetricsSnapshot` from registry
8. Update tests that assert on `MetricsSnapshot` / `ElectionMetricsSnapshot`

**Risk**: The management API proto (`MetricsSnapshot`, `ElectionMetricsSnapshot`)
is consumed by `crowdb-cli` and the console. Either keep the proto shapes
and fill them from the registry, or update all consumers.

### P1: Structured log context (s/g/leader/replica prefix)

**Problem**: Server logs don't consistently carry store/group/replica/leader
identity. The user wants every log from a store/group/replica context to
include `s=X g=X leader=X replica=X` so the role of the current runner is
visible at a glance.

**Current state**:
- ~210 tracing macro call sites in `lib/crowdb-kv/src/` (47 info, 41 warn,
  37 error, 77 debug, 8 trace)
- No existing tracing spans (`#[instrument]`, `span!`, `tracing::Span`,
  `.instrument()` — all absent in the entire repo)
- Identity is added manually per call site: `group_id`, `replica_id`,
  `store_id`, `replica_l_id` — inconsistent field names
- `tracing-subscriber` fmt layer does not render span fields (no
  `with_span_events` configured)
- Client library (`crowdb-kv-client`) has its own `store_id`/`group_id`
  fields for topology/watch, no spans

**Approach options**:

1. **Tracing spans** (preferred long-term):
   - Introduce `#[instrument]` or `span!()` at key async task entry points:
     - `group_maintenance::maintenance_loop` (has `group_id`, `replica_id`)
     - `local_replica_apply::apply_loop` (has `replica_id`, `group_id`)
     - `PxGroup::start` / `PxGroup::run` (has `group_id`)
     - RPC handlers in `px_rpc_service` / `kv_rpc_service` (has `store_id`)
   - Span fields: `s = store_id, g = group_id, replica = replica_id,
     leader = is_leader`
   - Configure `crowdb-common/rust/src/logging.rs` fmt layer to render
     span context (e.g. `with_span_events(FmtSpan::ACTIVE)` or custom
     format that prepends span fields to each event)
   - Challenge: span propagation across `tokio::spawn` boundaries requires
     `tracing::Instrument` on every spawn site
   - Challenge: `is_leader` changes over time (elections), so it must be
     read at span creation or updated via span reconfiguration

2. **Manual fields** (simpler, more tedious):
   - Add `s`, `g`, `leader`, `replica` fields to each of ~210 call sites
   - Requires threading context (store_id, group_id, replica_id, is_leader)
     into every struct/function that logs
   - High risk of missing sites, inconsistent field names

**Refinement needed**:
- Decide span vs manual approach
- If span: identify all `tokio::spawn` sites that need `.instrument(span)`
- If span: design the subscriber format (how to render span fields compactly)
- If span: handle `is_leader` changing (re-create span on election?)
- Consider a hybrid: spans for the outer loop, manual fields for one-off
  logs in functions that don't have span context

### P2: Maintenance loop observability (deferred from bench analysis)

**Problem**: The maintenance loop runs sequentially: flush -> snapshot ->
WAL flush -> GC. Long snapshots (1.6-2.6s) block flushes, causing frozen
queue full errors. We need logs to characterize each phase's duration
and the queue depth at entry/exit. Per-phase latency histograms are not
needed at this level — `tree.flush.l` already covers the storage-engine
flush call, and logs suffice for one-off stall characterization.

**Needed metrics**:
- Frozen memtable count + live records gauge: see P3
  (`tree.mt.frozen.g` / `tree.mt.records.g`) — defined there as tree-engine
  metrics; P2 consumes them to characterize the stall

**Needed logs**:
- Maintenance loop: log each phase start/stop with elapsed_ms
- Frozen queue full: log when queue is full with current depth + active
  memtable entries
- Snapshot trigger: log which threshold triggered (time/slot/flush_count)

**Refinement resolved**:
- Verified: the two memtable gauges don't exist yet (see P3).
- `tree.flush.l` exists and covers the storage-engine `Crowdbtree::flush()`
  call (draining frozen memtables to L1/pages). No separate
  maintenance-loop flush-phase histogram needed — logs cover it.
- Gauge naming owned by P3 (`tree.mt.frozen.g` / `tree.mt.records.g`).

### P3: Tree metrics refactor (3-layer gap review)

**Status**: Design doc written. Code partially done (backend label split,
scan l0/snapshot+l0/skip removal, scan l1.descent+l1.resolve merge,
apply.l/snapshot.l removal). Remaining work below, organized by the
three engine layers: tree operations, page mapping table, backend I/O.

**Naming**: Every tree metric is registered with a per-group prefix
`make_metrics_prefix(opt)` = `s.{store_id}.g.{group_id}.tree`, so the
full metric name in the log is `s.X.g.Y.tree.<rest>`. The names below
drop the `s.X.g.Y.` prefix for brevity; each one is scoped to its own
store/group at registration time. A tree belongs to exactly one group,
so per-group status is already distinguishable — no extra plumbing
needed.

**Done** (all layers):
- Backend label split: logical metrics use `tree.*`, I/O metrics use
  `tree.{backend}.*`
- Removed `tree.{backend}.apply.l` (duplicate of `paxos.learn.apply.l`)
- Removed `tree.{backend}.snapshot.l` wrapper (sub-phases are more useful)
- Removed `tree.{backend}.scan.l0.snapshot.l` (always 0us, cursor not copy)
- Removed `tree.{backend}.scan.l0.skip.l` (hardcoded 0, dead since R50)
- Merged `scan.l1.descent.l` + `scan.l1.resolve.l` -> `scan.l1.l`
- Brought back `tree.snapshot.l` as a logical metric (no backend label)

---

#### Layer 1 — Tree operations (B-tree behavior)

Existing: `mt.upsert.c`, `mt.get.c`, `mt.get.hit.c`, `l1.get.c`,
`l1.get.hit.c`, `flush.l`, `flush.drain.c`, `flush.entries.c`,
`page.write.l` (consolidate wall time), `scan.c`, `scan.entries.c`,
`scan.l`, `scan.l1.l`, `scan.merge.l`.

**Renames**:
- `tree.mt.upsert.c` -> `tree.mt.apply.c` (covers put + del, not just upsert)

**New metrics**:
- `tree.mt.apply.l` — memtable upsert batch latency
- `tree.mt.get.l` — memtable get latency (L0 lookup across all live tables)
- `tree.mt.frozen.g` — frozen memtable count (also consumed by P2 to
  characterize maintenance stall)
- `tree.mt.records.g` — total live records across all memtables (also
  consumed by P2)
- `tree.mt.freeze.c` — memtable freeze count (backpressure / write pacing)
- `tree.l1.get.l` — L1 get latency (B-tree descent + leaf chain resolve)
- `tree.page.split.c` — leaf split count (SMO churn → write amplification)
- `tree.page.merge.c` — leaf merge count
- `tree.page.consolidate.c` — consolidation count (pairs with
  `page.write.l` which already times the consolidate wall time)
- `tree.tree.height.g` — tree height gauge (sampled on root change;
  characterizes read amplification and structural growth)
- `tree.scan.retry.c` — scan async retry count (cold-page stall → tail
  latency)
- `tree.gc.tombstones.c` — GC reclaimed tombstone count (from `GcStats`)
- `tree.gc.pages.c` — GC reclaimed page count

**Considered but skipped**:
- `apply.c` / `put.c` / `del.c` (op-level counters) — `paxos.learn.apply.l`
  count already gives ops/s at the consensus layer; `mt.apply.c` counts
  per-key. Redundant.
- `tree.get.retry.c` — get async retry; lower priority than scan retry
  (get is point lookup, scan is long-running and more stall-prone).
- `tree.mt.freeze.{reason}.c` (per-reason freeze counters) — total
  `mt.freeze.c` + logs suffice; per-reason split is over-instrumented.
- Open/recovery timing (`open.l`) — one-shot at startup; a log line
  suffices, no need for a registry metric.
- EBR reclaim counter — GC counters cover the user-facing concern;
  EBR internals are not operator-visible.

---

#### Layer 2 — Page mapping table operations

Existing: `page.map.lookup.c` (bumped in `resident()` on every page
address resolution).

**Renames**:
- `tree.{backend}.page.map.lookup.c` -> `tree.{backend}.page.find.c`
- Merge `tree.{backend}.buf.hits.c` + `tree.{backend}.buf.misses.c` into
  `tree.{backend}.page.find.c` + `tree.{backend}.page.find.hit.c`
  (page.find.c = total page lookups, page.find.hit.c = buffer pool
  hits; hit rate = hit / find)

**New metrics**:
- `tree.page.map.alloc.c` — page ID allocations (hot path, tree growth
  rate; called on every new leaf/inner/overflow page, split, merge)
- `tree.page.map.total_pids.g` — `next_page_id_` gauge (total pages ever
  allocated — cumulative growth indicator)
- `tree.page.map.segments.g` — segments allocated gauge (mapping table
  memory growth)

**Considered but skipped**:
- `page.map.store.c` / `page.map.clear.c` (mapping table mutations) —
  redundant with `page.write.c` for mutation pressure.
- `page.map.live.g` (live page count) — O(num segments) to compute;
  `total_pids.g` minus `gc.pages.c` gives the same signal cheaper.
- `page.map.seg.free.c` (segment recycling) — niche; segment gauge
  covers growth.

---

#### Layer 3 — Backend I/O engine

Existing: `buf.hits.c`, `buf.misses.c`, `buf.evictions.c`,
`buf.writebacks.c`, `buf.resident.g`, `buf.dirty.g`, `demand.load.l`,
`page.read.bw`, `snapshot.apply.l`, `snapshot.page.write.io.l`,
`snapshot.page.write.cache.c`, `snapshot.page.write.bw`,
`snapshot.meta.write.bw`, `snapshot.pages.c` (registered but **never
incremented — bug to fix**).

**Renames**:
- `tree.{backend}.demand.load.l` -> `tree.{backend}.page.load.l`

**New metrics**:
- `tree.{backend}.page.writeback.l` — eviction writeback latency
  (`BufferPool::write_back` → `store_->write_at`). This is the biggest
  missing backend I/O signal — the main source of I/O outside snapshots
  and the likely cause of flush-drain stalls.
- `tree.{backend}.page.write.bw` — page write bandwidth (flush drain,
  distinct from snapshot writes)
- `tree.{backend}.fsync.l` — fsync/barrier latency (durability SLO;
  currently `page_store->sync()` and `submit_fsync` are untimed)
- Fix `tree.snapshot.pages.c` — wire to the snapshot prepare loop
  (registered but never incremented)

**Considered but skipped**:
- `buf.pin.l` (buffer pool pin latency) — miss latency (`page.load.l`)
  is the dominant component; hit path is near-zero.
- `async.read.l` (async I/O submission→callback latency) — moderate
  priority; `page.load.l` already covers the synchronous read path.
  Defer until async stall characterization is needed.
- Backend file/block store own metrics — none needed; buffer pool and
  persist.cpp already cover all I/O paths.

---

**Files to change**:
- `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` — MetricsHandles
  struct, ScanProfile struct
- `lib/crowdb-tree/src/crowdb-tree.cpp` — init_metrics, observe calls,
  split/merge/consolidate counters in maybe_split_or_merge_locked,
  tree height gauge on root change, memtable freeze counter in
  maybe_freeze_active, scan retry counter in scan_async_attempt,
  GC counters in collect_garbage, page alloc counter in
  MappingTable::allocate_page_id
- `lib/crowdb-tree/src/mapping_table.cpp` — page alloc counter,
  total_pids/segments gauges
- `lib/crowdb-tree/src/buffer_pool.cpp` — writeback latency, page.find
  rename
- `lib/crowdb-tree/src/persist.cpp` — snapshot timing, fsync latency,
  fix snapshot.pages.c
- `lib/crowdb-tree/bench/scan_step_bench.cpp` — print updates
- `lib/crowdb-tree/tests/` — init_metrics call signature fixes

---

## GC Trigger Review (2026-09-01 bench analysis)

**Observation**: `tree.gc.l` fired with `count=6` in a write-only
workload (no deletes). The user expected GC to be gated by a
"reclaimable pages" threshold, not fire unconditionally.

### Current flow

1. The maintenance loop (`group_maintenance.rs:310-315`) calls
   `engine.set_gc_watermark(snapshot_slot, safe_slot)` then
   `engine.collect_garbage()` on **every maintenance tick** (default
   ~50ms).
2. `set_gc_watermark` (`crowdb-tree.cpp:885-891`) sets
   `gc_floor_ = min(snapshot_slot, safe_slot)` via a monotonic CAS.
3. `collect_garbage` (`crowdb-tree.cpp:893-993`) has one gate:
   if `gc_floor_ <= last_gc_floor_`, skip the full tree walk. Otherwise,
   walk every leaf, resolve each chain, and drop tombstones with
   `slot <= gc_floor`.
4. The `tree.gc.l` latency summary increments its `count` on every call,
   even when zero tombstones are reclaimed.

### Why GC fires on a write-only workload

In a live group, `snapshot_slot` and `safe_slot` advance continuously
with consensus progress, so `gc_floor_` rises on most ticks. The gate
only suppresses re-sweeps at the *same* watermark — it does not check
whether the tree has any tombstones to reclaim. The sweep walks all
leaves, finds `dropped == 0` for each (no tombstones), and returns with
zero `GcStats` but still records latency.

The `count=6` means the maintenance loop called `collect_garbage()` six
times in the metrics window — not that six pages were reclaimed.
`tree.gc.tombstones.c` and `tree.gc.pages.c` will be zero.

### Decision needed

The current design runs a full tree walk on every watermark advance,
even when there are no tombstones. Options:

1. **Add a tombstone-aware gate.** Track a "has tombstones" flag or a
   live tombstone counter. Skip the sweep entirely when no tombstones
   exist. This avoids the tree walk cost on write-only workloads.
2. **Add a minimum-reclaimable threshold.** Only sweep when the
   estimated reclaimable page count exceeds a bar (e.g. N pages or M%
   of tree). This reduces sweep frequency but adds tracking overhead.
3. **Keep as-is.** The sweep is cheap when no tombstones are found
   (early return per leaf at `dropped == 0`). The `count` in the
   latency summary is misleading but the actual cost is low.

**Recommendation**: Option 1. A simple `AtomicU64 tombstone_count_`
incremented on tombstone insert and decremented on GC drop. Skip the
sweep when `tombstone_count_.load() == 0`. The counter is approximate
(tombstones can be counted multiple times across delta chains) but the
gate only needs to know "are there any tombstones at all," not the
exact count. This eliminates the tree walk on pure-write workloads
with zero overhead on the hot path (the increment is already paid at
insert time).

**Code positions**:
- Gate: `crowdb-tree.cpp:905` (`collect_garbage` entry)
- Tombstone insert: `crowdb-tree.cpp` `apply` path (where `is_tombstone`
  cells are created)
- Tombstone drop: `crowdb-tree.cpp:3971` (`resolve_leaf_chain_for_rebuild`)
- Watermark: `crowdb-tree.cpp:885-891` (`set_gc_watermark`)
- Maintenance call site: `group_maintenance.rs:310-315`

---

## Flush Latency Review (2026-09-01 bench analysis)

**Observation**: `tree.flush.l` avg=643ms, max=1422ms. 14,621 frozen
queue full errors. The flush is not keeping up with the write rate.

See `doc/design/tree/design-crowdb-tree-engine-flush-flow.md` for the
full flow analysis with code positions and bottleneck breakdown.

---

## Snapshot Latency Review (2026-09-01 bench analysis)

**Observation**: `tree.snapshot.l` avg=2027ms, max=2027ms. Sub-phase
metrics show `tree.mem.snapshot.apply.l` = 733-1309ms (CPU phase under
`write_mutex_`) and `tree.mem.snapshot.page.write.io.l` max=296-432ms
per page. `tree.mem.fsync.l` = 0ms (page cache, not a bottleneck on
this setup).

See `doc/design/tree/design-crowdb-tree-engine-snapshot-flow.md` for the
full flow analysis with code positions and bottleneck breakdown.

---

## Long-tail diagnostics: what logs exist

**C++ tree engine** (`lib/crowdb-tree/src/*.cpp`):
- `CRB_LOG_INFO` at flush completion: `flush: tables={} entries={}
  contiguous_slot={}` (`crowdb-tree.cpp:1334`)
- `CRB_LOG_INFO` at snapshot commit: `snapshot committed: seq={}
  last_applied={} live_pages={} written={} segdir_len={}`
  (`persist.cpp:691`)
- `CRB_LOG_ERROR` when frozen queue is full: `maybe_freeze_active:
  frozen queue full` with depth/entries/bytes (`crowdb-tree.cpp:1058`)
- **No `CRB_LOG_DEBUG` or `CRB_LOG_TRACE` calls exist anywhere in
  `lib/crowdb-tree/src/`** — per-phase timing is metrics-only.

**Rust maintenance loop** (`group_maintenance.rs`):
- `debug!` for flush/snapshot/WAL/GC phase completion (elapsed_ms) —
  not emitted at default `crowdb_kv=info` level
- `info!` for snapshot trigger and persist_snapshot completion (>100ms)
- Enable with `RUST_LOG=crowdb_kv=debug` (or
  `crowdb_kv::cluster::group_maintenance=debug`)

**Actual bench run logs** (node3):
- 65 flush completion logs, 6 snapshot commit logs
- 20,026 frozen queue full errors (the stall signal)
- Snapshot wall times from Rust log: 0ms, 1141ms, 2029ms, 1914ms, 1ms
- No per-phase C++ debug logs (none exist in the code)
