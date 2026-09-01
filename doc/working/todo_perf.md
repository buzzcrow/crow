# todo_perf.md — Performance Observer Mechanism

Context: bench `write_128t_4c_win32_coales16` showed 124K ops/s (34% below
189K reference). Root cause: sequential maintenance loop (flush -> snapshot ->
WAL flush -> GC) blocks flushes during 1.6-2.6s snapshots, causing frozen
queue full errors (81K) and active memtable growth.

Before fixing the maintenance loop, we need better observability to
characterize the stall behavior. This file tracks metrics/log refinements.

## Done

- [x] Rename `rpc.transport.round` -> `rpc.epoll.run` (clearer semantics)
- [x] Bandwidth metrics: KB -> MB with 2 decimal places (Rust + C++ flush)
- [x] Rename `rpc.transport.*` -> `rpc.*` (shorter, no confusion)
- [x] Rename `rpc.read.bw` -> `rpc.socket.read.bw`, `rpc.writev.bw` ->
      `rpc.socket.writev.bw` (distinguish socket-level I/O)
- [x] Merge `rpc.request.submit_fail.c` + `rpc.send_queue_reject.c` into
      `rpc.send.queue.full.c` (redundant — both fire on same enqueue_send
      failure at different layers)
- [x] `sys.cpu_user_us` / `sys.cpu_sys_us` -> `sys.cpu.util.user` /
      `sys.cpu.util.sys` (percent, 0-100%) — raw microseconds were hard to
      interpret
- [x] RPC transport lifecycle logs: listen, start, stop, connection
      accepted (with peer ip:port), connection established (with remote
      addr), connection closed (with peer addr)
- [x] Bench `kv write` start log: include all config fields (duration,
      loaders, connections, key_space, value_size, event_write, rpc_workers,
      send_queue_capacity, metrics_interval)
- [x] Bench `kv write` stop log: include ops + errors summary
- [x] Server config log: one setting per line, aligned columns

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
needed at this level — `ct.flush.l` already covers the storage-engine
flush call, and logs suffice for one-off stall characterization.

**Needed metrics**:
- Frozen memtable count + live records gauge: see P3
  (`ct.mt.frozen.g` / `ct.mt.records.g`) — defined there as tree-engine
  metrics; P2 consumes them to characterize the stall

**Needed logs**:
- Maintenance loop: log each phase start/stop with elapsed_ms
- Frozen queue full: log when queue is full with current depth + active
  memtable entries
- Snapshot trigger: log which threshold triggered (time/slot/flush_count)

**Refinement resolved**:
- Verified: the two memtable gauges don't exist yet (see P3).
- `ct.flush.l` exists and covers the storage-engine `Crowdbtree::flush()`
  call (draining frozen memtables to L1/pages). No separate
  maintenance-loop flush-phase histogram needed — logs cover it.
- Gauge naming owned by P3 (`ct.mt.frozen.g` / `ct.mt.records.g`).

### P3: Tree metrics refactor (3-layer gap review)

**Status**: Design doc written. Code partially done (backend label split,
scan l0/snapshot+l0/skip removal, scan l1.descent+l1.resolve merge,
apply.l/snapshot.l removal). Remaining work below, organized by the
three engine layers: tree operations, page mapping table, backend I/O.

**Naming**: Every tree metric is registered with a per-group prefix
`make_metrics_prefix(opt)` = `s.{store_id}.g.{group_id}.ct`, so the
full metric name in the log is `s.X.g.Y.ct.<rest>`. The names below
drop the `s.X.g.Y.` prefix for brevity; each one is scoped to its own
store/group at registration time. A tree belongs to exactly one group,
so per-group status is already distinguishable — no extra plumbing
needed.

**Done** (all layers):
- Backend label split: logical metrics use `ct.*`, I/O metrics use
  `ct.{backend}.*`
- Removed `ct.{backend}.apply.l` (duplicate of `paxos.learn.apply.l`)
- Removed `ct.{backend}.snapshot.l` wrapper (sub-phases are more useful)
- Removed `ct.{backend}.scan.l0.snapshot.l` (always 0us, cursor not copy)
- Removed `ct.{backend}.scan.l0.skip.l` (hardcoded 0, dead since R50)
- Merged `scan.l1.descent.l` + `scan.l1.resolve.l` -> `scan.l1.l`
- Brought back `ct.snapshot.l` as a logical metric (no backend label)

---

#### Layer 1 — Tree operations (B-tree behavior)

Existing: `mt.upsert.c`, `mt.get.c`, `mt.get.hit.c`, `l1.get.c`,
`l1.get.hit.c`, `flush.l`, `flush.drain.c`, `flush.entries.c`,
`page.write.l` (consolidate wall time), `scan.c`, `scan.entries.c`,
`scan.l`, `scan.l1.l`, `scan.merge.l`.

**Renames**:
- `ct.mt.upsert.c` -> `ct.mt.apply.c` (covers put + del, not just upsert)

**New metrics**:
- `ct.mt.apply.l` — memtable upsert batch latency
- `ct.mt.get.l` — memtable get latency (L0 lookup across all live tables)
- `ct.mt.frozen.g` — frozen memtable count (also consumed by P2 to
  characterize maintenance stall)
- `ct.mt.records.g` — total live records across all memtables (also
  consumed by P2)
- `ct.mt.freeze.c` — memtable freeze count (backpressure / write pacing)
- `ct.l1.get.l` — L1 get latency (B-tree descent + leaf chain resolve)
- `ct.page.split.c` — leaf split count (SMO churn → write amplification)
- `ct.page.merge.c` — leaf merge count
- `ct.page.consolidate.c` — consolidation count (pairs with
  `page.write.l` which already times the consolidate wall time)
- `ct.tree.height.g` — tree height gauge (sampled on root change;
  characterizes read amplification and structural growth)
- `ct.scan.retry.c` — scan async retry count (cold-page stall → tail
  latency)
- `ct.gc.tombstones.c` — GC reclaimed tombstone count (from `GcStats`)
- `ct.gc.pages.c` — GC reclaimed page count

**Considered but skipped**:
- `apply.c` / `put.c` / `del.c` (op-level counters) — `paxos.learn.apply.l`
  count already gives ops/s at the consensus layer; `mt.apply.c` counts
  per-key. Redundant.
- `ct.get.retry.c` — get async retry; lower priority than scan retry
  (get is point lookup, scan is long-running and more stall-prone).
- `ct.mt.freeze.{reason}.c` (per-reason freeze counters) — total
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
- `ct.{backend}.page.map.lookup.c` -> `ct.{backend}.page.find.c`
- Merge `ct.{backend}.buf.hits.c` + `ct.{backend}.buf.misses.c` into
  `ct.{backend}.page.find.c` + `ct.{backend}.page.find.hit.c`
  (page.find.c = total page lookups, page.find.hit.c = buffer pool
  hits; hit rate = hit / find)

**New metrics**:
- `ct.page.map.alloc.c` — page ID allocations (hot path, tree growth
  rate; called on every new leaf/inner/overflow page, split, merge)
- `ct.page.map.total_pids.g` — `next_page_id_` gauge (total pages ever
  allocated — cumulative growth indicator)
- `ct.page.map.segments.g` — segments allocated gauge (mapping table
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
- `ct.{backend}.demand.load.l` -> `ct.{backend}.page.load.l`

**New metrics**:
- `ct.{backend}.page.writeback.l` — eviction writeback latency
  (`BufferPool::write_back` → `store_->write_at`). This is the biggest
  missing backend I/O signal — the main source of I/O outside snapshots
  and the likely cause of flush-drain stalls.
- `ct.{backend}.page.write.bw` — page write bandwidth (flush drain,
  distinct from snapshot writes)
- `ct.{backend}.fsync.l` — fsync/barrier latency (durability SLO;
  currently `page_store->sync()` and `submit_fsync` are untimed)
- Fix `ct.snapshot.pages.c` — wire to the snapshot prepare loop
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
