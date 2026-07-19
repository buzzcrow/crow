# Metrics Plan: C++ Owned Registry + Flush/Snapshot Observability

## Metrics Log Header Format

Each metric-type section (Counter, Histogram, Summary, Bandwidth, Gauge) in
a `[metrics ...]` / `[cpp-metrics ...]` block prints its own header line,
confirmed against `flush_counters`/`flush_histograms`/`flush_summaries`/
`flush_bandwidths`/`flush_gauges` in `crowkv/src/metrics/mod.rs`.

**Rust format (current, target for both sides):**

- **Column 1** has no header label (printed as an empty string) — it is
  always the metric name, left-aligned to the widest name across the whole
  registry (`self.max_name_len`), not just within that section.
- **Columns 2-3 are common to every type**: `count` (window delta) and
  `tps(/s)` (rate over the real window). Counter, Histogram, Summary, and
  Bandwidth all agree on this — confirmed by inspecting each `flush_*`
  function.
- **Everything after column 3 is type-specific**: Counter adds `total`;
  Summary adds `avg(us)` / `max(us)`; Histogram adds `avg(us)` / `p50(us)` /
  `p99(us)` / `max(us)`; Bandwidth adds `avg_size(KB)` / `rate(KB/s)`.
  Gauges are the one exception — no `count`/`tps` at all, just `value`
  (point-in-time state, not a windowed event).

**C++ format (current — diverges, must be aligned):**

The C++ `flush_to` in `metrics.cpp` currently diverges from Rust in several
ways: Summary/Bandwidth/Histogram lack a `tps` column; latency units are
`ns` not `us`; Bandwidth uses bytes not KB; Histogram column order differs;
`max_name` is per-section not global; the section header is hardcoded as
`[metrics` (not `[cpp-metrics`); and the window format is `%.0fs` not `%.2fs`.

Phase 2 must align C++ `flush_to` to Rust's column layout, units, and
precision so both sections in the same log file are readable side-by-side
and parseable by the same scripts. See "C++ `flush_to` Format Alignment"
under Implementation Tasks.

## Problem

C++ engine metrics are frequently lost. Root cause: `Crowtree::set_metrics()`
requires an external `MetricsRegistry*` that Rust never provides — there is no
FFI binding for it. The current bridge (`engine_collector.rs`) only polls
**cumulative atomic counters** via `ct_get_stats` — it cannot bridge **latency
summaries** (`flush_l`, `snapshot_l`, `apply_l`, `page_write_l`,
`demand_load_l`) because those require `observe(ns)` calls in C++ hot paths,
and there is no way to relay latency samples through cumulative counter
deltas.

## Current Architecture

- **Rust `MetricsRunner`** owns a `MetricsRegistry`, periodically flushes to
  `RotatingLogWriter`.
- **`engine_collector.rs`** — pre-flush callback that polls C++ cumulative
  counters via FFI `ct_get_stats`, computes deltas, increments Rust `Counter`
  handles. Works for counters, **cannot bridge latencies**.
- **C++ `MetricsRegistry`** — fully implemented (`metrics.h/cpp`) with
  `flush_to(FILE*)`, same format as Rust. But `set_metrics()` is never called
  so all handles are null and all C++ latency metrics are silently dropped.
- **C++ atomic counters** (`flush_drain_total_`, `flush_entries_total_`,
  `snapshot_total_`) — always active, polled via `stats()`.

## Proposed Design: C++ Owns Its Metrics Registry

C++ owns its own `MetricsRegistry` per `Crowtree` instance. Rust triggers C++
to flush its metrics section into the same log file via FFI. No metric handle
marshaling across FFI.

### File Writing Strategy

**Rust owns the file handle.** `MetricsRunner` opens the metrics log file
and holds the `BufWriter`. Only the flush task thread ever writes to it —
no file locking needed.

1. Rust `MetricsRunner` flushes Rust metrics + misc section directly to the
   writer (as today).
2. Rust calls a post-flush callback that iterates all `CrowtreeEngine`
   instances.
3. For each engine, FFI call `ct_flush_metrics_str(tree, window_secs,
   timestamp, width)` returns a formatted string.
4. Rust writes the string to the same `BufWriter`.
5. C++ metrics appear in a `[cpp-metrics ...]` section.

C++ never touches the file handle. The `open_memstream` + string copy
overhead is one `memcpy` per flush tick per engine — negligible at 5s
intervals with a handful of engines.

### Shared Column Width (Max Name Length)

Both `[metrics]` and `[cpp-metrics]` sections should align to the same
column width so the log file reads as one coherent block. Since metric
names are known at registration time, the max name length is available
before printing.

- Rust queries each C++ engine's max name length via FFI:
  `ct_max_name_len(tree) -> usize`.
- Rust computes: `shared_width = max(rust_registry.max_name_len(),
  max(cpp_max_name_len for all engines))`.
- Rust flushes its own section with `shared_width` (instead of its own
  `max_name_len`).
- Rust passes `shared_width` to `ct_flush_metrics_str(tree, window_secs,
  timestamp, width)` — C++ uses it for its section too.
- Both sections align to the same column width in the log file.

This is a per-flush-tick computation (cheap: a few `max()` calls), not a
registration-time protocol — handles can be added dynamically between
ticks, and the width adapts automatically.

### C++ Side

- `Crowtree` creates its own `MetricsRegistry metrics_registry_` internally.
- `init_metrics(prefix)` replaces `set_metrics()` — registers all handles on
  the internal registry.
- All `metrics_.xxx` pointers point to handles on the internal registry (no
  external dependency).
- New: `flush_metrics_str(window_secs, timestamp, width)` uses
  `open_memstream` to capture `flush_to()` output as a string. The `width`
  parameter is the shared column width from Rust (see Shared Column Width
  above) — C++ uses it instead of its own per-section `max_name`.
- New: `max_name_len() -> usize` returns the current max name length from
  the C++ `MetricsRegistry` (for Rust's shared-width computation).
- Modify `flush_to()` to accept a `section_label` parameter (default
  `"metrics"`) and a `width` parameter (default 0 = use internal
  `max_name_len`) so `flush_metrics_str` can pass `"cpp-metrics"` and the
  shared width.
- Align `flush_to()` column format to match Rust (see "C++ `flush_to` Format
  Alignment" under Implementation Tasks).
- Remove: external `set_metrics(MetricsRegistry*, prefix)` API.
- Do **not** register `snapshot_c` in `init_metrics()` — `snapshot.l`'s
  count already covers the same event (see Non-Redundancy Principle). The
  internal `snapshot_total_` atomic stays for `ct_get_stats` / debug API.

### Rust Side

- `MetricsRunner` gets a `set_cpp_flush` callback:
  `Fn(&mut dyn Write, f64, &str, usize) + Send + Sync` — the callback
  receives `(writer, window_secs, timestamp, shared_width)`.
- Before flushing, Rust computes `shared_width = max(rust_max_name_len,
  max(cpp_max_name_len for all engines))`.
- Rust flushes its own section with `shared_width` (pass it to
  `reg.flush(writer, window_secs, timestamp, width)` — add a `width`
  parameter to the Rust `flush` method, defaulting to `max_name_len` if
  not provided).
- After Rust flush + misc, calls the callback.
- The callback (set up in `engine_collector.rs` or `main.rs`) iterates
  stores/groups, calls `ct_flush_metrics_str(tree, window_secs, timestamp,
  shared_width)` for each engine, writes result to the writer.

### Output Format Example

```
[metrics 2026-07-19T12:06:55.29Z window=5.00s]
s.1.g.1.wal.append.l                       19246      3849         3     19246
s.1.g.1.wal.fsync.l                          320        64       800       320
s.1.g.1.wal.write.bw                         320        64       4.0      1280
s.1.g.1.paxos.chosen_slot.g               19246
s.1.g.1.paxos.applied_slot.g              19240
...
[cpp-metrics 2026-07-19T12:06:55.29Z window=5.00s]
s.1.g.1.flush.l                              100      5000    50000    100
s.1.g.1.snapshot.l                             2    800000  1200000      2
s.1.g.1.snapshot.apply.l                      2    200000   300000      2
s.1.g.1.snapshot.page.write.io.l              8    120000   300000      8
s.1.g.1.page.write.l                         15     30000    100000     15
s.1.g.1.demand.load.l                          1    500000   500000      1
s.1.g.1.apply.l                              100      1000     5000    100
s.1.g.1.snapshot.page.write.bw                8        2       8.0      1280
s.1.g.1.snapshot.meta.write.bw                2        0       0.5        4
s.1.g.1.page.read.bw                           1        0       4.0        8
                                           count   tps(/s)     total
s.1.g.1.flush.drain.c                        120        24      1200
s.1.g.1.flush.entries.c                    12000      2400     12000
s.1.g.1.snapshot.page.write.cache.c            5         1        20
s.1.g.1.buf.hits.c                          8000      1600     80000
s.1.g.1.buf.misses.c                          10         2       100
                                           value
s.1.g.1.buf.dirty.g                            1
s.1.g.1.buf.resident.g                       512
```

Key changes from the first draft:
- C++ engine counters/gauges/summaries now appear in `[cpp-metrics]` with
  C++ naming (e.g. `flush.entries.c`, not `tree.flush_entries.c`). The Rust
  bridge no longer duplicates them in `[metrics]`.
- `[metrics]` contains only Rust-native metrics: WAL, RPC, Paxos, KV service.
- `wal.flush.c`, `wal.records_flushed.c`, `tree.snapshot.c` removed (see
  Non-Redundancy Principle).
- `snapshot.page.read.io.l` / `.cache.c` / `.read.bw` removed —
  `prepare_snapshot_locked` does not demand-load B-tree pages (it scans
  resident pages only; non-resident slots are skipped). See Latency Hierarchy
  Review for details.
- `snapshot.c` counter not registered in C++ `init_metrics()` — redundant
  with `snapshot.l`'s count in the same registry.

## Counter/Summary Non-Redundancy Principle

A `LatencySummary` (or `LatencyHistogram`) snapshot already carries `count`
(window delta) and `total_count` (cumulative) — the exact same shape as a
`Counter` snapshot's `{count, total}`. **Do not register a separate Counter
for the same event a Summary already observes** — it duplicates information
with no new signal and adds upkeep cost (two handles, two call sites, risk
of drifting out of sync).

A Counter next to a Summary is only justified when it tracks something the
Summary's count *cannot* express:

- **A magnitude, not an event count** — e.g. total entries drained
  (`flush_entries_total`) vs. number of drain calls (`flush_drain_total`);
  total pages written vs. number of snapshots. One call can move N units;
  the summary's count is per-call, the counter's total is per-unit.
- **A different population/outcome** — e.g. `rpc.errors.c` (failed subset)
  next to `rpc.l` (all attempts, or all successes — whichever population the
  summary observes); `buf.misses.c` (includes a racing-thread path that
  never reaches the actual IO) next to `demand.load.l` (only real IO
  attempts).
- **No latency is meaningful for the event** — e.g. a cache-hit path with
  zero IO has no latency worth summarizing, but the hit count itself is
  useful context next to the miss-path's IO latency
  (`snapshot.page.write.cache.c` next to `snapshot.page.write.io.l`).

This plan's design already followed this rule for the new snapshot page-IO
metrics (cache counters pair with IO summaries, not IO counters). It was
**violated** in the first draft for two Rust-layer metrics — fixed below.

## Metrics to Implement

### Rust Layer (in Rust `MetricsRegistry`)

Once C++ owns its own registry and flushes via FFI, the Rust-side bridge
(`engine_collector.rs`) no longer needs to poll C++ cumulative counters and
gauges — those metrics now appear natively in `[cpp-metrics]`. The Rust
`[metrics]` section keeps only Rust-native metrics: KV service, RPC, Paxos,
and WAL.

**Rust-native metrics (unchanged):**

- `kv.put.lh`, `kv.get.lh`, `kv.scan.l`, `kv.delete.c`, `kv.bytes_in.bw`,
  `kv.bytes_out.bw`, `kv.errors.c`, `kv.no-leader.c` — KV service.
- `rpc.l@{peer}`, `rpc.errors.c@{peer}` — RPC.
- `paxos.elections.c`, `paxos.step_downs.*.c`, `paxos.inflight_slots.g` —
  Paxos.
- `wal.append.l` — Summary — **exists**. Its own count is the per-record
  append count.

**New Rust-native metrics:**

- `wal.fsync.l` — Summary — **new** — `WalSegment::fdatasync()` latency
  inside `pipeline_writer.rs::write_batch()`. Thinnest-layer disk IO for the
  write path. Its own count subsumes the dropped `wal.flush.c` counter.
- `wal.write.bw` — Bandwidth — **new** — bytes physically written per
  `fdatasync`'d batch (`total_len`), observed at the same call site as
  `wal.fsync.l`.

**Dropped Rust-bridged metrics (now native in C++ `[cpp-metrics]`):**

- ~~`tree.flush_entries.c`~~, ~~`tree.flush_drain.c`~~, ~~`tree.mt_upsert.c`~~,
  ~~`tree.mt_get.c`~~, ~~`tree.mt_get_hit.c`~~, ~~`tree.l1_get.c`~~,
  ~~`tree.l1_get_hit.c`~~, ~~`tree.buf.hits.c`~~, ~~`tree.buf.misses.c`~~,
  ~~`tree.buf.evictions.c`~~, ~~`tree.buf.writebacks.c`~~,
  ~~`tree.buf.resident.g`~~, ~~`tree.buf.dirty.g`~~, ~~`tree.buf.used.g`~~,
  ~~`tree.buf.num_frames.g`~~ — all bridged via `engine_collector.rs` polling
  of `ct_get_stats`. Once C++ owns its registry, these appear natively in
  `[cpp-metrics]` with C++ naming (e.g. `flush.entries.c`, `buf.hits.c`).
  Remove the Rust handles, `EngineCounters`/`EngineGauges` structs, and
  delta-tracking state from `engine_collector.rs`.
- ~~`wal.flush.c`~~, ~~`wal.records_flushed.c`~~ — dropped (see Non-Redundancy
  Principle). `wal.fsync.l`'s count replaces `wal.flush.c`; `wal.append.l`'s
  count replaces `wal.records_flushed.c`.

**Paxos gauges** (`paxos.chosen_slot.g`, `paxos.applied_slot.g`, etc.) stay
in Rust `[metrics]` — they read from the Rust `LocalReplica`, not from C++.

### C++ Layer (in C++ `MetricsRegistry`, flushed via FFI)

**Existing handles (registered in `init_metrics()`, already observed):**

- `flush.l` — LatencySummary — `Crowtree::flush()` end-to-end.
- `snapshot.l` — LatencySummary — `Crowtree::snapshot()` end-to-end.
  Top-level snapshot latency (background flow, feature layer).
- `page.write.l` — LatencySummary — in-memory B-tree page mutation
  (split/merge/consolidate during `flush()`'s drain). **Not** a disk write.
  Name is misleading — kept to avoid test churn, but documented here to
  prevent confusion with `snapshot.page.write.io.l`.
- `demand.load.l` — LatencySummary — page-fault IO latency on the foreground
  get/put path (thinnest layer for that path). Pairs with `buf.hits.c` /
  `buf.misses.c` — hit/miss is already domain-separated.
- `apply.l` — LatencySummary — per-batch apply latency (memory-only, no IO).
- `buf.hits.c`, `buf.misses.c`, `buf.evictions.c`, `buf.writebacks.c` —
  Counters — buffer-pool events.
- `buf.resident.g`, `buf.dirty.g` — Gauges — buffer-pool state.
- `mt.upsert.c`, `mt.get.c`, `mt.get.hit.c` — Counters — memtable ops.
- `flush.drain.c`, `flush.entries.c` — Counters — magnitude metrics for
  flush. Note: C++ registers these as `flush.drain.c` / `flush.entries.c`
  (dot-separated), while the current Rust bridge uses `tree.flush_drain.c` /
  `tree.flush_entries.c` (underscore + `tree.` prefix). Once the Rust bridge
  is removed, only the C++ naming appears.
- `l1.get.c`, `l1.get.hit.c`, `map.lookup.c` — Counters — L1/mapping lookups.

**New handles (registered in `init_metrics()`, newly observed):**

- `snapshot.apply.l` — LatencySummary — **new** — `prepare_snapshot_locked`
  time: walking dirty pages, folding delta chains, building segment images.
  Mostly in-memory, but includes metadata reads (`read_best_anchor`,
  `collect_live_extents_from_directory`) on the first snapshot and after
  crash recovery — so "no syscalls" is inaccurate; say "mostly in-memory,
  occasional metadata reads."
- `snapshot.page.write.io.l` — LatencySummary — **new** — per-page
  `page_store->write_at()` latency in the `snapshot()` page-write loop
  (`persist.cpp:717-723`). Thinnest-layer disk write. Observed around each
  individual `write_at()` call, not around the whole loop.
- `snapshot.page.write.cache.c` — Counter — **new** — pages that were already
  clean (`durable_addr != kNoAddr`) during `prepare_snapshot_locked`'s
  `persist_one` lambda, so no write was queued. Incremented in `persist_one`
  on the `else` branch (clean page), not in `snapshot()`.
- `snapshot.page.write.bw` — Bandwidth — **new** — `w.blob.size()` bytes per
  individual `write_at()` call in the `snapshot()` page-write loop. Observed
  per-page (same granularity as `snapshot.page.write.io.l`), not summed per
  call.
- `snapshot.meta.write.bw` — Bandwidth — **new** — sum of blob sizes across
  `segment_writes` + `directory_write` + `anchor_write` in `snapshot()`.
  Observed once per `snapshot()` call (single `observe(total_bytes)`). No
  paired latency — these are small, infrequent bookkeeping writes.
- `page.read.bw` — Bandwidth — **new** — `blob.size()` bytes per demand-load
  in the foreground path (`crowtree.cpp:214`). Observed alongside
  `demand.load.l` at the same call site. **Note**: the async demand-load
  path (`get_async` / `snapshot_write_next_async`) also calls `read_at` /
  `submit_read` — instrumenting that path is out of scope for this plan
  (async path is `#ifdef CROWTREE_HAVE_LIBURING`-gated and not active in the
  default build); a TODO should be left in the code.

**Dropped from C++ `init_metrics()` (not registered):**

- ~~`snapshot.c`~~ — Counter — **not registered**. `snapshot.l`'s own count
  in the same C++ registry already covers the same event. The internal
  `snapshot_total_` atomic stays for `ct_get_stats` / debug API.
- ~~`snapshot.page.read.io.l`~~, ~~`snapshot.page.read.cache.c`~~,
  ~~`snapshot.page.read.bw`~~ — **dropped**. `prepare_snapshot_locked` does
  not demand-load B-tree pages: it scans `mapping_.segment_at(seg_idx)` and
  only processes `slot_word::is_resident(w)` slots (line 478: `if
  (!slot_word::is_resident(w)) { continue; }`). Non-resident pages are
  skipped, not loaded. There is no `read_at()` call in
  `prepare_snapshot_locked` for B-tree pages — the only reads are metadata
  reads (`read_best_anchor`, `collect_live_extents_from_directory`) which
  are already covered by `snapshot.apply.l`'s latency.

### Resolved Decisions (superseding prior Open Questions 1-3)

1. **`snapshot.entries.c`** — Resolved: use pages, not a key scan. Add a new
   **cumulative** atomic `snapshot_pages_total_` in C++ (mirrors
   `flush_entries_total_`), incremented by `pages_written` in
   `prepare_snapshot_locked` (where `pages_written` is counted, line 526:
   `snapshot_pages_written_.store(pages_written)`). The existing
   `snapshot_pages_written_` stays **per-call** (unchanged — required by
   `tests/integration/incremental_checkpoint_test.cpp`). Register
   `snapshot.pages.c` in the C++ `init_metrics()` and bridge the cumulative
   counter via `engine_collector.rs` delta polling (the one remaining C++
   counter that still needs Rust-side bridging — it is a magnitude metric
   with no paired latency in the C++ registry). Rationale: a per-call
   point-in-time value polled every 5s metrics tick would silently drop data
   if more than one snapshot happens within a window.
2. **`wal.write.l`** — Resolved: keep `wal.append.l` as-is, no rename, no new
   metric.
3. **Cache hit/miss domain separation** — Resolved: apply this pattern
   wherever page IO can be skipped by a cache hit. For snapshot page writes:
   `snapshot.page.write.cache.c` (clean page, no write queued) pairs with
   `snapshot.page.write.io.l` (dirty page, actual `write_at()` call). The
   snapshot read-side split was dropped (see above — no demand-load happens
   during snapshot prepare).

## Latency Hierarchy Review (Item 4)

Principle: for every flow, add a latency at the **thinnest layer** (the
actual syscall / disk IO / RPC boundary), and a latency at the **feature
layer** above it (the meaningful unit of work a human debugs against). If
Rust calls into C++, that FFI boundary is itself a natural place for a layer
split — measure from the Rust call-site down to the C++ entry, and again
inside C++ at its own syscall/IO boundary. Skip a layer if it isn't a
distinct decision point. Children of a layer should roughly sum to the
parent; a persistent gap indicates an unaccounted code path.

### Write Path (front-end, request-bound: `kv.put` -> Paxos -> WAL)

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Feature (end-to-end) | `kv.put.lh` | client request in -> response out | exists |
| Feature (per-peer RPC) | `rpc.l@{peer}` | leader's Accept RPC round-trip to one follower | exists |
| Feature (WAL call) | `wal.append.l` | `WalEngine::append()`: enqueue + batch-coalesce wait + write + fsync | exists |
| Thinnest (disk IO) | `wal.fsync.l` | `WalSegment::fdatasync()` inside `write_batch()` | **new** |

`wal.append.l` intentionally covers more than just the fsync — it also
includes queueing and batch-coalescing wait time. `wal.fsync.l` isolates the
"can't-go-faster" disk cost from queueing/batching overhead the code can
influence. `wal.append.l - wal.fsync.l` (approximately, via percentiles) is
the batching/queueing overhead.

`wal.fsync.l` **replaces** the existing `wal.flush.c` counter, not adds to
it (per the Non-Redundancy Principle above) — `flush_count` (batches/fsync
calls) becomes exactly `wal.fsync.l`'s own count once observed at the same
`write_batch()` call site.

Follower-side WAL append (via `on_prepare`/`on_accept` inbound RPC) uses the
same `wal.append.l`/`wal.fsync.l` pair — no separate follower metric needed
since it is the same call path.

### Snapshot Path (background, decoupled from client requests)

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Feature (top) | `snapshot.l` | `Crowtree::snapshot()` end-to-end (prepare + writes + syncs + commit) | exists |
| Feature (prepare) | `snapshot.apply.l` | `prepare_snapshot_locked`: dirty-page walk, delta fold, segment image build. Mostly in-memory; includes metadata reads (`read_best_anchor`, `collect_live_extents_from_directory`) on first/recovery snapshots | new |
| Thinnest (disk write, per-page) | `snapshot.page.write.io.l` | per-page `page_store->write_at()` in `snapshot()` page-write loop (`persist.cpp:717`) | new |
| Cache-hit counter (no write) | `snapshot.page.write.cache.c` | pages already clean (`durable_addr != kNoAddr`) in `persist_one` — no write queued | new |

No snapshot read-IO layer — `prepare_snapshot_locked` skips non-resident
slots (`!slot_word::is_resident(w) -> continue`), it does not demand-load
B-tree pages. The only reads during prepare are metadata reads (anchor,
segment directory) which are part of `snapshot.apply.l`'s latency.

`commit_prepared_snapshot` is a pure in-memory metadata update (setting
`durable_addr`, committing segment persists) — no IO, so no latency metric
there. The actual disk writes happen in `snapshot()` between prepare and
commit, in the page-write loop.

Snapshot is a background maintenance-tick flow (see `run_pass` in
`group_maintenance.rs`), fully decoupled from the client write path — it
never appears in `kv.put.lh` or `wal.append.l`.

### Flush Path (background, in-memory only — no IO layer needed)

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Feature | `flush.l` | `Crowtree::flush()`: drain L0 memtable into L1 | exists |
| Sub-step | `page.write.l` | in-memory B-tree page mutation (split/merge/consolidate) per drained entry | exists, renamed in docs only (see below) |

No thinnest/IO layer here — flush never touches disk (that happens later, in
snapshot). `page.write.l` is misleadingly named ("write" suggests disk); kept
as-is to avoid churn but documented as **in-memory only**, distinct from
`snapshot.page.write.io.l`.

### Foreground Read Path (get/scan, in-process cache miss -> disk)

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Domain split | `buf.hits.c` / `buf.misses.c` | buffer-pool lookup outcome | exists |
| Thinnest (disk read, only on miss) | `demand.load.l` | page-fault IO triggered by a miss | exists |

Already correctly domain-separated — used as the template for the new
snapshot page-write IO split above.

### Apply Path (Paxos -> state machine, in-process only)

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Feature | `apply.l` | per-batch apply from learner into the engine | exists |

No IO/RPC boundary inside apply — it is pure in-memory state transition, so
no thinner layer is needed.

### Summary of New Latency Metrics

- `wal.fsync.l` (Rust, `WalSegment::fdatasync()` call site in
  `pipeline_writer.rs::write_batch`).
- `snapshot.apply.l`, `snapshot.page.write.io.l`,
  `snapshot.page.write.cache.c` (C++, see Metrics to Implement above).

## Bandwidth Hierarchy (IO Bytes, Domain-Separated)

Bandwidth (`Bandwidth`/`.bw`) is its own metric family, parallel to Latency
— it measures bytes moved, not time spent. **A Bandwidth and a Latency
observed at the same call site are not redundant with each other**: they
measure different dimensions of the identical event (bytes vs. nanoseconds).
This is the one deliberate exception to the Counter/Summary Non-Redundancy
Principle above, which only forbids a *Counter* duplicating a *Summary's*
count — a Bandwidth's own count field duplicates that count too, but its
`avg_size`/`rate` fields are new information no Latency metric carries.

Today only one bandwidth exists — `kv.bytes_in.bw` / `kv.bytes_out.bw`, the
top-level client-facing payload bytes. Per this session's request, the
hierarchy below fills in every layer underneath it, mirroring the Latency
Hierarchy Review's layer boundaries and call sites exactly (same
thinnest-layer principle: measure real IO bytes, skip layers with no
distinct IO of their own).

### Write Path (front-end): KV bytes -> WAL disk bytes

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Feature (client payload) | `kv.bytes_in.bw` / `kv.bytes_out.bw` | request/response body bytes | exists |
| Thinnest (WAL disk IO) | `wal.write.bw` | bytes physically written per `fdatasync`'d batch in `write_batch()` (`total_len`) | **new** — pairs with `wal.fsync.l` at the same call site |

`kv.bytes_in.bw` is the logical payload size (key+value as seen by the
client); `wal.write.bw` is the physical WAL frame size actually written to
disk (includes record headers/CRC/framing overhead) — the two numbers are
expected to differ, and the gap is itself useful (framing overhead per
write).

### Foreground Read Path: B-tree page disk bytes (cache miss only)

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Thinnest (disk read, only on miss) | `page.read.bw` | bytes read via `page_store->read_at()` in the demand-load path | **new** — pairs with `demand.load.l` at the same call site |

No foreground **write** bandwidth exists on this path — B-tree page writes
never touch disk outside of a snapshot (see the Flush Path note in the
Latency Hierarchy Review: `page.write.l` is an in-memory mutation only).
This asymmetry (read bandwidth exists, write bandwidth does not, on the
foreground path) is intentional and mirrors the existing latency asymmetry.

### Snapshot Path (background): page-data bytes vs. auxiliary metadata bytes

`snapshot()` issues four categories of `page_store->write_at()` calls
(`persist.cpp`): dirty base **pages** (`page_writes`), mapping-table
**segment images** (`segment_writes`), the **segment directory**
(`directory_write`), and the **commit anchor / superblock**
(`anchor_write`). The first is page *data*; the other three are
bookkeeping *metadata* needed to find that data again on recovery — this is
exactly the "other auxiliary persistent info" the request called out, and
it deserves its own bandwidth distinct from page-data bandwidth so a reader
can tell "we wrote N MB of real data" apart from "we wrote a few KB of
bookkeeping" at a glance.

No snapshot read bandwidth — `prepare_snapshot_locked` does not demand-load
B-tree pages (see Latency Hierarchy Review above).

| Layer | Metric | Scope | Status |
| --- | --- | --- | --- |
| Thinnest (disk write, page data) | `snapshot.page.write.bw` | `w.blob.size()` per individual `write_at()` in `snapshot()` page-write loop | **new** — pairs with `snapshot.page.write.io.l` (per-page granularity) |
| Thinnest (disk write, metadata) | `snapshot.meta.write.bw` | sum of blob sizes across `segment_writes` + `directory_write` + `anchor_write` in `snapshot()` | **new** — no dedicated latency pairing (small, infrequent bookkeeping writes; not worth a separate summary) |

`snapshot.meta.write.bw` intentionally groups three call sites into one
metric (segment images, directory, anchor) rather than three separate ones
— they are all small bookkeeping writes serving the same purpose
(locate-the-data-on-recovery), and per-call-site granularity here would add
metric-surface noise without a debugging payoff (unlike the page data vs.
cache-hit split, which *does* pay off because the two populations have very
different magnitudes and IO cost).

### Bandwidth Metrics to Add

| Metric | Layer | Registers next to |
| --- | --- | --- |
| `wal.write.bw` | Rust, `pipeline_writer.rs::write_batch` | `wal.fsync.l` |
| `page.read.bw` | C++, demand-load path in `crowtree.cpp` (sync path only) | `demand.load.l` |
| `snapshot.page.write.bw` | C++, `persist.cpp::snapshot()` page-write loop (per-page) | `snapshot.page.write.io.l` |
| `snapshot.meta.write.bw` | C++, `persist.cpp::snapshot()` segment/directory/anchor writes (summed per call) | (no paired latency) |

## Real Window-Time Correctness (Item 5)

### Rust `MetricsRunner` — already correct, display precision improved

`start()` computes `window_secs` as the **real** elapsed
`Instant::now().duration_since(last_flush)` on every tick (not the nominal
configured interval), and `stop()` does the same for the final flush using
the real elapsed time since the last periodic flush. This is already
correct: the first window (between runner creation and the first real flush)
is whatever real time elapsed, and the final window (between the last
periodic flush and `stop()`) is also real elapsed time, not a rounded
multiple. TPS math (`count / window_secs`) already uses the unrounded float.

One fix needed: the printed header rounds to whole seconds
(`window={window_secs:.0}s`), which can misrepresent the real window during
manual audits of a log (e.g. a genuine 4.6s window prints as `window=5s`).
Change the format string to 2 decimal places (`window={window_secs:.2}s`) so
the printed window always matches what was actually used for the tps/rate
math.

### C++ `MetricsRegistry::start()` — real bug, fixed by this plan's design

Current C++ behavior (`flush_to_file()`) always passes the **nominal**
`interval_secs_` into `flush_to()`, never the real elapsed time — its
internal flush thread just `sleep_for(interval)`s in a loop with no drift
tracking, so under any scheduling delay (GC pause, OS jitter, slow
`flush_to_file()` itself) the reported window is wrong and tps/rate figures
skew accordingly.

This plan's design **removes the need to fix that loop directly**: since
Rust drives the flush now (`ct_flush_metrics_str(tree, window_secs,
timestamp)` in Phase 2/3), the C++ registry no longer runs its own
`start()`/`stop()` timer thread for the crowtree metrics — Rust computes the
real `window_secs` once per tick and passes the *same* value into both its
own `reg.flush()` and the FFI call. This guarantees the Rust and C++ sections
in one metrics log block always report an identical, real window with no
drift between them. `MetricsRegistry::start()`/`stop()` (the sleep-loop) is
left in place for any standalone/test-only use (e.g. FFI-less C++ tests) but
is not called from the server's production flush path after this plan lands.

## Existing Counter Audit

Every currently-registered Counter/Bandwidth/Gauge in the system, reviewed
against the Non-Redundancy Principle and the layering goals (clear, minimal,
correct hierarchy, captures the key decision points).

### KV Service (`rpc/kv_service.rs`)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `kv.put.lh` | Histogram | Keep | end-to-end write latency, own count suffices |
| `kv.get.lh` | Histogram | Keep | end-to-end read latency, own count suffices |
| `kv.delete.c` | Counter | Keep | no latency summary exists for delete; only signal for that op today (gap, not redundancy — could add `kv.delete.lh` later for parity with put/get, out of scope here) |
| `kv.scan.l` | Summary | Keep | own count suffices |
| `kv.bytes_in.bw` / `kv.bytes_out.bw` | Bandwidth | Keep | Bandwidth snapshot already carries count; no separate counter exists, correct |
| `kv.errors.c` | Counter | Keep | distinct failure-outcome population, no latency dimension applies |
| `kv.no-leader.c` | Counter | Keep | distinct rejection-reason population |

### RPC (`cluster/remote_replica.rs`)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `rpc.l@{peer}` | Summary | Keep | own count suffices |
| `rpc.errors.c@{peer}` | Counter | Keep | distinct failure subset, not expressible via the latency summary alone |

### Paxos Election (`cluster/local_replica.rs`)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `paxos.elections.c` | Counter | Keep | discrete event, no associated latency |
| `paxos.step_downs.higher_term.c` / `.lease.c` / `.admin.c` | Counter | Keep | discrete events, no associated latency |
| `paxos.inflight_slots.g` | Gauge | Keep | point-in-time state, different metric shape entirely |

### WAL (`wal_engine.rs`, `pipeline_writer.rs`)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `wal.append.l` | Summary | Keep | own count already is the per-record append count |
| `wal.flush.c` | Counter | **Drop** | redundant with new `wal.fsync.l`'s count (same call site: `write_batch()` success) |
| `wal.records_flushed.c` | Counter | **Drop** | redundant with `wal.append.l`'s count (same population: durably-flushed records) |
| `wal.fsync.l` | Summary | **New** | thinnest-layer disk IO latency; its own count subsumes the two dropped counters above |
| `wal.write.bw` | Bandwidth | **New** | physical WAL bytes per batch; pairs with `wal.fsync.l` |

### Buffer Pool (`buffer_pool.cpp`, C++ — now in `[cpp-metrics]`)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `buf.hits.c` | Counter | Keep | no latency applies (no IO on a hit) |
| `buf.misses.c` | Counter | Keep | superset of `demand.load.l`'s count — includes a racing-thread path (`load_mutex_` contention, "another loader won") that returns without ever calling into the IO path. Not redundant: `buf.misses.c >= demand.load.l.count` is an expected invariant, not equality. |
| `buf.evictions.c` / `buf.writebacks.c` | Counter | Keep | discrete events, no latency summary exists or is needed at this granularity |
| `buf.resident.g` / `buf.dirty.g` | Gauge | Keep | point-in-time state. Note: `buf.used.g` / `buf.num_frames.g` are Rust-bridged only (C++ `set_metrics` doesn't register them) — add to `init_metrics()` if still desired, or drop if redundant with `buf.resident.g`. |
| `demand.load.l` | Summary | Keep | thinnest-layer disk IO latency for the true miss+load path |
| `page.read.bw` | Bandwidth | **New** | bytes per demand-load; pairs with `demand.load.l` |

### Tree Flush/L1/Memtable (C++ — now in `[cpp-metrics]`)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `flush.l` | Summary | Keep | per-`flush()`-call latency (own count = number of flush() calls, including no-op ticks) |
| `flush.drain.c` | Counter | Keep | magnitude: number of `drain_memtable_into_l1` calls per `flush()` (0..N), different cardinality than `flush.l`'s count. Note: C++ naming (`flush.drain.c`), not the Rust bridge's `tree.flush_drain.c`. |
| `flush.entries.c` | Counter | Keep | magnitude: total entries moved, different cardinality than either of the above. C++ naming. |
| `mt.upsert.c` / `mt.get.c` / `mt.get.hit.c` | Counter | Keep | discrete memtable-op counters, no latency summary at this granularity |
| `l1.get.c` / `l1.get.hit.c` | Counter | Keep | discrete L1-lookup counters, no latency summary at this granularity |
| `map.lookup.c` | Counter | Keep | discrete mapping-table lookup counter, no latency summary needed |
| `page.write.l` | Summary | Keep | in-memory B-tree page mutation latency (own count suffices); **note**: name is misleading (no disk IO) — documented, not renamed, to avoid test churn |

### Tree Snapshot (C++ — now in `[cpp-metrics]`)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `snapshot.l` | Summary | Keep | top-level feature latency; own count/total_count already is the snapshot-call count — supersedes the `snapshot_total_`-bridged counter from the first draft |
| `snapshot.apply.l` | Summary | New | prepare-phase sub-latency (mostly in-memory + metadata reads) |
| `snapshot.page.write.io.l` / `.cache.c` | Summary + Counter | New | thinnest-layer disk write (per-page `write_at()`), paired with a no-IO cache-hit counter (clean page, no write queued) |
| `snapshot.page.write.bw` | Bandwidth | New | per-page write bytes; pairs with `snapshot.page.write.io.l` |
| `snapshot.meta.write.bw` | Bandwidth | New | metadata write bytes (segment images + directory + anchor), per-call sum |
| `snapshot.pages.c` | Counter | New | magnitude (total pages written across all snapshots), different cardinality than `snapshot.l`'s count — not redundant. Bridged via `engine_collector.rs` delta polling. |
| ~~`snapshot.page.read.io.l`~~ / ~~`.cache.c`~~ / ~~`.read.bw`~~ | — | **Dropped** | `prepare_snapshot_locked` does not demand-load B-tree pages |

### Apply (C++)

| Metric | Type | Verdict | Rationale |
| --- | --- | --- | --- |
| `apply.l` | Summary | Keep | own count suffices, no IO/RPC boundary inside apply |

### Net Effect

- **Rust `[metrics]` section** loses all C++-bridged counters/gauges (they
  move to `[cpp-metrics]` natively) and the two redundant WAL counters.
  Gains `wal.fsync.l` (Summary) and `wal.write.bw` (Bandwidth). Paxos
  gauges stay (Rust-native).
- **C++ `[cpp-metrics]` section** gains all existing C++ metrics (now
  actually registered and flushed), plus `snapshot.apply.l`,
  `snapshot.page.write.io.l`, `snapshot.page.write.cache.c`,
  `snapshot.page.write.bw`, `snapshot.meta.write.bw`, `page.read.bw`, and
  `snapshot.pages.c` (bridged). Loses `snapshot.c` (redundant with
  `snapshot.l`'s count). Snapshot read-side metrics dropped (no demand-load
  during prepare).
- `wal.fsync.l` replaces `wal.flush.c` with strictly more information
  (latency + count, instead of just count). `wal.append.l`'s count replaces
  `wal.records_flushed.c`.

## Implementation Tasks

### Phase 1: C++ MetricsRegistry Ownership

- [x] Add `MetricsRegistry metrics_registry_` member to `Crowtree`.
- [x] Replace `set_metrics(MetricsRegistry*, prefix)` with
  `init_metrics(prefix)` — creates the internal registry, registers all
  handles against it. Does **not** register `snapshot.c` (redundant with
  `snapshot.l`'s count in the same registry).
- [x] Call `init_metrics()` from `Crowtree::open()` (both fresh-tree and
  recovery paths).
- [x] Remove the external `set_metrics()` API and its call sites.
- [x] Remove `snapshot_c` handle from `MetricsHandles`. Remove the
  `metrics_.snapshot_c->inc()` call in `commit_prepared_snapshot`.

### Phase 2: C++ `flush_metrics_str` + FFI + Format Alignment

- [x] Add `Crowtree::flush_metrics_str(window_secs, timestamp, width) ->
  std::string` using `open_memstream`. The `width` parameter overrides
  per-section `max_name` for column alignment with the Rust section.
- [x] Add `Crowtree::max_name_len() -> usize` — returns the current
  `max_name_len` from the internal `MetricsRegistry`.
- [x] Modify `flush_to()` to accept a `section_label` parameter (default
  `"metrics"`) and a `width` parameter (default 0 = use internal
  `max_name_len`) so `flush_metrics_str` can pass `"cpp-metrics"` and the
  shared width.
- [x] **C++ `flush_to` format alignment** — aligned C++ output to Rust:
  - Added `tps(/s)` column to Summary, Bandwidth, and Histogram sections.
  - Changed latency units from `ns` to `us` (divide by 1000).
  - Changed Bandwidth `avg_size` to KB and `rate` to `KB/s`.
  - Aligned Histogram column order to Rust: `count tps avg p50 p99 max`.
  - Uses global `max_name_len` across all sections (not per-section).
  - Changed window format to `%.2fs` (not `%.0fs`).
- [x] Add C API `ct_flush_metrics_str(t, window_secs, timestamp, width) ->
  *mut c_char`.
- [x] Add C API `ct_max_name_len(t) -> usize`.
- [x] Add C API `ct_free_string(s)`.
- [x] Add Rust FFI bindings in `crowtree-ffi/src/lib.rs`.
- [x] Add `flush_metrics_str()` and `max_name_len()` wrappers on the Rust
  `Crowtree` type and on `CrowtreeEngine`.

### Phase 3: Rust Integration + `engine_collector` Cleanup

- [x] Add `set_cpp_flush` callback to `MetricsRunner` (`Fn(&mut dyn Write,
  f64, &str, usize) + Send + Sync`), invoked after the Rust `reg.flush()` +
  misc section in both `start()`'s loop and `stop()`.
- [x] Add `flush_with_width()` to Rust `MetricsRegistry` (uses explicit
  width for column padding). Original `flush()` delegates to it with
  `self.max_name_len`.
- [x] In the cpp_flush callback, compute `shared_width = max(rust_width,
  cpp_max_name_len)` per engine and pass it to `flush_metrics_str`.
- [x] Wire the callback in `engine_collector.rs`: iterate
  stores/groups, call `flush_metrics_str` per `CrowtreeEngine`, write the
  `[cpp-metrics ...]` block to the shared writer.
- [x] Change `flush`'s window header format to 2 decimal places
  (`window={window_secs:.2}s`) in `crowkv/src/metrics/mod.rs`.
- [x] Window is real elapsed time (`Instant::now() - last_flush`),
  passed through FFI to C++ `flush_metrics_str`.
- [x] **Remove C++-bridged handles from `engine_collector.rs`:** deleted the
  `EngineCounters`, `EngineGauges`, `WalCounters` structs; all C++-bridged
  counter/gauge fields (kept only Paxos gauge handles + `snapshot.pages.c`
  bridge); the `read_engine_counters_per_group` /
  `read_engine_gauges_per_group` / `read_wal_counters_per_group` functions;
  the `apply_counter_deltas` function; and all `last_values` / `last_wal`
  delta-tracking state. The collector callback now: registers new groups
  dynamically, polls Paxos gauges, polls `snapshot_pages_written` for the
  one remaining bridge.
- [x] Remove `wal.flush.c` and `wal.records_flushed.c` registration from
  `EngineHandles::register`.

### Phase 4: Rust-Layer Metric Changes

- [x] Add `wal.fsync.l` summary handle (registered in
  `local_replica.rs::set_metrics_registry`), observed directly around
  `segment.fdatasync().await` in `pipeline_writer.rs::write_batch`.
- [x] Add `wal.write.bw` bandwidth handle, registered and observed at the
  same `write_batch()` call site as `wal.fsync.l` (`total_len` bytes).
- [x] Keep `snapshot.pages.c` bridge in `engine_collector.rs`: poll via
  `ct_get_stats`, compute delta, increment the Rust `Counter` handle. This
  is the only C++ counter still bridged — it is a magnitude metric with
  no paired latency in the C++ registry.

### Phase 5: C++-Layer Metric Changes

- [x] Add cumulative `snapshot_pages_total_` atomic in `Crowtree`,
  incremented by `pages_written` in `prepare_snapshot_locked`. Keep
  existing per-call `snapshot_pages_written_` untouched for
  `incremental_checkpoint_test.cpp`. Exposed via `EngineStats`,
  `ct_stats`, FFI `Stats`, and `CrowtreeStatsView`.
- [x] Add `snapshot.apply.l` around `prepare_snapshot_locked` (in
  `snapshot()` — wraps the `prepare_snapshot_locked` call, not inside it
  since it's under `write_mutex_`).
- [x] Add `snapshot.page.write.io.l` around each individual
  `page_store->write_at()` call in `snapshot()`'s page-write loop.
- [x] Add `snapshot.page.write.cache.c` in `persist_one`'s `else` branch
  (clean page, `durable_addr != kNoAddr` — no write queued).
- [x] Add `snapshot.page.write.bw` alongside `snapshot.page.write.io.l` —
  `w.blob.size()` bytes per individual `write_at()` call in the page-write
  loop.
- [x] Add `snapshot.meta.write.bw` — sum of `sw.blob.size()` across all
  `segment_writes` + `directory_write.blob.size()` +
  `anchor_write.blob.size()`, observed once per `snapshot()` call.
- [x] Add `page.read.bw` alongside `demand.load.l` in the foreground
  demand-load path in `crowtree.cpp` (`blob.size()` bytes, sync path only).
- [x] Register all new handles in `init_metrics()`.

### Phase 6: Tests + Verification

- [x] C++ unit test: `init_metrics` + `flush_metrics_str` produces expected
  `[cpp-metrics ...]` section format with aligned columns (tps, us units,
  KB bandwidth).
- [x] C++ integration test: `snapshot_pages_total_` accumulates across
  multiple `snapshot()` calls while `snapshot_pages_written_` stays
  per-call (regression guard for the two-counter split).
- [x] Rust test: metrics log contains both `[metrics]` and `[cpp-metrics]`
  blocks with matching `window=` values (both at 2 decimal places).
- [x] Rust test: `[metrics]` section does NOT contain `tree.flush_entries.c`
  or other C++-bridged names (verify the bridge removal).
- [x] Rust test: `wal.fsync.l` and `wal.write.bw` appear in flush output
  after WAL appends with `fdatasync`.
- [ ] C++ test: `snapshot.page.write.bw` total bytes for one snapshot
  roughly match the sum of all `write_at()` blob sizes in the page-write
  loop; `snapshot.meta.write.bw` roughly matches segment + directory +
  anchor blob sizes.
- [ ] C++ test: `snapshot.page.write.cache.c` count +
  `snapshot.page.write.io.l` count == total dirty pages scanned in
  `prepare_snapshot_locked`.
- [x] Run `test-ct`, `cargo clippy -- -D warnings`, `cargo fmt --check`.