<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: Metrics Module + Periodic Logging (R8)

## Problem

CrowKV has no structured performance metrics. The existing
`crowkv/src/common/metrics.rs` provides `LayerMetrics` (RPC count, error
count, last RTT) and `ElectionMetrics` (election count, step-downs), but
these are **pull-only** snapshots consumed by the `/topology` endpoint.
There is no periodic logging, no latency distribution, no throughput
calculation, no gauge for current state. Operators must manually poll the
HTTP API to understand system behavior.

The C++ `crowtree` engine has `EngineStats` and `BufferPool::Stats` exposed
via FFI (`ct_get_stats`), but these are also pull-only with no time-windowed
aggregation.

The new metrics system **replaces** `LayerMetrics` and `ElectionMetrics`.
All call sites migrate to the unified registry. The old structs are removed
once migration is complete.

## Proposed Approach

A lightweight metrics system with five metric types and a periodic flush
to a dedicated metrics log file. Two independent implementations: Rust for
the consensus/RPC/WAL layer, C++ for the storage engine. Each owns its own
counters and logs its own summary. No metrics cross the FFI boundary at
runtime.

### Metric Types

**Counter** (monotonic, `AtomicU64` × 2):
- Tracks both a **window** value (reset each flush) and a **total** value
  (never reset, cumulative since registration).
- `inc()`: `fetch_add(1, Relaxed)` on both `window` and `total`.
- `inc_by(n)`: `fetch_add(n, Relaxed)` on both.
- Window flush: `swap(0, Relaxed)` on `window` for delta (divide by
  window seconds for rate), `load(Relaxed)` on `total` for cumulative.
- Log shows both: `count = 12345 (2469/s) total=987654`.
- The total is useful for lifetime reconciliation (e.g., allocations vs
  frees at process exit, total bytes written since start).
- Use cases: total puts, total gets, total deletes, total bytes written,
  total errors, WAL records appended.

**Gauge** (current state, `AtomicU64`):
- Set to the current value at any time; can go up or down.
- Holds a plain count — no units. If a latency value is needed, use a
  LatencySummary or LatencyHistogram instead.
- Window flush: report the last value seen (no aggregation needed).
- Use cases: active connections, queue depth, buffer pool resident pages,
  pending Phase-1 slots.

**Bandwidth** (monotonic bytes, `AtomicU64` × 3):
- Like Counter but also tracks byte sum for average size calculation.
- `observe(bytes)`: `fetch_add(1)` on count, `fetch_add(bytes)` on sum,
  `fetch_add(bytes)` on total_bytes.
- Window flush: `swap(0)` on count and sum, `load` on total_bytes.
  Compute avg_size = sum / count, rate = sum / window_seconds.
- Log shows count, TPS, avg_size (KB), and rate (KB/s). No total count
  (TPS × 5s gives window total, sufficient for bandwidth).
- Use cases: KV bytes in, KV bytes out.

**Latency Histogram** (fixed-bucket, array of `AtomicU64` + 2):
- Predefined latency buckets in nanoseconds:
  `0, 1µs, 10µs, 100µs, 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, ∞`
- Each observation increments the matching bucket via binary search on the
  bucket boundaries (13 buckets, branch-predictable), plus a `fetch_add(1)`
  on count and total_count.
- Window flush: `swap(0, Relaxed)` each bucket and count, `load` on
  total_count, then compute p50/p99 from the cumulative distribution.
  O(buckets) = O(13) per flush. Latency values are reported in µs.
- Overhead: one `fetch_add` on bucket + one `fetch_add` on count per
  observation. No allocation, no locks.
- Because count is built-in, the flush output includes count + TPS
  alongside p50/p99/max — no need for a separate Counter on the same path.
- Use cases: KV put latency, KV get latency.
- **Not** used for every operation. Only applied where percentile
  distribution is meaningful and the path is not so hot that even a
  bucketed histogram is too expensive.

**Latency Summary** (lightweight, `AtomicU64` × 4):
- Tracks count, sum, max, and total_count via four `AtomicU64`s.
- `observe(ns)`: `fetch_add(1)` on count and total_count, `fetch_add(ns)`
  on sum, `compare_exchange` loop on max.
- Window flush: `swap(0)` on count/sum/max, `load` on total_count.
  Compute average = sum / count, report avg and max. Latency values are
  reported in µs.
- No min (min is rarely useful for operational monitoring; it's almost
  always the fast-path cache hit and doesn't indicate problems).
- Overhead: four `AtomicU64` ops per observation. Cheaper than histogram,
  more informative than counter alone.
- Because count is built-in, the flush output includes count + TPS
  alongside avg/max — no need for a separate Counter on the same path.
- Use cases: scan latency, snapshot latency, WAL append latency, RPC
  paths where full histogram is overkill.

### Registry and Lifecycle

Each language has a `MetricsRegistry` that owns all metric instances and
a `flush()` method. The registry has an explicit lifecycle:

- **start(interval_secs)**: called at service startup. Spawns the flush
  thread/task. Interval is configurable (typically 5s or 10s), set once
  at start time.
- **stop()**: called at service shutdown. Performs a final flush, then
  joins the flush thread. Ensures the last window's data is not lost.
- **flush()**: iterates all metrics, takes snapshots, formats log line,
  resets windowed state.

**Rust** (`crowkv/src/metrics/mod.rs`):
- `MetricsRegistry` struct with separate collections per metric type:
  `Vec<Counter>`, `Vec<Gauge>`, `Vec<Bandwidth>`, `Vec<LatencyHistogram>`,
  `Vec<LatencySummary>`, plus `Vec<SystemCollector>`. Each metric has a
  name string. The registry tracks `max_name_len` per type group at
  registration time so flush output can align names within each group.
  Type-grouped storage also makes flush efficient: each group is flushed
  with its own column layout without scanning mixed types.
- `start()` spawns a tokio task that calls `flush()` on an interval.
  `stop()` cancels the task and does a final flush.
- The registry is `Arc`-shared; metric handles are references obtained at
  registration time and stored in the service/store structs.
- In-memory snapshots are also accessible via `registry.snapshot()` for
  future API access (e.g., a `/metrics` HTTP endpoint reading directly
  from memory, not parsing log files).

**C++** (`crowtree/include/crowtree/metrics.h`,
`crowtree/src/metrics.cpp`):
- `MetricsRegistry` class with the same five metric types plus
  `SystemCollector`, stored in separate type-grouped collections (same
  pattern as Rust).
- `start()` spawns a `std::thread` that loops on
  `std::this_thread::sleep_for(interval)`. `stop()` sets a cancel flag,
  does a final flush, and joins the thread.
- Metric handles are raw pointers (registry owns lifetime, engine outlives
  all metric users).

### Metric Ownership Scopes

Most metrics are **static** (process-global, registered once at startup).
But two additional patterns are supported:

- **Per-instance metrics**: some metrics have a lifecycle tied to a class
  instance (e.g., per-group, per-replica). These are stored as fields on
  the owning struct and registered with the registry at construction time.
  When the instance is dropped, the metric is deregistered.

- **Dynamic-name metrics**: some metrics need a name suffix to distinguish
  per-target instances. For example, RPC counters to different peer nodes
  are named `s.1.g.0.rpc.l@10.0.0.1:20001`, etc. The registry supports
  registering metrics with dynamically constructed names. The metric name
  is a `String` (or `Arc<str>`) rather than a `&'static str` in this case.

### Naming Convention

Metric names use dot-separated hierarchical paths. The path encodes
scope (store, group) and module (kv, wal, paxos, rpc, buf, apply,
snapshot), enabling prefix-based snapshot queries. This supports a future
GUI where clicking a store or group shows its metrics in a side panel.

**Abbreviations:** `s` = store, `g` = group. The module name (kv, wal,
paxos, rpc, buf, apply, snapshot) identifies which subsystem the metric
belongs to — no separate `px.`/`ct.` language prefix is needed.

**Type suffix conventions:**

Every metric name ends with a short type suffix so the type is visible in
the name without inspecting the registry:

- `.c` — Counter (monotonic, shows window delta + total)
- `.l` — LatencySummary (includes count + TPS + avg + max)
- `.lh` — LatencyHistogram (includes count + TPS + p50 + p99 + max)
- `.bw` — Bandwidth (includes count + TPS + avg_size + rate)
- `.g` — Gauge (current state, no aggregation)
- System metrics have no type suffix; each line carries its own description
  prefix instead (see Log Format).
- `@{peer_endpoint}` — dynamic suffix for per-peer metrics, appended after
  the type suffix (e.g., `rpc.l@10.0.0.2:20002`)

The type suffix replaces the full word — do not duplicate: `delete.c` not
`delete.count.c`, `put.lh` not `put.latency.lh`, `scan.l` not
`scan.latency.l`.

**Prefix hierarchy:**

- `sys.` — process-wide system metrics (no type suffix, each line is
  self-describing)
  - `sys.net.tcp_retransmits`
  - `sys.net.tcp_lost`
  - `sys.cpu.user`
  - `sys.cpu.sys`
  - `sys.mem.rss`

- `s.{store_id}.` — per-store metrics (Rust side: kv)
  - `s.{store_id}.kv.put.lh`
  - `s.{store_id}.kv.get.lh`
  - `s.{store_id}.kv.delete.c`
  - `s.{store_id}.kv.scan.l`
  - `s.{store_id}.kv.bytes_in.bw`
  - `s.{store_id}.kv.bytes_out.bw`
  - `s.{store_id}.kv.errors.c`

- `s.{store_id}.g.{group_id}.` — per-group metrics (Rust side: wal, paxos, rpc)
  - `s.{store_id}.g.{group_id}.wal.append.l`
  - `s.{store_id}.g.{group_id}.paxos.elections.c`
  - `s.{store_id}.g.{group_id}.paxos.step_downs.higher_term.c`
  - `s.{store_id}.g.{group_id}.paxos.step_downs.lease.c`
  - `s.{store_id}.g.{group_id}.paxos.step_downs.admin.c`
  - `s.{store_id}.g.{group_id}.paxos.inflight_slots.g`
  - `s.{store_id}.g.{group_id}.rpc.l@{peer_endpoint}`
  - `s.{store_id}.g.{group_id}.rpc.errors.c@{peer_endpoint}`

- `s.{store_id}.g.{group_id}.` — per-group metrics (C++ side: buf, apply, snapshot)
  - `s.{store_id}.g.{group_id}.buf.hits.c`
  - `s.{store_id}.g.{group_id}.buf.misses.c`
  - `s.{store_id}.g.{group_id}.buf.evictions.c`
  - `s.{store_id}.g.{group_id}.buf.writebacks.c`
  - `s.{store_id}.g.{group_id}.buf.resident.g`
  - `s.{store_id}.g.{group_id}.buf.dirty.g`
  - `s.{store_id}.g.{group_id}.apply.l`
  - `s.{store_id}.g.{group_id}.snapshot.l`

**Prefix-based snapshot:**

`registry.snapshot(prefix)` returns all metrics whose name starts with the
given prefix. Examples:
- `snapshot("sys.")` — all system metrics
- `snapshot("s.1.")` — all metrics for store 1
- `snapshot("s.1.g.2.")` — all metrics for group 2 in store 1
- `snapshot("")` — all metrics (full snapshot)

This is the foundation for future GUI integration: clicking a store node
shows `snapshot("s.{id}.")`, clicking a group shows
`snapshot("s.{id}.g.{gid}.")`. For now, the basic
implementation just needs to support the prefix filter; the GUI is a
future item.

### System Metrics Collector

A special collector type that reads OS-level stats every flush cycle.
These are not increment-on-event counters; they are polled at flush time.

- **TCP stats**: retransmits, lost packets. On Linux, read
  `/proc/net/snmp` (TCP section) and compute deltas between flushes.
  Parsing is lightweight (one file read + sscanf per flush, not per
  request). On macOS (dev), this is a no-op or reads `netstat -s` output.
- **Process CPU and memory**: use `getrusage(RUSAGE_SELF)` syscall
  (portable, no /proc parsing, no subprocess spawn). Returns user CPU
  time, system CPU time, max RSS. Compute CPU percentage as delta_time /
  wall_time over the flush window. Memory is max RSS (gauge).
- These collectors are registered with the registry like any other
  metric, but their `collect()` method is called at flush time rather
  than being incremented by call sites.

### Metrics Log File

Metrics are written to a **dedicated log file**, separate from the
application log. This keeps the metrics log clean and easy to parse.

- **File naming**: follows the same convention as the existing
  `crowkv/src/common/logging.rs` pattern:
  `metrics-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`
- **Location**: same log directory as the application log (e.g., `log/`).
- **Rotation**: the file is created fresh on each process start (timestamp
  + PID in the name). No in-process rotation; if the file grows large,
  external log rotation (logrotate) handles it.
- **Lock**: the metrics log writer holds an exclusive file lock
  (`metrics_lock`) to prevent concurrent writers from interleaving, same
  pattern as the KV service log.

### Log Format

Each flush produces a **block** with a timestamp header, followed by
**type-grouped sections**. Each section has its own column header tailored
to that metric type. This avoids the problem of a single unified header
with many blank columns. Metrics within each section are sorted by name.

The registry stores metrics grouped by type, so flush iterates each type
collection and emits its section. `max_name_len` is tracked per type group
at registration time for alignment within each section.

**Section order:** Counter, LatencyHistogram, LatencySummary, Bandwidth,
Gauge, System.

**Units:** latency values are always in microseconds (µs), bandwidth
values always in kilobytes (KB). The unit appears in the column header,
not on each value. This keeps values as plain numbers for easy script
parsing. Gauges are plain counts — no unit needed.

**Example:**

```
[metrics 2026-07-15T16:30:05.123Z window=5s]
name                                     count  tps(/s)  total
s.1.g.0.buf.evictions.c                     12        2   1234
s.1.g.0.buf.hits.c                         9845     1969 789234
s.1.g.0.buf.misses.c                        155       31  12345
s.1.g.0.buf.writebacks.c                      8        2    512
s.1.g.0.paxos.elections.c                     0        0      7
s.1.g.0.paxos.step_downs.admin.c              0        0      0
s.1.g.0.paxos.step_downs.higher_term.c        0        0      1
s.1.g.0.paxos.step_downs.lease.c              0        0      0
s.1.g.0.rpc.errors.c@10.0.0.2:20002           0        0      2
s.1.kv.delete.c                              42        8   3108
s.1.kv.errors.c                               0        0      3
name                                     count  tps(/s)  avg(us)  p50(us)  p99(us)  max(us)
s.1.kv.get.lh                              8901     1780      300      300     1100     3200
s.1.kv.put.lh                             12345     2469      800      800     2100     5300
name                                     count  tps(/s)  avg(us)  max(us)
s.1.g.0.apply.l                          12387     2477       50      300
s.1.g.0.rpc.l@10.0.0.2:20002               156       31      500     2100
s.1.g.0.snapshot.l                           1        0   12000    12000
s.1.g.0.wal.append.l                     12387     2477      120      800
s.1.kv.scan.l                               42        8     1200     5100
name                                     count  tps(/s)  avg_size(KB)  rate(KB/s)
s.1.kv.bytes_in.bw                       12345     2469           1.0       2560
s.1.kv.bytes_out.bw                       8901     1780           0.9       1640
name                                     value
s.1.g.0.buf.dirty.g                         12
s.1.g.0.buf.resident.g                     512
s.1.g.0.paxos.inflight_slots.g               3
misc
tcp_retransmits = 0  (delta=0 since last flush)
tcp_lost        = 0  (delta=0 since last flush)
cpu_user        = 23.4%
cpu_sys         =  4.1%
mem_rss         = 128MB

```

Design choices:
- **Type-grouped sections**: each metric type gets its own section with a
  column header line (starting with `name`) tailored to that type. No
  blank lines between sections — sections are back-to-back. A single
  blank line follows the entire flush block to separate it from the next
  interval's output.
- **Flush timing alignment**: the flush thread sleeps for the interval
  duration, so flushes land on interval-aligned timestamps (e.g., with
  5s interval: :05, :10, :15, ...). Typical intervals are 5s, 10s, or
  30s.
- **Name alignment**: within each section, names are padded to that
  section's `max_name_len`. The `name` header is also padded to the same
  width so column headers align with data. Different sections can have
  different name widths.
- **Units in headers, not values**: latency columns are labeled
  `avg(us)`, `p50(us)`, `p99(us)`, `max(us)`; bandwidth columns are
  labeled `avg_size(KB)`, `rate(KB/s)`. Values are plain numbers — no
  suffixes. This makes script parsing trivial (split on whitespace, parse
  as number). Gauges are plain counts, no unit.
- **Timestamp on the flush header only**, not per section or per line.
- **Sorted by name within each section**: alphabetical sort groups
  related metrics naturally (e.g., all `s.1.kv.*` together, all
  `s.1.g.0.buf.*` together).
- **System metrics section**: no column header. Each line is
  self-describing with `key = value` format and optional context in
  parentheses. System metrics have heterogeneous value types (percent,
  bytes, count, delta), so a unified column layout doesn't make sense.
- **Latency types include count + TPS**: since `observe()` increments an
  internal counter, latency sections show operation count and throughput
  alongside latency stats. No need for a separate Counter on the same
  path.
- **Only Counter section shows total**: cumulative since registration.
  Other types just show rate/TPS (window total = TPS × window_seconds).
- **Zero-suppression**: within Counter/Latency/Bandwidth sections, metrics
  with zero count are skipped (no activity in this window). Gauges are
  always printed (zero may be meaningful). System metrics always printed.

**Script parseability:** the format is designed for both human reading and
script parsing. Scripts can:
- Split the log on blank lines to separate flush blocks.
- Within a flush block, each line starting with `name` begins a new
  section with a column header. Lines not starting with `name` are data
  lines belonging to the current section.
- Data lines: split on `\s{2,}` (2+ spaces) to separate the name field
  from value fields. The name is the first field; values follow in the
  order indicated by the header.
- System section: preceded by a `misc` line (no `name` header). Lines
  after `misc` are parsed as `key = value`.
- The timestamp and window are on the `[metrics ...]` header line.

### Instrumentation Points

**Rust side** (`crowkv/src/`):

- `rpc/kv_service.rs`: `put`, `get`, `delete`, `scan` handlers
  - Latency histogram: `s.{id}.kv.put.lh`, `s.{id}.kv.get.lh`
    (includes count + TPS)
  - Latency summary: `s.{id}.kv.scan.l` (includes count + TPS; scan
    latency varies by range size, avg+max is sufficient)
  - Bandwidth: `s.{id}.kv.bytes_in.bw`, `s.{id}.kv.bytes_out.bw`
  - Counter: `s.{id}.kv.errors.c` (standalone — no latency, total
    matters for error reconciliation)
  - Counter: `s.{id}.kv.delete.c` (standalone — no latency histogram on
    delete path, total useful for op reconciliation)

- `wal/wal_engine.rs`: `append()` path
  - Latency summary: `s.{id}.g.{gid}.wal.append.l` (includes count + TPS)

- `cluster/local_replica.rs`: election path
  - Counter: `s.{id}.g.{gid}.paxos.elections.c`,
    `s.{id}.g.{gid}.paxos.step_downs.higher_term.c`,
    `s.{id}.g.{gid}.paxos.step_downs.lease.c`,
    `s.{id}.g.{gid}.paxos.step_downs.admin.c`
  - Gauge: `s.{id}.g.{gid}.paxos.inflight_slots.g`
  - (Replaces `ElectionMetrics`)

- `cluster/remote_replica.rs`: RPC path
  - Latency summary (dynamic name):
    `s.{id}.g.{gid}.rpc.l@{peer_endpoint}` (includes count + TPS + avg + max)
  - Counter (dynamic name):
    `s.{id}.g.{gid}.rpc.errors.c@{peer_endpoint}` (standalone — total
    matters for error reconciliation)
  - (Replaces `LayerMetrics`)

**C++ side** (`crowtree/src/`):

- `buffer_pool.cpp`: `pin()`, `pin_new()`
  - Counter: `s.{id}.g.{gid}.buf.hits.c`,
    `s.{id}.g.{gid}.buf.misses.c`,
    `s.{id}.g.{gid}.buf.evictions.c`,
    `s.{id}.g.{gid}.buf.writebacks.c` (standalone — total matters for
    cache hit ratio reconciliation)
  - Gauge: `s.{id}.g.{gid}.buf.resident.g`, `s.{id}.g.{gid}.buf.dirty.g`

- `crowtree.cpp`: `apply_put()`, `apply_delete()`
  - Latency summary: `s.{id}.g.{gid}.apply.l` (includes count + TPS +
    avg + max)

- `persist.cpp`: `snapshot()` path
  - Latency summary: `s.{id}.g.{gid}.snapshot.l` (includes count + TPS +
    avg + max)

### Migration from Existing Metrics

`LayerMetrics` and `ElectionMetrics` are replaced by the new registry.
Migration is straightforward:

- `LayerMetrics` fields (`rpc_count`, `err_count`, `last_rtt_ms`) map to
  `s.{id}.g.{gid}.rpc.l@{peer}` (LatencySummary — includes count) and
  `s.{id}.g.{gid}.rpc.errors.c@{peer}` (Counter) on the new registry.
  Call sites in `remote_replica.rs` that call `record_ok(latency)` /
  `record_err()` switch to `summary.observe(latency_ns)` /
  `err_counter.inc()`.
- `ElectionMetrics` fields map to Counters. Call sites in
  `local_replica.rs` that call `record_election()` /
  `record_step_down_*()` switch to `counter.inc()`.
- The `/topology` endpoint reads from `registry.snapshot()` instead of
  the old `snapshot()` methods.
- `crowkv/src/common/metrics.rs` is deleted after migration.
- C++ `EngineStats` and `BufferPool::Stats` stay as internal structs
  (they serve the FFI `ct_get_stats` call), but the periodic logging
  reads from the new C++ `MetricsRegistry`, not from `EngineStats`.

### FFI Boundary

No metrics cross the FFI boundary at runtime. The Rust registry logs
Rust-side metrics; the C++ registry logs C++-side metrics. Two
independent log blocks appear in the metrics log every flush cycle. This
keeps FFI overhead at zero for metrics.

The existing `ct_get_stats` FFI call (used by `/topology`) is unaffected.
A future `/metrics` HTTP endpoint can call `registry.snapshot()` on the
Rust side directly from memory. C++ metrics would need an FFI snapshot
call, but that is read-once-per-HTTP-request, not per-operation, so the
FFI overhead is negligible.

### In-Memory Access

All metrics are readable in-memory via `registry.snapshot(prefix)`, which
returns all metrics whose name starts with the given prefix (empty prefix
returns everything). This enables:

- Future `/metrics` HTTP endpoint reading directly from memory, with
  prefix filtering (e.g., `GET /metrics?prefix=s.1.`).
- Future GUI integration: clicking a store node in the console calls
  `snapshot("s.{id}.")`, clicking a group calls
  `snapshot("s.{id}.g.{gid}.")`.
- Code-level access for health checks, admission control, or adaptive
  behavior based on current metrics.
- No need to parse log files to get metric values.

For now, only the basic prefix-filtered snapshot is implemented. The GUI
and HTTP endpoint are future items.

## Alternatives Considered

**`metrics` crate (Rust) + `prometheus-cpp` (C++):**
Rejected. These libraries target scrape-based monitoring (Prometheus,
OpenTelemetry). They add dependency weight, global recorder overhead, and
don't naturally produce periodic log output. Self-implementation gives
full control over overhead, memory usage, and output format. The current
scope is log-based metrics with in-memory access; if external reporting
(Prometheus, OTel) is needed later, a `MetricsSnapshot` can be exported
through an adapter without changing call sites.

**HDR histogram for latency:**
Rejected. HDR histogram provides high precision across a wide range but
requires more memory and complex implementation. Fixed-bucket histogram
with 13 buckets is sufficient for p50/p99 at the granularity we need
(microsecond to second range). Memory usage is 13 × 8 bytes = 104 bytes
per histogram. If finer granularity is needed later, the bucket array can
be expanded without changing the API.

**Single registry shared across FFI:**
Rejected. Crossing FFI for every counter increment would add overhead on
the hot path (C++ to Rust FFI call per buffer pool hit). Two independent
registries with separate log blocks is simpler and faster.

**`tracing` spans for latency:**
Considered. `tracing` span instrumentation is already used in the
codebase. However, extracting percentile distributions from span data
requires a subscriber layer that aggregates and stores per-span durations
— effectively reimplementing a histogram. Direct atomic bucket increments
are cheaper and more predictable.

## File Changes

- New: `crowkv/src/metrics/mod.rs` — `MetricsRegistry`, `Counter`,
  `Gauge`, `Bandwidth`, `LatencyHistogram`, `LatencySummary`, `flush()`
  logic, `snapshot()`, lifecycle (start/stop).
- New: `crowkv/src/metrics/counter.rs` — `Counter` and `Gauge` impl.
- New: `crowkv/src/metrics/bandwidth.rs` — `Bandwidth` impl.
- New: `crowkv/src/metrics/histogram.rs` — `LatencyHistogram` impl.
- New: `crowkv/src/metrics/summary.rs` — `LatencySummary` impl.
- New: `crowkv/src/metrics/system.rs` — system metrics collector
  (TCP stats, CPU/memory via `getrusage`).
- Modify: `crowkv/src/lib.rs` — add `pub mod metrics`.
- Modify: `crowkv/src/rpc/kv_service.rs` — instrument put/get/delete/scan.
- Modify: `crowkv/src/wal/wal_engine.rs` — instrument append path.
- Modify: `crowkv/src/cluster/remote_replica.rs` — replace `LayerMetrics`
  with registry counters.
- Modify: `crowkv/src/cluster/local_replica.rs` — replace
  `ElectionMetrics` with registry counters.
- Modify: `crowkv/src/common/metrics.rs` — delete (or deprecate then
  delete in a follow-up).
- Modify: `crowkv-server/src/main.rs` — create registry, call start/stop,
  pass registry handle to services.
- New: `crowtree/include/crowtree/metrics.h` — C++ `MetricsRegistry`,
  `Counter`, `Gauge`, `Bandwidth`, `LatencyHistogram`, `LatencySummary`,
  `SystemCollector`.
- New: `crowtree/src/metrics.cpp` — implementation.
- Modify: `crowtree/src/buffer_pool.cpp` — instrument pin/pin_new.
- Modify: `crowtree/src/crowtree.cpp` — instrument apply_put/apply_delete.
- Modify: `crowtree/CMakeLists.txt` — add metrics.cpp to sources.
- New: `crowkv/tests/metrics_test.rs` — unit tests for counter/gauge/
  bandwidth/histogram/summary window behavior, flush formatting,
  snapshot access.
- New: `crowtree/tests/unit/metrics_test.cpp` — C++ unit tests.

## Acceptance Criteria

- `MetricsRegistry::start(5)` spawns a flush task; `stop()` does a final
  flush and joins. Verified by test that starts, waits 6s, checks two
  flushes happened, stops, checks one more flush.
- Periodic metrics log block appears every 5 seconds in the metrics log
  file, with timestamp header, grouped and aligned metric lines.
- Separate metrics log block appears from the C++ engine with buffer pool
  and apply latency stats.
- `Counter` window reset: after flush, counter reports 0 delta if no new
  increments, but `total` continues to accumulate. Verified by test that
  increments, flushes, increments more, flushes, and checks delta + total
  across both windows.
- `LatencyHistogram` computes p50/p99 within one bucket of the true
  percentile (verified by test with known distribution).
- `LatencySummary` reports avg and max correctly; max resets after flush.
  Verified by test.
- `Gauge` reports the last value set, not a delta. Verified by test.
- `Bandwidth` reports count, avg_size, and rate correctly; window resets
  after flush. Verified by test.
- No allocation on the hot path (counter increment, histogram observe,
  summary observe). Verified by code inspection confirming only
  `AtomicU64::fetch_add` / `compare_exchange`.
- `registry.snapshot()` returns current in-memory values without
  resetting windowed state. Verified by test.
- Dynamic-name metrics (e.g.,
  `s.1.g.0.rpc.l@10.0.0.2:20002`) appear in flush output
  with the full name. Verified by test.
- `registry.snapshot("s.1.")` returns only metrics with that
  prefix. `registry.snapshot("")` returns all. Verified by test.
- System metrics collector reports TCP retransmits, CPU%, and RSS.
  Verified by test (may be platform-specific; test on Linux, no-op on
  macOS).
- Existing `LayerMetrics` and `ElectionMetrics` call sites are migrated
  to the new registry. Old structs are removed. All existing tests pass.
- New metrics tests pass.
