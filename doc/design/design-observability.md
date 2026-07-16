<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Observability Design

## Mandatory Signals

Per-group leader/term/max-slot/safe-slot/in-flight/gap count; per-node WAL
flush latency and throughput; per-RPC rate/latency/error breakdown; structured
logs with `node_id`, `group_id`, `slot`, `term` on consensus events. Tracing
hooks reserved but not required in the initial design.

## Metrics Module

A lightweight metrics system with five metric types and periodic flush to a
dedicated metrics log file. Two independent implementations: Rust for the
consensus/RPC/WAL layer, C++ for the storage engine. Each owns its own
counters and logs its own summary. No metrics cross the FFI boundary at
runtime.

### Metric Types

- **Counter** (`AtomicU64` x 2) — monotonic, tracks window delta + total.
  `inc()` / `inc_by(n)`. Flush shows `count`, `tps`, `total`. Use cases:
  puts, gets, deletes, errors, WAL records, elections, step-downs.
- **Gauge** (`AtomicU64`) — current state, can go up or down. `set(v)`.
  Flush shows last value. Use cases: buffer pool resident/dirty pages,
  in-flight slots.
- **Bandwidth** (`AtomicU64` x 3) — monotonic bytes, tracks count + sum +
  total_bytes. `observe(bytes)`. Flush shows `count`, `tps`, `avg_size(KB)`,
  `rate(KB/s)`. Use cases: KV bytes in/out.
- **LatencyHistogram** (13 buckets + 2 `AtomicU64`) — fixed-bucket percentile
  distribution. Bucket boundaries: `0, 1us, 10us, 100us, 500us, 1ms, 5ms,
  10ms, 50ms, 100ms, 500ms, 1s, infinity`. `observe(ns)` does binary search +
  `fetch_add`. Flush computes p50/p99 from cumulative distribution. Use cases:
  KV put latency, KV get latency.
- **LatencySummary** (`AtomicU64` x 4) — lightweight latency tracking
  (count + sum + max + total_count). `observe(ns)`. Flush shows `avg(us)`,
  `max(us)`. Use cases: scan, snapshot, WAL append, RPC, apply.

### Registry and Lifecycle

Each language has a `MetricsRegistry` that owns all metric instances. The
registry has `start(interval_secs)` (spawns flush thread/task), `stop()`
(final flush + join), and `flush()` (iterate all metrics, snapshot, format,
reset window state). Interval is typically 5s or 10s.

- Rust (`crowkv/src/metrics/mod.rs`): `MetricsRegistry` with type-grouped
  `Vec<T>` collections, `Arc`-shared, metric handles stored on service/store
  structs. `start()` spawns tokio interval task. Also provides
  `snapshot(prefix)` for in-memory access without resetting window state.
- C++ (`crowtree/include/crowtree/metrics.h`, `crowtree/src/metrics.cpp`):
  Same type-grouped pattern. `start()` spawns `std::thread` with
  `sleep_for` loop. Metric handles are raw pointers (registry owns lifetime).

### Naming Convention

Dot-separated hierarchical paths: `s.{store_id}.g.{group_id}.{module}.{metric}`.
Type suffix on every metric name: `.c` (Counter), `.g` (Gauge), `.bw`
(Bandwidth), `.lh` (LatencyHistogram), `.l` (LatencySummary). Dynamic suffix
`@{peer_endpoint}` for per-peer metrics. System metrics use `sys.` prefix
with no type suffix.

Prefix-based snapshot: `registry.snapshot("s.1.")` returns all metrics for
store 1; `snapshot("")` returns all. This is the foundation for future GUI
integration (R11).

### Instrumentation Points

- Rust KV service (`kv_service.rs`): put/get latency histograms, scan summary,
  delete counter, bytes in/out bandwidth, error counter.
- Rust WAL (`wal_engine.rs`): append latency summary.
- Rust cluster (`local_replica.rs`): election/step-down counters, in-flight
  slots gauge. Replaces `ElectionMetrics`.
- Rust RPC (`remote_replica.rs`): per-peer RPC latency summary + error counter
  with dynamic names. Replaces `LayerMetrics`.
- C++ buffer pool (`buffer_pool.cpp`): hits/misses/evictions/writebacks
  counters, resident/dirty gauges.
- C++ engine (`crowtree.cpp`): apply latency summary.
- C++ persist (`persist.cpp`): snapshot latency summary.

### System Metrics Collector

Special collector type polled at flush time (not increment-on-event). Reads
TCP retransmits/lost (Linux `/proc/net/snmp`, macOS no-op), CPU user/sys
and memory RSS via `getrusage(RUSAGE_SELF)`. Computes CPU% as delta over
flush window.

### Metrics Log File

Dedicated file `metrics-{timestamp}-{pid}.log` in the log directory, separate
from application log. Each flush produces a block with timestamp header
`[metrics {ISO8601} window={N}s]`, followed by type-grouped sections (Counter,
LatencyHistogram, LatencySummary, Bandwidth, Gauge, System). Names sorted
alphabetically within each section, padded to `max_name_len` for alignment.
Zero-suppression: counters/histograms/summaries/bandwidth with zero window
activity are skipped; gauges always printed. Format designed for both human
reading and script parsing (split on whitespace, parse as numbers).

### In-Memory Access

`registry.snapshot(prefix)` returns current values without resetting window
state. Enables future `/metrics` HTTP endpoint and GUI integration. No need
to parse log files to get metric values.

### FFI Boundary

No metrics cross FFI at runtime. Rust registry logs Rust-side metrics; C++
registry logs C++-side metrics. Two independent log blocks per flush cycle.
The existing `ct_get_stats` FFI call (used by `/topology`) is unaffected.
