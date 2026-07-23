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
dedicated metrics log file. Rust owns the registry for consensus/RPC/WAL
metrics; C++ owns its own registry for storage-engine metrics. Rust drives
the flush cycle and triggers C++ to emit its section via FFI — no metric
handles cross the FFI boundary at runtime.

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
  structs. `MetricsRunner` spawns a tokio interval task, computes real
  elapsed `window_secs` per tick, flushes Rust metrics, then invokes a
  post-flush callback that calls C++ `flush_metrics_str()` for each engine
  and writes the `[cpp-metrics]` block. Also provides `snapshot(prefix)` for
  in-memory access without resetting window state.
- C++ (`crowtree/include/crowtree/metrics.h`, `crowtree/src/metrics.cpp`):
  Same type-grouped pattern. `Crowtree` owns its own `MetricsRegistry`
  internally (`init_metrics(prefix)` called from `open()`). Metric handles
  are raw pointers (registry owns lifetime). `flush_metrics_str()` captures
  `flush_to()` output via `open_memstream` for FFI return. The C++
  `start()`/`stop()` sleep-loop is retained for standalone/test use but not
  called from the server's production flush path.

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
  delete counter, bytes in/out bandwidth, read bytes in/out bandwidth,
  error/no-leader counters, get-forwarded / get-forward-failed counters.
- Rust WAL (`wal_engine.rs`, `pipeline_writer.rs`): append latency summary,
  fsync latency summary (thinnest-layer disk IO), write bandwidth.
- Rust cluster (`local_replica.rs`): election/step-down counters, in-flight
  slots gauge. Paxos slot watermarks (gauges bridged from `LocalReplica`).
- Rust cluster (`group.rs`): read-path handles (`ReadRegistryHandles`) —
  lease/ReadIndex path counters, read barrier latency summary, engine_get
  latency summary, MinSlot-fallback counter, read-state gauges (lease valid,
  contiguous applied, safe slot).
- Rust RPC (`remote_replica.rs`): per-peer RPC latency summary + error counter
  with dynamic names.
- C++ buffer pool (`buffer_pool.cpp`): hits/misses/evictions/writebacks
  counters, resident/dirty gauges.
- C++ engine (`crowtree.cpp`): apply latency summary, demand-load latency
  summary + page-read bandwidth.
- C++ persist (`persist.cpp`): snapshot latency summary, snapshot-apply
  latency summary, per-page write IO latency + cache-hit counter + write
  bandwidth, metadata write bandwidth.
- C++ flush (`crowtree.cpp`): flush latency summary, page-build (in-memory
  mutation) latency summary, flush drain/entries magnitude counters.

### System Metrics Collector

Special collector type polled at flush time (not increment-on-event). Reads
TCP retransmits/lost (Linux `/proc/net/snmp`, macOS no-op), CPU user/sys
and memory RSS via `getrusage(RUSAGE_SELF)`. Computes CPU% as delta over
flush window.

### Metrics Log File

Dedicated file `metrics-{timestamp}-{pid}.log` in the log directory, separate
from application log. Each flush cycle produces two blocks: `[metrics ...]`
(Rust) and `[cpp-metrics ...]` (C++), both with the same timestamp and
`window={N.NN}s` header (2 decimal places, real elapsed time). Blocks are
followed by type-grouped sections (Counter, LatencyHistogram, LatencySummary,
Bandwidth, Gauge, System). Names sorted alphabetically within each section,
padded to `max_name_len` for alignment. Zero-suppression:
counters/histograms/summaries/bandwidths with zero window activity are
skipped; gauges always printed. C++ `flush_to()` output is format-aligned to
Rust's column layout (same units, columns, precision). Format designed for
both human reading and script parsing (split on whitespace, parse as
numbers).

### In-Memory Access

`registry.snapshot(prefix)` returns current values without resetting window
state. Enables future `/metrics` HTTP endpoint and GUI integration. No need
to parse log files to get metric values.

### FFI Boundary

C++ owns its own `MetricsRegistry` per `Crowtree` instance. Rust triggers C++
to flush its metrics section into the same log file via FFI
(`ct_flush_metrics_str`). No metric handles cross FFI at runtime — only a
formatted string. Two log blocks per flush cycle: `[metrics]` (Rust) and
`[cpp-metrics]` (C++). The existing `ct_get_stats` FFI call (used by
`/topology` and the one remaining `snapshot.pages.c` delta bridge) is
unaffected.

### Design Principles

- **Counter/Summary Non-Redundancy** — A `LatencySummary` (or
  `LatencyHistogram`) already carries `count` (window delta) and
  `total_count` (cumulative). Do not register a separate Counter for the
  same event. A Counter next to a Summary is justified only for: (a) a
  magnitude (e.g. entries moved, not calls made), (b) a different
  population/outcome (e.g. errors vs. all attempts), or (c) a cache-hit
  path where no latency is meaningful. A `Bandwidth` next to a `Latency` is
  not redundant — they measure different dimensions (bytes vs. nanoseconds)
  of the same event.
- **Latency Hierarchy** — For every flow, add latency at the thinnest layer
  (actual syscall / disk IO / RPC boundary) and at the feature layer above
  it (the meaningful unit of work a human debugs against). Children should
  roughly sum to the parent; a persistent gap indicates an unaccounted code
  path. Skip a layer if it isn't a distinct decision point.
- **Bandwidth Hierarchy** — Parallel to latency: measure bytes moved at the
  IO boundary, domain-separated (page data vs. metadata, read vs. write).
  Skip layers with no distinct IO.
- **Real Window-Time** — The flush window is the real elapsed time, not the
  nominal configured interval. Rust computes `window_secs` once per tick
  and passes the same value to both its own `reg.flush()` and the C++ FFI
  call, guaranteeing identical windows across both sections.

### Read Path Metrics

The read path mirrors the write path's latency-bandwidth-counter hierarchy.
Handles live in two places: `KvMetrics` (per store, group; in
`kv_service.rs`) for RPC-layer metrics, and `ReadRegistryHandles` (per
store, group; on `PxGroup` via `OnceLock`, mirroring
`ElectionRegistryHandles` on `PxLocalReplica`) for consensus- and
engine-layer metrics.

- **Latency hierarchy** (feature layer → thinnest layer):
  - `kv.get.lh` — get RPC end-to-end (existing).
  - `read.barrier.l` — `LatencySummary` for
    `linearizable_read_barrier` (near-zero for lease path, one heartbeat
    RTT for ReadIndex).
  - `read.engine_get.l` — `LatencySummary` for `KVEngine::get_bytes`
    (isolates engine cost from consensus barrier cost).
  - `kv.scan.l` — scan RPC end-to-end (existing).
- **Bandwidth hierarchy** (read vs. write separation; combined kept for
  backward compat, read subset lets operators derive write by subtraction):
  - `kv.read_bytes_in.bw` / `kv.read_bytes_out.bw` — read traffic
    separated from the combined `bytes_in/out.bw`.
- **Counters** (outcome / population separation):
  - `read.lease_path.c` — linearizable reads via lease fast path.
  - `read.readindex_path.c` — linearizable reads via ReadIndex fallback.
  - `kv.get_forwarded.c` — reads forwarded to leader (server-side).
  - `kv.get_forward_failed.c` — forward attempts that failed.
  - `read.minslot_fallback.c` — MinSlot reads redirected to leader
    because the local replica hasn't caught up.
- **Gauges** (state, bridged on-demand at `resolve_read_point` — same
  pattern as `inflight_slots.g`):
  - `read.lease_valid.g` — 1 if leader's read lease is valid at the most
    recent barrier, 0 otherwise.
  - `read.contiguous_applied.g` — current `contiguous_applied`.
  - `read.safe_slot.g` — current `group_safe_slot`.

`read.lease_path.c + read.readindex_path.c` equals the total linearizable
get count in the same window. The path counters are outcome counters (which
path served the read), not call counters — `read.barrier.l` already carries
the total call count. This follows the counter/summary non-redundancy
principle (justified under "different population/outcome").

### Client Metrics

`crowkv-client` exposes its own `ClientMetrics` (lock-free `AtomicU64`
counters, snapshotted via `ClientMetricsSnapshot`) for retry, topology,
and read-distribution observability. They are not part of the server's
`MetricsRegistry` — the client is a separate process and has no C++
handle. Two counters cover R26 follower-read distribution:

- `read_endpoint_distributed` — incremented each time the `AnyReplica`
  selector picks a replica from the cached list for a `MinSlot` read
  (i.e. the distribution branch fired). Pairs with the server-side
  `read.minslot_fallback.c` to confirm the distribution rate and the
  fallback rate.
- `read_endpoint_fallback` — incremented each time a distributed
  `MinSlot` read fell back to the leader via `NotLeaderHint` (get) or
  the scan `"not leader; retry scan at "` parse. A high
  `read_endpoint_fallback / read_endpoint_distributed` ratio means
  followers are lagging and the policy is providing little benefit.

### C++ Registry Ownership

C++ `Crowtree` creates its own `MetricsRegistry` internally via
`init_metrics(prefix)`, called from `Crowtree::open()`. The external
`set_metrics(MetricsRegistry*, prefix)` API is removed. C++ metrics
(counters, gauges, summaries, bandwidths) are registered and observed
entirely in C++, then flushed to string via `flush_metrics_str()` and
written to the log by the Rust `MetricsRunner` post-flush callback.

### Rust/C++ Metric Deduplication

Once C++ owns its registry, the Rust-side bridge (`engine_collector.rs`)
no longer polls C++ cumulative counters and gauges — those metrics appear
natively in `[cpp-metrics]`. The Rust `[metrics]` section keeps only
Rust-native metrics (KV service, RPC, Paxos, WAL). The one exception is
`snapshot.pages.c`, a magnitude counter with no paired latency in the C++
registry, which remains bridged via `ct_get_stats` delta polling.

### C++ `flush_to` Format Alignment

C++ `flush_to()` output is aligned to Rust's column layout: `tps(/s)`
column on all windowed types, latency in `us` (not `ns`), bandwidth in KB,
Histogram column order matching Rust, and `window=%.2fs` precision. The
section header label is parameterized (`"metrics"` for standalone,
`"cpp-metrics"` for FFI-driven flush).

### Shared Column Width

Both `[metrics]` and `[cpp-metrics]` sections use the same column width
for metric names. Before each flush tick, Rust queries each C++ engine's
max name length via `ct_max_name_len()`, computes
`shared_width = max(rust_max, max(cpp_maxes))`, and passes it to both its
own `flush()` and C++ `flush_metrics_str()`. This adapts automatically as
handles are added dynamically.
