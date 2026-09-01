<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Observability

Depends on: [`design-crowdb-kv.md`](design-crowdb-kv.md) §16
Satisfies: [`design-crowdb-kv.md`](design-crowdb-kv.md) §16

## Table of Contents

- [1. Mandatory Signals](#1-mandatory-signals)
- [2. Metrics Module](#2-metrics-module)
  - [2.1 Metric Types](#21-metric-types)
  - [2.2 Registry and Lifecycle](#22-registry-and-lifecycle)
  - [2.3 Naming Convention](#23-naming-convention)
  - [2.4 Instrumentation Points](#24-instrumentation-points)
  - [2.5 System Metrics Collector](#25-system-metrics-collector)
  - [2.6 Metrics Log File](#26-metrics-log-file)
  - [2.7 In-Memory Access](#27-in-memory-access)
  - [2.8 FFI Boundary](#28-ffi-boundary)
  - [2.9 Design Principles](#29-design-principles)
  - [2.10 Read Path Metrics](#210-read-path-metrics)
  - [2.11 Write Path Metrics](#211-write-path-metrics)
  - [2.12 Client Metrics](#212-client-metrics)
  - [2.13 C++ Registry Ownership](#213-c-registry-ownership)
  - [2.14 Rust/C++ Metric Deduplication](#214-rustc-metric-deduplication)
  - [2.15 C++ flush_to Format Alignment](#215-c-flush_to-format-alignment)
  - [2.16 Shared Column Width](#216-shared-column-width)

## 1. Mandatory Signals

Per-group leader/term/max-slot/safe-slot/in-flight/gap count; per-node WAL
flush latency and throughput; per-RPC rate/latency/error breakdown; structured
logs with `node_id`, `group_id`, `slot`, `term` on consensus events. Tracing
hooks reserved but not required in the initial design.

## 2. Metrics Module

A lightweight metrics system with five metric types and periodic flush to a
dedicated metrics log file. Rust owns the registry for consensus/RPC/WAL
metrics; C++ owns its own registry for storage-engine metrics. Rust drives
the flush cycle and triggers C++ to emit its section via FFI — no metric
handles cross the FFI boundary at runtime.

### 2.1 Metric Types

- **Counter** (`AtomicU64` x 2) — monotonic, tracks window delta + total.
  `inc()` / `inc_by(n)`. Flush shows `count`, `tps`, `total`. Use cases:
  puts, gets, deletes, errors, WAL records, elections, step-downs.
- **Gauge** (`AtomicU64`) — current state, can go up or down. `set(v)`.
  Flush shows last value. Use cases: buffer pool resident/dirty pages,
  in-flight slots.
- **Bandwidth** (`AtomicU64` x 3) — monotonic bytes, tracks count + sum +
  total_bytes. `observe(bytes)`. Flush shows `count`, `tps`, `avg_size(KB)`,
  `rate(MB/s)`. Use cases: KV bytes in/out.
- **LatencyHistogram** (13 buckets + 2 `AtomicU64`) — fixed-bucket percentile
  distribution. Bucket boundaries: `0, 1us, 10us, 100us, 500us, 1ms, 5ms,
  10ms, 50ms, 100ms, 500ms, 1s, infinity`. `observe(ns)` does binary search +
  `fetch_add`. Flush computes p50/p99 from cumulative distribution. Use cases:
  KV put latency, KV get latency.
- **LatencySummary** (`AtomicU64` x 4) — lightweight latency tracking
  (count + sum + max + total_count). `observe(ns)`. Flush shows `avg(us)`,
  `max(us)`. Use cases: scan, snapshot, WAL append, RPC, apply.
- **PreciseHistogram** (`lib/crowdb-common/rust/src/metrics/precise.rs`) —
  HDR-style logarithmic histogram delivering ≥3 significant digits of
  percentile precision, the precise counterpart to the fixed-bucket
  `LatencyHistogram`. `1024` linear sub-buckets per power-of-2 magnitude
  (relative error ≤0.1%); pre-allocated range `0..=2^32 µs` (~71 min)
  makes `auto(true)` a no-op (no resize logic). `&mut self` methods
  (`record`, `add`, `reset`); every call site has exclusive access
  (`Mutex`-guarded in the client library, per-worker owned in the bench
  `OpStats`, owned by the flusher task for `CumulativeLatency`), so a
  lock-free impl is unnecessary. Tracks `count`/`sum`/`min`/`max` exactly
  (only percentiles are bucketed). Use cases: bench-client per-op
  latency (`OpStats`), client-library window latency
  (`WindowLatencySnapshot`), bench cumulative run-wide percentiles
  (`CumulativeLatency`). Replaces the external `hdrhistogram` crate.

### 2.2 Registry and Lifecycle

Each language has a `MetricsRegistry` that owns all metric instances. The
registry has `start(interval_secs)` (spawns flush thread/task), `stop()`
(final flush + join), and `flush()` (iterate all metrics, snapshot, format,
reset window state). Interval is typically 5s or 10s.

- Rust (`lib/crowdb-kv/src/metrics/mod.rs`): `MetricsRegistry` with type-grouped
  `Vec<T>` collections, `Arc`-shared, metric handles stored on service/store
  structs. `MetricsRunner` spawns a tokio interval task, computes real
  elapsed `window_secs` per tick, flushes Rust metrics, then invokes a
  post-flush callback that calls C++ `flush_metrics_str()` for each engine
  and writes the `cpp-tree` block. Also provides `snapshot(prefix)` for
  in-memory access without resetting window state.
- C++ (`lib/crowdb-tree/include/lib/crowdb-tree/metrics.h`, `lib/crowdb-tree/src/metrics.cpp`):
  Same type-grouped pattern. `Crowdbtree` owns its own `MetricsRegistry`
  internally (`init_metrics(prefix)` called from `open()`). Metric handles
  are raw pointers (registry owns lifetime). `flush_metrics_str()` captures
  `flush_to()` output via `open_memstream` for FFI return. The C++
  `start()`/`stop()` sleep-loop is retained for standalone/test use but not
  called from the server's production flush path.

### 2.3 Naming Convention

Dot-separated hierarchical paths: `s.{store_id}.g.{group_id}.{module}.{metric}`.
Type suffix on every metric name: `.c` (Counter), `.g` (Gauge), `.bw`
(Bandwidth), `.lh` (LatencyHistogram), `.l` (LatencySummary). Dynamic suffix
`@{peer_endpoint}` for per-peer metrics. System metrics use `sys.` prefix
with no type suffix.

Prefix-based snapshot: `registry.snapshot("s.1.")` returns all metrics for
store 1; `snapshot("")` returns all. This is the foundation for future GUI
integration.

### 2.4 Instrumentation Points

- Rust KV service (`kv_service.rs`): put/get latency histograms, scan summary,
  delete counter, bytes in/out bandwidth, read bytes in/out bandwidth,
  error/no-leader counters, get-forwarded / get-forward-failed counters.
- Rust WAL (`wal_engine.rs`, `pipeline_writer.rs`): append latency summary,
  fsync latency summary (thinnest-layer disk IO), write bandwidth.
- Rust cluster (`local_replica.rs`): election/step-down counters, in-flight
  slots gauge. Paxos slot watermarks (gauges bridged from `LocalReplica`).
  Engine-apply latency summary (registered here, observed in
  `learner.rs apply_entry`).
- Rust cluster (`group.rs`): read-path handles (`ReadRegistryHandles`) —
  lease/ReadIndex path counters, read barrier latency summary, engine_get
  latency summary, MinSlot-fallback counter, read-state gauges (lease valid,
  contiguous applied, safe slot). Write-path handles
  (`WriteRegistryHandles`) — propose-e2e, prepare-phase, accept-phase,
  accept-quorum-RPC latency summaries.
- Rust RPC (`remote_replica.rs`): per-peer RPC latency summary + error counter
  with dynamic names.
- C++ buffer pool (`buffer_pool.cpp`): hits/misses/evictions/writebacks
  counters, resident/dirty gauges.
- C++ engine (`crowdb-tree.cpp`): apply latency summary, demand-load latency
  summary + page-read bandwidth.
- C++ persist (`persist.cpp`): snapshot latency summary, snapshot-apply
  latency summary, per-page write IO latency + cache-hit counter + write
  bandwidth, metadata write bandwidth.
- C++ flush (`crowdb-tree.cpp`): flush latency summary, page-build (in-memory
  mutation) latency summary, flush drain/entries magnitude counters.

### 2.5 System Metrics Collector

Special collector type polled at flush time (not increment-on-event). Reads
TCP retransmits/lost (Linux `/proc/net/snmp`, macOS no-op), CPU user/sys
and memory RSS via `getrusage(RUSAGE_SELF)`. Computes CPU% as delta over
flush window.

### 2.6 Metrics Log File

Dedicated file `metrics-{timestamp}-{pid}.log` in the log directory, separate
from application log. Each flush cycle produces one shared timestamp header
`[{ISO8601} window={N.NN}s]`, then section blocks: `rust` (Rust metrics +
misc system metrics), `cpp-tree` (C++ per-engine), and `cpp-rpc` (C++ global /
crowdb-rpc). Blocks are followed by type-grouped sections in order:
LatencyHistogram, LatencySummary, Bandwidth, Counter, Gauge, then System.
Names sorted alphabetically within each section, padded to `max_name_len` for
alignment. Zero-suppression: counters/histograms/summaries/bandwidths with
zero window activity are skipped; gauges always printed. C++ `flush_to()`
output is format-aligned to Rust's column layout (same units, columns,
precision). Format designed for both human reading and script parsing (split
on whitespace, parse as numbers).

### 2.7 In-Memory Access

`registry.snapshot(prefix)` returns current values without resetting window
state. Enables future `/metrics` HTTP endpoint and GUI integration. No need
to parse log files to get metric values.

### 2.8 FFI Boundary

C++ owns its own `MetricsRegistry` per `Crowdbtree` instance. Rust triggers C++
to flush its metrics section into the same log file via FFI
(`ct_flush_metrics_str`). No metric handles cross FFI at runtime, only a
formatted string. Three log blocks per flush cycle: `rust` (Rust),
`cpp-tree` (C++ per-engine), and `cpp-rpc` (C++ global). The existing
`ct_get_stats` FFI call (used by `/topology` and the one remaining
`snapshot.pages.c` delta bridge) is unaffected.

### 2.9 Design Principles

- **Counter/Summary Non-Redundancy** — A `LatencySummary` (or
  `LatencyHistogram`) already carries `count` (window delta) and
  `total_count` (cumulative). Do not register a separate Counter for the
  same event. A Counter next to a Summary is justified only for: (a) a
  magnitude (e.g. entries moved, not calls made), (b) a different
  population/outcome (e.g. errors vs. all attempts), or (c) a cache-hit
  path where no latency is meaningful. A `Bandwidth` next to a `Latency` is
  not redundant; they measure different dimensions (bytes vs. nanoseconds)
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

### 2.10 Read Path Metrics

The read path mirrors the write path's latency-bandwidth-counter hierarchy.
Handles live in two places: `KvMetrics` (per store, group; in
`kv_service.rs`) for RPC-layer metrics, and `ReadRegistryHandles` (per
store, group; on `PxGroup` via `OnceLock`, mirroring
`ElectionRegistryHandles` on `PxLocalReplica`) for consensus- and
engine-layer metrics.

- **Latency hierarchy** (feature layer → thinnest layer):
  - `kv.get.lh` — get RPC end-to-end (existing).
  - `read.e2e.l` — `LatencySummary` for the full server-side read path
    (barrier + engine_get), measured in `kv_get` handler.
  - `read.barrier.l` — `LatencySummary` for
    `linearizable_read_barrier` (near-zero for lease path, one heartbeat
    RTT for ReadIndex).
  - `read.apply_fence.l` — `LatencySummary` for the R35 apply fence
    wait (fast path is a single atomic load).
  - `read.engine_get.l` — `LatencySummary` for `KVEngine::get_bytes`
    (isolates engine cost from consensus barrier cost).
  - `kv.scan.l` — scan RPC end-to-end (existing).
- **Bandwidth hierarchy** (read vs. write separation; combined kept for
  backward compat, read subset lets operators derive write by subtraction):
  - `kv.read_bytes_in.bw` / `kv.read_bytes_out.bw` — read traffic
    separated from the combined `bytes_in/out.bw`.
- **Counters** (outcome / population separation):
  - `kv.get_forwarded.c` — reads forwarded to leader (server-side).
  - `kv.get_forward_failed.c` — forward attempts that failed.
  - `read.minslot_fallback.c` — MinSlot reads redirected to leader
    because the local replica hasn't caught up.
- **Gauges** (state, bridged on-demand at `resolve_read_point`):
  - `read.safe_slot.g` — current `group_safe_slot`.

`read.barrier.l` count equals the total linearizable get count in the
same window (lease fast path + ReadIndex path combined).

### 2.11 Write Path Metrics

The write path mirrors the read path's latency hierarchy. Handles live in
`WriteRegistryHandles` (per store, group; on `PxGroup` via `OnceLock`,
mirroring `ReadRegistryHandles`), except `engine_apply` which is registered
on `PxLocalReplica` (alongside the WAL handles) and observed in
`PxLearner::apply_entry`.

- **Latency hierarchy** (feature layer → thinnest layer):
  - `paxos.propose.e2e.l` — `propose_inner` entry → return (the full
    client-observed proposal latency, including retries).
  - `paxos.classic.prepare.l` — `run_prepare_phase` entry → return
    (classic Paxos only; leader path skips prepare).
  - `paxos.accept.quorum_rpc.l` — accept-phase start → first-quorum
    reached (the k-th-fastest remote reply latency; recorded only on the
    quorum short-circuit success path, not on the failure path).
  - `paxos.learn.apply.l` — `PxLearner::apply_entry` entry → return
    (isolates engine apply cost from consensus phase cost).

All five are `LatencySummary` (count + sum + max), matching the read-path
`barrier` / `engine_get` pattern. Percentile precision is not needed for
phase-level attribution. The bench client already has `PreciseHistogram`
for client-observed p99. The `accept_quorum_rpc` timer is meaningful only
after the quorum short-circuit (§6.1 of `design-crowdb-kv-rpc.md`); it records the
quorum-th-fastest remote latency, not the full fan-out tail.

### 2.12 Client Metrics

`crowdb-kv-client` exposes its own `ClientMetrics` (lock-free `AtomicU64`
counters, snapshotted via `ClientMetricsSnapshot`) for retry, topology,
and read-distribution observability. They are not part of the server's
`MetricsRegistry`. The client is a separate process and has no C++
handle. Two counters cover follower-read distribution:

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

The client library also maintains per-op-kind window latency histograms
(`WindowLatency` / `WindowLatencySnapshot`), drained by `drain_window`
for periodic flushing and merged into the bench runner's cumulative
`bench.*.lh` histograms. These use `crowdb-common`'s `PreciseHistogram`
(not the server's fixed-bucket `LatencyHistogram`) because the bench
needs p50/p90/p99/p999/max at ≥3 significant digits, the same precision
the bench's own `OpStats` and `CumulativeLatency` require. The bench
runner's per-worker `WorkerCounters` use `crowdb-common`'s `Counter`
(window delta via `flush().count`, cumulative total via
`snapshot().total`), unifying the bench and client on the project's own
metrics primitives and removing the external `hdrhistogram` dependency.

### 2.13 C++ Registry Ownership

C++ `Crowdbtree` creates its own `MetricsRegistry` internally via
`init_metrics(prefix)`, called from `Crowdbtree::open()`. The external
`set_metrics(MetricsRegistry*, prefix)` API is removed. C++ metrics
(counters, gauges, summaries, bandwidths) are registered and observed
entirely in C++, then flushed to string via `flush_metrics_str()` and
written to the log by the Rust `MetricsRunner` post-flush callback.

### 2.14 Rust/C++ Metric Deduplication

Once C++ owns its registry, the Rust-side bridge (`engine_collector.rs`)
no longer polls C++ cumulative counters and gauges. Those metrics appear
natively in `cpp-tree`. The Rust `rust` section keeps only
Rust-native metrics (KV service, RPC, Paxos, WAL). The one exception is
`snapshot.pages.c`, a magnitude counter with no paired latency in the C++
registry, which remains bridged via `ct_get_stats` delta polling.

### 2.15 C++ `flush_to` Format Alignment

C++ `flush_to()` output is aligned to Rust's column layout: `tps(/s)`
column on all windowed types, latency in `us` (not `ns`), bandwidth in KB,
Histogram column order matching Rust, and `window=%.2fs` precision. The
section header label is parameterized (`"metrics"` for standalone,
`"cpp-tree"` for per-engine FFI-driven flush, `"cpp-rpc"` for global
FFI-driven flush).

### 2.16 Shared Column Width

Both `rust` and `cpp-tree` sections use the same column width
for metric names. Before each flush tick, Rust queries each C++ engine's
max name length via `ct_max_name_len()`, computes
`shared_width = max(rust_max, max(cpp_maxes))`, and passes it to both its
own `flush()` and C++ `flush_metrics_str()`. This adapts automatically as
handles are added dynamically.
