# Plan: Metrics Module + Periodic Logging (R8)

Design doc: `doc/working/design-metrics.md`

## Prerequisite Task: Rename Partition→Group in crowtree

Rename `partition_id` → `group_id` across crowtree C++ and FFI for
consistency with the metrics naming convention (`s.{store_id}.g.{group_id}`).

Files:
- `crowtree/include/crowtree/c_api.h` — `ct_options::partition_id` → `group_id`
- `crowtree/include/crowtree/block_page_store.h` — `partition_id_` member,
  `open_blocks` param, constructor param, comments
- `crowtree/include/crowtree/text_page_store.h` — `open` param, comments
- `crowtree/src/c_api.cpp` — `opt->partition_id` → `opt->group_id`
- `crowtree/src/block_page_store.cpp` — all `partition_id` references,
  `parse_block_filename` param name, filename comments
- `crowtree/src/text_page_store.cpp` — `open` param, directory path comment
- `crowtree/ffi/src/lib.rs` — `ct_options::partition_id` → `group_id`,
  `CrowtreeOptions::partition_id` → `group_id`, comment, field mapping
- `crowkv-server/src/startup.rs` — `partition_id:` → `group_id:` in
  `ct_options` construction, comment

Note: on-disk block filename format `{store_id}-{partition_id}.blk-*` does
NOT change — existing files remain compatible. Only code-level naming
changes.

- [x] Rename partition→group in crowtree C++ headers
- [x] Rename partition→group in crowtree C++ source files
- [x] Rename partition→group in crowtree FFI Rust bindings
- [x] Rename partition→group in crowkv-server startup
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes
- [x] `pixi exec clang-format --dry-run --Werror` on changed .cpp/.h passes
- [x] `pixi run test-ct` passes
- [ ] Commit

## Task 1: Rust metric types (core primitives)

Implement the five metric types as standalone modules with no registry
dependency. Each type is a struct of `AtomicU64`s with `inc()`/`observe()`/
`set()` methods and a `flush()` method that returns a snapshot struct.

Files:
- New: `crowkv/src/metrics/counter.rs` — `Counter` (window + total) and
  `Gauge` (current value)
- New: `crowkv/src/metrics/bandwidth.rs` — `Bandwidth` (count + sum +
  total_bytes)
- New: `crowkv/src/metrics/histogram.rs` — `LatencyHistogram` (13 buckets
  + count + total_count), bucket boundaries as const array, binary search
  for bucket index, p50/p99 computation from cumulative distribution
- New: `crowkv/src/metrics/summary.rs` — `LatencySummary` (count + sum +
  max + total_count)

Each type exposes:
- Constructor taking a name `&'static str` or `Arc<str>`
- Hot-path method(s): `inc()`, `inc_by(n)`, `set(v)`, `observe(ns)`,
  `observe(bytes)`
- `flush(window_secs) -> Snapshot` struct that swaps window state to 0
  and returns computed values
- `name() -> &str`

- [x] Implement `Counter` and `Gauge` in `counter.rs`
- [x] Implement `Bandwidth` in `bandwidth.rs`
- [x] Implement `LatencyHistogram` in `histogram.rs` (bucket boundaries,
  binary search, p50/p99)
- [x] Implement `LatencySummary` in `summary.rs`
- [x] Unit tests: counter window+total, gauge set/get, bandwidth
  avg+rate, histogram p50/p99 with known distribution, summary avg+max
  reset
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes
- [ ] Commit (accumulate with later tasks — single commit per task per
  AGENTS.md)

## Task 2: Rust MetricsRegistry

Implement the registry that owns all metric instances, tracks
`max_name_len` per type group, and provides `register_*()` methods
returning handles.

Files:
- New: `crowkv/src/metrics/mod.rs` — `MetricsRegistry` struct with
  `Vec<Counter>`, `Vec<Gauge>`, `Vec<Bandwidth>`, `Vec<LatencyHistogram>`,
  `Vec<LatencySummary>`, plus `Vec<SystemCollector>` (added in Task 5)
- `register_counter(name) -> Arc<Counter>`, same for other types
- `max_name_len` updated atomically on each registration per type group
- `snapshot(prefix) -> Vec<MetricSnapshot>` — iterates all type
  collections, filters by name prefix, returns snapshot without resetting
  window state
- `flush(writer, window_secs)` — iterates each type collection in order
  (Counter, LatencyHistogram, LatencySummary, Bandwidth, Gauge, System),
  calls `flush()` on each metric, formats output per design doc log
  format, writes to the provided writer
- Modify: `crowkv/src/lib.rs` — add `pub mod metrics`

Flush formatting:
- Header: `[metrics {ISO8601_timestamp} window={N}s]`
- Each type section: `name` header line padded to `max_name_len`, then
  data lines sorted by name, values as plain numbers
- No blank lines between sections
- `misc` line before system section
- Trailing blank line after the entire flush block

- [x] Implement `MetricsRegistry` struct with type-grouped collections
- [x] Implement `register_*()` methods returning `Arc<T>` handles
- [x] Implement `snapshot(prefix)` for in-memory access
- [x] Implement `flush()` with column-aligned output per design doc
- [x] Add `pub mod metrics` to `lib.rs`
- [x] Unit tests: snapshot prefix filtering, flush output format
  (capture to String, verify header, section order, alignment, zero-
  suppression, misc delimiter)
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes

## Task 3: Rust registry lifecycle (start/stop)

Wire the registry into the server lifecycle: spawn a tokio interval task,
align flush timing to interval boundaries, stop on shutdown.

Files:
- Modify: `crowkv/src/metrics/mod.rs` — add `start(interval_secs)` and
  `stop()` methods
  - `start()`: spawns `tokio::time::interval` task, calls `flush()` on
    each tick. Interval starts immediately so first tick aligns to the
    interval boundary.
  - `stop()`: cancels the task, does a final flush
- Modify: `crowkv/src/common/logging.rs` — add metrics log file
  initialization (separate file with timestamp+pid in filename, same
  pattern as existing log setup)
- Modify: `crowkv-server/src/main.rs` — create `Arc<MetricsRegistry>`,
  call `start(interval)`, pass registry to services, call `stop()` in
  shutdown handler

- [x] Implement `start()` / `stop()` with tokio interval task
- [x] Add metrics log file initialization in `logging.rs`
- [x] Wire registry creation + start/stop in `main.rs`
- [x] Integration test: start registry with 1s interval, wait 3s, verify
  2+ flush blocks in log output, stop, verify final flush
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes

## Task 4: Rust instrumentation — KV service

Instrument `put`, `get`, `delete`, `scan` handlers in `kv_service.rs`.
Register metrics at store creation time, store handles on the service
struct.

Files:
- Modify: `crowkv/src/rpc/kv_service.rs` — add metric handles to
  `KvService` struct, instrument handlers:
  - `s.{id}.kv.put.lh` — LatencyHistogram, observe in put handler
  - `s.{id}.kv.get.lh` — LatencyHistogram, observe in get handler
  - `s.{id}.kv.delete.c` — Counter, inc in delete handler
  - `s.{id}.kv.scan.l` — LatencySummary, observe in scan handler
  - `s.{id}.kv.bytes_in.bw` — Bandwidth, observe(request_size) in all
    handlers
  - `s.{id}.kv.bytes_out.bw` — Bandwidth, observe(response_size) in all
    handlers
  - `s.{id}.kv.errors.c` — Counter, inc on error paths

- [x] Add metric handle fields to `KvService` (or store-level struct)
- [x] Register metrics at store/group creation time
- [x] Instrument put handler: histogram observe + bandwidth in/out
- [x] Instrument get handler: histogram observe + bandwidth in/out
- [x] Instrument delete handler: counter inc + bandwidth in
- [x] Instrument scan handler: summary observe + bandwidth in/out
- [x] Instrument error paths: error counter inc
- [x] Existing `kv_service` tests pass
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes

## Task 5: Rust instrumentation — WAL + cluster

Instrument WAL append, election metrics, and RPC metrics. Replace
`LayerMetrics` and `ElectionMetrics` with registry handles.

Files:
- Modify: `crowkv/src/wal/wal_engine.rs` — add `s.{id}.g.{gid}.wal.append.l`
  LatencySummary, observe in append path
- Modify: `crowkv/src/cluster/local_replica.rs` — replace `ElectionMetrics`
  with:
  - `s.{id}.g.{gid}.paxos.elections.c` — Counter
  - `s.{id}.g.{gid}.paxos.step_downs.higher_term.c` — Counter
  - `s.{id}.g.{gid}.paxos.step_downs.lease.c` — Counter
  - `s.{id}.g.{gid}.paxos.step_downs.admin.c` — Counter
  - `s.{id}.g.{gid}.paxos.inflight_slots.g` — Gauge
- Modify: `crowkv/src/cluster/remote_replica.rs` — replace `LayerMetrics`
  with:
  - `s.{id}.g.{gid}.rpc.l@{peer}` — LatencySummary (dynamic name)
  - `s.{id}.g.{gid}.rpc.errors.c@{peer}` — Counter (dynamic name)
- Modify: `crowkv/src/common/metrics.rs` — delete `LayerMetrics` and
  `ElectionMetrics` structs
- Modify: any code reading old `snapshot()` methods (e.g., `/topology`
  endpoint) — switch to `registry.snapshot()`

- [x] Instrument WAL append path with LatencySummary
- [x] Replace ElectionMetrics with registry Counters + Gauge
- [x] Replace LayerMetrics with registry LatencySummary + Counter
  (dynamic names)
- [x] Update `/topology` endpoint to read from registry.snapshot()
- [x] Delete `crowkv/src/common/metrics.rs`
- [x] All existing tests pass
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes

## Task 6: Rust system metrics collector

Implement OS-level metrics polling at flush time.

Files:
- New: `crowkv/src/metrics/system.rs` — `SystemCollector` trait/struct:
  - TCP retransmits/lost: read `/proc/net/snmp` on Linux, no-op on macOS
  - CPU user/sys: `getrusage(RUSAGE_SELF)`, compute delta over window
  - Memory RSS: `getrusage` max RSS
  - `collect() -> SystemSnapshot` called at flush time
- Modify: `crowkv/src/metrics/mod.rs` — register system collectors,
  flush them in the `misc` section

- [x] Implement `SystemCollector` with `getrusage` for CPU/memory
- [x] Implement TCP stats reading on Linux (`/proc/net/snmp`)
- [x] macOS no-op / stub for TCP stats
- [x] Wire system collector into registry flush (misc section)
- [x] Unit test: CPU delta computation, RSS reading (platform-specific)
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes

## Task 7: C++ metric types + registry

Implement the C++ metrics module mirroring the Rust design.

Files:
- New: `crowtree/include/crowtree/metrics.h` — `Counter`, `Gauge`,
  `Bandwidth`, `LatencyHistogram`, `LatencySummary`, `SystemCollector`,
  `MetricsRegistry` class with type-grouped `std::vector<T*>`,
  `register_*()` methods, `start(interval)` / `stop()` (std::thread),
  `flush()` method
- New: `crowtree/src/metrics.cpp` — implementation
- Modify: `crowtree/CMakeLists.txt` — add `metrics.cpp` to sources

- [x] Implement C++ `Counter`, `Gauge`, `Bandwidth` (std::atomic<uint64_t>)
- [x] Implement C++ `LatencyHistogram` (13 buckets, binary search)
- [x] Implement C++ `LatencySummary` (count/sum/max/total_count)
- [x] Implement C++ `MetricsRegistry` with type-grouped storage
- [x] Implement `start()` / `stop()` with std::thread + sleep_for
- [x] Implement `flush()` with same log format as Rust
- [x] Implement `snapshot(prefix)` for future FFI access
- [x] Add `metrics.cpp` to CMakeLists.txt
- [x] C++ unit tests: counter window+total, histogram p50/p99, summary
  avg+max, flush format, snapshot prefix
- [x] `pixi exec clang-format --dry-run --Werror` on changed .cpp/.h
- [x] `pixi run test-ct` passes

## Task 8: C++ instrumentation

Instrument buffer pool, apply, and snapshot paths in the C++ engine.

Files:
- Modify: `crowtree/src/buffer_pool.cpp` — instrument `pin()` / `pin_new()`:
  - `s.{id}.g.{gid}.buf.hits.c` — Counter
  - `s.{id}.g.{gid}.buf.misses.c` — Counter
  - `s.{id}.g.{gid}.buf.evictions.c` — Counter
  - `s.{id}.g.{gid}.buf.writebacks.c` — Counter
  - `s.{id}.g.{gid}.buf.resident.g` — Gauge
  - `s.{id}.g.{gid}.buf.dirty.g` — Gauge
- Modify: `crowtree/src/crowtree.cpp` — instrument `apply_put()` /
  `apply_delete()`:
  - `s.{id}.g.{gid}.apply.l` — LatencySummary
- Modify: `crowtree/src/persist.cpp` — instrument `snapshot()`:
  - `s.{id}.g.{gid}.snapshot.l` — LatencySummary

The C++ registry needs store_id and group_id to construct metric names.
These are passed from the Rust side via the existing `ct_options` struct
(after the partition→group rename in the prerequisite task).

- [x] Add MetricsRegistry to Crowtree engine (owned by the tree handle)
- [x] Instrument buffer pool pin/pin_new with counters + gauges
- [x] Instrument apply_put/apply_delete with LatencySummary
- [x] Instrument snapshot with LatencySummary
- [x] C++ tests pass with instrumentation
- [x] `pixi exec clang-format --dry-run --Werror` on changed .cpp/.h
- [x] `pixi run test-ct` passes

## Task 9: Integration test + final verification

End-to-end verification that both Rust and C++ metrics flush to the
metrics log file with correct format.

Files:
- New: `crowkv/tests/metrics_test.rs` — integration tests:
  - Registry start/stop lifecycle (interval flush, final flush)
  - Counter window reset + total accumulation across flushes
  - LatencyHistogram p50/p99 with known distribution
  - LatencySummary avg/max + reset
  - Gauge last-value reporting
  - Bandwidth count/avg_size/rate + reset
  - Dynamic-name metric registration and flush output
  - Snapshot prefix filtering
  - Flush output format: header, section order, alignment, zero-
    suppression, misc delimiter, no blank lines between sections
- New: `crowtree/tests/unit/metrics_test.cpp` — C++ unit tests (if not
  already added in Task 7)

- [x] Write Rust integration tests
- [x] Write C++ unit tests (if not already done)
- [x] All tests pass: `pixi run cargo test` + `pixi run test-ct`
- [x] `pixi run cargo fmt --all -- --check` passes
- [x] `pixi run cargo clippy --all-targets -- -D warnings` passes
- [x] `pixi exec clang-format --dry-run --Werror` on all changed .cpp/.h
- [ ] Commit (single commit for the entire metrics task)

## Test Checklist

Rust:
- [x] Counter: inc → flush → delta=0, total=N; inc more → flush → delta=M,
  total=N+M
- [x] Gauge: set(42) → flush → value=42; set(0) → flush → value=0
- [x] Bandwidth: observe(100) × 10 → flush → count=10, avg_size=100,
  rate=2000 (for 0.5s window)
- [x] Histogram: 100 observations at 500µs → p50≈500, p99≈500
- [x] Summary: observe(100), observe(200), observe(300) → flush →
  avg=200, max=300; next flush → avg=0, max=0 (reset)
- [x] Registry: start(1) → wait 3s → 2+ flush blocks → stop → 1 final
  flush
- [x] Snapshot: `snapshot("s.1.")` returns only s.1.* metrics;
  `snapshot("")` returns all
- [x] Dynamic name: register `rpc.l@10.0.0.2:20002` → appears in flush
- [x] Flush format: verify header, section order, no blank lines between
  sections, misc delimiter, trailing blank line
- [x] Zero-suppression: counter with 0 inc in window → not printed;
  gauge with value 0 → printed

C++:
- [x] Counter: same window+total behavior as Rust
- [x] Histogram: same p50/p99 behavior
- [x] Summary: same avg/max behavior
- [x] Registry: start/stop lifecycle with std::thread
- [x] Flush format: matches Rust format

## Dependency Ordering

```
Prerequisite (Partition→Group rename)
  └→ Task 1 (Rust metric types)
       └→ Task 2 (Rust registry)
            └→ Task 3 (Rust lifecycle)
                 ├→ Task 4 (KV instrumentation)
                 ├→ Task 5 (WAL + cluster instrumentation)
                 └→ Task 6 (System metrics)
                      └→ Task 9 (Integration test)
  └→ Task 7 (C++ metric types + registry)
       └→ Task 8 (C++ instrumentation)
            └→ Task 9 (Integration test)
```

Tasks 1-6 (Rust) and Task 7-8 (C++) can proceed in parallel after the
prerequisite. Task 9 requires all others complete.
