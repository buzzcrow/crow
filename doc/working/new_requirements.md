<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

### Medium Priority

**Complexity — Medium:**
- **R2** — Persistent node config — Area: crowkv-server — Per-node server config
  is not persisted; a restart relies on the console to re-push topology, making
  standalone startup non-deterministic.
- **R12** — Crow Common shared project — Area: workspace — Extract a
  standalone `crow-common` project with a Rust crate and a C++ static
  library. Move reusable utilities (metrics core, logging wrapper, CRC32C,
  time helpers, operation report) out of `crowkv`/`crowtree` so future
  storage-system components can share them without re-implementing.
- **R10** — Benchmark framework — Area: console CLI — Add benchmark capability
  to console CLI; run single-node benchmarks (in-memory or no-fsync file mode)
  to identify system bottlenecks and inform performance tuning. Related to R3,
  R4, R6 (memory efficiency optimizations).
- **R11** — GUI internal state display — Area: web UI — Surface internal
  metrics (from R8) in the GUI via existing health/internal-state query
  infrastructure. Show recent operation counts and metrics per Store/Group
  with real-time refresh (5–10 s window).
- **R13** — Unify bench client stats with metrics library — Area: console CLI
  / metrics — Benchmark client-side statistics (`OpStats`, `WorkerCounters`
  in `bench/runner.rs`) currently use a hand-rolled `hdrhistogram` + manual
  `AtomicU64` counters instead of crowkv's own `MetricsRegistry` /
  `LatencyHistogram` / `Counter` classes. After R12 extracts metrics into
  `crow-common`, the bench client should reuse the same metrics primitives
  for consistency and to eliminate duplicate statistical infrastructure.

### Low Priority

**Complexity — Low (placeholder):**
- **R5** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **R3** — Zero-copy FFI write path — Area: crowtree FFI — `ct_apply_put` copies
  key+value into an internal buffer; for large values this memcpy is avoidable
  via a direct-write alloc handle.
- **R4** — Bounded memory pool — Area: crowtree engine — `buffer::allocate` uses
  unbounded `std::malloc`; a burst of large writes can spike RSS without
  backpressure.

**Complexity — High:**
- **R6** — Cross-thread EpochManager::Guard — Area: crowtree engine —
  `EpochManager::Guard` is thread-bound, forcing copies in async read handoff,
  snapshot consistency, and stale-root GC scenarios.

---

## Detailed Analysis

### R2: Persistent node config

**Problem**: Cluster config (racks, nodes, stores, groups, replicas) is
managed in-memory by the console and persisted via `crowkv-console-db.toml`.
The per-node server config is not persisted independently — a node restart
relies on the console to re-push topology. A per-node config file would make
standalone startup deterministic.

**Priority**: Medium — may cause UT bugs as-is; console-less deployments need
it.

**Complexity**: Medium — design a per-node config format, load at startup,
reconcile with runtime API changes.

**Files**: `crowkv-server/src/main.rs`, `crowkv-server/src/store_registry.rs`,
new config module.

**Acceptance**: Node starts with config file, creates stores/groups/replicas
without console intervention. Config file survives restart.

---

### R3: Zero-copy FFI write path

**Problem**: `ct_apply_put` copies key+value from the caller's buffer into an
internal `buffer`. For large values this memcpy is avoidable.

**Approach**: Expose `ct_alloc(key_len, val_len)` returning a writable
pointer + handle. The caller writes key/value directly into crowtree-owned
memory. `ct_apply_owned(tree, slot, handle)` consumes it via
`buffer::move_from` — zero memcpy. `ct_free(handle)` for the error path.
`header_reserve` stays internal; the C API hides it.

**Priority**: Low — optimization, no current profiling motivation.

**Complexity**: Medium — new C API surface, Rust FFI adapter changes,
lifetime/lifecycle of allocated-but-not-applied handles.

**Files**: `crowtree/include/crowtree/c_api.h`, `crowtree/src/c_api.cpp`,
`crowtree/include/crowtree/buffer.h`, `crowtree/ffi/src/lib.rs`

**Acceptance**: Benchmark showing zero memcpy on apply path for large values
(>4 KiB). Existing tests pass. New unit test for alloc/apply/free lifecycle.

---

### R4: Bounded memory pool for `buffer` allocations

**Problem**: `buffer::allocate` uses unbounded `std::malloc`. When crowtree is
embedded in crowkv-server, a burst of large writes can spike RSS without
backpressure.

**Approach**: Admission control at `Crowtree::apply()`/`apply_batch()` entry
via `Options.mem_budget_bytes` (0 = unlimited). Track outstanding buffer
bytes atomically; reject with `Status::resource_exhausted()` when over
budget. Flush/snapshot path is exempt (must always succeed).

**Priority**: Low — needed only when memory-bound deployment is a
requirement.

**Complexity**: Medium — atomic counter, budget accounting in apply path,
test for rejection + recovery.

**Files**: `crowtree/include/crowtree/options.h`,
`crowtree/include/crowtree/buffer.h`, `crowtree/src/crowtree.cpp`

**Acceptance**: Unit test: set `mem_budget_bytes=1MiB`, apply 2 MiB of
values, verify `resource_exhausted` after budget exceeded, verify success
after budget freed (entries applied + flushed).

---

### R5: RDMA-pinned allocation

**Problem**: `buffer::allocate` seam (`buffer.h:232`) is designed to swap
`std::malloc` for RDMA-pinned memory, but no RDMA backend exists.

**Priority**: Low — placeholder for future RDMA `BlockPageStore` medium.

**Complexity**: Low (the placeholder itself) — just keep the allocator seam
and document the intended interface. High when the RDMA backend itself is
built (separate effort).

**Blocked by**: RDMA `BlockPageStore` medium (not started).

**Files**: `crowtree/include/crowtree/buffer.h` (seam only)

**Acceptance**: N/A until RDMA backend exists. Seam remains intact.

---

### R6: Cross-thread `EpochManager::Guard`

**Problem**: `EpochManager::Guard` is thread-bound — must be created and
released on the same thread. This forces copies in three scenarios:

1. **`get_async` cross-thread handoff**: A `get_async` miss resolves on the
   Reactor (io_uring) thread. The epoch guard was entered on the caller's
   thread; the CQE callback fires on the Reactor thread. The guard cannot
   cross threads, so `materialize_owned()` copies the borrowed L1 value into
   an owned `buffer` and releases the guard before handoff. With a
   cross-thread Guard, the borrowed `Slice` could survive the thread boundary
   — true zero-copy async read.

2. **`snapshot_view()` / `install_snapshot()` consistency**:
   `snapshot_view()` materializes all entries into owned copies.
   `install_snapshot()` swaps the tree under `write_mutex_` — a concurrent
   lock-free reader may see a transient partial state. A pinned `RootVersion`
   (atomic pointer to the old root, kept alive by epoch) would give readers a
   consistent point-in-time view without copying, but the Guard holding the
   old root alive might need to cross threads (reader thread enters, result
   consumed on another thread).

3. **Deferred stale-`RootVersion` GC**: After `install_snapshot` swaps the
   root, old pages are epoch-retired. A reader that loaded the old root
   pointer keeps its pages alive until its guard drains. If the reader hands
   the result to another thread (e.g. Rust async runtime), the guard can't
   follow — old root pages can't be reclaimed until the original thread's
   guard drains, which may be delayed.

**Fix options**:
- (a) **Cross-thread Guard**: per-thread `Participant` but release-by-token
  (hand off a release token to another thread, which drains on its own
  participant). Lighter per-access, but adds token management complexity.
- (b) **Page-level refcount**: increment on pin, decrement on unpin, free at
  zero — independent of thread. Heavier per-access (atomic per pin/unpin vs.
  per-thread epoch), but decouples lifetime from threads entirely.

**Priority**: Low — no current consumer requires cross-thread zero-copy. All
three scenarios work correctly today (with copies).

**Complexity**: High — touches the epoch reclamation core, which is
correctness-critical for all lock-free readers. Option (a) requires careful
token protocol design; option (b) adds per-page atomic overhead to every
`resident()` call.

**Files**: `crowtree/include/crowtree/epoch.h`, `crowtree/src/epoch.cpp`,
`crowtree/include/crowtree/crowtree.h` (`GetView`, `get_async_attempt`,
`materialize_owned`), `crowtree/src/crowtree.cpp`

**Acceptance**:
- `get_async` miss on Reactor thread returns borrowed `Slice` (no copy)
  verified via test that checks pointer equality with frame address.
- `snapshot_view()` returns a pinned view that stays consistent across
  `install_snapshot()` — reader sees old or new tree, never partial.
- Epoch reclamation stress test: concurrent readers + writers + snapshot
  swaps, no use-after-free under ASan/TSan.

---

### R10: Benchmark framework

**Problem**: After R8 (metrics module) is implemented, the next step is to
establish a benchmark framework to identify system bottlenecks and inform
performance tuning. Currently there is no way to run sustained load against
a crowkv-server and measure throughput/latency.

**Approach**:
- Add a `benchmark` subcommand to the console CLI (`crowkv-console`).
- Single-node benchmark mode: start one server, create one store + one group,
  drive KV put/get/delete at configurable rate and concurrency.
- Storage modes: in-memory (no disk) or file-without-fsync (reduce disk IO
  bottleneck so we can isolate path-level overhead).
- Measure: throughput (ops/s), latency p50/p99, WAL append rate, engine apply
  rate. Report per-mode comparison.
- Initial goal: establish the benchmark infrastructure and get baseline
  numbers. Follow-up: create smaller targeted benchmarks for specific
  bottlenecks identified.

**Dependencies**: R8 (metrics module) for collecting latency/throughput
counters. Related to R3 (zero-copy FFI), R4 (bounded memory pool), R6
(cross-thread guard) — all memory efficiency optimizations that benchmark
results may motivate.

**Priority**: Medium — needed after R8 to guide performance work.

**Complexity**: Medium — CLI subcommand, load generator, metrics collection
integration, report format.

**Files**: `crowkv-console/cli/src/`, `crowkv-console/shared/src/`, new
benchmark module.

**Acceptance**: `crowkv-console benchmark --mode memory --duration 60s` runs
and prints throughput + latency summary. Same for `--mode file-nofsync`.
Baseline numbers recorded for future comparison.

---

### R11: GUI internal state display

**Problem**: The GUI already queries health and internal state from the
backend, but does not surface metrics (operation counts, latency, WAL stats)
in the UI. Operators cannot see what a Store or Group is doing in real time.

**Approach**:
- Extend the existing health/internal-state query infrastructure to carry
  metrics from R8 (op counts, latency p50/p99, WAL flush lag, election count)
  in the response.
- Display metrics in the Inspector panel — per Store and per Group — with
  real-time refresh (5–10 s polling window).
- Show recent operation counts (puts, gets, deletes, scans) and key latency
  indicators.
- Keep it lightweight: no historical charts in v1, just current snapshot
  values that update on refresh.

**Dependencies**: R8 (metrics module) for the underlying counters and
histograms.

**Priority**: Medium — improves operational visibility once metrics exist.

**Complexity**: Medium — extend API response, add UI components to Inspector,
wire up polling.

**Files**: `crowkv-console/web/src/` (API handlers), `crowkv-console/web/ui/src/`
(Inspector component), `crowkv-console/shared/src/` (shared types).

**Acceptance**: Select a Store or Group in the UI, see real-time metrics
(op count, latency, WAL stats) in the Inspector, values update every 5–10 s.

---

### R12: Crow Common shared project

**Problem**: The crowkv and crowtree codebases contain reusable utility code
that is embedded inside project-specific crates. As the broader storage-system
goal expands to multiple components, each project would need to re-implement or
vend these utilities. Extracting them into a standalone `crow-common` project
eliminates duplication and establishes a shared foundation.

**Approach**:
- Create a new `crow-common/` directory at the workspace root, containing two
  sub-projects:
  - **`crow-common/rust/`** — a Rust crate (`crow-common`) published as a
    library. Contains the Rust-side shared utilities.
  - **`crow-common/cpp/`** — a C++ static library (`libcrowcommon.a`).
    Contains the C++-side shared utilities. Only static libraries are
    published — no shared objects — so downstream projects link them in
    without runtime dependency concerns.
- Move the following Rust utilities from `crowkv/src/` into
  `crow-common/rust/src/`:
  - **Metrics core** (`crowkv/src/metrics/`) — `Counter`, `Gauge`,
    `LatencyHistogram`, `LatencySummary`, `Bandwidth`, `MetricsRegistry`,
    `MetricsRunner`, `SystemCollector`, `MetricName`. These are generic
    atomic-counter primitives with no crowkv-specific dependencies.
  - **Logging wrapper** (`crowkv/src/common/logging.rs`) —
    `init_file_logging`, `init_file_and_console_logging`, `open_metrics_log`,
    `LogGuards`, `RotatingLogWriter`, `format_timestamp`. These encapsulate
    the `tracing-subscriber` + `file-rotate` initialization with the
    project's naming conventions (`{process_name}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`),
    start/stop/flush lifecycle, and rotation/compression controls.
  - **Time helpers** (`crowkv/src/common/time.rs`) — `process_anchor`,
    `instant_to_anchor_ms`, `anchor_ms_to_instant`. Generic monotonic-time
    utilities.
  - **Operation report** (`crowkv/src/common/report.rs`) —
    `OperationReport`. Generic multi-step error aggregation.
- Move the following C++ utilities from `crowtree/` into
  `crow-common/cpp/`:
  - **CRC32C** (`crowtree/include/crowtree/crc32c.h`,
    `crowtree/src/crc32c.cpp`) — table-driven CRC32C (Castagnoli)
    implementation used for page/checksum/snapshot integrity. Move to
    `crow-common` so other storage components can share it. **Follow-up**:
    replace the hand-rolled table-driven implementation with a mature,
    well-known library (e.g. `crc32c` from Google's `crc32c` project or
    hardware-accelerated SSE4.2 intrinsics via a proven library) to avoid
    maintaining a custom implementation.
  - **Logging facade** (`crowtree/include/crowtree/log.h`,
    `crowtree/src/log.cpp`) — `init_logging`, `shutdown_logging`,
    `logging_enabled`, `CT_LOG_*` macros. The spdlog-backed async logger
    with rotating/compressing file sink, naming conventions aligned with the
    Rust side, and start/stop/flush lifecycle. This is a generic C++ logging
    wrapper, not crowtree-specific.
  - **Compressing sink** (`crowtree/include/crowtree/compressing_sink.h`,
    `crowtree/src/compressing_sink.cpp`) — custom spdlog sink with
    size-based rotation + gzip compression. Used by the logging facade;
    moves together with it.
- Review other utility code in `crowkv/src/common/` and `crowtree/src/` for
  additional candidates (e.g. `config.rs` profiles, byte-order helpers
  `put_u32`/`get_u32` used across persist/snapshot codecs). Move only code
  that is genuinely generic and has no project-specific coupling.
- Update `crowkv` and `crowtree` to depend on `crow-common` (Rust: add
  `crow-common` as a workspace dependency; C++: link `libcrowcommon.a` and
  update include paths). Replace the moved code with re-exports or thin
  wrappers so existing call sites compile with minimal changes.
- Update `Cargo.toml` workspace members and `pixi.toml` build/test tasks.

**Priority**: Medium — foundational for the multi-component storage-system
roadmap. Extracting now avoids deeper coupling as more components are built.

**Complexity**: Medium — mechanical extraction + dependency wiring. No new
algorithms or protocols. The main risk is breaking existing build/test paths;
mitigated by keeping re-exports at old paths during the transition.

**Files**:
- New: `crow-common/rust/Cargo.toml`, `crow-common/rust/src/` (moved from
  `crowkv/src/metrics/`, `crowkv/src/common/logging.rs`, `time.rs`,
  `report.rs`).
- New: `crow-common/cpp/CMakeLists.txt`, `crow-common/cpp/include/`,
  `crow-common/cpp/src/` (moved from `crowtree/include/crowtree/crc32c.h`,
  `log.h`, `compressing_sink.h` and their `.cpp` counterparts).
- Modified: `Cargo.toml` (workspace members), `pixi.toml`,
  `crowkv/Cargo.toml` (add `crow-common` dependency),
  `crowkv/src/lib.rs` / `crowkv/src/common/mod.rs` (re-export from
  `crow-common`), `crowtree/CMakeLists.txt` (link `libcrowcommon.a`),
  `crowtree/ffi/build.rs` (C++ include path update).

**Acceptance**:
- `pixi run cargo build` and `pixi run test-ct` pass with `crow-common` as a
  workspace member.
- `crowkv` metrics tests (`crowkv/tests/metrics_test.rs`) pass unchanged.
- `crowtree` CRC32C and logging tests pass unchanged.
- `crow-common` Rust crate compiles independently (`cargo build -p
  crow-common`).
- `libcrowcommon.a` builds independently via CMake.
- No functional changes — all moved code is byte-for-byte identical in
  behavior; only the module/crate boundary changes.

---

### R13: Unify bench client stats with metrics library

**Problem**: The benchmark client (`bench/runner.rs`) maintains its own
statistical infrastructure separate from crowkv's metrics module:

- `OpStats` uses the external `hdrhistogram` crate (`Histogram<u64>`) for
  latency distributions, plus manual `u64` counters for `ops`, `errors`,
  `not_found`.
- `WorkerCounters` uses hand-rolled `AtomicU64` for live progress
  snapshotting.

Meanwhile, crowkv's `MetricsRegistry` provides `LatencyHistogram`,
`Counter`, `LatencySummary`, and `Bandwidth` — the same primitives the
server uses for its metrics log. The bench client should reuse these
classes so there is one statistical infrastructure across the project.

**Two problems blocking immediate adoption**:

1. **Dependency boundary**: `crowkv-cli` does not depend on `crowkv`
   today (only `crowkv-client` and `crowkv-console-shared`/`crowkv-web`).
   Adding `crowkv` as a dependency just for metrics would pull in the
   entire consensus/cluster/WAL stack. This is resolved by R12 (Crow
   Common shared project), which extracts the metrics core into a
   standalone `crow-common` crate with no crowkv-specific coupling.

2. **Histogram precision**: crowkv's `LatencyHistogram` uses 12 fixed
   buckets (1µs, 10µs, 100µs, 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms,
   1s, ∞). Percentile queries return the bucket upper bound, not the
   actual value — e.g. any latency between 500µs and 1ms reports as 1ms.
   The bench client needs high-precision percentiles (p90, p99, p999, max)
   for meaningful latency analysis; the current `hdrhistogram` crate
   provides 3 significant digits with auto-resizing. The fixed-bucket
   design is intentional for the server hot path (zero allocation, no
   locks, cache-friendly), but it is too coarse for benchmark reporting.

**Plan**: After R12 extracts metrics into `crow-common`, implement a new
`PreciseHistogram` (or similar) in `crow-common` that offers higher
percentile precision at a slightly higher per-observe cost — e.g. an
HDR-style logarithmic bucket scheme or a lock-free variant of
`hdrhistogram`. The existing `LatencyHistogram` stays as the low-overhead
server hot-path option; the new histogram is used by the bench client and
any other consumer that needs precise tail latency. The bench client then
replaces `OpStats` / `WorkerCounters` with `crow-common` metrics
primitives, eliminating the `hdrhistogram` external dependency and
unifying the statistical infrastructure.

**Dependencies**: R12 (Crow Common shared project) — must extract metrics
core first.

**Priority**: Medium — consistency and maintainability improvement; not
blocking current benchmark work.

**Complexity**: Medium — new histogram implementation in `crow-common`,
bench client refactor to use metrics primitives, update report generation.

**Files**: `crow-common/rust/src/metrics/` (new precise histogram),
`crowkv-console/cli/src/bench/runner.rs`,
`crowkv-console/cli/src/bench/report.rs`,
`crowkv-console/cli/Cargo.toml` (replace `hdrhistogram` with
`crow-common`).

**Acceptance**:
- Bench client uses `crow-common` metrics primitives (`Counter`,
  precise histogram) instead of `hdrhistogram` and manual atomics.
- Benchmark report percentiles (p50, p90, p99, p999, max) are at least as
  precise as the previous `hdrhistogram`-based values.
- `hdrhistogram` dependency removed from `crowkv-cli/Cargo.toml`.
- Existing benchmark tests pass with the new infrastructure.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
