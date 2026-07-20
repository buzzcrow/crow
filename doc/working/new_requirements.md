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
- **R14** — Concurrent remote RPC fan-out in Paxos phases (DONE) — Area:
  consensus — `run_prepare_phase` and `run_accept_phase` now use
  `futures::future::join_all` to issue all remote RPCs concurrently instead
  of sequentially. +8.7% throughput, -25% avg latency on 3-node mem bench.
- **R15** — Zero-copy PxLogEntry in accept path — Area: consensus —
  `on_accept` clones `entry` for the acceptor and again for the WAL
  record; `inner_accept` clones again for `cas_accepted`. With `Bytes`
  payloads these are ref-count bumps today, but the goal is zero copy:
  pass `&PxLogEntry` through the acceptor and WAL encode without
  intermediate clones.
- **R16** — Overlap local WAL fsync with remote RPC fan-out — Area:
  consensus / WAL — The leader's local `on_accept` awaits `fdatasync`
  before returning `PxAcceptReply::Accepted`, putting the leader's disk
  fsync on the critical path *before* remote RPCs begin. Overlapping the
  local WAL persist with the remote accept RPCs would hide fsync latency
  behind network round-trips. **Concept change**: weakens the W6 ack
  contract (persist-before-reply) for the local replica — the proposer
  would need to track local persist completion separately from quorum.
  Gate behind a feature flag; test under crash-recovery scenarios.
- **R17** — Async engine apply after quorum — Area: consensus / engine —
  `learn_chosen` (decode payload + `KVEngine::apply`) runs on the
  proposer critical path before `ProposeResult::Chosen` is returned to
  the client. Returning `Chosen` immediately after quorum confirmation
  and applying asynchronously would remove engine apply latency from
  the write path. **Concept change**: the client receives "chosen"
  before the local engine has applied the value — read-your-writes
  semantics break unless a read barrier or apply-fence is added. Gate
  behind a feature flag; test read-after-write consistency.
- **R18** — Queue-based admission control for inflight proposals — Area:
  consensus — Replace the current `try_acquire` fail-fast `Busy` reject
  with a configurable queue-per-group admission model. Multiple queues
  per group, queue count configurable via CLI. Enables fair comparison
  with Raft-style block-and-queue behavior and eliminates reject storms
  under high concurrency. Adds queue depth / wait time metrics.

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

### R14: Concurrent remote RPC fan-out in Paxos phases

**Problem**: Both `run_prepare_phase` and `run_accept_phase` in
`PxGroup` iterate sequentially over `self.remote_replicas`, awaiting
each `send_prepare` / `send_accept` RPC before sending to the next
replica. With N followers, the leader pays (N-1) extra serial RPC
round-trips per proposal. In the current 3-node setup this is one
extra round-trip (~20-30 µs on loopback); in larger clusters the
latency penalty grows linearly with follower count.

**Approach**: Collect all remote RPC futures into a `Vec` and await
them concurrently using `futures::future::join_all` or
`FuturesUnordered`. The local replica's `on_prepare` / `on_accept`
call stays first (it is in-process and effectively free). After all
remote futures resolve, fold the results into the existing
accumulator variables (`accepted` / `promised`,
`highest_rejected_round`, `highest_seen_term`, `epoch_mismatch`,
`adopted`) using the same match logic. The quorum check and return
logic remain unchanged — they already run after all replicas have
been contacted.

Key considerations:
- The match arms are pure accumulation (increment counters, take max
  of rejected rounds / seen terms, consider adopted entries), so
  folding from a `Vec` of results is straightforward.
- `consider_accepted` for the prepare phase must be called in a
  deterministic order (e.g. by replica id) to keep adoption
  tie-breaking stable across runs.
- Error handling per-replica stays the same — each future produces
  its own `Result`, and errors are logged individually.
- No semantic change: the proposer still waits for all replicas
  before checking quorum. The only difference is that RPCs are sent
  in parallel rather than serially.

**Priority**: Low — optimization, no correctness impact. The
sequential overhead is small with 3 nodes but grows with cluster
size.

**Complexity**: Low — mechanical refactor of two loops into
`join_all` / `FuturesUnordered` + result folding. No new algorithms
or protocol changes.

**Files**: `crowkv/src/cluster/group.rs` (`run_prepare_phase`,
`run_accept_phase`).

**Acceptance**:
- Existing Paxos tests pass unchanged (election, group propose,
  replica concurrent tests).
- Benchmark (R10) shows reduced per-proposal latency with >2
  followers; no regression with 2 followers.
- No change in quorum / retry / TermStale behavior under concurrent
  send.

---

### R15: Zero-copy PxLogEntry in accept path

**Problem**: The local accept path performs multiple `PxLogEntry` clones:
- `on_accept` (`local_replica.rs:1099`) clones `entry` for
  `self.acceptor.accept(entry.clone())` because it needs `&entry` later
  for `WALRecord::from_accepted`.
- `inner_accept` (`acceptor.rs:124`) clones `entry` again for
  `node.cas_accepted(accepted_ptr, entry.clone())`.
- `base_entry` (`group.rs:1269`) constructs a new `PxLogEntry` per slot
  retry, cloning `payload: Bytes` each time.

Today `Bytes::clone` is an `O(1)` ref-count bump, so the cost is
small. However the goal is zero copy where possible: the acceptor
should accept `&PxLogEntry` and internally manage the single owned
copy stored in the slot node, and the WAL encoder should borrow from
the entry without cloning.

**Approach**:
- Change `Acceptor::accept` to take `&PxLogEntry` instead of
  `PxLogEntry`. The acceptor performs one clone internally for
  `cas_accepted` (unavoidable — the slot node must own its copy).
- In `on_accept`, avoid the clone for the acceptor call by passing
  `&entry`. The WAL record encoding (`WALRecord::from_accepted`)
  already takes `&PxLogEntry`, so no extra clone is needed there.
- In `base_entry`, pass `payload` by value (move) instead of cloning,
  and reuse the same `Bytes` handle across retry attempts by cloning
  only when a new `PxLogEntry` must be constructed (the clone is still
  `O(1)` but the move avoids one redundant bump per attempt).
- Audit the prepare path similarly: `on_prepare` already takes
  primitives by value, so no entry clone there.

**Priority**: Low — current `Bytes::clone` cost is negligible; this is
a code-quality and future-proofing improvement for when payloads may
not always be `Bytes`-backed.

**Complexity**: Low — signature changes (`&PxLogEntry` vs
`PxLogEntry`) and removing redundant clones. No algorithm or protocol
change.

**Files**: `crowkv/src/cluster/local_replica.rs` (`on_accept`),
`crowkv/src/paxos/acceptor.rs` (`accept`, `inner_accept`),
`crowkv/src/paxos/roles.rs` (`Acceptor` trait),
`crowkv/src/cluster/group.rs` (`base_entry`).

**Acceptance**:
- Existing Paxos tests pass unchanged.
- No `PxLogEntry` clone in `on_accept` between acceptor call and WAL
  encode (verified by code inspection or a debug counter).
- `Acceptor::accept` takes `&PxLogEntry`; the only clone is inside
  `cas_accepted`.

---

### R16: Overlap local WAL fsync with remote RPC fan-out

**Problem**: In `run_accept_phase`, the leader calls
`replica.on_accept(entry.clone()).await` first, which internally awaits
`wal.append(&record).await` (an `fdatasync` round-trip) before
returning `PxAcceptReply::Accepted`. Only after this local fsync
completes does the leader begin sending `send_accept` RPCs to remote
replicas. The leader's local disk fsync is therefore fully serial with
the remote RPC fan-out, adding ~10-100 µs (NVMe) to ~1-10 ms (SSD/HDD)
of disk latency to the critical path before any network I/O starts.

With R14 (concurrent fan-out), the remote RPCs overlap with each other
but still wait for the local fsync to finish first.

**Approach**: Start the local WAL append and the remote accept RPCs
concurrently. The local acceptor logic (`inner_accept`) is synchronous
and completes instantly — only the WAL persist (`fdatasync`) is slow.
Split `on_accept` into two phases:
1. **Accept logic** — run `inner_accept`, get the `PxAcceptReply`.
2. **WAL persist** — `wal.append(&record).await`.

The proposer would:
1. Call the accept logic on the local replica (instant).
2. Concurrently: (a) await the local WAL persist, (b) fan out
   `send_accept` RPCs to all remote replicas (R14 makes these
   concurrent).
3. After all futures resolve, fold results and check quorum.

**Concept change (highlighted)**: This weakens the W6 ack contract for
the *local* replica. Today, `on_accept` guarantees the Accepted record
is durably persisted before returning `Accepted`. With this change, the
local replica's `Accepted` reply is returned before the WAL flush
completes — the proposer tracks local persist separately. If the node
crashes between the accept reply and the WAL flush, the accepted value
may be lost, which is safe in Paxos (the value was not yet chosen) but
means the leader may re-propose a slot it had already accepted. This is
correctness-safe but changes the durability ordering.

**Feature flag**: Gate behind `wal_overlap_local_persist` (default
off). When enabled, the proposer overlaps; when disabled, the current
serial behavior is preserved.

**Testing**:
- Crash-recovery tests: kill the leader after local accept but before
  WAL flush; verify the slot is re-proposed and converges.
- Quorum tests: verify the proposer still waits for all replicas before
  declaring chosen.
- Benchmark: measure fsync latency hidden behind RPC round-trips.

**Priority**: Medium — hides fsync latency behind network latency,
which is the single largest non-network bottleneck in the write path.

**Complexity**: High — splits `on_accept`, changes the proposer's
local-replica handling from a simple `.await` to a concurrent join,
weakens W6 contract, requires feature flag and crash-recovery tests.

**Files**: `crowkv/src/cluster/local_replica.rs` (`on_accept` split),
`crowkv/src/cluster/group.rs` (`run_accept_phase` concurrent local +
remote), `crowkv/src/wal/wal_engine.rs` (no change to `append` itself).

**Acceptance**:
- With feature flag off: all existing tests pass, behavior unchanged.
- With feature flag on: Paxos election and group propose tests pass.
- Crash-recovery test: leader crash after accept, before WAL flush →
  slot re-proposed and converges to a single chosen value.
- Benchmark shows reduced per-proposal latency when fsync is the
  bottleneck (non-loopback or slow disk).

---

### R17: Async engine apply after quorum

**Problem**: After `AcceptAttempt::Chosen`, the leader calls
`replica.learn_chosen(&entry, client_id, seq).await` which decodes the
payload and applies it to the KV engine **before** returning
`ProposeResult::Chosen` to the client. For `InMemKV` this is trivial,
but for `CrowtreeEngine` the apply involves FFI + memtable insert,
potentially triggering a memtable flush. This puts engine apply latency
on the write critical path.

`fan_out_chosen_notice` (item 7) runs after `learn_chosen` but is a
non-blocking mpsc enqueue — negligible cost, can stay where it is.

**Approach**: Return `ProposeResult::Chosen` to the client immediately
after quorum is confirmed, then apply the entry to the local engine
asynchronously (spawn a task or use a apply queue). The
`fan_out_chosen_notice` can fire immediately after quorum as well
(before the async apply completes) since it only carries the slot/term
watermark, not the payload.

**Concept change (highlighted)**: The client receives "chosen" before
the local engine has applied the value. This breaks read-your-writes
semantics: a client that writes a key and then immediately reads it
may not see the written value if the async apply has not completed.
Mitigations:
- **Apply fence**: Track the highest slot applied in the local engine.
  Reads on the leader check that the applied frontier >= the slot being
  read; if not, the read waits (or returns a stale-read indicator).
- **Sync mode**: Gate behind a feature flag `async_engine_apply`
  (default off). When disabled, the current synchronous apply behavior
  is preserved.
- **Client-visible flag**: The `KvResponse` could carry an
  `applied_locally: bool` field so the client knows whether the value
  is immediately readable.

**Feature flag**: `async_engine_apply` (default off).

**Testing**:
- Read-after-write test: write a key, immediately read it; with flag
  off, must see the value; with flag on, may see stale until apply
  catches up (verify eventual consistency).
- Apply-ordering test: multiple writes to the same key; verify the
  final applied value is the highest-slot value (per-key
  highest-slot-wins in `KVEngine::apply`).
- Crash-recovery test: leader crash after quorum but before async
  apply → on restart, the slot is re-learned and applied from the WAL
  / peer replication.

**Priority**: Medium — removes engine apply from the write critical
path, which is significant for `CrowtreeEngine` under load.

**Complexity**: Medium — spawn apply task, track applied frontier, add
read barrier / fence, feature flag, tests. No protocol change (the
value is Paxos-chosen regardless of local apply).

**Files**: `crowkv/src/cluster/group.rs` (`propose` — return before
`learn_chosen`), `crowkv/src/paxos/learner.rs` (`learn` / `apply_entry`
— async dispatch), `crowkv/src/cluster/local_replica.rs`
(`learn_chosen` — split into notify + async apply).

**Acceptance**:
- With feature flag off: all existing tests pass, behavior unchanged.
- With feature flag on: Paxos tests pass; write latency reduced by
  engine apply time.
- Read-after-write test: with flag on, eventual consistency verified
  (read eventually returns the written value after apply catches up).
- Apply-ordering test: highest-slot-wins semantics preserved under
  async apply.
- Crash-recovery test: slot re-learned and applied after restart.

---

### R18: Queue-based admission control for inflight proposals

**Problem**: The current inflight admission control uses
`Semaphore::try_acquire` — when the inflight window is full, the
proposal is immediately rejected with `ProposeResult::Busy`. The
client library retries up to `max_retries` (default 3) with no
backoff for `Busy`, generating a reject-retry storm under high
concurrency. In the README benchmark (window=1, 16 threads), 40% of
RPCs are `Busy` rejections — wasted CPU on both client and server,
artificially depressing throughput and making the window=1 numbers
not representative of true sequential-commit performance.

Raft-style systems handle the same situation by **queuing** — the
leader blocks the caller until a log slot is available, never
rejecting. This is the behavior CrowKV should offer as an option for
fair comparison and for deployments where reject storms are
undesirable.

**Approach**: Replace the single `Semaphore::try_acquire` with a
configurable multi-queue admission system owned by `PxGroup`.

- **Queue count configurable**: `PxGroup` holds N admission queues
  (default 1). Each queue is a `tokio::sync::Semaphore` with
  `max_inflight_proposals / N` permits (rounded up). A proposal is
  routed to a queue via a deterministic but low-contention strategy
  (e.g. `hash(client_id) % N` or round-robin). Multiple queues reduce
  contention on a single semaphore's waiter list under high load, and
  allow future per-queue priority or isolation.

- **Queue mode vs reject mode**: Add a config field
  `inflight_admission: AdmissionPolicy` with variants `Reject` (current
  `try_acquire` behavior) and `Queue` (blocking `acquire().await`).
  Default stays `Reject` for backward compatibility. `Queue` mode
  makes the system behave like Raft — callers block until a permit is
  available, never seeing `Busy`.

- **No correctness impact**: In Multi-Paxos, slots are independent
  Paxos instances. The order in which proposals acquire inflight
  permits and enter the slot allocation pipeline does not affect
  safety — each slot is decided by its own Paxos round. Multi-queue
  routing only changes which proposals get admitted first, not the
  consensus protocol. This is the key insight that makes multi-queue
  safe: the slot order is not yet decided at admission time.

- **Metrics**: New counters and gauges for observability:
  - `inflight_queue_depth` (gauge per queue) — current number of
    waiting proposers (proposals blocked on `acquire()`). Computed as
    `window_size - available_permits - inflight_occupied` or tracked
    via an explicit `AtomicU64` waiter counter.
  - `inflight_total_enqueued` (counter) — cumulative count of
    proposals that entered the queue (did not get a fast-path permit).
  - `inflight_total_wait_us` (counter) — cumulative wait time in
    microseconds. Divided by `total_enqueued` gives average queue
    wait. Individual wait time is measured as `Instant` delta between
    entering `acquire()` and getting the permit.
  - `inflight_occupied` (gauge) — `window_size - available_permits`,
    same as current `inflight_slot_count()` but per-queue.

- **CLI configuration**: Add `--inflight-queues N` (default 1) and
  `--inflight-admission <reject|queue>` (default `reject`) to
  `crowkv-server` CLI. These are per-group settings applied at group
  creation time via `PxGroup::set_inflight_config(queues, policy)`.
  The bench provision tool (`crowkv-console/cli/src/bench/provision.rs`)
  passes these through so benchmark runs can compare reject vs queue
  mode and varying queue counts.

- **Fast path preserved**: When `AdmissionPolicy::Queue` is active and
  the queue is empty, the first `try_acquire` succeeds immediately —
  no async wait, no overhead. Only when `try_acquire` fails does the
  proposal fall through to `acquire().await` (blocking wait). This
  two-tier approach keeps the common case (low load) at zero overhead.

**Flow**:
1. `propose()` calls `try_acquire()` on the routed queue's semaphore.
2. If success → proceed to Paxos (fast path, zero overhead).
3. If fail and `AdmissionPolicy::Reject` → return `ProposeResult::Busy`
   (current behavior).
4. If fail and `AdmissionPolicy::Queue` → record wait start time,
   increment waiter counter, `acquire().await` (blocks until a permit
   is freed by a completing proposal), decrement waiter counter,
   record wait duration, proceed to Paxos.

**Performance analysis**:
- **Reject mode (current)**: under high load, 40%+ of RPCs are
  rejected. Each rejected RPC consumes a full gRPC round-trip +
  client retry logic + server-side processing, all producing zero
  useful work. TPS is artificially depressed by reject overhead.
- **Queue mode**: under high load, all proposals eventually succeed.
  No wasted RPCs. Latency increases for queued proposals (they wait
  for a permit), but throughput should be higher because all CPU is
  spent on useful work. The latency distribution shifts from
  bimodal (fast success / fast reject) to a long-tail (fast for
  fast-path, slower for queued).
- **Multi-queue benefit**: with N queues, contention on the
  semaphore's internal waiter list is reduced by ~N×. In practice
  tokio's `Semaphore` is already highly efficient (lock-free
  fast path, mutex only on waiter list), so multi-queue's primary
  benefit is observability isolation and future per-queue priority,
  not raw throughput. Benchmarking will confirm.
- **Queue depth as backpressure signal**: `inflight_queue_depth` is a
  direct measure of admission pressure — operators can alert on it
  and scale the `max_inflight` window or add replicas. This is more
  actionable than the current `Busy` reject rate.

**Priority**: Medium — improves fairness of benchmark comparisons,
eliminates reject storms, adds actionable backpressure metrics.

**Complexity**: Medium — new `AdmissionPolicy` enum, multi-queue
semaphore routing in `PxGroup`, wait-time tracking atomics, CLI flags,
metrics integration, benchmark provision plumbing. No protocol change.

**Files**:
- `crowkv/src/cluster/group.rs` — `PxGroup` inflight field refactor
  (single semaphore → `Vec<Semaphore>` + routing), `propose()`
  admission logic, `set_inflight_config()` method.
- `crowkv/src/common/config.rs` — `AdmissionPolicy` enum,
  `PaxosConfig` new fields (`inflight_queues`, `inflight_admission`).
- `crowkv/src/cluster/status.rs` — expose per-queue depth / wait
  metrics in `*Status` structs.
- `crowkv-server/src/cli.rs` — `--inflight-queues`, `--inflight-admission`
  flags.
- `crowkv-server/src/startup.rs` — wire CLI flags to
  `PxGroup::set_inflight_config()`.
- `crowkv-console/cli/src/bench/provision.rs` — pass queue config to
  provisioned groups.

**Acceptance**:
- `--inflight-admission reject` (default): all existing tests pass,
  behavior unchanged.
- `--inflight-admission queue --inflight-queues 1`: no `Busy` rejections
  under any load; proposals block and eventually succeed. Existing
  Paxos tests pass (may need longer timeouts for blocking paths).
- `--inflight-admission queue --inflight-queues 4`: proposals distributed
  across 4 queues; no `Busy` rejections; per-queue metrics visible in
  status/health output.
- Benchmark: queue mode window=1 shows higher throughput than reject
  mode window=1 (no wasted reject RPCs), and latency distribution is
  unimodal (no fast-reject spike).
- Metrics: `inflight_queue_depth`, `inflight_total_enqueued`,
  `inflight_total_wait_us` visible in metrics log and health API.
- Multi-queue correctness: concurrent proposals across queues all
  converge to chosen slots; no slot gaps or lost proposals.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
