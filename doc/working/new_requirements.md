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
- **R19** — Read performance profiling and metrics — Area: consensus /
  metrics / client — The read path lacks the latency-bandwidth-counter
  hierarchy that the write path has. No per-mode latency breakdown, no
  lease-vs-ReadIndex path counter, no read barrier latency, no engine
  get latency, no read bandwidth separation, no forward/fallback
  counters, no read-specific gauges. See
  [`read-flow-analysis.md`](read-flow-analysis.md) for the full gap
  analysis and proposed metrics hierarchy.
- **R20** — Eliminate O(n) payload copy in WAL encode — Area: WAL —
  `WALRecord::from_accepted` calls `encode_accepted_payload(entry)`
  which does `entry.payload.to_vec()`, an O(n) heap allocate + memcpy
  of the entire payload. Since `entry.payload` is already `Bytes`, the
  `WALRecord.payload` could be `entry.payload.clone()` (O(1) ref-count
  bump) instead. See
  [`write-flow-analysis.md`](write-flow-analysis.md) Memory Copy
  Analysis for the full audit.
- **R21** — Zero-copy engine read API — Area: crowtree FFI / engine —
  `CrowtreeEngine::get` copies the key (`key.to_vec()`) for the FFI
  call and copies the value (`copy_buf`) from the C++ engine's internal
  buffer because the epoch guard is released before the value is
  returned. A pinned-value API that extends the epoch guard lifetime to
  the Rust caller could eliminate the value copy. See
  [`read-flow-analysis.md`](read-flow-analysis.md) Memory Copy
  Analysis for the full audit.
- **R22** — Zero-copy Batch decode — Area: kv / consensus —
  `Batch::decode` does `to_vec()` per key and per value, creating O(n)
  heap allocations on every `learn_chosen` call. Changing `Op` and
  `BatchOp` from `Vec<u8>` to `Bytes` allows `Batch::decode` to use
  `Bytes::slice` (O(1) ref-count bump) instead of `to_vec()`, sharing
  the `PxLogEntry.payload` allocation. See
  [`write-flow-analysis.md`](write-flow-analysis.md) Memory Copy
  Analysis for the full audit.

### Low Priority

**Complexity — Low (placeholder):**
- **R5** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **R3** — Zero-copy FFI write path — Area: crowtree FFI — `ct_apply_put` copies
  key+value into an internal buffer; for large values this memcpy is avoidable
  via a direct-write alloc handle.
- **R23** — Eliminate FFI batch encode copy — Area: crowtree FFI —
  `encode_batch` packs `&[BatchOp]` into a flat `Vec<u8>` before calling
  `ct_apply_batch`, an O(n) heap allocate + memcpy of all keys and values.
  A new C API accepting an array of `(key_ptr, key_len, val_ptr, val_len)`
  structs would eliminate the packing copy. Related to R3 (single-put FFI
  copy) but targets the batch path. See
  [`write-flow-analysis.md`](write-flow-analysis.md) Memory Copy
  Analysis for the full audit.
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

### R19: Read performance profiling and metrics

**Problem**: The write path has a well-instrumented
latency-bandwidth-counter hierarchy (WAL append/fsync latency, write
bandwidth, election counters, inflight gauge). The read path has
only a single `get.lh` histogram and a `scan.l` summary — no
per-mode breakdown, no consensus-layer metrics (lease vs ReadIndex),
no engine-layer latency, no read-specific bandwidth, and no
forward/fallback counters. Operators cannot diagnose read
performance issues at the same granularity as write issues.

Full analysis in
[`read-flow-analysis.md`](read-flow-analysis.md).

**Approach**: Add a read metrics hierarchy mirroring the write
path's structure, following the design principles in
`design-observability.md`:

- **Latency hierarchy** (feature layer → thinnest layer):
  - `kv.get.lh` — existing, get RPC end-to-end
  - `read.barrier.l` — new LatencySummary for
    `linearizable_read_barrier` (near-zero for lease path, one
    heartbeat RTT for ReadIndex)
  - `read.engine_get.l` — new LatencySummary for `KVEngine::get`
    (isolates engine cost from consensus barrier cost)
  - `kv.scan.l` — existing, scan RPC end-to-end
- **Bandwidth hierarchy** (read vs write separation):
  - `kv.read_bytes_in.bw` / `kv.read_bytes_out.bw` — new, read
    traffic separated from the combined `bytes_in/out.bw`
- **Counters** (outcome / population separation):
  - `read.lease_path.c` — linearizable reads via lease fast path
  - `read.readindex_path.c` — linearizable reads via ReadIndex
    fallback
  - `kv.get_forwarded.c` — reads forwarded to leader (server-side)
  - `kv.get_forward_failed.c` — forward attempts that failed
  - `read.ryw_fallback.c` — ReadYourWrites reads redirected to
    leader
- **Gauges** (state, bridged from existing atomics):
  - `read.lease_valid.g` — 1 if leader's read lease is valid
  - `read.contiguous_applied.g` — current `contiguous_applied`
  - `read.safe_slot.g` — current `group_safe_slot`

**Priority**: Medium — read performance is undiagnosable today;
the metrics infrastructure (MetricsRegistry, KvMetrics,
ElectionRegistryHandles) already exists and just needs new handles
wired in.

**Complexity**: Medium — new metric handles in `KvMetrics` and
`ElectionRegistryHandles` (or a new `ReadRegistryHandles`), timing
instrumentation in `linearizable_read_barrier`, `resolve_read_point`,
`KvStoreService::get/scan`, and `PxLearner::engine_get`. No
algorithm or protocol change.

**Files**: `crowkv/src/rpc/kv_service.rs` (KvMetrics — new handles,
forward/fallback counters, read bandwidth), `crowkv/src/cluster/
local_replica.rs` (ReadRegistryHandles — lease/ReadIndex counters,
barrier latency, gauges), `crowkv/src/cluster/group_election.rs`
(`linearizable_read_barrier` — timing + path counter),
`crowkv/src/cluster/px_kv_store.rs` (`resolve_read_point` — RYW
fallback counter), `crowkv/src/paxos/learner.rs`
(`engine_get` — timing).

**Acceptance**:
- Metrics log shows read-specific counters, latency summaries,
  bandwidth, and gauges per (store, group).
- `read.lease_path.c + read.readindex_path.c` equals the total
  linearizable get count in the same window.
- `read.barrier.l` avg is near-zero when lease is valid; matches
  heartbeat RTT when ReadIndex path is taken.
- `read.engine_get.l` isolates engine cost (trivial for InMemKV,
  measurable for CrowtreeEngine demand-load misses).
- Read bandwidth (`read_bytes_in/out.bw`) + write bandwidth
  (derived: `bytes_in/out.bw` minus read) accounts for total KV
  bandwidth.

---

### R20: Eliminate O(n) payload copy in WAL encode

**Problem**: `WALRecord::from_accepted` (`record.rs:408`) calls
`encode_accepted_payload(entry)` which does `entry.payload.to_vec()`
(`record.rs:487`). This is an O(n) heap allocate + memcpy of the
entire payload — the only O(n) copy in the accept → WAL path that is
potentially avoidable. The `Vec<u8>` is then wrapped back into
`Bytes::from(vec)` for the `WALRecord.payload` field, making the
round-trip through `Vec<u8>` purely wasteful.

**Root cause**: `encode_accepted_payload` exists as a seam for future
encoding transformations (e.g. compression, encryption). Today it is a
straight `entry.payload.to_vec()` — the payload bytes are unchanged,
just copied into a new allocation.

**Approach**:
- Change `WALRecord::from_accepted` to store `entry.payload.clone()`
  directly (O(1) ref-count bump) when no encoding transformation is
  needed.
- Keep `encode_accepted_payload` as the seam, but make it return
  `Bytes` instead of `Vec<u8>`. When no transformation is active, it
  returns `entry.payload.clone()`. When a transformation is added
  later, it returns `Bytes::from(transformed_vec)`.
- Verify that `encode_frame` and the vectored write path work
  unchanged — they already operate on `WALRecord.payload: Bytes`.

**Priority**: Low — with `Bytes` payloads the copy is a single
`memcpy` per accept. For small payloads (≤512 B, the benchmark
default) the cost is negligible. For large payloads (≥1 MB) it is a
measurable but still small fraction of the total write latency
(consensus RPC round-trip dominates).

**Complexity**: Low — one function change, no API or trait changes.

**Files**: `crowkv/src/wal/record.rs` (`encode_accepted_payload`,
`from_accepted`).

**Acceptance**:
- All existing WAL tests pass unchanged.
- No `to_vec()` call in `WALRecord::from_accepted` when no encoding
  transformation is active.
- `WALRecord.payload` is a `Bytes::clone` of `entry.payload` (shared
  allocation), verified by code inspection.

---

### R21: Zero-copy engine read API

**Problem**: `CrowtreeEngine::get` has two O(n) copies on the read
path:
- **Key copy**: `try_get(key.to_vec())` (`crowtree_engine.rs:168`)
  allocates a `Vec<u8>` copy of the key for the FFI call. The C API
  `ct_get_async` takes `*const u8, len`, so a borrow could work, but
  the async FFI wrapper requires `Send` ownership for the reactor
  boundary.
- **Value copy**: `copy_buf(value)` (`ffi/src/lib.rs:1178`) does
  `slice::from_raw_parts(..).to_vec()`. The C++ engine's zero-copy
  fast path returns a `ct_buf` that may be a borrowed pointer into a
  still-live frame (epoch guard), but the Rust side must copy because
  the epoch guard is released immediately after `ct_future_free`.

For large values (≥1 MB) these copies are measurable on the read
critical path. The key copy is the smaller concern (keys are typically
small); the value copy is the primary target.

**Approach**:
- **Value copy**: Introduce a "pinned value" C API that returns a
  `ct_buf` along with a guard handle that keeps the epoch alive until
  the caller explicitly releases it. The Rust side wraps this in a
  `PinnedValue` type that owns the guard and derefs to `&[u8]`. The
  `KVEngine::get` trait would need a new return type (e.g.
  `KVFuture<Option<(SlotIndex, PinnedValue)>>` or a borrowing variant).
  This is a significant API change affecting the `KVEngine` trait,
  `PxLearner::engine_get`, `kv_get` response construction, and gRPC
  serialization.
- **Key copy**: Change the async FFI wrapper to accept a raw pointer
  + length instead of `Vec<u8>`, with a lifetime guarantee that the
  caller's buffer outlives the synchronous poll. This is simpler than
  the value fix but still requires careful `Send`/`unsafe` reasoning.

**Priority**: Low — the read path has fewer O(n) copies than the
write path, and the engine value copy is structurally similar to the
InMemKV clone (both return owned `Vec<u8>`). For small values the cost
is negligible. For large values the copy is measurable but still a
small fraction of total read latency (gRPC serialization + network
round-trip dominate).

**Complexity**: High — the value copy elimination requires a new C
API (`ct_get_pinned` or similar), a new Rust wrapper type with guard
semantics, and changes to the `KVEngine` trait. The key copy
elimination is Medium complexity.

**Files**: `crowtree/ffi/src/lib.rs` (`try_get`, `copy_buf`,
`AsyncCrowtree`), `crowkv/src/kv/kv_engine.rs` (`KVEngine` trait),
`crowkv/src/kv/crowtree_engine.rs` (`get`), `crowkv/src/paxos/learner.rs`
(`engine_get`), `crowkv/src/cluster/px_kv_store.rs` (`kv_get`),
`crowtree/include/crowtree/c_api.h` (new C API).

**Acceptance**:
- All existing read tests pass unchanged.
- `CrowtreeEngine::get` fast path returns a borrowed reference to the
  engine's internal buffer (no `copy_buf` on the fast path).
- The epoch guard is held until the `PinnedValue` is dropped.
- `InMemKV` continues to work (returns owned `Vec<u8>` as before, or
  adapts to the new trait signature).

---

### R22: Zero-copy Batch decode

**Problem**: `Batch::decode` (`op.rs:54`) decodes the Paxos payload
into `Vec<BatchOp>` where each `BatchOp` owns `key: Vec<u8>` and
`Op::Put(Vec<u8>)`. Each `to_vec()` is an O(n) heap allocate + memcpy.
For a batch with K keys and total payload size N, this is K+1
allocations and N bytes of memcpy — on every `learn_chosen` call,
i.e. on every write.

The `PxLogEntry.payload` is already `Bytes` (ref-counted, shared
across the accept path). `Batch::decode` could use `Bytes::slice(range)`
to create zero-copy views into the same allocation, eliminating all
`to_vec()` calls.

**Root cause**: `Op` and `BatchOp` use `Vec<u8>` for key and value
storage. `Vec<u8>` requires ownership of the underlying allocation, so
`Batch::decode` must copy. `Bytes` supports zero-copy slicing via
`Bytes::slice(range)` — O(1) ref-count bump, no allocation.

**Approach**:
- Change `Op::Put(Vec<u8>)` → `Op::Put(Bytes)`.
- Change `BatchOp.key: Vec<u8>` → `BatchOp.key: Bytes`.
- Change `Batch::decode` signature from `decode(payload: &[u8])` to
  `decode(payload: &Bytes)`. Use `payload.slice(start..end)` instead
  of `payload.get(..).to_vec()`.
- Change `apply_entry` in `learner.rs` from `payload: &[u8]` to
  `payload: &Bytes`, and the call site from `entry.payload.as_ref()`
  to `&entry.payload`.
- Update `CrowtreeEngine::apply` to use `b.key.as_ref()` and
  `v.as_ref()` when mapping to `CtBatchOp`.
- Update `InMemKV::apply` (test engine) to use `Bytes::clone` instead
  of `Vec::clone`.
- `Cell`, `EngineDiff` stay `Vec<u8>` — they are engine-internal
  storage and comparison types, separate from the decode path.
- `KVEngine` trait signature unchanged — still takes `&Batch`.

**Alternatives considered**:
- **Lifetime parameter on `Batch`** (`Batch<'a>`): would ripple through
  `KVEngine` trait, all engine impls, and all callers. High complexity,
  no benefit over `Bytes` (which is owned + zero-copy).
- **Borrowed slices (`&[u8]`)**: same lifetime problem as above.
  `Bytes` is the standard solution for owned + zero-copy in Rust.

**Priority**: Medium — eliminates one O(n) copy pass on every write.
  For small payloads (≤512 B) the effect is negligible, but for large
  payloads (≥1 MB) or high-throughput batch writes with many keys, the
  savings are meaningful.

**Complexity**: Low-Medium — mechanical type change, no trait or
  lifetime changes, ~7 files.

**Files**: `crowkv/src/kv/op.rs` (`Op`, `BatchOp`, `Batch::decode`),
`crowkv/src/paxos/learner.rs` (`apply_entry`),
`crowkv/src/kv/crowtree_engine.rs` (`apply`),
`crowkv/tests/kv/mem_kv_impl.rs` (`InMemKV::apply`),
`crowkv/tests/kv/conformance.rs` (`put`/`del` helpers),
`crowkv/tests/kv/op_codec_test.rs` (test helpers + assertions),
`crowkv/tests/kv/mem_kv_test.rs` (`Batch::decode` call sites),
`crowkv/tests/wal/replay_tests.rs` (`Batch::decode` call sites).

**Acceptance**:
- All existing KV, WAL replay, and op codec tests pass unchanged.
- No `to_vec()` call in `Batch::decode`.
- `Batch::decode` uses `Bytes::slice` for zero-copy key/value
  extraction.
- `cargo clippy -- -D warnings` passes.
- `cargo fmt --check` passes.

---

### R23: Eliminate FFI batch encode copy

**Problem**: `CrowtreeEngine::apply` (`crowtree_engine.rs:135`) maps
`BatchOp` to `CtBatchOp<'_>` (borrowed slices), then calls
`Crowtree::apply_batch` (`ffi/src/lib.rs:548`), which internally calls
`encode_batch(ops)` (`ffi/src/lib.rs:415`). `encode_batch` packs all
keys and values into a single flat `Vec<u8>` wire format
(`[u8 kind][u32 klen][key][u32 vlen][value] * count`) before passing it
to the C API `ct_apply_batch`. This is an O(n) heap allocate + memcpy
of the entire batch payload — all keys and values are copied into the
packed buffer, then the C++ engine copies them again into its internal
memtable.

The `CtBatchOp<'_>` already holds borrowed `&[u8]` slices into the
`Batch`'s `Bytes` payloads (after R22). The packing copy is purely an
FFI marshalling artifact — the C API `ct_apply_batch` accepts a single
contiguous buffer + count, forcing the Rust side to flatten the slices.

**Root cause**: `ct_apply_batch` (`c_api.h`) takes
`const uint8_t* packed, size_t packed_len, uint64_t count` — a single
packed buffer. There is no C API that accepts an array of key/value
pointer-length pairs, so the Rust FFI wrapper must serialize the slices
into one contiguous allocation.

**Approach**:
- Add a new C API `ct_apply_batch_slices` that accepts an array of
  `ct_kv_ref` structs (`{ const uint8_t* key; uint32_t key_len; const
  uint8_t* value; uint32_t value_len; uint8_t kind; }`) plus the slot
  and count. The C++ implementation iterates the array directly — no
  unpacking from a flat buffer needed.
- Change `Crowtree::apply_batch` (`ffi/src/lib.rs`) to call
  `ct_apply_batch_slices` with a stack-allocated `[ct_kv_ref; MAX_BATCH]`
  or a `Vec<ct_kv_ref>` (small, count-sized — not payload-sized).
  Eliminates `encode_batch` entirely.
- Keep `ct_apply_batch` (packed buffer API) for backward compatibility
  or remove it if no other caller exists.
- `CrowtreeEngine::apply` (`crowtree_engine.rs`) unchanged — already
  produces `CtBatchOp<'_>` with borrowed slices; only the FFI layer
  changes.

**Relationship to R3**: R3 targets the C++ *internal* copy in
`ct_apply_put` (single-key) via a direct-write alloc handle. R23
targets the Rust-side *packing* copy in `encode_batch` (multi-key).
They are independent: R3 eliminates the C++ memcpy from caller buffer
to internal buffer; R23 eliminates the Rust memcpy from scattered
slices to a flat FFI buffer. Both could be unified if the
`ct_apply_batch_slices` API is extended to accept crowtree-owned
handles (from R3's `ct_alloc`), but that is a future design decision.

**Priority**: Low — the packing copy is one `memcpy` of the total
batch payload size. For small batches (≤512 B, benchmark default) the
cost is negligible. For large batches (≥1 MB) or high-throughput
multi-key writes, the copy is measurable but still a small fraction of
total write latency (consensus RPC round-trip dominates). No current
profiling motivation.

**Complexity**: Medium — new C API surface (`ct_apply_batch_slices`,
`ct_kv_ref` struct), C++ implementation in `c_api.cpp`, Rust FFI
adapter change in `ffi/src/lib.rs`. No `KVEngine` trait or
`CrowtreeEngine` changes. Lifetime safety is straightforward (borrowed
slices outlive the synchronous FFI call).

**Files**: `crowtree/include/crowtree/c_api.h` (new C API + struct),
`crowtree/src/c_api.cpp` (implementation),
`crowtree/ffi/src/lib.rs` (`apply_batch` — call new API, remove
`encode_batch`), `crowtree/ffi/tests/ffi_test.rs` (batch tests pass
unchanged).

**Acceptance**:
- All existing FFI batch tests pass unchanged.
- No `encode_batch` call in `Crowtree::apply_batch`.
- No `Vec<u8>` allocation for batch packing in the apply path.
- `ct_apply_batch_slices` correctly applies multi-key batches with
  duplicate-key last-wins semantics (verified by existing tests).
- `pixi run cargo clippy -- -D warnings` passes.
- `pixi run cargo fmt --check` passes.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
