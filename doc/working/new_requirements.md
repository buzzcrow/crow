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

### Unprioritized

**Complexity — Medium:**
- **R8** — Metrics module + time-based logs — Area: crowkv observability — No
  structured metrics collection; operational visibility relies on ad-hoc log
  lines.

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

### R8: Metrics module + time-based logs

**Problem**: No structured metrics collection or periodic metrics logging.
Operational visibility relies on ad-hoc log lines.

**Priority**: Unprioritized.

**Complexity**: Medium — define metrics (latency p50/p99, throughput, error
rate, WAL flush lag, election count), collect via `tracing` spans or atomic
counters, emit periodic summary logs or expose a `/metrics` endpoint.

**Files**: new `crowkv/src/metrics/` module, `crowkv-server/src/main.rs`.

**Acceptance**: `/metrics` endpoint returns structured metrics. Unit test
for counter/histogram collection. Log line every N seconds with summary.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
