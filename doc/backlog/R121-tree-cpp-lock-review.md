<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R121: tree — C++ mutex/lock review fixes

A full review of all mutex/lock usage across the C++ codebase (37 files
scanned: `std::mutex`, `std::lock_guard`, `std::unique_lock`,
`std::scoped_lock`, `std::shared_mutex`/`shared_lock`,
`std::recursive_mutex`, spinlock, `std::call_once`) found 3 critical
hot-path findings, 5 medium findings, and 17 correct patterns. One
medium finding (the `MetricsRegistry::register_*` data race) has
already been addressed in landed code — `register_*` now acquires
`flush_mutex_`, serializing with `flush_to()`. This item tracks the
remaining fixes for the critical and medium findings; the OK findings
are documented for reference and need no action.

**Current behavior + impact:**
- The single highest-impact finding is `BufferPool::pin` holding the
  global mutex during synchronous disk I/O on a cache miss — every cache
  hit blocks behind every miss. This makes the buffer pool mutex a
  global serialization point on any workload with cold-page demand
  loading. The same `mu_` is also held across `pin_new`, `acquire_frame`,
  `release_frame`, `unpin`, `mark_dirty`, and `stats()`.
- `ConcurrentSkipList` uses a pure busy-wait spinlock (no backoff, no
  yield, no fairness) on the `apply_batch` → `MemTable::upsert` write
  hot path — under multi-threaded apply contention this wastes CPU and
  can starve writers indefinitely.
- `HandlerRegistry::get_handler` takes a mutex on every RPC frame
  dispatch even though handlers are registered once at startup and never
  change — unnecessary contention on the dispatch hot path.
- ~~`MetricsRegistry::register_*` data race~~ — **already fixed**: all
  `register_*` functions now acquire `flush_mutex_` (same lock as
  `flush_to()`), eliminating the unsynchronized `push_back` vs
  iteration race. No action needed.
- `ConnectionPool::get`/`get_for` takes a mutex on every outbound RPC
  connection acquire (low priority — small pool, short critical section).
- `thread_name_flag::format` takes a mutex on every log line even though
  thread names are set once and never change (runs on spdlog's async
  backend, so doesn't block app threads directly, but serializes log
  formatting).
- `Crowdbtree::resident` cold path holds `load_mutex_` during disk I/O,
  serializing all demand loads globally (by design — hot path is
  lock-free; only a concern if cold-path concurrency becomes a
  bottleneck).
- `slot_mutex_` guards a `std::set` insert on the apply path for slot
  tracking (low priority — slots are mostly in-order, set stays small).

**Design pointers:**
- `design/tree/design-crowdb-tree.md` — buffer pool and page cache design
  (the `BufferPool::pin` finding is the storage engine's core page-access
  path).
- `design/rpc/design-crowdb-rpc.md` §6 (zero-copy dispatch, handler
  registration model — the `HandlerRegistry` finding).
- `design/kv/design-crowdb-kv-observability.md` — metrics registry design
  (the `MetricsRegistry` data-race finding, now fixed).

**Use scenarios:**
- A multi-threaded workload with a working set larger than the buffer
  pool: concurrent `pin()` calls for different cold pages serialize on
  `mu_`, so cache hits stall behind misses — throughput collapses to
  single-thread I/O latency. Expected after fix: cache hits proceed
  lock-free while misses do I/O outside the pool lock.
- Multi-threaded `apply_batch` contending on the L0 memtable: writers
  spin on the skip-list spinlock with no backoff, burning CPU and
  starving under continuous contention. Expected after fix: writers
  yield/backoff, no indefinite starvation.
- A high-RPC-throughput server dispatching frames: every `get_handler`
  call pays a mutex lock/unlock for a table that never changes after
  startup. Expected after fix: dispatch is lock-free (read-only map or
  flat array).
- A metric registered lazily after `start()` while the flush thread is
  running: concurrent `push_back` + iteration was a data race (UB).
  **Already fixed** — `register_*` now acquires `flush_mutex_`,
  serializing with the flush thread. No action needed.

## Solution

**One-line summary:** Move disk I/O outside the buffer pool mutex, add
backoff to the skip-list spinlock, make handler dispatch lock-free, and
make thread-name lookup lock-free; lower-priority findings are
documented with recommended approaches for later.

1. **`BufferPool::pin` — I/O outside the pool lock** —
   `lib/crowdb-tree/src/buffer_pool.cpp` lines 185–220. Split the fast
   path (hit: lock, increment pin count, unlock) from the slow path
   (miss: lock, find victim, unlock, do I/O, re-lock, install). Also
   fix `acquire_victim()` (lines 152–183 → `write_back()` at line 170
   under `mu_`) and `flush_dirty()` (lines 296–308, holds `mu_` while
   writing back all dirty frames). The same `mu_` is held across
   `pin_new` (224), `acquire_frame` (247), `release_frame` (267),
   `unpin` (281), `mark_dirty` (289), and `stats()` (312) — all should
   be audited as part of the split. Highest-impact fix in the whole
   review. Consider a per-frame lock or lock-free hash table for the
   lookup if the split-path approach still contends on re-install.

2. **`ConcurrentSkipList` spinlock backoff** —
   `lib/crowdb-tree/include/crowdb-tree/skip_list.h` lines 225–244;
   `lib/crowdb-tree/src/skip_list.cpp` lines 109, 210. Add exponential
   backoff (`std::this_thread::yield()` or PAUSE) to the
   `SpinlockGuard`. For higher contention, consider a ticket lock
   (fair) or per-shard spinlocks. Verify the actual concurrency model
   (Paxos leader is single-writer per slot) before over-investing —
   contention may be low in practice.

3. **`HandlerRegistry::get_handler` — lock-free dispatch** —
   `lib/crowdb-rpc/include/crowdb-rpc/server/handler.h` lines 49–57.
   Populate a `std::unordered_map` (or flat array indexed by `msg_type`
   if the type space is small) at startup and make it read-only — no
   lock needed for reads. Falls back to `std::shared_mutex` if
   late registration must be supported.

4. **`thread_name_flag::format` — lock-free thread-name lookup** —
   `lib/crowdb-common/cpp/src/log.cpp` lines 59–65. Use
   `std::shared_mutex` (shared for format, unique for
   `set_current_thread_name`), or a lock-free read path since names
   are set once.

5. **`ConnectionPool::get`/`get_for` — low priority** —
   `lib/crowdb-rpc/src/pool.cpp` lines 13–28 (`get`), 30–45 (`get_for`).
   Only if profiling shows contention: lock-free round-robin over a
   snapshot, or per-thread connection caching.

6. **`Crowdbtree::resident` cold path — low priority** —
   `lib/crowdb-tree/src/crowdb-tree.cpp` lines 324–388. Acceptable as-is
   (hot path is lock-free by design). If cold-path concurrency becomes
   a bottleneck, use per-page load locks (striped by `page_id`).

7. **`slot_mutex_` — low priority** —
   `lib/crowdb-tree/src/crowdb-tree.cpp` lines 795–797 (`note_applied_slot`),
   876 (`apply` call site). Acceptable as-is (slots mostly in-order,
   set stays small). If out-of-order gaps are common, use a lock-free
   bitmap or bounded ring buffer.

**Edge cases at a glance:**
- `BufferPool::pin` miss → victim eviction needs `write_back()` — I/O
  for the victim must also be outside the pool lock; re-install after
  I/O must handle a concurrent `pin()` that found the same victim.
- Skip-list spinlock with backoff → a writer that yields must not lose
  fairness vs. a writer that doesn't yield (ticket lock avoids this).
- Handler registry made read-only → late registration after first
  dispatch must either panic or be supported via `shared_mutex`.

## Dependencies

- None — all findings are in landed code. The `BufferPool` fix is
  self-contained within `crowdb-tree`; the `HandlerRegistry` fix is
  self-contained within `crowdb-rpc`; the `thread_name_flag` fix is
  self-contained within `crowdb-common`. No cross-component ordering.
- The `MetricsRegistry::register_*` data race (originally finding #4)
  has already been fixed — `register_*` now acquires `flush_mutex_`.
  R122 (Rust lock review) item 5 shares the same `MetricsRunner`
  collector pattern (Rust `MetricsRunner` / `engine_collector` still
  holds `registry.lock()` during the collector callback); that side
  is partially addressed and tracked separately under R122.

## Acceptance

**BufferPool (work item 1):**
- `pin()` on a cache hit with a concurrent miss in progress → hit
  returns without waiting for the miss's I/O to complete. Unit test
  (mock `store_->read_at` with a delay, verify hit latency unaffected).
- `pin()` miss → `read_at` is called with `mu_` not held (verify via
  a test `PageStore` that records whether the lock is held during
  `read_at`). Unit test.
- `acquire_victim()` → `write_at` is called with `mu_` not held. Unit
  test.
- `flush_dirty()` → `write_at` calls are outside `mu_`. Unit test.
- Concurrent `pin()` for different cold pages → both I/Os proceed in
  parallel (not serialized). Unit test with two threads + mock store
  tracking concurrent `read_at` calls.

**Skip-list spinlock (work item 2):**
- Under N contending writers, no writer is starved indefinitely (every
  writer completes within a bounded number of iterations of a fairness
  probe). Unit test.
- `upsert()` correctness preserved under contention (no lost updates,
  no corruption). Unit test (existing `skip_list_test.cpp` multi-
  threaded tests pass).

**HandlerRegistry (work item 3):**
- `get_handler` on the dispatch path takes no mutex (verify via a
  test that registers handlers, then dispatches N frames concurrently
  with a contention probe — no lock contention detected). Unit test.
- Late registration after first dispatch → either panics with a clear
  message (read-only map) or succeeds safely (`shared_mutex` variant).
  Unit test.

**thread_name_flag (work item 4):**
- `format()` on the spdlog backend with a concurrent
  `set_current_thread_name` → no data race (ThreadSanitizer clean).
  Unit test.

**Lower-priority items (work items 5–7):**
- Documented as deferred — no acceptance bullets required unless
  implemented. If implemented, add per-item unit tests as above.

**All items:**
- `pixi run test-tree-ct` passes (existing 383 tests + new tests).
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
- `pixi run clang-format --dry-run --Werror` on changed `.cpp`/`.h`.
- `pixi run tree-lint` on changed C++ files.
