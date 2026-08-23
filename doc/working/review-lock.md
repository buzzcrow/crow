# Mutex / Lock Review

Review of all mutex and lock usage across the C++ codebase.
Each file with mutex usage was inspected; findings are organized by
severity — **Critical** (hot path, significant performance or correctness
impact), **Medium** (potential issue or suboptimal pattern), and **OK**
(correct, no action needed).

Scanned 37 files with `std::mutex`, `std::lock_guard`, `std::unique_lock`,
`std::scoped_lock`, `std::shared_mutex`/`shared_lock`, `std::recursive_mutex`,
spinlock (`std::atomic<bool>` test-and-set), and `std::call_once` usage.

---

## Critical — Hot Path / Significant Impact

### 1. `BufferPool::pin` holds global mutex during disk I/O

- **File:** `lib/crow-tree/src/buffer_pool.cpp` lines 177–218
- **Lock:** `std::scoped_lock lk(mu_)` held for the entire `pin()` call,
  including `store_->read_at()` (line 202) on the miss path.
- **Problem:** A cache **miss** performs synchronous disk I/O while holding
  `mu_`. Every other `pin()` call — including **cache hits** that only need
  to increment a pin count — blocks behind the I/O. On a workload with any
  cold-page demand loading, the buffer pool mutex becomes a global
  serialization point that stalls all page accesses.
- **Also affected:** `acquire_victim()` (line 144) calls `write_back()` →
  `store_->write_at()` under `mu_` (line 162). `flush_dirty()` (line 294)
  holds `mu_` while writing back all dirty frames.
- **Recommendation:** Separate the fast path (hit: lock, increment pin,
  unlock) from the slow path (miss: lock, find victim, unlock, do I/O,
  re-lock, install). Or use a finer-grained per-frame lock / lock-free
  hash table for the lookup, with I/O performed outside any pool-level
  lock. This is the single highest-impact lock finding.

### 2. `ConcurrentSkipList` spinlock on the write hot path

- **File:** `lib/crow-tree/include/crow-tree/skip_list.h` lines 225–244;
  `lib/crow-tree/src/skip_list.cpp` lines 109, 210
- **Lock:** `SpinlockGuard` — `std::atomic<bool>` with
  `exchange(true, acquire)` / `store(false, release)`, pure busy-wait
  (no backoff, no yield, no fairness).
- **Problem:** This spinlock serializes all writers in `upsert()` and
  `drain_up_to()`. `upsert()` is on the `apply_batch` → `MemTable::upsert`
  hot path. Under multi-threaded apply contention:
  - No backoff → excessive CPU spinning (wastes cycles, hurts throughput).
  - No fairness → a writer can starve indefinitely under continuous
    contention (thundering-herd on the atomic).
  - `std::mt19937 rng_` is accessed under the spinlock (required — not
    thread-safe), which adds state to the critical section.
- **Recommendation:** Add exponential backoff (`std::this_thread::yield()`
  or PAUSE). For higher contention, consider a ticket lock (fair) or
  per-shard spinlocks. If the skip list is the L0 memtable and apply is
  single-writer per slot (Paxos leader), contention may be low in
  practice — verify the actual concurrency model before over-investing.

### 3. `HandlerRegistry::get_handler` — mutex on every frame dispatch

- **File:** `lib/crow-rpc/include/crow-rpc/server/handler.h` lines 49–57
- **Lock:** `std::lock_guard<std::mutex> lock(mu_)` in `get_handler()`,
  called for **every received frame** to look up the handler by `msg_type`.
- **Problem:** Handlers are registered once at startup and never change
  during steady-state operation, yet every frame dispatch pays a full
  mutex lock/unlock. On a high-RPC-throughput server this is unnecessary
  contention on the dispatch hot path.
- **Recommendation:** Use `std::shared_mutex` (shared lock for
  `get_handler`, unique for `register_handler`), or — better — populate a
  `std::unordered_map` at startup and make it read-only (no lock needed
  for reads if no concurrent writes). A flat array indexed by `msg_type`
  (if the type space is small) would eliminate the lock entirely.

---

## Medium — Potential Issues / Suboptimal Patterns

### 4. `MetricsRegistry::register_*` — unsynchronized vector push_back

- **File:** `lib/crow-common/cpp/src/metrics/metrics.cpp` lines 35–73
- **Problem:** `register_counter`, `register_gauge`, etc. call
  `push_back()` on `counters_`/`gauges_`/etc. **without** holding
  `flush_mutex_`. Meanwhile `flush_to()` (line 101) iterates those same
  vectors **under** `flush_mutex_`. If a metric is registered
  concurrently with a flush (the background flush thread is running),
  this is a data race on the vector (UB: concurrent `push_back` +
  iteration).
- **Current safety assumption:** Metrics are registered at startup
  before `start()` spawns the flush thread. This is fragile — any
  late/lazy metric registration triggers the race.
- **Recommendation:** Either (a) hold `flush_mutex_` in `register_*`,
  or (b) document/enforce that all registration must complete before
  `start()`, or (c) use a concurrent container or freeze the registry
  after start.

### 5. `ConnectionPool::get` / `get_for` — mutex on every connection acquire

- **File:** `lib/crow-rpc/src/pool.cpp` lines 13–45
- **Lock:** `std::lock_guard<std::mutex> lock(mu_)` on every
  `get()` / `get_for()` call.
- **Problem:** These are on the RPC client send path — every outbound
  RPC acquires a connection from the pool. Under high QPS with many
  threads, the single mutex serializes all connection selection.
- **Mitigating factor:** The pool is typically small (few connections),
  and the critical section is short (vector scan). Contention is
  bounded by connection count, not request count.
- **Recommendation:** Low priority. If profiling shows contention,
  consider a lock-free round-robin over a snapshot, or per-thread
  connection caching.

### 6. `thread_name_flag::format` — mutex on every log line

- **File:** `lib/crow-common/cpp/src/log.cpp` lines 59–65
- **Lock:** `std::lock_guard<std::mutex> lk(g_thread_names_mu)` in the
  spdlog custom flag formatter, invoked for **every log message**.
- **Problem:** Thread names are set once (via `set_current_thread_name`)
  and never change, yet every log line locks the mutex to look one up.
- **Mitigating factor:** This runs on spdlog's **async backend thread**,
  not the caller thread, so it doesn't block application threads
  directly. But it does serialize all log formatting on one mutex.
- **Recommendation:** Use `std::shared_mutex` (shared lock for format,
  unique for `set_current_thread_name`), or a
  `folly::ConcurrentHashMap` / lock-free read path.

### 7. `Crowtree::resident` cold path — `load_mutex_` during disk I/O

- **File:** `lib/crow-tree/src/crow-tree.cpp` lines 332–371
- **Lock:** `std::lock_guard<std::mutex> lk(load_mutex_)` held during
  `opt_.page_store->read_at()` (line 345) and `install_loaded_page()`.
- **Problem:** All demand loads are serialized globally by `load_mutex_`.
  Two concurrent cold-page accesses to different pages block each other.
- **Mitigating factor:** This is **by design** — double-checked locking
  with the hot (resident) path lock-free (line 314–327). The cold path
  is expected to be rare once the working set is resident. The comment
  at line 328–331 documents this explicitly.
- **Recommendation:** Acceptable as-is for the common case. If cold-path
  concurrency becomes a bottleneck (large working set, limited buffer
  pool), consider per-page load locks (e.g. a striped lock by page_id)
  to allow parallel demand loads.

### 8. `slot_mutex_` — `std::set` insert on the apply path

- **File:** `lib/crow-tree/src/crow-tree.cpp` lines 776–779, 875–878
- **Lock:** `std::lock_guard<std::mutex> lk(slot_mutex_)` in
  `note_applied_slot()` / `force_advance_slot()`, inserting into
  `std::set<uint64_t> received_slots_`.
- **Problem:** `note_applied_slot` is called on the apply path. Under
  high-throughput out-of-order delivery, the set grows and the mutex
  serializes all slot tracking.
- **Mitigating factor:** Slots are mostly in-order (Paxos), so the set
  stays small and `recompute_contiguous_locked` prunes it frequently.
- **Recommendation:** Low priority. If out-of-order gaps are common,
  consider a lock-free bitmap or a bounded ring buffer.

---

## OK — Correct, No Action Needed

These were reviewed and found correct:

- **`MappingTable::alloc_mu_`** (`mapping_table.cpp` lines 75, 109, 161,
  170, 176) — guards segment allocation and `next_page_id_`. Not on the
  hot path; segment allocation is rare. Slot reads/writes are lock-free
  via `std::atomic`. Correct.

- **`EpochManager::reclaim_mu_`** (`epoch.cpp` lines 84, 122, 162, 168)
  — `std::recursive_mutex` on the writer/reclaim side only. Readers
  (`enter()`/`Guard::release()`) are fully lock-free (line 19–67).
  The recursive mutex is required because a deleter can call `retire()`
  again on the same thread (documented at line 131–158). The
  swap-before-iterate pattern (line 145–146) correctly avoids
  iterator invalidation from nested retirements. Correct.

- **`MemPageStore` / `MemoryMedium`** (`page_store.cpp`,
  `block_page_store.cpp` lines 84, 94, 113) — simple mutex for
  in-memory store. Used in tests and `open_mem` mode. Fine.

- **`BlockPageStore`** — no internal mutex; relies on caller
  serialization (via `BufferPool::mu_` or `write_mutex_`). Correct by
  design.

- **`ScheduledExecutor`** (`scheduled_executor.cpp`) — mutex guards the
  task map; callbacks are collected and run **outside** the lock (line
  58–61). Correct pattern (avoids re-entrant deadlock if a callback
  schedules a new task).

- **`BlockingEngine`** (`blocking_engine.cpp`) — standard
  producer/consumer queue with `mutex` + `condition_variable`. I/O
  performed outside the lock (line 108–132). Correct.

- **`RdmaBufferPool`** (`rdma_transport.cpp` lines 39, 111, 127) —
  mutex for free-list map. RDMA transport is a stub; not hot. Fine.

- **`RpcClient::reaper_mu_`** (`client.cpp` lines 265, 323) — only
  guards the condition_variable wait; `pending_` is a
  `folly::ConcurrentHashMap` (striped locks, thread-safe without
  external mutex). The reaper iterates `pending_` lock-free, which is
  safe for `folly::ConcurrentHashMap`. Correct.

- **`EpollEngine` / `KqueueEngine` `conn_mu_`** (`epoll_engine.cpp`
  lines 92, 105; `kqueue_engine.cpp` lines 61, 76) — only guards the
  `connections_` map for add/remove. Event dispatch (`wait()`) uses
  kernel-passed `udata`/`data.ptr` (Connection*) with **no userspace
  lock** (documented at line 69–70, 88). Correct and efficient.

- **`Worker::conns_mu_`** (`socket_transport.cpp` line 73) — only for
  connection map add/remove. The I/O loop (`run_loop`) dispatches via
  `ev.conn` with no map lookup. Correct.

- **`Crowtree::memtable_mutex_`** (`crow-tree.cpp` lines 987, 993,
  1007, 1040, 1146) — `std::shared_mutex`. Read path (`current_active`,
  `all_memtables`) takes a **shared** lock; write path
  (`maybe_swap_active`, `reset_memtables_locked`) takes a **unique**
  lock. The shared lock on the hot read path is efficient. Good pattern.

- **`Crowtree::write_mutex_`** (`crow-tree.cpp` lines 628, 711, 897,
  1126, 3048, 3094, 3194, 3251; `persist.cpp` lines 669, 721, 844) —
  serializes all writers (flush, consolidate, split/merge, GC, snapshot,
  install_snapshot, clear). Held for the duration of potentially slow
  operations (flush drains memtables into L1). This is **by design** —
  the engine is single-writer; readers are lock-free (epoch-guarded).
  The `snapshot_inflight_` atomic spin-gate (not a mutex) correctly
  handles the cross-thread async snapshot commit phase where a mutex
  cannot be used (documented at `crow-tree.h` lines 393–415). Correct.

- **`compressing_file_sink`** (`compressing_sink.cpp`) — the mutex is
  spdlog's sink mutex (`std::mutex` or `null_mutex`), standard spdlog
  pattern. Correct.

- **`compressing_sink.cpp` line 134** — `std::atomic_flag` match was a
  false positive (`vector::clear()`, not `atomic_flag::clear()`).
  No lock usage.

- **`framing.cpp`** — `std::atomic_flag`/`.clear()` matches were false
  positives (`std::vector::clear()`). `FrameParser` is single-threaded
  per connection (no shared state). No lock needed. Correct.

- **Tests/benches** (`skip_list_test.cpp`, `persist_test.cpp`,
  `leaf_cursor_test.cpp`, `scan_step_bench.cpp`) — mutex usage in test
  harnesses for thread synchronization. Not production code. Fine.

---

## Summary

- **3 critical findings** — all on hot paths (buffer pool I/O under lock,
  skip list spinlock, handler dispatch mutex).
- **5 medium findings** — data race risk in metrics registry, mutex on
  RPC/log hot paths, global load serialization, slot tracking.
- **17 OK** — correct patterns, no action needed.

The highest-impact fix is **#1 (BufferPool I/O under lock)** — moving
disk I/O outside the buffer pool mutex would unblock all cache-hit
accesses during demand loads and evictions.
