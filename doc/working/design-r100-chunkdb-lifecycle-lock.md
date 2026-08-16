<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Per-Chunk-ID Lifecycle Lock + Chunk Cache (R100)

Implementation design draft for
[`doc/backlog/R100-chunkdb-lifecycle-lock.md`](../backlog/R100-chunkdb-lifecycle-lock.md).
Root design: `doc/design/chunkdb/design-crow-chunkdb.md` §9 (Chunk
Lifecycle), §12 (Concurrency Model). Architecture decisions and rationale
are in the root design; this doc does not repeat them.

**Already landed:** R99 (chunkdb instance sharding — `RangeGuard`,
`BindingCache`, `NotMyRange` reject-and-retry) is committed (`9dcdfa0`).
The `LifecycleHandler` (`lifecycle.rs`) has 4 mutating RPCs
(allocate/append/seal/delete) with no per-chunk serialization — concurrent
RMW cycles race on `ChunkStore::put_chunk` (blind overwrite, no CAS).
`ChunkStore` (`storage.rs`) routes via `BindingCache` to the owning KV
group. The chunkdb HTTP server (`main.rs:129`) has `/ready` + `/health`.

**Gap decisions (from `doc/working/gap.md`):**
- GAP-R100-1: diskdb calls inside the lock (option A); `LockTimeout`
  frequency counter to consider switching to B; warning log when lock hold
  exceeds a configurable threshold (default 1s).
- GAP-R100-2: periodic sweep, interval configurable via
  `sweep_chunk_lock_interval` (default 60s).
- GAP-R100-3: default cache capacity 10_000 (configurable); design supports
  100_000+.
- GAP-R100-4: `quick-cache = "0.7"` (published 2026-06-27, >7 days old).

## 1. ChunkLockMap — per-chunk lock + payload cache

### 1.1 Why

`LifecycleHandler` has no per-chunk serialization. Two concurrent
`AppendChunk` RPCs on the same chunk ID both read the chunk, both append
strips, both `put_chunk` — the second overwrite loses the first's strips.
A per-chunk mutex serializes the RMW cycle. The payload cache avoids the
`get_chunk` store round-trip on every mutating RPC (the latest chunk is
known in-process right after the previous `put_chunk`).

The lock and the payload have different eviction requirements (lock: evict
only when uncontended; payload: evict freely by recency/frequency), so they
are separate structures — see the backlog doc's "Why two tiers" section.

### 1.2 Struct definitions

```rust
// app/crow-chunkdb/src/lifecycle.rs

use std::sync::Arc;
use std::time::Duration;
use dashmap::DashMap;
use quick_cache::Cache;
use tokio::sync::Mutex;

/// Per-chunk lock map + payload cache.
pub struct ChunkLockMap {
    locks: DashMap<ChunkId, Arc<Mutex<()>>>,
    chunks: Cache<ChunkId, Chunk>,
    metrics: LifecycleMetrics,
}

/// Lock policy — how to handle contention.
#[derive(Debug, Clone)]
pub enum LockPolicy {
    /// Fail fast with `LockBusy` on contention.
    TryLock,
    /// Park the task up to `duration`, then `LockTimeout`.
    Wait(Duration),
}

impl Default for LockPolicy {
    fn default() -> Self {
        Self::Wait(DEFAULT_LOCK_WAIT)
    }
}

const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(10);

/// Cache hint — whether to populate the payload cache.
#[derive(Debug, Clone, Copy, Default)]
pub enum CacheHint {
    /// Populate cache on miss, write to cache on refresh (default).
    #[default]
    Cache,
    /// Skip cache population — always fetch from store.
    NoCache,
}

/// Guard — holds the lock, carries the latest chunk record.
pub struct ChunkGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    chunk: Option<Chunk>,
    hint: CacheHint,
    chunk_id: ChunkId,
    hold_start: std::time::Instant,
    metrics: LifecycleMetrics,
}
```

### 1.3 ChunkLockMap methods

a. `new(cache_capacity, metrics) -> Self` — creates an empty `DashMap` and
a `Cache::new(cache_capacity)`.

b. `acquire(&self, chunk_id, store, policy, hint) -> Result<ChunkGuard,
   LifecycleError>` — for existing chunks (append/seal/delete). Steps:
   1. `entry().or_default()` to get-or-create the `Arc<Mutex<()>>`.
   2. Record lock-wait start time.
   3. Acquire the mutex per `policy`:
      - `TryLock` → `try_lock_owned()`. On `Err`, increment
        `lock_busy_count`, return `LockBusy`.
      - `Wait(d)` → `lock_owned()` with `tokio::time::timeout(d, ...)`. On
        timeout, increment `lock_timeout_count`, return `LockTimeout`.
   4. Record lock-wait time into the histogram.
   5. Serve payload: if `hint == Cache` and `self.chunks.get(&chunk_id)`
      returns `Some(chunk)` → cache hit (increment `cache_hit_count`),
      return guard with that chunk. Otherwise → cache miss (increment
      `cache_miss_count`), `store.get_chunk(chunk_id)`. On
      `StoreError::ChunkNotFound` → return `ChunkNotFound`. On success,
      if `hint == Cache`, `self.chunks.insert(chunk_id, chunk.clone())`.
   6. Return `ChunkGuard` with the chunk, hint, hold_start, metrics.

c. `acquire_for_create(&self, chunk_id, policy, hint) -> Result<ChunkGuard,
   LifecycleError>` — for `allocate_chunk` with caller-supplied ID. Same
   lock acquisition as (b) but does NOT fetch from store (chunk does not
   exist yet). Returns a guard with `chunk: None`. Caller must `refresh()`
   after creating the chunk.

d. `reap_idle(&self)` — iterates `self.locks.retain(|_, arc|
   Arc::strong_count(arc) > 1)`. Entries where only the map holds a clone
   (`strong_count == 1`) are removed. Increments `reap_idle_count` and
   `reap_idle_entries_removed` by the number removed. Payload cache is
   untouched (bounded by its own capacity).

e. `invalidate_chunk(&self, chunk_id) -> bool` — calls
   `self.chunks.remove(&chunk_id).is_some()`. Increments
   `invalidate_count`. Used by R99 range migration (not called in v1).

f. `invalidate_range(&self, bucket_start, bucket_end) -> u32` — iterates
   cache entries (via `self.chunks.iter()` if available, or a snapshot of
   keys) and removes those whose chunk ID hashes to a bucket in
   `[bucket_start, bucket_end]`. Returns the count removed. Increments
   `invalidate_count` by the count. O(n) on cache size — acceptable for
   rare range migrations.

### 1.4 ChunkGuard methods

a. `chunk(&self) -> Option<&Chunk>` — returns the latest chunk record.
   `None` for `acquire_for_create` before `refresh`.

b. `refresh(&mut self, chunk: Chunk)` — updates the guard's local copy. If
   `hint == Cache`, also writes to `self.chunks` via the lock map's cache
   reference (the guard holds a clone of the `Cache` or a reference to
   `ChunkLockMap`). Caller MUST have persisted via `put_chunk` first.

c. `Drop` — records lock-hold time into the histogram (from `hold_start`
   to now). If hold time > `lock_hold_warn_threshold` (configurable,
   default 1s), emits a `warn!` log with chunk_id and hold duration.

### 1.5 Edge cases

- Lock map entry does not exist → created on first `acquire` via
  `DashMap::entry().or_default()`.
- Lock holder panics → `tokio::sync::Mutex<()>` auto-releases (no
  poisoning for `Mutex<()>`); cache slot may be stale but next `acquire`
  re-fetches on miss.
- `reap_idle` runs while an acquirer holds a clone → `Arc::strong_count >
  1`, entry is retained. No race: `DashMap::retain` holds the shard lock.
- Process crash → all in-memory state lost; KV store is source of truth.

## 2. LockTimeout frequency counter + hold-time warning

### 2.1 Why

GAP-R100-1 decision: keep diskdb calls inside the lock (option A), but add
observability to detect when lock hold time becomes a problem. A
`LockTimeout` frequency counter tracks how often timeouts occur — if it
rises, the operator can tune `DEFAULT_LOCK_WAIT` or consider switching to
option B (release lock after `put_chunk`, before commit/free). A
hold-time warning log fires when any single lock hold exceeds a
configurable threshold, surfacing slow diskdb RPCs without waiting for
aggregate metrics.

### 2.2 Implementation

a. `LifecycleMetrics` gains `lock_timeout_count: AtomicU64` (already in
   the backlog doc) — incremented on every `LockTimeout` return from
   `acquire`.

b. `ChunkGuard` records `hold_start: Instant` at acquisition. On `Drop`,
   computes `hold_duration = hold_start.elapsed()`. If
   `hold_duration > lock_hold_warn_threshold`, emits:
   ```rust
   warn!(chunk_id = ?self.chunk_id, hold_ms = hold_duration.as_millis(),
         "chunk lock held longer than threshold");
   ```
   The threshold is read from `ChunkLockMap` config (passed into the guard
   at acquisition time).

c. The hold-time histogram (`lock_hold_time`) is always recorded (not just
   on threshold exceed) — the warning is an additional real-time signal
   on top of the histogram.

### 2.3 Edge cases

- Threshold = 0 → warning fires on every drop (useful for debugging;
  not the default).
- Guard dropped during panic → `Drop` still runs (Rust guarantee); the
  warning may fire if the panic was slow, which is correct behavior.

## 3. Chunk cache — quick-cache integration

### 3.1 Why

Every mutating RPC does a `get_chunk` round-trip before the RMW
(`lifecycle.rs:169/209/231`). Under repeated operations on the same chunk
(several appends in a row), this is wasted latency. The cache stores the
latest chunk record in-process, populated on first acquire (miss → one
`get_chunk`) and refreshed by the writer after each `put_chunk`. Subsequent
acquires for the same chunk are cache hits (zero store round-trips).

### 3.2 Configuration

GAP-R100-3: default capacity 10_000 entries (configurable via
`lifecycle.cache_capacity` in `crow-chunkdb.toml`). The design supports
100_000+ — `quick_cache::Cache::new(capacity)` accepts any `usize`; the
only constraint is memory (~1-2 KB per `Chunk` → 10k entries ≈ 10-20 MB,
100k entries ≈ 100-200 MB).

### 3.3 Dependency

`quick-cache = "0.7"` (GAP-R100-4). Published 2026-06-27, >7 days old.
Zero new transitive crates (`equivalent`, `hashbrown`, `foldhash` already
in `Cargo.lock`). Default features: `custom-hasher` (uses `foldhash`),
`parking_lot`.

### 3.4 Cache operations

a. `Cache::get(&key) -> Option<Chunk>` — cache hit check. O(1).
b. `Cache::insert(key, value)` — populate on miss, refresh after
   `put_chunk`. O(1) amortized.
c. `Cache::remove(&key) -> Option<(Key, Val)>` — `invalidate_chunk`.
   O(1).
d. `Cache::entry_count()` — gauge for metrics snapshot.
e. For `invalidate_range`, iterate via a key snapshot: collect all keys
   from `self.chunks.iter()` (if the API provides iteration) or maintain
   a parallel `DashMap<ChunkId, ()>` key index. Verify the `quick_cache`
   iteration API at implementation time; if no public iter, fall back to
   a parallel key set in `DashMap` (small overhead, acceptable for the
   rare invalidation path).

### 3.5 Edge cases

- Cache evicts a chunk between two operations → next `acquire` is a miss,
  re-fetches. Correctness unaffected.
- `CacheHint::NoCache` → skip `insert` on miss and `refresh`; the guard's
  local copy is still updated so the current operation sees the chunk.
- `delete_chunk` keeps the Deleted-state chunk cached via `refresh` →
  next `delete_chunk` retry gets a cache hit, returns `ChunkNotFound`
  without a store round-trip.

## 4. Sweep task — periodic reap_idle

### 4.1 Why

GAP-R100-2: the lock map grows unbounded without reaping (one entry per
chunk ever touched). `reap_idle` removes uncontended entries
(`Arc::strong_count == 1`) periodically, keeping the map bounded by
concurrent locks, not by chunks-ever-touched.

### 4.2 Implementation

a. Config field: `lifecycle.sweep_chunk_lock_interval` (default 60s).
   `LifecycleConfig` gains:
   ```rust
   pub struct LifecycleConfig {
       pub cache_capacity: usize,
       pub sweep_chunk_lock_interval_secs: u32,
       pub lock_hold_warn_threshold_ms: u64,
   }
   ```
   Defaults: `cache_capacity = 10_000`,
   `sweep_chunk_lock_interval_secs = 60`,
   `lock_hold_warn_threshold_ms = 1000`.

b. `main.rs` spawns a background task (alongside the topology refresh
   loop) that calls `locks.reap_idle()` every
   `sweep_chunk_lock_interval`. Uses the same `watch::channel(false)`
   stop signal pattern as the refresh loop.

c. `reap_idle` is a single `DashMap::retain` call — no allocation, no
   blocking. The task is lightweight.

### 4.3 Edge cases

- `sweep_chunk_lock_interval = 0` → treated as "disabled" (no sweep task
  spawned). `validate()` rejects 0 for the interval field if sweep is
  required; or the spawn logic skips when 0. Decision: `validate()`
  rejects 0 — sweep is required for correctness (unbounded lock map is a
  memory leak).

## 5. LifecycleHandler integration

### 5.1 Why

The 4 mutating RPCs (allocate/append/seal/delete) must acquire the
per-chunk lock before their RMW cycle. Read-only RPCs (query/list) bypass
both lock and cache.

### 5.2 Changes to LifecycleHandler

a. New field: `locks: Arc<ChunkLockMap>`.

b. `new()` gains a `cache_capacity` parameter (or a `LifecycleConfig`).
   `with_range_guard()` stays unchanged. Add `with_locks()` or pass
   `ChunkLockMap` into `new()`.

c. `allocate_chunk` (caller-supplied ID):
   1. `check_range(&id)?`
   2. `let mut guard = self.locks.acquire_for_create(&id, policy, hint)?`
   3. Existence check: `store.get_chunk(&id)`. If exists → return
      `ChunkAlreadyExists`. (One-shot fetch, not cached — the chunk
      exists and we're about to reject.)
   4. Build chunk, allocate strips, `put_chunk`, `commit_strip_segments`.
   5. `guard.refresh(chunk)`.
   6. Return chunk.

d. `allocate_chunk` (auto-generated ID): skip the lock (UUID collision
   negligible). After `put_chunk`, populate cache directly:
   `self.locks.chunks.insert(id, chunk.clone())` (expose a
   `populate_cache(id, chunk)` method on `ChunkLockMap` for this path).

e. `append_chunk`:
   1. `check_range(chunk_id)?`
   2. `let mut guard = self.locks.acquire(chunk_id, &self.store, policy, hint)?`
   3. `let mut chunk = guard.chunk().expect("acquire returns chunk").clone();`
   4. State check, allocate strips, `put_chunk`, `commit_strip_segments`.
   5. `guard.refresh(chunk)`.
   6. Return chunk.

f. `seal_chunk`: same as append but no diskdb calls (fast path).

g. `delete_chunk`:
   1. `check_range(chunk_id)?`
   2. `let mut guard = self.locks.acquire(chunk_id, &self.store, policy, hint)?`
   3. `let mut chunk = guard.chunk().expect("acquire returns chunk").clone();`
   4. State check (already deleted → `ChunkNotFound`).
   5. Free segments (`free_blocks` — inside the lock per GAP-R100-1).
   6. `put_chunk`, `guard.refresh(chunk)` (keeps Deleted chunk cached).
   7. Return chunk.

h. `query_chunk` / `list_chunks` — unchanged (no lock, no cache).

### 5.3 Lock policy + cache hint defaults

All mutating RPCs use `LockPolicy::default()` (Wait 10s) and
`CacheHint::Cache` (default). These are not exposed in the gRPC API in v1
— the policy/hint are internal. Future extensions could add request-level
overrides.

### 5.4 Edge cases

- `acquire` returns `ChunkNotFound` (store miss during acquire) → the
  chunk does not exist; `append`/`seal`/`delete` return `ChunkNotFound`
  (already the current behavior, now surfaced from `acquire` instead of
  the explicit `get_chunk`).
- `LockBusy` / `LockTimeout` → mapped to gRPC `UNAVAILABLE` with retry
  hint in `service.rs`.

## 6. Error variants + service mapping

### 6.1 New LifecycleError variants

```rust
#[error("chunk lock busy — retry later")]
LockBusy,
#[error("chunk lock acquire timed out")]
LockTimeout,
```

### 6.2 Service mapping

`map_error` in `service.rs` gains:
```rust
LifecycleError::LockBusy => Status::unavailable(e.to_string()),
LifecycleError::LockTimeout => Status::unavailable(e.to_string()),
```
Both map to `UNAVAILABLE` — the client's existing retry logic handles
this (same as `NotLeaderHint` transient errors).

## 7. Metrics + HTTP endpoints

### 7.1 LifecycleMetrics

New module `app/crow-chunkdb/src/metrics.rs`. Lightweight atomic counters
+ latency histograms, mirroring `crow-kv-client/src/metrics.rs`.

Counters (all `AtomicU64`, `Relaxed` ordering):
- `lock_wait_time` — `PreciseHistogram` (wait duration in acquire).
- `lock_timeout_count` — incremented on `LockTimeout`.
- `lock_busy_count` — incremented on `LockBusy`.
- `lock_hold_time` — `PreciseHistogram` (hold duration in guard Drop).
- `cache_hit_count` — incremented on cache hit in `acquire`.
- `cache_miss_count` — incremented on cache miss in `acquire`.
- `cache_size` — gauge, read from `Cache::entry_count()` at snapshot.
- `reap_idle_count` — incremented each `reap_idle` run.
- `reap_idle_entries_removed` — entries removed per `reap_idle`.
- `invalidate_count` — incremented on `invalidate_chunk`/`invalidate_range`.

`snapshot() -> LifecycleMetricsSnapshot` drains counters, reads
histograms, returns a serializable struct.

### 7.2 HTTP endpoints

`main.rs` HTTP server gains:
- `GET /metrics` → returns `LifecycleMetricsSnapshot` as JSON.
- `POST /invalidate_chunk` → body `{ "chunk_id": { "high": u64, "low": u64 } }`
  → calls `ChunkLockMap::invalidate_chunk`. Returns `{ "invalidated": bool }`.
- `POST /invalidate_range` → body `{ "bucket_start": u16, "bucket_end": u16 }`
  → calls `ChunkLockMap::invalidate_range`. Returns
  `{ "invalidated_count": u32 }`.

All internal (no auth, same as `/ready` and `/health`).

## 8. Config extensions

`ChunkdbConfig` gains a `lifecycle` section:

```toml
[lifecycle]
cache_capacity = 10000
sweep_chunk_lock_interval_secs = 60
lock_hold_warn_threshold_ms = 1000
```

```rust
// chunkdb_config.rs
pub struct LifecycleConfig {
    pub cache_capacity: usize,
    pub sweep_chunk_lock_interval_secs: u32,
    pub lock_hold_warn_threshold_ms: u64,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            cache_capacity: 10_000,
            sweep_chunk_lock_interval_secs: 60,
            lock_hold_warn_threshold_ms: 1000,
        }
    }
}
```

`validate()` checks: `cache_capacity > 0`,
`sweep_chunk_lock_interval_secs > 0`.

## 9. Server wiring

`main.rs` startup sequence changes:

a. Load `config.lifecycle` (new section).
b. Create `LifecycleMetrics::new()`.
c. Create `ChunkLockMap::new(config.lifecycle.cache_capacity, metrics)`.
d. Pass `ChunkLockMap` into `LifecycleHandler::new()` (or
   `with_locks()`).
e. Spawn sweep task: `tokio::spawn(sweep_loop(locks.clone(),
   sweep_interval, stop_rx.clone()))`.
f. HTTP server: add `/metrics`, `/invalidate_chunk`,
   `/invalidate_range` routes. The routes need access to `ChunkLockMap`
   — wrap in `Arc` and pass into the axum router state.

## Scope

- `app/crow-chunkdb/Cargo.toml` — add `quick-cache = "0.7"`.
- `app/crow-chunkdb/src/lifecycle.rs` — `ChunkLockMap`, `LockPolicy`,
  `CacheHint`, `ChunkGuard`, new `LifecycleError` variants, integrate
  lock acquisition into allocate/append/seal/delete.
- `app/crow-chunkdb/src/chunkdb_config.rs` — `LifecycleConfig` struct +
  defaults + validate.
- `app/crow-chunkdb/src/metrics.rs` (new) — `LifecycleMetrics`,
  `LifecycleMetricsSnapshot`.
- `app/crow-chunkdb/src/main.rs` — create `ChunkLockMap`, spawn sweep
  task, add HTTP routes.
- `app/crow-chunkdb/src/service.rs` — map `LockBusy`/`LockTimeout` to
  gRPC `UNAVAILABLE`.
- `app/crow-chunkdb/src/lib.rs` — export `metrics` module.

## Complexity

**Medium.** The per-chunk lock is a well-understood pattern (aioss uses
per-FID mutex in `chunk_manager::acquire_lock`). The main implementation
challenges: (1) `ChunkGuard` lifetime — it must hold the
`OwnedMutexGuard` and a reference to the cache for `refresh`, without
borrowing `ChunkLockMap` (use `Arc<Cache>` or clone the cache handle);
(2) `invalidate_range` iteration — verify `quick_cache` exposes an
iteration API or maintain a parallel key index; (3) metrics integration
without hot-path allocation — `AtomicU64` with `Relaxed` is
straightforward, `PreciseHistogram` needs the `crow_common::metrics`
API. No new proto changes. No cross-crate API changes.

## Test Design

### Unit tests (UT)

**Lock serialization:**
- UT: two concurrent `acquire` calls on the same chunk ID with
  `Wait(short)` → second parks until first drops guard; verify
  serialization via a shared counter incremented inside the guard.
- UT: `TryLock` on a held lock → returns `LockBusy` immediately, no wait.
- UT: `Wait(d)` on a lock held forever → returns `LockTimeout` after `d`.
- UT: `Wait(d)` on a lock released within `d` → acquires successfully.

**Cache behavior:**
- UT: first `acquire` after creation → cache miss, one `get_chunk` call,
  cache populated. Verify via a mock store counting calls.
- UT: second `acquire` for the same chunk → cache hit, zero `get_chunk`.
- UT: `guard.refresh(chunk)` → next `acquire` serves the refreshed chunk.
- UT: cache over capacity (capacity=2, insert 3 chunks) → first evicted,
  next `acquire` for evicted chunk is a miss.
- UT: `CacheHint::NoCache` on miss → fetches from store, does NOT
  populate cache (cache empty after).
- UT: `CacheHint::NoCache` on `refresh` → updates local copy, does NOT
  write to cache (cache empty after).

**reap_idle:**
- UT: after all guards dropped, `reap_idle()` removes the entry
  (`strong_count == 1`).
- UT: while a guard exists, `reap_idle()` retains the entry.
- UT: after `reap_idle()` removes an entry, next `acquire` creates a
  fresh mutex and works.

**invalidate:**
- UT: `invalidate_chunk(id)` removes the chunk from cache; next `acquire`
  is a miss.
- UT: `invalidate_chunk` on a non-cached chunk → no-op, returns false.
- UT: `invalidate_range(start, end)` removes only chunks whose bucket is
  in range; out-of-range chunks remain.

**allocate_chunk existence check:**
- UT: caller-supplied ID that already exists → returns
  `ChunkAlreadyExists`, does not overwrite.
- UT: caller-supplied ID that does not exist → creates the chunk.
- UT: auto-generated ID → no existence check, creates the chunk.

**Metrics:**
- UT: after 2 cache hits + 1 miss, `snapshot()` shows
  `cache_hit_count=2`, `cache_miss_count=1`.
- UT: `lock_timeout_count` increments on `LockTimeout`.
- UT: `lock_busy_count` increments on `LockBusy`.
- UT: `reap_idle_count` and `reap_idle_entries_removed` reflect the run.

**Lock hold warning:**
- UT: guard held longer than threshold → warn log emitted (verify via
  `tracing` test subscriber or a flag set in a test-only hook).

### Integration tests (IT)

**Lock serialization (real handler):**
- IT: `append_chunk` + `delete_chunk` concurrent on the same chunk → one
  completes fully before the other; final state is either `Deleted` or
  `Active` with appended strips then `Deleted`.
- IT: `seal_chunk` + `seal_chunk` concurrent → first succeeds, second
  returns `AlreadySealed`.
- IT: `delete_chunk` + `delete_chunk` concurrent → first succeeds,
  second returns `ChunkNotFound`.
- IT: `append_chunk` + `append_chunk` concurrent → both sets of strips
  present (no lost update).

**HTTP endpoints:**
- IT: `GET /metrics` returns JSON with all fields present and
  non-negative.
- IT: `POST /invalidate_chunk` with valid chunk ID → returns
  `{ "invalidated": bool }`.
- IT: `POST /invalidate_chunk` with malformed body → returns 400.
- IT: `POST /invalidate_range` with valid range → returns
  `{ "invalidated_count": N }`.

**No deadlock / no thread block:**
- IT: `query_chunk` concurrent with `append_chunk` → no deadlock; query
  may return stale read.
- IT: long-held lock on chunk A does not block operations on chunk B.

### Test commands

- `pixi run cargo test -p crow-chunkdb --test lifecycle_test`
- `pixi run cargo test -p crow-chunkdb --test full_stack_test`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`

## Module Structure

```
app/crow-chunkdb/src/
├── lifecycle.rs          # +ChunkLockMap, LockPolicy, CacheHint, ChunkGuard
│                         # +LockBusy/LockTimeout error variants
│                         # integrate lock into allocate/append/seal/delete
├── chunkdb_config.rs     # +LifecycleConfig (cache_capacity, sweep_interval,
│                         #   lock_hold_warn_threshold)
├── metrics.rs            # NEW: LifecycleMetrics, LifecycleMetricsSnapshot
├── main.rs               # create ChunkLockMap, spawn sweep task,
│                         #   add /metrics + /invalidate_* HTTP routes
├── service.rs            # map LockBusy/LockTimeout -> UNAVAILABLE
└── lib.rs                # export metrics module
```

## Config Extensions

| Field | Default | Description |
| --- | --- | --- |
| `lifecycle.cache_capacity` | 10_000 | Max entries in the chunk payload cache |
| `lifecycle.sweep_chunk_lock_interval_secs` | 60 | Reap idle locks every N seconds |
| `lifecycle.lock_hold_warn_threshold_ms` | 1000 | Warn if lock held longer than N ms |

`validate()`: `cache_capacity > 0`, `sweep_chunk_lock_interval_secs > 0`.

## Server Wiring

1. `main.rs` loads `config.lifecycle`.
2. Creates `LifecycleMetrics::new()`.
3. Creates `ChunkLockMap::new(config.lifecycle.cache_capacity, metrics)`.
4. Passes `ChunkLockMap` into `LifecycleHandler` via `with_locks()`.
5. Spawns sweep task with `sweep_chunk_lock_interval` + stop signal.
6. HTTP server adds `/metrics`, `/invalidate_chunk`, `/invalidate_range`
   routes with `Arc<ChunkLockMap>` in axum state.
