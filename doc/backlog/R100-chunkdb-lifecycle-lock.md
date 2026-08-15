<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R100: chunkdb — Per-Chunk-ID Lifecycle Lock + Chunk Cache

## Problem

**Current behavior + impact**

The chunkdb lifecycle handler (`app/crow-chunkdb/src/lifecycle.rs`) does not
serialize concurrent operations on the same chunk ID. If two RPCs target the
same chunk simultaneously — e.g. `AppendChunk` + `DeleteChunk`, or two
`SealChunk` calls — they race on the read-modify-write cycle in
`ChunkStore::put_chunk` (`storage.rs:51`, a blind overwrite — `KvSetRequest`
has no CAS field, `kv.proto:29`). The last writer wins, silently corrupting
state (e.g. a delete can be overwritten by a concurrent append, resurrecting
the chunk).

This is not a theoretical concern: the chunkdb service receives RPCs from
multiple console clients and internal reconciliation tasks. Without
serialization, any concurrent pair of mutating RPCs on the same chunk can
produce an inconsistent chunk record.

A secondary cost: every mutating RPC does a `get_chunk` round-trip to the KV
store before the read-modify-write (`lifecycle.rs:141/180/201`). Under
repeated operations on the same chunk (e.g. several appends in a row), this
is wasted latency — the latest chunk record is known to this process right
after the previous `put_chunk`.

**Design pointers**

- `doc/design/chunkdb/design-crow-chunkdb.md` §9 (Chunk Lifecycle) specifies
  the `Init → Active → Sealed → Deleted` state machine and notes that
  "Concurrency: KV CAS or state machine guards prevent conflicting
  transitions." KV CAS is not implemented today (`KvSetRequest` has no
  expected-version field); R100 provides in-process serialization via a
  per-chunk mutex. R99's ownership model (one instance owns a chunk) makes
  the in-process mutex the correctness boundary — no KV CAS needed for
  correctness. See Scope below.
- `doc/design/chunkdb/design-crow-chunkdb.md` §12 (Concurrency Model) calls
  for `Arc<RwLock<T>>` shared state and "acquire locks in `{}` blocks, drop
  before `.await`" — that rule is for `std`/`parking_lot` mutexes (which
  block the thread under `.await`). R100 uses `tokio::sync::Mutex`, which is
  designed to be held across `.await` (it parks the task, not the worker
  thread); §12 should be amended to note this exception.
- aioss analog: aioss uses a per-FID mutex in the chunk manager
  (`chunk_manager::acquire_lock`); the same pattern applies here.

**Scope**

R100 depends on R99 (dynamic range binding — implemented first). R99
ensures **one chunkdb instance owns a chunk** at any time, via hash-range
→ instance binding in group-0 + a `NotMyRange` reject-and-retry protocol.
Only the owning instance processes mutating RPCs for a chunk; other
instances reject out-of-range requests. This means the in-process
per-chunk mutex IS the correctness boundary — no KV CAS on `put_chunk` is
needed for correctness, because only one instance can mutate a chunk.

Given R99's ownership model, R100's task is:

- **Per-chunk lock** — serialize concurrent mutating RPCs on the same
  chunk within the owning instance. Without it, two concurrent
  `AppendChunk` RPCs (or append + delete) race on the read-modify-write
  cycle in `ChunkStore::put_chunk` (a blind overwrite — no CAS).
- **Chunk cache** — avoid the `get_chunk` store round-trip on every
  mutating RPC by caching the latest chunk record in-process. The cache
  is populated on first acquire (miss → one `get_chunk`) and refreshed
  by the writer after each `put_chunk`. Subsequent acquires for the same
  chunk are cache hits (zero store round-trips).
- **Cache invalidation for range migration** — when R99 transfers
  ownership of a bucket range from instance A to instance B, instance A
  must invalidate its cache entries for the transferred range (so it
  does not serve stale chunks if it ever re-acquires ownership). R100
  provides three mechanisms: `invalidate_chunk(chunk_id)` (per-chunk,
  O(1)), `invalidate_range(bucket_start, bucket_end)` (range-wide, O(n)
  on cache size), and `CacheHint::NoCache` on acquire (skip cache for
  chunks about to be migrated). All use `quick_cache::Cache::remove(&key)`
  (verified API).
- **Lock policy** — `TryLock` (fail fast with `LockBusy`) or
  `Wait(Duration)` (park the task up to the duration, then
  `LockTimeout`). Default 10s. No `WaitForever` — RPC lifetime must be
  bounded. Neither blocks the worker thread nor the gRPC connection
  (`tokio::sync::Mutex` parks the task, not the thread; HTTP/2
  multiplexes RPCs as independent streams).
- **Metrics and observability** — lock wait/hold time histograms, lock
  timeout/busy counters, cache hit/miss counters, cache size,
  `reap_idle` stats, invalidation counters. Exposed via a `/metrics`
  HTTP endpoint on the existing axum server. Needed for tuning
  `DEFAULT_LOCK_WAIT`, cache capacity, and `reap_idle` cadence.
- **`allocate_chunk` existence check** — for caller-supplied IDs, check
  existence before creating (currently silently overwrites). Returns
  `ChunkAlreadyExists` if the ID is taken.

What R100 does NOT do (out of scope):

- **KV CAS on `put_chunk`** — not needed for correctness given R99's
  ownership model. Feasible and filed as **R101** (KV compare-and-set)
  for defense-in-depth; R100 does not depend on it.
- **Cross-instance cache coherence** — R99's ownership model means only
  one instance caches a chunk at a time; invalidation on range
  migration (provided by R100) is sufficient. No watch/notify or
  version-based cache validation needed.

**Use scenarios**

- Concurrent append + delete on the same chunk: two clients issue
  `AppendChunk` and `DeleteChunk` simultaneously for the same chunk ID.
  The lock serializes them — either the append completes first (chunk is
  then deleted) or the delete completes first (append fails with
  `ChunkNotFound`).
- Concurrent seal + seal on the same chunk: two `SealChunk` RPCs arrive
  simultaneously. The lock serializes them — the first succeeds, the
  second fails with `AlreadySealed` (state check inside the lock).
- Concurrent delete + delete (idempotent retry): a client retries
  `DeleteChunk` after a timeout. The lock ensures the first delete
  completes before the second starts; the second returns `ChunkNotFound`
  (per GAP-9, already implemented at `lifecycle.rs:206`).
- Concurrent append + append on the same chunk: two `AppendChunk` RPCs
  add strips simultaneously. The lock serializes them so both sets of
  strips are persisted (no lost update).
- Repeated append on the same chunk (no contention): a client issues
  several `AppendChunk` RPCs sequentially. The first acquires the lock
  and fetches the chunk from the store (cache miss); subsequent appends
  acquire the lock and serve the chunk from the in-process cache (cache
  hit, no store round-trip on acquire).
- Lock held by a long operation: an `AppendChunk` is in progress (held
  lock, doing parallel strip allocation). A second `AppendChunk` for the
  same chunk arrives with the default `Wait(10s)` policy. It parks its
  task (no worker thread blocked, no gRPC connection blocked — HTTP/2
  multiplexes RPCs as independent streams) and resumes when the first
  releases. If the first takes longer than 10s, the second returns
  `LockTimeout` → gRPC `UNAVAILABLE` with retry hint; the client retries
  with backoff.

## Solution

**One-line summary**

Add a two-tier per-chunk-ID structure in `LifecycleHandler`: a
`DashMap<ChunkId, Arc<tokio::sync::Mutex<()>>>` lock map (reaped when
uncontended) plus a bounded `quick_cache::Cache<ChunkId, Chunk>` payload
cache (S3-FIFO eviction); each mutating RPC acquires the lock with a
pluggable `LockPolicy` (`TryLock` or `Wait(Duration)`, default 10s) and
receives the latest chunk record on acquire (cache hit → zero store
round-trips; cache miss → one `get_chunk`).

**Why two tiers**

The lock and the payload have different eviction requirements, so they must
be separate structures:

- The **lock** can only be evicted when uncontended (no waiter/holder holds
  a clone of the `Arc`). Evicting a contended lock breaks serialization: a
  racing inserter would create a fresh mutex and acquire it instantly while
  the original waiter is still parked on the old one. No generic LRU lib
  provides "evict only when uncontended" — this is a custom `reap_idle`
  pass on a `DashMap`, bounded by the number of concurrent locks (small),
  not by the number of chunks ever touched.
- The **payload** can be evicted freely by recency/frequency — a miss is
  just a store re-fetch. This is exactly what `quick_cache` is built for:
  bounded capacity, S3-FIFO eviction (three static FIFO queues — a small
  probationary queue filters one-hit wonders, a main queue holds popular
  objects, a ghost queue tracks history), 21 bytes/entry overhead, no
  background threads, async `get_or_insert_async` API. The cache survives
  lock release, so a chunk touched minutes ago still gets a cache hit on
  the next op (within capacity).

`quick_cache` is chosen over `moka` for this workload: the features that
would justify moka (TTL/TTI expiration, eviction listener) are not needed
here — in single-instance v1 the cache is always fresh (only this process
mutates chunks, and every mutation refreshes the cache before release), so
there is no staleness to expire and nothing to clean up on eviction. Both
libs support weighted size (chunks vary from 1 to many strips). On dep cost,
`quick_cache` adds zero new transitive crates (`equivalent`, `hashbrown`,
`foldhash` are already in `Cargo.lock`); `moka` would add five
(`portable-atomic`, `tagptr`, `async-lock`, `event-listener`, `quanta`).

**Per-operation lock + cache usage**

All 8 RPCs in `chunkdb_service.proto` are analyzed here. Each mutating
operation is a read-modify-write on the chunk record (read chunk → mutate
in memory → `put_chunk`), so each needs the lock to prevent interleaved
RMW cycles. Read-only operations bypass both lock and cache (go straight
to the store; a stale read under concurrent write is acceptable for
queries).

- **`AllocateChunk`** (`lifecycle.rs:67`, mutating — create) — two paths:
  - Caller-supplied ID: `acquire_for_create(id, policy)` → existence
    check (one-shot `store.get_chunk`; if exists → `ChunkAlreadyExists`)
    → build chunk → `put_chunk` → `guard.refresh(chunk)`. Lock held
    throughout to serialize concurrent allocates of the same ID.
  - Auto-generated ID (`generate_chunk_id`, `lifecycle.rs:79`): skip the
    lock entirely (UUID collision negligible). After `put_chunk`,
    populate the cache directly via
    `self.locks.chunks.insert(id, chunk.clone())` (no guard needed —
    no one else can be operating on a fresh UUID). This gives the next
    op on this chunk (e.g. `AppendChunk`) a cache hit.
- **`AppendChunk`** (`lifecycle.rs:131`, mutating — RMW) —
  `acquire(id, store, policy)` → `guard.chunk()` gives the latest chunk
  (cache hit or store fetch) → state check (`check_can_append`) →
  allocate strips → `put_chunk` → `guard.refresh(chunk)`.
- **`SealChunk`** (`lifecycle.rs:179`, mutating — RMW) —
  `acquire(id, store, policy)` → state check (`check_can_seal`) →
  update state/timestamp/length → `put_chunk` → `guard.refresh(chunk)`.
- **`DeleteChunk`** (`lifecycle.rs:200`, mutating — RMW) —
  `acquire(id, store, policy)` → state check (already deleted →
  `ChunkNotFound` per GAP-9; `check_can_delete`) → free segments →
  `put_chunk` → `guard.refresh(chunk)` (keeps the Deleted-state chunk
  cached so the next `delete_chunk` retry gets a cache hit and returns
  `ChunkNotFound` without a store round-trip).
- **`DeleteChunkRange`** (stub at `service.rs:157`, mutating — RMW
  partial delete) — **not yet implemented**, but when implemented it
  MUST use `acquire(id, store, policy)` → read chunk → modify the
  range within the chunk → `put_chunk` → `guard.refresh(chunk)`. The
  lock is required: a concurrent `AppendChunk` or `SealChunk` on the
  same chunk would otherwise race on the RMW cycle. This is called out
  here so the future implementer does not forget.
- **`UpdateChunkStrip`** (stub at `service.rs:164`, mutating — RMW
  single strip, e.g. after EC parity computation) — **not yet
  implemented**, but when implemented it MUST use
  `acquire(id, store, policy)` → read chunk → replace strip at
  `strip_index` → `put_chunk` → `guard.refresh(chunk)`. The lock is
  required: a concurrent `UpdateChunkStrip` or `AppendChunk` on the
  same chunk would otherwise race (e.g. two parity updates to different
  strips would lose one update). This is called out here so the future
  implementer does not forget.
- **`QueryChunk`** (`lifecycle.rs:227`, read-only) — no lock, no cache.
  Goes straight to `store.get_chunk`. A concurrent write may produce a
  stale read (acceptable for queries). Reads never block writes.
- **`ListChunks`** (`lifecycle.rs:235`, read-only scan) — no lock, no
  cache. Goes straight to `store.list_chunks`. Scans may span many
  chunks; locking each would be impractical and unnecessary for a
  read-only pagination scan.

**Numbered work items**

- **`ChunkLockMap`** (`app/crow-chunkdb/src/lifecycle.rs`, new struct) —
  wraps two fields: `locks: DashMap<ChunkId, Arc<tokio::sync::Mutex<()>>>`
  and `chunks: quick_cache::Cache<ChunkId, Chunk>`. Provides:
  - `acquire(chunk_id, &ChunkStore, policy) -> ChunkGuard` — for existing
    chunks (append/seal/delete). Locks, then serves payload from cache or
    fetches from store on miss (populating the cache). Returns a guard
    carrying the latest chunk.
  - `acquire_for_create(chunk_id, policy) -> ChunkGuard` — for
    `allocate_chunk` with a caller-supplied ID. Locks but does NOT fetch
    (the chunk does not exist yet); caller must `refresh()` after creating
    it.
  - `reap_idle()` — removes lock entries where `Arc::strong_count == 1`
    (only the map holds a clone → no waiter/holder). Called periodically
    from a background task. Payload cache is untouched (bounded by its own
    capacity).
- **`LockPolicy`** (`app/crow-chunkdb/src/lifecycle.rs`, new enum) —
  `TryLock` (fail fast with `LockBusy` on contention) or
  `Wait(Duration)` (park the task up to the duration, then `LockTimeout`).
  `Default` = `Wait(DEFAULT_LOCK_WAIT)` where `DEFAULT_LOCK_WAIT = 10s`.
  No `WaitForever` variant — RPC lifetime must be bounded.
- **`ChunkGuard`** (`app/crow-chunkdb/src/lifecycle.rs`, new struct) —
  holds the `OwnedMutexGuard<()>` (released on drop), the cached
  `Option<Chunk>`, and a `CacheHint` flag. Methods: `chunk() ->
  Option<&Chunk>` (the latest record, populated by `acquire`), `refresh(Chunk)`
  (update cache after a successful `put_chunk` — caller MUST have persisted
  first; if `CacheHint::NoCache`, only updates the guard's local copy, does
  NOT write to the payload cache), and `invalidate()` (drop the cached
  value; optional, since keeping a Deleted chunk cached is cheaper for the
  next acquirer).
- **`CacheHint`** (`app/crow-chunkdb/src/lifecycle.rs`, new enum) —
  `Cache` (default: populate cache on miss, write to cache on `refresh`)
  or `NoCache` (skip cache population on miss — always fetch from store;
  skip cache write on `refresh` — only update the guard's local copy).
  Passed to `acquire` and `acquire_for_create`. Used for operations on
  chunks that are about to be migrated (R99 range transfer) or that the
  caller knows will not be re-accessed soon. Default is `Cache` for all
  normal lifecycle RPCs.
- **`ChunkLockMap::invalidate_chunk` and `invalidate_range`**
  (`app/crow-chunkdb/src/lifecycle.rs`, new methods) —
  `invalidate_chunk(chunk_id)` calls `self.chunks.remove(&chunk_id)` to
  remove a single chunk from the payload cache (verified:
  `quick_cache::Cache::remove(&key) -> Option<(Key, Val)>`). Used during
  R99 range migration when a chunk's ownership transfers. Not called in
  v1 (no range migration yet), but the API is defined here so R99 can
  call it without modifying `ChunkLockMap`. `invalidate_range` iterates
  cache entries and removes those whose chunk ID hashes to a bucket
  range — used when a full range transfers. Both are O(n) on cache size
  for `invalidate_range` (acceptable — range migration is rare);
  `invalidate_chunk` is O(1).
- **`LifecycleHandler` integration** (`app/crow-chunkdb/src/lifecycle.rs`)
  — add a `locks: ChunkLockMap` field. Each implemented mutating method
  (`allocate_chunk`, `append_chunk`, `seal_chunk`, `delete_chunk`)
  acquires the lock for its chunk ID before proceeding, using
  `LockPolicy::default()`. `allocate_chunk` with a caller-supplied ID
  uses `acquire_for_create` and adds an existence check (see below);
  auto-generated IDs skip the lock but populate the cache directly
  (`self.locks.chunks.insert(id, chunk.clone())` after `put_chunk`) so
  the next op on this chunk gets a cache hit. After each successful
  `put_chunk`, methods that hold a guard call `guard.refresh(chunk)` to
  keep the cache in sync with the store. See **Per-operation lock +
  cache usage** above for the full per-RPC breakdown.
- **`allocate_chunk` existence check** (`lifecycle.rs:67`) — currently
  `allocate_chunk` has no existence check and silently overwrites via
  `put_chunk` if a caller-supplied ID already exists. With the lock in
  place, two concurrent allocates of the same ID would serialize but the
  second would still silently overwrite the first. Fix: for
  caller-supplied IDs, after `acquire_for_create`, do a one-shot
  `store.get_chunk`; if the chunk exists, return `ChunkAlreadyExists`
  (already a `LifecycleError` variant, `lifecycle.rs:37`). Auto-generated
  IDs skip this check.
- **`DeleteChunkRange` and `UpdateChunkStrip` lock contract** (stubs at
  `service.rs:157/164`) — these RPCs are not yet implemented, but the
  lock contract is specified here so the future implementer does not
  forget: when implemented, both MUST use `acquire(id, store, policy)`
  before their read-modify-write cycle, and MUST call `guard.refresh()`
  after `put_chunk`. No code change in this R-number — the contract is
  documented in the Per-operation section above and should be enforced
  by code review when those RPCs are implemented (separate R-number).
- **`query_chunk` and `list_chunks`** — read-only, do not acquire the
  lock and bypass the cache (go straight to the store). A concurrent
  write may produce a stale read, which is acceptable for queries (same
  trade-off as R100's original spec). Reads never block writes and vice
  versa.
- **New error variants** (`LifecycleError`, `lifecycle.rs:31`) — add
  `LockBusy` ("chunk lock busy — retry later") and `LockTimeout` ("chunk
  lock acquire timed out"). Both map to gRPC `UNAVAILABLE` with a retry
  hint in `service.rs` (alongside the existing `StateConflict` mapping at
  `service.rs:44`), so the client's existing retry logic in
  `crow-kv-client` can handle them. `StateConflict` remains
  unreachable in v1 (no CAS path) but is kept for the future KV-CAS
  follow-up.
- **`reap_idle` background task** (`app/crow-chunkdb/src/main.rs` or
  wherever the chunkdb service loop lives) — spawn a task that calls
  `locks.reap_idle()` every 60s to keep the lock map bounded by
  concurrent locks, not by chunks-ever-touched.
- **`quick-cache` dependency** (`app/crow-chunkdb/Cargo.toml`) — add
  `quick-cache = "0.7"` to `[dependencies]`. Verify the published version
  is at least 7 days old at add time (CROW dep policy). No other new deps
  (transitive deps `equivalent`, `hashbrown`, `foldhash` already in
  `Cargo.lock`).
- **`LifecycleMetrics`** (`app/crow-chunkdb/src/metrics.rs`, new module)
  — lightweight atomic counters + latency histograms, mirroring the
  `crow-kv-client` metrics pattern (`lib/crow-kv-client/src/metrics.rs`).
  Hot-path counters are `AtomicU64` with `Relaxed` ordering (no locks, no
  allocation); latency uses `crow_common::metrics::PreciseHistogram`
  (already in the workspace). Tracked metrics:
  - **Lock wait time** — histogram of time spent waiting in
    `acquire`/`acquire_for_create` (from call to guard acquisition).
  - **Lock timeout count** — counter, incremented on `LockTimeout`.
  - **Lock busy count** — counter, incremented on `LockBusy` (TryLock
    contention).
  - **Lock hold time** — histogram of time the lock is held (from
    acquisition to guard drop). Critical for tuning
    `DEFAULT_LOCK_WAIT` and diagnosing the lock-hold-time gap (see Open
    Questions).
  - **Cache hit count** — counter, incremented when `acquire` serves
    from cache (no store round-trip).
  - **Cache miss count** — counter, incremented when `acquire` fetches
    from store.
  - **Cache size (entries)** — gauge, read from
    `quick_cache::Cache::entry_count()` at snapshot time.
  - **`reap_idle` count** — counter, incremented each time `reap_idle`
    runs; plus `reap_idle_entries_removed` counter for the number of
    lock entries removed.
  - **`invalidate_chunk` count** — counter, incremented on each
    explicit cache invalidation (for R99 range migration observability).
  - `LifecycleMetrics` is held in `LifecycleHandler` (alongside
    `locks`) and passed to `ChunkLockMap` methods that need to record.
    Snapshot via a `snapshot() -> LifecycleMetricsSnapshot` method
    (drains counters, reads histograms), serialized as JSON.
- **Metrics HTTP endpoint** (`app/crow-chunkdb/src/main.rs`) — add a
  `/metrics` route to the existing axum HTTP server (`main.rs:94`,
  which already has `/ready` and `/health`). Returns
  `LifecycleMetricsSnapshot` as JSON. Read-only, no auth (internal
  management API, same as the existing health endpoints). Enables
  tuning `DEFAULT_LOCK_WAIT`, cache capacity, and `reap_idle` cadence
  based on observed behavior.
- **Cache invalidation HTTP endpoint** (`app/crow-chunkdb/src/main.rs`)
  — add two routes to the existing axum HTTP server:
  - `POST /invalidate_chunk` with JSON body `{ "chunk_id": { "high": u64, "low": u64 } }`
    → calls `ChunkLockMap::invalidate_chunk(chunk_id)`. Returns
    `{ "invalidated": bool }` (true if the chunk was in the cache).
  - `POST /invalidate_range` with JSON body
    `{ "bucket_start": u16, "bucket_end": u16 }` → calls
    `ChunkLockMap::invalidate_range(bucket_start, bucket_end)`. Returns
    `{ "invalidated_count": u32 }`.
  Both are internal management endpoints (same auth model as `/ready`
  and `/health` — no auth, internal network only). Used by: (a) R99's
  range migration handler (in-process call to
  `invalidate_chunk`/`invalidate_range` directly, not via HTTP — the
  HTTP endpoint is for external callers), (b) manual ops via curl, (c)
  future admin tooling. The HTTP endpoint wraps the same
  `ChunkLockMap` methods that R99 calls in-process, so there is one
  implementation path.

**Flow diagram**

```
Mutating RPC (append/seal/delete)          allocate_chunk (caller ID)
        │                                            │
        ▼                                            ▼
  locks.acquire(id, store, policy, hint)    locks.acquire_for_create(id, policy, hint)
        │                                            │
        ├─ cache hit? ──yes──► serve Chunk           │ (no fetch — chunk not yet exists)
        │              no                            │
        │              ▼                             │
        │         store.get_chunk                    │
        │         populate cache (if hint=Cache)     │
        ▼                                            ▼
  state check (can_seal? can_delete?)        existence check (get_chunk → ChunkAlreadyExists)
        │                                            │
        ▼                                            ▼
  mutate + store.put_chunk                  build chunk + store.put_chunk
        │                                            │
        ▼                                            ▼
  guard.refresh(chunk)                      guard.refresh(chunk)
  (writes cache if hint=Cache)              (writes cache if hint=Cache)
        │                                            │
        ▼                                            ▼
  drop guard → release lock                 drop guard → release lock
        │                                            │
        ▼                                            ▼
  (background) reap_idle() every 60s        (background) reap_idle() every 60s
  removes uncontended lock entries          removes uncontended lock entries
  (background) metrics recorded throughout  (background) metrics recorded throughout
  (background) invalidate_chunk/range (R99)  (background) invalidate_chunk/range (R99)
  removes payload cache entries on          removes payload cache entries on
  range migration                           range migration
```

**Edge cases at a glance**

- Lock map entry does not exist → created on first `acquire` via
  `DashMap::entry().or_default()`.
- Lock held, second acquirer uses `Wait(d)` → second task parks (no thread
  block, no connection block), resumes on release or returns `LockTimeout`
  after `d`.
- Lock held, second acquirer uses `TryLock` → returns `LockBusy`
  immediately; client decides whether to retry.
- Lock holder panics → `tokio::sync::Mutex<()>` auto-releases on panic
  (no poisoning for `Mutex<()>`); the cache slot may be stale but the next
  `acquire` re-fetches on miss.
- `allocate_chunk` with auto-generated ID → skips the lock and the
  existence check (UUID collision negligible).
- `allocate_chunk` with caller-supplied ID that already exists → returns
  `ChunkAlreadyExists` (no silent overwrite).
- Payload cache evicts a chunk between two operations → next `acquire` is
  a cache miss, does one `get_chunk`, repopulates. Correctness unaffected.
- `reap_idle` runs while an acquirer holds a clone → `Arc::strong_count >
  1`, entry is retained. No race: `DashMap::retain` holds the shard lock,
  and a concurrent `entry().or_default()` either sees the entry (count>1,
  skip) or, after removal, creates a fresh mutex (correct — no one was
  waiting on the old one).
- Process crash → all in-memory locks and cache are lost; on restart, the
  KV store is the source of truth (no persistent lock/cache state needed).

## Dependencies

- Depends on: GAP-9 (`DeleteChunk` returns `ChunkNotFound`) — the
  idempotent-retry scenario relies on the not-found semantics, already
  implemented at `lifecycle.rs:206`.
- Depends on: existing `ChunkStore` and `LifecycleHandler` (no new proto
  changes).
- Depends on: `crow_common::metrics::PreciseHistogram` (already in the
  workspace, used by `crow-kv-client/src/metrics.rs`).
- New external dep: `quick-cache = "0.7"` (zero new transitive crates).
- **Depends on R99** (dynamic range binding) — R99 is implemented first.
  R99's ownership model (one instance owns a chunk via hash-range binding
  + `NotMyRange` reject) is what makes the in-process mutex the
  correctness boundary — without R99, two chunkdb instances could
  concurrently mutate the same chunk and the lock would not prevent it.
  R99's range migration (`ChunkdbRangeMigrationValue`) calls
  `ChunkLockMap::invalidate_chunk` / `invalidate_range` (defined in this
  R-number) to clean up the payload cache on ownership transfer. R100
  must be implemented after R99 lands, or at minimum the
  `invalidate_chunk` / `invalidate_range` APIs must be available before
  R99's range migration is exercised.
- No downstream dependencies — this is a self-contained concurrency
  hardening + observability.
- **R101** (KV compare-and-set) — not a dependency. R101 adds
  `expected_revision` to `KvSetRequest` for defense-in-depth CAS on
  `put_chunk`. R100 does not require R101 (R99's ownership model is the
  correctness boundary). If R101 lands after R100, `ChunkStore::put_chunk`
  can be extended to pass the revision from the read; the
  `LifecycleError::StateConflict` variant (already defined,
  `lifecycle.rs:39`) would then be wired to the CAS-failure path.

## Acceptance

**Lock serialization**:

- `append_chunk` + `delete_chunk` concurrent on the same chunk → one
  completes fully before the other starts; the chunk's final state is
  either `Deleted` (delete won) or `Active` with the appended strips then
  `Deleted` (append won). Integration test.
- `seal_chunk` + `seal_chunk` concurrent on the same chunk → first
  succeeds, second returns `AlreadySealed`. Integration test.
- `delete_chunk` + `delete_chunk` concurrent on the same chunk → first
  succeeds (state=Deleted), second returns `ChunkNotFound`. Integration
  test.
- `append_chunk` + `append_chunk` concurrent on the same chunk → both
  sets of strips are present in the final chunk (no lost update).
  Integration test.

**Lock + chunk returned together**:

- `acquire` returns a `ChunkGuard` whose `chunk()` is the latest chunk
  record (non-None for append/seal/delete path). Unit test.
- `acquire_for_create` returns a `ChunkGuard` whose `chunk()` is `None`
  until `refresh` is called. Unit test.

**Cache behavior**:

- First `acquire` for a chunk after process start → cache miss, one
  `get_chunk` round-trip observed, cache populated. Unit test (instrument
  or count store calls).
- Second `acquire` for the same chunk (after first release) → cache hit,
  zero `get_chunk` round-trips. Unit test.
- `guard.refresh(chunk)` after `put_chunk` → next `acquire` serves the
  refreshed chunk (post-mutation state), not the pre-mutation state. Unit
  test.
- Payload cache over capacity → `quick_cache` evicts least-valuable
  entries (S3-FIFO); next `acquire` for an evicted chunk is a cache miss
  and re-fetches. Unit test with small capacity.
- `allocate_chunk` with auto-generated ID → after `put_chunk`, the chunk
  is in the payload cache; the next `acquire` for this chunk is a cache
  hit (zero `get_chunk` round-trips). Unit test.
- `delete_chunk` keeps the Deleted-state chunk in the cache via
  `guard.refresh`; a second `delete_chunk` on the same chunk gets a cache
  hit and returns `ChunkNotFound` without a store round-trip. Unit test.

**Lock policy**:

- `LockPolicy::TryLock` on a held lock → returns `LockBusy` immediately,
  no wait. Unit test.
- `LockPolicy::Wait(d)` on a held lock → parks up to `d`, returns
  `LockTimeout` if not acquired in time. Unit test with short `d` and a
  holder that never releases.
- `LockPolicy::Wait(d)` on a held lock that releases within `d` →
  acquires successfully. Unit test.
- Default policy is `Wait(10s)`. Unit test (`Default::default()` equals
  `Wait(Duration::from_secs(10))`).

**No deadlock, no thread/connection block**:

- `query_chunk` does not acquire the lock → concurrent query + mutate
  does not deadlock; query may return a stale read. Unit test.
- `allocate_chunk` with auto-generated ID → does not block other chunks'
  operations (no lock acquired). Unit test.
- A long-held lock (holder awaits >10s) does not block other chunks'
  operations on different chunk IDs (per-chunk mutexes are independent).
  Integration test.

**`reap_idle`**:

- After all holders release a chunk's lock, `reap_idle()` removes the
  lock entry (`Arc::strong_count == 1`). Unit test.
- While a holder/waiter exists, `reap_idle()` retains the entry
  (`strong_count > 1`). Unit test.
- After `reap_idle()` removes an entry, a subsequent `acquire` creates a
  fresh mutex and works correctly. Unit test.

**`allocate_chunk` existence check**:

- `allocate_chunk` with a caller-supplied ID that already exists →
  returns `ChunkAlreadyExists`, does not overwrite. Unit test.
- `allocate_chunk` with a caller-supplied ID that does not exist →
  creates the chunk. Unit test.
- `allocate_chunk` with auto-generated ID → no existence check, creates
  the chunk. Unit test.

**Stub lock contract** (no code change in this R-number — documented for
future implementers of `DeleteChunkRange` and `UpdateChunkStrip`):

- The Per-operation section specifies that `DeleteChunkRange` and
  `UpdateChunkStrip` MUST use `acquire` + `guard.refresh` when
  implemented. This is a documentation/contract acceptance, not a test —
  verified by code review when those RPCs are implemented (separate
  R-number). No test in this R-number.

**Error mapping**:

- `LockBusy` and `LockTimeout` map to gRPC `UNAVAILABLE` with a retry
  hint in `service.rs`. Unit test (status code + retry hint).

**`CacheHint`**:

- `acquire` with `CacheHint::Cache` (default) on a cache miss → fetches
  from store, populates cache. Unit test.
- `acquire` with `CacheHint::NoCache` on a cache miss → fetches from
  store, does NOT populate cache. Unit test (verify cache is empty after).
- `guard.refresh(chunk)` with `CacheHint::NoCache` → updates the guard's
  local copy but does NOT write to the payload cache. Unit test (verify
  cache is empty after refresh).
- `guard.refresh(chunk)` with `CacheHint::Cache` → writes to the payload
  cache. Unit test (verify cache has the chunk after refresh).

**Cache invalidation** (for R99 range migration — R100 depends on R99):

- `invalidate_chunk(chunk_id)` removes the chunk from the payload cache;
  next `acquire` is a cache miss. Unit test.
- `invalidate_chunk` on a chunk not in the cache → no-op, no error. Unit
  test.
- `invalidate_range(bucket_start, bucket_end)` removes all cache entries
  whose chunk ID hashes to the bucket range. Unit test with multiple
  chunks, some in range and some out.
- `POST /invalidate_chunk` HTTP endpoint with a valid chunk ID →
  returns `{ "invalidated": true }` if the chunk was cached, `false`
  if not. Integration test (HTTP POST).
- `POST /invalidate_chunk` with a malformed body → returns 400. Unit
  test.
- `POST /invalidate_range` HTTP endpoint with a valid bucket range →
  returns `{ "invalidated_count": N }` where N is the number of cache
  entries removed. Integration test (HTTP POST).

**Metrics**:

- `LifecycleMetrics::snapshot()` returns correct counters after a
  sequence of operations: e.g. 2 cache hits + 1 cache miss → snapshot
  shows `cache_hit_count=2`, `cache_miss_count=1`. Unit test.
- Lock wait time histogram records non-zero values after a contended
  `acquire` (one holder + one waiter). Unit test.
- Lock timeout counter increments on `LockTimeout`. Unit test.
- Lock busy counter increments on `LockBusy` (TryLock contention). Unit
  test.
- Lock hold time histogram records non-zero values after a guard is held
  for a measurable duration. Unit test.
- `reap_idle` counter increments each run; `reap_idle_entries_removed`
  reflects the number of entries removed. Unit test.
- `/metrics` HTTP endpoint returns JSON with all metrics fields present
  and non-negative. Integration test (HTTP GET to `/metrics`).

**Test commands**:

- `pixi run cargo test -p crow-chunkdb --test lifecycle_test`
- `pixi run cargo test -p crow-chunkdb --test full_stack_test`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`

## Open Questions

- **Lock hold time during diskdb network RPCs.** `allocate_chunk` and
  `append_chunk` call `allocate_strip` (N parallel diskdb RPCs, up to 3
  retries each, `allocator.rs:98-105`) and `commit_strip_segments`
  (diskdb RPC, `lifecycle.rs:252`) while the lock is held.
  `delete_chunk` calls `free_blocks` (diskdb RPC, `lifecycle.rs:215`)
  while the lock is held. If diskdb is slow or retrying, the lock is
  held for seconds, causing `LockTimeout` for concurrent ops on the
  same chunk with the default 10s wait. `seal_chunk` is fast (no diskdb
  call — only `put_chunk`). Two options:
  - **(a) Keep all diskdb calls inside the lock** — correct; the lock
    guards the entire RMW + commit/free cycle. Acceptable if same-chunk
    concurrency is rare (chunks are typically owned by one writer).
    Simple, no refactor.
  - **(b) Release the lock after `put_chunk`, before commit/free** —
    the chunk record is already persisted, so the lock can be released.
    `commit_strip_segments` and `free_blocks` are best-effort (orphan
    scanner reclaims failures), so deferring them is safe. Risk: a
    concurrent `delete_chunk` could `free_blocks` on segments that
    haven't been `commit_blocks`'d yet — but `free_blocks` and
    `commit_blocks` are independent operations on the same diskdb
    blocks, and free-after-uncommitted just means the blocks return to
    the free pool (the orphan scanner would have reclaimed them anyway).
    This halves the lock hold time for allocate/append and eliminates
    it for delete's `free_blocks`. Needs a decision: correctness
    simplicity (a) vs. concurrency throughput (b). Default to (a) for
    v1 — same-chunk concurrency is expected to be rare, and (a) is
    simpler to reason about. Revisit (b) if LockTimeout becomes
    frequent in practice.
- **`reap_idle` cadence and trigger.** 60s periodic background task is
  proposed. Alternative: opportunistic reap on every Nth `acquire`
  (avoids a dedicated task but adds latency to some acquires). Trade-off:
  background task is simpler and decoupled; opportunistic avoids a task
  and reaps more promptly under load. Cannot be resolved autonomously —
  needs a decision on whether chunkdb wants a background maintenance task
  at this layer (it already runs topology refresh etc., so a task is
  idiomatic here). Default to background task unless reviewed otherwise.
- **Cache capacity default.** `quick_cache::Cache::new(capacity)` needs a
  concrete number. 100k entries × ~1-2 KB per `Chunk` ≈ 100-200 MB.
  Proposed default: 100_000, configurable via the chunkdb config file
  (`crow-chunkdb.toml`) under a `lifecycle.cache_capacity` key.
  Alternative: derive from available memory. Needs a decision on whether
  to add a config field now or hardcode and revisit. Default to
  configurable with 100k default.
