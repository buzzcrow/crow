<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Per-Chunk-ID Lifecycle Lock + Chunk Cache (R100) Plan

Design draft: [`doc/working/design-r100-chunkdb-lifecycle-lock.md`](design-r100-chunkdb-lifecycle-lock.md).
Backlog doc: [`doc/backlog/R100-chunkdb-lifecycle-lock.md`](../backlog/R100-chunkdb-lifecycle-lock.md).
Goal: add per-chunk-ID mutex serialization + payload cache to chunkdb's
`LifecycleHandler`, with sweep task, metrics, and HTTP endpoints.

## 1. Config + dependency

- [ ] **Add `quick-cache` dep** — add `quick-cache = "0.7"` to
  `app/crow-chunkdb/Cargo.toml` `[dependencies]`. Verify build.
  Files: `app/crow-chunkdb/Cargo.toml`.
- [ ] **Add `LifecycleConfig`** — add `LifecycleConfig` struct to
  `chunkdb_config.rs` with `cache_capacity` (default 10_000),
  `sweep_chunk_lock_interval_secs` (default 60),
  `lock_hold_warn_threshold_ms` (default 1000). Add to `ChunkdbConfig`
  as `#[serde(default)] pub lifecycle: LifecycleConfig`. Add `validate()`
  checks: `cache_capacity > 0`, `sweep_chunk_lock_interval_secs > 0`.
  Files: `app/crow-chunkdb/src/chunkdb_config.rs`.

## 2. Metrics module

- [ ] **Create `metrics.rs`** — `LifecycleMetrics` struct with
  `AtomicU64` counters (lock_timeout_count, lock_busy_count,
  cache_hit_count, cache_miss_count, reap_idle_count,
  reap_idle_entries_removed, invalidate_count) + `PreciseHistogram`
  (lock_wait_time, lock_hold_time). `snapshot()` method returning
  `LifecycleMetricsSnapshot` (serde-serializable). Export from `lib.rs`.
  Files: `app/crow-chunkdb/src/metrics.rs` (new),
  `app/crow-chunkdb/src/lib.rs`.

## 3. ChunkLockMap + ChunkGuard

- [ ] **`ChunkLockMap` struct** — `DashMap<ChunkId, Arc<Mutex<()>>>` +
  `Cache<ChunkId, Chunk>` + `LifecycleMetrics` + hold-warn threshold.
  `new(capacity, metrics, warn_threshold)`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **`LockPolicy` + `CacheHint` enums** — `LockPolicy::TryLock` /
  `Wait(Duration)` (default `Wait(10s)`). `CacheHint::Cache` (default) /
  `NoCache`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **`ChunkGuard` struct** — holds `OwnedMutexGuard<()>`, `Option<Chunk>`,
  `CacheHint`, `chunk_id`, `hold_start`, metrics ref, warn threshold.
  Methods: `chunk()`, `refresh(chunk)`, `populate_cache(id, chunk)` (for
  auto-generated ID path). `Drop` records hold time + warns if over
  threshold.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **`acquire` method** — lock + cache-hit/miss + store fetch on miss.
  Returns `ChunkGuard` or `LockBusy`/`LockTimeout`/`ChunkNotFound`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **`acquire_for_create` method** — lock only, no fetch. Returns guard
  with `chunk: None`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **`reap_idle` method** — `DashMap::retain` with `strong_count > 1`.
  Updates metrics.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **`invalidate_chunk` + `invalidate_range` methods** —
  `Cache::remove` for single; iterate + filter by bucket for range.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **New `LifecycleError` variants** — `LockBusy`, `LockTimeout`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.

## 4. LifecycleHandler integration

- [ ] **Add `locks` field to `LifecycleHandler`** — `Arc<ChunkLockMap>`.
  Add `with_locks()` builder method.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **Integrate lock into `allocate_chunk`** — caller-supplied ID:
  `acquire_for_create` + existence check + build + `put_chunk` +
  `refresh`. Auto-generated ID: skip lock, `populate_cache` after
  `put_chunk`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **Integrate lock into `append_chunk`** — `acquire` + state check +
  allocate + `put_chunk` + `commit_strip_segments` + `refresh`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **Integrate lock into `seal_chunk`** — `acquire` + state check +
  update + `put_chunk` + `refresh`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **Integrate lock into `delete_chunk`** — `acquire` + state check +
  `free_blocks` (inside lock) + `put_chunk` + `refresh`.
  Files: `app/crow-chunkdb/src/lifecycle.rs`.

## 5. Service error mapping

- [ ] **Map `LockBusy`/`LockTimeout` to gRPC UNAVAILABLE** — update
  `map_error` in `service.rs`.
  Files: `app/crow-chunkdb/src/service.rs`.

## 6. Server wiring

- [ ] **Create `ChunkLockMap` in `main.rs`** — load `config.lifecycle`,
  create metrics + lock map, pass to `LifecycleHandler` via
  `with_locks()`.
  Files: `app/crow-chunkdb/src/main.rs`.
- [ ] **Spawn sweep task** — `tokio::spawn` periodic `reap_idle` loop
  with `sweep_chunk_lock_interval` + stop signal.
  Files: `app/crow-chunkdb/src/main.rs`.
- [ ] **Add HTTP endpoints** — `GET /metrics`, `POST /invalidate_chunk`,
  `POST /invalidate_range` with `Arc<ChunkLockMap>` in axum state.
  Files: `app/crow-chunkdb/src/main.rs`.

## 7. Tests

- [ ] **UT: lock serialization** — concurrent acquire, TryLock, Wait
  timeout, Wait success.
  Files: `app/crow-chunkdb/tests/lifecycle_test.rs`.
- [ ] **UT: cache behavior** — miss/hit, refresh, eviction, NoCache hint.
  Files: `app/crow-chunkdb/tests/lifecycle_test.rs`.
- [ ] **UT: reap_idle** — removes uncontended, retains contended, fresh
  mutex after reap.
  Files: `app/crow-chunkdb/tests/lifecycle_test.rs`.
- [ ] **UT: invalidate** — chunk + range, non-cached no-op.
  Files: `app/crow-chunkdb/tests/lifecycle_test.rs`.
- [ ] **UT: allocate_chunk existence check** — caller ID exists/not-exists,
  auto-gen skip.
  Files: `app/crow-chunkdb/tests/lifecycle_test.rs`.
- [ ] **UT: metrics** — counters after operations, snapshot correctness.
  Files: `app/crow-chunkdb/tests/lifecycle_test.rs`.
- [ ] **UT: lock hold warning** — guard held over threshold emits warn.
  Files: `app/crow-chunkdb/tests/lifecycle_test.rs`.
- [ ] **IT: lock serialization with real handler** — concurrent
  append+delete, seal+seal, delete+delete, append+append.
  Files: `app/crow-chunkdb/tests/full_stack_test.rs`.
- [ ] **IT: HTTP endpoints** — /metrics, /invalidate_chunk,
  /invalidate_range, malformed body 400.
  Files: `app/crow-chunkdb/tests/full_stack_test.rs`.
- [ ] **IT: no deadlock** — query+append concurrent, long-held lock on A
  doesn't block B.
  Files: `app/crow-chunkdb/tests/full_stack_test.rs`.

## File list

- `app/crow-chunkdb/Cargo.toml` — add `quick-cache = "0.7"`.
- `app/crow-chunkdb/src/chunkdb_config.rs` — `LifecycleConfig` + validate.
- `app/crow-chunkdb/src/metrics.rs` (new) — `LifecycleMetrics` + snapshot.
- `app/crow-chunkdb/src/lib.rs` — export `metrics` module.
- `app/crow-chunkdb/src/lifecycle.rs` — `ChunkLockMap`, `LockPolicy`,
  `CacheHint`, `ChunkGuard`, `LockBusy`/`LockTimeout`, handler integration.
- `app/crow-chunkdb/src/service.rs` — error mapping for lock errors.
- `app/crow-chunkdb/src/main.rs` — create lock map, spawn sweep, HTTP routes.
- `app/crow-chunkdb/tests/lifecycle_test.rs` — unit tests.
- `app/crow-chunkdb/tests/full_stack_test.rs` — integration tests.

## Test checklist

- [ ] `pixi run cargo test -p crow-chunkdb --test lifecycle_test` — all UT pass.
- [ ] `pixi run cargo test -p crow-chunkdb --test full_stack_test` — all IT pass.
- [ ] `pixi run cargo fmt --all -- --check` — clean.
- [ ] `pixi run cargo clippy --all-targets -- -D warnings` — clean.
