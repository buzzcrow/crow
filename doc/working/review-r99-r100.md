<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Review — R99 + R100 Implementation

Review of the R99 (dynamic range binding framework + chunkdb instance
sharding) and R100 (per-chunk-ID lifecycle lock + chunk cache)
implementations against their backlog docs, the gap decisions in
`gap.md`, and the rework plan in `plan-gaps-r99-r100.md`.

Verification commands run:
- `pixi run cargo test -p crow-chunkdb --test lifecycle_test` → 23/23 ok
- `pixi run cargo test -p crow-chunkdb --test routing_test` → 8/8 ok
- `pixi run cargo test -p crow-chunkdb --test full_stack_test` → 7/7 ok
  (incl. `chunkdb_lock_serializes_concurrent_append`,
  `chunkdb_lock_serializes_concurrent_seal`,
  `chunkdb_lock_serializes_concurrent_delete`,
  `chunkdb_lock_serializes_concurrent_append_delete`,
  `chunkdb_lock_no_deadlock_different_chunks`, `chunkdb_cache_hit_on_second_query`)
- `pixi run cargo test -p crow-kv-client --lib` → 28/28 ok
  (incl. `range_binding`, `chunkdb_binding_strategy` tests — 6 new
  incremental assignment tests)
- `pixi run cargo fmt --all -- --check` → clean
- `pixi run cargo clippy -p crow-chunkdb -p crow-kv-client -p crow-chunkdb-client -p crow-kv-server --all-targets -- -D warnings` → clean

---

## R99 — Dynamic Range Binding Framework + chunkdb Instance Sharding

### Implemented (matches backlog / gap decisions)

- **Proto schema** (`lib/crow-protocol/src/proto/sysdata_type.proto`):
  `ChunkdbRangeBindingValue` with the full sub-range metadata set
  (`sub_range_index`, `range_start`, `range_end`, `instance_id`,
  `grpc_endpoint`, `original_instance_id`, `original_endpoint`,
  `status`, `last_change_time_ms`) + `RangeStatus` enum
  (`STABLE` / `IN_TRANSITION`). Matches GAP-R99-3 (non-contiguous
  sub-ranges with per-sub-range metadata).
- **`NotMyRangeHint` proto** (`chunkdb_type.proto`) — the server does
  not track other instances' bindings, so the hint carries only the
  rejected bucket (in `range_start`/`range_end` as a diagnostic);
  `instance_id`, `grpc_endpoint`, `sub_range_index` are empty. The
  client refreshes its binding cache from group-0 and re-routes via
  `RangeBindingClient::refresh_and_route`.
- **Common framework** (`lib/crow-kv-client/src/binding_framework.rs`):
  `BindingStrategy` trait (with optional
  `compute_incremental_assignment` for write-on-change) + generic
  `BindingMonitor<S>`. Matches GAP-R99-1 (shared high-level interface,
  pluggable strategies).
- **chunkdb strategy** (`lib/crow-kv-client/src/chunkdb_binding_strategy.rs`):
  `ChunkdbRangeStrategy` + `compute_sub_range_assignment` (default
  1024 sub-ranges, uniform distribution across instances sorted by
  `instance_id`) + `compute_incremental_sub_range_assignment`
  (preserves `InTransition` state for unchanged sub-ranges, marks
  changed sub-ranges `InTransition` with `original_instance_id` set to
  the old owner, skips write when no sub-range changed). Matches
  GAP-R99-3 (cap at 1024/4096).
- **Range binding client** (`lib/crow-kv-client/src/range_binding.rs`):
  `RangeBindingClient` — refresh (scan group-0), route, route-with-
  fallback (current → original owner when `InTransition`),
  `refresh_and_route` (refresh + route in one call, used on
  `NotMyRange`), watch/notify notifier. Matches GAP-R99-7 (NotMyRange
  + refresh-and-retry).
- **Server range enforcement** (`app/crow-chunkdb/src/range_guard.rs`):
  `RangeGuard` with `allow_all_when_empty` (v1 compat), `check`,
  `replace`, `load_from_group0`. Non-contiguous `OwnedRange` list.
- **Server `NotMyRange` status** (`app/crow-chunkdb/src/service.rs`) —
  returns gRPC `FAILED_PRECONDITION` with a `NotMyRangeHint` carrying
  only the rejected bucket. The server does not track other instances'
  bindings, so it cannot fill the owning endpoint. The client
  refreshes + re-routes.
- **Client routing integration** (`lib/crow-chunkdb-client/src/client.rs`):
  `with_range_binding`, `client_for_chunk` routes via `RangeBindingClient`,
  `with_retry` calls `binding.refresh_and_route(chunk_id)` on
  `NotMyRange` (refresh + re-route in one call). Matches the R90 Open
  Question resolution (route to specific instance, not "any").
- **Server wiring** (`app/crow-chunkdb/src/main.rs`): loads binding from
  group-0 at startup, spawns the watch/notify notifier, attaches the
  range guard to the lifecycle handler + gRPC service.
- **`BindingMonitor` wired into `crow-kv-server`**
  (`app/crow-kv-server/src/binding_monitor_wiring.rs`) — spawns
  `BindingMonitor<ChunkdbRangeStrategy>` as a leader-gated background
  task on group-0 replicas. `main.rs` calls it after the keep-alive
  loop, gated by `--binding-monitor-interval` (default 30s). chunkdb
  instances register themselves via
  `ServiceRegistryClient::register_chunkdb` (convenience wrapper in
  `lib/crow-kv-client/src/service_registry.rs`) + a keep-alive loop
  in `crow-chunkdb/src/main.rs` (`spawn_chunkdb_keepalive`). Only the
  group-0 leader writes the binding table; followers compute but skip
  the write. The monitor uses incremental assignment — reads existing
  bindings, computes the diff, and writes only when a sub-range
  changed owner (avoids frequent rewrites + preserves `InTransition`
  state). `write_bindings` PUTs each entry (idempotent overwrite, no
  delete-all — avoids the non-atomic delete window).
- **Tests**: `routing_test.rs` (8), `range_binding` unit tests (9),
  `chunkdb_binding_strategy` unit tests (12 — 6 original + 6
  incremental: no-change, instance-join, instance-leave,
  empty-instances-keeps-existing, empty-current-all-new,
  preserves-InTransition-for-unchanged).

---

## R100 — Per-Chunk-ID Lifecycle Lock + Chunk Cache

### Implemented (matches backlog / gap decisions)

- **`ChunkLockMap`** (`app/crow-chunkdb/src/lifecycle.rs`): `locks:
  DashMap<ChunkId, Arc<Mutex<()>>>` + `chunks: Arc<quick_cache::Cache<...>>`.
  Two-tier structure matches the "Why two tiers" rationale (lock: evict
  only when uncontended; payload: evict freely).
- **`LockPolicy`** — `TryLock` (→ `LockBusy`) / `Wait(Duration)` (→
  `LockTimeout`), `Default = Wait(10s)`. No `WaitForever`. Matches spec.
- **`CacheHint`** — `Cache` (default) / `NoCache`. `acquire` populates
  on miss only when `Cache`; `refresh` writes to cache only when `Cache`.
- **`ChunkGuard`** — holds `OwnedMutexGuard` (held for Drop — releases
  the lock), chunk, hint; `chunk()`, `refresh(chunk)`; `Drop` records
  hold time + warns if over threshold.
- **Methods**: `acquire`, `acquire_for_create`, `populate_cache`,
  `reap_idle` (retains `Arc::strong_count > 1`), `invalidate_chunk`
  (O(1)), `invalidate_range` (O(n) scan), `cache_len`, `metrics_snapshot`.
- **`LifecycleMetrics`** (`metrics.rs`): `AtomicU64` counters (lock
  timeout/busy, cache hit/miss, reap_idle + entries removed, invalidate
  — one increment per chunk removed, consistent across
  `invalidate_chunk` + `invalidate_range`) + `Mutex<PreciseHistogram>`
  for lock wait/hold latency. Snapshot serializes to JSON.
- **`LifecycleConfig`** (`chunkdb_config.rs`): `cache_capacity = 10_000`,
  `sweep_chunk_lock_interval_secs = 60`, `lock_hold_warn_threshold_ms =
  1000`. Matches GAP-R100-2/3.
- **HTTP endpoints** (`main.rs`): `/metrics` (GET), `/invalidate_chunk`
  (POST), `/invalidate_range` (POST), alongside existing `/ready` +
  `/health`.
- **Sweep task** (`main.rs:run_sweep_loop`): periodic `reap_idle()`,
  configurable interval, stop via watch channel.
- **Integration into 6 mutating RPCs**: `allocate_chunk` (caller ID:
  `acquire_for_create` + existence check; auto ID: skip lock, populate
  cache directly), `append_chunk`, `seal_chunk`, `delete_chunk`,
  `delete_chunk_range`, `update_chunk_strip` (all `acquire` +
  `guard.refresh`). `query_chunk` + `list_chunks` bypass lock + cache
  (read-only). Matches the per-operation breakdown.
- **`delete_chunk_range`** — partial delete: partitions strips by
  overlap with `[offset, offset+size)`, frees removed strips' segments
  via diskdb, updates chunk record. Must be Active.
- **`update_chunk_strip`** — replaces the strip at `strip_index`, frees
  old strip's segments, commits new strip's segments. Must be Active or
  Sealed (EC parity rebuild can happen after seal). Returns
  `StripIndexOutOfRange` on invalid index.
- **`allocate_chunk` existence check** — caller-supplied ID: one-shot
  `store.get_chunk` inside the lock; returns `ChunkAlreadyExists` if
  taken. Auto ID: skips check. Matches spec.
- **`delete_chunk` frees segments inside the lock** (GAP-R100-1 option
  A). `LockTimeout` counter tracked for observation (GAP-R100-1
  "counter to consider switching to B"). Hold-time warning log
  configurable (GAP-R100-1, default 1s).
- **Dep**: `quick-cache = "0.7"` in `app/crow-chunkdb/Cargo.toml`.
  Matches GAP-R100-4.
- **Tests**: `lifecycle_test.rs` (23 — lock serialization, TryLock,
  Wait timeout, cache populate/invalidate/eviction, reap_idle
  contended/uncontended, all metrics counters, error display) +
  `full_stack_test.rs` (7 — concurrent append serialization, concurrent
  seal serialization, concurrent delete serialization, concurrent
  append+delete serialization, no deadlock on different chunks, cache
  hit on second query, full allocate→seal→delete).

---

## Doc cleanup status (plan Phase 6 — done)

Per `plan-gaps-r99-r100.md` Phase 6, the following cleanup is complete:

- **Fold R99 rework design** — `doc/working/design-r99-dynamic-range-binding.md`
  is folded into `doc/design/chunkdb/design-crow-chunkdb-range-binding.md`
  (now covers non-contiguous sub-ranges, `BindingStrategy`,
  `InTransition`, `original_instance`, monitor wiring in
  `crow-kv-server`, incremental assignment, migration flow). The folded
  doc explicitly notes routing is not part of the `BindingStrategy`
  trait (lives in `RangeBindingClient`).
- **Fold R100 design** — `doc/working/design-r100-chunkdb-lifecycle-lock.md`
  is folded into `doc/design/chunkdb/design-crow-chunkdb.md` §10
  (Per-Chunk-ID Lifecycle Lock + Chunk Cache, now covers all 6 mutating
  RPCs including `delete_chunk_range` + `update_chunk_strip`).
- **Delete working docs** — `gap.md`, `plan-gaps-r99-r100.md`,
  `plan-r100-chunkdb-lifecycle-lock.md`, `plan-test.md`,
  `design-r99-dynamic-range-binding.md`,
  `design-r100-chunkdb-lifecycle-lock.md` are deleted.
- **Backlog index** — R99 + R100 entries removed from
  `doc/backlog/backlog.md`; R101/R102/R103 entries updated to
  reference the design docs instead of the deleted R99/R100 backlog
  docs. R99 + R100 backlog doc files deleted.
- **R103 open questions resolved** — the `Copying` phase is replaced
  by `InTransition` (dual-serve reads + new-owner-only writes) since
  chunkdb is stateless. See §5.6 of the range-binding design doc.
- **Migration flow documented** — §5.6 (chunkdb: routing-change, no
  data copy) + §5.7 (diskdb: data-copy, ref to R102's five-step flow)
  added to `design-crow-chunkdb-range-binding.md`.

---

## Verdict

**Core implementation of R99 + R100 is functionally complete and
passing all relevant tests** (lifecycle 23/23, routing 8/8, full-stack
7/7, kv-client lib 28/28; fmt + clippy clean).

**The `BindingMonitor` is wired into `crow-kv-server`** as a
leader-gated background task on group-0 replicas
(`app/crow-kv-server/src/binding_monitor_wiring.rs`). chunkdb
instances self-register via `ServiceRegistryClient::register_chunkdb`
+ a keep-alive loop in `crow-chunkdb/src/main.rs`. R99's "dynamic
binding" is now dynamic in a running process — the binding table is
auto-maintained by the group-0 leader using incremental assignment
(write-on-change, preserves `InTransition` state, no delete-all).

**All open issues resolved**:
- `NotMyRangeHint` — server returns NotMyRange only (no endpoint); the
  client refreshes + re-routes via `RangeBindingClient::refresh_and_route`.
- `compute_sub_range_assignment` incremental — implemented
  `compute_incremental_sub_range_assignment` (preserves `InTransition`,
  marks changed sub-ranges, skips write when no change). Integration +
  migration flow tests deferred to R103.
- `delete_all_bindings` non-atomic — resolved: `write_bindings` now
  PUTs entries (idempotent overwrite, no delete-all). The incremental
  algorithm only writes when something changed.
- `DeleteChunkRange` + `UpdateChunkStrip` stubs — implemented with the
  per-chunk lock contract (`acquire` + `guard.refresh`).

**Doc cleanup is done**: the R99 + R100 design drafts are folded into
the permanent design docs
(`doc/design/chunkdb/design-crow-chunkdb-range-binding.md` +
`doc/design/chunkdb/design-crow-chunkdb.md` §10), the working docs
(`gap.md`, `plan-gaps-r99-r100.md`, `plan-r100-chunkdb-lifecycle-lock.md`,
`plan-test.md`, `design-r99-*.md`, `design-r100-*.md`) are deleted, and
the R99 + R100 backlog docs + index entries are removed. R101/R102/R103
backlog entries now reference the design docs instead of the deleted
R99/R100 backlog docs. The migration flow (chunkdb routing-change +
diskdb data-copy) is documented in §5.6/§5.7 of the range-binding
design doc. R103's open questions are resolved.
