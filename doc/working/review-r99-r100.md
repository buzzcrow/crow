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
- `pixi run cargo test -p crow-chunkdb --test full_stack_test` → 4/4 ok
  (incl. `chunkdb_lock_serializes_concurrent_append`,
  `chunkdb_lock_no_deadlock_different_chunks`, `chunkdb_cache_hit_on_second_query`)
- `pixi run cargo test -p crow-kv-client --lib` → 22/22 ok
  (incl. `range_binding`, `chunkdb_binding_strategy` tests)
- `pixi run cargo fmt --all -- --check` → clean
- `pixi run cargo clippy -p crow-chunkdb -p crow-kv-client -p crow-chunkdb-client --all-targets -- -D warnings` → clean

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
- **Common framework** (`lib/crow-kv-client/src/binding_framework.rs`):
  `BindingStrategy` trait + generic `BindingMonitor<S>`. Matches
  GAP-R99-1 (shared high-level interface, pluggable strategies).
- **chunkdb strategy** (`lib/crow-kv-client/src/chunkdb_binding_strategy.rs`):
  `ChunkdbRangeStrategy` + `compute_sub_range_assignment` (default
  1024 sub-ranges, uniform distribution across instances sorted by
  `instance_id`). Matches GAP-R99-3 (cap at 1024/4096).
- **Range binding client** (`lib/crow-kv-client/src/range_binding.rs`):
  `RangeBindingClient` — refresh (scan group-0), route, route-with-
  fallback (current → original owner when `InTransition`), watch/notify
  notifier. Matches GAP-R99-7 (NotMyRange + refresh-and-retry).
- **Server range enforcement** (`app/crow-chunkdb/src/range_guard.rs`):
  `RangeGuard` with `allow_all_when_empty` (v1 compat), `check`,
  `replace`, `load_from_group0`. Non-contiguous `OwnedRange` list.
- **Client routing integration** (`lib/crow-chunkdb-client/src/client.rs`):
  `with_range_binding`, `client_for_chunk` routes via `RangeBindingClient`,
  `with_retry` refreshes the binding cache on `NotMyRange`. Matches the
  R90 Open Question resolution (route to specific instance, not "any").
- **Server wiring** (`app/crow-chunkdb/src/main.rs`): loads binding from
  group-0 at startup, spawns the watch/notify notifier, attaches the
  range guard to the lifecycle handler + gRPC service.
- **Tests**: `routing_test.rs` (8), `range_binding` unit tests (9),
  `chunkdb_binding_strategy` unit tests (6).

### Gaps + issues

1. **`BindingMonitor` is wired into `crow-kv-server`** — resolved.
   `app/crow-kv-server/src/binding_monitor_wiring.rs` spawns
   `BindingMonitor<ChunkdbRangeStrategy>` as a leader-gated background
   task on group-0 replicas. `main.rs` calls it after the keep-alive
   loop, gated by `--binding-monitor-interval` (default 30s). chunkdb
   instances register themselves via
   `ServiceRegistryClient::register_chunkdb` (new convenience wrapper
   in `lib/crow-kv-client/src/service_registry.rs`) + a keep-alive loop
   in `crow-chunkdb/src/main.rs` (`spawn_chunkdb_keepalive`), so the
   monitor has instances to assign. Only the group-0 leader writes the
   binding table; followers compute but skip the write.

2. **`NotMyRangeHint` does not carry the owning instance endpoint** —
   `service.rs:55` `not_my_range_status` builds the hint with
   `instance_id: 0` and `grpc_endpoint: String::new()`. Only the range
   bounds + `sub_range_index` are filled. The R99 acceptance criteria
   states: "`NotMyRange` error includes the current owner's range start,
   range end, and instance endpoint → client can route directly without
   a full cache refresh." The current `RangeGuard` stores only
   `OwnedRange { start, end, sub_range_index }` — it does not know the
   owning instance's endpoint, so it cannot fill the hint. The client
   works around this by calling `binding.refresh()` on `NotMyRange`
   (`client.rs:188`), so correctness is fine, but the hint is incomplete
   and the spec'd "route directly without a full cache refresh" path is
   not implemented. To fix: `RangeGuard` would need to track the full
   binding table (all instances' ranges + endpoints), not just this
   instance's owned ranges.

3. **`route_bucket` comment is misleading** (`range_binding.rs:190`):
   comment claims "Fast path: if bindings are sorted by sub_range_index
   and dense, use binary search on range_start. Otherwise linear scan."
   but the code only does a linear scan. With 1024 sub-ranges this is
   fine, but the comment should be removed or the binary search
   implemented.

4. **Sort key inconsistency** — `RangeBindingClient::refresh` sorts by
   `range_start` (line 162) while `replace` sorts by `sub_range_index`
   (line 243). Both work with the linear-scan `route_bucket` (which
   checks `contains(bucket)`), but the inconsistency is a smell. Pick
   one (preferably `sub_range_index` since it's the canonical key).

5. **`BindingStrategy` trait diverges from the design draft** — the
   draft (§1.2) specifies `type Key` + `fn route(...)`. The actual trait
   dropped both; routing lives in `RangeBindingClient` separately. This
   is a reasonable simplification (the trait is only used for the
   monitor's compute/write/read cycle, not routing), but the design
   draft should be updated to match before folding.

6. **`compute_sub_range_assignment` is full-replace, not incremental** —
   every tick recomputes the entire assignment from scratch, ignoring
   the existing table. This is acceptable for v1 (no migration yet —
   R103 handles transitions), but means a monitor tick would wipe any
   in-progress `InTransition` state. The design draft §2.5 describes an
   incremental diff algorithm (set `InTransition` + `original_instance`
   for changed sub-ranges); that algorithm is not implemented. Acceptable
   to defer to R103, but worth noting.

7. **`delete_all_bindings` is non-atomic** (`chunkdb_binding_strategy.rs:122`)
   — scan + delete one key at a time. A crash mid-delete leaves the
   table partially empty. The next tick rewrites the full table
   (idempotent), so it's eventually consistent, but there's a window
   where bindings are missing. Acceptable for v1.

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
- **`ChunkGuard`** — holds `OwnedMutexGuard`, chunk, hint; `chunk()`,
  `refresh(chunk)`; `Drop` records hold time + warns if over threshold.
- **Methods**: `acquire`, `acquire_for_create`, `populate_cache`,
  `reap_idle` (retains `Arc::strong_count > 1`), `invalidate_chunk`
  (O(1)), `invalidate_range` (O(n) scan), `cache_len`, `metrics_snapshot`.
- **`LifecycleMetrics`** (`metrics.rs`): `AtomicU64` counters (lock
  timeout/busy, cache hit/miss, reap_idle + entries removed, invalidate)
  + `Mutex<PreciseHistogram>` for lock wait/hold latency. Snapshot
  serializes to JSON.
- **`LifecycleConfig`** (`chunkdb_config.rs`): `cache_capacity = 10_000`,
  `sweep_chunk_lock_interval_secs = 60`, `lock_hold_warn_threshold_ms =
  1000`. Matches GAP-R100-2/3.
- **HTTP endpoints** (`main.rs`): `/metrics` (GET), `/invalidate_chunk`
  (POST), `/invalidate_range` (POST), alongside existing `/ready` +
  `/health`.
- **Sweep task** (`main.rs:run_sweep_loop`): periodic `reap_idle()`,
  configurable interval, stop via watch channel.
- **Integration into 4 mutating RPCs**: `allocate_chunk` (caller ID:
  `acquire_for_create` + existence check; auto ID: skip lock, populate
  cache directly), `append_chunk`, `seal_chunk`, `delete_chunk` (all
  `acquire` + `guard.refresh`). `query_chunk` + `list_chunks` bypass
  lock + cache (read-only). Matches the per-operation breakdown.
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
  `full_stack_test.rs` (4 — concurrent append serialization, no
  deadlock on different chunks, cache hit on second query, full
  allocate→seal→delete).

### Gaps + issues

1. **`expect("acquire returns chunk")` in 3 places** (`lifecycle.rs:509,
   567, 607`) — `append_chunk`, `seal_chunk`, `delete_chunk` call
   `g.chunk().expect("acquire returns chunk")`. `acquire` does return
   `Some(chunk)` on success (returns `Err(ChunkNotFound)` on miss), so
   this is unreachable in practice. `expect` panics on logic error;
   prefer `unreachable!("acquire guarantees chunk on Ok")` or handle
   gracefully. Minor.

2. **`record_invalidate` semantics inconsistent** — `invalidate_chunk`
   calls `record_invalidate()` once per chunk removed (per-chunk
   counting); `invalidate_range` calls `record_invalidate()` once per
   call if `count > 0` (per-operation counting). The counter means
   different things for the two paths. Minor metrics inconsistency —
   either document the semantics or make them consistent (e.g. count
   chunks removed in both).

3. **`ChunkGuard.guard` has `#[allow(dead_code)]`** — the field is held
   only for its `Drop` (releases the lock); it's never read. The
   `#[allow]` is correct but a one-line comment ("held for Drop —
   releases the lock") would clarify intent. Minor.

4. **No `delete_chunk` + `append_chunk` concurrent serialization test** —
   the acceptance criteria lists 4 concurrent pairs (append+delete,
   seal+seal, delete+delete, append+append). `full_stack_test.rs` has
   `chunkdb_lock_serializes_concurrent_append` (append+append) but not
   the other 3 pairs. The lock mechanism is shared, so if append+append
   serializes the others will too, but the acceptance criteria are
   explicitly per-pair. Minor coverage gap.

5. **`Stub lock contract` for `DeleteChunkRange` + `UpdateChunkStrip`**
   — these remain `Err(Status::unimplemented(...))` in `service.rs:220/
   227`. The backlog doc specifies the lock contract (MUST use
   `acquire` + `guard.refresh` when implemented) as documentation for
   future implementers. No code change required in R100; the contract
   is in the backlog doc. Acceptable.

---

## Doc cleanup status (plan Phase 6 — done)

Per `plan-gaps-r99-r100.md` Phase 6, the following cleanup is complete:

- **Fold R99 rework design** — `doc/working/design-r99-dynamic-range-binding.md`
  is folded into `doc/design/chunkdb/design-crow-chunkdb-range-binding.md`
  (now covers non-contiguous sub-ranges, `BindingStrategy`,
  `InTransition`, `original_instance`, monitor wiring in
  `crow-kv-server`).
- **Fold R100 design** — `doc/working/design-r100-chunkdb-lifecycle-lock.md`
  is folded into `doc/design/chunkdb/design-crow-chunkdb.md` §10
  (Per-Chunk-ID Lifecycle Lock + Chunk Cache).
- **Delete working docs** — `gap.md`, `plan-gaps-r99-r100.md`,
  `plan-r100-chunkdb-lifecycle-lock.md`, `plan-test.md`,
  `design-r99-dynamic-range-binding.md`,
  `design-r100-chunkdb-lifecycle-lock.md` are deleted.
- **Backlog index** — R99 + R100 entries removed from
  `doc/backlog/backlog.md`; R101/R102/R103 entries updated to
  reference the design docs instead of the deleted R99/R100 backlog
  docs. R99 + R100 backlog doc files deleted.

---

## Verdict

**Core implementation of R99 + R100 is functionally complete and
passing all relevant tests** (lifecycle 23/23, routing 8/8, full-stack
4/4, kv-client lib 22/22; fmt + clippy clean).

**The `BindingMonitor` is wired into `crow-kv-server`** as a
leader-gated background task on group-0 replicas
(`app/crow-kv-server/src/binding_monitor_wiring.rs`). chunkdb
instances self-register via `ServiceRegistryClient::register_chunkdb`
+ a keep-alive loop in `crow-chunkdb/src/main.rs`. R99's "dynamic
binding" is now dynamic in a running process — the binding table is
auto-maintained by the group-0 leader.

**Doc cleanup is done**: the R99 + R100 design drafts are folded into
the permanent design docs
(`doc/design/chunkdb/design-crow-chunkdb-range-binding.md` +
`doc/design/chunkdb/design-crow-chunkdb.md` §10), the working docs
(`gap.md`, `plan-gaps-r99-r100.md`, `plan-r100-chunkdb-lifecycle-lock.md`,
`plan-test.md`, `design-r99-*.md`, `design-r100-*.md`) are deleted, and
the R99 + R100 backlog docs + index entries are removed. R101/R102/R103
backlog entries now reference the design docs instead of the deleted
R99/R100 backlog docs.

**Remaining minor issues** (not blockers):
- `NotMyRangeHint` does not carry the owning instance endpoint (R99
  gap #2 above) — the client works around this with a full cache
  refresh on `NotMyRange`. To fix: `RangeGuard` would need to track
  the full binding table, not just this instance's owned ranges.
- `route_bucket` comment mentions binary search but does linear scan
  (R99 gap #3) — cosmetic.
- Sort key inconsistency between `refresh` and `replace` (R99 gap #4)
  — smell, not a bug.
- `expect("acquire returns chunk")` in 3 places (R100 gap #1) —
  unreachable in practice; prefer `unreachable!`.
- `record_invalidate` semantics inconsistent between chunk vs range
  invalidation (R100 gap #2) — minor metrics inconsistency.
- Missing concurrent pair tests for seal+seal, delete+delete,
  append+delete (R100 gap #4) — the lock mechanism is shared, so
  append+append serializing implies the others do too, but the
  acceptance criteria are explicitly per-pair.
