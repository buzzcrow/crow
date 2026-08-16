<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R103: chunkdb — Range Ownership Migration

## Problem

### Current behavior + impact

R99 stubbed the chunkdb instance range migration flow. The `ChunkdbRangeMigrationValue` proto is defined with `Copying`/`Cutover`/`Complete` states, but the actual migration logic (dual-serve during cutover, background copy, ownership transfer) is not implemented. When the binding monitor reassigns a range from one chunkdb instance to another (for load balancing or instance crash recovery), there is no controlled migration — the binding table is updated atomically (old instance rejects, new instance accepts) but there is no data copy phase, no dual-serve, no graceful cutover. This can cause temporary request failures during the cutover window if clients have stale caches.

### Design pointers

- `doc/design/chunkdb/design-crow-chunkdb.md` §3.6 — stateless with KV persistence.
- `doc/backlog/R99-kv-dynamic-range-binding-framework.md` — binding framework, `ChunkdbRangeMigrationValue`.
- `doc/design/kv/design-crow-kv-group0.md` §2.6 — monitoring models.
- aioss analog: aioss has no chunkdb instance sharding (all instances share metadb); CROW's range migration is new work.

### Use scenarios

- **Load balancing split** — monitor splits an overloaded instance's range, migrates half to a new instance.
- **Instance crash recovery** — monitor reassigns crashed instance's range to a survivor; migration is fast (no data copy needed since chunk metadata is in KV groups, not in the chunkdb instance).
- **Graceful instance decommission** — operator marks instance for removal; monitor migrates all its ranges to other instances, instance drains and shuts down.
- **Client during migration** — client sends request to current owner; if in `Cutover` state, both old and new can serve reads; writes go to new owner only.

## Solution

Implement the full chunkdb range ownership migration flow (`Copying` → `Cutover` → `Complete`) with dual-serve during cutover, background metadata verification, and graceful client redirect.

### Work items

1. **Migration state machine** — `app/crow-chunkdb/src/migration.rs` extend: implement `Copying`/`Cutover`/`Complete` state transitions for range ownership transfer. The `ChunkdbRangeMigrationValue` proto already exists from R99.
2. **Dual-serve during Cutover** — both old and new instance serve read requests (`query_chunk`) during `Cutover`; write requests (`allocate`/`append`/`seal`/`delete`) go to the new owner only; old owner rejects writes with `NotMyRange` hint pointing to new owner.
3. **Background metadata verification** — during `Copying`, the new instance verifies it can access all chunk metadata for the range (chunk metadata is in KV groups, not in the chunkdb instance, so this is a routing verification, not a data copy).
4. **Client redirect** — `lib/crow-chunkdb-client/src/client.rs` update: on `NotMyRange` during migration, refresh binding cache, check migration status, route to current owner (or original owner during transition per R99 rework routing).
5. **Monitor coordination** — the binding monitor (in `crow-kv-server` per R99 rework) initiates migrations, writes `ChunkdbRangeMigrationValue` to group-0, monitors progress, and finalizes cutover.

### Flow diagram

```
 monitor initiates
        │
        ▼
 ┌───────────────┐
 │   Copying     │  new instance verifies it can reach all chunk
 │               │  metadata for the range (routing check, no data copy)
 └───────┬───────┘
         │  verification ok
         ▼
 ┌───────────────┐
 │   Cutover     │  reads: old + new both serve (query_chunk)
 │               │  writes: new owner only; old rejects w/ NotMyRange
 └───────┬───────┘
         │  monitor finalizes
         ▼
 ┌───────────────┐
 │   Complete    │  old instance rejects all; new owner fully serves
 └───────────────┘
```

### Edge cases at a glance

- **Migration interrupted by monitor crash** — migration value persists in group-0; new monitor leader resumes or aborts.
- **New instance fails during Copying** — abort migration; old instance continues serving.
- **Client cache stale during Cutover** — old instance serves read, redirects write to new owner.

## Dependencies

- Depends on **R99** — binding framework, `ChunkdbRangeMigrationValue` proto, `NotMyRange` protocol.
- Depends on **R99 rework** — non-contiguous ranges, monitor relocation to `crow-kv-server`.
- **R100** (per-chunk lock) is not a hard dependency, but the lock helps serialize concurrent migrations on the same chunk.

## Acceptance

### Migration state machine

- Unit test: start a `ChunkdbRangeMigrationValue` in `Copying` → drive transition to `Cutover` → assert state and persisted value in group-0 match. `[Unit test]`
- Unit test: `Cutover` → `Complete` transition → assert old instance no longer owns the range in the binding table. `[Unit test]`
- Integration test: monitor writes a migration value, chunkdb instance loads it → assert instance exposes correct migration state for the range. `[Integration test]`

### Dual-serve during Cutover

- Integration test: range in `Cutover`, client sends `query_chunk` to old instance → assert read succeeds. `[Integration test]`
- Integration test: range in `Cutover`, client sends `query_chunk` to new instance → assert read succeeds. `[Integration test]`
- Integration test: range in `Cutover`, client sends `allocate`/`append`/`seal`/`delete` to old instance → assert rejection with `NotMyRange` hint pointing to new owner. `[Integration test]`
- Integration test: range in `Cutover`, client sends write to new owner → assert write succeeds. `[Integration test]`

### Background metadata verification

- Unit test: new instance in `Copying`, all chunk metadata reachable via KV groups → assert verification completes and state advances to `Cutover`. `[Unit test]`
- Unit test: new instance in `Copying`, a chunk metadata key unreachable → assert verification fails and migration aborts (old instance keeps serving). `[Unit test]`

### Client redirect

- Unit test: client receives `NotMyRange` during migration → assert binding cache refreshed and request routed to current owner. `[Unit test]`
- Integration test: client cache stale during `Cutover`, write sent to old instance → assert client follows `NotMyRange` hint and retries on new owner successfully. `[Integration test]`
- E2E test: client request during active migration → assert request eventually served by correct owner with no permanent failure. `[E2E test]`

### Monitor coordination

- Integration test: monitor initiates migration for a range → assert `ChunkdbRangeMigrationValue` written to group-0 and both instances observe it. `[Integration test]`
- Integration test: monitor observes `Cutover` stable for grace period → assert monitor finalizes to `Complete` and updates binding table. `[Integration test]`
- E2E test: full migration lifecycle (`Copying` → `Cutover` → `Complete`) driven by monitor with live clients → assert no request failures beyond transient redirects. `[E2E test]`

### Edge cases

- Integration test: monitor leader crashes mid-migration → new leader reads persisted migration value → assert migration resumed or aborted consistently. `[Integration test]`
- Integration test: new instance fails during `Copying` → assert migration aborted, old instance continues serving all requests. `[Integration test]`
- Integration test: client with stale cache during `Cutover` sends read to old instance → assert read served; sends write to old instance → assert redirected to new owner. `[Integration test]`

### Build & style

- `pixi run test-chunkdb`
- `pixi run test-chunkdb-client`
- `pixi run test-kv-server`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`

## Open Questions

- Should `Cutover` allow writes to both instances with conflict resolution, or strictly new-owner-only?
- Is metadata verification needed if chunk metadata is already in shared KV groups, or can `Copying` be skipped for crash-recovery migrations?
