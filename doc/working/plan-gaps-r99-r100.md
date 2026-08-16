<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW Gaps R99/R100 Resolution Plan

**Override:** master plan for multiple requirements — covers filing follow-up
requirements, R100 (per-chunk lifecycle lock + chunk cache), and the R99
rework (binding framework). Per-requirement execution still follows
`/implement-requirement`; this file tracks the whole effort and is deleted
after all items land.

**Decisions log:** `doc/working/gap.md` — every `ai-todo:` entry is the
authoritative user decision for the gap it annotates.

**Goal:** resolve all open R99/R100 gaps: file the two follow-up
requirements, land R100 with the agreed lock/cache design, and rework R99's
binding framework per the new schema + monitor decisions — in that order.

## Phase 1 — File follow-up requirements

- [ ] **R102 diskdb dynamic binding migration doc** — write
  `doc/backlog/R102-diskdb-dynamic-binding-migration.md`: diskdb disk-group →
  paxos-group rebinding replacing the operator-manual `BindMapValue` write,
  reusing the common binding framework (GAP-R99-5). Files:
  `doc/backlog/R102-diskdb-dynamic-binding-migration.md` (new).
- [ ] **R103 chunkdb range migration doc** — write
  `doc/backlog/R103-chunkdb-range-migration.md`: chunkdb instance
  range-ownership transfer (Copying/Cutover/Complete states) — GAP-R99-8.
  Distinct from R102: R102 rebinds diskdb disk-groups, R103 transfers chunkdb
  range ownership; both reuse the common framework. Files:
  `doc/backlog/R103-chunkdb-range-migration.md` (new).
- [ ] **Backlog index update** — add R102/R103 entries to
  `doc/backlog/backlog.md`, bump `**Next R number: R104**`. Files:
  `doc/backlog/backlog.md`.
- [ ] **Record R99-8 answer in gap.md** — annotate GAP-R99-8's `ai-todo` with
  "not the same as GAP-R99-5 — separate requirement (R103)". Files:
  `doc/working/gap.md`.

## Phase 2 — R100 design draft

- [ ] **design-r100 draft** — write `doc/working/design-r100-chunkdb-lifecycle-lock.md`
  folding in GAP-R100-1..4:
  - Lock hold: diskdb calls inside the lock (A); `LockTimeout` frequency
    counter to switch to B if too frequent; warning log when lock hold
    exceeds a configurable threshold (default 1s).
  - Sweep: periodic task, interval configurable, name
    `sweep_chunk_lock_interval` (default 60s).
  - Cache: default capacity 10_000, configurable; design supports 100_000+.
  - Dep: `quick-cache = "0.7"`.
  Files: `doc/working/design-r100-chunkdb-lifecycle-lock.md` (new).
- [ ] **Align R100 backlog doc** — adjust
  `doc/backlog/R100-chunkdb-lifecycle-lock.md` where it conflicts with the
  gap decisions (LockTimeout counter, sweep naming, cache capacity).
  Files: `doc/backlog/R100-chunkdb-lifecycle-lock.md`.

## Phase 3 — R100 implementation

Detailed tasks are refined from the design draft; initial shape:
- [ ] **Per-chunk lock map** — `ChunkLockMap` in
  `app/crow-chunkdb/src/lifecycle.rs`: per-chunk-ID mutex serializing
  mutating RPCs (append/seal/delete); `LockTimeout` error + frequency
  counter; lock-hold warning log (configurable threshold). Files:
  `app/crow-chunkdb/src/lifecycle.rs`, `app/crow-chunkdb/src/chunkdb_config.rs`.
- [ ] **Chunk cache** — quick-cache-backed chunk cache (capacity 10_000,
  configurable) in `app/crow-chunkdb/src/`; version pin `quick-cache 0.7`
  (verify published ≥7 days old). Files: new `chunk_cache.rs`,
  `app/crow-chunkdb/Cargo.toml`, `app/crow-chunkdb/src/chunkdb_config.rs`.
- [ ] **Sweep task** — `sweep_chunk_lock_interval` (default 60s) periodic
  reap of idle locks/cache entries. Files: `app/crow-chunkdb/src/lifecycle.rs`,
  `app/crow-chunkdb/src/chunkdb_config.rs`.
- [ ] **Tests** — unit: lock serialization, LockTimeout counter, cache
  capacity, sweep; integration: concurrent mutating RPCs on the same chunk.
  Run `pixi run test-chunkdb`, fmt, clippy.

## Phase 4 — R99 rework design (amend R99 doc)

- [ ] **design-r99 rework draft** — write
  `doc/working/design-r99-dynamic-range-binding.md` (name referenced by
  `range_guard.rs`/`binding_monitor.rs`):
  - Common framework: one high-level interface for the "owner problem"
    (chunkdb instance binding + diskdb disk-group binding), pluggable
    strategies, duplication allowed (GAP-R99-1).
  - Range model: explicit ranges (B) but **non-contiguous** per node;
    range space capped at 1024/4096 sub-ranges; per-sub-range metadata
    (current owner, original owner, transition status, last change time);
    routing prefers current owner, falls back to original owner during
    transition (GAP-R99-3).
  - Monitor: task runs on group-0 replicas in kv-server; only the leader
    performs balancing; crow-storage concepts moved into kv-server; shared
    protocol/concepts in `crow-protocol` (GAP-R99-2/4/6).
  - NotMyRange: keep new `ErrorCode` + client refresh-and-retry (GAP-R99-7).
- [ ] **Amend R99 backlog doc** — update
  `doc/backlog/R99-kv-dynamic-range-binding-framework.md` Solution/Open
  Questions to record the decisions and the rework scope (per user: track
  rework in the existing R99 doc, no new R-item). Files:
  `doc/backlog/R99-kv-dynamic-range-binding-framework.md`.

## Phase 5 — R99 rework implementation

- [ ] **Proto schema** — sub-range metadata fields (`current_owner`,
  `original_owner`, `status`, `last_change_time`), range space cap; shared
  binding types in `crow-protocol`. Files: `lib/crow-protocol/src/proto/*`,
  `lib/crow-protocol/src/key.rs` (+ build.rs if new messages).
- [ ] **Routing/binding rework** — non-contiguous ranges in
  `app/crow-chunkdb/src/routing.rs` (`BindingTable`/`BindingCache`), routing
  order (current → original during transition). Files:
  `app/crow-chunkdb/src/routing.rs`, `app/crow-chunkdb/src/range_guard.rs`.
- [ ] **Monitor relocation** — `BindingMonitor` moves to `app/crow-kv-server`
  (leader-gated background task on group-0 replicas; follower runs but does
  not balance), removing it from `crow-chunkdb`. Files:
  `app/crow-kv-server/src/*`, `app/crow-chunkdb/src/binding_monitor.rs`
  (delete), `app/crow-chunkdb/src/lib.rs`.
- [ ] **Client routing updates** — `lib/crow-chunkdb-client/src/client.rs`
  route via new binding; refresh + retry unchanged. Files:
  `lib/crow-chunkdb-client/src/client.rs`, `lib/crow-kv-client/src/*`.
- [ ] **Tests** — unit: assignment, non-contiguous ranges, transition
  fallback; integration: sharding + reject-and-retry; kv-server monitor
  leader-gating. Run `pixi run test-chunkdb`, `pixi run test-protocol`,
  `pixi run test-kv-server`, fmt, clippy.

## Phase 6 — Merge + cleanup

- [ ] **Fold R100 design** — merge `design-r100-*` into
  `doc/design/chunkdb/design-crow-chunkdb.md` per `/doc-design` Folding
  section; delete the draft. Files: `doc/design/chunkdb/design-crow-chunkdb.md`.
- [ ] **Fold R99 design** — merge the rework design into
  `doc/design/chunkdb/design-crow-chunkdb-range-binding.md`; delete the
  draft. Files: `doc/design/chunkdb/design-crow-chunkdb-range-binding.md`.
- [ ] **Cleanup** — delete `doc/working/plan-gaps-r99-r100.md`,
  `doc/working/gap.md` (decisions folded into the design docs), R99/R100
  backlog entries + `backlog.md` entry; R102/R103 remain as backlog items.
  Files: `doc/backlog/backlog.md`, `doc/working/*`.
- [ ] **Final CI** — all test suites from `/implement-requirement` Step 9
  pass; fmt + clippy clean.

## File list

- `doc/backlog/R102-diskdb-dynamic-binding-migration.md` — new requirement (diskdb dynamic binding).
- `doc/backlog/R103-chunkdb-range-migration.md` — new requirement (chunkdb range migration).
- `doc/backlog/backlog.md` — add R102/R103; bump Next R number to R104; remove R99/R100 after merge.
- `doc/backlog/R100-chunkdb-lifecycle-lock.md` — align with gap decisions; deleted at cleanup.
- `doc/backlog/R99-kv-dynamic-range-binding-framework.md` — record rework scope; deleted at cleanup.
- `doc/working/gap.md` — decisions log; R99-8 answer added; deleted at cleanup.
- `doc/working/design-r100-chunkdb-lifecycle-lock.md` — new design draft; folded then deleted.
- `doc/working/design-r99-dynamic-range-binding.md` — rework design draft; folded then deleted.
- `app/crow-chunkdb/src/lifecycle.rs` — per-chunk lock, sweep, LockTimeout counter, hold warning.
- `app/crow-chunkdb/src/chunkdb_config.rs` — new config: `sweep_chunk_lock_interval`, lock threshold, cache capacity.
- `app/crow-chunkdb/src/chunk_cache.rs` — new: quick-cache chunk cache.
- `app/crow-chunkdb/Cargo.toml` — `quick-cache = "0.7"`.
- `app/crow-chunkdb/src/routing.rs`, `range_guard.rs` — non-contiguous ranges, transition fallback.
- `app/crow-chunkdb/src/binding_monitor.rs` — deleted (moves to kv-server).
- `app/crow-kv-server/src/*` — BindingMonitor as leader-gated background task.
- `lib/crow-protocol/src/proto/*`, `lib/crow-protocol/src/key.rs` — sub-range metadata schema, shared binding types.
- `lib/crow-chunkdb-client/src/client.rs` — route via new binding schema.
- `lib/crow-kv-client/src/*` — shared binding client/framework surface.
- `doc/design/chunkdb/design-crow-chunkdb.md` — R100 design folded in.
- `doc/design/chunkdb/design-crow-chunkdb-range-binding.md` — R99 rework design folded in.

## Test checklist

- [ ] `pixi run test-chunkdb` — R100 lock serialization, LockTimeout counter, cache, sweep; R99 rework routing/guard.
- [ ] `pixi run test-protocol` — sub-range metadata proto decode/encode.
- [ ] `pixi run test-kv-server` — BindingMonitor leader-gated wiring.
- [ ] `pixi run test-chunkdb-client` — client routing via new binding.
- [ ] `pixi run cargo fmt --all -- --check` and `pixi run cargo clippy --all-targets -- -D warnings` after each phase.
