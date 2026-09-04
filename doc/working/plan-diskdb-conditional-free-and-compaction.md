<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB Conditional Free and Compaction Closure Plan

This plan closes the stale-free and compaction-ordering findings discovered
while finishing R130. Work is ordered as DiskDB allocation-incarnation
fencing, R132 slot-ordered compaction, then R130 verification and cleanup.

## Timestamp and Revision Model

- `allocation_ts` is a monotonically increasing DiskDB allocation-incarnation
  identifier stored in `BusyBlockValue` and returned in `Segment`. An allocate
  retry reuses the same value.
- `pre_allocation_ts` is stored in `FreeBlockValue` and equals the
  `allocation_ts` supplied by the segment being freed. It identifies the busy
  incarnation that the free replaced.
- `free_ts` is a wall-clock or monotonic diagnostic timestamp stored in
  `FreeBlockValue`. It is useful for logs, age metrics, and operator inspection,
  but never controls correctness or compaction ordering.
- `commit_slot` is KV metadata: the Paxos slot at which the current record
  version committed. Point gets and scans expose it without encoding it into
  keys. R132 uses it as the authoritative ordering value.
- Application timestamps remain in values. `commit_slot` remains engine record
  metadata because an application cannot know the chosen slot when it encodes
  an opaque value.

## Phase 1: Fence DiskDB Allocation Incarnations

- [x] Add `allocation_ts` to `BusyBlockValue` and `Segment`, and
  `pre_allocation_ts` plus diagnostic `free_ts` to `FreeBlockValue`.
- [x] Generate `allocation_ts` monotonically across restart and immutable-owner
  operation. Define the R102 owner-handoff requirement to reconstruct or carry
  the high-water mark.
- [x] Preserve `allocation_ts` through allocate, commit, ChunkDB metadata,
  DiskDB client transport, and free requests.
- [x] Make `FreeBlockKey` incarnation-specific by appending `allocation_ts`.
  Free performs one blind immutable put and never deletes `BusyBlockKey` or
  clears the bitmap.
- [x] Remove the read-before-free validation path and its configuration.
  Compaction authoritatively matches `pre_allocation_ts`, unit count, and owner
  against the current busy value before clearing the bitmap and deleting the
  matching records.
- [x] Make repeated free naturally idempotent by rewriting the same immutable
  incarnation key. A delayed retry can create only its old incarnation event
  and can never affect a newer busy incarnation.
- [ ] Cover response-loss retry, retry before compaction, retry after reuse,
  concurrent duplicate free, owner mismatch, and multi-block batches.

## Phase 2: Implement R132 Slot-Ordered Compaction

- [x] Expose each scan item's `commit_slot` without copying its value.
- [x] Add a fixed-cutoff bounded current-version scan using the leader's
  contiguous-applied frontier, with one cutoff retained across pagination.
- [x] Reject incomplete scans, changed cutoffs, timeouts, decode failures, and
  leader changes before a caller can publish a watermark.
- [x] Add `compact_slot` to `ZoneValue` and its checksum.
- [x] Compact only free records in `(compact_slot, scan_cutoff]`, atomically
  persisting the new snapshot and deleting the exact scanned keys.
- [x] Retain `free_ts` for diagnostics only; remove `free_ts` from all
  correctness decisions.
- [ ] Verify owner fencing, delayed lower-slot apply, concurrent free,
  pagination, batch failure, and restart.

## Phase 3: Close R130

- [ ] Re-run the two failed mixed-workload sentinels: 64 workers with one block
  and 16 workers with four blocks.
- [ ] Run the complete memory regression sweep and the required block-mode
  exhaustion case on the three-node, 12-disk topology.
- [ ] Require zero correctness errors, a unique live set, and exact busy-space
  accounting before accepting benchmark results.
- [ ] Add the deterministic mixed-selection/accounting unit test and complete
  the three unchecked R130 benchmark checklist entries.

## Open Issues for Review

- The existing diskdb recovery integration harness can expose live records
  while reporting `contiguous_applied = 0` and per-record commit slot zero.
  Bounded compaction now defers without mutation or deletion in that state.
  Decide whether the harness should explicitly close Paxos gaps so it also
  exercises a successful positive-cutoff compaction pass.
- The bounded API reads current versions and filters versions newer than its
  cutoff; it does not reconstruct overwritten historical values. Diskdb makes
  this safe by compacting only non-active zones, where Busy keys cannot be
  overwritten by allocation, while Free keys are incarnation-qualified and
  immutable. Keep this restriction explicit if the API gains other callers.
- The repository-wide `tree-lint` gate currently fails before analyzing these
  Rust-only changes because the pixi clang-tidy environment cannot find
  `stddef.h`, `spdlog`, `isa-l`, and `liburing` headers. No C++ files changed;
  repair the lint environment separately before using this gate for release.
- After the rebase, workspace Clippy also fails in
  `crowdb-console-shared/src/ops/cluster.rs`: it reads removed `mgmt_url` and
  `rpc_url` fields from `DeployedDiskdb`. This is outside the DiskDB free and
  compaction changes; targeted checks for the changed crates remain required.
- [ ] Trace R130 public behavior to client E2E coverage and either add missing
  public-path cases or explicitly narrow acceptance where a behavior is
  intentionally server-internal.
- [ ] Finish the partial liveness/RPC module split or move it to a named
  follow-up requirement with explicit scope.
- [ ] Run the Pixi quality gates and relevant test commands, update permanent
  designs, close the working documents, and commit each coherent task once.

## Stop Conditions

- `allocation_ts` cannot be reconstructed above every durable busy/free value
  on restart or future ownership handoff.
- A bounded scan cannot prove a complete current-version prefix at its fixed
  cutoff without adding MVCC or stronger immutable-key constraints.
