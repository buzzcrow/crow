<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB Conditional Free and Compaction Closure Plan

This plan closes the stale-free and compaction-ordering findings discovered
while finishing R130. Work is ordered as R101 conditional mutation, DiskDB
allocation-incarnation fencing, R132 slot-ordered compaction, then R130
verification and cleanup.

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

## Phase 1: Redesign R101 as Atomic Conditional Batch

- [ ] Replace the current read-before-propose CAS design. Two concurrent
  requests on one leader must not both pass the same precondition while being
  proposed in parallel slots.
- [ ] Define conditional batch predicates needed by DiskDB: key absent, key
  present, record value or selected incarnation field matches, and optional
  idempotency-result recognition.
- [ ] Define the ordering lane for conditional mutations. Earlier mutations
  must be applied before predicate evaluation, and later mutations must not
  affect predicate keys before the conditional operation is applied.
- [ ] Keep blind writes on the existing parallel path. Implement the
  conditional lane without a blocking mutex; if a blocking lock becomes
  necessary, stop for explicit review.
- [ ] Return the applied conditional result, including mismatch details, only
  after the state machine has determined the predicate outcome.
- [ ] Add protocol, KV core, client, concurrency, retry, and leader-change
  tests before integrating DiskDB.

## Phase 2: Fence DiskDB Allocation Incarnations

- [ ] Add `allocation_ts` to `BusyBlockValue` and `Segment`, and
  `pre_allocation_ts` plus diagnostic `free_ts` to `FreeBlockValue`, with
  backward-compatible decode defaults.
- [ ] Generate `allocation_ts` monotonically across restart and immutable-owner
  operation. Define the R102 owner-handoff requirement to reconstruct or carry
  the high-water mark.
- [ ] Preserve `allocation_ts` through allocate, commit, ChunkDB metadata,
  DiskDB client transport, and free requests.
- [ ] Replace read-before-free owner validation with one R101 conditional
  batch: the current busy record must match `allocation_ts` and the required
  owner fields before it is deleted and replaced by a free record.
- [ ] Make a repeated free idempotent while its matching free record exists.
  After compaction removes that record, a delayed retry returns stale/not-found
  and can never affect a newer busy incarnation.
- [ ] Cover response-loss retry, retry before compaction, retry after reuse,
  concurrent duplicate free, owner mismatch, and multi-block batches.

## Phase 3: Implement R132 Slot-Ordered Compaction

- [ ] Expose each scan item's `commit_slot` without copying its value.
- [ ] Add a fixed-cutoff bounded current-version scan using the leader's
  contiguous-applied frontier, with one cutoff retained across pagination.
- [ ] Reject incomplete scans, changed cutoffs, timeouts, decode failures, and
  leader changes before a caller can publish a watermark.
- [ ] Add `compact_slot` to `ZoneValue` and its checksum. Legacy values start
  conservatively at slot zero and are rebuilt or fully replayed.
- [ ] Compact only free records in `(compact_slot, scan_cutoff]`, atomically
  persisting the new snapshot and deleting the exact scanned keys.
- [ ] Retain `free_ts` for diagnostics only; remove `free_ts` from all
  correctness decisions.
- [ ] Verify owner fencing, delayed lower-slot apply, concurrent free,
  pagination, batch failure, restart, and legacy recovery.

## Phase 4: Close R130

- [ ] Re-run the two failed mixed-workload sentinels: 64 workers with one block
  and 16 workers with four blocks.
- [ ] Run the complete memory regression sweep and the required block-mode
  exhaustion case on the three-node, 12-disk topology.
- [ ] Require zero correctness errors, a unique live set, and exact busy-space
  accounting before accepting benchmark results.
- [ ] Add the deterministic mixed-selection/accounting unit test and complete
  the three unchecked R130 benchmark checklist entries.
- [ ] Trace R130 public behavior to client E2E coverage and either add missing
  public-path cases or explicitly narrow acceptance where a behavior is
  intentionally server-internal.
- [ ] Finish the partial liveness/RPC module split or move it to a named
  follow-up requirement with explicit scope.
- [ ] Run the Pixi quality gates and relevant test commands, update permanent
  designs, close the working documents, and commit each coherent task once.

## Stop Conditions

- A conditional mutation requires a blocking lock or globally serializes the
  existing blind-write hot path.
- The conditional result cannot be tied to the applied Paxos slot across
  leader change and retry.
- `allocation_ts` cannot be reconstructed above every durable busy/free value
  on restart or future ownership handoff.
- A bounded scan cannot prove a complete current-version prefix at its fixed
  cutoff without adding MVCC or stronger immutable-key constraints.
