<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: R72 Cleanup — Metrics Wiring + Owner Validation + Status-Change Refresh

Addresses the gaps identified in
`doc/backlog/R72-diskdb-zone-allocator-journal.md` after the R72
implementation (commit `539f596` + `248989b`). Three items are R72's
own cleanup (do now); five items are reassigned to their owner
requirements (noted at the bottom).

## Do Now (R72 Cleanup)

### Task 1: Wire `DiskdbMetrics` into the running server

**Problem**: `DiskdbMetrics` is defined in
`app/crow-diskdb/src/metrics.rs` with `allocate_retry_cas_bit` and
`disk_bad_impacted_blocks` counter handles, but `main.rs` never
creates a `MetricsRegistry` or `DiskdbMetrics`. `Zone::
metrics_cas_retry` is always `None` in production — only the internal
`cas_retry_count` atomic works. The `zone.allocate.retry.cms.bit`
crow-common counter is never registered or published.

- [ ] Add `MetricsRegistry` creation in `main.rs` (or pass through
      from a top-level metrics setup).
- [ ] Register `DiskdbMetrics` in the registry.
- [ ] Pass the `allocate_retry_cas_bit` counter handle into the sync
      loop so `disk_add_init` can attach it to each `Zone` via
      `Zone::with_cas_retry_metric(counter)`.
- [ ] Verify: under CAS contention, the crow-common counter
      increments (not just the `Zone::cas_retry_count` atomic).

**Files**:
- `app/crow-diskdb/src/main.rs` — create `MetricsRegistry`, register
  `DiskdbMetrics`, pass counter handle into `SyncLoop`.
- `app/crow-diskdb/src/sync.rs` — accept an optional
  `Arc<Counter>` in `SyncConfig` or `SyncLoop`; in `disk_add_init`,
  call `Zone::with_cas_retry_metric` when the handle is `Some`.
- `app/crow-diskdb/src/metrics.rs` — no change (already defines the
  handles).

**Tests**:
- [ ] Unit test: create a `Zone` with `with_cas_retry_metric`, force
      CAS contention, verify the `Counter` increments.
- [ ] Existing `zone_cas_retry_counter_increments_under_contention`
      still passes.

### Task 2: Wire `validate_owner_on_free` into the free path

**Problem**: The config field `validate_owner_on_free` (default
`false`) exists in `StorageDefaults` but the free path in
`persistence.rs` never checks it. When `true`, the free path should
read the `BusyBlockValue` from the data group first and validate
`owner_chunk` before proceeding (one extra paxos round-trip, doubles
free latency). When `false` (default), the free is no-read (owner from
`Segment`) — current behavior.

- [ ] Add a `get_busy` method to `DataGroupClient` — point-lookup
      `BusyBlockKey` via `kv.get`, decode `BusyBlockValue`. Returns
      `Option<BusyBlockValue>` (`None` if the key doesn't exist =
      already freed or never allocated).
- [ ] In `free_block` (`persistence.rs`): when
      `validate_owner_on_free` is `true`, call `get_busy` first. If
      the record is `None` → return error (block not busy = double-
      free or never allocated). If the record's `owner_chunk` != the
      `Segment`'s `owner_chunk` → return error (ownership mismatch).
      Only proceed to bitmap clear + persist if validation passes.
- [ ] In `free_blocks` (`persistence.rs`): same check per segment
      when `validate_owner_on_free` is `true`. Collect validation
      errors; if any segment fails, return error before any bitmap
      clear (all-or-nothing).
- [ ] Pass `validate_owner_on_free` through the call chain: gRPC
      handler → `persistence::free_block` / `free_blocks`.

**Files**:
- `app/crow-diskdb/src/persistence.rs` — add `get_busy` to
  `DataGroupClient`; add `validate_owner_on_free: bool` parameter to
  `free_block` and `free_blocks`; add validation logic.
- `app/crow-diskdb/src/grpc.rs` — pass
  `self.storage.validate_owner_on_free` to `free_blocks`.

**Tests**:
- [ ] Unit test: `validate_owner_on_free = true` — free with
      matching owner succeeds (mock KV returns the right
      `BusyBlockValue`).
- [ ] Unit test: `validate_owner_on_free = true` — free with
      mismatched owner returns error, no bitmap clear.
- [ ] Unit test: `validate_owner_on_free = true` — free a non-busy
      block (no `BusyBlockKey`) returns error.
- [ ] Unit test: `validate_owner_on_free = false` — free skips
      validation (current behavior, no KV read).
- [ ] E2E test (`diskdb_e2e_test.rs`): free with
      `validate_owner_on_free = true` and matching owner succeeds;
      verify `FreeBlockValue` persisted and `BusyBlockKey` deleted.

**Error type**: Add `FreeError::NotBusy` and
`FreeError::OwnerMismatch { expected, actual }` (or reuse
`crow_kv_client::Error` with a descriptive variant). The gRPC handler
maps: `NotBusy` → `NotFound` (or `FailedPrecondition`),
`OwnerMismatch` → `PermissionDenied`.

### Task 3: Call `rebuild_allocating_disks()` on disk status change

**Problem**: The sync loop calls `rebuild_allocating_disks()` only in
`disk_add_init` (new disk). When a disk's `effective_status` changes
(e.g. `Up → Missing`), the RCU `allocating_disks` context is not
refreshed — it still includes the now-non-allocatable disk. The
`disk.allocatable()` check in `disk_allocate` still rejects it, so
correctness is preserved, but round-robin wastes cycles probing dead
disks.

- [ ] In `sync.rs::reconcile_disks`, after `set_effective_status`
      on an existing disk (both the status-change path and the
      Missing-detection path), call `node.rebuild_allocating_disks()`.

**Files**:
- `app/crow-diskdb/src/sync.rs` — add
  `node.rebuild_allocating_disks()` after each
  `set_effective_status` call in `reconcile_disks`.

**Tests**:
- [ ] Unit test: a node with 3 disks (all `Up`); transition one to
      `Missing` via `set_effective_status`; verify
      `allocating_disks` no longer includes it (round-robin skips
      it without probing).
- [ ] Existing `disk_alloc_test.rs` tests still pass.

## Dependency Ordering

1. Task 1 (metrics wiring) — independent.
2. Task 2 (validate_owner_on_free) — independent.
3. Task 3 (status-change refresh) — independent.

All three are independent; any order works. Recommend Task 3 first
(smallest), then Task 1, then Task 2 (most code).

## Verification

- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy -p crow-diskdb --all-targets -- -D warnings`
- [ ] `pixi run cargo test -p crow-diskdb --test zone_alloc_test`
- [ ] `pixi run cargo test -p crow-diskdb --test disk_alloc_test`
- [ ] `pixi run cargo test -p crow-diskdb --test diskdb_e2e_test`

## Reassigned to Later Requirements

These gaps are NOT R72's responsibility — they're owned by future
requirements. Noted here so they're not lost. Each should be added to
the respective backlog doc's Problem or Solution section.

- **`disk.bad.impacted_blocks` counter → R76** — The design doc (§10,
  updated) specifies that on a disk transitioning to `Bad`, the sync
  path scans zone records for live `BusyBlockValue`s (the impacted
  blocks), emits the `disk.bad.impacted_blocks` gauge, and logs the
  hand-off. This is R76's bad-disk handling scope. The metric handle
  already exists in `DiskdbMetrics`; R76 adds the producer.

- **R73/R75 gRPC stubs (`rebuild_zone_bitmap`, `mark_block_suspect`,
  `mark_block_corrupt`) → R73/R75** — These RPCs are not defined in
  `diskdb_service.proto` (only 5 RPCs exist). R73 adds
  `rebuild_zone_bitmap` (strategy 1 full scan); R75 adds
  `mark_block_suspect` / `mark_block_corrupt` (per-block state
  transitions). The proto must be extended first, then the stubs
  implemented.

- **`free_batch_enabled` / `free_flush_max_batch` wiring → R79** —
  Config fields exist in `PersistenceConfig` but the free path does
  not check them. R79 adds the size-threshold batch flush logic
  (grouping frees, flushing via `persist_free_batch` when the batch
  reaches `free_flush_max_batch`). R72 ships with immediate free
  only.

- **Keepalive usage summary piggyback (§11) → R74** — The sync loop's
  `heartbeat_diskdb` call passes empty arrays. §11 specifies a per-
  disk-group usage summary (`capacity_bytes`, `used_bytes`,
  `free_bytes`, `disk_count`, `allocatable_disk_count`) piggybacked
  on keepalive. R74 (space metrics) computes these from the in-memory
  bitmap and adds them to the heartbeat.

- **Latency hierarchy metrics (§11) → R74** — §11 specifies a detailed
  latency breakdown (`allocate.rpc.latency_us`,
  `allocate.bitmap_scan.latency_us`, `allocate.kv_persist.latency_us`,
  `free.*`, `sync.*`, `compaction.*`). None are implemented. R74
  (space metrics + query API) adds these using `LatencyHistogram` /
  `LatencySummary` per §11.
