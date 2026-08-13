<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Zone Management Implementation Plan

Design: [`doc/design/diskdb/design-crow-diskdb-zone-management.md`](../design/diskdb/design-crow-diskdb-zone-management.md).
Goal: bring the existing R72/R73 zone-management code in line with the
latest design — persist-only free, `compact_ts` watermark compaction,
atomic batch_write snapshot+delete, compaction-on-rotation with a
preparatory thread, and `compacted_ready` zone tracking.

The existing code was written under R72 (allocation) / R73 (recovery)
before the design evolved to the persist-only free model and the
`compact_ts` watermark. This plan tracks the delta.

## Proto + Protocol

- [x] **Add `compact_ts` to `ZoneValue`**: add `uint64 compact_ts = 8`
  to `diskdb_type.proto` `ZoneValue`. Regenerate proto. Update
  `diskdb_types_test.rs` sample/baseline tests to include `compact_ts`.
  Files: `lib/crow-protocol/src/proto/diskdb_type.proto`,
  `lib/crow-protocol/tests/diskdb_types_test.rs`.
- [x] **Add `freed_ts` to `FreeBlockValue`**: un-reserve field 3 and
  add `uint64 freed_ts = 3` to `FreeBlockValue`. Regenerate proto.
  Update tests. Files: `lib/crow-protocol/src/proto/diskdb_type.proto`,
  `lib/crow-protocol/tests/diskdb_types_test.rs`.
- [x] **Update `ZoneValueExt` CRC to cover `compact_ts`**:
  `compute_checksum` and `verify_checksum` must hash `usage_bitmap` +
  `compact_ts` (not `usage_bitmap` alone). A corrupted `compact_ts`
  would break the watermark logic, so it must be integrity-protected.
  Update the doc comment and the baseline test (`crc32 = 0` case
  changes if `compact_ts` is included). Files:
  `lib/crow-protocol/src/diskdb_type_util.rs`,
  `lib/crow-protocol/tests/diskdb_types_test.rs`.

## DdbZone Model

- [x] **Add `compacted_ready` and `compact_ts` fields**: add
  `compacted_ready: AtomicBool` and `compact_ts: AtomicU64` to `DdbZone`.
  Initialize `compacted_ready = false` in `new`, `true` in
  `from_zone_value` and recovery. Initialize `compact_ts` from
  `ZoneValue.compact_ts` in `from_zone_value`. Files:
  `app/crow-diskdb/src/model/zone.rs`.
- [x] **Add zone-level lock**: add `zone_lock: RwLock<()>` to `DdbZone`
  for non-allocate operations (compaction, scanner, health checks).
  This is separate from `zone_state: RwLock<DdbZoneHealth>`. The lock
  is not held across `.await` (I9). Files:
  `app/crow-diskdb/src/model/zone.rs`.
- [x] **Replace `free` with `rollback_allocate`**: rename the existing
  `DdbZone::free` method to `rollback_allocate` (CAS-clear bits,
  decrement `used_count`). This is allocate-only (I8) — used only by
  the allocate Phase 2 failure path. Remove the
  `uncompacted_free_record_count` increment from this method (that
  belongs to the free path, not rollback). Files:
  `app/crow-diskdb/src/model/zone.rs`.
- [x] **Update `to_zone_value` to include `compact_ts`**: serialize
  `compact_ts` into the `ZoneValue` before computing the CRC. Files:
  `app/crow-diskdb/src/model/zone.rs`.
- [x] **Update `from_zone_value` to set `compact_ts` and
  `compacted_ready`**: restore `compact_ts` from `ZoneValue.compact_ts`,
  set `compacted_ready = true`. Files:
  `app/crow-diskdb/src/model/zone.rs`.
- [x] **Add `compact_zone_inner` common method**: read free records +
  current `ZoneValue` (KV read, no lock), partition by `compact_ts`
  watermark (stale vs new), acquire zone lock, `range_clear` only new
  records + recompute `used_count` + advance `compact_ts` monotonically,
  release lock, write new `ZoneValue` + delete all free records in one
  atomic `batch_write` (KV write, no lock). Set `compacted_ready = true`
  on success. Files: `app/crow-diskdb/src/model/zone.rs`.
- [x] **Add `scan_zone_inner` and `health_check_zone_inner` stubs**:
  placeholder common methods for the scanner (R75) and health probe
  (R76) — acquire zone lock, verify/compare bitmap, release lock. Full
  implementation is R75/R76; this task adds the method signatures and
  lock discipline. Files: `app/crow-diskdb/src/model/zone.rs`.
- [x] **Fix existing `free`-based tests**: the accessor tests
  (`busy_free_blocks_track_allocations_and_frees`, `usage_ratio_*`)
  currently call `z.free(...)` which clears bits. Update them to use
  `rollback_allocate` for the allocate-rollback semantics, and add
  separate tests for the persist-only free path (which does not touch
  the bitmap). Files: `app/crow-diskdb/src/model/zone.rs`.

## DdbDisk Model

- [x] **Make `DdbDisk::free` persist-only**: remove the bitmap clear
  and `used_count` decrement. The method should only look up the zone
  and increment `uncompacted_free_record_count` (the KV persist is done
  by `alloc.rs::free_block`). Files: `app/crow-diskdb/src/model/disk.rs`.
- [x] **Rewrite `rotate_active_zones` to pick ready zones**: pick zones
  where `compacted_ready == true` and not in the current active set.
  Clear `compacted_ready` when publishing a zone into the active set.
  If fewer than `zone_rotate_count` ready zones exist, fall back to
  synchronous compaction for the remainder (call `compact_zone_inner`
  inline). Files: `app/crow-diskdb/src/model/disk.rs`.
- [x] **Update `rebuild_active_zones` for `compacted_ready`**: during
  disk-add init and recovery, pick zones that are `compacted_ready`
  (recovery sets it; fresh zones from `disk_add_init` need compaction
  first or are empty → ready). Files:
  `app/crow-diskdb/src/model/disk.rs`.
- [x] **Add preparatory thread**: a background task that pre-compacts
  the next `zone_rotate_count` zones in the rotation order (starting
  from `pos_v_zone + zone_rotate_count`, wrapping). For each zone that
  is not ready and not active: acquire zone lock, compact, mark ready.
  Sleep briefly, re-check. Owned by `CompactionEngine` or a dedicated
  struct. Files: `app/crow-diskdb/src/model/disk.rs`,
  `app/crow-diskdb/src/recovery/compaction.rs`.

## DdbDiskGroup Model

- [x] **Add per-disk-group monotonic timestamp source**: add
  `free_ts_source: AtomicU64` to `DdbDiskGroup`. Advance by
  `max(now(), last + 1)` on each free. Used by the free path to
  generate `FreeBlockValue.freed_ts`. Files:
  `app/crow-diskdb/src/model/disk_group.rs`.
- [x] **Add `next_freed_ts` accessor**: returns the next monotonic
  `freed_ts` (advances the atomic). Called by `alloc.rs::free_block`
  before persisting the `FreeBlockValue`. Files:
  `app/crow-diskdb/src/model/disk_group.rs`.
- [x] **Make `DdbDiskGroup::free_block` persist-only**: remove the
  bitmap clear (delegates to `DdbDisk::free` which is now persist-only).
  Files: `app/crow-diskdb/src/model/disk_group.rs`.

## alloc.rs (Free Path)

- [x] **Rewrite `free_block` to persist-only**: Phase 1 (persist):
  one `batch_write` (Delete `BusyBlockKey` + Put `FreeBlockValue` with
  `freed_ts` from the disk-group timestamp source). Phase 2
  (post-persist, in-memory): increment
  `uncompacted_free_record_count` on the zone. No bitmap mutation, no
  `used_count` decrement. Remove the `dg.free_block(...)` bitmap-clear
  call. Files: `app/crow-diskdb/src/model/alloc.rs`.
- [x] **Rewrite `free_blocks` to persist-only**: same as `free_block`
  but batched — one `batch_write` for all `FreeBlockValue`s, then
  increment `uncompacted_free_record_count` per zone. No bitmap clear.
  Files: `app/crow-diskdb/src/model/alloc.rs`.
- [x] **Update allocate rollback to use `rollback_allocate`**: replace
  `zone.free(...)` calls in `allocate_block` and `allocate_blocks`
  Phase 2 failure paths with `zone.rollback_allocate(...)`. Files:
  `app/crow-diskdb/src/model/alloc.rs`.
- [x] **Update `FreeBlockValue` construction**: include `freed_ts` from
  `dg.next_freed_ts()` in both `free_block` and `free_blocks`. Files:
  `app/crow-diskdb/src/model/alloc.rs`.

## DdbKvClient

- [x] **Add `compact_zone_batch` method**: one atomic `batch_write`
  that Puts the new `ZoneValue` + Deletes all scanned free records
  (both stale and new). This replaces the two separate calls
  (`put_zone` + `delete_free_records_batch`) in the current compaction.
  Files: `app/crow-diskdb/src/ddb_kv_client.rs`.
- [x] **Update `persist_free` / `persist_free_batch` for `freed_ts`**:
  the `FreeBlockValue` already carries `freed_ts` (from the caller);
  no client-side change needed beyond the proto field existing. Verify
  serialization includes `freed_ts`. Files:
  `app/crow-diskdb/src/ddb_kv_client.rs`.

## Compaction Engine

- [x] **Rewrite `compact_zone` with watermark partition**: scan free
  records (KV read, no lock), read `compact_ts` (in-memory atomic),
  partition by `freed_ts <= compact_ts` (stale, drop) vs `freed_ts >
  compact_ts` (new, `range_clear`). Acquire zone lock, `range_clear`
  only new records, recompute `used_count = popcount`, advance
  `compact_ts = max(zone.compact_ts, max(freed_ts of new records))`
  (monotonic). Release lock. Files:
  `app/crow-diskdb/src/recovery/compaction.rs`.
- [x] **Use atomic `compact_zone_batch`**: replace the two-step
  `put_zone` + `delete_free_records_batch` with one
  `compact_zone_batch` call (Put ZoneValue + Delete all free records).
  This enforces I6 (atomic snapshot + delete). Files:
  `app/crow-diskdb/src/recovery/compaction.rs`.
- [x] **Set `compacted_ready = true` after compaction**: on successful
  compaction, set `zone.compacted_ready = true`. Files:
  `app/crow-diskdb/src/recovery/compaction.rs`.
- [x] **Skip active zones in `compaction_cycle`**: the periodic
  compaction loop must skip zones in the disk's `active_zone_context`
  (I4 — no concurrent allocate). Files:
  `app/crow-diskdb/src/recovery/compaction.rs`.
- [x] **Wire preparatory thread**: spawn the preparatory thread as part
  of the compaction engine's background task. It pre-compacts the next
  batch of zones and marks them `compacted_ready`. Files:
  `app/crow-diskdb/src/recovery/compaction.rs`.

## Recovery

- [x] **Persist-only recovery (Option B)**: recovery (strategy 2)
  only applies `Put BusyBlockKey` ops to the bitmap (sets bits for
  allocations). `Delete BusyBlockKey` ops (frees) are ignored — the
  bitmap is a conservative over-estimate, same as during normal
  operation. `compact_ts` stays at `snapshot.compact_ts` (no
  advancement). The free records on disk are the source of truth;
  the common compaction flow clears the freed bits. This eliminates
  the double-free risk that would arise if recovery cleared bits for
  frees while the free records still exist on disk. Files:
  `app/crow-diskdb/src/recovery/journal_replay.rs`.
- [x] **Set `compacted_ready = true` after recovery**: both strategy 2
  (`recover_zone_inner`) and strategy 1
  (`rebuild_zone_bitmap_full_scan`) must set `compacted_ready = true`
  on the recovered zone (the bitmap is accurate from records). Files:
  `app/crow-diskdb/src/recovery/journal_replay.rs`,
  `app/crow-diskdb/src/recovery/full_scan.rs`.
- [x] **Initialize timestamp source during `recover_disk_group`**: after
  all zones in a disk-group are recovered, initialize the
  `DdbDiskGroup.free_ts_source` to `max(now(), max(freed_ts of all
  scanned free records) + 1)`. `recover_zone_inner` now returns
  `(DdbZone, max_freed_ts)` and `recover_disk_group` collects the max
  across all zones and calls `init_free_ts_source_after_recovery`.
  Files: `app/crow-diskdb/src/recovery.rs`,
  `app/crow-diskdb/src/recovery/journal_replay.rs`.
- [x] **Set `compact_ts = 0` in full-scan recovery**: strategy 1
  rebuilds from busy records only (no free records scanned), so
  `compact_ts = 0`. The next compaction will advance it. Files:
  `app/crow-diskdb/src/recovery/full_scan.rs`.

## Server Wiring

- [x] **Wire preparatory thread startup/shutdown**: the server startup
  path must spawn the preparatory thread (owned by `CompactionEngine`)
  after recovery completes. Shutdown must cancel it. Files:
  `app/crow-diskdb/src/service.rs` or `app/crow-diskdb/src/main.rs`.

## File List

- `lib/crow-protocol/src/proto/diskdb_type.proto` — add `compact_ts`
  to `ZoneValue`, `freed_ts` to `FreeBlockValue`.
- `lib/crow-protocol/src/diskdb_type_util.rs` — CRC over
  `usage_bitmap + compact_ts`.
- `lib/crow-protocol/tests/diskdb_types_test.rs` — update sample/baseline
  tests for new fields + CRC scope.
- `app/crow-diskdb/src/model/zone.rs` — `compacted_ready`,
  `compact_ts`, zone lock, `rollback_allocate`, `compact_zone_inner`,
  `to_zone_value`/`from_zone_value` updates, test fixes.
- `app/crow-diskdb/src/model/disk.rs` — persist-only `free`,
  `compacted_ready`-based rotation, preparatory thread.
- `app/crow-diskdb/src/model/disk_group.rs` — monotonic timestamp
  source, persist-only `free_block`.
- `app/crow-diskdb/src/model/alloc.rs` — persist-only free path with
  `freed_ts`, `rollback_allocate` for allocate rollback.
- `app/crow-diskdb/src/ddb_kv_client.rs` — `compact_zone_batch`
  (atomic Put ZoneValue + Delete free records).
- `app/crow-diskdb/src/recovery/compaction.rs` — watermark partition,
  atomic batch, zone lock, `compacted_ready`, skip active zones,
  preparatory thread.
- `app/crow-diskdb/src/recovery/journal_replay.rs` — advance
  `compact_ts` after replay, `compacted_ready`, return max `freed_ts`.
- `app/crow-diskdb/src/recovery/full_scan.rs` — `compacted_ready`,
  `compact_ts = 0`.
- `app/crow-diskdb/src/recovery.rs` — timestamp source init in
  `recover_disk_group`.
- `app/crow-diskdb/src/service.rs` / `app/crow-diskdb/src/main.rs` —
  preparatory thread lifecycle.
- `lib/crow-protocol/src/proto/diskdb_op.proto` — `CompactZoneRequest`
  / `CompactZoneResponse` / `ZoneCompactionResult` messages.
- `lib/crow-protocol/src/proto/diskdb_service.proto` — `CompactZone`
  RPC.
- `lib/crow-diskdb-client/src/client.rs` — `compact_zone` client
  method.
- `app/crow-diskdb/src/service/diskdb_service.rs` — `compact_zone`
  RPC handler.

## Test Checklist

### Unit tests (zone.rs)

- [x] **`rollback_allocate` clears bits + decrements `used_count`**:
  allocate, then rollback → bit clear, `used_count` back to 0.
  (`zone_rollback_multi_unit`, `zone_rollback_then_reallocate`)
- [x] **`rollback_allocate` does not increment
  `uncompacted_free_record_count`**: verify the counter is unchanged.
  (`rollback_allocate_does_not_increment_backlog`)
- [x] **Persist-only free does not touch bitmap**: after a free, the
  bit stays set, `used_count` unchanged, `uncompacted_free_record_count`
  incremented. (`disk_free_is_persist_only`, `record_free_increments_backlog`)
- [x] **`compacted_ready` lifecycle**: `false` on `new`, `true` after
  `from_zone_value` / compaction / recovery, `false` after rotation
  into active set. (`compacted_ready_lifecycle`)
- [x] **`compact_ts` restored from `ZoneValue`**: `from_zone_value`
  sets `compact_ts` from the snapshot. (`zone_to_from_zone_value_roundtrip`)
- [x] **`to_zone_value` includes `compact_ts`**: serialized `ZoneValue`
  has the correct `compact_ts` and CRC verifies. (`zone_to_from_zone_value_roundtrip`)
- [x] **CRC covers `compact_ts`**: corrupting `compact_ts` fails
  `verify_checksum`. (`zone_from_zone_value_rejects_bad_crc`)

### Unit tests (compaction)

- [x] **Watermark partition — stale records dropped**: free records
  with `freed_ts <= compact_ts` are not `range_clear`ed (bits already
  clear). (`compact_zone_inner_stale_records_dropped`)
- [x] **Watermark partition — new records cleared**: free records with
  `freed_ts > compact_ts` are `range_clear`ed, `used_count` recomputed.
  (`compact_zone_inner_new_records_cleared`)
- [x] **`compact_ts` monotonic**: `new_compact_ts = max(old, max(new
  freed_ts))` — no regression even with a stale step-1 read.
  (`compact_zone_inner_compact_ts_monotonic`)
- [x] **Atomic batch_write (I6)**: snapshot + delete succeed or fail
  together — no window where snapshot is durable but free records
  survive. (`compaction_compact_zone_writes_snapshot_and_deletes_free_records`)
- [x] **Watermark partition — mixed stale + new**: a batch with both
  stale and new records drops stale, clears new.
  (`compact_zone_inner_mixed_stale_and_new`)
- [ ] **Double-free prevention after crashed compaction**: orphaned
  free record (`freed_ts <= compact_ts`) is dropped, not `range_clear`ed,
  even if the block was re-allocated. (Requires crash-injection
  harness — deferred to a follow-up task.)
- [x] **`compacted_ready` set after compaction**: zone is eligible for
  rotation. (`compacted_ready_lifecycle`)

### Unit tests (recovery)

- [x] **Persist-only recovery (Option B)**: recovery only applies `Put
  BusyBlockKey` (sets bits). `Delete BusyBlockKey` (frees) are ignored
  — bitmap is a conservative over-estimate. `compact_ts` stays at
  `snapshot.compact_ts`. (`recovery_strategy2_journal_replay`)
- [x] **Compaction clears freed bits after recovery**: after recovery,
  running compaction on a zone with free records clears the freed bits
  and deletes the free records. (`recovery_strategy2_journal_replay`
  step 7)
- [x] **Timestamp source init**: `recover_disk_group` initializes
  `free_ts_source` to `max(now(), max(freed_ts) + 1)`.
  (`recover_zone_inner` returns `max_freed_ts` from scanned free
  records; `recover_disk_group` collects the max and calls
  `init_free_ts_source_after_recovery`.)
- [x] **`compacted_ready = true` after strategy 2**: recovered zone is
  eligible for the active set. (`recovery_strategy2_journal_replay`)
- [x] **`compacted_ready = true` after strategy 1**: full-scan
  recovered zone is eligible. (`recovery_strategy1_full_scan_rebuilds_bitmap`)

### Unit tests (rotation)

- [x] **Rotation picks `compacted_ready` zones**: only ready zones
  enter the active set. (`zone_rotate_one_uses_all_zones`,
  `disk_allocate_rotates_across_active_zones`)
- [x] **`compacted_ready` cleared on rotation**: published zones get
  `compacted_ready = false`. (`rotate_clears_compacted_ready_on_published_zones`)
- [x] **Fallback to synchronous compaction**: when no ready zones
  exist, `allocate_block`/`allocate_blocks` compacts non-active zones
  inline (via `compact_fallback`) then retries. (Implemented at the
  async `alloc.rs` layer — the sync `rotate_active_zones` picks
  un-compacted zones as a conservative fallback; the async caller
  compacts them before retrying.)

### Integration tests

- [x] **Allocate → free → compaction → re-allocate cycle**: verify
  space is reclaimed by compaction and re-allocated correctly.
  (`diskdb_e2e_compact_zone_rpc` — exercises the `CompactZone` RPC
  handler end-to-end.)
- [ ] **Crash during compaction (atomic batch)**: simulate a crash
  between snapshot write and delete (legacy two-op) → verify the
  watermark prevents double-free on the next compaction. (Requires
  crash-injection harness — deferred.)
- [ ] **Crash after journal replay**: recover, then crash, then
  recover again → verify persist-only recovery is idempotent.
  (Deferred — needs multi-restart harness.)
- [ ] **Preparatory thread keeps up with rotation**: under churn,
  verify ready zones are available for rotation without synchronous
  compaction fallback. (Deferred — needs long-running churn harness.)
- [x] **Persist-only free invariant (I1)**: after free, bitmap still
  shows busy; after compaction, bitmap shows free; `used_count` only
  decremented by compaction. (`diskdb_e2e_compact_zone_rpc`,
  `disk_free_is_persist_only`, `free_is_persist_only`)

## Open Follow-ups

- **Crash-injection harness** for the three deferred integration
  tests (crash during compaction, crash after replay, preparatory
  thread under churn).
