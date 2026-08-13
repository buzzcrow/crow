<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Crash Recovery + Snapshot Compaction Plan

Design draft: `doc/working/design-diskdb-recovery.md`.
Backlog: `doc/backlog/R73-diskdb-crash-recovery-snapshot.md`.
Goal: implement crash recovery (strategies 1 + 2) and snapshot compaction
(strategy 3) so diskdb can restart and transfer ownership safely, plus the
`JournalScan` crow-kv extension and `RebuildZoneBitmap` RPC they depend on.

## Phase 1 — Proto + types (foundation)

- [ ] **Add `JournalScan` proto to kv.proto**: add
  `KvJournalScanRequest` / `KvJournalScanResponse` / `KvJournalOp` messages,
  the `JournalScan` rpc, and `KV_ERROR_JOURNAL_SCAN_GC_GAP` to `KvErrorCode`.
  Files: `lib/crow-kv/src/rpc/proto/kv.proto`.
- [ ] **Add `RebuildZoneBitmap` proto**: add `RebuildZoneBitmapRequest` /
  `RebuildZoneBitmapResponse` messages and the `RebuildZoneBitmap` rpc to
  `DiskdbService`. Files: `lib/crow-protocol/src/proto/diskdb_service.proto`,
  `lib/crow-protocol/src/proto/diskdb_op.proto`.
- [ ] **Regenerate proto bindings**: run the proto build step so the new
  messages/rpcs are available in Rust. Files: generated `*.rs` under
  `lib/crow-kv/src/rpc/proto/` and `lib/crow-protocol/src/proto/`.

## Phase 2 — crow-kv `JournalScan` extension

- [ ] **Core `journal_scan` logic in px_kv_store**: iterate the WAL segment
  index in slot order from `min_slot` to `effective_max_slot`, decode each
  record's KV payload batch, filter by `key_prefix`, emit `KvJournalOp`s in
  slot order. Honor `limit` + `truncated` + `last_op_slot`. Return
  `KV_ERROR_JOURNAL_SCAN_GC_GAP` when `min_slot < gc_slot`. Files:
  `lib/crow-kv/src/cluster/px_kv_store.rs`.
- [ ] **gRPC handler in kv_service**: `JournalScan` handler mirroring the
  existing `scan` handler's leader-forward + read-mode (`LINEARIZABLE` /
  `MIN_SLOT`) logic. Files: `lib/crow-kv/src/rpc/kv_service.rs`.
- [ ] **Client method `CrowkvClient::journal_scan`**: transparent pagination
  (send first request, if `truncated` resend with `min_slot =
  last_op_slot + 1`, repeat until complete). Mirrors the existing `scan`
  method's `resolve_min_slot` / `resolve_read_endpoint` logic. Files:
  `lib/crow-kv-client/src/client.rs`.
- [ ] **Mock `JournalScan` support**: extend the mock crow-kv-compatible
  server used in integration tests to keep an in-memory op log and answer
  `JournalScan` by filtering `[min_slot, max_slot]` + key prefix, sorted by
  slot. Files: `app/crow-diskdb/tests/common/cluster.rs` (or the shared mock
  helper file).

## Phase 3 — diskdb persistence + zone extensions

- [ ] **`DataGroupClient::get_zone_value`**: point `get` on `ZoneKey` only
  (1 round-trip); returns `Option<ZoneValue>`. Avoids the 2 wasted scans of
  `read_zone_records` when only the snapshot is needed. Files:
  `app/crow-diskdb/src/persistence.rs`.
- [ ] **`DataGroupClient::get_applied_slot`**: get the data group's current
  applied frontier via a lightweight `Scan` with `read_mode = Linearizable`,
  reading `read_slot` from the response. Files:
  `app/crow-diskdb/src/persistence.rs`.
- [ ] **`DataGroupClient::journal_scan_busy` / `journal_scan_free`**: wrap
  `CrowkvClient::journal_scan` with `BusyBlockKey::prefix_for_zone` and
  `FreeBlockKey::prefix_for_zone` respectively. Files:
  `app/crow-diskdb/src/persistence.rs`.
- [ ] **`Zone::to_zone_value`**: snapshot current `usage_bits` +
  `snapshot_slot` + `crc32` (via `ZoneValueExt::compute_checksum`) for
  compaction. Files: `app/crow-diskdb/src/zone.rs`.
- [ ] **`Zone::from_zone_value`**: rebuild a `Zone` from a snapshot
  (deserialize `usage_bitmap` into `UsageBitmap`, `used_count` = popcount,
  set `snapshot_slot`). Verifies CRC via `ZoneValueExt::verify_checksum`
  before use. Files: `app/crow-diskdb/src/zone.rs`.

## Phase 4 — Recovery engine (strategies 1 + 2)

- [ ] **Register `recovery` module**: add `pub mod recovery;` to `lib.rs`.
  Files: `app/crow-diskdb/src/lib.rs`.
- [ ] **`RecoveryEngine` struct**: owns a `DataGroupClient`; constructed
  with the client from the server wiring. Files:
  `app/crow-diskdb/src/recovery.rs`.
- [ ] **`recover_zone` (strategy 2 — journal scan replay)**: load latest
  `ZoneValue` via `get_zone_value` (verify CRC, fall back to strategy 1 on
  failure); initialize replay state from snapshot or empty; two narrow
  journal scans (busy + free prefixes) merged by slot; apply ops in slot
  order (Put BusyBlockKey → `range_set`; Delete BusyBlockKey → `range_clear`
  using `unit_count` from the matching `FreeBlockValue` at the same slot;
  FreeBlockKey ops → no-op for state; Put ZoneKey after `snapshot_slot` →
  restart replay from the newer snapshot); build the recovered `Zone`.
  Fall back to strategy 1 on `KV_ERROR_JOURNAL_SCAN_GC_GAP`. Files:
  `app/crow-diskdb/src/recovery.rs`.
- [ ] **`rebuild_zone_bitmap_full_scan` (strategy 1)**: scan
  `BusyBlockKey`s via `read_zone_records` (key order = unit_offset order,
  no slot ordering needed); `range_set` each; ignore `FreeBlockKey`s;
  build the `Zone`; optionally write a fresh `ZoneValue` snapshot so the
  next restart can use strategy 2; return `(Zone, ZoneStats)`. Files:
  `app/crow-diskdb/src/recovery.rs`.
- [ ] **`recover_node`**: create empty `Node`; for each `(disk_id,
  DiskValue)` create a `ZoneDisk`, recover each zone (parallel, bounded by
  `recovery_concurrency` semaphore), `disk.add_zone`, call
  `disk.rebuild_active_zones(zone_rotate_count)`, `node.add_disk`; return
  `Arc<Node>`. Files: `app/crow-diskdb/src/recovery.rs`.

## Phase 5 — Compaction engine (strategy 3)

- [ ] **`CompactionEngine` + `compaction_loop`**: background task that
  sleeps `compaction_cadence`, then for each owned node/disk/zone checks
  `uncompacted_free_record_count` against `snapshot_compaction_threshold`
  and calls `compact_zone` when exceeded. Cadence OR threshold — whichever
  fires first. Files: `app/crow-diskdb/src/recovery/compaction.rs`.
- [ ] **`compact_zone`**: scan free records (`read_zone_records` `free`
  field or a dedicated `scan_free_records` helper); merge into the
  in-memory bitmap (`range_clear` per free record); determine
  `snapshot_slot` = `get_applied_slot`; build new `ZoneValue` (with CRC);
  write `ZoneValue` **before** deleting free records (crash-safety
  invariant); delete free records in one `batch_write`; update zone's
  `snapshot_slot` + decrement `uncompacted_free_record_count`; increment
  `compaction.count` / `compaction.error.count`. Files:
  `app/crow-diskdb/src/recovery/compaction.rs`.
- [ ] **`compact_zone_now` (on-demand)**: expose manual triggering for
  pre-transfer compaction or operator-triggered backlog drain. Files:
  `app/crow-diskdb/src/recovery/compaction.rs`.

## Phase 6 — Config + server wiring

- [ ] **Config rename + add `recovery_concurrency`**: rename
  `snapshot_interval_secs` → `compaction_cadence_secs`,
  `snapshot_journal_threshold` → `snapshot_compaction_threshold`; add
  `recovery_concurrency: usize` (default 16). Update `validate()`. Files:
  `app/crow-diskdb/src/config.rs`.
- [ ] **`RebuildZoneBitmap` gRPC handler**: add the handler to
  `DiskdbService` (delegates to
  `RecoveryEngine::rebuild_zone_bitmap_full_scan`); implement the
  `DiskdbService` trait method in `grpc.rs`. Files:
  `app/crow-diskdb/src/grpc.rs`.
- [ ] **Startup recovery in main.rs**: after the blocking `sync_once`,
  create `RecoveryEngine` and run `recover_node` for each owned disk-group
  (blocking — server must not serve RPCs until recovery completes); then
  create `CompactionEngine` and spawn `compaction_loop`; then start gRPC.
  Files: `app/crow-diskdb/src/main.rs`.
- [ ] **Ownership-transfer recovery in sync.rs**: when `sync_once` detects
  a new disk-group, check `get_zone_value`; if snapshots exist run
  `recover_node` (replay from `snapshot_slot`), else keep `disk_add_init`
  for fresh disks. Add `with_recovery_engine` builder to `SyncLoop`
  (mirrors `with_data_group_client`). Files: `app/crow-diskdb/src/sync.rs`.

## File list

- `lib/crow-kv/src/rpc/proto/kv.proto` — add `KvJournalScanRequest` /
  `KvJournalScanResponse` / `KvJournalOp`, `JournalScan` rpc,
  `KV_ERROR_JOURNAL_SCAN_GC_GAP`.
- `lib/crow-kv/src/cluster/px_kv_store.rs` — core `journal_scan` logic
  (WAL slot-ordered iteration, KV payload decode, prefix filter).
- `lib/crow-kv/src/rpc/kv_service.rs` — `JournalScan` gRPC handler.
- `lib/crow-kv-client/src/client.rs` — `CrowkvClient::journal_scan`
  (transparent pagination).
- `lib/crow-protocol/src/proto/diskdb_service.proto` — add
  `RebuildZoneBitmap` rpc.
- `lib/crow-protocol/src/proto/diskdb_op.proto` — add
  `RebuildZoneBitmapRequest` / `RebuildZoneBitmapResponse`.
- `app/crow-diskdb/src/recovery.rs` — new module: `RecoveryEngine`,
  `recover_node`, `recover_zone` (strategy 2),
  `rebuild_zone_bitmap_full_scan` (strategy 1).
- `app/crow-diskdb/src/recovery/compaction.rs` — new sub-module:
  `CompactionEngine`, `compaction_loop`, `compact_zone`, `compact_zone_now`.
- `app/crow-diskdb/src/persistence.rs` — add `get_zone_value`,
  `get_applied_slot`, `journal_scan_busy`, `journal_scan_free`.
- `app/crow-diskdb/src/zone.rs` — add `to_zone_value`, `from_zone_value`.
- `app/crow-diskdb/src/grpc.rs` — add `RebuildZoneBitmap` handler.
- `app/crow-diskdb/src/config.rs` — rename `snapshot_interval_secs` →
  `compaction_cadence_secs`, `snapshot_journal_threshold` →
  `snapshot_compaction_threshold`; add `recovery_concurrency`; update
  `validate()`.
- `app/crow-diskdb/src/sync.rs` — `with_recovery_engine` builder; trigger
  recovery on ownership transfer when snapshots exist; keep `disk_add_init`
  for fresh disks.
- `app/crow-diskdb/src/lib.rs` — add `recovery` module.
- `app/crow-diskdb/src/main.rs` — run recovery on startup (after
  `sync_once`), spawn compaction loop, wire `RebuildZoneBitmap` handler.
- `app/crow-diskdb/tests/common/cluster.rs` (or shared mock helper) — mock
  `JournalScan` support (in-memory op log).

## Test checklist

### Unit tests

- [ ] `recover_zone` no snapshot + no records → fresh zone (empty bitmap,
  `used_count = 0`).
- [ ] `recover_zone` snapshot + no records after → returns snapshot state
  (verify `usage_bits` + `used_count`).
- [ ] `recover_zone` snapshot + records after → applies ops in slot order:
  snapshot at slot 10, Put BusyBlockKey at 11–15, Delete BusyBlockKey + Put
  FreeBlockKey at 16 → bits set for 11–15, cleared for the freed one.
- [ ] `recover_zone` no snapshot + records → replays from slot 0.
- [ ] `recover_zone` corrupted snapshot (CRC fail) → logs error, falls
  back to strategy 1.
- [ ] `recover_zone` `KV_ERROR_JOURNAL_SCAN_GC_GAP` → falls back to
  strategy 1 for the affected zone.
- [ ] `rebuild_zone_bitmap_full_scan` scans BusyBlockKeys, rebuilds
  bitmap, ignores FreeBlockKeys (write busy + free, rebuild, verify =
  busy keys only).
- [ ] `rebuild_zone_bitmap_full_scan` writes a fresh `ZoneValue` snapshot
  after rebuild (verify snapshot exists with correct `snapshot_slot`).
- [ ] `compact_zone` merges free records into new `ZoneValue`, writes it,
  deletes only free records (busy records for live blocks not deleted).
- [ ] Compaction crash after `ZoneValue` write but before free-record
  delete → replay still correct (orphaned free records are no-ops).
- [ ] Compaction crash after free-record delete → snapshot is source of
  truth.
- [ ] `Zone::to_zone_value` / `from_zone_value` round-trip (bitmap +
  `snapshot_slot` + `crc32` match).
- [ ] Config validation — renamed `CompactionConfig` fields +
  `recovery_concurrency`.

### Integration tests

- [ ] `JournalScan` crow-kv RPC returns ops in slot order within key-prefix
  filter; verify slot ordering, prefix filtering, pagination (`limit` +
  `last_op_slot`).
- [ ] `recover_node` allocates/free blocks, restart diskdb (re-run
  recovery), verify in-memory state matches records.
- [ ] `recover_node` with compaction — allocate/free, compact, restart,
  verify recovery is fast (few records) and correct (bitmap matches).
- [ ] `RebuildZoneBitmap` gRPC handler triggers strategy 1 and returns
  derived stats.
- [ ] Compaction trigger on threshold — free enough to exceed
  `snapshot_compaction_threshold`, verify compaction runs.
- [ ] Compaction trigger on cadence — wait for `compaction_cadence`,
  verify compaction runs.
- [ ] Server startup gates gRPC on recovery — connect before recovery →
  `Unavailable`; connect after → success.
- [ ] Recovery concurrency — recover a node with many zones, verify
  parallel recovery (timing < sequential baseline).

### Ownership-transfer unit tests (mock data-group)

- [ ] Transfer with only busy records (no snapshot): A allocates 5,
  transfers to B → B recovers 5 bits (matches A's records, not A's stale
  state).
- [ ] Transfer after allocate + free (busy + free, no compaction): A
  allocates 5, frees 2, transfers to B → B recovers 3 live busy bits.
- [ ] Transfer after compaction (snapshot + few records): A compacts (6
  bits at slot S), transfers to B → B loads snapshot, journal-scan from
  S+1 empty → 6 bits (fast, no replay).
- [ ] Transfer after compaction + post-compaction allocates: A compacts (6
  bits at S), allocates 3 (S+1..S+3), transfers to B → B loads snapshot (6)
  + replays 3 Put ops → 9 bits.
- [ ] Transfer after compaction + post-compaction free: A compacts (6 at
  S), frees 1 (S+1), transfers to B → B loads snapshot (6) + replays free
  op → 5 bits.
- [ ] Transfer with no records (fresh disk-group): no `ZoneValue`, no
  records → `disk_add_init` path (not recovery), B's bitmap empty.
- [ ] Transfer back to original owner (A → B → A): A allocates 5,
  transfers to B (recovers 5), B allocates 3 (8 total), transfers back to
  A → A discards stale state, recovers 8 bits from records.
- [ ] Transfer with corrupted snapshot: A compacts, corrupts `crc32`,
  transfers to B → B's CRC check fails, falls back to strategy 1 (full
  scan).

### E2E test — disk-group transfer between diskdb instances

- [ ] Phase 1: instance A `sync_once` → `disk_add_init` → allocate 5,
  free 2 (3 live busy records on group 1).
- [ ] Phase 2: `hw.set_owner` → instance B; B `sync_once` → detects
  disk-group → `get_zone_value` finds snapshots → `recover_node`.
- [ ] Phase 3: verify B's recovered bitmap matches records (3 bits, not
  A's stale state); verify `snapshot_slot` + `used_count`.
- [ ] Phase 4: B allocates 3 more, frees 1 → verify records on group 1;
  recovery did not corrupt the allocator.
- [ ] Phase 5: transfer back to A (A → B → A); A discards stale
  `NodeContainer`, recovers from records (5 live busy: 3 old + 3 from B −
  1 freed by B); proves recovery is idempotent and stateless-on-disk.

### Verification commands

- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run clean-env && pixi run cargo test -p crow-kv` (JournalScan
  handler).
- [ ] `pixi run clean-env && pixi run cargo test -p crow-diskdb`
  (recovery + compaction + transfer).
