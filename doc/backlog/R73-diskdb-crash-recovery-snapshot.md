<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R73: diskdb — Crash Recovery + Snapshot Compaction

**Problem**: R72 implements the allocation engine with record-based
persistence — each allocate writes a `BusyBlockValue` at
`BusyBlockKey`, each free deletes the `BusyBlockKey` and writes a
`FreeBlockValue` at `FreeBlockKey` (one `batch_write`), and the
in-memory `usage_bits` bitmap is derived from the records. But on
crash or restart, the in-memory state is lost and must be
reconstructed from the records on the disk-group's bound paxos data
group. Without recovery, diskdb cannot restart safely — it would lose
track of all allocations.

The design doc (§7) specifies **three complementary strategies**, each
in its role:

- **Strategy 1 — full scan rebuild** (on-demand, via `RebuildZoneBitmap`
  RPC/API): scan all live `BusyBlockKey`s for a zone and set those
  bits; all other offsets are free. No snapshot needed, no slot
  ordering needed — the busy record's existence is the indicator (a
  block is busy iff its `BusyBlockKey` exists, §3.4). Not in the
  common code flow; used for consistency checks or a full rebuild
  (e.g. after corruption, or when no `ZoneValue` snapshot exists), and
  as the §12 scanner's rebuild mechanism.
- **Strategy 2 — journal scan replay** (primary restart path): load
  the latest `ZoneValue` snapshot, then replay only the operations
  (Put/Delete) written after `snapshot_slot`, in slot order, and
  apply them to the snapshot bitmap. **Requires a `JournalScan`
  crow-kv RPC** (slot-range + key-prefix filter, returns ops in slot
  order) — the sole crow-kv extension diskdb needs (§1, §3.4). Fast
  because compaction (strategy 3) keeps the uncompacted record set
  small.
- **Strategy 3 — compaction** (ongoing maintenance): periodically (or
  when a zone's free-record count exceeds a threshold), merge free
  records into the `ZoneValue` bitmap (clear the freed bits), write a
  new `ZoneValue` snapshot, and delete the free records in one
  `batch_write`. **Only free records are deleted**; busy records for
  freed blocks were already deleted on free (§3.4), and busy records
  for live blocks are untouched. Keeps strategy 2's replay fast.

A key prefix scan returns records in **lexicographic key order** (=
`unit_offset` order), not slot order (§7 note). Strategy 1 works
without slot ordering (busy existence is the indicator). Strategy 2
requires the `JournalScan` extension to get slot-ordered replay — a
plain prefix scan cannot do slot-ordered replay.

The aioss reference has a simple recovery (load `ZoneRecord` from
metadb, rebuild in-memory) — but aioss writes the **full** `ZoneRecord`
on every allocate, so no replay is needed. CROW's record model (§3.4)
requires actual replay. This is new work with no direct aioss analog.

**Solution**: Implement crash recovery (strategies 1 + 2) and snapshot
compaction (strategy 3) — the durability safety net that makes diskdb
stateless-on-disk.

1. **`JournalScan` crow-kv extension** — add to crow-kv (the sole
   crow-kv extension diskdb needs, §1):
   - A new RPC `JournalScan(KvJournalScanRequest) returns
     (KvJournalScanResponse)` where `KvJournalOp = { key, value (empty
     for Delete), is_delete, slot }`. Returns ops in **slot order**
     within `[min_slot, max_slot]` whose key starts with `key_prefix`.
     This is the only way to get slot-ordered replay; the existing
     `scan` API returns key order, not slot order. Pagination via
     `limit` + `last_op_slot` (caller sends `min_slot =
     last_op_slot + 1` for the next page).
   - Scope: `lib/crow-kv/src/rpc/proto/kv.proto` (messages + rpc +
     `KV_ERROR_JOURNAL_SCAN_GC_GAP` error code),
     `lib/crow-kv/src/rpc/kv_service.rs` (gRPC handler),
     `lib/crow-kv/src/cluster/px_kv_store.rs` (core scan logic — WAL
     segment index iteration in slot order, KV payload decode,
     key-prefix filter), `lib/crow-kv-client/src/client.rs` (client
     method with transparent pagination). Diskdb calls it via
     `CrowkvClient::journal_scan`.
   - This is a crow-kv change; R73 depends on it landing in crow-kv.
     If `JournalScan` is not yet available, strategy 1 (full scan) is
     the fallback restart path (correct but slower for many zones).

2. **`RebuildZoneBitmap` RPC** — add to `DiskdbService`
   (`lib/crow-protocol/src/proto/diskdb_service.proto` +
   `diskdb_op.proto`). The current `DiskdbService` has `AllocateBlocks`,
   `FreeBlocks`, `QueryCapacityStats`, `GetDiskGroupInfo`, `GetDiskInfo`
   — `RebuildZoneBitmap` is new:
   - `rpc RebuildZoneBitmap(RebuildZoneBitmapRequest) returns
     (RebuildZoneBitmapResponse)` — on-demand strategy 1 full scan
     rebuild for one zone (or all zones on a disk). `zone_index =
     u32::MAX` means all zones on the disk. Triggers
     `RecoveryEngine::rebuild_zone_bitmap_full_scan()`. Returns
     `RebuildZoneBitmapResponse { rebuilt_zone_count, total_busy_units,
     total_free_units }` for operator confirmation. Used by operators
     for consistency checks and by the §12 scanner (R75) as its
     rebuild mechanism.

3. **Recovery engine** — create
   `app/crow-diskdb/src/recovery.rs` (new module; registered in
   `lib.rs`):
   - `RecoveryEngine` — owns a `DataGroupClient` (from R72). Disk
     metadata (`DiskValue`s) is passed in by the caller (the sync loop
     already fetches it from group 0 via `HardwareClient`); the
     recovery engine does not need a group-0 client. Runs on startup
     and on ownership transfer (when a new disk-group is assigned to
     this instance).
   - `recover_node(dg_id: DiskGroupId, node_id: NodeId, rack_id:
     RackId, bind: (u64, u64), disks: &[(DiskId, DiskValue)]) ->
     Result<Arc<Node>>`:
     a. Create an empty `Node` via `Node::new(dg_id, node_id,
        rack_id)`; set `bind` from the bind map.
     b. For each `(disk_id, disk_value)` in `disks`:
        - Create a `ZoneDisk` via `ZoneDisk::new(disk_id, dg_id,
          node_id, rack_id, *disk_value)`.
        - For each zone index (0 to `disk_value.zone_count`):
          - Compute `unit_capacity` (last zone rounded down to a
            multiple of 64, matching `sync.rs::disk_add_init`).
          - Call `recover_zone(bind, disk_id, zone_idx,
            unit_capacity)`.
          - Add the recovered zone to the disk via `disk.add_zone`.
        - Call `disk.rebuild_active_zones(zone_rotate_count)`.
        - Add disk to node via `node.add_disk`.
     c. Return the reconstructed `Node` (wrapped in `Arc`).
   - `recover_zone(bind, disk_id, zone_idx, unit_capacity) ->
     Result<Zone>` — **strategy 2 (journal scan replay)**, the primary
     restart path:
     a. **Load the latest `ZoneValue`**: via
        `DataGroupClient::get_zone_value(bind, disk_id, zone_idx)` (a
        dedicated point-get on `ZoneKey` — 1 round-trip; avoids the 2
        wasted scans of `read_zone_records` when only the snapshot is
        needed). If present, deserialize and verify CRC32 via
        `ZoneValueExt::verify_checksum` (R70). If CRC fails, log error
        and fall back to strategy 1 (full scan rebuild) for this zone
        — conservative.
     b. **Initialize replay state**: if a snapshot exists, start with
        `usage_bits = snapshot.usage_bitmap` (restored),
        `used_count` = popcount of the bitmap, `snapshot_slot =
        snapshot.snapshot_slot`. If no snapshot, start with an empty
        bitmap and `snapshot_slot = 0` (then strategy 2 with
        `min_slot = 0` replays everything; or fall back to strategy 1).
     c. **Journal scan**: two narrow scans (chosen over one broad
        scan — less data transferred, merge-by-slot is trivial since
        both lists are slot-sorted):
        - `DataGroupClient::journal_scan_busy(bind, min_slot =
          snapshot_slot + 1, max_slot = 0 (MAX), disk_id, zone_idx)`
          — wraps `CrowkvClient::journal_scan` with
          `BusyBlockKey::prefix_for_zone`.
        - `DataGroupClient::journal_scan_free(bind, ...)` — wraps with
          `FreeBlockKey::prefix_for_zone`.
        Merge the two op lists by slot (merge-sort). If `JournalScan`
        returns `KV_ERROR_JOURNAL_SCAN_GC_GAP` (asked for already-GC'd
        slots), fall back to strategy 1 for this zone.
     d. **Apply ops in slot order** (merge the two scans by slot):
        - `Put BusyBlockKey { ..., unit_offset }` with
          `BusyBlockValue { unit_count, ... }` →
          `range_set(unit_offset, unit_count)`; `used_count +=
          unit_count`.
        - `Delete BusyBlockKey { ..., unit_offset }` → need
          `unit_count` to clear the right range. The Delete of
          `BusyBlockKey` and Put of `FreeBlockKey` are in the same
          `batch_write` (same slot). At each slot, decode the full
          batch: if the batch contains both `Delete BusyBlockKey` and
          `Put FreeBlockKey` for the same `unit_offset`, read
          `unit_count` from the `FreeBlockValue` for the
          `range_clear`. (Normal free case.) If a `Delete
          BusyBlockKey` appears without a matching `Put FreeBlockKey`
          (shouldn't happen in normal operation), fall back to
          tracking `unit_count` from the preceding Put, or log a
          warning and skip (the bit stays set — conservative, better
          to leak than to free incorrectly).
        - `Put FreeBlockKey` / `Delete FreeBlockKey` → no-op for state
          (free records are audit/carrier markers, not state
          indicators, §3.4). The bit is governed by `BusyBlockKey`
          presence.
        - `Put ZoneKey` (a newer compaction snapshot written after the
          one we loaded) → should not appear after `snapshot_slot` if
          we loaded the latest snapshot; if it does, restart replay
          from the newer `snapshot_slot` (defensive — reload the newer
          `ZoneValue`, reset the bitmap, restart the journal scan from
          the newer `snapshot_slot + 1`).
     e. **Build the recovered `Zone`**: set `usage_bits`, `used_count`,
        `zone_state` (Healthy by default; R76's health probe may
        update it later), `snapshot_slot`. Reset `last_pos_64 = 0`
        (the rotating cursor restarts; it spreads load on its own).
        `derived_alloc_state()` reflects the rebuilt `used_count`.
     f. Return the recovered `Zone`.
   - `rebuild_zone_bitmap_full_scan(bind, disk_id, zone_idx,
     unit_capacity) -> Result<(Zone, ZoneStats)>` — **strategy 1 (full
     scan rebuild)**, on-demand via the `RebuildZoneBitmap` RPC and the
     §12 scanner:
     a. `DataGroupClient::read_zone_records(bind, disk_id, zone_idx)`
        (returns `ZoneRecords { zone_value, busy, free }`); use the
        `busy` field. Key order = `unit_offset` order. No snapshot
        needed, no slot ordering needed.
     b. Start with an empty bitmap. For each `BusyRecord` in `busy`:
        `range_set(unit_offset, unit_count)`; `used_count +=
        unit_count`.
     c. (`FreeBlockKey`s are not scanned — they carry `previous_owner`
        audit info but are not state indicators, §3.4.)
     d. Build the `Zone` as in `recover_zone` step e. Optionally write
        a fresh `ZoneValue` snapshot (with `snapshot_slot =
        current_max_slot` from the data group's applied frontier) so
        the next restart can use strategy 2.
     e. Return the rebuilt `Zone` + derived stats
        (`ZoneStats { capacity_units, used_units, free_units }`).
   - **Edge cases**:
     - No snapshot, no records: fresh zone, empty bitmap,
       `used_count = 0`.
     - Snapshot exists, no records after it: zone is exactly the
       snapshot state.
     - Snapshot CRC fails: log error, fall back to strategy 1 (full
       scan) for this zone.
     - `JournalScan` not available (crow-kv extension not landed): fall
       back to strategy 1 for all zones (correct but slower).
     - `JournalScan` returns `KV_ERROR_JOURNAL_SCAN_GC_GAP`: fall back
       to strategy 1 for the affected zone.
     - Journal scan is truncated/paginated: the client method
       transparently pages until complete.

4. **Snapshot compaction** — create
   `app/crow-diskdb/src/recovery/compaction.rs` (sub-module of the
   `recovery` module, following the `node.rs` + `node/container.rs`
   pattern; strategy 3):
   - `CompactionEngine` — owns a `DataGroupClient`. Runs as a background
     task (spawned by the server).
   - `compaction_loop(node_container, kv: Arc<DataGroupClient>,
     config: CompactionConfig)`:
     a. `sleep(config.compaction_cadence)` (default periodic interval,
        §16).
     b. For each owned node (disk-group) in `NodeContainer`:
        - For each disk, for each zone:
          - Check if compaction is needed: the zone's
            `uncompacted_free_record_count` gauge (§11) exceeds
            `config.snapshot_compaction_threshold` (record count,
            §16). If not, skip.
          - `compact_zone(bind, disk_id, zone)`.
     c. Repeat. Cadence vs. threshold: compaction triggers on
        **either** the periodic cadence **or** the per-zone threshold
        — whichever fires first for a given zone.
   - `compact_zone(bind, disk_id, zone: &Zone) -> Result<()>`:
     a. **Scan free records** for the zone via
        `DataGroupClient::read_zone_records(bind, disk_id, zone_idx)`
        (use the `free` field), or a dedicated
        `scan_free_records` helper that calls only the free-prefix
        scan. The free records of one zone are contiguous in the
        crow-tree page, so this is efficient (§7). Record
        `compaction.scan_free.latency`.
     b. **Merge free records into the in-memory `ZoneValue` bitmap**:
        for each `FreeRecord`, clear the freed bits
        (`range_clear(unit_offset, unit_count)`). Record
        `compaction.merge_bitmap.latency`. (Busy records for freed
        blocks are already gone — deleted on free; busy records for
        live blocks stay set.)
     c. **Determine `snapshot_slot`**: `current_max_slot` = the data
        group's applied frontier at the time of compaction, via
        `DataGroupClient::get_applied_slot(bind)` (a lightweight
        `Scan` with `read_mode = Linearizable` reading `read_slot`
        from the response, or a dedicated crow-kv method).
     d. **Build the new `ZoneValue`**: `usage_bitmap` (the merged
        bitmap, serialized from `usage_bits`), `snapshot_slot =
        current_max_slot`, `crc32` via
        `ZoneValueExt::compute_checksum` (R70).
     e. **Write the new `ZoneValue`**: `DataGroupClient::put_zone(bind,
        disk_id, zone_idx, &zone_value)` (overwrites the old
        snapshot). Then **delete the free records** in one
        `batch_write` via `DataGroupClient::delete_free_records_batch`
        (already defined in R72). Record `compaction.kv_persist.
        latency`. One `batch_write` per data group; batch by `disk_id`
        prefix so one scan covers all zones on a disk (if compacting
        multiple zones on the same disk, collect all their free
        records and delete in one batch). The key invariant: the
        `ZoneValue` snapshot is written **before** the free records
        are deleted.
     f. **Update zone's `snapshot_slot`**: store `current_max_slot` in
        the zone's in-memory `snapshot_slot: AtomicU64`.
     g. **Decrement `uncompacted_free_record_count`** by the number of
        free records deleted.
     h. Increment `compaction.count` (or `compaction.error.count` on
        failure) (§11).
   - **Compaction trigger**: besides the periodic loop, compaction can
     be triggered on demand (e.g. before ownership transfer, or when a
     zone's free-record backlog is large). Expose
     `compact_zone_now()` for manual triggering.
   - **Crash safety of compaction**: if diskdb crashes after writing
     the new `ZoneValue` but before deleting the free records, the
     free records are orphaned but harmless — on strategy 2 replay,
     the `Put FreeBlockKey` ops are no-ops for state (the bit is
     already clear in the snapshot); strategy 1 ignores free records
     entirely. The next compaction cycle re-scans and deletes them. If
     diskdb crashes after deleting the free records, the snapshot is
     the source of truth. Both orders are safe — no two-phase commit
     needed.

5. **Recovery on startup** — integrate into
   `app/crow-diskdb/src/main.rs`. The current `main.rs` runs a blocking
   `sync_once` before starting the gRPC server, but does not yet run
   recovery — the sync loop's `disk_add_init` writes baseline
   `ZoneValue` records (empty bitmap, `snapshot_slot = 0`) but does
   not replay the journal. R73 adds recovery after the initial sync:
   - After the initial sync fetches owned disk-groups and their disks
     from group 0, run `RecoveryEngine::recover_node()` for each owned
     disk-group (strategy 2, with strategy 1 fallback).
   - This reconstructs in-memory zone state from the records before
     the server starts accepting allocate/free RPCs.
   - The server must not serve RPCs until recovery is complete for all
     owned disk-groups. The current `main.rs` already blocks on
     `sync_once` before starting gRPC; recovery runs in the same
     blocking phase, after `sync_once`.

6. **Recovery on ownership transfer** — integrate into
   `app/crow-diskdb/src/sync.rs`. The current sync loop's `sync_once`
   detects new disk-groups and calls `disk_add_init` (which writes
   baseline `ZoneValue` records but does not replay the journal). R73
   keeps `disk_add_init` for fresh disks and adds recovery for existing
   disks:
   - When `sync_once()` detects a new disk-group assigned to this
     instance (not previously owned), first check whether `ZoneValue`
     snapshots already exist for its zones (via `get_zone_value`). If
     snapshots exist (the disk-group was previously owned by another
     instance), run `RecoveryEngine::recover_node()` to replay the
     journal from `snapshot_slot`. If no snapshots exist (truly fresh
     disks), keep the existing `disk_add_init` path (writes baseline
     `ZoneValue` records with empty bitmap, `snapshot_slot = 0`).
     The `SyncLoop` needs a handle to the `RecoveryEngine` (add a
     `with_recovery_engine` builder, mirroring
     `with_data_group_client`).

7. **Zone struct extension** — update
   `app/crow-diskdb/src/zone.rs`. The `Zone` struct already has (from
   R72):
   - `snapshot_slot: AtomicU64` — the slot of the last compacted
     `ZoneValue` snapshot. Initialized to 0 on fresh zone, set by
     recovery and compaction.
   - `uncompacted_free_record_count: AtomicU32` — gauge for the
     compaction backlog (§11); incremented on free (in `Zone::free`),
     decremented when compaction deletes the free record.
   R73 adds:
   - `to_zone_value() -> ZoneValue` — snapshot current `usage_bits` +
     `snapshot_slot` + `crc32` (via `ZoneValueExt::compute_checksum`)
     for compaction.
   - `from_zone_value(value: &ZoneValue, unit_capacity: u32) -> Zone`
     — rebuild from a snapshot (used by recovery when no records exist
     after the snapshot). Verifies CRC via
     `ZoneValueExt::verify_checksum` before use.

**Scope** (expected changed files):
- `lib/crow-kv/src/rpc/proto/kv.proto` — add `KvJournalScanRequest` /
  `KvJournalScanResponse` / `KvJournalOp` messages, the `JournalScan`
  rpc, and `KV_ERROR_JOURNAL_SCAN_GC_GAP` to `KvErrorCode`.
- `lib/crow-kv/src/rpc/kv_service.rs` — `JournalScan` gRPC handler
  (mirrors the existing `scan` handler's leader-forward + read-mode
  logic).
- `lib/crow-kv/src/cluster/px_kv_store.rs` — core `journal_scan` logic
  (WAL segment index iteration in slot order, KV payload decode,
  key-prefix filter).
- `lib/crow-kv-client/src/client.rs` — `CrowkvClient::journal_scan`
  client method (transparent pagination, mirrors `scan`).
- `lib/crow-protocol/src/proto/diskdb_service.proto` — add
  `RebuildZoneBitmap` RPC.
- `lib/crow-protocol/src/proto/diskdb_op.proto` — add
  `RebuildZoneBitmapRequest` / `RebuildZoneBitmapResponse` messages.
- `app/crow-diskdb/src/recovery.rs` — new module: `RecoveryEngine`
  with `recover_node`, `recover_zone` (strategy 2), and
  `rebuild_zone_bitmap_full_scan` (strategy 1).
- `app/crow-diskdb/src/recovery/compaction.rs` — new sub-module:
  `CompactionEngine` with `compaction_loop`, `compact_zone` (strategy
  3).
- `app/crow-diskdb/src/zone.rs` — add `to_zone_value()`,
  `from_zone_value()` (`snapshot_slot` and
  `uncompacted_free_record_count` already exist from R72).
- `app/crow-diskdb/src/persistence.rs` — add `get_zone_value`,
  `get_applied_slot`, `journal_scan_busy`, `journal_scan_free`
  (`put_zone`, `read_zone_records`, `delete_free_records_batch`
  already exist from R72).
- `app/crow-diskdb/src/grpc.rs` — add the `RebuildZoneBitmap` handler
  to `DiskdbService` (delegates to
  `RecoveryEngine::rebuild_zone_bitmap_full_scan`). The current
  `DiskdbService` has no `RebuildZoneBitmap` method.
- `app/crow-diskdb/src/lib.rs` — add `recovery` module.
- `app/crow-diskdb/src/main.rs` — run recovery on startup (after
  `sync_once`), spawn compaction loop.
- `app/crow-diskdb/src/sync.rs` — trigger recovery on ownership
  transfer via `with_recovery_engine` builder; keep `disk_add_init`
  for fresh disks, run recovery when `ZoneValue` snapshots already
  exist.
- `app/crow-diskdb/src/config.rs` — rename
  `snapshot_interval_secs` → `compaction_cadence_secs`,
  `snapshot_journal_threshold` → `snapshot_compaction_threshold`; add
  `recovery_concurrency`. Update `validate()`.

**Complexity**: High. Journal scan replay (strategy 2) is new — the
aioss reference has no analog (it writes the full `ZoneRecord` on
every allocate). The replay algorithm is specified in the design doc
(§7) and requires the `JournalScan` crow-kv extension (the sole
crow-kv extension diskdb needs, §1) to get slot-ordered replay — a
plain prefix scan returns key order, not slot order. The main
challenges are: (1) implementing the `JournalScan` crow-kv extension
(server-side slot-ordered scan with key-prefix filter), (2) applying
ops in slot order with the delete-on-free record model (Put/Delete
`BusyBlockKey` governs state; `FreeBlockKey` ops are no-ops for
state), (3) compaction crash safety (write `ZoneValue` before deleting
free records), (4) falling back to strategy 1 (full scan) when
`JournalScan` is unavailable or a snapshot CRC fails. The CRC32
verification (R70) catches `ZoneValue` corruption. Compaction deletes
**only free records** — busy records for freed blocks were already
deleted on free (§3.4).

**Dependencies**: R70 (types, key layout, CRC, `ZoneValueExt`), R71
(`HardwareClient`, `ServiceRegistryClient`, `NodeContainer`, sync
loop), R72 (`DataGroupClient`, `Zone` allocator, bitmap,
`put_zone`, `read_zone_records`, `delete_free_records_batch`, record
model). Depends on the `JournalScan` crow-kv extension landing in
crow-kv (this requirement adds it). No dependency on R74–R77. R75
(scanner) depends on this requirement's strategy 1
(`rebuild_zone_bitmap_full_scan`) and the `RebuildZoneBitmap` RPC.

**Acceptance**:
- `recover_zone()` with no snapshot and no records returns a fresh
  zone (empty bitmap, `used_count = 0`). Unit test.
- `recover_zone()` with a `ZoneValue` snapshot and no records after it
  returns the snapshot state. Unit test: write a snapshot, recover,
  verify `usage_bits` and `used_count` match.
- `recover_zone()` with a snapshot and records after it correctly
  applies the ops in slot order. Unit test: write snapshot at slot 10,
  Put `BusyBlockKey` at slots 11–15, Delete `BusyBlockKey` + Put
  `FreeBlockKey` at slot 16, recover, verify bitmap reflects all
  operations (busy bits set for 11–15, cleared for the freed one).
- `recover_zone()` with no snapshot and records replays from slot 0
  (or falls back to strategy 1). Unit test.
- `recover_zone()` with a corrupted snapshot (CRC fail) logs error and
  falls back to strategy 1 (full scan). Unit test.
- `recover_zone()` with `KV_ERROR_JOURNAL_SCAN_GC_GAP` falls back to
  strategy 1 for the affected zone. Unit test.
- `rebuild_zone_bitmap_full_scan()` (strategy 1) scans `BusyBlockKey`s
  and rebuilds the bitmap; ignores `FreeBlockKey`s. Unit test: write
  busy + free records, rebuild, verify bitmap = busy keys only.
- `rebuild_zone_bitmap_full_scan()` writes a fresh `ZoneValue`
  snapshot after rebuild so the next restart can use strategy 2. Unit
  test.
- `recover_node()` reconstructs all disks and zones for a disk-group.
  Integration test with in-process mock crow-kv: allocate/free blocks,
  restart diskdb (re-run recovery), verify in-memory state matches the
  records.
- `JournalScan` crow-kv RPC returns ops in slot order within a
  key-prefix filter. Unit/integration test in crow-kv. Verify slot
  ordering, prefix filtering, pagination (`limit` + `last_op_slot`).
- `compact_zone()` (strategy 3) merges free records into a new
  `ZoneValue`, writes it, and deletes only the free records. Unit
  test: create a zone with busy + free records, compact, verify the
  new `ZoneValue` has the freed bits cleared, the free records are
  deleted, and the busy records for live blocks are **not** deleted.
- Compaction crash safety: if crash after `ZoneValue` write but before
  free-record delete, replay still correct (orphaned free records are
  no-ops for state). Unit test. If crash after free-record delete,
  snapshot is the source of truth. Unit test.
- `Zone::to_zone_value` / `from_zone_value` round-trip — serialize +
  deserialize, verify bitmap + `snapshot_slot` + `crc32` match. Unit
  test.
- `RebuildZoneBitmap` gRPC handler triggers strategy 1 and returns
  derived stats. Integration test.
- Server startup gates gRPC on recovery completion. Integration test:
  server does not accept RPCs until recovery finishes.
- Ownership transfer triggers recovery for newly assigned disk-groups.
  Unit tests (mock data-group, recovery engine in isolation):
  - Transfer with only busy records (no snapshot): A allocates 5,
    transfers to B → B recovers 5 bits. B's bitmap matches A's
    records, not A's stale in-memory state.
  - Transfer after allocate + free (busy + free records, no
    compaction): A allocates 5, frees 2, transfers to B → B recovers
    3 live busy bits (2 freed bits clear).
  - Transfer after compaction (snapshot + few records): A compacts
    (snapshot at slot S, 6 bits), transfers to B → B loads snapshot,
    journal-scan from S+1 is empty → B recovers 6 bits. Fast (no
    replay).
  - Transfer after compaction + post-compaction allocates: A compacts
    (6 bits at slot S), allocates 3 more (slots S+1..S+3), transfers
    to B → B loads snapshot (6 bits), replays 3 Put ops → 9 bits.
  - Transfer after compaction + post-compaction free: A compacts (6
    bits at slot S), frees 1 (slot S+1), transfers to B → B loads
    snapshot (6 bits), replays free op → 5 bits.
  - Transfer with no records (fresh disk-group): no `ZoneValue`, no
    records → `disk_add_init` path (not recovery). B's bitmap empty.
  - Transfer back to original owner (A → B → A): A allocates 5,
    transfers to B (recovers 5), B allocates 3 (8 total), transfers
    back to A → A discards stale state, recovers 8 bits from records.
  - Transfer with corrupted snapshot: A compacts, corrupts `crc32`,
    transfers to B → B's CRC check fails, falls back to strategy 1
    (full scan).
- Ownership transfer E2E test (real `KvCluster` harness, two in-process
  diskdb instances sharing one kv cluster):
  - Phase 1: instance A `sync_once` → `disk_add_init` → allocate 5,
    free 2 (3 live busy records on group 1).
  - Phase 2: `hw.set_owner` → instance B. B `sync_once` → detects
    disk-group → `get_zone_value` finds snapshots → `recover_node`.
  - Phase 3: verify B's recovered bitmap matches records (3 bits, not
    A's stale state). Verify `snapshot_slot` + `used_count`.
  - Phase 4: B allocates 3 more, frees 1 → verify records on group 1.
    Recovery did not corrupt the allocator.
  - Phase 5: transfer back to A (A → B → A). A discards stale
    `NodeContainer`, recovers from records (5 live busy: 3 old + 3
    from B − 1 freed by B). Proves recovery is idempotent and
    stateless-on-disk.
- Config validation — renamed `CompactionConfig` fields +
  `recovery_concurrency`. Unit test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass (`pixi run cargo test -p crow-diskdb` and
  `pixi run cargo test -p crow-kv` for the `JournalScan` handler).
