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
   - A new RPC `JournalScan(store_id, group_id, min_slot, max_slot,
     key_prefix) -> stream<JournalOp>` where `JournalOp = { key, value
     (Some for Put, None for Delete), slot }`. Returns ops in **slot
     order** within `[min_slot, max_slot]` whose key starts with
     `key_prefix`. This is the only way to get slot-ordered replay;
     the existing `scan` API returns key order, not slot order.
   - Scope: `lib/crow-kv/` (server-side handler reading the WAL/journal
     in slot order with a key-prefix filter) + `lib/crow-kv-client/`
     (client method). Diskdb calls it via `CrowkvClient::journal_scan`.
   - This is a crow-kv change; R73 depends on it landing in crow-kv.
     If `JournalScan` is not yet available, strategy 1 (full scan) is
     the fallback restart path (correct but slower for many zones).

2. **`RebuildZoneBitmap` RPC** — add to `DiskdbService`
   (`lib/crow-protocol/src/proto/diskdb_service.proto`):
   - `rpc RebuildZoneBitmap(RebuildZoneBitmapRequest) returns
     (RebuildZoneBitmapResponse)` — on-demand strategy 1 full scan
     rebuild for one zone (or all zones on a disk). Triggers
     `RecoveryEngine::rebuild_zone_bitmap_full_scan()`. Returns the
     zone's derived stats (capacity/used/free) for operator
     confirmation. Used by operators for consistency checks and by the
     §12 scanner (R75) as its rebuild mechanism.

3. **Recovery engine** — create
   `app/crow-diskdb/src/recovery/mod.rs`:
   - `RecoveryEngine` — owns a `DataGroupClient` (from R72) and a
     `SysdataClient` (from R71). Runs on startup and on ownership
     transfer (when a new disk-group is assigned to this instance).
   - `recover_node(dg_id: &DiskGroupId, bind: (u64, u64), disks:
     &[DiskMeta]) -> Result<Node>`:
     a. Create an empty `Node` with the disk-group's binding info.
     b. For each disk in `DiskMeta`:
        - Create a `ZoneDisk` (from R71).
        - For each zone index (0 to `disk.zone_count`):
          - Call `recover_zone(dg_id, bind, disk_id, zone_idx)`.
          - Add the recovered zone to the disk.
        - Call `disk.rebuild_active_zones()`.
        - Add disk to node.
     c. Return the reconstructed `Node`.
   - `recover_zone(dg_id, bind, disk_id, zone_idx) -> Result<Zone>` —
     **strategy 2 (journal scan replay)**, the primary restart path:
     a. **Load the latest `ZoneValue`**: `get` from
        `ZoneKey { disk_id, zone_idx }` on the bound data group. If
        present, deserialize and verify CRC32 (`ZoneValueExt::verify`,
        R70). If CRC fails, log error and fall back to strategy 1
        (full scan rebuild) for this zone — conservative.
     b. **Initialize replay state**: if a snapshot exists, start with
        `usage_bits = snapshot.usage_bitmap` (restored),
        `used_count` = popcount of the bitmap, `snapshot_slot =
        snapshot.snapshot_slot`. If no snapshot, start with an empty
        bitmap and `snapshot_slot = 0` (then strategy 2 with
        `min_slot = 0` replays everything; or fall back to strategy 1).
     c. **Journal scan**: `await CrowkvClient::journal_scan(store_id,
        group_id, min_slot = snapshot_slot + 1, max_slot = MAX,
        key_prefix = BusyBlockKey::prefix_for_zone(disk_id, zone_idx)
        ∪ FreeBlockKey::prefix_for_zone(...))`. Returns ops in slot
        order. (One scan covering both busy and free key tags — the
        `JournalScan` key-prefix filter matches both, or issue two
        scans and merge by slot.)
     d. **Apply ops in slot order**:
        - `Put BusyBlockKey { ..., unit_offset, unit_count }` →
          `range_set(unit_offset, unit_count)`; `used_count +=
          unit_count`.
        - `Delete BusyBlockKey { ..., unit_offset, unit_count }` →
          `range_clear(unit_offset, unit_count)`; `used_count -=
          unit_count`. (A free op = Delete `BusyBlockKey` + Put
          `FreeBlockKey` at the same slot; the Put `FreeBlockKey` does
          not affect state — the bit is governed by `BusyBlockKey`
          presence, §3.4.)
        - `Put FreeBlockKey` / `Delete FreeBlockKey` → no-op for state
          (free records are audit/carrier markers, not state
          indicators, §3.4).
        - `Put ZoneKey` (a newer compaction snapshot) → should not
          appear after `snapshot_slot` if we loaded the latest
          snapshot; if it does, restart replay from the newer
          `snapshot_slot` (defensive).
     e. **Build the recovered `Zone`**: set `usage_bits`, `used_count`,
        `zone_state` (Healthy by default; R76's health probe may
        update it later), `snapshot_slot`. Reset `last_pos_64 = 0`
        (the rotating cursor restarts; it spreads load on its own).
        `derived_alloc_state()` reflects the rebuilt `used_count`.
     f. Return the recovered `Zone`.
   - `rebuild_zone_bitmap_full_scan(dg_id, bind, disk_id, zone_idx) ->
     Result<Zone>` — **strategy 1 (full scan rebuild)**, on-demand via
     the `RebuildZoneBitmap` RPC and the §12 scanner:
     a. `scan` with prefix `BusyBlockKey::prefix_for_zone(disk_id,
        zone_idx)` (key order = `unit_offset` order). No snapshot
        needed, no slot ordering needed.
     b. Start with an empty bitmap. For each `BusyBlockKey` returned,
        `range_set(unit_offset, unit_count)`; `used_count +=
        unit_count`.
     c. (`FreeBlockKey`s are not scanned — they carry `previous_owner`
        audit info but are not state indicators, §3.4.)
     d. Build the `Zone` as in `recover_zone` step e. Optionally write
        a fresh `ZoneValue` snapshot (with `snapshot_slot =
        current_max_slot`) so the next restart can use strategy 2.
     e. Return the rebuilt `Zone` + derived stats.
   - **Edge cases**:
     - No snapshot, no records: fresh zone, empty bitmap.
     - Snapshot exists, no records after it: zone is exactly the
       snapshot state.
     - Snapshot CRC fails: log error, fall back to strategy 1 (full
       scan) for this zone.
     - `JournalScan` not available (crow-kv extension not landed): fall
       back to strategy 1 for all zones (correct but slower).
     - Journal scan is truncated/paginated: the client method
       transparently pages until complete.

4. **Snapshot compaction** — create
   `app/crow-diskdb/src/recovery/compaction.rs` (strategy 3):
   - `CompactionEngine` — owns a `DataGroupClient`. Runs as a background
     task (spawned by the server).
   - `compaction_loop(node_container, journal, config)`:
     a. `sleep(compaction_cadence)` (default periodic interval, §16).
     b. For each owned node (disk-group):
        - For each disk, for each zone:
          - Check if compaction is needed: the zone's
            `uncompacted_free_record_count` gauge (§11) exceeds
            `snapshot_compaction_threshold` (record count, §16). If
            not, skip.
          - `compact_zone(dg_id, bind, zone)`.
     c. Repeat.
   - `compact_zone(dg_id, bind, zone: &Zone) -> Result<()>`:
     a. **Scan free records** for the zone by key prefix
        (`FreeBlockKey::prefix_for_zone(disk_id, zone_idx)`). The free
        records of one zone are contiguous in the crow-tree page, so
        this is efficient (§7). Record `compaction.scan_free.latency`.
     b. **Merge free records into the in-memory `ZoneValue` bitmap**:
        for each `FreeBlockKey`, clear the freed bits
        (`range_clear(unit_offset, unit_count)`). Record
        `compaction.merge_bitmap.latency`. (Busy records for freed
        blocks are already gone — deleted on free; busy records for
        live blocks stay set.)
     c. **Determine `snapshot_slot`**: `current_max_slot` = the slot of
        the last op included in this compaction (from the journal scan
        frontier / `zone.next_journal_slot` equivalent).
     d. **Build the new `ZoneValue`**: `usage_bitmap` (the merged
        bitmap), `snapshot_slot = current_max_slot`, `crc32 =
        compute_checksum(usage_bitmap)` (R70).
     e. **Write the new `ZoneValue`**: `put` to
        `ZoneKey { disk_id, zone_idx }` (overwrites the old snapshot).
        Then **delete the free records** in one `batch_write`
        (`delete_free_records_batch`, defined in R72). Record
        `compaction.kv_persist.latency`. One `batch_write` per data
        group; batch by `disk_id` prefix so one scan covers all zones
        on a disk.
     f. **Update zone's `snapshot_slot`**: store `current_max_slot` in
        the zone's in-memory `snapshot_slot: AtomicU64`.
     g. Increment `compaction.count` (or `compaction.error.count` on
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
     entirely. If diskdb crashes after deleting the free records, the
     snapshot is the source of truth. Both orders are safe.

5. **Recovery on startup** — integrate into
   `app/crow-diskdb/src/main.rs`:
   - After the initial sync (R71) fetches owned disk-groups and their
     disks from group 0, run `RecoveryEngine::recover_node()` for
     each owned disk-group (strategy 2, with strategy 1 fallback).
   - This reconstructs in-memory zone state from the records before
     the server starts accepting allocate/free RPCs.
   - The server must not serve RPCs until recovery is complete for all
     owned disk-groups. Use a `Barrier` or `OnceCell` to gate the gRPC
     server startup.

6. **Recovery on ownership transfer** — integrate into the sync loop
   (R71):
   - When `sync_once()` detects a new disk-group assigned to this
     instance (not previously owned), run
     `RecoveryEngine::recover_node()` for it before adding it to the
     `NodeContainer` (in-memory state is discarded; the records
     persist in the data group for the next owner).

7. **Zone struct extension** — update `app/crow-diskdb/src/zone/mod.rs`
   (from R72):
   - Add `snapshot_slot: AtomicU64` — the slot of the last compacted
     `ZoneValue` snapshot. Initialized to 0 on fresh zone, set by
     recovery and compaction.
   - Add `uncompacted_free_record_count: AtomicU32` — gauge for the
     compaction backlog (§11); incremented on free, decremented when
     compaction deletes the free record.
   - Add `to_zone_value() -> ZoneValue` — snapshot current `usage_bits`
     + `snapshot_slot` + `crc32` for compaction.
   - Add `from_zone_value(value: ZoneValue) -> Zone` — rebuild from a
     snapshot (used by recovery when no records exist after the
     snapshot).

**Scope** (expected changed files):
- `lib/crow-kv/` + `lib/crow-kv-client/` — `JournalScan` RPC (server
  handler + client method). The sole crow-kv extension diskdb needs.
- `lib/crow-protocol/src/proto/diskdb_service.proto` — add
  `RebuildZoneBitmap` RPC + request/response messages.
- `app/crow-diskdb/src/recovery/mod.rs` — `RecoveryEngine` with
  `recover_node`, `recover_zone` (strategy 2), and
  `rebuild_zone_bitmap_full_scan` (strategy 1).
- `app/crow-diskdb/src/recovery/compaction.rs` — `CompactionEngine`
  with `compaction_loop`, `compact_zone` (strategy 3).
- `app/crow-diskdb/src/zone/mod.rs` — add `snapshot_slot`,
  `uncompacted_free_record_count`, `to_zone_value()`,
  `from_zone_value()`.
- `app/crow-diskdb/src/persistence/mod.rs` — `read_zone_records()`
  (prefix scan of `BusyBlockKey`/`FreeBlockKey`/`ZoneKey`),
  `delete_free_records_batch()` (already defined in R72, used by
  compaction).
- `app/crow-diskdb/src/grpc/service.rs` — implement the
  `RebuildZoneBitmap` handler (strategy 1, was stubbed `Unimplemented`
  in R72).
- `app/crow-diskdb/src/lib.rs` — add `recovery` module.
- `app/crow-diskdb/src/main.rs` — run recovery on startup, spawn
  compaction loop, gate gRPC server on recovery completion.
- `app/crow-diskdb/src/sync/mod.rs` (from R71) — trigger recovery on
  ownership transfer.
- `app/crow-diskdb/src/config.rs` — `snapshot_compaction_threshold`
  (record count), `compaction_cadence` (periodic interval) per §16.

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
(SysdataClient, NodeContainer, sync loop), R72 (DataGroupClient, Zone
allocator, bitmap, `delete_free_records_batch`, record model). Depends
on the `JournalScan` crow-kv extension landing in crow-kv (this
requirement adds it). No dependency on R74–R77. R75 (scanner) depends
on this requirement's strategy 1 (`rebuild_zone_bitmap_full_scan`) and
the `RebuildZoneBitmap` RPC.

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
- `rebuild_zone_bitmap_full_scan()` (strategy 1) scans `BusyBlockKey`s
  and rebuilds the bitmap; ignores `FreeBlockKey`s. Unit test: write
  busy + free records, rebuild, verify bitmap = busy keys only.
- `recover_node()` reconstructs all disks and zones for a disk-group.
  Integration test with in-process crow-kv: allocate/free blocks,
  restart diskdb (re-run recovery), verify in-memory state matches the
  records.
- `JournalScan` crow-kv RPC returns ops in slot order within a
  key-prefix filter. Unit/integration test in crow-kv.
- `compact_zone()` (strategy 3) merges free records into a new
  `ZoneValue`, writes it, and deletes only the free records. Unit
  test: create a zone with busy + free records, compact, verify the
  new `ZoneValue` has the freed bits cleared, the free records are
  deleted, and the busy records for live blocks are **not** deleted.
- Compaction crash safety: if crash after `ZoneValue` write but before
  free-record delete, replay still correct (orphaned free records are
  no-ops for state). Unit test.
- `RebuildZoneBitmap` gRPC handler triggers strategy 1 and returns
  derived stats. Integration test.
- Server startup gates gRPC on recovery completion. Integration test:
  server does not accept RPCs until recovery finishes.
- Ownership transfer triggers recovery for newly assigned disk-groups.
  Integration test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.
