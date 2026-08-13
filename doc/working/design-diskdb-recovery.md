<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Crash Recovery + Snapshot Compaction Design (R73)

Working design draft for the diskdb crash recovery and snapshot
compaction implementation. Covers the `JournalScan` crow-kv extension,
the `RebuildZoneBitmap` RPC, the recovery engine (strategies 1 + 2),
and the compaction engine (strategy 3). Folded into
`doc/design/diskdb/design-crow-diskdb.md` and deleted after merge.

Root design: `doc/design/diskdb/design-crow-diskdb.md` (§1, §3.4, §7,
§11, §16). R71 (sync loop, `HardwareClient`, `ServiceRegistryClient`,
`NodeContainer`) and R72 (`DataGroupClient`, `Zone` allocator, record
persistence, two-phase allocate/free) are already landed in
`app/crow-diskdb/src/` — this doc references the actual code paths.
This doc covers R73 implementation details — the crow-kv extension,
recovery/compaction engines, data structures, flows, tests.
Architecture decisions and rationale are in the root design; this doc
does not repeat them.

---

## 1. The `JournalScan` crow-kv Extension

The sole crow-kv extension diskdb needs (§1, §3.4). Strategy 2 (journal
scan replay) requires replaying KV operations in **slot order** — but
the existing `Scan` RPC returns records in lexicographic key order, not
slot order. `JournalScan` adds a slot-ordered scan over the WAL.

### 1.1 Why it's needed

- **`Scan`** (existing) — prefix scan over the applied learner store;
  returns `(key, value)` pairs in key order. Used by strategy 1 (full
  scan rebuild — key order is fine because busy existence is the
  indicator, not write order) and strategy 3 (compaction — scans free
  records by key prefix).
- **`JournalScan`** (new) — slot-ordered scan over the WAL; returns
  `(key, value_or_none, slot)` ops in slot order. Used by strategy 2
  (replay — must apply ops in the order they were committed to get the
  correct final bitmap).

A plain `Scan` cannot substitute: it reads the current applied state
(key → latest value), not the history of operations. Strategy 2 needs
the operation log (Put X at slot 11, Delete X at slot 16, ...), not the
current state.

### 1.2 WAL structure (existing, in crow-kv)

The WAL is already slot-indexed:
- `lib/crow-kv/src/wal/index.rs` — `SegmentIndex` maps `slot →
  SlotLocation { disk_idx, segment_id, file_offset }`. Supports
  `locate(slot)`, `slot_count()`, and segment slot-range scans.
- `lib/crow-kv/src/wal/record.rs` — `WALRecord { record_type,
  group_id, term, slot, ballot, payload }`. Each record at a slot
  carries one paxos-chosen value.
- `lib/crow-kv/src/cluster/px_kv_store.rs` — the paxos payload for a
  KV op is encoded as a batch: `u16 count | (is_delete:u8,
  key_len:u32, key, value_len:u32, value?)*`. One slot = one batch of
  KV ops (Put, Delete, or BatchWrite).

So `JournalScan` iterates the WAL segment index in slot order, reads
each record, decodes the KV payload into individual ops, and filters by
key prefix.

### 1.3 Proto additions (`lib/crow-kv/src/rpc/proto/kv.proto`)

Add a new error code to the existing `KvErrorCode` enum (currently
`NONE`, `NOT_LEADER`, `UNAVAILABLE`, `INTERNAL`):

```protobuf
enum KvErrorCode {
  KV_ERROR_NONE        = 0;
  KV_ERROR_NOT_LEADER  = 1;
  KV_ERROR_UNAVAILABLE = 2;
  KV_ERROR_INTERNAL    = 3;
  KV_ERROR_JOURNAL_SCAN_GC_GAP = 4;  // min_slot < gc_slot (already GC'd)
}
```

New messages — field naming follows the existing `KvScanRequest` /
`KvScanResponse` convention (`version`, `request_id`,
`request_create_ms`, `group_id`, `read_mode`, `read_slot`,
`error_code`, `not_leader_hint`):

```protobuf
// Slot-ordered scan over the WAL. Returns individual KV ops (Put /
// Delete) in commit order within [min_slot, max_slot], filtered by
// key prefix. Used by diskdb strategy 2 (journal scan replay).
message KvJournalScanRequest {
  uint32 version    = 1;
  uint64 group_id   = 2;
  uint64 min_slot   = 3;  // inclusive lower bound
  uint64 max_slot   = 4;  // inclusive upper bound; 0 = MAX (current applied)
  bytes  key_prefix = 5;  // only ops whose key starts with this prefix
  uint32 limit      = 6;  // max ops per response page; 0 = unlimited
  uint64 request_id = 7;
  uint64 request_create_ms = 8;
  ReadMode read_mode = 9;  // LINEARIZABLE or MIN_SLOT (min_slot field
                           // doubles as the read-freshness floor for
                           // MIN_SLOT mode)
}

message KvJournalOp {
  bytes  key    = 1;
  bytes  value  = 2;  // empty for Delete
  bool   is_delete = 3;
  uint64 slot   = 4;  // the slot at which this op was committed
}

message KvJournalScanResponse {
  uint32 version  = 1;
  bool   ok       = 2;
  string error    = 3;
  repeated KvJournalOp ops = 4;
  bool   truncated = 5;  // hit `limit`; more ops remain
  uint64 last_op_slot = 6;  // slot of the last op returned (for pagination)
  uint64 read_slot = 7;  // the applied frontier when the scan ran
  KvErrorCode error_code = 8;
  string not_leader_hint = 9;
  uint64 request_id = 10;
}
```

Add to `KvService` (after the existing `Scan` rpc):
```protobuf
  rpc JournalScan(KvJournalScanRequest) returns (KvJournalScanResponse);
```

### 1.4 Server-side handler

The gRPC handler goes in `lib/crow-kv/src/rpc/kv_service.rs` (where
the existing `scan` handler lives), with the core scan logic in
`lib/crow-kv/src/cluster/px_kv_store.rs` (where `scan_err` and the
store-level scan helpers live). The handler mirrors the existing
`scan` handler's leader-forward + read-mode logic.

- `journal_scan(group_id, min_slot, max_slot, key_prefix, limit) ->
  Result<KvJournalScanResponse>`:
  a. Resolve the group; check leadership (same as `Scan`).
  b. Determine `effective_max_slot` = `min(max_slot,
     contiguous_applied())` (don't read unapplied slots).
  c. Iterate the WAL segment index from `min_slot` to
     `effective_max_slot`:
     - For each slot, `locate(slot)` → read the `WALRecord` from the
       segment file.
     - Decode the KV payload (`encode_kv_payload` format): `u16 count`
       then `(is_delete, key, value)` tuples.
     - For each op: if `key.starts_with(key_prefix)`, emit
       `KvJournalOp { key, value, is_delete, slot }`.
     - If `key_prefix` is empty, emit all ops.
  d. Collect ops until `limit` is reached (if set); set `truncated =
     true`, `last_op_slot = slot of last emitted op`.
  e. Return `ops` in slot order (within a slot, ops are in batch order;
     across slots, strictly ascending by slot).
- **Pagination**: caller sends `min_slot = last_op_slot + 1` for the
  next page. The `last_op_slot` field makes this stateless.
- **Read consistency**: `LINEARIZABLE` waits for the leader lease /
  ReadIndex; `MIN_SLOT` serves locally if `contiguous_applied >=
  min_slot`. Same pattern as `Scan`.
- **GC safety**: the WAL GC (`lib/crow-kv/src/wal/gc.rs`) only removes
  segments with `slot < gc_slot`. `gc_slot` is bounded by the
  compaction frontier — diskdb's compaction (strategy 3) writes a new
  `ZoneValue` snapshot before the WAL GC can remove the corresponding
  slots. If `min_slot < gc_slot` (the caller asks for already-GC'd
  slots), return `KV_ERROR_JOURNAL_SCAN_GC_GAP` — the caller falls
  back to strategy 1 (full scan). This should not happen in normal
  operation because compaction writes the snapshot before the WAL GC
  advances.

### 1.5 Client method (`lib/crow-kv-client/src/client.rs`)

- `CrowkvClient::journal_scan(store_id, group_id, min_slot, max_slot,
  key_prefix, limit, read_mode) -> Result<JournalScanOutcome>` —
  paginates transparently (same inner-pagination pattern as the
  existing `scan` method): sends the first request, if `truncated`,
  sends `min_slot = last_op_slot + 1`, repeats until all ops collected
  or `limit` reached. Returns the full op list in slot order. The
  `read_mode` + `min_slot` resolution mirrors `scan`'s
  `resolve_min_slot` / `resolve_read_endpoint` logic.

### 1.6 Mock group-0 / mock data-group support

The mock crow-kv-compatible server used in integration tests needs to
support `JournalScan`. The mock keeps an in-memory op log:
`Vec<(slot, key, value_opt)>` appended on each `Put`/`Delete`/
`BatchWrite`. `JournalScan` filters this log by `[min_slot, max_slot]`
+ key prefix, sorts by slot, returns ops. No WAL files needed — the
mock's op log is the "journal".

---

## 2. Protocol Enhancements (R73-specific)

### 2.1 `diskdb_service.proto` — `RebuildZoneBitmap` RPC

The current `DiskdbService` has `AllocateBlocks`, `FreeBlocks`,
`QueryCapacityStats`, `GetDiskGroupInfo`, `GetDiskInfo`. R73 adds the
`RebuildZoneBitmap` RPC + messages:

```protobuf
  rpc RebuildZoneBitmap(RebuildZoneBitmapRequest)
      returns (RebuildZoneBitmapResponse);
```

### 2.2 `diskdb_op.proto` — new messages

Add to `diskdb_op.proto`:
- `RebuildZoneBitmapRequest { disk_id, zone_index }` — `zone_index =
  u32::MAX` means all zones on the disk.
- `RebuildZoneBitmapResponse { rebuilt_zone_count, total_busy_units,
  total_free_units }`.

---

## 3. Recovery Engine

New module `app/crow-diskdb/src/recovery.rs` (to be created; the
`recovery` sub-module is registered in `lib.rs`):

### 3.1 `RecoveryEngine`

- Owns a `DataGroupClient` (from R72). Disk metadata (`DiskValue`s)
  is passed in by the caller (the sync loop already fetches it from
  group 0 via `HardwareClient`); the recovery engine does not need a
  group-0 client itself.
- Runs on startup (before the gRPC server accepts RPCs) and on
  ownership transfer (when `SyncLoop` detects a new disk-group
  assigned to this instance).

### 3.2 `recover_node` — full node recovery

```rust
async fn recover_node(
    &self,
    dg_id: DiskGroupId,
    node_id: NodeId,
    rack_id: RackId,
    bind: Bind,  // (store_id, group_id)
    disks: &[(DiskId, DiskValue)],
    zone_rotate_count: u32,
) -> Result<Arc<Node>>
```

a. Create an empty `Node` via `Node::new(dg_id, node_id, rack_id)`;
   set `bind` from the bind map.
b. For each `(disk_id, disk_value)` in `disks`:
   - Create a `ZoneDisk` via `ZoneDisk::new(disk_id, dg_id, node_id,
     rack_id, *disk_value)`.
   - Compute zone count = `disk_value.zone_count` (last zone may be
     smaller, rounded down to a multiple of 64 — matching
     `sync.rs::disk_add_init`).
   - For each zone index (0 to `zone_count - 1`):
     - Call `recover_zone(bind, disk_id, zone_idx, unit_capacity)`.
     - Add the recovered zone to the disk via `disk.add_zone`.
   - Call `disk.rebuild_active_zones(zone_rotate_count)` — build the
     initial `ActiveZoneContext` with the first `zone_rotate_count`
     allocatable zones.
   - Add disk to node via `node.add_disk`.
c. Return the reconstructed `Node` (wrapped in `Arc`).

**Concurrency**: recover zones within a node in parallel
(`tokio::spawn` + `join_all`) — each zone's recovery is independent.
This speeds up restart for nodes with many zones/disks. Bounded by a
semaphore (default 16 concurrent zone recoveries) to avoid overwhelming
the data group.

### 3.3 `recover_zone` — strategy 2 (journal scan replay)

The primary restart path. Fast because compaction (strategy 3) keeps
the uncomplicated record set small.

```rust
async fn recover_zone(
    &self,
    bind: Bind,
    disk_id: DiskId,
    zone_idx: u32,
    unit_capacity: u32,  // from DiskValue metadata
) -> Result<Zone>
```

a. **Load the latest `ZoneValue` snapshot**: via
   `DataGroupClient::get_zone_value(bind, disk_id, zone_idx)` (a
   dedicated point-get on `ZoneKey` — 1 round-trip; avoids the 2
   wasted scans of `read_zone_records` when only the snapshot is
   needed). If present, deserialize and verify CRC32
   (`ZoneValueExt::verify_checksum` from R70 — `crc32 ==
   crc32fast::hash(usage_bitmap)`). If CRC fails, log error and **fall
   back to strategy 1** (`rebuild_zone_bitmap_full_scan`) for this
   zone — conservative.
b. **Initialize replay state**:
   - If snapshot exists: `usage_bits = snapshot.usage_bitmap` (restored
     into a `UsageBitmap`), `used_count = popcount(usage_bits)`,
     `snapshot_slot = snapshot.snapshot_slot`.
   - If no snapshot: empty bitmap, `used_count = 0`, `snapshot_slot =
     0`.
c. **Journal scan**: two narrow scans (chosen over one broad scan —
   less data transferred, merge-by-slot is trivial since both lists
   are slot-sorted):
   - `DataGroupClient::journal_scan_busy(bind, min_slot =
     snapshot_slot + 1, max_slot = 0 (MAX), disk_id, zone_idx)` —
     wraps `CrowkvClient::journal_scan` with
     `BusyBlockKey::prefix_for_zone`.
   - `DataGroupClient::journal_scan_free(bind, ...)` — wraps with
     `FreeBlockKey::prefix_for_zone`.
   Merge the two op lists by slot (merge-sort). If `JournalScan`
   returns `KV_ERROR_JOURNAL_SCAN_GC_GAP` (asked for slots already
   GC'd), fall back to strategy 1 for this zone.
d. **Apply ops in slot order** (merge the two scans by slot):
   - `Put BusyBlockKey { disk_id, zone_idx, unit_offset }` with
     `BusyBlockValue { unit_count, ... }` → `range_set(unit_offset,
     unit_count)`; `used_count += unit_count`.
   - `Delete BusyBlockKey { ..., unit_offset }` → need `unit_count` to
     clear the right range. **Problem**: the Delete op carries only the
     key, not the value (the value is gone). Two options:
     - **(i) Track `unit_count` from the preceding Put** — during
       replay, maintain a map `unit_offset → unit_count` from Put ops.
       When a Delete arrives, look up the `unit_count` from the map.
       This works because a Delete always follows a Put for the same
       key (you can't free a block that was never allocated).
     - **(ii) Store `unit_count` in the `FreeBlockValue`** — the
       `FreeBlockValue` already has `unit_count` (field 1). The Delete
       of `BusyBlockKey` and Put of `FreeBlockKey` are in the same
       `batch_write` (same slot). So at the slot where the Delete
       appears, the corresponding `FreeBlockKey` Put also appears (same
       slot, adjacent op in the batch). Read `unit_count` from the
       `FreeBlockValue` at the same slot.
   - **Choose (ii)** — more robust. At each slot, decode the full batch:
     if the batch contains both `Delete BusyBlockKey` and `Put
     FreeBlockKey` for the same `unit_offset`, use the
     `FreeBlockValue.unit_count` for the `range_clear`. This is the
     normal free case. If a `Delete BusyBlockKey` appears without a
     matching `Put FreeBlockKey` (shouldn't happen in normal
     operation), fall back to (i) or log a warning and skip (the bit
     stays set — conservative, better to leak than to free
     incorrectly).
   - `Put FreeBlockKey` / `Delete FreeBlockKey` → **no-op for state**
     (free records are audit/carrier markers, not state indicators,
     §3.4). The bit is governed by `BusyBlockKey` presence.
   - `Put ZoneKey` (a newer compaction snapshot written after the one
     we loaded) → should not appear after `snapshot_slot` if we loaded
     the latest snapshot; if it does, **restart replay from the newer
     `snapshot_slot`** (defensive — reload the newer `ZoneValue`, reset
     the bitmap, restart the journal scan from the newer
     `snapshot_slot + 1`).
e. **Build the recovered `Zone`**: set `usage_bits`, `used_count`,
   `zone_state = Healthy` (R76's health probe may update it later),
   `snapshot_slot`. Reset `last_pos_64 = 0` (the rotating cursor
   restarts; it spreads load on its own). `derived_alloc_state()`
   reflects the rebuilt `used_count`.
f. Return the recovered `Zone`.

**Edge cases**:
- No snapshot, no records: fresh zone, empty bitmap, `used_count = 0`.
- Snapshot exists, no records after it: zone is exactly the snapshot
  state.
- Snapshot CRC fails: log error, fall back to strategy 1 (full scan).
- `JournalScan` not available (crow-kv extension not landed): fall back
  to strategy 1 for all zones (correct but slower).
- `JournalScan` returns `JournalScanGcGap`: fall back to strategy 1 for
  the affected zone.

### 3.4 `rebuild_zone_bitmap_full_scan` — strategy 1

On-demand via the `RebuildZoneBitmap` RPC and the §12 scanner (R75).
Correct and always available, but O(all live busy records per zone) —
too slow for regular restart with many zones.

```rust
async fn rebuild_zone_bitmap_full_scan(
    &self,
    bind: Bind,
    disk_id: DiskId,
    zone_idx: u32,
    unit_capacity: u32,
) -> Result<(Zone, ZoneStats)>
```

a. `DataGroupClient::read_zone_records(bind, disk_id, zone_idx)`
   (returns `ZoneRecords { zone_value, busy, free }`); use the `busy`
   field. Key order = `unit_offset` order. No snapshot needed, no slot
   ordering needed.
b. Start with an empty bitmap. For each `BusyRecord` in `busy`:
   `range_set(unit_offset, unit_count)`; `used_count += unit_count`.
c. (`FreeBlockKey`s are not scanned — they carry `previous_owner` audit
   info but are not state indicators, §3.4.)
d. Build the `Zone` as in `recover_zone` step e.
e. **Optionally write a fresh `ZoneValue` snapshot** (with
   `snapshot_slot = current_max_slot` from the data group's applied
   frontier) so the next restart can use strategy 2. This is
   recommended after a full rebuild — it "resets" the replay baseline.
f. Compute `ZoneStats { capacity_units, used_units, free_units }`.
g. Return `(Zone, ZoneStats)`.

### 3.5 Recovery on startup

Integrated into `app/crow-diskdb/src/main.rs`. The current `main.rs`
runs a blocking initial sync (`sync_once`) before starting the gRPC
server, but does not yet run recovery — the sync loop's
`disk_add_init` writes baseline `ZoneValue` records (empty bitmap,
`snapshot_slot = 0`) but does not replay the journal. R73 adds
recovery after the initial sync:

1. After the initial sync fetches owned disk-groups and their disks
   from group 0, run `RecoveryEngine::recover_node()` for each owned
   disk-group (strategy 2, with strategy 1 fallback).
2. The server must not serve RPCs until recovery is complete for all
   owned disk-groups. The current `main.rs` already blocks on
   `sync_once` before starting gRPC; recovery runs in the same
   blocking phase.
3. Log recovery progress: per disk-group, per disk, per zone — with
   timing (`recovery.zone.latency`, `recovery.node.latency` gauges,
   §11).

### 3.6 Recovery on ownership transfer

Integrated into `app/crow-diskdb/src/sync.rs`. The current sync loop's
`sync_once()` detects new disk-groups and calls `disk_add_init` (which
writes baseline `ZoneValue` records but does not replay the journal).
R73 keeps `disk_add_init` for fresh disks and adds recovery for
existing disks:

- When `sync_once()` detects a new disk-group assigned to this instance
  (not previously owned), first check whether a `ZoneValue` snapshot
  already exists for its zones (via `get_zone_value`). If snapshots
  exist (the disk-group was previously owned by another instance),
  run `RecoveryEngine::recover_node()` to replay the journal from
  `snapshot_slot`. If no snapshots exist (truly fresh disks), keep the
  existing `disk_add_init` path (writes baseline `ZoneValue` records
  with empty bitmap, `snapshot_slot = 0`).
- The new owner recovers from the data group's records — the same
  `ZoneValue` snapshots + journal scan as startup. The previous owner's
  in-memory state is irrelevant.

---

## 4. Compaction Engine

`app/crow-diskdb/src/recovery/compaction.rs` (sub-module of the
`recovery` module, following the `node.rs` + `node/container.rs`
pattern) — strategy 3 (ongoing maintenance). Keeps the uncompacted
record set small so strategy 2's replay is fast.

### 4.1 `CompactionEngine`

- Owns a `DataGroupClient` (from R72). Runs as a background task
  (`tokio::spawn`).
- Does **not** own a `CrowkvClient` directly — uses `DataGroupClient`'s
  scan/batch_write methods (which wrap `CrowkvClient`).

### 4.2 `compaction_loop`

```rust
async fn compaction_loop(
    node_container: Arc<NodeContainer>,
    kv: Arc<DataGroupClient>,
    config: CompactionConfig,
) -> !
```

a. `sleep(config.compaction_cadence)` (default periodic interval, §16).
b. For each owned node (disk-group) in `NodeContainer`:
   - For each disk in the node:
     - For each zone in the disk:
       - Check if compaction is needed: the zone's
         `uncompacted_free_record_count` gauge (§11) exceeds
         `config.snapshot_compaction_threshold` (record count, §16).
         If not, skip.
       - `await compact_zone(bind, disk_id, zone)`.
c. Repeat.

**Cadence vs. threshold**: compaction triggers on **either** the
periodic cadence **or** the per-zone threshold — whichever fires first
for a given zone. The threshold prevents a single high-churn zone from
accumulating too many free records between periodic cycles.

### 4.3 `compact_zone`

```rust
async fn compact_zone(
    &self,
    bind: Bind,
    disk_id: DiskId,
    zone: &Zone,
) -> Result<()>
```

a. **Scan free records** for the zone via
   `DataGroupClient::read_zone_records(bind, disk_id, zone_idx)` (use
   the `free` field), or a dedicated `scan_free_records` helper that
   calls only the free-prefix scan. The free records of one zone are
   contiguous in the crow-tree page, so this is efficient (§7). Record
   `compaction.scan_free.latency` (§11).
b. **Merge free records into the in-memory `ZoneValue` bitmap**: for
   each `FreeRecord` in `free`: `range_clear(unit_offset, unit_count)`
   on the zone's `usage_bits`. Record
   `compaction.merge_bitmap.latency`.
   - Busy records for freed blocks are already gone (deleted on free);
     busy records for live blocks stay set. The merge only clears bits
     for free records that haven't been compacted yet.
c. **Determine `snapshot_slot`**: `current_max_slot` = the applied
   frontier of the data group at the time of compaction. Fetched via
   `DataGroupClient::get_applied_slot(bind)` (a lightweight
   `Scan` with `read_mode = Linearizable` to get the `read_slot` from
   the response, or a dedicated method if crow-kv exposes one).
d. **Build the new `ZoneValue`**: `usage_bitmap` (the merged bitmap,
   serialized from `usage_bits`), `snapshot_slot = current_max_slot`,
   `crc32 = compute_checksum(usage_bitmap)` (R70).
e. **Write the new `ZoneValue`**: `await DataGroupClient::put_zone(
   bind, disk_id, zone_idx, &zone_value)`. This overwrites the old
   snapshot. The key invariant: the `ZoneValue` snapshot is written
   **before** the free records are deleted.
f. **Delete the free records** in one `batch_write`:
   `await DataGroupClient::delete_free_records_batch(bind,
   &free_record_keys)`. One `batch_write` per data group; batch by
   `disk_id` prefix so one scan covers all zones on a disk (if
   compacting multiple zones on the same disk, collect all their free
   records and delete in one batch).
g. **Update zone's `snapshot_slot`**: store `current_max_slot` in the
   zone's in-memory `snapshot_slot: AtomicU64`.
h. **Decrement `uncompacted_free_record_count`** by the number of free
   records deleted.
i. Increment `compaction.count` (or `compaction.error.count` on
   failure) (§11).

### 4.4 Compaction crash safety

- If diskdb crashes **after writing the new `ZoneValue`** but **before
  deleting the free records**: the free records are orphaned but
  harmless. On strategy 2 replay, the `Put FreeBlockKey` ops are
  no-ops for state (the bit is already clear in the snapshot); strategy
  1 ignores free records entirely. The next compaction cycle will
  re-scan and delete them.
- If diskdb crashes **after deleting the free records**: the snapshot
  is the source of truth. Both orders are safe.
- **No two-phase commit needed** — the `ZoneValue` write and the
  free-record delete are separate `batch_write`s, but the replay logic
  is resilient to either crash point. The key invariant: the
  `ZoneValue` snapshot is written **before** the free records are
  deleted (so the snapshot always reflects the freed state).

### 4.5 Compaction trigger (on-demand)

Besides the periodic loop, compaction can be triggered on demand:
- Before ownership transfer (compact all zones on the disk-group so the
  next owner's recovery is fast).
- When a zone's free-record backlog is large (operator-triggered).
- Expose `compact_zone_now(dg_id, disk_id, zone_idx) -> Result<()>` via
  an admin RPC or HTTP endpoint (R77 console).

---

## 5. Zone Struct Extensions

`app/crow-diskdb/src/zone.rs` already has (from R72):
- `snapshot_slot: AtomicU64` — the slot of the last compacted
  `ZoneValue` snapshot. Initialized to 0 on fresh zone, set by recovery
  and compaction.
- `uncompacted_free_record_count: AtomicU32` — gauge for the compaction
  backlog (§11). Incremented on free (in `Zone::free`), decremented
  when compaction deletes the free record.

R73 adds:
- `to_zone_value() -> ZoneValue` — snapshot current `usage_bits` +
  `snapshot_slot` + `crc32` (via `ZoneValueExt::compute_checksum`) for
  compaction.
- `from_zone_value(value: &ZoneValue, unit_capacity: u32) -> Zone` —
  rebuild from a snapshot (used by recovery when no records exist after
  the snapshot). Deserializes `usage_bitmap` into a `UsageBitmap`,
  computes `used_count` = popcount, sets `snapshot_slot`. Verifies CRC
  via `ZoneValueExt::verify_checksum` before use.

---

## 6. DataGroupClient Extensions

`app/crow-diskdb/src/persistence.rs` already has (from R72):
- `put_zone(bind, disk_id, zone_idx, value: &ZoneValue) -> Result<()>`
  — `put` to `ZoneKey`. Used by compaction (step e) and full rebuild
  (strategy 1 step e). Also used by the sync loop's `disk_add_init` to
  write baseline `ZoneValue` records.
- `read_zone_records(bind, disk_id, zone_idx) -> Result<ZoneRecords>`
  — reads `ZoneValue` (point `get`) + all `BusyBlockKey` and
  `FreeBlockKey` entries (two prefix scans) for one zone, decoded into
  `ZoneRecords { zone_value, busy: Vec<BusyRecord>, free:
  Vec<FreeRecord> }`. Used by strategy 1 (full scan — use the `busy`
  field) and compaction (step a — use the `free` field). The
  `zone_value` field serves strategy 2 step a (load snapshot).
- `delete_free_records_batch(bind, keys: &[Vec<u8>]) -> Result<()>` —
  `batch_write` with `Delete` ops for free records only. Used by
  compaction (step f).

R73 adds:
- `get_zone_value(bind, disk_id, zone_idx) -> Result<Option<ZoneValue>>`
  — point `get` on `ZoneKey` only (1 round-trip). Used by recovery
  (strategy 2 step a — load snapshot). Avoids the 2 wasted scans of
  `read_zone_records` when only the snapshot is needed.
- `get_applied_slot(bind) -> Result<u64>` — get the data group's
  current applied frontier (for compaction's `snapshot_slot`). Uses a
  lightweight `Scan` with `read_mode = Linearizable` and reads
  `read_slot` from the response, or a dedicated crow-kv method if
  available.
- `journal_scan_busy(bind, min_slot, max_slot, disk_id, zone_idx) ->
  Result<Vec<KvJournalOp>>` — wraps `CrowkvClient::journal_scan` with
  `BusyBlockKey::prefix_for_zone`. Used by recovery (strategy 2 step
  c, scan 1).
- `journal_scan_free(bind, min_slot, max_slot, disk_id, zone_idx) ->
  Result<Vec<KvJournalOp>>` — wraps `CrowkvClient::journal_scan` with
  `FreeBlockKey::prefix_for_zone`. Used by recovery (strategy 2 step
  c, scan 2).

Note: `read_zone_records` does three round-trips (one `get` + two
scans) per zone. Strategy 1 (full scan) uses the `busy` field;
compaction (step a) uses the `free` field. Strategy 2 step a (load
snapshot) uses the dedicated `get_zone_value` (1 round-trip) instead —
the busy/free scans of `read_zone_records` are wasted there since
strategy 2 replays from the journal, not current state.

---

## 7. Config Extensions

`app/crow-diskdb/src/config.rs`. The current `PersistenceConfig`
already has `snapshot_interval_secs` (default 300) and
`snapshot_journal_threshold` (default 4096) — these were added in R72
as placeholders. R73 renames/restructures them to match the §16 config
naming:

- Rename `snapshot_interval_secs` → `compaction_cadence_secs` (default
  300) — periodic compaction interval. Keep the same field semantics
  (seconds in config; converted to `Duration` in `SyncConfig`-style
  wiring).
- Rename `snapshot_journal_threshold` →
  `snapshot_compaction_threshold` (default 4096) — free-record count
  per zone that triggers compaction (in addition to the periodic
  cadence).
- Add `recovery_concurrency: usize` (default 16) — max concurrent zone
  recoveries in `recover_node`. Add to `PersistenceConfig` (or a new
  `RecoveryConfig` section).
- Update `validate()` to check the renamed fields (replace the
  existing `snapshot_interval_secs == 0` check).

---

## 8. Server Wiring

Update `app/crow-diskdb/src/main.rs`. The current `main.rs` builds
the kv-client service classes, `NodeContainer`, `SyncLoop`, runs a
blocking `sync_once`, spawns the sync loop, then starts the gRPC
server. R73 inserts recovery + compaction into this flow:

1. ... (existing steps: config, clients, `NodeContainer`, `SyncLoop`,
   blocking `sync_once`).
2. Create `RecoveryEngine` with `DataGroupClient`.
3. Run `RecoveryEngine::recover_node()` for each owned disk-group
   (blocking — server must not serve RPCs until recovery completes).
   The current `main.rs` already blocks on `sync_once` before starting
   gRPC; recovery runs in the same blocking phase, after `sync_once`.
4. Create `CompactionEngine` with `DataGroupClient` + config.
5. Spawn `compaction_loop` as a background task.
6. Create gRPC service (`DiskdbService::new`) — now with the
   `RebuildZoneBitmap` handler implemented (delegates to
   `RecoveryEngine::rebuild_zone_bitmap_full_scan`). The current
   `DiskdbService` does not have `RebuildZoneBitmap`; R73 adds it to
   the `DiskdbService` struct + the `DiskdbService` trait impl in
   `grpc.rs`.
7. Start gRPC server (tonic) on `listen_addr` (existing).
8. HTTP management server (axum) on `http_listen_addr` — not yet
   implemented in the current `main.rs`; R73 does not add it (deferred
   to R77 console).

Update `app/crow-diskdb/src/sync.rs`:
- When `sync_once()` detects a new disk-group, check whether
  `ZoneValue` snapshots already exist for its zones (via
  `get_zone_value`). If they do, call `RecoveryEngine::recover_node()`
  instead of `disk_add_init` (replay the journal from
  `snapshot_slot`). If not (fresh disks), keep the existing
  `disk_add_init` path. The `SyncLoop` needs a handle to the
  `RecoveryEngine` (add a `with_recovery_engine` builder, mirroring
  `with_data_group_client`).

---

## 9. Module Structure

The current `app/crow-diskdb/src/` uses flat files with sub-module
directories where needed (e.g. `node.rs` + `node/container.rs` +
`node/disk.rs`). R73 follows the same pattern:

```
app/crow-diskdb/src/
├── recovery.rs             — RecoveryEngine, recover_node, recover_zone
│                             (strategy 2), rebuild_zone_bitmap_full_scan
│                             (strategy 1). New module.
├── recovery/
│   └── compaction.rs       — CompactionEngine, compaction_loop,
│                             compact_zone (strategy 3). New sub-module.
├── persistence.rs          — DataGroupClient (extended: get_zone_value,
│                             get_applied_slot, journal_scan_busy/free).
│                             Already has put_zone, read_zone_records,
│                             delete_free_records_batch.
├── zone.rs                 — Zone (extended: to_zone_value, from_zone_value).
│                             Already has snapshot_slot,
│                             uncompacted_free_record_count.
├── grpc.rs                 — DiskdbService (extended: RebuildZoneBitmap
│                             handler). Currently has Allocate/Free/Query/Info.
├── config.rs               — PersistenceConfig (renamed fields:
│                             compaction_cadence_secs,
│                             snapshot_compaction_threshold),
│                             recovery_concurrency.
├── sync.rs                 — SyncLoop (extended: trigger recovery on
│                             ownership transfer via with_recovery_engine;
│                             keep disk_add_init for fresh disks).
├── lib.rs                  — add `recovery` module.
└── main.rs                 — wire RecoveryEngine + CompactionEngine.
```

---

## 10. Test Strategy

### 10.1 Unit tests (no external deps)

- `recover_zone` with no snapshot and no records → fresh zone (empty
  bitmap, `used_count = 0`).
- `recover_zone` with a `ZoneValue` snapshot and no records after it →
  returns the snapshot state (verify `usage_bits` and `used_count`
  match).
- `recover_zone` with a snapshot and records after it → correctly
  applies ops in slot order:
  - Write snapshot at slot 10.
  - Put `BusyBlockKey` at slots 11–15 (5 allocates).
  - Delete `BusyBlockKey` + Put `FreeBlockKey` at slot 16 (1 free).
  - Recover → verify bitmap: bits set for slots 11–15, cleared for the
    freed one; `used_count` = 4 * unit_count.
- `recover_zone` with no snapshot and records → replays from slot 0.
- `recover_zone` with a corrupted snapshot (CRC fail) → logs error,
  falls back to strategy 1 (full scan).
- `recover_zone` with `JournalScanGcGap` → falls back to strategy 1.
- `rebuild_zone_bitmap_full_scan` (strategy 1) → scans `BusyBlockKey`s
  and rebuilds the bitmap; ignores `FreeBlockKey`s. Write busy + free
  records, rebuild, verify bitmap = busy keys only.
- `rebuild_zone_bitmap_full_scan` writes a fresh `ZoneValue` snapshot
  after rebuild → verify the snapshot exists with the correct
  `snapshot_slot`.
- `compact_zone` (strategy 3) → merges free records into a new
  `ZoneValue`, writes it, deletes only the free records:
  - Create a zone with busy + free records.
  - Compact → verify the new `ZoneValue` has the freed bits cleared.
  - Verify the free records are deleted.
  - Verify the busy records for live blocks are **not** deleted.
- Compaction crash safety:
  - Crash after `ZoneValue` write but before free-record delete →
    replay still correct (orphaned free records are no-ops for state).
  - Crash after free-record delete → snapshot is the source of truth.
- `Zone::to_zone_value` / `from_zone_value` round-trip — serialize +
  deserialize, verify bitmap + `snapshot_slot` + `crc32` match.
- Config validation — new `CompactionConfig` fields.

### 10.2 Integration tests (in-process mock group-0 + mock data-group)

Using an in-process mock crow-kv-compatible server (mock group-0 +
mock data-group), extended with `JournalScan` support (§1.6):

- **`JournalScan` crow-kv RPC** — write ops via `Put`/`Delete`/
  `BatchWrite`, then `JournalScan` returns them in slot order within a
  key-prefix filter. Verify slot ordering, prefix filtering, pagination
  (`limit` + `last_op_slot`).
- **`recover_node`** — allocate/free blocks via the R72 allocate/free
  paths, restart diskdb (re-run recovery), verify in-memory state
  matches the records.
- **`recover_node` with compaction** — allocate/free, run compaction,
  restart diskdb, verify recovery is fast (few records to replay) and
  correct (bitmap matches).
- **`RebuildZoneBitmap` gRPC handler** — trigger via gRPC, verify
  returned stats match the records.
- **Compaction trigger on threshold** — free enough blocks to exceed
  `snapshot_compaction_threshold`, verify compaction runs.
- **Compaction trigger on cadence** — wait for `compaction_cadence`,
  verify compaction runs.
- **Ownership transfer recovery (mock)** — simulate ownership transfer
  (re-assign disk-group in mock group-0), verify the new owner
  recovers from the data group's records. See §10.4 for the detailed
  scenarios.
- **Server startup gates gRPC on recovery** — server does not accept
  RPCs until recovery finishes. Integration test: connect before
  recovery completes → `Unavailable`; connect after → success.
- **Recovery concurrency** — recover a node with many zones, verify
  parallel recovery (timing < sequential baseline).

### 10.3 Ownership-transfer unit tests

The ownership-transfer scenario: instance A owns a disk-group,
allocates/frees blocks (producing records on the data group), then
ownership transfers to instance B. Instance B has never seen A's
in-memory state — it must reconstruct the bitmap purely from the data
group's records. These unit tests exercise the recovery engine in
isolation (no real kv cluster; use a mock data-group or direct
`DataGroupClient` calls against an in-process store).

- **Transfer with only busy records (no snapshot, no compaction)**:
  - Instance A: `disk_add_init` writes baseline `ZoneValue` (empty
    bitmap, `snapshot_slot = 0`). Allocate 5 blocks → 5
    `BusyBlockKey` records on the data group.
  - Transfer: ownership map changes to instance B. Instance B runs
    `recover_node` → `recover_zone` for each zone.
  - Verify: instance B's recovered bitmap has exactly the 5 bits set
    that A allocated. `used_count` = 5 * unit_count. No busy record
    for freed blocks (A freed none in this case).
  - Key assertion: B's bitmap matches A's bitmap even though B never
    saw A's in-memory state.

- **Transfer after allocate + free (busy + free records, no
  compaction)**:
  - Instance A: allocate 5 blocks, free 2 of them. Data group has 3
    `BusyBlockKey` + 2 `FreeBlockKey` records.
  - Transfer to instance B → `recover_zone`.
  - Verify: B's bitmap has exactly the 3 live busy bits set (the 2
    freed bits are clear). `used_count` = 3 * unit_count. The 2
    `FreeBlockKey` records are present but do not affect the bitmap
    (free records are no-ops for state in strategy 2 replay).

- **Transfer after compaction (snapshot + few records)**:
  - Instance A: allocate 10, free 4, run compaction → new `ZoneValue`
    snapshot with 6 bits set, `snapshot_slot = S`. The 4 free records
    are deleted. Data group now has: 1 `ZoneValue` (6 bits, slot S) +
    6 `BusyBlockKey` records.
  - Transfer to instance B → `recover_zone` loads the snapshot (6
    bits), journal-scans from `S+1` (no ops → empty replay).
  - Verify: B's bitmap = 6 bits set, `used_count` = 6 * unit_count,
    `snapshot_slot = S`. Recovery is fast (no replay needed).

- **Transfer after compaction + post-compaction allocates**:
  - Instance A: compact (snapshot at slot S, 6 bits), then allocate 3
    more blocks (slots S+1..S+3). Data group: `ZoneValue` (6 bits,
    slot S) + 6 old `BusyBlockKey` + 3 new `BusyBlockKey`.
  - Transfer to instance B → `recover_zone` loads snapshot (6 bits),
    journal-scans from `S+1`, replays 3 Put `BusyBlockKey` ops.
  - Verify: B's bitmap = 9 bits set (6 from snapshot + 3 replayed),
    `used_count` = 9 * unit_count.

- **Transfer after compaction + post-compaction free**:
  - Instance A: compact (snapshot at slot S, 6 bits), then free 1
    block (slot S+1: Delete `BusyBlockKey` + Put `FreeBlockKey`).
  - Transfer to instance B → `recover_zone` loads snapshot (6 bits),
    journal-scans from `S+1`, replays the free op (Delete
    `BusyBlockKey` → `range_clear` using `FreeBlockValue.unit_count`
    from the same slot).
  - Verify: B's bitmap = 5 bits set (6 from snapshot − 1 freed),
    `used_count` = 5 * unit_count.

- **Transfer with no records at all (fresh disk-group)**:
  - No `ZoneValue`, no `BusyBlockKey`, no `FreeBlockKey`. This is the
    `disk_add_init` path (fresh disks) — recovery is not triggered
    (no snapshot exists). Verify: `disk_add_init` writes baseline
    `ZoneValue` (empty bitmap, `snapshot_slot = 0`), B's bitmap is
    empty, `used_count = 0`.

- **Transfer back to original owner (A → B → A)**:
  - Instance A allocates 5, transfers to B (B recovers 5 bits), B
    allocates 3 more (8 total), transfers back to A.
  - Instance A's old in-memory state is stale (5 bits) — it must
    discard it and recover from records (8 bits).
  - Verify: A's recovered bitmap = 8 bits, `used_count` = 8 *
    unit_count. The stale in-memory state is irrelevant; recovery
    always reconstructs from records.

- **Transfer with corrupted snapshot on the new owner**:
  - Instance A: compact (snapshot at slot S), then corrupt the
    `ZoneValue`'s `crc32` field directly on the data group.
  - Transfer to instance B → `recover_zone` loads snapshot, CRC
    fails → falls back to strategy 1 (full scan of `BusyBlockKey`s).
  - Verify: B's bitmap = full-scan result (all live busy bits set),
    regardless of the corrupted snapshot.

### 10.4 Verification commands

- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
- Relevant tests pass (`pixi run cargo test -p crow-diskdb` and
  `pixi run cargo test -p crow-kv` for the `JournalScan` handler).

### 10.5 E2E test — disk-group transfer between diskdb instances

Uses the existing `KvCluster` harness (3-node `crow-kv-server` cluster,
groups 0 + 1) from `app/crow-diskdb/tests/common/cluster.rs`. Two
diskdb instances run in-process (same test binary, separate
`NodeContainer`s + `SyncLoop`s), sharing the same kv cluster. The test
mirrors `diskdb_e2e_allocate_free` for the setup, then adds the
transfer flow.

**Setup** (shared by both instances):
- Start `KvCluster` (3 kv-server nodes, group 0 + group 1).
- Seed hardware metadata into group 0: rack 1, node 10, disk-group
  100, 3 disks (disk_id 0:1, 0:2, 0:3), 4 zones × 128 units each.
- Set ownership → instance A (`instance_id = 999`).
- Set bind → `(store_id = 0, group_id = 1)`.

**Phase 1 — instance A populates state**:
- Build instance A: `NodeContainer::new(999)`, `SyncLoop` with
  `DataGroupClient` seeded to group-1 leader. `with_recovery_engine`
  (R73).
- `sync_once()` → `disk_add_init` writes baseline `ZoneValue` records
  (fresh disks, no snapshots exist yet).
- Allocate 5 blocks via `persistence::allocate_blocks` (owner_chunk =
  `0:0:42`). Verify 5 `BusyBlockKey` records on group 1.
- Free 2 of the 5 blocks via `persistence::free_blocks`. Verify 2
  `FreeBlockKey` records, 2 `BusyBlockKey` gone. Instance A's
  in-memory bitmap: 3 bits set.
- (Optional) Run compaction on one zone → verify `ZoneValue` snapshot
  written with the compacted bitmap.

**Phase 2 — transfer ownership to instance B**:
- `hw.set_owner(rack_id=1, node_id=10, dg_id=100, instance_id=888,
  lease_ms=now+3600s)` — re-assign ownership to instance B in group 0.
- Build instance B: `NodeContainer::new(888)`, `SyncLoop` with
  `with_recovery_engine`. Instance B has a fresh, empty
  `NodeContainer` — it has never seen instance A's in-memory state.
- `sync_once()` on instance B → detects disk-group 100 as new →
  checks `get_zone_value` → snapshots exist (from `disk_add_init` or
  compaction) → runs `RecoveryEngine::recover_node()` instead of
  `disk_add_init`.

**Phase 3 — verify instance B's recovered state**:
- Instance B's `NodeContainer` has disk-group 100 with 3 disks, 4
  zones each.
- For each zone that had allocates: verify `used_count` matches the
  record-derived count (3 live busy blocks if no compaction, or the
  snapshot's popcount if compacted).
- Verify instance B's bitmap matches the records: for each
  `BusyBlockKey` on the data group, the corresponding bit is set in
  B's in-memory `usage_bits`. For each freed offset, the bit is clear.
- Verify `snapshot_slot` is set correctly (0 if no compaction, or the
  compaction slot).

**Phase 4 — instance B serves allocates after recovery**:
- Allocate 3 more blocks via instance B's `NodeContainer` +
  `DataGroupClient`. Verify they succeed and land on the correct
  disks/zones (round-robin spreads across the 3 disks).
- Verify the 3 new `BusyBlockKey` records appear on group 1.
- Free 1 of the 3 new blocks. Verify the `FreeBlockKey` appears and
  the `BusyBlockKey` is gone.

**Phase 5 — transfer back to instance A (A → B → A)**:
- `hw.set_owner(..., instance_id=999)` — transfer back to A.
- Instance A's old `NodeContainer` is stale (3 bits from phase 1).
  Discard it; build a fresh `NodeContainer::new(999)`.
- `sync_once()` → recovery → reconstruct from records (3 old + 3 new
  from B − 1 freed by B = 5 live busy blocks).
- Verify A's recovered `used_count` = 5 * unit_count. The stale
  in-memory state (3 bits) is irrelevant — recovery always
  reconstructs from records.

**Assertions summary**:
- After phase 2, instance B's bitmap matches the data-group records
  (not instance A's stale in-memory state).
- After phase 4, instance B can allocate/free normally (recovery did
  not corrupt the allocator).
- After phase 5, instance A's recovered bitmap reflects all
  operations from both A and B (total 5 live busy blocks), proving
  recovery is idempotent and stateless-on-disk.
