<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R73: diskdb — Crash Recovery (Journal Replay) + Snapshot Compaction

**Problem**: R72 implements the allocation engine with journal-based
persistence — each allocate appends a `BusyRecord`, each free appends a
`FreeRecord` to the paxos data group. The in-memory bitmap and
`allocate_pos` are derived from the journal. But on crash or restart,
the in-memory state is lost and must be reconstructed from the journal.
Without recovery, diskdb cannot restart safely — it would lose track of
all allocations.

The design doc (§7) specifies the replay algorithm: load the latest
`ZoneSnapshot`, scan the journal by key prefix for all busy/free
records with slot > `snapshot_slot`, apply them to the snapshot's
bitmap, and rebuild the in-memory zone. It also specifies snapshot
compaction: periodically write a new `ZoneSnapshot` and batch-delete
expired busy/free records.

The aioss reference has a simple recovery (`persistence/recovery.rs`:
load `ZoneRecord` from metadb, rebuild in-memory) — but aioss writes
the **full** `ZoneRecord` on every allocate, so no replay is needed.
CROW's journal model (D4) requires actual replay. This is new work
with no direct aioss analog.

The D4 open question — whether replay needs crow-kv slot info or can
scan by key prefix — is resolved by R70's slot-based key layout: each
journal key embeds a monotonic `slot` number, so prefix-scan replay
works without crow-kv slot feedback.

**Solution**: Implement crash recovery and snapshot compaction — the
durability safety net that makes diskdb stateless-on-disk.

1. **Journal replay** — create
   `lib/crow-diskdb/src/recovery/mod.rs`:
   - `RecoveryEngine` — owns a `JournalClient` (from R72) and a
     `SysdataClient` (from R71). Runs on startup and on ownership
     transfer (when a new disk-group is assigned to this instance).
   - `recover_node(dg_id: &DiskGroupId, bind: (u64, u64), disks:
     &[DiskMeta]) -> Result<Node>`:
     a. Create an empty `Node` with the disk-group's binding info.
     b. For each disk in `DiskMeta`:
        - Create a `ZoneDisk` (from R71).
        - For each zone index (0 to `disk.zone_count`):
          - Call `recover_zone(dg_id, bind, disk_uuid, zone_idx)`.
          - Add the recovered zone to the disk.
        - Call `disk.rebuild_active_zones()`.
        - Add disk to node.
     c. Return the reconstructed `Node`.
   - `recover_zone(dg_id, bind, disk_uuid, zone_idx) ->
     Result<Zone>`:
     a. **Load the latest `ZoneSnapshot`**: `get` from
        `journal_key_snapshot(dg_id, disk_uuid, zone_idx)`. If
        present, deserialize and verify CRC (R70's
        `verify_checksum()`). If CRC fails, log error and treat as
        no snapshot (replay from journal start — conservative).
     b. **Initialize replay state**: if snapshot exists, start with
        `allocate_pos = snapshot.allocate_pos`, `usage_bits =
        snapshot.usage_bitmap` (restored), `snapshot_slot =
        snapshot.snapshot_slot`. If no snapshot, start with
        `allocate_pos = 0`, empty bitmap, `snapshot_slot = 0`.
     c. **Prefix-scan the journal**: `scan` with prefix
        `journal_prefix_zone(dg_id, disk_uuid, zone_idx)` on the
        bound data group. This returns all keys/values under that
        prefix — busy records, free records, and the snapshot key.
        Use `CrowkvClient::scan()` with `keys_only = false` to get
        values.
     d. **Parse and sort journal entries**: for each key, parse the
        suffix to determine type (`busy/{slot}`, `free/{slot}`, or
        `snapshot`) and slot number. Filter to entries with `slot >
        snapshot_slot` (entries already compacted into the snapshot
        are ignored). Sort by slot number to ensure correct replay
        order.
     e. **Apply busy records**: for each `BusyRecord` (in slot order):
        - `range_set(offset, count)` on the bitmap. If bit already
          set (ghost allocation or replay error), log warning but
          continue (conservative — the block is marked busy either
          way).
        - `allocate_pos = max(allocate_pos, offset + count)` (advance
          to the highest allocated position).
     f. **Apply free records**: for each `FreeRecord` (in slot order):
        - `range_clear(offset, count)` on the bitmap. If bit already
          clear, log warning but continue.
     g. **Build the recovered `Zone`**: set `allocate_pos`,
        `usage_bits`, `zone_state` (Healthy by default; R76's health
        probe may update it later). Set `allocation_state` to Full if
        `allocate_pos >= max_allocate_pos`, else Active. Set
        `next_journal_slot` to `max(all_slots) + 1`.
     h. Return the recovered `Zone`.
   - **Edge cases**:
     - No snapshot, no journal entries: fresh zone, `allocate_pos =
       0`, empty bitmap.
     - Snapshot exists, no journal entries after it: zone is exactly
       the snapshot state.
     - Snapshot CRC fails: log error, replay from journal start
       (slot 0). This may re-apply records already in the snapshot,
       but `range_set` is idempotent (bit already set → warning, no
       harm).
     - Journal scan is truncated (crow-kv paginated scan): the
       `CrowkvClient::scan()` method transparently pages until
       complete (R69's client design), so this is handled.

2. **Snapshot compaction** — create
   `lib/crow-diskdb/src/recovery/compaction.rs`:
   - `CompactionEngine` — owns a `JournalClient`. Runs as a background
     task (spawned by the server).
   - `compaction_loop(node_container, journal, config)`:
     a. `sleep(snapshot_interval_secs)` (default 300 s / 5 min).
     b. For each owned node (disk-group):
        - For each disk, for each zone:
          - Check if compaction is needed: `zone.next_journal_slot -
            zone.snapshot_slot > snapshot_journal_threshold` (default
            4096). If not, skip.
          - `compact_zone(dg_id, bind, zone)`.
     c. Repeat.
   - `compact_zone(dg_id, bind, zone: &Zone) -> Result<()>`:
     a. **Compute current state**: snapshot the zone's `allocate_pos`
        and `usage_bits` (via `UsageBitmap::snapshot()`).
     b. **Determine `snapshot_slot`**: `current_max_slot =
        zone.next_journal_slot.load() - 1` (the last journal entry
        included in the compaction).
     c. **Build `ZoneSnapshot`**: `ZoneRecord { allocate_pos,
        max_allocate_pos, usage_bitmap, zone_state, snapshot_slot:
        current_max_slot, checksum: 0 }`. Compute CRC
        (`compute_checksum()`).
     d. **Write the new snapshot**: `put` to
        `journal_key_snapshot(dg_id, disk_uuid, zone_idx)`. This
        overwrites the old snapshot.
     e. **Delete expired journal records**: collect all busy/free
        keys with `slot <= current_max_slot`. `batch_write` with
        `Delete` ops (one batch per data group). This is the "batch
        merge" from D4 — deletes span any disk/zone, so multiple
        zones' expired records can be collected and deleted in one
        `batch_write`.
     f. **Update zone's `snapshot_slot`**: store `current_max_slot`
        in the zone's in-memory tracking (new field
        `snapshot_slot: AtomicU64` on `Zone`).
   - **Compaction trigger**: besides the periodic loop, compaction can
     be triggered on demand (e.g. before ownership transfer, or when
     the journal for a zone exceeds a size threshold). Expose
     `compact_zone_now()` for manual triggering.
   - **Crash safety of compaction**: if diskdb crashes after writing
     the new snapshot but before deleting old records, the old records
     are orphaned but harmless — on replay, they have `slot <=
     snapshot_slot` and are filtered out. If diskdb crashes after
     deleting old records, the snapshot is the source of truth. Both
     orders are safe.

3. **Recovery on startup** — integrate into
   `app/crow-diskdb-server/src/main.rs`:
   - After the initial sync (R71) fetches owned disk-groups and their
     disks from group 0, run `RecoveryEngine::recover_node()` for
     each owned disk-group.
   - This reconstructs in-memory zone state from the journal before
     the server starts accepting allocate/free RPCs.
   - The server must not serve RPCs until recovery is complete for all
     owned disk-groups. Use a `Barrier` or `OnceCell` to gate the gRPC
     server startup.

4. **Recovery on ownership transfer** — integrate into the sync loop
   (R71):
   - When `sync_once()` detects a new disk-group assigned to this
     instance (not previously owned), run
     `RecoveryEngine::recover_node()` for it before adding it to the
     `NodeContainer`.
   - When a disk-group is removed (unassigned), remove it from the
     `NodeContainer` (in-memory state is discarded; the journal
     persists in the data group for the next owner).

5. **Zone struct extension** — update `lib/crow-diskdb/src/zone/mod.rs`
   (from R72):
   - Add `snapshot_slot: AtomicU64` — tracks the slot of the last
     compacted snapshot. Initialized to 0 on fresh zone, set by
     recovery and compaction.
   - Add `to_snapshot() -> ZoneSnapshot` — snapshot current state for
     compaction.
   - Add `from_snapshot(snapshot: ZoneSnapshot) -> Zone` — rebuild
     from a snapshot (used by recovery when no journal entries exist
     after the snapshot).

**Scope** (expected changed files):
- `lib/crow-diskdb/src/recovery/mod.rs` — `RecoveryEngine` with
  `recover_node`, `recover_zone`, journal replay logic.
- `lib/crow-diskdb/src/recovery/compaction.rs` — `CompactionEngine`
  with `compaction_loop`, `compact_zone`.
- `lib/crow-diskdb/src/zone/mod.rs` — add `snapshot_slot`,
  `to_snapshot()`, `from_snapshot()`.
- `lib/crow-diskdb/src/persistence/mod.rs` — `read_journal_zone()`
  (prefix scan), `delete_journal_records_batch()` (already defined in
  R72, used here).
- `lib/crow-diskdb/src/lib.rs` — add `recovery` module.
- `app/crow-diskdb-server/src/main.rs` — run recovery on startup,
  spawn compaction loop, gate gRPC server on recovery completion.
- `lib/crow-diskdb/src/sync/mod.rs` (from R71) — trigger recovery on
  ownership transfer.

**Complexity**: High. Journal replay is new — the aioss reference has
no analog (it writes full `ZoneRecord` on every allocate). The replay
algorithm is specified in the design doc (§7) and the slot-based key
layout (R70) makes prefix-scan replay straightforward. The main
challenges are: (1) parsing journal keys to extract type and slot, (2)
sorting entries by slot for correct replay order, (3) handling
truncated/paginated scans, (4) compaction crash safety (write snapshot
before deleting records). The CRC verification (R70) catches snapshot
corruption.

**Dependencies**: R70 (types, key layout, CRC), R71 (SysdataClient,
NodeContainer, sync loop), R72 (JournalClient, Zone CAS, bitmap). No
dependency on R74–R77.

**Acceptance**:
- `recover_zone()` with no snapshot and no journal entries returns a
  fresh zone (`allocate_pos = 0`, empty bitmap). Unit test.
- `recover_zone()` with a snapshot and no journal entries after it
  returns the snapshot state. Unit test: write a snapshot, recover,
  verify `allocate_pos` and bitmap match.
- `recover_zone()` with a snapshot and busy/free records after it
  correctly applies the records to the snapshot's bitmap. Unit test:
  write snapshot at slot 10, write busy records at slots 11–15, free
  at slot 16, recover, verify bitmap reflects all operations.
- `recover_zone()` with no snapshot and journal entries replays from
  slot 0. Unit test.
- `recover_zone()` with a corrupted snapshot (CRC fail) logs error
  and replays from journal start. Unit test.
- `recover_node()` reconstructs all disks and zones for a disk-group.
  Integration test with in-process crow-kv: allocate blocks, restart
  diskdb (re-run recovery), verify in-memory state matches.
- `compact_zone()` writes a new snapshot and deletes expired records.
  Unit test: create zone with journal entries, compact, verify
  snapshot exists and old records are deleted.
- Compaction crash safety: if crash after snapshot write but before
  delete, replay still correct (old records filtered by
  `snapshot_slot`). Unit test.
- Server startup gates gRPC on recovery completion. Integration test:
  server does not accept RPCs until recovery finishes.
- Ownership transfer triggers recovery for newly assigned
  disk-groups. Integration test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.
