<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R75: diskdb — Background Scanner (Ghost/Drift/Integrity Detection)

**Problem**: R72–R74 implement the allocation engine, crash recovery,
and metrics. But the journal-based design (D4) introduces consistency
risks that the aioss reference (which writes full `ZoneRecord` on every
allocate) does not have:
- **Ghost allocations**: a block is allocated (BusyRecord in journal)
  but the free batch flush crashed before the `FreeRecord` was
  persisted. On recovery, the block appears allocated but is actually
  free. The in-memory bitmap (replayed from journal) shows it as busy.
- **`allocate_pos` drift**: the in-memory `allocate_pos` drifts from
  the journal-derived position (e.g. a CAS claim succeeded locally but
  the BusyRecord persist failed and rollback was incomplete, or a bug
  in the CAS logic).
- **Record integrity**: a `ZoneSnapshot` or journal record is
  corrupted on disk (bit rot, storage error). The CRC check (R70)
  catches this, but only if someone runs it.

The design doc (§12) specifies a background scanner that detects these
conditions. Leak detection (blocks allocated but not referenced by any
caller) is **deferred** — it needs caller registries that do not exist
yet (non-goal per design doc §2).

The aioss reference has a `scanner/` module with `ghost.rs`,
`integrity.rs`, and `leak.rs` — but all three are stub implementations
with TODO comments. CROW implements the actual detection logic for
ghost/drift/integrity; leak remains deferred.

**Solution**: Implement the background scanner with real detection
logic for ghost allocations, `allocate_pos` drift, and record
integrity. Leak detection is scaffolded but returns "not implemented."

1. **Scanner loop** — create `app/crow-diskdb/src/scanner/mod.rs`:
   - `ScannerEngine` — owns a `DataGroupClient` (from R72), a
     `RecoveryEngine` (from R73), a `RecalcEngine` (from R74), and a
     `NodeContainer` (from R71). Runs as a background task.
   - `scanner_loop(node_container, journal, recovery, recalc, config)`:
     a. `sleep(scan_interval_secs)` (default 600 s / 10 min).
     b. If `config.detect_ghost_allocations`: run
        `scan_ghost_allocations()`.
     c. If `config.verify_record_integrity`: run
        `scan_record_integrity()`.
     d. Run `scan_allocate_pos_drift()` (always on — it's cheap and
        catches bugs).
     e. Optionally run `recalc_all()` (from R74) as a comprehensive
        drift check — can be enabled/disabled via config.
     f. Log a scan summary: zones scanned, issues found, duration.
     g. Repeat.
   - Each scan is independent and runs sequentially (matching aioss
     pattern). Scans are read-only — they detect and report, they do
     not modify state (except optional auto-correction, see below).

2. **Ghost allocation detection** — create
   `app/crow-diskdb/src/scanner/ghost.rs`:
   - `scan_ghost_allocations(node_container, journal) ->
     Result<GhostScanResult>`:
     a. For each owned node (disk-group), for each disk, for each zone:
        - Independently replay the journal for the zone (using R73's
          replay logic, into a separate bitmap — same as R74's
          `recalc_zone`).
        - Compare the replayed bitmap against the live in-memory
          bitmap.
        - **Ghost allocation**: a block is set in the replayed bitmap
          (allocated in journal) but the corresponding `FreeRecord`
          exists later in the journal (the block was freed). If the
          live bitmap still shows it as set, it's a ghost — the free
          was not applied to the live state (should not happen in
          normal operation, but can occur if a free batch flush
          partially failed).
        - Actually, the more common ghost scenario: a block has a
          `BusyRecord` in the journal but no corresponding
          `FreeRecord`, and the caller never uses the block (the
          allocate succeeded, the BusyRecord persisted, but the caller
          crashed before using it). This is a **leak**, not a ghost —
          leak detection is deferred. True ghost detection is: the
          live bitmap shows a block as busy, but replaying the journal
          shows it as free (the `FreeRecord` exists but was not
          applied to the live bitmap). This indicates the live state
          is stale.
        - For each ghost: record `(disk_uuid, zone_idx, zone_offset,
          size)`.
     b. Return `GhostScanResult { ghosts_found: u64, ghosts:
        Vec<GhostAllocation> }`.
   - **Auto-correction (optional, default off)**: if
     `config.auto_correct_ghosts` is true, clear the ghost block in
     the live bitmap and append a `FreeRecord` to the journal to make
     the durable state consistent. v1 defaults to off — report only,
     let the operator decide. The operator can trigger correction via
     an admin RPC.

3. **`allocate_pos` drift detection** — create
   `app/crow-diskdb/src/scanner/drift.rs`:
   - `scan_allocate_pos_drift(node_container, journal) ->
     Result<DriftScanResult>`:
     a. For each owned node, for each disk, for each zone:
        - Independently replay the journal (into a separate bitmap).
        - Compare the replayed `allocate_pos` against the live
          `allocate_pos`.
        - If they differ: record `(disk_uuid, zone_idx,
          live_allocate_pos, replayed_allocate_pos)`.
     b. Return `DriftScanResult { drift_found: u64, drifts:
        Vec<AllocatePosDrift> }`.
   - **Drift causes**: (1) CAS claim succeeded but rollback was
     incomplete (bug), (2) `allocate_pos` was not properly restored
     after a failed persist, (3) recovery did not correctly compute
     `allocate_pos` from the journal.
   - **Auto-correction (optional, default off)**: if
     `config.auto_correct_drift` is true, reload the zone from the
     journal (replace live state with replayed state). This is
     essentially re-running R73's `recover_zone()` for the drifted
     zone. v1 defaults to off — report only.

4. **Record integrity verification** — create
   `app/crow-diskdb/src/scanner/integrity.rs`:
   - `scan_record_integrity(node_container, journal) ->
     Result<IntegrityScanResult>`:
     a. For each owned node, for each disk, for each zone:
        - Load the `ZoneSnapshot` from the journal (if any).
        - Verify CRC: `snapshot.verify_checksum()`. If fails, record
          a `CorruptSnapshot { disk_uuid, zone_idx }`.
        - Load each `BusyRecord` / `FreeRecord` in the journal (via
          prefix scan). Verify that the record deserializes correctly
          (bincode). If a record fails to deserialize, record a
          `CorruptJournalRecord { disk_uuid, zone_idx, slot }`.
     b. Return `IntegrityScanResult { corrupt_snapshots: u64,
        corrupt_records: u64, details: Vec<CorruptRecord> }`.
   - **Handling**: corrupted snapshots are logged with a warning. The
     recovery engine (R73) already handles CRC failure by replaying
     from journal start. Corrupted journal records are logged — if a
     BusyRecord is corrupt, the block it represents is in an
     indeterminate state (allocated but metadata lost). v1 logs and
     reports; manual intervention required. Future: quarantine the
     zone (transition to Error state).

5. **Leak detection (scaffold)** — create
   `app/crow-diskdb/src/scanner/leak.rs`:
   - `scan_for_leaks(node_container) -> Result<LeakScanResult>`:
     - Returns `LeakScanResult { status: "deferred", message: "Leak
       detection requires caller registries (not yet implemented).
       Use ghost allocation detection for crash-related orphans." }`.
     - This is a scaffold — the interface exists so the scanner loop
       can call it, but it returns "deferred." Full implementation
       requires a caller-registry plugin interface (callers register
       "block liveness check" endpoints) that does not exist yet
       (non-goal per design doc §2).
   - **Future design**: when callers exist (object store, chunk
     service), they register a callback `is_block_alive(segment) ->
     bool`. The scanner iterates all allocated blocks, calls the
     callback, and marks blocks whose caller reports them as dead as
     leaked. Leaked blocks are freed (FreeRecord appended). This is a
     follow-up requirement.

6. **Scanner admin RPCs** — add to
   `app/crow-diskdb/src/grpc/admin.rs`:
   - `TriggerScan` — manually trigger a scan cycle (runs all enabled
     scans immediately, regardless of interval).
   - `GetScanStatus` — returns the last scan result summary
     (timestamps, counts, issues found).
   - Add corresponding proto messages to
     `lib/crow-protocol/src/proto/diskdb.proto`.

7. **Scanner metrics** — add to `app/crow-diskdb/src/metrics/mod.rs`
   (from R74):
   - `scanner_runs_total` (counter).
   - `scanner_duration_ms` (histogram).
   - `scanner_ghosts_found` (gauge, labeled by dg_id).
   - `scanner_drift_found` (gauge).
   - `scanner_corrupt_records` (gauge).
   - Updated by the scanner loop after each scan cycle.

**Scope** (expected changed files):
- `app/crow-diskdb/src/scanner/mod.rs` — `ScannerEngine`, scanner
  loop.
- `app/crow-diskdb/src/scanner/ghost.rs` — ghost allocation detection.
- `app/crow-diskdb/src/scanner/drift.rs` — `allocate_pos` drift
  detection.
- `app/crow-diskdb/src/scanner/integrity.rs` — CRC integrity
  verification.
- `app/crow-diskdb/src/scanner/leak.rs` — leak detection scaffold
  (deferred).
- `app/crow-diskdb/src/grpc/admin.rs` — `TriggerScan`,
  `GetScanStatus` handlers.
- `app/crow-diskdb/src/metrics/mod.rs` — scanner metrics.
- `app/crow-diskdb/src/lib.rs` — add `scanner` module.
- `lib/crow-protocol/src/proto/diskdb.proto` — add `TriggerScan`,
  `GetScanStatus` RPCs and messages.
- `app/crow-diskdb/src/main.rs` — spawn scanner loop.
- `app/crow-diskdb/src/config/mod.rs` (from R70) — add scanner
  auto-correct config flags (`auto_correct_ghosts`,
  `auto_correct_drift`, default false).

**Complexity**: Medium. The detection logic reuses R73's journal
replay and R74's recalc engine — the scanner is essentially a
periodic recalc with additional CRC checks and reporting. The main
work is the comparison logic and the reporting/admin surface. Ghost
and drift detection are the most valuable — they catch bugs in the
allocation engine and crash recovery edge cases. Leak detection is
explicitly deferred.

**Dependencies**: R70 (types, config), R71 (NodeContainer), R72
(DataGroupClient), R73 (RecoveryEngine, replay logic), R74 (RecalcEngine,
metrics). No dependency on R76–R77.

**Acceptance**:
- `ScannerEngine` runs a scan cycle every `scan_interval_secs` (default
  600 s). Each cycle runs ghost/drift/integrity scans sequentially.
  Unit test: mock node container, verify scan runs and returns
  results.
- `scan_ghost_allocations()` detects blocks that are busy in the live
  bitmap but free in the replayed journal. Unit test: create a zone,
  allocate + free a block, manually corrupt the live bitmap (set the
  freed bit), run ghost scan, verify the ghost is detected.
- `scan_allocate_pos_drift()` detects `allocate_pos` mismatch between
  live and replayed state. Unit test: manually advance live
  `allocate_pos` without a journal entry, run drift scan, verify
  drift detected.
- `scan_record_integrity()` detects CRC corruption in snapshots and
  deserialization failures in journal records. Unit test: write a
  snapshot, corrupt a byte, run integrity scan, verify corruption
  detected.
- `scan_for_leaks()` returns "deferred" status. Unit test.
- `TriggerScan` admin RPC runs a scan immediately and returns results.
  Integration test.
- `GetScanStatus` returns last scan summary. Integration test.
- Scanner metrics registered and updated after each scan. Integration
  test: run scan, verify `scanner_runs_total` incremented.
- Auto-correction (default off): when enabled, ghost/drift scans
  correct the live state. Unit test: enable auto-correct, run ghost
  scan, verify live bitmap corrected.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.
