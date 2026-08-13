<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R75: diskdb — Background Scanner (Ghost/Drift/Integrity Detection)

**Problem**: R72–R74 implement the allocation engine, crash recovery,
and space metrics. But the journal-based durability model (§7) introduces
live-state-vs-records consistency risks that are only caught if someone
runs a check:

- **Current behavior + impact** — R74's `RecalcEngine` (§11) replays
  each zone's journal into a throwaway bitmap and compares the
  **popcount** (busy-block count) against the live `DdbZone`. This
  detects count-level drift on demand (`RecalcDiskUsage` RPC), but:
  - It does not identify **which specific blocks** differ — only that
    the counts mismatch. An operator learns "zone 3 drifted" but not
    which bits are ghosts.
  - It is not periodic — nothing wakes it. Drift can persist
    indefinitely until an operator manually triggers a recalc.
  - It does not verify **record integrity** (CRC on `ZoneValue`
    snapshots, deserialization of `BusyBlockValue` / `FreeBlockValue`).
    R73's recovery checks CRC on the snapshot it loads, but a record
    that corrupts **after** recovery is never re-checked.
  - It does not validate **per-block state** — `BusyBlockValue.owner_chunk`
    (§14, ownership validation deferred from the free path) is never
    cross-checked after allocate.
  - Without periodic scanning, the crash-safety invariants in §7
    (ghost allocation: bit set in-memory, no record; ghost free: bit
    clear in-memory, `BusyBlockKey` still on disk) can violate
    silently. The block is either leaked (busy in memory, free in
    reality) or double-allocated (free in memory, busy in reality).

- **Design pointers** — §12 (Background Scanner) specifies
  ghost-allocation detection, bitmap drift detection, record
  integrity, and per-block state validation; leak detection is a §2
  non-goal (needs caller registries). §7 (crash-safety invariants)
  defines the two drift directions: ghost allocation and ghost free.
  §14 (Concurrency Model) defers ownership validation from the free
  path to the scanner. No direct aioss analog — new work (the aioss
  reference path cited in §15 is not accessible for verification).

- **Use scenarios**:
  - **Crash-mid-free** — a free RPC clears the live bitmap bit but
    crashes before the `batch_write` (Delete `BusyBlockKey` + Put
    `FreeBlockKey`) persists. The block is free in memory but busy in
    records. The scanner detects the ghost-free bit, reports it, and
    the operator triggers `RebuildZoneBitmap` (§7 strategy 1) to
    reload from records.
  - **Ungraceful shutdown with unflushed free batch** — (future, after
    R79 free-batching) unflushed frees leave `BusyBlockKey`s on disk
    with no matching live bitmap bits. The scanner reconciles by
    reporting ghost-free blocks; §12 design intent is that the scanner
    is the reconciliation path for unflushed batches.
  - **Ghost allocation after failed persist** — a CAS claim set the
    live bitmap bit but the `BusyBlockValue` persist failed and
    rollback was incomplete. The block is busy in memory, no record.
    The scanner detects the ghost-busy bit; auto-correct (if enabled)
    clears the live bit.
  - **Bit rot on a `ZoneValue` snapshot** — storage corruption flips a
    byte in the snapshot's `usage_bitmap`. The scanner's CRC check
    (`ZoneValueExt::verify_checksum`) fails; the scanner reports a
    corrupt snapshot. Recovery (R73) already handles this by falling
    back to strategy 1 (full scan) — the scanner just flags it for
    operator awareness.
  - **Corrupt `BusyBlockValue`** — a BusyBlockValue record fails to
    deserialize (bincode decode error). The scanner reports the
    corrupt record + slot; the block is in an indeterminate state
    (allocated but metadata lost). v1 logs and reports; manual
    intervention required.
  - **Ad-hoc consistency check** — an operator wants to verify cluster
    health before a maintenance window. They call `TriggerScan` and
    review the `GetScanStatus` result (ghosts found, drift found,
    corrupt records, last scan timestamp).
  - **Monitoring dashboard** — `scanner_ghosts_found` and
    `scanner_drift_found` gauges are scraped by the metrics system. A
    rising trend triggers an alert; the operator investigates via
    `GetScanStatus`.

**Solution**: A `BackgroundTask` that periodically replays each owned
zone's journal into a throwaway bitmap, compares it **bit-by-bit**
against the live `usage_bits` (extending R74's count-only recalc with
per-block ghost identification), verifies record CRC and
deserialization, validates `owner_chunk` well-formedness, and reports
findings via metrics + admin RPCs. Leak detection is scaffolded as
deferred.

1. **Scanner task** — new module `app/crow-diskdb/src/scanner/mod.rs`.
   `ScannerTask` implements the `BackgroundTask` trait (from
   `bg_task.rs`): `run_cycle` runs one scan cycle, `trigger` returns
   `Trigger::TimerFn` that reads `scan_interval_secs` from the shared
   `DdbConfig` handle (so config reloads take effect on the next tick),
   `name` returns `"scanner"`. Registered with `BgRunner` in `main.rs`
   alongside compaction, keep-alive, and reporting. Each cycle runs
   ghost/drift scan → integrity scan (gated by config flags) → log
   summary → update metrics. Scans are read-only — they detect and
   report; they do not modify live state (except optional
   auto-correction, default off).

2. **Ghost + bitmap drift detection** — new
   `app/crow-diskdb/src/scanner/ghost.rs`. `scan_ghosts(container, kv)`
   iterates owned disk-groups → disks → zones (via
   `DdbDiskGroupContainer::disk_group_ids` + `get_disk_group`), and for
   each zone:
   - Replays the journal into a throwaway `DdbZone` by calling
     `recover_zone_inner` (R73, strategy 2) with strategy-1 fallback
     (`rebuild_zone_bitmap_full_scan`) on `JournalScanGcGap` or
     `SnapshotCrcFail` — same fallback logic as `RecalcEngine`.
   - Compares `replayed.usage_bits` against `live.usage_bits`
     **bit-by-bit** (not just popcount). Reports two categories:
     - **Ghost-busy**: bit set in live, clear in replayed (allocated
       in memory, no `BusyBlockKey` — §7 ghost allocation).
     - **Ghost-free**: bit clear in live, set in replayed (freed in
       memory, `BusyBlockKey` still on disk — §7 ghost free).
   - Returns `GhostScanResult { ghost_busy: u64, ghost_free: u64,
     details: Vec<GhostBlock> }` where each `GhostBlock` records
     `(disk_id, zone_index, unit_offset, unit_count, direction)`.
   - This extends R74's `RecalcEngine::recalc_zone` (which compares
     only `live_busy_blocks == replayed_busy_blocks`) with per-block
     identification. The scanner may call `recalc_all` first as a
     cheap count-level pre-check and skip the per-bit diff for zones
     where counts match (optimization; correctness does not depend on
     it).
   - **Auto-correction (optional, default off)**: if
     `config.scanner.auto_correct_drift` is true, replace the live
     zone's `usage_bits` + `used_count` with the replayed values
     (essentially re-running R73's `recover_zone_inner` against the
     live zone). v1 defaults to off — report only; the operator
     triggers correction via `RebuildZoneBitmap` (§7).

3. **Record integrity verification** — new
   `app/crow-diskdb/src/scanner/integrity.rs`.
   `scan_integrity(container, kv)` iterates owned zones and for each:
   - Calls `DdbKvClient::read_zone_records` (R72) to load
     `ZoneRecords { zone_value, busy, free }`.
   - If `zone_value` is present, verifies CRC via
     `ZoneValueExt::verify_checksum`. On failure, records a
     `CorruptSnapshot { disk_id, zone_index }`.
   - Validates each `BusyRecord` and `FreeRecord` deserialized
     successfully (`read_zone_records` already decodes via bincode;
     a decode failure surfaces as an error entry in `ZoneRecords` or
     an `Err` — the scanner reports it as
     `CorruptJournalRecord { disk_id, zone_index, key }`).
   - **Per-block state validation** (§12): for each `BusyRecord`,
     validates `value.owner_chunk` is well-formed (non-zero `ChunkId`).
     A zeroed or malformed `owner_chunk` is reported as
     `OwnerMismatch { disk_id, zone_index, unit_offset }`. Full
     liveness cross-check against the caller's in-memory `Segment` is
     deferred (needs caller registries, same §2 non-goal as leak
     detection).
   - Returns `IntegrityScanResult { corrupt_snapshots: u64,
     corrupt_records: u64, owner_mismatches: u64, details: Vec<...> }`.
   - **Handling**: corrupted snapshots are logged with a warning; R73
     recovery already handles CRC failure by replaying from journal
     start (strategy 1). Corrupted journal records are logged — if a
     `BusyBlockValue` is corrupt, the block is indeterminate. v1 logs
     and reports; manual intervention. Future: quarantine the zone
     (transition to `Error` state).

4. **Leak detection scaffold** — new
   `app/crow-diskdb/src/scanner/leak.rs`. `scan_for_leaks()` returns
   `LeakScanResult { status: "deferred", message: "Leak detection
   requires caller registries (not yet implemented). Use ghost
   allocation detection for crash-related orphans." }`. The interface
   exists so the scanner loop can call it, but it returns "deferred."
   Full implementation requires a caller-registry plugin interface
   (callers register `is_block_alive(segment) -> bool` callbacks) that
   does not exist yet (§2 non-goal).

5. **Scanner admin RPCs** — add `TriggerScan` and `GetScanStatus` to
   `DiskdbService` in `app/crow-diskdb/src/service/diskdb_service.rs`
   (following the existing `RebuildZoneBitmap` / `RecalcDiskUsage`
   pattern — no separate admin service). Add corresponding messages to
   `lib/crow-protocol/src/proto/diskdb_service.proto`.
   - `TriggerScan` — runs all enabled scans immediately (bypasses the
     timer) and returns the result summary. Uses an `Arc<Notify>` to
     wake the scanner task (the scanner task's `trigger` is
     `TimerFn`, so `TriggerScan` needs an additional `Notify` channel
     or a shared `AtomicBool` "scan requested" flag checked each tick).
   - `GetScanStatus` — returns the last scan summary: timestamps,
     ghosts found, drift found, corrupt records, duration.

6. **Scanner metrics** — add to `DiskdbMetrics::register` in
   `app/crow-diskdb/src/metrics.rs` (R74's metrics registry pattern):
   - `scanner_runs_total` (counter).
   - `scanner_duration_ms` (histogram or summary).
   - `scanner_ghosts_found` (gauge, labeled by disk-group).
   - `scanner_drift_found` (gauge, labeled by disk-group).
   - `scanner_corrupt_records` (gauge).
   - Updated by the scanner task after each scan cycle.

7. **Config** — extend `ScannerConfig` in
   `app/crow-diskdb/src/ddb_config.rs`. `scan_interval_secs` (default
   600), `detect_ghost_allocations` (default true), and
   `verify_record_integrity` (default true) already exist. Add:
   - `auto_correct_drift: bool` (default false) — enable live-bitmap
     auto-correction in the ghost scan.
   - `detect_owner_mismatch: bool` (default false) — enable per-block
     `owner_chunk` validation in the integrity scan (gated because it
     piggybacks on `read_zone_records`, which is already loaded when
     `verify_record_integrity` is true; the flag controls whether
     owner validation runs).

```
                    ┌──────────────┐
   TriggerScan ──►  │  ScannerTask │  ◄── TimerFn(scan_interval_secs)
   (Notify/flag)    └──────┬───────┘
                          │ run_cycle
              ┌───────────┼───────────┐
              ▼           ▼           ▼
      ┌───────────┐ ┌───────────┐ ┌───────────┐
      │ ghost.rs  │ │integrity. │ │  leak.rs  │
      │           │ │    rs     │ │           │
      │ replay +  │ │ CRC +     │ │ "deferred"│
      │ bit-diff  │ │ decode +  │ │ (scaffold)│
      │           │ │ owner chk │ │           │
      └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
            │             │             │
            └─────────────┼─────────────┘
                          ▼
              ┌───────────────────────┐
              │  metrics + GetScanStatus │
              │  (DiskdbMetrics + RPC)  │
              └───────────────────────┘
```

**Edge cases at a glance**:
- `ZoneValue` CRC fail → scanner reports `CorruptSnapshot`; replay
  falls back to strategy 1 (`rebuild_zone_bitmap_full_scan`), same as
  R73/R74.
- Journal scan GC gap → strategy 1 fallback (reuse R73
  `RecoveryError::JournalScanGcGap` handling).
- `BusyRecord` / `FreeRecord` decode failure → report
  `CorruptJournalRecord`, skip the block, continue scan.
- Empty zone (no records, no snapshot) → no drift, no ghosts, no
  integrity issues; counted as scanned.
- Scanner cycle overlaps with compaction → scans are read-only
  (snapshot the live `usage_bits`); no conflict with compaction's
  writes.
- Auto-correct off (default) → report only; operator uses
  `RebuildZoneBitmap` to correct.
- `TriggerScan` while a scan is in progress → return "scan in
  progress" or queue (v1: return current status, do not overlap).
- Degraded mode (group-0 / data-group unreachable) → scanner skips
  zones it cannot read; logs a warning; resumes next cycle.

**Dependencies**: R70 (types, `ScannerConfig` scaffold), R71
(`DdbDiskGroupContainer` — disk-group/disk/zone iteration), R72
(`DdbKvClient::read_zone_records`, `journal_scan_busy`,
`journal_scan_free`), R73 (`recover_zone_inner`,
`rebuild_zone_bitmap_full_scan`, `RecoveryError` fallback), R74
(`RecalcEngine` — optional count-level pre-check; `DiskdbMetrics`
registry pattern). No dependency on R76–R77. R79 (free batching) will
increase ghost-free frequency but is not a blocker — the scanner
detects the same drift regardless of batching.

**Acceptance**:

- **Scanner task (work item 1)**:
  - `ScannerTask` registered with `BgRunner`, runs a scan cycle every
    `scan_interval_secs` (default 600). Setup: construct `ScannerTask`
    with `DdbKvClient` + `DdbDiskGroupContainer` + `RecalcEngine`,
    register with `BgRunner`, spawn → assert `run_cycle` called after
    interval, `scanner_runs_total` incremented. Unit test.
  - Config reload: change `scan_interval_secs` in the shared
    `ArcSwap<DdbConfig>` handle → assert next tick uses the new
    interval (not the old). Unit test.

- **Ghost + bitmap drift detection (work item 2)**:
  - `scan_ghosts` detects ghost-busy: setup a zone, allocate a block
    (persist `BusyBlockValue`), manually set the live bitmap bit for a
    **different** block with no record → run `scan_ghosts` → assert
    the ghost-busy block is in `details` with direction `GhostBusy`.
    Unit test.
  - `scan_ghosts` detects ghost-free: setup a zone, allocate + free a
    block (persist `BusyBlockValue` then persist `FreeBlockValue` +
    delete `BusyBlockValue`), manually **set** the live bitmap bit
    back (simulating crash-mid-free where the free didn't persist) →
    run `scan_ghosts` → assert the ghost-free block is in `details`
    with direction `GhostFree`. Unit test.
  - `scan_ghosts` reports zero ghosts when live and replayed bitmaps
    match: setup a zone, allocate blocks, persist all records → run
    `scan_ghosts` → assert `ghost_busy == 0`, `ghost_free == 0`.
    Unit test.
  - `scan_ghosts` falls back to strategy 1 on `SnapshotCrcFail`:
    corrupt the `ZoneValue` CRC → run `scan_ghosts` → assert it does
    not panic, falls back to `rebuild_zone_bitmap_full_scan`, and
    still reports ghosts. Unit test.
  - Auto-correction (default off): enable `auto_correct_drift`, run
    `scan_ghosts` on a zone with ghost-busy → assert the live
    `usage_bits` and `used_count` are corrected to match the replayed
    values. Unit test.
  - Invariant guarded: replayed bitmap is the source of truth; live
    bitmap bits with no backing record are reported as ghost-busy,
    live bitmap bits clear with a backing `BusyBlockKey` are reported
    as ghost-free. Unit test.

- **Record integrity (work item 3)**:
  - `scan_integrity` detects CRC corruption: write a `ZoneValue` with
    a valid CRC, corrupt one byte in `usage_bitmap` (without updating
    CRC) → run `scan_integrity` → assert `corrupt_snapshots == 1`
    and the zone is in `details`. Unit test.
  - `scan_integrity` detects deserialization failure: write a
    `BusyBlockValue`, corrupt the value bytes so bincode decode fails
    → run `scan_integrity` → assert `corrupt_records >= 1`. Unit
    test.
  - `scan_integrity` detects `owner_chunk` mismatch: enable
    `detect_owner_mismatch`, write a `BusyBlockValue` with
    `owner_chunk = 0` → run `scan_integrity` → assert
    `owner_mismatches == 1`. Unit test.
  - `scan_integrity` on a clean zone: all records valid, CRC valid →
    assert `corrupt_snapshots == 0`, `corrupt_records == 0`,
    `owner_mismatches == 0`. Unit test.

- **Leak scaffold (work item 4)**:
  - `scan_for_leaks` returns `LeakScanResult { status: "deferred",
    ... }` without panicking. Unit test.

- **Admin RPCs (work item 5)**:
  - `TriggerScan` runs a scan immediately and returns the result
    summary (ghosts, drift, corrupt records, duration). Setup: start
    a `DiskdbService` with a mock container + kv, call `TriggerScan`
    → assert response contains scan results. Integration test.
  - `GetScanStatus` returns the last scan summary after a scan has
    run. Setup: run a scan (via timer or `TriggerScan`), call
    `GetScanStatus` → assert timestamps and counts match the last
    scan. Integration test.
  - `TriggerScan` while a scan is in progress → assert response
    indicates "scan in progress" (no overlap). Integration test.

- **Metrics (work item 6)**:
  - `scanner_runs_total` incremented after each scan cycle.
    `scanner_duration_ms` recorded. `scanner_ghosts_found` /
    `scanner_drift_found` / `scanner_corrupt_records` gauges set to
    the last scan's counts. Setup: run a scan with known ghost count →
    assert gauges reflect the count. Integration test.

- **Config (work item 7)**:
  - `ScannerConfig::default` has `scan_interval_secs == 600`,
    `detect_ghost_allocations == true`, `verify_record_integrity ==
    true`, `auto_correct_drift == false`, `detect_owner_mismatch ==
    false`. Unit test.
  - Config deserialized from TOML with scanner section → assert fields
    parsed correctly. Unit test.

- **Edge cases**:
  - Empty zone (no records, no snapshot): `scan_ghosts` and
    `scan_integrity` report zero issues, zone counted as scanned.
    Unit test.
  - Degraded mode (data-group unreachable): scanner skips unreadable
    zones, logs a warning, does not panic. Unit test (mock kv returns
    `Unavailable`).
  - Scanner + compaction concurrency: run `scan_ghosts` while
    compaction writes a new `ZoneValue` → assert scanner does not
    panic and reports a consistent result (read-only snapshot).
    Integration test.

- `pixi run test-diskdb`, `pixi run cargo fmt --all -- --check`, and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
