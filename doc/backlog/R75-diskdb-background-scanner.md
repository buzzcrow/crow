<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R75: diskdb — Background Scanner (Consistency Check + Integrity Detection)

**Problem**: R72–R74 implement the allocation engine, crash recovery,
and space metrics. The free path now persists before clearing the
bitmap (eliminating the double-allocate window), and R73's recovery
rebuilds the bitmap from records on restart. But during uptime, there
is no mechanism to detect and reconcile live-state drift, catch
record corruption early, or give operators visibility into cluster
health.

- **Current behavior + impact** — R74's `RecalcEngine` (§11) replays
  each zone's journal into a throwaway bitmap and compares the
  **popcount** (busy-block count) against the live `DdbZone`. This
  detects count-level drift on demand (`RecalcDiskUsage` RPC), but:
  - It does not identify **which specific blocks** differ — only that
    the counts mismatch. An operator learns "zone 3 drifted" but not
    which bits to reclaim.
  - It is not periodic — nothing wakes it. Drift can persist
    indefinitely until an operator manually triggers a recalc.
    Ghost-busy blocks (bit set, no record — from a crash between
    allocate Phase 1 and Phase 2, or a crash after free persist but
    before bitmap clear) waste capacity until the next restart.
  - It does not verify **record integrity** (CRC on `ZoneValue`
    snapshots, deserialization of `BusyBlockValue` / `FreeBlockValue`).
    R73's recovery checks CRC on the snapshot it loads at startup, but
    a record that corrupts **after** recovery (bit rot, storage error)
    is never re-checked until the next restart — by then, recovery
    itself may fail.
  - It does not validate **per-block state** — `BusyBlockValue.owner_chunk`
    (§14, ownership validation deferred from the free path) is never
    cross-checked after allocate.
  - There is no **operator visibility** into cluster health during
    uptime — no metrics, no admin RPC to trigger a check, no status
    query. An operator has no signal that drift or corruption is
    accumulating until a restart surfaces it.

- **Design pointers** — §12 (Background Scanner) specifies
  ghost-allocation detection, bitmap drift detection, record
  integrity, and per-block state validation; leak detection is a §2
  non-goal (needs caller registries). §7 (crash-safety invariants)
  defines the drift directions; after the free-path fix, the remaining
  drift is ghost-busy (bit set, no record) — the safe direction
  (wasted space, not data risk). §14 (Concurrency Model) defers
  ownership validation from the free path to the scanner. No direct
  aioss analog — new work (the aioss reference path cited in §15 is
  not accessible for verification).

- **Data-safety principle** — a busy block may have data written to
  it. The scanner's first priority is to **never free a block that
  might have data**. When the scanner finds drift and the true state
  is uncertain (corrupt records, conflicting signals), it defaults to
  **busy** (keep the bit set, keep the block allocated). Wasting space
  is always preferable to freeing a block with data. The scanner only
  clears a bit when records confidently say "free" (no `BusyBlockKey`,
  records intact and readable). Specifically:
  - **Ghost-busy** (bit set, no `BusyBlockKey`, records intact):
    records are authoritative → block is free → safe to clear the
    bit. Auto-correction allowed.
  - **Ghost-free** (bit clear, `BusyBlockKey` exists, records intact):
    records are authoritative → block is busy → set the bit. Data may
    be written. Auto-correction allowed. (Cannot happen from the
    fixed free path, but the scanner detects it in case of bugs,
    hardware errors, or future code changes.)
  - **Corrupt `BusyBlockValue`** (key exists but undecodable):
    uncertain → keep busy, report for manual intervention. No
    auto-correction — the block might have data.
  - **Corrupt `ZoneValue` snapshot** (CRC fail): fall back to
    strategy 1 (full scan from records). If records are intact, use
    records as truth. If records are also corrupt → keep busy.

- **Use scenarios**:
  - **Crash between allocate phases** — a CAS claim sets the live
    bitmap bit but diskdb crashes before the `BusyBlockValue` persist.
    On restart, R73 recovery clears the bit (no record). But if the
    crash doesn't take the process down (bug, partial failure), the
    ghost-busy bit wastes capacity. The scanner detects it, and with
    auto-correct enabled, clears the bit — reclaiming the space
    without a restart.
  - **Crash after free persist, before bitmap clear** — the free path
    persists the `FreeBlockValue` + deletes the `BusyBlockKey`, then
    crashes before clearing the live bitmap bit. The block is free on
    disk but shows busy in memory (ghost-busy). The allocator won't
    re-hand it (bit is set), so no data risk — just wasted space. The
    scanner detects the ghost-busy bit and clears it (records
    confidently say free: no `BusyBlockKey`).
  - **Bit rot on a `ZoneValue` snapshot** — storage corruption flips a
    byte in the snapshot's `usage_bitmap` after recovery. The
    scanner's CRC check (`ZoneValueExt::verify_checksum`) fails; the
    scanner reports a `CorruptSnapshot`. The ghost scan falls back to
    strategy 1 (full scan from records) — if records are intact, the
    scanner still identifies drift correctly. The operator is alerted
    early, before the next restart's recovery is affected.
  - **Corrupt `BusyBlockValue`** — a `BusyBlockValue` record fails to
    deserialize (bincode decode error) due to storage corruption. The
    scanner reports the corrupt record + key. The block is in an
    indeterminate state (the key exists so the block is busy, but
    metadata is lost). Per the data-safety principle, the scanner
    **keeps the block busy** — it does not free it, because data may
    be written. The operator investigates and decides whether to
    quarantine the zone or attempt manual recovery.
  - **Bug or hardware error causing ghost-free** — despite the
    free-path fix, a bug in the CAS logic or a hardware memory error
    could clear a live bitmap bit while the `BusyBlockKey` still
    exists on disk. The allocator would re-hand the block
    (double-allocate). The scanner detects the ghost-free bit and
    **sets it back** (records say busy, data may be written) —
    preventing further re-allocation. This is a defense-in-depth
    check: the free-path fix prevents the known cause, the scanner
    catches unknown causes.
  - **Ad-hoc consistency check** — an operator wants to verify cluster
    health before a maintenance window. They call `TriggerScan` and
    review the `GetScanStatus` result (ghosts found, drift found,
    corrupt records, last scan timestamp). If the result is clean,
    they proceed with confidence.
  - **Monitoring dashboard** — `scanner_ghosts_found` and
    `scanner_drift_found` gauges are scraped by the metrics system. A
    rising trend triggers an alert; the operator investigates via
    `GetScanStatus` before the next restart.

**Solution**: A `BackgroundTask` that periodically replays each owned
zone's journal into a throwaway bitmap, compares it **bit-by-bit**
against the live `usage_bits` (extending R74's count-only recalc with
per-block ghost identification), verifies record CRC and
deserialization, validates `owner_chunk` well-formedness, and reports
findings via metrics + admin RPCs. The scanner **skips active zones**
(zones in the disk's `active_zone_context` — the allocator is actively
handing blocks from them, so transient drift is expected) and **skips
zones being compacted** (compaction is mid-merge of free records into
a snapshot). For non-active zones, drift detection uses a **re-verify
step** to filter transient states caused by in-flight allocate/free
operations. Auto-correction (when enabled) follows the data-safety
principle: clear ghost-busy bits only when records are intact and
authoritative; set ghost-free bits back; never free a block with a
corrupt or uncertain record. Compaction remains a separate
`BackgroundTask` (different cadence, different purpose — write vs
read); the scanner coordinates by skipping zones with an in-progress
compaction flag. Leak detection is scaffolded as deferred.

1. **Scanner task** — new module `app/crow-diskdb/src/scanner/mod.rs`.
   `ScannerTask` implements the `BackgroundTask` trait (from
   `bg_task.rs`): `run_cycle` runs one scan cycle, `trigger` returns
   `Trigger::TimerFn` that reads `scan_interval_secs` from the shared
   `DdbConfig` handle (so config reloads take effect on the next tick),
   `name` returns `"scanner"`. Registered with `BgRunner` in `main.rs`
   alongside compaction, keep-alive, and reporting. Each cycle runs
   ghost/drift scan → integrity scan (gated by config flags) → log
   summary → update metrics. Scans are read-only by default — they
   detect and report; auto-correction is opt-in (default off).

2. **Ghost + bitmap drift detection** — new
   `app/crow-diskdb/src/scanner/ghost.rs`. `scan_ghosts(container, kv)`
   iterates owned disk-groups → disks → zones (via
   `DdbDiskGroupContainer::disk_group_ids` + `get_disk_group`), and for
   each zone:
   - **Skip active zones**: a zone currently in the disk's
     `active_zone_context` (the RCU-published set the allocator
     round-robins through) is skipped — the allocator is actively
     handing blocks from it, so transient drift (bit set, no record
     yet; or record persisted, bit not yet cleared) is expected. The
     scanner checks these zones on a later cycle when they rotate out
     of the active set (become full or the set rotates). Skipped zones
     are counted in the scan summary as `skipped_active`.
   - **Skip zones being compacted**: check the zone's compaction flag
     (an `AtomicBool` on `DdbZone`, set by `CompactionEngine` at the
     start of `compact_zone` and cleared on completion). If set, skip
     the zone — compaction is mid-merge and the bitmap is changing.
     Counted as `skipped_compacting`.
   - Replays the journal into a throwaway `DdbZone` by calling
     `recover_zone_inner` (R73, strategy 2) with strategy-1 fallback
     (`rebuild_zone_bitmap_full_scan`) on `JournalScanGcGap` or
     `SnapshotCrcFail` — same fallback logic as `RecalcEngine`.
   - Compares `replayed.usage_bits` against `live.usage_bits`
     **bit-by-bit** (not just popcount). Reports two categories:
     - **Ghost-busy**: bit set in live, clear in replayed (no
       `BusyBlockKey` — records say free). Safe direction: wasted
       space, no data risk.
     - **Ghost-free**: bit clear in live, set in replayed
       (`BusyBlockKey` exists — records say busy). Dangerous
       direction: allocator may re-hand the block. Cannot happen from
       the fixed free path, but detected as defense-in-depth.
   - **Re-verify step**: if drift is detected, the scanner does not
     report immediately. It waits a short delay (default 1s,
     configurable via `reverify_delay_ms`) and re-snapshots the live
     `usage_bits`, then re-compares. If the drift disappears, it was
     transient (an in-flight allocate/free completed during the
     delay) — skip it. If the drift persists, it is real — report it.
     This filters false positives from the allocate/free Phase 1→2
     windows without holding any lock. The re-verify does not re-read
     the journal (records don't change during the delay); only the
     live bitmap is re-snapshotted.
   - Returns `GhostScanResult { ghost_busy: u64, ghost_free: u64,
     skipped_active: u64, skipped_compacting: u64,
     details: Vec<GhostBlock> }` where each `GhostBlock` records
     `(disk_id, zone_index, unit_offset, unit_count, direction)`.
   - This extends R74's `RecalcEngine::recalc_zone` (which compares
     only `live_busy_blocks == replayed_busy_blocks`) with per-block
     identification. The scanner may call `recalc_all` first as a
     cheap count-level pre-check and skip the per-bit diff for zones
     where counts match (optimization; correctness does not depend on
     it).
   - **Auto-correction (optional, default off)**: if
     `config.scanner.auto_correct_drift` is true, correct the live
     bitmap per the data-safety principle (only after re-verify
     confirms the drift is persistent):
     - Ghost-busy: clear the live bit (records confidently say free).
     - Ghost-free: set the live bit back (records say busy, data may
       be written).
     - If the replay fell back to strategy 1 due to
       `SnapshotCrcFail`, **do not auto-correct** — the snapshot was
       corrupt, and while strategy 1 rebuilds from records, the
       corruption signal means something is wrong; report only and
       let the operator decide.
     - v1 defaults to off — report only; the operator triggers
       correction via `RebuildZoneBitmap` (§7) or enables
       auto-correct in config.

3. **Record integrity verification** — new
   `app/crow-diskdb/src/scanner/integrity.rs`.
   `scan_integrity(container, kv)` iterates owned zones (same skip
   logic as ghost scan: skip active zones and zones being compacted)
   and for each:
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
   - **Handling (data-safety principle)**: corrupted snapshots are
     logged with a warning; the ghost scan falls back to strategy 1.
     Corrupted `BusyBlockValue` records are logged — the block is
     kept busy (the key exists, data may be written); **no
     auto-correction frees a block with a corrupt record**. v1 logs
     and reports; manual intervention required. Future: quarantine
     the zone (transition to `Error` state).

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
     auto-correction in the ghost scan (follows data-safety
     principle: clears ghost-busy, sets ghost-free, never frees
     corrupt-record blocks; only after re-verify confirms persistent
     drift).
   - `detect_owner_mismatch: bool` (default false) — enable per-block
     `owner_chunk` validation in the integrity scan (gated because it
     piggybacks on `read_zone_records`, which is already loaded when
     `verify_record_integrity` is true; the flag controls whether
     owner validation runs).
   - `reverify_delay_ms: u32` (default 1000) — delay before
     re-snapshot the live bitmap to filter transient drift. Set to 0
     to disable re-verify (report immediately, may include false
     positives from in-flight operations).

8. **Compaction coordination** — add an `AtomicBool` field
   `compacting: AtomicBool` to `DdbZone` in
   `app/crow-diskdb/src/model/zone.rs`. `CompactionEngine::compact_zone`
   sets it to `true` at the start and `false` on completion (success
   or error — use a guard/RAII pattern to ensure it's cleared on early
   return). The scanner checks this flag before scanning a zone and
   skips if set. This is a lightweight coordination mechanism — no
   lock, no waiting; the scanner simply tries again next cycle.
   Compaction remains a separate `BackgroundTask` (different cadence:
   300s vs 600s; different purpose: write vs read-check). The two
   tasks do not block each other — they skip overlapping zones.

```
                    ┌──────────────┐
   TriggerScan ──►  │  ScannerTask │  ◄── TimerFn(scan_interval_secs)
   (Notify/flag)    └──────┬───────┘
                          │ run_cycle
                          ▼
         ┌─────────────────────────────────┐
         │  for each owned zone:           │
         │  skip if in active_zone_context │
         │  skip if compacting flag is set │
         └────────────────┬────────────────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
      ┌───────────┐ ┌───────────┐ ┌───────────┐
      │ ghost.rs  │ │integrity. │ │  leak.rs  │
      │           │ │    rs     │ │           │
      │ replay +  │ │ CRC +     │ │ "deferred"│
      │ bit-diff  │ │ decode +  │ │ (scaffold)│
      │ → re-verify│ │ owner chk │ │           │
      └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
            │             │             │
            └─────────────┼─────────────┘
                          ▼
         ┌──────────────────────────────────┐
         │  data-safety principle applied:  │
         │  ghost-busy → clear (records OK) │
         │  ghost-free → set (data risk)    │
         │  corrupt rec → keep busy, report │
         └──────────────────────────────────┘
                          ▼
              ┌───────────────────────┐
              │  metrics + GetScanStatus │
              │  (DiskdbMetrics + RPC)  │
              └───────────────────────┘
```

**Edge cases at a glance**:
- `ZoneValue` CRC fail → scanner reports `CorruptSnapshot`; replay
  falls back to strategy 1 (`rebuild_zone_bitmap_full_scan`), same as
  R73/R74. No auto-correct when fallback was used (corruption signal).
- Journal scan GC gap → strategy 1 fallback (reuse R73
  `RecoveryError::JournalScanGcGap` handling).
- `BusyRecord` / `FreeRecord` decode failure → report
  `CorruptJournalRecord`, keep the block busy (key exists, data may
  be written), continue scan. No auto-correct.
- Empty zone (no records, no snapshot) → no drift, no ghosts, no
  integrity issues; counted as scanned.
- **Active zone (in `active_zone_context`)** → skipped; counted as
  `skipped_active`. Checked on a later cycle when it rotates out.
- **Zone being compacted** (`compacting` flag set) → skipped; counted
  as `skipped_compacting`. Checked on a later cycle.
- **Transient drift (in-flight allocate/free)** → re-verify step
  filters it: wait `reverify_delay_ms`, re-snapshot live bitmap,
  re-compare. If drift disappears, it was transient — skip. If
  persistent, report.
- **All zones active or compacting** → scan cycle completes with
  zero zones scanned; logged as a warning (the scanner is starved).
  The operator can increase `scan_interval_secs` or trigger an ad-hoc
  scan during a low-traffic window.
- Auto-correct off (default) → report only; operator uses
  `RebuildZoneBitmap` to correct.
- `TriggerScan` while a scan is in progress → return "scan in
  progress" (v1: return current status, do not overlap).
- Degraded mode (group-0 / data-group unreachable) → scanner skips
  zones it cannot read; logs a warning; resumes next cycle.
- Records and bitmap both suspect (corrupt snapshot + corrupt
  records) → keep all blocks busy, report for manual intervention.
  Never free a block when the durable state cannot be confidently
  determined.

**Dependencies**: R70 (types, `ScannerConfig` scaffold), R71
(`DdbDiskGroupContainer` — disk-group/disk/zone iteration;
`DdbDisk::active_zone_context` for active-zone skip), R72
(`DdbKvClient::read_zone_records`, `journal_scan_busy`,
`journal_scan_free`), R73 (`recover_zone_inner`,
`rebuild_zone_bitmap_full_scan`, `RecoveryError` fallback;
`CompactionEngine` — coordination via `compacting` flag on `DdbZone`),
R74 (`RecalcEngine` — optional count-level pre-check;
`DiskdbMetrics` registry pattern). No dependency on R76–R77. R79 (free
batching) does not change scanner scope — with
persist-before-bitmap-clear, unflushed frees on crash leave bits set
(ghost-busy, self-correcting); the scanner reclaims the wasted space.

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
  - `scan_ghosts` detects ghost-busy: setup a zone (not in active
    set, not compacting), manually set a live bitmap bit for a block
    with no `BusyBlockKey` on disk → run `scan_ghosts` → assert the
    ghost-busy block is in `details` with direction `GhostBusy`.
    Unit test.
  - `scan_ghosts` detects ghost-free: setup a zone (not in active
    set, not compacting), persist a `BusyBlockValue` on disk,
    manually **clear** the live bitmap bit (simulating a
    bug/hardware error that cleared the bit while the record exists)
    → run `scan_ghosts` → assert the ghost-free block is in
    `details` with direction `GhostFree`. Unit test.
  - `scan_ghosts` reports zero ghosts when live and replayed bitmaps
    match: setup a zone, allocate blocks, persist all records → run
    `scan_ghosts` → assert `ghost_busy == 0`, `ghost_free == 0`.
    Unit test.
  - `scan_ghosts` falls back to strategy 1 on `SnapshotCrcFail`:
    corrupt the `ZoneValue` CRC → run `scan_ghosts` → assert it does
    not panic, falls back to `rebuild_zone_bitmap_full_scan`, and
    still reports ghosts. Unit test.
  - **Skip active zones**: add a zone to the disk's
    `active_zone_context` → run `scan_ghosts` → assert the zone is
    counted as `skipped_active` and not in `details`. Unit test.
  - **Skip compacting zones**: set the zone's `compacting` flag →
    run `scan_ghosts` → assert the zone is counted as
    `skipped_compacting` and not in `details`. Unit test.
  - **Re-verify filters transient drift**: setup a zone with a
    transient ghost-busy (bit set, no record — simulate by setting
    the bit after the journal replay but before the re-verify) →
    run `scan_ghosts` with `reverify_delay_ms = 100` → during the
    delay, clear the bit (simulating the allocate rollback
    completing) → assert the ghost is **not** reported (transient,
    filtered by re-verify). Unit test.
  - **Re-verify confirms persistent drift**: setup a zone with a
    persistent ghost-busy (bit set, no record) → run `scan_ghosts`
    with `reverify_delay_ms = 100` → do not change the bit during
    the delay → assert the ghost **is** reported (persistent,
    confirmed by re-verify). Unit test.
  - **Auto-correction — ghost-busy (data-safety principle)**: enable
    `auto_correct_drift`, run `scan_ghosts` on a zone with a
    persistent ghost-busy bit (bit set, no `BusyBlockKey`, records
    intact, re-verify confirms) → assert the live bit is **cleared**
    and `used_count` is decremented (records confidently say free).
    Unit test.
  - **Auto-correction — ghost-free (data-safety principle)**: enable
    `auto_correct_drift`, run `scan_ghosts` on a zone with a
    persistent ghost-free bit (bit clear, `BusyBlockKey` exists,
    records intact, re-verify confirms) → assert the live bit is
    **set back** and `used_count` is incremented (records say busy,
    data may be written). Unit test.
  - **Auto-correction — no correct on fallback (data-safety
    principle)**: enable `auto_correct_drift`, corrupt the
    `ZoneValue` CRC so the scan falls back to strategy 1 → assert
    the scanner reports the drift but does **not** auto-correct
    (corruption signal → report only). Unit test.
  - Invariant guarded: replayed bitmap (from intact records) is the
    source of truth; live bits with no backing record are ghost-busy,
    live bits clear with a backing `BusyBlockKey` are ghost-free.
    Re-verify confirms persistence before reporting or correcting.
    Unit test.

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
  - **Data-safety on corrupt record**: a `BusyBlockValue` fails to
    deserialize → assert the scanner reports it but does **not** free
    the block (the block stays busy; no `FreeBlockValue` is written,
    no bitmap clear). Unit test.

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
    false`, `reverify_delay_ms == 1000`. Unit test.
  - Config deserialized from TOML with scanner section → assert fields
    parsed correctly. Unit test.

- **Compaction coordination (work item 8)**:
  - `DdbZone::compacting` flag defaults to `false`. Unit test.
  - `CompactionEngine::compact_zone` sets `compacting = true` at
    start, `false` on success. Unit test (verify flag is false after
    `compact_zone` returns Ok).
  - `CompactionEngine::compact_zone` sets `compacting = true` at
    start, `false` on error (RAII guard clears it). Unit test (mock
    kv error, verify flag is false after `compact_zone` returns Err).

- **Edge cases**:
  - Empty zone (no records, no snapshot): `scan_ghosts` and
    `scan_integrity` report zero issues, zone counted as scanned.
    Unit test.
  - Degraded mode (data-group unreachable): scanner skips unreadable
    zones, logs a warning, does not panic. Unit test (mock kv returns
    `Unavailable`).
  - **All zones active or compacting**: scan cycle completes with
    zero zones scanned; logged as a warning. Unit test (set all zones
    active, run scan, assert `skipped_active == zone_count`,
    `details` is empty).
  - **Scanner + compaction concurrency**: run `scan_ghosts` while
    compaction is in progress on a zone (flag set) → assert scanner
    skips the zone, does not panic. Integration test.
  - **Corrupt snapshot + corrupt records (data-safety)**: corrupt
    both the `ZoneValue` CRC and a `BusyBlockValue` → run scan →
    assert scanner reports both, keeps all blocks busy, performs no
    auto-correction. Unit test.

- `pixi run test-diskdb`, `pixi run cargo fmt --all -- --check`, and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
