<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R75: diskdb — Background Scanner (Consistency Check + Integrity Detection)

**Problem**: R72–R74 implement the allocation engine, crash recovery,
and space metrics. The free path is **persist-only** (§7: delete
`BusyBlockKey` + put `FreeBlockValue`, no bitmap mutation), and R73's
recovery rebuilds the bitmap from records on restart. Compaction
(§8) is the sole mechanism for clearing freed bits in the bitmap —
it runs before a zone enters the active set (preparatory thread) and
periodically as a fallback. But during uptime, there is no mechanism
to detect and reconcile live-state drift, catch record corruption
early, or give operators visibility into cluster health.

- **Current behavior + impact** — R74's `RecalcEngine` (§11) replays
  each zone's journal into a throwaway bitmap and compares the
  **popcount** (busy-block count) against the live `DdbZone`. This
  detects count-level drift on demand (`RecalcDiskUsage` RPC), but:
  - It does not identify **which specific blocks** differ — only that
    the counts mismatch. An operator learns "zone 3 drifted" but not
    which bits to reclaim.
  - It is not periodic — nothing wakes it. Drift can persist
    indefinitely until an operator manually triggers a recalc.
    Ghost-busy blocks (bit set, no `BusyBlockKey` and no
    `FreeBlockKey` — from a crash between allocate Phase 1 and Phase 2,
    or a bug) waste capacity until the next restart or compaction.
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

- **Design pointers** — §12 (Background Scanner) specifies drift
  detection, record integrity, and per-block state validation; leak
  detection is a §2 non-goal (needs caller registries). §7
  (crash-safety invariants) defines the drift directions in the
  persist-only model. §14 (Concurrency Model) defines the zone-level
  lock for non-allocate operations (compaction, scanner, health
  checks) and the common methods on `DdbZone`. No direct aioss analog
  — new work (the aioss reference path cited in §15 is not accessible
  for verification).

- **Data-safety principle** — a busy block may have data written to
  it. The scanner's first priority is to **never free a block that
  might have data**. When the scanner finds drift and the true state
  is uncertain (corrupt records, conflicting signals), it defaults to
  **busy** (keep the bit set, keep the block allocated). Wasting space
  is always preferable to freeing a block with data. The scanner only
  clears a bit when records confidently say "free" (no `BusyBlockKey`,
  no `FreeBlockKey`, records intact and readable). Specifically:
  - **Real ghost-busy** (drift): bit set, no `BusyBlockKey`, no
    `FreeBlockKey` — the block was never freed and never allocated
    (crash between allocate Phase 1 and Phase 2, or a bug). Records
    are authoritative → block is free → safe to clear the bit.
    Auto-correction allowed.
  - **Normal uncompacted** (NOT drift): bit set, no `BusyBlockKey`,
    `FreeBlockKey` exists — the block was freed (persist-only) but
    compaction hasn't cleared the bit yet. This is the **expected
    state** in the persist-only model. The scanner does **not** report
    it as drift. (If the zone is not active and has a high
    `uncompacted_free_record_count`, the scanner may log a hint that
    compaction is lagging, but it's not a drift finding.)
  - **Ghost-free** (drift): bit clear, `BusyBlockKey` exists — should
    not happen in the persist-only model (free never clears bits, and
    only allocate/compaction touch the bitmap). If detected, it
    indicates a bug or hardware error. Records are authoritative →
    block is busy → set the bit back. Data may be written.
    Auto-correction allowed.
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
    ghost-busy bit wastes capacity. The scanner detects it (bit set,
    no `BusyBlockKey`, no `FreeBlockKey`), and with auto-correct
    enabled, clears the bit — reclaiming the space without a restart.
  - **Compaction lag** — a non-active zone has many freed blocks
    (`FreeBlockKey` records exist) but compaction hasn't run yet (the
    preparatory thread is slow or the periodic fallback hasn't fired).
    The bitmap shows these blocks as busy (persist-only model). The
    scanner does **not** report this as drift — it's the expected
    state. But it logs a hint if `uncompacted_free_record_count` is
    high, giving the operator visibility into compaction health.
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
  - **Bug or hardware error causing ghost-free** — a bug in the CAS
    logic or a hardware memory error could clear a live bitmap bit
    while the `BusyBlockKey` still exists on disk. The allocator would
    re-hand the block (double-allocate). The scanner detects the
    ghost-free bit and **sets it back** (records say busy, data may be
    written) — preventing further re-allocation. This is a
    defense-in-depth check against unknown causes.
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
handing blocks from them) and acquires the **zone-level lock** (§14)
for non-active zones to coordinate with compaction and health checks.
Drift detection distinguishes **real ghost-busy** (no `BusyBlockKey`,
no `FreeBlockKey` — drift) from **normal uncompacted** (no
`BusyBlockKey`, `FreeBlockKey` exists — expected in the persist-only
model, not reported as drift). A **re-verify step** filters transient
drift from the allocate Phase 1→2 window when a zone rotates out with
an in-flight allocate. Auto-correction (when enabled) follows the
data-safety principle: clear real ghost-busy bits only when records
are intact and authoritative; set ghost-free bits back; never free a
block with a corrupt or uncertain record. Compaction and the scanner
share common methods on `DdbZone` (§14) and coordinate via the
zone-level lock. Leak detection is scaffolded as deferred.

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
     handing blocks from it, and the allocate Phase 1→2 window creates
     transient ghost-busy bits. The scanner checks these zones on a
     later cycle when they rotate out of the active set (and have been
     compacted by the preparatory thread). Skipped zones are counted
     in the scan summary as `skipped_active`.
   - **Acquire zone-level lock**: for non-active zones, acquire the
     zone-level lock (§14) via `zone.scan_zone_inner()` — the common
     method on `DdbZone` that encapsulates locking. This coordinates
     with compaction (`compact_zone_inner`) and health checks
     (`health_check_zone_inner`) — only one non-allocate operation runs
     on a zone at a time. If the lock is held by compaction, the
     scanner skips the zone (counted as `skipped_compacting`) and
     tries again next cycle.
   - Replays the journal into a throwaway `DdbZone` by calling
     `recover_zone_inner` (R73, strategy 2) with strategy-1 fallback
     (`rebuild_zone_bitmap_full_scan`) on `JournalScanGcGap` or
     `SnapshotCrcFail` — same fallback logic as `RecalcEngine`.
   - Compares `replayed.usage_bits` against `live.usage_bits`
     **bit-by-bit** (not just popcount). For each differing bit,
     classifies by checking the record set:
     - **Real ghost-busy** (drift): bit set in live, clear in replayed,
       no `BusyBlockKey` AND no `FreeBlockKey` on disk. The block was
       never freed and never allocated (crash between allocate phases,
       or a bug). Safe direction: wasted space, no data risk.
     - **Normal uncompacted** (NOT drift): bit set in live, clear in
       replayed, no `BusyBlockKey` but `FreeBlockKey` exists. The
       block was freed (persist-only) but compaction hasn't cleared
       the bit. This is the **expected state** — not reported as drift.
       If `uncompacted_free_record_count` is high, log a compaction-lag
       hint.
     - **Ghost-free** (drift): bit clear in live, set in replayed
       (`BusyBlockKey` exists). Should not happen in the persist-only
       model. Dangerous direction: allocator may re-hand the block.
       Detected as defense-in-depth.
   - **Re-verify step**: if real drift is detected, the scanner waits a
     short delay (default 1s, configurable via `reverify_delay_ms`)
     and re-snapshots the live `usage_bits`, then re-compares. If the
     drift disappears, it was transient (a zone rotated out with an
     in-flight allocate Phase 1 whose Phase 2 completed during the
     delay) — skip it. If the drift persists, it is real — report it.
     This filters false positives from the rotate-out transient
     window. The re-verify does not re-read the journal (records don't
     change during the delay); only the live bitmap is re-snapshotted.
     Set `reverify_delay_ms = 0` to disable re-verify.
   - Returns `GhostScanResult { ghost_busy: u64, ghost_free: u64,
     uncompacted_lag: u64, skipped_active: u64, skipped_compacting: u64,
     details: Vec<GhostBlock> }` where each `GhostBlock` records
     `(disk_id, zone_index, unit_offset, unit_count, direction)`.
     `uncompacted_lag` is the count of normal-uncompacted blocks (not
     drift, but reported for visibility).
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
     - Real ghost-busy: clear the live bit (records confidently say
       free — no `BusyBlockKey`, no `FreeBlockKey`).
     - Ghost-free: set the live bit back (records say busy, data may
       be written).
     - Normal uncompacted: **do not auto-correct** — this is
       compaction's job, not the scanner's. The scanner reports it as
       lag for visibility.
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
   `scan_integrity(container, kv)` iterates owned zones (same skip +
   lock logic as ghost scan: skip active zones, acquire zone-level
   lock for non-active zones, skip if locked by compaction) and for
   each:
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

8. **Zone-level lock + common methods** — add a zone-level lock
   (`RwLock<()>` or `Mutex`) to `DdbZone` in
   `app/crow-diskdb/src/model/zone.rs`, plus three common methods that
   encapsulate the lock + operation (§14):
   - `compact_zone_inner()` — read free records, clear bits, recompute
     `used_count`, write snapshot, delete free records. Used by
     `CompactionEngine` (preparatory thread + periodic fallback).
   - `scan_zone_inner()` — replay journal, compare bitmap, verify
     records. Used by the scanner.
   - `health_check_zone_inner()` — verify zone records, CRC, snapshot
     integrity. Used by the health probe (R76).
   - Each method acquires the zone lock, performs the in-memory bitmap
     mutation (if any), and releases the lock. The lock is **not** held
     across `.await` on the KV client — the KV read is done before
     acquiring the lock, and the KV write is done after releasing the
     lock (the lock protects only the in-memory bitmap mutation).
   - Allocate does **not** acquire this lock — it uses per-bit CAS
     (lock-free). The lock only coordinates non-allocate operations
     on non-active zones (no concurrent allocate).
   - Compaction remains a separate `BackgroundTask` (preparatory thread
     + periodic fallback). The scanner and compaction coordinate via
     the zone lock — if the scanner cannot acquire the lock
     (compaction is running), it skips the zone and tries again next
     cycle.

```
                    ┌──────────────┐
   TriggerScan ──►  │  ScannerTask │  ◄── TimerFn(scan_interval_secs)
   (Notify/flag)    └──────┬───────┘
                          │ run_cycle
                          ▼
         ┌─────────────────────────────────┐
         │  for each owned zone:           │
         │  skip if in active_zone_context │
         │  acquire zone-level lock        │
         │  (skip if locked by compaction) │
         └────────────────┬────────────────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
      ┌───────────┐ ┌───────────┐ ┌───────────┐
      │ ghost.rs  │ │integrity. │ │  leak.rs  │
      │           │ │    rs     │ │           │
      │ replay +  │ │ CRC +     │ │ "deferred"│
      │ bit-diff  │ │ decode +  │ │ (scaffold)│
      │ + classify│ │ owner chk │ │           │
      │ → re-verify│ │           │ │           │
      └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
            │             │             │
            └─────────────┼─────────────┘
                          ▼
         ┌──────────────────────────────────────┐
         │  data-safety principle applied:      │
         │  real ghost-busy → clear (no recs)   │
         │  normal uncompacted → not drift      │
         │  ghost-free → set (data risk)        │
         │  corrupt rec → keep busy, report     │
         └──────────────────────────────────────┘
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
- **Zone locked by compaction** → scanner cannot acquire zone lock;
  counted as `skipped_compacting`. Checked on a later cycle.
- **Normal uncompacted blocks** (bit set, no `BusyBlockKey`,
  `FreeBlockKey` exists) → not drift; counted as `uncompacted_lag`
  for visibility. If high, log a compaction-lag hint.
- **Transient drift (rotate-out with in-flight allocate)** → re-verify
  step filters it: wait `reverify_delay_ms`, re-snapshot live bitmap,
  re-compare. If drift disappears, it was transient — skip. If
  persistent, report.
- **All zones active or locked** → scan cycle completes with zero
  zones scanned; logged as a warning (the scanner is starved).
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
`CompactionEngine` — shares zone-level lock + common methods on
`DdbZone`), R74 (`RecalcEngine` — optional count-level pre-check;
`DiskdbMetrics` registry pattern). No dependency on R76–R77. R79 (free
batching) does not change scanner scope — the free path is
persist-only (no bitmap mutation), so batching only affects how many
records are in one `batch_write`; the bitmap is always reconciled by
compaction.

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
  - `scan_ghosts` detects real ghost-busy: setup a zone (not in
    active set, zone lock acquirable), manually set a live bitmap bit
    for a block with no `BusyBlockKey` and no `FreeBlockKey` on disk
    → run `scan_ghosts` → assert the ghost-busy block is in
    `details` with direction `GhostBusy`. Unit test.
  - `scan_ghosts` classifies normal uncompacted as NOT drift: setup
    a zone, persist a `FreeBlockValue` on disk (no `BusyBlockKey`),
    leave the live bitmap bit set (persist-only model) → run
    `scan_ghosts` → assert the block is **not** in `details` (not
    drift), but is counted in `uncompacted_lag`. Unit test.
  - `scan_ghosts` detects ghost-free: setup a zone (not in active
    set), persist a `BusyBlockValue` on disk, manually **clear** the
    live bitmap bit (simulating a bug/hardware error) → run
    `scan_ghosts` → assert the ghost-free block is in `details` with
    direction `GhostFree`. Unit test.
  - `scan_ghosts` reports zero ghosts when live and replayed bitmaps
    match: setup a zone, allocate blocks, persist all records, no
    uncompacted frees → run `scan_ghosts` → assert `ghost_busy == 0`,
    `ghost_free == 0`, `uncompacted_lag == 0`. Unit test.
  - `scan_ghosts` falls back to strategy 1 on `SnapshotCrcFail`:
    corrupt the `ZoneValue` CRC → run `scan_ghosts` → assert it does
    not panic, falls back to `rebuild_zone_bitmap_full_scan`, and
    still reports ghosts. Unit test.
  - **Skip active zones**: add a zone to the disk's
    `active_zone_context` → run `scan_ghosts` → assert the zone is
    counted as `skipped_active` and not in `details`. Unit test.
  - **Skip zones locked by compaction**: hold the zone lock (simulate
    compaction in progress) → run `scan_ghosts` → assert the zone is
    counted as `skipped_compacting` and not in `details`. Unit test.
  - **Re-verify filters transient drift**: setup a zone with a
    transient ghost-busy (bit set, no record — simulate by setting
    the bit after the journal replay but before the re-verify) →
    run `scan_ghosts` with `reverify_delay_ms = 100` → during the
    delay, clear the bit (simulating the allocate Phase 2 completing)
    → assert the ghost is **not** reported (transient, filtered by
    re-verify). Unit test.
  - **Re-verify confirms persistent drift**: setup a zone with a
    persistent ghost-busy (bit set, no record, no free record) → run
    `scan_ghosts` with `reverify_delay_ms = 100` → do not change the
    bit during the delay → assert the ghost **is** reported
    (persistent, confirmed by re-verify). Unit test.
  - **Auto-correction — real ghost-busy (data-safety principle)**:
    enable `auto_correct_drift`, run `scan_ghosts` on a zone with a
    persistent real ghost-busy bit (bit set, no `BusyBlockKey`, no
    `FreeBlockKey`, records intact, re-verify confirms) → assert the
    live bit is **cleared** and `used_count` is decremented (records
    confidently say free). Unit test.
  - **Auto-correction — normal uncompacted NOT corrected**: enable
    `auto_correct_drift`, run `scan_ghosts` on a zone with
    uncompacted free blocks (bit set, no `BusyBlockKey`,
    `FreeBlockKey` exists) → assert the live bits are **not**
    cleared (compaction's job, not the scanner's). Unit test.
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
    source of truth; real ghost-busy = no `BusyBlockKey` + no
    `FreeBlockKey`; normal uncompacted = no `BusyBlockKey` +
    `FreeBlockKey` exists (not drift); ghost-free = `BusyBlockKey`
    exists + bit clear. Re-verify confirms persistence before
    reporting or correcting. Unit test.

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

- **Zone-level lock + common methods (work item 8)**:
  - `DdbZone` has a zone-level lock field. `compact_zone_inner`,
    `scan_zone_inner`, `health_check_zone_inner` methods exist and
    acquire the lock. Unit test (verify lock is acquired and released).
  - `compact_zone_inner` and `scan_zone_inner` cannot run
    concurrently on the same zone — one blocks or the other skips.
    Unit test (acquire lock in one task, attempt the other, verify
    skip/block behavior).
  - Allocate does **not** acquire the zone lock — verify per-bit CAS
    still works without the lock. Unit test.
  - The lock is not held across `.await` on the KV client — verify
    the KV read is done before acquiring the lock and the KV write is
    done after releasing the lock. Unit test (mock kv with delay,
    verify lock is not held during the delay).

- **Edge cases**:
  - Empty zone (no records, no snapshot): `scan_ghosts` and
    `scan_integrity` report zero issues, zone counted as scanned.
    Unit test.
  - Degraded mode (data-group unreachable): scanner skips unreadable
    zones, logs a warning, does not panic. Unit test (mock kv returns
    `Unavailable`).
  - **All zones active or locked**: scan cycle completes with zero
    zones scanned; logged as a warning. Unit test (set all zones
    active, run scan, assert `skipped_active == zone_count`,
    `details` is empty).
  - **Scanner + compaction concurrency**: run `scan_ghosts` while
    compaction holds the zone lock → assert scanner skips the zone
    (counted as `skipped_compacting`), does not panic. Integration
    test.
  - **Normal uncompacted blocks (persist-only model)**: persist free
    records without clearing bitmap bits → run `scan_ghosts` → assert
    blocks are counted as `uncompacted_lag`, not as `ghost_busy`.
    Unit test.
  - **Corrupt snapshot + corrupt records (data-safety)**: corrupt
    both the `ZoneValue` CRC and a `BusyBlockValue` → run scan →
    assert scanner reports both, keeps all blocks busy, performs no
    auto-correction. Unit test.

- `pixi run test-diskdb`, `pixi run cargo fmt --all -- --check`, and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
