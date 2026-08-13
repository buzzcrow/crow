<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R76: diskdb — Disk Failure Detection + Recovery Scan Flow

**Problem**: R71–R75 built the sync loop (`KeepAlive`), the hardware
status machine (`HwStateMachine`), the crash recovery engine
(`RecoveryEngine` — strategy 1 full scan + strategy 2 journal
replay), and the background scanner (`ScannerTask` —
ghost/integrity/leak). But the disk failure detection + recovery
flow that ties them together is incomplete:

- **`HwStatus::Missing` detection works, `HwStatus::Bad` confirmation
  does not.** `KeepAlive::reconcile_disks` transitions a disk absent
  from the sync response to `HwStatus::Missing` (design doc §8). But
  the `HwStatus::Missing → HwStatus::Bad` confirmation step (after N
  sync intervals absent) is not built — a disk stays
  `HwStatus::Missing` forever instead of progressing to
  `HwStatus::Bad`, so the bad-disk recovery scan never starts.
- **No recovery scan on `HwStatus::Bad` disks.** When a disk
  transitions to `HwStatus::Bad`, its `effective_status` is set to
  `HwStatus::Bad` — `DdbDisk::allocatable()` returns `false`
  (disk.rs:73), and `DdbDiskGroup::rebuild_allocating_disks()`
  excludes it from the allocating set (disk_group.rs:74). The
  allocate path skips `HwStatus::Bad` disks at the disk-group level
  — zones do not have their own health status; they follow the
  disk-level `HwStatus` (top-layer status overrides). But there is
  no background scan
  that iterates the bad disk's busy blocks zone by zone, triggers
  (placeholder) recovery, and persists progress. Operators have no
  visibility into what was lost, and a restart loses all scan state —
  the scan would restart from zone 0 every time.
- **Disk recovery (`HwStatus::Missing → Up`, `HwStatus::Bad → Up`)
  is not wired.** When a disk comes back to `HwStatus::Up` — either
  rediscovered before reaching `HwStatus::Bad`
  (`HwStatus::Missing → Up`) or operator-marked `HwStatus::Up` after
  repair
  (`HwStatus::Bad → Up`) — the design says stop the recovery scan,
  leave already-recovered data untouched, and run compaction to bring
  the bitmap up to date. The in-memory bitmap was never lost (diskdb
  is still running while the disk is `HwStatus::Bad`); the recovery
  scan frees blocks persist-only (writes `FreeBlockValue` + deletes
  `BusyBlockKey`, bitmap untouched — zone-management §4); compaction
  (strategy 3) is the sole bit-clearer that merges those frees into
  the bitmap. No full RecoveryEngine rebuild (strategy 1/2) is needed
  on this path —
  that is for startup / ownership transfer where the in-memory
  bitmap is lost. `HwStatus::Missing → Up` and `HwStatus::Bad → Up`
  are the same path: stop scan + compaction. The recovery scan is not
  stopped (it does not exist yet).
- **No disk-block repair component.** CROW has no `diskio` service
  (unlike aioss) — diskdb allocates blocks, it does not do data I/O.
  There is no component that can rebuild a bad disk's data from
  EC/mirror or relocate allocations elsewhere. Real data recovery is
  explicitly **skipped** in v1: the recovery scan iterates busy
  blocks and calls a placeholder recovery function, but does not
  repair disk blocks. The impacted-blocks list is collected and handed
  to a future recovery/relocation path that does not exist yet.

**Design pointers**: design doc §8 (Disk Status Management —
transitions, Missing detection via sync absence, bad-disk handling,
disk-add init flow, effective status = `max(node, group, disk)`),
§10 (Background Scanner — ghost/integrity/leak), zone-management §6
(Crash Recovery — strategy 1/2/3; strategy 3 compaction is the
bit-clearer used on the `Bad → Up` path), zone-management §4
(persist-only free — bitmap untouched, compaction is sole
bit-clearer), zone-management §5 (Compaction algorithm). The design
doc §8 states: "diskdb does **not** rebuild or relocate them inline
on the sync path — relocation/rebuild is a follow-up requirement"
and "The disk stays `HwStatus::Bad` — its records are read-only
until an operator removes the disk or marks it `HwStatus::Up` after
repair."

**Use scenarios**:
- **Disk goes `HwStatus::Missing → HwStatus::Bad` → recovery scan
  starts** — a disk's `DiskValue` disappears from the group-0 sync
  response (operator removed it, or the node/rack went down). After
  N sync intervals absent, diskdb transitions the disk
  `HwStatus::Missing → HwStatus::Bad` (sets `effective_status =
  HwStatus::Bad`). The allocate path skips the disk at the disk-group
  level (`rebuild_allocating_disks` excludes it — zones do not have
  their own health status; they follow the disk-level `HwStatus`).
  A per-disk background recovery scan starts: it iterates the bad
  disk's zones zone by zone, lists live `BusyBlockValue`s, calls a
  placeholder recovery function (no real repair), persists scan
  progress to KV after each zone, and emits the
  `disk.bad.impacted_blocks` gauge with the running count. No data
  is repaired.
- **Recovery scan resumes after restart** — diskdb restarts while a
  disk is `HwStatus::Bad` and the recovery scan is mid-way (e.g.
  completed zones 0–2 of 4). On restart, the sync loop sees the disk
  still `HwStatus::Bad`, reads the persisted scan progress from KV,
  and resumes the scan from zone 3 — not from zone 0.
- **Disk comes back (`HwStatus::Missing → Up` or `HwStatus::Bad →
  Up`)** — a disk that was `HwStatus::Missing` or `HwStatus::Bad`
  comes back to `HwStatus::Up` (rediscovered, or operator-marked
  `HwStatus::Up` after repair). diskdb stops the recovery scan (if
  running —
  no-op if the disk was only `HwStatus::Missing` and the scan never
  started), leaves already-recovered data untouched (blocks the scan
  freed stay freed — we do not undo them), transitions the disk to
  `HwStatus::Up` (sets `effective_status = HwStatus::Up`), and runs
  compaction (strategy 3) to merge any free records the scan wrote
  into the bitmap. The in-memory bitmap was never lost (diskdb is
  still running); compaction is the sole bit-clearer (zone-management
  §4). In v1 (placeholder recovery = LogOnly, no blocks were
  actually freed), compaction is a no-op — the disk comes back with
  its data intact. In the future (real recovery = Relocate), the
  scan's `FreeBlockValue`s are merged by compaction — if the scan
  completed, the bitmap shows all blocks free and the disk provides
  fresh allocation capacity. The disk resumes accepting allocates
  (`rebuild_allocating_disks` re-includes it).
- **Scanner detects drift on a healthy disk** — the background scanner
  (R75) detects ghost-busy/ghost-free drift or record corruption on a
  healthy disk's zones. The scanner auto-corrects ghost bits (when
  enabled) and reports corrupt records. This is a separate scan flow
  for live disks — it works independently of the bad-disk recovery
  scan.

**Solution**: Wire the disk failure detection + recovery scan flow
into the sync loop, completing the `HwStatus::Missing → Bad → Up`
lifecycle. The recovery scan is a persistent per-disk background
task that starts on `HwStatus::Bad`, persists progress to KV, and
stops on `HwStatus::Up`. On `→ HwStatus::Up`, compaction (strategy
3) brings the bitmap up to date — no full RecoveryEngine rebuild
(strategy 1/2) needed (the in-memory bitmap was never lost;
compaction is the sole bit-clearer). The disk's `effective_status`
(`HwStatus`) is the sole gatekeeper for the allocate path — zones
do not have their own health status; they follow the disk-level
`HwStatus` (top-layer status overrides). Real data
repair/relocation is explicitly skipped — placeholder recovery
function only, no disk-block repair component.

1. **`HwStatus::Missing → HwStatus::Bad` confirmation** —
   `app/crow-diskdb/src/liveness/keepalive.rs`:
   - `reconcile_disks` already transitions absent disks to
     `HwStatus::Missing`. Add a confirmation counter per disk: a disk
     that has been `HwStatus::Missing` for N consecutive sync
     intervals (configurable, default = `miss_threshold`, same as the
     heartbeat degraded-mode threshold) transitions
     `HwStatus::Missing → HwStatus::Bad` via
     `HwStateMachine::transition_disk`. The
     `HwStateMachine::is_legal_transition` already allows
     `HwStatus::Missing → HwStatus::Bad` (state_machine.rs).
   - On `HwStatus::Missing → HwStatus::Bad`, `transition_disk` sets
     `effective_status = HwStatus::Bad`. `DdbDisk::allocatable()`
     returns `false` (disk.rs:73);
     `DdbDiskGroup::rebuild_allocating_disks()` excludes the disk
     from the allocating set (disk_group.rs:74). The allocate path
     skips the disk at the disk-group level — zones do not have their
     own health status; they follow the disk-level `HwStatus`. The
     top-layer status overrides: if a disk-group is
     `HwStatus::Bad`, all disks under it are effectively bad by
     inheritance.
   - **Remove the zone-marking side-effect** — the existing
     `HwStateMachine::on_enter_disk(HwStatus::Bad)` marks all zones
     `DdbZoneHealth::Bad` (state_machine.rs:125–132), and
     `DdbDisk::set_effective_status(HwStatus::Bad)` does the same
     (disk.rs:81–90). This is redundant — zones do not have their
     own health status; they follow the disk-level `HwStatus`. R76
     removes the zone-marking from both paths (the `HwStatus::Bad`
     entry side-effect becomes a no-op, or the zone-marking code is
     deleted). `DdbZoneHealth` may be removed from the Bad-disk path
     entirely; if the type stays for other uses (e.g. the scanner),
     the Bad-disk path does not touch it.
   - Track the missing-since tick per disk (in-memory on `DdbDisk` or
     a side map in `KeepAlive`). Reset on rediscovery.
   - On `Missing → Bad`, start the per-disk recovery scan (work item
     2).

2. **Per-disk recovery scan (background task + KV-persisted
   progress)** — `app/crow-diskdb/src/recovery/disk_recovery.rs`
   (currently a placeholder):
   - `RecoveryScanTask` — a per-disk background task, spawned when a
     disk transitions to `HwStatus::Bad`, cancelled when the disk
     transitions back to `HwStatus::Up`. One task per bad disk. Runs
     on the shared `BgRunner`
     (same pattern as `ScannerTask`, R75) or as an independent
     `tokio::spawn` with a stop handle.
   - **Scan loop** — for the bad disk, iterate zones zone by zone
     (zone 0 → `zone_count - 1`). For each zone, call
     `DdbKvClient::read_zone_records` (R72) and list all live
     `BusyBlockValue`s. Each carries `owner_chunk` (the chunk that
     owns the allocation) so the caller / data-IO layer can be
     notified in the future.
   - **Placeholder recovery** — for each zone's busy-block batch,
     call `recover_zone_blocks(disk_id, zone_index, busy_blocks) ->
     RecoveryAction` — a placeholder function that logs the
     impacted blocks + owner chunks but does **not** perform any real
     data repair or relocation (no `diskio` component). v1's
     `RecoveryAction` is `LogOnly`; future versions add
     `Relocate`/`RebuildFromEc` when the `diskio` service exists.
   - **Persist scan progress to KV** — after completing each zone,
     write a `RecoveryScanProgressValue` to the bound data group at a
     per-disk key (`RecoveryScanProgressKey { disk_id }`). The
     progress value carries: `status` (`InProgress` / `Stopped` /
     `Complete`), `last_completed_zone`, `impacted_blocks_count`
     (running total across scanned zones), `started_at_ms`,
     `updated_at_ms`. This survives restart — on restart, the sync
     loop reads the progress and the scan resumes from
     `last_completed_zone + 1`.
   - **Gauge emission** — update the `disk.bad.impacted_blocks` gauge
     (design doc §9; the metric handle already exists in
     `DiskdbMetrics` from R74) with the running impacted count after
     each zone.
   - **Scan completion** — when all zones are scanned, mark the
     progress `Complete`. The task ends (the disk stays
     `HwStatus::Bad`; the
     scan does not loop — it is a one-pass enumeration of the bad
     disk's busy blocks). A re-scan can be triggered by an admin RPC
     or by toggling the disk `Bad → Up → Bad` again.
   - **Per-disk vs per-disk-group progress** — v1 uses per-disk
     progress (one `RecoveryScanProgressKey` per disk, one scan task
     per bad disk). Alternative: per-disk-group progress (one scan
     task per bad disk-group, iterating all bad disks in the group).
     Per-disk is simpler and parallelizes naturally; per-disk-group
     is noted as a design alternative for the design draft.

3. **Disk recovery (`HwStatus::Missing → Up`, `HwStatus::Bad → Up`,
   `HwStatus::Offline → Up` — unified path)** —
   `app/crow-diskdb/src/liveness/state_machine.rs` +
   `app/crow-diskdb/src/liveness/keepalive.rs`:
   - **State machine change**: `HwStateMachine::is_legal_transition`
     currently blocks all transitions involving `HwStatus::Bad`
     (`(Bad, _) | (_, Bad) => false`, state_machine.rs:70). Add
     `HwStatus::Bad → HwStatus::Up` as an operator-only override (the
     design doc §8 says "marks it Up after repair").
     `HwStatus::Missing → Up` and `HwStatus::Offline → Up` are
     already legal (state_machine.rs:67–68).
   - **Unified recovery path** — `HwStatus::Missing → Up`,
     `HwStatus::Bad → Up`, and `HwStatus::Offline → Up` all follow
     the same steps:
     a. **Stop the recovery scan** — cancel the per-disk recovery
        scan task (if running — no-op if the disk was only
        `HwStatus::Missing` and the scan never started, or if the
        scan already completed). Mark the persisted progress
        `Stopped` (or delete the progress key).
     b. **Leave already-recovered data untouched** — blocks the scan
        freed stay freed. We do not undo them. In v1 (placeholder =
        LogOnly), no blocks were actually freed, so this is a no-op.
        In the future (real recovery = Relocate), freed blocks stay
        freed.
     c. **Transition to `HwStatus::Up`** —
        `HwStateMachine::transition_disk` sets `effective_status =
        HwStatus::Up`.
     d. **Run compaction (strategy 3)** — the in-memory bitmap was
        never lost (diskdb is still running while the disk was
        `HwStatus::Bad`). The recovery scan frees blocks persist-only
        (writes `FreeBlockValue` + deletes `BusyBlockKey`, bitmap
        untouched — zone-management §4). Compaction is the sole
        bit-clearer: it merges the free records into the `ZoneValue`
        bitmap, clearing the freed bits and recomputing `used_count`
        (zone-management §5). After compaction, the bitmap is
        latest. In v1 (placeholder = LogOnly, no frees written),
        compaction is a no-op — the bitmap is already correct, the
        disk comes back with its data intact. In the future (real
        recovery = Relocate), compaction clears the freed bits — if
        the scan completed, the bitmap shows all blocks free and the
        disk provides fresh allocation capacity.
     e. **Rebuild the active zone deque**
        (`DdbDisk::rebuild_active_zones`).
     f. **Re-add the disk to `allocating_disks`**
        (`DdbDiskGroup::rebuild_allocating_disks` — the disk is now
        `HwStatus::Up`, so `DdbDisk::allocatable()` returns `true`
        and it re-enters the allocating set).
   - **No RecoveryEngine (strategy 1/2) on this path** — the full
     bitmap rebuild (strategy 1/2) is for startup / ownership
     transfer (R73) where the in-memory bitmap is lost and must be
     reconstructed from records. On the `HwStatus::Bad → Up` path,
     diskdb is still running and the bitmap is in memory —
     compaction is sufficient.
   - **No zone-level health marking** — zones are not touched on the
     recovery path. Zones do not have their own health status; they
     follow the disk-level `HwStatus`. The disk's
     `effective_status = HwStatus::Up` is sufficient;
     `DdbDisk::allocatable()` returns `true`, and
     `rebuild_allocating_disks` re-includes the disk.

4. **Skip real data recovery** — explicit non-goal:
   - v1 does **not** rebuild or relocate data on a bad disk. There is
     no `diskio` service — diskdb allocates blocks, it does not do
     data I/O. The recovery scan's `recover_zone_blocks` is a
     placeholder that logs impacted blocks but **does not act on
     them**.
   - The disk stays `HwStatus::Bad` — its busy/free records are
     read-only (no `BusyBlockKey` deletes, no `FreeBlockValue`
     writes) until an operator removes the disk or marks it
     `HwStatus::Up` after physical repair (which triggers work item
     3).
   - Callers that hold `Segment`s for the failed disk will encounter
     I/O errors when they try to write — that's the caller's problem,
     not diskdb's. diskdb's job is to stop allocating on the failed
     disk, enumerate the impacted blocks, and report.
   - Automatic data migration / relocation is a future requirement
     (needs the `diskio` service that does not exist yet).

5. **Remove redundant zone-marking (zones follow disk `HwStatus`)** —
   `app/crow-diskdb/src/liveness/state_machine.rs` +
   `app/crow-diskdb/src/model/disk.rs`:
   - The existing `HwStateMachine::on_enter_disk(HwStatus::Bad)`
     marks all zones `DdbZoneHealth::Bad` (state_machine.rs:125–132).
     The existing `DdbDisk::set_effective_status(HwStatus::Bad)`
     does the same (disk.rs:81–90). Both are redundant — zones do
     not have their own health status; they follow the disk-level
     `HwStatus`. The disk's `effective_status = HwStatus::Bad`
     already makes `DdbDisk::allocatable()` return `false`, and
     `rebuild_allocating_disks` excludes the disk at the disk-group
     level. The top-layer status overrides.
   - R76 removes the zone-marking from both paths. The
     `HwStatus::Bad` entry side-effect becomes a no-op (or the
     zone-marking code is deleted). `DdbZoneHealth` may be removed
     from the Bad-disk path entirely; if the type stays for other
     uses (e.g. the scanner), the Bad-disk path does not touch it.
   - `DdbZone::allocatable()` (zone.rs:124–127) still checks
     `zone_state == Healthy` — this remains as a defense-in-depth
     check for per-zone health (e.g. a zone individually marked Bad
     by the scanner), but the Bad-disk path does not rely on it. The
     disk-level `DdbDisk::allocatable()` check (disk.rs:73) fires
     first in `disk_allocate` and returns `None` early.

6. **Scan progress KV schema** —
   `lib/crow-protocol/src/proto/diskdb_type.proto` +
   `lib/crow-protocol/src/key/`:
   - New key type `RecoveryScanProgressKey { disk_id }` (BinaryKey,
     on the bound data group alongside zone records).
   - New value type `RecoveryScanProgressValue { status,
     last_completed_zone, impacted_blocks_count, started_at_ms,
     updated_at_ms }` — bincode-serialized (same as other diskdb
     data-group values). `status`: `InProgress` / `Stopped` /
     `Complete`.
   - Read/written by the recovery scan task (work item 2) and the
     sync loop (work item 3, stop on `HwStatus::Up`).

```
  sync tick (KeepAlive::reconcile_disks)
       │
       ├─ disk absent from sync response
       │      │
       │      ├─ first absence → transition Up → HwStatus::Missing
       │      │
       │      ├─ Nth absence → transition HwStatus::Missing → HwStatus::Bad
       │      │      │
       │      │      ├─ effective_status = HwStatus::Bad
       │      │      │     → DdbDisk::allocatable() = false
       │      │      │     → rebuild_allocating_disks excludes disk
       │      │      │     → allocate skips disk (zones follow disk HwStatus)
       │      │      │
       │      │      ├─ spawn RecoveryScanTask (per-disk background)
       │      │      │     │
       │      │      │     ├─ for each zone (0..zone_count):
       │      │      │     │     ├─ read_zone_records → list BusyBlockValues
       │      │      │     │     ├─ recover_zone_blocks() → PLACEHOLDER (log only)
       │      │      │     │     ├─ persist RecoveryScanProgressValue to KV
       │      │      │     │     └─ update disk.bad.impacted_blocks gauge
       │      │      │     │
       │      │      │     └─ all zones done → status = Complete
       │      │      │
       │      │      └─ disk stays HwStatus::Bad (records read-only)
       │      │
       │      └─ disk reappears (before Bad) → HwStatus::Missing → Up
       │             │  (same path as HwStatus::Bad → Up below)
       │             └─ → unified recovery (see below)
       │
       └─ disk present, status changed in group 0
              │
              └─ HwStateMachine::transition_disk
                    → HwStatus::Bad → Up (operator override, needs state
                      machine change) → unified recovery (see below)
                    → HwStatus::Offline → Up (already legal) → unified recovery
                    → HwStatus::Missing → Up (already legal) → unified recovery

  unified recovery (HwStatus::Missing → Up, Bad → Up, Offline → Up):
       ├─ STOP RecoveryScanTask (no-op if not running) + clear progress
       ├─ leave already-recovered data untouched (no undo)
       ├─ effective_status = HwStatus::Up
       ├─ compaction (strategy 3) — sole bit-clearer
       │     → merge scan's FreeBlockValues into bitmap
       │     → v1: no-op (placeholder = LogOnly, no frees written)
       │     → future: clear freed bits → disk may be empty if scan
       │       completed → fresh allocation capacity
       ├─ rebuild_active_zones()
       └─ rebuild_allocating_disks() → disk re-included

  restart while disk is HwStatus::Bad:
       sync loop reads RecoveryScanProgressValue from KV
       → resume RecoveryScanTask from last_completed_zone + 1
```

**Edge cases at a glance**:
- Disk absent for 1 tick then reappears → `HwStatus::Missing → Up`
  (unified recovery): stop scan (no-op — scan never started),
  compaction (no-op in v1 — no frees written), disk comes back with
  data intact.
- Disk absent for N+ ticks → `HwStatus::Missing → Bad`, recovery
  scan starts (zone 0). Disk stays `HwStatus::Bad` until operator
  intervention.
- `HwStatus::Bad → Up` operator override (disk physically repaired)
  → state machine allows the transition (R76 adds
  `HwStatus::Bad → Up` operator override), unified recovery: stop
  scan + leave recovered data + compaction. In v1, compaction is a
  no-op (placeholder = LogOnly), disk comes back with data intact.
  In future, compaction clears freed bits — if scan completed, disk
  comes back empty.
- `HwStatus::Offline → Up` (already legal) → unified recovery (no
  scan to stop — scan only starts on `HwStatus::Bad`).
- Restart while disk is `HwStatus::Bad` and scan is mid-way → sync
  loop reads persisted progress, resumes scan from
  `last_completed_zone + 1`. Scan does not restart from zone 0.
- Restart while disk is `HwStatus::Bad` and scan was `Complete` →
  sync loop reads `Complete` status, does not re-spawn the scan
  (one-pass enumeration). A re-scan requires operator toggle
  (`HwStatus::Bad → Up → Bad`) or admin RPC.
- Recovery scan task is killed mid-zone (process crash) → on
  restart, resume from `last_completed_zone` (the in-progress zone
  is re-scanned — progress is persisted per-zone, not per-block).
- All disks in a disk-group go `HwStatus::Bad` → one recovery scan
  task per disk (parallel). `rebuild_allocating_disks` produces an
  empty set → allocate returns `NoSpace`. Free is also blocked on
  `HwStatus::Bad` disks (`HwStateMachine::permits(HwStatus::Bad,
  Free) = false`).
- Disk-group goes `HwStatus::Bad` (operator sets group status) →
  all disks under it are effectively bad by inheritance
  (`effective_status = max(node, group, disk)`). No per-disk or
  per-zone marking needed — `DdbDiskGroup::allocatable()` returns
  `false` (disk_group.rs:108), and `allocate_block` returns
  `NoSpace` early (disk_group.rs:123).
- Recovery engine fails for a zone (strategy 2 GC gap + strategy 1
  failure) → zone starts empty (existing R73 fallback behavior).
  Disk is still marked `HwStatus::Up`; the scanner will detect any
  drift on the next scan cycle.

**Dependencies**: R70 (types, config), R71 (`KeepAlive`,
`HardwareClient`, `DdbDiskGroupContainer`), R72 (`DdbDisk`,
`DdbZone`, `DdbKvClient::read_zone_records`), R73
(`recovery::compaction` — compaction engine, strategy 3; the full
`RecoveryEngine` strategy 1/2 is for startup/ownership transfer only,
not the R76 recovery path), R74 (`DiskdbMetrics` —
`disk.bad.impacted_blocks` gauge handle), R75 (`ScannerTask`,
`BgRunner` pattern — recovery scan task follows the same background
task structure). No dependency on R77 (console) or R78 (notify/watch).
No dependency on a future `diskio` service — real data recovery is
explicitly skipped (placeholder recovery function only).

**Acceptance**:
- **`HwStatus::Missing → Bad` confirmation**:
  - `reconcile_disks` with a disk absent for 1 tick → disk
    transitions `Up → HwStatus::Missing`, no `HwStatus::Bad`
    transition, no recovery scan started. Integration test.
  - `reconcile_disks` with a disk absent for N ticks (N =
    `miss_threshold`) → disk transitions
    `HwStatus::Missing → HwStatus::Bad`,
    `effective_status = HwStatus::Bad`, `DdbDisk::allocatable()`
    returns `false`, `rebuild_allocating_disks` excludes the disk,
    recovery scan task started. Integration test.
  - `reconcile_disks` with a disk absent for N-1 ticks then
    reappears → disk transitions `HwStatus::Missing → Up`, no
    `HwStatus::Bad` transition, no recovery scan. Integration test.
- **Per-disk recovery scan**:
  - `RecoveryScanTask` on a disk with 4 zones, 2 live
    `BusyBlockValue` records (1 in zone 0, 1 in zone 1, zones 2 + 3
    empty) → scans all 4 zones, `impacted_blocks_count = 2`, calls
    `recover_zone_blocks` (placeholder) 4 times, progress persisted
    after each zone with `last_completed_zone` advancing 0 → 1 → 2 →
    3, final status `Complete`. Integration test.
  - `RecoveryScanTask` on a disk with no live busy records → scans
    all zones, `impacted_blocks_count = 0`, status `Complete`.
    Integration test.
  - `RecoveryScanTask` on a disk with 4 zones, killed after zone 1
    completes → `RecoveryScanProgressValue` in KV has
    `last_completed_zone = 1`, `status = InProgress`. Unit test
    (verify progress write after each zone).
  - `recover_zone_blocks` placeholder is called with the correct
    `busy_blocks` list per zone and returns `RecoveryAction::
    LogOnly` — no `BusyBlockKey` deletes or `FreeBlockValue` writes.
    Unit test.
  - On `HwStatus::Missing → Bad` transition in `reconcile_disks`,
    the `disk.bad.impacted_blocks` gauge is updated as the scan
    progresses. Integration test: allocate 5 blocks, remove disk
    from group 0, wait N ticks, verify gauge = 5 after scan
    completes.
- **Scan progress persistence + resume**:
  - Restart diskdb while a disk is `HwStatus::Bad` and scan completed
    zones 0–1 of 4 → sync loop reads `RecoveryScanProgressValue`
    (status = `InProgress`, `last_completed_zone = 1`), resumes scan
    from zone 2, completes zones 2–3, final status `Complete`.
    Integration test.
  - Restart diskdb while a disk is `HwStatus::Bad` and scan was
    `Complete` → sync loop reads `Complete` status, does not
    re-spawn the scan. Integration test.
  - `RecoveryScanProgressKey` + `RecoveryScanProgressValue` round-trip
    through KV (write + read back). Unit test.
- **Disk recovery (`HwStatus::Missing → Up`, `Bad → Up`,
  `Offline → Up` — unified path)**:
  - `HwStateMachine::is_legal_transition(HwStatus::Bad,
    HwStatus::Up)` returns `true` after the state machine change
    (operator override). Unit test.
  - Disk absent 1 tick then reappears with status `HwStatus::Up` →
    disk
    transitions `HwStatus::Missing → Up`,
    `effective_status = HwStatus::Up`, compaction runs (no-op in v1
    — no frees written), `used_count` matches pre-absence state
    (data intact), active zone deque rebuilt,
    `rebuild_allocating_disks` re-includes the disk. Integration
    test.
  - `HwStatus::Bad → Up` operator override → recovery scan task
    stopped + progress cleared, `effective_status = HwStatus::Up`,
    compaction runs (no-op in v1), `used_count` matches pre-Bad
    state (data intact), disk re-included in allocating set.
    Integration test.
  - `HwStatus::Offline → Up` → same unified path (no scan to stop).
    Integration test.
  - Compaction merges scan-written `FreeBlockValue`s into the bitmap
    (strategy 3) — verify freed bits cleared, `used_count`
    decremented. Unit test (inject free records, run compaction,
    verify bitmap).
- **Skip real data recovery**:
  - On `HwStatus::Missing → Bad`, the recovery scan logs impacted
    blocks but no data repair/relocation occurs (no `diskio` calls).
    Verify no `BusyBlockKey` deletes or `FreeBlockValue` writes
    happen on the bad disk's records. Integration test.
  - A `HwStatus::Bad` disk's records are read-only: allocate and free
    both rejected on `HwStatus::Bad` disks
    (`HwStateMachine::permits(HwStatus::Bad, Allocate) = false`,
    `permits(HwStatus::Bad, Free) = false`). Unit test.
- **Remove redundant zone-marking (zones follow disk `HwStatus`)**:
  - `HwStateMachine::on_enter_disk(HwStatus::Bad)` does **not** mark
    zones `DdbZoneHealth::Bad` after the R76 change — the
    `HwStatus::Bad` entry side-effect is a no-op (or the
    zone-marking code is deleted). Unit test: transition a disk to
    `HwStatus::Bad`, verify zones are not touched (zones follow the
    disk-level `HwStatus`).
  - `DdbDisk::set_effective_status(HwStatus::Bad)` does **not** mark
    zones `DdbZoneHealth::Bad` after the R76 change. Unit test.
  - A disk with `effective_status = HwStatus::Bad` →
    `DdbDisk::allocatable()` returns `false` (disk-level check fires
    first), `disk_allocate` returns `None` early. Unit test.
  - Disk-group status set to `HwStatus::Bad` →
    `DdbDiskGroup::allocatable()` returns `false`,
    `allocate_block` returns `NoSpace` early — no per-disk or
    per-zone marking needed (top-layer override). Integration test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run test-diskdb` (relevant integration tests pass).
