<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Background Scanner (R75)

This draft covers the diskdb background scanner — a periodic
consistency check that detects live-state drift, catches record
corruption early, and gives operators visibility into cluster health
during uptime. The backlog doc is
[`doc/backlog/R75-diskdb-background-scanner.md`](../backlog/R75-diskdb-background-scanner.md);
the root design context is
[`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
§10 (Background Scanner), with zone-level coordination in
[`doc/design/diskdb/design-crow-diskdb-zone-management.md`](../design/diskdb/design-crow-diskdb-zone-management.md)
§8 (Concurrency Model) + §9 (Background Scanner Coordination). Already
landed: R70's `ScannerConfig` scaffold (3 fields);
`DdbZone::zone_lock` + `scan_zone_inner` / `health_check_zone_inner`
stubs (R73); R72's `DdbKvClient::read_zone_records` /
`journal_scan_busy` / `journal_scan_free`; R73's `recover_zone_inner`
+ `rebuild_zone_bitmap_full_scan` + `RecoveryError` fallback;
R74's `RecalcEngine` (count-level drift check) + `DiskdbMetrics`
registry pattern + `BgRunner` / `BackgroundTask` framework.
Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. Scanner task + shared state

### 1.1 Why

R74's `RecalcEngine` detects count-level drift on demand
(`RecalcDiskUsage` RPC) but nothing wakes it periodically, it does not
identify which blocks differ, and it does not verify record integrity.
A `BackgroundTask` is the existing pattern for periodic work
(`CompactionEngine`, `ReportingTask`, `KeepAlive`); the scanner
follows it. The scanner also needs shared state for the admin RPCs
(`TriggerScan` / `GetScanStatus`) and a wakeup channel
(`TriggerScan` bypasses the timer).

### 1.2 ScannerTask + ScanState

New module `app/crow-diskdb/src/scanner/mod.rs`.

`ScanState` holds the last scan summary (shared between the task and
the service handlers) + a `Notify` for `TriggerScan` wakeups:

```rust
pub struct ScanState {
    /// Last scan summary (behind a RwLock so GetScanStatus is cheap).
    last: RwLock<Option<ScanSummary>>,
    /// Wakeup channel for TriggerScan (the task's trigger is TimerFn,
    /// so TriggerScan needs an additional Notify).
    notify: Arc<Notify>,
    /// "scan in progress" flag — TriggerScan returns the current
    /// status instead of overlapping when a scan is already running.
    in_progress: AtomicBool,
}
```

`ScanSummary` is the result of one scan cycle:

```rust
pub struct ScanSummary {
    pub started_at_ms: u64,
    pub duration_ms: u64,
    pub ghost_result: GhostScanResult,
    pub integrity_result: IntegrityScanResult,
    pub leak_result: LeakScanResult,
    pub zones_scanned: u64,
    pub zones_skipped_active: u64,
    pub zones_skipped_compacting: u64,
}
```

`ScannerTask` implements `BackgroundTask`:

```rust
pub struct ScannerTask {
    state: Arc<ScanState>,
}
```

- `run_cycle(&BgCtx)`: reads `scanner` config from `ctx.config`; runs
  `scan_ghosts` (if `detect_ghost_allocations`) → `scan_integrity`
  (if `verify_record_integrity`) → `scan_for_leaks` (always — returns
  deferred); builds a `ScanSummary`; stores it in `state.last`;
  updates metrics (`scanner_runs_total`, `scanner_duration_ms`,
  `scanner_ghosts_found`, `scanner_drift_found`,
  `scanner_corrupt_records`); logs a summary line. Returns `Ok(())`.
- `trigger()`: `Trigger::TimerFn` reading
  `ctx.config.load().scanner.scan_interval_secs`. The task also
  checks `state.notify` each cycle via `tokio::select!` in the run
  loop — but since `BgRunner` owns the trigger wait, the scanner uses
  a hybrid: the `TimerFn` duration is the normal interval, and
  `TriggerScan` sets an `AtomicBool` "scan requested" flag that
  `run_cycle` checks at the start (if set, it clears it and runs
  immediately regardless of the timer). This avoids a second Notify
  channel competing with the runner's trigger wait. The flag is
  checked inside `run_cycle`, so a `TriggerScan` that arrives between
  ticks is serviced on the next tick — acceptable for an admin
  operation (the operator can also wait for the next interval).
- `name()`: `"scanner"`.

### 1.3 TriggerScan behavior

`TriggerScan` sets `state.scan_requested` (an `AtomicBool` on
`ScanState`) and returns the current `last` summary (or an empty
summary if no scan has run yet). The next `run_cycle` tick sees the
flag, clears it, and runs a full scan. This is simpler than a
`Notify` wakeup (which would require restructuring the runner's
trigger loop) and the latency is bounded by `scan_interval_secs` (the
operator can lower it for ad-hoc checks). The `in_progress` flag
prevents overlap: if a scan is already running, `TriggerScan` returns
the current status with a note.

- Edge cases:
  - `TriggerScan` while a scan is in progress → return current
    `last` summary (v1: no overlap; the in-progress scan will update
    `last` when it finishes).
  - `TriggerScan` before any scan has run → return an empty
    `ScanSummary` with `started_at_ms = 0`.
  - Scanner task not registered (e.g. during recovery) →
    `TriggerScan` returns `Unavailable`.

## 2. Ghost + bitmap drift detection

### 2.1 Why

`RecalcEngine` compares only popcounts — it cannot tell the operator
which bits to reclaim. The ghost scan does a bit-by-bit diff and
classifies each differing bit by checking the record set, following
the data-safety principle (zone-management §6 crash-safety
invariants).

### 2.2 scan_ghosts

New file `app/crow-diskdb/src/scanner/ghost.rs`.

```rust
pub struct GhostScanResult {
    pub ghost_busy: u64,
    pub ghost_free: u64,
    pub uncompacted_lag: u64,
    pub skipped_active: u64,
    pub skipped_compacting: u64,
    pub details: Vec<GhostBlock>,
}

pub struct GhostBlock {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub unit_offset: u64,
    pub unit_count: u32,
    pub direction: GhostDirection,
}

pub enum GhostDirection { GhostBusy, GhostFree }
```

`scan_ghosts(container, kv, config) -> GhostScanResult` iterates owned
disk-groups → disks → zones:

a. **Skip active zones**: read `disk.active_zone_context` (RCU clone
   the `Arc<Vec<Arc<DdbZone>>>`); a zone whose `Arc` pointer matches
   one in the active set is skipped (`skipped_active += 1`).
b. **Acquire zone-level lock**: for non-active zones, try
   `zone.zone_lock.try_write()`. On failure (compaction holds it),
   skip (`skipped_compacting += 1`). The lock is held only for the
   in-memory bitmap comparison + auto-correct (I9).
c. **Replay the journal** into a throwaway `DdbZone` via
   `recover_zone_inner` (strategy 2). On `JournalScanGcGap` or
   `SnapshotCrcFail`, fall back to `rebuild_zone_bitmap_full_scan`
   (strategy 1) — same logic as `RecalcEngine::recalc_zone`. Record
   whether fallback was used (suppresses auto-correct).
d. **Bit-by-bit diff**: for each word, `live.load_word(i)` XOR
   `replayed.load_word(i)`; for each set bit in the XOR, classify:
   - Bit set in live, clear in replayed → check records: if no
     `BusyBlockKey` AND no `FreeBlockKey` for that offset → **real
     ghost-busy** (drift). If no `BusyBlockKey` but `FreeBlockKey`
     exists → **normal uncompacted** (not drift; `uncompacted_lag +=
     1`; log a hint if `uncompacted_free_record_count` is high).
   - Bit clear in live, set in replayed → `BusyBlockKey` exists →
     **ghost-free** (drift, defense-in-depth).
   The record check uses `read_zone_records` (already loaded for the
   replay fallback path) — build a `HashSet<u64>` of busy offsets and
   free offsets from `ZoneRecords.busy` / `.free` for O(1) lookup.
e. **Re-verify step**: if real drift is detected and
   `config.reverify_delay_ms > 0`, sleep
   `Duration::from_millis(reverify_delay_ms)`, re-snapshot the live
   `usage_bits` (re-read each word), re-compare. If the drift
   disappears, it was transient (a zone rotated out with an in-flight
   allocate Phase 1 whose Phase 2 completed during the delay) — skip
   it. If persistent, report it. The re-verify does not re-read the
   journal (records don't change during the delay).
f. **Auto-correct** (if `config.auto_correct_drift` AND not fallback
   AND re-verify confirms): real ghost-busy → `usage_bits.cas_bit(off,
   false)` + `used_count.fetch_sub(unit_count)`; ghost-free →
   `usage_bits.cas_bit(off, true)` + `used_count.fetch_add(unit_count)`.
   Normal uncompacted → never auto-correct (compaction's job).

- Edge cases:
  - Empty zone (no records, no snapshot) → no drift, no ghosts.
  - All zones active or locked → `zones_scanned == 0`; log a warning.
  - Degraded mode (KV unreachable) → skip unreadable zones, log
    warning, continue.
  - Fallback used (CRC fail / GC gap) → report drift but do NOT
    auto-correct (corruption signal).

## 3. Record integrity verification

### 3.1 Why

R73's recovery checks CRC on the snapshot it loads at startup, but a
record that corrupts after recovery (bit rot, storage error) is never
re-checked. The integrity scan re-verifies CRC + deserialization +
`owner_chunk` well-formedness on every cycle.

### 3.2 scan_integrity

New file `app/crow-diskdb/src/scanner/integrity.rs`.

```rust
pub struct IntegrityScanResult {
    pub corrupt_snapshots: u64,
    pub corrupt_records: u64,
    pub owner_mismatches: u64,
    pub details: Vec<IntegrityFinding>,
}

pub enum IntegrityFinding {
    CorruptSnapshot { disk_id: DiskId, zone_index: u32 },
    CorruptJournalRecord { disk_id: DiskId, zone_index: u32, key: Vec<u8> },
    OwnerMismatch { disk_id: DiskId, zone_index: u32, unit_offset: u64 },
}
```

`scan_integrity(container, kv, config) -> IntegrityScanResult`
iterates owned zones (same skip + lock logic as ghost scan):

a. Call `read_zone_records` to load `ZoneRecords`.
b. If `zone_value` is present, verify CRC via
   `ZoneValueExt::verify_checksum`. On failure, record
   `CorruptSnapshot`.
c. **Detect corrupt records**: `read_zone_records` silently skips
   undecodable records (the `if let Ok(...)` pattern in its scan
   loops). The integrity scan does its own prefix scans
   (`kv.journal_scan_busy` / `journal_scan_free` are slot-ordered and
   not suitable; instead use `kv.kv().scan(...)` with the busy/free
   prefix and compare the raw item count against the decoded record
   count). A mismatch indicates corrupt records — report each
   undecodable key as `CorruptJournalRecord`. To get the raw keys,
   the scan re-uses `BusyBlockKey::prefix_for_zone` /
   `FreeBlockKey::prefix_for_zone` and iterates `scan.items`,
   attempting `BusyBlockKey::from_bytes` + `bincode::deserialize` on
   each; failures are reported.
d. **Per-block state validation** (if `config.detect_owner_mismatch`):
   for each `BusyRecord`, check `value.owner_chunk` is non-zero
   (`high == 0 && mid == 0 && low == 0` → `OwnerMismatch`). Full
   liveness cross-check against the caller's in-memory `Segment` is
   deferred (needs caller registries, same §2 non-goal as leak
   detection).

- Edge cases:
  - Corrupt `BusyBlockValue` → report, keep the block busy (key
    exists, data may be written), no auto-correction.
  - Corrupt `ZoneValue` snapshot → report `CorruptSnapshot`; the
    ghost scan's fallback path handles the rebuild.
  - Clean zone → all counts zero.

## 4. Leak detection scaffold

### 4.1 Why

Leak detection needs caller registries that do not exist yet (§2
non-goal). The scaffold establishes the interface so the scanner loop
can call it.

### 4.2 scan_for_leaks

New file `app/crow-diskdb/src/scanner/leak.rs`.

```rust
pub struct LeakScanResult {
    pub status: &'static str,  // "deferred"
    pub message: &'static str,
}

pub async fn scan_for_leaks() -> LeakScanResult {
    LeakScanResult {
        status: "deferred",
        message: "Leak detection requires caller registries (not yet \
                  implemented). Use ghost allocation detection for \
                  crash-related orphans.",
    }
}
```

## 5. scan_zone_inner body

### 5.1 Why

`DdbZone::scan_zone_inner` is a stub from R73 (acquires read lock,
body is `// R75`). R75 replaces it with the in-memory bitmap
comparison step. The KV read (journal replay) is done by the scanner
before calling this method; the method does only the locked in-memory
comparison (I9 — lock not held across `.await`).

### 5.2 Implementation

Change `scan_zone_inner(&self)` to
`scan_zone_inner(&self, replayed: &UsageBitmap) -> ScanZoneDiff`:

```rust
pub struct ScanZoneDiff {
    pub ghost_busy_bits: Vec<u32>,   // set in live, clear in replayed
    pub ghost_free_bits: Vec<u32>,   // clear in live, set in replayed
}

pub fn scan_zone_inner(&self, replayed: &UsageBitmap) -> ScanZoneDiff {
    let _guard = self.zone_lock.read().unwrap();
    let mut ghost_busy = Vec::new();
    let mut ghost_free = Vec::new();
    for i in 0..self.usage_bits.word_count() {
        let live_word = self.usage_bits.load_word(i);
        let rep_word = replayed.load_word(i);
        let diff = live_word ^ rep_word;
        if diff == 0 { continue; }
        for bit in 0..64 {
            let mask = 1u64 << bit;
            if diff & mask == 0 { continue; }
            let abs_bit = i * 64 + bit as usize;
            if abs_bit as u32 >= self.unit_capacity { break; }
            if live_word & mask != 0 {
                ghost_busy.push(abs_bit as u32);
            } else {
                ghost_free.push(abs_bit as u32);
            }
        }
    }
    ScanZoneDiff { ghost_busy_bits: ghost_busy, ghost_free_bits: ghost_free }
}
```

The scanner calls this with the replayed bitmap from
`recover_zone_inner` / `rebuild_zone_bitmap_full_scan`. The
classification (real ghost-busy vs normal uncompacted) is done by the
scanner using the record set, not by this method — it only reports
the bit-level diff.

## 6. Scanner metrics

### 6.1 Why

Operator visibility into cluster health during uptime requires
metrics that a monitoring system can scrape and alert on.

### 6.2 Additions to DiskdbMetrics

Add to `DiskdbMetrics::register` in `app/crow-diskdb/src/metrics.rs`:

```rust
pub scanner_runs_total: Arc<Counter>,
pub scanner_duration_ms: Arc<LatencySummary>,
pub scanner_ghosts_found: Arc<Gauge>,
pub scanner_drift_found: Arc<Gauge>,
pub scanner_corrupt_records: Arc<Gauge>,
```

- `scanner_runs_total` — counter, incremented after each scan cycle.
- `scanner_duration_ms` — summary (cold path), observed with the
  cycle duration in ms.
- `scanner_ghosts_found` — gauge, set to the last scan's
  `ghost_busy + ghost_free` count.
- `scanner_drift_found` — gauge, set to the last scan's
  `ghost_busy + ghost_free` (drift = real ghost-busy + ghost-free;
  normal uncompacted is not drift).
- `scanner_corrupt_records` — gauge, set to the last scan's
  `corrupt_snapshots + corrupt_records`.

## 7. Admin RPCs

### 7.1 Why

Operators need a way to trigger an ad-hoc scan (before a maintenance
window) and query the last scan result without waiting for the timer.

### 7.2 Proto

Add to `lib/crow-protocol/src/proto/diskdb_op.proto`:

```protobuf
// ── Scanner admin (R75) ─────────────────────────────────────────

message TriggerScanRequest {
  // empty — runs all enabled scans
}

message TriggerScanResponse {
  ScanSummary summary = 1;
  bool scan_in_progress = 2;  // true if a scan was already running
}

message GetScanStatusRequest {
  // empty
}

message GetScanStatusResponse {
  ScanSummary summary = 1;  // absent if no scan has run
  bool has_run = 2;
}

message ScanSummary {
  uint64 started_at_ms = 1;
  uint64 duration_ms = 2;
  uint64 zones_scanned = 3;
  uint64 zones_skipped_active = 4;
  uint64 zones_skipped_compacting = 5;
  uint64 ghost_busy = 6;
  uint64 ghost_free = 7;
  uint64 uncompacted_lag = 8;
  uint64 corrupt_snapshots = 9;
  uint64 corrupt_records = 10;
  uint64 owner_mismatches = 11;
  string leak_status = 12;
}
```

Add to `lib/crow-protocol/src/proto/diskdb_service.proto`:

```protobuf
  rpc TriggerScan(TriggerScanRequest)   returns (TriggerScanResponse);
  rpc GetScanStatus(GetScanStatusRequest) returns (GetScanStatusResponse);
```

### 7.3 Service handlers

Add `scan_state: Arc<ScanState>` field to `DiskdbService`. Wire it in
`main.rs` (construct `ScanState`, pass to both `ScannerTask` and
`DiskdbService::new`).

- `TriggerScan`: set `scan_state.scan_requested` flag; return current
  `last` summary + `scan_in_progress` flag.
- `GetScanStatus`: return current `last` summary + `has_run` flag.

## 8. Config

### 8.1 Why

The scanner needs tunables for auto-correction, owner validation, and
re-verify delay.

### 8.2 ScannerConfig extensions

Add to `ScannerConfig` in `app/crow-diskdb/src/ddb_config.rs`:

```rust
pub auto_correct_drift: bool,      // default false
pub detect_owner_mismatch: bool,   // default false
pub reverify_delay_ms: u32,        // default 1000
```

## Scope

- `app/crow-diskdb/src/scanner/mod.rs` — new: `ScannerTask`,
  `ScanState`, `ScanSummary`.
- `app/crow-diskdb/src/scanner/ghost.rs` — new: `scan_ghosts`,
  `GhostScanResult`, `GhostBlock`, `GhostDirection`.
- `app/crow-diskdb/src/scanner/integrity.rs` — new: `scan_integrity`,
  `IntegrityScanResult`, `IntegrityFinding`.
- `app/crow-diskdb/src/scanner/leak.rs` — new: `scan_for_leaks`,
  `LeakScanResult`.
- `app/crow-diskdb/src/model/zone.rs` — modify: replace
  `scan_zone_inner` stub with `ScanZoneDiff` body.
- `app/crow-diskdb/src/metrics.rs` — modify: add 5 scanner metrics.
- `app/crow-diskdb/src/ddb_config.rs` — modify: add 3 `ScannerConfig`
  fields.
- `app/crow-diskdb/src/service/diskdb_service.rs` — modify: add
  `scan_state` field + `TriggerScan` / `GetScanStatus` handlers.
- `app/crow-diskdb/src/main.rs` — modify: construct `ScanState` +
  `ScannerTask`, register with `BgRunner`, pass to `DiskdbService`.
- `app/crow-diskdb/src/lib.rs` — modify: add `pub mod scanner`.
- `lib/crow-protocol/src/proto/diskdb_op.proto` — modify: add
  `TriggerScan*` / `GetScanStatus*` / `ScanSummary` messages.
- `lib/crow-protocol/src/proto/diskdb_service.proto` — modify: add 2
  rpc entries.
- `app/crow-diskdb/tests/scanner_test.rs` — new: unit + integration
  tests.

## Complexity

Medium. The ghost scan's bit-by-bit diff + record classification is
the genuinely hard part — it must correctly distinguish real
ghost-busy (drift) from normal uncompacted (expected in the
persist-only model) and apply the data-safety principle (never free a
block that might have data). The re-verify step adds timing
sensitivity. The rest is wiring (metrics, config, proto, service
handlers) that mirrors existing patterns (`RecalcEngine`,
`ReportingTask`, `CompactZone`).

## Test Design

### Unit tests (UT)

- **scan_zone_inner diff**: set up a `DdbZone` (128 units), set bits
  0..4 in live, set bits 0..2 in replayed → assert
  `ghost_busy_bits == [3, 4]`, `ghost_free_bits == []`. Set bit 6 in
  replayed only → assert `ghost_free_bits == [6]`.
- **scan_zone_inner no diff**: live == replayed → empty diff.
- **scan_for_leaks**: returns `LeakScanResult { status: "deferred" }`
  without panicking.
- **ScannerConfig defaults**: `auto_correct_drift == false`,
  `detect_owner_mismatch == false`, `reverify_delay_ms == 1000`.
- **ScannerConfig TOML**: deserialize a TOML string with the scanner
  section → assert all 6 fields parsed.
- **ScanState**: `new()` → `last` is `None`, `scan_requested` is
  false; `set_scan_requested()` → flag is true; `record_summary()`
  → `last` is `Some`.

### Integration tests (E2E — KvCluster harness)

- **scan_ghosts detects real ghost-busy**: seed cluster, allocate 3
  blocks, free 1, manually set a live bitmap bit for a block with no
  `BusyBlockKey` + no `FreeBlockKey` → run `scan_ghosts` → assert
  `ghost_busy >= 1` and the block is in `details` with direction
  `GhostBusy`.
- **scan_ghosts classifies normal uncompacted as NOT drift**: persist
  a `FreeBlockValue` (no `BusyBlockKey`), leave the live bit set →
  run `scan_ghosts` → assert `ghost_busy == 0`, `uncompacted_lag >=
  1`.
- **scan_ghosts detects ghost-free**: persist a `BusyBlockValue`,
  manually clear the live bit → run `scan_ghosts` → assert
  `ghost_free >= 1`.
- **scan_ghosts zero ghosts when clean**: allocate + persist all, no
  frees → `ghost_busy == 0`, `ghost_free == 0`, `uncompacted_lag ==
  0`.
- **scan_ghosts skips active zones**: add a zone to
  `active_zone_context` → `skipped_active >= 1`, zone not in
  `details`.
- **scan_ghosts skips zones locked by compaction**: hold the zone
  lock → `skipped_compacting >= 1`.
- **re-verify filters transient drift**: set `reverify_delay_ms =
  100`, set a transient ghost-busy bit, clear it during the delay →
  ghost not reported.
- **re-verify confirms persistent drift**: set
  `reverify_delay_ms = 100`, persistent ghost-busy → ghost reported.
- **auto-correct real ghost-busy**: enable `auto_correct_drift`,
  persistent ghost-busy → live bit cleared, `used_count`
  decremented.
- **auto-correct ghost-free**: enable `auto_correct_drift`,
  persistent ghost-free → live bit set back, `used_count`
  incremented.
- **auto-correct normal uncompacted NOT corrected**: enable
  `auto_correct_drift`, uncompacted free → live bits NOT cleared.
- **auto-correct no correct on fallback**: enable
  `auto_correct_drift`, corrupt `ZoneValue` CRC → no auto-correct.
- **scan_integrity detects CRC corruption**: write `ZoneValue` with
  valid CRC, corrupt one byte → `corrupt_snapshots == 1`.
- **scan_integrity detects deserialization failure**: write a
  `BusyBlockValue`, corrupt the value bytes → `corrupt_records >= 1`.
- **scan_integrity detects owner_chunk mismatch**: enable
  `detect_owner_mismatch`, write `BusyBlockValue` with
  `owner_chunk = 0` → `owner_mismatches == 1`.
- **scan_integrity clean zone**: all valid → all counts zero.
- **data-safety on corrupt record**: `BusyBlockValue` fails to
  deserialize → scanner reports it but does NOT free the block.
- **TriggerScan + GetScanStatus**: start a `DiskdbService` with a
  `ScanState`, call `TriggerScan` → response contains a summary (or
  empty); call `GetScanStatus` → returns the last summary.
- **ScannerTask runs on BgRunner**: register `ScannerTask`, spawn,
  wait > interval → assert `scanner_runs_total` incremented.

## Module Structure

```
app/crow-diskdb/src/scanner/
    mod.rs          — ScannerTask, ScanState, ScanSummary, run_cycle
    ghost.rs        — scan_ghosts, GhostScanResult, GhostBlock, GhostDirection
    integrity.rs    — scan_integrity, IntegrityScanResult, IntegrityFinding
    leak.rs         — scan_for_leaks, LeakScanResult (scaffold)
```

## Config Extensions

- `ScannerConfig.auto_correct_drift: bool` (default `false`).
- `ScannerConfig.detect_owner_mismatch: bool` (default `false`).
- `ScannerConfig.reverify_delay_ms: u32` (default `1000`).
- No `validate()` changes (all are non-negative with sensible
  defaults; `reverify_delay_ms == 0` is valid = disable re-verify).

## Server Wiring

1. `main.rs`: construct `Arc<ScanState>` after `DiskdbMetrics::register`.
2. `main.rs`: construct `ScannerTask::new(Arc::clone(&scan_state))`,
   register with `BgRunner` (after `reporting_task`).
3. `main.rs`: pass `Arc::clone(&scan_state)` to `DiskdbService::new`
   (new sixth parameter).
4. `BgRunner::spawn` — the scanner task runs on the same stop signal
   as the other bg tasks.
