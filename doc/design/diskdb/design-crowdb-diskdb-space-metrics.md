<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: diskdb Space Metrics + Query API

Depends on: [`design-crowdb-diskdb.md`](design-crowdb-diskdb.md) §3.4 (records are
source of truth), §7 (record model + recovery strategies), §3.7 (reuse
`crowdb-common` metrics), §8 (allocation algorithm); [`design-crowdb-kv-group0.md`](../kv/design-crowdb-kv-group0.md)
(hardware hierarchy + service registry).
Satisfies: `design-crowdb-diskdb.md` §11 (Space Metrics).

Detailed design for diskdb's space-metrics component — the third major
component after the allocator (§8) and recovery (§7). Architecture
decisions and rationale live in the root design doc; this doc carries
the structs, algorithms, RPC shapes, and data flow. Reused surfaces
(named, not re-built): `DdbDiskGroupContainer`/`DdbDiskGroup`/`DdbDisk`,
`DdbKvClient`, `DiskdbMetrics`, `model/alloc.rs` two-phase
allocate/free, `RecoveryEngine` (`recover_zone_inner`,
`rebuild_zone_bitmap_full_scan`), `CompactionEngine`,
`HardwareClient` (hierarchy scans), `ServiceRegistryClient`
(`read_all_diskdb_instances`, `heartbeat_diskdb` accepting
`DiskGroupUsageSummary[]`), `DiskGroupUsageKey` + `prefix_all`.

## Table of Contents

- [1. Per-zone usage accessors](#1-per-zone-usage-accessors)
- [2. Per-disk + per-disk-group aggregation](#2-per-disk--per-disk-group-aggregation)
- [3. Per-disk hot-path counters](#3-per-disk-hot-path-counters)
- [4. QueryCapacityStats handler](#4-querycapacitystats-handler)
- [5. Recalculation path](#5-recalculation-path)
- [6. crowdb-common metrics integration](#6-crowdb-common-metrics-integration)
- [7. Reporting loop](#7-reporting-loop)
- [8. Keepalive usage piggyback](#8-keepalive-usage-piggyback)
- [9. Schema extension](#9-schema-extension)
- [10. kv-client space-usage aggregation](#10-kv-client-space-usage-aggregation)
- [11. diskdb-client — full client library](#11-diskdb-client--full-client-library)
- [12. E2E test migration](#12-e2e-test-migration)
- [Invariants](#invariants)
- [Module Structure](#module-structure)
- [Config Extensions](#config-extensions)
- [Server Wiring](#server-wiring)

## 1. Per-zone usage accessors

`DdbZone` tracks `used_count: AtomicU32`, `unit_capacity: u32`, and
`UsageBitmap::count_set()`, but exposes no busy/free/bytes/ratio
accessors. Every higher-level aggregation (disk, disk-group, query
handler, keepalive piggyback, reporting-loop gauges) bottoms out at the
zone, so the accessors live here. CROWDB reuses freed space immediately
via bitmap scan (no append-only `allocate_pos`, §3.4/§8), so
`free = capacity - busy`. There is no separate free cursor to read.

```rust
/// Busy block units (live `used_count`).
pub fn busy_blocks(&self) -> u32 { self.used_count.load(Ordering::Acquire) }
/// Free block units = `unit_capacity - used_count`.
pub fn free_blocks(&self) -> u32 { self.unit_capacity.saturating_sub(self.busy_blocks()) }
/// Busy bytes = `busy_blocks * unit_size_bytes`.
pub fn busy_bytes(&self, unit_size_bytes: u32) -> u64 { u64::from(self.busy_blocks()) * u64::from(unit_size_bytes) }
/// Free bytes = `free_blocks * unit_size_bytes`.
pub fn free_bytes(&self, unit_size_bytes: u32) -> u64 { u64::from(self.free_blocks()) * u64::from(unit_size_bytes) }
/// Capacity bytes = `unit_capacity * unit_size_bytes`.
pub fn capacity_bytes(&self, unit_size_bytes: u32) -> u64 { u64::from(self.unit_capacity) * u64::from(unit_size_bytes) }
/// Usage ratio = `used_count / unit_capacity` as f64.
pub fn usage_ratio(&self) -> f64 {
    let used = f64::from(self.busy_blocks());
    let cap = f64::from(self.unit_capacity);
    if cap == 0.0 { 0.0 } else { used / cap }
}
```

- `busy_blocks` reads the live atomic; `free_blocks` derives from
  capacity. `UsageBitmap::count_set()` is **not** used on the read path
  — it is reserved for the recalc verifier (§5) which needs an
  independent popcount of the bitmap bytes, not the cached
  `used_count`.
- The `unit_size_bytes` argument is per-disk (from
  `DiskValue.unit_size_bytes`), not a global shift — disks in one
  disk-group may have different unit sizes in principle, so the caller
  passes the owning disk's value.

Edge cases:
- `unit_capacity == 0` (degenerate) → `usage_ratio` returns 0.0
  (guarded).
- Saturating sub on `free_blocks` guards against a transient
  `used_count > unit_capacity` (should never happen; a CAS bug would
  set it).
- These are read-only and lock-free; safe to call concurrently with
  allocate/free.

## 2. Per-disk + per-disk-group aggregation

`DdbDisk` and `DdbDiskGroup` own the zone/disk collections but expose
no aggregated usage. The query handler, keepalive piggyback, and
reporting loop all need a single struct built from in-memory state (no
KV reads). The aggregation is a straightforward sum, but the struct
shape is reused in three places, so it is defined once.

```rust
/// Per-zone brief usage (counts only — no bitmap bytes).
pub struct ZoneUsage {
    pub zone_index: u32,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub busy_block_count: u32,
    pub free_block_count: u32,
    pub alloc_state: ZoneAllocationState,  // derived_alloc_state()
    pub zone_state: DdbZoneHealth,
}

/// Per-disk usage (aggregated across zones).
pub struct DiskUsage {
    pub disk_id: DiskId,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub zone_count: u32,
    pub active_zone_count: u32,
    pub busy_zone_count: u32,   // zones with used_count == unit_capacity
    pub free_zone_count: u32,   // zones with used_count < unit_capacity
}

/// Per-disk-group usage (aggregated across disks).
pub struct DiskGroupUsage {
    pub disk_group_id: DiskGroupId,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub disk_count: u32,
    pub allocatable_disk_count: u32,
    pub disks: Vec<DiskUsage>,
}
```

`DdbDisk::usage(&self, unit_size_bytes: u32) -> DiskUsage`:
- Read `disk_value` + `zones` under their read locks.
- Sum `capacity_bytes`/`busy_bytes`/`free_bytes` via each zone's
  accessors (§1) using the disk's `unit_size_bytes` (passed in, or read
  from `disk_value.unit_size_bytes`).
- `zone_count` = `zones.len()`; `active_zone_count` =
  `active_zone_context.read().len()` (RCU snapshot size).
- `busy_zone_count` = zones with `busy_blocks() == unit_capacity`;
  `free_zone_count` = the rest.

`DdbDiskGroup::aggregate_usage(&self) -> DiskGroupUsage`:
- Read `disks` under the read lock; read each disk's `disk_value` for
  its `unit_size_bytes`.
- Sum across disks; `disk_count` = `disks.len()`;
  `allocatable_disk_count` = `allocating_disks.read().len()` (RCU
  context size — matches the `allocatable_disk_count` semantics: disks
  currently `Up` and allocatable).
- Carry the per-disk `DiskUsage` breakdown in `disks`.

`DdbDiskGroup::zone_usage(&self, disk_id: DiskId, zone_index: u32) -> Option<ZoneUsage>`:
- Locate the disk via `disk_index` (read lock); `None` if unknown disk.
- Locate the zone by `zone_index` (read lock on `disk.zones`); `None`
  if out of range.
- Build `ZoneUsage` from the zone's accessors +
  `derived_alloc_state()` + `*zone.zone_state.read()`. **Brief counts
  only** — no bitmap bytes.

Edge cases:
- Disk with zero zones → `DiskUsage` zeroes, `zone_count == 0`.
- A `Bad` disk is still summed (its zones carry their last-known
  `used_count`); it is excluded from `allocatable_disk_count` but its
  capacity still counts in the total. This matches "capacity is total;
  allocatable is the live set."
- `zone_usage` for an out-of-range index → `None` (caller maps to
  `NotFound`).
- Read locks make the snapshot best-effort-consistent across
  zones/disks; a concurrent `disk_add_init`/`remove_disk` may miss a
  transient disk this tick. Acceptable for metrics (§11: exact
  consistency not required; recalc gives exact verification).

## 3. Per-disk hot-path counters

§11 calls for per-disk event counters (allocate/free count + bytes) on
the hot path. The capacity/busy/free **gauges** are derived from the
bitmap on the reporting tick (single source of truth). Maintaining
parallel capacity atomics would drift from the bitmap. So the per-disk
hot-path struct holds **only** period/total event counters; gauges are
computed in the reporting loop (§7).

`metrics.rs` is a module dir: `metrics/mod.rs` (existing `DiskdbMetrics`
moves here) + `metrics/disk.rs`. `DiskMetrics` holds lock-free atomic
counters per `DdbDisk`:

```rust
pub struct DiskMetrics {
    // Period counters (swapped to 0 by the reporting loop each tick).
    period_allocate_count: AtomicU64,
    period_allocate_bytes: AtomicU64,
    period_free_count: AtomicU64,
    period_free_bytes: AtomicU64,
    // Total counters (monotonic, never reset).
    total_allocate_count: AtomicU64,
    total_allocate_bytes: AtomicU64,
    total_free_count: AtomicU64,
    total_free_bytes: AtomicU64,
}

impl DiskMetrics {
    pub fn new() -> Self { /* all zero */ }
    /// Bump period + total count/bytes. Called after the Phase 1 bitmap
    /// CAS succeeds (exactly once per durable-bound allocation).
    pub fn record_allocate(&self, unit_count: u32, unit_size_bytes: u32) { ... }
    /// Bump period + total count/bytes. Called after the Phase 1 bitmap
    /// clear succeeds.
    pub fn record_free(&self, unit_count: u32, unit_size_bytes: u32) { ... }
    /// Atomically swap period counters to 0, return the deltas.
    pub fn swap_periods(&self) -> PeriodSnapshot { ... }
}

pub struct PeriodSnapshot {
    pub allocate_count: u64, pub allocate_bytes: u64,
    pub free_count: u64, pub free_bytes: u64,
}
```

- All atomics use `Ordering::Relaxed` (counters, no cross-variable
  ordering).
- `record_allocate`/`record_free` do `fetch_add(unit_count)` on count
  and `fetch_add(u64::from(unit_count) * u64::from(unit_size_bytes))`
  on bytes, on both period and total.
- `swap_periods` does `swap(0, Relaxed)` on each period counter; totals
  are kept inline (also incremented in `record_*`) for crash-safe
  monotonicity — the reporting loop does **not** add period deltas to
  totals (they're already there); it only flushes period deltas into
  the crowdb-common `allocate_total`/`free_total` counters.

Wiring: a `metrics: Option<Arc<DiskMetrics>>` field on `DdbDisk`, attached
during `disk_add_init` (keepalive) and `recover_disk_group` (recovery).
`None` in tests that don't care. The allocate/free paths in
`model/alloc.rs` call
`disk.metrics.as_ref().map(|m| m.record_allocate(range.unit_count, unit_size))`
after the Phase 1 CAS succeeds (before Phase 2 persist. The counter
reflects a durable-bound allocation; on Phase 2 failure the bitmap
rolls back but the counter over-counts by one, which is acceptable for a
best-effort event counter and avoids a second hot-path branch on the
persist result).

Edge cases:
- `AtomicU64` overflow at 1M-unit blocks → extremely unlikely;
  `swap_periods` saturating semantics avoid loss; no special handling.
- `DiskMetrics == None` (test disk) → record calls are no-ops;
  aggregation unaffected.
- Counter over-count on Phase 2 rollback (above) — documented,
  acceptable for an event counter; the `allocate_errors_total` counter
  (§6) separately tracks persist failures.

## 4. QueryCapacityStats handler

`QueryCapacityStats` is the operator/console drill-down surface for live
per-disk/per-zone usage, the authoritative counterpart to the
kv-client's stale cluster-wide view. The request carries two drill-down
fields (`disk_id`, `zone_index`) to select three query shapes without
three separate RPCs.

Proto (§9): `QueryCapacityStatsRequest` gains `optional uint64 disk_id = 2`
and `optional uint32 zone_index = 3` (0 = not set). `DiskInfo`/
`DiskGroupInfo` gain usage fields + `repeated ZoneUsage zone_usages`.
`ZoneUsage` message added.

The handler in `service/diskdb_service.rs` is read-only, allowed in any
lifecycle phase (like `GetDiskGroupInfo`); it does **not** check
`allows_mutating_rpcs` or `is_degraded`.

- **Cluster/disk-group level** (`disk_group_id` only, `disk_id == 0`):
  if `disk_group_id == 0`, iterate `container.disk_group_ids()` and
  build one `DiskGroupInfo` per owned group; if set, look up that one
  group (`NotFound` if not owned). Each `DiskGroupInfo` carries the
  aggregated capacity/busy/free + member `DiskInfo`s with per-disk
  usage + zone counts. **No `zone_usages` entries** at this level.
- **Disk level** (`disk_group_id` + `disk_id` set, `zone_index == 0`):
  return the one `DiskInfo` with a `zone_usages` list of **brief**
  per-zone `ZoneUsage` entries (counts only — no `usage_bitmap`). Built
  via `DdbDisk::usage` + a loop over zones calling `DdbZone` accessors
  + `derived_alloc_state`. `NotFound` if the disk is not in the group.
- **Zone level** (`disk_group_id` + `disk_id` + `zone_index` all set):
  return the one `ZoneUsage` with the **full `usage_bitmap` bytes** via
  `DdbZone::usage_bits.snapshot()`. Out-of-range `zone_index` →
  `NotFound`.

`disk_id == 0` sentinel: `DiskId` is a message (`{high, low}`); 0 means
`Some(DiskId{high:0,low:0})` is treated as "not set" only when the field
is `None`. The schema field is `disk_id: DiskId` so
"not set" = `None`; a real disk with all-zero id is pathological and
rejected at disk-add time. `zone_index == 0` is ambiguous (zone 0 is
valid), so the disk-level shape is selected by `disk_id` being set
**and** `zone_index` being absent. Use proto3 `optional` on `zone_index`
(field presence) so "not set" is distinguishable from 0. (proto3 scalar
presence requires `optional` keyword; flatbuffers generates `Option<u32>`.)

Edge cases:
- `disk_group_id == 0` + zero owned groups → empty `disk_groups`, `Ok`.
- Unowned `disk_group_id` → `NotFound` (matches `GetDiskGroupInfo`).
- Disk with thousands of zones → disk-level returns brief entries (no
  bitmap bytes); only zone-level serializes one bitmap.
- Out-of-range `zone_index` → `NotFound` (not silent empty bitmap).
- Zone with `used_count == unit_capacity` → `Full` alloc_state,
  `free == 0`; reported as-is, not an error.
- Last zone smaller (word-aligned) → `unit_capacity` is the rounded
  value; `capacity_bytes` reflects the real smaller capacity.

## 5. Recalculation path

§11 requires "accurate statistics with a recalculation path": replay
the journal into a **separate** bitmap and compare against the live
`DdbZone` to detect drift. This is the exact-verification counterpart to
the best-effort-consistent gauges. The background scanner (§12 of the
root doc) reuses `recalc_all`. v1 reports drift only (no auto-correct;
operator runs `RebuildZoneBitmap`).

`RecalcEngine` in `metrics/recalc.rs`:

```rust
pub struct RecalcEngine {
    kv: Arc<DdbKvClient>,
    container: Arc<DdbDiskGroupContainer>,
}

pub struct RecalcResult {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub matches: bool,
    pub live_busy_blocks: u32,
    pub replayed_busy_blocks: u32,
    pub live_snapshot_slot: u64,
    pub replayed_snapshot_slot: u64,
    pub drift_detected: bool,
    pub fallback_used: Option<FallbackReason>,  // None | JournalScanGcGap | SnapshotCrcFail
}

pub enum FallbackReason { JournalScanGcGap, SnapshotCrcFail }

pub struct DiskGroupRecalcResult {
    pub disk_group_id: DiskGroupId,
    pub zone_results: Vec<RecalcResult>,
    pub drift_detected: bool,
}
```

`recalc_zone(bind, disk_id, zone_idx, unit_capacity, live_zone: &DdbZone) -> Result<RecalcResult>`:
- Call `recover_zone_inner(&self.kv, bind, disk_id, zone_idx, unit_capacity)`
  (strategy 2) into a throwaway `DdbZone`. On `JournalScanGcGap` /
  `SnapshotCrcFail`, fall back to `rebuild_zone_bitmap_full_scan`
  (strategy 1); record `fallback_used`.
- `replayed_busy_blocks` = replayed zone's `usage_bits.count_set()`
  (independent popcount, not its `used_count`).
- `live_busy_blocks` = live zone's `busy_blocks()` (its `used_count`).
- `matches = (replayed_busy_blocks == live_busy_blocks)`;
  `drift_detected = !matches`.
- v1 does **not** mutate the live zone — drift is reported only.

`recalc_disk_group(dg_id) -> Result<DiskGroupRecalcResult>`: iterate all
disks + zones in the disk-group, call `recalc_zone` per zone (bounded
concurrency via a semaphore, matching `recover_disk_group`), aggregate.

`recalc_all() -> Result<Vec<DiskGroupRecalcResult>>`: iterate
`container.disk_group_ids()`, call `recalc_disk_group` per owned group.

The `RecalcDiskUsage` admin RPC (§9) delegates to `RecalcEngine`; the
handler lives in `service/diskdb_service.rs` (mutating RPC, requires
`allows_mutating_rpcs`, since recalc does KV reads/journal scans and is
an admin operation, not a read-only query).

Edge cases:
- `JournalScanGcGap` → strategy 1 fallback;
  `fallback_used = Some(JournalScanGcGap)`.
- `SnapshotCrcFail` → strategy 1 fallback; the live bitmap is suspect;
  `drift_detected = true` with `FallbackReason::SnapshotCrcFail` even
  if counts happen to match (the snapshot was corrupt).
- Strategy 1 also fails → return a `RecalcResult` with
  `matches = false`, `drift_detected = true`, and a KV error noted (do
  not panic; the operator sees the failure in the response).
- Live `used_count` unchanged after recalc (v1 no auto-fix).

## 6. crowdb-common metrics integration

§11 specifies three metric categories (counters, gauges, latency
hierarchy) reusing `crowdb-common`'s `MetricsRegistry`. `DiskdbMetrics` in
`metrics/mod.rs` holds the full set. Gauges are derived snapshots
(bitmap-derived on the reporting tick), not hot-path writes.

- **Gauges** (`Arc<Gauge>`): `disk_capacity_bytes`, `disk_busy_bytes`,
  `disk_free_bytes`, `disk_active_zone_count`, `disk_total_zone_count`,
  `dg_capacity_bytes`, `dg_busy_bytes`, `dg_free_bytes`,
  `owned_disk_group_count`, `degraded`, `last_sync_age_secs`.
  (Per-disk/per-disk-group gauges are single instances updated each tick
  with the summed value — v1 does not label per-disk-id in the registry;
  the reporting loop sums across disks into the single gauge. The
  console reads the live per-disk view via `QueryCapacityStats`, not the
  metrics log.)
- **Counters** (`Arc<Counter>`): `allocate_total`, `free_total` (flushed
  from per-disk `DiskMetrics` totals by the reporting loop),
  `allocate_errors_total`, `sync_success_total`,
  `sync_failure_total`, `compaction_records_deleted_total`.
- **Latency histograms** (`Arc<LatencyHistogram>`, hot paths):
  `allocate.rpc.latency_us`, `allocate.bitmap_scan.latency_us`,
  `allocate.kv_persist.latency_us`, `free.rpc.latency_us`,
  `free.persist.latency_us`, `free.kv_persist.latency_us`.
- **Latency summaries** (`Arc<LatencySummary>`, cold paths):
  `allocate.zone_rotate.latency_us`, `sync.latency_us`,
  `sync.read_group0.latency_us`, `sync.apply_changes.latency_us`,
  `compaction.latency_us`, `compaction.scan_free.latency_us`,
  `compaction.merge_bitmap.latency_us`, `compaction.kv_persist.latency_us`,
  `recovery_duration_ms`.

`register(registry: &mut MetricsRegistry) -> Self` registers all of the
above (plus the two existing R72 counters). `disabled()` constructs a
throwaway registry for tests.

Instrumentation points (start-instant → observe-on-completion,
nanoseconds):
- `model/alloc.rs` `allocate_block`/`allocate_blocks`: observe
  `allocate.rpc.latency_us` (total), `allocate.bitmap_scan.latency_us`
  (around `dg.allocate_block(s)`), `allocate.kv_persist.latency_us`
  (around `kv.persist_busy_batch`). On persist failure,
  `allocate_errors_total.inc()`.
- `model/alloc.rs` `free_block`/`free_blocks`: observe
  `free.rpc.latency_us`, `free.persist.latency_us` (around
  `dg.free_block`), `free.kv_persist.latency_us`.
- `liveness/keepalive.rs` `tick`: observe `sync.latency_us`,
  `sync.read_group0.latency_us` (around `observe_ownership` +
  `observe_disks`), `sync.apply_changes.latency_us` (around
  reconcile). `sync_success_total`/`sync_failure_total` per tick
  outcome.
- `recovery/compaction.rs`: observe `compaction.*` summaries;
  `compaction_records_deleted_total` per compaction.

The latency handles are passed into the allocate/free paths via
`DiskdbMetrics` (already threaded through `BgCtx`; for the service
handler, `DiskdbService` gains an `Arc<DiskdbMetrics>` field). To avoid
plumbing metrics through every `alloc::allocate_block` signature, wrap
the latency observation in the service handler around the `alloc::` call
(rpc + kv_persist measured together at the handler boundary) and add a
finer-grained `bitmap_scan` observation inside `alloc.rs` guarded by an
optional `&DiskdbMetrics` parameter. v1 measures rpc + bitmap_scan +
kv_persist at the handler/alloc boundary; full per-phase splitting is
best-effort.

Edge cases:
- Histogram/summary `observe(0)` is valid (sub-microsecond ops).
- Metrics handle `None` in test paths → `disabled()` registry is used.

## 7. Reporting loop

The hot-path atomic counters (`DiskMetrics`) + bitmap state must be
bridged to the crowdb-common `Gauge`/`Counter` reporting layer on a
cadence. §11: gauges are derived snapshots updated on the reporting
interval, not hot-path writes.

`ReportingTask` in `metrics/reporting.rs` implements `BackgroundTask`.
Registered in `BgRunner` alongside keepalive + compaction in `main.rs`.
Every 10 s (configurable via `reporting.interval_secs`, live-reload via
the shared config handle):

- For each owned disk-group → each disk:
  `DiskMetrics::swap_periods()` → flush period deltas into
  `allocate_total`/`free_total` counters (`inc_by(period.allocate_count)`
  etc.).
- Recompute gauges from the bitmap:
  `DdbDiskGroup::aggregate_usage()` → set
  `dg_capacity_bytes`/`dg_busy_bytes`/`dg_free_bytes` (summed across
  groups); per-disk sums → `disk_*` gauges;
  `disk_active_zone_count`/`disk_total_zone_count` summed.
- `owned_disk_group_count.set(container.disk_group_ids().len())`;
  `degraded.set(if container.is_degraded() {1} else {0})`;
  `last_sync_age_secs.set(...)` (tracked from the last successful
  keepalive tick — keepalive records a
  `last_sync_at: Mutex<Option<Instant>>` on the container, or the
  reporting task reads a shared `AtomicU64` epoch).

`ReportingTask` owns an `Arc<DdbDiskGroupContainer>` +
`Arc<DiskdbMetrics>` + the shared config handle (for the interval).
`run_cycle` does the flush; `trigger` returns `Trigger::TimerFn` reading
`reporting.interval_secs`.

Edge cases:
- Tick during concurrent `disk_add_init`/`remove_disk` → read locks
  make the gauge snapshot best-effort; a transient disk may be missed
  this tick. Acceptable (§11).
- Zero owned groups → gauges set to 0, counters unchanged.

## 8. Keepalive usage piggyback

`heartbeat_diskdb` accepts `DiskGroupUsageSummary[]` but keepalive
passes `&[]`. Group 0 stores disk-group-level usage at
`/hw/dg_usage/<dg_id>`; the console reads it for the cluster-wide view.
The summary is derived (recomputed each tick from the bitmap), not a
source of truth.

In `liveness/keepalive.rs` `tick()`, after `observe_disks`, compute one
`DiskGroupUsageSummary` per owned disk-group from `aggregate_usage()`:

```rust
let owned_dg_ids: Vec<u64> = ...;
let group_usages: Vec<DiskGroupUsageSummary> = container
    .disk_group_ids()
    .into_iter()
    .map(|dg_id| {
        let dg = container.get_disk_group(dg_id).unwrap();
        let u = dg.aggregate_usage();
        DiskGroupUsageSummary {
            disk_group_id: dg_id,
            capacity_bytes: u.capacity_bytes,
            used_bytes: u.busy_bytes,
            free_bytes: u.free_bytes,
            disk_count: u.disk_count,
            allocatable_disk_count: u.allocatable_disk_count,
        }
    })
    .collect();
```

Pass `&owned_dg_ids` + `&group_usages` to `svc.heartbeat_diskdb(instance_id,
endpoint, &owned_dg_ids, &group_usages)` instead of `&[]`. The endpoint
string is the diskdb rpc listen address (from config; passed as the
real `server.listen_addr` so group 0 records a reachable endpoint for
the diskdb-client cache). The summary is recomputed each tick (not
cached).

Edge cases:
- Zero owned groups → empty `group_usages` (not an error).
- `heartbeat_diskdb` fails (group 0 unreachable) → keepalive already
  handles this (missed count → degraded); the summary is just not
  delivered this tick.
- Group 0 stores **only disk-group-level** usage; per-disk/per-zone
  live usage is never written to group 0.

## 9. Schema extension

`lib/crowdb-protocol/src/fbs/diskdb_type.fbs`:

```fbs
table ZoneUsage {
  zone_index: uint32;
  capacity_bytes: uint64;
  busy_bytes: uint64;
  free_bytes: uint64;
  busy_block_count: uint32;
  free_block_count: uint32;
  alloc_state: ZoneAllocationState;
  // Populated only for a specific-zone query; omitted at disk level.
  usage_bitmap: [ubyte];
}
```

`DiskInfo` gains `busy_units`, `free_units`, `capacity_bytes`,
`busy_bytes`, `free_bytes`, `active_zone_count`, and
`[ZoneUsage] zone_usages`. `DiskGroupInfo` gains
`capacity_bytes`, `busy_bytes`, `free_bytes`,
`allocatable_disk_count`.

`diskdb_op.fbs`:
- `QueryCapacityStatsRequest`: add
  `disk_id: DiskId` and
  `zone_index: uint32`.
- Add `RecalcDiskUsageRequest { disk_group_id: uint64; }` /
  `RecalcDiskUsageResponse { results: [DiskGroupRecalcResult]; }`
  + `DiskGroupRecalcResult { disk_group_id: uint64; drift_detected: bool;
  zones: [ZoneRecalcResult]; }` + `ZoneRecalcResult { ... }`
  (mirror `RecalcResult`).

`diskdb_service.fbs`: add
`rpc RecalcDiskUsage(RecalcDiskUsageRequest) -> (RecalcDiskUsageResponse);`

Edge cases:
- flatbuffers scalar presence → flatc generates `Option<u32>` so
  `zone_index` absence is distinguishable from 0.
- `usage_bitmap` is `[ubyte]` → `Option<Vec<u8>>`; omitted at
  disk level.

## 10. kv-client space-usage aggregation

No client reads `/hw/dg_usage/*` back. The console needs a
cluster/rack/node/disk-group capacity view built from the piggyback
summaries joined with the hardware hierarchy. This is the stale (≤1
sync interval) cluster-wide view; live per-disk/per-zone drill-down is
via the diskdb-client (§11).

`SpaceUsageClient` in `lib/crowdb-kv-client/src/space_usage.rs` wraps
`HardwareClient` (which already has the hierarchy scans +
`scan_prefix`). Re-exported from `lib.rs`.

```rust
pub struct SpaceUsageClient { hw: HardwareClient }

impl SpaceUsageClient {
    pub fn new(hw: HardwareClient) -> Self { Self { hw } }

    /// Prefix-scan `/hw/dg_usage/` → one summary per disk-group.
    pub async fn list_disk_group_usages(&self)
        -> Result<Vec<(DiskGroupId, DiskGroupUsageSummary)>>;

    /// Join summaries with the hardware hierarchy → cluster-level totals.
    pub async fn cluster_usage(&self) -> Result<ClusterUsage>;

    /// Scoped aggregations down the hierarchy.
    pub async fn rack_usage(&self, rack_id: RackId) -> Result<RackUsage>;
    pub async fn node_usage(&self, rack_id: RackId, node_id: NodeId) -> Result<NodeUsage>;
    pub async fn disk_group_usage(&self, dg_id: DiskGroupId) -> Result<Option<DiskGroupUsageSummary>>;
}
```

- `list_disk_group_usages`: `scan_prefix::<DiskGroupUsageSummary>` over
  `DiskGroupUsageKey::prefix_all()` text path (`/hw/dg_usage/`). The
  values are JSON-encoded (group 0 stores text-path keys with JSON
  values, matching `HardwareClient`'s convention).
- `cluster_usage`: read all summaries + `list_racks`/`list_nodes`/
  `list_disk_groups`/`list_disks_in_group`; sum capacity/used/free +
  disk-group count. Per-disk live busy/free is **not** available here
  (group 0 has only disk-group-level usage + per-disk static
  `DiskValue.capacity_units`); the disk-level view carries capacity
  only.
- `rack_usage`/`node_usage`/`disk_group_usage`: scope the join to the
  hierarchy subtree.

`ClusterUsage`/`RackUsage`/`NodeUsage` structs hold capacity/used/free
bytes + disk-group/disk counts. Mirror `crowdb-kv-client`'s retry pattern
(the underlying `HardwareClient` already retries).

Edge cases:
- No summaries written yet (fresh cluster) → empty vec / zero totals.
- Stale summary (≤1 sync interval behind) → documented; not an error.
- Per-disk busy/free absent → `NodeUsage.disk_capacity_bytes` only, no
  busy/free.

## 11. diskdb-client — full client library

`crowdb-diskdb-client` is the primary client surface for all diskdb rpc
operations (allocate/free/query with retry + endpoint caching),
mirroring `crowdb-kv-client`'s pattern. Without it, callers must use raw
rpc stubs and do endpoint discovery manually.

`DiskdbClient` in `lib/crowdb-diskdb-client/src/client.rs`:

```rust
pub struct DiskdbClient {
    svc: ServiceRegistryClient,        // endpoint discovery from group 0
    cache: DashMap<DiskGroupId, String>, // dg_id -> rpc_endpoint
    retry: RetryConfig,
}

impl DiskdbClient {
    pub fn new(svc: ServiceRegistryClient) -> Self;
    /// Eager warm: read all diskdb instances, populate cache.
    pub async fn refresh_endpoints(&self) -> Result<()>;
    /// Lazy refresh on cache miss for one dg_id.
    async fn refresh_for(&self, dg_id: DiskGroupId) -> Result<()>;

    pub async fn allocate_blocks(&self, req: AllocateBlocksRequest) -> Result<AllocateResponse>;
    pub async fn free_blocks(&self, req: FreeBlocksRequest) -> Result<FreeResponse>;
    pub async fn query_capacity_stats(&self, req: QueryCapacityStatsRequest) -> Result<QueryCapacityStatsResponse>;
    pub async fn query_disk_group(&self, dg_id: u64) -> Result<QueryCapacityStatsResponse>;
    pub async fn query_disk(&self, dg_id: u64, disk_id: DiskId) -> Result<QueryCapacityStatsResponse>;
    pub async fn query_zone(&self, dg_id: u64, disk_id: DiskId, zone_index: u32) -> Result<QueryCapacityStatsResponse>;
    pub async fn get_disk_group_info(&self, dg_id: u64) -> Result<GetDiskGroupInfoResponse>;
    pub async fn get_disk_info(&self, dg_id: u64, disk_id: DiskId) -> Result<GetDiskInfoResponse>;
    pub async fn recalc_disk_usage(&self, req: RecalcDiskUsageRequest) -> Result<RecalcDiskUsageResponse>;
}
```

- **Endpoint discovery + cache**: `refresh_endpoints` calls
  `svc.read_all_diskdb_instances()`, reads each
  `InstanceValue.rpc_endpoint` + `DiskdbExtra.owned_dg_ids`, populates
  `cache: dg_id -> endpoint`. Called on startup (eager), on cache miss
  (lazy `refresh_for`), and on `Unavailable`/`ResourceExhausted`
  (refresh + retry). `DashMap` for concurrent reads.
- **Channel pool**: a `DashMap<String, crowdb_rpc::Channel>` per
  endpoint; lazily created on first use. Channels are reused across
  calls.
- **`allocate_blocks`**: look up endpoint for `req.disk_group_id`
  (refresh on miss), open channel, call `AllocateBlocks`, retry on
  transient errors (`Unavailable`, deadline-exceeded).
  `ResourceExhausted` (no space) → return to caller (not retryable).
- **`free_blocks`**: the request carries `Segment`s (each with
  `disk_id`); route by looking up which diskdb instance owns the
  `disk_id`'s disk-group. If segments span multiple disk-groups, split
  the request per-group and issue one `FreeBlocks` per group (v1: no
  cross-group batch). Retries on transient errors. To map
  `disk_id -> dg_id`, the client needs the disk→dg mapping from group
  0 (`HardwareClient::list_disks_in_group` per dg, or a cached reverse
  map built during `refresh_endpoints` from the hardware hierarchy). v1
  builds a `disk_id -> dg_id` reverse map during `refresh_endpoints` by
  reading the hardware hierarchy once; refreshes on cache miss.
- **`query_*`**: convenience helpers building the right
  `QueryCapacityStatsRequest` shape (§4).
  `query_disk_group(dg_id)` → `disk_id=None, zone_index=None`;
  `query_disk(dg_id, disk_id)` → `disk_id=Some, zone_index=None`;
  `query_zone(dg_id, disk_id, zi)` → all set.
- **`recalc_disk_usage`**: wraps the admin RPC.
- **Error model**: `DiskdbClientError` with `Rpc(crowdb_rpc::Status)` /
  `NoSpace` / `NotFound` / `InvalidArgument` mappings. `RetryConfig`
  mirrors `crowdb-kv-client` (max retries, backoff).

Edge cases:
- Cache miss (unknown `dg_id`) → lazy refresh + retry; if still not
  found → `Unreachable`.
- Endpoint moved (instance restarted on new port) → cached endpoint
  stale → next call `Unavailable` → refresh → retry → succeeds. v1
  reactive (proactive refresh needs group-0 notify/watch, tracked as a
  use case).
- `free_blocks` spans multiple disk-groups → split per-group; partial
  failure returns per-group results (v1: returns the first error; a
  follow-up can aggregate). Documented.
- v1 no proactive refresh — no background refresh task.

## 12. E2E test migration

New rpc-level e2e tests in `lib/crowdb-diskdb-client/tests/` using
`DiskdbClient` against an in-process diskdb rpc server + `KvCluster`
(mirroring `crowdb-kv-client/tests/` pattern):
- `allocate_free_e2e.rs` — allocate → verify busy record → free →
  verify free.
- `query_e2e.rs` — allocate →
  `query_disk_group`/`query_disk`/`query_zone`.
- `endpoint_cache_e2e.rs` — endpoint discovery, cache miss → refresh →
  retry.
- `recalc_e2e.rs` — allocate → recalc → matches; corrupt live bitmap →
  drift.

The existing in-process `app/crowdb-diskdb/tests/diskdb_e2e_test.rs`
(calls `alloc::allocate_block` directly) stays as a server-internal
integration test.

## Invariants

- **I1 — Free = capacity − busy**: CROWDB reuses freed space immediately
  via bitmap scan (no append-only `allocate_pos`, §3.4/§8); there is no
  separate free cursor. `free_blocks = unit_capacity - used_count`.
- **I2 — Gauges are derived snapshots**: capacity/busy/free gauges are
  recomputed from the bitmap on the reporting tick (single source of
  truth), not maintained as parallel hot-path atomics that could drift.
- **I3 — Recalc uses an independent popcount**: the recalc verifier
  reads `usage_bits.count_set()` on the replayed zone (not its
  `used_count`) and compares against the live `used_count`, so drift in
  either the bitmap or the cached counter is detected.
- **I4 — Group 0 carries disk-group-level usage only**: per-disk and
  per-zone live usage are never written to group 0 (a disk can have
  thousands of zones; group 0 is not a per-zone registry). Per-disk/
  per-zone drill-down is served by diskdb directly.
- **I5 — Counters increment once per durable-bound op**:
  `record_allocate`/`record_free` fire after the Phase 1 bitmap CAS
  succeeds, so a CAS-failed retry does not bump the counter. A Phase 2
  rollback over-counts by one — documented, acceptable for a
  best-effort event counter.

## Module Structure

```
app/crowdb-diskdb/src/
  model/
    zone.rs            # + accessors, ZoneUsage
    disk.rs            # + DiskUsage, DdbDisk::usage, metrics field
    disk_group.rs      # + DiskGroupUsage, aggregate_usage, zone_usage
    usage.rs           # (optional) ZoneUsage/DiskUsage/DiskGroupUsage structs
    alloc.rs           # + DiskMetrics record calls + latency observation
  metrics/
    mod.rs             # DiskdbMetrics (extended §11 set)
    disk.rs            # DiskMetrics (period/total counters)
    recalc.rs          # RecalcEngine + RecalcResult
    reporting.rs       # ReportingTask (BackgroundTask)
  service/diskdb_service.rs  # QueryCapacityStats + RecalcDiskUsage handlers
  liveness/keepalive.rs      # usage piggyback + endpoint + sync metrics
  ddb_config.rs        # + ReportingConfig
  main.rs              # wire ReportingTask + RecalcEngine + DiskdbMetrics
lib/crowdb-protocol/src/fbs/
  diskdb_type.fbs    # ZoneUsage, extend DiskInfo/DiskGroupInfo
  diskdb_op.fbs      # extend QueryCapacityStatsRequest, RecalcDiskUsage*
  diskdb_service.fbs # RecalcDiskUsage RPC
lib/crowdb-kv-client/src/
  space_usage.rs       # SpaceUsageClient + ClusterUsage/RackUsage/NodeUsage
  lib.rs               # re-export
lib/crowdb-diskdb-client/src/
  lib.rs               # DiskdbClientError (extended)
  client.rs            # DiskdbClient (allocate/free/query/recalc + cache)
app/crowdb-diskdb/tests/
  space_flow_test.rs   # whole-flow single + multi-thread
lib/crowdb-diskdb-client/tests/
  allocate_free_e2e.rs
  query_e2e.rs
  endpoint_cache_e2e.rs
  recalc_e2e.rs
```

## Config Extensions

- `ddb_config.rs`: `ReportingConfig { interval_secs: u32 }` (default
  10), added to `DdbConfig` as `pub reporting: ReportingConfig`.
  `validate()`: `interval_secs > 0`. Live-reloadable (dynamic field),
  read by `ReportingTask::trigger` via the shared config handle.

## Server Wiring

1. `main.rs`: build `DiskdbMetrics::register(&mut metrics_registry)`
   (extended set), already constructed; the §11 handles are now
   populated.
2. Construct `RecalcEngine::new(Arc::clone(&dg_kv), Arc::clone(&container))`;
   pass `Arc<RecalcEngine>` + `Arc<DiskdbMetrics>` into
   `DiskdbService::new` (signature gains two fields).
3. Construct `ReportingTask::new(container, metrics, config_handle)`;
   register in `BgRunner` alongside keepalive + compaction.
4. `keepalive` gains the real `server.listen_addr` endpoint for the
   piggyback (passed via `with_endpoint` or read from config in `tick`).
5. Recovery (`run_recovery`) attaches `DiskMetrics` to each recovered
   disk (matching `disk_add_init`).
