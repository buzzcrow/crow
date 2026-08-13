<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb — Space Metrics + Query API (R74)

This draft expands the R74 solution into implementation detail: per-zone/
per-disk/per-disk-group usage accessors, the `QueryCapacityStats` handler,
per-disk hot-path counters, the keepalive usage piggyback, a journal-replay
recalc verifier, the §11 `crow-common` metric set + reporting loop, the
proto extensions, the kv-client space-usage aggregation class, and the full
`crow-diskdb-client` library.

- Backlog doc: [`doc/backlog/R74-diskdb-space-metrics-query.md`](../backlog/R74-diskdb-space-metrics-query.md)
  — problem, dependencies, acceptance criteria, edge cases, flow diagrams.
- Root design: [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md) §11 (Space Metrics),
  §3.4 (records are source of truth), §7 (record model + recovery strategies),
  §3.7 (reuse `crow-common` metrics). Architecture decisions and rationale are
  in the root design; this doc does not repeat them.

Already landed (reused, not re-built): R70 proto types (`DiskInfo`,
`DiskGroupInfo`, `DiskGroupUsageSummary`, `DiskGroupUsageKey` + `prefix_all`,
`UsageBitmap`, `ZoneValueExt`); R71 `DdbDiskGroupContainer`/`DdbDiskGroup`/
`DdbDisk`; R72 `DdbKvClient`, `DiskdbMetrics` (two counters), `model/alloc.rs`
two-phase allocate/free; R73 `RecoveryEngine`, `recover_zone_inner`,
`rebuild_zone_bitmap_full_scan`, `CompactionEngine`. `crow-kv-client`'s
`HardwareClient` (hierarchy scans) + `ServiceRegistryClient`
(`read_all_diskdb_instances`, `heartbeat_diskdb` accepting
`DiskGroupUsageSummary[]`) already exist. `crow-diskdb-client`'s `Cargo.toml`
already has the right deps; R74 fills in the implementation.

## 1. Per-zone usage accessors

### 1.1 Why

`DdbZone` already tracks `used_count: AtomicU32`, `unit_capacity: u32`, and
`UsageBitmap::count_set()`, but exposes no busy/free/bytes/ratio accessors.
Every higher-level aggregation (disk, disk-group, query handler, keepalive
piggyback, reporting-loop gauges) bottoms out at the zone, so the accessors
must live here. CROW reuses freed space immediately via bitmap scan (no
append-only `allocate_pos`, §3.4/§8), so `free = capacity - busy` — there is
no separate free cursor to read.

### 1.2 How

Add to `app/crow-diskdb/src/model/zone.rs` (`impl DdbZone`):

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

a. `busy_blocks` reads the live atomic; `free_blocks` derives from capacity.
   `UsageBitmap::count_set()` is **not** used on the read path — it is reserved
   for the recalc verifier (§5) which needs an independent popcount of the
   bitmap bytes, not the cached `used_count`.
b. The `unit_size_bytes` argument is per-disk (from `DiskValue.unit_size_bytes`),
   not a global shift — disks in one disk-group may have different unit sizes
   in principle, so the caller passes the owning disk's value.

Edge cases:
- `unit_capacity == 0` (degenerate) → `usage_ratio` returns 0.0 (guarded).
- Saturating sub on `free_blocks` guards against a transient
  `used_count > unit_capacity` (should never happen; a CAS bug would set it).
- These are read-only and lock-free; safe to call concurrently with allocate/free.

## 2. Per-disk + per-disk-group aggregation

### 2.1 Why

`DdbDisk` and `DdbDiskGroup` own the zone/disk collections but expose no
aggregated usage. The query handler, keepalive piggyback, and reporting loop
all need a single struct built from in-memory state (no KV reads). The
aggregation is a straightforward sum, but the struct shape is reused in three
places, so it must be defined once.

### 2.2 How

New structs in `app/crow-diskdb/src/model/zone.rs` (or a new
`model/usage.rs` re-exported from `model.rs`):

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

`DdbDisk::usage(&self, unit_size_bytes: u32) -> DiskUsage` in `disk.rs`:
a. Read `disk_value` + `zones` under their read locks.
b. Sum `capacity_bytes`/`busy_bytes`/`free_bytes` via each zone's accessors
   (§1.2) using the disk's `unit_size_bytes` (passed in, or read from
   `disk_value.unit_size_bytes`).
c. `zone_count` = `zones.len()`; `active_zone_count` =
   `active_zone_context.read().len()` (RCU snapshot size).
d. `busy_zone_count` = zones with `busy_blocks() == unit_capacity`;
   `free_zone_count` = the rest.

`DdbDiskGroup::aggregate_usage(&self) -> DiskGroupUsage` in `disk_group.rs`:
a. Read `disks` under the read lock; read each disk's `disk_value` for its
   `unit_size_bytes`.
b. Sum across disks; `disk_count` = `disks.len()`; `allocatable_disk_count` =
   `allocating_disks.read().len()` (RCU context size — matches the
   `allocatable_disk_count` semantics: disks currently `Up` and allocatable).
c. Carry the per-disk `DiskUsage` breakdown in `disks`.

`DdbDiskGroup::zone_usage(&self, disk_id: DiskId, zone_index: u32) -> Option<ZoneUsage>`:
a. Locate the disk via `disk_index` (read lock); `None` if unknown disk.
b. Locate the zone by `zone_index` (read lock on `disk.zones`); `None` if
   out of range.
c. Build `ZoneUsage` from the zone's accessors + `derived_alloc_state()` +
   `*zone.zone_state.read()`. **Brief counts only** — no bitmap bytes.

Edge cases:
- Disk with zero zones → `DiskUsage` zeroes, `zone_count == 0`.
- A `Bad` disk is still summed (its zones carry their last-known `used_count`);
   it is excluded from `allocatable_disk_count` but its capacity still counts
   in the total. This matches "capacity is total; allocatable is the live set."
- `zone_usage` for an out-of-range index → `None` (caller maps to `NotFound`).
- Read locks make the snapshot best-effort-consistent across zones/disks; a
   concurrent `disk_add_init`/`remove_disk` may miss a transient disk this
   tick. Acceptable for metrics (§11: exact consistency not required; recalc
   gives exact verification).

## 3. Per-disk hot-path counters

### 3.1 Why

§11 calls for per-disk event counters (allocate/free count + bytes) on the hot
path. The capacity/busy/free **gauges** are derived from the bitmap on the
reporting tick (single source of truth) — maintaining parallel capacity
atomics would drift from the bitmap. So the per-disk hot-path struct holds
**only** period/total event counters; gauges are computed in the reporting
loop (§7).

### 3.2 How

Convert `app/crow-diskdb/src/metrics.rs` into a module dir:
`metrics/mod.rs` (existing `DiskdbMetrics` moves here) + `metrics/disk.rs`.

`metrics/disk.rs` — `DiskMetrics` (lock-free atomic counters per `DdbDisk`):

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

a. All atomics use `Ordering::Relaxed` (counters, no cross-variable ordering).
b. `record_allocate`/`record_free` do `fetch_add(unit_count)` on count and
   `fetch_add(u64::from(unit_count) * u64::from(unit_size_bytes))` on bytes,
   on both period and total.
c. `swap_periods` does `swap(0, Relaxed)` on each period counter; totals are
   kept inline (also incremented in `record_*`) for crash-safe monotonicity —
   the reporting loop does **not** add period deltas to totals (they're already
   there); it only flushes period deltas into the crow-common
   `allocate_total`/`free_total` counters.

Wiring: add a `metrics: Option<Arc<DiskMetrics>>` field to `DdbDisk` (in
`disk.rs`), attached during `disk_add_init` (keepalive) and
`recover_disk_group` (recovery). `None` in tests that don't care. The
allocate/free paths in `model/alloc.rs` call
`disk.metrics.as_ref().map(|m| m.record_allocate(range.unit_count, unit_size))`
after the Phase 1 CAS succeeds (before Phase 2 persist — the counter reflects
a durable-bound allocation; on Phase 2 failure the bitmap rolls back but the
counter over-counts by one, which is acceptable for a best-effort event
counter and avoids a second hot-path branch on the persist result).

Edge cases:
- `AtomicU64` overflow at 1M-unit blocks → extremely unlikely;
  `swap_periods` saturating semantics avoid loss; no special handling.
- `DiskMetrics == None` (test disk) → record calls are no-ops; aggregation
  unaffected.
- Counter over-count on Phase 2 rollback (above) — documented, acceptable for
  an event counter; the `allocate_errors_total` counter (§6) separately tracks
  persist failures.

## 4. `QueryCapacityStats` handler

### 4.1 Why

`QueryCapacityStats` is stubbed (`Status::unimplemented`). It is the
operator/console drill-down surface for live per-disk/per-zone usage — the
authoritative counterpart to the kv-client's stale cluster-wide view. The
request needs two drill-down fields (`disk_id`, `zone_index`) to select three
query shapes without three separate RPCs.

### 4.2 How

Proto (§9): extend `QueryCapacityStatsRequest` with `uint64 disk_id = 2` and
`uint32 zone_index = 3` (0 = not set). Extend `DiskInfo`/`DiskGroupInfo` with
usage fields + `repeated ZoneUsage zone_usages`. Add `ZoneUsage` message.

Handler in `service/diskdb_service.rs` (replace the `unimplemented` stub).
Read-only — allowed in any lifecycle phase (like `GetDiskGroupInfo`); does
**not** check `allows_mutating_rpcs` or `is_degraded`.

a. **Cluster/disk-group level** (`disk_group_id` only, `disk_id == 0`):
   if `disk_group_id == 0`, iterate `container.disk_group_ids()` and build one
   `DiskGroupInfo` per owned group; if set, look up that one group
   (`NotFound` if not owned). Each `DiskGroupInfo` carries the aggregated
   capacity/busy/free + member `DiskInfo`s with per-disk usage + zone counts.
   **No `zone_usages` entries** at this level.
b. **Disk level** (`disk_group_id` + `disk_id` set, `zone_index == 0`):
   return the one `DiskInfo` with a `zone_usages` list of **brief** per-zone
   `ZoneUsage` entries (counts only — no `usage_bitmap`). Built via
   `DdbDisk::usage` + a loop over zones calling `DdbZone` accessors +
   `derived_alloc_state`. `NotFound` if the disk is not in the group.
c. **Zone level** (`disk_group_id` + `disk_id` + `zone_index` all set):
   return the one `ZoneUsage` with the **full `usage_bitmap` bytes** via
   `DdbZone::usage_bits.snapshot()`. Out-of-range `zone_index` → `NotFound`.

`disk_id == 0` sentinel: `DiskId` is a message (`{high, low}`); 0 means
`Some(DiskId{high:0,low:0})` is treated as "not set" only when the field is
`None`. The proto field is `optional crow.common.DiskId disk_id` so "not set"
= `None`; a real disk with all-zero id is pathological and rejected at disk-add
time. `zone_index == 0` is ambiguous (zone 0 is valid) — so the disk-level
shape is selected by `disk_id` being set **and** `zone_index` being absent.
Use proto3 `optional` on `zone_index` (field presence) so "not set" is
distinguishable from 0. (proto3 scalar presence requires `optional` keyword;
prost generates `Option<u32>`.)

Edge cases:
- `disk_group_id == 0` + zero owned groups → empty `disk_groups`, `Ok`.
- Unowned `disk_group_id` → `NotFound` (matches `GetDiskGroupInfo`).
- Disk with thousands of zones → disk-level returns brief entries (no bitmap
  bytes); only zone-level serializes one bitmap.
- Out-of-range `zone_index` → `NotFound` (not silent empty bitmap).
- Zone with `used_count == unit_capacity` → `Full` alloc_state, `free == 0`;
  reported as-is, not an error.
- Last zone smaller (word-aligned) → `unit_capacity` is the rounded value;
  `capacity_bytes` reflects the real smaller capacity.

## 5. Recalculation path

### 5.1 Why

§11 requires "accurate statistics with a recalculation path" — replay the
journal into a **separate** bitmap and compare against the live `DdbZone` to
detect drift. This is the exact-verification counterpart to the
best-effort-consistent gauges. R75's scanner reuses `recalc_all`. v1 reports
drift only (no auto-correct; operator runs `RebuildZoneBitmap`).

### 5.2 How

New `app/crow-diskdb/src/metrics/recalc.rs`:

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
a. Call `recover_zone_inner(&self.kv, bind, disk_id, zone_idx, unit_capacity)`
   (strategy 2) into a throwaway `DdbZone`. On `JournalScanGcGap` /
   `SnapshotCrcFail`, fall back to `rebuild_zone_bitmap_full_scan`
   (strategy 1); record `fallback_used`.
b. `replayed_busy_blocks` = replayed zone's `usage_bits.count_set()` (independent
   popcount, not its `used_count`).
c. `live_busy_blocks` = live zone's `busy_blocks()` (its `used_count`).
d. `matches = (replayed_busy_blocks == live_busy_blocks)`;
   `drift_detected = !matches`.
e. v1 does **not** mutate the live zone — drift is reported only.

`recalc_disk_group(dg_id) -> Result<DiskGroupRecalcResult>`: iterate all disks
+ zones in the disk-group, call `recalc_zone` per zone (bounded concurrency
via a semaphore, matching `recover_disk_group`), aggregate.

`recalc_all() -> Result<Vec<DiskGroupRecalcResult>>`: iterate
`container.disk_group_ids()`, call `recalc_disk_group` per owned group.

The `RecalcDiskUsage` admin RPC (§9) delegates to `RecalcEngine`; the handler
lives in `service/diskdb_service.rs` (mutating RPC — requires
`allows_mutating_rpcs`, since recalc does KV reads/journal scans and is an
admin operation, not a read-only query).

Edge cases:
- `JournalScanGcGap` → strategy 1 fallback; `fallback_used = Some(JournalScanGcGap)`.
- `SnapshotCrcFail` → strategy 1 fallback; the live bitmap is suspect;
  `drift_detected = true` with `FallbackReason::SnapshotCrcFail` even if
  counts happen to match (the snapshot was corrupt).
- Strategy 1 also fails → return a `RecalcResult` with `matches = false`,
  `drift_detected = true`, and a KV error noted (do not panic; the operator
  sees the failure in the response).
- Live `used_count` unchanged after recalc (v1 no auto-fix) — unit test asserts.

## 6. crow-common metrics integration

### 6.1 Why

§11 specifies three metric categories (counters, gauges, latency hierarchy)
reusing `crow-common`'s `MetricsRegistry`. The existing `DiskdbMetrics` holds
only the two R72 counters; R74 extends it with the full §11 set. Gauges are
derived snapshots (bitmap-derived on the reporting tick), not hot-path writes.

### 6.2 How

Extend `DiskdbMetrics` in `metrics/mod.rs` (the converted module). New fields
(handles from `MetricsRegistry`):

- **Gauges** (`Arc<Gauge>`): `disk_capacity_bytes`, `disk_busy_bytes`,
  `disk_free_bytes`, `disk_active_zone_count`, `disk_total_zone_count`,
  `dg_capacity_bytes`, `dg_busy_bytes`, `dg_free_bytes`,
  `owned_disk_group_count`, `degraded`, `last_sync_age_secs`.
  (Per-disk/per-disk-group gauges are single instances updated each tick with
  the summed value — v1 does not label per-disk-id in the registry; the
  reporting loop sums across disks into the single gauge. R77's console reads
  the live per-disk view via `QueryCapacityStats`, not the metrics log.)
- **Counters** (`Arc<Counter>`): `allocate_total`, `free_total` (flushed from
  per-disk `DiskMetrics` totals by the reporting loop), `allocate_errors_total`,
  `sync_success_total`, `sync_failure_total`,
  `compaction_records_deleted_total`.
- **Latency histograms** (`Arc<LatencyHistogram>`, hot paths):
  `allocate.rpc.latency_us`, `allocate.bitmap_scan.latency_us`,
  `allocate.kv_persist.latency_us`, `free.rpc.latency_us`,
  `free.bitmap_clear.latency_us`, `free.kv_persist.latency_us`.
- **Latency summaries** (`Arc<LatencySummary>`, cold paths):
  `allocate.zone_rotate.latency_us`, `sync.latency_us`,
  `sync.read_group0.latency_us`, `sync.apply_changes.latency_us`,
  `compaction.latency_us`, `compaction.scan_free.latency_us`,
  `compaction.merge_bitmap.latency_us`, `compaction.kv_persist.latency_us`,
  `sync_duration_ms`, `recovery_duration_ms`.

`register(registry: &mut MetricsRegistry) -> Self` registers all of the above
(plus the existing two R72 counters). `disabled()` constructs a throwaway
registry for tests.

Instrumentation points (start-instant → observe-on-completion, nanoseconds):
- `model/alloc.rs` `allocate_block`/`allocate_blocks`: observe
  `allocate.rpc.latency_us` (total), `allocate.bitmap_scan.latency_us`
  (around `dg.allocate_block(s)`), `allocate.kv_persist.latency_us` (around
  `kv.persist_busy_batch`). On persist failure, `allocate_errors_total.inc()`.
- `model/alloc.rs` `free_block`/`free_blocks`: observe `free.rpc.latency_us`,
  `free.bitmap_clear.latency_us` (around `dg.free_block`), `free.kv_persist.latency_us`.
- `liveness/keepalive.rs` `tick`: observe `sync.latency_us`,
  `sync.read_group0.latency_us` (around `observe_ownership` + `observe_disks`),
  `sync.apply_changes.latency_us` (around reconcile). `sync_success_total`/
  `sync_failure_total` per tick outcome.
- `recovery/compaction.rs`: observe `compaction.*` summaries;
  `compaction_records_deleted_total` per compaction.

The latency handles are passed into the allocate/free paths via the
`DiskdbMetrics` (already threaded through `BgCtx`; for the service handler,
`DiskdbService` gains an `Arc<DiskdbMetrics>` field). To avoid plumbing
metrics through every `alloc::allocate_block` signature, wrap the latency
observation in the service handler around the `alloc::` call (rpc + kv_persist
measured together at the handler boundary) and add a finer-grained
`bitmap_scan` observation inside `alloc.rs` guarded by an optional
`&DiskdbMetrics` parameter. v1 measures rpc + bitmap_scan + kv_persist at the
handler/alloc boundary; full per-phase splitting is best-effort.

Edge cases:
- Histogram/summary `observe(0)` is valid (sub-microsecond ops).
- Metrics handle `None` in test paths → `disabled()` registry is used.

## 7. Reporting loop

### 7.1 Why

The hot-path atomic counters (`DiskMetrics`) + bitmap state must be bridged to
the crow-common `Gauge`/`Counter` reporting layer on a cadence. §11: gauges
are derived snapshots updated on the reporting interval, not hot-path writes.

### 7.2 How

New `app/crow-diskdb/src/metrics/reporting.rs` — `ReportingTask` implementing
`BackgroundTask`. Registered in `BgRunner` alongside keepalive + compaction in
`main.rs`. Every 10 s (configurable via a new `reporting.interval_secs`,
live-reload via the shared config handle):

a. For each owned disk-group → each disk: `DiskMetrics::swap_periods()` →
   flush period deltas into `allocate_total`/`free_total` counters
   (`inc_by(period.allocate_count)` etc.).
b. Recompute gauges from the bitmap: `DdbDiskGroup::aggregate_usage()` →
   set `dg_capacity_bytes`/`dg_busy_bytes`/`dg_free_bytes` (summed across
   groups); per-disk sums → `disk_*` gauges; `disk_active_zone_count`/
   `disk_total_zone_count` summed.
c. `owned_disk_group_count.set(container.disk_group_ids().len())`;
   `degraded.set(if container.is_degraded() {1} else {0})`;
   `last_sync_age_secs.set(...)` (tracked from the last successful keepalive
   tick — keepalive records a `last_sync_at: Mutex<Option<Instant>>` on the
   container, or the reporting task reads a shared `AtomicU64` epoch).

`ReportingTask` owns an `Arc<DdbDiskGroupContainer>` + `Arc<DiskdbMetrics>` +
the shared config handle (for the interval). `run_cycle` does the flush;
`trigger` returns `Trigger::TimerFn` reading `reporting.interval_secs`.

Edge cases:
- Tick during concurrent `disk_add_init`/`remove_disk` → read locks make the
  gauge snapshot best-effort; a transient disk may be missed this tick.
  Acceptable (§11).
- Zero owned groups → gauges set to 0, counters unchanged.

## 8. Keepalive usage piggyback

### 8.1 Why

`heartbeat_diskdb` already accepts `DiskGroupUsageSummary[]` but keepalive
passes `&[]`. Group 0 stores disk-group-level usage at `/hw/dg_usage/<dg_id>`;
the console (R77) reads it for the cluster-wide view. The summary is derived
(recomputed each tick from the bitmap), not a source of truth.

### 8.2 How

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
endpoint, &owned_dg_ids, &group_usages)` instead of `&[]`. The endpoint string
is the diskdb gRPC listen address (from config; currently passed as `""` — R74
passes the real `server.listen_addr` so group 0 records a reachable endpoint
for the diskdb-client cache). The summary is recomputed each tick (not cached).

Edge cases:
- Zero owned groups → empty `group_usages` (not an error).
- `heartbeat_diskdb` fails (group 0 unreachable) → keepalive already handles
  this (missed count → degraded); the summary is just not delivered this tick.
- Group 0 stores **only disk-group-level** usage; per-disk/per-zone live usage
  is never written to group 0.

## 9. Proto extension

### 9.1 How

Update `lib/crow-protocol/src/proto/diskdb_type.proto`:

```proto
message ZoneUsage {
  uint32 zone_index        = 1;
  uint64 capacity_bytes    = 2;
  uint64 busy_bytes        = 3;
  uint64 free_bytes        = 4;
  uint32 busy_block_count  = 5;
  uint32 free_block_count  = 6;
  ZoneAllocationState alloc_state = 7;
  // Populated only for a specific-zone query; omitted at disk level.
  optional bytes usage_bitmap = 8;
}
```

Extend `DiskInfo` with `busy_units`, `free_units`, `capacity_bytes`,
`busy_bytes`, `free_bytes`, `active_zone_count`, and
`repeated ZoneUsage zone_usages`. Extend `DiskGroupInfo` with
`capacity_bytes`, `busy_bytes`, `free_bytes`, `allocatable_disk_count`.

Update `diskdb_op.proto`:
- `QueryCapacityStatsRequest`: add `optional uint64 disk_id = 2` (DiskId is a
  message — use `optional crow.common.DiskId disk_id = 2`) and
  `optional uint32 zone_index = 3`.
- Add `RecalcDiskUsageRequest { optional uint64 disk_group_id = 1; }` /
  `RecalcDiskUsageResponse { repeated DiskGroupRecalcResult results = 1; }`
  + `DiskGroupRecalcResult { uint64 disk_group_id = 1; bool drift_detected = 2;
  repeated ZoneRecalcResult zones = 3; }` + `ZoneRecalcResult { ... }` (mirror
  `RecalcResult`).

Update `diskdb_service.proto`: add
`rpc RecalcDiskUsage(RecalcDiskUsageRequest) returns (RecalcDiskUsageResponse);`

Build clean: `pixi run cargo build -p crow-protocol`.

Edge cases:
- proto3 `optional` on scalars → prost generates `Option<u32>` so `zone_index`
  absence is distinguishable from 0.
- `usage_bitmap` is `optional bytes` → `Option<Vec<u8>>`; omitted at disk level.

## 10. kv-client space-usage aggregation

### 10.1 Why

No client reads `/hw/dg_usage/*` back. The console (R77) needs a
cluster/rack/node/disk-group capacity view built from the piggyback summaries
joined with the hardware hierarchy. This is the stale (≤1 sync interval)
cluster-wide view; live per-disk/per-zone drill-down is via the diskdb-client
(§11).

### 10.2 How

New module in `lib/crow-kv-client`: `src/space_usage.rs` — `SpaceUsageClient`
wrapping `HardwareClient` (which already has the hierarchy scans +
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

a. `list_disk_group_usages`: `scan_prefix::<DiskGroupUsageSummary>` over
   `DiskGroupUsageKey::prefix_all()` text path (`/hw/dg_usage/`). The values
   are JSON-encoded (group 0 stores text-path keys with JSON values, matching
   `HardwareClient`'s convention).
b. `cluster_usage`: read all summaries + `list_racks`/`list_nodes`/
   `list_disk_groups`/`list_disks_in_group`; sum capacity/used/free +
   disk-group count. Per-disk live busy/free is **not** available here (group 0
   has only disk-group-level usage + per-disk static `DiskValue.capacity_units`);
   the disk-level view carries capacity only.
c. `rack_usage`/`node_usage`/`disk_group_usage`: scope the join to the
   hierarchy subtree.

`ClusterUsage`/`RackUsage`/`NodeUsage` structs hold capacity/used/free bytes +
disk-group/disk counts. Mirror `crow-kv-client`'s retry pattern (the underlying
`HardwareClient` already retries).

Edge cases:
- No summaries written yet (fresh cluster) → empty vec / zero totals.
- Stale summary (≤1 sync interval behind) → documented; not an error.
- Per-disk busy/free absent → `NodeUsage.disk_capacity_bytes` only, no busy/free.

## 11. diskdb-client — full client library

### 11.1 Why

`crow-diskdb-client` is a skeleton (only an error type). It is the primary
client surface for all diskdb gRPC operations — allocate/free/query with retry
+ endpoint caching, mirroring `crow-kv-client`'s pattern. Without it, callers
must use raw gRPC stubs and do endpoint discovery manually.

### 11.2 How

Fill in `lib/crow-diskdb-client/src/lib.rs` (add `client.rs` module):

```rust
pub struct DiskdbClient {
    svc: ServiceRegistryClient,        // endpoint discovery from group 0
    cache: DashMap<DiskGroupId, String>, // dg_id -> grpc_endpoint
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

a. **Endpoint discovery + cache**: `refresh_endpoints` calls
   `svc.read_all_diskdb_instances()`, reads each `InstanceValue.grpc_endpoint`
   + `DiskdbExtra.owned_dg_ids`, populates `cache: dg_id -> endpoint`. Called
   on startup (eager), on cache miss (lazy `refresh_for`), and on
   `Unavailable`/`ResourceExhausted` (refresh + retry). `DashMap` for
   concurrent reads.
b. **Channel pool**: a `DashMap<String, tonic::transport::Channel>` per
   endpoint; lazily created on first use. Channels are reused across calls.
c. **`allocate_blocks`**: look up endpoint for `req.disk_group_id` (refresh on
   miss), open channel, call `AllocateBlocks`, retry on transient errors
   (`Unavailable`, deadline-exceeded). `ResourceExhausted` (no space) → return
   to caller (not retryable).
d. **`free_blocks`**: the request carries `Segment`s (each with `disk_id`);
   route by looking up which diskdb instance owns the `disk_id`'s disk-group.
   If segments span multiple disk-groups, split the request per-group and issue
   one `FreeBlocks` per group (v1: no cross-group batch). Retries on transient
   errors. To map `disk_id -> dg_id`, the client needs the disk→dg mapping
   from group 0 (`HardwareClient::list_disks_in_group` per dg, or a cached
   reverse map built during `refresh_endpoints` from the hardware hierarchy).
   v1 builds a `disk_id -> dg_id` reverse map during `refresh_endpoints` by
   reading the hardware hierarchy once; refreshes on cache miss.
e. **`query_*`**: convenience helpers building the right `QueryCapacityStatsRequest`
   shape (§4). `query_disk_group(dg_id)` → `disk_id=None, zone_index=None`;
   `query_disk(dg_id, disk_id)` → `disk_id=Some, zone_index=None`;
   `query_zone(dg_id, disk_id, zi)` → all set.
f. **`recalc_disk_usage`**: wraps the admin RPC.
g. **Error model**: extend `DiskdbClientError` with `Rpc(tonic::Status)` /
   `NoSpace` / `NotFound` / `InvalidArgument` mappings. `RetryConfig` mirrors
   `crow-kv-client` (max retries, backoff).

Edge cases:
- Cache miss (unknown `dg_id`) → lazy refresh + retry; if still not found →
  `Unreachable`.
- Endpoint moved (instance restarted on new port) → cached endpoint stale →
  next call `Unavailable` → refresh → retry → succeeds. v1 reactive (R78
  proactive).
- `free_blocks` spans multiple disk-groups → split per-group; partial failure
  returns per-group results (v1: returns the first error; a follow-up can
  aggregate). Documented.
- v1 no proactive refresh — no background refresh task (unit test asserts).

## 12. E2E test migration

New gRPC-level e2e tests in `lib/crow-diskdb-client/tests/` using `DiskdbClient`
against an in-process diskdb gRPC server + `KvCluster` (mirroring
`crow-kv-client/tests/` pattern):
- `allocate_free_e2e.rs` — allocate → verify busy record → free → verify free.
- `query_e2e.rs` — allocate → `query_disk_group`/`query_disk`/`query_zone`.
- `endpoint_cache_e2e.rs` — endpoint discovery, cache miss → refresh → retry.
- `recalc_e2e.rs` — allocate → recalc → matches; corrupt live bitmap → drift.

The existing in-process `app/crow-diskdb/tests/diskdb_e2e_test.rs` (calls
`alloc::allocate_block` directly) stays as a server-internal integration test.

## Scope

- `app/crow-diskdb/src/model/zone.rs` — add busy/free/bytes/ratio accessors +
  `ZoneUsage` struct.
- `app/crow-diskdb/src/model/disk.rs` — add `DiskUsage` + `DdbDisk::usage` +
  `metrics: Option<Arc<DiskMetrics>>` field.
- `app/crow-diskdb/src/model/disk_group.rs` — add `DiskGroupUsage` +
  `aggregate_usage` + `zone_usage`.
- `app/crow-diskdb/src/model.rs` — re-export `ZoneUsage`/`DiskUsage`/`DiskGroupUsage`.
- `app/crow-diskdb/src/metrics.rs` → `metrics/mod.rs` (move `DiskdbMetrics`,
  extend with §11 set) + `metrics/disk.rs` (`DiskMetrics`) +
  `metrics/recalc.rs` (`RecalcEngine`) + `metrics/reporting.rs` (`ReportingTask`).
- `app/crow-diskdb/src/model/alloc.rs` — instrument allocate/free with
  `DiskMetrics::record_*` + latency observations.
- `app/crow-diskdb/src/service/diskdb_service.rs` — implement
  `QueryCapacityStats` + `RecalcDiskUsage`; add `DiskdbMetrics`/`RecalcEngine`
  fields; extend `GetDiskGroupInfo`/`GetDiskInfo` with usage fields.
- `app/crow-diskdb/src/liveness/keepalive.rs` — usage piggyback + endpoint +
  sync latency/success counters.
- `app/crow-diskdb/src/main.rs` — wire `ReportingTask` into `BgRunner`; pass
  `DiskdbMetrics` + `RecalcEngine` into the service; reporting config.
- `app/crow-diskdb/src/ddb_config.rs` — add `ReportingConfig` (`interval_secs`).
- `app/crow-diskdb/src/lib.rs` — `pub mod metrics` (now a dir).
- `lib/crow-protocol/src/proto/diskdb_type.proto` — `ZoneUsage`, extend
  `DiskInfo`/`DiskGroupInfo`.
- `lib/crow-protocol/src/proto/diskdb_op.proto` — extend
  `QueryCapacityStatsRequest`; add `RecalcDiskUsage*` messages.
- `lib/crow-protocol/src/proto/diskdb_service.proto` — add `RecalcDiskUsage` RPC.
- `lib/crow-kv-client/src/space_usage.rs` — `SpaceUsageClient` + aggregation.
- `lib/crow-kv-client/src/lib.rs` — re-export `SpaceUsageClient`.
- `lib/crow-diskdb-client/src/lib.rs` + `src/client.rs` — full `DiskdbClient`.
- `app/crow-diskdb/tests/space_flow_test.rs` — whole-flow space verification
  (single + multi-thread).
- `lib/crow-diskdb-client/tests/{allocate_free_e2e,query_e2e,endpoint_cache_e2e,recalc_e2e}.rs`
  — gRPC-level e2e tests.
- Unit tests alongside each new module (zone accessors, disk/dg aggregation,
  `DiskMetrics`, `RecalcEngine`, `SpaceUsageClient`).

## Complexity

**High.** Twelve interlocking components across three crates
(`crow-diskdb`, `crow-kv-client`, `crow-diskdb-client`) + proto changes. The
genuinely hard parts: (1) the recalc verifier reusing `recover_zone_inner`
into a throwaway bitmap and correctly handling both fallback strategies +
the CRC-fail drift semantics; (2) the diskdb-client endpoint cache +
disk_id→dg_id reverse routing for `free_blocks` across multiple disk-groups;
(3) the reporting loop bridging per-disk atomic period counters + bitmap-derived
gauges without double-counting (totals kept inline, period deltas only flush
to the crow-common counters). Mostly reused from aioss/`crow-kv-client`:
the `DiskMetrics` atomic pattern, the `SpaceUsageClient` hierarchy-join (mirrors
`HardwareClient` scans), the `DiskdbClient` retry + cache (mirrors
`CrowkvClient`). Main challenge is plumbing `DiskdbMetrics`/`DiskMetrics`
handles through the allocate/free/keepalive/compaction paths without bloating
signatures (mitigated by handler-boundary latency observation + optional
metrics fields).

## Test Design

### Unit tests (UT)

- **Zone accessors** (`zone.rs` tests): new zone cap=128, allocate 5 →
  `busy_blocks==5`, `free_blocks==123`; free 2 → `busy==3`, `free==125`;
  `busy_bytes(n)==busy*n`, `capacity_bytes(n)==cap*n`; `usage_ratio` == 5/128
  then 3/128; `unit_capacity==0` → ratio 0.0.
- **Disk aggregation** (`disk.rs` tests): disk with 2 zones, allocate in zone 0
  only → disk busy == zone-0 busy, free == sum of both zones' free;
  `active_zone_count` matches `rebuild_active_zones(2)`; `busy_zone_count`/
  `free_zone_count` correct after filling one zone.
- **Disk-group aggregation** (`disk_group.rs` tests): 2 disks, one `Bad` →
  `allocatable_disk_count` excludes it; `aggregate_usage` sums capacity across
  both; `zone_usage(disk_id, idx)` returns `Some` for existing, `None` for
  unknown disk / out-of-range zone.
- **`DiskMetrics`** (`metrics/disk.rs` tests): record 3 allocates of 4 units
  each, `swap_periods` → period count=3, bytes=12, total count=3; record 1
  more, swap → period count=1, total count=4 (totals monotonic across swaps).
- **`RecalcEngine`** (`metrics/recalc.rs` tests): allocate 5, free 2, recalc →
  `matches==true`, replayed busy==3==live; manually flip a live bit without a
  record → `matches==false`, `drift_detected==true`; inject corrupt snapshot
  → fallback to strategy 1, `fallback_used==Some(SnapshotCrcFail)`;
  `recalc_all` covers every owned group; live `used_count` unchanged when
  drift detected (v1 no auto-fix).
- **`SpaceUsageClient`** (`crow-kv-client` tests): write a few summaries to
  group 0 → `list_disk_group_usages` returns all; `cluster_usage` with 2
  racks × 2 nodes × 1 dg each → rack + cluster totals sum correctly;
  disk-level view carries capacity only (no busy/free).
- **`DiskdbClient` v1 no proactive refresh** (unit test): assert no background
  refresh task runs (cache populated only on startup/miss/error).

### End-to-end tests (E2E)

- **Whole-flow space verification** (`app/crow-diskdb/tests/space_flow_test.rs`,
  in-process mock KV, <5 s): single-thread + multi-thread flows per the
  backlog acceptance — 3 disks × 4 zones × 128 units, fill 1536, verify
  rotation across all 4 zones, free all, reclaim, re-allocate; 8 concurrent
  tasks × 192 units, verify no double-allocation + CAS retry counter
  incremented; `zone_rotate_count=1` edge case (sequential rotation).
- **`allocate_free_e2e.rs`** (`crow-diskdb-client`): `KvCluster` + in-process
  diskdb gRPC + `DiskdbClient` → allocate 3 → 3 `Segment`s → free →
  `freed_count==3`.
- **`query_e2e.rs`**: allocate → `query_disk_group` asserts `busy_units`;
  `query_disk` asserts brief `ZoneUsage[]` no bitmap; `query_zone` asserts
  `usage_bitmap` present.
- **`endpoint_cache_e2e.rs`**: start diskdb, register in service registry,
  `DiskdbClient` seeded with group-0 leader → first `allocate_blocks` succeeds
  (cache populated on miss); restart diskdb on new port → next call refreshes
  + succeeds.
- **`recalc_e2e.rs`**: allocate → recalc → matches; corrupt live bitmap →
  recalc → drift detected.
- **`QueryCapacityStats` handler integration** (in `app/crow-diskdb/tests/`):
  2 owned disk-groups, allocate in one → disk-group-level response reflects
  allocations, other busy=0, no `ZoneUsage`; disk-level → brief entries, no
  bitmap; zone-level → full bitmap; out-of-range zone → `NotFound`; read-only
  in `Recovering` phase (no `allows_mutating_rpcs` required).
- **Keepalive piggyback integration**: owned disk-group with allocations →
  `heartbeat_diskdb` receives non-empty `group_usages` with `used_bytes`
  matching; zero owned → empty slice.

## Module Structure

```
app/crow-diskdb/src/
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
lib/crow-protocol/src/proto/
  diskdb_type.proto    # ZoneUsage, extend DiskInfo/DiskGroupInfo
  diskdb_op.proto      # extend QueryCapacityStatsRequest, RecalcDiskUsage*
  diskdb_service.proto # RecalcDiskUsage RPC
lib/crow-kv-client/src/
  space_usage.rs       # SpaceUsageClient + ClusterUsage/RackUsage/NodeUsage
  lib.rs               # re-export
lib/crow-diskdb-client/src/
  lib.rs               # DiskdbClientError (extended)
  client.rs            # DiskdbClient (allocate/free/query/recalc + cache)
app/crow-diskdb/tests/
  space_flow_test.rs   # whole-flow single + multi-thread
lib/crow-diskdb-client/tests/
  allocate_free_e2e.rs
  query_e2e.rs
  endpoint_cache_e2e.rs
  recalc_e2e.rs
```

## Config Extensions

- `ddb_config.rs`: new `ReportingConfig { interval_secs: u32 }` (default 10),
  added to `DdbConfig` as `pub reporting: ReportingConfig`. `validate()`:
  `interval_secs > 0`. Live-reloadable (dynamic field), read by
  `ReportingTask::trigger` via the shared config handle.

## Server Wiring

1. `main.rs`: build `DiskdbMetrics::register(&mut metrics_registry)` (extended
   set) — already constructed; the §11 handles are now populated.
2. Construct `RecalcEngine::new(Arc::clone(&dg_kv), Arc::clone(&container))`;
   pass `Arc<RecalcEngine>` + `Arc<DiskdbMetrics>` into `DiskdbService::new`
   (signature gains two fields).
3. Construct `ReportingTask::new(container, metrics, config_handle)`; register
   in `BgRunner` alongside keepalive + compaction.
4. `keepalive` gains the real `server.listen_addr` endpoint for the piggyback
   (passed via `with_endpoint` or read from config in `tick`).
5. Recovery (`run_recovery`) attaches `DiskMetrics` to each recovered disk
   (matching `disk_add_init`).

## Open Questions

None — the backlog doc resolves the architectural decisions (two-tier read
model, derived gauges, v1 no auto-fix, v1 reactive cache refresh). The
disk_id→dg_id reverse routing for `free_blocks` (§11.2d) is an implementation
detail resolved above (build the reverse map during `refresh_endpoints` from
the hardware hierarchy). The `zone_index == 0` ambiguity is resolved via
proto3 `optional` field presence.
