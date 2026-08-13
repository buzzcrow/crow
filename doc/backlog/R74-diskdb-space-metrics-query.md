<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R74: diskdb — Space Metrics + Query API (Space Metrics Component)

**Problem**: R72 implements allocation/free and R73 implements crash
recovery, but there is no way to query disk usage. The
`query_disk_usage` gRPC RPC is stubbed (returns empty). The design doc
(§11) specifies per-disk, per-disk-group, and per-zone space metrics
with accurate accounting and a recalculation path to verify
correctness. Without metrics, operators cannot monitor capacity, and
the console (R77) has no data to display.

The design doc emphasizes: "We prefer **accurate** statistics with a
way to **recalculate** the value (rebuild from the journal) to verify
correctness and detect drift." This means metrics must be derived from
the in-memory bitmap (which is derived from the journal), and there
must be a verification path that replays the journal independently to
check the bitmap matches.

The aioss reference has a two-tier metrics system: per-disk atomic
counters (`DiskMetrics` with period/total counters) and Prometheus
metrics (`DiskdbMetrics` with labeled gauges/counters). CROW reuses
`crow-common`'s metrics module (D8) instead of a parallel Prometheus
registry, but the per-disk atomic counter pattern is directly
applicable.

**Solution**: Implement the third major component — space metrics —
with accurate accounting, a query API, and a recalculation verification
path.

1. **Per-disk hot-path counters** — create
   `app/crow-diskdb/src/metrics/disk.rs`:
   - `DiskMetrics` — lock-free atomic counters per disk (matching
     aioss pattern, adapted for CROW):
     - **Period counters** (reset each reporting interval):
       `period_allocate_count: AtomicU64`,
       `period_allocate_bytes: AtomicU64`,
       `period_free_count: AtomicU64`,
       `period_free_bytes: AtomicU64`.
     - **Total counters** (monotonic):
       `total_allocate_count: AtomicU64`,
       `total_allocate_bytes: AtomicU64`,
       `total_free_count: AtomicU64`,
       `total_free_bytes: AtomicU64`.
     - **Capacity gauges** (current state):
       `capacity_bytes: AtomicU64` (constant after disk add),
       `busy_bytes: AtomicI64` (updated on alloc/free),
       `free_bytes: AtomicI64` (updated on alloc/free),
       `active_zone_count: AtomicU64` (zones in active deque),
       `total_zone_count: AtomicU64` (constant after disk add).
   - `record_allocate(bytes: u64)` — increment period/total allocate
     counters, increase `busy_bytes`, decrease `free_bytes`. All
     `Ordering::Relaxed` (hot path, no synchronization needed).
   - `record_free(bytes: u64)` — increment period/total free
     counters, decrease `busy_bytes`, increase `free_bytes`.
   - `swap_periods() -> PeriodSnapshot` — atomically swap period
     counters to 0, add to totals, return the swapped values. Called
     by the reporting loop.
   - `snapshot() -> DiskMetricsSnapshot` — read all counters for
     query API. Returns a consistent-enough snapshot (relaxed reads;
     exact consistency is not required for metrics, but the
     recalculation path provides exact verification).
   - Integrated into `ZoneDisk` (from R71/R72): `DiskMetrics` field
     on each `ZoneDisk`, updated in `disk_claim()`, `free()`,
     `active_zone()`.

2. **Per-zone space accounting** — add to
   `app/crow-diskdb/src/zone/mod.rs` (from R72):
   - `Zone::busy_blocks() -> u64` — `usage_bits.count_set()` (count
     of set bits in the bitmap).
   - `Zone::free_blocks() -> u64` — `max_allocate_pos -
     busy_blocks()` (free = total allocatable blocks minus busy).
     Note: `free_blocks` here means "blocks not currently allocated"
     — since allocation is append-only, blocks below
     `allocate_pos` that are freed are counted as free, and blocks
     above `allocate_pos` are also free (unallocated). So `free_blocks
     = max_allocate_pos - busy_blocks()`.
   - `Zone::busy_bytes() -> u64` — `busy_blocks() <<
     granularity_shift`.
   - `Zone::free_bytes() -> u64` — `free_blocks() <<
     granularity_shift`.
   - `Zone::capacity_bytes() -> u64` — `max_allocate_pos <<
     granularity_shift`.
   - `Zone::usage_ratio() -> f64` — `busy_blocks() as f64 /
     max_allocate_pos as f64`.

3. **Per-disk-group aggregation** — add to
   `app/crow-diskdb/src/node/mod.rs` (from R71):
   - `Node::disk_usage() -> Vec<DiskUsage>` — iterate disks, build
     `DiskUsage` per disk with capacity/busy/free/zone counts.
   - `Node::aggregate_usage() -> NodeUsage` — sum across disks:
     `capacity_bytes`, `busy_bytes`, `free_bytes`, plus per-disk
     breakdown.
   - `Node::zone_usage(disk_uuid, zone_idx) -> Option<ZoneUsage>` —
     dive into a specific zone's busy/free blocks. Returns
     `ZoneUsage { zone_index, capacity_bytes, busy_bytes,
     free_bytes, busy_block_count, free_block_count, allocation_state,
     zone_state }`. Used by R77's block-array visualization.

4. **Query API** — implement `query_disk_usage` gRPC handler in
   `app/crow-diskdb/src/grpc/service.rs` (from R72):
   - `query_disk_usage(request) -> Result<QueryDiskUsageResponse>`:
     a. If `request.node_id` is empty: iterate all nodes in
        `NodeContainer`, build `NodeUsage` for each.
     b. If `request.node_id` is set: look up that specific node,
        build `NodeUsage`. Return `NotFound` if not owned.
     c. Each `NodeUsage` includes aggregated capacity/busy/free and
        a `DiskUsage` breakdown per disk.
     d. Each `DiskUsage` includes `zone_count`,
        `active_zone_count`, `busy_zone_count`, `bad_zone_count`,
        capacity/busy/free (using the extended proto from R70).
   - Response is built from in-memory state (fast, no KV reads).
     Accuracy is guaranteed by the bitmap-derived accounting.

5. **Recalculation path** — create
   `app/crow-diskdb/src/metrics/recalc.rs`:
   - `RecalcEngine` — verifies in-memory metrics against the journal.
     Uses `DataGroupClient` (from R72) and `RecoveryEngine` (from R73).
   - `recalc_zone(dg_id, bind, disk_uuid, zone_idx) ->
     Result<RecalcResult>`:
     a. Independently replay the journal for the zone (same algorithm
        as R73's `recover_zone`, but into a **separate** bitmap — not
        the live in-memory one).
     b. Compare the replayed bitmap and `allocate_pos` against the
        live zone's state.
     c. Return `RecalcResult { matches: bool, live_busy_blocks: u64,
        replayed_busy_blocks: u64, live_allocate_pos: u32,
        replayed_allocate_pos: u32, drift_detected: bool }`.
   - `recalc_node(dg_id) -> Result<NodeRecalcResult>` — recalc all
     zones for a disk-group, aggregate results.
   - `recalc_all() -> Result<Vec<NodeRecalcResult>>` — recalc all
     owned disk-groups.
   - **Trigger**: recalc can be triggered:
     - Manually via a gRPC admin RPC (`RecalcDiskUsage`) — add to
       the proto and handler.
     - Periodically as part of the background scanner (R75) — the
       scanner runs recalc and logs/alerts on drift.
   - **Drift handling**: if drift is detected (live state ≠ replayed
     state), log a warning with details. The scanner (R75) can
     trigger a reload from journal to correct the live state. v1 does
     not auto-correct — it reports and lets the operator decide.

6. **crow-common metrics integration** — create
   `app/crow-diskdb/src/metrics/mod.rs`:
   - `DiskdbMetrics` — registers diskdb-specific metrics with
     `crow-common`'s metrics registry (D8). Per-disk hot-path
     counters (from `DiskMetrics`) flush into the crow-common
     registry at reporting intervals. (Note: `DiskdbMetrics` already
     exists from R72 with `allocate_retry_cas_bit` and
     `disk_bad_impacted_blocks` handles; R74 extends it with the
     full gauge/counter/histogram set below.)
   - **Gauge counters** (for console/UI display, R77):
     - `disk_capacity_bytes` (labeled by disk_uuid, dg_id).
     - `disk_busy_bytes`, `disk_free_bytes`.
     - `disk_active_zone_count`, `disk_total_zone_count`.
     - `node_capacity_bytes`, `node_busy_bytes`, `node_free_bytes`
       (aggregated).
   - **Counter metrics** (for perf monitoring):
     - `allocate_total` (labeled by dg_id, disk_uuid).
     - `free_total`.
     - `allocate_errors_total` (labeled by dg_id, error_code).
     - `allocate_latency` (histogram, buckets: 1ms, 5ms, 10ms, 25ms,
       50ms, 100ms, 250ms, 500ms, 1s).
   - **Latency hierarchy (§11)** — the full per-layer latency
     breakdown specified in §11. R74 implements these using
     `LatencyHistogram` for hot paths (allocate/free bitmap scan, KV
     persist) and `LatencySummary` for cold paths (sync, compaction,
     zone rotate):
     - `allocate.rpc.latency_us`, `allocate.bitmap_scan.latency_us`,
       `allocate.kv_persist.latency_us`,
       `allocate.zone_rotate.latency_us`.
     - `free.rpc.latency_us`, `free.bitmap_clear.latency_us`,
       `free.kv_persist.latency_us`.
     - `sync.latency_us`, `sync.read_group0.latency_us`,
       `sync.apply_changes.latency_us`.
     - `compaction.latency_us`, `compaction.scan_free.latency_us`,
       `compaction.merge_bitmap.latency_us`,
       `compaction.kv_persist.latency_us`.
   - **Sync/recovery metrics**:
     - `sync_success_total`, `sync_failure_total`.
     - `sync_duration_ms` (histogram).
     - `recovery_duration_ms` (histogram).
     - `compaction_records_deleted_total`.
   - `reporting_loop(node_container, metrics_registry)` — background
     task: every 10 s, call `swap_periods()` on all `DiskMetrics`,
     update gauge counters in crow-common registry. This bridges the
     hot-path atomic counters to the reporting layer.

7. **Keepalive usage summary piggyback (§11)** — update
   `app/crow-diskdb/src/sync.rs`:
   - The sync loop's `heartbeat_diskdb` call currently passes empty
     arrays (from R72). §11 specifies a per-disk-group usage summary
     piggybacked on keepalive: `capacity_bytes`, `used_bytes`,
     `free_bytes`, `disk_count`, `allocatable_disk_count`. Group 0
     maintains this at the disk-group level
     (`DiskGroupUsageKey { disk_group_id }`). The console reads this
     for cluster-wide overview; per-disk/per-zone drill-down is via
     the `QueryCapacityStats` API (also R74).
   - Compute the summary from the in-memory bitmap on each sync tick
     (derived, not a source of truth). Pass it to
     `heartbeat_diskdb`.

7. **Proto extension** — update `lib/crow-protocol/src/proto/diskdb.proto`
   (from R70):
   - Add `RecalcDiskUsageRequest` / `RecalcDiskUsageResponse` for the
     recalc admin RPC.
   - Add `ZoneUsage` message (if not already in R70) with per-zone
     busy/free breakdown.
   - Add `zone_usage` field to `DiskUsage` (repeated `ZoneUsage`) for
     optional per-zone detail in `QueryDiskUsageResponse`.

**Scope** (expected changed files):
- `app/crow-diskdb/src/metrics/mod.rs` — `DiskdbMetrics`, reporting
  loop, crow-common integration.
- `app/crow-diskdb/src/metrics/disk.rs` — `DiskMetrics` per-disk
  atomic counters.
- `app/crow-diskdb/src/metrics/recalc.rs` — `RecalcEngine` for
  verification.
- `app/crow-diskdb/src/zone/mod.rs` — add `busy_blocks()`,
  `free_blocks()`, `busy_bytes()`, `free_bytes()`, `capacity_bytes()`,
  `usage_ratio()`.
- `app/crow-diskdb/src/node/mod.rs` — add `disk_usage()`,
  `aggregate_usage()`, `zone_usage()`.
- `app/crow-diskdb/src/grpc/service.rs` — implement
  `query_disk_usage`, add `recalc_disk_usage` admin handler.
- `app/crow-diskdb/src/grpc/admin.rs` — add recalc RPC handler.
- `app/crow-diskdb/src/lib.rs` — add `metrics` module.
- `lib/crow-protocol/src/proto/diskdb.proto` — add `RecalcDiskUsage` RPC,
  `ZoneUsage` message (if not in R70).
- `app/crow-diskdb/src/main.rs` — spawn metrics reporting loop.
- `app/crow-diskdb/Cargo.toml` — ensure `crow-common` dependency.

**Complexity**: Medium. The per-disk atomic counter pattern is
directly from the aioss reference. The per-zone accounting is simple
bitmap arithmetic. The recalculation path reuses R73's replay logic
into a separate bitmap — the main work is the comparison and drift
reporting. The crow-common integration follows the existing metrics
patterns in `crow-kv`.

**Dependencies**: R70 (types, proto), R71 (NodeContainer), R72
(allocation engine, DataGroupClient), R73 (RecoveryEngine, replay
logic). No dependency on R75–R77.

**Acceptance**:
- `DiskMetrics` tracks per-disk allocate/free counts and bytes with
  lock-free atomics. `swap_periods()` correctly moves period counters
  to totals. Unit test.
- `Zone::busy_blocks()` / `free_blocks()` correctly count set/unset
  bits in the bitmap. Unit test: allocate 5 blocks, free 2, verify
  `busy_blocks = 3`, `free_blocks = max - 3`.
- `Node::aggregate_usage()` sums across disks. Unit test.
- `query_disk_usage` gRPC returns correct `NodeUsage` / `DiskUsage`
  with capacity/busy/free/zone counts. Integration test: allocate
  blocks, query, verify metrics match.
- `RecalcEngine.recalc_zone()` independently replays the journal and
  compares against live state. Unit test: no drift → `matches = true`.
  Unit test: simulate drift (manually corrupt live bitmap) →
  `matches = false`, `drift_detected = true`.
- `RecalcDiskUsage` admin RPC triggers recalc and returns results.
  Integration test.
- crow-common metrics registered: gauges for capacity/busy/free,
  counters for allocate/free, histograms for latency. Reporting loop
  flushes every 10 s. Integration test: allocate blocks, wait for
  reporting tick, verify gauges updated.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.
