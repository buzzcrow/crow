<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R74: diskdb — Space Metrics + Query API (Space Metrics Component)

**Problem**: R72 implements allocation/free and R73 implements crash
recovery, but there is no way to query disk usage. The
`QueryCapacityStats` gRPC RPC is stubbed — `service/diskdb_service.rs`
returns `Status::unimplemented("query_capacity_stats not implemented
until R74")`. The design doc (§11) specifies per-disk,
per-disk-group, and per-zone space metrics with accurate accounting
and a recalculation path to verify correctness. Without metrics,
operators cannot monitor capacity, the keepalive piggyback carries
empty usage summaries, and the console (R77) has no data to display.

**Current behavior + impact**:
- `QueryCapacityStats` returns `Unimplemented` — no operator or
  console can read capacity/busy/free. `DiskInfo` / `DiskGroupInfo`
  protos carry identity + `DiskValue` fields only (capacity_units,
  zone_size_units, unit_size_bytes, zone_count, status) — no
  busy/free/active-zone fields.
- `KeepAlive::heartbeat` calls `svc.heartbeat_diskdb(instance_id, "",
  &[], &[])` — the per-disk-group usage summary (`DiskGroupUsageSummary`
  in `sysdata_type.proto`) is already wired through
  `ServiceRegistryClient::heartbeat_diskdb` but always passed empty, so
  group 0 never receives usage data and the cluster-wide overview is
  blank.
- `DiskdbMetrics` (in `metrics.rs`) registers only the two R72
  counters (`zone.allocate.retry.cms.bit`, `disk.bad.impacted_blocks`).
  The §11 gauge/latency-hierarchy/sync-recovery metric set is absent.
- `DdbZone` already tracks `used_count: AtomicU32`, `unit_capacity:
  u32`, `uncompacted_free_record_count`, and `snapshot_slot`, and
  `UsageBitmap::count_set()` exists — but no per-zone/per-disk/
  per-disk-group usage accessors or aggregation exist. The data is
  present in memory; it is just not exposed or aggregated.
- **No client reads the usage summaries back.** `DiskGroupUsageKey`
  (`/hw/dg_usage/<dg_id>`) is defined in
  `lib/crow-protocol/src/key/diskdb.rs` with `prefix_all()`, and
  `HardwareClient` has the full hierarchy scans (`list_racks`,
  `list_nodes`, `list_disk_groups`, `list_disks_in_group`), but no
  client class reads `/hw/dg_usage/*` and aggregates up to
  cluster/rack/node level. The console (R77) has no cluster-wide
  capacity view.
- **`crow-diskdb-client` is a skeleton** (`lib.rs` is just an error
  type) — no `QueryCapacityStats` wrapper, so callers have no retry/
  topology-cached way to drill into a specific diskdb instance for
  per-disk/per-zone detail.
- Root cause: deferred placeholder — R70–R73 built the allocator,
  records, and recovery; the metrics/query component (§11, the third
  major component) was never implemented. The read model is two-tier:
  group 0 stores **only disk-group-level** usage (the keepalive
  piggyback); per-disk/per-zone live usage is served **by diskdb
  directly** via `QueryCapacityStats` (a disk can have thousands of
  zones — group 0 never carries per-zone data).

**Design pointers**: `doc/design/diskdb/design-crow-diskdb.md` §11
(Space Metrics) — per-disk/per-disk-group/per-zone accounting,
keepalive usage piggyback, three metric categories (counters, gauges,
latency hierarchy), and the "accurate statistics with a recalculation
path" requirement. §3.4 (records are the source of truth; bitmap is
derived; freed space reused immediately — no append-only
`allocate_pos`). §7 (record model + three recovery strategies — the
recalc path reuses strategy 2 journal replay into a separate bitmap).
§3.7 (reuse `crow-common` metrics; per-disk atomics flush into the
registry at reporting intervals). aioss analog: per-disk atomic
`DiskMetrics` (period/total counters) + Prometheus labeled
gauges/counters; CROW reuses `crow-common`'s `MetricsRegistry`
(`Counter`, `Gauge`, `LatencyHistogram`, `LatencySummary`) instead of
a parallel registry, and derives gauges from the bitmap on the
reporting tick (single source of truth) rather than maintaining
parallel hot-path capacity atomics.

**Use scenarios**:
- **Caller allocates blocks** — a caller (a future object store or
  chunk service) uses `DiskdbClient::allocate_blocks` to get disk
  blocks, then `free_blocks` when done. The client discovers the
  diskdb instance endpoint for the `disk_group_id` from the service
  registry (group 0), caches it, and routes the gRPC call with retry.
  Expected: the caller doesn't need to know which diskdb instance
  owns the group — the client handles endpoint discovery + routing.
- **Operator capacity check** — an operator (via the console or CLI)
  uses `DiskdbClient::query_disk_group` to read per-disk-group +
  per-disk capacity/busy/free + zone counts to decide whether to add
  disks. Expected: response built from in-memory state, fast (no KV
  reads), accurate (bitmap-derived); the client handles endpoint
  discovery transparently.
- **Console cluster overview** — the console (R77) reads
  `DiskGroupUsageSummary` records from group 0 via the kv-client
  space-usage class (written by the keepalive piggyback) and
  aggregates them up to cluster/rack/node/disk-group level for a
  cluster-wide capacity view, then drills into a specific diskdb
  instance via `DiskdbClient` for per-disk/per-zone detail.
  Expected: piggyback summary recomputed each sync tick from the
  bitmap (disk-group level only in group 0); drill-down returns brief
  per-zone counts at disk level, full bitmap for a specific zone.
- **Drift verification** — an operator (or the R75 scanner) triggers
  `RecalcDiskUsage`; the engine independently replays the journal for
  each zone into a separate bitmap and compares against the live
  `DdbZone`. Expected: `matches = true` when live state agrees with
  the records; `drift_detected = true` + details when they differ.
- **Performance diagnosis** — an operator inspects the §11 latency
  hierarchy (`allocate.bitmap_scan.latency_us` vs
  `allocate.kv_persist.latency_us`) to localize whether allocation
  latency is the in-memory allocator or the paxos round-trip.
  Expected: hot paths instrumented with `LatencyHistogram`, cold paths
  with `LatencySummary`, flushed by the reporting loop.
- **Alerting on health** — an external scraper reads the
  `degraded` gauge and `last_sync_age_secs` to alert on diskdb
  instances that have missed sync. Expected: gauges updated each
  reporting tick.

**Solution**: Implement the third major component — space metrics —
with accurate bitmap-derived accounting, a `QueryCapacityStats`
handler (brief zone counts at disk level, full bitmap only for a
specific zone), a keepalive usage piggyback writing disk-group-level
summaries to group 0, a kv-client class aggregating those summaries
to cluster/rack/node level, a full `crow-diskdb-client` library
(allocate/free/query with endpoint caching from the service
registry), a recalculation verification path, and the §11 metric set
(counters, gauges, latency hierarchy) flushed by a reporting loop.

**One-line summary**: expose the bitmap-derived usage already tracked
in `DdbZone`/`DdbDisk`/`DdbDiskGroup` through per-zone/per-disk/
per-disk-group accessors, a `QueryCapacityStats` handler (brief zone
counts at disk level, full bitmap only for a specific zone), a
keepalive piggyback writing disk-group-level summaries to group 0, a
kv-client class aggregating those summaries to cluster/rack/node
level, a full `crow-diskdb-client` library (allocate/free/query with
endpoint caching from the service registry), a journal-replay recalc
verifier, and the full §11 `crow-common` metric set driven by a
reporting loop.

1. **Per-zone usage accessors** — add to
   `app/crow-diskdb/src/model/zone.rs` (`DdbZone`):
   - `busy_blocks() -> u32` reads `used_count`; `free_blocks() -> u32`
     returns `unit_capacity - used_count` (CROW reuses freed space
     immediately via bitmap scan — there is no append-only
     `allocate_pos`; free = capacity minus busy, per §3.4/§8).
   - `busy_bytes(unit_size_bytes) / free_bytes(unit_size_bytes) /
     capacity_bytes(unit_size_bytes) -> u64` — blocks × the disk's
     `unit_size_bytes` (per-disk, from `DiskValue.unit_size_bytes`;
     not a global shift).
   - `usage_ratio() -> f64` — `used_count / unit_capacity`.
   - These read the live atomics/bitmap; `UsageBitmap::count_set()`
     is used only by recalc/verification for an independent popcount.

2. **Per-disk + per-disk-group aggregation** — add to
   `app/crow-diskdb/src/model/disk.rs` (`DdbDisk`) and
   `app/crow-diskdb/src/model/disk_group.rs` (`DdbDiskGroup`):
   - `DdbDisk::usage(unit_size_bytes) -> DiskUsage` — sum across
     zones: capacity/busy/free bytes, `zone_count`,
     `active_zone_count` (size of the RCU `active_zone_context`),
     busy/free zone counts (zones with `used_count == unit_capacity`
     vs not). Reads `disk_value` + `zones` under their read locks.
   - `DdbDiskGroup::aggregate_usage() -> DiskGroupUsage` — sum across
     disks: capacity/busy/free bytes, `disk_count`,
     `allocatable_disk_count` (size of `allocating_disks`), plus the
     per-disk `DiskUsage` breakdown. This is the struct reused by the
     query handler and the keepalive piggyback.
   - `DdbDiskGroup::zone_usage(disk_id, zone_index) -> Option<ZoneUsage>`
     — locate the disk via the `disk_index`, the zone by index, return
     `ZoneUsage { zone_index, capacity_bytes, busy_bytes,
     free_bytes, busy_block_count, free_block_count, alloc_state
     (derived_alloc_state), zone_state }` — **brief counts only**, no
     bitmap bytes. The full bitmap is returned only by the
     specific-zone query path (item 4) via `DdbZone::usage_bits` /
     `UsageBitmap::snapshot`. Used by R77's per-zone visualization.

3. **Per-disk hot-path counters** — convert
   `app/crow-diskdb/src/metrics.rs` to a module dir
   (`metrics/mod.rs` holding the existing `DiskdbMetrics`) and add
   `app/crow-diskdb/src/metrics/disk.rs`:
   - `DiskMetrics` — lock-free atomic counters per `DdbDisk`, adapted
     from the aioss pattern but holding **only** period/total event
     counters (the capacity/busy/free gauges are derived from the
     bitmap on the reporting tick — §11 "gauges are derived snapshots,
     not hot-path writes" — so no parallel capacity atomics that could
     drift from the bitmap):
     - period: `period_allocate_count`, `period_allocate_bytes`,
       `period_free_count`, `period_free_bytes` (`AtomicU64`,
       `Relaxed`).
     - total: `total_allocate_count`, `total_allocate_bytes`,
       `total_free_count`, `total_free_bytes` (`AtomicU64`,
       `Relaxed`, monotonic).
   - `record_allocate(unit_count, unit_size_bytes)` /
     `record_free(unit_count, unit_size_bytes)` — bump period + total
     count/bytes. Called from `model/alloc.rs` Phase 1 (sync, after
     the bitmap CAS succeeds) so the counter is incremented exactly
     once per durable-bound allocation/free.
   - `swap_periods() -> PeriodSnapshot` — atomically swap period
     counters to 0 and return the deltas; the reporting loop adds
     deltas to totals (totals also kept inline for crash-safe
     monotonicity).
   - `DiskMetrics` field on each `DdbDisk` (added in `disk.rs`),
     attached during `disk_add_init` and `recover_disk_group`.

4. **`QueryCapacityStats` handler** — implement in
   `app/crow-diskdb/src/service/diskdb_service.rs` (replace the
   `unimplemented` stub). The request gains two optional drill-down
   fields: `disk_id` and `zone_index` (both default 0 = "not set";
   see proto item 9). Three query shapes:
   - **Cluster/disk-group level** (`disk_group_id` only, `disk_id` =
     0): if `disk_group_id == 0`, iterate `container.disk_group_ids()`
     and build a `DiskGroupInfo` per owned group; if set, look up that
     one group (`NotFound` if not owned). Each `DiskGroupInfo` carries
     the aggregated capacity/busy/free + member `DiskInfo`s with
     per-disk usage + zone counts. **No per-zone entries** at this
     level — a disk can have thousands of zones; including them all
     would blow up the response.
   - **Disk level** (`disk_group_id` + `disk_id` set, `zone_index` =
     0): return the one `DiskInfo` with a `repeated ZoneUsage
     zone_usages` of **brief** per-zone entries (counts only:
     busy/free blocks, capacity, alloc_state, zone_state — **no
     bitmap bytes**). Thousands of small count-only entries is
     acceptable; thousands of full bitmaps is not.
   - **Zone level** (`disk_group_id` + `disk_id` + `zone_index` all
     set): return the one `ZoneUsage` with the **full `usage_bitmap`
     bytes** (via `UsageBitmap::snapshot`) for R77's block-array
     visualization. Only one zone's bitmap is ever serialized per
     request.
   - Read-only — allowed in any lifecycle phase (like
     `GetDiskGroupInfo`); built from in-memory state, no KV reads,
     accuracy guaranteed by bitmap-derived accounting.

5. **Recalculation path** — create
   `app/crow-diskdb/src/metrics/recalc.rs`:
   - `RecalcEngine` — owns an `Arc<DdbKvClient>` and a reference to
     the `DdbDiskGroupContainer`. Verifies live in-memory metrics
     against the durable records by replaying the journal into a
     **separate** bitmap (not the live one), reusing
     `recovery::journal_replay::recover_zone_inner` (strategy 2) and
     falling back to `recovery::full_scan::rebuild_zone_bitmap_full_scan`
     (strategy 1) on `JournalScanGcGap` / `SnapshotCrcFail`.
   - `recalc_zone(bind, disk_id, zone_idx, unit_capacity) ->
     Result<RecalcResult>` — replay the zone into a throwaway
     `DdbZone`, then compare its `usage_bits` (via `count_set()`) and
     `used_count` against the live `DdbZone`. Return `RecalcResult {
     matches, live_busy_blocks, replayed_busy_blocks,
     live_snapshot_slot, replayed_snapshot_slot, drift_detected }`.
     `drift_detected = !matches`. CROW has no `allocate_pos` to
     compare — drift is purely bitmap/used-count divergence.
   - `recalc_disk_group(dg_id) -> Result<DiskGroupRecalcResult>` —
     recalc all zones across all disks in one disk-group, aggregate.
   - `recalc_all() -> Result<Vec<DiskGroupRecalcResult>>` — recalc
     every owned disk-group.
   - **Trigger**: manually via the `RecalcDiskUsage` admin RPC (item
     9); periodically by the R75 scanner (which calls `recalc_all`
     and logs/alerts on drift). v1 reports drift only — it does not
     auto-correct the live bitmap (the operator runs
     `RebuildZoneBitmap`, strategy 1, to correct).

6. **crow-common metrics integration** — extend `DiskdbMetrics` in
   `app/crow-diskdb/src/metrics/mod.rs` (the converted module) with
   the §11 set, registered against `crow-common`'s
   `MetricsRegistry`:
   - **Gauges** (derived from the bitmap on the reporting tick, not
     hot-path): per-disk `disk_capacity_bytes`, `disk_busy_bytes`,
     `disk_free_bytes`, `disk_active_zone_count`,
     `disk_total_zone_count`; per-disk-group `dg_capacity_bytes`,
     `dg_busy_bytes`, `dg_free_bytes`; per-instance
     `owned_disk_group_count`, `degraded` (from
     `DdbDiskGroupContainer::is_degraded`), `last_sync_age_secs`.
   - **Counters**: `allocate_total`, `free_total` (from the per-disk
     `DiskMetrics` totals, flushed by the reporting loop);
     `allocate_errors_total`, `sync_success_total`,
     `sync_failure_total`, `compaction_records_deleted_total`.
   - **Latency hierarchy (§11)** — `LatencyHistogram` for hot paths
     (allocate/free bitmap scan + KV persist), `LatencySummary` for
     cold paths (sync, compaction, zone rotate):
     `allocate.rpc.latency_us`, `allocate.bitmap_scan.latency_us`,
     `allocate.kv_persist.latency_us`, `allocate.zone_rotate.latency_us`;
     `free.rpc.latency_us`, `free.bitmap_clear.latency_us`,
     `free.kv_persist.latency_us`; `sync.latency_us`,
     `sync.read_group0.latency_us`, `sync.apply_changes.latency_us`;
     `compaction.latency_us`, `compaction.scan_free.latency_us`,
     `compaction.merge_bitmap.latency_us`,
     `compaction.kv_persist.latency_us`.
   - **Sync/recovery histograms**: `sync_duration_ms`,
     `recovery_duration_ms`.
   - Instrument the allocate/free paths in `model/alloc.rs`, the sync
     path in `liveness/keepalive.rs`, and the compaction path in
     `recovery/compaction.rs` with the matching latency handles
     (start-instant → observe-on-completion).

7. **Reporting loop** — a `BackgroundTask` (registered in `BgRunner`
   alongside keepalive + compaction in `main.rs`) that every 10 s:
   - calls `DiskMetrics::swap_periods()` on every disk, flushes the
     period deltas into the `allocate_total` / `free_total` counters;
   - recomputes the gauges from the in-memory bitmap
     (`DdbDisk::usage`, `DdbDiskGroup::aggregate_usage`) and pushes
     them to the crow-common `Gauge`s;
   - updates `degraded` + `last_sync_age_secs`.
   This bridges the hot-path atomic counters + bitmap state to the
   reporting layer. Cadence is configurable (live-reload via the
   shared config handle, matching keepalive/compaction).

8. **Keepalive usage piggyback (§11)** — update
   `app/crow-diskdb/src/liveness/keepalive.rs`:
   - In `tick()`, after `observe_disks`, compute one
     `DiskGroupUsageSummary` per owned disk-group from
     `DdbDiskGroup::aggregate_usage()` (capacity/used/free bytes +
     disk_count + allocatable_disk_count) and pass the slice to
     `svc.heartbeat_diskdb(instance_id, endpoint, &owned_dg_ids,
     &group_usages)` instead of the current empty `&[]`. The summary
     is derived (recomputed each tick from the bitmap), not a source
     of truth; group 0 stores it at `/hw/dg_usage/<dg_id>`.
   - **Group 0 stores only disk-group-level usage** — per-disk and
     per-zone live usage are never written to group 0 (a disk can have
     thousands of zones; group 0 is not a per-zone registry). The
     cluster/rack/node/disk-group aggregation view is built client-side
     by joining `/hw/dg_usage/*` with the hardware hierarchy (item 10);
     per-disk/per-zone drill-down is served by diskdb directly (item 4
     + item 11).

9. **Proto extension** — update
   `lib/crow-protocol/src/proto/diskdb_type.proto` and
   `diskdb_op.proto` (the actual proto files; there is no single
   `diskdb.proto`):
   - Extend `DiskInfo` with `busy_units`, `free_units`,
     `capacity_bytes`, `busy_bytes`, `free_bytes`, `active_zone_count`
     (zone_count already present), and a `repeated ZoneUsage
     zone_usages` field.
   - Extend `DiskGroupInfo` with aggregated `capacity_bytes`,
     `busy_bytes`, `free_bytes`, `allocatable_disk_count`
     (disk_count is `disks.len()`).
   - Add `ZoneUsage` message with **brief count fields**
     (`zone_index`, `capacity_bytes`, `busy_bytes`, `free_bytes`,
     `busy_block_count`, `free_block_count`, `alloc_state`,
     `zone_state`) + an **optional** `bytes usage_bitmap` field
     (populated only for a specific-zone query; omitted at disk level
     to keep the response small when a disk has thousands of zones).
   - Extend `QueryCapacityStatsRequest` with optional `disk_id` and
     `zone_index` fields (0 = not set) to select the three query
     shapes in item 4.
   - Add `RecalcDiskUsageRequest` / `RecalcDiskUsageResponse` to
     `diskdb_op.proto` and the `RecalcDiskUsage` RPC to
     `DiskdbService` in `diskdb_service.proto`; implement the handler
     in `service/diskdb_service.rs` (delegates to `RecalcEngine`).
     `RecalcDiskUsageRequest` takes optional `disk_group_id` (0 = all
     owned); the response carries per-disk-group `RecalcResult`
     summaries.

10. **kv-client space-usage aggregation** — add a client class to
    `lib/crow-kv-client` (e.g. `SpaceUsageClient` wrapping
    `CrowkvClient`, or a new method set on `HardwareClient`) that
    reads group-0 sysdata and aggregates the disk-group-level usage
    summaries up the hardware hierarchy for the cluster-wide view:
    - `list_disk_group_usages() -> Vec<(DiskGroupId, DiskGroupUsageSummary)>`
      — prefix scan `/hw/dg_usage/` via `DiskGroupUsageKey::prefix_all()`
      (key + `DiskGroupUsageSummary` value, both already defined in
      `crow-protocol`).
    - `cluster_usage() -> ClusterUsage` — join all
      `DiskGroupUsageSummary` records with the hardware hierarchy
      (`list_racks`, `list_nodes`, `list_disk_groups`,
      `list_disks_in_group`) and aggregate to cluster-level
      capacity/used/free + disk-group count.
    - `rack_usage(rack_id) / node_usage(rack_id, node_id) /
      disk_group_usage(dg_id)` — scoped aggregations down the
      hierarchy.
    - **Per-disk live busy/free is not available here** — group 0
      carries only disk-group-level usage + per-disk static capacity
      (`DiskValue.capacity_units`). Live per-disk/per-zone usage is
      served by diskdb via the diskdb-client (item 11). The kv-client
      view may be up to one sync interval stale (the piggyback
      cadence); the diskdb-client view is live.
    - Used by the console (R77) for the cluster/rack/node/disk-group
      overview; mirrors `crow-kv-client`'s retry + topology-cache
      pattern.

11. **diskdb-client — full client library** — fill in
    `lib/crow-diskdb-client/src/lib.rs` (currently a skeleton with
    only an error type; `Cargo.toml` already has the right deps:
    `crow-protocol`, `crow-kv-client`, `tonic`, `dashmap`). This is
    the primary client surface for all diskdb gRPC operations, not
    just queries — it provides allocate, free, and query with retry
    + endpoint caching, mirroring `crow-kv-client`'s pattern.
    - **`DiskdbClient`** — the main client struct. Owns a
      `ServiceRegistryClient` (from `crow-kv-client`, for endpoint
      discovery from group 0) and a `dashmap` endpoint cache
      (`disk_group_id -> grpc_endpoint`). Holds a tonic channel pool
      per endpoint.
    - **Endpoint discovery + cache** —
      `refresh_endpoints()` calls
      `ServiceRegistryClient::read_all_diskdb_instances()` (already
      exists), reads each `InstanceValue.grpc_endpoint` +
      `DiskdbExtra.owned_dg_ids`, and populates the
      `disk_group_id -> grpc_endpoint` cache. Called on startup
      (eager warm), on cache miss (lazy refresh for an unknown
      `disk_group_id`), and on `ResourceExhausted`/`Unavailable`
      (endpoint may have moved — refresh + retry). The cache is a
      `dashmap` so concurrent reads don't block on a single lock.
    - **Cache refresh gap (R78 use case)** — v1 refreshes the cache
      on demand (startup + cache miss + error retry). Proactive
      refresh (the cache updates immediately when a diskdb instance
      registers/deregisters/moves, without waiting for a cache miss)
      requires group-0 notify/watch, which does not exist yet —
      tracked as an R78 use case. v1 accepts that a moved instance
      is detected on the next error-triggered refresh, not
      proactively.
    - **`allocate_blocks(request) -> AllocateResponse`** — wraps the
      `AllocateBlocks` gRPC call. Looks up the endpoint for
      `request.disk_group_id` from the cache (refreshes on miss),
      opens a tonic channel, calls `allocate_blocks`, retries on
      transient errors (`Unavailable`, deadline-exceeded). On
      `ResourceExhausted` (no space) returns the error to the caller
      (not a retryable condition).
    - **`free_blocks(request) -> FreeResponse`** — wraps
      `FreeBlocks`. The request carries `Segment`s (each with
      `disk_id`); the client routes by looking up which diskdb
      instance owns the `disk_id`'s disk-group. If segments span
      multiple disk-groups, the client splits the request per
      disk-group and issues one `FreeBlocks` per group (v1: no
      cross-group batch). Retries on transient errors.
    - **`query_capacity_stats(request) -> QueryCapacityStatsResponse`**
      — wraps `QueryCapacityStats`. Convenience helpers:
      `query_disk_group(dg_id)` (disk-group level, no zones),
      `query_disk(dg_id, disk_id)` (brief zone counts),
      `query_zone(dg_id, disk_id, zone_index)` (full bitmap). Returns
      live, authoritative per-disk/per-zone usage from the owning
      diskdb instance — the drill-down counterpart to the kv-client's
      cluster-wide aggregated view.
    - **`get_disk_group_info(dg_id)` / `get_disk_info(dg_id, disk_id)`**
      — wrap the read-only `GetDiskGroupInfo` / `GetDiskInfo` RPCs
      (identity + static disk fields; `get_disk_info` is extended
      with the R74 usage fields by item 4).
    - **`recalc_disk_usage(request)`** — wraps the
      `RecalcDiskUsage` admin RPC (item 9).
    - **Error model** — `DiskdbClientError` (already in the
      skeleton) extended with gRPC status mapping: `Unreachable`
      (no endpoint found for `disk_group_id` after refresh),
      `Rpc(Status)` (gRPC error), `NoSpace`, `NotFound`,
      `InvalidArgument`. Retry logic mirrors `crow-kv-client`'s
      `RetryConfig` (max retries, backoff).

12. **E2E test migration** — move diskdb e2e tests that exercise the
    gRPC service to use `crow-diskdb-client` instead of raw in-process
    library calls or raw gRPC stubs:
    - The current `app/crow-diskdb/tests/diskdb_e2e_test.rs` calls
      `alloc::allocate_block` / `alloc::free_block` **in-process**
      (bypassing gRPC entirely) — it tests the server library
      internals, not the client surface. These stay as-is (they're
      server-internal integration tests).
    - New gRPC-level e2e tests (allocate/free/query via the gRPC
      service) should live in `lib/crow-diskdb-client/tests/` and use
      `DiskdbClient` for all operations — this tests the real client
      + server round-trip (endpoint discovery, retry, gRPC
      serialization) that in-process tests miss.
    - The test harness starts a real `crow-kv-server` cluster (via
      `KvCluster`) + a real `crow-diskdb` gRPC server (in-process
      tonic server bound to a loopback port), seeds hardware
      metadata, then uses `DiskdbClient` (pointed at the group-0
      leader for endpoint discovery) to allocate/free/query and
      verify. This is the pattern `crow-kv-client/tests/` uses
      against embedded `PxKvStore` gRPC servers.
    - Tests to add in `lib/crow-diskdb-client/tests/`:
      `allocate_free_e2e.rs` (allocate → verify busy record → free →
      verify free record), `query_e2e.rs` (allocate →
      `query_disk_group` → assert busy_units; `query_disk` → assert
      brief zone counts; `query_zone` → assert full bitmap),
      `endpoint_cache_e2e.rs` (endpoint discovery from service
      registry, cache miss → refresh → retry), `recalc_e2e.rs`
      (allocate → recalc → assert matches; corrupt live bitmap →
      recalc → assert drift detected).

**Flow diagram**:

```
  ┌─── cluster-wide view (kv-client, from group 0) ──────────────┐
  │                                                              │
  │  group 0: /hw/dg_usage/<dg_id>  (disk-group level only)      │
  │      + /hw/rack, /hw/node, /hw/dg, /hw/disk (hierarchy)      │
  │                    │                                         │
  │                    v                                         │
  │  SpaceUsageClient (crow-kv-client)                           │
  │    cluster_usage / rack_usage / node_usage / disk_group_usage│
  │    (aggregated from DiskGroupUsageSummary; ≤1 sync stale)    │
  │    per-disk = static capacity only; no live busy/free        │
  └──────────────────────────────────────────────────────────────┘

  ┌─── diskdb-client → diskdb gRPC (allocate/free/query) ────────┐
  │                                                              │
  │  DiskdbClient (crow-diskdb-client)                           │
  │    allocate_blocks / free_blocks / query_* / recalc          │
  │         │                                                    │
  │         │  endpoint cache: dg_id -> grpc_endpoint (dashmap)  │
  │         │  miss/refresh ──> ServiceRegistryClient            │
  │         │                     .read_all_diskdb_instances()   │
  │         │                     (group 0 /srv/diskdb/*)        │
  │         v                                                    │
  │  diskdb gRPC (DiskdbService, on one diskdb instance)         │
  │    AllocateBlocks / FreeBlocks / QueryCapacityStats /        │
  │    GetDiskGroupInfo / GetDiskInfo / RecalcDiskUsage          │
  │                    │                                         │
  │                    v                                         │
  │  DdbDiskGroupContainer ──> DdbDiskGroup::aggregate_usage     │
  │       (owned groups)            │                            │
  │                                 v                            │
  │                        DdbDisk::usage ──> DdbZone::busy/free │
  │                                 │              (used_count)  │
  │                                 v                            │
  │  query disk-group: DiskGroupInfo + DiskInfo (no zones)       │
  │  query disk:       DiskInfo + brief ZoneUsage[] (counts)     │
  │  query zone:       one ZoneUsage + full usage_bitmap         │
  │  (live, authoritative; bitmap-derived)                      │
  │                                                              │
  │  v1: cache refreshes on startup / miss / error-retry         │
  │  R78: proactive refresh via group-0 notify/watch (use case)  │
  └──────────────────────────────────────────────────────────────┘

  ┌─── write path: keepalive piggyback ──────────────────────────┐
  │  keepalive tick ──> aggregate_usage ──> DiskGroupUsageSummary[]│
  │                                          │                   │
  │                                          v                   │
  │                    heartbeat_diskdb ──> group 0 (/hw/dg_usage)│
  │                    (disk-group level only; no per-disk/zone)  │
  └──────────────────────────────────────────────────────────────┘

  ┌─── metrics + reporting ──────────────────────────────────────┐
  │  alloc/free hot path ──> DiskMetrics (period/total counters) │
  │  DdbZone bitmap + DdbDiskGroup::aggregate_usage              │
  │                    │                                         │
  │                    v                                         │
  │           reporting loop (10s)                               │
  │   swap_periods ─────────────┤                                │
  │   recompute gauges from bitmap ─┤                            │
  │   update degraded/sync_age ─────┤                            │
  │                    v                                         │
  │       crow-common MetricsRegistry                            │
  │       (Gauge / Counter / LatencyHist / LatencySummary)       │
  └──────────────────────────────────────────────────────────────┘

  ┌─── recalc verification ──────────────────────────────────────┐
  │  RecalcDiskUsage (admin RPC)                                 │
  │                    │                                         │
  │                    v                                         │
  │  RecalcEngine::recalc_zone                                   │
  │    ├─ recover_zone_inner (strategy 2, separate bitmap)        │
  │    │     └─ JournalScanGcGap / SnapshotCrcFail               │
  │    │           └─> rebuild_zone_bitmap_full_scan (strategy 1)│
  │    └─ compare replayed vs live DdbZone                       │
  │          └─ drift_detected? ──> log + report (v1 no auto-fix)│
  └──────────────────────────────────────────────────────────────┘
```

**Edge cases at a glance**:
- `QueryCapacityStats` with `disk_group_id == 0` and zero owned
  groups → empty `disk_groups` list, `Ok` (not an error).
- `QueryCapacityStats` for a disk-group not owned by this instance →
  `NotFound` (matches `GetDiskGroupInfo` behavior).
- **Disk with thousands of zones** (sizing) → disk-level query
  returns brief `ZoneUsage` entries (counts only, no bitmap bytes);
  the full `usage_bitmap` is returned only for a specific-zone query.
  A disk-level request never serializes thousands of full bitmaps.
- **Zone-level query for an out-of-range `zone_index`** → `NotFound`
  (not a silent empty bitmap).
- Zone with `used_count == unit_capacity` → `Full` alloc_state,
  `free_blocks == 0`; not an error, reported as-is.
- Last zone on a disk (smaller, word-aligned per §3.5) →
  `unit_capacity` is the rounded value; `capacity_bytes` reflects the
  real smaller capacity, not the uniform zone size.
- Recalc hits `JournalScanGcGap` (slots already GC'd) → fall back to
  strategy 1 full scan for that zone; `RecalcResult` notes the
  fallback path.
- Recalc hits `SnapshotCrcFail` → fall back to strategy 1; the live
  bitmap is suspect (its snapshot was corrupt) — `drift_detected =
  true` with a CRC-fail reason.
- Reporting loop tick during a concurrent `disk_add_init` /
  `remove_disk` → read locks make the gauge snapshot
  best-effort-consistent; a transient disk may be missed this tick
  and appear next tick. Acceptable for metrics (§11: exact consistency
  not required; recalc gives exact verification).
- `DiskMetrics` period counter wraps (`AtomicU64` overflow) →
  extremely unlikely at 1M-unit blocks; `swap_periods` saturating-add
  to totals avoids loss. No special handling.
- `heartbeat_diskdb` fails (group 0 unreachable) → keepalive already
  handles this (missed count → degraded); the usage summary is just
  not delivered this tick, retried next tick.
- **kv-client cluster view is stale** → the `/hw/dg_usage/*` records
  are up to one sync interval (10 s default) behind the live bitmap.
  The console shows this as the cluster overview; the operator drills
  into diskdb (diskdb-client) for the live authoritative view. Not an
  error — the two-tier model accepts this freshness tradeoff.
- **diskdb-client endpoint cache miss** (unknown `disk_group_id`) →
  lazy refresh from `read_all_diskdb_instances` + retry; if still not
  found after refresh → `Unreachable` error (no diskdb instance owns
  this group, or the instance is down/expired in the registry).
- **diskdb-client endpoint moved** (instance restarted on a new port)
  → the cached endpoint is stale; the next gRPC call fails with
  `Unavailable` → client refreshes cache → retries → succeeds. v1
  detects this reactively (on error), not proactively (R78 use case).
- **`free_blocks` spans multiple disk-groups** → the client splits
  the request per disk-group (one `FreeBlocks` gRPC call per group);
  v1 does not batch across groups. Partial failure (some groups
  succeed, some fail) returns per-group results.

**Dependencies**:
- **Depends on**: R70 (proto types, `DiskInfo`/`DiskGroupInfo`,
  `DiskGroupUsageSummary`, `DiskGroupUsageKey` + `prefix_all`,
  `UsageBitmap`, `ZoneValueExt`), R71 (`DdbDiskGroupContainer`,
  `DdbDiskGroup`, `DdbDisk`), R72 (`DdbKvClient`, `DiskdbMetrics`,
  `model/alloc.rs` two-phase allocate/free), R73 (`RecoveryEngine`,
  `recover_zone_inner`, `rebuild_zone_bitmap_full_scan`,
  `CompactionEngine`). All landed. `crow-kv-client`'s `HardwareClient`
  (hierarchy scans) + `ServiceRegistryClient` (instance discovery —
  `read_all_diskdb_instances` already exists) already exist; R74 adds
  the usage-aggregation method set. `crow-diskdb-client`'s
  `Cargo.toml` already has the right deps (`crow-protocol`,
  `crow-kv-client`, `tonic`, `dashmap`); R74 fills in the
  implementation.
- **Depended on by**: R75 (scanner reuses `RecalcEngine::recalc_all`
  + the metrics set), R77 (console reads the kv-client cluster view +
  uses `DiskdbClient` for allocate/free/query drill-down in the
  capacity UI). No dependency on R78–R79.
- **Unlanded extension**: the diskdb-client endpoint cache refreshes
  on demand (startup + cache miss + error retry) in v1. **Proactive
  refresh** (the cache updates immediately when a diskdb instance
  registers/deregisters/moves) requires group-0 notify/watch, which
  does not exist yet — added as a use case to R78. v1 detects a moved
  instance on the next error-triggered refresh, not proactively.
- **Already-built surfaces reused**: `heartbeat_diskdb` already
  accepts `DiskGroupUsageSummary[]`, `QueryCapacityStats` RPC +
  response proto already exist (response returns `repeated
  DiskGroupInfo`), `DiskGroupUsageKey` + `prefix_all` already
  defined, `read_all_diskdb_instances` already exists; R74 extends
  the messages, implements the handlers, and fills in the two client
  classes.

**Acceptance**:

**Per-zone accounting**:
- `DdbZone::busy_blocks()` returns `used_count`; `free_blocks()`
  returns `unit_capacity - used_count`. Setup: new zone cap=128,
  allocate 5 units → assert `busy_blocks == 5`, `free_blocks == 123`.
  Free 2 → assert `busy_blocks == 3`, `free_blocks == 125` (freed
  space reused immediately, no append-only position). `busy_bytes(n)`
  = `busy_blocks * n`, `capacity_bytes(n)` = `unit_capacity * n`.
  Unit test.
- `usage_ratio()` = `used_count / unit_capacity` as f64. Unit test.

**Per-disk + per-disk-group aggregation**:
- `DdbDisk::usage()` sums across zones: capacity/busy/free bytes,
  zone_count, active_zone_count (size of `active_zone_context`).
  Setup: disk with 2 zones, allocate in zone 0 only → assert
  disk-level busy = zone-0 busy, free = sum of both zones' free.
  Unit test.
- `DdbDiskGroup::aggregate_usage()` sums across disks: capacity/busy/
  free, disk_count, allocatable_disk_count (size of
  `allocating_disks`). Setup: 2 disks, one `Bad` →
  `allocatable_disk_count` excludes it. Unit test.
- `DdbDiskGroup::zone_usage(disk_id, zone_index)` returns
  `Some(ZoneUsage)` for an existing zone, `None` for an unknown disk
  or out-of-range zone. Unit test.

**Per-disk hot-path counters**:
- `DiskMetrics::record_allocate` / `record_free` bump period + total
  count/bytes (`Relaxed`). `swap_periods()` zeroes period counters
  and returns the deltas; totals are monotonic across swaps. Setup:
  record 3 allocates of 4 units each, swap → assert period count=3,
  bytes=12, total count=3; record 1 more, swap → period count=1,
  total count=4. Unit test.
- Counters are incremented exactly once per allocation/free: the
  `record_allocate` call is after the Phase 1 bitmap CAS succeeds
  (in `model/alloc.rs`), so a CAS-failed retry does not bump the
  counter. Unit test (allocate with forced CAS contention → counter
  matches successful allocations, not attempts).

**QueryCapacityStats handler**:
- **Disk-group level** (`disk_group_id` only, `disk_id == 0`):
  `disk_group_id == 0` returns one `DiskGroupInfo` per owned
  disk-group, each with aggregated capacity/busy/free + member
  `DiskInfo`s carrying per-disk usage + zone counts and **no
  `zone_usages`**; a specific `disk_group_id` returns just that
  group; an unowned id → `NotFound`. Setup: 2 owned disk-groups,
  allocate blocks in one → assert the response reflects the
  allocations, the other shows busy=0, and no `ZoneUsage` entries are
  present. Integration test.
- **Disk level** (`disk_group_id` + `disk_id` set, `zone_index == 0`):
  returns the one `DiskInfo` with a `zone_usages` list of **brief**
  per-zone entries (busy/free blocks, capacity, alloc_state,
  zone_state — **no `usage_bitmap` bytes**). Setup: disk with 3
  zones, allocate in zone 0 → assert 3 brief `ZoneUsage` entries,
  none carrying `usage_bitmap`. Integration test.
- **Zone level** (`disk_group_id` + `disk_id` + `zone_index` all set):
  returns the one `ZoneUsage` with the **full `usage_bitmap`** bytes
  matching `DdbZone::usage_bits.snapshot()`. Out-of-range
  `zone_index` → `NotFound`. Integration test.
- Response is built from in-memory state (no KV reads): accuracy
  matches the bitmap — allocate N units, query at disk-group level,
  assert `busy_units == N`. Integration test.
- Read-only RPC allowed in any lifecycle phase (Syncing/Recovering/Up)
  — does not require `allows_mutating_rpcs`. Integration test.

**Recalculation path**:
- `RecalcEngine::recalc_zone` with no drift → `matches == true`,
  `drift_detected == false`, `live_busy_blocks ==
  replayed_busy_blocks`. Setup: allocate 5, free 2, recalc → assert
  replayed busy = 3 = live busy. Unit test.
- Simulated drift (manually flip a live `DdbZone` bit without a
  record) → `matches == false`, `drift_detected == true`, details
  carry live vs replayed busy counts. Unit test.
- `recalc_zone` falls back to strategy 1 on `JournalScanGcGap` and
  `SnapshotCrcFail` and still returns a `RecalcResult` (with the
  fallback noted). Unit test (inject a corrupt snapshot).
- `recalc_disk_group` / `recalc_all` aggregate per-zone results;
  `recalc_all` covers every owned disk-group. Integration test.
- `RecalcDiskUsage` admin RPC triggers recalc and returns the
  aggregated results; `disk_group_id == 0` recalc all, specific id
  recalc one. Integration test.
- v1 does not auto-correct: drift is reported only; the live bitmap
  is unchanged after recalc. Unit test (assert live `used_count`
  unchanged when drift detected).

**crow-common metrics + reporting loop**:
- Gauges for per-disk/per-disk-group capacity/busy/free + zone counts
  registered in `MetricsRegistry`; counters for allocate/free;
  histograms/summaries for the §11 latency hierarchy. Setup: register
  → assert handles are non-null. Unit test.
- Reporting loop flushes every cadence tick: allocate blocks, wait
  for one reporting tick → assert `allocate_total` counter
  incremented and `disk_busy_bytes` gauge matches the bitmap-derived
  busy. Integration test.
- `degraded` gauge reflects
  `DdbDiskGroupContainer::is_degraded`; `last_sync_age_secs` grows
  after a missed sync. Integration test.
- Latency hierarchy: an allocate RPC observes
  `allocate.rpc.latency_us` + `allocate.bitmap_scan.latency_us` +
  `allocate.kv_persist.latency_us`; assert each histogram has
  count ≥ 1 after one allocate. Integration test.

**Keepalive piggyback**:
- `heartbeat_diskdb` receives a non-empty `group_usages` slice with
  one `DiskGroupUsageSummary` per owned disk-group, carrying
  capacity/used/free bytes + disk_count + allocatable_disk_count
  derived from `aggregate_usage`. Setup: owned disk-group with
  allocations → assert the summary's `used_bytes` matches. The
  summary is recomputed each tick (not cached). Integration test.
- Zero owned disk-groups → empty `group_usages` slice (not an error).
  Integration test.

**Proto**:
- `DiskInfo` / `DiskGroupInfo` carry the new usage fields;
  `ZoneUsage` has brief count fields + optional `usage_bitmap`;
  `QueryCapacityStatsRequest` has `disk_id` + `zone_index` drill-down
  fields; `RecalcDiskUsage` RPC + request/response messages present in
  `diskdb_service.proto` / `diskdb_op.proto`. Build clean
  (`pixi run cargo build -p crow-protocol`). Integration test.

**kv-client space-usage aggregation**:
- `list_disk_group_usages()` prefix-scans `/hw/dg_usage/` and returns
  one `DiskGroupUsageSummary` per disk-group. Setup: write a few
  summaries to group 0 → assert all returned. Unit test.
- `cluster_usage()` joins the summaries with the hardware hierarchy
  and aggregates to cluster-level capacity/used/free + disk-group
  count. `rack_usage(rack_id)` / `node_usage(rack_id, node_id)` /
  `disk_group_usage(dg_id)` scope the aggregation down the hierarchy.
  Setup: 2 racks, 2 nodes each, 1 disk-group each with known usage →
  assert rack + cluster totals sum correctly. Unit test.
- Per-disk live busy/free is **not** returned by the kv-client (group
  0 has only disk-group-level usage + per-disk static capacity);
  callers use the diskdb-client for live per-disk/per-zone. Unit test
  (assert the kv-client disk-level view carries capacity only, no
  busy/free).

**diskdb-client (full client library)**:
- `DiskdbClient::allocate_blocks(request)` routes to the owning
  diskdb instance via the endpoint cache, calls `AllocateBlocks` gRPC,
  returns `AllocateResponse` with `Segment`s. Setup: running diskdb
  with a disk-group → allocate 3 blocks → assert 3 `Segment`s
  returned, each with valid `disk_id`/`zone_index`/`unit_offset`.
  E2E test (`lib/crow-diskdb-client/tests/allocate_free_e2e.rs`).
- `DiskdbClient::free_blocks(request)` frees the `Segment`s; segments
  spanning multiple disk-groups are split per-group. Setup: allocate
  3 blocks → free them → assert `freed_count == 3`. E2E test.
- `DiskdbClient::query_disk_group(dg_id)` / `query_disk(dg_id,
  disk_id)` (brief zone counts) / `query_zone(dg_id, disk_id,
  zone_index)` (full bitmap) map to the three query shapes. Setup:
  allocate blocks → `query_disk_group` → assert `busy_units`;
  `query_disk` → assert brief `ZoneUsage[]` with no bitmap;
  `query_zone` → assert `usage_bitmap` present. E2E test
  (`lib/crow-diskdb-client/tests/query_e2e.rs`).
- `DiskdbClient::get_disk_group_info(dg_id)` /
  `get_disk_info(dg_id, disk_id)` return identity + static + usage
  fields. E2E test.
- `DiskdbClient::recalc_disk_usage(request)` triggers recalc and
  returns results. E2E test (`lib/crow-diskdb-client/tests/recalc_e2e.rs`).
- **Endpoint cache**: `refresh_endpoints()` calls
  `read_all_diskdb_instances()` and populates `dg_id -> endpoint`.
  Cache miss → lazy refresh → retry. Setup: start diskdb, register
  in service registry, create `DiskdbClient` with group-0 leader
  seed → assert first `allocate_blocks` succeeds (cache populated on
  first miss). E2E test
  (`lib/crow-diskdb-client/tests/endpoint_cache_e2e.rs`).
- **Endpoint-moved retry**: when the diskdb instance moves (endpoint
  changes), the client gets `Unavailable` → refreshes cache → retries
  → succeeds. Setup: restart diskdb on a new port → assert next
  `allocate_blocks` refreshes + succeeds. E2E test.
- **v1 no proactive refresh**: the cache does not update until a
  miss/error; a moved instance is detected on the next operation, not
  proactively. Documented as an R78 use case. Unit test (assert no
  background refresh task runs in v1).

**E2E test migration**:
- New gRPC-level e2e tests live in `lib/crow-diskdb-client/tests/` and
  use `DiskdbClient` for all operations (allocate/free/query/recalc).
  Setup: `KvCluster` + in-process diskdb gRPC server + `DiskdbClient`
  seeded with group-0 leader → allocate → query → free → verify.
  E2E test.
- The existing in-process `app/crow-diskdb/tests/diskdb_e2e_test.rs`
  (calls `alloc::allocate_block` directly) stays as a server-internal
  integration test — it is not migrated (it tests library internals,
  not the gRPC client surface).

**Whole-flow space verification (single-thread + multi-thread)**:
End-to-end test verifying the full allocate → fill → free → reclaim
cycle with space statistics, covering multiple disks, multiple zones,
and zone rotation. Lives in
`app/crow-diskdb/tests/space_flow_test.rs` (in-process, no
`crow-kv-server` binary — uses a mock/in-memory `DdbKvClient` so the
test finishes in <5 s). Test dimensions chosen for UI-friendly
display and fast bitmap scan:

- **Dimensions**: 3 disks per disk-group, 4 zones per disk, 128 units
  per zone (2 × 64-bit words, word-aligned), `unit_size = 1 MB`,
  `zone_rotate_count = 2` (active set = 2 zones; rotates when both
  full → exercises rotation twice per disk). Total capacity = 3 × 4
  × 128 = 1536 units = 1536 MB (1.5 GB per disk-group). Each disk =
  512 MB; each zone = 128 MB — realistic numbers for the R77 capacity
  UI.
- **Single-thread flow**:
  1. Create `DdbDiskGroupContainer` + 1 `DdbDiskGroup` + 3 `DdbDisk`s
     with 4 zones each (128 units, word-aligned). Call
     `rebuild_active_zones(2)` on each disk.
  2. Query `DdbDiskGroup::aggregate_usage()` → assert
     `capacity_bytes == 1536 MB`, `busy_bytes == 0`,
     `free_bytes == 1536 MB`, `disk_count == 3`,
     `allocatable_disk_count == 3`.
  3. Allocate all 1536 units sequentially (`allocate_blocks(1, 1536,
     ...)`, unit_count=1, count=1536) → assert 1536 `Segment`s
     returned, no `NoSpace` error.
  4. Query `aggregate_usage()` → assert `busy_bytes == 1536 MB`,
     `free_bytes == 0`. Query each `DdbDisk::usage()` → assert
     `busy_bytes == 512 MB`, `free_bytes == 0` per disk. Query each
     `DdbZone` → assert `busy_blocks == 128`, `free_blocks == 0`,
     `derived_alloc_state == Full`.
  5. **Zone rotation check**: with `zone_rotate_count=2` and 4 zones,
     the first 256 units per disk fill zones 0+1 (active set), then
     rotation picks zones 2+3 for the next 256. Verify all 4 zones
     per disk are `Full` (all used, not just the first 2) → rotation
     worked.
  6. Free all 1536 `Segment`s sequentially (`free_blocks(&segments,
     ...)`).
  7. Query `aggregate_usage()` → assert `busy_bytes == 0`,
     `free_bytes == 1536 MB` (all space reclaimed). Query each
     `DdbZone` → assert `busy_blocks == 0`, `free_blocks == 128`,
     `derived_alloc_state == Active` (freed space reusable, not
     append-only).
  8. **Re-allocate after full free**: allocate 1536 again → assert
     all 1536 succeed (freed space is immediately reusable, no
     append-only exhaustion). Free all. Assert `free == 1536`.
  Integration test (<5 s, in-process mock KV).

- **Multi-thread flow** (concurrent allocate/free):
  1. Same setup as single-thread.
  2. Spawn 8 concurrent tokio tasks, each allocating 192 units
     (`allocate_blocks(1, 192, ...)`) → 8 × 192 = 1536 total. Each
     task collects its own `Segment`s.
  3. After all 8 complete: query `aggregate_usage()` → assert
     `busy_bytes == 1536 MB`, `free_bytes == 0` (no double-
     allocations — CAS guarantees each unit claimed exactly once).
  4. **Zone rotation under concurrency**: verify all 4 zones per disk
     are `Full` — concurrent tasks didn't all pile into one zone;
     rotation distributed across the active set + rotated when full.
  5. Spawn 8 concurrent tasks, each freeing its 192 `Segment`s
     (`free_blocks(&my_segments, ...)`).
  6. After all 8 complete: query `aggregate_usage()` → assert
     `busy_bytes == 0`, `free_bytes == 1536 MB` (concurrent free
     reclaimed all space, no lost updates).
  7. **CAS contention**: the multi-thread test exercises CAS retries
     (multiple tasks racing for the same word) — verify
     `zone.allocate.retry.cms.bit` counter incremented (from R72
     metrics) and the final count is correct (CAS retry didn't cause
     lost or duplicate allocations).
  Integration test (<5 s, in-process mock KV).

- **Zone rotation edge case**: `zone_rotate_count=1` (active set =
  1 zone) with 4 zones → each zone fills completely before rotating
  to the next. Allocate 512 units on one disk → verify zone 0 gets
  128, then rotate to zone 1 (128), then zone 2 (128), then zone 3
  (128). All 4 zones `Full`, rotation happened 3 times. Unit test.

**Quality gate**:
- `pixi run cargo fmt --all -- --check` clean.
- `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass: `pixi run test-diskdb` (diskdb server) +
  `pixi run cargo test -p crow-kv-client --all-targets` (kv-client
  usage aggregation) + `pixi run cargo test -p crow-diskdb-client
  --all-targets` (diskdb-client full client + e2e tests) for the new
  unit + integration + e2e tests.
