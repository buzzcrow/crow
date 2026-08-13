<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Space Metrics + Query API (R74) Plan

Design draft: [`doc/working/design-crow-diskdb-space-metrics.md`](design-crow-diskdb-space-metrics.md).
Backlog doc: [`doc/backlog/R74-diskdb-space-metrics-query.md`](../backlog/R74-diskdb-space-metrics-query.md).

Goal: implement the diskdb space-metrics component — usage accessors,
`QueryCapacityStats` handler, per-disk counters, keepalive piggyback, recalc
verifier, §11 metric set + reporting loop, proto extensions, kv-client
space-usage aggregation, and the full `crow-diskdb-client` library.

Tasks are dependency-ordered. Proto first (everything depends on the new
types), then model accessors, then handlers/clients that consume them.

## Proto + types

- [ ] **Extend `diskdb_type.proto`**: add `ZoneUsage` message (brief counts +
  optional `usage_bitmap`); extend `DiskInfo` with `busy_units`, `free_units`,
  `capacity_bytes`, `busy_bytes`, `free_bytes`, `active_zone_count`,
  `repeated ZoneUsage zone_usages`; extend `DiskGroupInfo` with
  `capacity_bytes`, `busy_bytes`, `free_bytes`, `allocatable_disk_count`.
  Files: `lib/crow-protocol/src/proto/diskdb_type.proto`.
- [ ] **Extend `diskdb_op.proto`**: add `optional DiskId disk_id` +
  `optional uint32 zone_index` to `QueryCapacityStatsRequest`; add
  `RecalcDiskUsageRequest`/`RecalcDiskUsageResponse` +
  `DiskGroupRecalcResult`/`ZoneRecalcResult` messages. Files:
  `lib/crow-protocol/src/proto/diskdb_op.proto`.
- [ ] **Extend `diskdb_service.proto`**: add `RecalcDiskUsage` RPC. Files:
  `lib/crow-protocol/src/proto/diskdb_service.proto`.
- [ ] **Build proto crate clean**: `pixi run cargo build -p crow-protocol`.
  Files: `lib/crow-protocol/`.

## Model: usage accessors + aggregation

- [ ] **Per-zone accessors**: add `busy_blocks`/`free_blocks`/`busy_bytes`/
  `free_bytes`/`capacity_bytes`/`usage_ratio` to `DdbZone`. Add `ZoneUsage`
  struct. Files: `app/crow-diskdb/src/model/zone.rs`.
- [ ] **Per-disk aggregation**: add `DiskUsage` + `DdbDisk::usage(unit_size_bytes)`.
  Add `metrics: Option<Arc<DiskMetrics>>` field (forward-declared; filled when
  `DiskMetrics` exists). Files: `app/crow-diskdb/src/model/disk.rs`.
- [ ] **Per-disk-group aggregation**: add `DiskGroupUsage` +
  `DdbDiskGroup::aggregate_usage` + `DdbDiskGroup::zone_usage`. Files:
  `app/crow-diskdb/src/model/disk_group.rs`.
- [ ] **Re-export usage structs** from `model.rs`. Files:
  `app/crow-diskdb/src/model.rs`.

## Metrics: per-disk counters + §11 set + recalc + reporting

- [ ] **Convert `metrics.rs` → `metrics/mod.rs`**: move existing `DiskdbMetrics`
  into `metrics/mod.rs`; extend with the §11 gauge/counter/histogram/summary
  handles + `register`/`disabled`. Files: `app/crow-diskdb/src/metrics.rs` →
  `app/crow-diskdb/src/metrics/mod.rs`, `app/crow-diskdb/src/lib.rs`.
- [ ] **`DiskMetrics` (per-disk counters)**: `metrics/disk.rs` —
  `DiskMetrics` (period/total atomics) + `record_allocate`/`record_free`/
  `swap_periods`/`PeriodSnapshot`. Files:
  `app/crow-diskdb/src/metrics/disk.rs`.
- [ ] **`RecalcEngine`**: `metrics/recalc.rs` — `RecalcEngine` +
  `recalc_zone`/`recalc_disk_group`/`recalc_all` + `RecalcResult`/
  `DiskGroupRecalcResult`/`FallbackReason`. Reuses `recover_zone_inner` +
  `rebuild_zone_bitmap_full_scan`. Files:
  `app/crow-diskdb/src/metrics/recalc.rs`.
- [ ] **`ReportingTask`**: `metrics/reporting.rs` — `BackgroundTask` that
  swaps periods, recomputes gauges, updates degraded/sync_age each tick.
  Files: `app/crow-diskdb/src/metrics/reporting.rs`.

## Allocate/free instrumentation

- [ ] **Wire `DiskMetrics` into alloc paths**: `model/alloc.rs` — call
  `record_allocate`/`record_free` after Phase 1 CAS succeeds; add latency
  observations (bitmap_scan, kv_persist) via optional `&DiskdbMetrics`.
  Attach `DiskMetrics` in `disk_add_init` + `recover_disk_group`. Files:
  `app/crow-diskdb/src/model/alloc.rs`,
  `app/crow-diskdb/src/liveness/keepalive.rs`,
  `app/crow-diskdb/src/recovery.rs`.

## Service handlers

- [ ] **`QueryCapacityStats` handler**: implement the three query shapes
  (disk-group / disk / zone level) in `service/diskdb_service.rs`; read-only,
  no lifecycle gate. Extend `GetDiskGroupInfo`/`GetDiskInfo` with usage fields.
  Files: `app/crow-diskdb/src/service/diskdb_service.rs`.
- [ ] **`RecalcDiskUsage` handler**: delegate to `RecalcEngine`; mutating RPC
  (requires `allows_mutating_rpcs`). Add `RecalcEngine` + `DiskdbMetrics`
  fields to `DiskdbService`. Files:
  `app/crow-diskdb/src/service/diskdb_service.rs`.

## Keepalive piggyback + sync metrics

- [ ] **Usage piggyback**: `keepalive.rs` `tick` — compute
  `DiskGroupUsageSummary[]` from `aggregate_usage`, pass to
  `heartbeat_diskdb` with the real endpoint + owned_dg_ids. Add sync
  latency/success/failure observations. Files:
  `app/crow-diskdb/src/liveness/keepalive.rs`.

## Config + server wiring

- [ ] **`ReportingConfig`**: add to `DdbConfig` (default 10s) + `validate`.
  Files: `app/crow-diskdb/src/ddb_config.rs`.
- [ ] **Wire `ReportingTask` + `RecalcEngine` + `DiskdbMetrics` in `main.rs`**:
  register `ReportingTask` in `BgRunner`; pass `RecalcEngine` +
  `DiskdbMetrics` into `DiskdbService`; pass endpoint to keepalive. Files:
  `app/crow-diskdb/src/main.rs`.

## kv-client space-usage aggregation

- [ ] **`SpaceUsageClient`**: `lib/crow-kv-client/src/space_usage.rs` —
  `list_disk_group_usages`/`cluster_usage`/`rack_usage`/`node_usage`/
  `disk_group_usage` + `ClusterUsage`/`RackUsage`/`NodeUsage` structs.
  Re-export from `lib.rs`. Files: `lib/crow-kv-client/src/space_usage.rs`,
  `lib/crow-kv-client/src/lib.rs`.

## diskdb-client full library

- [ ] **`DiskdbClient` core + endpoint cache**: `lib/crow-diskdb-client/src/client.rs`
  — `DiskdbClient`, `refresh_endpoints`/`refresh_for`, `DashMap` cache +
  channel pool, `RetryConfig`. Files: `lib/crow-diskdb-client/src/client.rs`,
  `lib/crow-diskdb-client/src/lib.rs`.
- [ ] **`DiskdbClient` operations**: `allocate_blocks`/`free_blocks` (per-group
  split + disk_id→dg_id reverse routing)/`query_*`/`get_*_info`/
  `recalc_disk_usage`. Extend `DiskdbClientError`. Files:
  `lib/crow-diskdb-client/src/client.rs`,
  `lib/crow-diskdb-client/src/lib.rs`.

## Tests

- [ ] **UT: zone accessors** (`zone.rs` tests).
- [ ] **UT: disk + disk-group aggregation** (`disk.rs`, `disk_group.rs` tests).
- [ ] **UT: `DiskMetrics` period/total + `swap_periods`** (`metrics/disk.rs` tests).
- [ ] **UT: `RecalcEngine` matches/drift/fallback** (`metrics/recalc.rs` tests).
- [ ] **UT: `SpaceUsageClient` aggregation** (`crow-kv-client` tests).
- [ ] **UT: `DiskdbClient` no proactive refresh** (`crow-diskdb-client` tests).
- [ ] **Integration: `QueryCapacityStats` three shapes + read-only in Recovering**
  (`app/crow-diskdb/tests/`).
- [ ] **Integration: keepalive piggyback non-empty** (`app/crow-diskdb/tests/`).
- [ ] **Integration: whole-flow space verification** (`app/crow-diskdb/tests/space_flow_test.rs`).
- [ ] **E2E: `allocate_free_e2e.rs`** (`lib/crow-diskdb-client/tests/`).
- [ ] **E2E: `query_e2e.rs`** (`lib/crow-diskdb-client/tests/`).
- [ ] **E2E: `endpoint_cache_e2e.rs`** (`lib/crow-diskdb-client/tests/`).
- [ ] **E2E: `recalc_e2e.rs`** (`lib/crow-diskdb-client/tests/`).

## File list

- `lib/crow-protocol/src/proto/diskdb_type.proto` — `ZoneUsage`, extend `DiskInfo`/`DiskGroupInfo`.
- `lib/crow-protocol/src/proto/diskdb_op.proto` — extend `QueryCapacityStatsRequest`, `RecalcDiskUsage*`.
- `lib/crow-protocol/src/proto/diskdb_service.proto` — `RecalcDiskUsage` RPC.
- `app/crow-diskdb/src/model/zone.rs` — accessors + `ZoneUsage`.
- `app/crow-diskdb/src/model/disk.rs` — `DiskUsage` + `DdbDisk::usage` + `metrics` field.
- `app/crow-diskdb/src/model/disk_group.rs` — `DiskGroupUsage` + `aggregate_usage` + `zone_usage`.
- `app/crow-diskdb/src/model.rs` — re-exports.
- `app/crow-diskdb/src/metrics.rs` → `metrics/mod.rs` — extended `DiskdbMetrics`.
- `app/crow-diskdb/src/metrics/disk.rs` — `DiskMetrics`.
- `app/crow-diskdb/src/metrics/recalc.rs` — `RecalcEngine`.
- `app/crow-diskdb/src/metrics/reporting.rs` — `ReportingTask`.
- `app/crow-diskdb/src/model/alloc.rs` — `DiskMetrics` record + latency.
- `app/crow-diskdb/src/service/diskdb_service.rs` — `QueryCapacityStats` + `RecalcDiskUsage` + usage fields.
- `app/crow-diskdb/src/liveness/keepalive.rs` — piggyback + sync metrics.
- `app/crow-diskdb/src/recovery.rs` — attach `DiskMetrics` to recovered disks.
- `app/crow-diskdb/src/ddb_config.rs` — `ReportingConfig`.
- `app/crow-diskdb/src/lib.rs` — `pub mod metrics` (dir).
- `app/crow-diskdb/src/main.rs` — wire `ReportingTask` + `RecalcEngine` + `DiskdbMetrics`.
- `lib/crow-kv-client/src/space_usage.rs` — `SpaceUsageClient`.
- `lib/crow-kv-client/src/lib.rs` — re-export.
- `lib/crow-diskdb-client/src/lib.rs` — extended error.
- `lib/crow-diskdb-client/src/client.rs` — `DiskdbClient`.
- `app/crow-diskdb/tests/space_flow_test.rs` — whole-flow test.
- `lib/crow-diskdb-client/tests/allocate_free_e2e.rs`.
- `lib/crow-diskdb-client/tests/query_e2e.rs`.
- `lib/crow-diskdb-client/tests/endpoint_cache_e2e.rs`.
- `lib/crow-diskdb-client/tests/recalc_e2e.rs`.

## Test checklist

Run after implementation (Step 6 — affected subset; Step 9 — all):
- `pixi run cargo test -p crow-protocol` (proto build).
- `pixi run test-diskdb` (diskdb server: accessors, aggregation, handlers, recalc, space-flow, piggyback).
- `pixi run cargo test -p crow-kv-client --all-targets` (`SpaceUsageClient`).
- `pixi run cargo test -p crow-diskdb-client --all-targets` (client + e2e).
- `pixi run cargo fmt --all -- --check` + `pixi run cargo clippy --all-targets -- -D warnings`.
