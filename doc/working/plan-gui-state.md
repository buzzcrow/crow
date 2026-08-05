<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R11 — GUI Internal State Display (Plan)

Design: `doc/working/design-gui-state.md`. Requirement:
`doc/backlog/R11-gui-state.md`.

## Task Breakdown

Dependency order — each task builds on the one above. Commit after each
task (or merge small adjacent tasks into one commit per the workflow's
"small, closely-related changes may be merged" rule).

### T1 — `MetricsRegistry::snapshot_struct(prefix)` (crow-common)

- [ ] Add `MetricPoint` enum/struct in `lib/crow-common/rust/src/metrics/mod.rs`
      with typed variants: `Counter { count, tps, total }`,
      `Gauge { value }`, `Bandwidth { count, avg_size, rate }`,
      `Histogram { count, p50_us, p99_us, total }`,
      `Summary { count, avg_us, max_us, total }`. Each carries its `name`.
- [ ] Add `pub fn snapshot_struct(&self, prefix: &str, window_secs: f64)
      -> Vec<MetricPoint>` that iterates the same typed collections as
      `snapshot()` but returns typed values instead of encoded strings.
      Reuse the per-type `snapshot()` methods already on each metric.
- [ ] Unit test: register one of each type, snapshot_struct, assert
      field values match `snapshot()` encoded form.

### T2 — `ElectionStateView` + `ReadStateView` in topology (crow-kv)

- [ ] Add `ElectionStateView` (mirror of `ElectionMetricsSnapshot` in
      `common/metrics.rs:146`) with `Serialize`+`Deserialize`+`ToSchema`
      to `lib/crow-kv/src/cluster/status.rs`. `Option<u64>` fields stay
      `Option<u64>`.
- [ ] Add `ReadStateView { lease_valid: u64, contiguous_applied: u64,
      safe_slot: u64 }` (Serialize/Deserialize/ToSchema) to `status.rs`.
- [ ] Add `election: Option<ElectionStateView>` to `ReplicaStatus`,
      `read_state: Option<ReadStateView>` to `GroupStatus`. Both
      `#[serde(default, skip_serializing_if = "Option::is_none")]`.
- [ ] Populate `election` in `PxLocalReplica::status()`
      (`local_replica.rs:1058`) via `election_metrics_snapshot(...)`.
      The `bulk_phase1_in_flight_slots` arg comes from the group — change
      `PxLocalReplica::status()` to take it as a parameter (or add a
      `status_with_inflight` variant). Update `PxGroup::status()`
      (`group.rs:922`) to pass `self.inflight.bulk_phase1_in_flight_slots()`
      (or whatever the existing accessor is — check `inflight`).
- [ ] Populate `read_state` in `PxGroup::status()` from
      `self.read_handles.get()` (gauges `lease_valid`,
      `contiguous_applied`, `safe_slot` — `Gauge::snapshot()` returns
      `u64`). If `read_handles` is `None`, leave `read_state` as `None`.
- [ ] Update any `status()` callers / tests that construct
      `ReplicaStatus`/`GroupStatus` directly.

### T3 — `GET /metrics` endpoint (crow-kv-server)

- [ ] Add `MetricsResponse { window_secs: f64, timestamp: String,
      metrics: Vec<MetricPointDto> }` and `MetricPointDto { name: String,
      kind: String, fields: Vec<(String, f64)> }` (or a
      `BTreeMap<String,f64>`) in `app/crow-kv-server/src/mgmt_api.rs`
      with `ToSchema`+`Serialize`. Map from `MetricPoint` (T1) to the DTO.
- [ ] Add `#[utoipa::path]` `GET /metrics` with `prefix` query param
      (default empty). Handler locks `state.metrics_registry`, calls
      `snapshot_struct(prefix, window_secs)`, maps to DTO. `window_secs`
      = configured flush interval (read from registry/runner config or
      default 5.0); label as approximate.
- [ ] Register `.route("/metrics", get(metrics))` in `router()`.
- [ ] Add to `OpenApi` derive.
- [ ] Test (`test-mgmt-api-ci`): hit `/metrics?prefix=nonexistent.` →
      empty `metrics` vec; hit `/metrics` with no prefix → non-empty for
      a server with a started store. Pin the JSON shape
      (`window_secs`, `timestamp`, `metrics[].{name,kind,fields}`).

### T4 — `ServerClient::metrics` + shared view types (crow-console-shared)

- [ ] Add `MetricPointView` / `MetricsResponseView` (mirror of the
      server DTO) to `lib/crow-console-shared/src/snapshot.rs` (the
      existing home of `/topology` mirror types).
- [ ] Add `pub async fn metrics(&self, prefix: &str) -> Result<MetricsResponseView>`
      to `ServerClient` (`clients/http.rs`) hitting `/metrics?prefix=`.
- [ ] Re-export from `lib.rs` `pub use snapshot::{...}`.

### T5 — Console proxy + aggregation routes (crow-web)

- [ ] Add `GET /api/nodes/:id/metrics` handler in
      `app/crow-web/src/physical.rs` (or a new `metrics.rs`): query
      params `store_id`, `group_id` (both optional). Build prefix
      `s.{store_id}.` or `s.{store_id}.g.{group_id}.` and proxy to the
      node's `/metrics` via `ServerClient::metrics`. Returns the
      `MetricsResponseView` verbatim.
- [ ] Add `GET /api/stores/:sid/metrics` and
      `GET /api/stores/:sid/groups/:gid/metrics` handlers in
      `app/crow-web/src/mgmt.rs`:
      - Group: resolve leader node via `monitor_cache.leader_for(sid, gid)`,
        proxy to that node's `/metrics` with prefix
        `s.{sid}.g.{gid}.`. If no leader, 502 with a message.
      - Store: for each group in the store, fetch the leader node's
        metrics for `s.{sid}.g.{gid}.`, then merge — sum counters/
        bandwidths, take max of latency p50/p99/avg/max, take max of
        gauges. Return one merged `MetricsResponseView`.
      - Add a `merge_metrics(responses: Vec<MetricsResponseView>)` helper.
- [ ] Register the three routes in `app/crow-web/src/lib.rs`.
- [ ] Test (`test-mgmt-api-ci`): proxy returns the leader node's metrics
      for a group; store aggregation sums counters across groups.

### T6 — UI: types + api.ts + Inspector Details regions

- [ ] Add `MetricPoint`, `MetricsResponse` TS types to
      `app/crow-web/ui/src/types/index.ts` matching the DTO shape.
- [ ] Add `fetchNodeMetrics(nodeId, storeId?, groupId?)`,
      `fetchStoreMetrics(storeId)`, `fetchGroupMetrics(storeId, groupId)`
      to `app/crow-web/ui/src/api.ts`.
- [ ] Add `useMetricsPoll(selection)` hook (new file
      `app/crow-web/ui/src/hooks/useMetricsPoll.ts`): 7s interval,
      visibility-paused, fetches the right route based on selection type
      + viewMode, returns `{ metrics, loading, error }`. Only fetches for
      Store/Group/Replica selections.
- [ ] Refactor `DetailsTab` in `Inspector.tsx` into three stacked
      regions with subheadings (Identity / State / Metrics):
      - **Identity**: existing `fields` list (unchanged content).
      - **State**: new fields from the enriched topology response
        (election state for group/replica, read_state for group). These
        arrive in the already-polled `stores` prop — extract from the
        same `replica`/`group` lookup already used for `crowtree_stats`.
      - **Metrics**: grouped sections by metric-name prefix
        (Operations: `kv.put.c`, `kv.get.c`, `kv.delete.c`, `kv.scan.l`;
        Latency: `kv.put.lh`, `kv.get.lh`; WAL: `wal.append.l`,
        `wal.fsync.l`, `wal.write.bw`; Consensus: `election_count`,
        `step_downs_*`; Storage: `buffer.*`, `snapshot.*`; Read path:
        `read.lease_path.c`, `read.readindex_path.c`, `read.barrier.l`).
        Each row: label + formatted primary value. Render only when
        `metrics` is non-empty.
- [ ] Vitest unit test: `useMetricsPoll` calls the right fetch for each
      selection type; `DetailsTab` renders the Metrics region when
      metrics are present.
- [ ] Playwright step: select a store, assert the Details tab shows a
      "Metrics" subheading with at least one row.

### T7 — Quality gate + full test suite

- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run test-core` (covers T1, T2, T3, T4, T5)
- [ ] `pixi run test-mgmt-api-ci` (covers T3, T5)
- [ ] `pixi run test-ui` (covers T6)
- [ ] Fix up to 3 times per the pre-commit gate.

## File List

- `lib/crow-common/rust/src/metrics/mod.rs` — T1
- `lib/crow-kv/src/cluster/status.rs` — T2
- `lib/crow-kv/src/cluster/local_replica.rs` — T2
- `lib/crow-kv/src/cluster/group.rs` — T2
- `app/crow-kv-server/src/mgmt_api.rs` — T3
- `lib/crow-console-shared/src/snapshot.rs` — T4
- `lib/crow-console-shared/src/clients/http.rs` — T4
- `lib/crow-console-shared/src/lib.rs` — T4
- `app/crow-web/src/physical.rs` (or new `metrics.rs`) — T5
- `app/crow-web/src/mgmt.rs` — T5
- `app/crow-web/src/lib.rs` — T5
- `app/crow-web/ui/src/types/index.ts` — T6
- `app/crow-web/ui/src/api.ts` — T6
- `app/crow-web/ui/src/hooks/useMetricsPoll.ts` — T6 (new)
- `app/crow-web/ui/src/shell/Inspector.tsx` — T6

## Test Checklist

- [ ] T1 unit: `snapshot_struct` matches `snapshot()` for all 5 types
- [ ] T3 mgmt-api: `/metrics` JSON shape pinned; prefix filter works
- [ ] T5 mgmt-api: group proxy returns leader metrics; store aggregation merges
- [ ] T6 Vitest: `useMetricsPoll` route selection; Details regions render
- [ ] T6 Playwright: Metrics region visible for a selected store
- [ ] T7 full suite passes

## Notes

- `window_secs` in `/metrics` is approximate (snapshot path does not
  reset window state). Exact per-window tps is the flush log's job.
- Logical-view group metrics = leader node only (confirmed decision).
- Store aggregation = sum counters/bandwidths, max latency/gauges.
- No charts/sparklines in v1 (per R11).
- Activity stays a separate Inspector tab (UI-op log, not entity state).
