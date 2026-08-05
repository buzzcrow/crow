<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R11 — GUI Internal State Display (Design)

Upstream: `doc/backlog/R11-gui-state.md` (requirement),
`doc/design/kv/design-crow-kv-observability.md` (metrics module),
`doc/design/console/design-crow-console-ui.md` (UI shell / Inspector).

## 1. Problem

The GUI already polls cluster topology and renders a Details tab for the
selected entity, but it surfaces only **cumulative** storage counters
(`crowtree_stats`: last_applied_slot, buffer-pool hits/misses, snapshot
pages) and per-peer RPC totals (`RemoteStatus.metrics`: rpc_count,
err_count, last_rtt_ms). It does **not** show the window-based runtime
metrics that R8 added: operation counts/tps, latency p50/p99, WAL flush
lag, election count, read-path split. An operator selecting a Store or
Group cannot see what that entity is *doing* in real time.

Two pieces of infrastructure are already in place but not wired to the
UI:

- `MetricsRegistry::snapshot(prefix)` (`lib/crow-common/rust/src/metrics/mod.rs:265`)
  returns all window metric values for a prefix without resetting window
  state. The observability doc explicitly notes this is "the foundation
  for future GUI integration" and "enables future `/metrics` HTTP
  endpoint and GUI integration."
- `PxLocalReplica::election_metrics_snapshot()` (`lib/crow-kv/src/cluster/local_replica.rs:902`)
  computes term, election_count, step-down counters, last_heartbeat_age,
  lease_remaining, and bulk-phase1 in-flight slots — but these are only
  written to the metrics log file; they are **not** included in
  `GroupStatus`/`ReplicaStatus`, so the topology response never carries
  them.

No `/metrics` HTTP endpoint exists on `crow-kv-server` today
(`app/crow-kv-server/src/mgmt_api.rs` routes: `/health`, `/topology`,
store/group CRUD — no metrics route).

## 2. Current Data Flow

```
crow-kv-server  /topology  →  crow-web monitor cache  →  /api/stores  →  React UI
   (per node)                 (polls each node ~5s)        (aggregated)     (Inspector)
```

- `/topology` (`mgmt_api.rs:409` `health_check` / `export_topology`)
  returns `Vec<StoreStatus>` → `GroupStatus` → `ReplicaStatus` →
  `KvStoreStatus { crowtree_stats }` + `RemoteStatus { metrics }`.
- `crow-web` polls each node's `/topology` via `ServerClient::topology`
  (`lib/crow-console-shared/src/clients/http.rs:71`) into the monitor
  cache; the logical `/api/stores` and physical `/api/nodes/:id/stores`
  routes read from that cache (`app/crow-web/src/mgmt.rs:558`,
  `physical.rs:46`).
- The Inspector Details tab (`app/crow-web/ui/src/shell/Inspector.tsx:155`)
  builds a key/value list from the selected entity; for a logical
  Replica it digs `crowtree_stats` out of the enriched `StoreView`. There
  is no metrics tab — only Details and Activity.

## 3. What to Surface

Split into two tiers by weight and cadence:

### 3.1 Lightweight internal state (belongs with status, every poll)

Election/consensus state — a handful of numbers, cheap to compute, and
genuinely "internal state" (the R11 title). Add to the topology response:

- `current_term`, `election_count`, `step_downs_*` (3 counters)
- `last_heartbeat_age_ms`, `lease_remaining_ms` (leader only)
- `bulk_phase1_in_flight_slots`
- Read-path gauges already bridged on demand: `read.lease_valid`,
  `read.contiguous_applied`, `read.safe_slot` (from `ReadRegistryHandles`)

These are already computed by `election_metrics_snapshot()` and the read
gauges; exposing them is a struct-field addition, not a new poll.

### 3.2 Window metrics (heavier, per-entity on demand)

The R8 registry metrics — op counts/tps, latency p50/p99, WAL flush
lag/throughput, bandwidth, read-path counters. These are the
"op counts, latency, WAL stats" the R11 acceptance names. Per-store/group
the set is ~20–40 named metrics; embedding all of them in every 5s
whole-cluster topology poll is wasteful when no inspector is open.

## 4. Chosen Approach: Dedicated `/metrics` endpoint + console proxy

Add `GET /metrics?prefix=...` on `crow-kv-server` returning a structured
JSON snapshot of registry metrics for the prefix. Add a console proxy
route that the Inspector polls only for the selected entity on a 5–10s
cadence. Keep `/topology` lean (status only) but add the §3.1 lightweight
election state to it.

- Matches the observability doc's planned `/metrics` endpoint.
- Per-entity polling keeps it lightweight (R11: "no historical charts in
  v1, just current snapshot values").
- Decouples metrics cadence from topology cadence.
- Structured JSON avoids a string parser in the UI.
- `/metrics` is reusable by non-UI consumers (scripts, scraping).
- Cost: one new server route, one new console proxy route, one new UI
  polling hook.

Rejected alternatives: embedding all metrics in `/topology` (bloats the
5s whole-cluster poll with ~20–40 strings per group even when no
inspector is open, and the `Vec<(String,String)>` snapshot shape is not
UI-renderable without a parser); an opt-in `/topology?metrics=1` flag
(one endpoint carrying two unrelated shapes keyed by a flag is awkward
to schema/cache/type, and complicates the console's request dedup).

## 5. Proposed Approach (B + §3.1 state in topology)

### 5.1 Server: structured `/metrics` endpoint

- New route `GET /metrics` in `app/crow-kv-server/src/mgmt_api.rs`.
- Query param `prefix` (default empty = all). Returns a structured
  response, not the `Vec<(String,String)>` log shape:

  ```
  MetricsResponse { window_secs: f64, timestamp: String,
                    metrics: Vec<MetricPoint> }
  MetricPoint { name: String, kind: "counter"|"gauge"|"bandwidth"
                          |"histogram"|"summary", fields: Map<String,f64> }
  ```

  `fields` carries type-specific values (counter: `count`,`tps`,`total`;
  histogram: `count`,`p50_us`,`p99_us`,`total`; summary: `count`,`avg_us`,
  `max_us`; bandwidth: `count`,`avg_size`,`rate`; gauge: `value`). The UI
  renders by `kind` without parsing strings.
- Implementation calls `state.metrics_registry.lock().snapshot(prefix)`
  and maps the `(name, encoded_string)` pairs into `MetricPoint`s. To
  avoid re-implementing the decoder, add a `registry.snapshot_struct(prefix)`
  method returning typed snapshots directly (cleaner than parsing the
  log-format strings). This is a small addition to
  `lib/crow-common/rust/src/metrics/mod.rs`.
- `window_secs`: the registry does not track last-flush elapsed on the
  snapshot path (snapshot does not reset). Report the configured flush
  interval as `window_secs` and label the response "window (approx)".
  Exact per-window tps is the flush log's job; the GUI shows approximate
  rates. (Alternative: track `last_window_secs` on the runner and expose
  it — noted as a minor follow-up.)

### 5.2 Server: election state in topology

- Add `election: Option<ElectionStateView>` to `ReplicaStatus`
  (`lib/crow-kv/src/cluster/status.rs:86`) populated in
  `PxLocalReplica::status()` (`local_replica.rs:1058`) from
  `election_metrics_snapshot(bulk_phase1_in_flight_slots)`. The
  `bulk_phase1_in_flight_slots` arg is already available on the group;
  pass it through `PxGroup::status()` (`group.rs:922`).
- `ElectionStateView` mirrors `ElectionMetricsSnapshot` fields
  (`common/metrics.rs:146`) as `Serialize`+`ToSchema`.
- Read-path gauges (`lease_valid`, `contiguous_applied`, `safe_slot`) are
  bridged on demand in `resolve_read_point`; expose them here too as a
  small `read_state: Option<ReadStateView>` on `GroupStatus` (cheap
  atomic reads, same pattern as `inflight`).

### 5.3 Console: proxy + aggregation

- New `ServerClient::metrics(prefix)` method
  (`lib/crow-console-shared/src/clients/http.rs`) hitting `/metrics`.
- New console routes:
  - `GET /api/nodes/:id/metrics?store_id=&group_id=` — single-node
    (physical view); proxies to that node's `/metrics` with prefix
    `s.{store_id}.g.{group_id}.` (or `s.{store_id}.` for a store).
  - `GET /api/stores/:sid/metrics` and
    `GET /api/stores/:sid/groups/:gid/metrics` — logical view; the
    console resolves which nodes host the store/group, calls each
    node's `/metrics`, and merges. For a **group**, report the **leader
    node's** metrics (puts/gets/latency are leader-served; follower
    reads are a secondary signal). For a **store**, aggregate across
    its groups (sum counters, take max latency).
- Aggregation lives in `app/crow-web/src/mgmt.rs` alongside the existing
  logical-tree aggregation. The monitor cache already knows node→store
  hosting, so leader resolution reuses `monitor_cache.leader_for`.

### 5.4 UI: merge metrics into the Details tab

No new tab. The Inspector keeps its two existing tabs (Details, Activity).
Metrics are rendered as additional grouped sections **within the Details
tab**, so all entity info — identity fields, election/read state, and
window metrics — is visible in one scroll without switching tabs.

- The Details tab (`app/crow-web/ui/src/shell/Inspector.tsx`,
  `DetailsTab`) becomes a single scrollable view with three stacked
  regions, separated by subheadings:
  1. **Identity** — the existing key/value fields (Type, ID, ports,
     parents, engine healthy, crowtree_stats). Unchanged from today.
  2. **State** — the §3.1 election/read fields (Term, Elections,
     Last heartbeat age, Lease remaining, Step-downs, lease_valid /
     contiguous_applied / safe_slot). Arrives via the existing topology
     poll — no new fetch.
  3. **Metrics** — grouped sections (Operations, Latency, WAL, Consensus,
     Storage, Read path) by metric-name prefix, not a flat 40-row list.
     Each row: label + primary value (e.g. `puts 1.2k (240 tps)`,
     `get p50 0.8ms / p99 4.2ms`, `wal append avg 0.3ms / max 2.1ms`).
- The Metrics region is shown only for `Store`, `Group`, and (physical)
  `Replica` selections; hidden for Rack/Node/Server (no metrics to show).
- A `useMetricsPoll(selection)` hook fetches the matching console route
  on a 7s interval while the Details tab is visible and the entity is
  selected; pauses when the browser tab is hidden (reuse the existing
  `document.visibilityState` pause pattern from the topology poll).
- Activity remains a separate tab — it is a UI-operation log, not entity
  state, and doesn't belong in the same scroll.
- No charts, no history, no sparklines in v1 (per R11). Just current
  snapshot values that refresh on poll.

## 6. Files Touched

- `lib/crow-common/rust/src/metrics/mod.rs` — add `snapshot_struct(prefix)`
  returning typed metric points.
- `lib/crow-kv/src/cluster/status.rs` — `ElectionStateView`,
  `ReadStateView`, add fields to `ReplicaStatus`/`GroupStatus`.
- `lib/crow-kv/src/cluster/local_replica.rs` / `group.rs` — populate the
  new status fields in `status()`.
- `app/crow-kv-server/src/mgmt_api.rs` — `GET /metrics` route + handler
  + OpenAPI spec entry; structured `MetricsResponse` types.
- `lib/crow-console-shared/src/clients/http.rs` — `ServerClient::metrics`.
- `lib/crow-console-shared/src/cluster.rs` (or a new `metrics.rs`) —
  shared `MetricPoint`/`MetricsResponse` view types for the UI.
- `app/crow-web/src/mgmt.rs` (and/or a new `metrics.rs`) — console proxy
  + logical aggregation routes.
- `app/crow-web/src/lib.rs` — register the new routes.
- `app/crow-web/ui/src/api.ts` — `fetchNodeMetrics` /
  `fetchStoreMetrics` / `fetchGroupMetrics`.
- `app/crow-web/ui/src/shell/Inspector.tsx` — metrics sections in the
  Details tab + poll hook.
- `app/crow-web/ui/src/types/index.ts` — metric view types.

## 7. Acceptance

- Select a Store in the UI → Inspector Details tab shows op counts
  (puts/gets/deletes/scans) with tps, get/put latency p50/p99, WAL
  append/fsync avg/max, bandwidth in the Metrics region; values refresh
  every ~7s.
- Select a Group → same metrics scoped to that group (leader node's
  values for the logical view).
- Select a Replica (physical view) → that node's metrics for the
  replica's store/group.
- Details tab for a group/replica shows Term, Elections, Last heartbeat
  age, Lease remaining (leader), Step-down counts, read-path state
  (lease_valid / contiguous_applied / safe_slot) in the State region —
  updating with the existing topology refresh.
- All entity info (identity, state, metrics) visible in one scroll in
  the Details tab — no tab switching needed to see metrics.
- Polling pauses when the browser tab is hidden; resumes on focus.
- `/metrics` endpoint on `crow-kv-server` returns structured JSON for an
  arbitrary prefix; OpenAPI documents it.
- No regression in `/topology` response size for the default (no-metrics)
  path — only the small §3.1 election/read-state fields are added.
- `pixi run test-mgmt-api-ci` and `pixi run test-ui` pass; a Vitest unit
  test pins the `/metrics` JSON shape; a Playwright step asserts the
  Details tab's Metrics region renders values for a selected store.

## 8. Confirmed Decisions

- **Metrics transport**: dedicated `GET /metrics` endpoint (Alternative B)
  for window metrics, polled per-entity by the Inspector on ~7s. The
  lightweight election/read state (§3.1) still extends `/topology`.
  Rationale: the observability doc already plans a `/metrics` endpoint;
  embedding ~20–40 metrics per group in every 5s whole-cluster poll is
  wasteful; per-entity polling is lighter and matches R11's "keep it
  lightweight" guidance.
- **Logical-view aggregation**: report the **leader node's** metrics for a
  Group (puts/gets/latency are leader-served; follower reads are a
  secondary signal). For a Store, aggregate across its groups (sum
  counters, take max latency). Rationale: simplest correct signal;
  summing across all hosting nodes double-counts forwarded reads and
  blurs latency.
