<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: Protocol Type Consolidation

Design: [`design-protocol-consolidation.md`](design-protocol-consolidation.md)

---

## Task Breakdown

### Stage 1 — Move topology/status/metrics types to `crow-protocol::mgmt`

- [ ] Add `utoipa = { version = "5", optional = true }` and feature
      `schema = ["utoipa"]` to `lib/crow-protocol/Cargo.toml`
- [ ] Move `StatusLevel`, `StoreStatus`, `GroupStatus`, `ReplicaStatus`,
      `RemoteStatus`, `KvStoreStatus`, `InflightStatus`,
      `ElectionStateView`, `ReadStateView`, `CrowTreeStatsView` from
      `lib/crow-kv/src/cluster/status.rs` to
      `lib/crow-protocol/src/mgmt.rs`
- [ ] Move `MetricsSnapshot` from `lib/crow-kv/src/common/metrics.rs`
      to `lib/crow-protocol/src/mgmt.rs`
- [ ] Add `TopologyResponse`, `HealthResponse`, `MetricsResponse`,
      `MetricPoint`, `MetricField` to `lib/crow-protocol/src/mgmt.rs`
- [ ] Add `#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]`
      to all moved/new types in `mgmt.rs`
- [ ] Re-export new types from `lib/crow-protocol/src/lib.rs`
- [ ] Replace `lib/crow-kv/src/cluster/status.rs` struct/enum defs
      with `pub use crow_protocol::mgmt::{...}` re-exports
- [ ] Keep `From<ElectionMetricsSnapshot> for ElectionStateView` impl
      in `status.rs` (orphan rule OK — `ElectionMetricsSnapshot` is
      local to `crow-kv`)
- [ ] Convert `From<CrowTreeStats> for CrowTreeStatsView` to free
      function `pub fn crow_tree_stats_to_view(s: CrowTreeStats) ->
      CrowTreeStatsView` (orphan rule — both foreign)
- [ ] Update `lib/crow-kv/src/cluster/mod.rs` re-exports
- [ ] Update `lib/crow-kv/src/common/metrics.rs` —
      `LayerMetrics::snapshot()` returns
      `crow_protocol::mgmt::MetricsSnapshot`
- [ ] Add `crow-protocol = { path = "../crow-protocol", features =
      ["schema"] }` to `lib/crow-kv/Cargo.toml`
- [ ] Add `features = ["schema"]` to `crow-protocol` dep in
      `lib/crow-console-shared/Cargo.toml`
- [ ] `cargo check -p crow-protocol -p crow-kv`

### Stage 2 — Compact server `mgmt_api.rs`

- [ ] Add `use crow_protocol::mgmt::*` import
- [ ] Remove local defs: `StoreListResponse`, `StoreSummary`,
      `StoreDetail`, `AddGroupRequest`, `GroupSummary`,
      `RemoteReplicaInfo`, `RemoteListResponse`, `SystemInitRequest`,
      `SystemInitResponse`, `AddStoreRequest`, `StepDownResult`,
      `TopologyResponse`, `HealthResponse`, `ErrorResponse`,
      `MetricsResponse`, `MetricPointDto`, `MetricFieldDto`
- [ ] Keep server-local: `JoinGroupRequest`, `FlushResult`,
      `ReadinessResponse`, `OperationResponse`, `OperationTarget`,
      `AsyncOperationResponse`, `MetricsQuery`
- [ ] Update `#[openapi(components(schemas(...)))]` to reference
      imported types
- [ ] Update handler signatures if any local struct had different
      field names
- [ ] `cargo check -p crow-kv-server`

### Stage 3 — Compact console `snapshot.rs`

- [ ] Remove mirror types: `StoreView`, `GroupView`,
      `LocalReplicaView`, `RemoteReplicaView`, `KvStoreView`,
      `ElectionStateSnapshot`, `ReadStateSnapshot`,
      `CrowTreeStatsSnapshot`, `RemoteMetrics`, `HealthInfo`
- [ ] Remove `pub(crate) struct TopologyResponse`
- [ ] Add `pub use crow_protocol::mgmt::{...}` type aliases
      preserving console API names (see design §2.3)
- [ ] Keep `ClusterSnapshot`, `ServerSnapshot` (console-internal
      aggregation wrappers)
- [ ] Update `lib/crow-console-shared/src/lib.rs` re-exports
- [ ] Update `lib/crow-console-shared/src/clients/http.rs` —
      `health()` returns `HealthResponse`, `topology()` returns
      `Vec<StoreStatus>`, remove local `TopologyResponse` import
- [ ] `cargo check -p crow-console-shared`

### Stage 4 — Compact `crow-kv-client/src/topology.rs`

- [ ] Remove private `struct TopologyResponse` (lines 21–24)
- [ ] Add `use crow_protocol::mgmt::TopologyResponse`
- [ ] `cargo check -p crow-kv-client`

### Stage 5 — Redefine console ID types

- [ ] Remove `pub type RackId = String` ... `pub type ReplicaId = u64`
      from `lib/crow-console-shared/src/cluster.rs`
- [ ] Add `use crow_protocol::common_type::{RackId, NodeId, StoreId,
      GroupId, ReplicaId}` to `cluster.rs`
- [ ] Widen `Rack.id`, `Node.id`, `Node.rack_id`, `ServerProcess`,
      `NodeStore`, `NodeGroup`, `LocalReplicaInfo`,
      `RemoteReplicaInfo`, `StoreView`, `GroupView`, `ReplicaView`,
      `GroupSummary` fields from `String` to numeric aliases
- [ ] Widen `AddRackBody.id`, `CreateGroupBody.nodes`,
      `DeployResult.node_id`, `AddReplicaBody.node_id` in
      `clients/console.rs`
- [ ] Update `monitor.rs`, `topology.rs` consumers
- [ ] Update `crow-web/src/mgmt.rs`, `crow-web/src/physical.rs`
      consumers
- [ ] Update `crow-cli/src/commands/cluster.rs` consumers
- [ ] `cargo check -p crow-console-shared -p crow-web -p crow-cli`

### Stage 6 — Remove dead `model.rs`

- [ ] Delete `lib/crow-console-shared/src/model.rs`
- [ ] Remove `pub mod model;` from `lib/crow-console-shared/src/lib.rs`
- [ ] `cargo check -p crow-console-shared`

### Stage 7 — Fix crow-web `Path<String>` → `Path<u64>`

- [ ] Change `Path(node_id): Path<String>` → `Path<NodeId>` in
      `physical.rs`
- [ ] Change `Path((node_id, ...)): Path<(String, ...)>` →
      `Path<(NodeId, ...)>`
- [ ] Update `mgmt_url_for_node` to take `NodeId` instead of `&str`
- [ ] Remove `.parse().unwrap()` at `mgmt.rs:39`
- [ ] `cargo check -p crow-web`

---

## File List

**Stage 1:**
- `lib/crow-protocol/Cargo.toml` — add utoipa optional dep + schema feature
- `lib/crow-protocol/src/mgmt.rs` — add all topology/status/metrics types
- `lib/crow-protocol/src/lib.rs` — re-exports
- `lib/crow-kv/Cargo.toml` — add crow-protocol dep with schema feature
- `lib/crow-kv/src/cluster/status.rs` — replace defs with re-exports
- `lib/crow-kv/src/cluster/mod.rs` — update re-exports
- `lib/crow-kv/src/common/metrics.rs` — MetricsSnapshot moves out
- `lib/crow-console-shared/Cargo.toml` — add schema feature

**Stage 2:**
- `app/crow-kv-server/src/mgmt_api.rs` — remove duplicates, import from crow-protocol

**Stage 3:**
- `lib/crow-console-shared/src/snapshot.rs` — remove mirrors, add aliases
- `lib/crow-console-shared/src/clients/http.rs` — update return types
- `lib/crow-console-shared/src/lib.rs` — update re-exports

**Stage 4:**
- `lib/crow-kv-client/src/topology.rs` — remove private TopologyResponse

**Stage 5:**
- `lib/crow-console-shared/src/cluster.rs` — remove String aliases, widen fields
- `lib/crow-console-shared/src/clients/console.rs` — widen ID fields
- `lib/crow-console-shared/src/monitor.rs` — update consumers
- `lib/crow-console-shared/src/topology.rs` — update consumers
- `app/crow-web/src/mgmt.rs` — update consumers
- `app/crow-web/src/physical.rs` — update consumers
- `app/crow-cli/src/commands/cluster.rs` — update consumers

**Stage 6:**
- `lib/crow-console-shared/src/model.rs` — delete
- `lib/crow-console-shared/src/lib.rs` — remove module

**Stage 7:**
- `app/crow-web/src/physical.rs` — Path<u64>
- `app/crow-web/src/mgmt.rs` — mgmt_url_for_node signature

---

## Dependency Ordering

```
Stage 1 (move types to crow-protocol)
  ├── Stage 2 (server imports)     ← depends on Stage 1
  ├── Stage 3 (console aliases)    ← depends on Stage 1
  └── Stage 4 (kv-client imports)  ← depends on Stage 1

Stage 5 (redefine console IDs)     ← independent of 1–4
  └── Stage 7 (crow-web Path<u64>) ← depends on Stage 5

Stage 6 (delete model.rs)          ← independent
```

Stages 1–4 + 6 form commit 1 (type move + compaction + dead code
removal). Stages 5 + 7 form commit 2 (ID type widening).

---

## Test Checklist

After each stage:
- [ ] `pixi run cargo fmt --check`
- [ ] `pixi run cargo clippy -- -D warnings` (changed crates only)

After Stage 1–4 + 6 (commit 1):
- [ ] `pixi run clean-env && pixi run test-kv-core`
- [ ] `pixi run clean-env && pixi run test-kv-server`
- [ ] `pixi run clean-env && pixi run test-console-cli`
- [ ] `pixi run clean-env && pixi run test-console-server`

After Stage 5 + 7 (commit 2):
- [ ] Same four test commands

Final local CI (Step 9):
- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] All test commands, each separately
