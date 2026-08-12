<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: Protocol Type Consolidation

Depends on: [`design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md) §2.4, §2.5
Satisfies: §2.4 ("All cross-component protocol types live in
`crow-protocol`"), §2.5 ("ID aliases defined once in `crow-protocol`")

---

## 1. Problem

`design-crow-kv-group0.md` §2.4 mandates that `crow-protocol` is the
single home for all cross-component protocol types — data structures,
RPC messages, key types, and HTTP mgmt API contracts. §2.5 mandates
that ID aliases (`RackId`, `NodeId`, etc.) are defined once in
`crow-protocol` and imported by every other crate — no per-crate
redefinition. Three categories of violation exist in the codebase
today:

### 1.1 Triple-duplicated topology/status type hierarchy

The `/topology`, `/health`, and `/metrics` response types exist in
three independent copies:

- **Copy 1 (canonical):** `lib/crow-kv/src/cluster/status.rs` —
  `StoreStatus`, `GroupStatus`, `ReplicaStatus`, `RemoteStatus`,
  `StatusLevel`, `KvStoreStatus`, `InflightStatus`,
  `ElectionStateView`, `ReadStateView`, `CrowTreeStatsView`.
  `MetricsSnapshot` lives in `lib/crow-kv/src/common/metrics.rs`.
  These are the structs the server serializes.
- **Copy 2 (console mirror):** `lib/crow-console-shared/src/snapshot.rs`
  — `StoreView`, `GroupView`, `LocalReplicaView`, `RemoteReplicaView`,
  `KvStoreView`, `ElectionStateSnapshot`, `ReadStateSnapshot`,
  `CrowTreeStatsSnapshot`, `RemoteMetrics`, `HealthInfo`. A
  field-subset mirror of Copy 1, kept "so both the HTTP client and the
  public API surface use the same shape" (snapshot.rs:7). The mirror
  silently drops `status`/`messages` fields that Copy 1 carries.
- **Copy 3 (client private):** `lib/crow-kv-client/src/topology.rs`
  — a private `TopologyResponse` struct (lines 21–24) used only for
  deserialization, duplicating the server's `TopologyResponse`.

### 1.2 Triple-duplicated `TopologyResponse`

- `app/crow-kv-server/src/mgmt_api.rs:231` — server response struct.
- `lib/crow-kv-client/src/topology.rs:21` — client deserialization
  struct.
- `lib/crow-console-shared/src/snapshot.rs:182` — console
  deserialization struct (`pub(crate)`).

§2.4 explicitly names `TopologyResponse` as a type that should live in
`crow-protocol::mgmt`.

### 1.3 Server duplicates all mgmt DTOs

`app/crow-kv-server/src/mgmt_api.rs` defines local copies of
`StoreListResponse`, `StoreSummary`, `StoreDetail`, `AddGroupRequest`,
`GroupSummary`, `RemoteReplicaInfo`, `RemoteListResponse`,
`SystemInitRequest`, `SystemInitResponse`, `AddStoreRequest`,
`StepDownResult` — all of which already exist in
`lib/crow-protocol/src/mgmt.rs`. The server does not import
`crow_protocol::mgmt` at all. The console (`crow-console-shared/
src/mgmt.rs`) correctly re-exports `crow_protocol::mgmt::*`, but the
server (the producer side) ignores the canonical definitions — two
struct definitions for every wire type.

### 1.4 Console redefines `RackId`/`NodeId` as `String`

`lib/crow-console-shared/src/cluster.rs:23-24`:
```
pub type RackId = String;
pub type NodeId = String;
```

§2.5: "`RackId` — `u64`", "`NodeId` — `u64`", "ID aliases are defined
**once** in `crow-protocol` ... no per-crate redefinition." The same
crate's `config.rs:12` correctly imports `RackId`/`NodeId` from
`crow_protocol::common_type` (u64), but `cluster.rs` redefines them
as `String`. The entire console cluster model (`Rack`, `Node`,
`NodeStore`, `NodeGroup`, `StoreView`, `GroupView`, `ReplicaView`,
etc.) flows through the `String` aliases. `StoreId`/`GroupId`/
`ReplicaId` are also redefined (same u64 value, but still a forbidden
per-crate redefinition).

### 1.5 Console HTTP API DTOs use `String` for numeric IDs

`lib/crow-console-shared/src/clients/console.rs`:
- `AddRackBody.id: String` (line 50)
- `CreateGroupBody.nodes: Vec<String>` (line 45)
- `DeployResult.node_id: String` (line 98)
- `AddReplicaBody.node_id: String` (line 111)

§2.5: "No crate uses `String` for an ID that is fundamentally
numeric."

### 1.6 Dead `model.rs` with `String` IDs

`lib/crow-console-shared/src/model.rs` defines `Rack`, `Node`,
`ServerInstance` with `String` IDs. No imports of these types exist
anywhere in the codebase — dead placeholder code, but a leftover that
contradicts §2.5.

### 1.7 crow-web HTTP handlers use `Path<String>` for `node_id`

`app/crow-web/src/physical.rs` uses `Path<String>` for `node_id` path
params, then `mgmt_url_for_node` parses to `NodeId` internally
(`node_id.parse().unwrap()` at `mgmt.rs:39`). Should use `Path<u64>`
directly at the axum boundary.

---

## 2. Proposed Approach

### 2.1 Move topology/status/metrics types to `crow-protocol::mgmt`

Move all `/topology`, `/health`, `/metrics` wire types from
`lib/crow-kv/src/cluster/status.rs` and `lib/crow-kv/src/common/
metrics.rs` to `lib/crow-protocol/src/mgmt.rs`. Add `TopologyResponse`,
`HealthResponse`, `MetricsResponse`, `MetricPoint`, `MetricField` as
new structs in `crow-protocol::mgmt`.

`crow-protocol` gains an optional `utoipa` dependency (feature-gated
as `schema`) so `ToSchema` derives are available for the server's
OpenAPI spec without forcing non-server consumers to pull in utoipa.

`crow-kv/src/cluster/status.rs` becomes a thin re-export module:
`pub use crow_protocol::mgmt::{...}`. The `From<ElectionMetricsSnapshot>
for ElectionStateView` impl stays in `crow-kv` (orphan rule permits it
— `ElectionMetricsSnapshot` is local). The `From<CrowTreeStats> for
CrowTreeStatsView` impl becomes a free function
`crow_tree_stats_to_view()` because both `CrowTreeStats` (from
`crow-tree-ffi`) and `CrowTreeStatsView` (from `crow-protocol`) would
be foreign — the trait impl violates the orphan rule.

### 2.2 Compact server `mgmt_api.rs`

Remove all DTO struct definitions that duplicate `crow-protocol::mgmt`.
Import `use crow_protocol::mgmt::*`. Keep server-local types that have
no external caller today: `JoinGroupRequest`, `FlushResult`,
`ReadinessResponse`, `OperationResponse`, `OperationTarget`,
`AsyncOperationResponse`, `MetricsQuery`.

### 2.3 Compact console `snapshot.rs`

Remove the mirror types (`StoreView`, `GroupView`, etc.) and the
private `TopologyResponse`. Replace with type aliases re-exporting
from `crow-protocol::mgmt`:

```rust
pub use crow_protocol::mgmt::{
    StoreStatus as StoreView,
    GroupStatus as GroupView,
    ReplicaStatus as LocalReplicaView,
    RemoteStatus as RemoteReplicaView,
    KvStoreStatus as KvStoreView,
    ElectionStateView as ElectionStateSnapshot,
    ReadStateView as ReadStateSnapshot,
    CrowTreeStatsView as CrowTreeStatsSnapshot,
    MetricsSnapshot as RemoteMetrics,
    HealthResponse as HealthInfo,
    TopologyResponse,
};
```

The aliases preserve the console's public API names so downstream code
(`monitor.rs`, `topology.rs`, `crow-web`, `crow-cli`) doesn't need
massive rename churn. The console gains the extra `status`/`messages`
fields it was previously dropping — serde populates them, the console
ignores them when rendering.

Keep `ClusterSnapshot`, `ServerSnapshot` in `snapshot.rs` — these are
console-internal aggregation wrappers (one entry per polled server,
including failed polls), not wire types.

### 2.4 Compact `crow-kv-client/src/topology.rs`

Remove the private `TopologyResponse` struct. Import
`crow_protocol::mgmt::TopologyResponse`. The `merge()` logic uses
`StoreStatus` which is already imported via `crow_kv::cluster::status`
(now a re-export from `crow_protocol::mgmt`).

### 2.5 Redefine console ID types

Remove the `pub type` aliases from `cluster.rs`. Import
`RackId`/`NodeId`/`StoreId`/`GroupId`/`ReplicaId` from
`crow_protocol::common_type`. Widen all struct fields in `cluster.rs`
and `clients/console.rs` from `String` to the numeric `u64` aliases.

`error.rs`'s `UpstreamRpc.node_id` stays `String` — it holds a URL,
not a numeric ID. `lifecycle.rs`'s `server_id` stays `String` — §2.5
explicitly allows the console-side `ServerEntry.id` label to remain
`String` (it is a console handle, not a numeric cluster ID).

### 2.6 Remove dead `model.rs`

Delete `lib/crow-console-shared/src/model.rs` and remove `pub mod
model;` from `lib.rs`.

### 2.7 Fix crow-web `Path<String>` → `Path<u64>`

Change `node_id` path params in `physical.rs` from `Path<String>` to
`Path<NodeId>` (axum deserializes `u64` from path segments). Remove
the `.parse().unwrap()` in `mgmt.rs:39`.

---

## 3. Alternatives Considered

### 3.1 Keep status types in `crow-kv`, only move `TopologyResponse`

Rejected. §2.4 says "all cross-component protocol types" live in
`crow-protocol`. `StoreStatus`/`GroupStatus`/etc. are cross-component
— the console imports them (via the mirror in `snapshot.rs`), and
`crow-kv-client` imports `StoreStatus` from `crow-kv`. Leaving them in
`crow-kv` means `crow-protocol` is not the single home, and the
mirror duplication persists.

### 3.2 Merge console mirror types into `crow-kv` status types (no move)

Rejected. `crow-kv` is the KV engine crate; it should not be a
dependency of `crow-console-shared` for wire types. Today
`crow-console-shared` already depends on `crow-kv` indirectly via
`crow-kv-client`, but the design direction (§2.4) is to centralize
protocol types in `crow-protocol`, not to make `crow-kv` the protocol
crate. `crow-protocol` has no heavy dependencies (no tokio runtime,
no engine code) — it is the right home.

### 3.3 Make `RackId`/`NodeId` newtypes instead of type aliases

Rejected. §2.5 explicitly says "The simple integer IDs ... are type
aliases (`pub type RackId = u64;`) in `crow-protocol`, not newtypes."
Newtypes would add conversion friction at every boundary without
runtime benefit.

### 3.4 Keep console `RackId = String`, widen only at the boundary

Rejected. §2.5 says "no per-crate redefinition" and "Console config
schema changes to `u64`". The `config.rs` module already uses `u64`
aliases from `crow-protocol`; `cluster.rs` using `String` creates a
split-brain where the same crate has two different `RackId` types.
The `server_for_node(node_id: NodeId)` call at `mgmt.rs:39` already
parses `String` → `NodeId`, proving the boundary already needs
conversion — moving to `u64` everywhere removes the parse, it doesn't
add one.

---

## 4. Acceptance Criteria

- `crow-protocol::mgmt` contains `TopologyResponse`, `HealthResponse`,
  `MetricsResponse`, `StoreStatus`, `GroupStatus`, `ReplicaStatus`,
  `RemoteStatus`, `StatusLevel`, `KvStoreStatus`, `InflightStatus`,
  `ElectionStateView`, `ReadStateView`, `CrowTreeStatsView`,
  `MetricsSnapshot`, `MetricPoint`, `MetricField`.
- `lib/crow-kv/src/cluster/status.rs` contains no struct/enum
  definitions — only re-exports from `crow_protocol::mgmt` and the
  `From<ElectionMetricsSnapshot>` impl + `crow_tree_stats_to_view`
  function.
- `app/crow-kv-server/src/mgmt_api.rs` defines zero structs that
  duplicate `crow-protocol::mgmt` (verified by grep for struct names).
- `lib/crow-console-shared/src/snapshot.rs` defines zero mirror types
  — only `ClusterSnapshot`, `ServerSnapshot`, and type aliases.
- `lib/crow-kv-client/src/topology.rs` has no local `TopologyResponse`
  struct.
- `lib/crow-console-shared/src/cluster.rs` has no `pub type` ID
  aliases — imports from `crow_protocol::common_type`.
- `lib/crow-console-shared/src/model.rs` does not exist.
- No `String`-typed `rack_id` or `node_id` field in any struct in
  `crow-console-shared` (except `ServerEntry.id` and
  `lifecycle.rs`'s `server_id`, which §2.5 allows as `String`).
- `cargo fmt --check`, `cargo clippy -- -D warnings` pass.
- `test-kv-core`, `test-kv-server`, `test-console-cli`,
  `test-console-server` pass.
