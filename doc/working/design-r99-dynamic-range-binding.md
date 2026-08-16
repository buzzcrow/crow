<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R99 Rework: Dynamic Range Binding Framework (R99)

Backlog: [`doc/backlog/R99-kv-dynamic-range-binding-framework.md`](../backlog/R99-kv-dynamic-range-binding-framework.md).
Root design: [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md) §3.6, §5.4.
Decisions log: [`doc/working/gap.md`](gap.md) — GAP-R99-1..8.

**Already landed:** R99 v1 committed — `RangeGuard`, `BindingTable`/`BindingCache`,
`RangeBindingClient`, `BindingMonitor` (in `crow-chunkdb`), `NotMyRange` error +
client refresh-and-retry, `ChunkdbRangeBindingValue`/`ChunkdbRangeMigrationValue`
protos. This rework addresses four gap decisions that change the range model,
monitor location, and framework structure.

Architecture decisions and rationale are in the root design; this doc does not
repeat them.

## 1. Common Binding Framework (GAP-R99-1)

### 1.1 Why

chunkdb instance sharding and diskdb disk-group binding are the same problem
("owner problem"): a key → service-instance binding stored in group-0, read by
clients, updated dynamically. R99 v1 built chunkdb's monitor in isolation;
diskdb has no monitor at all (operator-manual `BindMapValue`). The user wants
conceptual unification — one high-level interface — with flexibility for
different strategies (range-based for chunkdb, table-based for diskdb). Some
code duplication is acceptable.

### 1.2 Common trait

Define a trait in `lib/crow-kv-client/src/binding_framework.rs` that abstracts
the owner problem. Both chunkdb's range binding and diskdb's disk-group binding
(R102) implement it.

```rust
/// A binding strategy — maps a key to an owning instance.
pub trait BindingStrategy: Send + Sync {
    type Key;
    type Binding;
    /// Compute a new assignment for the given instances.
    fn compute_assignment(
        &self,
        instances: &[(u64, InstanceValue)],
    ) -> Vec<Self::Binding>;
    /// Write the assignment to group-0.
    async fn write_bindings(
        &self,
        kv: &CrowkvClient,
        bindings: &[Self::Binding],
    ) -> Result<()>;
    /// Read the current assignment from group-0.
    async fn read_bindings(
        &self,
        kv: &CrowkvClient,
    ) -> Result<Vec<Self::Binding>>;
    /// Route a key to its owning instance.
    fn route(
        &self,
        bindings: &[Self::Binding],
        key: &Self::Key,
    ) -> Option<&Self::Binding>;
}
```

- chunkdb: `Key = ChunkId`, `Binding = ChunkdbRangeBinding` (range-based).
- diskdb (R102): `Key = DiskGroupId`, `Binding = DiskdbBinding` (table-based).

### 1.3 Generic monitor

`BindingMonitor<S: BindingStrategy>` — generic over the strategy. The monitor
loop (tick periodically, read instances, compute assignment, write to group-0)
is shared; only the strategy differs. Lives in `lib/crow-kv-client/src/binding_monitor.rs`.

Edge cases:
- Strategy `compute_assignment` returns empty (no instances) → monitor deletes
  all bindings, leaves the table empty.
- `write_bindings` fails mid-batch → partial write; next tick rewrites the full
  table (idempotent full-replace).

## 2. Non-Contiguous Sub-Range Model (GAP-R99-3)

### 2.1 Why

Contiguous ranges (`[0, 21844]`, `[21845, 43689]`, ...) make incremental
rebalancing expensive: splitting a range requires updating two entries and
carefully choosing the boundary. Non-contiguous sub-ranges (e.g. instance A
owns buckets 0-99, 200-299, 500-599) allow fine-grained rebalancing by moving
individual sub-ranges without touching neighbors. Capping the space at 1024
sub-ranges (configurable to 4096) keeps the binding table small and routing
fast.

### 2.2 Sub-range space

- The 16-bit bucket space (0-65535) is divided into `N` fixed sub-ranges,
  where `N` is a power of 2: 1024 (default) or 4096.
- Each sub-range is `[i * 65536 / N, (i+1) * 65536 / N - 1]` (inclusive).
- For `N = 1024`: each sub-range is 64 buckets wide.
- Sub-ranges are the atomic unit of ownership — a sub-range is owned by exactly
  one instance. Instances can own non-contiguous sets of sub-ranges.
- The sub-range count is fixed at cluster bootstrap (stored in group-0 as
  `/chunkdb/range_config/sub_range_count`). Changing it requires a full
  rebinding.

### 2.3 Per-sub-range metadata

Each sub-range entry in group-0 carries:

```protobuf
message ChunkdbRangeBindingValue {
  uint32 sub_range_index = 1;  // 0..N-1
  uint32 range_start     = 2;  // derived from sub_range_index (redundant but cached)
  uint32 range_end       = 3;  // derived from sub_range_index
  uint64 instance_id     = 4;  // current owner
  string grpc_endpoint   = 5;
  uint64 original_instance_id = 6;  // for migration fallback
  string original_endpoint     = 7;
  RangeStatus status     = 8;  // STABLE or IN_TRANSITION
  uint64 last_change_time_ms = 9;
}

enum RangeStatus {
  RANGE_STATUS_STABLE        = 0;
  RANGE_STATUS_IN_TRANSITION = 1;
}
```

- Key: `/chunkdb/range_bind/<sub_range_index>` (zero-padded 4 digits).
- `range_start`/`range_end` are derived from `sub_range_index` but cached for
  routing convenience (avoids recomputation on every route).
- `original_instance_id`/`original_endpoint` are set during migration; routing
  falls back to the original owner when `status = IN_TRANSITION` and the
  current owner is unreachable.
- `last_change_time_ms` is updated on every ownership change for diagnostics.

### 2.4 Routing with transition fallback

Routing order for a bucket:
a. Hash chunk ID → 16-bit bucket.
b. Map bucket → sub-range index: `sub_range_index = bucket / (65536 / N)`.
c. Look up the sub-range binding by index.
d. If `status = STABLE`, route to `instance_id`.
e. If `status = IN_TRANSITION`, route to `instance_id` (current owner); on
   `NotMyRange` or connection error, fall back to `original_instance_id`.

Edge cases:
- Sub-range index out of range (bucket space misconfigured) → `BucketUnbound`
  error.
- Both current and original owner unreachable → client retries with full
  binding cache refresh after a short delay.
- `original_instance_id` not set (no migration in progress) but
  `status = IN_TRANSITION` → treat as `STABLE` (data corruption guard).

### 2.5 Assignment algorithm

`compute_assignment` for non-contiguous sub-ranges:
a. Sort instances by `instance_id`.
b. Divide `N` sub-ranges as evenly as possible: instance `i` gets sub-ranges
   `[i * N / M, (i+1) * N / M)` where `M = instances.len()`.
c. Each sub-range entry is written with `instance_id`, `grpc_endpoint`,
   `status = STABLE`, `original_instance_id = 0`, `last_change_time_ms = now`.

For incremental rebalancing (instance join/leave):
a. Compute the new target assignment.
b. Diff against the current assignment.
c. For each sub-range that changes owner: set `status = IN_TRANSITION`,
   `original_instance_id = old_owner`, `instance_id = new_owner`.
d. After migration completes (R103), set `status = STABLE`.

Edge cases:
- Instance join: assign it sub-ranges from the most loaded instance(s).
- Instance leave: reassign its sub-ranges to surviving instances.
- No instances → empty binding table (all requests rejected).

## 3. Monitor Relocation to crow-kv-server (GAP-R99-2/4/6)

### 3.1 Why

The binding monitor must run on group-0 replicas (not on chunkdb instances)
because:
- It writes to group-0 — running on the leader avoids extra network hops.
- It needs service registry access, which is a group-0 sysdata concern.
- chunkdb instances may not even be running on the same nodes as group-0.

Only the group-0 leader should perform balancing writes; followers run the
monitor loop but skip the write phase (they monitor for leader failover
readiness but don't write).

### 3.2 Monitor in crow-kv-server

Move `BindingMonitor` from `app/crow-chunkdb/src/binding_monitor.rs` to
`lib/crow-kv-client/src/binding_monitor.rs` (as a generic monitor over
`BindingStrategy`). The chunkdb-specific strategy (`ChunkdbRangeStrategy`)
lives in `lib/crow-kv-client/src/binding_monitor.rs` or a new
`lib/crow-kv-client/src/chunkdb_binding_strategy.rs`.

`crow-kv-server` wires the monitor into its group-0 leader startup:
a. On startup, spawn the monitor task on every group-0 replica.
b. The monitor checks leader status on each tick (via the local store's
   `is_leader()` API).
c. If leader: read instances, compute assignment, write bindings.
d. If follower: read instances, compute assignment (for readiness), but do
   NOT write.

### 3.3 Leader detection

`crow-kv-server` already has leader detection for group-0. The monitor task
calls `store.is_leader_for_group(0, 0)` (or equivalent) before writing. If
leadership is lost mid-tick, the write fails safely (the new leader's next
tick overwrites).

Edge cases:
- Leader change mid-write: the partial write is overwritten by the new
  leader's next tick (full-replace semantics).
- Monitor task crashes: `tokio::spawn` task loss is detected by the supervisor
  (if present) or on next server restart.
- No group-0 leader (cluster bootstrapping): monitor skips ticks until a
  leader is elected.

## 4. NotMyRange Protocol (GAP-R99-7) — No Change

Already implemented and confirmed:
- `ERROR_CODE_NOT_MY_RANGE = 30` in `error_code.proto`.
- `NotMyRangeHint` in `chunkdb_type.proto` with range + endpoint.
- Server returns `NotMyRange` in `service.rs`.
- Client decodes hint, refreshes binding cache, retries.

The only update needed: `NotMyRangeHint` should carry `sub_range_index`
instead of `range_start`/`range_end` to match the new model. This is a
backward-compatible proto change (add field, keep old fields for v1 clients).

## 5. Shared Protocol Types (GAP-R99-2/6)

### 5.1 Why

The user wants shared binding concepts in `crow-protocol` so both
`crow-kv-server` (monitor) and `crow-chunkdb`/`crow-diskdb` (enforcement) can
use the same types without cross-app dependencies.

### 5.2 What moves

- `RangeStatus` enum → `lib/crow-protocol/src/proto/sysdata_type.proto`.
- `ChunkdbRangeBindingValue` extended fields → same proto file.
- `BindingStrategy` trait → `lib/crow-kv-client/src/binding_framework.rs`
  (depends on `crow-protocol` types, not on any app crate).
- `BindingMonitor` generic monitor → `lib/crow-kv-client/src/binding_monitor.rs`.
- `ChunkdbRangeStrategy` → `lib/crow-kv-client/src/chunkdb_binding_strategy.rs`.

`crow-chunkdb` keeps:
- `RangeGuard` (server-side enforcement, reads from group-0).
- `routing.rs` (KV group routing — unchanged, this is R88 storage routing,
  not instance routing).

`crow-chunkdb` loses:
- `binding_monitor.rs` (moved to `crow-kv-client`).

## Scope

- `lib/crow-protocol/src/proto/sysdata_type.proto` — extend
  `ChunkdbRangeBindingValue` with `sub_range_index`, `original_instance_id`,
  `original_endpoint`, `status`, `last_change_time_ms`; add `RangeStatus` enum.
- `lib/crow-protocol/src/proto/chunkdb_type.proto` — add `sub_range_index`
  field to `NotMyRangeHint`.
- `lib/crow-protocol/build.rs` — add serde/Eq/Hash derives for `RangeStatus`.
- `lib/crow-protocol/src/key/chunkdb.rs` — update
  `ChunkdbRangeBindingKey` to use `sub_range_index` instead of `range_start`.
- `lib/crow-kv-client/src/binding_framework.rs` (new) — `BindingStrategy`
  trait.
- `lib/crow-kv-client/src/binding_monitor.rs` (new) — generic
  `BindingMonitor<S: BindingStrategy>`.
- `lib/crow-kv-client/src/chunkdb_binding_strategy.rs` (new) —
  `ChunkdbRangeStrategy` implementing `BindingStrategy` with non-contiguous
  sub-range assignment.
- `lib/crow-kv-client/src/range_binding.rs` — update `ChunkdbRangeBinding`
  struct with new fields; update `route()` to use sub-range index; add
  transition fallback.
- `lib/crow-kv-client/src/lib.rs` — export new modules.
- `app/crow-chunkdb/src/binding_monitor.rs` — delete (moved to
  `crow-kv-client`).
- `app/crow-chunkdb/src/range_guard.rs` — update `OwnedRange` to
  `OwnedSubRange` (index-based); update `check()` and `load_from_group0()`.
- `app/crow-chunkdb/src/lib.rs` — remove `binding_monitor` module.
- `app/crow-chunkdb/src/main.rs` — remove monitor-related code (if any).
- `app/crow-kv-server/src/main.rs` — wire `BindingMonitor` into group-0
  leader startup.
- `app/crow-kv-server/src/binding_monitor_wiring.rs` (new) — helper for
  spawning the monitor task with leader gating.
- `lib/crow-chunkdb-client/src/client.rs` — update routing to use sub-range
  index + transition fallback.

## Complexity

**High.** The non-contiguous sub-range model changes the core data structure
of the binding table — every read/write/route path needs updating. The monitor
relocation crosses crate boundaries (chunkdb → kv-client → kv-server) and
requires understanding kv-server's leader detection API. The common framework
trait is conceptually simple but needs careful generic design to avoid
over-abstraction. The main challenge is backward compatibility: existing
clusters with contiguous-range binding tables need a migration path (or the
rework is a breaking change deployed during a maintenance window).

## Test Design

### Unit tests (UT)

**Sub-range model:**
- `sub_range_index(bucket=0, N=1024)` → 0. `sub_range_index(bucket=63, N=1024)`
  → 0. `sub_range_index(bucket=64, N=1024)` → 1. UT.
- `sub_range_bounds(index=0, N=1024)` → [0, 63]. `sub_range_bounds(index=1023,
  N=1024)` → [65472, 65535]. UT.
- `compute_assignment` with 3 instances, N=1024 → instance 0 gets sub-ranges
  0-341, instance 1 gets 342-682, instance 2 gets 683-1023. UT.
- `compute_assignment` with 0 instances → empty vec. UT.
- `compute_assignment` with 1 instance → all 1024 sub-ranges assigned to it.
  UT.

**Routing with transition fallback:**
- Route to sub-range with `status=STABLE` → routes to `instance_id`. UT.
- Route to sub-range with `status=IN_TRANSITION`, current owner reachable →
  routes to `instance_id`. UT.
- Route to sub-range with `status=IN_TRANSITION`, current owner returns
  `NotMyRange` → falls back to `original_instance_id`. UT.
- Route to sub-range with `status=IN_TRANSITION`, no `original_instance_id`
  set → treats as `STABLE`, routes to `instance_id`. UT.

**Range guard:**
- `check()` with owned sub-range index → Ok. UT.
- `check()` with unowned sub-range index → `NotMyRange` with correct bucket.
  UT.
- `load_from_group0()` filters by `instance_id` and loads sub-range indices.
  UT.
- `replace()` with non-contiguous sub-ranges → all pass, outside fails. UT.

**Binding strategy:**
- `ChunkdbRangeStrategy::compute_assignment` matches expected sub-range
  distribution. UT.
- `ChunkdbRangeStrategy::route` finds the correct binding for a bucket. UT.
- `ChunkdbRangeStrategy::write_bindings` writes all entries; `read_bindings`
  reads them back. UT (with mock KV client).

**Monitor:**
- `BindingMonitor::tick` as leader → writes bindings. UT (with mock).
- `BindingMonitor::tick` as follower → computes but does NOT write. UT.
- `BindingMonitor::tick` with no instances → deletes all bindings. UT.

### End-to-end tests (E2E)

**Sharding + reject-and-retry:**
- 2 chunkdb instances + kv cluster → monitor assigns sub-ranges → client
  routes to correct instance → wrong instance rejects with `NotMyRange` →
  client refreshes + retries → succeeds. E2E.
- Instance join (3rd instance) → monitor rebalances → new instance gets
  sub-ranges → clients route correctly. E2E.

**Monitor leader gating:**
- kv-server group-0 leader runs monitor → binding table written. Kill leader
  → new leader elected → new leader's monitor takes over. E2E.

**Non-contiguous ranges:**
- Instance A owns sub-ranges 0-99, 200-299; instance B owns 100-199, 300-399.
  Client routes bucket 50 → A, bucket 150 → B, bucket 250 → A. E2E.

**Transition fallback:**
- Sub-range in IN_TRANSITION, current owner down → client falls back to
  original owner → read succeeds. E2E.

## Module Structure

```
lib/crow-protocol/src/proto/
  sysdata_type.proto        — extend ChunkdbRangeBindingValue, add RangeStatus
  chunkdb_type.proto        — add sub_range_index to NotMyRangeHint
lib/crow-protocol/src/key/
  chunkdb.rs                — update ChunkdbRangeBindingKey
lib/crow-kv-client/src/
  binding_framework.rs      (new) — BindingStrategy trait
  binding_monitor.rs        (new) — generic BindingMonitor<S>
  chunkdb_binding_strategy.rs (new) — ChunkdbRangeStrategy
  range_binding.rs          — update ChunkdbRangeBinding + routing
  lib.rs                    — export new modules
app/crow-chunkdb/src/
  binding_monitor.rs        — deleted
  range_guard.rs            — update to sub-range model
  lib.rs                    — remove binding_monitor
  main.rs                   — remove monitor code
app/crow-kv-server/src/
  binding_monitor_wiring.rs (new) — spawn monitor with leader gating
  main.rs                   — wire monitor into group-0 startup
lib/crow-chunkdb-client/src/
  client.rs                 — update routing + transition fallback
```

## Config Extensions

- `ChunkdbConfig.range_guard.sub_range_count` — `u32`, default 1024, max 4096.
  Must be a power of 2. Added to `RangeGuardConfig` in `chunkdb_config.rs`.
  `validate()` checks `is_power_of_two` and `<= 4096`.
- `crow-kv-server` config: `binding_monitor.interval_secs` — `u32`, default 30.
  Added to a new `BindingMonitorConfig` section.

## Server Wiring

1. `crow-kv-server` startup: after group-0 is bootstrapped and leader is
   elected, spawn `BindingMonitor<ChunkdbRangeStrategy>` as a background task.
2. The monitor task checks `is_leader()` on each tick; only the leader writes.
3. `crow-chunkdb` startup: `RangeGuard::load_from_group0()` reads the new
  sub-range-based binding table; `replace()` updates owned sub-ranges.
4. `crow-chunkdb-client`: `RangeBindingClient::route()` uses sub-range index
  for lookup; on `NotMyRange`, refreshes and retries with transition fallback.

## Open Questions

- **Breaking change vs migration path** — the sub-range model changes the
  group-0 binding table schema. Existing clusters with contiguous-range
  tables need either a migration script or a breaking-change deployment during
  maintenance. Given that R99 is not yet deployed to production, a breaking
  change is likely acceptable. Needs confirmation.
- **Sub-range count change** — if the sub-range count needs to change after
  deployment (e.g. 1024 → 4096), all bindings must be recomputed. Should this
  be an online operation (monitor reads new count, recomputes) or a manual
  operator action? Default: manual (write new count to group-0, restart
  monitor).
