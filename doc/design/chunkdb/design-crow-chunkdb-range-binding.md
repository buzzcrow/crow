<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: chunkdb Range Binding + Instance Sharding

Depends on: [`design-crow-chunkdb.md`](design-crow-chunkdb.md) §3.6 (stateless),
§5.4a (hash bucket system), §5.4b (migration);
[`../kv/design-crow-kv-group0.md`](../kv/design-crow-kv-group0.md) §2.1 (sysdata API surface).
Satisfies: `design-crow-chunkdb.md` §5.4a (instance sharding by bucket range).

This sub-design covers the chunkdb instance range binding framework:
the group-0 binding schema, the `RangeBindingClient` (read + cache +
route + retry), the server-side `RangeGuard` enforcement, the
`BindingMonitor` that keeps the binding table in sync with the service
registry, and the `NotMyRange` reject-and-retry protocol. Architecture
decisions (why sharding, why group-0, why hash buckets) live in the
root design; this doc carries the implementation detail.

The binding model is **non-contiguous sub-ranges**: the 16-bit bucket
space (0-65535) is divided into `N` fixed sub-ranges (default 1024),
and each sub-range is owned by exactly one chunkdb instance. Instances
can own non-contiguous sets of sub-ranges, enabling fine-grained
rebalancing by moving individual sub-ranges without touching
neighbors. A common `BindingStrategy` trait abstracts the "owner
problem" (key → service-instance binding) so chunkdb's range strategy
and diskdb's table strategy (R102) share one monitor loop.

---

## Table of Contents

- [1. Instance Binding Schema](#1-instance-binding-schema)
  - [1.1 Proto types](#11-proto-types)
  - [1.2 Key types](#12-key-types)
  - [1.3 Sub-range space](#13-sub-range-space)
  - [1.4 Edge cases](#14-edge-cases)
- [2. RangeBindingClient](#2-rangebindingclient)
  - [2.1 Struct + methods](#21-struct--methods)
  - [2.2 Routing with transition fallback](#22-routing-with-transition-fallback)
  - [2.3 Watch/notify integration](#23-watchnotify-integration)
  - [2.4 Reject-and-retry](#24-reject-and-retry)
  - [2.5 Edge cases](#25-edge-cases)
- [3. Server Range Enforcement](#3-server-range-enforcement)
  - [3.1 RangeGuard](#31-rangeguard)
  - [3.2 Lifecycle integration](#32-lifecycle-integration)
  - [3.3 Edge cases](#33-edge-cases)
- [4. Client Routing Integration](#4-client-routing-integration)
- [5. Dynamic Binding Monitor](#5-dynamic-binding-monitor)
  - [5.1 Common BindingStrategy trait](#51-common-bindingstrategy-trait)
  - [5.2 ChunkdbRangeStrategy](#52-chunkdbrangestrategy)
  - [5.3 Generic BindingMonitor](#53-generic-bindingmonitor)
  - [5.4 Monitor wiring in crow-kv-server](#54-monitor-wiring-in-crow-kv-server)
  - [5.5 Edge cases](#55-edge-cases)
- [6. DiskdbClientPool Precise free_blocks Routing](#6-diskdbclientpool-precise-free_blocks-routing)
- [7. Server Wiring](#7-server-wiring)
- [8. Configuration](#8-configuration)
- [9. References](#9-references)

## 1. Instance Binding Schema

A parallel binding table (bucket sub-range → chunkdb instance) is
stored in group-0 alongside the KV group binding (`BindingCache` in
`routing.rs`). The KV group binding maps bucket ranges → KV group
(R88); the instance binding maps bucket sub-ranges → chunkdb instance.
Both coexist: the KV group binding decides where chunk metadata is
persisted; the instance binding decides which chunkdb instance
processes the request.

### 1.1 Proto types

`sysdata_type.proto` defines `ChunkdbRangeBindingValue` +
`ChunkdbRangeMigrationValue` + the `RangeStatus` enum. The binding
value carries the sub-range index, the derived bucket bounds, the
current + original owner, and the transition status.

```proto
message ChunkdbRangeBindingValue {
  uint32 sub_range_index = 1;  // sub-range index (0..N-1, N = sub_range_count)
  uint32 range_start     = 2;  // derived from sub_range_index (cached for routing)
  uint32 range_end       = 3;  // derived from sub_range_index
  uint64 instance_id     = 4;  // current owner
  string grpc_endpoint   = 5;
  uint64 original_instance_id = 6;  // original owner (for migration fallback)
  string original_endpoint     = 7;
  RangeStatus status     = 8;  // STABLE or IN_TRANSITION
  uint64 last_change_time_ms = 9;  // last ownership change timestamp
}

enum RangeStatus {
  RANGE_STATUS_STABLE        = 0;
  RANGE_STATUS_IN_TRANSITION = 1;
}
```

`chunkdb_type.proto` defines `NotMyRangeHint`, the detail message
carried in gRPC status when a chunkdb instance receives a request for
a chunk outside its owned range. The server does not track other
instances' bindings, so the hint carries only the rejected bucket (in
`range_start`/`range_end` as a diagnostic); `instance_id`,
`grpc_endpoint`, and `sub_range_index` are empty. The client refreshes
its binding cache from group-0 and re-routes via
`RangeBindingClient::refresh_and_route`.

```proto
message NotMyRangeHint {
  uint32 range_start   = 1;  // rejected bucket (diagnostic)
  uint32 range_end     = 2;  // rejected bucket (diagnostic)
  uint64 instance_id   = 3;  // unused — server does not know the owner
  string grpc_endpoint = 4;  // unused — server does not know the owner
  uint32 sub_range_index = 5;  // unused
}
```

`error_code.proto` reserves `ERROR_CODE_NOT_MY_RANGE = 30` for
structured error reporting.

`build.rs` adds `#[derive(serde::Serialize, serde::Deserialize)]` to
the new types so they round-trip through group-0's JSON value
encoding.

### 1.2 Key types

`lib/crow-protocol/src/key/chunkdb.rs` defines:

- `ChunkdbRangeBindingKey { sub_range_index: u32 }` — text path
  `/chunkdb/range_bind/<sub_range_index>`. Implements `TextKey` only
  (group-0 only). Prefix scan: `/chunkdb/range_bind/`.
- `ChunkdbRangeMigrationKey { sub_range_index: u32 }` — text path
  `/chunkdb/range_mig/<sub_range_index>`.

Both are wired into `key.rs` via `pub mod chunkdb;` + re-exports.

### 1.3 Sub-range space

- The 16-bit bucket space (0-65535) is divided into `N` fixed
  sub-ranges, where `N` is a power of 2: 1024 (default) or 4096.
- Each sub-range is `[i * 65536 / N, (i+1) * 65536 / N - 1]`
  (inclusive). For `N = 1024`: each sub-range is 64 buckets wide.
- Sub-ranges are the atomic unit of ownership — a sub-range is owned
  by exactly one instance. Instances can own non-contiguous sets of
  sub-ranges.
- `range_start`/`range_end` are derived from `sub_range_index` but
  cached in the binding value for routing convenience (avoids
  recomputation on every route).
- `original_instance_id`/`original_endpoint` are set during migration
  (R103); routing falls back to the original owner when
  `status = IN_TRANSITION` and the current owner is unreachable.
- `last_change_time_ms` is updated on every ownership change for
  diagnostics.

### 1.4 Edge cases

- Binding table empty in group-0 → client falls back to "any instance"
  (preserves single-instance behavior; logged as a warning).
- Sub-range index out of range (bucket space misconfigured) →
  `BucketUnbound` error.
- `original_instance_id` not set (no migration in progress) but
  `status = IN_TRANSITION` → treat as `STABLE` (data corruption guard).
- Both current and original owner unreachable → client retries with
  full binding cache refresh after a short delay.

## 2. RangeBindingClient

The chunkdb client routes a chunk ID to the correct chunkdb instance
via `RangeBindingClient`. This is a client concern, separate from the
KV group routing (`BindingCache`). It lives in `crow-kv-client` (the
single sysdata API surface, group0 §2.1).

### 2.1 Struct + methods

`lib/crow-kv-client/src/range_binding.rs`:

```rust
pub struct RangeBindingClient {
    kv: Arc<CrowkvClient>,
    bindings: Arc<RwLock<Vec<ChunkdbRangeBinding>>>,
}

pub struct ChunkdbRangeBinding {
    pub sub_range_index: u32,
    pub range_start: u16,
    pub range_end: u16,
    pub instance_id: u64,
    pub grpc_endpoint: String,
    pub original_instance_id: u64,
    pub original_endpoint: String,
    pub status: RangeStatus,
    pub last_change_time_ms: u64,
}
```

- `from_shared(kv: Arc<CrowkvClient>) -> Self` — wrap an existing
  `CrowkvClient`.
- `refresh() -> Result<()>` — scan `/chunkdb/range_bind/` prefix in
  group-0, parse `ChunkdbRangeBindingValue` (JSON), replace the cached
  table (sorted by `range_start`).
- `route(chunk_id: &ChunkId) -> Result<ChunkdbRangeBinding, RangeRouteError>`
  — hash chunk ID → bucket → scan the cached table for the owning
  sub-range → return the binding. On empty cache, synchronous
  `refresh()` first.
- `route_bucket(bucket: u16) -> Result<ChunkdbRangeBinding, RangeRouteError>`
  — direct bucket lookup (no refresh). Linear scan over the cached
  sub-ranges (1024 entries is small; binary search is a future
  optimization).
- `route_with_fallback(bucket) -> Result<RouteWithFallback, RangeRouteError>`
  — returns both the current owner and the original owner when
  `status = IN_TRANSITION` (see §2.2).
- `is_empty() -> bool`, `snapshot() -> Vec<ChunkdbRangeBinding>`,
  `replace(bindings)` — for watch/notify updates + test injection.
  `replace` sorts by `sub_range_index`.
- `spawn_notifier() -> Result<JoinHandle<()>>` — subscribe to
  `/chunkdb/range_bind/` prefix via `WatchNotifyClient`; on notify,
  call `refresh()`.

### 2.2 Routing with transition fallback

Routing order for a bucket:

1. Hash chunk ID → 16-bit bucket.
2. Scan the cached sub-ranges for the one whose `[range_start,
   range_end]` contains the bucket.
3. If `status = STABLE`, route to `instance_id`.
4. If `status = IN_TRANSITION`, route to `instance_id` (current
   owner); on `NotMyRange` or connection error, fall back to
   `original_instance_id`.

`route_with_fallback` returns a `RouteWithFallback { primary, fallback }
` so the caller can implement the fallback without re-scanning.

### 2.3 Watch/notify integration

`spawn_notifier` subscribes to the `/chunkdb/range_bind/` prefix via
`WatchNotifyClient`. On any notify in the prefix, it calls `refresh()`
to re-scan the binding table. This mirrors the topology notify pattern
in `topology/notify.rs`. The notifier is optional. The client works
with periodic refresh alone (safety-net poller pattern). Missed
notifies during a reconnect gap are caught by the caller's periodic
refresh.

### 2.4 Reject-and-retry

`RangeRouteError`:

```rust
pub enum RangeRouteError {
    NoBinding,
    BucketUnbound { bucket: u16 },
    Refresh(String),
}
```

The chunkdb client's `with_retry` loop (in `crow-chunkdb-client`) is
extended: on `NotMyRange` gRPC status, the client calls
`RangeBindingClient::refresh_and_route(chunk_id)` to refresh the
binding cache from group-0 and re-route in one call, then retries.
This follows the `NotLeaderHint` pattern in
`crow-kv-client/src/config.rs`. `NotMyRange` is treated as transient
(retryable) in `ChunkdbClientError::is_transient()`.

### 2.5 Edge cases

- Binding cache empty on startup → first `route` triggers synchronous
  `refresh`; if refresh fails, returns `NoBinding` (caller falls back
  to "any instance" or errors).
- Stale cache (sub-range moved) → server returns `NotMyRange` with
  hint; client refreshes + retries.
- All instances for a sub-range down → client exhausts retries;
  returns `Unavailable`.

## 3. Server Range Enforcement

Each chunkdb instance rejects requests for chunks outside its owned
sub-ranges, returning a `NotMyRange` hint so the client can re-route.
Without enforcement, a stale client cache sends requests to the wrong
instance, which processes them (no rejection), violating the
one-owner invariant that the per-chunk lock (R100) depends on.

### 3.1 RangeGuard

`app/crow-chunkdb/src/range_guard.rs`:

```rust
pub struct RangeGuard {
    owned: Arc<RwLock<Vec<OwnedRange>>>,
    allow_all_when_empty: bool,
}

pub struct OwnedRange {
    pub start: u16,
    pub end: u16,
    pub sub_range_index: u32,
}

impl RangeGuard {
    pub fn new(allow_all_when_empty: bool) -> Self;
    pub fn check(&self, chunk_id: &ChunkId) -> Result<(), NotMyRange>;
    pub fn replace(&self, ranges: Vec<OwnedRange>);
    pub async fn load_from_group0(&self, kv: &CrowkvClient, instance_id: u64) -> Result<()>;
}
```

`check` hashes the chunk ID to a bucket and scans the owned
sub-ranges. When the guard is empty and `allow_all_when_empty` is
`true`, all requests are allowed (single-instance backward compat).
When `false`, an empty guard rejects all mutating requests until the
binding table is loaded.

### 3.2 Lifecycle integration

`LifecycleHandler` gains a `range_guard: Option<Arc<RangeGuard>>`
field. Each mutating RPC (`allocate_chunk`, `append_chunk`, `seal_chunk`,
`delete_chunk`) calls `range_guard.check(chunk_id)` before proceeding.
If the guard is `None` (single-instance mode, no binding table), the
check is skipped. `QueryChunk` and `ListChunks` are read-only and
bypass the guard (a stale read is acceptable).

On `NotMyRange`, the service layer returns gRPC `FAILED_PRECONDITION`
with a `NotMyRangeHint` detail carrying only the rejected bucket (the
server does not track other instances' bindings, so it cannot fill the
owning instance endpoint). The client refreshes its binding cache from
group-0 and re-routes via `RangeBindingClient::refresh_and_route`.

### 3.3 Edge cases

- `RangeGuard` empty (instance just started, binding not loaded yet) →
  reject all mutating RPCs with `NoBinding` error until the first
  binding table load completes, unless `allow_all_when_empty` is
  `true` (default — processes requests when no binding table exists).
- Instance owns multiple disjoint sub-ranges → `check` scans all
  ranges (small list, linear scan is fine).

## 4. Client Routing Integration

`lib/crow-chunkdb-client/src/client.rs`:

- `ChunkdbClient` gains a `range_binding: Option<RangeBindingClient>`
  field. `None` preserves "any instance" behavior; `Some` enables
  range-based routing.
- `client_for_chunk(chunk_id)` — if `range_binding` is `Some`, call
  `binding.route(chunk_id)` to get the endpoint; otherwise fall back
  to `first_endpoint()`.
- `with_retry` loop: on `NotMyRange` status, call
  `binding.refresh_and_route(chunk_id)` (refresh the binding cache from
  group-0 + re-route in one call), retry (counted against `max_retries`,
  like `NotLeaderHint`). The server only signals "not my range" — it
  does not carry the owning instance endpoint.
- Constructor: `with_range_binding(binding)` — enables range routing.

The `with_retry` signature accepts the chunk ID for routing. Each RPC
method passes its chunk ID to `with_retry`. Read-only RPCs without a
chunk ID (`list_chunks`) pass `None` and use "any instance".

`ChunkdbClientError` gains a `NotMyRange(String)` variant, treated as
transient by `is_transient()`. `from_status` decodes the
`NotMyRangeHint` detail from the gRPC status when the code is
`FAILED_PRECONDITION`.

## 5. Dynamic Binding Monitor

The binding table is updated when chunkdb instances join or leave.
`BindingMonitor` automates this. Without it, the operator must
manually write bindings.

The monitor is built on a **common `BindingStrategy` trait** so
chunkdb's range strategy and diskdb's table strategy (R102) share one
generic monitor loop. The trait + generic monitor live in
`crow-kv-client`; the chunkdb-specific strategy lives alongside it.
The monitor is wired into `crow-kv-server`'s group-0 leader (not into
`crow-chunkdb`) because it writes to group-0 and needs service-registry
access. Both are group-0 concerns.

### 5.1 Common BindingStrategy trait

`lib/crow-kv-client/src/binding_framework.rs`:

```rust
pub trait BindingStrategy: Send + Sync {
    type Binding: Send + Sync;
    fn compute_assignment(&self, instances: &[(u64, InstanceValue)]) -> Vec<Self::Binding>;
    fn write_bindings(&self, kv: &CrowkvClient, bindings: &[Self::Binding]) -> impl Future<Output = Result<()>> + Send;
    fn read_bindings(&self, kv: &CrowkvClient) -> impl Future<Output = Result<Vec<Self::Binding>>> + Send;
}
```

- chunkdb: `Binding = ChunkdbRangeBindingValue` (range-based).
- diskdb (R102): `Binding = DiskdbBinding` (table-based).

Routing is not part of the trait. It lives in `RangeBindingClient`
(routing is a client concern, not a monitor concern).

### 5.2 ChunkdbRangeStrategy

`lib/crow-kv-client/src/chunkdb_binding_strategy.rs`:

```rust
pub struct ChunkdbRangeStrategy {
    sub_range_count: u32,  // default 1024
}

impl BindingStrategy for ChunkdbRangeStrategy {
    type Binding = ChunkdbRangeBindingValue;
    fn compute_assignment(&self, instances: &[(u64, InstanceValue)]) -> Vec<Self::Binding>;
    async fn write_bindings(&self, kv: &CrowkvClient, bindings: &[Self::Binding]) -> Result<()>;
    async fn read_bindings(&self, kv: &CrowkvClient) -> Result<Vec<Self::Binding>>;
}
```

`compute_sub_range_assignment` (the free function backing
`compute_assignment`):

- Sort instances by `instance_id` (deterministic).
- Divide `N` sub-ranges as evenly as possible: instance `i` gets
  sub-ranges `[i * N / M, (i+1) * N / M)` where `M = instances.len()`.
- Each sub-range entry is written with `instance_id`,
  `grpc_endpoint`, `status = STABLE`, `original_instance_id = 0`,
  `last_change_time_ms = now`.

`write_bindings` PUTs each binding (idempotent overwrite, no
delete-all). The sub-range count is fixed, so the key set is stable;
changed entries are overwritten in place. This avoids the non-atomic
delete-all window that a scan + delete-per-key approach would have.

The monitor uses **incremental assignment** (`compute_incremental_assignment`):
it reads the existing bindings, computes the desired assignment, and
diffs them. Sub-ranges whose owner is unchanged keep their existing
binding (preserving any in-progress `InTransition` state). Sub-ranges
whose owner changed are marked `InTransition` with `original_instance_id`
set to the old owner. When no sub-range changed, the monitor skips the
write entirely (avoids frequent rewrites). When instances is empty, the
existing table is preserved (not wiped).

The actual migration flow (dual-serve during `InTransition`, cutover to
`Stable`, completion) is R103. This algorithm only sets up the
transition state; R103 drives the `InTransition → Stable` cutover.

### 5.3 Generic BindingMonitor

`lib/crow-kv-client/src/binding_framework.rs`:

```rust
pub struct BindingMonitor<S: BindingStrategy> {
    kv: Arc<CrowkvClient>,
    svc: ServiceRegistryClient,
    strategy: S,
    interval: Duration,
    service_name: &'static str,
}

impl<S: BindingStrategy> BindingMonitor<S> {
    pub fn new(kv, svc, strategy, interval, service_name) -> Self;
    pub async fn tick(&self, is_leader: bool) -> Result<MonitorTickResult>;
    pub async fn run(self, stop: watch::Receiver<bool>, is_leader: impl Fn() -> bool + Send + 'static);
}
```

`tick` reads instances from the service registry (via
`svc.read_all_instances(service_name)`), reads the existing bindings
from group-0, computes the incremental assignment via the strategy
(preserving `InTransition` state for unchanged sub-ranges), and, only
when `is_leader` is `true` and something changed, writes the bindings
to group-0. Followers compute but skip the write phase, so they are
ready to take over immediately on leader change.

`run` ticks periodically until the stop signal fires. The `is_leader`
closure is called each tick.

### 5.4 Monitor wiring in crow-kv-server

`app/crow-kv-server/src/binding_monitor_wiring.rs`:

```rust
pub fn spawn_chunkdb_binding_monitor(
    registry: &Arc<KvStoreRegistry>,
    group0_endpoint: String,
    interval_secs: u64,
) -> BindingMonitorHandle;
```

The wiring:

1. Build a `CrowkvClient` seeded with the group-0 leader endpoint.
2. Build `ServiceRegistryClient` + `ChunkdbRangeStrategy::new()`.
3. Build `BindingMonitor::new(kv, svc, strategy, interval, "chunkdb")`.
4. Spawn `monitor.run(stop, is_leader)` where `is_leader` checks
   `registry.get_store(0)?.get_group(0)?.local_replica().is_leader()`.
5. `BindingMonitorHandle::stop()` sends the stop signal on shutdown.

`crow-kv-server`'s `main.rs` calls this after the keep-alive loop is
started, gated by `--binding-monitor-interval` (default 30s; 0
disables). chunkdb instances register themselves via
`ServiceRegistryClient::register_chunkdb` (see §7) so the monitor has
instances to assign.

### 5.5 Edge cases

- Zero chunkdb instances → monitor writes empty binding table; clients
  fall back to "any instance" or error.
- One instance → all sub-ranges assigned to it.
- Instance heartbeat expiry → `read_all_instances` filters expired
  entries (TTL); the monitor only sees live instances.
- Leader change mid-write → the partial write is overwritten by the
  new leader's next tick (PUT is idempotent; no delete-all window).
- Monitor task crashes → detected on next server restart (no
  supervisor; `tokio::spawn` task loss is logged via the metrics
  runner's join handle collection).
- No group-0 leader (cluster bootstrapping) → `is_leader` returns
  `false`; monitor skips ticks until a leader is elected.

### 5.6 Migration flow (chunkdb)

chunkdb instances are stateless (chunk metadata lives in KV groups,
design §3.6). Migration is a **routing change**, not a data copy.
The incremental assignment algorithm (§5.2) sets up the transition
state; the migration flow drives it to completion.

**During `InTransition`** (sub-range moved from old owner to new owner):

- **Writes** (`allocate`/`append`/`seal`/`delete`) → new owner only.
  The old owner rejects writes with `NotMyRange`; the client refreshes
  its binding cache + re-routes (`refresh_and_route`).
- **Reads** (`query`) → try the new owner first (it has the latest
  writes), fall back to the old owner if the new owner is unreachable
  or the chunk is not yet visible. This dual-serve window covers
  clients with stale caches that still send to the old owner.

**After a grace period** (all clients have refreshed caches, no more
`NotMyRange` redirects observed): the monitor sets `status = Stable`,
`original_instance_id = 0`, `original_endpoint = ""`. All operations
go to the new owner. The old owner stops serving the sub-range.

**No data cleanup** is needed. chunkdb instances hold no per-chunk
state (the chunk payload cache is in-memory and evicts naturally; the
lock map reaps idle entries). The old owner's `RangeGuard` drops the
sub-range on its next binding refresh.

The grace period + `InTransition → Stable` cutover is driven by R103
(chunkdb range migration). The incremental algorithm (§5.2) only sets
up the `InTransition` state; R103 monitors redirect traffic and
finalizes the cutover.

### 5.7 Migration flow (diskdb)

diskdb migration is a **data copy** (zone records are physically stored
on a KV group, keyed by `disk_id`). The flow is five steps:
**Quiesce → Copy → Switch → Cleanup → Resume**, driven by the console
(operator-triggered; the monitor only warns). The target disk-group/disk
is placed in `Maintenance` mode (blocks allocates, suspends frees +
compaction) before the copy; after the copy + bind flip, it is set back
to `Up` and the `MigrationIntentValue` is deleted.

See `doc/backlog/R102-diskdb-dynamic-binding-migration.md` §Work item 5
for the full five-step flow + edge cases.

## 6. DiskdbClientPool Precise free_blocks Routing

`allocator/pool.rs` `free_blocks` groups segments by disk-group (via
`disk_id → dg_id` reverse lookup) and sends each group's free RPC to
the owning instance only. A `disk_id_to_dg: DashMap<DiskId, u64>`
cache is populated from the topology cache's `DiskGroupEntry` list
(each entry has `disk_ids`). `update_disk_id_lookup` is called by the
topology refresh loop.

Fallback: if the reverse lookup misses (cache cold or `disk_id`
unknown), the segments are broadcast to all channels (preserves
correctness; the owning instance accepts the free, others reject).

## 7. Server Wiring

### 7.1 crow-chunkdb startup

1. `main.rs` startup → create `RangeBindingClient::from_shared(kv)`.
2. Call `refresh()` to load the chunkdb instance binding table from
   group-0. If empty, log warning + use `allow_all_when_empty` mode.
3. Create `RangeGuard` from the binding table (filter for this
   instance's owned sub-ranges via `load_from_group0`).
4. Pass `RangeGuard` into `LifecycleHandler::with_range_guard()` and
   `ChunkdbService::with_range_guard()`.
5. Spawn `RangeBindingClient::spawn_notifier()` to keep the binding
   cache fresh.
6. Spawn the chunkdb service-registry keep-alive loop
   (`spawn_chunkdb_keepalive`) — registers under
   `/srv/chunkdb/<instance_id>` and heartbeats periodically so the
   `BindingMonitor` in `crow-kv-server` can see this instance.

### 7.2 crow-kv-server startup

1. After the keep-alive loop is started, call
   `spawn_chunkdb_binding_monitor(&registry, group0_ep, interval)`.
2. The monitor runs as a leader-gated background task on every
   group-0 replica; only the leader writes the binding table.
3. On shutdown, `BindingMonitorHandle::stop()` sends the stop signal.

## 8. Configuration

`chunkdb_config.rs`:

- `RangeGuardConfig { allow_all_when_empty: bool }` (default `true`).
  When `true`, an empty range guard allows all requests — preserving
  single-instance behavior before the binding table is loaded. When
  `false`, an empty guard rejects all mutating requests until the
  binding table is loaded.
- `ServerConfig.keepalive_interval_secs: u32` (default 10) —
  service-registry heartbeat interval. 0 disables registration (the
  binding monitor will not see this instance).
- Added under `ChunkdbConfig.range_guard` + `ChunkdbConfig.server`.

`crow-kv-server` CLI:

- `--binding-monitor-interval <secs>` (default 30) — chunkdb range
  binding monitor tick interval. 0 disables the monitor (the binding
  table is then operator-manual).

## 9. References

- Root design: [`design-crow-chunkdb.md`](design-crow-chunkdb.md)
  §3.6 (stateless), §5.4a (hash bucket system), §5.4b (migration).
- Group-0 sysdata API: [`../kv/design-crow-kv-group0.md`](../kv/design-crow-kv-group0.md) §2.1.
- `RangeBindingClient`: `lib/crow-kv-client/src/range_binding.rs`.
- `BindingStrategy` + `BindingMonitor`: `lib/crow-kv-client/src/binding_framework.rs`.
- `ChunkdbRangeStrategy`: `lib/crow-kv-client/src/chunkdb_binding_strategy.rs`.
- `RangeGuard`: `app/crow-chunkdb/src/range_guard.rs`.
- Monitor wiring: `app/crow-kv-server/src/binding_monitor_wiring.rs`.
- chunkdb keep-alive: `app/crow-chunkdb/src/main.rs` (`spawn_chunkdb_keepalive`).
- `NotMyRangeHint`: `lib/crow-protocol/src/proto/chunkdb_type.proto`.
- `ChunkdbRangeBindingValue` + `RangeStatus`: `lib/crow-protocol/src/proto/sysdata_type.proto`.
- Follow-ups: R102 (diskdb dynamic binding migration — reuses
  `BindingStrategy`), R103 (chunkdb range migration — transition
  states + dual-serve cutover).
