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

---

## Table of Contents

- [1. Instance Binding Schema](#1-instance-binding-schema)
  - [1.1 Proto types](#11-proto-types)
  - [1.2 Key types](#12-key-types)
  - [1.3 Edge cases](#13-edge-cases)
- [2. RangeBindingClient](#2-rangebindingclient)
  - [2.1 Struct + methods](#21-struct--methods)
  - [2.2 Watch/notify integration](#22-watchnotify-integration)
  - [2.3 Reject-and-retry](#23-reject-and-retry)
  - [2.4 Edge cases](#24-edge-cases)
- [3. Server Range Enforcement](#3-server-range-enforcement)
  - [3.1 RangeGuard](#31-rangeguard)
  - [3.2 Lifecycle integration](#32-lifecycle-integration)
  - [3.3 Edge cases](#33-edge-cases)
- [4. Client Routing Integration](#4-client-routing-integration)
- [5. Dynamic Binding Monitor](#5-dynamic-binding-monitor)
  - [5.1 Range assignment algorithm](#51-range-assignment-algorithm)
  - [5.2 Edge cases](#52-edge-cases)
- [6. DiskdbClientPool Precise free_blocks Routing](#6-diskdbclientpool-precise-free_blocks-routing)
- [7. Server Wiring](#7-server-wiring)
- [8. Configuration](#8-configuration)
- [9. References](#9-references)

## 1. Instance Binding Schema

A parallel binding table — bucket range → chunkdb instance — is stored
in group-0 alongside the KV group binding (`BindingCache` in
`routing.rs`). The KV group binding maps bucket ranges → KV group
(R88); the instance binding maps bucket ranges → chunkdb instance.
Both coexist: the KV group binding decides where chunk metadata is
persisted; the instance binding decides which chunkdb instance
processes the request.

### 1.1 Proto types

`sysdata_type.proto` defines `ChunkdbRangeBindingValue` and
`ChunkdbRangeMigrationValue` + the `ChunkdbMigrationState` enum. The
binding value carries the bucket range `[range_start, range_end]`
(both inclusive `u16`, 0-65535), the `instance_id`, and the
`grpc_endpoint`.

```proto
message ChunkdbRangeBindingValue {
  uint32 range_start   = 1;  // bucket range start (inclusive), 0-65535
  uint32 range_end     = 2;  // bucket range end (inclusive), 0-65535
  uint64 instance_id   = 3;  // chunkdb instance ID
  string grpc_endpoint = 4;  // chunkdb gRPC endpoint
}
```

`chunkdb_type.proto` defines `NotMyRangeHint` — the detail message
carried in gRPC status when a chunkdb instance receives a request for
a chunk outside its owned range. The hint carries the current owner's
range + endpoint so the client can re-route without a full refresh.

```proto
message NotMyRangeHint {
  uint32 range_start   = 1;
  uint32 range_end     = 2;
  uint64 instance_id   = 3;
  string grpc_endpoint = 4;
}
```

`error_code.proto` reserves `ERROR_CODE_NOT_MY_RANGE = 30` for
structured error reporting.

`build.rs` adds `#[derive(serde::Serialize, serde::Deserialize)]` to
the new types so they round-trip through group-0's JSON value
encoding.

### 1.2 Key types

`lib/crow-protocol/src/key/chunkdb.rs` defines:

- `ChunkdbRangeBindingKey { range_start: u16 }` — text path
  `/chunkdb/range_bind/<range_start>`. Implements `TextKey` only
  (group-0 only). Prefix scan: `/chunkdb/range_bind/`.
- `ChunkdbRangeMigrationKey { range_start: u16 }` — text path
  `/chunkdb/range_mig/<range_start>`.

Both are wired into `key.rs` via `pub mod chunkdb;` + re-exports.

### 1.3 Edge cases

- Binding table empty in group-0 → client falls back to "any instance"
  (preserves single-instance behavior; logged as a warning).
- Overlapping ranges in the binding table → last-writer-wins by
  `range_start` (the monitor ensures no overlaps; manual writes are
  operator error).
- `range_end = 65535` → covers the full bucket space (buckets are
  0-65535, range is `[start, end]` inclusive).

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
    pub range_start: u16,
    pub range_end: u16,
    pub instance_id: u64,
    pub grpc_endpoint: String,
}
```

- `from_shared(kv: Arc<CrowkvClient>) -> Self` — wrap an existing
  `CrowkvClient`.
- `refresh() -> Result<()>` — scan `/chunkdb/range_bind/` prefix in
  group-0, parse `ChunkdbRangeBindingValue` (JSON), replace the cached
  table (sorted by `range_start`).
- `route(chunk_id: &ChunkId) -> Result<ChunkdbRangeBinding, RangeRouteError>`
  — hash chunk ID → bucket → scan the cached table for the owning
  range → return the binding. On empty cache, synchronous `refresh()`
  first.
- `route_bucket(bucket: u16) -> Result<ChunkdbRangeBinding, RangeRouteError>`
  — direct bucket lookup (no refresh).
- `is_empty() -> bool`, `snapshot() -> Vec<ChunkdbRangeBinding>`,
  `replace(bindings)` — for watch/notify updates + test injection.
- `spawn_notifier() -> Result<JoinHandle<()>>` — subscribe to
  `/chunkdb/range_bind/` prefix via `WatchNotifyClient`; on notify,
  call `refresh()`.

### 2.2 Watch/notify integration

`spawn_notifier` subscribes to the `/chunkdb/range_bind/` prefix via
`WatchNotifyClient`. On any notify in the prefix, it calls `refresh()`
to re-scan the binding table. This mirrors the topology notify pattern
in `topology/notify.rs`. The notifier is optional — the client works
with periodic refresh alone (safety-net poller pattern). Missed
notifies during a reconnect gap are caught by the caller's periodic
refresh.

### 2.3 Reject-and-retry

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
`RangeBindingClient::refresh()` to update the binding cache, re-routes,
and retries. This follows the `NotLeaderHint` pattern in
`crow-kv-client/src/config.rs`. `NotMyRange` is treated as transient
(retryable) in `ChunkdbClientError::is_transient()`.

### 2.4 Edge cases

- Binding cache empty on startup → first `route` triggers synchronous
  `refresh`; if refresh fails, returns `NoBinding` (caller falls back
  to "any instance" or errors).
- Stale cache (range moved) → server returns `NotMyRange` with hint;
  client refreshes + retries.
- All instances for a range down → client exhausts retries; returns
  `Unavailable`.

## 3. Server Range Enforcement

Each chunkdb instance rejects requests for chunks outside its owned
range, returning a `NotMyRange` hint so the client can re-route.
Without enforcement, a stale client cache sends requests to the wrong
instance, which processes them (no rejection) — violating the
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
}

impl RangeGuard {
    pub fn new(allow_all_when_empty: bool) -> Self;
    pub fn check(&self, chunk_id: &ChunkId) -> Result<(), NotMyRange>;
    pub fn replace(&self, ranges: Vec<OwnedRange>);
    pub async fn load_from_group0(&self, kv: &CrowkvClient, instance_id: u64) -> Result<()>;
}
```

`check` hashes the chunk ID to a bucket and scans the owned ranges.
When the guard is empty and `allow_all_when_empty` is `true`, all
requests are allowed (single-instance backward compat). When `false`,
an empty guard rejects all mutating requests until the binding table
is loaded.

### 3.2 Lifecycle integration

`LifecycleHandler` gains a `range_guard: Option<Arc<RangeGuard>>`
field. Each mutating RPC (`allocate_chunk`, `append_chunk`, `seal_chunk`,
`delete_chunk`) calls `range_guard.check(chunk_id)` before proceeding.
If the guard is `None` (single-instance mode, no binding table), the
check is skipped. `QueryChunk` and `ListChunks` are read-only and
bypass the guard (a stale read is acceptable).

On `NotMyRange`, the service layer returns gRPC `FAILED_PRECONDITION`
with `NotMyRangeHint` details (the current owner's range + endpoint,
looked up from the guard's snapshot). `ChunkdbService` carries an
optional `range_guard` field for hint construction.

### 3.3 Edge cases

- `RangeGuard` empty (instance just started, binding not loaded yet) →
  reject all mutating RPCs with `NoBinding` error until the first
  binding table load completes, unless `allow_all_when_empty` is
  `true` (default — processes requests when no binding table exists).
- Instance owns multiple disjoint ranges → `check` scans all ranges
  (small list, linear scan is fine).

## 4. Client Routing Integration

`lib/crow-chunkdb-client/src/client.rs`:

- `ChunkdbClient` gains a `range_binding: Option<RangeBindingClient>`
  field. `None` preserves "any instance" behavior; `Some` enables
  range-based routing.
- `client_for_chunk(chunk_id)` — if `range_binding` is `Some`, call
  `binding.route(chunk_id)` to get the endpoint; otherwise fall back
  to `first_endpoint()`.
- `with_retry` loop: on `NotMyRange` status, call `binding.refresh()`,
  re-route, retry (counted against `max_retries`, like
  `NotLeaderHint`).
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
`BindingMonitor` automates this — without it, the operator must
manually write bindings.

`app/crow-chunkdb/src/binding_monitor.rs`:

```rust
pub struct BindingMonitor {
    kv: Arc<CrowkvClient>,
    svc: ServiceRegistryClient,
    interval: Duration,
}

impl BindingMonitor {
    pub fn new(kv, svc, interval) -> Self;
    pub async fn tick(&self) -> Result<()>;
    pub async fn run(self, stop: watch::Receiver<bool>);
}
```

`tick` reads chunkdb instances from the service registry, computes a
uniform range assignment, and writes the binding table to group-0
(delete existing `/chunkdb/range_bind/` entries, then write new ones).
`run` ticks periodically until the stop signal fires.

The monitor is a library module. Wiring into `crow-kv-server`'s
group-0 leader is deferred — the operator can manually write the
binding table via direct group-0 puts, or a future admin CLI command.

### 5.1 Range assignment algorithm

`compute_assignment(instances: &[(u64, InstanceValue)]) -> Vec<ChunkdbRangeBindingValue>`:

- Sort instances by `instance_id` (deterministic).
- Divide `[0, 65535]` into `N` equal inclusive ranges where
  `N = instances.len()`. Each instance gets
  `[i * 65536 / N, (i+1) * 65536 / N - 1]` (inclusive).
- On instance join: recompute (ranges shrink for existing instances).
- On instance leave: recompute (ranges grow for surviving instances).

This is the simplest correct algorithm. Split/merge for load balancing
is a follow-up — the monitor does uniform division.

### 5.2 Edge cases

- Zero chunkdb instances → monitor writes empty binding table; clients
  fall back to "any instance" or error.
- One instance → full range `[0, 65535]` → that instance.
- Instance heartbeat expiry → `read_all_instances` filters expired
  entries (TTL 15s); the monitor only sees live instances.

## 6. DiskdbClientPool Precise free_blocks Routing

`allocator/pool.rs` `free_blocks` groups segments by disk-group (via
`disk_id → dg_id` reverse lookup) and sends each group's free RPC to
the owning instance only. A `disk_id_to_dg: DashMap<DiskId, u64>`
cache is populated from the topology cache's `DiskGroupEntry` list
(each entry has `disk_ids`). `update_disk_id_lookup` is called by the
topology refresh loop.

Fallback: if the reverse lookup misses (cache cold or `disk_id`
unknown), the segments are broadcast to all channels (preserves
correctness — the owning instance accepts the free, others reject).

## 7. Server Wiring

1. `main.rs` startup → create `RangeBindingClient::from_shared(kv)`.
2. Call `refresh()` to load the chunkdb instance binding table from
   group-0. If empty, log warning + use `allow_all_when_empty` mode.
3. Create `RangeGuard` from the binding table (filter for this
   instance's owned ranges via `load_from_group0`).
4. Pass `RangeGuard` into `LifecycleHandler::with_range_guard()` and
   `ChunkdbService::with_range_guard()`.
5. Spawn `RangeBindingClient::spawn_notifier()` to keep the binding
   cache fresh.
6. (Deferred) Spawn `BindingMonitor` in group-0 leader.

## 8. Configuration

`chunkdb_config.rs`:

- `RangeGuardConfig { allow_all_when_empty: bool }` (default `true`).
  When `true`, an empty range guard allows all requests — preserving
  single-instance behavior before the binding table is loaded. When
  `false`, an empty guard rejects all mutating requests until the
  binding table is loaded.
- Added under `ChunkdbConfig.range_guard`.

## 9. References

- Root design: [`design-crow-chunkdb.md`](design-crow-chunkdb.md)
  §3.6 (stateless), §5.4a (hash bucket system), §5.4b (migration).
- Group-0 sysdata API: [`../kv/design-crow-kv-group0.md`](../kv/design-crow-kv-group0.md) §2.1.
- `RangeBindingClient`: `lib/crow-kv-client/src/range_binding.rs`.
- `RangeGuard`: `app/crow-chunkdb/src/range_guard.rs`.
- `BindingMonitor`: `app/crow-chunkdb/src/binding_monitor.rs`.
- `NotMyRangeHint`: `lib/crow-protocol/src/proto/chunkdb_type.proto`.
- `ChunkdbRangeBindingValue`: `lib/crow-protocol/src/proto/sysdata_type.proto`.
