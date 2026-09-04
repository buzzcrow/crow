<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Diskdb Behavioral Conformance (R130)

This draft expands [R130](../backlog/R130-diskdb-behavior-conformance.md)
against `doc/design/diskdb/design-crowdb-diskdb.md` §3, §5, and §8–§19.
The allocator, recovery engines, diskdb client, group-0 clients, and the
initial least-loaded owner picker are already landed. Architecture
decisions and rationale are in the root design; this doc does not repeat
them.

## 1. Review Findings

### 1.1 Correctness blockers

- `DdbDiskGroup::new` defaults its bind to `(0, 0)`, and
  `KeepAlive::observe_ownership` publishes an owned group even when its
  bind entry is absent. `disk_add_init` can therefore read and write zone
  records in the system group.
- The initial keepalive tick spawns per-disk zone loads. `main` then
  starts `run_zone_load` for the same groups and replaces their container
  entries, leaving the first tasks to mutate detached groups.
- `ZoneLoader::load_disk_group` substitutes empty zones when journal
  replay and full scan both fail, then promotes the disk and process to
  `Up`.
- `DdbDiskGroup::allocate_blocks` can claim some bitmap ranges and then
  return `NoSpace` without returning or rolling back those claims. The
  orchestration layer's partial-success branch is unreachable.

### 1.2 Control-plane and client gaps

- Heartbeat runs before ownership reconciliation and advertises the
  previous tick's groups and usage. Disk-list read failures can still
  finish as a successful sync and clear degraded mode; missing or failed
  node/group reads are treated as `Up`.
- Offline status write-back is best-effort. If it fails, a following sync
  sees group-0 `Up` and can recover a disk whose zone load failed.
- `DiskdbClient::refresh_endpoints` only inserts routes, leaving removed
  group and disk mappings cached. `NotOwner` is not retryable.
- The transport and server support commit, but `DiskdbClient` has no
  `commit_blocks` method. Commit also omits the degraded gate used by
  allocate and free.
- `/ready` says readiness requires `Up` and non-degraded, but returns
  HTTP 200 with `ready: true` for degraded instances.

### 1.3 Structure, concurrency, and tests

- Allocation reads `RwLock`-protected status, bind, disk, active-zone,
  and zone-health state. This conflicts with the lock-free hot-path rule.
- `keepalive.rs` and `diskdb_rpc_service.rs` exceed 1,000 lines, while
  touched modules still contain inline unit tests.
- Client E2E tests duplicate the same full RPC flow and silently return
  success when required binaries are missing.
- The current benchmark is a fixed allocate/free loop inside a client
  correctness test. It has no topology lifecycle, storage mode, stop
  reason, latency distribution, or durable accounting.

Both current relevant suites pass: `pixi run test-diskdb` and
`pixi run test-diskdb-client`. They do not cover these failures.

## 2. Disk-Group Creation and Ownership

### 2.1 Why

`http_add_node_disk_group` currently persists the disk-group before it
calls `auto_assign_owner`. Failure to discover an instance or write the
owner is logged, while the HTTP request still returns `201 Created`.
`HardwareClient::set_owner` is an unconditional put, so the public owner
endpoint can replace an existing owner. The pure least-loaded picker is
correct for a single caller but concurrent callers can read the same
counts and select the same instance.

### 2.2 Owner creation contract

The creation operation has one externally visible outcome: a disk-group
and its immutable owner both exist, or creation fails and neither is
usable. Eligible owners are live `/srv/diskdb/` instances. Selection uses
the fewest committed owner records, with the lowest instance ID as the
tie-breaker. Owner renewal may change `lease_expiry_ms` only when the
instance ID matches.

The group-0 API needs these semantic operations:

```rust
pub async fn create_disk_group_with_owner(
    rack_id: RackId,
    node_id: NodeId,
    dg_id: DiskGroupId,
    disk_group: &DiskGroupValue,
    eligible_instances: &[InstanceId],
) -> Result<DiskdbOwnerEntry>;

pub async fn renew_owner(
    rack_id: RackId,
    node_id: NodeId,
    dg_id: DiskGroupId,
    instance_id: InstanceId,
    lease_expiry_ms: u64,
) -> Result<()>;
```

a. Creation rejects an empty eligible set.

b. Exact least-loaded balance is guaranteed for serialized management
create requests. Concurrent exact balance is deferred until group 0 has
conditional writes.

c. A duplicate disk-group or owner key returns conflict without
overwriting either value.

d. The management handler returns success only after the group-0
operation commits. Its local console configuration is committed only for
the same successful outcome, or compensated on a later failure.

Edge cases:

- A failed group-0 batch leaves neither record. A failed local config
  commit compensates the group-0 pair before returning failure.
- An expired owner remains the owner; expiry affects availability, not
  assignment.
- Owner replacement remains unsupported.

## 3. Allocation and Recovery Safety

### 3.1 Partial claims

The model returns all multi-block claims with a completion result. The
orchestrator rolls back every incomplete attempt before compaction or
failure. Persistence failures remain distinguishable from capacity
exhaustion.

### 3.2 Loader ownership and failure

Startup has one whole-group loader. Later disk discovery uses the
per-disk loader. Neither path substitutes an empty `Up` zone. Failed
loads remain non-writable, and failure status must reach group 0 before a
later sync can treat the disk as recovered.

## 4. Group-0 Reconciliation

### 4.1 Why

`KeepAlive::observe_ownership` reads owner and bind maps separately and
publishes a new `DdbDiskGroup` before zones are loaded. The review must
preserve last-known-good state on partial reads and keep an owner record
without a bind non-writable.

### 4.2 Reconciliation generation

Build a complete observed view before mutating the container. Each new
group carries a load generation. Zone-load completion publishes only if
the group, owner, and bind still match that generation. Watch events wake
the same reconciliation path; polling remains the convergence fallback.

Edge cases:

- Missing bind keeps the group pending and marks sync degraded.
- Failed owner, bind, or hierarchy read publishes no partial delta.
- Duplicate notifications are harmless.
- Lease renewal for the same owner does not restart zone loading.

## 5. Client Routing and Service State

Endpoint refresh replaces both routing maps. `NotOwner`, unavailable,
and transport failures evict and refresh the affected route before a
bounded retry. The client exposes commit, all mutations share one
lifecycle/degraded gate, and degraded readiness returns false with HTTP
503.

## 6. Lock-Free Publication

Group status and bind become atomic values. Disk, active-zone, and
allocatable-disk collections publish immutable `ArcSwap` snapshots;
container membership uses a lock-free map. Zone health becomes atomic.
Recovery-only bookkeeping may retain a short lock only after explicit
review because it is outside allocation and RPC hot paths.

## 7. Feature Test Organization

### 7.1 Why

The public-path behavior is concentrated in one full-flow test, while
server tests are named by implementation module. Feature-named client
E2E files make the design contract reviewable and independently runnable.

### 7.2 Layout

Move public behavior to client E2E files for startup/sync, owner creation,
allocation lifecycle, recovery/compaction, hardware lifecycle,
query/metrics, scanner/admin, and endpoint retry. Server tests retain
internal failure injection and pure engine coverage under matching names.

## 8. Console CLI Benchmark

### 8.1 Why

The existing concurrent diskdb workload is embedded in a correctness
test. It cannot select the storage backend, control lifecycle, distinguish
capacity exhaustion from deadline, or emit the common benchmark result.

### 8.2 Command surface

```text
crowdb-cli bench diskdb allocate --mode mem|block
crowdb-cli bench diskdb mix --mode mem|block
```

Both commands create three KV/diskdb nodes, one disk-group per node, and
four disks per group. Requests rotate across all three groups. Common
arguments include duration, loaders, allocation unit count, seed, and
metrics interval.

Allocate mode issues allocation requests until the deadline or the first
confirmed cluster-wide capacity exhaustion. Mix mode selects operations
with a deterministic 70/30 allocate/free distribution. A free removes one
segment from the run's live set before dispatch and restores it if the RPC
fails, preventing duplicate concurrent frees. If the live set is empty, a
selected free becomes an allocate. There is no free-only subcommand.

### 8.3 Statistics and verification

The report contains effective arguments, stop reason, elapsed time,
throughput and latency per operation, RPC errors, correctness errors,
allocated units, freed units, live units, and capacity snapshots.

After loaders drain, verification compares the tracked live segments with
durable busy records. Because free is persist-only in memory, the verifier
then compacts/recalculates before asserting `busy + free == capacity` at
disk and disk-group levels and that busy units equal tracked live units.
Any mismatch makes the command fail independently of performance.

## Scope

- `lib/crowdb-kv-client/src/hardware.rs` — atomic owner creation and
  same-owner lease renewal after the concurrency primitive is chosen.
- `app/crowdb-web/src/lifecycle.rs` — mandatory balanced assignment as
  part of disk-group creation.
- `app/crowdb-web/src/diskdb.rs` — reject owner replacement.
- `lib/crowdb-console-shared/src/` — expose creation failures and remove
  mutable-owner behavior.
- `app/crowdb-diskdb/src/liveness/keepalive.rs` — complete-view
  reconciliation and load generation.
- `app/crowdb-diskdb/src/main.rs` — generation-aware initial loading.
- `lib/crowdb-diskdb-client/tests/` — feature-grouped component E2E.
- `app/crowdb-diskdb/tests/` — matching internal integration coverage.
- `app/crowdb-cli/src/commands/bench/` — diskdb benchmark verbs,
  lifecycle, workloads, results, and verification.
- `lib/crowdb-test-harness/src/diskdb.rs` — three-node/12-disk fixture.
- `doc/design/diskdb/design-crowdb-diskdb.md` — folded ownership,
  reconciliation, test, and benchmark behavior.

## Complexity

High. Recovery and publication changes must preserve object lifetime
while group-0 sync, background tasks, and RPCs overlap. Benchmark
verification crosses group 0, three data groups, diskdb processes, and
the client.

## Test Design

### Unit tests

- **Owner selection:** counts `[4,4,4]` plus one create select the lowest
  ID; 13 sequential choices across three IDs finish `[5,4,4]`.
- **Immutable owner:** create A, attempt create/renew as B, assert conflict
  and unchanged value; renew as A, assert expiry-only update.
- **Sync generation:** complete load under generation N after generation
  N+1 is observed, assert N cannot publish.
- **Mix selection:** fixed seed produces the requested 70/30 distribution;
  empty live set converts free to allocate; failed free restores exactly
  one segment.
- **Accounting:** generated acknowledged alloc/free sequence yields
  `allocated - freed == live`, with duplicate or unknown segments flagged.

### End-to-end tests

- Register three diskdb instances, create 13 disk-groups serially, and assert one
  immutable owner per group with sorted counts `[4,4,5]`.
- Fail owner creation and assert the management request fails without an
  acknowledged ownerless group.
- Start diskdb from a complete owner/bind/hardware view and assert it
  becomes writable only after zone load; inject each partial read and
  assert last-known-good state remains.
- Run allocate benchmark in memory and block modes, stopping at deadline
  and exhaustion respectively, and verify all 12 disks plus all capacity
  totals.
- Run deterministic mixed mode, verify no duplicate free, reconcile
  persist-only frees, and assert durable records equal the final live set.

## Module Structure

```text
lib/crowdb-kv-client/src/hardware.rs       # owner creation semantics
app/crowdb-web/src/lifecycle.rs            # balanced creation workflow
app/crowdb-web/src/diskdb.rs               # immutable owner API
app/crowdb-diskdb/src/liveness/keepalive.rs # complete-view sync
lib/crowdb-diskdb-client/tests/            # feature E2E contracts
app/crowdb-cli/src/commands/bench/
  verb.rs                                  # diskdb subcommands and args
  diskdb.rs                                # lifecycle + dispatch
  diskdb_allocate.rs                       # exhaustion/deadline workload
  diskdb_mix.rs                            # 70/30 live-set workload
  diskdb_verify.rs                         # records + space checks
lib/crowdb-test-harness/src/diskdb.rs      # three-node/12-disk fixture
```

## Config Extensions

No server configuration is added. Benchmark topology and workload values
are CLI arguments with stable defaults.

## Server Wiring

1. Management disk-group creation discovers eligible diskdb instances.
2. The chosen group-0 primitive commits the disk-group and owner.
3. Diskdb watch/poll reconciliation observes the complete pair and loads
   zones under a generation.
4. The RPC service becomes writable for the group only after that load
   publishes.
5. Benchmark lifecycle uses the same management and client paths.
