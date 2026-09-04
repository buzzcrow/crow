<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Diskdb Behavioral Conformance (R130)

This draft expands [R130](../backlog/R130-diskdb-behavior-conformance.md)
against `doc/design/diskdb/design-crowdb-diskdb.md` §3, §5, and §8–§19.
The allocator, recovery engines, diskdb client, group-0 clients, and the
initial least-loaded owner picker are already landed. Architecture
decisions and rationale are in the root design; this doc does not repeat
them.

## 1. Disk-Group Creation and Ownership

### 1.1 Why

`http_add_node_disk_group` currently persists the disk-group before it
calls `auto_assign_owner`. Failure to discover an instance or write the
owner is logged, while the HTTP request still returns `201 Created`.
`HardwareClient::set_owner` is an unconditional put, so the public owner
endpoint can replace an existing owner. The pure least-loaded picker is
correct for a single caller but concurrent callers can read the same
counts and select the same instance.

### 1.2 Owner creation contract

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

b. Selection and owner creation are serialized by group-0 state, not a
process-local lock.

c. A duplicate disk-group or owner key returns conflict without
overwriting either value.

d. The management handler returns success only after the group-0
operation commits. Its local console configuration is committed only for
the same successful outcome, or compensated on a later failure.

Edge cases:

- Concurrent creates retry a group-0 conflict with a fresh owner count.
- A crashed caller leaves either a complete committed pair or a durable
  reservation that another caller can finish or remove.
- An expired owner remains the owner; expiry affects availability, not
  assignment.
- Owner replacement remains unsupported.

### 1.3 Unresolved concurrency primitive

The current KV API supports blind put/delete and atomic `batch_write`, but
not conditional create or compare-and-set. `batch_write` can commit a
disk-group and chosen owner together, yet cannot prove that the owner
counts used for selection are still current. Exact balance under
concurrent creation therefore needs one of:

- R101 conditional writes, using a revision-guarded assignment counter or
  reservation key. This reuses a general primitive but makes R130 depend
  on an unfinished requirement.
- A group-0-specific `CreateDiskGroupWithOwner` state-machine operation.
  This gives one linearization point and a narrow surface, but adds a new
  KV RPC and server behavior solely for sysdata assignment.
- Sequential management semantics. The console rejects or queues
  concurrent creates. This is simplest but requires distributed console
  leader ownership or a lock; a process-local blocking lock is forbidden
  and would not protect multiple console instances.

No implementation proceeds until this choice is made.

## 2. Group-0 Reconciliation

### 2.1 Why

`KeepAlive::observe_ownership` reads owner and bind maps separately and
publishes a new `DdbDiskGroup` before zones are loaded. The review must
preserve last-known-good state on partial reads and keep an owner record
without a bind non-writable.

### 2.2 Reconciliation generation

Build a complete observed view before mutating the container. Each new
group carries a load generation. Zone-load completion publishes only if
the group, owner, and bind still match that generation. Watch events wake
the same reconciliation path; polling remains the convergence fallback.

Edge cases:

- Missing bind keeps the group pending and marks sync degraded.
- Failed owner, bind, or hierarchy read publishes no partial delta.
- Duplicate notifications are harmless.
- Lease renewal for the same owner does not restart zone loading.

## 3. Feature Test Organization

### 3.1 Why

The public-path behavior is concentrated in one full-flow test, while
server tests are named by implementation module. Feature-named client
E2E files make the design contract reviewable and independently runnable.

### 3.2 Layout

Move public behavior to client E2E files for startup/sync, owner creation,
allocation lifecycle, recovery/compaction, hardware lifecycle,
query/metrics, scanner/admin, and endpoint retry. Server tests retain
internal failure injection and pure engine coverage under matching names.

## 4. Console CLI Benchmark

### 4.1 Why

The existing concurrent diskdb workload is embedded in a correctness
test. It cannot select the storage backend, control lifecycle, distinguish
capacity exhaustion from deadline, or emit the common benchmark result.

### 4.2 Command surface

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

### 4.3 Statistics and verification

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

High. Concurrent balanced owner creation needs a distributed
linearization point that the current API lacks. Generation-safe sync and
benchmark verification cross group-0, three data groups, diskdb processes,
and the client while preserving the lock-free hot-path constraint.

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

- Register three diskdb instances, create 13 disk-groups, and assert one
  immutable owner per group with sorted counts `[4,4,5]`.
- Create disk-groups concurrently and assert owner-count spread at most
  one after every acknowledged create.
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

## Open Questions

- Which group-0 linearization primitive should R130 use for concurrent
  least-loaded owner creation: depend on R101 conditional writes, add a
  dedicated group-0 operation, or require a distributed single-writer
  management coordinator? The existing blind-put API cannot meet the
  accepted concurrent balance and immutability contract.
