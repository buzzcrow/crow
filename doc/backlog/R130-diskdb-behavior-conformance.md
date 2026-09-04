<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R130: diskdb — Behavioral Conformance Review

## Problem

### Current behavior + impact

The diskdb implementation covers the designed allocator, persistence,
recovery, health, metrics, scanner, client, and RPC surfaces, but the
coverage is organized mainly by implementation module. There is no
single feature-by-feature conformance review proving that the running
component obeys the design across process and group-0 boundaries.
Passing unit tests therefore does not establish the behavior an
operator or diskdb client observes.

The highest-risk gaps are in control-plane changes:

- The group-0 sync loop reads owner and bind maps separately, applies
  them to live state, and may add, update, or remove a disk-group while
  requests and background tasks are active. The required behavior for
  partial reads, missing binds, map changes during initial zone load,
  watch/poll overlap, and stale observations is not covered end to end.
- Exclusive ownership is the reason data-group writes do not use KV
  CAS. A newly created disk-group therefore needs exactly one diskdb
  instance owner before creation is acknowledged. The current web path
  chooses the live instance with the fewest owner records, but logs and
  ignores assignment failure, does nothing when no instance is live,
  and exposes a public endpoint that can overwrite an existing owner.
  Concurrent creates can also choose from the same owner snapshot and
  produce avoidable imbalance.
- Startup and allocation contain uncovered correctness failures. An
  owned group without a bind defaults to `(0, 0)`, startup launches both
  per-disk and whole-group zone loaders, failed recovery substitutes
  empty zones and promotes them to `Up`, and an incomplete multi-block
  allocation loses access to its bitmap claims instead of rolling them
  back.
- Client routing and service-state reporting can remain stale. Endpoint
  refresh only inserts cache entries, `NotOwner` is not retryable,
  `commit_blocks` has no public client wrapper, commit omits the degraded
  check used by allocate/free, and `/ready` reports `ready: true` while
  degraded.
- Ownership is immutable after creation in this requirement. There is
  no owner handoff, owner rebalance, KV-group rebinding, or record
  migration. R102 remains the only requirement for KV-group binding
  and data migration.
- The current component E2E coverage is concentrated in a long
  `diskdb_full_flow_test.rs`, while server tests mix unit, integration,
  and process behavior. Failures are hard to map back to a feature or
  design invariant. The existing concurrent workload also runs inside
  the full-flow correctness test instead of being an independently
  repeatable benchmark contract.

These gaps can cause split ownership, writes through a stale bind,
empty or partially recovered in-memory zones, leaked or double-
allocated blocks, incorrect status write-back, misleading capacity
reports, or tests that pass without exercising the public client path.

### Design pointers

- `doc/design/diskdb/design-crowdb-diskdb.md` §3.1–§3.4, §5,
  §8–§12, and §17–§19.
- `doc/design/diskdb/design-crowdb-diskdb-zone-management.md` §3–§10.
- `doc/design/diskdb/design-crowdb-diskdb-space-metrics.md` §1–§12.
- `doc/design/kv/design-crowdb-kv-group0.md` §2.6 and §2.8.
- `doc/backlog/R102-diskdb-dynamic-binding-migration.md` defines the
  data-migration boundary excluded from this requirement.

### Use scenarios

- **Normal startup** — an operator starts diskdb after group-0 contains
  hardware, owner, and bind records. The instance registers itself,
  discovers only its owned disk-groups, loads every disk's zones from
  the bound data group, becomes ready, and serves client operations.
- **Group-0 interruption** — heartbeat or metadata reads fail for
  several ticks. The instance enters degraded mode without discarding
  last-known-good ownership or accepting a partially applied metadata
  view, then recovers after a complete successful sync.
- **Balanced creation** — three live diskdb instances exist and the
  operator creates 13 disk-groups. Each creation writes one immutable
  owner record; the final ownership counts are 5, 4, and 4.
- **Creation without an owner** — no diskdb instance is eligible, or
  the owner write fails. Disk-group creation is not reported as
  successful and no ownerless usable disk-group remains.
- **Owner modification attempt** — an operator or client tries to
  assign a different owner to an existing disk-group. The request is
  rejected and the original owner record remains unchanged.
- **Disk lifecycle and restart** — disks are added, disappear, recover,
  enter maintenance, or fail zone loading. Runtime status, group-0
  status, allocation eligibility, recovery scans, and restart behavior
  remain consistent.
- **Allocator lifecycle** — a client allocates, commits, frees,
  compacts, restarts diskdb, rebuilds a bitmap, and queries usage. Busy
  records remain authoritative and freed capacity is reclaimed only by
  the designed compaction path.
- **Operational verification** — scanner, recalc, metrics, health, and
  configuration reload are exercised through their public surfaces.
- **Allocate benchmark** — an operator runs `crowdb-cli bench diskdb
  allocate` against the three-node cluster produced by cluster initialization
  with four disks per node. Loaders allocate until all disk space is
  exhausted or the configured time limit is reached, then the command
  reports performance and verifies space accounting.
- **Mixed benchmark** — an operator runs `crowdb-cli bench diskdb mix`
  against the same topology. Each operation is selected using a 70%
  allocate and 30% free distribution; frees draw only from blocks
  successfully allocated by that run. The command stops at its time
  limit and verifies the final live allocation set against diskdb
  live-set accounting and capacity statistics. Free-only benchmark mode is not
  supported.

## Solution

Audit diskdb by documented feature, fix each confirmed mismatch, and
make the public `DiskdbClient` E2E suite the executable behavioral
contract.

### Work items

1. **Feature conformance inventory** —
   `doc/design/diskdb/`, `app/crowdb-diskdb/src/`, and
   `lib/crowdb-diskdb-client/src/`: trace each designed feature to its
   implementation entry point, invariant, error behavior, and test.
   Record confirmed implementation gaps in this requirement's design
   draft before changing code; correct the permanent design first when
   observed behavior is intentionally different.
2. **Atomic group-0 view and reconciliation** —
   `app/crowdb-diskdb/src/liveness/keepalive.rs`, `notify.rs`, and
   `main.rs`: define and enforce how heartbeat, owner records, bind
   records, hardware hierarchy, watch notifications, and polling form
   one usable sync result. Never publish a newly owned group without a
   valid bind and loaded zones, or replace last-known-good state with a
   partial read.
3. **Balanced owner assignment at creation** —
   `app/crowdb-web/src/lifecycle.rs`,
   `lib/crowdb-console-shared/src/ops/hardware.rs`, and
   `lib/crowdb-kv-client/src/hardware.rs`: make owner selection and
   owner-record creation part of the disk-group creation result. Select
   the eligible live diskdb instance with the fewest owned disk-groups,
   break ties by stable instance ID, and make owner persistence mandatory
   before acknowledging creation. Thirteen sequential creations across
   three eligible owners must converge to 5/4/4. Concurrent management
   requests are outside the exact-balance guarantee until group 0 gains
   conditional writes.
4. **Immutable owner enforcement** —
   `lib/crowdb-kv-client/src/hardware.rs`,
   `app/crowdb-web/src/diskdb.rs`, and console clients: separate
   create-owner from refresh/read behavior and reject replacement of an
   existing owner by API or internal caller. Lease refresh, if retained,
   may extend only the same owner's record and must not select a new
   owner. Keep the assignment path lock-free; any proposed blocking lock
   requires explicit review before implementation.
5. **Allocator, durability, and recovery review** —
   `app/crowdb-diskdb/src/model/`, `ddb_kv_client.rs`, and
   `recovery/`: verify allocation rollback, commit/free owner checks,
   persist-only free, compaction watermarks, startup load, full-scan
   fallback, journal replay, and crash points against the record-source-
   of-truth invariants.
6. **Hardware lifecycle and background behavior review** —
   `app/crowdb-diskdb/src/liveness/`, `scanner/`, `metrics/`, and
   `bg_task.rs`: verify effective status propagation, group-0
   write-back, allocation eligibility, recovery task cancellation,
   scanner coordination, reporting, degraded readiness, and live
   configuration behavior.
7. **Feature-grouped component E2E suite** —
   `lib/crowdb-diskdb-client/tests/` and
   `lib/crowdb-test-harness/src/diskdb.rs`: split public-path E2E cases
   into feature files for sync/startup, ownership,
   allocation lifecycle, recovery/compaction, hardware status,
   query/metrics, scanner/admin, endpoint discovery/retry, and failure
   injection. Keep implementation-level tests in
   `app/crowdb-diskdb/tests/`, grouped by the same feature names.
8. **Console CLI diskdb benchmark** —
   `app/crowdb-cli/src/commands/bench/` and
   `lib/crowdb-test-harness/src/diskdb.rs`: add `crowdb-cli bench
   diskdb allocate` and `crowdb-cli bench diskdb mix`, following the
   existing KV benchmark command structure and result format. The
   cluster initialization creates three KV/diskdb nodes, one disk-group
   per node, and four disks in each group; the command validates that
   topology before starting. It supports `--mode mem|block` for the
   configured CROWDB KV backing store and drives requests across all three groups.
   Allocate mode stops when capacity is exhausted or `--duration-secs`
   expires. Mix mode uses a deterministic 70/30 allocate/free selection
   and stops at the deadline; there is no standalone free mode. Report
   per-operation throughput and latency, stop reason, successful/failed operation
   counts, allocated/freed/live units, and capacity before/after. Verify
   the non-duplicated live segment set and compare its expected bytes
   with reported busy/free/capacity totals after compaction.

### Flow diagram

```
disk-group create + live diskdb registry + current owner map
                             │
                             ▼
                  choose least-owned instance
                             │
                      create owner once
                       │            │
                    success      conflict/failure
                       │            │
          acknowledge disk-group   reject/compensate
                       │
            diskdb sync + zone load + serve
```

### Edge cases at a glance

- Owner exists but bind is absent or invalid → retain no writable
  runtime group; report degraded sync and retry.
- Owner and bind records are created between separate group-0 reads →
  do not publish the mixed view; retry from a stable revision.
- Notification is lost, duplicated, or reordered → polling converges
  to the same state and reconciliation stays idempotent.
- No live eligible diskdb instance exists → creation fails without an
  acknowledged ownerless disk-group.
- Existing owner is replaced with a different instance ID → reject the
  write and retain the original value.
- Existing owner renews its lease → accept only the expiry update; the
  instance ID cannot change.
- Benchmark reaches full capacity before its deadline → stop normally
  with reason `space_exhausted`, preserving results for verification.
- Benchmark reaches its deadline with capacity remaining → stop normally
  with reason `time_limit` and drain in-flight operations before
  verification.
- Mixed workload chooses free while no live allocation is available →
  choose allocate instead; never issue a fabricated or duplicate free.
- Process crashes after bitmap claim but before busy-record durability →
  rollback or restart reconstruction cannot expose an unpersisted busy
  block as durable.
- Group-0 reports `Up` after zone-load failure → write back `Offline`
  before local transition and remain non-allocatable across sync.

## Dependencies

- **R102 is explicitly out of scope.** It owns KV-group binding
  changes, record copy, migration cutover, and cleanup. R130 neither
  implements nor tests that migration flow. R130 also does not change
  disk-group owners after creation.
- Uses the existing group-0 `HardwareClient`,
  `ServiceRegistryClient`, watch/notify path, diskdb RPC transport, and
  test harness. Creation uses the existing group-0 batch surface; exact
  balance is required for the serialized management workflow.
- R79 free batching and R80 rebalance are separate performance and
  placement requirements. Their eventual behavior should follow the
  feature test layout established here but is not required to complete
  this audit unless already implemented code is encountered.

## Acceptance

### Group-0 synchronization and startup

- Group-0 contains one complete owned disk-group with a valid bind and
  disks → start diskdb → wait through sync and zone load → readiness is
  `Up`, only that group is served, and its bitmap matches durable busy
  records. E2E test.
- Owner record exists while its bind is absent → trigger initial and
  periodic sync → assert the group never becomes writable, readiness or
  metrics expose degraded state, and adding the bind later causes a
  complete load before activation. E2E test.
- Inject failure after the owner read but before the bind or hardware
  read → sync → assert no partial additions, removals, or bind changes
  are published and last-known-good groups remain intact. Integration
  test.
- Deliver duplicate and reordered watch events, then suppress one
  event → run the polling safety net → assert final runtime state equals
  the latest complete group-0 view and each transition is applied once.
  Integration test.
- Refresh the same owner's lease while initial zone load is blocked →
  release the load → assert the group activates once under the original
  owner and the refresh does not restart or duplicate loading.
  Integration test.

### Disk-group owner assignment

- Register three eligible diskdb instances, create 13 disk-groups
  sequentially through the management API, then list owner records →
  assert every disk-group has exactly one owner and counts sorted by
  instance are 4, 4, and 5. E2E test.
- Create a disk-group when no diskdb instance is eligible → assert the
  operation fails, no owner record exists, and no usable disk-group is
  reported as successfully created. E2E test.
- Inject failure while writing the selected owner record → create a
  disk-group → assert creation fails and compensation leaves no
  acknowledged ownerless disk-group. Integration test.
- Pre-create an owner record for instance A, then call the owner API and
  `HardwareClient` with instance B → assert conflict, preserve A and its
  lease, and perform no diskdb data-group write. Integration test.
- Pre-create an owner record for instance A, then refresh its lease as A
  → assert only `lease_expiry_ms` advances and ownership stays A.
  Integration test.
- Create a disk-group owned by A, remove A from the live service
  registry, and run sync → assert ownership remains A and the system
  reports the unavailable owner without automatic reassignment. E2E
  test.

### Allocation, free, compaction, and recovery

- Allocate and commit blocks across eligible disks with exclusions →
  verify non-overlap, owner and segment fields, durable busy records,
  and exclusion enforcement; free them → verify busy deletion plus free
  records while bitmap usage remains conservative until compaction.
  E2E test.
- Free with the wrong owner when validation is enabled, double-free, or
  free an unknown/out-of-range segment → assert the documented error and
  no mutation of records, bitmap, or counters. E2E test.
- Inject KV failure after in-memory allocation and before busy-record
  acknowledgement → retry/restart → assert the claim is rolled back or
  reconstructed solely from durable records and is never reported as a
  successful allocation. Integration test.
- Compact a non-active zone containing old and new free records →
  restart → assert snapshot and free-record deletion are atomic,
  watermark rules prevent double-free, and reclaimed ranges are
  allocatable exactly once. E2E test.
- Make journal replay unavailable or invalid while full-scan data is
  valid → restart/rebuild → assert full-scan fallback restores the same
  bitmap; corrupt both paths → assert the disk is written `Offline` in
  group 0 and remains non-allocatable. E2E test.

### Hardware lifecycle, scanner, and metrics

- Add an `Init` disk with existing records → sync → assert it is not
  allocatable until zone load completes, then becomes eligible with its
  busy ranges preserved. E2E test.
- Exercise Up→Suspect→Missing/Offline→Bad and recovery→Up with node,
  disk-group, and disk status combinations → assert effective status,
  group-0 write-back, allocator membership, and recovery task behavior
  at each transition. E2E test.
- Run scanner and recalculation after deliberately introducing ghost,
  drift, integrity, and owner-mismatch cases → assert findings are
  classified by feature, no automatic destructive repair occurs where
  the design says report-only, and a requested rebuild restores exact
  usage. E2E test.
- Allocate/free/compact while collecting query responses, heartbeat
  summaries, and metrics → assert capacity equals busy plus free at each
  stable observation, zone bitmap is returned only for zone detail, and
  counters follow acknowledged durable-bound operations. E2E test.
- Reload every documented dynamic config field and attempt to reload a
  restart-only field → assert dynamic behavior changes on the next task
  cycle and restart-only state is unchanged with a diagnostic.
  Integration test.

### Client E2E organization and benchmark

- Each public behavior above is located in a feature-named file under
  `lib/crowdb-diskdb-client/tests/`; run the files independently → each
  provisions and tears down its own cluster state without order
  dependence. E2E test.
- Server-internal tests under `app/crowdb-diskdb/tests/` use the same
  feature vocabulary and do not substitute for a required client E2E
  case → review the test manifest and trace every design feature to at
  least one public-path case. Integration test.
- Run `crowdb-cli bench diskdb allocate --mode mem` with a short
  duration → initialize three nodes with four disks each → assert all 12
  disks participate, stop reason is `time_limit` or `space_exhausted`,
  and successful allocated units equal the durable live records and
  verified busy space. Integration test.
- Run `crowdb-cli bench diskdb allocate --mode block` with capacity
  smaller than the workload demand → assert the run stops with
  `space_exhausted`, no successful allocations overlap, and after
  verification `busy + free == capacity` for every disk and disk-group.
  Integration test.
- Run `crowdb-cli bench diskdb mix --mode mem` with fixed seed and enough
  capacity → assert the attempted operation distribution converges to
  70% allocate and 30% free, every free references a currently live
  segment exactly once, and `successful_allocated_units -
  successful_freed_units == final_live_units`. Integration test.
- Complete a mixed run → compare its tracked live set with busy records,
  run compaction/recalculation to account for persist-only free, and
  query cluster/disk-group/disk statistics → assert record counts and
  all capacity totals agree; any mismatch increments correctness errors
  and fails the command. Integration test.
- Run either mode with a short deadline while operations are in flight →
  stop issuing work, drain responses, then verify → assert the report
  counts every acknowledged allocation/free exactly once and identifies
  `time_limit` as the stop reason. Integration test.
- Invoke `crowdb-cli bench diskdb free` → clap rejects the unsupported
  workload and directs users to `diskdb mix`. Integration test.
- Run benchmark smoke mode twice against the same build → assert each
  run creates and tears down its three-node, 12-disk topology, output is
  machine-readable, and correctness failures are distinct from the
  performance comparison. Integration test.

### Test commands

- `pixi run -- cargo build -p crowdb-kv-server -p crowdb-diskdb`
- `pixi run test-diskdb-client`
- `pixi run test-diskdb`
- `pixi run test-protocol`
- `pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb`
- The `crowdb-cli bench diskdb allocate --mode mem` smoke command added
  by work item 8.
- The `crowdb-cli bench diskdb mix --mode mem` smoke command added by
  work item 8.
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
