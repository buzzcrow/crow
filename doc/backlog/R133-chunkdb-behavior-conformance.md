<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R133: chunkdb — Behavioral Conformance and Benchmark

## Problem

### Current behavior + impact

ChunkDB implements chunk allocation, append, seal, delete, range delete,
strip replacement, query/list, range routing, topology refresh, mirror/EC
placement, DiskDB allocation, and KV persistence. The implementation has not
received the same feature-by-feature review and production-path benchmark as
DiskDB. Most server tests exercise internal handlers or in-process wiring;
`crowdb-chunkdb-client` contains only retry-policy unit tests and does not act
as the component-level E2E contract.

Several high-risk behaviors cross KV, DiskDB, and ChunkDB boundaries:

- Mirror and EC selection must honor rack/node failure domains while mapping
  every placement entry to the intended disk group. Safe EC mode must never
  silently become unsafe, and fallback behavior must be visible to callers
  and metrics.
- Allocation and append reserve blocks across several DiskDB instances,
  persist one chunk record, then commit blocks. Partial allocation, KV
  failure, commit failure, timeout, and process crash can leave leaked,
  tentative, or unreferenced blocks unless the ordering and recovery contract
  is complete.
- Delete currently frees segments before persisting the Deleted state and
  treats free failures as best-effort. A crash or failed metadata write can
  leave an active chunk referring to freed blocks; the intended durable
  ordering and retry behavior need review.
- Append, seal, delete, range delete, and strip replacement are read-modify-
  write transitions. The process-local lifecycle lock prevents same-instance
  overlap, but correctness also depends on range ownership, cache freshness,
  KV revision handling, and behavior across restart or misrouted requests.
- The current tests do not prove lifecycle and placement through the public
  RPC client against real KV and DiskDB services. There is no benchmark that
  separates ChunkDB throughput from DiskDB/KV cost or verifies the resulting
  metadata and physical-space accounting.

Without this review, successful API responses may conceal unsafe EC
placement, lost appends, premature frees, orphaned allocations, invalid state
transitions, stale routing, or capacity drift. Performance work also lacks a
reproducible full-stack baseline.

### Design pointers

- `doc/design/chunkdb/design-crowdb-chunkdb.md` §3–§10 and §13–§15.
- `doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md` §1–§7.
- `doc/design/chunkdb/design-crowdb-chunkdb-rpc.md` §1–§8.
- `doc/design/diskdb/design-crowdb-diskdb-zone-management.md` §3–§6 and
  §10 defines the allocation, tentative/committed, free, and recovery
  behavior ChunkDB relies on.

### Use scenarios

- **Mirror allocation:** a client allocates a three-copy chunk in a
  three-rack cluster. Every strip uses distinct nodes and racks where
  available, the chunk becomes Active only after durable metadata, and all
  returned segments belong to the requested chunk.
- **EC allocation:** a client allocates 4+2 and 8+4 EC chunks. Safe placement
  respects the per-node failure bound and spreads blocks across racks;
  insufficient topology fails without leaking blocks unless unsafe mode was
  explicitly requested.
- **Append and alter:** concurrent clients append strips, seal, delete a
  range, or replace one strip. Each accepted transition is serialized,
  preserves strip ordering and capacity, and rejects stale or invalid state.
- **Chunk deletion:** a client deletes an Active or Sealed chunk. Metadata
  and every physical segment reach one recoverable final state despite free,
  persistence, response, or restart failures.
- **Routing change:** requests cross several ChunkDB instances and bucket
  ranges. `NotMyRange` refreshes routing and retries the correct owner without
  applying a mutation twice.
- **Restart and reconciliation:** ChunkDB restarts with durable chunks and
  tentative/orphaned DiskDB blocks. It rebuilds caches and reports or repairs
  every mismatch according to the reviewed recovery contract.
- **Allocation benchmark:** an engineer deploys the full stack and measures
  mirror and EC chunk creation with one worker and increasing concurrency,
  retaining service metrics and exact placement/accounting verification.
- **Lifecycle benchmark:** an engineer runs a deterministic configurable
  mix of allocate, append/alter, seal, query, and delete/free operations. The
  run retains a live chunk model and verifies KV metadata plus DiskDB busy
  space after all in-flight operations drain.

## Solution

Audit ChunkDB by externally visible feature, fix confirmed local defects,
make `ChunkdbClient` tests the component E2E boundary, and add a full-stack
benchmark with independent correctness verification.

1. **Feature and invariant review:** `doc/design/chunkdb/`,
   `app/crowdb-chunkdb/src/`, `lib/crowdb-chunkdb-client/src/`, and existing
   tests: trace allocation, placement, lifecycle, persistence, routing,
   topology, RPC error mapping, restart, metrics, and shutdown from public
   request to durable effects. Fix straightforward defects during the review;
   record non-local or architectural work as separate backlog requirements.
2. **Mirror and EC placement conformance:**
   `app/crowdb-chunkdb/src/selector/`, `topology/`, and `allocator.rs`:
   validate distinct failure-domain selection, health/capacity filtering,
   negative constraints, safe/unsafe EC policy, deterministic verification,
   and exact correspondence between a placement plan and returned DiskDB
   segments.
3. **Allocate and append durability:**
   `app/crowdb-chunkdb/src/allocator.rs`, `lifecycle.rs`, and `storage.rs`:
   define and enforce success, rollback, tentative-block commit, idempotency,
   and restart behavior for multi-strip mirror and EC operations. No failure
   may return success with incomplete metadata or silently discard ownership
   of allocated segments.
4. **Alter and delete lifecycle:** `app/crowdb-chunkdb/src/lifecycle.rs` and
   `lifecycle/state.rs`: review append, seal, delete, range delete, and
   `update_chunk_strip` ordering under concurrency and partial failure. The
   durable chunk record must never reference a block that has already been
   made reusable; retry and recovery outcomes must be explicit.
5. **Routing, topology, and service lifecycle:**
   `app/crowdb-chunkdb/src/range_guard.rs`, `routing.rs`, `topology/`,
   `service/`, and `main.rs`: verify complete group-0 views, watch/poll
   convergence, range enforcement, `NotMyRange` retries, cache invalidation,
   service registration, readiness, metrics, and graceful shutdown.
6. **Feature-grouped component E2E suite:**
   `lib/crowdb-chunkdb-client/tests/` and the shared test harness: replace
   retry-only coverage as the primary client suite with independently
   runnable feature files for mirror allocation, EC placement, lifecycle
   alteration, deletion/free, rollback/recovery, routing/topology, query/list,
   concurrency, and failure injection. Server tests use the same feature
   vocabulary for internal faults and pure algorithms.
7. **Combined local deployment:** `lib/crowdb-console-shared/src/ops/cluster.rs`
   and `app/crowdb-cli/src/commands/cluster.rs`: extend CLI deployment to
   start and stop KV, DiskDB, and ChunkDB as one timestamped topology. The
   benchmark profile contains six storage nodes across three racks, two nodes
   per rack, one disk group with four disks per node, at least three KV
   replicas for group 0 and data groups, and three ChunkDB instances with
   complete bucket ownership. Every process writes logs and periodic metrics
   below the same run root.
8. **ChunkDB benchmark command:** `app/crowdb-cli/src/commands/bench/`: add
   `crowdb-cli bench chunkdb allocate` for mirror/EC creation and `crowdb-cli
   bench chunkdb mix` for a seeded, configurable allocate/append/seal/query/
   delete distribution. Support one-thread latency and multi-thread
   saturation runs, explicit EC/copy parameters, duration/exhaustion limits,
   and machine-readable results.
9. **Independent benchmark verification:** benchmark verification and
   `lib/crowdb-test-harness/`: maintain a non-duplicated expected chunk model,
   query durable chunks through `ChunkdbClient`, validate strip sequences and
   placement constraints, then reconcile referenced segments with DiskDB
   records and per-disk/per-group space. Correctness failure invalidates the
   performance sample.
10. **Regression history:** `tools/bench-chunkdb-regression.sh`: deploy a
    fresh combined cluster per case under a datetime-named root; run mirror
    allocate, EC allocate, and lifecycle mix with one and multiple workers;
    retain CLI, ChunkDB, DiskDB, KV, RPC, WAL, storage, and system metric logs.
    Record reviewed baselines as comments in the script without tracking
    generated TSV or log directories.

### Flow diagram

```text
crowdb-cli bench chunkdb
          │
          ▼
ChunkdbClient ──route──► owning ChunkDB instance
                              │
                 topology snapshot + placement
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
      DiskDB block allocation          KV chunk metadata
              │                               │
              └──────── commit/rollback ──────┘
                              │
                              ▼
             client query + independent verifier
                    │                    │
                    ▼                    ▼
             chunk/strip model     DiskDB busy space
                    └──────── exact match ────────┘
```

### Edge cases at a glance

- No healthy placement satisfies mirror or safe EC constraints → reject
  before persistence and free every partial allocation.
- Unsafe EC placement is not explicitly enabled → never fall back silently.
- DiskDB returns fewer segments than requested → retry only under the defined
  idempotency contract; otherwise roll back the complete attempt.
- KV chunk persistence fails after block allocation → free every allocated
  segment or persist a recoverable allocation intent.
- Segment commit fails after chunk persistence → report a recoverable pending
  state; do not present the chunk as fully durable without diagnosis.
- Delete frees blocks but its metadata write fails → the original chunk must
  not remain readable as though its segments were valid.
- Metadata becomes Deleted but one free fails → retry/reconcile the orphan;
  never reuse the chunk ID as a new Active chunk.
- Two appends race on one chunk → both strips survive in one ordered record or
  one request receives a retryable conflict.
- Seal races with append or delete → exactly one valid state-machine ordering
  becomes durable.
- Strip replacement frees the old segment → persist the replacement before
  the old block becomes reusable.
- Topology notification is lost or reordered → polling converges without
  publishing a partial topology snapshot.
- ChunkDB owner route changes during an RPC → refresh and retry without
  duplicating the mutation.
- Benchmark reaches DiskDB `NoSpace` → drain requests and verify all accepted
  chunks before reporting `space_exhausted`.
- Benchmark reaches its deadline → stop issuance, drain every response, and
  include all acknowledged mutations in verification.

## Dependencies

- Uses the ChunkDB lifecycle, crowdb-rpc transport, range binding, DiskDB
  client pool, topology cache, and large-object writer integration already
  present in the repository.
- R101 KV compare-and-set may be required if process-local lifecycle locking
  and range ownership cannot prove single-writer read-modify-write semantics.
  The review must not add a new blocking lock; the existing per-chunk lock is
  explicitly reviewed against the repository lock-free constraint.
- R132 must complete before the benchmark can use DiskDB mixed free/
  compaction accounting as a correctness oracle under concurrency.
- R103 owns chunk range migration, R93 owns mirror-to-EC conversion, R106 owns
  the small-object writer, R107 owns object reads, R110–R112 own data-I/O
  failure repair, and R113 owns batch strip allocation. R133 tests their
  landed integration points but does not absorb unfinished scope.

## Acceptance

### Review and test organization

- Trace every public ChunkDB RPC from `ChunkdbClient` through routing,
  service, lifecycle, DiskDB, and KV effects → record each invariant and map
  it to a feature-named test; fix local mismatches and create a focused
  backlog item for every deferred complex gap. Integration test.
- Run each file under `lib/crowdb-chunkdb-client/tests/` independently → each
  provisions and tears down its own required state, fails when binaries are
  missing, and exercises only public client/RPC behavior. E2E test.
- Review the existing per-chunk blocking mutex under concurrent same-chunk
  and different-chunk traffic → document measured contention and either
  obtain explicit approval to retain it or replace it with a lock-free/CAS
  lifecycle protocol. Integration test.

### Mirror and EC allocation

- Start six storage nodes in three racks and allocate a three-copy mirror
  chunk → assert each strip has three segments on distinct nodes and racks,
  every segment owner is the chunk ID, and durable capacity matches the
  returned chunk. E2E test.
- Allocate 4+2 and 8+4 EC chunks on sufficient topology → assert exact data/
  code counts, monotonically ordered strip sequences, at least three racks
  when possible, and no node holds more blocks than the safe failure bound.
  E2E test.
- Remove enough healthy nodes to make safe EC placement impossible with
  unsafe mode disabled → allocate → assert a typed placement error, no chunk
  record, and no busy-space increase after rollback/compaction. E2E test.
- Enable unsafe mode explicitly on insufficient topology → allocate → assert
  the response and metrics mark unsafe placement and the resulting segment
  count remains exact. Integration test.
- Make one DiskDB return a partial allocation and another fail → allocate a
  multi-strip chunk → assert the request fails and every acknowledged segment
  is freed or represented by a durable recovery intent. Integration test.

### Allocate, append, and persistence

- Allocate the same caller-supplied chunk ID concurrently → assert exactly
  one Active record is created and the loser receives AlreadyExists or a
  retryable conflict without allocating leaked segments. E2E test.
- Fail KV persistence after all mirror/EC blocks are allocated → allocate →
  assert no chunk is queryable and DiskDB returns to the original busy-space
  total after reconciliation. Integration test.
- Fail one tentative-block commit after chunk metadata persists → restart
  ChunkDB and run reconciliation → assert the chunk is either completed or
  made unavailable with every tentative segment accounted for. E2E test.
- Append several mirror and EC strips → query the chunk → assert existing and
  new strips occur exactly once in sequence order, capacity is recomputed,
  and DiskDB owner tags match the chunk ID. E2E test.
- Race two appends to the same chunk while appending to unrelated chunks →
  assert no lost update on the shared chunk and no global serialization of
  unrelated chunk IDs. Integration test.

### Alter, seal, and delete/free

- Seal an Active chunk at a valid length, then attempt append and reseal →
  assert the first transition persists its timestamp/length and later invalid
  transitions do not change metadata or allocate blocks. E2E test.
- Race append, seal, and delete on one Active chunk → query after all replies →
  assert the durable state corresponds to one legal serialization and every
  segment is either referenced or freed, never both. Integration test.
- Replace a strip while injecting failure before and after metadata persistence
  → assert the chunk always references a durable replacement or the original,
  and only the unreferenced segment is freed. Integration test.
- Delete a range spanning exact and partial strip boundaries → assert only the
  contractually selected strips are removed/freed, remaining offsets and
  capacity are valid, and invalid/overflow ranges make no change. E2E test.
- Delete Active and Sealed chunks, repeat each request, and restart → assert
  idempotent final state, no query exposes reusable segments, and DiskDB busy
  space falls by exactly the deleted segments. E2E test.
- Inject DiskDB free failure and KV write failure at each delete ordering
  point → reconcile → assert no Active/Sealed chunk refers to a reusable block
  and every orphan is reported for retry. Integration test.

### Routing, topology, and operations

- Bind disjoint bucket ranges to three ChunkDB instances and issue operations
  through one client → assert each chunk reaches its owner and queries/lists
  return the expected range-local records. E2E test.
- Return `NotMyRange`, change the binding, and retry a mutation → assert the
  client refreshes once, reaches the new owner, and applies the mutation once.
  E2E test.
- Deliver stale, duplicate, and missing topology notifications while polling
  continues → assert placement uses only a complete latest snapshot and
  excludes Offline/Maintenance disk groups. Integration test.
- Restart a ChunkDB process with existing chunks → assert registration,
  readiness, topology/range loading, cache misses, query, and later mutations
  work without local durable state. E2E test.

### Benchmark and verification

- Deploy the benchmark profile → assert six storage nodes span three racks,
  24 disks are available through six DiskDB instances, three ChunkDB
  instances own the complete bucket space, required KV groups are healthy,
  and every process emits logs and metrics below one timestamped root. E2E
  test.
- Run mirror allocate and 4+2 EC allocate for a short duration with one worker
  and multiple workers → assert the report contains TPS, p50/p99, stop reason,
  errors, chunk/strip/segment counts, safe-placement counts, and exact
  metadata/space verification. Integration test.
- Run the configured lifecycle mix with a fixed seed → assert only Active
  chunks are appended/sealed, only live chunks are deleted, acknowledged
  operation counts reproduce the final expected model, and all KV/DiskDB
  totals agree after reconciliation. Integration test.
- Run until capacity exhaustion → assert `space_exhausted`, no segment
  overlaps, every remaining busy segment is referenced by exactly one live
  chunk, and another allocation cannot succeed. E2E test.
- Run `tools/bench-chunkdb-regression.sh` → assert it covers mirror and EC
  allocation plus lifecycle mix at one worker and a concurrency sweep,
  continues collecting diagnostics after a failed case, exits nonzero for any
  correctness failure, and retains reviewed baseline comments without adding
  generated logs or TSV files to Git. Integration test.

### Test commands

- `pixi run test-protocol`
- `pixi run test-chunkdb`
- `pixi run test-chunkdb-client`
- `pixi run test-diskdb`
- `pixi run test-diskdb-client`
- `pixi run test-console-cli`
- `pixi run test-console-shared`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
