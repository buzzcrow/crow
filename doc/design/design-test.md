<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV Test Strategy

## Overview

CrowKV is a distributed key-value store built on Paxos consensus. The test
suite is organized in layers that mirror the system architecture, so a
failure points at the lowest broken layer. Each layer gets its own test
binary and tests only the logic that belongs to that layer.

This document defines the **test strategy**, **scope of each layer**, and
**coverage rules** per layer. It is the reference for designing new tests
when implementing features or components — consult this doc to determine
which layer a new test belongs to and what coverage rules apply.

For the live task backlog (unfinished test gaps), see [`plan-test.md`](../working/plan-test.md).

## Architecture Stack

```
store      (PxKvStore: many groups, one node identity, routing)   <- crowkv/tests/store.rs
  group    (PxGroup: 1 local + N remote replicas, full Paxos)      <- crowkv/tests/group.rs
    replica  (PxLocalReplica: acceptor + learner + WAL + slots)    <- crowkv/tests/replica.rs
      election (PxLocalReplica election state machine, pure logic) <- crowkv/tests/election.rs
      wal    (WalEngine: durable log)   slot (PxSlotList)           <- crowkv/tests/wal.rs, slot.rs
        unit (pure modules: codec, classifier, kv engine, roles)   <- crowkv/tests/{paxos,kv}.rs + wal/slot codec tests
deployment (crowkv-server binary + HTTP mgmt API + multi-process)  <- crowkv-server/tests/*
console    (mgmt API server + CLI: Axum REST, CLI commands)        <- crowkv-console/{web,cli}/tests/*
ui e2e     (Playwright browser: SPA + real backend)                <- crowkv-console/web/ui/e2e/*
```

## Test Binary Map

| Binary | Layer | Drives | Source under test |
| --- | --- | --- | --- |
| `crowkv/tests/paxos.rs` | unit | `acceptor`, `learner`, `error` classifier in isolation | `paxos/{acceptor,learner,error}.rs` |
| `crowkv/tests/kv.rs` | unit | `InMemKV` / `KVEngine` apply semantics | `kv/{mem_kv,kv_engine,op}.rs` |
| `crowkv/tests/election.rs` | unit | `PxLocalReplica` election state machine (role transitions, vote granting, heartbeat, lease, term fencing) | `cluster/local_replica.rs` election logic |
| `crowkv/tests/wal.rs` | subsystem | `WalEngine` append/replay/gc/segment + record codec + backends | `wal/*` |
| `crowkv/tests/slot.rs` | subsystem | `PxSlotList` / `PxSlotNode` | `paxos/{slot_list,slot_node}.rs` |
| `crowkv/tests/replica.rs` | replica | single `PxLocalReplica` (no peers) | `cluster/local_replica.rs`, `paxos/*` |
| `crowkv/tests/group.rs` | group | `PxGroup` multi-node clusters (real loopback gRPC, no mocks) | `cluster/{group,group_election,remote_replica,learner_stream}.rs` |
| `crowkv/tests/store.rs` | store | `PxKvStore` routing / lifecycle / status / health | `cluster/{px_kv_store,kv_store,kv_server,status}.rs` |
| `crowkv-server/tests/*` | deployment | server binary + HTTP API, multi-process clusters, CLI, startup | `crowkv-server/src/*` |
| `crowkv-client/tests/*` | client e2e | `CrowkvClient` retry, topology cache, `NotLeaderHint` follow, `AnyReplica` read distribution + fallback against embedded gRPC servers | `crowkv-client/src/*` |
| `crowkv-console/web/tests/*` | console mgmt API | Axum REST API server: node management, OpenAPI proxy, API forwarding | `crowkv-console/web/src/*` |
| `crowkv-console/{shared,cli}/tests/*` | console mgmt API | shared core (config, API client, health aggregation) + CLI commands | `crowkv-console/{shared,cli}/src/*` |
| `crowkv-console/cli/tests/bench_benchmark.rs` | benchmark | `bench benchmark` lifecycle (deploy → run → collect → report → cleanup) + `bench compare` | `crowkv-console/cli/src/bench/*` |
| `crowkv-console/web/ui/e2e/*` | UI E2E | Playwright browser tests: SPA interactions, context menus, dialogs, KV panel | `crowkv-console/web/ui/src/*` + real backend |

**Placement rule:** a test that only needs the `crowkv` library (even if it
binds the embedded gRPC server via `PxKvStore::start`) lives in `crowkv`. A
test that boots the `crowkv-server` binary / HTTP management API lives in
`crowkv-server`.

**KV operation correctness rule:** every layer that applies KV mutations
(replica, group, store) must test all operation types and orderings:
- Put (single key)
- Overwrite (same key, new value)
- Delete (produces tombstone)
- Delete on non-existent key (no-op)
- Batch with multiple puts
- Batch with intra-batch last-wins (put → delete → put on same key)
- Batch with put-then-delete same key (delete wins)
- Batch with delete-then-put same key (put wins)
- Empty batch / NoOp (advances frontier, no keys)
- Mixed put/delete across multiple slots and batches
- Persistence round-trip: all above operations survive WAL replay + restart

**Cluster verification rule:** every tier that creates a real `PxKvStore`,
`PxGroup`, or `crowkv-server` process must verify two things before
proceeding to tier-specific assertions:

1. **Leader election succeeded within a bounded timeout.** If leader
   election does not complete within the timeout (typically 10 s for
   multi-node, 3 s for single-node), the test fails — slow election is a
   bug, not a flaky test. The test must assert that exactly one leader
   exists, not just that "some leader appeared."

2. **Basic KV CRUD correctness via the client library.** After leader
   election, perform a minimal CRUD cycle through `CrowkvClient` (or
   direct `PxKvStore` API for lower layers) and verify correctness:
   - Put a key → Get returns the same value
   - Overwrite → Get returns new value
   - Delete → Get returns not-found
   - Scan returns expected keys

   **Edge-case coverage** (at least one test per layer must cover these):
   - Empty key (`""`)
   - Large key (≥1 KB)
   - Key with special bytes (null bytes, high-UTF8, whitespace-only)
   - Large value (≥1 MB)
   - Small value (1 byte)
   - Empty value (`""`)

   Performance is not asserted here — correctness only. A put that takes
   5 s but returns `ok=true` and is readable via get is a pass. Performance
   tuning is tracked separately (see Benchmark Layer below).

   KV operations are gRPC only (no REST API for KV). Tests use
   `CrowkvClient` (deployment/UI layers) or `PxKvStore` public API
   directly (group/store layers). The console mgmt API layer verifies
   topology management via REST but does not perform KV operations — KV
   correctness is delegated to the deployment or UI E2E layer.

**Leader change & reconfig rule:** every tiered layer (Group, Store,
Deployment, UI E2E) that creates a multi-replica cluster must cover:

1. **Leader change:** trigger step-down on the current leader, verify a new
   leader is elected within bounded timeout (10 s multi-node, 3 s
   single-node), verify KV operations continue to work after the new leader
   is established, verify the old leader rejoins as follower after restart
   (if applicable).

2. **Reconfig — add replica:** add a new replica to a running group with
   existing data, verify the new replica catches up (data visible via scan
   or get on the new node) within bounded timeout. Since tests write few
   keys, catch-up should complete in seconds — slow catch-up is a bug.

3. **Reconfig — remove replica:** remove a non-leader replica from a running
   group, verify the group continues to accept KV operations (quorum
   intact), verify health status reflects the reduced membership.

4. **Reconfig — remove leader:** remove or stop the leader's replica, verify
   a new leader is elected within bounded timeout, verify KV operations
   resume. This is the most operationally sensitive scenario — the test must
   not block indefinitely waiting for election.

## Layer Definitions

### Unit Layer

**Scope:** Pure modules with no I/O, no async runtime, no network. Tests are
deterministic and run in microseconds.

**Modules:**
- `paxos/acceptor` — promise / accept fence, ballot ordering, prior accepted return.
- `paxos/learner` — note-chosen advances chosen/applied, dedup cache, durable watermark.
- `paxos/error` — keyword + retry-action classification.
- `paxos/roles` — `PxLogEntry` / `PxBallot` edge cases (NoOp vs Accepted, ballot tie-break).
- `kv/mem_kv` — highest-slot-wins, idempotent apply, tombstone, intra-batch last-wins, prefix scan, compare, wire decode.
- `kv/op` — wire-format encode/decode in isolation.

**Coverage rules:**
- Acceptor: promise/accept fence, ballot ordering, prior accepted value
  return.
- Learner: note-chosen advances chosen/applied, dedup cache lookup, durable
  watermark tracking.
- Error: keyword classification + retry-action mapping for all error types.
- Roles: `PxLogEntry` / `PxBallot` edge cases (NoOp vs Accepted, ballot
  tie-break).
- `mem_kv`: highest-slot-wins, idempotent apply, tombstone semantics,
  intra-batch last-wins, prefix scan, compare, wire decode.
- `kv/op`: wire-format encode/decode round-trip.
- `wal/record`: encode/decode round-trip, CRC validation, truncation errors.

### Election Unit Layer

**Scope:** `PxLocalReplica` election state machine — pure logic with no peers,
no network, no WAL I/O. Tests construct a `PxLocalReplica` directly and call
election methods (`handle_pre_vote`, `handle_request_vote`, `handle_heartbeat`,
role transitions, lease management).

**Modules:**
- Role transitions (`become_follower`, `become_precandidate`, `become_candidate`, `become_leader`).
- `PreVote` / `RequestVote` granting and rejection logic.
- Heartbeat handler (term adoption, leader recording, committed-entry application).
- Lease management (validity, expiry, monotonic extension, reset).
- Term fencing in prepare/accept.
- Election metrics.

**Coverage rules:**
- Role transitions: follower (adopt higher term, clear leader), precandidate
  (no term bump), candidate (term bump, self-vote, lockout), leader (set self,
  reset lease).
- `new_inheriting_election_state`: preserves term/voted_for/role, shares
  acceptor + learner.
- `PreVote`: grant on higher term + up-to-date log, reject stale term/log/
  lockout, no term bump or voted_for mutation.
- `RequestVote`: grant on higher term + up-to-date log, reject stale log/
  lockout/lower term, adopt term + set voted_for, lockout prevents re-grant,
  reply carries frontier triple.
- Heartbeat: adopt higher term, record leader, reject lower term, accept
  equal term, apply committed entries up to commit_slot, idempotent, step
  down leader on higher-term heartbeat.
- Lease: requires leader + unexpired, expiry after deadline, monotonic
  extension, reset on role change, renew_lease extends by configured duration
  minus skew.
- Term fencing in prepare/accept: reject stale term, adopt higher term,
  forward on equal term.
- `handle_step_down`: strict-fence policy (leader + matching term + matching
  target_leader_id), reject when not leader / term mismatch / wrong target,
  preserve term on accept, double step-down rejected, reply reports actual
  term and leader.
- `frontier_triple` consistency: zero on fresh replica, reflects accepted
  and learned slots, preserves across role transitions, handles gaps in
  accepted log, advances with progressive learn, carried in PreVote and
  RequestVote replies.

### WAL Subsystem Layer

**Scope:** `WalEngine` and all WAL internals — durable log, segment management,
replay, GC, I/O backends, pipeline writer. Tests use real temp filesystems
(`IoBackend::File`) or in-memory simulated disks (`BlockDevice` under
`test-util` feature).

**Modules:**
- `wal_engine` — append, rotation, seal, durable-flush batching, affinity disks, multi-disk distribution, writer failure, large/iov-split batches, concurrent appends.
- `segment` — header/seal, reader over text + binary, aligned padding recovery.
- `replay` — record recovery, term/voted-for/watermark rebuild, dedup checkpoint, determinism, config-change recovery, restore-to-watermark.
- `gc` — remove-below-watermark, zero-watermark no-op, replay-after-gc, snapshot-slot prefix.
- `block_backend` / `pipeline_backend` — alignment, RMW, amplification accounting.
- `file_backend` — real-filesystem fallback path (open/append/flush/fsync/truncate) via `IoBackend::File` / `AsyncFile`.
- `file_restore` — durability round-trip over real `File` backend: close + reopen recovers all records, term/vote/watermark, and resume-append works.
- `io_backend` — `IoBackend::detect()` selection, `open`/`rename`/`unlink`/`read_dir`/`create_dir_all`/`exists`.
- `index` — in-memory segment index (insert/locate, register/remove segment, rebuild-from-scans, slot_count).
- `pipeline_writer` — batch coalescing, ack ordering, seal, index updates, batch stats (via `WalEngine` public API).
- `record` — encode/decode round-trip, CRC + truncation errors.

**Coverage rules:**
- Engine: append, rotation, seal, durable-flush batching, affinity disks,
  multi-disk distribution, writer failure, large/iov-split batches,
  concurrent appends.
- Segment: header/seal, reader over text + binary, aligned padding recovery.
- Replay: record recovery, term/voted-for/watermark rebuild, dedup
  checkpoint, determinism, config-change recovery, restore-to-watermark.
- GC: remove-below-watermark, zero-watermark no-op, replay-after-gc,
  snapshot-slot prefix.
- Block/pipeline backend: alignment, RMW, amplification accounting.
- File backend: real-filesystem fallback (open/append/flush/fsync/truncate).
- File restore: durability round-trip over real File backend (close + reopen
  recovers all records, term/vote/watermark, resume-append).
- IO backend: detect() selection, open/rename/unlink/read_dir/create_dir_all/
  exists.
- Index: insert/locate, register/remove segment, rebuild-from-scans,
  slot_count.
- Pipeline writer: batch coalescing, ack ordering, seal, index updates,
  batch stats.
- Record: encode/decode round-trip, CRC + truncation errors.

### Slot Subsystem Layer

**Scope:** `PxSlotList` — lock-free chunked sparse array. Tests cover
single-threaded operations, concurrent stress, and reclamation watermark
interactions with long-lived read guards.

**Modules:**
- `PxSlotList` — insert/get/trim/reclaim, chunk growth, sparse insert, tail lookup, atomic guard, range iteration.
- `PxSlotNode` — accept-after-promise, duplicate insert, trim/reclaim with live refs.
- Concurrent stress — multi-thread insert at disjoint ranges, concurrent insert+read, insert+trim+reclaim, full insert+trim+reclaim+read stress.
- Reclamation watermark — long-lived guard prevents reclaim, multiple guards pin chunk, idempotent reclaim, progressive trim across chunks.

**Coverage rules:**
- `PxSlotList`: insert/get/trim/reclaim, chunk growth, sparse insert, tail
  lookup, atomic guard, range iteration.
- `PxSlotNode`: accept-after-promise, duplicate insert, trim/reclaim with
  live refs.
- Concurrent stress: multi-thread insert at disjoint ranges, concurrent
  insert+read, insert+trim+reclaim, full insert+trim+reclaim+read stress.
- Reclamation watermark: long-lived guard prevents reclaim, multiple guards
  pin chunk, idempotent reclaim, progressive trim across chunks.

### Replica Layer

**Scope:** Single `PxLocalReplica` with no peers. Tests exercise the
acceptor + learner + WAL + slot integration: prepare/accept with WAL
persistence, dedup suppression, snapshot install, WAL replay ordering.

**Coverage rules:**
- KV operation correctness: all op types and orderings per the KV operation
  correctness rule above, with WAL-backed persistence round-trip.
- Prepare/accept tracking: classic Paxos prepare → accept → learn cycle.
- Dedup suppression: duplicate prepare/accept does not double-apply.
- Snapshot install / truncate-and-resume: snapshot replaces local state,
  WAL truncated to snapshot point, append resumes correctly.
- Multi-slot WAL replay: contiguous slots below watermark all applied, hole
  below watermark stops at hole (partial apply), out-of-order records sorted
  by slot during replay, slots above watermark not applied, empty WAL
  produces zero state, watermark higher than accepted stops at first hole.
- Concurrency: sequential learn-then-accept and accept-then-learn on same
  slot, concurrent tokio::join! on same slot (no panic, consistent state),
  concurrent on adjacent slots, re-accept with different value after learn,
  concurrent accepts on disjoint slots, concurrent learns on sequential
  slots.

### Group Layer

**Scope:** `PxGroup` with 1 local + N remote replicas using real loopback
gRPC (no mocks). Tests exercise full Paxos rounds, leader election, KV
through the group, durability under crash/restart.

#### Tiered Strategy

Tests are organized in three tiers of increasing complexity. Each tier
builds on the confidence of the one below.

**Tier 1 — Basic Group Operations.** Single-leader propose, sequential slot
allocation, KV through the group (all op types per KV correctness rule),
follower forwarding, durability under crash/restart. These verify the core
Paxos + KV integration works end-to-end within one group.

**Tier 2 — Membership & Recovery.** Election across 1–7 replicas, step-down
and propose-after-step-down, new-member snapshot join, membership epoch
fencing, learner stream, recovery above durable-commit watermark. These
verify the group handles dynamic membership and catch-up correctly.

**Tier 3 — Failure & Edge Cases.** Leader kill + restart no-data-loss,
two-replica even-quorum (no progress without both up), leader change
simulation, remote replica unreachable, preemption retry, kv-slot retry on
prior accepted. These verify the group degrades gracefully and recovers
from failures.

#### Coverage Rules

- Every KV op type and ordering per the KV operation correctness rule,
  verified via `engine_get` on all replicas (not just the leader).
- Election must be tested for 1, 2, 3, 5, 7 replica counts — single leader
  elected, no split-brain.
- Follower must reject direct client writes and return `NotLeaderHint`.
- Proposer window full must queue (queue admission); repair must
  fill gap and advance frontier.
- Durability: single-node crash/restart and full-cluster restart must
  preserve all committed entries (including tombstones).
- New-member snapshot join: fresh replica pulls snapshot and catches up to
  current state.
- Membership epoch fencing: stale-member accept/reject behavior across
  membership changes.
- Remote replica transport: unreachable/invalid endpoint returns error.

### Store Layer

**Scope:** `PxKvStore` — multi-group routing, node identity, lifecycle
management, topology status. Tests use embedded gRPC server via
`PxKvStore::start`.

#### Tiered Strategy

**Tier 1 — Single-Store Operations.** Single-node KV with all read modes,
follower redirect hint, dedup, KV ops via public API (`kv_put`, `kv_delete`,
`kv_batch_write`), persistence round-trip. These verify the store-level
routing and KV interface works correctly.

**Tier 2 — Multi-Group Interactions.** Multi-group routing within one node,
dynamic add/remove group, missing-group error, per-group WAL-root isolation,
per-group independent leadership. These verify groups within a store are
properly isolated.

**Tier 3 — Lifecycle & Topology.** Topology `status` composition, `health`
levels, graceful `shutdown` cascade + idempotency, store-wide shutdown with
multiple active groups under load. These verify the store manages its
lifecycle correctly across all groups.

#### Coverage Rules

- Every KV op type and ordering per the KV operation correctness rule, through
  `PxKvStore` public API.
- Single-node: all read modes (leader read, follower redirect, stale read if
  applicable), dedup.
- Multi-group: routing to correct group, dynamic add/remove group,
  missing-group returns error.
- Persistence: put/overwrite/delete survive restart.
- Per-group isolation: no cross-group slot/key bleed, independent WAL roots.
- Topology: `status` composition is correct, `health` levels reflect group
  states, `shutdown` cascades to all groups and is idempotent.

### Client Library E2E

**Scope:** `CrowkvClient` behavior against embedded `PxKvStore` gRPC
servers — retry, topology cache refresh, `NotLeaderHint` follow,
`AnyReplica` read-endpoint distribution, and the `MinSlot` lagging-follower
fallback. Tests bind real loopback gRPC endpoints and drive the public
client API; no `crowkv-server` binary is booted.

**Source:** `crowkv-client/tests/*`.

**Lagging-follower harness:** to exercise the `AnyReplica`
`MinSlot` fallback end-to-end (distributed read → lagging follower →
`NotLeader` redirect → leader retry → `read_endpoint_fallback`
increments), a 3-node cluster stands up A (leader, voting), B (follower,
voting), and C (non-voting lagging learner). C is **not** wired as a
remote on A's group: the accept and chosen-notice fan-out
(`group.rs::run_accept_phase`, `fan_out_chosen_notice`) sends to every
real remote regardless of the `voting` flag, and `on_accept` applies via
`learn_chosen` directly — so a non-voting C wired on A would still apply
and would not lag. Keeping C off A's remote list makes it
deterministically lag (`contiguous_applied` stays 0), mirroring a real
learner catching up via snapshot + WAL tail outside the accept fan-out.
C's election driver is disabled (`add_group_without_election`) so the
non-voting follower does not time out and spin up elections. A
hand-crafted `/topology` (A's real `status()` with C appended to group
1's remotes) exposes all three endpoints to the `AnyReplica` selector so
reads round-robin over `[A, B, C]` and deterministically hit C.

### Deployment Layer

**Scope:** `crowkv-server` binary + HTTP management API + multi-process
clusters. Tests boot the actual server binary, exercise the HTTP API, and
verify multi-process cluster formation and lifecycle.

**Source:** `crowkv-server/tests/*`.

**Test runner:** `pixi run test-server`.

#### Tiered Strategy

**Tier 1 — Single-Server API.** Server startup/shutdown, HTTP management API
endpoints (stores, groups, replicas, nodes, racks), health check, OpenAPI
serving. These verify the server binary boots and the HTTP API works.

**Tier 2 — Multi-Process Clusters.** Multi-node cluster formation, KV
operations through the HTTP API, leader election across processes, store/
group lifecycle via API calls. These verify the server integrates correctly
in a distributed setting.

**Tier 3 — Failure & Recovery.** Process crash/restart, network partition
between processes, multi-store-per-node under failure, graceful shutdown
under load. These verify the deployment handles operational failures.

#### Coverage Rules

- Every HTTP management API endpoint must have at least one test: stores
  (list/add/remove), groups (add/remove), replicas (add/remove/list), nodes
  (add/remove/deploy/stop/restart), racks (add/remove), health, topology.
- Async operation API: `POST /step-down` returns `202 {operation_id}` by
  default; `?sync=true` preserves synchronous behavior. `GET /operations/:id`
  returns operation status (`pending`/`running`/`completed`/`failed`).
  `GET /stores/:sid/groups/:gid/ready` returns `200` when ready (leader
  elected, quorum reachable), `503` when not ready with reason.
- Server startup must be tested with various initial configurations.
- Multi-process cluster: ≥3 nodes form a cluster, elect leaders, and serve
  KV operations.
- Shutdown: graceful shutdown terminates all groups cleanly; restart recovers
  state.

### Console Mgmt API Layer

**Scope:** Console management API server (Axum) and CLI frontend. Tests
verify the HTTP REST API endpoints, OpenAPI proxy, API forwarding to
crowkv-server nodes, shared core logic (config management, API client,
health aggregation), and CLI commands.

**Source:** `crowkv-console/web/tests/*` (Axum server),
`crowkv-console/shared/tests/*` (shared core),
`crowkv-console/cli/tests/*` (CLI).

**Test runners:** `pixi run test-mgmt-api` (web server),
`pixi run test-cli` (shared core + CLI).

#### Tiered Strategy

**Tier 1 — Single-Endpoint API.** Each REST endpoint tested in isolation:
add/list/remove racks, nodes, stores, groups, replicas; health check;
topology export; OpenAPI proxy. These verify individual API correctness.

**Tier 2 — Multi-Node Cluster Creation.** Create a full cluster topology via
API calls in sequence: add racks → add nodes → deploy servers → create
stores → add groups → add replicas → wait for leaders. Verify topology
state, health aggregation, and API forwarding at each step. This verifies
the API composition works for real deployments.

**Tier 3 — Lifecycle & Error Handling.** Add/remove operations on a live
cluster (remove group from running store, remove node from rack, re-deploy
after stop). Error cases: duplicate ID rejection, not-found errors,
unreachable node forwarding failure, config persistence across restart.

#### Coverage Rules
- Every REST API endpoint must have at least one test: racks (add/list/
  remove), nodes (add/remove/deploy/stop/restart), stores (add/list/remove),
  groups (add/remove), replicas (add/remove/list), health, topology,
  OpenAPI proxy.
- Every CLI command must have at least one test: store/group/node lifecycle,
  health check, topology export, KV operations (if supported via CLI).
- API forwarding: requests to a node's crowkv-server are correctly proxied.
- Health aggregation: multi-node health is correctly aggregated into overall
  status.
- Config persistence: cluster config survives process restart.

### UI E2E Layer

**Scope:** Playwright browser tests against a real `crowkv-web` + `crowkv-server`
backend. Tests drive the SPA exactly as an operator would — clicks, context menus,
dialogs, KV panel — and verify both UI feedback (toasts, tree updates) and backend
state (API checks). No mocks; the only network interception is route aborting for
the backend-unreachable test.

**Source:** `crowkv-console/web/ui/e2e/flows/*.spec.ts`, fixtures in
`crowkv-console/web/ui/e2e/fixtures/consoleSetup.ts`.

**Test runner:** `pixi run test-ui` (Playwright, headless Chromium).

#### Tiered Strategy

Tests are organized in three tiers of increasing complexity. Each tier builds on
the confidence of the one below. A failure in a lower tier explains failures above.

**Tier 1 — Single-Feature UI Coverage.** Each test exercises one UI element in
isolation on a minimal topology (1 rack, 1 node, 1 server, 1 store, 1 group).
Purpose: verify that every clickable control works and produces the expected
toast/backend state. These are fast, deterministic, and catch UI regressions
when components are refactored.

**Tier 2 — Complex Topology & Multi-Store.** Tests verify that basic operations
remain correct under non-trivial deployments: multiple stores, groups on subsets
of a larger node pool, cross-store isolation. A shared `setupCluster()` helper
accepts a topology descriptor (node count, store count, groups-per-store,
replicas-per-group). The same assertion suite runs on both a **simple** topology
(3 nodes, 1 store, 1 group) and a **complex** topology (8 nodes, 2 stores, subset
groups). If a test passes on simple but fails on complex, the gap is multi-node
interaction — this comparative approach isolates topology-specific bugs.

**Tier 3 — Reconfiguration & Partial Degradation.** Tests exercise the reconfig
feature: stopping/deleting nodes while groups are active, adding replicas to
running groups, verifying the cluster continues to operate with reduced membership.
These are the highest-value tests for operational confidence and the most sensitive
to timing. Leader election timeouts are capped at 10 s; all other assertions at 3 s.

#### Coverage Rules

- **Every context menu action** must have at least one test: Add Node, Delete Rack,
  Deploy, Ping, Restart, Stop, Delete Node (Physical); Add Group, Delete Store, Add
  Replica, Delete Group, Delete Replica (Logical).
- **Every dialog** must have at least one test that fills it and verifies the
  backend result: AddRack, AddNode, AddStore, AddGroup, AddReplica, DeployServer,
  ConfirmDelete.
- **Every KV panel operation** must have at least one test: Get (found + not-found),
  Put, Delete, Delete Prefix, Delete Selected, inline delete, Scan (with prefix),
  Load More, All Groups mode, auto-scan toggle, demo inject, demo delete, copy.
- **Every inspector feature** must have at least one test: Details tab (entity
  fields), Activity tab (log entries + clear), cross-jump (both directions).
- **Async operation feedback** (Tier 3): when a step-down or reconfig
  operation is triggered via UI, the UI should show progress feedback (spinner
  or status indicator) and poll the async operation API until completion,
  then refresh topology. Tests verify the UI does not block indefinitely.
- **Comparative tests** (Tier 2) must run on both simple and complex topologies
  using the same assertion code path.

## Benchmark Layer

**Scope:** Self-contained benchmark lifecycle — deploy a 3-node cluster,
drive write/read load, collect server-side metrics + logs, produce a
report, then clean up. The benchmark is a test type: it exercises the
full production path (client → leader → Paxos → WAL → memtable →
async flush) end-to-end, with disk IO isolation to measure consensus +
memtable throughput rather than fsync latency.

**Source:** `crowkv-console/cli/tests/bench_benchmark.rs`.

**Test runner:** `pixi run test-cli` (includes bench benchmark +
compare integration tests).

### Benchmark Verb

The `bench benchmark` verb orchestrates the full lifecycle:

- **Deploy** — auto-provision a minimal cluster (1 rack, 3 nodes on
  localhost) via an embedded `crowkv-web` instance started in-process.
  Topology creation follows the same pattern as the UI test fixture
  (`consoleSetup.ts::setupCluster`), but through the typed
  `ConsoleClient`. Config-driven SSH deployment is accepted but
  stubbed for future work.
- **Run** — drive KV put/get/delete at configurable concurrency using
  the existing closed-loop load generator (`run_bench`). Each worker
  maintains one in-flight RPC at a time; threads = max concurrency.
- **Collect** — gather server-side perf counters (WAL append rate, KV
  op counts), system metrics (CPU, RSS, TCP retransmits), and runtime
  logs from all 3 node workspaces. Metrics are parsed from each
  server's `log/metrics-*.log` file and aggregated across nodes.
- **Cleanup** — stop all server processes, shut down embedded
  console-web, optionally remove workspace (`--keep-workspace` to
  retain).
- **Report** — Markdown report at `bench-runs/<datetime>/report.md`
  with throughput (ops/s), latency (avg + p50 + p90 + p99 + p999 +
  max), error rate, WAL metrics, system resource usage, and anomaly
  detection (non-zero error rate, TCP retransmits, server log
  warnings).

Reports are stored under `<project_root>/bench-runs/<YYYY-MM-DD_HHMMSS>/`
— each run gets its own datetime-stamped directory containing
`report.md`, `workspace/` (deploy artifacts), and `logs/` (node logs).
The `bench compare <tag1> <tag2>` verb finds runs by partial
datetime match and prints a side-by-side comparison table.

### Storage Modes

- **`memory`** — in-memory KV engine, WAL with `no_fsync=true`.
  Baseline: pure consensus + WAL append + memtable insert cost, zero
  disk IO.
- **`file-nofsync`** — crow-tree engine with `text` backend, WAL with
  `no_fsync=true`. Durable engine + WAL, but `fdatasync` skipped to
  isolate path-level overhead from disk IO.
- **`block`** (planned) — crow-tree engine with `block` backend (real
  block device page store). Memtable is in-memory; async flush writes
  SST files to block device. WAL also on block device with
  `no_fsync=true` (page cache only). Tests whether memtable flush
  can keep up without blocking the write path.

`no_fsync=true` in all modes: WAL writes go to page cache only.
This is unsafe for production but valid for benchmarking — the goal
is to measure consensus + memtable throughput, not disk fsync
latency.

### Write-Only Benchmark Design

**Objective:** Measure maximum write TPS and average latency on the
leader, with unique keys (no overwrite) to exercise the real
memtable insert + flush path.

**Key decisions:**

- **Unique keys** — each worker generates monotonically increasing
  keys from a disjoint range (`worker_id * range + counter`). This
  forces memtable growth → flush → new SST/snapshot, exercising the
  real production flow. Random key selection with a small key space
  causes massive overwrite, hitting memtable in-place update rather
  than insert + flush.
- **Connections** — scale with threads: 8 threads → 4 connections,
  64 threads → 8 connections, 128 threads → 16 connections.
  Connections double while threads jump faster, keeping channel
  utilization high without excessive overhead.
- **Threads sweep** — 8 → 64 → 128. Finds the throughput plateau
  where adding more workers no longer increases TPS (server-side
  bottleneck: consensus batch processing, memtable insert, or leader
  serialization).
- **Duration** — 10s per run. Short enough for a full sweep under 2
  minutes, long enough for stable numbers.

**Configuration matrix:** All runs use `--workload write --value-size
64 --duration-secs 10`.

- Run 1: memory, threads=8, connections=4
- Run 2: memory, threads=64, connections=8
- Run 3: memory, threads=128, connections=16
- Run 4: block, threads=8, connections=4
- Run 5: block, threads=64, connections=8
- Run 6: block, threads=128, connections=16

### Baseline Results

Current benchmark results are in
[`doc/working/write-flow-analysis.md`](../working/write-flow-analysis.md)
§ Benchmark Results. Key findings: peak 50K ops/s (64T/8C/MI=64),
zero errors with queue admission, scaling ceiling is per-proposal
consensus latency (~1.2ms).

### Coverage Rules

- `bench benchmark --mode memory` runs end-to-end: deploys cluster,
  drives load, collects metrics + logs, prints report, cleans up.
- `bench benchmark --mode file-nofsync` does the same with crow-tree
  engine + no-fsync WAL.
- `--keep-workspace` retains the workspace for debugging.
- `bench compare <tag1> <tag2>` prints a side-by-side comparison
  table with throughput, latency, error rate, WAL metrics, system
  metrics.
- Report includes `avg_us` latency alongside p50/p99/p999.
- Report includes server-side metrics: WAL append counts, KV put/get
  counts, CPU/RSS/TCP from system metrics.
- Report includes anomaly detection: non-zero error rate, TCP
  retransmits, server log warnings.
- All existing `bench run` / `bench stress` / `bench report` commands
  continue to work unchanged.

## crow-tree C++ Test Layers

The C++ crow-tree library (`libcrow-tree`) has its own test layers, separate from
the Rust test binaries above. They run as `test-ct` in CI (291 tests, ~8 s).

| Layer | Where | What it proves |
| --- | --- | --- |
| C++ unit | `libcrow-tree/tests/unit` | Single component correctness: cell encoding, leaf page build/read, delta replay, consolidation triggers, mapping table, epoch manager, split point. |
| C++ integration | `libcrow-tree/tests/integration` | Multi-component flows over `InMemoryPageStore` and `FilePageStore`: basic CRUD, batch apply, scan, split/merge, consistent view, snapshot roundtrip, GC. |
| Crash/recovery | `libcrow-tree/tests/recovery` | Durability: snapshot + recover, torn-page, superblock A/B, double-apply idempotency. Uses a `FaultyPageStore` with fault points (drop-write, tear, flip-bytes, reorder) for the FI matrix. |
| Rust FFI | `crowkv` `tests/` | `CrowTreeEngine` trait conformance (shared parametrized tests with `InMemKV`), async bridge, buffer ownership, error mapping. |
| Cross-engine parity | `crowkv` `tests/` | `InMemKV` and `CrowTreeEngine` produce identical state via `compare()` after random op streams, snapshot export/import round-trips, and mid-stream restart. |
| Concurrency | sanitizer CI | No data races / UAF under reader+writer load (TSan/ASan/UBSan); epoch reclamation under load; version pin + GC. |

The authoritative correctness oracle is **`compare()` against `InMemKV`**: for
any op sequence, the two engines' `EngineView::iter_all` must be byte-for-byte
equal (same key set, same `(slot, cell)`).

Benchmarks (Google Benchmark C++ + criterion Rust): point read, batch apply
throughput, range scan rate, snapshot cost, delta+consolidate vs pure COW
comparison.

Tooling: `FaultyPageStore` decorator for FI matrix, seeded RNG op-stream
generator shared by parity and property tests, `InMemKV` as reference oracle.

## Sequencing

Fill gaps bottom-up so a new failure is always attributable to the lowest layer:
1. Unit + WAL/slot + Election — cheap, deterministic.
2. Replica layer — highest-value gap; unblocks confident group debugging.
3. Group reconfiguration + LearnerStream.
4. Multi-node store and deployment re-enables, after repair-correctness fixes tracked in [`plan-test.md`](../working/plan-test.md).

## Test Pairing Rule

Every feature or component milestone includes:

1. **Unit invariants** from the matching test design area (property-based or deterministic).
2. **Failure-injection** matching [`design.md`](design.md) §9 scenarios.
3. **Benchmark integration test** (end-to-end correctness + performance)
   once the RPC/client layer is reached — see Benchmark Layer above.

See [`plan-test.md`](../working/plan-test.md) for pending test task tracking.

## Feature-Dependent Test Gaps

These gaps require **new feature implementation** before tests can be written.
They are tracked here so they are not mistaken for pure test-coverage gaps.
Future implementation work should reference this section to design and define
tests for these features.

### KV Snapshot Export / Import

| Attribute | Value |
| --- | --- |
| Design doc | `design-state-machine.md` §6 |
| Depends on | New `KVEngine` trait methods (`snapshot_export`, `snapshot_import`) + `InMemKV` implementation + snapshot streaming module |
| Target layer | Unit → Replica |
| Description | Snapshot export serializes the KV state at a given slot; snapshot import replaces local KV state from a received snapshot. Tests will cover: export produces deterministic bytes, import restores state, import clears prior state, export/import round-trip preserves tombstones, snapshot install triggers WAL truncation. |

### KV Compaction / Tombstone GC

| Attribute | Value |
| --- | --- |
| Design doc | `design-state-machine.md` §7 |
| Depends on | `KVEngine::compact(watermark)` method + watermark wiring from `PxLearner` (`snapshot_slot`, `safe_slot`) + background sweeper task |
| Target layer | Unit → Replica |
| Description | Compaction removes tombstones below the safe watermark. Tests will cover: compact removes tombstones below watermark, live keys preserved, compact is idempotent, compact after snapshot install, watermark advances with learner progress. |

### WAL GC Safe-Slot Integration

| Attribute | Value |
| --- | --- |
| Design doc | — |
| Depends on | Wire group's `contiguous_applied` into WAL GC watermark instead of `u64::MAX`; see [`plan-test.md`](../working/plan-test.md) WAL GC safe slot integration |
| Target layer | WAL → Group |
| Description | Today WAL GC uses `u64::MAX` as the watermark, meaning it never trims. The group's `contiguous_applied` (the highest slot chosen by quorum) should bound GC. Tests will cover: GC trims below `contiguous_applied`, replay after GC still recovers chosen entries, GC does not trim unchosen slots. |

### WAL Disk-Loss Recovery

| Attribute | Value |
| --- | --- |
| Design doc | — |
| Depends on | Replay must handle one of N WAL disks disappearing on restart; recover surviving prefix + report loss |
| Target layer | WAL |
| Description | When a WAL disk is lost, replay should recover the surviving prefix (slots covered by remaining disks) and report the loss. Tests will cover: replay with missing disk recovers surviving slots, missing disk is reported, subsequent appends go to remaining disks. |

### Online Reconfiguration

| Attribute | Value |
| --- | --- |
| Design doc | `design-reconfiguration.md` |
| Depends on | `reconfig/` is a skeleton stub ("Real content lands in P5"); needs full joint-consensus protocol, leader transfer, quorum-overlap safety |
| Target layer | Group |
| Description | Joint consensus member add/remove, leader transfer, quorum-overlap safety. Tests will cover: add member via joint consensus, remove member, leader transfer to specified node, quorum overlap during transition, configuration change is durable. |

**Note:** The existing `KVEngine::clear()` method (used by snapshot-install
reset) is already tested via `replica/snapshot_test.rs`. Full snapshot
streaming and compaction are future features — schedule implementation before
writing tests.
