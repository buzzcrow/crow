<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: Test Strategy

Depends on: [`design-crow-kv.md`](design-crow-kv.md) §17
Satisfies: [`design-crow-kv.md`](design-crow-kv.md) §17

## Table of Contents

- [1. Overview](#1-overview)
- [2. Architecture Stack](#2-architecture-stack)
- [3. Test Binary Map](#3-test-binary-map)
- [4. Cross-Cutting Coverage Rules](#4-cross-cutting-coverage-rules)
- [5. Layer Scope](#5-layer-scope)
- [6. crow-tree C++ Test Layers](#6-crow-tree-c-test-layers)
- [7. Sequencing](#7-sequencing)
- [8. Test Pairing Rule](#8-test-pairing-rule)

## 1. Overview

CROW is a distributed key-value store built on Paxos consensus. The test
suite is organized in layers that mirror the system architecture, so a
failure points at the lowest broken layer. Each layer gets its own test
binary and tests only the logic that belongs to that layer.

This document is the **layer guide**: it defines the test strategy, the
scope of each layer, and the cross-cutting coverage rules that apply across
layers. Consult it to determine which layer a new test belongs to and what
rules apply. Per-layer coverage checklists and the live task backlog live in
[`plan-test.md`](../working/plan-test.md); benchmark design and baseline
results live in [`kv-write-flow-analysis.md`](kv-write-flow-analysis.md).

## 2. Architecture Stack

```
store      (PxKvStore: many groups, one node identity, routing)   <- lib/crow-kv/tests/store.rs
  group    (PxGroup: 1 local + N remote replicas, full Paxos)      <- lib/crow-kv/tests/group.rs
    replica  (PxLocalReplica: acceptor + learner + WAL + slots)    <- lib/crow-kv/tests/replica.rs
      election (PxLocalReplica election state machine, pure logic) <- lib/crow-kv/tests/election.rs
      wal    (WalEngine: durable log)   slot (PxSlotList)           <- lib/crow-kv/tests/wal.rs, slot.rs
        unit (pure modules: codec, classifier, kv engine, roles)   <- lib/crow-kv/tests/{paxos,kv}.rs + wal/slot codec tests
deployment (crow-kv-server binary + HTTP mgmt API + multi-process)  <- app/crow-kv-server/tests/*
console    (mgmt API server + CLI: Axum REST, CLI commands)        <- app/crow-{web,cli}/tests/*
ui e2e     (Playwright browser: SPA + real backend)                <- app/crow-web/ui/e2e/*
```

## 3. Test Binary Map

| Binary | Layer | Drives |
| --- | --- | --- |
| `lib/crow-kv/tests/paxos.rs` | unit | `acceptor`, `learner`, `error` classifier, `roles` in isolation |
| `lib/crow-kv/tests/kv.rs` | unit | `InMemKV` / `KVEngine` / `CrowTreeEngine` apply semantics |
| `lib/crow-kv/tests/election.rs` | unit | `PxLocalReplica` election state machine (role transitions, vote granting, heartbeat, lease, term fencing) |
| `lib/crow-kv/tests/wal.rs` | subsystem | `WalEngine` append/replay/gc/segment + record codec + backends |
| `lib/crow-kv/tests/slot.rs` | subsystem | `PxSlotList` / `PxSlotNode` |
| `lib/crow-kv/tests/replica.rs` | replica | single `PxLocalReplica` (no peers) |
| `lib/crow-kv/tests/group.rs` | group | `PxGroup` multi-node clusters (real loopback crow-rpc, no mocks) |
| `lib/crow-kv/tests/store.rs` | store | `PxKvStore` routing / lifecycle / status / health |
| `app/crow-kv-server/tests/*` | deployment | server binary + HTTP API, multi-process clusters, CLI, startup |
| `lib/crow-kv-client/tests/*` | client e2e | `CrowkvClient` retry, topology cache, `NotLeaderHint` follow, `AnyReplica` read distribution + fallback against embedded crow-rpc servers |
| `app/crow-web/tests/*` | console mgmt API | Axum REST API server: node management, OpenAPI proxy, API forwarding |
| `lib/crow-console-shared/tests/*`, `app/crow-cli/tests/*` | console mgmt API | shared core (config, API client, health aggregation) + CLI commands |
| `app/crow-cli/tests/bench_benchmark.rs` | benchmark | `bench benchmark` lifecycle (deploy → run → collect → report → cleanup) + `bench compare` |
| `app/crow-web/ui/e2e/*` | UI E2E | Playwright browser tests: SPA interactions, context menus, dialogs, KV panel |

## 4. Cross-Cutting Coverage Rules

**Placement rule:** a test that only needs the `crow-kv` library (even if it
binds the embedded crow-rpc server via `PxKvStore::start`) lives in `crow-kv`. A
test that boots the `crow-kv-server` binary / HTTP management API lives in
`crow-kv-server`.

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
`PxGroup`, or `crow-kv-server` process must verify two things before
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

   Performance is not asserted here; correctness only. A put that takes
   5 s but returns `ok=true` and is readable via get is a pass. Performance
   tuning is tracked separately (see Benchmark row in the binary map above
   and [`kv-write-flow-analysis.md`](kv-write-flow-analysis.md)).

   KV operations are crow-rpc only (no REST API for KV). Tests use
   `CrowkvClient` (deployment/UI layers) or `PxKvStore` public API
   directly (group/store layers). The console mgmt API layer verifies
   topology management via REST but does not perform KV operations; KV
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
   keys, catch-up should complete in seconds. Slow catch-up is a bug.

3. **Reconfig — remove replica:** remove a non-leader replica from a running
   group, verify the group continues to accept KV operations (quorum
   intact), verify health status reflects the reduced membership.

4. **Reconfig — remove leader:** remove or stop the leader's replica, verify
   a new leader is elected within bounded timeout, verify KV operations
   resume. This is the most operationally sensitive scenario; the test must
   not block indefinitely waiting for election.

## 5. Layer Scope

Each layer tests only the logic that belongs to it; a failure points at the
lowest broken layer. Detailed per-layer coverage checklists live in
[`plan-test.md`](../working/plan-test.md).

- **Unit** — Pure modules with no I/O, async runtime, or network;
  deterministic, microsecond tests. Covers `paxos/{acceptor,learner,error,
  roles}`, `kv/{mem_kv,op}`, and the `PxLocalReplica` election state
  machine (role transitions, vote granting, heartbeat, lease, term fencing).
- **WAL subsystem** — `WalEngine` and WAL internals: durable log, segment
  management, replay, GC, I/O backends, pipeline writer. Uses real temp
  filesystems or in-memory simulated disks.
- **Slot subsystem** — `PxSlotList` lock-free chunked sparse array:
  single-threaded ops, concurrent stress, reclamation watermark
  interactions with long-lived read guards.
- **Replica** — Single `PxLocalReplica` with no peers: acceptor + learner +
  WAL + slot integration: prepare/accept with WAL persistence, dedup,
  snapshot install, WAL replay ordering.
- **Group** — `PxGroup` with 1 local + N remote replicas over real loopback
  crow-rpc (no mocks): full Paxos rounds, leader election, KV through the
  group, durability under crash/restart.
- **Store** — `PxKvStore`: multi-group routing, node identity, lifecycle,
  topology status. Uses embedded crow-rpc server via `PxKvStore::start`.
- **Client E2E** — `CrowkvClient` against embedded `PxKvStore` crow-rpc
  servers: retry, topology cache refresh, `NotLeaderHint` follow,
  `AnyReplica` read distribution, `MinSlot` lagging-follower fallback. No
  `crow-kv-server` binary is booted.
- **Deployment** — `crow-kv-server` binary + HTTP management API +
  multi-process clusters: boots the actual binary, exercises the HTTP API,
  verifies multi-process cluster formation and lifecycle.
- **Console mgmt API** — Console management API server (Axum) + CLI
  frontend: REST endpoints, OpenAPI proxy, API forwarding to
  crow-kv-server nodes, shared core (config, API client, health
  aggregation), CLI commands. KV correctness is delegated to the
  deployment or UI E2E layer.
- **UI E2E** — Playwright browser tests against a real `crow-web` +
  `crow-kv-server` backend: drives the SPA as an operator would (clicks,
  context menus, dialogs, KV panel), verifies both UI feedback and backend
  state. No mocks; the only network interception is route aborting for the
  backend-unreachable test.
- **Benchmark** — Self-contained `bench benchmark` lifecycle: deploy a
  3-node cluster, drive write/read load, collect server-side metrics +
  logs, produce a report, then clean up. Exercises the full production
  path end-to-end. Design, storage modes, and baseline results are in
  [`kv-write-flow-analysis.md`](kv-write-flow-analysis.md).

## 6. crow-tree C++ Test Layers

The C++ crow-tree library (`libcrow-tree`) has its own test layers, separate
from the Rust test binaries above. They run as `test-tree-ct` in CI.

| Layer | Where | What it proves |
| --- | --- | --- |
| C++ unit | `liblib/crow-tree/tests/unit` | Single component correctness: cell encoding, leaf page build/read, delta replay, consolidation triggers, mapping table, epoch manager, split point. |
| C++ integration | `liblib/crow-tree/tests/integration` | Multi-component flows over `InMemoryPageStore` and `FilePageStore`: basic CRUD, batch apply, scan, split/merge, consistent view, snapshot roundtrip, GC. |
| Crash/recovery | `liblib/crow-tree/tests/recovery` | Durability: snapshot + recover, torn-page, superblock A/B, double-apply idempotency. Uses a `FaultyPageStore` with fault points for the FI matrix. |
| Rust FFI | `crow-kv` `tests/` | `CrowTreeEngine` trait conformance (shared parametrized tests with `InMemKV`), async bridge, buffer ownership, error mapping. |
| Cross-engine parity | `crow-kv` `tests/` | `InMemKV` and `CrowTreeEngine` produce identical state via `compare()` after random op streams, snapshot export/import round-trips, and mid-stream restart. |
| Concurrency | sanitizer CI | No data races / UAF under reader+writer load (TSan/ASan/UBSan); epoch reclamation under load; version pin + GC. |

The authoritative correctness oracle is **`compare()` against `InMemKV`**: for
any op sequence, the two engines' `EngineView::iter_all` must be byte-for-byte
equal (same key set, same `(slot, cell)`).

## 7. Sequencing

Fill gaps bottom-up so a new failure is always attributable to the lowest
layer:
1. Unit + WAL/slot + Election — cheap, deterministic.
2. Replica layer — highest-value gap; unblocks confident group debugging.
3. Group reconfiguration + LearnerStream.
4. Multi-node store and deployment re-enables, after repair-correctness
   fixes tracked in [`plan-test.md`](../working/plan-test.md).

## 8. Test Pairing Rule

Every feature or component milestone includes:

1. **Unit invariants** from the matching test layer (property-based or
   deterministic).
2. **Failure-injection** matching [`design-crow-kv.md`](design-crow-kv.md) §9
   scenarios.
3. **Benchmark integration test** (end-to-end correctness + performance)
   once the RPC/client layer is reached.

See [`plan-test.md`](../working/plan-test.md) for pending test task tracking.
