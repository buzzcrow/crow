# CrowKV Test Strategy

## Overview

CrowKV is a distributed key-value store built on Paxos consensus. The test
suite is organized in layers that mirror the system architecture, so a
failure points at the lowest broken layer. Each layer gets its own test
binary and tests only the logic that belongs to that layer.

This document defines the **test strategy**, **scope of each layer**, and
**high-level test coverage**. It is the reference for designing new tests
when implementing features or components — consult this doc to determine
which layer a new test belongs to and what it should cover.

For the live task backlog (unfinished test gaps), see `plan-test.md`.

## Architecture Stack

```
store      (PxKvStore: many groups, one node identity, routing)   <- crowkv/tests/store.rs
  group    (PxGroup: 1 local + N remote replicas, full Paxos)      <- crowkv/tests/group.rs
    replica  (PxLocalReplica: acceptor + learner + WAL + slots)    <- crowkv/tests/replica.rs
      election (PxLocalReplica election state machine, pure logic) <- crowkv/tests/election.rs
      wal    (WalEngine: durable log)   slot (PxSlotList)           <- crowkv/tests/wal.rs, slot.rs
        unit (pure modules: codec, classifier, kv engine, roles)   <- crowkv/tests/{paxos,kv}.rs + wal/slot codec tests
deployment (crowkv-server binary + HTTP mgmt API + multi-process)  <- crowkv-server/tests/*
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
| `crowkv/tests/group.rs` | group | `PxGroup` multi-node clusters (real loopback gRPC, no mocks) | `cluster/{group,group_election,remote_replica,peer_stream}.rs` |
| `crowkv/tests/store.rs` | store | `PxKvStore` routing / lifecycle / status / health | `cluster/{px_kv_store,kv_store,kv_server,status}.rs` |
| `crowkv-server/tests/*` | deployment | server binary + HTTP API, multi-process clusters, CLI, startup | `crowkv-server/src/*` |

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

**Covered:**
- Acceptor promise/accept fence, ballot ordering, prior accepted.
- Error keyword + retry-action classification.
- Learner note-chosen, dedup cache (`dedup_lookup`), durable-watermark tracking.
- `PxLogEntry` / `PxBallot` edge cases (NoOp vs Accepted, ballot tie-break).
- `InMemKV` highest-slot-wins, idempotent apply, tombstone, intra-batch, prefix scan, compare, wire decode.
- `kv/op` wire-format encode/decode.
- `wal/record` encode/decode round-trip, CRC + truncation errors.

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

**Covered:**
- Role transitions: follower (adopts higher term, clears leader), precandidate (no term bump), candidate (bumps term, votes self, extends lockout), leader (sets self, resets lease).
- `new_inheriting_election_state`: preserves term/voted_for/role, shares acceptor + learner.
- `PreVote`: grants on higher term + up-to-date log, rejects stale term/log/lockout, no term bump or voted_for mutation.
- `RequestVote`: grants on higher term + up-to-date log, rejects stale log/lockout/lower term, adopts term + sets voted_for, lockout prevents re-grant, reply carries frontier triple.
- Heartbeat: adopts higher term, records leader, rejects lower term, accepts equal term, applies committed entries up to commit_slot, idempotent, steps down leader on higher-term heartbeat.
- Lease: requires leader + unexpired lease, expires after deadline, monotonic extension, reset on role change, renew_lease extends by configured duration minus skew.
- Term fencing in prepare/accept: rejects stale term, adopts higher term, forwards on equal term.
- Election metrics: election_count bumps on become_candidate, snapshot reflects state.
- `handle_step_down`: strict-fence policy (accepts only if leader + matching term + matching target_leader_id), rejects when not leader / term mismatch / wrong target, preserves term on accept, double step-down rejected, reply reports actual term and leader.
- `frontier_triple` consistency: zero on fresh replica, reflects accepted and learned slots, preserves across role transitions (candidate, leader, follower), handles gaps in accepted log, advances with progressive learn, carried in both PreVote and RequestVote replies.

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
- `io_backend` — `IoBackend::detect()` selection, `open`/`rename`/`unlink`/`read_dir`/`create_dir_all`/`exists`.
- `index` — in-memory segment index (insert/locate, register/remove segment, rebuild-from-scans, slot_count).
- `pipeline_writer` — batch coalescing, ack ordering, seal, index updates, batch stats (via `WalEngine` public API).
- `record` — encode/decode round-trip, CRC + truncation errors.

**Covered:** All modules listed above are tested. No open gaps.

### Slot Subsystem Layer

**Scope:** `PxSlotList` — lock-free chunked sparse array. Tests cover
single-threaded operations, concurrent stress, and reclamation watermark
interactions with long-lived read guards.

**Modules:**
- `PxSlotList` — insert/get/trim/reclaim, chunk growth, sparse insert, tail lookup, atomic guard, range iteration.
- `PxSlotNode` — accept-after-promise, duplicate insert, trim/reclaim with live refs.
- Concurrent stress — multi-thread insert at disjoint ranges, concurrent insert+read, insert+trim+reclaim, full insert+trim+reclaim+read stress.
- Reclamation watermark — long-lived guard prevents reclaim, multiple guards pin chunk, idempotent reclaim, progressive trim across chunks.

**Covered:** All modules listed above are tested. No open gaps.

### Replica Layer

**Scope:** Single `PxLocalReplica` with no peers. Tests exercise the
acceptor + learner + WAL + slot integration: prepare/accept with WAL
persistence, dedup suppression, snapshot install, WAL replay ordering.

**Covered:**
- WAL-backed persistence round-trip: single Put, overwrite (highest-slot-wins), Delete (tombstone survives restart), put-then-delete same key, batch with intra-batch put+delete (last-wins), mixed put/delete across multiple slots and batches.
- Classic prepare/accept tracking.
- KV operation correctness: Put applies value, overwrite replaces previous, Delete produces tombstone, delete on non-existent key is no-op, batch with multiple puts, intra-batch last-wins (put→delete→put ordering), empty batch (NoOp), multiple slots with mixed ops.
- Dedup suppression.
- Snapshot install / truncate-and-resume.
- Multi-slot WAL replay ordering: contiguous slots below watermark all applied, hole below watermark stops at hole (partial apply), out-of-order WAL records rebuilt correctly (replay sorts by slot), slots above watermark not applied (left for consensus), empty WAL produces zero state, watermark higher than accepted stops at first hole.
- Concurrent learn_chosen + on_accept: sequential learn-then-accept and accept-then-learn on same slot, concurrent tokio::join! on same slot (no panic, consistent state), concurrent on adjacent slots, re-accept with different value after learn, 10 concurrent accepts on disjoint slots, 5 concurrent learn_chosen on sequential slots.

### Group Layer

**Scope:** `PxGroup` with 1 local + N remote replicas using real loopback
gRPC (no mocks). Tests exercise full Paxos rounds, leader election, KV
through the group, durability under crash/restart.

**Covered:**
- Single-leader propose, sequential slot allocation, follower rejects, classic propose.
- Proposer window full → busy; repair fills gap and advances frontier.
- Election: 1–7 replica counts elect a single leader, driver scaffold, step-down, propose-after-step-down.
- KV through the group: Put + BatchWrite (puts) + Delete apply to all learners, follower forwards Get/Scan, forward loop-guard. **Gap:** full op correctness checklist not yet covered (see `plan-test.md`).
- Remote replica transport: unreachable/invalid endpoint returns error.
- Preemption retry, kv-slot retry on prior accepted value.
- Durability: single-node crash/restart, full-cluster restart keeps deletes.

### Store Layer

**Scope:** `PxKvStore` — multi-group routing, node identity, lifecycle
management, topology status. Tests use embedded gRPC server via
`PxKvStore::start`.

**Covered:**
- Single-node KV / read modes, follower redirect hint, dedup.
- Multi-group routing within one node, dynamic add/remove group, missing-group error.
- KV ops: Put + BatchWrite (puts) + Delete via `kv_put`/`kv_delete`/`kv_batch_write`, persistence round-trip (put/overwrite/delete survive restart). **Gap:** full op correctness checklist not yet covered (see `plan-test.md`).
- Topology `status` composition, `health` levels, `shutdown` cascade + idempotency.

### Deployment Layer

**Scope:** `crowkv-server` binary + HTTP management API + multi-process
clusters. Tests boot real processes and exercise the HTTP API end-to-end.

**Covered:**
- HTTP management API (health, openapi, stores/groups CRUD, conflicts).
- Real-process 3-node cluster KV + topology + dynamic group mgmt.
- CLI parsing, startup WAL restore/resume.

## Sequencing

Fill gaps bottom-up so a new failure is always attributable to the lowest layer:
1. Unit + WAL/slot + Election — cheap, deterministic.
2. Replica layer — highest-value gap; unblocks confident group debugging.
3. Group reconfiguration + PeerStream.
4. Multi-node store and deployment re-enables, after repair-correctness fixes tracked in `plan-test.md`.

## Suite Timing

Measured on 2026-07-12 on the current development machine. All six suites passed
with zero failures. Times are the wall-clock duration of `pixi run <suite>`
(including build/compile overhead where applicable).

| Suite | Result | Tests | Real time |
| --- | --- | --- | --- |
| `test-ct` | pass | 291/291 | 8.05 s |
| `test-core` | pass | all green | 8.18 s |
| `test-server` | pass | all green | 8.08 s |
| `test-cli` | pass | all green | 8.94 s |
| `test-web` | pass | all green | 34.55 s |
| `test-ui` | pass | 23/23 | 33.19 s |
| **Total** | **pass** | — | **~101 s** |

The C++ Crowtree tests (`test-ct`) and the Rust core tests (`test-core`) are
the fastest. The console/web suites dominate total wall time because they boot
real browser and server processes.

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
| Depends on | Wire group's `contiguous_applied` into WAL GC watermark instead of `u64::MAX`; see `plan-test.md` WAL GC safe slot integration |
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
