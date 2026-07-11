# CrowKV - Design: Master

Depends on: [`requirement.md`](requirement.md)
Satisfies: all of [`requirement.md`](requirement.md) (this is the root design doc)

This is the master design document. It establishes the conceptual model, module decomposition, and cross-cutting flows. Deep dives for the heavy areas live in sibling sub-topic docs (`design-parallel-slots.md`, `design-leader-election.md`, `design-wal.md`, `design-reconfiguration.md`, `design-storage-engine.md`, `design-async-io.md`, `design-rpc.md`).

The doc explains **what the system is** and **how it behaves**. It does not prescribe an implementation phasing (that lives in `plan.md`) and does not enumerate test scenarios (those live in `test.md`).

## Table of Contents

- [1. Design Philosophy](#1-design-philosophy)
- [2. Architecture Overview](#2-architecture-overview)
- [3. Module Decomposition](#3-module-decomposition)
- [4. Core Data Shapes](#4-core-data-shapes)
  - [4.1 PxLogEntry](#41-pxlogentry)
  - [4.2 Ballot and Term](#42-ballot-and-term)
  - [4.3 Slot State Machine](#43-slot-state-machine)
  - [4.4 Group Configuration](#44-group-configuration)
- [5. Write Flow](#5-write-flow)
  - [5.1 Steady-State Pipelined Write](#51-steady-state-pipelined-write)
  - [5.2 Cold-Start / New-Leader Write](#52-cold-start--new-leader-write)
  - [5.3 Batched Write](#53-batched-write)
- [6. Read Flows](#6-read-flows)
  - [6.1 Linearizable Leader Read](#61-linearizable-leader-read)
  - [6.2 Read-Your-Writes Follower Read](#62-read-your-writes-follower-read)
  - [6.3 Bounded-Stale Follower Read](#63-bounded-stale-follower-read)
  - [6.4 Scan Modes](#64-scan-modes)
- [7. Cluster Bootstrap and Group-0](#7-cluster-bootstrap-and-group-0)
- [8. Cross-Cutting Topics](#8-cross-cutting-topics)
  - [8.1 Leader Election](#81-leader-election)
  - [8.2 Parallel Slot Pipelining](#82-parallel-slot-pipelining)
  - [8.3 Durability and WAL](#83-durability-and-wal)
  - [8.4 Snapshot and Install](#84-snapshot-and-install)
  - [8.5 Reconfiguration](#85-reconfiguration)
  - [8.6 Idempotency / Dedup Cache](#86-idempotency--dedup-cache)
  - [8.7 Storage Engine Plug-In](#87-storage-engine-plug-in)
- [9. Failure Mode Catalogue](#9-failure-mode-catalogue)
- [10. Observability Hooks](#10-observability-hooks)
- [11. Open Design Questions](#11-open-design-questions)
- [12. References](#12-references)

---

## 1. Design Philosophy

CrowKV is engineered as **"Raft for everything that doesn't matter for performance, Multi-Paxos for the one thing that does."**

- **Reuse Raft where it is mature.** Leader election, leader leases, snapshot install, joint-consensus reconfiguration, log replay semantics — Raft has settled designs that are well-understood, well-tested, and easy to reason about. CrowKV adopts them.
- **Diverge from Raft only on the hot path.** A Raft leader cannot acknowledge slot N+1 until slot N has been committed; the log is contiguous by construction. Multi-Paxos lifts that constraint: each slot is an independent Paxos instance and may be decided in any order. Under load this is the difference between a sequential bottleneck and a fully pipelined commit path. We pay for it with extra complexity around gap repair and a slightly more conservative cross-key read frontier (see [§6.4](#64-scan-modes), [§8.2](#82-parallel-slot-pipelining)).
- **Blind operations only.** `Put` and `Delete` do not depend on the current value, so out-of-order apply is safe. This is the enabling premise; it is why `CAS` and `Increment` are excluded ([requirement.md §5.2](requirement.md#52-operations)).
- **Linearizability for the leader; explicit weaker modes for followers.** Clients that want strong reads pay for them; clients that want low-latency stale reads can opt in. The system never silently gives weaker semantics than the client asked for.
- **Pluggable storage; persistent log is the source of truth.** The Acceptor's WAL is durable; the learner's btree is a derived projection. The engine can be swapped (in-memory, ordered file, crowtree) without changing consensus semantics.

The literature backing each choice is collected in [§12 References](#12-references).

---

## 2. Architecture Overview

A CrowKV cluster is a set of physical nodes. Each physical node runs one `KvStore`. A `KvStore` may host **multiple** `PxGroup`s — Group-0 (system, holds topology) and any number of data groups. Membership in a group is independent of physical-node identity.

```
                              CrowKV Cluster
   ┌────────────────────────────────────────────────────────────────┐
   │   KvStore A               KvStore B               KvStore C    │
   │   ┌─────────┐             ┌─────────┐             ┌─────────┐  │
   │   │Group-0 L│  ◄────────► │Group-0 F│  ◄────────► │Group-0 F│  │
   │   │Group-1 F│             │Group-1 L│             │Group-1 F│  │
   │   │Group-2 F│             │Group-2 F│             │Group-2 L│  │
   │   └─────────┘             └─────────┘             └─────────┘  │
   │                                                                │
   └─────────▲──────────────────────────────────────────────────────┘
             │  describe-cluster RPC + per-group writes/reads
             │
        ┌────┴────┐
        │ Client  │  sends KV RPC → KvStore selects group by group_id
        │ library │
        └─────────┘
```

- **Group-0** is a special system group whose log records cluster topology, partitioning rule (`num_groups`), the per-group membership, and the cluster `config_version`. Clients learn this via the describe-cluster RPC at startup.
- **KvStore** is the KV-facing runtime on one physical node. `KvService` delegates KV operations to `KvStore`; the store selects a `PxGroup` by explicit `group_id` routing hint and drives that group through Paxos.
- **PxGroup** is Paxos-only. It does not depend on KV semantics. It exposes Paxos behavior such as proposing values and running accept/learn processing over opaque log entries.
- **Local and remote replicas** compose a group on one store. Each `PxGroup` contains exactly one `PxLocalReplica` for the local member and zero or more `PxRemoteReplica` proxies for members on other stores.
- **PxLocalReplica** plays acceptor and learner roles for one group member. It owns the slot list and learner storage, delegating acceptor state operations to `PxAcceptor` and learned-value application to `PxLearner`.
- **PxRemoteReplica** is an RPC utility/proxy for a replica on a remote physical node. It owns or uses connection-management facilities for that remote endpoint; the local group communicates with it only through RPC.
- **Data groups** (`Group-1` … `Group-N`) hold key ranges. The mapping is `group_id = hash(key) % num_groups`, fixed at cluster creation ([requirement.md §7.4](requirement.md#74-sharding)).
- A node typically participates in Group-0 plus a subset of data groups. Group sizes can differ (e.g. Group-0 = 5, data groups = 3) for higher metadata availability.
- All inter-node and client-facing communication is gRPC + protobuf, with append-only field numbers for rolling-upgrade compatibility ([requirement.md §9.2](requirement.md#92-rolling-upgrade)).

---

## 3. Module Decomposition

The `crowkv` library is structured as a small set of cooperating modules. The separation is logical, not necessarily one-to-one with future Rust crates.

| Module | Responsibility | Key collaborators |
| --- | --- | --- |
| **Proposer** | Owns the slot counter on the leader; assigns slots; drives Phase 1 once on leader change and Phase 2 per write in steady state. | Leader Elector, WAL, Replicator |
| **Acceptor** | Maintains promised/accepted state per slot; persists every state change to WAL before responding. | WAL |
| **Learner** | Tracks chosen values; applies them to the storage engine; maintains per-key resolved-slot. | Storage Engine, Acceptor |
| **WAL** | Durable, multi-disk write-ahead log. Sole persistent ground truth. | (disk) |
| **KvStore** | KV-facing runtime per physical node; owns one or more `PxGroup`s; implements KV routing by explicit `group_id` and exposes `KvService`. | PxGroup, RPC |
| **PxGroup** | Paxos-only group runtime; coordinates one local replica with remote replica proxies and drives proposer/accept/learn flows over opaque log entries. | Local Replica, Remote Replica |
| **PxLocalReplica** | Local group member; plays acceptor and learner; owns slot list and learner storage. | Acceptor, Learner |
| **PxRemoteReplica** | RPC proxy/connection utility for a group member on another physical node. | RPC |
| **Replicator** | Streams `Accept` and `Chosen` messages from leader to peers; handles backpressure. | Proposer, Learner, RPC |
| **Leader Elector** | Raft-style election; manages `PxTerm`; emits leader-change events. | RPC |
| **Lease** | Holds and renews the leader lease used for fast linearizable reads; falls back to ReadIndex on demand. | Leader Elector |
| **Repair** | Background async task that detects and resolves slot gaps via classic Paxos. | Proposer, Acceptor |
| **Snapshot** | Takes per-group snapshots; serves snapshot install to lagging peers. | Storage Engine, WAL |
| **Dedup Cache** | Per-`client_id` last-applied sequence; persisted in the log stream so it survives leader change. | Learner, Storage Engine |
| **Storage Engine** | Pluggable trait. In-memory tree, ordered file, and crowtree backends share one interface. | Learner, Snapshot |
| **Topology / Group-0 Client** | Caches cluster topology and the group→leader map; refreshed on `NotLeader`. | RPC |
| **RPC** | Thin gRPC layer with retries and `NotLeader` handling. | (network) |

Single-leader hot path on the leader: **Proposer → WAL → Replicator → Learner → ack to client.** All other modules support this path or recover from its failures.

---

## 4. Core Data Shapes

This section defines the conceptual shape of the durable and runtime types. No Rust syntax — those live in code, not in `design.md`.

### 4.1 `PxLogEntry`

A single durable record at one slot. Conceptually a tuple of:

- `slot` — monotonically increasing, gap-free in *intent*, may temporarily have gaps in *resolution*.
- `ballot` — the `(round, leader_id)` pair under which this value was accepted (Paxos Phase 2 semantics).
- `term` — the leader's `PxTerm` at the time of proposal (used for fencing; see [§8.1](#81-leader-election)).
- `kind` — one of `{ Write, NoOp, ConfigChange, DedupCheckpoint }`.
- `payload` — for `Write`, the batch of `(key, op, value?)` tuples; for `NoOp`, empty (used to fill gaps); for `ConfigChange`, the new group membership; for `DedupCheckpoint`, a snapshot of the dedup cache.
- `client_id` + `seq` — for `Write` only, used by the Dedup Cache.
- `crc` — record-level checksum.

### 4.2 Ballot and Term

`PxBallot` and `PxTerm` are kept separate, as decided in [requirement.md §4.2](requirement.md#42-paxos-core).

- **`PxTerm`** is a monotonic, persistent epoch incremented on every leader election (Raft's "term"). It is the unit of fencing: any message tagged with a term lower than the receiver's current term is rejected.
- **`PxBallot`** is the Paxos proposal number, `(round, leader_id)`. In steady state a leader uses `(0, leader_id)` for Phase-2-only writes; round increments only when classic-Paxos repair is needed for a specific slot.
- A new leader elected at term `T` runs Phase 1 once for the open slot prefix using ballot `(T, leader_id)`. From then on, accepts use ballot `(0, leader_id)` indexed by term-implied freshness.

This decoupling means election logic does not interfere with per-slot Paxos rounds, and per-slot repair does not require an election.

### 4.3 Slot State Machine

Per slot, on the leader (proposer view):

| State | Entered by | Next on success | Next on failure |
| --- | --- | --- | --- |
| `Empty` | initial | `Proposed` (slot assigned) | — |
| `Proposed` | leader assigns slot | `Accepted` (a quorum has accepted) | `Repairing` (timeout, leader change) |
| `Accepted` | quorum accept observed | `Chosen` (≡ same condition; conceptually atomic) | — |
| `Chosen` | ≥ majority of acceptors have `Accepted` for the same `(ballot, value)` | `Applied` (learner applied) | — |
| `Applied` | learner wrote to storage engine | terminal | — |
| `Repairing` | gap detected by Repair | `Chosen` (classic Paxos chose original or no-op) | re-enter `Repairing` |

Per slot, on an acceptor:

| State | Entered by | Persists |
| --- | --- | --- |
| `None` | initial | nothing |
| `Promised(b)` | Phase 1 from ballot `b` | yes |
| `Accepted(b, v)` | Phase 2 from ballot `b` value `v` | yes (via WAL fsync) |

`Chosen` is *learned*, not persisted as a separate state by acceptors; it is reconstructed by the learner from a quorum of `Accepted` messages.

### 4.4 Group Configuration

`PxGroupConfig` is the membership of one group. Stored in two places:

- The current `PxGroupConfig` lives in **Group-0's log** as a `ConfigChange` entry; this is the cluster-level source of truth.
- Each member's local state caches the *active* config and, during reconfiguration, the *joint* config (see [§8.5](#85-reconfiguration)).

A config carries: `group_id`, `members[]` (each with `node_id`, `endpoint`, voting flag), `quorum_size`, `config_version`.

---

## 5. Write Flow

### 5.1 Steady-State Pipelined Write

This is the hot path that the design optimizes. Multiple writes are in flight at the same time, each at its own slot, with consensus running in parallel.

```
   client       leader (Proposer + WAL)        followers (Acceptors + Learners)
     │                  │                                    │
     │── Put(k,v) ─────►│                                    │
     │                  │ assign slot N (counter++)          │
     │                  │ append PxLogEntry to WAL           │
     │                  │── Accept(N, ballot, v) ─────────► │
     │                  │                                    │ fsync WAL
     │                  │◄──────────── Accepted(N) ──────────│
     │                  │ (quorum reached → Chosen)          │
     │                  │ apply slot N to its own learner    │
     │                  │── Chosen(N) ───────────────────► │ apply
     │◄── ack(slot=N) ──│                                    │
     │                                                       │
     │── Put(k2,v2) ───►│  meanwhile slot N+1 already in flight, slot N+2 too...
```

Key properties:

- **Slot assignment is the linearization point** ([requirement.md §6.1](requirement.md#61-write-guarantee)). The counter is owned by a single async task on the leader (serial assignment, no shared mutex); assignment happens before any I/O. See [`plan.md`](plan.md) §5 for the project-wide concurrency model.
- **Ack contract**: the leader ack to the client requires (a) leader's own WAL fsync completed and (b) a quorum of acceptors have responded `Accepted` after their fsync. Until both, the leader does not respond.
- **Parallelism**: slots N, N+1, N+2 may be in any of `Proposed` / `Accepted` / `Chosen` / `Applied` independently. The leader does not wait for slot N to apply before assigning N+1.
- **Backpressure**: if the in-flight window is full, the leader admits to a bounded queue and beyond that returns `Busy` ([requirement.md §7.3](requirement.md#73-parallel-slot-processing)). The leader never blocks indefinitely.

The mechanics — sliding window, fanout, gap detection — are detailed in [`design-parallel-slots.md`](design/design-parallel-slots.md).

### 5.2 Cold-Start / New-Leader Write

On leader change, the new leader does **not** know which slots were chosen by the previous leader. It runs a single Phase 1 round at its new term:

```
   newLeader                          followers
       │                                  │
       │── Prepare(ballot=(T,me)) ──────►│
       │                                  │ for each slot ≥ open_prefix:
       │                                  │   if Promised(b<my) → return Promised(my)
       │                                  │   if Accepted(b,v) → return (b,v)
       │◄──────── Promise(slots, accepts)─│
       │ for each slot with returned (b,v): adopt v at this slot
       │ for each empty slot in the open range: ready to accept new values
       │ (proposer is now in steady state)
```

After this single round, the leader can issue Phase-2-only writes for new slots. Open slots returned with prior accepts are immediately re-proposed at the new ballot to make them `Chosen`.

This recovery step is bounded by the open-slot range, which is bounded by the parallel-slot window — typically tens of slots. It is amortized over thousands of subsequent writes.

### 5.3 Batched Write

A `BatchPut` / `BatchDelete` arrives as one client request. The proposer assigns it **one** slot. The payload carries multiple `(key, op, value?)` tuples. Intra-batch order is "as written by the client" ([requirement.md §7.3.1](requirement.md#731-correctness-analysis-for-parallel-slot-writes)). On apply, the learner walks the tuples in order and updates per-key `(slot, value)` for each.

Batching is preferred for high-throughput clients; per-op overhead drops to a fraction of an unbatched write. The optimization is described in [requirement.md §12.2](requirement.md#122-batch-operations).

---

## 6. Read Flows

### 6.1 Linearizable Leader Read

```
   client                        leader
     │                              │
     │── Get(k, mode=Linearizable)─►│
     │                              │ if lease valid → serve from learner state
     │                              │ else (lease expired / disabled) → ReadIndex:
     │                              │   send heartbeat to quorum, await acks
     │                              │   then serve from learner state
     │◄────── value, slot ──────────│
```

The lease check is constant-time when valid. ReadIndex adds one round-trip but no fsync. The leader's learner already reflects every chosen slot on the leader (the leader applies before acking writes), so the returned value is the latest in the linearization order.

Details of lease and ReadIndex live in [`design-leader-election.md`](design/design-leader-election.md).

### 6.2 Read-Your-Writes Follower Read

```
   client                        follower
     │                              │
     │── Get(k, mode=RYW, slot=N)──►│
     │                              │ wait until per-key resolved-slot[k] ≥ N
     │                              │ (typically already the case; bounded wait)
     │◄────── value ────────────────│
```

`N` is the slot the client received from its last write to key `k`. The follower compares against per-key resolved-slot, not the global safe-slot. This avoids being held up by gaps that do not affect this key.

### 6.3 Bounded-Stale Follower Read

```
   client                        follower
     │                              │
     │── Get(k, mode=Stale, ss=S)─►│
     │                              │ wait until follower's global safe-slot ≥ S
     │◄────── value ────────────────│
```

`S` is the client's last known safe-slot, returned in any prior server response. The mode trades freshness for the ability to serve from any follower without per-key tracking.

### 6.4 Scan Modes

`Scan(start, end, limit, mode)` has three modes; each maps to a different replica and a different wait condition.

| Mode | Where served | Wait condition | Use case |
| --- | --- | --- | --- |
| `Linearizable` (default) | Leader | Leader's own contiguous applied frontier ≥ `target` (= leader's max-chosen at request entry) | Strict cross-key reads; tolerates gap-bounded latency |
| `SafeSlot` | Any follower | Follower's applied slot ≥ group `safe-slot` | Bounded-stale, zero-wait analytics-style scans |
| `AtSlot(N)` | Any replica with applied ≥ `N` | applied ≥ `N` | Repeating a previous snapshot read at the same logical instant |

The linearizable mode uses the **leader's own** contiguous frontier, not the cross-learner safe-slot, because the leader's learner is always at-or-ahead of safe-slot. This is strictly faster than a safe-slot wait while preserving linearizability ([requirement.md §6.5](requirement.md#65-parallel-slot-linearizability-analysis)).

---

## 7. Cluster Bootstrap and Group-0

A fresh cluster bootstraps through Group-0:

1. Operator starts each `PxNode` with a static config: own `node_id`, listen endpoint, seed-list, and the initial Group-0 membership.
2. The Group-0 members run a leader election among themselves. The first leader writes the initial cluster topology entry into Group-0's log: `num_groups`, partitioning rule, per-group membership.
3. Each `PxNode` reads Group-0 (as a Group-0 follower or as a regular RPC client) to learn which data groups it must host. It then starts the per-group state machines.
4. Data-group leaders are elected; each leader writes a `NoOp` at slot 1 of its log to assert leadership (this is the standard Raft-style commit-empty-entry pattern adapted to Paxos).

Steady-state client discovery:

- The client library is configured with a **seed list** of one or more `PxNode` endpoints.
- It calls **describe-cluster** on any seed; the response carries the current Group-0 leader and the cached group→leader map.
- For each subsequent operation, the client hashes the key, finds the group, and sends the request directly to that group's cached leader.
- On `NotLeader { hint }`, the client retries the hint immediately and refreshes its cache. On unknown leader, it waits 1 s then retries ([requirement.md §10.2](requirement.md#102-retry-and-idempotency)).
- The client never queries Group-0 on the hot path; it does so only on cache miss, on `NotLeader`, or on a scheduled refresh.

Group-0 vs data groups behave identically with respect to consensus; they differ only in the kinds of payloads they accept (`ConfigChange` for Group-0, `Write` for data groups).

---

## 8. Cross-Cutting Topics

This section is a one-page-each summary of each cross-cutting flow. The full design lives in the linked sub-topic doc.

### 8.1 Leader Election

CrowKV uses Raft-style randomized-timeout elections within a group. Each member maintains a current `PxTerm`. Followers expect to hear from the leader (heartbeat) within a randomized election timeout; if not, they increment term, become a candidate, and request votes from peers. A candidate wins on majority votes within the same term.

A new leader runs **one** Phase-1 Paxos round at ballot `(T, leader_id)` over the open slot prefix to discover any in-flight values from the previous leader (see [§5.2](#52-cold-start--new-leader-write)). After that, it serves Phase-2-only writes.

The leader holds a **lease** for fast linearizable reads. Lease duration must be much greater than the assumed clock skew bound ([requirement.md §3](requirement.md#3-dependencies-and-assumptions)). On lease expiry, the leader either renews via a heartbeat round-trip or downgrades to ReadIndex per linearizable read.

Step-down triggers: lease unrenewable (lost contact with quorum), seeing a higher term in any RPC response, admin-forced step-down, or being removed from the group via reconfiguration.

→ Full design: [`design-leader-election.md`](design/design-leader-election.md).

### 8.2 Parallel Slot Pipelining

The defining feature of CrowKV. Within a group:

- A **sliding window** caps in-flight slots at a configurable size (default 16). Smaller windows reduce gap-repair work; larger windows raise throughput but worst-case latency.
- The leader **pipelines** Phase-2 messages: it does not wait for slot N's `Accepted` quorum before fanning out slot N+1.
- A **background repair async task** scans for stale undecided slots (e.g. older than a threshold or below a moving median) and runs classic Paxos to resolve them. If no acceptor has a value, the repair fills with a `NoOp`.
- The **safe-slot** is computed as `min(per-learner contiguous-applied)`; it is the cluster's no-gap frontier and the basis for follower read modes.
- The leader's own **contiguous applied frontier** is maintained separately and used by `Scan(Linearizable)` because it is strictly ≥ safe-slot.

Correctness rests on the blind-ops premise: out-of-order apply is safe when no operation reads before writing.

→ Full design: [`design-parallel-slots.md`](design/design-parallel-slots.md).

### 8.3 Durability and WAL

The Acceptor's WAL is the only persistent log. Properties:

- **Multi-disk segments**, slot-tagged. Slots are assigned to segments by simple round-robin (or tag-by-disk-load) so multiple disks fsync in parallel.
- **Batched fsync** with a configurable batch size or time interval; a watchdog timer forces flush in case the batch never fills.
- **CRC32C** per record; replay truncates at the first CRC failure and triggers peer-based catch-up.
- **Ack contract**: an `Accepted` is sent only after that record's fsync completes. A client write is acked only after a quorum of `Accepted`s ([requirement.md §8.1](requirement.md#81-wal-write-ahead-log)).
- **Disk loss** → the node fails itself out of that group and rebuilds from peers via snapshot install.

→ Full design: [`design-wal.md`](design/design-wal.md). The async disk-I/O substrate (io_uring + fallback) is specified in [`design-async-io.md`](design/design-async-io.md).

### 8.4 Snapshot and Install

Each group takes per-group snapshots when its WAL exceeds a configured size or slot count. A snapshot is a consistent dump of the storage engine's state at some applied slot, plus the persisted dedup cache.

- **Trigger**: WAL size / slot threshold.
- **Effect**: WAL prefix below the snapshot slot becomes safe to GC.
- **Install protocol**: chunked, byte-offset based (resumable), end-to-end CRC, throttleable. A new node or one whose WAL is older than the leader's earliest retained slot must receive a snapshot before resuming WAL-based catch-up.
- **Source**: leader or any caught-up learner.

The snapshot file format and engine-specific export/import mechanics live in [`design-storage-engine.md`](design/design-storage-engine.md).

### 8.5 Reconfiguration

Membership changes use Raft-style **joint consensus** adapted to Paxos:

- The transition `C_old → C_new` goes through an intermediate joint config `C_old ∪ C_new` where decisions require quorums from *both* old and new memberships.
- New members first receive a **snapshot install** to bootstrap, then catch up the WAL tail before becoming voting members.
- Removed members complete their in-flight responsibilities, transfer leadership if needed, and retire after the new config is committed.
- Group-0 reconfiguration uses the same machinery; the only special case is that Group-0 holds the cluster topology and must serialize topology changes with its own membership changes.

Supported transitions: 3 ↔ 5 ↔ 7. Larger or smaller groups are not in scope.

→ Full design: [`design-reconfiguration.md`](design/design-reconfiguration.md).

### 8.6 Idempotency / Dedup Cache

To make retries safe, the leader maintains a per-`client_id` dedup cache of last-applied `(seq, result)`. To survive leader change, the cache is **persisted into the log stream**:

- Each `Write` log entry carries `(client_id, seq)`. On apply, the learner updates the in-memory dedup map.
- Periodically (or on size threshold) the leader appends a `DedupCheckpoint` entry that snapshots the cache. After a leader change, the new leader rebuilds the cache by scanning back from the latest checkpoint.
- Retention is bounded: at least *N* requests per active client and *T* seconds, evicted by LRU after that. Outside the window, retries are no longer guaranteed idempotent ([requirement.md §10.2](requirement.md#102-retry-and-idempotency)).

This puts dedup state in the same fault domain as the data, so it cannot diverge.

### 8.7 Storage Engine Plug-In

The Learner talks to a single engine trait. Three engines satisfy it: in-memory tree (testing), local ordered file (testing / debug), crowtree (production). The trait surface exposes:

- `apply(slot, batch)` — atomic apply of one batch with its slot.
- `get(key) -> (slot, value)?` — point read with per-key resolved-slot.
- `scan(range, limit) -> iter` — ordered iteration.
- `compare(other) -> diff` — used by `crowbench` to assert state equality across learners.
- `snapshot_export()` / `snapshot_import()` — for snapshot install.
- Per-key MVCC of one version: tombstones with slots; compacted only after both the snapshot watermark and the safe-slot watermark have passed.

→ Full design: [`design-storage-engine.md`](design/design-storage-engine.md).

---

## 9. Failure Mode Catalogue

Sketch of recovery flow for each failure scenario in [requirement.md §14.2](requirement.md#142-failure-scenarios-must-be-covered-in-test-designmd). Each is a paragraph-level outline; full state-machine details live in the relevant sub-topic.

**Network partition (majority / minority).** The minority side cannot maintain a quorum; its leader's lease expires; it cannot serve reads or writes. Clients on the minority side receive a retryable error and reconnect via their seed list to a majority-side node. On heal, the minority side rejoins as followers and catches up via WAL replay or snapshot install if its log has fallen too far behind.

**Message delay / slow node.** Slow acceptors do not block the quorum, but they do contribute to gap-rate growth if the slow node is also a learner needed for safe-slot. Mitigations: per-peer flow control, exclusion from the safe-slot computation if persistently lagging beyond a threshold (with admin alert).

**Message duplication.** Each `PxLogEntry` is keyed by `(slot, ballot)`; the acceptor recognizes a duplicate `Accept` for `(slot, ballot, v)` it has already accepted and replies idempotently without re-fsyncing.

**Message loss.** The replicator retransmits unacknowledged messages. Out-of-order delivery is tolerated by Paxos semantics. Permanent loss is detected by the gap repair task and resolved via classic Paxos.

**Clock skew between nodes.** Leader leases use the bounded-skew assumption ([requirement.md §3](requirement.md#3-dependencies-and-assumptions)). If observed skew exceeds the bound, the leader downgrades to ReadIndex automatically and emits an alert.

**Node crash (kill -9).** On restart, the node replays its WAL into the storage engine, then re-joins the group. If WAL replay reveals a CRC failure, replay truncates at that point and the node catches up from peers.

**Node restart / WAL replay.** Standard recovery flow: open WAL, validate CRCs, rebuild slot state, reconstruct dedup cache from latest `DedupCheckpoint` plus subsequent entries, register with the group leader.

**Disk full on WAL.** The WAL cannot fsync; the acceptor stops sending `Accepted`. The leader observes the timeout and does not count this acceptor in the quorum. If the cluster falls below quorum, writes pause until disk is freed (typically by snapshot-driven GC) or operator intervention.

**Corrupted WAL segment.** CRC fails on replay; the node truncates the WAL at the corruption point, snapshot-installs from a peer if necessary, and resumes.

**Leader injection failure (controlled step-down).** Admin RPC forces step-down; the current leader sends a final heartbeat with `step_down=true`; the group elects a new leader.

**Multiple leaders due to partition.** Term comparison fences out the stale leader: any RPC carrying a higher term causes the recipient to step down. Lease prevents the stale leader from serving reads; for writes, the stale leader cannot reach quorum.

**Leader failure mid-proposal.** The new leader's Phase-1 round adopts any value that any acceptor has already accepted at that slot. If no acceptor has, the new leader can choose freely (typically a `NoOp`).

**Acceptor failure mid-vote.** If the leader still has a quorum without this acceptor, the slot is chosen. The down acceptor catches up on restart.

**Lagging learner catch-up.** The learner falls behind, requests missing slots from the leader (or any peer with them), and catches up via WAL streaming. If it is too far behind, it falls back to snapshot install.

**Missing slot repair via classic Paxos.** Repair task runs Phase 1 with a higher ballot at the gap slot, learns whatever value (if any) is the highest-accepted, and re-Accepts it; if none, fills `NoOp`.

**WAL truncation and snapshot recovery.** After a snapshot at slot S is durable on the leader and at least one other learner, the leader broadcasts the snapshot watermark; each acceptor may GC WAL records below S. New learners bootstrap from snapshot + WAL tail.

---

## 10. Observability Hooks

The metric and log signals required by [requirement.md §13.2](requirement.md#132-mandatory-observability-signals) are produced at well-defined points in the module map:

- **Per group (current leader, term, max-chosen, max-applied, safe-slot, in-flight count, gap count).** Emitted by the Group Manager every metric tick. Internally sourced from Proposer (in-flight, max-chosen), Learner (max-applied), and the safe-slot tracker.
- **WAL fsync latency, WAL bytes/sec, snapshot age, disk usage per WAL disk.** Emitted by the WAL module. Snapshot age comes from the Snapshot module.
- **RPC request rate, latency histogram, error rate by code.** Emitted by the RPC module via interceptors; `NotLeader` and `Busy` are first-class labels.
- **Structured logs with `node_id`, `group_id`, `slot`, `term`** — every consensus-relevant event (Accept sent/received, Chosen learned, leader change, gap repair start/finish, snapshot start/finish) emits a log event with these fields baked in via a per-group logging context.
- **OpenTelemetry hooks** are exposed by the RPC and Group Manager modules, but no spans are required; instrumentation can be enabled later without protocol changes.

---

## 11. Open Design Questions

These are intentional gaps left for sub-topic docs or for a future iteration. They are not requirement gaps.

- **Exact lease duration formula.** Should be a function of heartbeat interval, observed skew, and a safety margin. To be specified in [`design-leader-election.md`](design/design-leader-election.md).
- **Repair-task cadence and trigger heuristics.** Default scan period, gap-age threshold, batch size. To be specified in [`design-parallel-slots.md`](design/design-parallel-slots.md).
- **WAL segment rotation policy.** Size threshold, retention, multi-disk allocation algorithm (round-robin vs load-aware). To be specified in [`design-wal.md`](design/design-wal.md).
- **Joint-consensus quorum overlap proof for asymmetric transitions.** E.g. when going 3 → 5 with the new two members not yet caught up, what is the safe ordering of catch-up vs vote-eligibility? To be specified in [`design-reconfiguration.md`](design/design-reconfiguration.md).
- **Compaction policy for the storage engine.** When are tombstones safe to drop? Two watermarks (snapshot-slot and safe-slot) must both pass. Exact policy to be specified in [`design-storage-engine.md`](design/design-storage-engine.md).
- **Snapshot transfer chunk size and throttling defaults.** Network-friendly defaults; pluggable.
- **Group-0 special handling during simultaneous topology + Group-0 membership change.** Likely serialized by holding a Group-0 leader lease, but the sequencing must be made explicit.

None of these block design completeness; they refine numbers and corner-case ordering.

---

## 12. References

- Lamport, *The Part-Time Parliament* (1998); *Paxos Made Simple* (2001); *Paxos Made Live* with Chandra & Griesemer (2007). The classical and practical Paxos foundations.
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Raft)* (2014). Source of the leader election, lease, and joint-consensus reconfiguration patterns CrowKV reuses.
- Mao, Junqueira & Marzullo, *Mencius* (2008); Moraru, Andersen & Kaminsky, *EPaxos* (2013). Lessons on slot pipelining and the cost of cross-key dependencies.
- Lampson, *How to Build a Highly Available System Using Consensus* (1996). The decoupling of "leader election" from "consensus per slot" used here.
- TiKV closed-timestamp design notes; CockroachDB closed-timestamp design notes. Source of the safe-slot pattern.
- Diego Ongaro's PhD thesis, *Consensus: Bridging Theory and Practice* (2014). Practical guidance on log compaction and snapshot install.
