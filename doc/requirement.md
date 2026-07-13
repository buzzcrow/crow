<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Requirements

This is the authoritative requirements document. All other documents (`design.md`, sub-design docs) must follow definitions and conclusions from this document.

Conventions:
- **Decision record** callouts (`> **Decision record:** ...`) capture the `Suggest → Confirm` resolution for traceability. They are informational; the surrounding prose is normative.
- Cross-references use markdown anchor links to section numbers, e.g. [§7.3](#73-parallel-slot-processing).

## Table of Contents

- [1. Overview](#1-overview)
- [2. Non-Goals (Out of Scope)](#2-non-goals-out-of-scope)
- [3. Dependencies and Assumptions](#3-dependencies-and-assumptions)
- [4. Concepts and Terminology](#4-concepts-and-terminology)
  - [4.1 Architecture](#41-architecture)
  - [4.2 Paxos Core](#42-paxos-core)
  - [4.3 Roles](#43-roles)
  - [4.4 Messages and States](#44-messages-and-states)
  - [4.5 Storage and Recovery](#45-storage-and-recovery)
- [5. Data Model and Client API](#5-data-model-and-client-api)
  - [5.1 Key and Value](#51-key-and-value)
  - [5.2 Operations](#52-operations)
  - [5.3 Resource Limits](#53-resource-limits)
- [6. Consistency and Read Model](#6-consistency-and-read-model)
  - [6.1 Write Guarantee](#61-write-guarantee)
  - [6.2 Leader-Read Fencing](#62-leader-read-fencing)
  - [6.3 Safe-Slot Design](#63-safe-slot-design)
  - [6.4 Client Read Modes](#64-client-read-modes)
  - [6.5 Parallel-Slot Linearizability Analysis](#65-parallel-slot-linearizability-analysis)
- [7. Consensus Architecture](#7-consensus-architecture)
  - [7.1 Groups and Cluster Topology](#71-groups-and-cluster-topology)
  - [7.2 Leader Election and Terms](#72-leader-election-and-terms)
  - [7.3 Parallel Slot Processing](#73-parallel-slot-processing)
  - [7.4 Group Routing](#74-group-routing)
  - [7.5 Partition and Availability Behavior](#75-partition-and-availability-behavior)
- [8. Storage and Durability](#8-storage-and-durability)
  - [8.1 WAL (Write-Ahead Log)](#81-wal-write-ahead-log)
  - [8.2 Acceptor](#82-acceptor)
  - [8.3 Learner Storage](#83-learner-storage)
  - [8.4 Snapshot and Install](#84-snapshot-and-install)
- [9. Cluster Lifecycle](#9-cluster-lifecycle)
  - [9.1 Reconfiguration](#91-reconfiguration)
  - [9.2 Rolling Upgrade](#92-rolling-upgrade)
  - [9.3 Backup and Disaster Recovery](#93-backup-and-disaster-recovery)
- [10. Client Interaction](#10-client-interaction)
  - [10.1 Client Discovery](#101-client-discovery)
  - [10.2 Retry and Idempotency](#102-retry-and-idempotency)
  - [10.3 Cross-Group Operations](#103-cross-group-operations)
- [11. Security](#11-security)
- [12. Performance and Batching](#12-performance-and-batching)
  - [12.1 Performance Targets](#121-performance-targets)
  - [12.2 Batch Operations](#122-batch-operations)
- [13. Operational Tooling and Observability](#13-operational-tooling-and-observability)
  - [13.1 Admin and Monitoring RPCs](#131-admin-and-monitoring-rpcs)
  - [13.2 Mandatory Observability Signals](#132-mandatory-observability-signals)
- [14. Testing Requirements](#14-testing-requirements)
  - [14.1 Correctness Criteria for crowbench](#141-correctness-criteria-for-crowbench)
  - [14.2 Failure Scenarios](#142-failure-scenarios-must-be-covered-in-test-designmd)
- [15. Components](#15-components)
  - [15.1 `crowkv` library (core)](#151-crowkv-library-core)
  - [15.2 `crowkv-server`](#152-crowkv-server)
  - [15.3 `crowbench`](#153-crowbench)
  - [15.4 `crowkv-console`](#154-crowkv-console)
  - [15.5 RPC and Communication](#155-rpc-and-communication)

---

## 1. Overview

CrowKV is a high-performance distributed key-value cluster based on Multi-Paxos with multiple groups for sharding. It has performance advantages over multi-group Raft clusters due to parallel slot processing.

**Programming language:** Rust.

**Primary goals:**
- **Linearizability** for all acknowledged operations served by the leader (writes and reads). Follower reads offer weaker modes by explicit client choice. See [§6](#6-consistency-and-read-model) for the full consistency model and [§6.5](#65-parallel-slot-linearizability-analysis) for the parallel-slot linearizability analysis.
- Bounded idempotency for retried requests via `(client_id, sequence_number)` dedup ([§10.2](#102-retry-and-idempotency)).
- High throughput via parallel per-slot Paxos within a group, and multiple independent groups across the cluster.
- Pluggable storage engine so the same core library works for in-memory tests, local file tests, and crowtree in production.

## 2. Non-Goals (Out of Scope)

These are intentionally excluded from the initial design:

- **Multi-group transactions / 2PC** — Each operation targets a single PxGroup only. Cross-group atomic operations are not supported.
- **Dynamic group split/merge** — Group configuration is static at startup. Membership changes require planned reconfiguration.
- **Read-modify-write operations (`CAS`, `Increment`)** — These require leader state reads and are not supported in the initial design. Future extension: key-level dependency tracking or leader-read-after-catch-up.
- **Full Jepsen-style linearizability checking** — Testing verifies same-state comparison and controlled-order verification. Full formal linearizability checking is a future enhancement.
- **Client-managed transaction boundaries** — No multi-operation transactions from the client. Each request is independent and idempotent.
- **Dynamic workload-based rebalancing** — Group membership and count are operator-managed (see [§7.1](#71-groups-and-cluster-topology)); no automatic key migration between groups based on load.
- **Client-side hash partitioning as a core-library feature** — `crowkv` itself does not compute `group_id` from a key; every KV RPC takes an explicit `group_id` ([§7.4](#74-group-routing)). An application layer built on top of `crowkv` is free to implement its own `hash(key) -> group_id` convention, but that convention is outside this document's scope.

## 3. Dependencies and Assumptions

External factors the design must accommodate:

- **RPC framework:** gRPC with protobuf serialization (mature Rust library). Raw TCP may be considered later if overhead becomes a bottleneck.
- **Storage engine interface:** Pluggable design supporting in-memory btree, ordered local file, and crowtree btree library. All engines must implement the same interface with a `compare` method for testing.
- **TLS:** Not required for the initial implementation, but the design must consider TLS for future node-to-node and client-to-node encryption.
- **Deployment model:** Static configuration file defines groups, nodes, and partitioning. No runtime config changes without restart in the initial design.
- **Hardware assumptions:** Target deployment is LAN with SSD storage. Multiple disks may be used for parallel WAL segments.
- **Clock assumption:** Leader leases require *bounded clock skew* between nodes within one PxGroup. Architectural bound: **≤ 100ms skew per heartbeat interval**, monotonic clock used for lease math (not wall clock). Lease duration must be ≫ max-skew + max-message-delay.
- **Wire-protocol compatibility:** All RPC messages (protobuf) must be designed for forward/backward compatibility (no field renumbering, no required fields). This is a precondition for rolling upgrades — see [§9.2 Rolling Upgrade](#92-rolling-upgrade).

## 4. Concepts and Terminology

### 4.1 Architecture

- **PxNode** — A node in the CrowKV cluster that participates in Paxos consensus. A PxNode can participate in multiple PxGroups.
- **PxGroup** — An independent Paxos ensemble. CrowKV supports multiple static groups with different membership sizes. A PxGroup contains multiple PxGroupMembers (3, 5, 7...) distributed across PxNodes.
- **PxGroupMember** — A member of one PxGroup. Combines Proposer, Acceptor, and Learner roles. The member can be one of: PxLeader, PxFollower, or PxCandidate (following Raft terminology).

### 4.2 Paxos Core

- **PxSlot** — A log position / sequence number where a single PxLogEntry is decided.
- **PxLogEntry** — The durable result of a single Paxos execution: the chosen value at a specific PxSlot, persisted to the Acceptor WAL.
- **PxBallot** — A monotonically increasing proposal number scoped to the group (not per-slot), often written as `(round, leader_id)`. Higher ballots override lower ones. In Multi-Paxos, a leader holds one ballot across a consecutive range of slots.
- **PxTerm** — A separate monotonic epoch for Raft-style leader election (distinct from PxBallot).
- **PxInstance** — One complete in-flight execution of the Paxos algorithm for a single PxSlot. This is transient runtime state (ballot, promises received, accept count). Multi-Paxos runs many PxInstances in sequence.

### 4.3 Roles

- **PxLeader** — The distinguished proposer that drives consensus for a consecutive range of PxSlots. In steady state, the leader skips Phase 1 and directly issues Accepts.
- **PxProposer** — Any node that can initiate a Paxos round (Prepare/Accept). In CrowKV, only the leader acts as proposer in normal operation; other nodes may propose only to fill gaps during recovery.
- **PxAcceptor** — Nodes that vote on values. They persist promises and accepts to WAL, forming the durable source of truth.
- **PxLearner** — Nodes that learn chosen values and apply them to the state machine. In CrowKV, the leader's learner replicates chosen values to other learners, each maintaining its own btree.

### 4.4 Messages and States

- **PxPromise** — Phase 1b response: an acceptor promises not to accept lower ballots and may return the highest previously accepted value.
- **PxAccepted** — Phase 2b response: an acceptor votes for a value at a given PxBallot.
- **PxChosen** — A value is chosen (decided) when a majority of acceptors have sent Accepted for it.

### 4.5 Storage and Recovery

- **PxWAL** — Write-Ahead Log used by acceptors for durability. The only persistent log in CrowKV.
- **PxCatch-up** — When a lagging learner replays the WAL or requests missing PxLogEntries from the leader to reach the latest state.
- **PxLease / Leader Lease** — A time-bound guarantee that no other node will successfully propose, reducing Phase 1 overhead.

## 5. Data Model and Client API

### 5.1 Key and Value

Key = `Vec<u8>` (opaque bytes), Value = `Vec<u8>`. Keys are ordered (lexicographic) to support range scans. Max key size 1KB, max value size 1MB.

### 5.2 Operations

**Point operations:** `Get`, `Put`, `Delete`.

**Range / batch operations (included):**
- `Scan(start_key, end_key, limit, mode)` — single-group range scan. Natural fit since keys are already ordered and crowtree is a btree. Three modes (see also [§6.4](#64-client-read-modes) and [§6.5](#65-parallel-slot-linearizability-analysis)):
  - **`Linearizable` (default):** the leader records its current max-chosen slot `target` at request entry, waits for its own **contiguous applied frontier ≥ `target`**, then serves from its btree. Latency bounded by (parallel-slot window) × (slot-resolution time).
  - **`SafeSlot` (bounded stale, zero wait):** served from any replica whose local applied slot is ≥ the group `safe-slot` ([§6.3](#63-safe-slot-design)). May lag behind real-time but never blocks.
  - **`AtSlot(N)`:** served from any replica whose contiguous applied slot is ≥ `N`. Intended for consistent repeat reads of the same snapshot; `N` is typically a slot previously returned by the server.
- `BatchGet` / `BatchPut` / `BatchDelete` — multi-key, single-group (see also [§12.2 Batch Operations](#122-batch-operations)).

**Not supported:**
- Cross-group `Scan` or global ordering (consistent with the no-multi-group-tx non-goal).
- Read-modify-write: `CAS`, `Increment` — deferred. They require the leader to read current state, which needs all prior `PxSlot`s resolved (no gaps). The solution would be Option C (leader read after catching up), but latency cannot be guaranteed with parallel slots. Future extension: key-level dependency tracking or leader-read-after-catch-up.

**Out of scope (deferred to a future, separate design):**
- `Watch` / change feed, TTL / expiry.

### 5.3 Resource Limits

- Max key size: **1 KB**. Max value size: **1 MB**.
- Max batch size: **1024 ops or 4 MiB**, whichever is smaller (configurable).
- Max in-flight requests per client connection: **1024** (configurable). Clients aiming for lowest latency may set this to 1; benchmarks exploring max throughput should raise it.
- Max concurrent client connections per node: configurable; default **10 000**.

> **Decision record:** Confirmed — data model, operation set, and limits as above.

## 6. Consistency and Read Model

> **Decision record:**
> - *Suggest:* Strict linearizability for writes; reads can choose between linearizable (read from leader) or follower-read (possibly stale but bounded lag).
> - *Confirm:* Support the read modes listed below (leader linearizable, read-your-writes, bounded stale, best-effort).

### 6.1 Write Guarantee

**Linearizability** for all acknowledged writes. A write is acknowledged to the client only after a quorum of acceptors has durably flushed the corresponding `PxLogEntry` (see [§8.1 Ack contract](#81-wal-write-ahead-log)).

**Linearization point:** the leader's slot-assignment is **serialized** (single monotonic counter; no two threads stamp a slot independently). An op is ordered at its assigned slot and becomes visible to any observer after a quorum of acceptors has durably flushed it. Real-time ordering is preserved because the counter advances before the leader begins consensus on the next request. Consensus on different slots may then proceed in parallel without affecting this ordering — see [§6.5](#65-parallel-slot-linearizability-analysis).

**Failure semantics for un-acked writes:** a request that times out or returns `Busy` has an *unknown* outcome — it may have been applied or not. The client recovers via `request_id` retry, which is deduplicated at the leader within the bounded retention window ([§10.2](#102-retry-and-idempotency)). Outside that window, the outcome remains unknown and the client must treat the operation accordingly. (This is why we say "linearizability with bounded idempotency" rather than textbook *strict linearizability*.)

### 6.2 Leader-Read Fencing

A linearizable read served by the leader is only correct if the leader proves it is *still* the leader at read time. **Chosen: (a) lease-based**, with (b) available as a fallback configurable per-read.

- **(a) Lease-based (default):** leader serves linearizable reads locally while its lease is valid. Requires the bounded-clock-skew assumption (see [§3 Dependencies](#3-dependencies-and-assumptions)). Cheapest.
- **(b) ReadIndex / quorum check (fallback):** leader exchanges a heartbeat with a quorum on every linearizable-read batch before responding. No clock assumption. Higher latency.

### 6.3 Safe-Slot Design

- Each `PxLearner` periodically reports its **max contiguous applied slot** (also called **resolved-slot**) to the leader.
- Leader computes the **`safe-slot`** = `min(resolved_slot)` across all learners in the group. This is the equivalent of TiKV's `safe-ts` / CockroachDB's `closed timestamp`.
- The `safe-slot` represents: "all slots ≤ safe-slot are chosen and applied on every learner."
- Leader exposes the `safe-slot` to clients via write responses and a lightweight RPC.

**Write response:** Every `Put`/`Delete` response returns the **assigned `PxSlot`** to the client. The client can save this slot index and use it later to decide where to read.

**Key constraints with parallel slots:**
- The `safe-slot` must be **contiguous** (no gaps before it), not just the max slot. If slot 3 is missing and slot 5 is chosen, the `safe-slot` stays at 2 until 3 is resolved.
- With per-key slot tracking (see [§8.3](#83-learner-storage)), a follower might have slot 5 for key K applied before slot 3 is resolved. The global `safe-slot` is conservative, but the follower can still serve **read-your-writes** for key K using its per-key resolved slot.

### 6.4 Client Read Modes

**Point reads (`Get`, `BatchGet`):**

1. **Linearizable read** → always go to leader (fenced per [§6.2](#62-leader-read-fencing)). No slot check needed by the client.
2. **Read-your-writes** → client carries the specific `PxSlot` from its last write. It reads from any follower whose **per-key resolved-slot** ≥ that `PxSlot`. No need to wait for the global `safe-slot`.
3. **Bounded stale read** → client carries `last_known_safe_slot`. Reads from any follower whose **global resolved-slot** ≥ `last_known_safe_slot`. Slightly more stale but works across all keys without per-key tracking.
4. **Best-effort stale read** → read any follower, accept possible inconsistency (metrics, analytics only).

**Range reads (`Scan`):** three explicit modes (mirroring the API in [§5.2](#52-operations)), because cross-key consistency interacts with the parallel-slot gap frontier differently from point reads.

1. **`Linearizable` (default)** → served by the leader. At request entry the leader captures its current max-chosen slot `target`, waits until its own **contiguous applied frontier ≥ `target`**, then scans its btree. Note the leader's *own* frontier is used, not the cross-learner `safe-slot` — the leader's learner is typically ahead of `safe-slot`, so this gives the lowest-latency linearizable scan.
2. **`SafeSlot`** → served from any replica whose local applied slot is ≥ the group `safe-slot`. Zero wait; bounded staleness.
3. **`AtSlot(N)`** → served from any replica whose contiguous applied slot is ≥ `N`. Typically `N` is a slot the client previously obtained from a server response, used to repeat a read at the same snapshot.

### 6.5 Parallel-Slot Linearizability Analysis

Parallel-slot Paxos is the reason CrowKV chooses Multi-Paxos over Raft. The full linearizability proof — premises, claim, sketch, the single remaining cost (linearizable `Scan`), and why `CAS`/`Increment` would break it — lives in [`design/design-slot.md`](design/design-slot.md) §14.

## 7. Consensus Architecture

Raft is a mature design. We reuse most proven Raft patterns except for **parallel slot writes**, which is the key performance advantage of Multi-Paxos over Raft. This creates some conflicts with Raft design that must be raised and resolved.

### 7.1 Groups and Cluster Topology

Each `PxGroup` is an independent Paxos ensemble and can define a different member count (e.g. 3, 5, 7). There is no special system group — topology (which groups exist, their membership, and where they run) is **operator-managed via an HTTP management API**, not self-hosted inside a Paxos-replicated "Group-0".

> **Decision record (supersedes the original Group-0 design in `design.md` §7 for history):**
> - *Original suggestion:* a self-hosted system group (`Group-0`) whose Paxos log is the source of truth for the node registry, per-group membership, a fixed `num_groups`, and the `hash(key) -> group_id` partitioning rule, bootstrapped from a static seed file.
> - *Confirmed (2026-07):* **not built** — operator-managed topology (below) is the accepted, implemented, and tested model instead. `Group-0` never had any implementation; this decision retires the design rather than deviating from a shipped one.

**Topology model actually built and in use:**
- Each physical node runs one `crowkv-server` process, exposing an **HTTP management API** (`crowkv-server/src/mgmt_api.rs`) for creating/removing stores and groups (`POST/DELETE /stores`, `POST/DELETE /stores/:sid/groups`, `POST/DELETE /stores/:sid/groups/:gid/remotes/...`).
- Every group's membership is **persisted to a config file** on that node — one file per `(store_id, group_id)` under `--config-root` (`GroupConfigStore`, see [`design/design-kv-server.md`](design/design-kv-server.md)) — so a restarted node recovers its own groups' membership without re-issuing the HTTP calls.
- There is no cluster-wide `num_groups` or `hash(key) -> group_id` rule owned by `crowkv` itself. Every KV RPC takes an explicit `group_id` ([§7.4](#74-group-routing)); routing/sharding policy, if any, lives in the calling application, not in the core library.
- An operator, or a higher-level orchestrator (`crowkv-console` today) is responsible for creating groups consistently across the nodes that host their members, and for tracking the resulting cluster-wide store/group inventory (`crowkv-console`'s own declarative config file is one such orchestrator-level implementation; it is not part of `crowkv`/`crowkv-server`).
- **Nothing prevents another system built on top of `crowkv`'s group/replica primitives from implementing its own self-hosted "Group-0"-style metadata group** if it needs one (e.g. for fully decentralized topology management without an external orchestrator) — that would be a feature of the embedding system's design, not a requirement `crowkv` itself takes on.

**Failure mode:** each group fails independently — if a group loses quorum, only that group stops accepting writes; other groups on the same or different nodes are unaffected. There is no cluster-wide single point of failure analogous to a lost Group-0 quorum, since no such group exists.

### 7.2 Leader Election and Terms

CrowKV uses Raft-style leader election with a separate `PxTerm`. The `PxBallot` (for Paxos proposal) and `PxTerm` (for leader election) are kept separate to cleanly decouple concerns:

- `PxBallot = (round, leader_id)` — Paxos proposal number; higher ballots override lower ones within a slot.
- `PxTerm` — monotonic epoch for Raft-style leader election; persists across leader changes.

Leader election uses heartbeat + timeout. When a follower times out, it increments its term and initiates a new election.

**Leader change requirement:** When leader changes for any reason, the new leader must be able to catch up the old leader's state and continue Paxos consensus.

**Injected failure mode:** Leader can be forced to step down via an admin command for testing. This allows controlled simulation of leader changes with accurate recording for measuring test results.

> **Decision record:** Confirmed — separate `PxTerm` from `PxBallot`; Raft-style election with heartbeat + timeout.

### 7.3 Parallel Slot Processing

Multi-Paxos supports parallel writes on different PxSlots within the same group, achieving maximum performance — this is the key advantage over Raft's strictly sequential log.

**Sliding window control:** A configurable window limits the maximum number of parallel in-flight slots (e.g., 8, 16, 32, 64). This prevents unbounded gaps and ensures the background repair thread can keep up.

**Backpressure (when window is full):** New client requests are queued up to a bounded admission queue; beyond that, the leader rejects with a retryable `Busy` error. The leader must never block indefinitely on a full window. Queue depth is configurable. Clients treat `Busy` as retryable (back off and retry).

**Gap repair:** Parallelism increases complexity for fixing missing slots. We use classic Paxos for gap resolution, driven by a background repair thread that periodically identifies and resolves undecided slots. 

**Default window size:** 16 (see [§12.1 Performance Targets](#121-performance-targets)).

#### 7.3.1 Correctness Analysis for Parallel Slot Writes

Parallel slot writes are safe because only blind operations (`Put`, `Delete`) are supported — the final value of each key is determined solely by the highest slot that touched it. The full correctness analysis, including per-key slot tracking, gap handling, and resolved edge cases, lives in [`design/design-slot.md`](design/design-slot.md) §13.

### 7.4 Group Routing

Every KV RPC (`Put`/`Get`/`Delete`/`BatchWrite`/`Scan`) carries an explicit `group_id` field ([`design/design-rpc.md`](design/design-rpc.md) §2, `kv.proto`). `crowkv` performs no key-to-group mapping itself; the caller supplies `group_id` directly.

An application built on `crowkv` (e.g. a client library) is free to implement its own `group_id` selection policy — static hash partitioning (`hash(key) % num_groups`), range partitioning, or a fixed single group — but that policy is layered on top of, not inside, the core library. Clients learn the current set of groups and their leaders via the HTTP management API's `/topology` endpoint (see [§10.1 Client Discovery](#101-client-discovery)), not a `DescribeCluster` gRPC call.

> **Decision record:** Confirmed (2026-07, supersedes the original hash-partitioning/static-`num_groups` design) — explicit `group_id` on every RPC; no core-library sharding rule.

### 7.5 Partition and Availability Behavior

**Minority partition rejects all requests (both reads and writes).** Majority partition continues serving. Clients on the minority side receive a retryable error and either wait for the partition to heal or reconnect via their seed list to a majority-side node.

This avoids split-brain at the cost of making the minority side fully unavailable.

> **Decision record:** Confirmed — minority-side reject-all policy.

## 8. Storage and Durability

### 8.1 WAL (Write-Ahead Log)

The Acceptor's WAL is the **only persistent log** in CrowKV. The learner's btree can replay the WAL on crash and find missing values. During replay, if some `PxSlot` is missing, we use classic Paxos to decide the missing value.

**Durability contract:**
- Batched durable flush (aggregation) with configurable batch size or time interval.
- Multiple WAL segments on multiple disks for parallelism, tagged with slot index.
- Since apply order is determined by slot index (not WAL order), we can write slots to any available WAL segment.
- Async WAL write with completion notification. A timer forces flush to prevent indefinite stalls in case of aggregation bugs.
- Target: lowest possible latency under normal conditions.
- **Integrity:** every WAL record carries a CRC32C (or equivalent) checksum. On replay, a record failing CRC truncates the WAL at that point and triggers catch-up via peers.
- **Ack contract:** an `Accepted` response is only sent **after** the WAL record's durable flush has completed. A client write is acknowledged only after a quorum of acceptors have durably flushed.

**Multi-disk WAL failure semantics:** if one disk fails on a node with multi-disk WAL, the node marks itself failed for that group and rebuilds from peers via snapshot install, rather than attempting to keep running on remaining disks. (Running degraded would require per-slot replication across local disks, which adds complexity.)

> **Decision record:** Confirmed — batched durable flush, multi-disk segments tagged by slot, CRC integrity, quorum durable-flush ack contract, fail-out on disk loss.

### 8.2 Acceptor

The Acceptor writes to WAL to persist the PxLogEntry, then updates PxSlot state in memory.

**WAL GC (garbage collection):** Old PxLogEntries must be cleared to GC the WAL and in-memory slots. GC is safe when:
- All learners have caught up to that PxSlot and persisted their state to disk, **and**
- A snapshot has been taken that covers slots ≤ snapshot slot.

Detailed GC mechanics are in `design.md`.

### 8.3 Learner Storage

The PxLogEntry payloads are key-value pairs.

**Debug property:** The PxSlot sequence number may be embedded in the key for easier debugging and ordering.

**Pluggable storage engines** — all must implement the same interface (including a `compare` method to verify identical KV state across learners for testing):
- In-memory btree/tree (for testing).
- Local ordered file (for testing).
- crowtree btree library (for production).

**Per-key slot tracking:** To support read-your-writes ([§6.4](#64-client-read-modes)), the engine stores `(slot, value)` per key. `Delete` writes a tombstone with its slot. Tombstones are GC'd only after the slot is below the snapshot slot **and** the per-key slot is no longer needed for read-your-writes (i.e. the global safe-slot has passed it). Compaction policy is detailed in `design.md`.

### 8.4 Snapshot and Install

**Scope:** Per PxGroup. Each Paxos group maintains its own snapshot independently. Global snapshots across groups are not required.

**Trigger:** Snapshot when WAL reaches a configured size or slot count threshold. Snapshot truncates the WAL and allows GC of old slots.

**Snapshot install (required for new-node bootstrap):** A node added via reconfiguration, or one whose WAL is older than the leader's earliest retained slot, must be able to receive a full snapshot from a peer (leader or any caught-up learner) before resuming WAL-based catch-up. The snapshot transfer protocol must be:
- Resumable (chunked, byte-offset based) so a partial transfer is not wasted.
- Verified end-to-end (checksum) before being installed.
- Throttleable so a recovery does not saturate the network.

## 9. Cluster Lifecycle

### 9.1 Reconfiguration

Support adding new nodes to a group and removing old nodes. Specifically:
- Extend: 3 → 5 → 7 nodes.
- Reduce: 7 → 5 → 3 nodes.

**Shipped mechanism:** per-node HTTP mutation of each replica's remote-replica list, persisted to the local `GroupConfigStore` config file, with a `membership_epoch` fence (exact-match on `Prepare`/`Accept`) and a non-voting-then-voting catch-up dance. This is intentionally not the original Raft-style joint-consensus design described in `design/design-reconfiguration.md` §7 (historical); see `design/design-reconfiguration.md` §11 for the design history and safety rationale. Requirements:

- `add_remote_replicas` and `remove_remote_replica` must be applied to every node in the group (console orchestrates the fan-out).
- A new member is added as `voting: false`, brought up to date via `SnapshotService` streaming, then re-added as `voting: true`.
- Removing the current leader triggers `StepDown` on the leader node first; if the leader is unreachable, survivors elect a new leader via the normal lease-expiry path.
- `membership_epoch` is bumped on every voting-set change and is required to match exactly between a leader's `Prepare`/`Accept` and an acceptor's local epoch; an `epoch_mismatch` reply carries the responder's epoch and self-heals to `max(own, peer)`.
- Writes may stall during the propagation window while the epoch fan-out is in progress; the stall is bounded and self-heals once the last node adopts the new epoch.

Reconfiguration design is detailed in `design/design-reconfiguration.md` (§§1-6, 8-11).

### 9.2 Rolling Upgrade

The cluster must support rolling upgrade: nodes are restarted one at a time, group quorum is preserved throughout, and mixed-version operation must be safe for at least one major version step.

- All wire messages are protobuf with append-only field numbers; no removal of fields in use.
- Persistent on-disk formats (WAL, snapshot) carry an explicit version header; older versions must be readable for at least one release.
- A cluster-level `config_version` in Group-0 prevents an older binary from joining a newer-format cluster.

**Compatibility window:** one major version step (version *N* must interoperate with version *N-1* during a rolling upgrade; no compatibility guarantee across two or more major versions).

### 9.3 Backup and Disaster Recovery

- **In-cluster recovery:** A node losing its disk can be rebuilt from a peer via snapshot install + WAL replay. This is the primary mechanism.
- **External backup:** a documented procedure to copy a learner's snapshot file + WAL tail offline. A first-class `crowkv-backup` tool is planned as a future extension.


## 10. Client Interaction

### 10.1 Client Discovery

Clients must be able to find the cluster without knowing per-group leaders.

- **Seed list:** clients are configured with a list of one or more `crowkv-server` **HTTP management API** endpoints. Mechanism: static config.
- **Topology discovery over HTTP:** the client library polls a seed's `/topology` endpoint (`crowkv-server/src/mgmt_api.rs::export_topology`) to build a `(store_id, group_id) -> leader_endpoint` cache. There is no gRPC `DescribeCluster` RPC (see [§7.1](#71-groups-and-cluster-topology), [§7.4](#74-group-routing)) — gRPC-only clients depend on this one HTTP call for discovery, the same way `crowkv-console` already does.
- **Routing:** the caller supplies an explicit `group_id` ([§7.4](#74-group-routing)); the client library sends the request to that group's cached leader endpoint, falling back to any known group member, which responds with `NotLeader { hint }` if needed.
- The client never re-polls `/topology` on the hot path — only on cache miss, `NotLeader`, or a scheduled refresh interval.

### 10.2 Retry and Idempotency

**Retry policy:** On timeout or `NotLeader` response, the client retries. On `NotLeader` with a hint, it follows the hint and retries immediately. If `NotLeader` and the new leader is unknown, it waits 1s then retries. For other errors, it retries 3 times then returns the error. Retry count and interval must be configurable.

**Idempotency / dedup cache scope:** `request_id` deduplication holds within a bounded window:
- Each `request_id` is `(client_id, sequence_number)`. `client_id` is opaque, assigned once per client session.
- The leader maintains a per-`client_id` dedup cache: last applied sequence + result, **persisted into the PxLogEntry stream** (so it survives leader change).
- Retention: at least the last *N* requests per active client (*N* = 64) **and** at least *T* seconds (*T* = 60s).
- After eviction, a retried request is no longer guaranteed idempotent; the client must treat the operation as having unknown outcome. This is documented client behavior.

> **Decision record:** Confirmed — per-`client_id` persisted dedup cache with N=64, T=60s retention.

### 10.3 Cross-Group Operations

Only single-group operations are supported. No multi-group transactions in the initial design. If ever needed, add a 2PC layer on top.

> **Decision record:** Confirmed — keep the KV simple and high-performance; no multi-group transactions.

## 11. Security

**TLS:** The architecture must accommodate TLS for both node-to-node and client-to-node channels. Implementation is deferred to a future extension, but the RPC layer and config schema must reserve hooks for it from day one.

**Authentication / authorization:** *trusted-network assumption, no authn/authz*. Cluster-internal RPCs are unauthenticated; client RPCs are unauthenticated. CrowKV is consumed as an internal library / embedded cluster; if a deployment needs authenticated access, it should wrap the `crowkv lib` in a KV server that adds auth.

> **Decision record:** Confirmed — TLS hooks reserved, implementation deferred; no authn/authz in core library.

## 12. Performance and Batching

### 12.1 Performance Targets

Initial targets (3-node group, LAN):

- 100K+ writes/sec per group.
- p99 write latency < 10ms under normal load.
- Parallel `PxSlot` window default = 16.

These are starting targets; optimize for higher throughput in later iterations.

### 12.2 Batch Operations

The interface supports batching multiple `Put` / `Delete` commands into a single PxLogEntry. This reduces per-operation overhead and improves throughput.

The library can aggregate batch operations in a background thread and send them as a single message to acceptors and learners. This is an important optimization for high throughput.

## 13. Operational Tooling and Observability

### 13.1 Admin and Monitoring RPCs

Admin and monitoring is exposed via gRPC:
- Query node status, current leader per group, learner lag per group.
- Force leader step-down (for testing).
- Push metrics to external monitoring services.

### 13.2 Mandatory Observability Signals

- **Per group:** current leader, current `PxTerm`, current max-chosen `PxSlot`, max-applied `PxSlot`, `safe-slot`, in-flight slot count, gap count.
- **Per node:** WAL durable-flush latency (p50/p99), WAL bytes/sec, snapshot age, disk usage per WAL disk.
- **Per RPC:** request rate, latency histogram, error rate by code (especially `NotLeader`, `Busy`).
- **Logs:** structured logs with `node_id`, `group_id`, `slot`, `term` on every consensus-relevant event.
- **Tracing:** out of scope for the initial design; add OpenTelemetry hooks so spans can be enabled later without wire changes, but no required spans are defined.

## 14. Testing Requirements

### 14.1 Correctness Criteria for crowbench

Two test modes depending on client behavior:

- **Controlled client (single thread, known order):** The storage engine records the operation order. Verify the exact sequence across learners.
- **Uncontrolled client (multi-thread, unknown order):** Verify identical KV state across all learners. Same-state comparison is sufficient.

Full Jepsen-style linearizability checking is deferred and will be specified in a separate test-design document when introduced.

### 14.2 Failure Scenarios (must be covered in tests)

**Network / RPC failures:**
- Network partition (majority/minority split).
- Message delay / slow node.
- Message duplication.
- Message loss / dropped packets.
- Clock skew between nodes.

**Node failures:**
- Node crash (kill -9, sudden termination).
- Node restart and WAL replay.
- Slow / overloaded node (delayed responses).
- Disk full on WAL.
- Corrupted WAL segment.

**Consensus failures:**
- Leader injection failure (controlled step-down).
- Multiple leaders due to partition (split-brain prevention).
- Leader failure mid-proposal.
- Acceptor failure mid-vote.

**Recovery scenarios:**
- Lagging learner catch-up.
- Missing slot repair via classic Paxos.
- WAL truncation and snapshot recovery.

## 15. Components

### 15.1 `crowkv` library (core)

Contains all core logic and functionality. Other top-level programs (server, benchmark) use this library to build their own applications.

### 15.2 `crowkv-server`

Reference server that wraps the `crowkv` library into a runnable daemon. It provides:
- CLI-driven startup of one or more `PxKvStore` instances, each hosting one or more `PxGroup`s.
- An HTTP management API for runtime inspection and topology wiring.
- A complete deployment unit that can form a CrowKV cluster with other `crowkv-server` instances.

CLI arguments, HTTP management API endpoints, topology wiring workflow, error handling, and logging details are specified in [`design/design-kv-server.md`](design/design-kv-server.md).

### 15.3 `crowbench`

A KV client that is also a benchmark tool. In the current implementation
this role is fulfilled by the `shared` library and the `crowkv`
CLI (`bench` subcommand) under `crowkv-console`; see [§15.4](#154-crowkv-console).

- Supports multiple test modes with configurable behavior.
- Records expected KV operation order for later verification against learner storage.
- Each client request carries a unique `request_id`. The same `request_id` always produces the same result, regardless of retries (idempotent) — see [§10.2](#102-retry-and-idempotency).

### 15.4 `crowkv-console`

`crowkv-console` is the unified management project for observing and
operating CrowKV clusters. It ships two frontends sharing the same
operation core:

- **Web UI** — a single-page, embeddable cluster console. Requirements
  and design live in [`design/design-ui.md`](design/design-ui.md) §13.
- **CLI** — scripting, automation, CI/CD, and load testing.

All cluster access goes through `crowkv-server` public endpoints (HTTP
management API + gRPC KV / health). The console must not bypass the
server. The internal architecture (shared core lib, Axum web backend,
SSH lifecycle, Swagger UI hosting) is described in
`design/design-console.md`.

#### 15.4.1 Architecture

- A shared Rust core (`crowkv-console-shared`) encapsulates business
  logic, HTTP/gRPC clients, data models, error types, and config
  parsing.
- The CLI (`crowkv`) and the Web backend (`crowkv-web`) are thin
  command/argument layers on top of the shared core.
- The Web UI is a static SPA (React + Vite + Tailwind) served by
  `crowkv-web`.

#### 15.4.2 Core Capabilities

1. **Cluster observation** — inventory of registered server instances,
   `Rack → Node → Server Instance → Store → Group → Replica` hierarchy,
   live status / metrics, aggregated topology.
2. **Simulated hardware cluster** — `Rack` and `Node` abstractions that
   model a realistic topology while running on a single host (one
   server instance per simulated node, ports differentiate hosts).
   Every node is treated as remote even on `127.0.0.1`. Nodes carry
   SSH credentials (key or password); SSH is used for lifecycle
   (deploy / start / stop / log tail) while runtime control uses
   HTTP/gRPC.
3. **Dynamic management** — add/remove stores, groups, replicas, remote
   endpoints; modify group configuration. All via the server's
   management API.
4. **KV operations** — browse keys per `(store, group)`, put/get/delete/
   list/scan, display full value content (internal demo data, not
   protected). Prefix scan is supported by `crowkv-server`'s scan RPC.
5. **Load testing (CLI only)** — workload runs, percentile latency
   reports, predesigned stress scenarios.
6. **API integration** — bundled offline Swagger UI served by the
   console; users select a registered `crowkv-server` to view its
   OpenAPI document. The Web UI embeds Swagger inside the SPA (no new
   browser page); the CLI is unaffected.

#### 15.4.3 Swagger UI Hosting

Swagger UI is hosted by `crowkv-console`, not by `crowkv-server`. The
console bundles a pinned Swagger UI release under
`crowkv-console/web/swagger-ui/` and proxies the OpenAPI document from
the selected upstream server. `crowkv-server` keeps `ToSchema` derives
(OpenAPI JSON still generated) but does not depend on Swagger UI.

#### 15.4.4 Scope Boundaries

- **No persistent server-side state beyond local config.** Cluster
  registry (racks, nodes, servers) is persisted in a TOML file
  (`~/.crowkv/console.toml`); runtime state (PID, snapshots) is always
  fetched fresh.
- **No authn/authz** — trusted-network assumption per [§11](#11-security).
- **No multi-tenant isolation.**

#### 15.4.5 CLI Command Hierarchy

The CLI uses a two-layer structure (`crowkv <group> <verb> [options]`) covering cluster observation, hardware lifecycle, store/group/replica management, KV data plane, and load testing. The full command tree and design rules live in [`design/design-console.md`](design/design-console.md) §12.

#### 15.4.6 Web UI Requirements

The Web UI is a single-page, embeddable cluster console with two hierarchy views (physical and logical), full operator surface (rack/node/server lifecycle, store/group/replica CRUD, KV data plane, embedded Swagger), and style/routing/asset isolation for embedding. The authoritative requirements spec lives in [`design/design-ui.md`](design/design-ui.md) §13.

### 15.5 RPC and Communication

All communication uses protobuf over gRPC (via a mature Rust gRPC library). While raw TCP would have lower overhead, we prioritize a proven RPC library.
