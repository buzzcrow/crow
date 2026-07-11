# CrowKV - Requirements

This is the authoritative requirements document. All other documents (`design.md`, `plan.md`, `test.md`) must follow definitions and conclusions from this document.

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
  - [7.1 Groups and System Group (Group-0)](#71-groups-and-system-group-group-0)
  - [7.2 Leader Election and Terms](#72-leader-election-and-terms)
  - [7.3 Parallel Slot Processing](#73-parallel-slot-processing)
    - [7.3.1 Correctness Analysis for Parallel Slot Writes](#731-correctness-analysis-for-parallel-slot-writes)
  - [7.4 Sharding](#74-sharding)
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
    - [15.2.1 CLI Interface](#1521-cli-interface)
    - [15.2.2 HTTP Management API](#1522-http-management-api)
    - [15.2.3 Topology Wiring Workflow](#1523-topology-wiring-workflow)
    - [15.2.4 Non-Goals](#1524-non-goals)
    - [15.2.5 Error Handling](#1525-error-handling)
    - [15.2.6 Logging](#1526-logging)
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
- **Dynamic workload-based rebalancing** — Hash partitioning is static; no automatic key migration between groups based on load.
- **Changing `num_groups` after cluster creation** — Because sharding is `hash(key) % num_groups`, the group count is **fixed at cluster creation forever**. Operators can only change *membership inside* a group (see [§9.1 Reconfiguration](#91-reconfiguration)), not the number of groups. Sizing must be done up front.

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

**Linearizability** for all acknowledged writes. A write is acknowledged to the client only after a quorum of acceptors has fsynced the corresponding `PxLogEntry` (see [§8.1 Ack contract](#81-wal-write-ahead-log)).

**Linearization point:** the leader's slot-assignment is **serialized** (single monotonic counter; no two threads stamp a slot independently). An op is ordered at its assigned slot and becomes visible to any observer after a quorum of acceptors has fsynced. Real-time ordering is preserved because the counter advances before the leader begins consensus on the next request. Consensus on different slots may then proceed in parallel without affecting this ordering — see [§6.5](#65-parallel-slot-linearizability-analysis).

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

Parallel-slot Paxos is the reason CrowKV chooses Multi-Paxos over Raft. This section shows that linearizability is preserved under parallel consensus, given the rest of the design constraints. It also records the one latency cost (Scan) that follows from the parallelism.

**Premises (all stated elsewhere, restated here for the proof):**
1. Only **blind ops** (`Put`, `Delete`, and their batch forms) are supported ([§5.2](#52-operations)). No `CAS` / `Increment`.
2. The leader **serializes slot assignment** via a single monotonic counter ([§6.1](#61-write-guarantee)).
3. A client write is acknowledged only after a **quorum of acceptors has fsynced** the chosen value at its slot ([§8.1](#81-wal-write-ahead-log)).
4. Learners track `(slot, value)` **per key** and only accept writes where `slot > current_slot` for that key ([§7.3.1](#731-correctness-analysis-for-parallel-slot-writes), [§8.3](#83-learner-storage)).
5. Leader reads are fenced by lease or ReadIndex ([§6.2](#62-leader-read-fencing)).

**Claim:** The assigned slot number is a valid linearization point for every `Put`, `Delete`, `Get`, and batch op.

**Sketch of why it holds:**

- **Real-time order → slot order.** If `ack(A)` completes before `invoke(B)` in real time, then by the time the leader sees B, the counter has already advanced past `slot(A)`, so `slot(A) < slot(B)`. Concurrent operations may appear in either order, which linearizability allows.
- **Blind apply-order independence.** For any key *k*, the final value is determined solely by `max{ slot | slot writes k }`. An undecided earlier slot that also writes *k* will, when eventually chosen, be ordered earlier in the linearization and immediately overwritten by the later slot's value — so observers never see an inconsistent final state for *k*.
- **Durability before visibility.** The ack contract (quorum-fsync before ack) ensures that a client-observed write cannot be lost by a leader change. Classic-Paxos gap repair is guaranteed to re-choose the same value for any slot where at least one acceptor persisted it.
- **Leader read correctness.** The leader's learner has applied slot N to its per-key state before acking slot N. For any subsequent `Get(k)` on the leader, the returned `(slot, value)` reflects the highest slot that has written k *and that has been chosen*. Any earlier in-flight slot writing k, when resolved, will be ordered before the returned value in the linearization and immediately overwritten — so the returned value is the correct one in the total order.
- **Follower read correctness.** Read-your-writes uses the per-key resolved-slot to wait for exactly the client's slot; bounded-stale uses the global `safe-slot` which is by construction gap-free.

**Single remaining cost — linearizable `Scan`.**

A cross-key read at "now" must reflect a consistent prefix of the total order. Because the leader's max-chosen slot may have gaps (e.g. slot 5 chosen while slot 3 is still in flight), linearizable `Scan` must wait for the leader's **contiguous applied frontier** to cover the target. Under heavy parallel-write load this adds latency bounded by (parallel-slot window size) × (slot-resolution time). In Raft this cost is hidden because its log is contiguous by construction; in CrowKV it is exposed and bounded by the window size.

API mitigation ([§5.2](#52-operations), [§6.4](#64-client-read-modes)): clients that cannot tolerate this wait use `Scan(mode = SafeSlot)` (bounded stale, zero wait) or `Scan(mode = AtSlot(N))` for repeat reads of a known snapshot. Point `Get`s are **not** affected — they bypass the gap entirely (see leader-read correctness above).

Note that the linearizable `Scan` uses the *leader's own contiguous applied frontier*, not the cross-learner `safe-slot`. The leader's learner always leads or matches the global `safe-slot`, so this choice strictly dominates in latency while preserving linearizability.

**Why `CAS` / `Increment` would break this.** Read-modify-write ops must read the *current* value of a key before proposing the new value. With gaps, the current value is unknown until all earlier slots for that key are resolved. Supporting them would require either (a) forcing slot consensus to be sequential (erasing the parallelism) or (b) per-key dependency tracking at the leader. Neither is in scope for the initial design. This is why giving them up is the enabling trade.

**Implementation invariants this analysis depends on (must be enforced by the code):**
- Slot assignment is a single point (no parallel counter increments).
- `Accepted` responses are not sent before fsync of the WAL record.
- Classic-Paxos recovery never discards an already-accepted value at a slot.
- Leader lease / ReadIndex fencing is applied on every linearizable read before returning.
- Per-key slot comparison on apply (`slot > current_slot`) is atomic with the write.

A violation of any of these invariants breaks linearizability independently of the parallel-slot design; they are standard Paxos/Raft correctness invariants.

## 7. Consensus Architecture

Raft is a mature design. We reuse most proven Raft patterns except for **parallel slot writes**, which is the key performance advantage of Multi-Paxos over Raft. This creates some conflicts with Raft design that must be raised and resolved.

### 7.1 Groups and System Group (Group-0)

Each group can define different member counts. Example:
- **Group-0 (system group):** 5 members, used for cluster metadata.
- **Group-1 to Group-N (data groups):** 3 members each, used for the partitioned KV cluster.

**Group-0 contents** — the cluster's source of truth for topology:
- The full `PxNode` registry (`node_id`, addresses, status).
- The `PxGroup` table: for each group, its `group_id`, member list, and current leader hint.
- The fixed `num_groups` value and the `hash(key) -> group_id` partitioning rule.
- Cluster-level `config_version` (for rolling-upgrade compatibility checks).

**Bootstrap procedure** (architectural; details in `design.md`):
1. Operator provides a static seed file listing the initial Group-0 members on every node at first start.
2. Group-0 elects a leader via the normal Raft-style election; once elected, it writes the initial topology (registered nodes, data-group memberships) into its own log.
3. Data groups (Group-1..N) start only after Group-0 has chosen and broadcast their membership.

**Failure mode:** if Group-0 loses quorum, the whole cluster stops accepting writes. Data groups can keep serving reads from learners until their leader leases expire, then they also stop. Default: **fail-stop for writes; bounded-stale reads continue until leases expire**.

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

**Key insight:** Parallel slot writes are safe because we only support **blind operations** (`Put`, `Delete`). These operations do NOT depend on the current value of the key.

- **Consensus phase (parallel):** The leader can propose `PxSlot` 3, 5, 7 simultaneously. Each `PxSlot` is an independent Paxos instance. They can be decided (chosen) in any order.
- **Apply phase (per-key slot tracking):** `PxLearner`s store `(slot, value)` per key and only accept writes where `slot > current_slot` for that key.
  - `Put(k, v)` at slot 3 and `Put(k, v2)` at slot 5 — regardless of apply order, the highest slot wins. Final state is deterministic.
  - `Delete(k)` at slot 3 and `Put(k, v)` at slot 5 — same logic, highest slot determines the final value.
  - A batch operation (multiple `Put`/`Delete` at one slot) uses the batch's slot for each key.
- **Why gaps are not blocking for point reads:** An undecided slot 3 does not block `Get(k)` if slot 5 for key `k` is already applied. The gap only blocks:
  - Cross-key consistent snapshots / range scans at a specific log position.
  - WAL truncation (can't truncate before the minimum applied slot across all keys).
- **This is why blind ops are sufficient:** No operation reads current state before writing. The final value of each key is determined solely by the highest slot that touched it.

**Note:** This would NOT be safe for `CAS` or `Increment`, which read current state. Those require all prior slots resolved before the leader can reliably read. Not supported in the initial design (see [§5.2](#52-operations)).

**Resolved edge cases:**
- Batches that mix `Put` and `Delete` on the same key within one slot: intra-batch order is **as written by the client**.
- Per-key resolved-slot is required for read-your-writes; its memory cost (one slot per live key on every learner) is accepted (see [§6](#6-consistency-and-read-model) and [§8.3](#83-learner-storage)).

### 7.4 Sharding

Hash partitioning on key → `PxGroup` ID: `group_id = hash(key) % num_groups`. Static mapping loaded from Group-0 at startup. The single system group (Group-0) holds the cluster topology and the partitioning rule; clients learn both via the describe-cluster RPC (see [§10.1 Client Discovery](#101-client-discovery)).

> **Decision record:** Confirmed — hash partitioning, static `num_groups` (see [§2 Non-Goals](#2-non-goals-out-of-scope)).

### 7.5 Partition and Availability Behavior

**Minority partition rejects all requests (both reads and writes).** Majority partition continues serving. Clients on the minority side receive a retryable error and either wait for the partition to heal or reconnect via their seed list to a majority-side node.

This avoids split-brain at the cost of making the minority side fully unavailable.

> **Decision record:** Confirmed — minority-side reject-all policy.

## 8. Storage and Durability

### 8.1 WAL (Write-Ahead Log)

The Acceptor's WAL is the **only persistent log** in CrowKV. The learner's btree can replay the WAL on crash and find missing values. During replay, if some `PxSlot` is missing, we use classic Paxos to decide the missing value.

**Durability contract:**
- Batched fsync (aggregation) with configurable batch size or time interval.
- Multiple WAL segments on multiple disks for parallelism, tagged with slot index.
- Since apply order is determined by slot index (not WAL order), we can write slots to any available WAL segment.
- Async WAL write with completion notification. A timer forces flush to prevent indefinite stalls in case of aggregation bugs.
- Target: lowest possible latency under normal conditions.
- **Integrity:** every WAL record carries a CRC32C (or equivalent) checksum. On replay, a record failing CRC truncates the WAL at that point and triggers catch-up via peers.
- **Ack contract:** an `Accepted` response is only sent **after** the WAL record's fsync has completed. A client write is acknowledged only after a quorum of acceptors have fsynced.

**Multi-disk WAL failure semantics:** if one disk fails on a node with multi-disk WAL, the node marks itself failed for that group and rebuilds from peers via snapshot install, rather than attempting to keep running on remaining disks. (Running degraded would require per-slot replication across local disks, which adds complexity.)

> **Decision record:** Confirmed — batched fsync, multi-disk segments tagged by slot, CRC integrity, quorum-fsync ack contract, fail-out on disk loss.

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

Reconfiguration design (Raft-style joint consensus) will be detailed in `plan.md`. This doc states the requirement only.

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

- **Seed list:** clients are configured with a list of one or more `PxNode` endpoints (any node, any group). Mechanism: static config.
- **Describe-cluster RPC:** every `PxNode` exposes a read-only RPC that returns the current Group-0 leader and the cached group→leader map. Clients use it once at startup and then refresh on `NotLeader` or on a configurable interval.
- **Routing:** the client library hashes the key to pick the data group, then sends the request to that group's cached leader; falls back to any group member, which responds with `NotLeader { hint }` if needed.
- The client never queries Group-0 on the hot path — only on cache miss, `NotLeader`, or scheduled refresh.

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
- **Per node:** WAL fsync latency (p50/p99), WAL bytes/sec, snapshot age, disk usage per WAL disk.
- **Per RPC:** request rate, latency histogram, error rate by code (especially `NotLeader`, `Busy`).
- **Logs:** structured logs with `node_id`, `group_id`, `slot`, `term` on every consensus-relevant event.
- **Tracing:** out of scope for the initial design; add OpenTelemetry hooks so spans can be enabled later without wire changes, but no required spans are defined.

## 14. Testing Requirements

### 14.1 Correctness Criteria for crowbench

Two test modes depending on client behavior:

- **Controlled client (single thread, known order):** The storage engine records the operation order. Verify the exact sequence across learners.
- **Uncontrolled client (multi-thread, unknown order):** Verify identical KV state across all learners. Same-state comparison is sufficient.

Full Jepsen-style linearizability checking is deferred and will be specified in a separate test-design document when introduced.

### 14.2 Failure Scenarios (must be covered in `test.md`)

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

#### 15.2.1 CLI Interface

| Argument | Required | Default | Description |
|---|---|---|---|
| `--management-port` | No | `9910` | HTTP management API listen port. |
| `--management-addr` | No | `0.0.0.0` | HTTP management API bind address. |
| `--ports` | No | (OS-assigned) | Port pool for gRPC `PxKvStore` listeners. Comma/range format. |
| `--stores` | No | `0` | Store ID list (comma/range). |
| `--groups` | No | `1` | Group ID list (comma/range). Each store gets all listed groups. |
| `--replicas` | No | `0` | Local replica ID (single value). Used as the local replica for every group in every store. Max 128. |

When `--ports` is omitted, the server uses port `0` for each `PxKvStore` and lets the OS assign ephemeral ports.

#### 15.2.2 HTTP Management API

The management API is a lightweight HTTP/JSON service for operational control.

**Health:** `GET /health` → `{"status": "ok"}`.

**Store management:**
- `GET /stores` — list all stores with bound addresses and group counts.
- `GET /stores/:sid` — store detail (address, groups, replicas).
- `POST /stores` — add a new store (requires `store_id`, `group_id`, `replica_id`).
- `DELETE /stores/:sid` — remove a store and all its groups.

**Group management:**
- `GET /stores/:sid/groups` — list groups in a store.
- `POST /stores/:sid/groups` — add a group (requires `group_id`, `replica_id`).
- `DELETE /stores/:sid/groups/:gid` — remove a group.

**Remote replica management:**
- `GET /stores/:sid/groups/:gid/remotes` — list remote replicas.
- `POST /stores/:sid/groups/:gid/remotes` — add remote replicas.
- `DELETE /stores/:sid/groups/:gid/remotes/:rid` — remove a remote replica.
- `POST /stores/:sid/groups/:gid/remotes/batch` — batch-add from topology export.

Adding or deleting a **local** replica is not supported — local replicas are created/destroyed with the group.

**Topology:** `GET /topology` (alias `GET /top`) — export full server topology as JSON.

#### 15.2.3 Topology Wiring Workflow

To form a cluster from multiple `crowkv-server` instances:
1. Start each server (each creates its stores with groups).
2. Export topology from each server via `GET /topology`.
3. On each server, batch-add other servers' replicas via `POST .../remotes/batch`.
4. Assign leaders for each group.

#### 15.2.4 Non-Goals

- **Persistent configuration** — the server is stateless; topology is wired via the management API each time.
- **Automatic leader election** — leader assignment is explicit until leader election is integrated.
- **Authentication/TLS on management API** — trusted-network assumption per [§11](#11-security).

#### 15.2.5 Error Handling

- Invalid CLI arguments → exit with descriptive error message.
- Port already in use → log error, skip that store, continue.
- Management API errors → appropriate HTTP status codes (400, 404, 409, 500) with JSON error body.

#### 15.2.6 Logging

- Use `tracing` with file-based logging (via `crowkv::common::logging`).
- Log bound addresses for all stores and the management endpoint at startup.
- Log all management API mutations at INFO level.
- All log messages in the `crowkv` library include `store_id` and `group_id` where applicable.

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

- **Web UI** — a single-page, embeddable cluster console. Detailed UI
  requirements are normative in `requirement-ui.md`; the design lives
  in `design/design-ui.md`.
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

The CLI uses a two-layer command structure: `crowkv <group> <verb>
[options]`. Top-level groups separate concerns; verbs are consistent
within a group.

```
crowkv
├── cluster              # observation
│   ├── status           # high-level health summary
│   ├── topology         # print full hierarchy
│   └── inspect <id>     # detailed view of one entity
│
├── rack                 # simulated hardware: racks
│   ├── add <name>
│   ├── remove <name>
│   └── list
│
├── node                 # simulated hardware: nodes
│   ├── add --rack <r> --host <addr> --ssh-user <u> [--ssh-pass | --ssh-key]
│   ├── remove <node>
│   ├── list
│   └── ping <node>      # SSH + HTTP reachability
│
├── server               # crowkv-server lifecycle on a node
│   ├── deploy --node-id <n> [--mgmt-port <p> --grpc-port <p>]
│   ├── start --node-id <n>
│   ├── stop --node-id <n>
│   └── list
│
├── store                # store mgmt (logical, cluster-wide)
│   ├── add --store-id <id> --nodes <n1,n2,...>
│   ├── remove --store-id <id>
│   └── list
│
├── group                # paxos group mgmt
│   ├── add --store-id <s> --group-id <id> --replica-id <r> --nodes <n1,n2,...>
│   ├── remove --store-id <s> --group-id <id>
│   ├── list --store-id <s>
│   └── inspect --store-id <s> --group-id <id>
│
├── replica              # add/remove individual replicas
│   ├── add --store-id <s> --group-id <g> --node <n> [--replica-id <r>]
│   └── remove --store-id <s> --group-id <g> --replica-id <r>
│
├── kv                   # data plane
│   ├── put --store-id <s> --group-id <g> <key> <value>
│   ├── get --store-id <s> --group-id <g> <key>
│   ├── delete --store-id <s> --group-id <g> <key>
│   ├── scan --store-id <s> --group-id <g> [--prefix <p>] [--limit N]
│   └── list --store-id <s> --group-id <g> [--prefix <p>]
│
└── bench                # load testing (CLI-only)
    ├── run --workload <name> [--qps N --duration T ...]
    ├── stress --duration T --target-qps N
    └── report <run-id>
```

Design rules:
- **Two layers max** — `crowkv <group> <verb>`. No three-level chains.
- Verb vocabulary stays consistent: `add / remove / list / inspect`.
  Lifecycle verbs (`deploy / start / stop`) are reserved for `server`;
  data verbs (`put / get / delete / scan / list`) for `kv`.
- Every command targets the same shared core library; CLI is a thin
  argument-parsing layer.
- Output: human-friendly table by default, `--json` flag for scripting.
- **Logical entity addressing**: store/group/replica/KV commands use
  `--store-id` / `--group-id` (cluster-wide logical ids); the backend
  resolves placement from topology. Server lifecycle uses `--node-id`
  (one server per node).
- **Leaders are elected, not assigned.** `group add` takes no `--leader`
  flag: group leadership is decided by Paxos election among the replicas,
  and the console exposes no forced-leadership control. Operators observe
  the elected leader via `group inspect` / `cluster inspect`.

### 15.5 RPC and Communication

All communication uses protobuf over gRPC (via a mature Rust gRPC library). While raw TCP would have lower overhead, we prioritize a proven RPC library.
