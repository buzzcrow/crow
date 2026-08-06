<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design

This is the root design document. It defines **what CROW is**, **why
key choices were made**, and **how the system is structured**.
Implementation-level detail lives in sub-design docs (`design-*.md`);
this doc covers decisions and architecture only.

---

## 1. Overview

CROW is a high-performance distributed key-value cluster based on
Multi-Paxos with multiple groups for sharding. The key differentiator
from Raft-based KV stores is **parallel slot processing**: within a
group, multiple Paxos slots can be in flight simultaneously, achieving
higher throughput than Raft's strictly sequential log.

**Language:** Rust. **Runtime:** tokio (async everywhere).

**Core goals:**
- **Linearizability** for all acknowledged leader-served operations.
- **Bounded idempotency** for retried requests via `(client_id, seq)` dedup.
- **High throughput** via parallel per-slot Paxos + multiple independent groups.
- **Pluggable storage** — same core library works for in-memory tests,
  local file tests, and crow-tree btree in production.

**Design philosophy:** "Raft for everything that doesn't matter for
performance, Multi-Paxos for the one thing that does." Leader election,
leases, snapshot install, log replay — Raft patterns, well-understood.
The hot path — parallel slot writes — is where Multi-Paxos diverges.
Blind operations only (`Put`, `Delete`); out-of-order apply is safe
because no operation reads before writing.

## 2. Non-Goals (Design Envelope)

- **No multi-group transactions / 2PC.** Each operation targets a
  single group.
- **No read-modify-write (`CAS`, `Increment`).** Only blind operations.
  This is what makes parallel slot writes safe.
- **No dynamic group split/merge.** Operator-managed; membership changes
  require planned reconfiguration.
- **No core-library sharding.** Every KV RPC carries an explicit
  `group_id`. Sharding policy lives in the calling application.
- **No self-hosted topology group (no "Group-0").** Topology is
  operator-managed via HTTP management API. Avoids cluster-wide SPOF.
- **No client-side transactions.** Each request is independent and
  idempotent.
- **No authn/authz.** Trusted-network assumption. Consumers needing
  auth should wrap `crow-kv` in a server that adds it.
- **No full Jepsen-style linearizability checking.** Testing verifies
  same-state comparison and controlled-order verification.

## 3. Key Design Decisions

### 3.1 Multi-Paxos over Raft

Raft is mature; CROW reuses most proven Raft patterns (leader
election, heartbeats, terms, log replication). The one deliberate
departure is **parallel slot writes**: Multi-Paxos allows multiple
slots to be decided in parallel within a group. This creates complexity
in gap repair and linearizable scans, but the throughput gain is worth
it.

### 3.2 Separate PxBallot and PxTerm

`PxBallot = (round, leader_id)` is the Paxos proposal number. `PxTerm`
is a separate monotonic epoch for Raft-style leader election. Keeping
them separate cleanly decouples consensus from election.

### 3.3 System group (Group 0) for topology metadata

A designated Paxos group — **system group (store 0, group 0)** —
stores the full cluster topology as regular KV entries. Since it is a
Paxos group, the topology is replicated, consistent, and HA by the same
mechanism that protects user data. No external coordinator needed.

**Two-phase bootstrap:** Phase 1 uses console TOML as the topology
source of truth (operator-managed, existing behavior). Phase 2 cuts
over to group 0 authoritative via an idempotent `POST /topology/finalize`
that writes all TOML topology into group 0 KV and sets the
`/topology/ready` flag. Console restart uses a three-way fallback:
group 0 missing → TOML mode; group 0 not ready → TOML mode + warning;
group 0 ready → group 0 authoritative.

Group 0 membership evolves using the shipped Model B reconfiguration
(direct HTTP mutation + `membership_epoch` fence). No new consensus
primitive required.

**Design history:** Originally rejected in favor of pure
operator-managed topology. Re-evaluated and adopted when the
single-point-of-failure risk of console-only TOML became the blocking
concern for HA deployments. The operator-managed HTTP management API
remains as the Phase 1 bootstrap path; group 0 adds the HA cutover.

Full design: `../console/design-crow-console.md` §4.3, `design-crow-kv-server.md` §2.2/§2.4.

### 3.4 Explicit group_id on every RPC

`crow-kv` performs no key-to-group mapping. An application built on top
is free to implement `hash(key) % num_groups` or any other policy —
that policy is layered on top of, not inside, the core library.

### 3.5 Lease-based leader reads (default)

Linearizable reads served by the leader use lease-based fencing
(bounded clock skew assumption). ReadIndex/quorum check is available
as a fallback per-read. This is the cheapest correct option.

### 3.6 Safe-slot for bounded stale reads

`safe-slot = min(resolved_slot)` across all learners in a group. All
slots ≤ safe-slot are chosen and applied on every learner. Enables
zero-wait bounded-stale follower reads, equivalent to TiKV's
`safe-ts` / CockroachDB's `closed timestamp`.

### 3.7 Per-key slot tracking

The engine stores `(slot, value)` per key, enabling read-your-writes
without waiting for the global safe-slot.

### 3.8 WAL is the only persistent log

The Acceptor's WAL is the sole durable source of truth. The learner's
btree can replay the WAL on crash. A write is acknowledged to the
client only after a quorum of acceptors have durably flushed. Multi-
disk WAL segments are tagged by slot index for parallelism.

### 3.9 Plaintext transport, TLS hooks reserved

Node-to-node and client-to-node channels are plaintext initially. The
RPC layer and config schema reserve hooks for TLS from day one.

### 3.10 Unified `CROWConfig`

All cluster tunables — WAL, election, paxos, server, and the per-group
internal flags (`force_classic`, `wal_early_ack`,
`async_engine_apply`) — live in one `CROWConfig` struct with `serde`
derives, loaded from a JSON config file (CLI args override individual
fields). `PxGroup` holds a single `config: CROWConfig` field as the
source of truth; individual setters (`set_force_classic`,
`set_wal_early_ack`, etc.) delegate into `self.config.*` for surgical
single-field overrides without rebuilding the whole struct. The
`mgmt_api` rebuild path carries the config as one unit via
`set_from_config(group.config())` instead of per-flag blocks.

## 4. Architecture Overview

```
                              CROW Cluster
   ┌────────────────────────────────────────────────────────────────┐
   │   KvStore A               KvStore B               KvStore C    │
   │   ┌─────────┐             ┌─────────┐             ┌─────────┐  │
   │   │Group-1 L│  ◄────────► │Group-1 F│  ◄────────► │Group-1 F│  │
   │   │Group-2 F│             │Group-2 L│             │Group-2 F│  │
   │   └─────────┘             └─────────┘             └─────────┘  │
   └─────────▲──────────────────────────────────────────────────────┘
             │  HTTP /topology (mgmt API) + per-group writes/reads (gRPC)
        ┌────┴────┐
        │ Client  │  sends KV RPC with explicit group_id
        └─────────┘
```

- **KvStore** — KV-facing runtime on one physical node (`crow-kv-server`
  process). Hosts multiple `PxGroup`s; routes by explicit `group_id`.
- **PxGroup** — independent Paxos ensemble. Contains one
  `PxLocalReplica` (acceptor + learner) and N−1 `PxRemoteReplica`
  proxies for peers on other nodes.
- **Group sizes** can differ per group (3, 5, 7…). No cluster-wide
  `num_groups` or `hash(key) -> group_id` concept.
- **All inter-node communication** is protobuf over gRPC with
  append-only field numbers for rolling-upgrade compatibility.
  Topology discovery is HTTP, not gRPC.

## 5. Data Model

- **Key:** `Vec<u8>` (opaque bytes, lexicographic ordering for scans).
- **Value:** `Vec<u8>`.
- **Operations:** `Get`, `Put`, `Delete`, `Scan`, `BatchPut`,
  `BatchGet`, `BatchDelete` — all single-group.
- **Not supported:** `CAS`, `Increment`, `Watch`/change feed, TTL/expiry.
- **Limits:** key ≤ 1 KB, value ≤ 1 MB, batch ≤ 1024 ops or 4 MiB.

## 6. Read Modes

**Point reads:**
- **Linearizable** — leader-served, fenced by lease or ReadIndex.
- **MinSlot** — client carries `min_slot`; replica serves locally if
  its applied frontier ≥ `min_slot`, otherwise redirects to leader.
  The client chooses the freshness policy by setting `min_slot`:
  `0` = accept any staleness; write watermark = read-your-writes;
  last known `safe_slot` = bounded stale. Under
  `read_endpoint_policy = AnyReplica`, the client round-robins these
  reads across all replicas in the group (leader included), so
  follower capacity is used and the leader's read share drops to
  ~`1/N`.

**Range reads (Scan):** same two modes. Linearizable scan waits for
the leader's own contiguous applied frontier — this is the one
latency cost of parallel slots.

Full read-flow details: `design-crow-kv-leader-election.md`,
`design-crow-kv-state-machine.md`.

## 7. Consensus

- **PxGroup** — independent Paxos ensemble, variable member count.
- **PxSlot** — log position where a single entry is decided.
- **Parallel slot window** — configurable (default 16); limits
  in-flight slots. When full, leader rejects with retryable `Busy`.
- **Gap repair** — background task resolves undecided slots via
  classic Paxos.
- **Leader election** — Raft-style heartbeat + timeout, separate
  `PxTerm`. New leader runs one Phase-1 round, then steady-state
  Phase-2-only writes.
- **Partition behavior** — minority partition rejects all requests;
  majority continues serving.

Full design: `design-crow-kv-slot.md` (parallel slots, gap repair,
correctness proof), `design-crow-kv-leader-election.md` (election, lease,
ReadIndex), `design-crow-kv-rpc.md` (wire protocol, LearnerStream).

## 8. Storage and Durability

- **WAL** — batched durable flush, multi-disk segments tagged by slot,
  CRC integrity, quorum-durable-flush ack contract. Disk loss → node
  rebuilds from peers via snapshot install.
- **Pluggable engines** — in-memory btree (testing), local ordered file
  (testing), crow-tree btree (production). All implement the same
  interface including a `compare` method for cross-engine verification.
- **Per-key slot tracking** — engine stores `(slot, value)` per key;
  tombstones GC'd after slot is below snapshot and safe-slot.
- **Snapshot** — per-group, triggered by WAL size/slot threshold.
  Resumable, checksum-verified, throttleable install for new-node
  bootstrap.

Full design: `design-crow-kv-wal.md`, `design-crow-kv-state-machine.md`,
`../tree/design-crow-tree.md`.

## 9. Cluster Lifecycle

- **Bootstrap** — operator starts each `crow-kv-server`, deploys via
  console, then calls `POST /api/cluster/init` to create the system
  group (store 0, group 0). Data store/group creation is blocked
  (`409`) until the cluster is initialized. Each node persists its
  store/group config to `conf/node-config.json` (per-node config cache)
  and resumes on restart without re-issuing HTTP calls. After startup,
  the node reconciles with group 0 topology KV if group 0 is reachable
  and finalized.
- **Reconfiguration** — per-node HTTP mutation of remote-replica
  lists, persisted to config file, `membership_epoch` fence
  (exact-match on Prepare/Accept). New members join as non-voting,
  catch up via snapshot, then become voting.
- **Rolling upgrade** — protobuf with append-only field numbers;
  on-disk formats carry version headers; one major version step
  compatibility.
- **Backup** — in-cluster recovery via snapshot install + WAL replay.
  External backup tool planned as future extension.
- **Async operations** — management API operations that trigger cluster
  state changes (step-down, remove replica, add replica) may take
  seconds during leader re-election. The async operation pattern:
  trigger returns immediately with `202 {operation_id}`; caller polls
  `GET /operations/:id` for status (`pending` → `running` →
  `completed`/`failed`). `?sync=true` preserves the old synchronous
  behavior for backward compatibility. A `GET
  /stores/:sid/groups/:gid/ready` endpoint checks cluster readiness
  (leader elected, quorum reachable, applied-slot lag). The operation
  registry is an in-memory `DashMap` in `crow-kv-server`; background
  tasks poll group status until a new leader appears or timeout.

Full design: `design-crow-kv-reconfiguration.md`, `design-crow-kv-server.md`.

## 10. Client Interaction

- **Discovery** — client polls a seed server's HTTP `/topology` endpoint
  to build `(store_id, group_id) → leader_endpoint` cache plus a
  `(store_id, group_id) → replica_endpoints` list (local + remotes). No
  gRPC `DescribeCluster`. Re-polls only on cache miss / `NotLeader`.
- **Read-endpoint policy** — `ClientConfig::read_endpoint_policy`
  selects how `MinSlot` reads pick a target replica:
  - `Leader` (default) — every read targets the leader; backward
    compatible, linearizable-safe.
  - `AnyReplica` — `MinSlot` reads round-robin across the cached
    replica list (leader included as one of the N replicas). A follower
    whose applied frontier has not reached `min_slot` returns
    `NotLeader` with the leader hint; the client follows the hint for
    that request and increments `read_endpoint_fallback`. Linearizable
    reads always target the leader regardless of policy. Scans use the
    same selector; the scan fallback parses the server's
    `"not leader; retry scan at {endpoint}"` error string (no protocol
    field today).
  - `LeastConnections` — routes to the replica with the fewest
    in-flight reads (tracked client-side via per-endpoint atomic
    counters, incremented on send and decremented on response via an
    RAII guard). Ties and the first request (no history) fall back to
    round-robin. Same `NotLeader` fallback as `AnyReplica`.
  - `Latency` — routes to the replica with the lowest recent RTT
    (per-endpoint EWMA, `alpha = 0.25`; first sample initializes). Ties
    and the first request (no RTT history) fall back to round-robin.
    Same `NotLeader` fallback as `AnyReplica`.
  - All distributed policies (`AnyReplica`, `LeastConnections`,
    `Latency`) increment `read_endpoint_distributed` on selection and
    `read_endpoint_fallback` on `NotLeader` redirect. Per-endpoint
    statistics live in a `DashMap<String, Arc<EndpointStats>>` keyed by
    endpoint string; entries are created lazily and never evicted
    (stale entries are harmless — zero in-flight, zero RTT, never
    selected).
- **Retry** — on timeout or `NotLeader`, client retries with backoff.
  `NotLeader` with hint → follow hint immediately.
- **Idempotency** — `(client_id, seq)` dedup, persisted into the
  PxLogEntry stream (survives leader change). Per-client retention of
  the last 64 committed `(seq, slot)` mappings, exact-match lookup: a
  recorded `seq` returns its own commit slot; an unrecorded `seq`
  (lower or otherwise) is a miss. Outside the window, outcome is
  unknown, safe to re-propose.

## 11. Module Decomposition

| Module | Responsibility |
| --- | --- |
| **Proposer** | Owns the slot counter; assigns slots; drives Phase 1 on leader change, Phase 2 per write. |
| **Acceptor** | Maintains promised/accepted state per slot; persists to WAL before responding. |
| **Learner** | Tracks chosen values; applies to storage engine; maintains per-key resolved-slot. |
| **WAL** | Durable, multi-disk write-ahead log. Sole persistent ground truth. |
| **KvStore** | KV-facing runtime per node; owns `PxGroup`s; routes by `group_id`. |
| **PxGroup** | Paxos group runtime; coordinates local + remote replicas; holds one `CROWConfig`. |
| **Replicator** | Streams `Accept`/`Chosen` from leader to peers; handles backpressure. |
| **Leader Elector** | Raft-style election; manages `PxTerm`; lease management. |
| **Repair** | Background task: detects and resolves slot gaps via classic Paxos. |
| **Snapshot** | Per-group snapshots; serves install to lagging peers. |
| **Storage Engine** | Pluggable `KVEngine` trait: `InMemKV`, `CrowTreeEngine`. |
| **RPC** | gRPC layer: `PxReplicaService`, `KvStoreService`, `PxSnapshotService`. |

Single-leader hot path: **Proposer → WAL → Replicator → Learner → ack.**

## 12. Crate Layout

```
lib/crow-common/rust    (shared Rust crate: metrics, logging, time, report)
lib/crow-common/cpp     (shared C++ static lib: crc32c, log, compressing_sink, gzip, metrics)
lib/crow-kv              (core library: consensus, engine, wal, rpc, cluster)
lib/crow-kv-client       (client library: topology cache, retry, NotLeader handling)
app/crow-kv-server       (binary: CLI, HTTP mgmt API, store/group wiring)
lib/crow-console-shared  (console core: API client, models)
app/crow-web             (Axum web server + React SPA)
app/crow-cli             (CLI binary: cluster management command)
lib/crow-tree/ffi        (C++ B+tree engine, FFI bridge to Rust)
```

`crow-common` holds project-agnostic utilities shared across the
storage-system stack. The Rust crate (`crow-common`) provides the
metrics registry, `tracing`-based logging wrapper, monotonic-time
helpers, and multi-step error aggregator; `crow-kv` re-exports them at
their original module paths so call sites compile unchanged. The C++
static library (`libcrowcommon.a`, namespace `crow::common`) provides
CRC32C, the spdlog logging facade (`CR_LOG_*` macros), the compressing
file sink, gzip helpers, and the atomic-counter metrics core;
`crow-tree` links against it and bridges the moved types into the
`crow-tree` namespace via using-declarations.

## 13. Concurrency Model

All public and inter-module APIs are `async`. Runtime is `tokio`
(single-threaded `current_thread` for unit tests; multi-threaded for
production).

1. No blocking calls in business-logic paths.
2. Blocking syscalls (`fdatasync`, etc.) go through the project I/O
   facade (`AsyncFile` in `io/mod.rs`).
3. No `std::sync::Mutex` in async paths; use `tokio::sync` primitives.
4. Tests use `#[tokio::test(flavor = "current_thread", start_paused = true)]`.

The async disk I/O substrate (`AsyncFile`: io_uring on Linux ≥ 5.11,
`tokio::fs` fallback otherwise) is detailed in `design-crow-kv-wal.md`.

## 14. Components

- **`crow-kv`** — core library: consensus, engine, WAL, I/O, RPC, reconfiguration.
- **`crow-common`** — shared utilities (Rust crate + C++ static lib):
  metrics, logging, time, report (Rust); CRC32C, spdlog facade,
  compressing sink, gzip, atomic metrics (C++).
- **`crow-kv-server`** — reference server binary. Design: `design-crow-kv-server.md`.
- **`crow-console`** — unified management project (Web UI + CLI).
  Design: `../console/design-crow-console.md`, `../console/design-crow-console-ui.md`.
- **`crowbench`** — benchmark tool, fulfilled by `crow-console` CLI
  `bench` subcommand.
- **RPC** — protobuf over gRPC (tonic + prost). Design: `design-crow-kv-rpc.md`.

## 15. Performance Targets

Initial targets (3-node group, LAN):
- 100K+ writes/sec per group.
- p99 write latency < 10ms under normal load.
- Parallel slot window default = 16.

## 16. Observability

Mandatory signals: per-group leader/term/max-slot/safe-slot/in-flight/gap
count; per-node WAL flush latency and throughput; per-RPC rate/latency/error
breakdown; structured logs with `node_id`, `group_id`, `slot`, `term` on
consensus events.

Full metrics module design (metric types, registry lifecycle, naming
convention, instrumentation points, system collector, log format, in-memory
access, FFI boundary): `design-crow-kv-observability.md`.

## 17. Testing

- **Controlled client (single thread, known order):** verify exact
  operation sequence across learners.
- **Uncontrolled client (multi-thread):** verify identical KV state
  across all learners via `compare`.
- **Failure scenarios:** network partition, message delay/loss/dup,
  clock skew, node crash/restart, disk full, WAL corruption, leader
  step-down, split-brain prevention, leader failure mid-proposal,
  lagging learner catch-up, missing slot repair, snapshot recovery.

Full test strategy: `design-crow-kv-test.md`.

## 18. References

- Lamport, *The Part-Time Parliament* (1998); *Paxos Made Simple* (2001);
  *Paxos Made Live* with Chandra & Griesemer (2007).
- Ongaro & Ousterhout, *In Search of an Understandable Consensus
  Algorithm (Raft)* (2014).
- Mao, Junqueira & Marzullo, *Mencius* (2008); Moraru, Andersen &
  Kaminsky, *EPaxos* (2013).
- Lampson, *How to Build a Highly Available System Using Consensus* (1996).
- TiKV closed-timestamp design notes; CockroachDB closed-timestamp
  design notes.
- Ongaro PhD thesis, *Consensus: Bridging Theory and Practice* (2014).
