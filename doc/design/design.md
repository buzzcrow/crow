<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Design

This is the root design document. It defines **what CrowKV is**, **why
key choices were made**, and **how the system is structured**.
Implementation-level detail lives in sub-design docs (`design-*.md`);
this doc covers decisions and architecture only.

---

## 1. Overview

CrowKV is a high-performance distributed key-value cluster based on
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
  local file tests, and crowtree btree in production.

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
  auth should wrap `crowkv` in a server that adds it.
- **No full Jepsen-style linearizability checking.** Testing verifies
  same-state comparison and controlled-order verification.

## 3. Key Design Decisions

### 3.1 Multi-Paxos over Raft

Raft is mature; CrowKV reuses most proven Raft patterns (leader
election, heartbeats, terms, log replication). The one deliberate
departure is **parallel slot writes**: Multi-Paxos allows multiple
slots to be decided in parallel within a group. This creates complexity
in gap repair and linearizable scans, but the throughput gain is worth
it.

### 3.2 Separate PxBallot and PxTerm

`PxBallot = (round, leader_id)` is the Paxos proposal number. `PxTerm`
is a separate monotonic epoch for Raft-style leader election. Keeping
them separate cleanly decouples consensus from election.

### 3.3 Operator-managed topology (no Group-0)

Originally designed with a self-hosted system group ("Group-0").
**Rejected** — operator-managed topology via HTTP management API is
simpler, avoids a cluster-wide SPOF, and is the implemented and tested
model. Nothing prevents an embedding system from building its own
Group-0 on top.

### 3.4 Explicit group_id on every RPC

`crowkv` performs no key-to-group mapping. An application built on top
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

## 4. Architecture Overview

```
                              CrowKV Cluster
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

- **KvStore** — KV-facing runtime on one physical node (`crowkv-server`
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
- **Read-your-writes** — client carries last write's slot; reads from
  any follower whose per-key resolved slot ≥ that slot.
- **Bounded stale** — client carries last known safe-slot; reads from
  any follower whose global resolved slot ≥ that slot.
- **Best-effort** — read any follower, accept possible inconsistency.

**Range reads (Scan):** `Linearizable` (default), `SafeSlot`,
`AtSlot(N)`. Linearizable scan waits for the leader's own contiguous
applied frontier — this is the one latency cost of parallel slots.

Full read-flow details: `design-leader-election.md`,
`design-state-machine.md`.

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

Full design: `design-slot.md` (parallel slots, gap repair,
correctness proof), `design-leader-election.md` (election, lease,
ReadIndex), `design-rpc.md` (wire protocol, LearnerStream).

## 8. Storage and Durability

- **WAL** — batched durable flush, multi-disk segments tagged by slot,
  CRC integrity, quorum-durable-flush ack contract. Disk loss → node
  rebuilds from peers via snapshot install.
- **Pluggable engines** — in-memory btree (testing), local ordered file
  (testing), crowtree btree (production). All implement the same
  interface including a `compare` method for cross-engine verification.
- **Per-key slot tracking** — engine stores `(slot, value)` per key;
  tombstones GC'd after slot is below snapshot and safe-slot.
- **Snapshot** — per-group, triggered by WAL size/slot threshold.
  Resumable, checksum-verified, throttleable install for new-node
  bootstrap.

Full design: `design-wal.md`, `design-state-machine.md`,
`design-crowtree.md`.

## 9. Cluster Lifecycle

- **Bootstrap** — operator starts each `crowkv-server`, creates
  stores/groups via HTTP management API. Each node persists group
  config to its own config file and resumes on restart without
  re-issuing HTTP calls.
- **Reconfiguration** — per-node HTTP mutation of remote-replica
  lists, persisted to config file, `membership_epoch` fence
  (exact-match on Prepare/Accept). New members join as non-voting,
  catch up via snapshot, then become voting.
- **Rolling upgrade** — protobuf with append-only field numbers;
  on-disk formats carry version headers; one major version step
  compatibility.
- **Backup** — in-cluster recovery via snapshot install + WAL replay.
  External backup tool planned as future extension.

Full design: `design-reconfiguration.md`, `design-kv-server.md`.

## 10. Client Interaction

- **Discovery** — client polls a seed server's HTTP `/topology` endpoint
  to build `(store_id, group_id) → leader_endpoint` cache. No gRPC
  `DescribeCluster`. Re-polls only on cache miss / `NotLeader`.
- **Retry** — on timeout or `NotLeader`, client retries with backoff.
  `NotLeader` with hint → follow hint immediately.
- **Idempotency** — `(client_id, seq)` dedup, persisted into the
  PxLogEntry stream (survives leader change). Retention: ≥ 64 requests
  per client AND ≥ 60s. Outside the window, outcome is unknown.

## 11. Module Decomposition

| Module | Responsibility |
| --- | --- |
| **Proposer** | Owns the slot counter; assigns slots; drives Phase 1 on leader change, Phase 2 per write. |
| **Acceptor** | Maintains promised/accepted state per slot; persists to WAL before responding. |
| **Learner** | Tracks chosen values; applies to storage engine; maintains per-key resolved-slot. |
| **WAL** | Durable, multi-disk write-ahead log. Sole persistent ground truth. |
| **KvStore** | KV-facing runtime per node; owns `PxGroup`s; routes by `group_id`. |
| **PxGroup** | Paxos group runtime; coordinates local + remote replicas. |
| **Replicator** | Streams `Accept`/`Chosen` from leader to peers; handles backpressure. |
| **Leader Elector** | Raft-style election; manages `PxTerm`; lease management. |
| **Repair** | Background task: detects and resolves slot gaps via classic Paxos. |
| **Snapshot** | Per-group snapshots; serves install to lagging peers. |
| **Storage Engine** | Pluggable `KVEngine` trait: `InMemKV`, `CrowtreeEngine`. |
| **RPC** | gRPC layer: `PxReplicaService`, `KvStoreService`, `PxSnapshotService`. |

Single-leader hot path: **Proposer → WAL → Replicator → Learner → ack.**

## 12. Crate Layout

```
crowkv              (core library: consensus, engine, wal, rpc, cluster)
crowkv-client       (client library: topology cache, retry, NotLeader handling)
crowkv-server       (binary: CLI, HTTP mgmt API, store/group wiring)
crowkv-console/shared  (console core: API client, models)
crowkv-console/web     (Axum web server + React SPA)
crowkv-console/cli     (CLI binary: crowkv command)
crowtree/ffi        (C++ B+tree engine, FFI bridge to Rust)
```

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
`tokio::fs` fallback otherwise) is detailed in `design-wal.md`.

## 14. Components

- **`crowkv`** — core library: consensus, engine, WAL, I/O, RPC, reconfiguration.
- **`crowkv-server`** — reference server binary. Design: `design-kv-server.md`.
- **`crowkv-console`** — unified management project (Web UI + CLI).
  Design: `design-console.md`, `design-ui.md`.
- **`crowbench`** — benchmark tool, fulfilled by `crowkv-console` CLI
  `bench` subcommand.
- **RPC** — protobuf over gRPC (tonic + prost). Design: `design-rpc.md`.

## 15. Performance Targets

Initial targets (3-node group, LAN):
- 100K+ writes/sec per group.
- p99 write latency < 10ms under normal load.
- Parallel slot window default = 16.

## 16. Observability

### Mandatory Signals

Per-group leader/term/max-slot/safe-slot/in-flight/gap count; per-node WAL
flush latency and throughput; per-RPC rate/latency/error breakdown; structured
logs with `node_id`, `group_id`, `slot`, `term` on consensus events. Tracing
hooks reserved but not required in the initial design.

### Metrics Module

A lightweight metrics system with five metric types and periodic flush to a
dedicated metrics log file. Two independent implementations: Rust for the
consensus/RPC/WAL layer, C++ for the storage engine. Each owns its own
counters and logs its own summary. No metrics cross the FFI boundary at
runtime.

**Metric types:**

- **Counter** (`AtomicU64` x 2) — monotonic, tracks window delta + total.
  `inc()` / `inc_by(n)`. Flush shows `count`, `tps`, `total`. Use cases:
  puts, gets, deletes, errors, WAL records, elections, step-downs.
- **Gauge** (`AtomicU64`) — current state, can go up or down. `set(v)`.
  Flush shows last value. Use cases: buffer pool resident/dirty pages,
  in-flight slots.
- **Bandwidth** (`AtomicU64` x 3) — monotonic bytes, tracks count + sum +
  total_bytes. `observe(bytes)`. Flush shows `count`, `tps`, `avg_size(KB)`,
  `rate(KB/s)`. Use cases: KV bytes in/out.
- **LatencyHistogram** (13 buckets + 2 `AtomicU64`) — fixed-bucket percentile
  distribution. Bucket boundaries: `0, 1us, 10us, 100us, 500us, 1ms, 5ms,
  10ms, 50ms, 100ms, 500ms, 1s, infinity`. `observe(ns)` does binary search +
  `fetch_add`. Flush computes p50/p99 from cumulative distribution. Use cases:
  KV put latency, KV get latency.
- **LatencySummary** (`AtomicU64` x 4) — lightweight latency tracking
  (count + sum + max + total_count). `observe(ns)`. Flush shows `avg(us)`,
  `max(us)`. Use cases: scan, snapshot, WAL append, RPC, apply.

**Registry and lifecycle:**

Each language has a `MetricsRegistry` that owns all metric instances. The
registry has `start(interval_secs)` (spawns flush thread/task), `stop()`
(final flush + join), and `flush()` (iterate all metrics, snapshot, format,
reset window state). Interval is typically 5s or 10s.

- Rust (`crowkv/src/metrics/mod.rs`): `MetricsRegistry` with type-grouped
  `Vec<T>` collections, `Arc`-shared, metric handles stored on service/store
  structs. `start()` spawns tokio interval task. Also provides
  `snapshot(prefix)` for in-memory access without resetting window state.
- C++ (`crowtree/include/crowtree/metrics.h`, `crowtree/src/metrics.cpp`):
  Same type-grouped pattern. `start()` spawns `std::thread` with
  `sleep_for` loop. Metric handles are raw pointers (registry owns lifetime).

**Naming convention:**

Dot-separated hierarchical paths: `s.{store_id}.g.{group_id}.{module}.{metric}`.
Type suffix on every metric name: `.c` (Counter), `.g` (Gauge), `.bw`
(Bandwidth), `.lh` (LatencyHistogram), `.l` (LatencySummary). Dynamic suffix
`@{peer_endpoint}` for per-peer metrics. System metrics use `sys.` prefix
with no type suffix.

Prefix-based snapshot: `registry.snapshot("s.1.")` returns all metrics for
store 1; `snapshot("")` returns all. This is the foundation for future GUI
integration (R11).

**Instrumentation points:**

- Rust KV service (`kv_service.rs`): put/get latency histograms, scan summary,
  delete counter, bytes in/out bandwidth, error counter.
- Rust WAL (`wal_engine.rs`): append latency summary.
- Rust cluster (`local_replica.rs`): election/step-down counters, in-flight
  slots gauge. Replaces `ElectionMetrics`.
- Rust RPC (`remote_replica.rs`): per-peer RPC latency summary + error counter
  with dynamic names. Replaces `LayerMetrics`.
- C++ buffer pool (`buffer_pool.cpp`): hits/misses/evictions/writebacks
  counters, resident/dirty gauges.
- C++ engine (`crowtree.cpp`): apply latency summary.
- C++ persist (`persist.cpp`): snapshot latency summary.

**System metrics collector:**

Special collector type polled at flush time (not increment-on-event). Reads
TCP retransmits/lost (Linux `/proc/net/snmp`, macOS no-op), CPU user/sys
and memory RSS via `getrusage(RUSAGE_SELF)`. Computes CPU% as delta over
flush window.

**Metrics log file:**

Dedicated file `metrics-{timestamp}-{pid}.log` in the log directory, separate
from application log. Each flush produces a block with timestamp header
`[metrics {ISO8601} window={N}s]`, followed by type-grouped sections (Counter,
LatencyHistogram, LatencySummary, Bandwidth, Gauge, System). Names sorted
alphabetically within each section, padded to `max_name_len` for alignment.
Zero-suppression: counters/histograms/summaries/bandwidth with zero window
activity are skipped; gauges always printed. Format designed for both human
reading and script parsing (split on whitespace, parse as numbers).

**In-memory access:**

`registry.snapshot(prefix)` returns current values without resetting window
state. Enables future `/metrics` HTTP endpoint and GUI integration. No need
to parse log files to get metric values.

**FFI boundary:**

No metrics cross FFI at runtime. Rust registry logs Rust-side metrics; C++
registry logs C++-side metrics. Two independent log blocks per flush cycle.
The existing `ct_get_stats` FFI call (used by `/topology`) is unaffected.

## 17. Testing

- **Controlled client (single thread, known order):** verify exact
  operation sequence across learners.
- **Uncontrolled client (multi-thread):** verify identical KV state
  across all learners via `compare`.
- **Failure scenarios:** network partition, message delay/loss/dup,
  clock skew, node crash/restart, disk full, WAL corruption, leader
  step-down, split-brain prevention, leader failure mid-proposal,
  lagging learner catch-up, missing slot repair, snapshot recovery.

Full test strategy: `design-test.md`.

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
