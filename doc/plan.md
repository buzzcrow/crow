# CrowKV - Plan: Implementation Master Schedule

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md), [`design-leader-election.md`](design/design-leader-election.md), [`design-parallel-slots.md`](design/design-parallel-slots.md), [`design-wal.md`](design/design-wal.md), [`design-state-machine.md`](design/design-state-machine.md), [`design-reconfiguration.md`](design/design-reconfiguration.md)
Satisfies: all of [`requirement.md`](requirement.md) (phased implementation)

This document defines the phased implementation schedule and cross-stream dependencies.
Detailed per-milestone plans are created temporarily before each step (see §6).

> **Note:** sub-topic plan files (`plan-consensus.md`, etc.) have been merged into this outline. Before implementing a milestone, create a temporary detailed plan if needed.

---

## Table of Contents

- [1. Phase Overview](#1-phase-overview)
  - [P1 — Consensus Core](#p1--consensus-core)
  - [P2 — WAL](#p2--wal)
  - [P3 — Storage Engine](#p3--storage-engine)
  - [P4 — RPC / Client](#p4--rpc--client)
  - [P5 — Reconfiguration](#p5--reconfiguration)
- [2. Cross-Stream Dependencies](#2-cross-stream-dependencies)
- [3. Global Milestones](#3-global-milestones)
- [4. Test Pairing Rule](#4-test-pairing-rule)
- [5. Concurrency Model](#5-concurrency-model)
- [6. Decision Log](#6-decision-log)

---

## 1. Phase Overview

Phasing is dependency-ordered, not waterfall. WAL (P2) and Storage (P3) can proceed in parallel once consensus core `PxLogEntry` shape and trait boundaries are frozen. RPC (P4) needs consensus message types. Reconfiguration (P5) needs RPC + WAL + consensus.

---

### P1 — Consensus Core (in-memory, no persistence)

| Milestone | Scope | Acceptance |
|---|---|---|
| **M1** | Core types (`PxTerm`, `PxBallot`, `PxSlot`, `PxLogEntry`, `LogEntryKind`, `Operation`, `OpKind`, `PxGroupConfig`, `PxGroupMember`). Lock-free `SlotList` + `PxSlotNode`. In-memory `PxAcceptor`: `prepare()` and `accept()` handlers; ballot monotonicity; `promised_cloned` / `accepted_cloned` helpers. Slot-node reclamation (epoch-based). | Unit tests: ballot ordering, acceptor basic Paxos round, slot_list insert/get/reclaim, slot_node trim. |
| **M2** | Minimal RPC service over loopback gRPC (`tonic` + `prost`). `.proto` schema for `Prepare`/`Promise`/`Accept`/`Accepted` with `version: u32` at tag 1 plus append-only unary trace fields (`request_id`, `request_create_ms`). Current KV RPCs carry `client_id`, `request_id`, and `request_create_ms`; `(client_id, seq)` is plumbed into `LogEntry` for later dedup. `Node` struct with `NodeRole` (hard-coded leader/follower). `TestNodeHarness` spawns nodes on configurable loopback addresses. KV-driven Paxos fanout uses unary gRPC peer endpoints from `PxClusterInfo`, with direct in-process peer fallback retained only for old/single-node tests. `MinimalProposer` drives classic / optimized / multi-Paxos rounds. | Integration tests: S0-A classic (3-node), S0-B optimized (3-node), S0-C multi-Paxos 10 slots (5-node), S0-D quorum rejection (5-node), KV unary trace echo/client-id plumbing, and KV gRPC peer fanout. All pass. |
| **M3** | Leader election + election-side lease + bidi peer stream substrate. Randomized timeout, `PreVote`, `RequestVote`/`Vote`, `Heartbeat`, `StepDown` (admin primitive). Term tracking with `(term, ballot)` two-fence rule; `term` plumbed onto all messages. Role transitions (Follower → PreCandidate → Candidate → Leader, step-down on higher term **or** unrenewable lease). Election-side lease state machine (vote-refusal promise, leader self-expiry, monotonic-clock-only); read-side lease consumption stays in M5. Bulk Phase 1 on new leader: scan open prefix, adopt values, fill gaps with `NoOp`. Per-peer bidi gRPC stream (`PeerStream`) multiplexing `Accept`+`Heartbeat`+`Chosen` — server handler, client skeleton, reconnect loop, and `PendingMap` correlation all land here. `PxLearner.contiguous_chosen` / `contiguous_applied` / `last_chosen_term` watermark (lifted from M5 because needed for vote "log up-to-date" + bulk-Phase-1 floor). `PxAcceptor::highest_seen_slot` cursor. `PxReplicaError` enum (drops `tonic::Status` from `ReplicaHandler`/`ReplicaClient`). Observability counters (`election_count`, `current_term`, `last_heartbeat_age_ms`, `lease_remaining_ms`). | Unit tests: single leader elected, stale leader fenced, lease-window blocks disruptive candidate, PreVote does not bump term, bulk Phase 1 adopts in-flight value, NoOp apply path, term fencing in acceptor, admin step-down via RPC, PreVote prevents partition disruption, PeerStream reconnect drains pending oneshots. |
| **M4** | `Proposer`: owns slot allocation after the M2 temporary slot counter, sliding window (default 16), bounded admission queue, `Busy` backpressure. Parallel Accept fanout via `JoinSet` on top of the M3 `PeerStream` substrate (leveraging its per-request `PendingMap` + oneshot correlation so slot N+1 can be sent before slot N's reply returns). `Replicator`: per-slot quorum bitmap, per-peer flow control via the stream's bounded mpsc. Background `Repair` task: gap detection by age/count threshold, classic Paxos repair and safe-slot/frontier tracking. Unary `Accept` handler deprecated once all traffic is on `PeerStream`. | Unit tests: 10 parallel slots chosen out of order, gap repair fills missing slot, safe-slot advances, PeerStream backpressure (`Busy`) exercised under overload. |
| **M5** | `Learner`: apply chosen values to engine, per-key resolved-slot, **safe-slot publication/propagation** (the in-memory `contiguous_chosen`/`contiguous_applied` watermarks already exist from M3). `Engine` trait + `InMemoryEngine`: `apply(slot, batch)`, `get(k)`, `scan(range, limit)`, `snapshot_export/import`, `compare() → Diff`. **Read-side lease + ReadIndex** wiring (lease *state* already exists from M3; this milestone connects it to the `Get(Linearizable)` fast path and adds the ReadIndex round-trip fallback). Read modes: `Linearizable` (lease-valid fast path / ReadIndex fallback), `SafeSlot`, `BestEffortStale`. | Unit tests: write acked value immediately readable, lease-valid fast read, lease-expired ReadIndex, `compare()` zero divergence after 100 random ops. (Lease-unrenewable step-down test stays with M3.) |
| **M6** | Per-`client_id` last-sequence map + last-result cache using the client identity fields already present in current KV RPCs and `LogEntry`. `DedupCheckpoint` entry kind (stubbed, in-memory only). | Unit tests: retry same `(client_id, seq)` returns same result; different seq advances. |

**P1 freeze gates:** `PxLogEntry` shape + `PxBallot`/`PxTerm` at end of M1; classic-Paxos wire shapes (`Prepare`/`Promise`/`Accept`/`Accepted`) at end of M2; full consensus message types at end of M4; Engine trait surface at end of M5.

---

### P2 — WAL (write-ahead log, multi-disk)

| Milestone | Scope | Acceptance |
|---|---|---|
| **M0** | Async I/O facade: `AsyncFile` trait (`open`, `read_at`, `write_at`, `fsync`/`fdatasync`, `close`). Three backends: `tokio-uring` (Linux ≥ 5.11), `tokio::fs` + `spawn_blocking` fallback, `SimDisk` (in-memory for tests). Capability probe at startup; same binary runs everywhere. | Unit tests: all three backends pass same `AsyncFile` matrix; backend selection logged once at INFO. |
| **M1** | `Segment` file format: header, length-prefixed records, footer with slot range. `WALRecord`: magic, version, record_type, `group_id`, `term`, `slot`, `ballot`, payload, CRC32C. `SegmentIndex`: in-memory `slot → (disk, segment_id, offset)` map, rebuildable from headers. | Unit tests: write segment, close, reopen, read back all records with valid CRC. |
| **M2** | `FsyncWorker` per disk: long-running async task, `mpsc` queue, batch by bytes/time/watchdog. Async completion future per `Accept`; `Accepted` gated on fsync completion. Single-disk throughput benchmark (`criterion`). | Unit tests: batch of 100 records fsynced in ≤ 3 calls; p50/p99 latency documented. |
| **M3** | `WalManager`: distributes slots across configured disks, per-disk segment rotation on size threshold. Multi-disk aggregate throughput benchmark. | Unit tests: IOPS scales (sub-linear acceptable; slope documented). |
| **M4** | Startup replay: discover segments, order by `segment_id`, walk records. CRC failure → truncate at failure point, log warning, continue later segments. Rebuild acceptor in-memory state (`promised`, `accepted`) from records; highest-ballot accept wins per slot. Rebuild `current_term` **and `voted_for`** from max seen term (resolves the in-memory-only gap from P1 M3). Rebuild dedup cache from latest `DedupCheckpoint` + subsequent `Write` records. | Unit tests: 1000 records written, simulated crash (drop last un-fsynced batch), restart, state deterministic; `current_term`/`voted_for` survive restart. |
| **M5** | GC: `gc_slot = min(safe_slot, snapshot_slot)`. Unlink whole segments when all records have `slot < gc_slot`. Disk-pressure eager GC trigger. | Unit tests: GC removes segments below watermark; replay after GC skips them correctly. |

**P2 freeze gates:** `WALRecord` format frozen and versioned; ack contract enforced (`Accepted` only after fsync); replay deterministic for any prefix-fsynced stream.

---

### P3 — Storage Engine (crowtree)

The production engine is **crowtree**, a C++ `libcrowtree` (single-writer COW
B+tree + per-leaf delta chains + epoch GC + versioned root) consumed from Rust
over a coarse C ABI. Full design: [`design/design-crowtree.md`](design/design-crowtree.md)
and its sub-docs (core / persistence / snapshot-gc / test). The list below is the
**high-level** milestone shape; a detailed implementation plan is written before
each milestone (per §6 / `doc.md` conventions).

| Milestone | Scope (high level) | Acceptance |
|---|---|---|
| **M1** | Redefined async `KVEngine` + `EngineView` trait surface (`apply`, `get`, `scan`, `snapshot_view`, `last_applied_slot`, `persist_checkpoint`, `set_gc_watermark`, `collect_garbage`, `snapshot_export/import`, `clear`). `InMemKV` migrated to it; learner driven through the new surface. | `InMemKV` implements the trait; existing learner/consensus tests pass on the new surface. |
| **M2** | `libcrowtree` core (C++): pages, mapping table, slot cell, write path (apply→delta→consolidate→split/merge), epoch GC, versioned root; `InMemoryPageStore`. C API + Rust `CrowtreeEngine` FFI adapter. | C++ unit/integration green; `CrowtreeEngine` passes the shared `KVEngine` suite; parity `compare()` vs `InMemKV` empty. |
| **M3** | Persistence: `PageStore` backends (`FilePageStore` first; block/RDMA stubs), on-disk page format + IU alignment + CRC, checkpoint, lazy recovery, superblock A/B. | Crash/recovery tests (G2-style) pass; `last_applied_slot` survives restart; torn-page handled. |
| **M4** | Snapshot + GC flow integration: portable `snapshot_export/import`, watermark-driven tombstone + stale-version GC, learner/consensus-WAL/new-member wiring. | Export→import round-trip `compare()` equal; resume-from-offset deterministic; GC reclaims below watermark; new-member install parity. |

**P3 freeze gates:** `KVEngine`/`EngineView` trait surface frozen (no additions in
P4 without explicit version bump); `compare()` deterministic and cross-engine;
snapshot format versioned and self-describing; crowtree C ABI append-only.

---

### P4 — RPC / Client

| Milestone | Scope | Acceptance |
|---|---|---|
| **M1** | Formalize `.proto` files: full message set (`Prepare`, `Promise`, `Accept`, `Accepted` inherited from P1 M2 at same field numbers; plus `Chosen`, `Heartbeat`, `RequestVote`, `Vote`, `SnapshotChunk`, `ClientRequest`, `ClientResponse`, `DescribeCluster`, `NotLeaderHint`). `version: u32` at fixed tag 1 on every message. Append-only field numbers. `build.rs` invokes `tonic-build`. | All message types encode/decode round-trip; `protoc --decode_raw` succeeds on every generated message. |
| **M2** | Node-to-node gRPC: `PeerService` (bidi stream per `(group_id, peer_id)` carrying `Accept`/`Accepted`/`Chosen`/`Heartbeat`). `VoteService` (unary `RequestVote` → `Vote`). `SnapshotService` (server-streaming `SnapshotChunk`). Plaintext only (TLS deferred). | 3-node cluster on loopback passes S1–S3 scenarios (leader change, parallel slots with gap, partition). |
| **M3** | Client library: seed list (static config), `DescribeCluster` RPC, topology cache (`group_id → leader_endpoint`). Key hash → `group_id` → cached leader; fallback to any member responding `NotLeaderHint`. Retry: exponential backoff on timeout, immediate retry on `NotLeaderHint`. `(client_id, sequence_number)` per write. Cache `safe_slot` from responses. | Client survives leader change mid-request with auto-retry, returns same result; survives Group-0 leader change. |
| **M4** | Read mode routing: `Linearizable` → leader only, lease check enforced (lease state machine unchanged from P1 M5, only clock source swaps `TestTimer` → real monotonic). `SafeSlot`/`AtSlot(N)` → any replica with sufficient resolved-slot. `BestEffortStale` → any replica. Lease fallback: ReadIndex as quorum heartbeat round-trip. | Mixed workload (50% leader reads, 50% follower reads) returns zero divergence; ReadIndex fallback exercised by disabling lease in test config. |

**P4 freeze gates:** `.proto` schema frozen (append-only field numbers, version at tag 1); snapshot streaming protocol frozen (P5 uses it for new-member install); lease wired to real monotonic clock, P1 lease tests still pass under `TestTimer`.

---

### P5 — Reconfiguration

| Milestone | Scope | Acceptance |
|---|---|---|
| **M1** | Snapshot install: wire `SnapshotService` to `Engine::snapshot_export`/`snapshot_import`. Resumable chunked transfer with `(snapshot_id, chunk_offset)` checkpointing; restart-after-failure resumes at last offset. End-to-end CRC before activation; throttle via `chunk_rate_bytes_per_sec`. New-node bootstrap: receive snapshot at slot S, catch up via WAL streaming `[S+1, current_max_chosen]`. | New empty node added to running 3-node group: snapshot installs, catches up, joins quorum, `compare()` equals existing learners. |
| **M2** | Joint consensus: `ConfigChange(joint = C_old ∪ C_new)` and `ConfigChange(C_new)` log entries. Both-quorum rule active while joint config is *applied* (not merely chosen). New members join as non-voting catch-up readers during joint phase. Failure recovery: roll back to `C_old` if `catchup_timeout` exceeded. | 3 → 5 single-member add succeeds online; failed catch-up rolls back cleanly to 3. |
| **M3** | Leader transfer: `TimeoutNow` RPC instructs target follower to start election at `term + 1`. Pre-condition: target's `contiguous_applied == leader's max_chosen`. Used during leader-removal reconfig; exposed as admin RPC for planned maintenance. | Explicit transfer completes within `leader_transfer_timeout` (default 5 s); old leader steps down; clients redirected via `NotLeaderHint`. |
| **M4** | Rolling upgrade: version header in `WALRecord`, snapshot, and protobuf messages. `config_version` in Group-0 prevents older binary joining newer-format cluster. Operational procedure in `README.md`: stop → upgrade → restart → wait catch-up → repeat. Test scope: consensus protocol compat only (no WAL/snapshot version compat in test scope). | Mixed-version cluster (one node N+1, two nodes N) serves traffic without divergence; full upgrade completes without write unavailability. |

**P5 release gates:** G5 passes (3 → 5 → 7 online, zero crowbench divergence); rolling binary upgrade 1 version step succeeds in CI; snapshot install resumes after simulated network failure; failed catch-up rolls back cleanly without leaving cluster in joint mode.

---

## 2. Cross-Stream Dependencies

### Phase order

```
P1 Consensus Core ───────────────────────────┐
    ├─frozen PxLogEntry shape────────┼──► P2 WAL (parallel with P3)
    ├─frozen engine trait boundary────┼──► P3 Storage (parallel with P2)
    └─message types frozen────────────┼──► P4 RPC

P2 WAL + P3 Storage ─────────────────────► P4 RPC (needs persistence for real restart)
P4 RPC ──────────────────────────────────► P5 Reconfig (needs transport + snapshot stream)
```

### Crate layout

```
crowkv    (all core logic: consensus, engine, wal, io, rpc, reconfig)
  └─ crowkv-server                                 [P4] (binary)

crowkv::testkit  (dev-dep only; TestTimer, TestRouter, TestNode, SimDisk)
  └─ crowkv::io (re-exports SimDisk)
```

Dependency rule: a crate may depend only on crates **above** it in this list. `crowkv::testkit` is reachable only as a `dev-dependency`.

### Freeze points (must complete before downstream starts)

- `PxLogEntry` shape + `PxBallot`/`PxTerm` — end of **P1 M1**
- Classic-Paxos RPC shapes (`Prepare`/`Promise`/`Accept`/`Accepted`) — end of **P1 M2**
- Consensus message types (full set) — end of **P1 M4**
- Engine trait surface — end of **P1 M5** (P3 M1 reviews)
- gRPC `.proto` schema — end of **P4 M1**

---

## 3. Global Milestones

| ID | Name | Criteria | Phase |
|---|---|---|---|
| G1 | Core linearizable | 3-node unit test: writes survive forced leader step-down | P1 |
| G2 | Persistent core | `kill -9` of leader, restart, re-elect, no data loss | P2 |
| G3 | Engine parity | Ordered-file engine passes same `compare()` as in-memory | P3 |
| G4 | Networked cluster | 3 real processes on loopback, crowbench 10k ops, zero divergence | P4 |
| G5 | Elastic membership | 3 → 5 → 7 online, rolling upgrade 1 version step | P5 |

**Gate ordering:** G1 must pass before P4 starts. G2 and G3 must both pass before P4 starts. G4 must pass before P5 starts.

---

## 4. Test Pairing Rule

Every phase milestone includes:
1. **Unit invariants** from the matching test design area (property-based or deterministic).
2. **Failure-injection** matching [`design.md`](design.md) §9 scenarios.
3. **crowbench** integration test (end-to-end correctness) once P4 is reached.

See [`plan-test.md`](plan-test.md) for pending test task tracking.

---

## 5. Concurrency Model

All public and inter-module APIs are `async`. Runtime is `tokio` (single-threaded `current_thread` for P1 tests; multi-threaded for production from P4).

**Rules:**
1. No blocking calls in business-logic paths.
2. Blocking syscalls (`fdatasync`, etc.) go through the project I/O facade ([`design-async-io.md`](design/design-async-io.md)).
3. No `std::sync::Mutex` in async paths; use `tokio::sync::{Mutex, RwLock, Notify, mpsc, oneshot}`.
4. No `std::thread::sleep`; use `tokio::time::sleep`.
5. Tests use `#[tokio::test(flavor = "current_thread", start_paused = true)]` for determinism.

---

## 6. Decision Log

Resolved cross-cutting questions (audit trail). New questions get `**TODO-CONFIRM:**` prefix and resolved in place.

- ~~**TODO-CONFIRM (P1):** Lease-based linearizable reads in P1.~~ **Resolved:** deterministic lease via `TestTimer`, full state machine in P1 M4.
- ~~**TODO-CONFIRM (P1):** Synchronous step loop vs `tokio` `LocalSet`.~~ **Resolved:** `tokio` everywhere.
- ~~**TODO-CONFIRM (P1):** Single-group only or 2-group smoke test?~~ **Resolved:** include `integration_two_group_smoke` in P1.
- ~~**TODO-CONFIRM (P2):** `criterion` for fsync benchmarks?~~ **Resolved:** `criterion`.
- ~~**TODO-CONFIRM (P4):** Group-0 bootstrap timing — G4 or P5?~~ **Resolved:** static in P4, dynamic in P5.
- ~~**TODO-CONFIRM (P5):** Rolling-upgrade testing scope.~~ **Resolved:** consensus protocol compat only.
- ~~**TODO-CONFIRM (P1 M3):** Does the leader lease belong to election (M3) or to reads (M5)?~~ **Resolved:** split. The **election-side** lease (vote-refusal promise, leader self-expiry, step-down on unrenewable) lands in **M3** because it is required for the safety proof of "at most one leader per term." The **read-side** lease (using lease state to short-circuit `Get(Linearizable)` and ReadIndex fallback) stays in **M5** alongside the read pipeline. See [`design/design-leader-election.md`](design/design-leader-election.md) §6.
- ~~**TODO-CONFIRM (P1 M3):** Heartbeat/election defaults — 100 ms / 800–1500 ms appropriate?~~ **Resolved:** bump defaults to heartbeat `500 ms` / election `4000–8000 ms` / lease `4500 ms` / max-clock-skew `500 ms`. The previous defaults are aggressive even for a single datacenter; the new values trade ~5× failover latency for 5× lower heartbeat chatter and broader operational tolerance. Single-DC operators can override down to the old values; cross-DC operators can raise to ~3 s heartbeat. Tests use `PxElectionConfig::for_tests()` (ms-scale) under `tokio::time::pause()`. See [`design/design-leader-election.md`](design/design-leader-election.md) §10.
- ~~**TODO-CONFIRM (P1 M3):** Move "per-peer connection pool / bidi stream" from M4 to M3?~~ **Resolved:** moved. Heartbeats and `Accept`s share ordering requirements (the lease grant must not reorder ahead of an `Accept`), so a single bidi stream per `(group_id, peer_id)` is required from the moment heartbeats exist. M4 retains the `Proposer` admission queue / sliding window and the `Replicator` quorum bitmap on top of this substrate.
- ~~**TODO-CONFIRM (P1 M3):** Lift `PxLearner.contiguous_chosen` frontier from M5 to M3?~~ **Resolved:** lifted. Required by the Raft "log up-to-date" vote rule and by the bulk-Phase-1 floor computation; the same field is reused by M5 for safe-slot publication, so it is not throwaway work. M5 retains responsibility for safe-slot **propagation** and read-time consumption.
- ~~**TODO-CONFIRM (P1 M3):** Drop `tonic::Status` from `ReplicaHandler`/`ReplicaClient` trait surface?~~ **Resolved:** yes. New `PxReplicaError` enum is the in-library error type; `tonic::Status` only appears in `crowkv/src/rpc/`. Conversions live at the gRPC adapter boundary. Lands in M3 alongside the new vote/heartbeat handlers.
- ~~**TODO-CONFIRM (P1 M3):** Persist `current_term` + `voted_for` in M3?~~ **Resolved:** no — kept in `AtomicU64` for M3 (no WAL exists yet). P2 M4 replay row is updated to rebuild both from WAL on startup.
- ~~**TODO-CONFIRM (P1 M3):** Remove `PxGroup::set_leader_id` test seed?~~ **Resolved:** keep for M3 as an initial-value seed (election driver may override at runtime). Removal tracked as a post-M3 cleanup in `todo_code.md`. 1-replica and 2-replica clusters rely on the election driver, not on the seed — documented in `testkit/cluster.rs`.
