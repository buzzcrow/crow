# CrowKV - Plan: Implementation Master Schedule

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md), [`design-leader-election.md`](design/design-leader-election.md), [`design-parallel-slots.md`](design/design-parallel-slots.md), [`design-wal.md`](design/design-wal.md), [`design-storage-engine.md`](design/design-storage-engine.md), [`design-reconfiguration.md`](design/design-reconfiguration.md)
Satisfies: all of [`requirement.md`](requirement.md) (phased implementation)

This document defines the phased implementation schedule and cross-stream dependencies. Deep dives for each workstream live in sibling sub-topic docs (`plan-consensus.md`, `plan-wal.md`, `plan-storage.md`, `plan-rpc.md`, `plan-reconfig.md`).

## Table of Contents

- [1. Phase Overview](#1-phase-overview)
- [2. Document Structure](#2-document-structure)
- [3. Cross-Stream Dependencies](#3-cross-stream-dependencies)
- [4. Global Milestones](#4-global-milestones)
- [5. Test Pairing Rule](#5-test-pairing-rule)
- [6. crowtree Placeholder](#6-crowtree-placeholder)
- [7. Concurrency Model](#7-concurrency-model-project-wide-decision)
- [8. Decision Log](#8-decision-log)

---

## 1. Phase Overview

| Phase | Workstream | What is built | Acceptance criteria |
| --- | --- | --- | --- |
| 1 | `plan-consensus.md` | In-memory consensus core: leader election, parallel slots, gap repair, in-memory proposer/acceptor/learner, in-memory btree engine | Single-group `Put`/`Get` linearizable across leader change in unit tests |
| 2 | `plan-wal.md` | Multi-disk WAL: segments, batched fsync, replay, CRC, ack contract | Survive `kill -9`; replay deterministic state; fsync latency benchmark |
| 3 | `plan-storage.md` | Engine trait freeze, ordered-file backend, snapshot export/import skeleton, `compare()` | Engine swap-in without consensus code changes; in-memory vs ordered-file `compare()` returns empty diff |
| 4 | `plan-rpc.md` | gRPC/protobuf wire protocol, node-to-node transport, client discovery/routing/retry/idempotency | 3-node cluster on loopback passes crowbench correctness suite |
| 5 | `plan-reconfig.md` | Joint consensus, snapshot install, rolling upgrade version gating | 3 → 5 → 7 member change without downtime; rolling binary upgrade |

Phasing is dependency-ordered, not waterfall. WAL (P2) and Storage (P3) can proceed in parallel once the consensus core's `PxLogEntry` shape and trait boundaries are frozen. RPC (P4) needs consensus message types. Reconfiguration (P5) needs RPC + WAL + consensus.

## 2. Document Structure

| Master Doc | Sub-Docs (one per workstream) |
|---|---|
| `plan.md` — this doc | `plan-consensus.md`, `plan-wal.md`, `plan-storage.md`, `plan-rpc.md`, `plan-reconfig.md` |
| `test-design.md` — strategy, invariant framework, crowbench architecture | `test-design-consensus.md`, `test-design-wal.md`, `test-design-storage.md`, `test-design-rpc.md`, `test-design-reconfig.md` |
| `test-plan.md` — integration schedule, regression suites | `test-plan-consensus.md`, `test-plan-wal.md`, `test-plan-storage.md`, `test-plan-rpc.md`, `test-plan-reconfig.md` |

## 3. Cross-Stream Dependencies

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

The workspace at the repo root holds one library crate (`crowkv`) containing all core logic as modules, plus two binary crates (`crowkv-server`, `crowkv-bench`) and a shared dev-dependency crate (`crowkv::testkit`).

```
crowkv    (all core logic: consensus, engine, wal, io, rpc, reconfig)
  └─ crowkv-server                                 [P4] (binary, top-level integration tests)
  └─ crowkv-bench                                  [P4] (benchmark / load test binary)

crowkv::testkit  (dev-dep only; consumed by `crowkv` tests for TestTimer, TestRouter, TestNode, SimDisk)
  └─ crowkv::io (re-exports SimDisk)
```

Dependency rule: a crate may depend only on crates **above** it in this list. `crowkv::testkit` is reachable only as a `dev-dependency`, never a regular `dependency`.

**Freeze points** (must complete and be reviewed before downstream starts):
- `PxLogEntry` shape + `PxBallot`/`PxTerm` definitions — end of P1 M1
- Engine trait surface (`apply`, `get`, `scan`, `snapshot_export/import`, `compare`) — end of P1 M4
- Consensus message types (`Prepare`, `Accept`, `RequestVote`, `Heartbeat`, ...) — end of P1 M3
- gRPC `.proto` schema — end of P4 M1

## 4. Global Milestones

| ID | Name | Criteria | Phase |
|---|---|---|---|
| G1 | Core linearizable | Unit test: 3 in-process nodes, writes survive forced leader step-down | P1 |
| G2 | Persistent core | `kill -9` of leader, restart, re-elect, continue without data loss | P2 |
| G3 | Engine parity | Ordered-file engine passes same `compare()` tests as in-memory | P3 |
| G4 | Networked cluster | 3 real processes on loopback, crowbench 10k ops, zero divergence | P4 |
| G5 | Elastic membership | 3 → 5 → 7 online, rolling upgrade 1 version step | P5 |

**Gate ordering:** G1 must pass before P4 starts (P2/P3 may begin in parallel after the P1 freeze points). G2 and G3 must both pass before P4 starts (P4 needs persistent storage to do real restarts). G4 must pass before P5 starts.

## 5. Test Pairing Rule

Every phase milestone includes:
1. **Unit invariants** from the matching `test-design-*.md` (property-based or deterministic).
2. **Failure-injection** matching [`design.md`](design.md) §9 scenarios for that area.
3. **crowbench** integration test verifying end-to-end correctness (`test-design.md` §14.1) once P4 is reached.

## 6. crowtree Placeholder

Production storage backend is stubbed in P3: trait is defined, `crowtree` module exists but delegates to in-memory engine with a `todo!("crowtree integration")` in `snapshot_export`. Dedicated `plan-crowtree.md` deferred until core is stable.

## 7. Concurrency Model (Project-Wide Decision)

All public and inter-module APIs in CrowKV are `async`. The runtime is `tokio` (single-threaded `current_thread` flavor for P1 tests; multi-threaded for production from P4). This applies to every phase and every workstream.

**Rules:**

1. **No blocking calls** in any business-logic path (consensus, learner, dedup, lease, replicator, etc.).
2. **Blocking syscalls** (`fdatasync`, blocking file I/O, blocking client libs) are exposed as `async fn` via the project I/O layer ([`design-async-io.md`](design/design-async-io.md)). On Linux ≥ 5.11 this layer uses `tokio-uring`; otherwise it falls back to `tokio::task::spawn_blocking`. Callers do not branch on backend.
3. **No `std::sync::Mutex`** in async paths; use `tokio::sync::{Mutex, RwLock, Notify, mpsc, oneshot}` or, where logic is naturally serial, run inside a single owning task that receives commands via `mpsc`.
4. **No `std::thread::sleep`**; use `tokio::time::sleep` (or `TestTimer::advance` in P1 tests).
5. **Tests** run with `#[tokio::test(flavor = "current_thread", start_paused = true)]` for full determinism.

This decision supersedes any earlier per-doc TODO about sync vs async harness; the synchronous step-loop option is dropped.

## 8. Decision Log

Resolved cross-cutting design questions, kept here as an audit trail. New questions get added with `**TODO-CONFIRM:**` prefix and resolved in place (strikethrough + `**Resolved:**` note).

- ~~**TODO-CONFIRM (P1):** Lease-based linearizable reads in P1.~~ **Resolved:** Option B — deterministic lease via `TestTimer`, full state machine implemented in P1 M4 (`lease.rs`).
- ~~**TODO-CONFIRM (P1):** Synchronous step loop vs `tokio` `LocalSet` for the harness.~~ **Resolved:** `tokio` everywhere — see §7 Concurrency Model.
- ~~**TODO-CONFIRM (P1):** Single-group only in P1, or 2-group smoke test?~~ **Resolved:** include `integration_two_group_smoke` in P1 to exercise Group Manager dispatch.
- ~~**TODO-CONFIRM (P1):** Are the exact Rust type definitions in `plan-consensus.md` §3 normative or illustrative?~~ **Resolved:** normative — frozen once M1 review passes.
- ~~**TODO-CONFIRM (P2):** `criterion` for fsync benchmarks vs hand-rolled timing?~~ **Resolved:** `criterion`.
- ~~**TODO-CONFIRM (P4):** Group-0 bootstrap timing — G4 or P5?~~ **Resolved:** required for G4 (networked cluster); static topology in P4, dynamic in P5.
- ~~**TODO-CONFIRM (P5):** Rolling-upgrade testing scope.~~ **Resolved:** only consensus protocol compatibility (no WAL/snapshot version compat in test scope).

All P1–P5 plan-level decisions above are resolved. Sub-test-design docs maintain their own "Resolved Decisions" sections for area-scoped questions.
