# CrowKV - Test Design & Plan

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md), [`plan.md`](plan.md)
Satisfies: [requirement.md §14 Testing Requirements](requirement.md#14-testing-requirements)

This document defines the overall test strategy, invariant framework, failure taxonomy, execution schedule, and regression suites. Per-area details (consensus, WAL, storage, RPC, reconfig) are outlined here; create a temporary detailed test plan before implementing each milestone if needed.

> **Note:** sub-topic test files (`test-design-*.md`, `test-plan-*.md`) have been merged into this outline.

---

## Table of Contents

- [1. Test Pyramid](#1-test-pyramid)
- [2. Invariant Framework](#2-invariant-framework)
- [3. Failure Injection Taxonomy](#3-failure-injection-taxonomy)
- [4. Milestone Test Gates](#4-milestone-test-gates)
- [5. Regression Suites](#5-regression-suites)
- [6. CI Pipeline](#6-ci-pipeline)
- [7. crowbench Architecture](#7-crowbench-architecture)
- [8. Per-Area Test Outlines](#8-per-area-test-outlines)

---

## 1. Test Pyramid

| Level | Purpose | Tooling | Frequency |
|---|---|---|---|
| Unit | Module invariants, state-machine transitions | `cargo test --lib` | Every build |
| Integration | Multi-module correctness under failure | `testkit` harness, deterministic simulation | Every build |
| crowbench | End-to-end load + linearizability check | Custom binary, property-based workload | CI, pre-release |
| Manual / Jepsen-style | Long-running chaos, partition healing | Docker cluster, `tc` / `iptables` | Weekly |

---

## 2. Invariant Framework

Every testable claim is stated as an **invariant** with:
- **Trigger:** when it is checked
- **Precondition:** required system state
- **Assertion:** what must hold
- **Ref:** upstream design doc section

### 2.1 Consensus Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| C1 | At most one leader per term | After every `RequestVote` / heartbeat | [`design-leader-election.md`](design/design-leader-election.md) §9.1 |
| C2 | Ballot monotonic per slot | Every `Prepare`/`Accept` handler | [`design.md`](design.md) §4.2 |
| C3 | Chosen value immutable | Slot transitions to `Chosen` | [`design-leader-election.md`](design/design-leader-election.md) §9.2 |
| C4 | Slot order = real-time ack order | Every client write ack | [`design-parallel-slots.md`](design/design-parallel-slots.md) §2 I1 |
| C5 | Safe-slot contiguous | Every heartbeat aggregation | [`design-parallel-slots.md`](design/design-parallel-slots.md) §7 |
| C6 | Per-key resolved-slot monotone | Every `apply` | [`design-parallel-slots.md`](design/design-parallel-slots.md) §6 |
| C7 | Dedup cache idempotent | Every write request | [`design.md`](design.md) §8.6 |
| C8 | Liveness under quorum | Heartbeats + write attempts | [`design-leader-election.md`](design/design-leader-election.md) §3 |
| C9 | Window backpressure bounded | Every admission decision | [`design-parallel-slots.md`](design/design-parallel-slots.md) §4 |
| C10 | Lease never overlaps | Two leaders coexist in time | [`design-leader-election.md`](design/design-leader-election.md) §6 |
| C11 | Lease-invalid → ReadIndex fallback | Linearizable read without valid lease | [`design-leader-election.md`](design/design-leader-election.md) §7 |
| C12 | Lease-unrenewable → step-down | Leader cannot reach quorum to renew | [`design-leader-election.md`](design/design-leader-election.md) §8 |

### 2.2 WAL Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| W1 | Ack only after fsync | `Accepted` response sent | [`design-wal.md`](design/design-wal.md) §5.1 |
| W2 | Replay deterministic | Startup segment walk | [`design-wal.md`](design/design-wal.md) §6 |
| W3 | CRC failure truncates local | Bad CRC during replay | [`design-wal.md`](design/design-wal.md) §6.2 |
| W4 | GC only below both watermarks | Segment unlink | [`design-wal.md`](design/design-wal.md) §7 |
| W5 | Multi-disk parallel fsync | Aggregate IOPS measurement | [`design-wal.md`](design/design-wal.md) §3 |
| W6 | Disk loss → fail-out, not partial | fsync error | [`design-wal.md`](design/design-wal.md) §8.1 |

### 2.3 Storage Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| S1 | Apply atomic | Every `apply(slot, batch)` | [`design-storage-engine.md`](design/design-storage-engine.md) §4.3 |
| S2 | Per-key resolved-slot monotone | Every `apply` | [`design-storage-engine.md`](design/design-storage-engine.md) §3.3 |
| S3 | Compare logical not byte-level | `compare(other)` | [`design-storage-engine.md`](design/design-storage-engine.md) §8 |
| S4 | Snapshot round-trip equal | `export` then `import` | [`design-storage-engine.md`](design/design-storage-engine.md) §6 |

### 2.4 Reconfiguration Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| RC1 | Joint config both-quorum | Every decision during joint | [`design-reconfiguration.md`](design/design-reconfiguration.md) §2 |
| RC2 | New member catch-up before voting | `ConfigChange(C_new)` proposed | [`design-reconfiguration.md`](design/design-reconfiguration.md) §4 |
| RC3 | No split-brain during transition | Any two quorums intersect | [`design-reconfiguration.md`](design/design-reconfiguration.md) §8 |
| RC4 | Rolling upgrade one version step | Mixed-version cluster | [`requirement.md`](requirement.md) §9.2 |

---

## 3. Failure Injection Taxonomy

Harness method names are normative; any API change must update both code and this doc.

| Failure | Unit sim (P1–P3 via `testkit`) | crowbench (P4+) | Jepsen-style (manual) |
|---|---|---|---|
| Node crash | `TestNode::crash()` | `SIGKILL` container | `kill -9` process |
| Node restart | `TestNode::restart()` | restart container | restart process |
| Network partition | `TestRouter::partition(set_a, set_b)` / `heal()` | Docker network isolate | `tc` / `iptables` |
| Message delay | `TestRouter::delay(from, to, ms)` | `tc qdisc` | `tc` |
| Message loss | `TestRouter::drop(from, to, pct)` | `tc` loss emulator | `iptables` drop |
| Disk full | (P2+) `TestDisk::set_full()` | loopback FS size limit | loopback FS size limit |
| Clock skew | `TestTimer::skew(node, ms)` | `libfaketime` | `libfaketime` |
| Forced step-down | `TestNode::force_step_down()` | admin RPC | admin RPC |

---

## 4. Milestone Test Gates

| Phase | Milestone | Test Set | Runner | Gate |
|---|---|---|---|---|
| P1 | M1 — Core types + acceptor | `acceptor.rs` unit + C2 invariant | `cargo test --lib` | All pass |
| P1 | M2 — Wire + Paxos flows | `rpc` unit + S0-A/B/C/D loopback | `cargo test --test rpc_paxos` | All pass |
| P1 | M3 — Election + bulk Phase 1 | `election.rs` unit + S1 scenario | `cargo test` | All pass |
| P1 | M4 — Proposer + pipeline | `proposer.rs` + `repair.rs` + S2 | `cargo test` | All pass |
| P1 | M5 — Learner + engine + lease | `learner.rs` + `engine.rs` + `lease.rs` + S3 + C10–C12 | `cargo test` | All pass |
| P1 | M6 — Dedup cache | `dedup.rs` unit + C7 | `cargo test --lib` | All pass |
| P1 | **G1 — Core linearizable** | All M1–M6 + 1000 random ops | `cargo test` | **All pass; gates P4 start** |
| P2 | M1–M5 — WAL segments → GC | `wal` unit + crash recovery script | `cargo test --lib` + script | All pass |
| P2 | **G2 — Persistent core** | All WAL + `kill9_recovery` | `cargo test` + script | **All pass; gates P4 start** |
| P3 | M1–M4 — Engine trait → crowtree | `engine` unit + cross-compare | `cargo test --lib` | All pass |
| P3 | **G3 — Engine parity** | All backends same test matrix | `cargo test` | **All pass; gates P4 start** |
| P4 | M1–M4 — RPC / client / reads | protobuf roundtrip + loopback + crowbench | `cargo test` + `cargo run --bin crowbench` | All pass |
| P4 | **G4 — Networked cluster** | crowbench 10k ops zero divergence | `cargo run --bin crowbench` | **All pass** |
| P5 | M1–M4 — Reconfig / upgrade | snapshot install + joint + rolling upgrade | `cargo test --test reconfig` | All pass |
| P5 | **G5 — Elastic membership** | crowbench during reconfig | `cargo run --bin crowbench` | **All pass; release gate** |

---

## 5. Regression Suites

**Suite A — Unit (`cargo test --lib`):**
- All per-module unit tests (acceptor, election, proposer, repair, learner, engine, lease, dedup, wal, rpc).
- Duration target: < 30 s.

**Suite B — Integration (`cargo test --test '*'`):**
- `testkit` scenarios from all areas.
- Deterministic seeds, reproducible.
- Duration target: < 5 min.

**Suite C — crowbench (`cargo run --bin crowbench`):**
- Random workload + failure injection, property-based.
- Run nightly on CI, pre-release manually.
- Duration target: 30 min.
- Available from P4 onward.

---

## 6. CI Pipeline

```
PR      → Suite A (must pass)
Merge   → Suite A + Suite B (must pass)
Nightly → Suite A + Suite B + Suite C (must pass)
Release → All suites + manual Jepsen-style run (1 h)
```

---

## 7. crowbench Architecture

`crowbench` is the integration correctness harness:
- **Generator:** produces random workloads (key distribution, op mix, batch sizes, read modes)
- **Injector:** applies failures (node kill, partition, delay, drop) at deterministic seeds
- **Driver:** runs the cluster, sends ops, records a trace of `(invoke, ack, result)`
- **Checker:** verifies one of:
  - **Same-state:** all learners have identical `(slot, value)` per key via `engine.compare()`
  - **Controlled-order:** for a single-threaded workload, ack order = slot order
  - **Linearizability:** full Lamport-history check (future extension)

crowbench is required only from Phase 4 onward. In-memory tests use `testkit` + `compare()`.

---

## 8. Per-Area Test Outlines

### 8.1 Consensus

**Unit tests:** `prepare_promise`, `reject_lower_ballot`, `accept_after_promise`, `elect_single_leader`, `step_down_higher_term`, `bulk_phase1_adopts`, `parallel_window`, `quorum_bitmap`, `gap_repair_noop`, `gap_repair_preserve`, `apply_out_of_order`, `contiguous_applied`, `repeat_same_seq`, `valid_lease_fast_read`, `expired_lease_readindex`, `unrenewable_step_down`, `clock_skew_bound`, `compare_equal`, `compare_divergent`.

**Integration scenarios (S0–S3):**
- **S0-A** — Classic Paxos (3-node, full Phase 1+2)
- **S0-B** — Optimized Paxos (3-node, skip Phase 1)
- **S0-C** — Multi-Paxos (5-node, 10 slots, ballot reuse)
- **S0-D** — Quorum with rejection (5-node, pre-promised higher ballot)
- **S1** — Leader change mid-write (bulk Phase 1 adopts in-flight value)
- **S2** — Parallel slots with gap (window=16, drop slot 5, repair resolves)
- **S3** — Partition minority (majority continues, minority catches up on heal)

### 8.2 WAL

**Unit tests:** `write_read_roundtrip`, `crc_failure_truncate`, `batch_coalesce`, `multi_disk_round_robin`, `kill9_recovery`.

**Integration scenarios:**
- **S-W1** — 1000-record crash recovery (last batch unfsynced)
- **S-W2** — Multi-disk throughput scaling (1/2/4 disks)
- **S-W3** — CRC corruption survival (truncation + later segments preserved)

### 8.3 Storage Engine

**Unit tests:** `apply_get`, `apply_idempotent`, `scan_no_tombstones`, `compare_with_in_memory`, `export_import_roundtrip`.

**Integration scenarios:**
- **S-S1** — Cross-engine equivalence (in-memory vs ordered-file after 1000 ops)
- **S-S2** — Snapshot resume (interrupt at 60 MiB, resume, `compare()` equal)

**Backends:** In-memory (P1), Ordered-file (P3), crowtree placeholder (P3, `#[ignore]`).

### 8.4 RPC / Client

**Unit tests:** `protobuf_roundtrip`, `client_retry`, `read_mode_routing`.

**Integration scenarios:** 3-node loopback, leader change retry, mixed follower-read workload.

### 8.5 Reconfiguration

**Unit tests:** `joint_quorum_requirement`, `catchup_before_voting`, `leader_transfer_before_removal`.

**Integration scenarios:**
- **S-RC1** — 3 → 5 online (zero write unavailability)
- **S-RC2** — 5 → 3 online (leader removal forces transfer)
- **S-RC3** — Rolling upgrade (mixed-version traffic)
- **S-RC4** — Failed catch-up rollback (timeout → revert to `C_old`)

---

## 9. Test Commands

```bash
# Suite A — unit only
cargo test --lib

# Suite B — integration only
cargo test --test '*'

# Suite A+B — full gate
cargo test

# WAL benchmarks
cargo bench -- wal_throughput

# crowbench (P4+)
cargo run --bin crowbench -- --ops 10000 --nodes 3
cargo run --bin crowbench -- --scenario reconfig_3_to_5
```
