# CrowKV - Test Design: Master Strategy

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md)
Satisfies: [requirement.md §14 Testing Requirements](requirement.md#14-testing-requirements)

This document defines the overall test strategy, invariant framework, and crowbench architecture. Deep dives for each workstream live in sibling sub-topic docs (`test-design-consensus.md`, `test-design-wal.md`, `test-design-storage.md`, `test-design-rpc.md`, `test-design-reconfig.md`).

## Table of Contents

- [1. Test Pyramid](#1-test-pyramid)
- [2. Invariant Framework](#2-invariant-framework)
- [3. crowbench Architecture](#3-crowbench-architecture)
- [4. Failure Injection Taxonomy](#4-failure-injection-taxonomy)
- [5. Test-Doc Pairing](#5-test-doc-pairing)

---

## 1. Test Pyramid

| Level | Purpose | Tooling | Frequency |
|---|---|---|---|
| Unit | Module invariants, state-machine transitions | `cargo test` in-process | Every build |
| Integration | Multi-module correctness under failure | `test_harness`, deterministic simulation | Every build |
| crowbench | End-to-end load + linearizability check | Custom binary, property-based workload | CI, pre-release |
| Manual / Jepsen-style | Long-running chaos, partition healing | Docker cluster, `tc` / `iptables` | Weekly |

## 2. Invariant Framework

Every testable claim is stated as an **invariant** with:
- **Trigger:** when it is checked
- **Precondition:** required system state
- **Assertion:** what must hold
- **Ref:** upstream design doc section

Example (consensus):
> **Invariant C1:** At most one leader per term. Checked after every `RequestVote` response. Precondition: quorum exists. Assertion: no two nodes report `is_leader && same_term`. Ref: [`design-leader-election.md`](design/design-leader-election.md) §9.1.

Concrete invariants are listed in sub-topic `test-design-*.md` files.

## 3. crowbench Architecture

`crowbench` is the integration correctness harness:
- **Generator:** produces random workloads (key distribution, op mix, batch sizes, read modes)
- **Injector:** applies failures (node kill, partition, delay, drop) at deterministic seeds
- **Driver:** runs the cluster, sends ops, records a trace of `(invoke, ack, result)`
- **Checker:** verifies one of:
  - **Same-state:** all learners have identical `(slot, value)` per key via `engine.compare()`
  - **Controlled-order:** for a single-threaded workload, ack order = slot order
  - **Linearizability:** full Lamport-history check (future extension)

 crowbench is required only from Phase 4 onward (networked cluster). In-memory tests use `test_harness` + `compare()`.

## 4. Failure Injection Taxonomy

Harness method names below are normative and must match `plan-consensus.md` §4. Any change to the harness API requires updating both docs in the same change.

| Failure | Unit sim (P1–P3 via `test_harness`) | crowbench (P4+) | Jepsen-style (manual) |
|---|---|---|---|
| Node crash | `TestNode::crash()` | `SIGKILL` container | `kill -9` process |
| Node restart | `TestNode::restart()` | restart container | restart process |
| Network partition | `TestRouter::partition(set_a, set_b)` / `heal()` | Docker network isolate | `tc` / `iptables` |
| Message delay | `TestRouter::delay(from, to, ms)` | `tc qdisc` | `tc` |
| Message loss | `TestRouter::drop(from, to, pct)` | `tc` loss emulator | `iptables` drop |
| Disk full | (P2+) `TestDisk::set_full()` on simulated disk | loopback FS size limit | loopback FS size limit |
| Clock skew | `TestTimer::skew(node, ms)` | `libfaketime` | `libfaketime` |
| Forced step-down | `TestNode::force_step_down()` | admin RPC | admin RPC |

## 5. Test-Doc Pairing

| Sub-test-design | Covers | Paired plan | Paired test-plan |
|---|---|---|---|
| `test-design-consensus.md` | Leader election, parallel slots, gap repair, linearizability | `plan-consensus.md` | `test-plan-consensus.md` |
| `test-design-wal.md` | Fsync contract, replay determinism, CRC, truncation | `plan-wal.md` | `test-plan-wal.md` |
| `test-design-storage.md` | Engine trait, `compare()`, snapshot round-trip | `plan-storage.md` | `test-plan-storage.md` |
| `test-design-rpc.md` | Wire compatibility, client retry, idempotency | `plan-rpc.md` | `test-plan-rpc.md` |
| `test-design-reconfig.md` | Joint consensus, snapshot install, rolling upgrade | `plan-reconfig.md` | `test-plan-reconfig.md` |