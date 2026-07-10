# CrowKV - Test Plan: Master Integration Schedule

Depends on: [`plan.md`](plan.md), [`test-design.md`](test-design.md)
Satisfies: [requirement.md §14 Testing Requirements](requirement.md#14-testing-requirements)

This document maps test execution to plan milestones and defines regression suites. Deep dives per workstream live in sibling sub-topic docs (`test-plan-consensus.md`, `test-plan-wal.md`, `test-plan-storage.md`, `test-plan-rpc.md`, `test-plan-reconfig.md`).

## Table of Contents

- [1. Milestone-Test Mapping](#1-milestone-test-mapping)
- [2. Regression Suites](#2-regression-suites)
- [3. CI Pipeline](#3-ci-pipeline)

---

## 1. Milestone-Test Mapping

| Global Milestone | Triggered Tests | Runner | Gate |
|---|---|---|---|
| G1 — Core linearizable | `test-plan-consensus.md` unit + integration | `cargo test` | All pass before P4 start (P2/P3 may begin after the P1 freeze points) |
| G2 — Persistent core | `test-plan-wal.md` replay + crash | `cargo test` + script | All pass before P4 start |
| G3 — Engine parity | `test-plan-storage.md` compare + snapshot | `cargo test` | All pass before P4 start |
| G4 — Networked cluster | `test-plan-rpc.md` + crowbench 10k ops | `cargo test` + `cargo run --bin crowbench` | All pass before P5 start |
| G5 — Elastic membership | `test-plan-reconfig.md` + crowbench reconfig | `cargo test` + `cargo run --bin crowbench` | Release gate |

## 2. Regression Suites

**Suite A — Unit (`cargo test --lib`):**
- All `test-design-consensus.md` unit tests.
- All `test-design-wal.md` unit tests (from P2 onward).
- All `test-design-storage.md` unit tests (from P3 onward).
- Duration target: < 30 s.

**Suite B — Integration (`cargo test --test integration`):**
- `test_harness` scenarios from all `test-design-*.md` files.
- Deterministic seeds, reproducible.
- Duration target: < 5 min.

**Suite C — crowbench (`cargo run --bin crowbench`):**
- Random workload + failure injection, property-based.
- Run nightly on CI, pre-release manually.
- Duration target: 30 min.
- Available from P4 onward (needs networked cluster). Before P4, the equivalent role is played by `test_harness`-driven integration scenarios in Suite B.

## 3. CI Pipeline

```
PR → Suite A (must pass)
Merge → Suite A + Suite B (must pass)
Nightly → Suite A + Suite B + Suite C (must pass)
Release → All suites + manual Jepsen-style run (1 h)
```