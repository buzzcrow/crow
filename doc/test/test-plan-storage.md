# CrowKV - Test Plan: Storage Engine

Depends on: [`test-plan.md`](test/test-plan.md), [`test-design-storage.md`](test/test-design-storage.md), [`plan-storage.md`](plan/plan-storage.md)
Satisfies: [requirement.md §8.3](requirement.md#83-learner-storage)

Execution plan for Phase 3 storage engine tests.

## 1. Milestone Test Gates

| Plan Milestone | Test Set | Gate |
|---|---|---|
| M1 — Trait definition | Compile-time trait impl check | Pass |
| M2 — Ordered-file backend | `ordered_file.rs` unit + `compare` with in-memory | All pass |
| M3 — Snapshot round-trip | `snapshot.rs` unit | All pass |
| M4 — crowtree placeholder | `#[ignore]` crowtree tests compile | Pass |
| **G3 — Engine parity** | All backends pass same test matrix | **All pass; gates start of P4 (P2 runs in parallel with P3)** |

## 2. Test Commands

```bash
cargo test --lib engine
cargo test --test engine_cross_compare
```

## 3. Backend Matrix

| Test | In-memory | Ordered-file | crowtree |
|---|---|---|---|
| apply_get | required | required | #[ignore] |
| apply_idempotent | required | required | #[ignore] |
| scan_no_tombstones | required | required | #[ignore] |
| compare_equal | required | required | #[ignore] |
| snapshot_roundtrip | required | required | #[ignore] |
