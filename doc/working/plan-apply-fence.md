<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R35 — Apply Fence: Implementation Plan

Reference: `doc/backlog/R35-apply-fence.md`, design at
`doc/working/design-apply-fence.md`.

## Tasks

- [ ] T1 — `PxLearner` write-side split + apply-fence primitive
      - Split `update_frontier` into `update_chosen_frontier(slot, term)`
        (advances `contiguous_chosen` + `last_chosen_*` + chosen out-of-order
        drain; NO `contiguous_applied`) and `advance_applied_frontier(slot)`
        (advances `contiguous_applied` + applied out-of-order drain +
        `notify_waiters`).
      - Add `applied_out_of_order: Mutex<BTreeMap<SlotIndex, ()>>` field
        (construct in `default`, `with_engine`).
      - Add `apply_notify: tokio::sync::Notify` field (construct in `default`,
        `with_engine`).
      - `Learner::learn` (V1 sync): `apply_entry` → `update_chosen_frontier`
        → `advance_applied_frontier` → `record_dedup`.
      - Make `update_chosen_frontier`, `advance_applied_frontier`,
        `apply_entry`, `record_dedup` `pub(crate)` so `spawn_learn_chosen`
        can use them.
      - Add `pub async fn await_applied(&self, slot)`: register-before-load
        loop.
- [ ] T2 — `spawn_learn_chosen` write-side split + `await_apply_fence`
      - `spawn_learn_chosen`: `update_chosen_frontier` + `record_dedup`
        synchronously, then spawn `apply_entry` + `advance_applied_frontier`.
      - Add `PxLocalReplica::await_apply_fence(slot)` delegating to learner.
- [ ] T3 — Wire fence into Linearizable read path
      - In `resolve_read_point` (`px_kv_store.rs`), Linearizable `Ready`
        arm: `replica.await_apply_fence(read_slot).await` before constructing
        `ReadDecision::Serve`. Observe the fence latency via the new metric.
- [ ] T4 — `apply_fence` latency metric
      - Add `apply_fence: Arc<LatencySummary>` to `ReadRegistryHandles`;
        register in `set_metrics_registry`; observe in the fence site.
- [ ] T5 — Flip `async_engine_apply` default
      - `CrowKVConfig::default()`: `async_engine_apply: true`.
      - `CrowKVConfig::for_tests()`: explicit `async_engine_apply: false`.
      - `PxGroup::new` test path: explicit `async_engine_apply: false`
        (alongside the existing `wal_early_ack: false`).
- [ ] T6 — R35 read-your-writes test
      - New test with `set_async_engine_apply(true)`: put then linearizable
        get of the same key on the leader returns the written value. Use the
        testkit single-leader cluster pattern.
- [ ] T7 — Pre-commit quality gate
      - `cargo fmt --check`, `cargo clippy -- -D warnings`, relevant tests.
- [ ] T8 — Commit implementation + working docs.
- [ ] T9 — Full test suite (`pixi run test-suite`).
- [ ] T10 — Merge design into formal design doc; delete working docs and
      `R35-apply-fence.md`; remove backlog index entry.
- [ ] T11 — Local CI check (fmt, clippy, test-ct/ffi/core).

## File list

- `crowkv/src/paxos/learner.rs` (T1)
- `crowkv/src/cluster/local_replica.rs` (T2)
- `crowkv/src/cluster/px_kv_store.rs` (T3)
- `crowkv/src/cluster/group.rs` (T4, T5)
- `crowkv/src/common/config.rs` (T5)
- `crowkv/tests/store/apply_fence_test.rs` (T6, new)

## Test checklist

- [ ] New R35 test passes (read-your-writes with R17 on).
- [ ] `readindex_batch_test` passes (batching composes with fence).
- [ ] `election_test` / `lease_test` pass (lease fast path unaffected).
- [ ] `startup_test` passes (config default flip + restore).
- [ ] `proposer_test` / `maintenance_test` pass.
- [ ] `cargo clippy --all-targets -- -D warnings` clean.
