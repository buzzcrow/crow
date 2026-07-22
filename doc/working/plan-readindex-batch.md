<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan — ReadIndex batching (R27)

Tracks implementation progress for R27. Design:
[`design-readindex-batch.md`](design-readindex-batch.md).

## Tasks

- [x] T1 — `ReadBarrierOutcome` gains `Clone` (`group_election.rs`).
- [x] T2 — `PendingReadBarrier` struct + `pending_read_barrier` field on
      `PxGroup` (`group.rs`); initialize in `PxGroup::new`.
- [x] T3 — `readindex_rounds` counter on `ReadRegistryHandles` +
      registration in `set_metrics_registry` (`group.rs`).
- [x] T4 — Rewrite ReadIndex branch of `linearizable_read_barrier`
      (`group_election.rs`): queue-join path + round-leader drain +
      waiter fan-out; inc `readindex_rounds` once per round.
- [x] T5 — Test-only round gate (`test-util`): field on `PxGroup` +
      `set_readindex_round_gate` / `take` accessor + waiter-count
      accessor.
- [x] T6 — Integration test `crowkv/tests/store/readindex_batch_test.rs`:
      success batching (single-voter + gate) + no-quorum fan-out
      (3-member dead remotes + gate).
- [x] T7 — Run `pixi run test-core`; fix lint/clippy.

## Files

- `crowkv/src/cluster/group.rs` — struct, field, metric handle,
  registration, test gate.
- `crowkv/src/cluster/group_election.rs` — `ReadBarrierOutcome::Clone`,
  `linearizable_read_barrier` ReadIndex branch.
- `crowkv/tests/store/readindex_batch_test.rs` — new.

## Test checklist

- [ ] N concurrent linearizable gets, expired lease, gated round → all
      `Ready`, same `read_slot`, `readindex_rounds.c == 1`,
      `readindex_path.c == N`.
- [ ] N concurrent linearizable gets, no-quorum round → all
      `Unavailable`, one round.
- [ ] R19 `read_metrics_test.rs` still passes (lease_path +
      readindex_path == get count; barrier.l count).
- [ ] `cargo fmt --check`, `cargo clippy -- -D warnings` clean.

## Dependency ordering

T1 → T2 → T3 → T4 → T5 → T6 → T7. T1/T2/T3 are independent field/struct
additions; T4 depends on all three; T5 depends on T2; T6 depends on
T4/T5.
