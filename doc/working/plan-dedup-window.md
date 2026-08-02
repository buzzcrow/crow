<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R41 Plan — Bounded per-client dedup window

Companion design: `design-dedup-window.md`. Backlog: `R41-dedup-window.md`.

## Tasks

- [ ] Replace `DedupEntry` with `DedupWindow` in `paxos/learner.rs`
      — `VecDeque<(u64, SlotIndex)>` cap 64, plus a `const DEDUP_WINDOW: usize = 64`.
- [ ] Rewrite `dedup_lookup` — exact-match scan, `client_id == 0`
      sentinel short-circuit unchanged.
- [ ] Rewrite `record_dedup` — append (skip if `seq` already present),
      evict oldest when len > 64.
- [ ] Flip 4 existing assertions (2 in `dedup_test.rs`, 2 in
      `learner_dedup_test.rs`) to expect `None` for unrecorded lower
      seq.
- [ ] Add `dedup_does_not_false_positive_on_out_of_order_higher_seq`
      to `dedup_test.rs`.
- [ ] Add `dedup_retains_at_least_64_requests_per_client` to
      `dedup_test.rs`.
- [ ] Update `design.md` §10 Idempotency retention wording.
- [ ] Run `pixi run test-core` (dedup tests) — fix until green.
- [ ] Commit (impl + design + plan + R41 doc fixes + backlog fix).
- [ ] Run full test suite (`pixi run test-suite`).
- [ ] Merge design into `design.md` §10; delete working docs and
      `R41-dedup-window.md`; remove R41 entry from `backlog.md`.
- [ ] Local CI: `pixi run cargo fmt --all -- --check`,
      `pixi run cargo clippy --all-targets -- -D warnings`,
      `pixi run test-ct`, `pixi run test-ffi`, `pixi run test-core`.

## Files

- `crowkv/src/paxos/learner.rs` — dedup structure + lookup/record.
- `crowkv/tests/replica/dedup_test.rs` — flip + add.
- `crowkv/tests/paxos/learner_dedup_test.rs` — flip.
- `doc/design/design.md` §10 — retention wording.
- `doc/backlog/R41-dedup-window.md` — already corrected (bench
  framing + full affected-tests list); deleted in cleanup step.
- `doc/backlog/backlog.md` — R41 entry already corrected; removed in
  cleanup step.
