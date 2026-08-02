<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R36 — Server-side Proposal Coalescing (Plan)

See `design-proposal-coalescing.md` for the design.

## Task breakdown

- [ ] T1 — `DedupTag` type + coalesce config
- [ ] T2 — Extend `AcceptRequest` proto with `repeated DedupTag dedup_tags`
- [ ] T3 — Refactor `propose` → `propose_inner(payload, &[DedupTag])`; thread tags through `run_accept_phase` / `send_accept`
- [ ] T4 — Follower `handle_accept_inner` + `learn`/`learn_chosen`/`spawn_learn_chosen` record `&[DedupTag]`
- [ ] T5 — Coalescer component on `PxGroup` (enqueue, timer flush, max_keys flush, waiter fanout)
- [ ] T6 — Wire coalesce config into group (`set_from_config`) + CLI/main
- [ ] T7 — Integration tests
- [ ] T8 — Lint + relevant tests
- [ ] T9 — Commit
- [ ] T10 — Full test-suite
- [ ] T11 — Merge design + cleanup
- [ ] T12 — Local CI check

## File-level changes

- `crowkv/src/paxos/roles.rs` — add `DedupTag`; change `Learner::learn`
  signature to `&[DedupTag]`.
- `crowkv/src/paxos/learner.rs` — `learn` impl: apply once, record each
  tag. Add `record_dedup_tags` helper.
- `crowkv/src/common/config.rs` — `PaxosConfig`: add
  `coalesce_window_us`, `coalesce_max_keys` (+ `DEFAULT`).
- `crowkv/src/rpc/proto/pxos.proto` — `DedupTag` message;
  `AcceptRequest.dedup_tags` repeated field 13.
- `crowkv/src/cluster/remote_replica.rs` — `send_accept` takes
  `&[DedupTag]`; populate `dedup_tags` + legacy first-tag.
- `crowkv/src/rpc/px_service.rs` — `handle_accept_inner`: extract
  `dedup_tags` (fallback legacy), call `learn_chosen_batch`.
- `crowkv/src/cluster/local_replica.rs` —
  `learn_chosen`/`spawn_learn_chosen` → `learn_chosen_batch`/
  `spawn_learn_chosen_batch` taking `&[DedupTag]`; restore/catch-up
  `learn` calls pass `&[]`.
- `crowkv/src/cluster/group.rs` — `propose` refactor (gate + dedup +
  coalesce-or-inner); new `propose_inner`; `run_accept_phase` takes
  `&[DedupTag]`; coalescer state + `PendingBatch` + flush; `ProposeResult:
  Clone`; `set_from_config` wires coalesce config; shutdown drops
  pending.
- `crowkv-server/src/cli.rs` — `--coalesce-window-us`,
  `--coalesce-max-keys`.
- `crowkv-server/src/main.rs` — apply CLI into `config.paxos`.
- `crowkv/tests/paxos/*.rs` (new or existing) — coalescing tests.
- `tools/bench-write-*.sh` — coalesce sweep (deferred to benchmark
  pass; not blocking correctness).
- `doc/working/write-flow-analysis.md` — record benchmark results
  (deferred to benchmark pass).

## Test checklist

- [ ] `coalesce_window_us = 0`: a paxos suite run passes unchanged.
- [ ] Coalescing on: K concurrent puts to distinct keys → same slot,
      all readable.
- [ ] Dedup on leader: retried coalesced `(client_id, seq)` → cached
      slot, no new round.
- [ ] Dedup on promoted follower: same, after leader transfer.
- [ ] Per-key ordering across batches.
- [ ] `coalesce_max_keys` caps batch; timer flush fires at
      `window_us` with < max_keys ops.
- [ ] `cargo fmt --check`, `cargo clippy -- -D warnings` clean.
