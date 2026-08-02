<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# I/O-Path Cleanup & Tuning Tasks

**Override:** This file is **persistent** — it is not deleted after a
task is complete. Only completed tasks are removed; the file itself
remains as the ongoing I/O-path (read + write) cleanup/tuning backlog.
This overrides the `/implement-requirement` workflow's cleanup step
which would normally delete `plan-<topic>.md`.

Small-scope read- and write-path tasks traced here for later
implementation. Each has a checkbox. Larger changes live in the
backlog (R35 apply fence, R36 proposal coalescing, R37 scan
`start_after` push-down, R38 scan value zero-copy, R39 read-endpoint
policy). See [`write-flow-analysis.md`](write-flow-analysis.md) §
Write-Path Enhancement Ideas and
[`read-flow-analysis.md`](read-flow-analysis.md) § Gaps and
Optimization Opportunities for the full lists and rationale.

---

## T1 — Crash-recovery hardening for R16b (enable `wal_early_ack`)

Design: [`design-t1-crash-recovery.md`](design-t1-crash-recovery.md).
Decisions: (1) test-only `Notify` gate on `spawn_accept_persist` to
deterministically hit the CAS→persist window (mirrors
`readindex_round_gate` pattern); (2) `wal_early_ack` becomes a field
in the new `CrowKVConfig` (R40 prerequisite), default flips to `true`
in `CrowKVConfig::default()` after tests pass.

- [ ] **T1.1** — Crash-recovery test: install `test-util` `Notify` gate
      on leader's `spawn_accept_persist`, fire `put` (returns `Chosen`
      before gated persist), kill leader, restart, assert chosen value
      is recoverable (WAL or `repair_once`). Mirrors
      `g2_crash_restart_no_data_loss_test.rs`'s `kill`/`restart`.
- [ ] **T1.2** — Persist-failure test: `wal_early_ack` on, `chmod 000`
      the WAL dir after `Chosen`, confirm value is still chosen
      (Paxos-safe) and `repair_once` re-drives the slot.
- [ ] **T1.3** — Flip `wal_early_ack` default to `true` in
      `CrowKVConfig::default()` (after R40); verify rebuild carries
      the config object across `mgmt_api` rebuild.
- [ ] **T1.4** — No regression: existing crash-restart tests pass with
      the new default.
- [ ] **T1.5** (deferred to Linux bench) — Benchmark per-proposal
      latency drop; document in `write-flow-analysis.md`.

**Scope**: Medium — the R16b mechanism is implemented and merged; this
is verification + default-flip. The fault-injection harness exists
(`g2`'s `kill`/`restart`); the new work is the `test-util` `Notify`
gate + two test files + the default flip in `CrowKVConfig`.

**Dependency**: **R40** (prerequisite for T1.3) — `CrowKVConfig`
refactor. T1.1 + T1.2 (crash tests) can proceed in parallel with R40
(the `Notify` gate is independent of the config struct). R35 shares
the rebuild-carry pattern (both flags become `CrowKVConfig` fields
after R40). T1.5 (benchmark) is platform-gated (Linux); T1.1–T1.4 are
not.

**Files**: `crowkv/src/cluster/local_replica.rs` (`test-util` `Notify`
gate), `crowkv/src/common/config.rs` (default flip in
`CrowKVConfig::default()` after R40), `crowkv-server/src/mgmt_api.rs`
(verify rebuild-carry), `crowkv/tests/group/` (new test files),
`doc/working/write-flow-analysis.md` (T1.5 results, deferred).

---

## T3 — WAL group-commit coalesce tuning

- [ ] Sweep `wal_flush_coalesce_us` (currently default 0) across
      {0, 10, 25, 50, 100, 200} µs at a saturated write config
      (48T:48C, MI=64) on one platform.
- [ ] Measure throughput vs p99/p999 latency tradeoff; the coalesce
      window lets more in-flight proposals land in one `writev` +
      `fdatasync`, amortizing fsync cost.
- [ ] Pick a default (likely 0 if no win, or the smallest value with a
      measurable throughput gain and acceptable tail); document in
      `write-flow-analysis.md`.

**Scope**: Small — pure config/benchmarking, no code change. The
coalesce mechanism already exists in the WAL writer task
(`wal_flush_coalesce_us`).

**Files**: `tools/bench-write-*.sh` (add coalesce sweep column),
`crowkv/src/wal/` (default constant only if changed),
`doc/working/write-flow-analysis.md` (results).

---
