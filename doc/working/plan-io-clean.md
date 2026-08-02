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

- [x] **T1.1** — Crash-recovery test: install `test-util` `Notify` gate
      on leader's `spawn_accept_persist`, fire `put` (returns `Chosen`
      before gated persist), kill leader, restart, assert chosen value
      is recoverable (WAL or `repair_once`). Mirrors
      `g2_crash_restart_no_data_loss_test.rs`'s `kill`/`restart`.
- [x] **T1.2** — Persist-failure test: `wal_early_ack` on, set WAL
      `failed` flag after `Chosen`, release gate so `wal.append()`
      fails (error logged), confirm value is still chosen (Paxos-safe)
      and the leader engine has the value.
- [x] **T1.3** — Flip `wal_early_ack` default to `true` in
      `CrowKVConfig::default()` (after R40); gated on `quorum > 1`
      for single-node safety (a single-node group has no survivors to
      re-drive a chosen-but-not-durable slot after a crash).
- [x] **T1.4** — No regression: existing crash-restart tests pass with
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
      (48T:48C, MI=64) on one platform. Note: `wal_flush_coalesce_us`
      is **optional**. The default (`0`) is wake-drain-flush with no
      timer — the writer parks on `rx.recv()`, drains all already-queued
      records via `try_recv` on wake, then flushes immediately. Natural
      batching already occurs because records arriving during an
      in-flight flush queue in the mpsc channel and are drained
      together on the next wake cycle. A non-zero `wal_flush_coalesce_us`
      adds an explicit bounded wait window (`min(coalesce, watchdog)`)
      to gather more records before flushing. The watchdog
      (`wal_flush_watchdog_ms`) is a safety cap on that window only —
      it does nothing when `coalesce = 0` (exists just in case of bugs).
- [ ] Measure throughput vs p99/p999 latency tradeoff; with `coalesce
      = 0` the baseline already amortizes fsync across records that
      arrive during a flush, so the question is whether an explicit
      wait window adds any measurable gain on top of wake-drain-flush.
- [ ] Decide: keep `wal_flush_coalesce_us` only if some non-zero value
      shows an obvious advantage (clear throughput gain with
      acceptable tail). If no value shows an obvious advantage, **remove
      the option entirely** — delete the config field, the coalesce
      plumbing in `pipeline_writer.rs`, the watchdog cap on the window,
      and related tests/bench columns. Document the decision and any
      results in `write-flow-analysis.md`.

**Scope**: Small — config/benchmarking first. If the option is kept,
no code change (just pick a default). If the option is removed, small
code change: drop `wal_flush_coalesce_us` **and** `wal_flush_watchdog_ms`
(the watchdog only caps the coalesce window — see `pipeline_writer.rs:249`
and `design-wal.md` "When coalescing is 0 (default), the watchdog is not
used"; it guards no other path) from `WalConfig`, `pipeline_writer.rs`,
`wal_engine.rs`, tests, and bench scripts. The wake-drain-flush
baseline stays as the only batching path.

**Files**: `tools/bench-write-*.sh` (add coalesce sweep column),
`crowkv/src/common/config.rs` (default if kept, or remove
`wal_flush_coalesce_us` + `wal_flush_watchdog_ms` if removed),
`crowkv/src/wal/pipeline_writer.rs` + `wal_engine.rs` (remove coalesce
arm + watchdog plumbing if removed),
`crowkv/tests/wal/wal_engine_tests.rs` (update if removed),
`doc/working/write-flow-analysis.md` (results + decision),
`doc/design/design-wal.md` (update if removed).

---
