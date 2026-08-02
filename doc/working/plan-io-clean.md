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
- [x] **T1.5** (Linux bench, done) — Benchmark per-proposal latency drop;
      documented in `write-flow-analysis.md`. Results: 1T:1C +3.5%
      throughput / −3.4% avg latency; 48T:48C +7.7% throughput / −7.2%
      avg latency / −11.7% p999.

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

- [x] Sweep `wal_flush_coalesce_us` (previously default 0) across
      {0, 10, 25, 50, 100, 200} µs at a saturated write config
      (48T:48C, MI=64) on Linux. Results: throughput flat at ~29.2K
      ops/s (±1% noise), p99/p999 no trend — no non-zero value showed
      any advantage over the wake-drain-flush baseline (coalesce=0).
- [x] Measure throughput vs p99/p999 latency tradeoff; with `coalesce
      = 0` the baseline already amortizes fsync across records that
      arrive during a flush, so an explicit wait window adds no
      measurable gain on top of wake-drain-flush.
- [x] Decide on `wal_flush_coalesce_us`: **removed** — no non-zero
      value showed an obvious advantage (clear throughput gain with
      acceptable tail). Deleted the config field, the coalesce arm in
      `pipeline_writer.rs` (the `if !coalesce.is_zero()` block), and
      related tests/bench columns. `wal_flush_watchdog_ms` stays
      (guards the flush path via T3.1). Decision and results
      documented in `write-flow-analysis.md`.
- [x] **T3.1** — Wire the watchdog into the wake-drain-flush path so
      `wal_flush_watchdog_ms` is a real safety net when `coalesce = 0`
      (or after coalesce removal), not just a cap on the coalesce window.
      Currently `watchdog` is only referenced at
      `pipeline_writer.rs:249` inside `if !coalesce.is_zero()`, so with
      `coalesce = 0` it does nothing. The watchdog exists "just in
      case of bugs" — e.g. a record stuck in the queue or a missed
      wake — and should force a drain+flush within `watchdog` ms even
      when idle/no-coalesce. Small code change in `pipeline_writer.rs`
      (e.g. `timeout(watchdog, rx.recv())` or a select with an interval
      timer; on timeout, `try_recv` + flush any queued records, then
      re-park). Add a test that a queued record flushes within
      `watchdog` ms even if the normal wake is missed.

**Scope**: Small. T3.1 (watchdog wiring) is independent of the coalesce
sweep and can proceed first. The sweep is pure config/benchmarking; if
coalesce is removed, the code change is dropping the `if !coalesce.is_zero()`
block + the `coalesce` plumbing tied to it (the watchdog itself stays —
it now guards the flush path via T3.1). The wake-drain-flush baseline
stays as the only batching path.

**Files**: `tools/bench-write-*.sh` (add coalesce sweep column),
`crowkv/src/common/config.rs` (remove `wal_flush_coalesce_us` if
removed; `wal_flush_watchdog_ms` stays),
`crowkv/src/wal/pipeline_writer.rs` (T3.1 watchdog wiring; remove
coalesce arm if removed),
`crowkv/src/wal/wal_engine.rs` (drop `coalesce` plumbing if removed),
`crowkv/tests/wal/wal_engine_tests.rs` (watchdog test; update coalesce
tests if removed),
`doc/working/write-flow-analysis.md` (results + decision),
`doc/design/design-wal.md` (update watchdog description + remove
coalesce rows if removed).

---
