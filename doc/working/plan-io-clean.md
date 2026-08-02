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

- [ ] Add a fault-injection crash-recovery test: kill the leader
      between `on_accept_inner` (CAS) and `spawn_accept_persist`
      (deferred WAL append), restart, verify WAL replay does not lose
      the Paxos-chosen value and that a subsequent leader re-adopts it.
- [ ] Add a test for the window where `wal_early_ack` is on and the
      local persist fails (background task logs the error): confirm the
      value is still chosen (Paxos-safe) and that repair `repair_once`
      re-drives the slot if needed.
- [ ] Once tests pass, flip `wal_early_ack` default to true and
      benchmark the per-proposal latency drop (local fsync removed from
      the critical path; critical path becomes `quorum RPC` only).

**Scope**: Medium — the R16b mechanism is implemented and merged
(`on_accept_inner` / `on_accept_persist` / `spawn_accept_persist`); this
is verification + default-flip, not new design. The work is the
fault-injection harness (process kill + restart + WAL replay assertion).

**Dependency**: R35 carries `wal_early_ack` across group rebuild in
`mgmt_api` (shared with R17's `async_engine_apply`). T1's own work
(crash tests + default flip) is independent of R35's apply fence; R16b's
enable is gated on T1, not R35.

**Files**: `crowkv/tests/` (new crash-recovery test),
`crowkv/src/cluster/group.rs` (`wal_early_ack` default),
`crowkv/src/cluster/local_replica.rs` (no code change expected).

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
