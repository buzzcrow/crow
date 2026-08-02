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

## T2 — Rust WAL CRC32C hardware path

- [ ] Replace the `crc32c` crate (0.6) in the Rust WAL with a hardware
      path: either FFI to `crow_common::crc32c` (now ISA-L
      `crc32_iscsi`, R34) or a thin `crc32_iscsi` binding.
- [ ] Verify CRC values are byte-identical (same Castagnoli polynomial
      + reflected/seeded convention) so existing WAL segments decode
      without a migration.
- [ ] Benchmark encode/replay CRC cost on x86 and ARM (the `crc32c`
      crate has an SSE4.2 path on x86 but no NEON path on ARM; ISA-L
      covers both).

**Scope**: Small — swap the checksum backend in `wal/record.rs` and
`wal/segment.rs`. Low critical-path impact (CRC is a small fraction of
WAL encode), but aligns the Rust and C++ checksum and helps ARM.

**Files**: `crowkv/src/wal/record.rs`, `crowkv/src/wal/segment.rs`,
`crowkv/Cargo.toml` (drop `crc32c` crate, add binding if FFI),
`crowtree/ffi/build.rs` (link `crowcommon` if FFI).

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

## T4 — Per-mode get latency breakdown (read G1)

- [ ] Split `kv.get.lh` into `kv.get.linearizable.lh` and
      `kv.get.min_slot.lh` in `KvMetrics`, recording each get against
      the histogram matching its `read_mode`.
- [ ] Keep the combined `kv.get.lh` for backward compat (record into
      both the per-mode and the combined histogram on each get).
- [ ] Verify the per-mode histograms surface in the metrics dump
      (`/internal-state` or equivalent); confirm linearizable shows
      the ReadIndex RTT tail and MinSlot does not.

**Scope**: Small — register two extra `LatencyHistogram`s and branch the
`record_get` call on `read_mode`. No logic change; pure instrumentation.

**Files**: `crowkv/src/rpc/kv_service.rs` (`KvMetrics` struct,
`record_get`, metric registration in `new`).

---

## T6 — `InMemKV` read/apply concurrency (read E5, low priority)

- [ ] Replace `InMemKV`'s `RwLock<BTreeMap>` with a `DashMap` (or
      sharded map) so reads proceed concurrent with `apply`.
- [ ] Verify `scan` still returns key-ordered results (`DashMap` is not
      sorted; either sort on scan or keep a secondary sorted index —
      confirm the test-only cost is acceptable).
- [ ] Confirm no test regression; `InMemKV` is test-only (not
      selectable via the server CLI), so production is unaffected.

**Scope**: Small — but low priority. `InMemKV` is test-only; the
`RwLock` only matters under heavy concurrent read+write in tests. The
prior version of `read-flow-analysis.md` incorrectly described
`InMemKV` as lock-free, which it is not; this task makes it true, or
the doc stays corrected.

**Files**: `crowkv/tests/kv/mem_kv_impl.rs` (`InMemKV` struct, `apply`,
`get`, `scan`).
