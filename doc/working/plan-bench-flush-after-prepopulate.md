<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R47 Plan — Bench flush-after-prepopulate flag

## Tasks

- [ ] Add `POST /stores/{sid}/groups/{gid}/flush` mgmt API endpoint
  - `FlushResult` schema, `flush_group` handler, route, OpenAPI reg
- [ ] Add `ServerClient::flush(sid, gid)` HTTP client method
- [ ] Add `flush_after_prepopulate` + `flush_mgmt_urls` to `BenchConfig`;
  drain L0 after pre-pop in `run_bench`
- [ ] Add `--flush-after-prepopulate` CLI flag; wire fixture mgmt URLs
- [ ] Add flushed value-size variants to `tools/bench-scan-regression.sh`
- [ ] Run the flushed vs unflushed value-size bench; verify the gap closes
- [ ] Update `scan-perf-baseline.md` + `kv-scan-flow-analysis.md` with
  results
- [ ] Add a management-API integration test for the flush endpoint
- [ ] Lint + relevant tests pass (`cargo fmt --check`, `cargo clippy --
  -D warnings`, `test-core`, `test-server`, `test-cli`)
- [ ] Commit (impl + design + plan docs)
- [ ] Full test suite; fix any regressions
- [ ] Merge design into `design/kv/design-crow-kv-server.md` (mgmt API)
  and `design/kv/design-crow-kv-test.md` (bench layer); delete working
  docs
- [ ] Delete `R47-bench-flush-after-prepopulate.md` + backlog index entry
- [ ] Local CI check (fmt, clippy, test-ct, test-ffi, test-core)

## Files

- `app/crow-kv-server/src/mgmt_api.rs` — flush route + handler + schema
  + OpenAPI reg
- `lib/crow-console-shared/src/clients/http.rs` — `ServerClient::flush`
- `app/crow-cli/src/bench/runner.rs` — `BenchConfig` fields + drain
  logic in `run_bench`
- `app/crow-cli/src/commands/bench.rs` — `--flush-after-prepopulate`
  flag + wiring
- `tools/bench-scan-regression.sh` — flushed value-size variants
- `doc/working/scan-perf-baseline.md` — flushed results section
- `doc/working/kv-scan-flow-analysis.md` — note the flag + verified
  hypothesis
- `lib/crow-kv/tests/...` — mgmt API flush endpoint test (locate the
  existing mgmt-api test layer)

## Test checklist

- [x] flush endpoint 200 on hosting node, 404 on non-hosting
- [x] `--flush-after-prepopulate` runs without error (drains L0)
- [x] no-flag baseline reproducible
- [x] clippy/fmt clean

## Empirical finding — hypothesis REFUTED (2026-08-05)

Ran the 4 value-size configs (1T:1C, 10s mem, 100k pre-pop,
limit=1000, linearizable):

- `valuesize_64B` (no flush): 224 scans/s, avg=4455us
- `valuesize_1KiB` (no flush): 709 scans/s, avg=1409us  (3.2x faster — anomaly reproduces)
- `valuesize_64B_flushed`: 219 scans/s, avg=4564us  (unchanged)
- `valuesize_1KiB_flushed`: 721 scans/s, avg=1386us  (unchanged)

The `--flush-after-prepopulate` flag drains L0 deterministically (the
leader's `contiguous_slot` = 100k after pre-pop, so `flush()` drains
every memtable), yet the 3.2x gap is unchanged. So the 1KiB anomaly is
NOT caused by `MemTable::snapshot()` O(N_l0) cost.

Why the flag is a no-op: the "e2e" election profile
(`lib/crow-kv/src/common/config.rs:356`) sets `maintenance_tick_ms =
3000`, so the per-group maintenance loop (`group_maintenance.rs:108`)
calls `KVEngine::flush()` every 3s. During the 10s measurement window
(~3 ticks) and the multi-second pre-pop, L0 is already largely drained
by the maintenance loop. The scan bench (`--workload list`) issues no
writes during measurement, so L0 does not refill. L0 is small at scan
time with or without the flag.

Conclusion: the anomaly is in the L1 B+tree scan path (or the
decode/copy path), not L0. The R46 code-reading hypothesis was wrong.
R48 (lazy L0 cursor) would NOT fix the anomaly — its premise
(`MemTable::snapshot()` O(N_l0) is the root cause) is refuted. The real
root cause needs a separate investigation (likely an engine-level C++
microbench isolating per-leaf merge / delta-chain cost vs value size,
with a flushed L1-only tree).

## Blocked — decision needed

The R47 flag is implemented and works (drains L0, endpoint tested,
lint clean). R47's own acceptance ("verify the hypothesis") is met —
the verification result is negative. But the finding breaks the
dependency chain for the remaining scan requirements:

- **R48** (lazy L0 cursor) is predicated on the L0-snapshot hypothesis.
  Since the hypothesis is refuted, R48 would not fix the 1KiB anomaly.
  Decision needed: (a) still do R48 for O(limit) correctness even
  though it won't fix the anomaly, (b) deprioritize R48 and
  investigate the real L1 root cause first (new requirement), or
  (c) drop R48.
- **R38/R44/R49** are independent of the L0 hypothesis and remain
  valid as originally scoped.

Awaiting user direction before proceeding to R48.
