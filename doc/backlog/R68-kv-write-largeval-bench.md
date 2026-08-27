<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R68: Large-Value Write Benchmark — Verify Maintenance-Loop Snapshot Stall Doesn't Cause Election Churn Under Write Load

**Problem**: R67 fixed the 16 KiB scan error spike on Linux by wrapping
the maintenance loop's `flush()`, `persist_snapshot()`, and
`collect_garbage()` in `tokio::task::spawn_blocking` so they no longer
hold the C++ `write_mutex_` on the async runtime and starve the election
driver. The fix was verified **only on the scan path** — the
`largeval_16k` config in `tools/bench-kv-scan-regression.sh` (100k × 16 KiB
pre-populated values, 0 scan errors post-fix).

But the maintenance loop runs **identically under write load**. A
write-heavy workload with large values accumulates live data at least as
fast as the scan bench's pre-populate phase (writes are the source of
the data the snapshot serializes), so it triggers the same
`persist_snapshot` / `flush` / `collect_garbage` calls that stalled the
election driver in R67. The write regression sentinel
(`tools/bench-kv-write-regression.sh`) only exercises **512 B values**
across 1T:1C → 256T:32C — there is **no large-value write config**, so
the large-value write path is completely untested.

If the R67 fix has a gap that only manifests under write load — e.g. a
different code path that still holds `write_mutex_` synchronously, write
backpressure interacting with the blocking pool, or the proposal/WAL
path adding latency that compounds with the snapshot stall — it would
surface as `NotLeader` **write** errors during election churn: the same
symptom class as R67, just on writes instead of scans. The scan bench
caught R67 because scans are acutely sensitive to leader changes (every
in-flight scan returns `NotLeader` on leadership loss); writes may mask
a brief stall differently (fewer in-flight ops per leader change, or the
proposal path retries internally), so a dedicated write benchmark is
needed to confirm the fix actually covers the write path.

**Hypothesis**: R67's fix is in the shared maintenance loop
(`group_maintenance.rs::run_pass`), which is workload-agnostic. The
write path should therefore be covered. This requirement verifies that
hypothesis; if it holds, the bench becomes a regression sentinel. If it
fails, the bench surfaces a real write-path correctness bug that needs
its own RCA.

**Solution**: Add a `largeval_16k` config to
`tools/bench-kv-write-regression.sh` mirroring the scan sentinel's
large-value config, and verify 0 write errors across consecutive runs.

1. **Bench config** — `--workload write --value-size 16384`, 100k key
   space, 10s mem mode, 3-node cluster. Start at 1T:1C (isolate
   per-write cost and the maintenance-loop interaction, matching the
   scan sentinel's `largeval_16k` shape). Add a higher-thread config
   (e.g. 32T:32C) only if 1T:1C is clean and a throughput ceiling is
   worth recording.
2. **Error budget** — the config must show **0 `total_errors`** (0
   `put_errors` / `batch_write_errors`, 0 `retries_exhausted`) across
   **3 consecutive runs** on Linux, matching R67's acceptance bar. If
   errors appear, do **not** tune the bench to hide them — RCA into
   whether the R67 fix has a write-path gap and file a follow-up
   requirement with the evidence.
3. **Reference results** — record ops/s, avg/p50/p99/p999 latency,
   `wal_append_count`, and errors in the script's reference block with
   the CPU model (absolute write throughput is platform-dependent, same
   caveat as the existing 512 B configs).
4. **Documentation** — the script header references
   `doc/design/kv/kv-write-flow-analysis.md` for the benchmark section,
   but that file does not yet cover large-value results. Either add a
   large-value results section there, or record the results in the
   script's reference block only (decide during implementation; the scan
   sentinel uses `doc/design/kv/kv-scan-flow-analysis.md`, so the
   parallel `kv-write-flow-analysis.md` is the consistent choice).

**Scope** (expected changed files):
- `tools/bench-kv-write-regression.sh` — add `largeval_16k` config(s);
  record reference results in the reference block.
- `doc/design/kv/kv-write-flow-analysis.md` — add a large-value
  results section (if chosen) documenting the large-value write
  results and CPU model.
- No `crow-kv` / `crow-cli` code changes expected unless the bench
  surfaces a real write-path stall, in which case scope expands to the
  RCA fix and a follow-up requirement.

**Complexity**: Low. Adding a bench config and running it 3×. If a real
bug surfaces, complexity escalates to whatever the RCA demands (R67 was
Low too — the fix itself was small, the RCA was the work).

**Dependencies**: None. R67 is Done; this verifies its coverage extends
to the write path. Independent of R66 (WAL io_uring) — the large-value
write bench uses the existing `File` I/O backend.

**Acceptance**:
- `tools/bench-kv-write-regression.sh` includes a `largeval_16k` config
  (`--value-size 16384`, 100k key space, 10s mem mode).
- The config runs with **0 errors** across **3 consecutive runs** on
  Linux (0 `total_errors`, 0 `retries_exhausted`).
- Reference results (ops/s, latency percentiles, `wal_append_count`,
  errors) recorded with the CPU model.
- If errors appear: a follow-up requirement is filed with the RCA
  evidence (server logs, `snapshot.apply.l` metrics, leader-change
  count), and this requirement is kept open until the write-path gap is
  fixed and the bench runs clean.
- `cargo fmt --check` and `cargo clippy -- -D warnings` clean (only
  relevant if any code change is needed; the bench script itself is not
  linted by cargo).
