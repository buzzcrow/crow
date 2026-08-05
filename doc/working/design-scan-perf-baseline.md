<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design — R46 Scan Path Perf Baseline

Working design draft for `R46-scan-perf-baseline.md`. Will be folded into
`doc/design/tree/design-crow-tree-engine.md` (a new §1.9 "Scan Path
Perf Baseline" subsection, or appended to §1.7) after implementation,
then this file is deleted.

## Problem

The scan path has one microbenchmark today — `BM_ReadPath_Scan` in
`lib/crow-tree/bench/read_path_bench.cpp` — a whole-keyspace scan over
1k/10k keys with ~6-byte values (`"val" + i`). It reports only
`items_processed`. That single number cannot answer:

- How scan cost scales with `limit` (10 / 100 / 1k / 10k / whole).
- Per-entry vs per-byte cost as value size grows (64 B / 1 KiB / 16 KiB
  / 64 KiB) — the cost split that R38 (zero-copy scan values) targets.
- Whether `start_after` deep-pagination is actually O(limit) FFI +
  decode cost as §1.7 of `design-crow-tree-engine.md` claims, or
  whether it over-fetches the prefix range (the etcd-style "fetch all
  then truncate" regression).
- Prefix-range scan cost vs whole-keyspace.
- L0-overlay-heavy scan cost (unflushed MemTable entries interspersed
  in the range — exercises the merge cursor).

R44 already flags "no per-mode scan latency split or over-fetch
counters" as a hardening gap; R38 has no baseline to measure its win
against. Without a baseline, scan regressions are invisible until they
surface in end-to-end tests.

## Current behavior

`Crowtree::scan` signature (`lib/crow-tree/include/crow-tree/crow-tree.h`
line 545):

```cpp
Status scan(Slice prefix, Slice start_after, size_t limit,
            std::vector<scan_entry> *out, bool *truncated,
            bool include_tombstones = false) const;
```

`scan_entry` (line 62) holds owned `std::string key` / `std::string
value` — every entry's value is copied out of the packed result buffer
into a fresh allocation. This per-entry copy is exactly what R38
removes; R46 must measure it before R38 can prove a win.

The existing bench helper `build_tree` (read_path_bench.cpp line 37)
constructs a flushed + snapshotted tree over `MemPageStore` with
`leaf_split_bytes = 160`, `frame_bytes = 4096`. R46's scenarios are
mechanical variations on this helper.

## Proposed approach

Two layers, matching the existing write/read regression pattern
(`tools/bench-write-regression.sh` + `tools/bench-read-regression.sh`
drive `crow-cli bench run`; `read_path_test.cpp` / `async_scan_test.cpp`
are the engine-level correctness tests). **The crow-cli end-to-end
layer is the primary deliverable** — it is the regression sentinel that
gates scan perf going forward, mirroring how write/read regression is
tracked. The C++ layer is a correctness UT only: verify the sync `scan`
function is there, has no code bugs, and the §1.7 `start_after`
deep-pagination pushdown actually returns the right tail entries (not
the whole prefix). No C++ perf microbench — the perf measurement is the
CLI's job.

### Layer 1 (primary) — end-to-end crow-cli scan regression sentinel

The existing write/read regression sentinels use `crow-cli bench run`
against a real 3-node cluster (mem mode) with a shell script that
drives configs and appends results to a TSV. The scan workload
(`WorkloadKind::List`) exists in the CLI but the runner hardcodes
`prefix=""`, `start_after=""`, `limit=1`, `ReadMode::Linearizable`
(`runner.rs` line 593–614) — a stub. R46 adds the missing knobs:

- New `RunArgs` CLI flags (`app/crow-cli/src/commands/bench.rs`):
  `--scan-limit` (default 1), `--scan-prefix` (default empty),
  `--scan-start-after` (default empty). Defaults preserve backward
  compat.
- New `BenchConfig` fields (`runner.rs`): `scan_limit`, `scan_prefix`,
  `scan_start_after`.
- The `OpKind::List` arm uses `cfg.read_mode` +
  `cfg.min_slot_policy.to_min_slot()` instead of hardcoded
  `Linearizable`/`None`, so the scan bench covers both read modes
  (R44's "no per-mode scan latency split" gap).
- New `tools/bench-scan-regression.sh` mirroring
  `bench-write-regression.sh`: drives configs covering full-keyspace,
  bounded limit, deep pagination (with from-start companion for the
  over-fetch proxy), value-size sweep, prefix range, and read-mode
  split. Appends results to `doc/working/bench-scan-regression.tsv`.

This is a small production code change to `crow-cli`'s bench module
(adding optional flags + wiring them through). It contradicts R46's
original "no production code change" scope, so R46's scope is amended
to include the crow-cli scan bench flags. The engine itself
(`lib/crow-tree`, `lib/crow-kv`) is unchanged — the scan RPC and
`kv_scan` server handler are already implemented (`px_kv_store.rs`
line 138).

L0-overlay is not covered by either layer in R46: the end-to-end
client cannot hold the server's flush off, and the C++ layer is a
correctness UT only (no perf microbench). L0-overlay scan perf is
noted as a future gap in the baseline doc; the existing
`ReadPath.L0OverridesL1` / `ReadPath.L0TombstoneHidesL1` UTs already
cover L0-overlay scan correctness.

### Layer 2 (secondary) — sync `scan` start_after correctness UT

The sync `scan` path has zero tests with a non-empty `start_after` —
every sync `scan` call in the test suite uses `Slice()` (empty) for
`start_after` (verified by grep across
`lib/crow-tree/tests/integration/*.cpp`). The async twin has
`AsyncScan.StartAfterCursorSkipsEarlierEntries`
(`async_scan_test.cpp` line 267) covering the cursor pushdown, but
the sync path — the one the §1.7 O(limit) claim is about — is
untested for correctness.

Add a `ReadPath.ScanStartAfterCursor*` UT in
`lib/crow-tree/tests/integration/read_path_test.cpp` mirroring the
async test: build a multi-leaf tree, scan with `start_after` set near
the end + small `limit`, verify only keys strictly greater than the
cursor are returned (not the whole prefix), in order, with the correct
first key. This verifies the function is there, has no code bugs, and
the deep-pagination pushdown works. No perf measurement.

### Baseline doc

Baseline numbers captured in `doc/working/scan-perf-baseline.md` with:
- Machine info (CPU, cores, page size) matching the
  `kv-write-flow-analysis.md` / `kv-read-flow-analysis.md` Platform
  section format.
- The end-to-end crow-cli baseline (primary): per config, scans/s +
  avg/p50/p99/p999 latency, from
  `doc/working/bench-scan-regression.tsv`.
- A cost-split conclusion drawn from the end-to-end numbers: which
  config makes per-entry overhead dominant vs per-byte copy (the cost
  R38 removes), to prioritize R38 vs R44 scan work. Without a C++
  microbench this is coarser than an engine-level split, but the
  value-size sweep (64 B vs 16 KiB at fixed scan width) separates
  per-entry from per-byte cost at the end-to-end level — if 16 KiB
  scans/s drops far below 64 B scans/s at the same limit, per-byte
  copy (R38's target) is dominant.
- The deep-pagination O(limit) verdict (confirmed / refuted) from the
  end-to-end deep-pagination config vs the from-start companion: if
  scans/s is ~equal, the pushdown works; if deep-pagination is far
  slower, the engine over-fetches the prefix (the etcd-style
  regression).
- L0-overlay scan perf noted as a future gap (not measured by either
  layer in R46).

### Platform note

The write baseline (`bench-write-regression.tsv`) and the read-flow
analysis were captured on an AMD Ryzen 9 5950X Linux machine. This
working session is on macOS, so the numbers captured here are
**Apple-silicon macOS numbers**, not directly comparable to the Linux
write baseline. The baseline doc records the actual capture platform
and notes that the official cross-comparable baseline should be
re-captured on the same Linux Ryzen machine. The crow-cli bench runs
identically on both platforms (mem mode, no io_uring dependency).

## Alternatives considered

- **C++ engine-level perf microbench (six `ScanPath_*` families).**
  Rejected for R46: the perf measurement is the CLI's job (it is the
  regression sentinel, mirroring write/read). A C++ microbench would
  duplicate the end-to-end measurement at the engine level and add a
  build/run surface (`-DCROW_TREE_BENCH=ON`) that is not part of the
  normal CI gate. The sync `scan` correctness UT is enough at the C++
  layer; engine-level perf isolation can be added later if R38 needs a
  tighter before/after measurement than the end-to-end baseline
  provides.

- **Expose internal leaf-touch count from `scan` for an exact
  over-fetch ratio.** Rejected: would require a production code change
  to `lib/crow-tree` (new out-param or a debug counter on `Crowtree`).
  The end-to-end deep-pagination-vs-from-start scans/s comparison
  detects the O(limit) vs O(prefix) regression with the existing API.

- **TSV-only baseline mirroring `bench-write-regression.tsv`.**
  Rejected: R46 needs a cost-split prose conclusion and the
  deep-pagination verdict, which are not naturally tabular. A markdown
  doc with embedded result tables is the right shape; the write baseline
  is a flat TSV only because it has no prose analysis.

- **Add reverse-scan and L0-overlay end-to-end axes.** Out of scope per
  R46: `scan` is forward-only today, and L0-overlay is not controllable
  from the end-to-end client (cannot hold the server's flush off).
  Noted as follow-on items in the baseline doc.

## Acceptance test plan

- `crow-cli bench run --workload list --scan-limit N --scan-prefix P \
  --scan-start-after K --read-mode {linearizable|minslot}` works
  end-to-end against the 3-node fixture.
- `tools/bench-scan-regression.sh` runs to completion and appends
  results to `doc/working/bench-scan-regression.tsv`.
- The new `ReadPath.ScanStartAfterCursor*` UT passes under
  `pixi run test-tree-ct`, verifying the sync `scan` deep-pagination
  pushdown returns only keys strictly greater than the cursor (not the
  whole prefix).
- `doc/working/scan-perf-baseline.md` contains the end-to-end crow-cli
  baseline (scans/s, latency), the cost-split conclusion, the
  deep-pagination O(limit) verdict, and the platform note.
- Engine code (`lib/crow-tree`, `lib/crow-kv`) unchanged; only
  `crow-cli` bench module gets the new scan flags, and
  `lib/crow-tree/tests/integration/read_path_test.cpp` gets the new UT.
- Pre-commit gate passes: `clang-format --dry-run --Werror` on the
  modified `.cpp`, `tree-lint` on it, `cargo fmt --check`,
  `cargo clippy -- -D warnings`, `test-tree-ct`, `test-core`.

## Files

- `app/crow-cli/src/commands/bench.rs` (modified) — new `--scan-limit`,
  `--scan-prefix`, `--scan-start-after` CLI args + wiring.
- `app/crow-cli/src/bench/runner.rs` (modified) — new `BenchConfig`
  scan fields + `OpKind::List` arm uses them + `cfg.read_mode`.
- `tools/bench-scan-regression.sh` (new) — end-to-end regression
  sentinel script mirroring `bench-write-regression.sh`.
- `lib/crow-tree/tests/integration/read_path_test.cpp` (modified) —
  new `ReadPath.ScanStartAfterCursor*` UT for sync `scan` deep-pagination
  correctness (mirrors `AsyncScan.StartAfterCursorSkipsEarlierEntries`).
- `doc/working/scan-perf-baseline.md` (new) — captured baseline +
  cost-split conclusion + deep-pagination verdict + platform note.
- `doc/working/bench-scan-regression.tsv` (new, generated by the
  script) — end-to-end TSV mirroring `bench-write-regression.tsv`.
- No changes to `lib/crow-tree/CMakeLists.txt`.
- No changes to `doc/doc_index.md` (working docs are self-indexed, per
  the `/doc` workflow rule 6).
