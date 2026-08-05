<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R46: Scan path perf design + baseline

**Problem**: Scan is part of the read flow, but for perf it is a
separate track from the random point-read test — the two have different
cost shapes (per-entry overhead vs leaf-chain traversal vs per-byte
copy) and must be measured independently. Today the write path has
`memtable_bench.cpp` + `doc/working/bench-write-regression.tsv`, and the
random point-read path has `BM_ReadPath_GetHit` in `read_path_bench.cpp`.
The scan path has only `BM_ReadPath_Scan` — a single whole-keyspace
scan over 1k/10k tiny values (`"val" + i`, ~6 bytes) — and that one
number cannot answer:

- How does scan cost scale with `limit` (10 / 100 / 1k / 10k / whole)?
- What is the per-entry and per-byte cost as value size grows
  (64 B / 1 KiB / 16 KiB / 64 KiB)?
- Does `start_after` deep-pagination actually hit the design's claimed
  O(limit) FFI + decode cost (§1.7), or does it over-fetch the prefix
  range? No measurement exists for a scan starting near the end of a
  large keyspace.
- What is the cost split between leaf-chain traversal, L0 merge,
  packed-buffer decode, and the per-entry value copy that R38 targets?
- What is the prefix-range scan cost (bounded prefix vs whole-keyspace)?

R44 already calls out "no per-mode scan latency split or over-fetch
counters" as a hardening gap; R38 (scan value zero-copy) has no baseline
to measure its win against. Without a baseline, scan regressions are
invisible until they show up in end-to-end tests.

**Target**:
- A scan perf benchmark (new `lib/crow-tree/bench/scan_path_bench.cpp`,
  picked up by the existing `file(GLOB bench/*.cpp)` in
  `lib/crow-tree/CMakeLists.txt`) covering:
  - Full-keyspace scan at 1k / 10k / 100k keys.
  - Bounded `limit` scan (10 / 100 / 1k / 10k) over a 100k keyspace —
    isolates per-entry cost from leaf-chain setup.
  - Deep pagination: `start_after` set near the end of a 100k keyspace
    with small `limit` — verifies O(limit) vs O(prefix range). Reports
    an over-fetch ratio (leaves touched / entries returned, mirroring
    TiKV's seek-vs-processed_keys ratio) so the etcd-style "fetch all
    then truncate" regression is caught by a number, not just a
    latency change.
  - Value-size sweep at fixed key count: 64 B / 1 KiB / 16 KiB / 64 KiB
    — separates per-entry overhead from per-byte copy cost (the cost
    R38 removes).
  - Prefix-range scan over a bounded prefix vs whole-keyspace.
  - L0-overlay-heavy scan: scan with unflushed MemTable entries
    interspersed in the range — exercises the merge cursor.
- All scenarios report `items_processed` and `bytes_processed`
  (`state.SetBytesProcessed`) so per-entry and per-byte costs are both
  visible.
- Baseline numbers captured in a new
  `doc/working/scan-perf-baseline.md` (or `.tsv` mirroring
  `bench-write-regression.tsv`): per scenario, per arg, ns/entry and
  MB/s, on the same machine used for the write baseline.
- A short cost-split analysis in the working doc: which scenario makes
  the per-entry value copy (R38's target) dominant, and which makes
  leaf-chain / decode dominant — to prioritize R38 vs R44 scan work.
  Notes packed-buffer peak size for the large-value / whole-keyspace
  scenarios as the in-memory analog of etcd's range-read OOM risk
  (issue #12342), even though this baseline does not measure RSS.

**Acceptance**:
- `scan_path_bench.cpp` builds under `pixi run -- cmake ... -DCROW_TREE_BENCH=ON`
  and runs via `./crow_tree_bench --benchmark_filter=ScanPath`.
- All six scenario families above are present and produce stable
  numbers (run-to-run variance < ~5% on a quiet machine).
- `doc/working/scan-perf-baseline.md` contains the captured baseline
  with machine info (CPU, cores, page size) matching the write-baseline
  format, plus the cost-split conclusion.
- The deep-pagination scenario confirms (or refutes) the §1.7 O(limit)
  claim with a measured number, not just the design assertion, and
  reports the over-fetch ratio (leaves touched / entries returned).
- No production code changes — bench + working doc only.

**Dependencies**: None new — uses the existing `Crowtree` C++ API
(`apply`, `flush`, `snapshot`, `scan` with `start_after`), the existing
benchmark glob in `lib/crow-tree/CMakeLists.txt`, and the existing
`MemPageStore`. The `start_after` overload is already on `scan`
(§1.7).

**Priority**: Medium — needed to (a) gate scan regressions, (b) measure
R38's zero-copy win, and (c) prioritize the R44 scan-hardening subitems.
Not on the critical write path; no production behavior change.

**Complexity**: Low-medium — benchmark + measurement work only. The
scenarios are mechanical variations on `BM_ReadPath_Scan`'s
`build_tree` helper; the only non-trivial part is the L0-overlay-heavy
scenario (hold flush off, interleave MemTable puts in the scan range)
and the deep-pagination `start_after` setup. No FFI or trait changes.

**Files**: `lib/crow-tree/bench/scan_path_bench.cpp` (new),
`doc/working/scan-perf-baseline.md` (new, baseline numbers + cost
split). No changes to `doc_index.md` (working docs are self-indexed, per
the `/doc` workflow rule 6).

**Prior art** (how peer Raft/Paxos KV systems measure and announce
scan/list perf; the baseline doc should cite these so CROW numbers are
comparable to published peer-system numbers):

- **TiKV** (Raft, MVCC over RocksDB) — YCSB Workload E (short ranges)
  is the standard scan bench. The Follower Read blog reports scan perf
  as QPS + p99 latency vs number of scan keys (10 / 100 / 200 / 500 /
  1000 / 1500). Tracks a **seek-vs-processed_keys ratio** as a scan
  efficiency metric (issue #19167: 5:1 ratio = most seeks useless) —
  the direct analog of the over-fetch ratio R46's deep-pagination
  scenario reports. Range-scan regressions are also reported as
  end-to-end SQL query latency (TPC-H Q3/Q8).
- **etcd** (Raft, bolt/btree) — `benchmark range <start> <end>
  --consistency={s,l}` reports Requests/sec + avg/stddev latency + p99
  + histogram, **split by read consistency mode** (serializable vs
  linearizable). The limit-pushdown PR #21326 benchmarks
  `RangeLimitOptimization/KeyAscendLimit100_{10k,100k,1m}` old-vs-new
  sec/op — exactly R46's deep-pagination scenario; etcd proved O(limit)
  vs O(prefix) with a measured 99% improvement. The streaming range PR
  #19766 measures **peak memory (GiB)** alongside p50/p90/p95 latency
  because large range reads can OOM etcd (issue #12342) — memory is a
  first-class scan metric there.
- **CockroachDB** (Raft, MVCC over Pebble/RocksDB) — `BenchmarkMVCCScan`
  is a full Cartesian sweep: numRows {1,10,100,1k,10k,50k} ×
  numVersions {1,2,10,100,1000} × valueSize {8,64,512} × numRangeKeys
  {0,1,100}. Reports time/op with percent deltas + p-values. The C++
  MVCCScan reimplementation (PR #21395) reports **keys/sec (cumulative)**
  in addition to ops/sec — separates "scan operations/sec" from "keys
  delivered/sec", which matters when comparing scan widths. Also
  benchmarks `MVCCReverseScan` separately (reverse scan is a distinct
  cost shape). CROW's `scan` is forward-only today, so reverse scan is
  out of scope for R46 but is a future gap if reverse scan is added.
- **FoundationDB** (Paxos-ish, sharded RocksDB) — `performance_test.c`
  has a `getRange` test: random start key, fixed 100k keys, paginated
  via `fdb_transaction_get_range` with `WANT_ALL` streaming. Notable
  scan-perf work is **iterator pooling** (PR #6204: "throughput double
  after this change"), tracked via an iterator-reuse-count metric and
  split bounded vs unbounded reuse.

Patterns R46 adopts from this prior art: scan width as a primary axis
(TiKV, etcd, CockroachDB), value size as an axis (CockroachDB),
limit-pushdown / deep-pagination measurement with an over-fetch ratio
(etcd's `RangeLimitOptimization` + TiKV's seek-vs-processed ratio),
keys/sec alongside ops/sec (CockroachDB — covered by
`items_processed` + `bytes_processed`), and the L0-overlay scenario as
CROW's analog of CockroachDB's `numVersions` axis (CROW has no MVCC
version retention — highest-slot-wins — so a full version sweep is not
needed; the merge-cursor overlay is the equivalent cost). Read-mode
split (MinSlot vs Linearizable) and reverse scan are out of scope for
this engine-level baseline but noted as follow-on end-to-end items.
