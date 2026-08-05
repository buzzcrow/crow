<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R47: Bench flush-after-prepopulate flag

**Problem**: The scan perf baseline (R46) uncovered a 1KiB anomaly:
1KiB values scan 3.2x faster than 64B values (666 vs 206 scans/s)
despite returning 16x more data. Root cause identified by code
reading: `MemTable::snapshot()` (`memtable.cpp:195`) copies all N_l0
entries into a vector on every scan call — O(N_l0) per scan, not
O(limit). With `memtable_flush_bytes = 4 MiB`, 64B values (~104B/entry)
hit the byte threshold at ~40k entries, leaving ~60k unflushed after
100k pre-pop; 1KiB values (~1064B/entry) hit it at ~4k, leaving only
~4k. The 60k vs 4k snapshot cost difference explains the 3.2x
throughput difference. But this is a code-reading hypothesis — not
yet verified empirically because the bench has no way to force a flush
after pre-population.

**Target**:
- Add a `--flush-after-prepopulate` flag to `crow-cli bench run` that
  calls `flush` on the engine after the pre-population phase, before
  the measurement window opens. This drains L0 (MemTable) into L1
  (B+tree), so the scan runs against a fully L1-resident tree.
- The flag is bench-only (no production code change); it calls the
  existing `KVEngine::flush` path via the same RPC the maintenance
  loop uses, or a dedicated bench RPC if no flush RPC exists.

**Acceptance**:
- With `--flush-after-prepopulate`, the `valuesize_64B` and
  `valuesize_1KiB` scan configs produce comparable throughput (the
  3.2x gap closes), confirming the `MemTable::snapshot()` O(N_l0)
  hypothesis.
- Without the flag, the existing baseline numbers are reproducible
  (no behavior change when the flag is absent).
- The flag is documented in the scan perf baseline
  (`doc/working/scan-perf-baseline.md`) and the regression sentinel
  (`tools/bench-scan-regression.sh`) gains a flushed variant for
  comparison.

**Complexity**: Low — one CLI flag, one flush call after the
pre-population loop in `bench/runner.rs`. The flush RPC may need to
be added if it doesn't exist yet (check `crow-kv-client` for a flush
method; the maintenance loop already calls `KVEngine::flush`).

**Dependencies**: None. Follow-on: a lazy/range-bounded L0 cursor
(the actual fix for `MemTable::snapshot()` O(N_l0) cost) is a
separate task.
