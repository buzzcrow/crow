<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R48: Scan lazy/range-bounded L0 cursor

**Problem**: `MemTable::snapshot()` (`memtable.cpp:195`) copies all
N_l0 entries into a `std::vector<mem_entry>` on every scan call,
regardless of the scan limit — O(N_l0) per scan, not O(limit). The
scan's merge loop then binary-searches to `start_after` and produces
only `limit` entries, but the snapshot already paid the full copy cost.
This is the root cause of the 1KiB anomaly in the R46 baseline: 64B
values leave ~60k unflushed entries (snapshot copies 60k per scan),
1KiB leaves ~4k (snapshot copies 4k) — a 15x cost difference producing
the 3.2x throughput gap.

**Target**:
- Replace `MemTable::snapshot()` in the scan path with a lazy cursor
  that iterates the `absl::btree_map` directly (lower_bound to
  `start_after`, then advance up to `limit` entries). The MemTable
  mutex is held for the cursor's lifetime (same as `snapshot()`'s
  lock scope, but now O(limit) entries touched instead of O(N_l0)).
- The merge loop in `Crowtree::scan` (`crow-tree.cpp:1882`) iterates
  each L0 cursor alongside the L1 leaf chain, same merge logic, but
  the cursor advances lazily instead of consuming a pre-built vector.
- `try_scan_no_load` and `scan_async` get the same treatment.

**Acceptance**:
- `valuesize_64B` scan throughput with unflushed memtable matches
  `valuesize_1KiB` (the 3.2x gap closes) — verified via
  `tools/bench-scan-regression.sh` without `--flush-after-prepopulate`.
- Existing scan tests pass (`test-tree-ct` ReadPath.* + AsyncScan.*).
- No behavior change for `iter_all` / `compare` (those still use
  `snapshot()` — they need the full set, O(N) is correct there).

**Complexity**: Medium — the `absl::btree_map` supports `lower_bound`,
but the merge loop currently indexes into a `std::vector<mem_entry>`
(`c.entries[c.idx]`); a cursor abstraction that wraps the btree_map
iterator is needed. The mutex hold scope changes from "snapshot + copy"
to "cursor lifetime" — need to verify this doesn't deadlock with the
epoch guard or the L1 leaf walk.

**Dependencies**: R47 (flush-after-prepopulate) to verify the
hypothesis first; this is the actual fix.
