<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R60: Scan — Sibling-Leaf Readahead on Cold Scans

**Problem**: the scan path demand-loads each L1 leaf inline. The sync
path (`Crowtree::scan`) resolves a leaf via the page cache when the
merge loop reaches it — the read stalls the loop until the leaf is
resident. The async path (`scan_async_attempt`) resolves one pending
page per reactor round trip and retries the whole scan, so a multi-leaf
cold range pays one reactor round trip per cold leaf, serialized with
the merge work on prior leaves.

A scan knows its next leaf before finishing the current one: the merge
loop reads `base->right_sibling()` (`crow-tree.cpp:1822/2074`) right
after descending into a leaf, before iterating that leaf's entries. So
the page id of the next leaf is available while the current leaf is
still being merged. Today nothing is done with it until the current
leaf exhausts and `refill_l1` walks to the next — at which point the
read stalls.

**Solution**: issue a readahead (prefetch) for the right-sibling leaf
as soon as its page id is known, overlapping the next leaf's I/O with
the current leaf's merge work.

- **Sync path** (`Crowtree::scan` / `try_scan_no_load`): after reading
  `right_sibling`, call the page cache's prefetch/async-resolve seam
  for that page id (non-blocking — enqueues the read, does not wait).
  When `refill_l1` later walks to that leaf, the read is already in
  flight or complete, so the stall is hidden. If the leaf is already
  resident the prefetch is a no-op.
- **Async path** (`scan_async_attempt`): instead of resolving one
  pending page per reactor round trip, submit the current leaf's read
  AND the right-sibling's read in the same reactor batch, so two leaves
  are in flight at once. The retry loop then advances two leaves per
  round trip on cold ranges, halving the round-trip count. Generalizes
  to a small readahead window (1–2 leaves ahead) bounded by a per-scan
  in-flight cap to avoid memory blowup on huge ranges.
- **Prefetch depth**: a single-leaf readahead is the minimal win (hides
  one leaf's latency). A small window (2–4) helps when leaf read
  latency is high relative to merge time (cold disk, slow NVMe). The
  window should be a tunable, defaulting to 1 (one leaf ahead) to keep
  memory bounded; the bench can sweep it.

**Scope**:
- `lib/crow-tree/src/crow-tree.cpp` — `scan` / `try_scan_no_load`:
  after `page_id = base->right_sibling()`, issue a prefetch. The
  prefetch seam must already exist or be added to the page cache
  (check `PageCache::resolve` / `Reactor` for an async-resolve or
  `posix_fadvise`/`readahead` hook). If no seam exists, this is blocked
  on adding one (small).
- `lib/crow-tree/src/crow-tree.cpp` — `scan_async_attempt`: batch the
  right-sibling read with the current leaf's read in the reactor
  submission.
- Tests: `test-tree-ct` scan tests must pass unchanged (readahead is
  observational — output is identical). Add a cold-scan test that
  evicts all leaves, scans a multi-leaf range, and asserts correct
  results (the existing `AsyncScan.MissAfterEvictionCompletesViaReactor`
  test covers the cold path; extend or mirror it for readahead).

**Complexity**: Medium. The sync-path prefetch is small IF a
prefetch/async-resolve seam exists in the page cache (likely needs a
small addition — the cache today resolves on demand). The async-path
batching is the bulk: the reactor submission and retry loop assume one
pending page today; batching two+ needs the loop to track multiple
in-flight reads and resume from the correct key when any completes.
Measurement is required to confirm the win (cold ranges only — the
bench today runs mem-mode where leaves are resident, so a cold/disk
bench config is a prerequisite for validation).

**Dependencies**:
- May need a page-cache prefetch seam (check first). If the cache has
  no non-blocking resolve, add one as a sub-task.
- Independent of R57 (staging copies) and R58 (merge loop compares) —
  they touch different parts of the scan path. R60 overlaps I/O with
  merge work; R57/R58 speed up the merge work itself. Complementary.
- R54 (scan engine profiling) would confirm whether cold-leaf I/O
  stall is a real bottleneck before investing — consider profiling
  first if cold-scan workloads are not yet a measured pain point.

**Acceptance**:
- Scan output is byte-identical to today across all `test-tree-ct` scan
  tests.
- A cold-scan benchmark (all leaves evicted, multi-leaf range) shows
  reduced per-leaf stall — measured as lower scan latency vs the
  no-readahead baseline. The win is zero on mem-mode (leaves resident).
- Readahead memory is bounded (per-scan in-flight cap, default window
  = 1); a full-keyspace cold scan does not grow unbounded RSS.
- No regression on `tools/bench-scan-regression.sh` (mem-mode configs
  unchanged — readahead is a no-op when leaves are resident).

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Performance →
"No sibling-leaf readahead on cold scans".
