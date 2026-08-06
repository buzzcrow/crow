<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R48: Lazy limit-bounded L1 leaf resolver

**Status**: Root cause identified by per-step C++ metrics (see
`doc/working/kv-scan-flow-analysis.md` § Scan Per-Step Profile). The
original L0-snapshot premise is refuted — `l0_snapshot` is 0us in
production (maintenance loop drains L0 during pre-populate). The real
cause is `resolve_chain_sorted` rebuilding each touched leaf's full
entry set into a `std::map` per scan. R48 is now scoped to the actual
fix: a lazy, limit-bounded per-page resolver. The L0 copy issue is
covered by R50 (epoch-protected MemTable).

**Problem**: `resolve_chain_sorted` (`crow-tree.cpp:65`) resolves one
Bw-tree logical page's delta chain (`BatchDelta → ... → LeafBase`) into
a sorted `std::vector<leaf_entry>` on every scan. Two costs:

1. **Resolves the whole page, not the scan's needed entries.** The scan
   merge loop's `refill_l1` (`crow-tree.cpp:1847`) calls
   `resolve_chain_sorted` on the entire leaf, then the merge loop reads
   `l1_leaf[j]` one entry at a time with `consider()` applying
   `start_after` / `prefix` / `limit`. For a limit=1000 scan hitting a
   640-entry 64B leaf, all 640 entries are resolved just to emit ≤640.
   The microbench confirmed this: limit=10 costs the same as limit=1000.
   Cost is O(entries-per-leaf × log), not O(limit).

2. **`std::map` is the wrong structure for the dedup.** The base
   `LeafBase` is already key-sorted; only `BatchDelta` nodes are
   unsorted relative to the base. `std::map<Slice, Slice>` allocates one
   red-black tree node per entry (~640 heap allocations for a 64B leaf)
   even though the Slices themselves are borrowed (no byte copy in the
   map). The final output loop (`crow-tree.cpp:100`) then copies every
   key (`to_string()`) and cell (`cell_of()` = alloc + memcpy) into the
   result vector — copying every entry in the leaf, not just what the
   scan needs.

This is the root cause of the 1KiB anomaly: 64B values pack ~640
entries per 64KiB leaf vs ~58 for 1KiB, so each leaf resolve is far
more expensive for 64B. Production per-step metrics: `l1_resolve` is
99.5% of the 64B C++ scan (3985us of 4004us) and 93% of the 1KiB scan
(492us of 529us) — a 7.6x per-step ratio, diluted to 3.8x end-to-end
by the ~625us fixed consensus+gRPC overhead.

**Target**:
- Replace `resolve_chain_sorted` with a lazy per-page resolver that
  produces entries on demand from the sorted base + deltas, deduping
  by highest-slot-wins as it goes, stopping when the scan has enough.
  Makes the per-leaf cost O(limit), not O(entries-per-leaf).
- Replace the `std::map` dedup with a flat merge of the sorted base +
  unsorted deltas into a `std::vector<leaf_entry>` (reserve + push +
  sort by key, or k-way merge of sorted base + each delta). Avoids the
  per-entry red-black tree node allocation.
- Keep borrowed Slices in the resolved result — only copy when
  `consider()` emits to the caller (`crow-tree.cpp:1882/1887` already
  does `to_string()` there, so the intermediate copy at line 100 is
  redundant with the final emit).
- `scan()`, `try_scan_no_load()`, and `collect_in_order()` (snapshot /
  iter_all — needs the full set, O(N) is correct there) all use the new
  resolver.

**Acceptance**:
- `valuesize_64B` scan throughput matches `valuesize_1KiB` (the 3.8x
  gap closes) — verified via `tools/bench-scan-regression.sh`.
- `scan_step_profile` microbench shows `l1_resolve` drops to O(limit)
  (limit=10 measurably cheaper than limit=1000, both cheaper than
  current limit=1000).
- Existing scan tests pass (`test-tree-ct` ReadPath.* + AsyncScan.*).
- No behavior change for `iter_all` / `compare` / `snapshot()` (full-set
  callers get the same entries, just via the new resolver).

**Complexity**: Medium — the base is already sorted (LeafFrameView
iterates in key order); the deltas are the only unsorted input. A flat
vector + std::sort of just the delta entries, then a 2-way merge with
the sorted base, is simpler than the current std::map approach. The
lazy variant (produce on demand, stop at limit) needs the resolver to
hold iteration state across merge-loop iterations — a cursor struct
replacing the current `l1_leaf` vector + `j` index.

**Dependencies**: None. The per-step metrics instrumentation
(`ScanProfile`, `scan.*` C++ metrics, `scan_step_profile` microbench)
is already in place from the R48 measurement phase.
