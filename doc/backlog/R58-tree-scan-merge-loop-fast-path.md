<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R58: Scan — Merge Loop Fast Path + Loser Tree for Many Sources

**Problem**: the scan merge loop re-compares **every** L0 cursor plus
L1 to select the min-key winner for each output entry
(`crow-tree.cpp:1890-1970`). Per entry it does:

- A min-key scan over all sources (`:1897-1906`): for each L0 cursor,
  `c.cur.key()` + `k.compare(min_key)` — a byte-wise `Slice::compare`.
- A second pass to find cursors sitting on the winning key and pick the
  highest-slot cell (`:1923-1934`): another `c.cur.key().compare(min_key)`
  per cursor.

So each output entry costs **2 × N_sources** byte-wise compares, where
`N_sources = 1 + N_frozen_memtables + 1 (L1)`. With one active L0 and
no frozen memtables that is 4 compares/entry (2 sources × 2 passes) —
tolerable. But with several frozen memtables live (a burst of writes
that freezes multiple 4 MiB memtables before they drain to L1) it
multiplies linearly: 5 frozen + 1 active + L1 = 7 sources → 14
compares/entry. No prefetch (`__builtin_prefetch`) is issued for the
next skip-list node or the right-sibling leaf, so each compare is also a
likely cache miss on cold/warm ranges.

**Design justification (no profiling needed)**: the common case is
**2 sources** (1 active L0 + L1, no frozen memtables) — the merge is a
trivial 2-way min that needs exactly 1 compare, not a 2-pass scan over
a vector. The general case is a k-way merge, for which a **loser tree**
is the textbook O(log k) per-merge structure (k = N_sources): it keeps
the per-entry compare count at `log2(k)` instead of `2k`, and the tree
is trivially updated when a cursor advances. The 2-source case is a
degenerate loser tree (1 compare) and can be special-cased as a branch
to avoid the tree overhead entirely. Prefetch is independent of the
merge structure: issuing `__builtin_prefetch` for the next skip-list
node (before advancing the cursor) and the right-sibling leaf (before
the current leaf exhausts) overlaps the next memory access with the
current merge step.

**Solution**:

- **2-source fast path**: at the top of the merge loop, if
  `l0.size() == 1 && have_l1`, skip the vector scan and do a direct
  `l0[0].cur.key().compare(l1.key())` to pick the winner. This is the
  overwhelmingly common case (steady-state: 1 active memtable, frozen
  memtables drained to L1). Falls through to the general path when
  `l0.size() != 1` or L1 is exhausted.
- **Loser tree for k > 2**: when `N_sources > 2`, build a loser tree
  over the cursors (L0 cursors + L1). Each merge step reads the tree
  root (current winner), emits it, advances that cursor, and sifts the
  new key up the tree (`log2(k)` compares). Rebuild the tree only when a
  source exhausts (cursor goes invalid). The highest-slot-wins tiebreak
  on key collision is handled in the sift-up: when two cursors share the
  winning key, the higher-slot cell wins and both cursors advance (same
  as today's second pass, but folded into the tree update).
- **Prefetch**: in the 2-source fast path and the loser tree, issue
  `__builtin_prefetch` for the next skip-list node when advancing an L0
  cursor, and for the right-sibling leaf when `refill_l1` is about to
  walk to the next leaf. Prefetch is a hint — no correctness impact, and
  the win is largest on cold/warm ranges where the next node/leaf is not
  in cache.

**Scope**:
- `lib/crow-tree/src/crow-tree.cpp` — `scan` / `try_scan_no_load` merge
  loop: add the 2-source fast path branch; add a `LoserTree` helper
  (small, ~50-80 lines) for the k > 2 case; add `__builtin_prefetch`
  calls at the cursor-advance and leaf-refill sites. The `consider`
  lambda and the rest of the scan path are unchanged.
- `lib/crow-tree/src/crow-tree.h` — if `LoserTree` is a class, declare
  it (or keep it in the .cpp as an internal helper).
- Tests: `test-tree-ct` scan tests must pass unchanged (output is
  identical). Add a test with multiple frozen memtables (force several
  freezes without draining) to exercise the k > 2 path and assert
  correct merge order + highest-slot-wins.

**Complexity**: Medium. The 2-source fast path is small and low-risk
(a branch + 1 compare). The loser tree is the bulk of the work — it's a
well-understood structure but the highest-slot-wins tiebreak on key
collision across L0 sources (the out-of-order slot delivery case noted
at `crow-tree.cpp:1760-1765`) must be handled correctly in the sift-up.
Prefetch is trivial to add but needs measurement to confirm the win
(don't over-prefetch — one prefetch per advance, not per compare).

**Dependencies**: none. Independent of R54 (the O(2k) vs O(log k)
redundancy is a design-level inefficiency, not a profiling-discovered
hot spot). Independent of R57 (R57 removes staging copies; R58 reduces
compare count — they touch different parts of the merge loop and can
land in either order, though merging them into one pass is cleanest if
both are picked up together).

**Acceptance**:
- Scan output is byte-identical to today across all `test-tree-ct` scan
  tests (key order, slot-wins tiebreak, tombstone filtering).
- The 2-source case (`l0.size() == 1`, the common steady-state) takes
  the fast path (verifiable via a counter or by code inspection in
  review).
- A multi-frozen-memtable scan (k > 2) produces correct merge order
  with the loser tree — a dedicated test with 3+ frozen memtables
  passes.
- `tools/bench-scan-regression.sh` shows no regression on the common
  configs; a multi-frozen-memtable config (if added to the bench) shows
  improvement.

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Performance →
"Merge loop is O(N_sources) comparisons per entry". Originally deferred
to R54 profiling; raised now because the 2-pass O(2k) scan is a
design-level redundancy (a k-way merge has a known O(log k) structure),
and the common 2-source case has an obvious 1-compare fast path.
