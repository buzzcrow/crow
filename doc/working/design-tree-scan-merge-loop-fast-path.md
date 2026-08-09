<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R58 Design — Merge Loop 2-Source Fast Path + Loser Tree

## Problem

The scan merge loop re-compares **every** L0 cursor plus L1 to select the
min-key winner for each output entry. Both `scan` (`crow-tree.cpp:1895-1995`)
and `try_scan_no_load` (`:2148-2243`) do the same 2-pass structure per entry:

- **Pass 1** (min-key select, `:1911-1926` / `:2162-2181`): scan every valid
  cursor, `c.cur.key().compare(min_key)` — O(N_sources) byte-wise compares.
- **Pass 2** (winner + advance, `:1931-1954` / `:2186-2209`): scan every valid
  cursor again, `c.cur.key().compare(min_key) != 0` to find those sitting on
  the winning key, pick highest-slot, advance them — O(N_sources) compares.

So each output entry costs **2 × N_sources** byte-wise compares, where
`N_sources = N_valid_l0 + 1 (L1)`. With 1 active L0 and no frozen memtables
(steady state) that is 4 compares/entry (2 sources × 2 passes). With several
frozen memtables (a burst of writes before drain) it multiplies linearly: 5
frozen + 1 active + L1 = 7 sources → 14 compares/entry. No prefetch is issued
for the next skip-list node or the right-sibling leaf, so each compare is also
a likely cache miss on cold/warm ranges.

## Current Behavior

The merge loop (`scan` at `:1895`, `try_scan_no_load` at `:2148`) runs a
fixed 2-pass structure every iteration regardless of source count:

1. `refill_l1()` — pull the next non-exhausted L1 leaf.
2. Pass 1: linear scan over `l0` cursors + `l1` to find `min_key`.
3. Pass 2: linear scan over `l0` cursors + `l1` to find all cursors on
   `min_key`, pick highest-slot winner, advance all of them.
4. Materialize the winning cell, call `consider`.

`l0` is a `std::vector<L0Cursor>` built from `all_memtables()` (active +
frozen). In steady state `l0.size() == 1` (one active memtable, no frozen);
frozen memtables accumulate only during a write burst before `flush()` drains
them. `l0.size()` is fixed for the whole scan (the vector is built once at the
top), but cursors within it exhaust individually as they reach their end.

`scan_async_attempt` (`:2288`) delegates to `try_scan_no_load`, so the async
path inherits any merge loop change automatically — no separate loop to update.

## Proposed Approach

Three changes, all confined to the merge loop in `scan` and
`try_scan_no_load`:

### 1. 2-source fast path (the common case)

At the top of each merge loop iteration, after `refill_l1()`, count valid
sources. `n_valid_l0` is tracked incrementally (starts at `l0.size()`,
decremented when a cursor exhausts) — not recomputed by a scan. When
`n_sources == 2`, skip the vector scan and do a direct 1-compare min between
the two sources (identified by scanning `l0` for the one valid cursor + L1, or
two valid L0 cursors). On a key tie, pick higher slot and advance both. This
is the overwhelmingly common steady-state case (1 active L0 + L1, no frozen
memtables): 1 compare instead of 4.

When `n_sources == 1`, emit directly from the single valid source with no
merge compare at all (the current code still does the full 2-pass even with 1
source).

When `n_sources == 0`, break (unchanged).

### 2. Loser tree for k > 2

When `n_sources > 2` (several frozen memtables live), build a loser tree over
the cursors (valid L0 + L1). The loser tree is the textbook O(log k) per-merge
structure: it keeps the per-entry compare count at `log2(k)` instead of `2k`.

**Structure** (internal helper in `crow-tree.cpp`, ~60-80 lines):

- `k` = number of valid sources (L0 cursors + L1).
- `losers[k]` — array of source indices (the loser at each internal node,
  1-indexed binary tree layout).
- `winner_` — index of the current overall winner (root).
- A `MergeSource` view wraps each source uniformly:
  `{kind (kL0/kL1), l0_cursor*, l1_cursor*}` with `valid()`, `key()`,
  `slot()`, `advance()`.

**Match function** (`less(a, b)` → a wins over b):
- Lower key wins.
- On key tie: higher slot wins.
- On key+slot tie: lower source index wins (deterministic, matches the
  current code's iteration order).

**Per merge step:**
1. `w = winner_` — the current winner (min key, highest slot among those on
   min key, guaranteed by the match function).
2. Emit `w`'s key/cell via `consider`.
3. Advance `w`'s cursor. If it exhausts, decrement `n_valid_l0` (if L0) and
   mark the tree for rebuild.
4. Sift `w`'s new key up the tree from its leaf (O(log k) compares).
5. **Collision drain**: peek the new root. If its key == the emitted key:
   - Advance that cursor (duplicate key, already emitted — no second emit).
   - Sift its new key up the tree.
   - Repeat until root key != emitted key or tree empty.
   - O(collision_count × log k); in the common no-collision case, 1 compare.

The collision drain works because after the winner advances, any other cursor
still on the same key naturally bubbles to the root (it was the min key). This
folds the current pass-2 "find all cursors on min_key and advance them" into
the tree update, matching the R58 backlog doc's "both cursors advance, folded
into the tree update."

**Rebuild**: when a source exhausts (cursor goes invalid after advance),
rebuild the tree from the remaining valid sources. If the remaining count
drops to ≤ 2, abandon the tree and let the fast path / single-source path
take over on the next iteration.

### 3. Prefetch

- **Skip-list next node**: before advancing an L0 cursor, issue
  `__builtin_prefetch` for the next node's memory. Add a
  `Cursor::prefetch_next()` method that does
  `__builtin_prefetch(cur_->next(0))` (the node `advance()` will move to).
  Called once per advance, not per compare.
- **Right-sibling leaf**: in `refill_l1`, after updating `page_id` to the
  right-sibling, issue `__builtin_prefetch` for the right-sibling page's
  memory (`resident(page_id)` pointer). This brings the next leaf into CPU
  cache while the merge loop works on the current leaf. One prefetch per leaf
  entry, not per entry.

Prefetch is a hint — no correctness impact. The win is largest on cold/warm
ranges where the next node/leaf is not in CPU cache. On mem-mode (everything
resident and hot) the win is zero, but the prefetch instruction cost is also
near-zero (a single non-faulting hint).

### Scope

- `lib/crow-tree/src/crow-tree.cpp` — `scan` and `try_scan_no_load` merge
  loops: add the single-source / 2-source fast path / loser tree dispatch at
  the top of each iteration; add the `LoserTree` helper class (internal, in
  the .cpp); add `__builtin_prefetch` calls at cursor-advance and
  `refill_l1` sites. The `consider` lambda, the deadline check, the early-stop
  checks, and the metrics recording are unchanged.
- `lib/crow-tree/include/crow-tree/skip_list.h` — add
  `Cursor::prefetch_next()` (1-line method).
- `lib/crow-tree/include/crow-tree/crow-tree.h` — no public API change (the
  `LoserTree` is an internal helper; if it's a class, keep it in the .cpp).
- Tests: `test-tree-ct` scan tests must pass unchanged (output is identical).
  Add a test with multiple frozen memtables (force several freezes without
  draining, using the `memtable_flush_entries = 1` + non-contiguous slots
  pattern from `double_buffer_test.cpp`) to exercise the k > 2 path and
  assert correct merge order + highest-slot-wins.

## Alternatives Considered

- **Heap (priority queue) instead of loser tree**: a binary min-heap gives the
  same O(log k) per entry. Rejected: the loser tree has a smaller constant
  factor (the winner is compared against stored losers, not two children per
  node), and the R58 backlog doc specifies a loser tree. The collision drain
  (peek root, pop duplicates) is equally natural with either structure.
- **Keep the 2-pass approach, just add the 2-source fast path**: the fast path
  alone handles the common case (k == 2), but the frozen-memtable burst case
  (k > 2) would still be O(2k). Rejected: the R58 doc explicitly calls for the
  loser tree for k > 2, and the frozen-memtable burst is exactly when the
  merge cost matters most (many sources, each compare is a cache miss).
- **Loser tree for all k ≥ 2 (no separate fast path)**: the 2-source case is a
  degenerate loser tree (1 compare), but the tree overhead (build, sift, array
  indexing) is non-trivial for k == 2. The direct 1-compare branch avoids that
  overhead entirely in the common case. Rejected per the R58 doc: "the 2-source
  case is a degenerate loser tree (1 compare) and can be special-cased as a
  branch to avoid the tree overhead entirely."
- **Track collision cursors in the tree structure**: instead of the collision
  drain (peek root repeatedly), maintain a list of cursors on the winning key
  within the tree. Rejected: adds complexity to the tree for a rare case
  (collisions only happen with out-of-order slot delivery across freeze
  boundaries). The drain is O(collision_count × log k) and collisions are
  typically 0 or 1.

## Acceptance Test Plan

- **No regression**: all existing `test-tree-ct` scan tests pass unchanged —
  scan output is byte-identical (key order, slot-wins tiebreak, tombstone
  filtering, prefix/end_key bounds, limit/byte_budget truncation, deadline
  truncation).
- **2-source fast path**: the common steady-state case (`l0.size() == 1`,
  L1 present) takes the fast path — verifiable by code inspection in review
  and by the existing `ReadPath.ScanOrderLimitTruncatedAcrossLeaves` test
  (which has 1 active L0 + L1 after flush).
- **k > 2 loser tree**: a dedicated test with 3+ frozen memtables (force
  several freezes without draining, using the `memtable_flush_entries = 1` +
  non-contiguous slots pattern) produces correct merge order with the loser
  tree — key order, highest-slot-wins tiebreak, no duplicate keys, no missing
  keys.
- **Prefetch**: no functional test (prefetch is a hint); verified by no
  regression in scan tests and no new TSan/ASan warnings.
- **`tools/bench-scan-regression.sh`**: no regression on common configs; a
  multi-frozen-memtable config (if added to the bench) shows improvement.
