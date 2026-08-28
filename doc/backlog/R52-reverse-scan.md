<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R52 — Reverse Scan

## Summary

`scan` is forward-only today (ascending key order). Reverse scan
(descending order) is a distinct cost shape and needs its own
implementation across the engine, FFI, RPC, and client layers.

## Problem

Some workloads need descending-key iteration — e.g. "newest first" when
keys are timestamp-ordered, or tail-of-keyspace pagination. Today the
only way to get reverse order is to scan forward and sort client-side,
which is O(N log N) and defeats the O(limit) scan pushdown.

## Scope

- **Engine** (`crow-tree`): the `LeafChainCursor` (R48) seeks and
  advances forward. A reverse cursor needs backward traversal:
  - `seek(start_before)` targets the leaf containing `start_before`
    and positions at the last entry < `start_before`.
  - `advance()` moves to the previous entry in key order (prev slot
    in the leaf, or the last entry of the previous leaf).
  - The merge loop walks L0 + L1 cursors backward, selecting the
    max-key entry (vs min-key forward), highest-slot-wins on collision.
- **L0 cursor** (R50): the `ConcurrentSkipList` is forward-only. A
  reverse cursor needs either `prev()` links (doubling the node's
  pointer tower memory) or a reverse traversal path (e.g. a
  right-to-left sentinel walk). The simpler approach: add a
  `cursor_reverse(start_before)` that seeks via `upper_bound` and
  walks the tower backward — but this requires prev pointers.
- **FFI** (`ct_scan_async`): add a `direction` parameter
  (`CT_SCAN_FORWARD = 0`, `CT_SCAN_REVERSE = 1`).
- **RPC** (`KvScanRequest`): add a `direction` field. The
  S3-style pagination uses the first key of each page as the next
  `start_before` (vs the last key as `start_after` in forward mode).
- **Client** (`CrowkvClient::scan`): add a `direction` parameter;
  pagination state tracks `start_before` instead of `start_after`.

## Cost Shape

Reverse scans have different cache behavior than forward scans:
backward leaf traversal touches pages in reverse allocation order,
which may have worse prefetch/sequential-read characteristics. Needs
its own scan perf baseline — add reverse-scan configs to
`tools/bench-kv-scan-regression.sh`.

## Dependencies

- R48 (lazy `LeafChainCursor`) — the cursor infrastructure exists;
  reverse mode adds a new traversal direction to it.
- R50 (epoch-protected MemTable) — the skip-list cursor needs prev
  links or a reverse traversal path.

## Complexity

Medium. The engine cursor work is the hardest part; the FFI/RPC/client
plumbing is straightforward (one new field per layer).
