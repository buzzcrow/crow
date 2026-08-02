<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R37: Scan `start_after` push-down into the C++ engine

**Problem**: `CrowtreeEngine::scan` cannot push `start_after` into the
C++ scan API. `ct_scan_async` takes only `prefix` + `limit` (no cursor).
When `start_after` is non-empty, the Rust wrapper sets `fetch_limit = 0`
(over-fetch the whole prefix range), ships the packed result across
the FFI boundary, then filters keys `<= start_after` in Rust before
applying the limit. Deep pagination transfers and decodes entries the
client will discard — O(prefix range) FFI + decode cost instead of
O(limit) for a page near the end of a large prefix.

**Target**:
- Extend `ct_scan_async` (and the sync `ct_scan`) with a `start_after`
  cursor + lower-bound seek, so the C++ engine starts iteration at the
  cursor and applies the limit natively.
- The Rust `CrowtreeEngine::scan` fast path passes `start_after` through
  to the C API; the `fetch_limit = 0` over-fetch + Rust-side filter is
  removed.
- `InMemKV::scan` already does the right thing (`BTreeMap::range` from
  `start_after`); no change needed there.

**Acceptance**:
- A deep-pagination scan (`start_after` near the end of a large prefix
  range) returns in O(limit) time, not O(prefix range). Verified by a
  benchmark or a test that measures entries decoded (not just returned).
- Existing scan tests pass unchanged (the packed result format and
  the `ScanEntry` decode are preserved; only the over-fetch path is
  gone).
- The `start_after` cursor is exclusive (keys `> start_after`), matching
  the current Rust-side filter semantics.

**Dependencies**: None — the C++ `Crowtree::scan_async` already does an
in-order memtable traversal; adding a lower-bound seek is a local
change. The FFI binding (`ct_scan_async`) signature changes (new
param), so `crowtree-ffi` and `CrowtreeEngine::scan` update in lockstep.

**Priority**: Medium — matters for workloads that paginate over large
prefix ranges; no effect on point reads or shallow scans.

**Complexity**: Medium — touches the C++ scan API (new cursor param +
lower-bound seek in the memtable traversal), the FFI binding, and the
Rust wrapper. The packed result format and decode path are unchanged.

**Files**: `crowtree/include/crowtree/c_api.h` (`ct_scan_async`,
`ct_scan` signatures), `crowtree/include/crowtree/crowtree.h`
(`scan_async`, `scan` signatures), `crowtree/src/crowtree.cpp`
(`scan_async`, `scan_async_attempt` lower-bound seek),
`crowtree/src/c_api.cpp` (`ct_scan_async`, `ct_scan`),
`crowtree/ffi/src/lib.rs` (FFI binding), `crowkv/src/kv/crowtree_engine.rs`
(`scan` — pass `start_after` through, remove over-fetch),
`crowtree/tests/integration/async_scan_test.cpp` (cursor test).
