<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R59: Scan — Cross-Page Snapshot Isolation (`snapshot_scan`)

**Problem**: a paginated scan is a sequence of per-page-consistent
slices, not one snapshot. `PxKvStore::kv_scan` resolves a fresh read
point on every page (`px_kv_store.rs:177`) and the engine's `scan`
reads live data (`crow_tree_engine.rs:203` → `Crowtree::scan`,
`crow-tree.h:583`) — so a value can change or a key vanish between
pages of the same logical scan. This matches S3-list semantics and is
fine for the KV Operator UI (interactive, idempotent re-issue), but is
**wrong for backup/analytics-style consumers** that need a
point-in-time view of the keyspace: a backup taken this way can
duplicate a key (present on page N, re-written on page N+1), miss a key
(deleted between pages), or observe a value that never existed
atomically.

The engine already pins point-in-time views:
`Crowtree::snapshot_view()` (`crow-tree.h:609`) returns a
`shared_ptr<Snapshot>` pinned at `last_applied_slot` (durable L1 state,
zero-copy via `PinnedSnapshot` page refcounts), exposed through the C
API as `ct_snapshot_view` (`c_api.cpp:933`) and the FFI as
`Crowtree::snapshot_view` (`ffi/src/lib.rs:1081`). The missing piece is
a scan that runs **against a pinned snapshot** and a way to keep that
snapshot alive across pages.

**Solution**: add a `snapshot_scan` variant that pins one engine
snapshot at the start of the scan and serves every page from it.

- New `ReadMode::Snapshot` (or a parallel `KvSnapshotScanRequest`) —
  page 1 resolves a read point as today (`Linearizable` or `MinSlot`
  per the caller's freshness need), then pins the engine at that
  `read_slot` via `snapshot_view()` and returns a server-side
  **snapshot handle** (opaque id) plus the first page. Subsequent
  pages carry the handle in `KvScanRequest`; the store looks up the
  pinned snapshot and scans against it instead of live data.
- The handle is held by a per-store registry with a lease/expiry
  (default ~30 s, configurable). The client releases the handle on
  normal scan completion; the lease reaps it if the client disconnects
  mid-scan (prevents unbounded pin retention — pinned pages delay
  their epoch retirement, blocking reclaim).
- Engine side: a `scan_at(snapshot, prefix, start_after, limit,
  byte_budget)` that runs the existing merge loop
  (`crow-tree.cpp:1890-1934`) against the pinned `Snapshot`'s root
  version instead of the live root. The merge loop, byte budget, and
  `truncated` semantics are unchanged — only the read root differs.
  `scan_async` gets the same `scan_at` twin.
- FFI: `ct_scan_at(ct_view *snap, ...)` mirroring `ct_scan`, plus the
  handle lifecycle (`ct_snapshot_pin` / `ct_snapshot_release` — or
  reuse the existing `ct_view` refcount if it already supports
  keep-alive across calls).
- Client: `CrowkvClient::snapshot_scan(...)` mirrors `scan` but
  threads the handle through the pagination loop. The retry-resume
  fix (tiny task 1) applies unchanged — the handle stays valid across
  a redirect/transport retry since it is keyed on `read_slot`, not on
  the endpoint.
- Proto: `KvScanRequest` gains `uint64 snapshot_handle` (0 = live
  scan, backward compatible); `KvScanResponse` gains
  `uint64 snapshot_handle` (set on page 1, echoed on later pages).
  Alternatively a dedicated `SnapshotScan` RPC keeps the surface
  clean — trade-off to resolve in design.

**Semantics**: a `snapshot_scan` returns a point-in-time-consistent
view of the keyspace at the pinned `read_slot` — no key appears twice,
no key vanishes mid-scan, every value is the one that held at
`read_slot`. The caller pays one snapshot pin (page-refcount bump on
the reachable tree) for the duration of the scan; pages are still
byte-budgeted so each unary response stays bounded. Cost is unchanged
for the common case (the merge loop runs against a pinned root instead
of the live root — same cursor work, same I/O).

**Scope**:
- `lib/crow-tree/include/crow-tree/crow-tree.h` + `src/crow-tree.cpp` —
  `scan_at(snapshot, ...)` and `scan_async_at(snapshot, ...)`. The
  merge loop is parameterized by the read root.
- `lib/crow-tree/src/c_api.cpp` + `include/crow-tree/c_api.h` —
  `ct_scan_at`, handle lifecycle.
- `lib/crow-tree/ffi/src/lib.rs` — safe wrappers.
- `lib/crow-kv/src/kv/crow_tree_engine.rs` — `engine_scan_at`.
- `lib/crow-kv/src/cluster/px_kv_store.rs` — snapshot handle registry
  (per-store, lease-expired), `kv_snapshot_scan` path.
- `lib/crow-kv/src/rpc/proto/kv.proto` — `snapshot_handle` fields (or
  new RPC).
- `lib/crow-kv-client/src/client.rs` — `snapshot_scan` method.
- Tests: integration test that writes concurrently with a
  `snapshot_scan` and asserts the scan observes exactly the keyspace
  at the pinned slot (no duplicates, no misses); handle lease expiry
  test; backward-compat test (live scan with `snapshot_handle = 0`).

**Complexity**: Medium–high. The engine `scan_at` is a
straightforward re-rooting of the existing merge loop, but the
server-side handle registry with lease/expiry, the FFI lifecycle, and
the proto/client threading add real surface area. Estimate ~600–900
lines across layers.

**Dependencies**: none hard. R55 (carry `read_slot` forward as
`min_slot`) is complementary — R55 optimizes the per-page barrier for
*live* scans, R59 makes *snapshot* scans possible; they don't interact.
R56 (end-key bound) composes cleanly with `scan_at` (same early-stop,
just against the pinned root).

**Acceptance**:
- A `snapshot_scan` started at slot `S` returns exactly the live keys
  at slot `S`, even as concurrent writes mutate the keyspace during the
  scan (no duplicates, no gaps, no values newer than `S`).
- The snapshot handle is released on normal scan completion and is
  reaped by lease expiry if the client disconnects mid-scan (no
  unbounded pin retention; pinned pages become reclaimable after
  release/expiry).
- A live `scan` with `snapshot_handle = 0` is byte-for-byte unchanged
  (backward compatible).
- Existing scan tests and `tools/bench-scan-regression.sh` pass; a
  `snapshot_scan` perf baseline is recorded (expected: parity with
  live scan, modulo the one-time pin cost).

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Functionality
→ "No cross-page snapshot isolation (by design, undocumented)". The
user-guide note about current S3-list semantics is deferred until R59
lands (the guide will document both the live-scan semantics and the
new `snapshot_scan` variant together).
