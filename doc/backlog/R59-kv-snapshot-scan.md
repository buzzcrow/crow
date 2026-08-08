<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R59: Scan — Two Read Modes + Snapshot Versioning API

**Problem**: the current `scan` RPC is the only range-read surface. It
resolves a fresh read point on every page (`px_kv_store.rs:177`) and reads
live L0+L1 data — a paginated scan is a sequence of per-page-consistent
slices, not one snapshot (S3-list semantics). This is fine for interactive
listing (KV Operator UI), but wrong for backup/analytics that need a
point-in-time-consistent view: a key can vanish (tombstoned between pages)
or a value can drift (overwritten between pages) mid-scan.

The engine already pins point-in-time L1 views:
`Crowtree::snapshot_view()` (`crow-tree.h:609`) returns a
`shared_ptr<Snapshot>` pinned at `last_applied_slot` (zero-copy via
`PinnedSnapshot` page refcounts), exposed through the C API as
`ct_snapshot_view` (`c_api.cpp:933`) with `ct_view_iter`/`ct_iter_next`
(`c_api.cpp:949-988`) for entry-by-entry iteration, and through the FFI as
`Crowtree::snapshot_view` (`ffi/src/lib.rs:1081`). After `flush()`
(`crow-tree.h:507`) drains L0 MemTables into L1, the snapshot is a complete
durable point-in-time view — no L0 involvement, no concurrency issues, no
version-chain machinery. GC (`set_gc_watermark` + `collect_garbage`,
`crow-tree.cpp:722-730`) naturally respects pinned pages: refcounts keep
snapshot pages alive until the snapshot is released, then the next GC sweep
reclaims them.

The missing piece is not engine machinery — it is exposing the existing
snapshot + iterate path to clients, and documenting the two read modes.

**Solution**: two range-read modes, sharing the existing engine.

**Mode 1 — List scan (existing `scan` RPC, clarified)**:
- Fast, always returns the latest value per key at each page's read point.
- S3-list semantics: per-page-consistent, not cross-page snapshot. A key
  can vanish or a value can drift between pages of the same logical scan.
- No pinning, no handle, no server-side state beyond the per-page read
  barrier. This is the existing `KvScanRequest`/`KvScanResponse` path,
  unchanged — the only change is documenting the semantics.
- Use case: interactive listing, KV Operator UI, key discovery.

**Mode 2 — Snapshot versioning API (new)**:
- `CreateSnapshot` — flush L0 → L1, then `snapshot_view()` to pin L1 at
  `last_applied_slot`. Returns a server-side snapshot handle (opaque id)
  and the `at_slot` the snapshot covers. The handle is held by a per-store
  registry with a lease/expiry (default ~5 min, configurable) to reap
  abandoned snapshots.
- `ListSnapshots` — list active snapshot handles for a store/group with
  their `at_slot` and remaining lease.
- `SnapshotScan` — iterate a pinned snapshot with `prefix`, `start_after`,
  `limit`, and byte budget. The server binary-searches the frozen snapshot
  vector to `start_after`, filters by `prefix`, and applies `limit`/byte
  budget — same pagination contract as `scan` (S3-style, `truncated` +
  `start_after`), but against the frozen vector instead of live data.
  `snapshot_handle` is carried in the request; the server looks up the
  pinned snapshot. No per-page read barrier — the snapshot is already
  pinned.
- `ReleaseSnapshot` — drops the snapshot handle, releasing the page
  refcounts. The next GC sweep can reclaim the pages.
- `SetGcWatermark` (management API only) — explicitly advance the GC
  watermark for a store/group. `gc_slot = min(snapshot_slot, safe_slot)`.
  Data (tombstones, stale versions) with `slot <= gc_slot` becomes
  eligible for reclamation. Active snapshots protect their pinned pages
  via refcount regardless of the GC watermark — GC never frees a page a
  live snapshot still references. This is the existing
  `set_gc_watermark`/`collect_garbage` path, exposed for operational
  control.

**Semantics**:
- A `SnapshotScan` returns a point-in-time-consistent view of the keyspace
  at the pinned `at_slot` — no key vanishes, no value drifts, no phantom
  keys appear. The caller pays one snapshot pin (page-refcount bump on the
  reachable L1 tree) for the duration of the scan; pages are still
  byte-budgeted so each unary response stays bounded. Cost is the same
  merge work as a live scan (the snapshot is a frozen vector, iterated
  with binary-search + linear scan — no `LeafChainCursor`, no epoch guard
  per page).
- The one-time flush cost (draining L0 → L1) briefly stalls the write
  path on the serving replica. For backup workloads this is acceptable;
  for high-write-throughput stores, schedule snapshots during low-write
  windows or accept the brief stall.
- The snapshot is L1-only at `at_slot` — writes that arrive after the
  flush are not included (correct for backup: a consistent durable
  point-in-time view).

**Proto** (new RPCs in `KvService`):
```protobuf
message CreateSnapshotRequest {
  uint64 group_id = 1;
  ReadMode read_mode = 2;   // freshness of the pinned slot
  uint64 min_slot = 3;      // for MIN_SLOT
}
message CreateSnapshotResponse {
  bool   ok = 1;
  string error = 2;
  uint64 snapshot_handle = 3;
  uint64 at_slot = 4;
  KvErrorCode error_code = 5;
}

message ListSnapshotsRequest {
  uint64 group_id = 1;
}
message SnapshotInfo {
  uint64 snapshot_handle = 1;
  uint64 at_slot = 2;
  uint64 lease_remaining_ms = 3;
}
message ListSnapshotsResponse {
  bool   ok = 1;
  string error = 2;
  repeated SnapshotInfo snapshots = 3;
}

message SnapshotScanRequest {
  uint64 snapshot_handle = 1;
  bytes  prefix = 2;
  bytes  start_after = 3;
  uint32 limit = 4;
  uint64 group_id = 5;
}
message SnapshotScanResponse {
  bool   ok = 1;
  string error = 2;
  bool   truncated = 3;
  repeated KvScanItem items = 4;
  KvErrorCode error_code = 5;
}

message ReleaseSnapshotRequest {
  uint64 snapshot_handle = 1;
  uint64 group_id = 2;
}
message ReleaseSnapshotResponse {
  bool   ok = 1;
  string error = 2;
}

service KvService {
  // ... existing RPCs ...
  rpc CreateSnapshot(CreateSnapshotRequest) returns (CreateSnapshotResponse);
  rpc ListSnapshots(ListSnapshotsRequest) returns (ListSnapshotsResponse);
  rpc SnapshotScan(SnapshotScanRequest) returns (SnapshotScanResponse);
  rpc ReleaseSnapshot(ReleaseSnapshotRequest) returns (ReleaseSnapshotResponse);
}
```

**Scope**:
- `lib/crow-kv/src/rpc/proto/kv.proto` — new messages + 4 RPCs.
- `lib/crow-kv/src/rpc/kv_service.rs` — 4 new handlers.
- `lib/crow-kv/src/cluster/px_kv_store.rs` — snapshot handle registry
  (per-store, `HashMap<u64, PinnedSnapshot>` + lease/expiry task),
  `kv_create_snapshot` (flush + `snapshot_view` + register),
  `kv_snapshot_scan` (lookup + iterate), `kv_release_snapshot`,
  `kv_list_snapshots`.
- `lib/crow-kv/src/kv/crow_tree_engine.rs` —
  `engine_snapshot_view()` (wraps existing FFI `snapshot_view`, returns
  a handle that keeps the `PinnedSnapshot` alive),
  `engine_snapshot_scan(handle, prefix, start_after, limit, byte_budget)`
  (binary-search + linear scan over the frozen vector).
- `lib/crow-tree/ffi/src/lib.rs` — `snapshot_view` already returns
  `(at_slot, Vec<ViewEntry>)`; add a `snapshot_view_handle` that returns
  the `ct_view` pointer without consuming it into a `Vec`, plus
  `snapshot_scan_at(handle, prefix, start_after, limit)` that iterates
  the `ct_view` with prefix/pagination. Alternatively, keep the FFI as-is
  and materialize the full `Vec<ViewEntry>` at pin time on the Rust side
  (simpler, O(N) memory at pin time, fine for typical keyspaces).
- `lib/crow-kv-client/src/client.rs` — `snapshot_scan(...)` method
  (create → paginate → release), `create_snapshot`, `release_snapshot`,
  `list_snapshots`.
- Management API (HTTP): `POST /api/stores/{sid}/groups/{gid}/snapshots`,
  `GET /api/stores/{sid}/groups/{gid}/snapshots`,
  `DELETE /api/stores/{sid}/groups/{gid}/snapshots/{handle}`,
  `POST /api/stores/{sid}/groups/{gid}/snapshots/{handle}/scan`.
- CLI: `crow-cli snapshot create/list/scan/release`.
- Tests: integration test that writes concurrently with a
  `snapshot_scan` and asserts the scan observes exactly the keyspace at
  the pinned slot (no vanishing keys, no value drift); handle lease
  expiry test; backward-compat test (existing `scan` unchanged).

**Complexity**: Medium. The engine `snapshot_view` + iterator already
exist — the work is the server-side handle registry with lease/expiry,
the proto/client threading, the management API + CLI surface, and the
prefix/pagination wrapper over the frozen vector. No new engine
machinery (no version chain, no epoch extension, no L0 pinning). Estimate
~500–700 lines across layers.

**Dependencies**: none hard. R56 (end-key bound) composes cleanly with
`SnapshotScan` (same early-stop, just against the frozen vector). R62
(scan deadline/cancellation) applies to `SnapshotScan` the same as
`scan`.

**Acceptance**:
- A `snapshot_scan` started at slot `S` returns a consistent view of the
  keyspace at slot `S` — no key vanishes, no value drifts, no phantom keys
  appear — even as concurrent writes mutate the keyspace during the scan.
- The snapshot handle is released on normal scan completion and is reaped
  by lease expiry if the client disconnects mid-scan (no unbounded pin
  retention; pinned pages become reclaimable by GC after release/expiry).
- A live `scan` (mode 1) is byte-for-byte unchanged (backward compatible).
- `ListSnapshots` returns all active handles with their `at_slot` and
  lease remaining.
- `SetGcWatermark` (management API) advances the GC watermark; active
  snapshots protect their pinned pages from GC regardless of the
  watermark.
- Existing scan tests and `tools/bench-scan-regression.sh` pass; a
  `snapshot_scan` perf baseline is recorded (expected: parity with live
  scan, modulo the one-time flush + pin cost).

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Functionality
→ "No cross-page snapshot isolation (by design, undocumented)". The
user guide will document both read modes (list scan semantics + snapshot
versioning API) together.
