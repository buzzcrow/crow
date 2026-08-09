<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R59 Design: Two Scan Modes + Snapshot Versioning API

## Problem

The current `scan` RPC is the only range-read surface. It resolves a
fresh read point on every page (`px_kv_store.rs:177`) and reads live
L0+L1 data — a paginated scan is a sequence of per-page-consistent
slices, not one snapshot (S3-list semantics). This is fine for
interactive listing (KV Operator UI), but wrong for backup/analytics
that need a point-in-time-consistent view: a key can vanish (tombstoned
between pages) or a value can drift (overwritten between pages)
mid-scan.

The engine already pins point-in-time L1 views:
`Crowtree::snapshot_view()` returns a `shared_ptr<Snapshot>` pinned at
`last_applied_slot` (zero-copy via `PinnedSnapshot` page refcounts),
exposed through the C API as `ct_snapshot_view` with
`ct_view_iter`/`ct_iter_next` for entry-by-entry iteration, and through
the FFI as `Crowtree::snapshot_view` which returns `(at_slot,
Vec<ViewEntry>)`. After `flush()` drains L0 MemTables into L1, the
snapshot is a complete durable point-in-time view.

The missing piece is not engine machinery — it is exposing the existing
snapshot + iterate path to clients, and documenting the two read modes.

## Proposed Approach

Two range-read modes, sharing the existing engine.

### Mode 1 — List scan (existing `scan` RPC, clarified)

Unchanged. Fast, always returns the latest value per key at each page's
read point. S3-list semantics: per-page-consistent, not cross-page
snapshot. No pinning, no handle, no server-side state beyond the per-page
read barrier. Use case: interactive listing, KV Operator UI, key
discovery.

### Mode 2 — Snapshot versioning API (new)

Four new RPCs in `KvService`:

- **`CreateSnapshot`** — flush L0 → L1, then `snapshot_view()` to pin
  L1 at `last_applied_slot`. Returns a server-side snapshot handle
  (opaque u64 id) and the `at_slot`. The handle is held by a per-group
  registry with a lease/expiry (default 5 min, configurable) to reap
  abandoned snapshots.
- **`ListSnapshots`** — list active snapshot handles for a group with
  their `at_slot` and remaining lease.
- **`SnapshotScan`** — iterate a pinned snapshot with `prefix`,
  `start_after`, `limit`. The server binary-searches the frozen
  `Vec<ViewEntry>` to `start_after`, filters by `prefix`, applies
  `limit`, skips tombstones. Same pagination contract as `scan`
  (`truncated` + `start_after`), but against the frozen vector instead
  of live data. `snapshot_handle` is carried in the request.
- **`ReleaseSnapshot`** — drops the snapshot handle, releasing the
  pinned `Vec<ViewEntry>` (and the underlying `PinnedSnapshot`
  refcounts). The next GC sweep can reclaim the pages.

### Snapshot handle registry

Per-group, stored in `PxGroup`:
```rust
pub(crate) snapshots: DashMap<u64, Arc<SnapshotHandle>>,
```
where:
```rust
pub struct SnapshotHandle {
    pub handle: u64,
    pub at_slot: u64,
    pub entries: Vec<ViewEntry>,  // materialized at pin time
    pub created_at: Instant,
    pub lease: Duration,
}
```

A background task (or lazy sweep on `create`/`list`/`scan`) reaps
expired handles. The handle id is a monotonic `AtomicU64` per store.

### FFI approach

Keep the FFI as-is: `snapshot_view()` materializes the full
`Vec<ViewEntry>` at pin time on the Rust side (simpler, O(N) memory at
pin time, fine for typical keyspaces). No new FFI functions needed —
the existing `Crowtree::snapshot_view()` is called once at
`CreateSnapshot` time, and the `Vec<ViewEntry>` is stored in the handle.
`SnapshotScan` iterates the in-memory `Vec` with binary search + linear
scan — no engine calls per page.

### Pagination

`SnapshotScan` uses the same `start_after` + `limit` + `truncated`
contract as `scan`. The server binary-searches the frozen vector to
find the first entry > `start_after`, then linearly scans filtering by
`prefix` and skipping tombstones until `limit` is reached or the vector
ends.

## Alternatives Considered

1. **Streaming FFI with `ct_view`/`ct_iter`**: keep the `ct_view` handle
   alive across pages, iterate per-page via `ct_iter_next`. Avoids
   materializing the full `Vec<ViewEntry>` at pin time. Rejected — more
   complex FFI threading (handle lifetime, thread safety of `ct_iter`),
   and the simpler materialize-at-pin approach is fine for typical
   keyspaces (the snapshot is already a frozen vector in the engine).

2. **Extend `scan` with a `snapshot_handle` field**: one RPC for both
   modes. Rejected — conflates two different semantics (live vs frozen)
   in one RPC, harder to document and test. Separate RPCs are clearer.

3. **No lease/expiry — explicit release only**: simpler, but a crashed
   client leaves the snapshot pinned forever, blocking GC. Rejected —
   lease/expiry is necessary for production safety.

4. **Per-store handle registry instead of per-group**: fewer registries,
   but `SnapshotScan` needs to find the handle by id regardless of
   group. Per-group is cleaner — the handle is scoped to the group that
   created it, and `ListSnapshots` is per-group.

## Acceptance Test Plan

- A `snapshot_scan` started at slot `S` returns a consistent view of
  the keyspace at slot `S` — no key vanishes, no value drifts, no
  phantom keys appear — even as concurrent writes mutate the keyspace
  during the scan.
- The snapshot handle is released on normal scan completion and is
  reaped by lease expiry if the client disconnects mid-scan.
- A live `scan` (mode 1) is byte-for-byte unchanged (backward
  compatible).
- `ListSnapshots` returns all active handles with their `at_slot` and
  lease remaining.
- Existing scan tests pass unchanged.
- CLI `crow-cli kv snapshot create/list/scan/release` works end-to-end.
