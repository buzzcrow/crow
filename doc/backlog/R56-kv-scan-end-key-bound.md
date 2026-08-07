<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R56: Scan — Optional Exclusive `end_key` Range Bound

**Problem**: `KvScanRequest` carries `prefix` + `start_after` but no
upper bound, so an arbitrary `[start, end)` range cannot be expressed.
A caller wanting keys in `["user:1000", "user:2000")` must either pick a
common `prefix` that over-reads (then filter client-side) or scan to the
end of the prefix and discard — both waste engine work and network
bytes. The engine's merge loop already early-stops past `prefix`
(`crow-tree.cpp:1964`: `!winner_key.starts_with(prefix) &&
winner_key.compare(prefix) > 0`); an exclusive `end_key` is the same
shape of check on the upper side.

**Solution**: add an optional exclusive `end_key` to the scan path.
Empty `end_key` preserves today's prefix-only behavior.

- **Proto** (`kv.proto`): add `bytes end_key = 10;` to `KvScanRequest`
  (exclusive upper bound; empty = unbounded, same as today). Bump
  `version` if the field numbering requires it (currently 1; adding a
  field is forward-compatible, so likely no bump needed — confirm
  against the version policy in `kv_service.rs`).
- **Engine** (`crow-tree.cpp`): `Crowtree::scan` /
  `try_scan_no_load` gain a `Slice end_key` parameter. The merge loop's
  early-stop gains `if (!end_key.empty() && winner_key.compare(end_key)
  >= 0) break;` alongside the existing prefix stop. The `consider`
  lambda's prefix filter is unchanged (prefix still applies); `end_key`
  is an additional upper bound, independent of `prefix`. When both are
  set, the effective range is `[start_after, end_key)` intersected with
  the `prefix` range.
- **FFI** (`ct_scan` / `ct_scan_async`, `lib/crow-tree/ffi/src/lib.rs`):
  add `end_key: *const u8` + `end_key_len: usize` parameters mirroring
  `start_after`. Thread through `CrowTreeEngine::scan` / `try_scan`
  (`crow_tree_engine.rs`) and `PxKvStore::kv_scan`
  (`px_kv_store.rs:160`).
- **Client** (`CrowkvClient::scan`): add an optional `end_key`
  parameter; pass it on every page (it is a fixed bound, unlike
  `start_after` which advances per page).
- **gRPC service** (`kv_service.rs::scan`): forward `end_key` to the
  store; on leader-forward (linearizable) carry it through.

**Scope**: one new field per layer (proto, engine, FFI, Rust engine
wrapper, store, service, client). Each is a small, mechanical addition
parallel to the existing `start_after` plumbing.

**Complexity**: Low–medium. No algorithmic change — the merge loop
already does a prefix early-stop; `end_key` reuses the same compare-then-
break pattern. The bulk of the work is threading the parameter through
~6 layers without dropping it.

**Dependencies**:
- Prerequisite shape for **R52 (reverse scan)**: a reverse scan uses
  `start_before` as the upper bound and needs a lower bound to know
  where to stop; `end_key` (or its reverse equivalent) is that bound.
  Implementing the forward `end_key` first establishes the proto/FFI
  shape R52 will mirror.

**Acceptance**:
- A scan with `prefix=""`, `start_after="k010"`, `end_key="k020"`
  returns exactly the keys in `("k010", "k020")` — no client-side
  filtering, no over-read.
- A scan with `end_key` empty behaves identically to today.
- `prefix` + `end_key` together intersect correctly (range is
  prefix-matching keys that are also `< end_key`).
- Pagination across an `end_key`-bounded range produces no duplicates
  and no gaps, and stops at `end_key` (not at prefix end).
- Existing scan tests and `tools/bench-scan-regression.sh` pass
  unchanged (default `end_key` empty).

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Functionality →
"Prefix-only range predicate".
