<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R61: Scan — Keys-Only / Count-Only Projection

**Problem**: scans always materialize and ship values. In the engine's
`consider` lambda (`crow-tree.cpp:1857-1868`) every non-tombstone entry
pays:

- `v.is_overflow()` ? `assemble_overflow_value(v.overflow_head(),
  v.overflow_len())` : `v.value().to_string()` — for overflow values
  this walks the overflow chain and assembles the full value, the most
  expensive materialization in the scan path.
- `out->push_back({.value = std::move(val), ...})` — stages the value
  into the result vector.
- The packed wire format then serializes key + value per entry, and the
  client receives both as `Bytes`.

For workloads that only need keys — key listing, prefix cardinality,
existence checks, the console UI's key browser — this is pure waste:
the value bytes are materialized, copied across the FFI, serialized on
the wire, and decoded on the client, then discarded. A 16 KiB value
scan that only needs keys ships 16 KiB × N bytes for nothing.

**Solution**: add a `keys_only` projection flag that skips value
materialization and shrinks the wire format.

- **Proto** (`kv.proto`): add `bool keys_only = 10;` to `KvScanRequest`
  (field number follows R56's `end_key = 10` if R56 lands first —
  coordinate the field number; the next free slot after whichever lands
  first). `KvScanResponse` items carry empty `value` fields when
  `keys_only` is set (the field stays present for proto compatibility;
  it's just empty). A `count_only` variant returns zero items and a
  count field instead — falls out of the same engine pushdown (the
  merge loop counts matches instead of staging them).
- **Engine** (`crow-tree.cpp`): `Crowtree::scan` /
  `try_scan_no_load` gain a `bool keys_only` parameter. In the
  `consider` lambda, when `keys_only` is set, skip
  `assemble_overflow_value` / `v.value().to_string()` entirely — stage
  only `key.to_string()` with an empty value. The byte budget then
  accounts for key bytes only, so a `keys_only` page fits far more
  entries per page (fewer round trips). For `count_only`, the lambda
  increments a counter and never stages — the response carries the
  count.
- **FFI** (`ct_scan` / `ct_scan_async`, `lib/crow-tree/ffi/src/lib.rs`):
  add a `keys_only` parameter (and `count_only` if included). Thread
  through `CrowTreeEngine::scan` / `try_scan`
  (`crow_tree_engine.rs`) and `PxKvStore::kv_scan`
  (`px_kv_store.rs`).
- **Client** (`CrowkvClient::scan`): add an optional `keys_only`
  parameter; pass it on every page (fixed, like `prefix`). The
  returned `ScanOutcome.items` carry empty values — or a separate
  `scan_keys` method returns `Vec<Bytes>` directly. For `count_only`,
  a `scan_count` method returns `u64`.
- **gRPC service** (`kv_service.rs::scan`): forward `keys_only` to the
  store.

**Scope**: one new flag per layer (proto, engine, FFI, Rust engine
wrapper, store, service, client), plus the `consider` lambda branch.
The overflow-chain skip is the key win — it removes the most expensive
materialization for large-value keyspaces.

**Complexity**: Low–medium. The engine change is a branch in the
`consider` lambda (skip value assembly). The bulk is threading the
flag through ~6 layers, parallel to the existing `start_after` /
`prefix` plumbing. `count_only` adds a small amount (count field +
counter path) but is naturally included.

**Dependencies**:
- Coordinate the `KvScanRequest` field number with R56 (`end_key`) —
  whichever lands first takes the next free slot, the other follows.
- Independent of R57 (staging copies) — R57 changes how values are
  staged; R61 skips staging values entirely. They compose: a
  `keys_only` scan benefits from R57's single-buffer packing for the
  key-only entries.
- Useful for the console UI key browser (crow-web) — a follow-up can
  wire the UI to use `keys_only` for key-listing views.

**Acceptance**:
- A `keys_only` scan returns the same keys as a full scan over the same
  range, with empty values — no keys dropped or reordered.
- A `keys_only` scan over 16 KiB values is dramatically faster than a
  full scan (no overflow-chain assembly, smaller pages) — measured via
  a bench config with `keys_only=true` and 16 KiB values.
- A `count_only` scan returns the correct count matching
  `keys_only` scan's key count, with zero items shipped.
- `keys_only = false` (default) behaves identically to today.
- Existing scan tests and `tools/bench-scan-regression.sh` pass
  unchanged (default `keys_only = false`).

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Functionality →
"No keys-only / count-only projection".
