<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R38 Plan: Scan value zero-copy

## Steps

1. **FFI: `ScanEntry` → `Bytes`** — change `ScanEntry.key`/`.value` from
   `Vec<u8>` to `Bytes`. Rewrite `decode_scan` to convert packed buffer
   to `Bytes` once, then `packed.slice(range)` per entry. Update sync
   `Crowtree::scan` and async `try_scan`/`scan` (they all call
   `decode_scan`).

2. **Trait: `KVEngine::scan` signature** — change return to
   `KVFuture<(Vec<(Bytes, u64, Bytes)>, bool)>`.

3. **`CrowTreeEngine::scan`** — update `decode_scan` helper to map
   `ScanEntry` (Bytes) → `(Bytes, u64, Bytes)` directly.

4. **`InMemKV::scan`** — update return type; `Bytes::from(v.clone())`.

5. **`px_kv_store.rs`** — `KvScanItem { key, value }` directly (already
   Bytes).

6. **Tests** — conformance.rs, crow_tree_engine_test.rs, mem_kv_test.rs,
   ffi_test.rs: mechanical updates.

7. **Lint + test** — fmt, clippy, test-kv-core, test-tree-ffi.

8. **Commit** — single commit.

9. **Merge design** — fold into formal docs (kv-scan-flow-analysis.md,
   design-crow-kv.md engine section).

10. **Cleanup** — delete working docs, update backlog.md.

## Files

- `lib/crow-tree/ffi/src/lib.rs` — `ScanEntry`, `decode_scan`
- `lib/crow-kv/src/kv/kv_engine.rs` — trait signature
- `lib/crow-kv/src/kv/crow_tree_engine.rs` — `scan`, `decode_scan`
- `lib/crow-kv/tests/kv/mem_kv_impl.rs` — `InMemKV::scan`
- `lib/crow-kv/src/cluster/px_kv_store.rs` — response build
- `lib/crow-kv/tests/kv/conformance.rs` — test updates
- `lib/crow-kv/tests/kv/crow_tree_engine_test.rs` — test updates
- `lib/crow-kv/tests/kv/mem_kv_test.rs` — test updates (if any)
- `lib/crow-tree/ffi/tests/ffi_test.rs` — test updates

## Test checklist

- [ ] fmt + clippy clean
- [ ] test-kv-core passes (conformance, engine tests, mem tests)
- [ ] test-tree-ffi passes (ffi scan tests)
- [ ] no new allocations in decode_scan (verified by code reading)
