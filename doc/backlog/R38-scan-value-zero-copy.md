<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R38: Scan value zero-copy (mirror R6 for the scan path)

**Problem**: The get path is zero-copy after R6:
`PinnedValue::into_bytes()` produces a `Bytes` via `Bytes::from_owner`
backed by the C++ frame, with page refcount pins keeping the frame alive
until the `Bytes` is dropped on any thread. The scan path is not —
`decode_scan` unpacks the C++ packed result buffer (`take_buf`) into
per-entry owned `Vec<u8>` for both key and value. Each entry's value is
copied out of the packed buffer into a fresh allocation. For large-value
range reads this is the dominant copy cost on the read path.

**Target**:
- A `PinnedScanEntry` (or equivalent) type that borrows the C++ packed
  result buffer, with `Bytes::from_owner` for scan values mirroring R6's
  `PinnedValue::into_bytes`.
- The scan result holds a reference to the packed buffer (keeping its
  pages pinned) until all derived `Bytes` handles are dropped, on any
  thread (`Send`).
- `CrowtreeEngine::scan` returns entries whose values are zero-copy
  `Bytes` instead of owned `Vec<u8>`.
- The `KVEngine::scan` trait signature may need to change from
  `Vec<(Vec<u8>, u64, Vec<u8>)>` to `Vec<(Bytes, u64, Bytes)>` (or a
  `PinnedScanEntry` wrapper), aligning with `get_bytes`'s `Bytes`
  return. `InMemKV::scan` would produce `Bytes::from(vec)` (one copy,
  same as its get path).

**Acceptance**:
- A scan returning N large values performs N fewer value copies than
  the current path (measured by benchmark or allocation counting).
- Existing scan tests pass (the trait signature change is internal;
  `KvScanResponse` still serializes `Bytes` into the gRPC frame).
- The packed buffer's pages stay alive until the last `Bytes` is
  dropped, including across thread moves (R6's `Send` guarantee
  extends to scan entries).
- `InMemKV::scan` is updated to the new trait signature without
  regressing its (test-only) performance.

**Dependencies**: None new — builds on R6's `Bytes::from_owner` +
page-refcount mechanism. The `KVEngine::scan` trait change touches all
implementations (`InMemKV`, `CrowtreeEngine`, any test doubles) but is
mechanical.

**Priority**: Medium — matters for large-value range reads; no effect
on point reads or small-value scans.

**Complexity**: Medium-high — the C++ packed scan format must support
borrowing individual entry values out of the buffer (offset + length
into the packed frame, with the frame's pages pinned). The
`KVEngine::scan` trait signature change ripples through the scan
response build path. Must preserve R6's `Send` guarantee for the
derived `Bytes`.

**Files**: `crowtree/ffi/src/lib.rs` (`PinnedScanEntry` / scan result
decode, `Bytes::from_owner` for scan values), `crowkv/src/kv/kv_engine.rs`
(`KVEngine::scan` trait signature), `crowkv/src/kv/crowtree_engine.rs`
(`scan` implementation), `crowkv/tests/kv/mem_kv_impl.rs` (`InMemKV::scan`
signature update), `crowkv/src/cluster/px_kv_store.rs` (`kv_scan`
response build).
