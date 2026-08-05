<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R38 Design: Scan value zero-copy

## Problem

The get path is zero-copy after R6 (`PinnedValue::into_bytes` →
`Bytes::from_owner` backed by the C++ frame). The scan path is not:
`decode_scan` (crow-tree-ffi/src/lib.rs:1243) unpacks the C++ packed
result buffer into per-entry owned `Vec<u8>` for both key and value via
`.to_vec()` (lines 1261, 1272). Each entry's key and value is copied
out of the packed buffer into a fresh allocation — 2N copies for N
entries.

## Key finding (simplifies the backlog's estimate)

The backlog estimated "medium-high" complexity assuming the C++ packed
buffer needed page-refcount pinning (like R6's `PinnedValue`). It does
not. `take_buf` (line 324) already copies the packed buffer out of the
C++ allocation into a Rust-owned `Vec<u8>` and frees the C side via
`ct_free_buf`. By the time `decode_scan` runs, the data is already in
a single Rust `Vec<u8>` — no C++ pages to keep pinned.

The zero-copy fix is therefore just `bytes::Bytes` slicing:
`Bytes::from(packed_vec)` is a zero-copy move (takes ownership of the
Vec's allocation), and `packed.slice(pos..pos+len)` returns a new
`Bytes` sharing the same allocation — zero-copy. All slices keep the
allocation alive until the last one is dropped. `Bytes` is `Send` by
default, so R6's cross-thread guarantee extends for free.

## Design

### 1. FFI layer: `ScanEntry` uses `Bytes`

Change `ScanEntry` (crow-tree-ffi/src/lib.rs:503) from `Vec<u8>` to
`Bytes` for `key` and `value`. Update `decode_scan` to convert the
packed buffer to `Bytes` once, then slice per entry instead of
`.to_vec()`. Both the sync `Crowtree::scan` (line 1043) and async
`try_scan`/`scan` (lines 1668, 1695) paths benefit — they all flow
through `decode_scan`.

### 2. `KVEngine::scan` trait signature

Change `KVEngine::scan` (kv_engine.rs:59) return from
`KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)>` to
`KVFuture<(Vec<(Bytes, u64, Bytes)>, bool)>`, aligning with
`get_bytes`'s `Bytes` return.

### 3. `CrowTreeEngine::scan`

Update `decode_scan` (crow_tree_engine.rs:329) to map `ScanEntry`
(now with `Bytes`) directly to `(Bytes, u64, Bytes)` — no conversion
needed.

### 4. `InMemKV::scan`

Update return type; produce `Bytes::from(v.clone())` for values (one
copy, same as its get path — `InMemKV` is test-only).

### 5. `px_kv_store.rs` response build

`for (key, _slot, value) in scanned` — `key` and `value` are now
`Bytes`, so `KvScanItem { key, value }` directly (the current
`Bytes::from(key)` / `Bytes::from(value)` are already zero-copy moves
of `Vec<u8>`; with `Bytes` they're no-ops).

### 6. Tests

conformance.rs, crow_tree_engine_test.rs, mem_kv_test.rs, ffi_test.rs:
mechanical updates (`k.clone()` → `k.to_vec()` where `Vec<u8>` is
needed; `Bytes` comparisons where applicable).

## What is NOT changed

- The C++ packed scan format (`ct_scan` / `ct_scan_async`) — unchanged.
- The `PinnedValue` / `ct_future` mechanism — R6's get path, untouched.
- `KvScanResponse` gRPC serialization — still serializes `Bytes` into
  the gRPC frame (prost treats `Bytes` as a field, no copy).

## Acceptance

- A scan returning N entries performs 0 value copies + 0 key copies
  in `decode_scan` (down from 2N), replaced by 2N zero-copy `Bytes`
  slices.
- Existing scan tests pass.
- `Bytes` is `Send` — cross-thread safety preserved.
- `InMemKV::scan` updated without regressing its test-only performance.
