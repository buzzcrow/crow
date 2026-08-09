<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R57 Design: Zero-Copy Engine Scan Result Staging

## Problem

The scan path copies each page's result set **three times** on the C++ side
before crossing the FFI:

1. `Crowtree::scan`'s `consider` lambda stages every winning entry via
   `key.to_string()` + `value.to_string()` into a
   `std::vector<scan_entry>` (`crow-tree.cpp` ~1998 tombstone path,
   ~2023 value path).
2. `ct_scan` re-packs those `scan_entry` strings into a single
   `std::string packed` (`c_api.cpp:916-924` — `pack_u32` + `append` +
   `pack_u64` + flag + `pack_u32` + `append` per entry). The async path
   does the same into `impl->scan_packed` (`c_api.cpp:804-811`).
3. `make_buf` mallocs and memcpys `packed` again (`c_api.cpp:925` →
   `:43-54`: `std::malloc(len)` + `std::memcpy`).

For a full 3.5 MiB page (`SCAN_BYTE_BUDGET`) that is ~10.5 MiB of memcpy
plus 2 transient allocations. The async path has the same 3 copies (the
callback re-packs, `ct_future_poll` does `make_buf`).

## Proposed Approach

**Pack the wire format directly in `consider` into a growing
`ScanPackedBuf`, and transfer ownership of its `malloc`'d buffer across
the FFI instead of `make_buf`.**

### New class: `ScanPackedBuf`

A growing byte buffer using `std::malloc`/`std::realloc` (so
`ct_free_buf`'s `std::free` correctly frees it). Provides:
- `pack_u32`, `pack_u64`, `push_back`, `append(Slice)`,
  `append(const char*, size_t)` — wire-format append helpers.
- `release()` — extracts the raw `uint8_t *` pointer (for FFI ownership
  transfer); the `ScanPackedBuf` no longer owns it.
- `size()`, `data()` — for non-release access (tests, async accumulation).
- Move-only; destructor `std::free`s the buffer if not released.

Defined in `lib/crow-tree/include/crow-tree/scan_packed.h`.

### Engine changes (`crow-tree.cpp` / `crow-tree.h`)

Add optional `ScanPackedBuf *out_packed, size_t *out_count` params to
`scan` / `try_scan_no_load`:

```cpp
Status scan(Slice prefix, ..., std::vector<scan_entry> *out, bool *truncated,
            bool include_tombstones = false,
            ScanPackedBuf *out_packed = nullptr, size_t *out_count = nullptr) const;
```

If `out_packed != nullptr`, the `consider` lambda packs the wire format
directly into it (same format as `ct_scan`'s packed buffer):
`[u32 klen][key][u64 slot][u8 tombstone][u32 vlen][value]` per entry.
If `out_packed == nullptr`, `consider` stages into `std::vector<scan_entry>`
as before (C++ tests use this path, unchanged).

The `consider` branch is predictable — always takes the same path for a
given call — so no branch-prediction cost.

Change `scan_async` / `scan_async_attempt` callback to:
```cpp
std::function<void(Status, ScanPackedBuf, bool truncated)> on_done
```
`scan_async_attempt` accumulates `ScanPackedBuf` across cold-leaf retries
(appending packed entries) + tracks `last_key` (for resume), `count`, and
`accumulated_bytes` separately (previously computed by iterating
`std::vector<scan_entry>`).

### FFI changes (`c_api.cpp`)

- `ct_scan`: pass a `ScanPackedBuf` to `scan`, then `release()` the raw
  pointer into `ct_buf`. No `make_buf`, no re-pack loop.
- `ct_scan_async`: callback receives `ScanPackedBuf`, moves it into
  `impl->scan_packed` (change `impl->scan_packed` from `std::string` to
  `ScanPackedBuf`).
- `ct_future_poll` scan case: `release()` from `impl->scan_packed` into
  `ct_buf`. No `make_buf`.
- Remove the re-pack loops in both `ct_scan` and `ct_scan_async`.

### Rust FFI (`lib.rs`)

No change. `take_buf` still copies the `ct_buf` bytes into a `Vec<u8>` and
calls `ct_free_buf`. The `ct_buf` now owns `malloc`'d memory (from
`ScanPackedBuf::release()`), so `ct_free_buf`'s `std::free` is correct.

### Copy count after

- C++ side: 1 (the single pack in `consider`) + 0 across FFI (ownership
  transfer via `release()`) = 1 copy, down from 3.
- Rust side: 1 (`take_buf` copies into `Vec<u8>`) — unavoidable since
  Rust owns its memory. Total: 2 copies, down from 4.

## Alternatives Considered

1. **Steal `std::string`'s internal buffer**: `std::string` uses
   `::operator new` (typically `malloc`), but there's no standard way to
   extract the pointer without the string freeing it. Implementation-
   defined and unsafe. Rejected.

2. **Add `release()` to `buffer`**: `buffer` already uses `std::malloc`,
   but it's a fixed-size buffer without growing append. Adding growing
   methods to a core data structure is more invasive than a standalone
   `ScanPackedBuf`. Rejected.

3. **Add ownership mode to `ct_buf`**: would change the ABI and affect
   all `ct_buf` users (get, iter, snapshot_export). Rejected — too
   invasive for this optimization.

4. **Keep `make_buf`, eliminate only copies 1 and 2**: pack directly into
   `std::string`, then `make_buf` (malloc + memcpy). Reduces 3 copies to
   2. Rejected — the requirement explicitly wants `make_buf` eliminated.

5. **Keep `std::vector<scan_entry>` for async accumulation**: would not
   eliminate copy 1 from the async path. Rejected — the async path has
   the same 3-copy problem.

## Acceptance Test Plan

- All `test-tree-ct` scan tests pass unchanged (C++ tests use the
  `std::vector<scan_entry>` path, which is unchanged).
- All `test-tree-ffi` scan tests pass (the packed wire format is
  byte-identical; `decode_scan` is unchanged).
- No `make_buf` call on the scan path (grep `c_api.cpp` for `make_buf` in
  `ct_scan` / `ct_scan_async` / `ct_future_poll` scan case — should be
  gone).
- No `std::vector<scan_entry>` on the FFI scan path (the re-pack loops in
  `ct_scan` and `ct_scan_async` are gone).
