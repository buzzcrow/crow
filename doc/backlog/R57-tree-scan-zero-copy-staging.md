<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R57: Scan — Zero-Copy Engine Result Staging (Pack in `consider`, Transfer Ownership)

**Problem**: the scan path's "zero-copy" claim holds only from the FFI
packed buffer to the client. Inside the engine, each page's result set
is copied **three times** before crossing the FFI boundary:

1. `Crowtree::scan`'s `consider` lambda stages every winning entry via
   `key.to_string()` + `value.to_string()` into a
   `std::vector<scan_entry>` (`crow-tree.cpp:1853` tombstone path,
   `:1868` value path — `out->push_back({.key = key.to_string(), ...,
   .value = std::move(val), ...})`). The overflow case also calls
   `assemble_overflow_value` into a fresh `std::string` (`:1857-1858`).
2. `ct_scan` re-packs those `scan_entry` strings into a single
   `std::string packed` (`c_api.cpp:912-920` — `pack_u32` + `append` +
   `pack_u64` + flag + `pack_u32` + `append` per entry). The async path
   does the same into `impl->scan_packed` (`c_api.cpp:803-808`).
3. `make_buf` mallocs and memcpys `packed` again (`c_api.cpp:921` →
   `:43-54`: `std::malloc(len)` + `std::memcpy`).

For a full 3.5 MiB page (`SCAN_BYTE_BUDGET`) that is ~10.5 MiB of memcpy
plus 2 transient allocations (`std::vector<scan_entry>` + the
intermediate `std::string packed`) that are freed immediately after the
copy. At 32T:32C saturation (~38k scans/s) this staging is on the hot
path of every page.

**Design justification (no profiling needed)**: the three copies are
structurally redundant — the final wire format is the `packed` buffer,
and copy 1 (into `scan_entry`) and copy 2 (into `packed`) produce the
same bytes in two different shapes. Copy 3 (`make_buf`) exists only
because `ct_scan` returns a `ct_buf` that owns its memory, but the
engine already owns the `packed` string and could transfer that
ownership across the FFI instead of copying it. An ownership-transfer
path already exists — `make_borrowed_buf` (`c_api.cpp:62`) returns the
pointer without copying and is used by the get fast path
(`c_api.cpp:847`). The scan path just doesn't use it.

**Solution**: pack the wire format directly in the `consider` lambda
into a single growing buffer, and transfer ownership of that buffer
across the FFI instead of `make_buf`.

- **Engine** (`crow-tree.cpp`): replace the `std::vector<scan_entry>
  *out` parameter of `Crowtree::scan` / `try_scan_no_load` with a
  growing `std::string` (or `buffer`) that the `consider` lambda appends
  the wire format into directly — `pack_u32(key_len)` + key + `pack_u64`
  slot + flag + `pack_u32(value_len)` + value. This collapses copies 1
  and 2 into one append. The overflow case appends the assembled value
  directly instead of staging it in a `std::string` first. The
  byte-budget check stays in `consider` (it already tracks
  `accumulated_bytes`); the limit check stays too. The `scan_entry`
  struct and the `std::vector<scan_entry>` intermediate are removed from
  the scan path.
- **FFI** (`c_api.cpp`): `ct_scan` / `ct_scan_async` receive the packed
  buffer from the engine and transfer ownership to the caller via a new
  ownership-transfer `ct_buf` variant (extend `make_borrowed_buf` with
  an ownership mode, or add a `make_owned_buf` that transfers a
  `std::string`/`buffer` into a `ct_buf` with a free function that
  deletes it — the get fast path's borrowed lifetime is too short for
  scan since the caller polls the future and then decodes). The Rust
  side (`lib.rs`) already calls `ct_free_buf` to release the buffer; the
  free function must match the ownership mode.
- **Rust FFI** (`lib/crow-tree/ffi/src/lib.rs`): `ct_scan` /
  `ct_scan_async` bindings stay the same shape (`ct_buf` in, `ct_free`
  out); the decode path (`decode_scan`) is unchanged because the packed
  format is unchanged. The only difference is that the `ct_buf` now
  owns transferred memory instead of a malloc'd copy.

**Copy count after**: 1 (the single append in `consider`) + 0 across
FFI (ownership transfer) = 1 copy total, down from 3. The remaining
copy is unavoidable (the engine must assemble the wire format from
borrowed L0/L1 slices that may be freed once the epoch guard drops).

**Scope**:
- `lib/crow-tree/src/crow-tree.cpp` — `scan` / `try_scan_no_load`:
  replace `std::vector<scan_entry> *out` with a packed buffer; rewrite
  `consider` to append wire format directly. Remove `scan_entry` from
  the scan path (keep it if other callers use it — check
  `ct_scan_async`'s callback shape).
- `lib/crow-tree/src/c_api.cpp` — `ct_scan` / `ct_scan_async`: remove
  the re-pack loop; add ownership-transfer `ct_buf` construction. Add
  the matching `ct_free` path if a new ownership mode is introduced.
- `lib/crow-tree/src/crow-tree.h` — update `scan` / `try_scan_no_load`
  signatures and the `scan_entry` struct (if removed).
- `lib/crow-tree/ffi/src/lib.rs` — no decode change; verify
  `ct_free_buf` matches the new ownership mode.
- Tests: `test-tree-ct` scan tests must pass unchanged (the packed
  format is identical). Add a test asserting no intermediate
  `std::vector<scan_entry>` allocation on the scan path if feasible.

**Complexity**: Medium. The `consider` rewrite is mechanical, but the
ownership-transfer across the FFI is the delicate part — the `ct_buf`
must remain valid until the Rust caller finishes decoding (the async
future path keeps the buffer alive in `ct_future_impl`; the sync path
returns it directly). The get fast path's `make_borrowed_buf` is the
template, but scan needs owned (not borrowed) memory because the
engine's epoch guard drops before the caller decodes the sync result.

**Dependencies**: none. Independent of R54 (this is a design-level
redundancy, not a profiling-guided micro-optimization). R54 profiling
can still rank this against R58 if both land, but neither needs to wait
for the other.

**Acceptance**:
- The packed wire format is byte-identical to today (all `test-tree-ct`
  scan tests and `tools/bench-scan-regression.sh` pass unchanged).
- No `std::vector<scan_entry>` allocation on the scan path (the
  intermediate struct is gone).
- No `make_buf` malloc+memcpy on the scan path (ownership transfer
  instead).
- A full-page scan (`full_100k` or `bounded_10k` at 64B values) shows
  reduced memcpy volume — measurable via a reduced `loop_ns` / total
  scan time in the bench, or a direct allocation counter if added.

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Performance →
"Engine-side result staging is 3 copies, not zero-copy". Originally
deferred to R54 profiling; raised now because the 3-copy redundancy is
a design-level inefficiency, not a profiling-discovered hot spot.
