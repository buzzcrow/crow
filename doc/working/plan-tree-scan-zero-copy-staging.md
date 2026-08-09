<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R57 Plan: Zero-Copy Engine Scan Result Staging

## Tasks

- [ ] 1. Create `ScanPackedBuf` class in
      `lib/crow-tree/include/crow-tree/scan_packed.h` — growing buffer
      using `std::malloc`/`std::realloc`, with `pack_u32`, `pack_u64`,
      `push_back`, `append`, `release()`, `size()`, `data()`. Move-only.
- [ ] 2. Add optional `ScanPackedBuf *out_packed, size_t *out_count` params
      to `scan` / `try_scan_no_load` in `crow-tree.h` and `crow-tree.cpp`.
      Rewrite `consider` to pack directly when `out_packed != nullptr`.
- [ ] 3. Change `scan_async` / `scan_async_attempt` callback to
      `std::function<void(Status, ScanPackedBuf, bool)>`. Update
      accumulation to use `ScanPackedBuf` + `last_key` + `count` +
      `accumulated_bytes`.
- [ ] 4. Update `c_api.cpp`: `ct_scan` uses `ScanPackedBuf` + `release()`
      into `ct_buf`. `ct_scan_async` callback receives `ScanPackedBuf`,
      stores in `impl`. `ct_future_poll` scan case uses `release()`.
      Remove re-pack loops. Change `impl->scan_packed` from `std::string`
      to `ScanPackedBuf`.
- [ ] 5. Build + run `test-tree-ct` (C++ tests, `std::vector<scan_entry>`
      path unchanged) and `test-tree-ffi` (FFI scan tests, packed format
      unchanged). Both must pass.
- [ ] 6. Lint + commit.

## Files

- `lib/crow-tree/include/crow-tree/scan_packed.h` — NEW: `ScanPackedBuf`
- `lib/crow-tree/include/crow-tree/crow-tree.h` — update `scan`,
  `try_scan_no_load`, `scan_async`, `scan_async_attempt` signatures
- `lib/crow-tree/src/crow-tree.cpp` — `consider` lambda branch,
  `scan_async_attempt` accumulation
- `lib/crow-tree/src/c_api.cpp` — `ct_scan`, `ct_scan_async`,
  `ct_future_poll`, `ct_future_impl`

## Test Checklist

- [ ] `test-tree-ct` passes (395 tests, scan tests use
      `std::vector<scan_entry>` path)
- [ ] `test-tree-ffi` passes (30 tests, scan tests use packed path)
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `clang-format --dry-run --Werror` on changed files
