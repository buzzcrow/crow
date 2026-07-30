<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R34: Introduce ISA-L dependency for SIMD-optimized CRC32C, EC, and deflate compression

**Problem**: The C++ CRC32C implementation in
`crow-common/cpp/include/crow-common/crc32c.h` is a software table-driven
loop — correct but slow on data paths that checksum every durable page and
the superblock. ISA-L (Intel Storage Acceleration Library) provides a
runtime-dispatched `crc32_iscsi` that selects the best SIMD implementation
(SSE4.2+PCLMULQDQ → AVX → AVX2 → AVX512 by16) at first call, giving
5–20× throughput on modern x86 and NEON-optimized paths on ARM. ISA-L also
provides Reed-Solomon erasure codes (GF(2^8)) and deflate-compatible
compression (igzip) — both future CrowKV needs.

**Scope**: Three phases, only Phase 1 is this item's deliverable.

**Phase 1 — Replace C++ CRC32C with ISA-L (this item)**

- Add `isa-l` to `pixi.toml` dependencies.
- Update `crow-common/cpp/CMakeLists.txt` to `find_package(isa-l)` (or
  `find_library`/`find_path` fallback) and link `crowcommon` against it.
- Rewrite `crow-common/cpp/include/crow-common/crc32c.h` to delegate to
  `crc32_iscsi()` from `<isa-l/crc.h>`, preserving the existing API:
  - `crc32c(data, len)` → `crc32_iscsi(buffer, len, 0)`
  - `crc32c_update(crc, data, len)` → `crc32_iscsi(buffer, len, crc)`
  - The `crc32_iscsi` signature uses `int len` (signed 32-bit) and
    `unsigned int init_crc`; add a static cast and a debug assert that
    `len <= INT32_MAX` (no buffer in practice exceeds 2 GiB).
  - Remove the table-driven `detail::crc32c_table()` and the software loop.
  - Keep the header inline-friendly: `crc32_iscsi` is a C function with
    external linkage, so the wrapper is a thin `inline` call — no TU bloat.
- Verify CRC value compatibility: ISA-L `crc32_iscsi` uses the same
  Castagnoli polynomial (0x1EDC6F41) with the same reflected/seeded
  convention as the current implementation. Existing tests that check CRC
  values (e.g. `frame_page_test`, `persist_test`, `snapshot_io_test`) serve
  as regression validation — no new test needed beyond ensuring the
  existing suite passes.
- Optional: add a micro-benchmark comparing old table-driven vs ISA-L
  throughput for 4 KiB / 64 KiB / 1 MiB buffers.

**Phase 2 — ISA-L erasure codes (future, separate item)**

- ISA-L provides `ec_encode_data` / `ec_decode_data` for Reed-Solomon
  GF(2^8) erasure codes with SIMD-optimized matrix operations.
- Applicable when CrowKV adds data redundancy beyond Paxos replication
  (e.g. erasure-coded page segments in `BlockPageStore`, or cross-rack
  redundancy for cold data).
- Not blocked by Phase 1 — can be picked up independently once the ISA-L
  dependency is in place.

**Phase 3 — ISA-L deflate compression (future, separate item)**

- ISA-L provides `igzip` — deflate-compatible compression with AVX/SIMD
  optimization, significantly faster than zlib deflate at similar ratios.
- Currently crowtree uses LZ4 for page compression (PT10). ISA-L deflate
  could be added as an alternative compression algorithm in
  `crowtree/src/compressor.cpp` alongside the existing LZ4 codec, selected
  via `CompressionAlgo` enum.
- `doc/todo_tree.md` lists "fully vendor LZ4 source" as a follow-up; ISA-L
  deflate is an orthogonal option — both can coexist.
- Not blocked by Phase 1 or Phase 2.

**Priority**: Medium — CRC32C is on every durable write path (page flush,
superblock commit, snapshot export). The performance gain is proportional
to write throughput, which is the current optimization focus (R16a/R17/R30).

**Complexity**: Low (Phase 1) — thin wrapper + CMake/pixi wiring. The API
surface is unchanged; all callers of `crow::common::crc32c` /
`crc32c_update` continue to work without modification.

**Platform compatibility**:
- ISA-L v2.31.1 is on conda-forge for linux-64, linux-aarch64,
  linux-ppc64le, osx-64, osx-arm64, win-64 — covers all pixi platforms.
- CRC32C (`crc32_iscsi`) is available on all platforms with
  architecture-appropriate SIMD (x86: SSE4.2/AVX/AVX2/AVX512; ARM: NEON).
- macOS arm64 support was fixed in v2.31.1 (conda-forge includes the
  patches).

**Rust side**: No change. The `crc32c = "0.6"` crate already has SSE4.2
hardware acceleration with runtime cpuid detection on x86-64 and a software
fallback on other architectures. It is used in `crowkv/src/wal/record.rs`
and `crowkv/src/wal/segment.rs`. Switching the Rust WAL to ISA-L via FFI
would add complexity for no measurable gain.

**Files**:
- `pixi.toml` — add `isa-l = "*"` dependency
- `crow-common/cpp/include/crow-common/crc32c.h` — rewrite to delegate to
  `crc32_iscsi`
- `crow-common/cpp/CMakeLists.txt` — find and link ISA-L

**Acceptance**:
- `pixi run test-ct` passes (all 176+ C++ tests, including CRC-dependent
  tests in frame_page, persist, snapshot_io, compressor, page_codec,
  mapping_persist).
- `pixi run ct-asan`, `pixi run ct-tsan`, `pixi run ct-ubsan` all clean.
- `cargo test -p crowkv --test wal` passes (Rust WAL CRC unaffected).
- No new compile warnings or Clippy/clang-tidy findings.
