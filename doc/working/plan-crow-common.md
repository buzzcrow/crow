<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan — R12 Crow Common shared project

Tracks implementation progress. See
`doc/working/design-crow-common.md` for the design.

## Task breakdown

### A. Rust crate `crow-common`

- [ ] A1. Create `crow-common/rust/Cargo.toml` (lib, workspace
      metadata, `[lints] workspace = true`, deps: tokio, tracing,
      tracing-appender, tracing-subscriber, flate2, tempfile dev-dep).
- [ ] A2. Move `crowkv/src/metrics/{mod,bandwidth,counter,histogram,
      summary,system}.rs` → `crow-common/rust/src/metrics/`.
- [ ] A3. Move `crowkv/src/common/logging.rs` →
      `crow-common/rust/src/logging.rs`; move `time.rs` →
      `crow-common/rust/src/time.rs`; move `report.rs` →
      `crow-common/rust/src/report.rs`.
- [ ] A4. Write `crow-common/rust/src/lib.rs` re-exporting
      `metrics`, `logging`, `time`, `report`.
- [ ] A5. Fix intra-crate path in `metrics/mod.rs`:
      `crate::common::logging::RotatingLogWriter` →
      `crate::logging::RotatingLogWriter`.
- [ ] A6. Bump visibility: `time.rs` `pub(crate)` → `pub`; metrics
      submodule `new`/`flush`/`snapshot` `pub(crate)` → `pub`.
- [ ] A7. Parameterize `logging.rs` `init_file_logging` /
      `init_file_and_console_logging` with a `default_filter: &str`
      arg.
- [ ] A8. `cargo build -p crow-common` passes.

### B. `crowkv` re-exports

- [ ] B1. `crowkv/Cargo.toml`: add `crow-common = { workspace = true }`.
- [ ] B2. `crowkv/src/metrics/mod.rs` → shim: re-export from
      `crow_common::metrics`; replace `pub mod <sub>;` with
      `pub use crow_common::metrics::<sub>;` for each submodule;
      delete the moved submodule files from `crowkv/src/metrics/`.
- [ ] B3. `crowkv/src/common/logging.rs` → shim: re-export
      `crow_common::logging::*` plus crowkv-specific wrappers for
      `init_file_logging` / `init_file_and_console_logging` that
      supply the crowkv default filter and keep the existing 4-arg
      signature.
- [ ] B4. `crowkv/src/common/time.rs` → `pub use crow_common::time::*;`.
- [ ] B5. `crowkv/src/common/report.rs` →
      `pub use crow_common::report::*;`.
- [ ] B6. `pixi run cargo build` (crowkv) passes; `pixi run test-core`
      passes (metrics_test.rs + registry unit tests unchanged).

### C. C++ lib `libcrowcommon`

- [ ] C1. Create `crow-common/cpp/include/crow-common/` and
      `crow-common/cpp/src/`.
- [ ] C2. Move + renamespace `crc32c.h` (header-only, `crow::common`).
- [ ] C3. Move + renamespace `log.h` / `log.cpp`: `crow::common`,
      `CT_LOG_*` → `CR_LOG_*`, `CROWTREE_HAVE_SPDLOG` →
      `CROW_HAVE_SPDLOG`, include `crow-common/compressing_sink.h`.
- [ ] C4. Move + renamespace `compressing_sink.h` / `.cpp`:
      `crow::common`, `CROW_HAVE_SPDLOG`, include `crow-common/gzip.h`.
- [ ] C5. Move + renamespace `gzip.h` / `.cpp`: `crow::common`.
- [ ] C6. Move + renamespace `metrics.h` / `.cpp`: `crow::common`,
      includes `crow-common/gzip.h` + `crow-common/log.h`.
- [ ] C7. Write `crow-common/cpp/CMakeLists.txt` (static lib
      `crowcommon`, PUBLIC include dir, PUBLIC `CROW_HAVE_SPDLOG=1`,
      PRIVATE spdlog + zlib link, SPDLOG_ACTIVE_LEVEL floor).

### D. `crowtree` consumes `libcrowcommon`

- [ ] D1. `crowtree/CMakeLists.txt`: `add_subdirectory(../crow-common/cpp
      crow-common-build)`; `target_link_libraries(crowtree PUBLIC
      crowcommon)`; drop the now-redundant spdlog/zlib PRIVATE link +
      `CROWTREE_HAVE_SPDLOG` define (the moved sources are gone; the
      remaining crowtree sources get spdlog via `crowcommon`'s PUBLIC
      definition). Keep SPDLOG_ACTIVE_LEVEL on `crowtree` for its
      remaining includers of `crow-common/log.h`.
- [ ] D2. Delete moved `.h` from `crowtree/include/crowtree/` and moved
      `.cpp` from `crowtree/src/` (glob auto-drops the .cpp).
- [ ] D3. Update `crowtree` call sites: includes
      `crowtree/{log,crc32c,gzip,compressing_sink,metrics}.h` →
      `crow-common/...`; `CT_LOG_*` → `CR_LOG_*` (persist.cpp,
      crowtree.cpp, reactor.cpp); `crowtree::init_logging` etc. in
      c_api.cpp → `crow::common::...`; `crc32c(...)` inside
      `namespace crowtree {}` → `crow::common::crc32c(...)` (persist,
      frame_page, page_codec, compressor, mapping_persist,
      snapshot_io).
- [ ] D4. Update tests: `metrics_test.cpp` / `logging_test.cpp`
      includes + `crowtree::` → `crow::common::` in test namespace
      blocks.
- [ ] D5. `pixi run test-ct` passes.

### E. FFI build

- [ ] E1. `crowtree/ffi/build.rs`: add `crow-common/cpp/src/*.cpp` to
      the file set; add `crow-common/cpp/include` include dir; define
      `CROW_HAVE_SPDLOG=1` (replaces `CROWTREE_HAVE_SPDLOG`); keep
      spdlog/fmt/zlib link directives.
- [ ] E2. `pixi run test-ffi` passes.

### F. Workspace + pixi wiring

- [ ] F1. Root `Cargo.toml`: add `"crow-common/rust"` to members;
      add `crow-common = { path = "crow-common/rust" }` to
      `[workspace.dependencies]`.
- [ ] F2. `pixi.toml`: add `crow-common/cpp` to `ct-fmt` find dirs and
      to `ct_lint.py` `SEARCH_DIRS`.
- [ ] F3. `pixi run build` passes end-to-end.

### G. Quality gate + commit

- [ ] G1. `pixi run cargo fmt --all -- --check`.
- [ ] G2. `pixi run cargo clippy --all-targets -- -D warnings`.
- [ ] G3. `pixi run ct-fmt` then `clang-format --dry-run --Werror` on
      changed C++.
- [ ] G4. `pixi run ct-lint` on changed C++.
- [ ] G5. Commit implementation + design/plan docs.

## File list

New:
- `crow-common/rust/Cargo.toml`, `crow-common/rust/src/lib.rs`,
  `crow-common/rust/src/{logging,time,report}.rs`,
  `crow-common/rust/src/metrics/{mod,bandwidth,counter,histogram,summary,system}.rs`.
- `crow-common/cpp/CMakeLists.txt`,
  `crow-common/cpp/include/crow-common/{crc32c,log,compressing_sink,gzip,metrics}.h`,
  `crow-common/cpp/src/{log,compressing_sink,gzip,metrics}.cpp`.
- `doc/working/design-crow-common.md`, `doc/working/plan-crow-common.md`.

Moved (deleted from old location):
- `crowkv/src/metrics/{mod,bandwidth,counter,histogram,summary,system}.rs`.
- `crowkv/src/common/{logging,time,report}.rs` (replaced by shims).
- `crowtree/include/crowtree/{crc32c,log,compressing_sink,gzip,metrics}.h`.
- `crowtree/src/{log,compressing_sink,gzip,metrics}.cpp`.

Modified:
- `Cargo.toml` (workspace members + deps).
- `crowkv/Cargo.toml` (add `crow-common` dep).
- `crowkv/src/metrics/mod.rs` (shim).
- `crowkv/src/common/{logging,time,report}.rs` (shims).
- `crowtree/CMakeLists.txt` (add_subdirectory + link).
- `crowtree/ffi/build.rs` (compile crow-common sources).
- `crowtree/src/{persist,frame_page,page_codec,compressor,mapping_persist,snapshot_io,crowtree,reactor,c_api}.cpp`
  (includes + `CR_LOG_*` + `crow::common::`).
- `crowtree/tests/{unit/metrics_test,integration/logging_test}.cpp`.
- `pixi.toml` (`ct-fmt` dirs), `tools/ct_lint.py` (`SEARCH_DIRS`).

## Test checklist

- [ ] `cargo build -p crow-common` (independent crate build).
- [ ] `pixi run test-core` (metrics_test.rs + registry unit tests).
- [ ] `pixi run test-ct` (logging_test.cpp, metrics_test.cpp,
      page_codec/persist CRC tests).
- [ ] `pixi run test-ffi` (FFI build links crow-common).
- [ ] `pixi run cargo build` (full workspace).
