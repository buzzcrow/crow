<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design — R12 Crow Common shared project

Working design draft. After implementation this is folded into the
relevant formal design docs and this file is deleted (see
`/implement-requirement` workflow Step 7).

## Problem

`crowkv` and `crowtree` each embed reusable utility code inside
project-specific crates/libraries:

- Rust (`crowkv/src/`): the generic metrics registry
  (`metrics/{counter,gauge,bandwidth,histogram,summary,system,mod}.rs`),
  the `tracing-subscriber` + `file-rotate` logging wrapper
  (`common/logging.rs`), monotonic-time helpers (`common/time.rs`), and
  the multi-step error aggregator (`common/report.rs`).
- C++ (`crowtree/`): `crc32c.{h,cpp}`, the spdlog logging facade
  `log.{h,cpp}` + `compressing_sink.{h,cpp}`, the gzip helper
  `gzip.{h,cpp}`, and the atomic-counter metrics core
  `metrics.{h,cpp}`.

As the storage-system roadmap adds more components (R33 `crow-tree`,
future `crow-*` libs), each would re-implement or vendor these
utilities. Extracting them into a standalone `crow-common` project
establishes a shared foundation and eliminates duplication.

## Current behavior (verified)

- Rust: `crowkv/src/metrics/mod.rs` defines `MetricsRegistry` and
  `MetricsRunner`; `MetricsRunner` holds an
  `Arc<Mutex<crate::common::logging::RotatingLogWriter>>` (intra-crate
  dep on `common::logging`). The submodules `bandwidth/counter/histogram/
  summary/system` are leaf types with no crowkv coupling. `common/logging.rs`
  hardcodes a crowkv-specific `EnvFilter` default
  (`"warn,crowkv=info,crowkv_server=info,crowkv_web=info,crowkv_console_shared=info,crowkv_cli=info"`).
  `common/time.rs` exposes `process_anchor`/`instant_to_anchor_ms`/
  `anchor_ms_to_instant` as `pub(crate)`. `common/report.rs` is
  self-contained. `common/metrics.rs` (LayerMetrics / ElectionMetrics)
  is crowkv-specific and stays.
- C++: the five util TUs live under `crowtree/include/crowtree/` and
  `crowtree/src/`, namespace `crowtree`. `log.h` defines `CT_LOG_*`
  macros gated by `CROWTREE_HAVE_SPDLOG`. `metrics.cpp` includes
  `crowtree/gzip.h` + `crowtree/log.h`; `compressing_sink.cpp` includes
  `crowtree/gzip.h`; `log.cpp` includes `crowtree/compressing_sink.h`.
  `crc32c.h` is header-only inline. Call sites of `CT_LOG_*`:
  `persist.cpp`, `crowtree.cpp`, `reactor.cpp`, `log.h` itself. Call
  sites of `crowtree::init_logging`/`flush_logging`/`shutdown_logging`:
  `c_api.cpp` (wraps them as `ct_*`). `crc32c(...)` is called unqualified
  inside `namespace crowtree {}` blocks in `persist.cpp`,
  `frame_page.cpp`, `page_codec.cpp`, `compressor.cpp`,
  `mapping_persist.cpp`, `snapshot_io.cpp`; the headers
  `frame_page.h`/`page_codec.h`/`compressor.h` only mention `crc32c` in
  comments and include `crc32c.h`.
- Build: `crowtree/CMakeLists.txt` globs `src/*.cpp` into `libcrowtree`,
  links spdlog/zlib PRIVATE, defines `CROWTREE_HAVE_SPDLOG=1`.
  `crowtree/ffi/build.rs` globs `crowtree/src/*.cpp` into one
  `cc::Build`, defines `CROWTREE_HAVE_SPDLOG` when spdlog is found, and
  links spdlog/fmt/zlib. `Cargo.toml` workspace members list does not
  include `crow-common`. `pixi.toml` `build` runs `cmake -S crowtree`
  then `cargo build`.

## Proposed approach

Create `crow-common/` at the workspace root with two sub-projects.

### Rust — `crow-common/rust/`

- New crate `crow-common` (lib). Workspace member + workspace
  dependency. `unsafe_code = deny` via `[lints] workspace = true`.
- Module layout mirrors the moved source:
  - `src/metrics/` ← `crowkv/src/metrics/` (all 6 files + `mod.rs`)
  - `src/logging.rs` ← `crowkv/src/common/logging.rs`
  - `src/time.rs` ← `crowkv/src/common/time.rs`
  - `src/report.rs` ← `crowkv/src/common/report.rs`
  - `src/lib.rs` re-exports `metrics`, `logging`, `time`, `report`.
- Intra-crate path fix: `MetricsRunner`'s
  `crate::common::logging::RotatingLogWriter` → `crate::logging::RotatingLogWriter`.
- Visibility: bump `time.rs` `process_anchor`/`instant_to_anchor_ms`/
  `anchor_ms_to_instant` from `pub(crate)` to `pub`. Bump the metrics
  submodule constructors (`Counter::new`, `Gauge::new`, `Bandwidth::new`,
  `LatencyHistogram::new`, `LatencySummary::new`) and the `flush`/
  `snapshot` methods from `pub(crate)` to `pub` — the registry (now in
  `crow-common`) needs to construct them; external callers go through
  the registry handles, so this does not broaden the usable API surface
  meaningfully. (Alternative: keep `pub(crate)` and gate via a
  `crow-common-internal` feature — rejected, adds friction for no real
  protection since the types are trivial.)
- `EnvFilter` default parameterization: the two `init_*` functions take
  an extra `default_filter: &str` argument. `crowkv`'s re-export wrapper
  passes its existing crowkv string; `crowtree-ffi` (if it ever uses the
  Rust logging) passes its own. This keeps the crowkv default out of the
  shared library. The `open_metrics_log` function needs no change (no
  filter).
- `crowkv` re-exports at old paths so call sites compile unchanged:
  - `crowkv/src/metrics/mod.rs` becomes a thin shim:
    `pub use crow_common::metrics::*;` plus re-exports of submodule
    types. Submodule files (`bandwidth.rs` etc.) are deleted from
    `crowkv`; the `pub mod bandwidth;` declarations are replaced by
    `pub use crow_common::metrics::bandwidth;` etc. so
    `crowkv::metrics::bandwidth::Bandwidth` still resolves.
  - `crowkv/src/common/logging.rs` becomes
    `pub use crow_common::logging::*;` (and re-exports the
    parameterized `init_*` behind crowkv-specific wrappers that supply
    the crowkv default filter, preserving the existing 4-arg signature
    so all crowkv call sites compile unchanged).
  - `crowkv/src/common/time.rs` becomes `pub use crow_common::time::*;`.
  - `crowkv/src/common/report.rs` becomes
    `pub use crow_common::report::*;`.
  - `crowkv/src/common/metrics.rs` (LayerMetrics/ElectionMetrics) stays
    in place — it is crowkv-specific.

### C++ — `crow-common/cpp/`

- New static lib `libcrowcommon.a`, namespace `crow::common`, built by
  `crow-common/cpp/CMakeLists.txt`.
- Header layout: `crow-common/cpp/include/crow-common/<name>.h` so
  includes are `#include "crow-common/log.h"` (mirrors the
  `crowtree/include/crowtree/` pattern; avoids a top-level `crow/` dir
  that could clash with future `crow-tree` headers).
- Moved + renamespaced files:
  - `crc32c.h` → `crow-common/cpp/include/crow-common/crc32c.h`
    (header-only inline; namespace `crow::common`).
  - `log.h` / `log.cpp` → `crow-common/cpp/...`, namespace
    `crow::common`; macros renamed `CT_LOG_*` → `CR_LOG_*`; gate macro
    renamed `CROWTREE_HAVE_SPDLOG` → `CROW_HAVE_SPDLOG` (within the
    moved files only).
  - `compressing_sink.h` / `.cpp` → moved, namespace `crow::common`,
    gate `CROW_HAVE_SPDLOG`.
  - `gzip.h` / `.cpp` → moved, namespace `crow::common`.
  - `metrics.h` / `.cpp` → moved, namespace `crow::common`; includes
    become `"crow-common/gzip.h"` + `"crow-common/log.h"`.
- `crow-common/cpp/CMakeLists.txt`:
  - `add_library(crowcommon STATIC <globbed src>)`.
  - `target_include_directories(crowcommon PUBLIC include)`.
  - `find_package(spdlog REQUIRED)` + `target_link_libraries(crowcommon
    PRIVATE spdlog::spdlog)` + `target_compile_definitions(crowcommon
    PRIVATE CROW_HAVE_SPDLOG=1)`.
  - `find_package(ZLIB REQUIRED)` + link `ZLIB::ZLIB` PRIVATE.
  - SPDLOG_ACTIVE_LEVEL floor like crowtree's.
- `crowtree` consumes `libcrowcommon.a`:
  - `crowtree/CMakeLists.txt`: add `add_subdirectory(../crow-common/cpp
    crow-common-build)` (or `target_link_libraries(crowtree PUBLIC
    crowcommon)` + `target_include_directories(crowtree PUBLIC
    $<TARGET_PROPERTY:crowcommon,INTERFACE_INCLUDE_DIRECTORIES>)`).
    Using `add_subdirectory` keeps the pixi `cmake -S crowtree` command
    working without a separate build step.
  - Remove the moved `.cpp` files from `crowtree/src` (the glob drops
    them automatically) and the moved `.h` files from
    `crowtree/include/crowtree/`.
  - spdlog/zlib link + `CROWTREE_HAVE_SPDLOG` definition stay on
    `libcrowtree` only for the *remaining* crowtree sources that still
    include `crowtree/log.h`... but `crowtree/log.h` is gone. So the
    remaining crowtree sources include `crow-common/log.h` instead and
    use `CR_LOG_*` + `crow::common::*`. `CROWTREE_HAVE_SPDLOG` is no
    longer defined anywhere; the moved `log.h` uses `CROW_HAVE_SPDLOG`
    which is defined on `crowcommon` (PRIVATE). Since `log.h` is a
    public header of `crowcommon` and the macro gate is evaluated in
    including TUs, `CROW_HAVE_SPDLOG` must be visible to includers —
    promote it to PUBLIC on `crowcommon`. (spdlog link stays PRIVATE
    because `libcrowcommon` is static and CMake propagates the link
    requirement to final executables; the *definition* needs to be
    PUBLIC so includers of `log.h` see the spdlog branch.)
- `crowtree` call-site updates (mechanical):
  - All `#include "crowtree/log.h"` → `#include "crow-common/log.h"`;
    `crowtree/crc32c.h` → `crow-common/crc32c.h`;
    `crowtree/gzip.h` → `crow-common/gzip.h`;
    `crowtree/compressing_sink.h` → `crow-common/compressing_sink.h`;
    `crowtree/metrics.h` → `crow-common/metrics.h`.
  - All `CT_LOG_*` → `CR_LOG_*` (in `persist.cpp`, `crowtree.cpp`,
    `reactor.cpp`).
  - `c_api.cpp`: `crowtree::init_logging` → `crow::common::init_logging`,
    `crowtree::flush_logging` → `crow::common::flush_logging`,
    `crowtree::shutdown_logging` → `crow::common::shutdown_logging`.
    The `ct_*` C ABI names are unchanged.
  - `crc32c(...)` calls inside `namespace crowtree {}` blocks →
    `crow::common::crc32c(...)` (qualified, since the unqualified
    lookup no longer finds it in `namespace crowtree`). Files:
    `persist.cpp`, `frame_page.cpp`, `page_codec.cpp`, `compressor.cpp`,
    `mapping_persist.cpp`, `snapshot_io.cpp`.
  - `metrics_test.cpp` / `logging_test.cpp`: update includes + the
    `namespace crowtree {}` test blocks that construct `Counter` etc.
    now need `using namespace crow::common;` or qualified names. The
    tests reference `crowtree::Counter` etc. — update to
    `crow::common::Counter`.
- `crowtree/ffi/build.rs`:
  - Add `crow-common/cpp/src/*.cpp` to the `cc::Build` file set.
  - Add `crow-common/cpp/include` as an include dir.
  - Define `CROW_HAVE_SPDLOG=1` for the crow-common sources (so
    `log.h`'s spdlog branch compiles) — `cc::Build` defines are
    TU-global, so defining `CROW_HAVE_SPDLOG` for the whole build is
    fine (the remaining crowtree sources no longer reference
    `CROWTREE_HAVE_SPDLOG` after the rename).
  - Keep the existing spdlog/fmt/zlib link directives (now needed by
    crow-common sources too).
  - The `crowtree/src/*.cpp` glob automatically drops the moved files.

### Workspace / pixi wiring

- `Cargo.toml`: add `"crow-common/rust"` to `members`; add
  `crow-common = { path = "crow-common/rust" }` to
  `[workspace.dependencies]`.
- `crowkv/Cargo.toml`: add `crow-common = { workspace = true }`.
- `pixi.toml`: no change needed — `build` runs `cmake -S crowtree`
  (which pulls in `add_subdirectory(../crow-common/cpp)`) then
  `cargo build` (which picks up the new workspace member). The
  `ct-fmt` / `ct-lint` search dirs stay `crowtree/...`; add
  `crow-common/cpp` to both so the moved files stay formatted/linted.

## Alternatives considered

- **Keep `pub(crate)` on metrics constructors, gate via feature flag.**
  Rejected — adds a feature indirection for no real protection; the
  types are trivial atomics and the registry is the intended public
  entry point.
- **`namespace crowtree = crow::common;` alias shim in crowtree.** The
  R12 spec explicitly rejects this ("No `namespace crowtree =
  crow::common;` shim") — call sites are updated directly so the
  `crowtree::` brand is gone from the moved utils.
- **Separate `cmake -S crow-common` build step in pixi.** Rejected —
  `add_subdirectory` from `crowtree/CMakeLists.txt` keeps a single
  `cmake -S crowtree` invocation and a single build tree, matching the
  existing pixi task structure.
- **Header path `crow/common/log.h` (mirroring namespace).** Rejected
  for now — creates a `crow/` include root that future `crow-tree`
  headers would share, risking collisions before R33 lands. Use
  `crow-common/` as the include subdir; R33 can reorganize if desired.
- **Move `common/metrics.rs` (LayerMetrics) too.** Rejected — it
  carries `utoipa::ToSchema` + serde derives tied to the crowkv
  management API and has crowkv-specific election counters. Stays in
  `crowkv`.

## Acceptance test plan

- `pixi run cargo build` passes with `crow-common` as a workspace
  member.
- `cargo build -p crow-common` compiles the crate independently.
- `pixi run test-core` — `crowkv/tests/metrics_test.rs` and the
  `metrics::mod.rs` registry unit tests pass unchanged (re-exports
  preserve the `crowkv::metrics::*` paths).
- `pixi run test-ct` — `logging_test.cpp` passes with `CR_LOG_*`;
  `metrics_test.cpp` passes with `crow::common::` types; CRC32C
  exercised indirectly via `page_codec`/`persist` tests.
- `pixi run test-ffi` passes (FFI build compiles crow-common sources,
  links spdlog/zlib).
- `libcrowcommon.a` builds via the `cmake -S crowtree` step (visible in
  build output as the `crowcommon` target).
- No functional changes — moved code is byte-for-byte identical in
  behavior; only module/crate/namespace boundaries and the
  `CT_LOG_*`→`CR_LOG_*` / `CROWTREE_HAVE_SPDLOG`→`CROW_HAVE_SPDLOG`
  macro names change.
