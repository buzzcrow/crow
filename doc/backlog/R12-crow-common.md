<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R12: Crow Common shared project

**Problem**: The crowkv and crowtree codebases contain reusable utility code
that is embedded inside project-specific crates. As the broader storage-system
goal expands to multiple components, each project would need to re-implement or
vend these utilities. Extracting them into a standalone `crow-common` project
eliminates duplication and establishes a shared foundation.

**Approach**:
- Create a new `crow-common/` directory at the workspace root, containing two
  sub-projects. `crow` is the future root project name; `common` is one of its
  sub-lib projects (others, e.g. `crow-tree` from R33, follow the same
  pattern).
  - **`crow-common/rust/`** — a Rust crate (`crow-common`) published as a
    library. Contains the Rust-side shared utilities.
  - **`crow-common/cpp/`** — a C++ static library (`libcrowcommon.a`),
    namespace `crow::common`. Contains the C++-side shared utilities. Only
    static libraries are published — no shared objects — so downstream
    projects link them in without runtime dependency concerns.
- R12 proceeds against the current `crowtree` names; the `crowtree` →
  `crow-tree` / `crowtree::` → `crow::tree` rename is tracked separately as
  R33 (low priority). The extracted `crow-common` code uses the target
  `crow::common` namespace and `CR_LOG_*` / `CROW_HAVE_SPDLOG` brands from
  the start (it is new code); the remaining `crowtree` code keeps
  `crowtree::` / `CT_*` / `CROWTREE_*` until R33.
- Move the following Rust utilities from `crowkv/src/` into
  `crow-common/rust/src/`:
  - **Metrics core** (`crowkv/src/metrics/`) — `Counter`, `Gauge`,
    `LatencyHistogram`, `LatencySummary`, `Bandwidth`, `MetricsRegistry`,
    `MetricsRunner`, `SystemCollector`, `MetricName`. These are generic
    atomic-counter primitives with no crowkv-specific dependencies.
  - **Logging wrapper** (`crowkv/src/common/logging.rs`) —
    `init_file_logging`, `init_file_and_console_logging`, `open_metrics_log`,
    `LogGuards`, `RotatingLogWriter`, `format_timestamp`. These encapsulate
    the `tracing-subscriber` + `file-rotate` initialization with the
    project's naming conventions (`{process_name}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`),
    start/stop/flush lifecycle, and rotation/compression controls.
  - **Time helpers** (`crowkv/src/common/time.rs`) — `process_anchor`,
    `instant_to_anchor_ms`, `anchor_ms_to_instant`. Generic monotonic-time
    utilities.
  - **Operation report** (`crowkv/src/common/report.rs`) —
    `OperationReport`. Generic multi-step error aggregation.
- Move the following C++ utilities from `crowtree/` into
  `crow-common/cpp/`, renamespacing each from `crowtree::` to
  `crow::common`:
  - **CRC32C** (`crowtree/include/crowtree/crc32c.h`,
    `crowtree/src/crc32c.cpp`) — table-driven CRC32C (Castagnoli)
    implementation used for page/checksum/snapshot integrity. Move to
    `crow-common` so other storage components can share it. **Follow-up**:
    replace the hand-rolled table-driven implementation with a mature,
    well-known library (e.g. `crc32c` from Google's `crc32c` project or
    hardware-accelerated SSE4.2 intrinsics via a proven library) to avoid
    maintaining a custom implementation.
  - **Logging facade** (`crowtree/include/crowtree/log.h`,
    `crowtree/src/log.cpp`) — `init_logging`, `shutdown_logging`,
    `logging_enabled`, and the `CT_LOG_*` macros. The spdlog-backed async
    logger with rotating/compressing file sink, naming conventions aligned
    with the Rust side, and start/stop/flush lifecycle. This is a generic
    C++ logging wrapper, not crowtree-specific. **Rename macros**
    `CT_LOG_*` → `CR_LOG_*` (crow log) as part of the move so the shared
    facade carries the `crow` brand rather than the legacy `crowtree`
    prefix; all call sites in `crowtree` are updated in the same commit.
  - **Compressing sink** (`crowtree/include/crowtree/compressing_sink.h`,
    `crowtree/src/compressing_sink.cpp`) — custom spdlog sink with
    size-based rotation + gzip compression. Used by the logging facade;
    moves together with it.
  - **Gzip helper** (`crowtree/include/crowtree/gzip.h`,
    `crowtree/src/gzip.cpp`) — `gzip_compress_file`, used by both
    `compressing_sink` and `metrics`. Must move with them or the moved
    files have dangling `#include "crowtree/gzip.h"`.
  - **Metrics core** (`crowtree/include/crowtree/metrics.h`,
    `crowtree/src/metrics.cpp`) — `Counter`, `Gauge`, `Bandwidth`,
    `LatencyHistogram`, `LatencySummary`, `MetricsRegistry`. A generic
    atomic-counter registry mirroring the Rust metrics core; its only
    includes are `gzip.h` + `log.h` (both moving), with no crowtree-specific
    coupling. By the same inclusion criterion used for the Rust side it
    belongs in `crow-common`.
- Review other utility code in `crowkv/src/common/` and `crowtree/src/` for
  additional candidates (e.g. `config.rs` profiles). Note: the speculative
  "byte-order helpers `put_u32`/`get_u32`" do not exist as named functions
  in `crowkv/src` — that candidate is a no-op. Move only code that is
  genuinely generic and has no project-specific coupling.
- Update `crowkv` and `crowtree` to depend on `crow-common` (Rust: add
  `crow-common` as a workspace dependency; C++: link `libcrowcommon.a` and
  update include paths). Replace the moved code with re-exports or thin
  wrappers so existing call sites compile with minimal changes.
- **C++ namespace bridging**: moved files use `crow::common`; `crowtree`
  call sites previously referenced `crowtree::crc32c`,
  `crowtree::init_logging`, `CT_LOG_*` (in `persist`, `snapshot_io`,
  `page_codec`, `frame_page`, `compressor`, `mapping_persist`, `crowtree`,
  `reactor`, `c_api`). Update these call sites to `crow::common::*` and
  `CR_LOG_*` for the moved utils only — the remaining `crowtree::` engine
  code keeps `crowtree::` until R33. No `namespace crowtree = crow::common;`
  shim.
- **`c_api.{h,cpp}`**: the logging facade is exposed via
  `ct_init_logging`/`ct_flush_logging`/`ct_shutdown_logging`. These stay in
  `crowtree` but their implementation moves, so `c_api.cpp` must include
  `crow-common` headers and `crowtree/CMakeLists.txt` must link
  `libcrowcommon.a` (already covered by the general link rule, but the
  source/include change in `c_api.cpp` is a distinct edit).
- **`crowtree/ffi/build.rs`**: not just an include-path tweak. The FFI
  build globs `crowtree/src/*.cpp` into a single `cc::Build`; removing
  `crc32c.cpp`/`log.cpp`/`compressing_sink.cpp`/`gzip.cpp`/`metrics.cpp`
  from `crowtree/src` means the build must also compile the `crow-common`
  cpp sources — add them to the file set, add the `crow-common/cpp/include`
  dir, and define `CROW_HAVE_SPDLOG` (crow-common's gate) for them while
  keeping `CROWTREE_HAVE_SPDLOG` for the remaining `crowtree` sources, plus
  the zlib link directives.
- **`logging.rs` default `EnvFilter`**: the hardcoded
  `"warn,crowkv=info,crowkv_server=info,crowkv_web=info,crowkv_console_shared=info,crowkv_cli=info"`
  is crowkv-specific and must not be baked into a shared library.
  Parameterize: callers pass the default filter string (crowkv passes its
  own, `crowtree-ffi` passes its own), or keep the crowkv default behind a
  crowkv-side wrapper.
- **`time.rs` visibility**: `process_anchor`, `instant_to_anchor_ms`,
  `anchor_ms_to_instant` are currently `pub(crate)`; bump to `pub` so they
  can be re-exported from `crow-common`.
- **Metrics↔logging intra-crate dep**: `MetricsRunner` depends on
  `common::logging::RotatingLogWriter`. Both move into `crow-common`
  together — the path becomes intra-crate (`crate::logging::RotatingLogWriter`).
  Do not attempt to move the metrics core without the logging wrapper.
- Update `Cargo.toml` workspace members and `pixi.toml` build/test tasks.

**Priority**: Medium — foundational for the multi-component storage-system
roadmap. Extracting now avoids deeper coupling as more components are built.

**Complexity**: Medium — mechanical extraction + dependency wiring. No new
algorithms or protocols. The main risk is breaking existing build/test
paths; mitigated by keeping Rust re-exports at old paths during the
transition. The `CT_LOG_*` → `CR_LOG_*` macro rename touches all `crowtree`
call sites but is mechanical. The broader `crowtree` → `crow-tree` rename
is deferred to R33.

**Files**:
- New: `crow-common/rust/Cargo.toml`, `crow-common/rust/src/` (moved from
  `crowkv/src/metrics/`, `crowkv/src/common/logging.rs`, `time.rs`,
  `report.rs`).
- New: `crow-common/cpp/CMakeLists.txt`, `crow-common/cpp/include/`,
  `crow-common/cpp/src/` (moved from `crowtree/include/crowtree/crc32c.h`,
  `log.h`, `compressing_sink.h`, `gzip.h`, `metrics.h` and their `.cpp`
  counterparts), all renamespaced `crowtree::` → `crow::common`.
- Modified: `Cargo.toml` (workspace members), `pixi.toml`,
  `crowkv/Cargo.toml` (add `crow-common` dependency),
  `crowkv/src/lib.rs` / `crowkv/src/common/mod.rs` (re-export from
  `crow-common`), `crowtree/CMakeLists.txt` (link `libcrowcommon.a`, add
  include path), `crowtree/ffi/build.rs` (compile `crow-common` cpp sources
  in the `cc::Build`, add include dir, define `CROW_HAVE_SPDLOG` for them,
  propagate zlib link), `crowtree/include/crowtree/c_api.{h,cpp}` and all
  `crowtree` call sites of moved utils (`CT_LOG_*` → `CR_LOG_*`,
  `crowtree::` → `crow::common::` for the moved symbols only).

**Acceptance**:
- `pixi run cargo build` and `pixi run test-ct` pass with `crow-common` as a
  workspace member.
- `crowkv` metrics tests (`crowkv/tests/metrics_test.rs`) pass unchanged.
- `crowtree` logging tests (`crowtree/tests/integration/logging_test.cpp`)
  pass with `CR_LOG_*` macros. CRC32C is exercised indirectly via
  `page_codec`/`persist` tests (no dedicated `crc32c_test.cpp` exists).
- `crow-common` Rust crate compiles independently (`cargo build -p
  crow-common`).
- `libcrowcommon.a` builds independently via CMake.
- No functional changes — all moved code is byte-for-byte identical in
  behavior; only the module/crate/namespace boundary and the
  `CT_LOG_*`→`CR_LOG_*` macro names change. The `crowtree` → `crow-tree`
  rename is out of scope (R33).
