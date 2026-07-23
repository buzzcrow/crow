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
  sub-projects:
  - **`crow-common/rust/`** — a Rust crate (`crow-common`) published as a
    library. Contains the Rust-side shared utilities.
  - **`crow-common/cpp/`** — a C++ static library (`libcrowcommon.a`).
    Contains the C++-side shared utilities. Only static libraries are
    published — no shared objects — so downstream projects link them in
    without runtime dependency concerns.
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
  `crow-common/cpp/`:
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
    `logging_enabled`, `CT_LOG_*` macros. The spdlog-backed async logger
    with rotating/compressing file sink, naming conventions aligned with the
    Rust side, and start/stop/flush lifecycle. This is a generic C++ logging
    wrapper, not crowtree-specific.
  - **Compressing sink** (`crowtree/include/crowtree/compressing_sink.h`,
    `crowtree/src/compressing_sink.cpp`) — custom spdlog sink with
    size-based rotation + gzip compression. Used by the logging facade;
    moves together with it.
- Review other utility code in `crowkv/src/common/` and `crowtree/src/` for
  additional candidates (e.g. `config.rs` profiles, byte-order helpers
  `put_u32`/`get_u32` used across persist/snapshot codecs). Move only code
  that is genuinely generic and has no project-specific coupling.
- Update `crowkv` and `crowtree` to depend on `crow-common` (Rust: add
  `crow-common` as a workspace dependency; C++: link `libcrowcommon.a` and
  update include paths). Replace the moved code with re-exports or thin
  wrappers so existing call sites compile with minimal changes.
- Update `Cargo.toml` workspace members and `pixi.toml` build/test tasks.

**Priority**: Medium — foundational for the multi-component storage-system
roadmap. Extracting now avoids deeper coupling as more components are built.

**Complexity**: Medium — mechanical extraction + dependency wiring. No new
algorithms or protocols. The main risk is breaking existing build/test paths;
mitigated by keeping re-exports at old paths during the transition.

**Files**:
- New: `crow-common/rust/Cargo.toml`, `crow-common/rust/src/` (moved from
  `crowkv/src/metrics/`, `crowkv/src/common/logging.rs`, `time.rs`,
  `report.rs`).
- New: `crow-common/cpp/CMakeLists.txt`, `crow-common/cpp/include/`,
  `crow-common/cpp/src/` (moved from `crowtree/include/crowtree/crc32c.h`,
  `log.h`, `compressing_sink.h` and their `.cpp` counterparts).
- Modified: `Cargo.toml` (workspace members), `pixi.toml`,
  `crowkv/Cargo.toml` (add `crow-common` dependency),
  `crowkv/src/lib.rs` / `crowkv/src/common/mod.rs` (re-export from
  `crow-common`), `crowtree/CMakeLists.txt` (link `libcrowcommon.a`),
  `crowtree/ffi/build.rs` (C++ include path update).

**Acceptance**:
- `pixi run cargo build` and `pixi run test-ct` pass with `crow-common` as a
  workspace member.
- `crowkv` metrics tests (`crowkv/tests/metrics_test.rs`) pass unchanged.
- `crowtree` CRC32C and logging tests pass unchanged.
- `crow-common` Rust crate compiles independently (`cargo build -p
  crow-common`).
- `libcrowcommon.a` builds independently via CMake.
- No functional changes — all moved code is byte-for-byte identical in
  behavior; only the module/crate boundary changes.
