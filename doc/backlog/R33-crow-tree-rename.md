<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R33: Rename crowtree → crow-tree

**Problem**: `crowtree` is the legacy name from before the `crow-*` project
convention was adopted. `crow` is the future root project name; sub-lib
projects follow the `crow-*` pattern (e.g. `crow-common` from R12). The
storage engine should be `crow-tree` to match, with C++ namespace
`crow::tree` instead of `crowtree::`.

**Approach**:
- Rename the directory `crowtree/` → `crow-tree/`.
- Rename the Rust FFI crate `crowtree-ffi` → `crow-tree-ffi` (crate name in
  `Cargo.toml`, directory `crowtree/ffi/` → `crow-tree/ffi/`, and all
  `use`/`extern crate` references).
- Renamespace all remaining C++ from `crowtree::` → `crow::tree` (the
  non-extracted engine code — everything not moved to `crow-common` by R12).
  R12 already moved shared utils to `crow::common` and renamed
  `CT_LOG_*` → `CR_LOG_*`; R33 does not touch those.
- Rename preprocessor macros / compile defines that carry the `CROWTREE_`
  prefix to `CROW_TREE_` (e.g. `CROWTREE_HAVE_SPDLOG` →
  `CROW_TREE_HAVE_SPDLOG`, `CROWTREE_HAVE_LZ4` → `CROW_TREE_HAVE_LZ4`,
  `CROWTREE_HAVE_LIBURING` → `CROW_TREE_HAVE_LIBURING`,
  `CROWTREE_SANITIZER` → `CROW_TREE_SANITIZER`, `CROWTREE_BENCH` →
  `CROW_TREE_BENCH`, `CROWTREE_BENCH_FOLLY` → `CROW_TREE_BENCH_FOLLY`).
- Update include paths `crowtree/...` → `crow-tree/...` (or keep the
  `crowtree/` include subdir name if preferred to avoid touching every
  `#include` — decide during design; the directory rename is the
  canonical change, the include subdir can stay as a stable header root).
- Update `Cargo.toml` workspace member paths, `pixi.toml` task/path
  references (`test-ct`, `test-ffi`, `build`, `clean`, sanitizer tasks,
  `ct-fmt`, `ct-lint`), `crowkv/Cargo.toml` (dep name), and any docs /
  scripts referencing `crowtree`.

**Priority**: Low — cosmetic / naming consistency. No functional change.
Can be done independently of R12 (R12 proceeds against the current
`crowtree` names; R33 is follow-up cleanup). Safe to do before or after
R12, but most naturally after so R12's extraction is not blocked by an
in-flight rename.

**Complexity**: Medium — mechanical rename touching every `crow-tree`
source file, the FFI crate, CMake, `pixi.toml`, and `Cargo.toml`. No
algorithm or behavior change. Main risk is missing a reference and
breaking the build; mitigated by compiler/linker errors being exhaustive
and the rename being a single commit.

**Files**:
- Renamed: `crowtree/` → `crow-tree/`, `crowtree/ffi/` → `crow-tree/ffi/`.
- Modified: `Cargo.toml`, `pixi.toml`, `crowkv/Cargo.toml`,
  `crow-tree/CMakeLists.txt`, `crow-tree/ffi/Cargo.toml`,
  `crow-tree/ffi/build.rs`, all `crow-tree/**/*.{h,cpp}` (namespace +
  macro prefixes), all `crow-tree/tests/**`, any scripts under `tools/`
  referencing `crowtree`.

**Acceptance**:
- `pixi run build`, `pixi run test-ct`, `pixi run test-ffi` pass.
- No `crowtree` (lowercase, as a dir/crate/namespace/macro prefix) remains
  in source or build config (a grep returns zero hits outside `doc/`
  history).
- No functional changes — pure rename.
