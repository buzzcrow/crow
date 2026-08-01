<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R33: Extract crow-tree to separate repo and rename

**Problem**: `crowtree` is the legacy name from before the `crow-*` project
convention was adopted. `crow` is the future root project name; sub-lib
projects follow the `crow-*` pattern (e.g. `crow-common` from R12). The
storage engine should be `crow-tree` to match, with C++ namespace
`crow::tree` instead of `crowtree::`.

Beyond naming, the storage engine has matured into a self-contained
subsystem with its own C++ build, tests, sanitizers, and FFI crate. It
should live in its own git repository so it can be versioned, released,
and consumed independently — `crowkv` declares `crow-tree` as an external
dependency (git or path) rather than carrying it as a workspace member.
This establishes a clean dependency boundary: `crowkv` → `crow-tree`
(analogous to `crowkv` → `crow-common`).

**Approach**:

*Repo extraction:*
- Create a new git repository for `crow-tree` (the `crowtree/` directory
  becomes the repo root). Preserve history via `git filter-repo` or
  subtree split so past commits are retained.
- Move `crowtree/CMakeLists.txt`, `crowtree/src/`, `crowtree/include/`,
  `crowtree/bench/`, and `crowtree/ffi/` into the new repo as top-level
  paths (`CMakeLists.txt`, `src/`, `include/`, `bench/`, `ffi/`).
- Add its own `pixi.toml` (or equivalent) for C++ build/test/sanitizer
  tasks, `.clang-format`, `.clang-tidy`, and CI workflow — the engine's
  tooling becomes self-contained.
- Remove `crowtree/` from the crowkv workspace: delete the workspace
  member entry in `Cargo.toml` and all `pixi.toml` crow-tree tasks
  (`test-ct`, `test-ffi`, `ct-fmt`, `ct-lint`, sanitizer tasks).
- Wire `crowkv` to depend on the new repo: `crow-tree-ffi` becomes a
  `[patch]` or git/path dependency in `crowkv/Cargo.toml` (and
  `crow-common` if it references crow-tree headers). The FFI crate's
  `build.rs` must locate the C++ library via an env var or pkg-config
  rather than relative workspace paths.
- Decide during design whether `crow-common` C++ shared headers stay in
  the crowkv repo (consumed by crow-tree as a git/path dependency) or
  move to the crow-tree repo — prefer keeping `crow-common` independent
  so both repos depend on it.

*Rename (applied inside the new repo):*
- Rename the Rust FFI crate `crowtree-ffi` → `crow-tree-ffi` (crate name
  in `Cargo.toml`, directory `ffi/`, and all `use`/`extern crate`
  references).
- Renamespace all remaining C++ from `crowtree::` → `crow::tree` (the
  non-extracted engine code — everything not moved to `crow-common` by
  R12). R12 already moved shared utils to `crow::common` and renamed
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
  references, `crowkv/Cargo.toml` (dep name), and any docs / scripts
  referencing `crowtree`.

**Priority**: Medium — establishes the dependency architecture for the
`crow-*` ecosystem. The rename portion is cosmetic, but the repo
extraction has real architectural value: independent versioning, clean
build boundary, and reuse potential. Most naturally done after R12
(shared-utils extraction) so `crow-common` is already a separate
dependency and the crow-tree repo can depend on it symmetrically.

**Complexity**: Medium-High — the rename is mechanical (touching every
source file, CMake, `pixi.toml`, `Cargo.toml`), but the repo extraction
adds git history splitting, CI duplication, cross-repo dependency
wiring (FFI `build.rs` must find the C++ lib without workspace-relative
paths), and pixi task redistribution. Main risks: breaking the FFI
build link, losing git history, or creating a circular dependency
between crow-tree and crow-common. Mitigated by doing extraction and
rename as a single coordinated commit per repo.

**Files**:
- New repo root: `crow-tree/` (from `crowtree/`), with `CMakeLists.txt`,
  `src/`, `include/`, `bench/`, `ffi/`, `pixi.toml`, `.clang-format`,
  `.clang-tidy`, CI workflow.
- Removed from crowkv: `crowtree/` directory, workspace member in
  `Cargo.toml`, crow-tree tasks in `pixi.toml`.
- Modified in crowkv: `Cargo.toml` (add `crow-tree-ffi` as external dep),
  `crowkv/Cargo.toml` (dep name), `pixi.toml` (remove ct tasks, add
  dependency-fetch if needed), any scripts under `tools/` referencing
  `crowtree`.
- Modified in crow-tree repo: all `**/*.{h,cpp}` (namespace + macro
  prefixes), `ffi/Cargo.toml`, `ffi/build.rs`, all `tests/**`.

**Acceptance**:
- `crow-tree` repo builds and tests independently: `pixi run build`,
  `pixi run test-ct`, `pixi run test-ffi` pass inside the crow-tree repo.
- `crowkv` builds and tests with `crow-tree-ffi` as an external
  dependency: `pixi run build`, `pixi run test` pass.
- No `crowtree` (lowercase, as a dir/crate/namespace/macro prefix) remains
  in either repo's source or build config (a grep returns zero hits
  outside `doc/` history).
- Git history for the engine code is preserved in the new repo.
- No functional changes — pure extraction + rename.
