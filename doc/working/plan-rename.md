<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Rename crow → crowdb Plan

Goal: rename the project brand from `crow` to `crowdb` comprehensively —
crate names, C++ component names, namespaces, type names, env vars, on-disk
markers, docs, repo URL. No backward compatibility is preserved (project is
pre-delivery).

## Naming map

| Context | Old | New |
| --- | --- |
| Project brand | `CROWDB` / `Crow` / `crowdb` / `CrowDB` | `CROWDB` / `CrowDB` / `crowdb` |
| Crate names | `crowdb-kv`, `crowdb-tree`, `crowdb-rpc`, etc. | `crowdb-kv`, `crowdb-tree`, `crowdb-rpc`, etc. |
| Rust crate ident | `crowdb_kv`, `crowdb_tree` | `crowdb_kv`, `crowdb_tree` |
| C++ namespaces | `crow::tree`, `crow::common`, `crow::rpc` | `crowdb::tree`, `crowdb::common`, `crowdb::rpc` |
| C++ includes | `#include "crowdb-tree/..."` | `#include "crowdb-tree/..."` |
| CMake vars/targets | `CROWDB_TREE_*`, `CROWDB_RPC_*` | `CROWDB_TREE_*`, `CROWDB_RPC_*` |
| Log macros | `CR_LOG_*` | `CRB_LOG_*` |
| Env vars | `CROWDB_KV_SERVER_BIN`, etc. | `CROWDB_KV_SERVER_BIN`, etc. |
| On-disk markers | `CROWDB_CT_*`, `CROWDB_WAL_*` | `CROWDB_CT_*`, `CROWDB_WAL_*` |
| File extension | `.ck` | `.crb` |
| Repo URL | `github.com/buzzcrow/crowdb` | `github.com/buzzcrow/crowdb` |

## NOT changed

- `Gian` / `crow.db@outlook.com` — author identity (license headers).
- WAL magic numeric value `0x4352_4F57` — raw bytes on disk; only the
  code comment is updated.

## Phase 1 — C++ components

- [ ] **Step 1: Rename C++ directories** via `git mv`:
  `lib/crowdb-tree` → `lib/crowdb-tree`, `lib/crowdb-rpc` → `lib/crowdb-rpc`,
  `lib/crowdb-common` → `lib/crowdb-common`, `app/crowdb-diskio` →
  `app/crowdb-diskio`. Includes `include/crowdb-tree/` →
  `include/crowdb-tree/` subdirs and `ffi/` subdirs.
- [ ] **Step 2: Update CMakeLists.txt** — `project()` names, targets,
  variables (`CROWDB_TREE_*` → `CROWDB_TREE_*`, etc.), `add_subdirectory`
  paths, include dirs. Files: `lib/crowdb-tree/CMakeLists.txt`,
  `lib/crowdb-rpc/CMakeLists.txt`, `lib/crowdb-common/cpp/CMakeLists.txt`,
  `app/crowdb-diskio/CMakeLists.txt`.
- [ ] **Step 3: Update C++ namespaces & includes** — `crow::` → `crowdb::`
  in all `.cpp`/`.h` (328 namespace decls + `using`), `#include
  "crowdb-tree/..."` → `"crowdb-tree/..."`, `#include "crowdb-common/..."` →
  `"crowdb-common/..."`. Files: all under `lib/crowdb-tree/{src,include,tests,bench}`,
  `lib/crowdb-rpc/{src,include,tests,examples}`, `lib/crowdb-common/cpp/{src,include,tests}`,
  `app/crowdb-diskio/src`.
- [ ] **Step 4: Update log macros** `CR_LOG_*` → `CRB_LOG_*` — definition
  in `lib/crowdb-common/cpp/include/crowdb-common/log.h` + 61 call sites
  across `crowdb-common`, `crowdb-rpc`, `crowdb-tree`, `crowdb-diskio`.
- [ ] **Step 5: Update on-disk text markers** — `CROWDB_CT_*` →
  `CROWDB_CT_*` in `text_codec.cpp` + tests; `crowdb-tree-frame-text` →
  `crowdb-tree-frame-text` in `debug_codec.cpp` + tests; `.ck` → `.crb`
  extension refs in source/tests/docs.
- [ ] **Step 6: C++ build checkpoint** — `cmake --build` for crowdb-tree,
  crowdb-rpc, crowdb-common, crowdb-diskio. Fix any compile errors.

## Phase 2 — Rust workspace

- [ ] **Step 7: Rename Rust crate directories** via `git mv`:
  `lib/crowdb-kv` → `lib/crowdb-kv`, `lib/crowdb-kv-client` →
  `lib/crowdb-kv-client`, `lib/crowdb-common/rust` stays (it's under
  crowdb-common now), `lib/crowdb-protocol` → `lib/crowdb-protocol`,
  `lib/crowdb-console-shared` → `lib/crowdb-console-shared`,
  `lib/crowdb-diskdb-client` → `lib/crowdb-diskdb-client`,
  `lib/crowdb-chunkdb-client` → `lib/crowdb-chunkdb-client`,
  `lib/crowdb-diskio-client` → `lib/crowdb-diskio-client`,
  `lib/crowdb-chunk-client` → `lib/crowdb-chunk-client`,
  `lib/crowdb-test-harness` → `lib/crowdb-test-harness`,
  `app/crowdb-kv-server` → `app/crowdb-kv-server`,
  `app/crowdb-diskdb` → `app/crowdb-diskdb`,
  `app/crowdb-chunkdb` → `app/crowdb-chunkdb`,
  `app/crowdb-web` → `app/crowdb-web`, `app/crowdb-cli` → `app/crowdb-cli`.
  FFI crates: `lib/crowdb-tree/ffi` and `lib/crowdb-rpc/ffi` (dirs already
  renamed in Step 1, just update crate names inside).
- [ ] **Step 8: Update root Cargo.toml** — workspace members paths, repo
  URL (`crowdb` → `crowdb`), `workspace.dependencies` paths. File:
  `Cargo.toml`.
- [ ] **Step 9: Update all crate Cargo.toml** — `name = "crowdb-*"` →
  `"crowdb-*"`, dependency refs (`crowdb-kv = ...` → `crowdb-kv = ...`),
  descriptions (`CROWDB ...` → `CROWDB ...`). Files: every crate's
  `Cargo.toml`.
- [ ] **Step 10: Update Rust source** — type names (`CrowDBConfig` →
  `CrowDBConfig`, `CROWDB_KEY_MAGIC` → `CROWDB_KEY_MAGIC`), `use crowdb_kv`
  → `use crowdb_kv`, doc comments (`CrowDB` → `CrowDB`, `CROWDB` →
  `CROWDB`), env vars (`CROWDB_KV_SERVER_BIN` → `CROWDB_KV_SERVER_BIN`,
  `CROWDB_TEST_LOG` → `CROWDB_TEST_LOG`), on-disk markers
  (`CROWDB_WAL_*` → `CROWDB_WAL_*`), WAL magic comment. Files: all `.rs`
  under `lib/crowdb-*/src/`, `app/crowdb-*/src/`, all `tests/`.
- [ ] **Step 11: Update FFI crates** — `build.rs` paths (`lib/crowdb-tree`
  → `lib/crowdb-tree`), FFI function names if they contain `crowdb_tree`
  → `crowdb_tree`, link refs. Files: `lib/crowdb-tree/ffi/{build.rs,src/*.rs}`,
  `lib/crowdb-rpc/ffi/{build.rs,src/*.rs}`.
- [ ] **Step 12: Rust build checkpoint** — `cargo check && cargo test`.
  Fix any compile errors.

## Phase 3 — Docs & config

- [ ] **Step 13: Top-level docs** — `README.md` (title, badges URL
  `buzzcrow/crowdb` → `buzzcrow/crowdbdb` (GitHub URL unchanged), prose), `AGENTS.md`, `CONTRIBUTING.md`,
  `CHANGELOG.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`.
- [ ] **Step 14: Design/backlog/user-guide docs** — all `doc/design/**/*.md`
  (titles `# CROWDB - Design:` → `# CROWDB - Design:`, prose), `doc/backlog/*.md`,
  `doc/user-manual/user-guide.md`, `doc/user-manual/user-guide.html`
  (title `CrowDB` → `CrowDB`), `doc/doc_index.md`.
- [ ] **Step 15: Skills** — `.devin/skills/*/SKILL.md` (descriptions,
  titles, prose; keep `CROWDB_TEST_LOG` → `CROWDB_TEST_LOG` refs).
- [ ] **Step 16: Config & tooling** — `pixi.toml` (description, task
  cmds, paths `lib/crowdb-tree` → `lib/crowdb-tree`), `.github/**`
  (workflow paths, issue templates), `.githooks/*`, `tools/*`,
  `.cargo/*`.

## Phase 4 — Final verification

- [ ] **Step 17: Full build** — `pixi run build` + relevant tests
  (`cargo check`, `cargo test`, `cmake --build`). Fix any remaining
  references found by compiler/test failures.

## File list (consolidated)

- C++ CMake: `lib/crowdb-tree/CMakeLists.txt`,
  `lib/crowdb-rpc/CMakeLists.txt`, `lib/crowdb-common/cpp/CMakeLists.txt`,
  `app/crowdb-diskio/CMakeLists.txt`.
- C++ source: all `.cpp`/`.h` under `lib/crowdb-tree/`, `lib/crowdb-rpc/`,
  `lib/crowdb-common/cpp/`, `app/crowdb-diskio/`.
- Rust workspace: `Cargo.toml`, `Cargo.lock` (regenerated).
- Rust crates: all `Cargo.toml` + `.rs` under `lib/crowdb-*/`,
  `app/crowdb-*/`.
- FFI: `lib/crowdb-tree/ffi/`, `lib/crowdb-rpc/ffi/`.
- Docs: `README.md`, `AGENTS.md`, `CONTRIBUTING.md`, `CHANGELOG.md`,
  `SECURITY.md`, `CODE_OF_CONDUCT.md`, `doc/**/*.md`,
  `doc/user-manual/user-guide.html`, `doc/doc_index.md`.
- Skills: `.devin/skills/*/SKILL.md`.
- Config: `pixi.toml`, `.github/**`, `.githooks/*`, `tools/*`, `.cargo/*`.

## Test checklist

- [ ] C++ unit tests: `cmake --build` + `ctest` for crowdb-tree,
  crowdb-rpc, crowdb-common.
- [ ] Rust: `cargo check` (workspace compiles).
- [ ] Rust: `cargo test` (unit + integration tests pass).
- [ ] On-disk format: WAL text markers (`CROWDB_WAL_*`) and crowdb-tree
  text markers (`CROWDB_CT_*`) round-trip in tests.
- [ ] Env vars: test harness picks up `CROWDB_KV_SERVER_BIN` etc.
