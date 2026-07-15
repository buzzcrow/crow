<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV

Distributed KV store: Paxos consensus, per-key slots, WAL durability, crowtree storage engine.
Rust workspace + C++ storage engine (via FFI).

## Crates

- **`crowkv`** — core lib: consensus, engine, WAL, I/O, RPC, reconfiguration.
- **`crowkv-client`** — client library (retry, topology cache, `NotLeaderHint`).
- **`crowkv-server`** — binary: CLI, HTTP management API, store/group/replica wiring.
- **`crowkv-console/{shared,web,cli}`** — management console (Axum+React web, `clap` CLI, shared core).
- **`crowtree/ffi`** — Rust FFI bindings to C++ crowtree storage engine.

## Hard Constraints

- All build/test/format commands run under **pixi** (`pixi run cargo ...`, `pixi exec clang-format ...`, `pixi run test-ct`, etc.) — never call bare `cargo` or `clang-format`.
- `unsafe_code = deny` (except `crowtree-ffi`). Clippy `pedantic = warn`.
- Markdown docs are read as raw text — avoid tables except in `doc/doc_index.md`. Use bullet lists or definition lists instead.
- `test-util` feature auto-enabled for tests via self dev-dependency — `cargo test` needs no flags.
- Commit messages: single line, no doc references or task numbers. Code comments: same rule.
- **Pre-commit quality gate — do not skip:**
  - `pixi run cargo fmt --all -- --check` and `pixi run cargo clippy --all-targets -- -D warnings` must pass.
  - `pixi exec clang-format --dry-run --Werror` on changed `.cpp`/`.h` files must pass.
  - Run tests relevant to the changed code (Rust or C++ `pixi run test-ct`), not the entire suite.
  - Only skip if the toolchain is broken and unfixable — state the reason explicitly.

## Dispatch — Read Before Acting

| Action | Read first |
| --- | --- |
| Write/modify code | `/coding` workflow (conventions, doc-first) |
| Design or architecture question | `doc/doc_index.md` → match row → open only that doc, grep for `##` section |
| Write/modify docs | `/doc` workflow (hierarchy, naming, formatting rules) |
| Commit changes | Hard Constraints above — no extra doc needed |
| Debug a test failure | `/debug-test` workflow (env check, log-first, data-first, add missing logs) |
| Pre-push review | `/review` workflow (checklist, hot-path rules, clippy exceptions) |
| Implement a new-requirements item | `doc/working/new_requirements.md` → `/implement-requirement` workflow (lifecycle: understand → design → plan → implement → merge → cleanup) |
| Operator procedures | `doc/procedures.md` (bootstrap, upgrade, replacement, API) |
