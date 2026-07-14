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

- `unsafe_code = deny` (except `crowtree-ffi`). Clippy `pedantic = warn`.
- `test-util` feature auto-enabled for tests via self dev-dependency — `cargo test` needs no flags.
- Commit messages: single line, no doc references or task numbers. Code comments: same rule.
- **Pre-commit quality gate — do not skip:**
  - `cargo fmt --all -- --check` must pass.
  - `cargo clippy --all-targets -- -D warnings` must pass.
  - `clang-format --dry-run --Werror` on all changed `.cpp`/`.h` files must pass.
  - C++ tests (`crowtree_tests`) must pass.
  - Only skip if the toolchain itself is broken (e.g. cargo too old to download deps) and no fix is available — state the reason explicitly.
  - Use the pixi env toolchain (`PATH=$PIXI_ENV/bin:$PATH`) when the system cargo is too old.

## Dispatch — Read Before Acting

| Action | Read first |
| --- | --- |
| Write/modify code | `/coding` workflow (conventions, doc-first, pre-commit, commit) |
| Design or architecture question | `doc/doc_index.md` → match row → open only that doc, grep for `##` section |
| Write/modify docs | `/doc` workflow (hierarchy, naming, formatting rules) |
| Pre-push review | `/review` workflow (checklist, hot-path rules, clippy exceptions) |
| Operator procedures | `doc/procedures.md` (bootstrap, upgrade, replacement, API) |
