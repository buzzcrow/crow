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

- All build/test/format commands run under **pixi** — never bare `cargo` or `clang-format`.
- `unsafe_code = deny` (except `crowtree-ffi`); Clippy `pedantic = warn`.
- Markdown is read as raw text — no tables except in `doc/doc_index.md`; use bullet or definition lists.
- `test-util` auto-enabled for tests via self dev-dependency — no flags needed.
- Commit messages and code comments: single line, no doc references or task numbers.
- **One commit per task, not per interaction** — a "task" is a coherent unit of work (e.g. "restructure docs", "add CLI rename", "implement R7"). Before pushing, squash unpushed commits from the same task into one (soft reset to remote tip, re-commit). Before committing, verify no temp/generated files are staged; add to `.gitignore` if needed.
- **Pre-commit quality gate — do not skip:**
  - Lint must pass: `cargo fmt --check`, `cargo clippy -- -D warnings`, `clang-format --dry-run --Werror` (changed `.cpp`/`.h`), `ct-lint` (clang-tidy, changed C++). Fix up to 3 times — always, regardless of cause.
  - Tests: run only relevant tests (Rust or `test-ct`), not the entire suite. Fix up to 3 times; skip pre-existing failures with a stated reason.

## Dispatch — Read Before Acting

| Action | Read first |
| --- | --- |
| Write/modify code | `/coding` workflow (conventions, doc-first) |
| Design or architecture question | `doc/doc_index.md` → match row → open only that doc, grep for `##` section |
| Write/modify docs | `/doc` workflow (hierarchy, naming, formatting rules) |
| Commit changes | Hard Constraints above — no extra doc needed |
| Debug a test failure | `/debug-test` workflow (env check, log-first, data-first, add missing logs) |
| Pre-push review | `/review` workflow (checklist, hot-path rules, clippy exceptions) |
| Implement a new-requirements item | `doc/working/new_requirements.md` → `/implement-requirement` workflow (lifecycle: understand → design → plan → implement → commit → merge → cleanup) |
| User guide / operations | `doc/user-manual/user-guide.md` (quick start, KV ops, cluster management, upgrade, API reference) |
