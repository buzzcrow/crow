<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan — Rename crowkv -> crow-kv + restructure into lib/ and app/

## Goal

Rename the `crowkv*` crates to the `crow-kv*` / `crow-*` convention and
reorganize the workspace into `lib/` (libraries) and `app/` (binaries).
Pure rename + move; no functional changes.

## Crate renames

- `crowkv` -> `crow-kv` (lib `crow_kv`)
- `crowkv-client` -> `crow-kv-client` (lib `crow_kv_client`)
- `crowkv-server` -> `crow-kv-server` (bin `crow-kv-server`)
- `crowkv-console-shared` -> `crow-console-shared` (lib `crow_console_shared`)
- `crowkv-web` -> `crow-web` (bin `crow-web`)
- `crowkv-cli` -> `crow-cli` (bin `crow-cli`)
- `crow-common`, `crow-tree-ffi` — unchanged

## Directory moves (git mv)

- `lib/crow-common/` <- `crow-common/`
- `lib/crow-kv/` <- `crowkv/`
- `lib/crow-kv-client/` <- `crowkv-client/`
- `lib/crow-tree/` <- `crow-tree/` (C++ + ffi together)
- `lib/crow-console-shared/` <- `crowkv-console/shared/`
- `app/crow-kv-server/` <- `crowkv-server/`
- `app/crow-web/` <- `crowkv-console/web/`
- `app/crow-cli/` <- `crowkv-console/cli/`

## Compat-sensitive renames (aggressive — confirmed)

- Env vars `CROWKV_*` -> `CROW_KV_*`; `CROWKV_CONSOLE_*` -> `CROW_CONSOLE_*`
- Config dir `~/.crowkv/` -> `~/.crow-kv/`
- Proto package `crowkv.rpc` -> `crow_kv.rpc`

## Hyphen vs underscore rule

- Rust identifiers / `use` / proto package -> `crow_kv` (underscore)
- Filesystem paths, Cargo crate names, `cargo -p`, binary names -> `crow-kv` (hyphen)

## Path-depth note

`app/` crates are one level deeper: path-deps to libs become `../../lib/...`.
lib<->lib and crow-tree/CMake relative refs stay sibling-relative (no change).

## Tasks

- [ ] git mv directories to lib/ and app/
- [ ] update 8 Cargo.toml + root workspace (names, paths, dep keys)
- [ ] bulk text replacements (compound names, env vars, bare crowkv by context)
- [ ] proto package + build.rs refs
- [ ] pixi.toml, .gitignore, ct_lint.py, .githooks/pre-commit, ci.yml
- [ ] console UI (package.json, vite.config, e2e, src)
- [ ] docs (AGENTS, README, design.md, doc_index, backlog, config sample)
- [ ] verify: cargo metadata, fmt, clippy, tests; fix iteratively
- [ ] commit
