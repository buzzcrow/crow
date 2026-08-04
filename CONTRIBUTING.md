<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Contributing to CROW

Thanks for your interest in contributing! This guide covers setup, conventions, and the PR process.

## Development Environment

CROW uses [Pixi](https://pixi.sh) to pin the C++ toolchain, Rust compiler, and all native dependencies in a single lockfile.

```bash
# Install pixi
curl -fsSL https://pixi.sh/install.sh | sh

# Build everything (crow-tree C++ + Rust workspace + web UI)
pixi run build

# Run all tests
pixi run test-suite

# Lint
pixi run rs-fmt     # Rust format
pixi run rs-lint    # Rust clippy
pixi run tree-fmt     # C++ format
pixi run tree-lint    # C++ lint
```

See `pixi.toml` for the full list of tasks.

## Code Conventions

### Rust

- `unsafe_code = deny` (except `crow-tree-ffi`). Clippy `pedantic = warn`.
- `Px` prefix for Paxos types (e.g. `PxGroupId`, `PxReplicaService`).
- Integration tests only — under each crate's `tests/`. No inline `#[cfg(test)] mod tests`.
- Shared test helpers: `tests/testkit/<topic>.rs`.
- Logging via `tracing` with structured fields, not inline in messages.
- No doc references in code comments — keep docs in `doc/`.

### C++ (crow-tree)

- Follow `.clang-format` and `.clang-tidy` configs.
- GoogleTest for tests under `lib/crow-tree/tests/`.

### Design Docs

- Start at `doc/doc_index.md` — match your task to a row, then open only that doc.
- If you add/rename/rescope a doc, update `doc_index.md` in the same commit.
- See `doc/design/kv/design-crow-kv.md` for architecture context before making non-trivial changes.

## Pull Request Process

1. Fork the repo and create a branch from `main`.
2. Write tests for your changes. All existing tests must pass.
3. Run `pixi run rs-fmt && pixi run rs-lint` before pushing.
4. Keep commits focused — one logical change per commit.
5. Reference the upstream design doc in your commit body (e.g. `design-slot.md §3`).
6. Open a PR with a clear description of what and why.

## Project Structure

| Crate | What it is |
| --- | --- |
| `crow-kv` | Core library: Multi-Paxos consensus, WAL, storage engine, RPC |
| `crow-kv-server` | Server binary: gRPC + HTTP management API |
| `crow-kv-client` | Client library: topology cache, retry, idempotency |
| `crow-tree` | C++ storage engine (B+tree, delta chains, io_uring, buffer pool) |
| `crow-console` | Operations console: web UI (Axum + React) and CLI |

See `AGENTS.md` for a dispatch table on which docs to read for each type of task.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
