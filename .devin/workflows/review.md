<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: Review Rust code
---

# CROW - Code Review

Goal: lean, correct, no dead code.

## Hot-Path Rules

Hot paths: `propose`, `accept`, `learn`, `kv_get`, `kv_put`, `kv_delete`, `kv_batch_write`.

- Prefer `&T` over `T` in signatures.
- No mutex on critical paths — use atomics. `std::sync::Mutex` ok for `start`/`stop` lifecycle. `tokio::sync::Mutex` only when held across `.await`.
- Pre-size collections (`with_capacity`) or use stack.

## Checklist

1. **Health & Info exposure** — when adding internal state, consider if it should be exposed via `HealthStatus` variants or info struct fields. Default to exposing useful internal state.
2. **Comments** — `//!` for module purpose, `///` for items, inline for non-obvious logic. No doc references in code. TODO/FIXME tracked in `doc/todo_code.md`.
3. **Clone** — review every `#[derive(Clone)]` and `.clone()`. In hot paths, justify with a comment if non-trivial overhead is accepted.
4. **Arc** — drop inner `Arc` when parent is already `Arc`. Return `&T` instead of `Arc<T>` when parent keeps it alive. Inner `Arc` only if it outlives parent (e.g. moved into `tokio::spawn`).
5. **Mutex** — `std::sync::Mutex` for short non-async sections; `tokio::sync::Mutex` only across `.await`; remove if all state is atomic.
6. **Enum vs `dyn Trait`** — prefer enum dispatch. `dyn` only for open-ended / cross-crate.
7. **Dead code** — remove unused types/imports/fields/methods/deps. Collapse always-same-value enum variants to unit.
8. **Duplication** — move shared helpers to `common/` or `rpc/`.
9. **Errors** — no `panic!` in non-test code. Replace `OnceCell::get_or_init + unwrap` with `get_or_try_init`. gRPC client init must propagate.
10. **Naming** — `Px` prefix for Paxos types. `&self` when interior mutability suffices.
11. **Visibility** — minimise `pub`; use `pub(crate)` / `pub(super)`. Test-only access via `#[cfg(feature = "test-util")]` gates + `_for_tests` setters, not `pub`. Review every changed `pub` item per `plan-coding-style.md` §1.11.
12. **Debug** — all public structs implement `Debug`. Manual: identity fields + `finish_non_exhaustive()`.
13. **Tests** — integration tests only, under each crate's `tests/<topic>_test.rs`. No inline `#[cfg(test)] mod tests`. Shared helpers in `tests/common/` (2018 style, not `testkit/`).
14. **Module & file layout** — changed file passes `plan-coding-style.md` §1.4 (size) and §1.6 (naming); `foo.rs` is a pure index; §1.5 split considered for grown files.
15. **File cohesion** — passes §1.7 stranger check: one responsibility, grouped by shared state/imports, handlers by resource not verb.
16. **Function length** — new/changed functions pass §1.8 (≤40 healthy / ≤80 orchestrator / ≤150 justified / >150 split). Extract by responsibility.

## Steps

// turbo
```bash
grep -rn '#\[derive(.*Clone' src/
grep -rn 'Arc<' src/
grep -rn '\.unwrap()' src/
grep -rn 'fn .*(&self' src/
grep -rn '#\[cfg\(test\)\]' src/
grep -rn 'mod\.rs' src/ tests/
grep -rn 'tests/testkit' .
grep -rn '\bpub ' src/ | grep -v 'pub(crate)\|pub(super)\|pub use\|pub mod\|pub fn.*for_tests'
```

Pre-commit gate (AGENTS.md Hard Constraints) must have already passed — fmt, clippy, clang-format, relevant tests.

## Clippy Exceptions

- `async_fn_in_trait` — allowed (exploring coroutine async trait API at lib boundary).
- Add new exceptions here before suppressing.

## Pitfalls

- Removing `Clone` without updating call sites.
- Removing `Arc` from a field shared across spawned tasks.
- Changing getter return type (`Arc<T>` → `&T`) without updating callers/tests.
- Removing a dep used only by generated code (check `OUT_DIR`).
- `&T` across `.await` is unsafe once moved into spawned task — must be `Arc<T>` then.
- Inline `#[cfg(test)] mod tests` instead of `tests/<topic>_test.rs`.
- Adding a headline type or impl logic to `foo.rs` — it must stay a pure index (docs + `pub mod` + `pub use`).
- Adding code to a >1000-line file — must extract a submodule first.
- New file named `types.rs` / `utils.rs` / `impl.rs` / `core.rs` / `mod.rs`-with-logic — rename by subject per §1.6.
- `pub` on a test-only item — gate behind `#[cfg(feature = "test-util")]` instead.
- New file under `tests/testkit/` — use `tests/common/` (2018 style).
