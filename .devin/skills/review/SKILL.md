---
name: review
description: Review Rust code
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Code Review

Goal: lean, correct, no dead code.

## Hot-Path Rules

Hot paths: `propose`, `accept`, `learn`, `kv_get`, `kv_put`, `kv_delete`, `kv_batch_write`.

- Prefer `&T` over `T` in signatures.
- No mutex on critical paths — use atomics. `std::sync::Mutex` ok for `start`/`stop` lifecycle. `tokio::sync::Mutex` only when held across `.await`.
- Pre-size collections (`with_capacity`) or use stack.

### Flatbuffer (crow-rpc handlers)

The flatbuffer control message IS the buffer — field access is a memory-offset read, no deserialize step. Full spec: `doc/design/rpc/design-crow-rpc.md` §6.

- **No owned intermediate struct.** Read through the flatbuffer root pointer in place; don't deserialize into a Rust struct with `String` + `Vec` fields.
- **No allocating accessor on the hot path.** `fb.field().to_string()` / `.to_vec()` heap-allocates per call. Use the flatbuffer reference directly.
- **Wrappers live in `crow-protocol`.** One shared definition per flatbuffer type.
- **Data payload: zero-copy when consumed by reference.** Copy to owned only when the handler retains data past the frame's lifetime.
- **Write path: build, finish, attach, drop.** No retained builder state.

## Checklist

1. **Health & Info exposure** — expose useful internal state via `HealthStatus` variants or info struct fields by default.
2. **Comments** — `//!` for module, `///` for items, inline for non-obvious logic. No doc references. TODO/FIXME in `doc/todo_code.md`.
3. **Clone** — justify every `.clone()` in hot paths with a comment if non-trivial.
4. **Arc** — drop inner `Arc` when parent is already `Arc`. Return `&T` instead of `Arc<T>` when parent keeps it alive. Inner `Arc` only if it outlives parent (e.g. moved into `tokio::spawn`).
5. **Mutex** — `std::sync::Mutex` for short non-async; `tokio::sync::Mutex` only across `.await`; remove if all state is atomic.
6. **Enum vs `dyn Trait`** — prefer enum dispatch. `dyn` only for open-ended / cross-crate.
7. **Dead code** — remove unused types/imports/fields/methods/deps. Collapse always-same-value enum variants.
8. **Duplication** — move shared helpers to `common/` or `rpc/`.
9. **Errors** — no `panic!` in non-test code. Replace `OnceCell::get_or_init + unwrap` with `get_or_try_init`.
10. **Naming** — `Px` prefix for Paxos types. `&self` when interior mutability suffices.
11. **Visibility** — minimise `pub`; use `pub(crate)` / `pub(super)`. Test-only via `#[cfg(feature = "test-util")]` gates.
12. **Debug** — all public structs implement `Debug`. Manual: identity fields + `finish_non_exhaustive()`.
13. **Tests** — integration tests only, under `tests/<topic>_test.rs`. No inline `#[cfg(test)] mod tests`. Helpers in `tests/common/`.
14. **Module & file layout** — changed file passes size caps and naming rules; `foo.rs` is a pure index.
15. **File cohesion** — passes the stranger check: one responsibility, handlers by resource not verb.
16. **Function length** — new/changed functions pass length caps (≤40 healthy / ≤80 orchestrator / ≤150 justified / >150 split).

## Clippy Exceptions

- `async_fn_in_trait` — allowed (exploring coroutine async trait API at lib boundary).
- Add new exceptions here before suppressing.

## Pitfalls

- Removing `Clone` without updating call sites.
- Removing `Arc` from a field shared across spawned tasks.
- Changing getter return type (`Arc<T>` → `&T`) without updating callers/tests.
- `&T` across `.await` is unsafe once moved into spawned task — must be `Arc<T>`.
- Inline `#[cfg(test)] mod tests` instead of `tests/<topic>_test.rs`.
- Adding headline type or impl logic to `foo.rs` — it must stay a pure index.
- Adding code to a >1000-line file — extract a submodule first.
- New file named `types.rs` / `utils.rs` / `impl.rs` / `core.rs` — rename by subject.
- `pub` on a test-only item — gate behind `#[cfg(feature = "test-util")]`.
- New file under `tests/testkit/` — use `tests/common/`.
