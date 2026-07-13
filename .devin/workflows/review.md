---
description: Review Rust code
---

# CrowKV - Code Review

Goal: lean, correct, no dead code.

## Hot-Path Rules

Hot paths: `propose`, `accept`, `learn`, `kv_get`, `kv_put`, `kv_delete`, `kv_batch_write`.

- Prefer `&T` over `T` in signatures.
- No mutex on critical paths — use atomics. `std::sync::Mutex` ok for `start`/`stop` lifecycle. `tokio::sync::Mutex` only when held across `.await`.
- Pre-size collections (`with_capacity`) or use stack.

## Checklist

1. **Health & Info exposure** — when adding internal state, consider if it should be exposed to UI:
   - Add `HealthStatus` variants for distinct operational states operators need to see
   - Add info struct fields that help operators understand cluster state
   - Default to exposing useful internal state (internal UI, no security concerns)
2. **Comments** — ensure code comments follow conventions:
   - Module-level comments (`//!`) summarize purpose, explain why, list key work areas
   - No doc references (`doc/`, `design.md`, etc.) in code comments
   - Function/struct comments (`///`) describe what and why
   - Inline comments explain non-obvious logic, invariants, trade-offs
   - TODO/FIXME markers tracked in `doc/todo_code.md`
3. **Clone** — review every `#[derive(Clone)]` and `.clone()`. In hot paths, justify with a comment if non-trivial overhead is accepted (e.g. `Arc<Vec<u8>>` payloads sharing).
3. **Arc** — drop inner `Arc` when parent is already `Arc`. Return `&T` instead of `Arc<T>` when parent keeps it alive. Inner `Arc` only needed if it outlives parent (e.g. moved into `tokio::spawn`). `&T` across `.await` is safe while owner is in scope.
4. **Mutex** — `std::sync::Mutex` for short non-async sections; `tokio::sync::Mutex` only across `.await`; remove if all state is atomic.
5. **Enum vs `dyn Trait`** — prefer enum dispatch. `dyn` only for open-ended / cross-crate.
6. **Dead code** — remove unused types/imports/fields/methods/deps. Collapse always-same-value enum variants to unit.
7. **Duplication** — move shared helpers to `common/` or `rpc/`.
8. **Errors** — no `panic!` in non-test code. Replace `OnceCell::get_or_init + unwrap` with `get_or_try_init`. gRPC client init must propagate.
9. **Naming** — `Px` prefix for Paxos types. `&self` when interior mutability suffices.
10. **Visibility** — minimise `pub`; use `pub(crate)` / `pub(super)`. Test helpers `#[cfg(test)]` or test feature.
11. **Debug** — all public structs implement `Debug`. Manual: identity fields + `finish_non_exhaustive()`.
12. **Tests** — integration tests only, under each crate's `tests/<topic>.rs`. No new inline `#[cfg(test)] mod tests`; migrate existing inline tests when you next touch the file. Shared helpers live in `tests/testkit/`. Entry stubs in `tests/<suite>.rs`.

## Steps

// turbo
```bash
grep -rn '#\[derive(.*Clone' src/
grep -rn 'Arc<' src/
grep -rn '\.unwrap()' src/
grep -rn 'fn .*(&self' src/
grep -rn '#\[cfg\(test\)\]' src/
cargo clippy --all-targets -- -D warnings
cargo build && cargo test
```

## Clippy Exceptions

- `async_fn_in_trait` — allowed (exploring coroutine async trait API at lib boundary).
- Add new exceptions here before suppressing.

## Pitfalls

- Removing `Clone` without updating call sites.
- Removing `Arc` from a field shared across spawned tasks.
- Changing getter return type (`Arc<T>` → `&T`) without updating callers/tests.
- Removing a dep used only by generated code (check `OUT_DIR`).
- `&T` across `.await` is unsafe once moved into spawned task — must be `Arc<T>` then.
- Inline `#[cfg(test)] mod tests` instead of `tests/<topic>.rs`.
