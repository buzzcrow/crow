---
description: Review Rust code
---

# CrowKV - Workflow: Code Review for Refactoring

## Purpose

Remove unnecessary overhead, dead code, and unsafe patterns. Keep code lean and correct.

## General Performance Rule

**Hot paths (propose, accept, learn, kv_get, kv_put, kv_delete, kv_batch_write):**

- Prefer `&T` over `T` in function signatures to avoid clones
- Avoid mutex on critical paths — use lock-free atomic operations
- Avoid dynamic allocation on arrays — prefer `with_capacity` or stack
- `std::sync::Mutex` is acceptable for non-critical-path operations (e.g. `start()`/`stop()` server lifecycle)
- `tokio::sync::Mutex` only when lock must be held across `.await`

## Checklist

### 1. Clone Derive

- Grep `#[derive(.*Clone` — is the type ever actually cloned at a call site?
- Internal-only enums (e.g. result types matched immediately) rarely need `Clone`
- `Copy` types (`PxBallot`, config) are fine with `Clone`
- Types with `Vec<u8>` payload: question every `.clone()` in loops

### 2. Arc Wrapper

- If the struct is already behind `Arc<T>` at a higher level, internal fields don't need their own `Arc`
- `DashMap` values wrapped in `Arc` are valid when callers need to hold refs across `.await`
- Getter should return `&T` instead of `Arc<T>` when the parent `Arc` keeps it alive
- Holding `&T` across `.await` is safe when the parent `Arc` stays in scope (Rust borrow checker enforces this). Only need `Arc<T>` on the inner field if it must outlive the parent (e.g. moved into `tokio::spawn`)

### 3. Mutex Wrapper

- `std::sync::Mutex` for short non-async critical sections (ok)
- `tokio::sync::Mutex` only when lock is held across `.await`
- If all inner state is atomic, remove the Mutex

### 4. Enum over `dyn Trait`

- Prefer enum dispatch over `dyn Trait` objects — idiomatic Rust, zero-cost dispatch, no heap allocation
- Use `dyn Trait` only when the set of variants is truly open-ended or crosses crate boundaries

### 5. Dead Code

- Unused types, imports, struct fields, and functions should be removed
- Enum variants with fields that are always the same value → simplify to unit variant
- Grep for types defined but never referenced outside their own module
- Remove unused dependencies from `Cargo.toml`

### 6. Duplicated Code

- Grep small utility functions (e.g. `optional_u64`) — if duplicated, move to `common`
- Shared proto-conversion helpers belong in `rpc/` or `common/`

### 7. Error Handling

- `OnceCell::get_or_init` + `.unwrap()` panics on failure → use `get_or_try_init`
- gRPC client init must propagate errors, never panic
- Prefer `Result` over `panic!` in non-test code

### 8. Naming & Consistency

- Px prefix for all Paxos-layer types and structs
- Consistent `&self` vs `&mut self` — `&self` where interior mutability suffices
- Dead accessor methods (never called) should be removed

### 9. Visibility

- Minimize `pub` — use `pub(crate)` or `pub(super)` when callers are internal only
- Audit `pub fn` and `pub struct` that are never referenced outside their module
- Test-only helpers should be `#[cfg(test)]` or `pub(crate)` behind a test feature

### 10. Debug Implementation

- All public structs need `Debug` (derive or manual)
- Manual `Debug`: show identity fields, omit complex internals with `finish_non_exhaustive()`

### 11. Dependency Hygiene

- After removing types, check if their crate dependency (`serde`, etc.) is still used
- Run `cargo build` to catch unused imports/dead code warnings
- Run `cargo test` to verify no regressions

## Steps

1. `grep -rn '#\[derive(.*Clone' src/` — trace each to call sites
2. `grep -rn 'Arc<' src/` — check if parent is already `Arc`
3. `grep -rn '\.unwrap()' src/` — check for panic-on-failure
4. `grep -rn 'fn .*(&self' src/` — check for dead methods
5. `cargo clippy -- -W clippy::all` — fix safe warnings; exceptions below
6. Build and test: `cargo build && cargo test`

### Clippy Exceptions

- `async_fn_in_trait` — suppress with `#[allow]`; we want to explore coroutine async trait API on the lib boundary
- For uncertain fixes, add the exception here before suppressing

## Pitfalls

- Removing `Clone` without updating call sites
- Removing `Arc` from a field that is shared across tasks (not just across methods)
- Changing getter return types (`Arc<T>` → `&T`) requires updating all callers + tests
- Removing a dependency that is used only by generated code (check `OUT_DIR`)
- Holding `&T` across `.await` is safe only while the owning `Arc`/container is in scope; if the ref needs to move into a spawned task, it must be `Arc<T>`
