---
description: Reorder Rust items per CrowKV pub-first conventions
---

# Code Ordering Workflow

Invoke: `/format-ordering <filepath>`

## Conventions

Within a `.rs` file:
1. `use` groups: `std` → external → `crate::`
2. `pub` types before private types
3. Type definitions before their `impl` blocks
4. Within an `impl`: `pub` methods before private methods
5. Within the same visibility group, items ordered alphabetically by name
6. No inline `#[cfg(test)]` modules — tests live in `tests/` as integration tests

## Steps

1. Read the file. List every item with kind, visibility, and line range.
2. Detect violations:
   - private type/function before `pub` (same kind)
   - `impl` block before its type definition
   - items in same visibility group not alphabetically ordered by name
   - `#[cfg(test)]` module exists in a `.rs` file (should be in `tests/` folder)
   - import groups not in `std` → external → `crate::` order
3. Propose minimal moves (smallest diff).
4. **[CHECKPOINT]** — user approves.
5. Apply via `multi_edit`.
6. Run `cargo check` to verify compilation.

## Rust Semantics Note

Within a single Rust file, **function order is completely free** — use-before-definition is 100% legal. Reordering items via this workflow is always semantically safe. The only exception is moving `#[cfg(test)]` modules to `tests/` as integration tests — this requires analysis because:
- Integration tests can only access the crate's public API
- Internal helpers used by tests must be made `pub` or `pub(crate)`
- Test logic may need restructuring to work through the public interface

## Limitations

- This runs only when invoked, not on save.
- Macros and `mod` declarations are left untouched.
- Complex interdependencies may need manual review.
- Moving `#[cfg(test)]` to `tests/` is opt-in per file (requires manual analysis).
