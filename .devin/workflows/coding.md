---
description: CrowKV coding flow with logging rules and test layout
---

# CrowKV - Coding Flow

Companion: `/review` (pre-push), `/doc` (doc rules).

## Model Scope Division

- **AI scope (expensive model)**: implement core code and fix bugs surfaced by tests

## 1. Doc-First

- Start at `doc/doc_index.md` — match the task to a row, then open only that doc and grep for the listed `##` section. Avoid full reads.
- Gap with code intent → fix the upstream doc first. Never violate upstream.
- If you add/rename/rescope any doc, update `doc_index.md` in the same commit.
- Mid-impl decision:
  - **Simple/local** → decide, note in commit msg.
  - **Ambiguous / needs review** → discuss with the user for decision. Do not silently guess.

## 2. Logging (`tracing`)

| Level | Use |
| --- | --- |
| `critical!` (macro = `error!("critical: …")`) | Invariant violated / unreachable-by-design. Always include `next step:`. |
| `error!` | Recoverable error; state how it's handled (skip / retry / propagate). |
| `warn!` | Anomaly worth attention (timeout retried, transient failure). |
| `info!` | Major lifecycle / state transitions (start, stop, leader change, group add/remove). |
| `debug!` | Per-request entry/exit, hot-path decisions. Goal: reproduce bugs from log. |
| `trace!` | Ad-hoc only; not in production code. |

**Required structured fields** in any Paxos-scoped log (never inline in message):
`store_id`, `group_id`, `replica_l_id`, `replica_r_id`, `slot`, `ballot` — when in scope.

Propagate via `#[tracing::instrument(fields(store_id, group_id, replica_l_id))]` on public methods of `PxKvStore` / `PxGroup` / `PxLocalReplica` / `PxRemoteReplica`.

Defaults: file=`debug`, console (`-l`)=`info`. Override via `RUST_LOG`. See `crowkv/src/common/logging.rs`.

## 3. Comments

- **Module-level comments** (`//!`): summarize the module's purpose, explain why it exists, and list key work areas for searchability.
  - Do **not** reference external docs (`doc/`, `design.md`, etc.) in code comments.
  - Example: "Key work: AsyncFile API, io_uring integration, fallback mode, SimDisk."
- **Function/struct comments** (`///`): describe what the item does and why it's needed.
- **Inline comments**: explain non-obvious logic, invariants, or trade-offs.
- **TODO/FIXME markers**: add to `doc/todo_code.md` when creating; remove when resolved.
- **No doc references**: keep all documentation in actual docs, not in code comments.

## 4. Tests

- Integration tests only — under each crate's `tests/`. Do **not** add new `#[cfg(test)] mod tests` inline; migrate existing inline tests when you next touch the file.
- Shared helpers: `tests/testkit/<topic>.rs` (e.g. `logging.rs`, `cluster.rs`).
- Paxos suite: `crowkv/tests/paxos/*.rs` with `tests/paxos.rs` as entry stub.
- Tracing in tests: set `CROWKV_TEST_LOG=1`; init in `tests/testkit/logging.rs`.

## 5. Health & Info Reporting

When adding new internal state to `crowkv` lib:

- **HealthStatus** (`crowkv/src/cluster/health.rs`): add new variants if they represent distinct operational states that operators need to see (e.g., `Initializing`, `Draining`). These are exposed to UI for internal monitoring.
- **Info structs** (`crowkv/src/cluster/info.rs`): add fields that help operators understand cluster state (e.g., pending operations, configuration drift). Default to exposing useful internal state since this is internal UI usage with no security concerns.

Rule: if the state helps operators debug or understand the system, expose it via health or info.

## 7. Context Management

When finishing a task, compress the context if the next task doesn't have a direct relationship with the current task.

**Purpose**: Save token cost by avoiding carrying unrelated context between tasks.

**When to compress**:
- After completing a task (e.g., C7 implementation)
- Before starting a new task that is unrelated (e.g., moving from C7 to C8)
- If the new task touches different modules, files, or subsystems

**How to compress**:
- The system automatically summarizes your work when token limits are approached
- Proactively signal task completion to trigger context cleanup
- Keep only: project structure, recent file changes, and open gaps
- Discard: detailed implementation history, intermediate debugging steps, resolved issues

**Benefits**:
- Reduces token usage by ~30-50% for unrelated task transitions
- Improves focus by clearing irrelevant context
- Prevents context pollution from stale information

**When NOT to compress**:
- The next task directly depends on the current task (e.g., fixing a bug you just introduced)
- The next task modifies the same files or modules
- Debugging an issue that requires full history

## 8. Token Optimization

**Goal**: Minimize token usage while maintaining code quality.

### File Reading
- **Use grep/search first**: Search for patterns before reading files to locate relevant sections
- **Read with limits**: Use `offset` and `limit` parameters when reading large files (>1000 lines)
- **Avoid full reads**: Don't read entire files unless necessary. Read only the sections you need
- **Reuse viewed content**: Don't re-read files you've already viewed in the session

### Tool Usage
- **Maximize parallel calls**: Execute independent read/search operations in parallel
- **Batch edits**: Use `multi_edit` for multiple changes in the same file instead of sequential `edit` calls
- **Targeted edits**: Edit only the specific lines that need changing, not large blocks
- **Avoid redundant operations**: Don't repeat operations you've already done

### Search Strategy
- **Specific patterns**: Use precise regex patterns in grep to narrow results
- **File type filtering**: Use `type` parameter to search only relevant file types
- **Glob patterns**: Use `glob` to filter files before searching
- **Output mode**: Use `files_with_matches` for initial searches, then `content` for specific files

### When to Be Thorough vs. Concise
- **Be thorough**: When implementing core logic, fixing bugs, or adding new features
- **Be concise**: When updating documentation, formatting, or making trivial changes
- **Context matters**: Carry full context for complex tasks, compress for simple tasks

### Estimated Savings
- **Grep-first**: ~40% token reduction vs. reading entire files
- **Parallel calls**: ~30% reduction vs. sequential operations
- **Batch edits**: ~50% reduction vs. sequential single-file edits
- **Combined**: Up to ~70% reduction when all practices are used

## 9. Pre-Commit (auto via `.githooks/pre-commit`)

// turbo
```bash
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

Do not bypass.

**Important:** When following `/coding` flow:
- **Do NOT fix clippy errors** — leave them for the user to fix separately
- **DO fix test issues** — ensure tests pass
- Leave code changes in place; user will fix clippy and commit later

## 6. Commit & Push

- One logical change per commit. Subject ≤72 chars, imperative.
- Reference upstream doc (`design-xxx.md §N`) in the body.
- Run `/review` before push for non-trivial changes.

## Pitfalls

- Inline `#[cfg(test)] mod tests` instead of `tests/<topic>.rs`.
- IDs in message string instead of structured fields → unfilterable.
- Silent guess on ambiguous design → discuss with user instead.
- `error!` for what is really `critical:` → mis-routed alerts.
- Doc references in comments (`doc/`, `design.md`, etc.) → keep docs in docs, not code.
