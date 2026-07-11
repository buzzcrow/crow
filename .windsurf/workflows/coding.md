---
description: CrowKV coding flow with logging rules and test layout
---

# CrowKV - Coding Flow

Companion: `/review` (pre-push), `/doc` (doc rules).

## Model Scope Division

- **AI scope (expensive model)**: implement core code and fix bugs surfaced by tests
- **User scope (free model / SWE-1.6)**: cargo fmt, clippy fixes, doc comments, README/index updates, git commit, push

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
  - Do **not** reference external docs (`doc/`, `plan.md`, `design.md`, etc.) in code comments.
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

## 6. Pre-Commit (auto via `.githooks/pre-commit`)

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
- Reference upstream doc (`plan-paxos.md M2`) in the body.
- Run `/review` before push for non-trivial changes.

## Pitfalls

- Inline `#[cfg(test)] mod tests` instead of `tests/<topic>.rs`.
- IDs in message string instead of structured fields → unfilterable.
- Silent guess on ambiguous design → discuss with user instead.
- `error!` for what is really `critical:` → mis-routed alerts.
- Doc references in comments (`doc/`, `plan.md`, etc.) → keep docs in docs, not code.
