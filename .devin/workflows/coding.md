<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: CROW coding flow — conventions, doc-first
---

# CROW - Coding Flow

Companion workflows: `/review` (pre-push), `/doc` (doc rules).

## Conventions

### Logging (`tracing`)

- `critical!` (macro = `error!("critical: …")`) — invariant violated / unreachable. Always include `next step:`.
- `error!` — recoverable error; state how it's handled.
- `warn!` — anomaly worth attention (timeout retried, transient failure).
- `info!` — system state changes only: component start/stop, node add/remove, leader change, election transitions, membership changes, shutdown. No per-request or per-operation logs.
- `debug!` — per-request entry/exit, hot-path decisions, routine startup details (replay scan, segment sealed, gc pass).
- `trace!` — ad-hoc only; not in production code.
- Standard: after running a cluster with operations, an AI reading only `info`-level logs and metrics should understand the full system lifecycle without debugging.

Structured fields in Paxos-scoped logs (never inline in message):
`store_id`, `group_id`, `replica_l_id`, `replica_r_id`, `slot`, `ballot` — when in scope.

Propagate via `#[tracing::instrument(fields(store_id, group_id, replica_l_id))]` on public methods of `PxKvStore` / `PxGroup` / `PxLocalReplica` / `PxRemoteReplica`.

Defaults: file=`debug`, console (`-l`)=`info`. Override via `RUST_LOG`. See `crowkv/src/common/logging.rs`.

### Comments

- No doc references in code comments — keep docs in docs.
- TODO/FIXME: add to `doc/todo_code.md` when creating; remove when resolved.

### Tests

- Integration tests only — under each crate's `tests/`. No new inline `#[cfg(test)] mod tests`; migrate existing inline tests when you next touch the file.
- Shared helpers: `tests/testkit/<topic>.rs`.
- Paxos suite: `crowkv/tests/paxos/*.rs` with `tests/paxos.rs` as entry stub.
- Tracing in tests: set `CROWKV_TEST_LOG=1`; init in `tests/testkit/logging.rs`.

### E2E / Playwright Tests (`crowkv-console/web/ui/e2e`)

- **No ignoring errors** — never swallow API failures silently; log cleanup errors with `console.warn`.
- **Precise selectors** — use `getByLabel`, `getByRole`, `getByTestId`, or scoped locators. Avoid unscoped `page.getByText` and `.first()` on page-level locators.
- **Timeout discipline** — assertion timeouts ≤ 3 s; leader election may use up to 10 s. No inflating timeouts to work around slowness. `expect.poll` must set `intervals: [100]` for fast polling (default 2 s interval causes false slowness).
- **`data-testid`** — add to dynamic elements that could match in multiple places; select via `getByTestId`.
- **Ignore toasts** — never assert on `getByRole('alert')` or wait for toast dismiss. If a toast blocks a click, use `locator.evaluate((el) => el.click())` to bypass.
- **Baseline timing** — every E2E spec file has a `// Baseline: Xs (date)` comment after the license header. If a test's runtime exceeds 2x its baseline, investigate for regression. Update the baseline only when a deliberate change justifies it.

### Health & Info Reporting

When adding internal state to `crowkv` lib:
- **StatusLevel / *Status structs** (`crowkv/src/cluster/status.rs`): add variants/fields for distinct operational states operators need to see. Default to exposing useful internal state (internal UI, no security concerns).

## Doc-First

- Start at `doc/doc_index.md` — match the task to a row, then open only that doc and grep for the listed `##` section. Avoid full reads.
- Gap with code intent → fix the upstream doc first. Never violate upstream.
- If you add/rename/rescope any doc, update `doc_index.md` in the same commit.
- Mid-impl decision:
  - **Simple/local** → decide, note in commit msg.
  - **Ambiguous / needs review** → discuss with the user. Do not silently guess.
