<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: CrowKV coding flow — conventions, doc-first
---

# CrowKV - Coding Flow

Companion workflows: `/review` (pre-push), `/doc` (doc rules).

## Conventions

### Logging (`tracing`)

- `critical!` (macro = `error!("critical: …")`) — invariant violated / unreachable. Always include `next step:`.
- `error!` — recoverable error; state how it's handled.
- `warn!` — anomaly worth attention (timeout retried, transient failure).
- `info!` — major lifecycle / state transitions.
- `debug!` — per-request entry/exit, hot-path decisions.
- `trace!` — ad-hoc only; not in production code.

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

- **No ignoring errors** — never swallow API failures with `.catch(() => undefined)` or similar. If a cleanup call fails, log it with `console.warn` and the error details. Tests must surface all errors, not hide them.
- **Precise selectors** — use `getByLabel`, `getByRole`, `getByTestId`, or scoped locators (`page.locator('header').getByRole(...)`, `aside.getByText(...)`). Avoid unscoped `page.getByText(...)` that could match toast messages plus inline text. Avoid `.first()` on page-level locators — scope to a container instead.
- **Timeout discipline** — all assertion timeouts must be ≤ 3 seconds. The only exception is leader election (`waitForLeader`, `expect.poll` for leader status) which may use up to 10 seconds. No `setTimeout` or `waitForTimeout` increases to work around slow operations.
- **`data-testid` for dynamic content** — when a test asserts on text that could appear in multiple places (e.g., KV get result, not-found message, scan table), add a `data-testid` to the target element and use `getByTestId` in the test.

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
