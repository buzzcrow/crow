<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: CROW coding flow — conventions, doc-first
---

# CROW - Coding Flow

Companion workflows: `/review` (pre-push), `/doc` (doc rules), `/console-ui-e2e` (console-ui-e2e).

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
- Shared helpers: `tests/common/<topic>.rs` (2018 style: `tests/common.rs` + `tests/common/`). `tests/testkit/` is being migrated to `tests/common/` — do not add new files under `testkit/`.
- Test case files use the `*_test.rs` suffix (`group_test.rs`, `wal_test.rs`). Test helper files live in `common/` named by subject (`cluster.rs`, `logging.rs`). Test helper types use the `Test*` prefix (`TestCluster`, `TestNode`).
- Test fixtures (`TestCluster`, `init_test_subscriber`, `unique_port`) stay in `tests/`, never in `src/` under `test-util`. The `test-util` feature is for production-type hooks only (gates, setters, internal field exposure).
- Paxos suite: `crowkv/tests/paxos/*.rs` with `tests/paxos.rs` as entry stub.
- Tracing in tests: set `CROWKV_TEST_LOG=1`; init in `tests/common/logging.rs`.

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

## Style & Layout Rules

- **Module layout** — Rust 2018: `foo.rs` + `foo/`. `foo.rs` is a pure index (docs + `pub mod` + `pub use` only); no type definitions, no impl logic, no inline tests.
- **File size** (non-blank/non-comment) — ≤300 healthy, 301–600 ok if single responsibility, 601–1000 smell (needs justification), >1000 must split before adding code.
- **File naming** — `snake_case`, subject not kind (`segment.rs` not `engine_impl.rs`), 1–2 words. Allowed abbreviations: `kv`, `rpc`, `wal`, `gc`, `ffi`, `cfg`, `mgmt`, `px`, `cli`. Banned: `types.rs`, `impl.rs`, `core.rs`, `misc.rs`, `mod.rs`-with-logic, `_helpers`/`_utils`/`_common` suffixes. Conventional suffixes: `_engine`, `_service`, `_handler`, `_backend`, `_worker`/`_loop`, `_codec`, `_view`/`_status`, `_config`, `_error`, `_tests`.
- **Cohesion** — group by domain then subject, never by layer. One responsibility per file. Types live with their impl. Handlers group by resource not verb. Stranger check: any function should share the file's primary type, imports, and reader expectations.
- **Function length** — ≤40 healthy, 41–80 orchestrator-only, 81–150 smell (needs reason), >150 must split. Extract by responsibility, not line count.
- **Type placement** — headline types always in named submodules (`foo/wal_engine.rs`), re-exported from `foo.rs`. Supporting types with their owner or in a shared submodule named by subject.
- **Visibility** — narrowest that works: private < `pub(super)` < `pub(crate)` < `pub`. Test-only access via `#[cfg(feature = "test-util")]` + `_for_tests` setters, never `pub`.
- **Special cases** — `crow-tree-ffi`: `unsafe_code = deny` relaxed, 1000-line cap still applies. Test code: strict 2018 style (same as `src/`). Generated code: exempt from size rules.
- **Enforcement** — `[workspace.lints.clippy]`: `mod_module_files`, `too_many_lines` (default threshold 100), `items_after_statements` set to `"warn"`. No `clippy.toml`. No new `#[allow]` suppressions.
