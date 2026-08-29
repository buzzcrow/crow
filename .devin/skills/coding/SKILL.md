---
name: coding
description: CROWDB coding flow — conventions, doc-first
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Coding Flow

Companion skills: `/review` (pre-push), `/doc` (doc rules), `/console-ui-e2e` (console-ui-e2e).

## Conventions

### Logging (`tracing`)

- `critical!` — invariant violated / unreachable. Always include `next step:`.
- `error!` — recoverable error; state how it's handled.
- `warn!` — anomaly worth attention (timeout retried, transient failure).
- `info!` — system state changes only: component start/stop, node add/remove, leader change, election transitions, membership changes, shutdown. No per-request logs.
- `debug!` — per-request entry/exit, hot-path decisions, routine startup details.
- `trace!` — ad-hoc only; not in production code.
- Standard: an AI reading only `info`-level logs and metrics should understand the full system lifecycle.

Structured fields in Paxos-scoped logs (never inline in message):
`store_id`, `group_id`, `replica_l_id`, `replica_r_id`, `slot`, `ballot` — when in scope. Propagate via `#[tracing::instrument(fields(...))]` on public methods of `PxKvStore` / `PxGroup` / `PxLocalReplica` / `PxRemoteReplica`.

Defaults: file=`debug`, console (`-l`)=`info`. Override via `RUST_LOG`.

### Comments

- No doc references in code comments. TODO/FIXME: add to `doc/todo_code.md` when creating; remove when resolved.

### Tests

- Integration tests only — under each crate's `tests/`. No inline `#[cfg(test)] mod tests`; migrate existing when touched.
- Shared helpers: `tests/common/` (2018 style). Do not add new files under `testkit/`.
- Test case files: `*_test.rs` suffix. Helper files: `common/<subject>.rs`. Helper types: `Test*` prefix.
- Test fixtures stay in `tests/`, never in `src/` under `test-util` (that's for production-type hooks only).
- Paxos suite: `crowdb/tests/paxos/*.rs` with `tests/paxos.rs` as entry stub.
- Tracing in tests: `CROWDB_TEST_LOG=1`; init in `tests/common/logging.rs`.

### Health & Info Reporting

When adding internal state to `crowdb` lib, add variants/fields to `StatusLevel` / `*Status` structs (`crowdb/src/cluster/status.rs`). Default to exposing useful internal state.

## Doc-First

- Start at `doc/doc_index.md` — match the task to a row, open only that doc, grep for the listed `##` section.
- Gap with code intent → fix the upstream doc first. Never violate upstream.
- If you add/rename/rescope any doc, update `doc_index.md` in the same commit.
- Mid-impl decision: simple/local → decide, note in commit msg. Ambiguous → discuss with user.

## Style & Layout Rules

- **Module layout** — Rust 2018: `foo.rs` + `foo/`. `foo.rs` is a pure index (docs + `pub mod` + `pub use` only); no types, no impl, no inline tests.
- **File size** (non-blank/non-comment) — ≤300 healthy, 301–600 ok if single responsibility, 601–1000 smell, >1000 must split before adding code.
- **File naming** — `snake_case`, subject not kind (`segment.rs` not `engine_impl.rs`), 1–2 words. Allowed abbreviations: `kv`, `rpc`, `wal`, `gc`, `ffi`, `cfg`, `mgmt`, `px`, `cli`. Banned: `types.rs`, `impl.rs`, `core.rs`, `misc.rs`, `mod.rs`-with-logic, `_helpers`/`_utils`/`_common` suffixes.
- **Function length** — ≤40 healthy, 41–80 orchestrator-only, 81–150 smell, >150 must split. Extract by responsibility.
- **Visibility** — narrowest that works: private < `pub(super)` < `pub(crate)` < `pub`. Test-only via `#[cfg(feature = "test-util")]` + `_for_tests` setters, never `pub`.
- **Cohesion** — group by domain then subject, never by layer. One responsibility per file. Types live with their impl. Handlers group by resource not verb.
- **Enforcement** — `[workspace.lints.clippy]`: `mod_module_files`, `too_many_lines` (default 100), `items_after_statements` = `"warn"`. No `clippy.toml`. No new `#[allow]` suppressions.

### Module Design Rules

- **Name by subject, not by kind or transport** — `keepalive.rs` not `sync.rs`; `diskdb_service.rs` not `rpc.rs`.
- **Name by the domain concept, not a borrowed/legacy term** — `DdbDiskGroup`, not `Node` if the unit is a disk-group. Prefix local manager types to avoid shadowing shared/protocol crate types.
- **One concept = one module** — concepts that belong together live together, separate from infrastructure (I/O, recovery, RPC, config).
- **File layout surfaces conceptual structure** — three strategies → three files; multiple services → `service/` with one file per service. The file tree reads like a table of contents.
- **One file per resource/service, not per verb** — `service/diskdb_service.rs`, not `allocate.rs` / `free.rs` / `query.rs`.
- **A module's responsibility must be nameable in one phrase** — if you can't, it's doing too much. Split.
- **Separate domain from runtime** — domain = in-memory model + invariants + orchestration. Runtime = keep-alive, recovery, bg-task framework, service, config, lifecycle, metrics. They don't entangle.
