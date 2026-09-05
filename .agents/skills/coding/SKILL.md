---
name: coding
description: Apply CROWDB code, logging, test, and module conventions.
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Coding

Read `doc/doc_index.md`, select the matching design section, and keep code
consistent with it. Update upstream design first when intent is missing or
contradictory. Ask only when architectural choices remain equivalent.

## Logging

- `critical!`: broken invariant; include `next step:`.
- `error!`: recoverable failure and its handling.
- `warn!`: actionable anomaly or transient failure.
- `info!`: lifecycle and topology changes only; never per request.
- `debug!`: requests, hot-path decisions, and routine detail.
- Do not leave production `trace!` calls.

Use structured Paxos fields when available: `store_id`, `group_id`,
`replica_l_id`, `replica_r_id`, `slot`, `ballot`. Instrument public Paxos
object methods so fields propagate. Defaults remain file=`debug`, console=`info`.

## Tests and status

- Put integration tests in `<crate>/tests/*_test.rs`; helpers belong in
  `tests/common/` and use `Test*` names. Do not add inline test modules,
  `tests/testkit/`, or fixtures under `src/`.
- Keep Paxos tests under `lib/crowdb-kv/tests/paxos_test/`, entered by
  `lib/crowdb-kv/tests/paxos_test.rs`.
- Use `CROWDB_TEST_LOG=1` and `tests/common/logging.rs` for test tracing.
- Expose useful new state through `StatusLevel` or the relevant `*Status`
  type in `lib/crowdb-kv/src/cluster/status.rs`; shared wire types live in
  `lib/crowdb-protocol/src/mgmt.rs`.
- Track new TODO/FIXME items in `doc/todo_code.md`; remove both together.

## Layout

- Use the non-`mod.rs` layout: `foo.rs` + `foo/`; `foo.rs` contains module docs,
  declarations, and re-exports.
- Name files by domain subject, not kind, verb, transport, or legacy wording.
  Avoid `types.rs`, `impl.rs`, `core.rs`, `misc.rs`, and helper suffixes.
- Keep one concept and one nameable responsibility per module. Group handlers by
  resource, strategies by file, and services one per file.
- Separate domain state/invariants from runtime wiring and infrastructure.
- Keep code files near 300 lines; split before adding to one over 1000. Keep
  functions near 40 lines, at most 80 for orchestration; split over 150.
- Use the narrowest visibility. Test hooks require `test-util` and `_for_tests`.
- Do not add lint suppressions.

Use `/console-ui-e2e` for visible UI changes and `/review` before handoff.
