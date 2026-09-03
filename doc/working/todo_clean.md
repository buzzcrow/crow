<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Cleanup Work Plan

This file is persistent. Completed tasks are removed; the file remains until
all cleanup work is complete.

The tasks were consolidated from `todo_perf.md` and `todo_tree_count.md` after
the completed work in those files was verified. Tasks are independent unless a
dependency is stated explicitly.

## P1: Structured server log context

Logs emitted while processing a store/group/replica should consistently carry
`s`, `g`, and `replica`. Leadership is dynamic and must be recorded at the
event where it matters rather than captured once in a long-lived span.

The same field vocabulary applies in `crowdb-kv-client`. Client events use
`replica` when the target is merely a replica and `leader` when the client
currently believes the target is the leader. Unknown fields are omitted.

The earlier inventory was stale: the KV crate already uses
`#[tracing::instrument]` in election, membership, shutdown, and store paths.
The formatter also renders the active span context on events by default;
`with_span_events(FmtSpan::ACTIVE)` would emit span lifecycle events and is not
required to show inherited fields.

Implementation:

- [ ] **Update the logging contract first**: replace the current design rule
  that rejects a strict field template with the chosen stable-context rule.
  Define `s`, `g`, and `replica` as canonical field names; define when an event
  must add `leader` or `role`. File:
  `doc/design/kv/design-crowdb-kv-observability.md` section 3.9.
- [ ] **Inventory context boundaries**: list every long-lived loop, spawned
  task, and inbound RPC handler in `lib/crowdb-kv/src/` and
  `app/crowdb-kv-server/src/`; record which identifiers are available at each
  boundary. This inventory determines where context must be threaded instead
  of guessing from tracing call-site counts.
- [ ] **Add stable parent spans**: create spans at group maintenance, apply,
  election-driver, store/group runner, and RPC request entry points. Use only
  identifiers whose values remain valid for the span lifetime. Files are
  expected under `lib/crowdb-kv/src/cluster/`, `lib/crowdb-kv/src/rpc/`, and
  `app/crowdb-kv-server/src/`.
- [ ] **Propagate spans across task boundaries**: attach the parent span to
  each relevant `tokio::spawn` and `spawn_blocking` future with
  `tracing::Instrument`. Do not assume task inheritance.
- [ ] **Normalize existing event fields**: replace `group_id`, `replica_id`,
  `replica_l_id`, and `replica_r_id` only where the enclosing span does not
  already provide the canonical value. Keep operation-specific fields such as
  `slot`, `term`, `peer`, and endpoint.
- [ ] **Normalize client context**: apply the same `s`, `g`, `replica`, and
  `leader` vocabulary to `crowdb-kv-client`; use `leader` only for topology's
  current leader choice. File: `lib/crowdb-kv-client/src/`.
- [ ] **Report dynamic leadership at events**: add `leader` or `role` to
  leadership transitions, lease decisions, request rejection/forwarding, and
  other events whose interpretation depends on current leadership. Do not
  re-create long-lived spans after every election.
- [ ] **Avoid duplicate context**: inspect representative file and stderr
  output and remove fields repeated by both an event and its active span.

Tests:

- [ ] Add a formatter test that enters nested store/group/replica spans and
  verifies one emitted event contains the canonical context fields.
- [ ] Add an async propagation test covering `tokio::spawn`; add a
  `spawn_blocking` case if maintenance uses a separately instrumented span.
- [ ] Run the relevant `crowdb-common`, `crowdb-kv`, and
  `crowdb-kv-server` tests under `pixi`.

## Open issues

No open issues.
