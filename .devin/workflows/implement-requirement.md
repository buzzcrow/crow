<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: Lifecycle for implementing requirement items from doc/backlog/backlog.md
---

# CROW - Implement Requirement Flow

Use this workflow when picking up an item from `doc/backlog/backlog.md`.
Open the matched `R**-<component>-<topic>.md` detail doc for the full
problem/approach/files/acceptance analysis.

Per-type doc guides (open the matched one before writing that artifact):

- Backlog doc (`R**-<component>-<topic>.md`) — [`doc-backlog`](doc-backlog.md)
- Design draft (`design-<topic>.md`) — [`doc-working-design`](doc-working-design.md)
- Task plan (`plan-<topic>.md`) — [`doc-working-plan`](doc-working-plan.md)
- Formal design doc (folding target) — [`doc-design`](doc-design.md)

## Lifecycle

```
1. Understand       → Read relevant code + design docs, confirm the problem.
2. Design           → Write doc/working/design-<topic>.md (doc-working-design guide).
3. Plan             → Write doc/working/plan-<topic>.md (doc-working-plan guide).
4. Implement        → Code changes per plan, run tests per acceptance criteria.
5. Commit           → Commit code + tests + working docs (design draft, plan).
6. Affected tests   → Run only the test commands touched by the changes (see
                      below). Every selected test must pass — no skips. Full
                      suite runs in Step 9.
7. Merge design     → Fold the design draft into the formal design doc it
                      belongs to, following the doc-design guide's Folding
                      section. Delete the standalone design-<topic>.md.
8. Cleanup          → Delete R**-<component>-<topic>.md + its backlog.md entry.
                      Delete plan-<topic>.md. Commit cleanup (final commit).
9. Local CI check   → Before pushing: pixi run cargo fmt --all -- --check,
                      pixi run cargo clippy --all-targets -- -D warnings,
                      all Test Commands below (each separately). All must pass.
```

## Test Commands

Prefix server-spawning tests with `pixi run clean-env &&` to reset test
state. Non-server tests can run back-to-back without clean-env.

Non-server (no clean-env needed):

- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run test-common`
- `pixi run test-protocol`
- `pixi run test-kv-core`
- `pixi run test-kv-client`
- `pixi run test-diskdb-client`

Server-spawning (prefix with `pixi run clean-env &&`):

- `pixi run test-kv-server`
- `pixi run test-diskdb`
- `pixi run test-console-shared`
- `pixi run test-console-cli`
- `pixi run test-console-server`
- `pixi run test-console-ui`

Step 6 runs the subset affected by the changes; Step 9 runs all of them.

## Blocking Conditions

The workflow runs autonomously end-to-end. Stop and ask the user only in
these two cases — in both, append a `## Blocked` section to
`doc/working/plan-<topic>.md`, commit, then wait:

1. **Design gap** (Step 2 or 3) — architectural decision with multiple valid
   approaches and no clearly superior choice. Record: the decision, the
   alternatives + trade-offs, why it cannot be resolved automatically.
2. **Test failure after 5 retries** (Step 4, 6, or 9) — record: failing test
   name(s) + reproduce command, what was attempted per retry, root-cause
   analysis, exact error output. Do not guess fixes or weaken tests.

## Commit Cadence

At least **two commits** per requirement:

1. **Implementation commits** (Step 4) — one per plan task (or grouped for
   small tasks). The final implementation commit includes the design draft
   and plan doc.
2. **Cleanup commit** (after Step 8) — merged design doc + deletion of
   working docs, `R**-<component>-<topic>.md`, and its `backlog.md` entry.

All commits must pass the pre-commit quality gate (fmt, clippy, tests).
