---
name: implement-requirement
description: Implement a CROWDB backlog requirement through design and cleanup.
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Implement Requirement

Use for an item in `doc/backlog/backlog.md`. Open its detail and matched doc
guides before creating artifacts.

1. Read relevant code and indexed design; confirm the problem.
2. Create `doc/working/design-<topic>.md` with `/doc-working-design`.
3. Create `doc/working/plan-<topic>.md` with `/doc-working-plan`.
4. Implement in plan order and keep the plan current.
5. Commit code, tests, design, and plan by task; group only small related tasks.
6. Run every affected acceptance test separately; no skips.
7. Fold the draft into formal design with `/doc-design`; delete the draft.
8. Delete the requirement, backlog entry, and completed plan. Commit this
   cleanup separately.
9. Before push, run `pixi run -- cargo fmt --all -- --check`,
   `pixi run rs-lint`, and `pixi run test-suite`.

Discover affected tests with `pixi task list`. Prefix server-spawning tests
with `pixi run clean-env &&`; use it when uncertain. Step 6 runs the affected
tasks separately; `test-suite` owns the ordered full local CI set in Step 9.

Proceed autonomously except when a design has multiple valid choices with no
clear winner, or a test still fails after five root-cause-driven attempts.
Then add `## Blocked` to the plan with trade-offs or the command, attempts,
diagnosis, and exact failure; commit that state and ask the user.

This overrides ordinary commit cadence: use implementation commit(s) plus a
separate final design/cleanup commit. Every commit passes its applicable gate.
