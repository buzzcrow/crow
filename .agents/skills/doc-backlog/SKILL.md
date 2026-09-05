---
name: doc-backlog
description: Write a CROWDB per-requirement backlog analysis.
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Backlog Requirement

Write `doc/backlog/R**-<component>-<topic>.md` as one requirement's testable
contract. It is removed by `/implement-requirement`.

Use this exact order:

1. `### R**: <component> — <Title>` after the license.
2. Optional `Status` when the item is explicitly deferred or blocked; state the
   reason and what would unblock it.
3. `Problem`: current behavior, impact, root cause, root-design link, and
   concrete operator/system scenarios.
4. `Solution`: one-line approach, numbered work items naming real modules, and
   edge-case outcomes. If unsettled, state `No clear solution yet - deferred
   to design` and list decisions instead of inventing one.
5. `Dependencies`: incoming/outgoing requirements, named artifacts, and
   fallback for unlanded work.
6. `Acceptance`: one test case per solution and edge-case claim.
7. `Open Questions`, only for human decisions; include alternatives and trade-offs.

Each acceptance case states setup -> action -> assertion, names the invariant,
and ends with `Unit test`, `Integration test`, or `E2E test`. End with exact
pixi test, fmt, and clippy commands.

Keep architecture in formal design and signatures/file-level detail in the
working design. Requirement numbers are allowed here. Use an ASCII flow only
for three or more components or a primary/fallback path. Do not use tables.
