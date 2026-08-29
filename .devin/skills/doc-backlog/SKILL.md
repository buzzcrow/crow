---
name: doc-backlog
description: How to write doc/backlog/R**-<component>-<topic>.md (per-requirement analysis)
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Backlog Requirement Doc Guide

How to write `doc/backlog/R**-<component>-<topic>.md` — the per-requirement
analysis doc. Entry artifact for every backlog item: states the problem (with
use scenarios), the high-level approach, dependencies, and acceptance
criteria. Deleted after merge (see `/implement-requirement`); design content
is folded into the formal design doc. File-level scope and complexity live in
the design draft (`doc/working/design-<topic>.md`), not here.

## Structure

Every `R**` doc uses exactly these sections, in this order:

- **Header** — license comment (Apache 2.0), then `### R**:
  <component> — <Title>`. `<component>` is the owning crate/area
  (`kv`, `tree`, `console`, `client`, `server`, `diskdb`). Title is a short
  noun phrase.
- **Problem** — what is broken or missing, plus the scenarios that exercise
  it. About the *problem*, not the solution.
  - **Current behavior + impact** — current behavior, why it matters (what
    breaks without this work), root cause if any (deferred placeholder,
    missing extension).
  - **Design pointers** — reference the root design doc by `§X` anchor; never
    paraphrase.
  - **Use scenarios** — bullet list of concrete operating scenarios as short
    end-to-end narratives: who/what triggers, what the system does, expected
    outcome. Written from the operator/system perspective, not internal
    function calls. Each scenario maps to at least one acceptance bullet —
    these are the primary input to E2E and component-level tests.
- **Solution** — high-level approach: *what* is built and *how the pieces fit
  together*. Implementation detail (signatures, lettered steps, edge-case
  handling) goes in the design draft, not here.
  - **No clear solution yet** — if the approach is unsettled, write
    `**No clear solution yet — deferred to design.**`, then list open
    questions or candidate directions. Do not invent a premature solution.
    Acceptance can still be written from the problem statement; mark
    approach-dependent bullets `pending design`.
  - **One-line summary** — single sentence stating the approach.
  - **Numbered work items** — coherent units of work (new module, proto
    change, integration point). Each: bold name + file/module, 1–3 sentences
    of *what* and *why* (not *how*). State cross-component dependencies inline.
  - **Flow diagram** — ASCII diagram in a fenced block, required when the
    solution has 3+ interacting components or a primary/fallback path. Shows
    the *shape*: modules + arrows + fallback branches, not field-level detail.
  - **Edge cases at a glance** — short bullet list with one-line outcome each
    ("CRC fails → fall back to full scan"). States *that* it's handled and
    *what* the outcome is; the *how* goes in the design doc.
- **Dependencies** — which `R**` items this depends on (name the specific
  artifact: types, clients, structs) and which depend on it. If a dependency
  is on an unlanded extension, say so and name the fallback.
- **Acceptance** — the testable contract. The most important section: source
  for the design draft's Test Strategy and the plan's test checklist; what a
  reviewer checks against the final diff. Each bullet maps to one test case.
  - **Layer tag** — every bullet tagged `Unit test` / `Integration test` /
    `E2E test`.
  - **Group by feature** — cluster under bold lead-ins
    (`**Recovery (strategy 2)**:`).
  - **Complete test spec** — each bullet is `setup → action → assertion +
    layer tag`. Example: `recover_zone() with snapshot at slot 10 + Put
    BusyBlockKey at slots 11–15 → recover → verify bits 11–15 set,
    used_count = 5 * unit_count. Unit test.`
  - **Cover every work item** — every Solution item needs a matching bullet;
    if none, the Solution is under-specified or acceptance is incomplete.
  - **Cover every edge case** — every edge case in Solution (CRC failure, GC
    gap, fallback, crash-mid-operation, empty/corrupted input) needs a bullet.
  - **Cover invariants, not just happy paths** — name the invariant guarded
    ("B's bitmap matches A's records, not A's stale state").
  - **Name the test command** — end with the specific `pixi run test-*`
    commands + `pixi run cargo fmt --all -- --check` and
    `pixi run cargo clippy --all-targets -- -D warnings`.
- **Open Questions** (last section, only if needed) — issues that need
  discussion with a human, or decisions that cannot be made autonomously.
  Each item: the question or decision needed, the alternatives considered
  and their trade-offs, and why it cannot be resolved automatically. Do not
  guess or invent a decision — leave it open until reviewed.

## Writing rules

- **Problem-first** — a reviewer who reads only **Problem** should know why
  this work exists.
- **Design pointers, not design copies** — reference `§X` of the root design
  doc for rationale. The backlog is the *what* and *where*; the design doc is
  the *why*. If the design doc lacks a needed section, flag it as a design
  gap rather than inventing architecture here.
- **Code-grounded** — every work item names real file paths and function/
  struct names ("new module" + intended path if not yet existing). Vague
  items ("improve recovery") are not acceptable.
- **Acceptance is exhaustive and test-shaped** — every behavioral claim in
  **Solution** has a matching test-shaped bullet. If you can't write one, the
  Solution item is too vague — tighten it.
- **R-numbers allowed** — unlike formal design docs (Core Rule 9), backlog
  docs reference `R**` and `§` freely; they are deleted after merge.
- **Raw-readable** — no tables (Core Rule 8). Nested bullets and lettered
  steps. No signatures (they go in the design draft); fenced blocks are only
  for flow diagrams.
