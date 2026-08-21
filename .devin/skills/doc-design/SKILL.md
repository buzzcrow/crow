---
name: doc-design
description: How to write and refine doc/design/<area>/design-crow-<area>(-<topic>)?.md
triggers:
  - user
  - model
---

<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Formal Design Doc Guide

How to write and refine `doc/design/<area>/design-crow-<area>.md` (root)
and `doc/design/<area>/design-crow-<area>-<topic>.md` (sub-design) — the
permanent, human-readable design record. `<area>` ∈ `kv`, `tree`,
`console`, `diskdb`, `diskio`, `protocol`, `rpc`.

**Audience:** humans reading to understand the design, flow, and choices;
AI reading doc + code to understand and continue the work. After each
backlog item lands, the working draft folds back in
(`/implement-requirement` Step 7) so this stays the single source of
truth for the current state.

## Two Flavors

- **Root** (`design-crow-<area>.md`) — *what* the area is, *why* key
  choices were made, *how* it is structured. Architecture and decisions
  only; a Non-Goals section bounds the envelope; maps its sub-designs.
- **Sub-design** (`design-crow-<area>-<topic>.md`) — detailed design for
  one topic. Created when a topic exceeds ~200 lines or has independent
  phases (Core Rule 5). Carries `Depends on:` / `Satisfies:` linking
  upstream. No Non-Goals (the root sets the envelope once).

## Structure

- **Header** — license comment, then `# CROW - Design: <Title>` (root
  may append `(Overview)`).
- **Depends on / Satisfies** (sub-design only) — two lines after the
  title: `Depends on:` links upstream docs; `Satisfies:` lists the
  upstream `§X` sections this doc fulfills.
- **Intro** — one or two paragraphs: scope, and (sub-design) where
  architecture decisions live vs. implementation detail here.
- **Table of Contents** — `## Table of Contents` + `- [N. Title](#n-title)`
  links to every `##` section.
- **Numbered sections** — `## N. <Title>`, sub-sections `### N.M`.
  Common spine (omit any that don't apply):
  - Why / Problem — the one place the doc argues *why*.
  - Concepts and Invariants — named (`I1`, `I2`, ...), one line each;
    the correctness backbone referenced by later sections.
  - Design / How — structs, algorithms, RPC shapes, data flow.
  - Interaction with neighbors — cross-link adjacent topics by `§X`.
  - Tunables and Defaults — table of parameters, defaults, where each
    lives.
  - Correctness Analysis — premises → claim → sketch that invariants
    hold.
  - Risks / Open Questions — only if any.

## Writing Rules

- **Current state, not change history** (Core Rule 9) — no `R**` references, no "pre-R**", "post-R**", "legacy", "supersedes", or before/after narratives. Change history lives in commit messages.
- **Single source of truth** (Core Rule 3) — architecture in the root doc, detail in sub-designs, user ops in `user-manual/user-guide.md`. Link upstream by `§X` anchor instead of duplicating.
- **Traceability** (Core Rule 4) — sub-designs link upstream via `Depends on` / `Satisfies` + inline `§X`; root links downstream to its sub-designs.
- **Name, don't cite** — reference function/struct/type/module *names* (`PxSlotNode`, `SlotList<T>`, `PaxosConfig`), which are searchable and survive most refactors. Avoid file paths and line numbers — they go stale on every move and rename.
- **Raw-readable formatting** (Core Rule 8) — prefer bullets and definition lists. Tables only for data/metric comparison (tunables, invariants, benchmarks). `doc_index.md` is the exception (always tables).
- **Fenced blocks for code and diagrams** — Rust/proto signatures, ASCII data-structure/timing diagrams go in fenced blocks. Behavior steps use lettered or numbered prose.
- **Named invariants** — give every correctness property a stable identifier (`I1`, `I2`, ...) so other docs and code comments can reference it by name.
- **Index sync** (Core Rule 1) — update `doc/doc_index.md` when adding, renaming, deleting, or re-scoping a formal design doc. One row per doc, one line per `##` section.

## Folding a Working Draft In

See `/implement-requirement` Step 7. In short: pick the target doc (root
vs. existing vs. new sub-design), drop the draft scaffolding and all
`R**` references, re-prose as current state, renumber + update ToC,
update `doc_index.md`, delete the working draft.
