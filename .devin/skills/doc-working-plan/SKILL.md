---
name: doc-working-plan
description: How to write doc/working/plan-<topic>.md (task plan)
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan Doc Guide

How to write `doc/working/plan-<topic>.md` — the task plan produced during
Step 3 of the `/implement-requirement` skill. This is the execution
checklist: it breaks the design into ordered, checkbox-tracked tasks, lists
the files each task touches, and records progress. It is deleted after merge
(unless it is a persistent backlog like `plan-test.md`, which states an
explicit override in its header).

Use this guide when writing the plan for a requirement. The existing
`plan-test.md` is an example of the persistent variant; a normal per-requirement
plan is simpler.

## Structure

- **Header** — license comment, then `# <Title> Plan`.
- **Intro** — one line linking to the design draft
  (`doc/working/design-<topic>.md`) and the backlog doc. State the goal in
  one sentence.
- **Task sections** — group tasks by phase or component (e.g. "Proto +
  client", "Recovery engine", "Compaction engine", "Server wiring",
  "Tests"). Each section is a `##` heading. Within each section:
  - Tasks as `- [ ]` checkboxes. Each task is a single, completable unit:
    `- [ ] **<task name>**: <description>. Files: <paths>.`
  - One task `in_progress` at a time (mark it `- [~]` or note it in the
    section). Update checkboxes as work proceeds.
  - Order tasks by dependency — a task that produces a type used by a later
    task comes first. Note blocking dependencies inline
    (`blocked on: JournalScan client method`).
- **File list** — a consolidated bullet list of every file the requirement
  touches, with the intended change per file. This mirrors the design doc's
  Module Structure and Scope; the plan is where they are tracked for
  execution.
- **Test checklist** — a `- [ ]` list of the tests to write/run, drawn from
  the design doc's Test Strategy and the backlog doc's Acceptance. Group by
  layer (unit, integration, E2E). Each test names what it verifies.
- **Blocked** (only if blocked) — a `## Blocked` section at the end, per the
  `/implement-requirement` blocking conditions:
  - The decision or failure that needs human input.
  - The alternatives considered and their trade-offs (for a design gap) or
    the retry attempts and error output (for a test failure).
  - Why it cannot be resolved automatically.

## Writing rules

- **Checkbox-driven** — every unit of work is a `- [ ]`. If a task cannot be
  checked off in one sitting, it is too coarse — split it.
- **Dependency-ordered** — tasks are listed in the order they must be done.
  If a task can be parallelized with another, note it; otherwise the order is
  the execution order.
- **File-level granularity** — each task names the files it touches. The
  reviewer should be able to map each checkbox to a diff hunk.
- **Progress is truth** — update the plan as you go. A completed task is
  `- [x]`; an in-progress task is `- [~]`. The plan is the live status of the
  requirement, not a static artifact.
- **Persistent variant** — if the plan is a long-lived backlog (like
  `plan-test.md`), state the override in the header: "This file is
  persistent — it is not deleted after the requirement is complete." Only
  completed tasks are removed; the file remains.
