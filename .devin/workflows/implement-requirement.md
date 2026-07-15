<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: Lifecycle for implementing requirement items from doc/working/new_requirements.md
---

# CrowKV - Implement Requirement Flow

Use this workflow when picking up an item from
`doc/working/new_requirements.md`. Each item follows the full lifecycle below.

## Lifecycle

```
1. Understand    → Read relevant code + design docs, confirm the problem
2. Design        → Write doc/working/design-<topic>.md
                   - Problem statement + current behavior
                   - Proposed approach + alternatives considered
                   - Acceptance test plan (what tests prove it works)
3. Plan          → Write doc/working/plan-<topic>.md
                   - Task breakdown with checkboxes
                   - File-level changes
                   - Dependency ordering
                   - Track progress here
4. Implement     → Code changes per plan, run tests per acceptance criteria
5. Commit        → Commit all code + tests + working docs (design draft, plan doc)
                   This checkpoint preserves work in git history in case of
                   later blocking or human intervention.
6. Merge design  → Fold the design doc into the formal design doc it belongs
                   to (e.g. design-crowtree-engine.md, design-wal.md), following
                   that doc's style and detail level. Delete the standalone
                   working/design-<topic>.md.
7. Cleanup       → Mark item done in new_requirements.md, delete plan-<topic>.md
                   and design-<topic>.md
                   → Commit cleanup (second and final commit)
```

## Blocking Conditions

The workflow runs autonomously end-to-end. Stop and ask the user only in
the following two cases:

### 1. Design gap requiring human decision

During Step 2 (Design) or Step 3 (Plan), if there is an architectural
decision with multiple valid approaches and no clearly superior choice,
append the gap to the end of `doc/working/plan-<topic>.md` under a
`## Blocked` heading with:

- The decision that needs to be made.
- The alternatives considered and their trade-offs.
- Why it cannot be resolved automatically.

Commit the plan doc with the gap recorded, then stop and wait for human
review.

### 2. Test failure after 5 retries

During Step 4 (Implement), if a test fails (new or existing regression)
and remains failing after 5 retry attempts, append to the end of
`doc/working/plan-<topic>.md` under a `## Blocked` heading with:

- The failing test name(s) and command to reproduce.
- What was attempted in each retry.
- Preliminary root-cause analysis.
- The exact error output.

Commit current work, then stop and wait for human intervention. Do not
guess fixes or weaken tests.

In all other cases, proceed autonomously through all seven steps.

## Design doc style (from existing design docs)

- **Problem-first**: state what's broken/missing before proposing solutions
- **Alternatives**: list rejected approaches with rationale
- **Code-grounded**: reference actual file paths, function names, line numbers
- **Concise**: aim for the detail level of existing §-level content in
  `design/design-crowtree-engine.md` or `design/design-crowtree-storage.md`
- **Acceptance criteria**: concrete, testable conditions

## Plan doc style

- Task breakdown as `- [ ]` checkboxes
- One task in progress at a time
- File list with intended changes
- Test checklist
- Update `doc/doc_index.md` if a new formal doc is added (not for working docs)

## Commit Cadence

The workflow produces at least **two commits** per requirement:

1. **Implementation commits** (during Step 4) — commit after each task in the
   plan completes. For small tasks a single commit covering multiple tasks is
   fine; for large or independent tasks, one commit per task keeps the history
   reviewable. Use judgment based on task type and change size. The final
   implementation commit includes the design draft and plan doc, preserving
   the full working state in git history.
2. **Cleanup commit** (after Step 7) — merged design doc, deletion of
   working docs, item marked done in new_requirements.md.

All commits must pass the pre-commit quality gate (fmt, clippy, tests).
