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
5. Merge design  → Fold the design doc into the formal design doc it belongs
                   to (e.g. design-crowtree-engine.md, design-wal.md), following
                   that doc's style and detail level. Delete the standalone
                   working/design-<topic>.md.
6. Cleanup       → Mark item done in new_requirements.md, delete plan-<topic>.md
```

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
- Update `doc/doc_index.md` if a new doc is added

## Post-merge cleanup

After the Pull Request is merged:

1. **Merge design doc** — fold `doc/working/design-<topic>.md` into the formal
   design doc it belongs to, then delete the standalone file.
2. **Delete plan doc** — remove `doc/working/plan-<topic>.md`.
3. **Mark item done** — check off the item in `doc/working/new_requirements.md`
   (or remove it if all items in a section are complete).
4. **Update index** — update `doc/doc_index.md` if any doc was added, renamed,
   or deleted during the process.
5. **Remove all obsolete working docs** — delete any `doc/working/plan-<topic>.md`
   and `doc/working/design-<topic>.md` files that were created for this item.
   The `doc/working/` directory should only contain actively-tracked items.
   Do not create standalone design docs under `doc/design/` during the
   implementation process — that directory is reserved for formal, permanent
   design docs only.
