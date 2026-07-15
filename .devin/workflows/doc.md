<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: CrowKV documentation hierarchy and conventions
---

# CrowKV Documentation Structure

## Entry Point

**Always start at `doc/doc_index.md`** — one-line pointers to every doc and section. Open detailed docs only when the index row matches the task.

## Hierarchy

```
doc_index.md            (table of contents for all docs below)
requirement.md
    └── design/design-xxx.md
working/
    ├── plan-<topic>.md     (task plans, deleted after merge)
    └── new_requirements.md (implementation backlog)
```

## Naming

Sub-design topics: `lowercase-kebab-case`. Examples: `design/design-wal.md`, `design/design-paxos.md`.

## Working Docs (`doc/working/`)

- `doc/working/plan-<topic>.md` — task plans with checkboxes, file-level changes, dependency ordering.
- `doc/working/new_requirements.md` — forward-looking implementation backlog.
- Create when starting a new effort; delete when the effort is complete (after PR merge). Do not leave fully-completed plan docs in the repo.
- Update `doc/doc_index.md` when adding or removing working docs.

## Core Rules

1. **Index first** — read `doc_index.md` before opening any other doc; update it in the same commit when you add, rename, delete, or re-scope a doc. One row per doc, one line per `##` section.
2. **No upstream violations** — fix `requirement.md` first if a gap is found.
3. **Single source of truth** — requirements in `requirement.md`, design in `design/design-xxx.md`.
4. **Traceability** — every doc links upstream via section anchors.
5. **Sub-topic split** — when a design topic exceeds ~200 lines or has independent phases, create `design/design-xxx.md` and add a row to `doc_index.md`.
6. **Working doc hygiene** — delete `doc/working/plan-<topic>.md` when the effort is complete (and remove from `doc_index.md`).
7. **Raw-readable formatting** — docs are read as raw markdown most times, not rendered. Avoid tables; use definition lists (`- **term**: description`) or nested bullets instead. Tables are only acceptable in `doc_index.md`.
