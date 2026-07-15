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
doc_index.md                        (table of contents — the only file at doc root)
design/
    ├── design.md                   (root design: what + why + architecture)
    └── design-xxx.md               (sub-design docs)
user-manual/
    ├── user-guide.md               (user guide: Web UI, CLI, REST API)
    └── build_html.py               (MD → HTML converter with tabs)
working/
    ├── plan-<topic>.md              (task plans, deleted after merge)
    ├── design-<topic>.md            (design drafts, deleted after merge)
    └── new_requirements.md          (implementation backlog)
```

## Naming

Sub-design topics: `lowercase-kebab-case`. Examples: `design/design-wal.md`, `design/design-paxos.md`.

## Working Docs (`doc/working/`)

- `doc/working/plan-<topic>.md` — task plans with checkboxes, file-level changes, dependency ordering.
- `doc/working/design-<topic>.md` — design drafts created during requirement implementation, folded into formal design docs and deleted after merge.
- `doc/working/new_requirements.md` — forward-looking implementation backlog.
- Create when starting a new effort; delete when the effort is complete (after PR merge). Do not leave fully-completed plan or design-draft docs in the repo.
- Do NOT add temporary working docs to `doc/doc_index.md` — the index only tracks long-lived, permanent documentation.

## Core Rules

1. **Index first** — read `doc_index.md` before opening any other doc; update it in the same commit when you add, rename, delete, or re-scope a doc. One row per doc, one line per `##` section.
2. **No upstream violations** — fix `design/design.md` first if a gap is found.
3. **Single source of truth** — design decisions in `design/design.md`, detailed design in `design/design-xxx.md`, user operations in `user-manual/user-guide.md`.
4. **Traceability** — every doc links upstream via section anchors.
5. **Sub-topic split** — when a design topic exceeds ~200 lines or has independent phases, create `design/design-xxx.md` and add a row to `doc_index.md`.
6. **Working doc hygiene** — delete `doc/working/plan-<topic>.md` and `doc/working/design-<topic>.md` when the effort is complete. Do not add temporary working docs to `doc_index.md`.
7. **Raw-readable formatting** — docs are read as raw markdown most times, not rendered. Avoid tables; use definition lists (`- **term**: description`) or nested bullets instead. Tables are only acceptable in `doc_index.md`.
