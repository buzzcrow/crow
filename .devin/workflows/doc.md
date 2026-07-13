---
description: CrowKV documentation hierarchy and conventions
---

# CrowKV Documentation Structure

Quick reference.

## Entry Point

**Always start at `doc/doc_index.md`** — one-line pointers to every doc and
section. Open detailed docs only when the index row matches the task.

## Hierarchy

```
doc_index.md            (table of contents for all docs below)
requirement.md
    ├── plan.md
    └── design/design-xxx.md
```

## Naming

Sub-design topics: `lowercase-kebab-case`.

Examples: `design/design-wal.md`, `design/design-paxos.md`.

## Temporary TODO Docs

- `todo_plan.md` — tracks unfinished planning tasks

**Lifecycle:**
1. Create when you need to track multiple related tasks
2. Mark tasks as complete when finished (strikethrough or checkbox)
3. Delete the file when all tasks are complete

Do not leave empty or fully-completed TODO files in the repo.

## Core Rules

1. **Index first** — read `doc_index.md` before opening any other doc, and update it in the same commit when you add, rename, delete, or materially re-scope a doc (top-level or sub-design). Keep one row per doc, one line per `##` section.
2. **No upstream violations** — fix `requirement.md` first if a gap is found.
3. **Single source of truth** — requirements only in `requirement.md`, design in `design/design-xxx.md`, plans in `plan.md`.
4. **Traceability** — every doc links upstream via section anchors.
5. **Sub-topic split** — when a design topic exceeds ~200 lines or has independent phases, create `design/design-xxx.md` and add a row to `doc_index.md`.
6. **TODO hygiene** — delete `todo_plan.md` when all tasks are complete (and remove from `doc_index.md`).
7. **Raw-readable formatting** — docs are read as raw markdown in most timeß, not rendered. Avoid tables; use definition lists (`- **term**: description`) or nested bullet lists instead. Tables are only acceptable in `doc_index.md` (which is a reference index, not prose). 
