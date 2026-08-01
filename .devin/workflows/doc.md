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
backlog/
    ├── backlog.md             (backlog index: R** list with brief intros)
    └── R**-<topic>.md              (per-requirement detail, deleted after merge)
working/
    ├── plan-<topic>.md              (task plans, deleted after merge)
    ├── design-<topic>.md            (design drafts, deleted after merge)
    ├── read-flow-analysis.md       (read path gap analysis)
    └── write-flow-analysis.md      (write path trace + optimizations)
```

## Naming

Sub-design topics: `lowercase-kebab-case`. Examples: `design/design-wal.md`, `design/design-paxos.md`.

## Backlog (`doc/backlog/`)

- `doc/backlog/backlog.md` — backlog index. Lists each requirement (`R**`) with priority/complexity classification and a brief intro. Each entry links to its detail doc. Remove the entry when the requirement is implemented and merged.
- `doc/backlog/R**-<topic>.md` — per-requirement detailed analysis (problem, approach, files, acceptance). Created when a requirement is first analyzed; deleted after the requirement is implemented and merged (the design content is folded into the formal `design/design-xxx.md` doc).
- Do NOT add requirement docs to `doc/doc_index.md` — the index tracks long-lived permanent docs; the backlog is self-indexed in `backlog.md`.

## Working Docs (`doc/working/`)

- `doc/working/plan-<topic>.md` — task plans with checkboxes, file-level changes, dependency ordering.
- `doc/working/design-<topic>.md` — design drafts created during requirement implementation, folded into formal design docs and deleted after merge.
- `doc/working/read-flow-analysis.md` / `write-flow-analysis.md` — flow analysis docs that track the current state and gaps of the read/write paths. These are long-lived working docs, not deleted after a single requirement.
- Create when starting a new effort; delete when the effort is complete (after PR merge). Do not leave fully-completed plan or design-draft docs in the repo.
- Do NOT add temporary working docs to `doc/doc_index.md` — the index only tracks long-lived, permanent documentation.

## Core Rules

1. **Index first** — read `doc_index.md` before opening any other doc; update it in the same commit when you add, rename, delete, or re-scope a doc. One row per doc, one line per `##` section.
2. **No upstream violations** — fix `design/design.md` first if a gap is found.
3. **Single source of truth** — design decisions in `design/design.md`, detailed design in `design/design-xxx.md`, user operations in `user-manual/user-guide.md`.
4. **Traceability** — every doc links upstream via section anchors.
5. **Sub-topic split** — when a design topic exceeds ~200 lines or has independent phases, create `design/design-xxx.md` and add a row to `doc_index.md`.
6. **Working doc hygiene** — delete `doc/working/plan-<topic>.md` and `doc/working/design-<topic>.md` when the effort is complete. Do not add temporary working docs to `doc_index.md`.
7. **Raw-readable formatting** — docs are read as raw markdown most times, not rendered. Prefer definition lists (`- **term**: description`) or nested bullets. Tables are allowed only when genuinely necessary for data/metric comparison (e.g. benchmark results with multiple columns per row); otherwise avoid them. `doc_index.md` always uses tables.
8. **Design docs describe current state, not change history** — `design/design-xxx.md` documents the system as it exists today, not a chronological record of changes. Avoid "pre-R**", "post-R**", "legacy", "supersedes", "before/after" narratives. Do not use R-number references (`(R30)`, `R6`) — backlog docs are deleted after implementation, making them dead links. Change history belongs in commit messages.
