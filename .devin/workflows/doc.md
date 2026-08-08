<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: CROW documentation hierarchy and conventions
---

# CROW Documentation Structure

## Entry Point

**Always start at `doc/doc_index.md`** — one-line pointers to every doc and section. Open detailed docs only when the index row matches the task.

## Hierarchy

```
doc_index.md                        (table of contents — the only file at doc root)
design/
    ├── {kv,tree,console}/          (one subdir per component area)
    │   ├── design-crow-<area>.md   (root sub-design for that area)
    │   ├── design-crow-<area>-<topic>.md  (sub-design docs)
    │   └── kv-{read,scan,write}-flow-analysis.md  (KV only: long-lived flow analyses)
user-manual/
    ├── user-guide.md               (user guide: Web UI, CLI, REST API)
    ├── user-guide.html             (build artifact — do not hand-edit)
    └── build_html.py               (MD → HTML converter with tabs)
backlog/
    ├── backlog.md                  (backlog index: R** list with brief intros)
    └── R**-<component>-<topic>.md  (per-requirement detail, deleted after merge)
working/
    ├── plan-<topic>.md             (task plans, deleted after merge)
    └── design-<topic>.md           (design drafts, deleted after merge)
```

## Naming

Sub-design topics: `lowercase-kebab-case`. Examples: `design/design-wal.md`, `design/design-paxos.md`.

Backlog requirements: `R**-<component>-<topic>.md`, where `<component>` is the owning crate/area
(`kv`, `tree`, `console`, `client`, `server`). Examples: `R32-kv-custom-rust-rpc.md`,
`R52-tree-reverse-scan.md`. A requirement is prefixed by the component that owns the work, not
every component it touches — cross-component requirements take the prefix of the primary owner.

## Backlog (`doc/backlog/`)

- `backlog.md` — index of requirements (`R**`) with priority/complexity and a brief intro. Remove the entry when implemented and merged.
- `R**-<component>-<topic>.md` — per-requirement analysis (problem, approach, files, acceptance). `<component>` is the owning crate/area (`kv`, `tree`, `console`, `client`, `server`). Deleted after merge; design content is folded into `design/design-xxx.md`.

## Working Docs (`doc/working/`)

- `plan-<topic>.md` — task plans with checkboxes, file-level changes, dependency ordering.
- `design-<topic>.md` — design drafts, folded into formal design docs and deleted after merge.
- Create when starting a new effort; delete when complete (after PR merge).

## Flow-Analysis Docs (`doc/design/kv/`)

- `kv-read-flow-analysis.md` / `kv-scan-flow-analysis.md` / `kv-write-flow-analysis.md` — long-lived per-path flow traces, benchmark results, and open issues. Not deleted after a single requirement. Live under `doc/design/kv/` (not `doc/working/`) because they are permanent design references.

## Core Rules

1. **Index first** — read `doc_index.md` before opening any other doc; update it in the same commit when you add, rename, delete, or re-scope a doc. One row per doc, one line per `##` section.
2. **No upstream violations** — fix `design/design.md` first if a gap is found.
3. **Single source of truth** — design decisions in `design/design.md`, detailed design in `design/design-xxx.md`, user operations in `user-manual/user-guide.md`.
4. **Traceability** — every doc links upstream via section anchors.
5. **Sub-topic split** — when a design topic exceeds ~200 lines or has independent phases, create `design/design-xxx.md` and add a row to `doc_index.md`.
6. **No temp docs in `doc_index.md`** — the index tracks long-lived permanent docs only. Backlog and working docs are self-indexed.
7. **Working doc hygiene** — delete `plan-<topic>.md` and `design-<topic>.md` when the effort is complete.
8. **Raw-readable formatting** — docs are read as raw markdown most times, not rendered. Prefer definition lists or nested bullets. Tables only for data/metric comparison; `doc_index.md` always uses tables.
9. **Design docs describe current state, not change history** — no "pre-R**", "post-R**", "legacy", "supersedes", "before/after" narratives. No R-number references — backlog docs are deleted, making them dead links. Change history belongs in commit messages.
10. **Rebuild HTML when user-guide.md changes** — run `python3 doc/user-manual/build_html.py` in the same commit. Never hand-edit `user-guide.html` or use other markdown tools.
