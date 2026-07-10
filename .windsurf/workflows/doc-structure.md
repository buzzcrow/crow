---
description: CrowKV documentation hierarchy and conventions
---

# CrowKV Documentation Structure

Quick reference.

## Hierarchy

```
requirement.md
    ├── design.md → design/design-xxx.md
    ├── plan.md → plan/plan-xxx.md
    ├── test-design.md → test/test-design-xxx.md
    └── test-plan.md → test/test-plan-xxx.md
```

## Naming

Sub-topics: `lowercase-kebab-case`.

Examples: `design/design-wal.md`, `plan/plan-leader-election.md`, `test/test-design-storage.md`, `test/test-plan-storage.md`.

## Core Rules

1. **No upstream violations** — fix `requirement.md` first if a gap is found.
2. **Single source of truth** — requirements only in `requirement.md`, design in `design.md`/`design/design-xxx.md`, tests in `test-design.md`/`test/test-design-xxx.md`.
3. **Traceability** — every doc links upstream via section anchors.
4. **Sub-topic split** — when a topic exceeds ~200 lines or has independent phases.
