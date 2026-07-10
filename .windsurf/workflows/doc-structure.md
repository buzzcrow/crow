---
description: CrowKV documentation hierarchy and conventions
---

# CrowKV Documentation Structure

Quick reference. Detailed rules live in `.windsurfrules`.

## Hierarchy

```
requirement.md
    ├── design.md → design-xxx.md
    ├── plan.md → plan-xxx.md
    ├── test-design.md → test-xxx-design.md
    └── test-plan.md → test-xxx-plan.md
```

## Naming

Sub-topics: `lowercase-kebab-case`.

Examples: `design-wal.md`, `plan-leader-election.md`, `test-storage-design.md`, `test-storage-plan.md`.

## Core Rules

1. **No upstream violations** — fix `requirement.md` first if a gap is found.
2. **Single source of truth** — requirements only in `requirement.md`, design in `design.md`/`design-xxx.md`, tests in `test-design.md`/`test-xxx-design.md`.
3. **Traceability** — every doc links upstream via section anchors.
4. **Sub-topic split** — when a topic exceeds ~200 lines or has independent phases.
