---
name: doc-working-plan
description: Write and maintain a requirement execution plan.
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Working Plan

Write `doc/working/plan-<topic>.md` from the working design and acceptance
criteria. It is the live checklist and is deleted after completion.

- Start with the license, `# <Title> Plan`, upstream links, and one-line goal.
- Group dependency-ordered tasks by phase or component.
- Use `- [ ] **Name**: action. Files: paths.`; `[~]` marks the only active
  task and `[x]` a verified task.
- Split tasks until each is completable and maps to identifiable diff hunks.
- Include a consolidated file list and tests grouped by unit, integration, E2E.
- Keep status truthful as work proceeds.
- Add `## Blocked` only under `/implement-requirement` conditions, recording
  the decision/failure, alternatives or retries, and why work cannot continue.

For a persistent plan, state the exception in its header and remove completed
tasks instead of deleting the file.
