---
name: doc
description: Route and maintain CROWDB documentation.
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Documentation

Start at `doc/doc_index.md`; open only the row and section matching the task.

## Locations

- `doc/design/<area>/design-crowdb-<area>.md`: architecture and rationale.
- `doc/design/<area>/design-crowdb-<area>-<topic>.md`: permanent topic detail.
- `doc/design/kv/kv-*-flow-analysis.md`: permanent KV path analysis.
- `doc/user-manual/user-guide.md`: operations; generated HTML is not hand-edited.
- `doc/backlog/`: requirement index and analysis.
- `doc/working/`: temporary implementation designs and plans.

## Rules

- Keep one source of truth. Link by section anchor instead of copying text.
- Fix missing or conflicting upstream design before downstream docs or code.
- Update `doc_index.md` with permanent doc add/remove/rename/rescope changes.
  Do not index backlog or working docs.
- Split a topic near 200 lines or when it has independent phases.
- Permanent design describes current state without requirement numbers,
  migration history, or before/after narrative.
- Delete working docs when their requirement is complete.
- Prefer concrete, tight prose and raw-readable bullets. Remove filler,
  repetition, rhetorical openings, adjective lists, and excessive em dashes.
- Rebuild `user-guide.html` with
  `pixi run -- python doc/user-manual/build_html.py` whenever its Markdown
  source changes.

Before editing, read `/doc-design`, `/doc-backlog`,
`/doc-working-design`, or `/doc-working-plan` as applicable.
