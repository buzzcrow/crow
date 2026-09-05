---
name: doc-working-design
description: Write a temporary implementation design for a requirement.
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Working Design

Write `doc/working/design-<topic>.md` during `/implement-requirement`. Link
the backlog and root design; do not repeat their problem, dependencies,
architecture, or rationale.

Include:

- License, `# <Title> (R**)`, and a short introduction with upstream links and
  landed dependencies.
- Numbered solution sections by component: why a new mechanism is needed,
  signatures, behavior, failures, and fallback.
- `Scope`: every changed path and intended change.
- `Complexity`: High/Medium/Low and the actual implementation difficulty.
- `Test Design`: concrete unit and E2E cases for every acceptance item and
  edge case; each states setup -> action -> assertion and invariant.
- `Module Structure`: annotated file tree.
- `Config Extensions` and `Server Wiring` when applicable.
- `Open Questions` only for unresolved human decisions, with trade-offs.

Use real paths and symbols. Fence signatures and diagrams; use ordered prose
for behavior. Requirement references are allowed only while the draft is
temporary and are removed when it is folded into formal design.
