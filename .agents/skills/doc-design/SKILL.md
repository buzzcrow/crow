---
name: doc-design
description: Write or refine a permanent CROWDB design document.
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Formal Design

Root docs define an area's architecture, rationale, non-goals, and sub-design
map. Sub-designs hold one topic's detail and link upstream with `Depends on:`
and `Satisfies:`.

Use a license, `# CROWDB - Design: <Title>`, short scope introduction, a table
of contents, and numbered `## N. <Title>` sections. Include only applicable
content: problem/why, concepts and named invariants, design, neighbor
interactions, tunables, correctness, risks, and open questions. Give invariants
stable IDs such as `I1`.

Describe current state only. Omit requirement numbers, change history,
before/after prose, file paths, and line numbers. Refer to searchable symbols.
Keep architecture in root docs, detail in sub-designs, and operations in the
user guide; link rather than repeat.

When folding a working design, remove temporary scaffolding and requirement
references, rewrite as current state, renumber the doc and TOC, update
`doc/doc_index.md`, and delete the draft.
