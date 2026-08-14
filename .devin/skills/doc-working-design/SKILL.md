---
name: doc-working-design
description: How to write doc/working/design-<topic>.md (implementation design draft)
triggers:
  - user
  - model
---

<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design Doc Guide (working draft)

How to write `doc/working/design-<topic>.md` — the implementation design
draft produced during Step 2 of the `/implement-requirement` skill. This
is where the real solution lives: detailed design, change scope, complexity,
and test case design. Folded into the formal design doc
(`doc/design/{kv,tree,console}/design-*.md`) and deleted after merge.

Companion to the [`doc-backlog`](doc-backlog.md) guide. The backlog doc
states the problem, high-level approach, dependencies, and acceptance
criteria; this draft expands the solution into implementation detail and
designs the test cases. Do not repeat the problem or dependencies here —
link back to the backlog doc instead.

## Structure

- **Header** — license comment, then `# <Title> (R**)`.
- **Intro** — one paragraph: what this draft covers, a link to the backlog
  doc (`doc/backlog/R**-<component>-<topic>.md`), a pointer to the root
  design doc (`doc/design/<area>/design-crow-<area>.md` §X), and what is
  already landed (prior `R**` + code paths). State: "Architecture decisions
  and rationale are in the root design; this doc does not repeat them."
- **Solution** (numbered sections, `## 1`, `## 2`, ...) — the real design,
  one section per component (new RPC, new engine, integration point). Each
  section:
  - **Why** (`### N.1`) — contrast with existing APIs/paths; why a new
    mechanism is required. The one place the draft argues *why*; the rest
    is *how*.
  - **How** (`### N.2`, `### N.3`, ...) — proto additions, struct
    definitions, handler logic, client methods. Fenced code blocks for
    proto/Rust signatures; lettered steps (`a.`, `b.`) for behavior under
    each signature. This is the implementation detail the backlog doc
    defers.
  - **Edge cases** — inline bullet list at the end of each subsection:
    failure modes, fallbacks, crash safety.
- **Scope** — bullet list, one line per file: `<path> — <change>`. New and
  modified files; group by crate if the change spans crates. The reviewer's
  map of the diff.
- **Complexity** — `High` / `Medium` / `Low` + 2–4 sentences: what is
  genuinely hard, what is new vs reused from aioss, the main implementation
  challenges. Do not restate the solution.
- **Test Design** — designed from the backlog doc's Acceptance criteria.
  Two layers:
  - **Unit tests (UT)** — pure logic, no external deps. Each test: setup →
    action → assertion, naming the invariant guarded. Covers edge cases
    (CRC failure, GC gap, fallback, crash-mid-operation, empty/corrupted
    input). Group by feature; scenario-heavy features (e.g. ownership
    transfer) list each variant as a named bullet.
  - **End-to-end tests (E2E)** — real `KvCluster` harness or in-process
    cluster, multi-component. Each test is an end-to-end narrative mapped
    from a backlog use scenario: operator/system trigger → system behavior
    → expected outcome. Proves the invariant across component boundaries
    (e.g. "B's bitmap matches A's records, not A's stale in-memory state").
- **Module Structure** — fenced file tree, new and modified files with
  one-line annotations (what each file contains).
- **Config Extensions** — new/renamed config fields with defaults and
  `validate()` changes, if any.
- **Server Wiring** — how new modules plug into `main.rs` / `sync.rs` /
  startup sequence. Numbered steps matching the startup flow.
- **Open Questions** (last section, only if needed) — issues that need
  discussion with a human, or decisions that cannot be made autonomously.
  Each item: the question or decision needed, the alternatives considered
  and their trade-offs, and why it cannot be resolved automatically. Do not
  guess or invent a decision — leave it open until reviewed.

## Writing rules

- **Implementation detail, not architecture** — the root design doc holds
  architecture and rationale. If you find yourself justifying a design
  decision, it belongs in the root design doc, not here.
- **Link to the backlog doc** — do not repeat the problem, dependencies, or
  acceptance criteria; link to `doc/backlog/R**-<component>-<topic>.md`.
- **R-numbers allowed** — the draft references `R**` (e.g. "from R72",
  "R70's `ZoneValueExt`") because it is temporary. Removed when folded into
  the formal design doc (Core Rule 9).
- **Code-grounded** — reference actual file paths, function names, and line
  numbers from already-landed code. The draft is the bridge between the
  backlog's *what* and the code's *how*.
- **Fenced blocks for code** — proto/Rust signatures use fenced blocks;
  behavior steps use lettered prose, not code.
- **Test design is concrete** — each test bullet names setup, action, and
  assertion. "Test recovery" is not acceptable; "write snapshot at slot 10,
  Put BusyBlockKey at slots 11–15, recover, verify bits 11–15 set" is.
- **Folding** — when folding into the formal design doc: drop the intro,
  drop all `R**` references, renumber sections, match the formal doc's
  current-state prose (Core Rule 9).
