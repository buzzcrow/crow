---
description: Implement one milestone step in CrowKV
---


# CrowKV Implementation Step Workflow

Use this when tackling a single milestone (`/implement-step P2 M0`, etc.). It keeps the canonical order: **read → audit → fix docs → write tests → implement → verify → close**. 

Before starting, know: milestone identifier, the referenced plan/design/test-design docs, and which requirements sections it satisfies.

---

## Step 1 — Re-read the contract // turbo

Read (in order) `requirement.md`, the milestone’s plan doc, linked design doc(s), then the matching test-design. Summarize the milestone to the user in ≤5 bullets (scope, acceptance criteria, invariants, cross-cutting rules).

---

## Step 2 — Audit code // turbo

Use `code_search` (preferred) to map modules listed under the plan’s “Module Breakdown”: note whether they exist, are stubbed, or finished, plus any `todo!()` / `unimplemented!()` and current tests. Report back as `Module | State | Notes` table.

---

## Step 3 — Reconcile docs

Follow CrowKV doc rules (`doc-structure.md`). For each gap from Step 2: update plan/design/test docs before code if reality differs. Escalate `requirement.md` edits to the user. After each doc edit, mention any downstream doc that also changed.

---

## Step 4 — Tests first

Translate the test-design rows into real `#[tokio::test]` cases (use existing harnesses). Tests should compile but fail because the feature isn’t implemented yet. Run the targeted suite once to show the failure mode (`cargo test -p <crate> <filter> -- --nocapture`).


---

## Step 5 — Implement + verify

Implement the minimum code to satisfy the tests, respecting plan §7 rules (async-only, no blocking/std mutexes, keep module layout). If a new ambiguity appears, stop and loop back to Step 3. Run:

```
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test -p <crate>
```

Add benches only if the plan demands them.

---

## Step 6 — Close out // turbo

Confirm acceptance criteria, note any freeze gates or downstream unlocks, suggest the next milestone, and stop until the user opts to continue.

---

### Notes

- Invoke with `/implement-step <milestone>`.
- Steps marked `// turbo` can run automatically; others require explicit confirmation.
- You may loop back to an earlier step if new information contradicts prior assumptions; state the loop clearly so the user knows which step you’re repeating.
