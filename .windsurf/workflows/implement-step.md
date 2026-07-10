---
description: Implement one milestone step in CrowKV with checkpoints for review between phases
---

# CrowKV Implementation Step Workflow

Use this workflow when starting work on a single milestone (e.g. `P1 M1`, `P2 M0`, `P3 M2`). It enforces a strict order: **audit → reconcile docs → tests-first → implement → verify → checkpoint**, with an explicit pause for the user to review and refine between every phase.

**Inputs you must know before starting:**

- Phase + milestone identifier (e.g. `P2 M0` = WAL Phase 2, Milestone 0).
- Relevant plan doc (e.g. `doc/plan-wal.md`).
- Relevant design doc(s) (e.g. `doc/design-wal.md`, `doc/design-async-io.md`).
- Relevant test-design doc (e.g. `doc/test-design-wal.md`).

**Hard rule:** never skip a checkpoint. Stop and wait for the user after every step that ends with `**[CHECKPOINT]**`.

---

## Step 1 — Re-read the contract

// turbo

Read in this exact order, top to bottom:

1. `requirement.md` — relevant section(s) only (the milestone's plan doc lists which `Satisfies:` upstream sections).
2. The milestone's plan doc — locate the exact milestone, read its bullets and acceptance criteria.
3. The matching design doc(s) — read the section the plan references.
4. The matching test-design doc — read the invariants and unit-test rows for this milestone.

Summarize back to the user in ≤ 10 bullets:
- What the milestone asks for.
- Acceptance criteria (verbatim).
- Test invariants this milestone introduces.
- Any cross-cutting decisions that apply (lease in P1, async I/O in P2, etc. — check `plan.md` §7 + §8).

**[CHECKPOINT]** — wait for user to confirm the summary is correct, or to adjust scope.

---

## Step 2 — Audit existing code

// turbo

Use `code_search` (preferred) or `grep_search` / `find_by_name` to locate:

1. Which modules listed in the plan doc's "Module Breakdown" already exist.
2. Which already have stubs vs full implementations vs nothing.
3. Whether earlier milestones left any `todo!()` / `unimplemented!()` / placeholder that this milestone is supposed to remove.
4. Existing tests that touch this area.

Report back as a small table: `Module | State | Notes`.

**[CHECKPOINT]** — user decides whether to continue, or to back-fill an earlier milestone first.

---

## Step 3 — Doc reconciliation (CRITICAL)

This step is the place where surprises surface. CrowKV doc rules (in your user rules + `.windsurf/workflows/doc-structure.md`) require: **fix upstream docs before code if reality diverges from plan**.

For each gap discovered in Step 2:

- If the gap is "code missing" → continue to Step 4.
- If the gap is "code does X, plan says Y" → ask the user which is correct. If plan was wrong, update the plan doc (and any downstream test-design / test-plan that referenced it). If code was wrong, this is a bug; flag it for Step 5.
- If the gap is "design doesn't say what to do here" → propose a 1–3-line addition to the relevant design doc. Update upstream first.
- If the gap requires changing `requirement.md` → STOP. Bring it to the user explicitly. `requirement.md` changes are rare and require explicit approval.

After every doc edit, also check whether sibling docs need an update (e.g. updating `plan-wal.md` may require a tweak to `test-plan-wal.md`'s milestone gates).

**[CHECKPOINT]** — user reviews any doc updates before code is written. Iterate until the user confirms docs are accurate.

---

## Step 4 — Tests first

Per the user's testing-discipline rule and CrowKV's test-pairing rule (`plan.md` §5):

1. Translate the test-design rows for this milestone into actual `#[tokio::test]` / unit-test stubs in the appropriate `tests/` or inline `#[cfg(test)]` module.
2. Use the harness contracts already established (`TestNode`, `TestRouter`, `TestTimer`, `SimDisk` once it exists).
3. Tests should compile but **fail** at this step (target functionality doesn't exist yet) — that proves the test exercises the right code path.
4. Use `#[tokio::test(flavor = "current_thread", start_paused = true)]` per `plan.md` §7 rule 5.

Run tests once to confirm the expected failure mode (compile-pass, run-fail with a clear "not yet implemented" or assertion error):

```bash
cargo test -p <crate> <test_filter> -- --nocapture
```

**[CHECKPOINT]** — user reviews the test list and shape. Easy place to add a missing case before implementing.

---

## Step 5 — Implement minimally

Write the minimum code to make Step 4's tests pass. Constraints (in priority order):

1. **Async-everywhere** (`plan.md` §7): no blocking calls, no `std::sync::Mutex` on async paths, no `std::thread::sleep`. Disk I/O via `AsyncFile` (`design-async-io.md`).
2. **No emojis or ad-hoc comments** unless explicitly requested (per user rules).
3. **Module structure follows the plan doc's Module Breakdown** — do not invent module names, do not split or merge without updating the plan doc first (which means looping back to Step 3).
4. **No helper scripts** unless the plan explicitly calls for them.
5. **Edits, not rewrites** — prefer `edit` / `multi_edit` over `write_to_file` for existing files.
6. Avoid `unwrap()` in non-test code; bubble errors with `?` and a typed `Error` enum.

If during implementation a previously-unseen design question appears:
- Stop coding.
- Add a `**TODO-CONFIRM:**` to the relevant design or plan doc.
- Bring it to the user (back to Step 3 mini-loop).

**[CHECKPOINT]** — user reviews the diff before tests run.

---

## Step 6 — Verify

// turbo

Run, in order:

```bash
cargo build --all-targets
cargo clippy --all-targets -- -D warnings
cargo test -p <crate>
```

If any of these fail, fix and re-run. Do not silence clippy with `#[allow(...)]` without explaining why in the commit message and in a `// SAFETY:` / `// NOTE:` comment.

For milestones with benchmarks (e.g. `P2 M2` fsync throughput):

```bash
cargo bench --bench <bench_name> -- --save-baseline <milestone>
```

Record the p50/p99 numbers in the milestone's acceptance section if the plan doc asks for them.

**[CHECKPOINT]** — user reviews test output, clippy output, and bench numbers (if any) before declaring the milestone complete.

---

## Step 7 — Close out

// turbo

1. Confirm the milestone's plan-doc acceptance criteria are met (re-read them from Step 1).
2. If a freeze gate is reached (e.g. `P1 M3` freezes consensus message types per `plan-consensus.md` §5), explicitly state which freeze applies and that the gate is satisfied.
3. If this milestone unlocks a downstream phase (e.g. `P1 M4` engine trait freeze unlocks `P3 M1`), note it.
4. Suggest the next milestone to tackle. Do not start it.

**[CHECKPOINT]** — user decides whether to continue with the next milestone or pause for refactor / review.

---

## Notes on use

- Invoke this workflow with `/implement-step` followed by the milestone identifier, e.g. `/implement-step P2 M0`.
- Each `**[CHECKPOINT]**` is a hard pause. The agent must wait for the user to type "continue" or equivalent before proceeding.
- Steps marked `// turbo` are auto-runnable (read-only inspection, building, testing). Steps that mutate code or docs are NOT `// turbo`.
- The agent may loop back from any step to an earlier one if a discovery invalidates the prior summary — but must announce the loop-back explicitly so the user knows where they are.
