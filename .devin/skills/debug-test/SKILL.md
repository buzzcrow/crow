---
name: debug-test
description: Debug a failing test — step-by-step verification, no workarounds
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Debug Test Failure Flow

Companion skills: `/coding`, `/review`.

## Principles

- **No workarounds** — never increase timeouts, ignore errors, or weaken assertions to make a test pass.
- **Step-by-step verification** — find the first step where reality diverges from expectation.
- **Root cause only** — fix the upstream cause, not the symptom.

## Step 0 — Environment Check

Check for proxy env vars, lingering processes, stale test-logs, and stale build artifacts before investigating code. If the failure disappears after cleanup → environmental, not a code bug.

## Step 1 — Reproduce and Isolate

Run the single failing test, not the whole suite. Passes alone, fails in-suite → port conflict or lingering process. Fails alone → proceed to Step 2.

## Step 2 — Decompose into Steps

Do not jump to a fix — first identify exactly where the test diverges from expectation.

1. List every step the test performs (setup, action, assertion).
2. For each step, verify the expected state via logs, data files, API checks, or temporary instrumentation.
3. Find the first gap and classify: design gap (code never written to provide expected behavior), ambiguity gap (timing-dependent behavior), or environmental gap.
4. If the gap isn't obvious from logs, measure per-step time — a step at 2x+ its share of the spec's `// Baseline: Xs` is a regression signal.

## Step 3 — Inspect Logs and On-Disk Data

Check Rust logs (`RUST_BACKTRACE=full`), C++ logs (`log_level="debug"`), WAL segments, and crow-tree files for `WARN`/`ERROR`, `panic`, or unexpected state.

## Step 3b — Crash Analysis

If a process crashed or disappeared without a log line, **always** get the crash report and analyze the call stack before attempting any fix. Never skip to workarounds.

## Step 4 — Add Temporary Instrumentation

If existing logs don't reveal the gap, add targeted logs at decision points (`tracing::debug!` / `CT_LOG_DEBUG` / `console.log`), rebuild, rerun. Remove after fixing.

## Step 5 — Fix and Verify

1. Minimal upstream root-cause fix.
2. Rerun failing test alone → must pass.
3. Rerun full suite for affected area → must pass.
4. Quality gate: `cargo fmt --check`, `cargo clippy -- -D warnings`, `clang-format --dry-run --Werror` on changed `.cpp`/`.h`.
5. Add regression test if real bug (not environmental).

## Anti-Patterns (never do)

- **Increasing timeouts** to make a slow test pass — investigate why it's slow first.
- **Ignoring errors** — log and surface every error.
- **Weakening assertions** — fix the selector or the code, not the assertion.
- **Downstream workarounds** — fix the upstream root cause.
- **Waiting on toasts** — never assert on `getByRole('alert')`. If a toast blocks a click, use `locator.evaluate((el) => el.click())`. See `/coding` E2E rules.
- **Ignoring baseline timing** — if a test exceeds 2x its `// Baseline: Xs`, investigate the regression before accepting the run.
- **Blocking on long-running test output** — use a short timeout (10–30s); if output shows repeat errors (same line 3+ times, no progress), investigate the log file directly — the test is stuck, not progressing.
