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

- **No workarounds** — never increase timeouts, ignore errors, or weaken assertions to make a test pass. A slow test is a suspicious test; investigate where the time goes before accepting it.
- **Step-by-step verification** — break the test into discrete steps, each with an expected outcome and a verification method (log, data file, or temporary instrumentation). Find the first step where reality diverges from expectation.
- **Root cause only** — fix the upstream cause, not the symptom. Downstream workarounds hide bugs and rot.

## Step 0 — Environment Check

Many failures are environmental, not code bugs. Check first:

- **Proxy env vars**: `env | grep -i proxy` — reqwest/curl route localhost through proxy. Fix: `no_proxy='*' http_proxy='' https_proxy='' all_proxy=''`
- **Lingering processes**: `ps aux | grep crowkv-server | grep -v grep` → `pkill -9 -f 'crowkv-server.*test-logs'`
- **Stale test-logs**: `rm -rf test-logs/*` before rerunning web/server suites
- **Stale build artifacts**: `pixi run cargo clean` if compilation fails

If the failure disappears after cleanup → environmental, not a code bug.

## Step 1 — Reproduce and Isolate

Run the single failing test, not the whole suite:

```bash
# Rust
no_proxy='*' pixi run cargo test -p <crate> --test <file> <test_name> -- --nocapture
# C++ — via ctest filter
pixi run ctest -R '<pattern>' --output-on-failure
# C++ — via gtest_filter (more precise, runs inside the test binary)
./build/crow_tree_tests --gtest_filter='<TestSuite.TestName>'
# Playwright E2E
npx playwright test --config=e2e/realBackend.config.ts e2e/flows/<file>.spec.ts
```

- Passes alone, fails in-suite → port conflict or lingering process (Step 0)
- Fails alone → proceed to Step 2

## Step 2 — Decompose into Steps and Verify Each

This is the core technique. Do not jump to a fix — first identify exactly where the test diverges from expectation.

1. **List every step** the test performs (setup, action, assertion).
2. **For each step, write down**: what is the expected state? How do you verify it?
   - **Logs**: server logs, `RUST_LOG=debug`, Playwright trace (`npx playwright show-trace`).
   - **Data files**: WAL segments, crow-tree files, runtime-data dirs.
   - **API checks**: `curl` or `fetch` the relevant endpoint mid-test.
   - **Temporary logs**: add `tracing::debug!` / `console.log` at decision points, rebuild, rerun. Remove after fixing.
3. **Find the gap**: the first step where the actual state differs from expected. Classify the gap:
   - **Design gap** — the test expects behavior the code was never written to provide. Fix the code or the test, whichever is wrong.
   - **Ambiguity gap** — an intermediate process has undefined or timing-dependent behavior (e.g., polling races, missing setup calls). Pin it down with explicit sequencing or additional setup.
   - **Environmental gap** — external factors (proxy, stale state, port conflicts). Fix the environment, not the code.
4. **Measure per-step time** if the gap isn't obvious from logs (long-running tests): bracket each step with wall-clock deltas — Playwright `Date.now()` + `console.log` (`--reporter=line`), Rust `Instant::now()` + `eprintln!` (`--nocapture`), C++ `steady_clock` to stderr. The step whose elapsed approaches the assertion timeout is the prime suspect — drill into its upstream call. A step at 2x+ its share of the spec's `// Baseline: Xs` is a regression signal even if the test still passes.

## Step 3 — Inspect Logs and On-Disk Data

- **Rust logs**: `log/crowkv-server-*.log`, `runtime-data/N-<node_id>/log/`, `test-logs/`. Look for `WARN`/`ERROR`, startup sequence, `panic` (use `RUST_BACKTRACE=full`).
- **C++ logs**: set `log_dir` + `log_level="debug"` in `ct_options`/`Options` to enable spdlog output.
- **WAL segments**: `hexdump -C` or WAL replay tool — expected slots present? record types correct?
- **crow-tree files**: `ct_debug_dump` C API or integration test helpers — file sizes non-zero? unexpected stale files?

## Step 4 — Add Temporary Instrumentation

If existing logs don't reveal the gap, add targeted logs at decision points:

1. Find the failing function from backtrace or last log line.
2. Rust: add `tracing::debug!` with structured fields. C++: add `CT_LOG_DEBUG(...)`. Playwright: add `console.log` in test or inspect DOM snapshot via trace.
3. Rebuild, rerun, compare actual vs expected at each step.
4. Remove temporary logs once fixed (or keep as `trace!`/`CT_LOG_TRACE` if genuinely useful).

## Step 5 — Fix and Verify

1. Minimal upstream root-cause fix.
2. Rerun failing test alone → must pass.
3. Rerun full suite for affected area → must pass.
4. Quality gate: `pixi run cargo fmt --all -- --check`, `pixi run cargo clippy --all-targets -- -D warnings`, `pixi exec clang-format --dry-run --Werror` on changed `.cpp`/`.h`.
5. Add regression test if real bug (not environmental).

## Anti-Patterns (never do)

- **Increasing timeouts** to make a slow test pass — investigate why it's slow first.
- **Ignoring errors** (`.catch(() => ...)`, `unwrap_or_default`, swallowing status codes) — log and surface every error.
- **Weakening assertions** (looser matchers, `.first()` to avoid strict mode, removing checks) — fix the selector or the code, not the assertion.
- **Downstream workarounds** (patching the test to avoid the failing path) — fix the upstream root cause.
- **Waiting on toasts** — never assert on `getByRole('alert')` or wait for toast dismiss. If a toast blocks a click, use `locator.evaluate((el) => el.click())`. See `/coding` E2E rules.
- **Ignoring baseline timing** — every E2E spec has a `// Baseline: Xs (date)` comment. If a test exceeds 2x its baseline, treat it as a regression signal: investigate where the extra time went before accepting the run. Update the baseline only when a deliberate change justifies it.
- **Blocking on long-running test output** — never wait indefinitely with `get_output` for a slow/hung test. Use a short timeout (10–30s); if the output shows **repeat errors** (the same `WARN`/`ERROR`/exception line recurring 3+ times with no progress), stop waiting and investigate the log file directly — the test is stuck in a retry loop or deadlock, not making progress. Read the overflow log file with `grep`/`read` to find the first error and the repeating pattern, then trace the root cause. A test that is passing will show advancing `✓`/`passed` markers; a test with repeat errors and no advancing markers is stuck.
