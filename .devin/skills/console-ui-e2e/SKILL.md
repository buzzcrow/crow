---
name: console-ui-e2e
description: console-ui-e2e
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - E2E / Playwright Tests

Applies to `crow-web/ui/e2e`. Companion: `/coding` (general conventions).

## File Organization

Specs live in `e2e/flows/` and are named `NN-<area>-<function>.spec.ts` —
a two-digit ordering prefix, then the page area, then the function under
test. Playwright runs files alphabetically with `workers: 1`, so the
prefix is the execution order: cheap/foundational first, expensive/
composite last. Decades group by area, with gaps for future inserts.

- `0x` — shell / app-level (no cluster needed)
- `1x` — Physical view
- `2x` — KV Cluster view (logical topology)
- `3x` — KV data operations
- `4x` — Inspector + canvas
- `5x` — Capacity / DiskDB
- `9x` — cross-function end-to-end flows

Rules:

- **One file per page function** — a file covers one page area's one
  function (e.g. rack/node CRUD, server lifecycle, KV data ops). The
  filename must say what is tested; never an opaque sequence number alone.
- **Cross-function flows go in `9x`** — a flow test drives a full user
  journey across areas (rack → node → server → store → group → KV ops).
  Keep these few; they are the most expensive.
- **Adding a test** — extend an existing file whose area+function matches
  rather than creating a new file. Create a new file only for a genuinely
  new page function, and pick the next free number in that area's decade.

## Cost Discipline

Cluster setup (`crow-kv-server` / `crow-diskdb` deploys) dominates
runtime, not browser interaction. Therefore:

- **Prefer fewer, longer tests** — one `test()` that exercises several
  related behaviors against a shared cluster beats several short `test()`s
  that each redeploy. Do not split a test purely for readability.
- **Share setup with `beforeAll`** — build the rack/node/server/store/
  group once per `test.describe` and reuse it; tear down in `afterAll`.
- **Order mutating tests last** — tests that delete or reconfigure shared
  state run after the read-only ones in the same file, or clean up locally
  so later assertions still hold.
- **`resetAll` sparingly** — only where a test genuinely needs an empty
  backend (fresh-registry, comparative-suite). It is not a per-test hammer.
- **Unique IDs and ports per file** — use `freePort()` and a per-file ID
  base so files never collide.

## Conventions

- **No ignoring errors** — never swallow API failures silently; log cleanup errors with `console.warn`.
- **Precise selectors** — use `getByLabel`, `getByRole`, `getByTestId`, or scoped locators. Avoid unscoped `page.getByText` and `.first()` on page-level locators.
- **Timeout discipline** — assertion timeouts ≤ 3 s; leader election may use up to 10 s. No inflating timeouts to work around slowness. `expect.poll` must set `intervals: [100]` for fast polling (default 2 s interval causes false slowness).
- **`data-testid`** — add to dynamic elements that could match in multiple places; select via `getByTestId`.
- **Ignore toasts** — never assert on `getByRole('alert')` or wait for toast dismiss. If a toast blocks a click, use `locator.evaluate((el) => el.click())` to bypass.
- **Baseline timing** — every E2E spec file has a `// Baseline: Xs (date)` comment after the license header. If a test's runtime exceeds 2x its baseline, investigate for regression. Update the baseline only when a deliberate change justifies it.
