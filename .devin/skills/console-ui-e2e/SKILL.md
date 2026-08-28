---
name: console-ui-e2e
description: CROW console UI E2E (Playwright) — invoke when writing or modifying E2E tests, or when fixing a UI bug or changing user-visible UI code under app/crow-web/ui
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - E2E / Playwright Tests

Applies to `crow-web/ui/e2e`. Companion: `/coding` (general conventions).
E2E is the most important test surface — it covers the real end-to-end path
(browser → `crow-web` → `crow-kv-server` / `crow-diskdb` → group-0 sysdata).

## File Organization

- Specs in `e2e/flows/` named `NN-<area>-<function>.spec.ts`. Two-digit prefix = execution order (alphabetical, `workers: 1`): cheap/foundational first, expensive last. Decades: `0x` shell · `1x` Physical · `2x` KV Cluster · `3x` KV data · `4x` Inspector/canvas · `5x` Capacity/DiskDB · `9x` cross-function.
- One file per page function. Cross-function flows go in `9x` (keep these few). Extend existing files; new file only for a genuinely new page function.

## Cost Discipline

Cluster setup (server deploys) dominates runtime, not browser interaction.

- Prefer fewer, longer tests on a shared cluster over several short ones that each redeploy.
- Share setup with `beforeAll`; tear down in `afterAll`. Order mutating tests last.
- `resetAll` sparingly — only when a test needs an empty backend.
- Unique IDs/ports per file (`freePort()` + per-file ID base).

## Conventions

- **No swallowing errors** — log cleanup errors with `console.warn`, never silent.
- **Precise selectors** — `getByLabel`/`getByRole`/`getByTestId`/scoped locators. Avoid unscoped `page.getByText` and `.first()` on page-level locators.
- **Timeout discipline** — assertions ≤ 3 s; leader election ≤ 10 s. Never inflate to work around slowness. `expect.poll` must set `intervals: [100]`.
- **`data-testid` for disambiguation** — when two elements share a label, add `data-testid` and select via `getByTestId`. Never use positional `.first()`/`.last()`.
- **Ignore toasts** — never assert on `getByRole('alert')`. If a toast blocks a click, use `locator.evaluate((el) => el.click())`.
- **Baseline timing** — every spec has `// Baseline: Xs (date)`. Runtime > 2x baseline → investigate.

## Verification & Regression

- **Run what you change** — any change to `app/crow-web/ui/src/**` or `e2e/**` must be verified by running the affected spec. TypeScript compiling is not proof for a UI fix.
- **Every UI bug fix gets a regression assertion** — add to an existing test in the matching area file. Must fail pre-fix, pass post-fix.
- **Label/text changes: update specs in the same change** — grep E2E specs for the old string and fix every match.
- **When E2E is genuinely blocked** — state the blocker, still add the regression assertion, mark the run deferred with the reason. Never silently skip.

## Discover Before Asserting

Most E2E iteration loops come from asserting on assumptions about the UI/API that turn out wrong. Before writing an assertion on an unfamiliar element:

- **Trace the render path** — read the actual source: backend handler → API JSON → UI hook → component prop → rendered DOM. Don't infer from behavior.
- **Probe the API first** — make a debug `api.get` call and `console.log` the raw JSON before writing DOM assertions on dynamic state. Catches wrong field names, wrong enum values, stale cache.
- **Poll, don't sleep** — after lifecycle ops (deploy, restart, stop), use `expect.poll` with `intervals: [100]` for API state and `toBeVisible` for DOM state. Never use fixed `page.waitForTimeout`.

## Run Commands

Prerequisite: build spawned binaries first.

```
pixi run cargo build -p crow-kv-server -p crow-diskdb
pixi run bash -c 'export CROW_KV_SERVER_BINARY=$(pwd)/target/debug/crow-kv-server \
  && cd app/crow-web/ui \
  && npx playwright test --config=e2e/realBackend.config.ts e2e/flows/NN-<area>-<fn>.spec.ts'
```

Full suite: `pixi run test-console-ui`. Always paste the full test output in your response.
