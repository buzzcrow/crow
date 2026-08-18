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
E2E is the most important test surface here — it covers the real
end-to-end path (browser → `crow-web` → `crow-kv-server` / `crow-diskdb`
→ group-0 sysdata), is complex, and is expensive to maintain. Treat it
accordingly.

## File Organization

Specs in `e2e/flows/` named `NN-<area>-<function>.spec.ts`. The two-digit
prefix is execution order (Playwright runs alphabetically, `workers: 1`):
cheap/foundational first, expensive/composite last. Decades group by area:

- `0x` shell/app · `1x` Physical · `2x` KV Cluster · `3x` KV data ·
  `4x` Inspector/canvas · `5x` Capacity/DiskDB · `9x` cross-function

- **One file per page function** — filename says what is tested, never an opaque number.
- **Cross-function flows go in `9x`** — full user journeys; keep these few (most expensive).
- **Extend, don't split** — add to the existing file whose area+function matches. New file only for a genuinely new page function (next free number in its decade).

## Cost Discipline

Cluster setup (`crow-kv-server` / `crow-diskdb` deploys) dominates runtime, not browser interaction.

- **Prefer fewer, longer tests** — one `test()` exercising several related behaviors on a shared cluster beats several short ones that each redeploy. Don't split purely for readability.
- **Share setup with `beforeAll`** — build rack/node/server/store/group once per `describe`; tear down in `afterAll`.
- **Order mutating tests last** — deletes/reconfigs run after read-only ones, or clean up locally.
- **`resetAll` sparingly** — only when a test needs an empty backend; not a per-test hammer.
- **Unique IDs/ports per file** — `freePort()` + per-file ID base; files never collide.

## Conventions

- **No swallowing errors** — log cleanup errors with `console.warn`, never silent.
- **Precise selectors** — `getByLabel`/`getByRole`/`getByTestId`/scoped locators. Avoid unscoped `page.getByText` and `.first()` on page-level locators.
- **Timeout discipline** — assertions ≤ 3 s; leader election ≤ 10 s. Never inflate to work around slowness. `expect.poll` must set `intervals: [100]` (default 2 s causes false slowness).
- **`data-testid` for disambiguation** — when two elements share a label (e.g. two "RPC Port" inputs), add `data-testid` to each and select via `getByTestId`. Never use positional `.first()`/`.last()` — they break silently on reorder.
- **Ignore toasts** — never assert on `getByRole('alert')` or wait for dismiss. If a toast blocks a click, use `locator.evaluate((el) => el.click())`.
- **Baseline timing** — every spec has `// Baseline: Xs (date)` after the license header. Runtime > 2x baseline → investigate. Update baseline only on deliberate change.

## Verification & Regression Discipline

- **Run what you change — before declaring done.** Any change to `app/crow-web/ui/src/**` or `e2e/**` must be verified by running the affected spec file(s) first. TypeScript compiling or unit tests passing is *not* proof for a UI fix — the E2E run is. Never claim a UI fix is done unverified.
- **Every UI bug fix gets a regression assertion.** Add it to an *existing* test in the matching area file (not a new test/file). The assertion must fail pre-fix and pass post-fix; if you can't write one, the fix isn't verifiable end-to-end — get a narrower repro first.
- **Label/text changes: update the spec in the same change.** Grep E2E specs for the old string and fix every match. A label change that breaks a selector is a regression you can catch at write time.
- **When E2E is genuinely blocked** (backend feature not implemented, env unavailable): state the blocker, still add the regression assertion, mark the run deferred with the reason. Never silently skip.

## Discover Before Asserting (avoid iterate-and-fix loops)

Most E2E iteration loops come from asserting on assumptions
about the UI or API that turn out to be wrong. Follow these
steps **in order** before writing any assertion on a UI element
you haven't asserted on before in this spec file:

### 1. Trace the render path (read, don't guess)

Trace the full path from backend to DOM for the element you
want to assert on. Read the actual source files — don't infer
from behavior.

- **Backend → API JSON**: read the handler in
  `app/crow-web/src/` — what field names, what enum serialization
  (`#[serde(rename_all = "snake_case")]` → `"up"`, not `"healthy"`)?
- **API JSON → UI state**: read the hook in
  `app/crow-web/ui/src/data/` — how is the JSON mapped to state?
- **UI state → component prop**: read `Sidebar.tsx` (or the
  relevant panel) — what prop carries the value, what transform
  (`toUiHealth`) is applied?
- **Component prop → rendered DOM**: read the leaf component
  (`Badge.tsx`, `Tree.tsx`) — **does it render text, an icon, or
  both? In what mode?**

This takes 2-3 minutes. Iterating on a wrong selector takes
5-10 minutes per round and can loop 3+ times.

### 2. Probe the API first (see the actual JSON)

Before writing a DOM assertion on dynamic state (health, PID,
server presence), make a debug `api.get` call and `console.log`
the raw JSON. This catches:
- Wrong field names (`health` vs `state` vs `status`)
- Wrong enum values (`"up"` vs `"healthy"` vs `"running"`)
- Missing fields (`server: null` vs `server: undefined` vs absent)
- Stale cache vs live state mismatch

```ts
const api = await apiContext(baseURL!);
const r = await api.get('/api/racks?recursive=3');
console.log('DEBUG racks:', JSON.stringify(await r.json(), null, 2));
await api.dispose();
```

Remove the debug call once the assertion passes.

### 3. Use the correct selector for the render mode

Project-specific rendering facts (verify by reading the component
if unsure):

- **`HealthBadge` in compact mode** (Sidebar tree items): renders
  **icon only, no text**. The status string is in the `title`
  attribute. Assert via `getByTitle('Healthy')` /
  `getByTitle('Failed')`, **not** `hasText`.
- **`HealthBadge` in non-compact mode**: renders the status text.
  `hasText` works.
- **`RoleBadge` in compact mode**: renders a single-letter label
  (`L`, `F`, `R`). Assert via `getByTitle('Leader')` for the full
  name.
- **Tree items** (`TreeNodeComponent`): wrap label + badges in a
  `treeitem` role. Scope with
  `aside.getByRole('treeitem').filter({ hasText: 'KV-501' })`
  then drill into badges.

### 4. Timing: poll, don't sleep

After lifecycle operations (deploy, restart, stop), the backend
state is eventually-consistent:
- **Spawned processes** take variable time to bind ports (100ms–3s).
- **`refresh_node_cache`** probes the mgmt URL — if the process
  isn't listening yet, the cache is marked `Down` and only a retry
  flips it to `Up`.

Never use fixed `page.waitForTimeout` after lifecycle ops. Use:
- `expect.poll(async () => { ... }, { timeout: 10_000, intervals: [100] })`
  for API state (PID presence, health field).
- `expect(locator).toBeVisible({ timeout: 10_000 })` for DOM state
  (Playwright auto-polls).

If the backend itself needs retry (e.g. `refresh_node_cache` after
restart), the **backend handler** should spawn a retry loop, not
the test. The test should just poll the observable result.

### Run commands

Prerequisite: build the spawned binaries first.

```
pixi run cargo build -p crow-kv-server -p crow-diskdb
pixi run bash -c 'export CROW_KV_SERVER_BINARY=$(pwd)/target/debug/crow-kv-server \
  && cd app/crow-web/ui \
  && npx playwright test --config=e2e/realBackend.config.ts e2e/flows/NN-<area>-<fn>.spec.ts'
```

Full suite: `pixi run test-console-ui` (builds binaries, runs vitest, then all Playwright specs).

Always paste the full test output (pass/fail lines + timing) in your response — never just "it passed."
