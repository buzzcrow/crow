<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: CROW E2E (Playwright) test rules
---

# CROW - E2E / Playwright Tests

Applies to `crow-web/ui/e2e`. Companion: `/coding` (general conventions).

- **No ignoring errors** — never swallow API failures silently; log cleanup errors with `console.warn`.
- **Precise selectors** — use `getByLabel`, `getByRole`, `getByTestId`, or scoped locators. Avoid unscoped `page.getByText` and `.first()` on page-level locators.
- **Timeout discipline** — assertion timeouts ≤ 3 s; leader election may use up to 10 s. No inflating timeouts to work around slowness. `expect.poll` must set `intervals: [100]` for fast polling (default 2 s interval causes false slowness).
- **`data-testid`** — add to dynamic elements that could match in multiple places; select via `getByTestId`.
- **Ignore toasts** — never assert on `getByRole('alert')` or wait for toast dismiss. If a toast blocks a click, use `locator.evaluate((el) => el.click())` to bypass.
- **Baseline timing** — every E2E spec file has a `// Baseline: Xs (date)` comment after the license header. If a test's runtime exceeds 2x its baseline, investigate for regression. Update the baseline only when a deliberate change justifies it.
