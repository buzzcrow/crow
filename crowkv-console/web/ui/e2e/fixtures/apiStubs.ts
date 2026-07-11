import { Page, Route } from '@playwright/test';
import { mockRacks, mockNodes, mockStores } from './mockData';

/**
 * Intercept all /api/* and /healthz traffic so the SPA renders without a
 * live crowkv-web backend. Unmatched routes return 200 + `{}` rather than
 * failing the page; we just want a fully-rendered shell.
 */
export async function stubBackend(page: Page) {
  const json = (route: Route, body: unknown, status = 200) =>
    route.fulfill({
      status,
      contentType: 'application/json',
      body: JSON.stringify(body),
    });

  // Playwright matches routes in REVERSE registration order — register the
  // catch-all FIRST so the more-specific handlers below take precedence.
  await page.route(/\/api\//, (route) => json(route, []));

  await page.route('**/healthz', (route) => json(route, { status: 'ok' }));
  await page.route(/\/api\/racks(\?|$)/, (route) => json(route, mockRacks));
  await page.route(/\/api\/nodes(\?|$)/, (route) => json(route, mockNodes));
  await page.route(/\/api\/stores(\?|$)/, (route) => json(route, mockStores));
}
