// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.5s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import {
  addGroup,
  createNode,
  createRack,
  createStore,
  deployNodeServer,
  freePort,
  seedRackAndNode,
  stopNodeServer,
} from '../fixtures/consoleSetup';

/**
 * Shell-level surfaces that need no shared cluster: backend-unreachable
 * alert, the embedded Swagger panel (Req §3.5), and the embedding contract
 * read from the URL query string (Req §4, design §8).
 *
 * Each test needs its own page state (aborted routes / proxied routes /
 * a different mount URL), so they stay separate `test()`s.
 */
test.describe('shell · embedding + swagger', () => {
  test('shows an alert when backend API requests fail', async ({ page }) => {
    await page.route('**/api/**', route => route.abort('failed'));

    await page.goto('/');

    // Scope to the banner alert — a toast (also role=alert) may appear
    // concurrently with "Failed to load server list:" text.
    await expect(page.getByRole('alert').filter({ hasText: 'Backend unreachable' })).toBeVisible({ timeout: 3_000 });
  });

  test('swagger panel renders inline and re-targets the node selection', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 22, name: 'Rack TwentyTwo' });
    await createNode(baseURL!, { id: 221, rack_id: 22 });
    await createNode(baseURL!, { id: 222, rack_id: 22 });
    await Promise.all([
      deployNodeServer(baseURL!, 221, freePort(), freePort()),
      deployNodeServer(baseURL!, 222, freePort(), freePort()),
    ]);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // Target n22a, then open the API panel.
      await expect(aside.getByText('N-221', { exact: true })).toBeVisible({ timeout: 3_000 });
      await aside.getByRole('button', { name: 'N-221' }).click();
      await page.getByRole('button', { name: 'API' }).click();

      const frameA = page.locator('iframe[title="Swagger UI for 221"]');
      await expect(frameA).toBeVisible({ timeout: 3_000 });
      expect(decodeURIComponent((await frameA.getAttribute('src')) ?? '')).toContain('/nodes/221/openapi.json');
      // Shell stays mounted (no full-page navigation).
      await expect(page.locator('header').getByText('Crow Storage Console')).toBeVisible();

      // Switch the selection to n22b -> the iframe re-targets inline.
      await aside.getByRole('button', { name: 'N-222' }).click();
      const frameB = page.locator('iframe[title="Swagger UI for 222"]');
      await expect(frameB).toBeVisible({ timeout: 3_000 });
      expect(decodeURIComponent((await frameB.getAttribute('src')) ?? '')).toContain('/nodes/222/openapi.json');
      await expect(page.locator('header').getByText('Crow Storage Console')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 221);
      await stopNodeServer(baseURL!, 222);
    }
  });

  test('embedding honors apiPrefix, readonly, and module opt-out', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 23, 23);
    await deployNodeServer(baseURL!, 23, freePort(), freePort());
    await createStore(baseURL!, 233, [23]);
    await addGroup(baseURL!, 233, 2330, 23300, [23]);

    // Reverse-proxy emulation: the SPA issues /proxy/api/* which we rewrite
    // back onto the real /api/* surface served by crow-web.
    await page.route('**/proxy/api/**', (route) => {
      const u = new URL(route.request().url());
      u.pathname = u.pathname.replace('/proxy/api', '/api');
      route.continue({ url: u.toString() });
    });

    const seen: string[] = [];
    page.on('request', (req) => seen.push(req.url()));

    try {
      const apiPrefix = encodeURIComponent('/proxy/api');
      const proxyRequest = page.waitForRequest('**/proxy/api/**', { timeout: 3_000 });
      await page.goto(`/?view=Logical&readonly=1&disableModules=${encodeURIComponent('kv,swagger')}&apiPrefix=${apiPrefix}`);

      // apiPrefix: the SPA re-roots every data-plane call under /proxy/api.
      await proxyRequest;
      expect(seen.some((u) => u.includes('/proxy/api/'))).toBeTruthy();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      // Data still loads (rewritten back onto /api by the route above).
      await expect(aside.getByText('S-233', { exact: true })).toBeVisible({ timeout: 3_000 });

      // readonly: no Add control in the sidebar.
      await expect(aside.getByRole('button', { name: 'Add Store' })).toHaveCount(0);

      // modules: Swagger (API) button is absent from the header.
      await expect(page.getByRole('button', { name: 'API' })).toHaveCount(0);

      // modules: selecting the group exposes Details/Activity but no KV tab.
      const group233 = page.getByRole('treeitem').filter({ hasText: 'G-2330' });
      const expandStore = page.getByRole('treeitem').filter({ hasText: 'S-233' }).getByRole('button', { name: 'Expand' });
      if (await expandStore.count()) await expandStore.click();
      await group233.getByRole('button', { name: 'G-2330' }).click();
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector.getByRole('tab', { name: 'Details' })).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByRole('tab', { name: 'KV' })).toHaveCount(0);
    } finally {
      await stopNodeServer(baseURL!, 23);
    }
  });
});
