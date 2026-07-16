// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.4s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

/**
 * E2E-23 · Embedding isolation (Req §4, design §8).
 *
 * The standalone mount reads the embedding contract from the URL query
 * string (see `main.tsx`). This exercises the three properties an
 * embedding host relies on:
 *   - `apiPrefix` re-roots every data-plane request (`/proxy/api/...`);
 *   - `readonly` hides all mutating controls;
 *   - `modules` opt-out removes the KV and Swagger surfaces.
 */
test.describe('E2E-23 embedding isolation', () => {
  test('honors apiPrefix, readonly, and module opt-out', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r23', 'n23');
    await deployNodeServer(baseURL!, 'n23', 9955, 9965);
    await createStore(baseURL!, 233, ['n23']);
    await addGroup(baseURL!, 233, 2330, 23300, ['n23']);

    // Reverse-proxy emulation: the SPA issues /proxy/api/* which we rewrite
    // back onto the real /api/* surface served by crowkv-web.
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
      await stopNodeServer(baseURL!, 'n23');
    }
  });
});
