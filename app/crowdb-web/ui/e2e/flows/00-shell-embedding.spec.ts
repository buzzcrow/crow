// Copyright 2026-present Gian <crow.db@outlook.com>
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
  resetAll,
  seedRackAndNode,
  stopNodeServer,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

/**
 * Shell-level surfaces that need no shared cluster: backend-unreachable
 * alert and the embedding contract read from the URL query string.
 *
 * Each test needs its own page state (aborted routes / proxied routes /
 * a different mount URL), so they stay separate `test()`s.
 */
test.describe('shell · embedding', () => {
  test('shows an alert when backend API requests fail', async ({ page }) => {
    await step('shell: route abort', () => page.route('**/api/**', route => route.abort('failed')));

    await step('shell: goto', () => page.goto('/'));

    // Scope to the banner alert — a toast (also role=alert) may appear
    // concurrently with "Failed to load server list:" text.
    await expect(page.getByRole('alert').filter({ hasText: 'Backend unreachable' })).toBeVisible({ timeout: 3_000 });
  });

  test('embedding honors apiPrefix, readonly, and module opt-out', async ({ page, baseURL }) => {
    await step('shell: resetAll', () => resetAll(baseURL!));
    await step('shell: seed rack/node', () => seedRackAndNode(baseURL!, 23, 23));
    await step('shell: deploy server', () => deployNodeServer(baseURL!, 23, freePort(), freePort()));
    await step('shell: create store', () => createStore(baseURL!, 233, [23]));
    await step('shell: add group', () => addGroup(baseURL!, 233, 2330, 23300, [23]));

    // Reverse-proxy emulation: the SPA issues /proxy/api/* which we rewrite
    // back onto the real /api/* surface served by crowdb-web.
    await step('shell: route proxy', () => page.route('**/proxy/api/**', (route) => {
      const u = new URL(route.request().url());
      u.pathname = u.pathname.replace('/proxy/api', '/api');
      route.continue({ url: u.toString() });
    }));

    const seen: string[] = [];
    page.on('request', (req) => seen.push(req.url()));

    try {
      const apiPrefix = encodeURIComponent('/proxy/api');
      const proxyRequest = page.waitForRequest('**/proxy/api/**', { timeout: 3_000 });
      await step('shell: goto embed', () => page.goto(`/?domain=KV&readonly=1&disableModules=${encodeURIComponent('kv')}&apiPrefix=${apiPrefix}`));

      // apiPrefix: the SPA re-roots every data-plane call under /proxy/api.
      await step('shell: wait proxy request', () => proxyRequest);
      expect(seen.some((u) => u.includes('/proxy/api/'))).toBeTruthy();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      // Data still loads (rewritten back onto /api by the route above).
      await expect(aside.getByText('S-233', { exact: true })).toBeVisible({ timeout: 3_000 });

      // readonly: no Add control in the sidebar.
      await expect(aside.getByRole('button', { name: 'Add Store' })).toHaveCount(0);

      // modules: selecting the group exposes Details/Activity but no KV tab.
      const group233 = page.getByRole('treeitem').filter({ hasText: 'G-2330' });
      const expandStore = page.getByRole('treeitem').filter({ hasText: 'S-233' }).getByRole('button', { name: 'Expand' });
      if (await expandStore.count()) await expandStore.click();
      await group233.getByRole('button', { name: 'G-2330' }).click();
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector.getByRole('tab', { name: 'Details' })).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByRole('tab', { name: 'KV' })).toHaveCount(0);
    } finally {
      await step('shell: stop server', () => stopNodeServer(baseURL!, 23));
    }
  });

  test('domain toggle switches between Cluster, KV, and Chunk', async ({ page }) => {
    await step('shell: goto', () => page.goto('/'));

    // Domain toggle buttons are visible.
    await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('domain-kv')).toBeVisible();
    await expect(page.getByTestId('domain-chunk')).toBeVisible();

    // Default domain is Cluster.
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');

    // Switch to KV.
    await page.getByTestId('domain-kv').click();
    await expect(page.getByTestId('domain-kv')).toHaveAttribute('aria-pressed', 'true');

    // Switch to Chunk.
    await page.getByTestId('domain-chunk').click();
    await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');

    // Switch back to Cluster.
    await page.getByTestId('domain-cluster').click();
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');
  });
});
