// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 3.7s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createNode, createRack, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

/**
 * E2E-25 · Destructive confirms for store / node / rack (Req §3.2, §6).
 *
 * Replica/group deletes are covered by 07/08; this closes the physical and
 * logical *root* deletes. Each delete is confirm-gated: we cancel once to
 * prove the guard, then confirm and verify removal in the DOM and via the
 * backend.
 */
test.describe('E2E-25 root deletes', () => {
  test('confirm-gates store, node, and rack deletion', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 25, 25);
    await deployNodeServer(baseURL!, 25, freePort(), freePort());
    await createStore(baseURL!, 255, [25]);
    // A serverless node (clean to delete) and an empty rack (clean to delete).
    await createNode(baseURL!, { id: 274, rack_id: 25 });
    await createRack(baseURL!, { id: 255, name: 'Rack TwentyFive Empty' });

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // ── Store (logical) ──────────────────────────────────────────
      await page.getByRole('button', { name: 'KV Cluster' }).click();
      await expect(aside.getByText('S-255', { exact: true })).toBeVisible({ timeout: 3_000 });

      // Cancel first.
      await aside.getByText('S-255', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete store/i }).click();
      await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: 'Cancel' }).click();
      await expect(aside.getByText('S-255', { exact: true })).toBeVisible();

      // Confirm.
      await aside.getByText('S-255', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete store/i }).click();
      await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: /delete store/i }).click();
      await expect(aside.getByText('S-255', { exact: true })).toHaveCount(0, { timeout: 3_000 });

      const storesResp = await api.get('/api/stores');
      expect(storesResp.ok(), await storesResp.text()).toBeTruthy();
      expect(await storesResp.json()).not.toEqual(
        expect.arrayContaining([expect.objectContaining({ store_id: 255 })]),
      );

      // ── Node (physical, serverless n25x) ─────────────────────────
      await page.getByRole('button', { name: 'Physical' }).click();
      const node25x = page.getByRole('treeitem').filter({ hasText: 'N-274' });
      await expect(node25x).toBeVisible({ timeout: 3_000 });

      await node25x.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete node/i }).click();
      await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: 'Cancel' }).click();
      await expect(node25x).toBeVisible();

      await node25x.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete node/i }).click();
      await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: /delete node/i }).click();
      await expect(page.getByRole('treeitem').filter({ hasText: 'N-274' })).toHaveCount(0, { timeout: 3_000 });

      const nodeResp = await api.get('/api/nodes/274');
      expect(nodeResp.status()).toBe(404);

      // ── Rack (physical, empty r25e) ──────────────────────────────
      const rack25e = page.getByRole('treeitem').filter({ hasText: 'R-255' });
      await expect(rack25e).toBeVisible({ timeout: 3_000 });
      await rack25e.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete rack/i }).click();
      await page.getByRole('dialog', { name: 'Delete Rack' }).getByRole('button', { name: /delete rack/i }).click();
      await expect(page.getByRole('treeitem').filter({ hasText: 'R-255' })).toHaveCount(0, { timeout: 3_000 });

      const racksResp = await api.get('/api/racks');
      expect(racksResp.ok(), await racksResp.text()).toBeTruthy();
      expect(await racksResp.json()).not.toEqual(
        expect.arrayContaining([expect.objectContaining({ id: 255 })]),
      );
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 25);
    }
  });
});
