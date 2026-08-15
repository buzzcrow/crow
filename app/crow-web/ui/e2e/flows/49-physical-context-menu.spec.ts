// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 8s (2026-08-15)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack, createNode, deployNodeServer, stopNodeServer, freePort } from '../fixtures/consoleSetup';

test.describe('E2E-49 physical view context menu', () => {
  test('node without server: shows Deploy Crow Storage + Delete Node, no restart/stop', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 490, name: 'Rack 490' });
    await createNode(baseURL!, { id: 490, rack_id: 490 });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: 'R-490' }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText('N-490', { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click the node in the tree.
    await aside.getByText('N-490', { exact: true }).click({ button: 'right' });

    // Should have Deploy Crow Storage and Delete Node.
    await expect(page.getByRole('menuitem', { name: /deploy crow storage/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /delete node/i })).toBeVisible();

    // Should NOT have restart/stop (no server deployed).
    await expect(page.getByRole('menuitem', { name: /restart crow storage/i })).toHaveCount(0);
    await expect(page.getByRole('menuitem', { name: /stop crow storage/i })).toHaveCount(0);

    await page.keyboard.press('Escape');
  });

  test('node with server: shows Deploy DiskDB + Ping + Delete Node, no restart/stop on node', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 491, name: 'Rack 491' });
    await createNode(baseURL!, { id: 491, rack_id: 491 });
    await deployNodeServer(baseURL!, 491, freePort(), freePort());

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    // Wait for the server to appear in the tree (polling takes a moment).
    await expect(aside.getByText('KV-491')).toBeVisible({ timeout: 10_000 });

    // Right-click the node (not the server).
    await aside.getByText('N-491', { exact: true }).click({ button: 'right' });

    // Server is deployed, so no "Deploy Crow Storage" but "Deploy DiskDB" appears.
    await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /ping/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /delete node/i })).toBeVisible();

    // Should NOT have restart/stop on the node — those are on the service.
    await expect(page.getByRole('menuitem', { name: /restart crow storage/i })).toHaveCount(0);
    await expect(page.getByRole('menuitem', { name: /stop crow storage/i })).toHaveCount(0);

    await page.keyboard.press('Escape');
    await stopNodeServer(baseURL!, 491);
  });

  test('server node: shows Restart, Stop, Delete Crow Storage', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 492, name: 'Rack 492' });
    await createNode(baseURL!, { id: 492, rack_id: 492 });
    await deployNodeServer(baseURL!, 492, freePort(), freePort());

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    // Wait for the server node to appear.
    await expect(aside.getByText('KV-492')).toBeVisible({ timeout: 10_000 });

    // Right-click the server (KV) node.
    await aside.getByText('KV-492', { exact: true }).click({ button: 'right' });

    await expect(page.getByRole('menuitem', { name: /restart crow storage/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /stop crow storage/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /delete crow storage/i })).toBeVisible();

    // Should NOT have "Deploy" or "Delete Node" on the service.
    await expect(page.getByRole('menuitem', { name: /deploy/i })).toHaveCount(0);
    await expect(page.getByRole('menuitem', { name: /delete node/i })).toHaveCount(0);

    await page.keyboard.press('Escape');
    await stopNodeServer(baseURL!, 492);
  });

  test('delete node with deployed server cascades service shutdown', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 493, name: 'Rack 493' });
    await createNode(baseURL!, { id: 493, rack_id: 493 });
    await deployNodeServer(baseURL!, 493, freePort(), freePort());

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    // Wait for server to appear so the cascade knows to remove it.
    await expect(aside.getByText('KV-493')).toBeVisible({ timeout: 10_000 });

    // Right-click node → Delete Node.
    await aside.getByText('N-493', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /delete node/i }).click();

    // Confirm delete dialog.
    const deleteDialog = page.getByRole('dialog', { name: /delete node/i });
    await expect(deleteDialog).toBeVisible();
    const confirmBtn = deleteDialog.getByRole('button', { name: /delete node/i });
    await confirmBtn.evaluate((el) => (el as HTMLElement).click());

    // Node should disappear from the tree.
    await expect(aside.getByText('N-493', { exact: true })).toHaveCount(0, { timeout: 10_000 });

    // Verify via API: node is gone.
    const api = await apiContext(baseURL!);
    try {
      const r = await api.get('/api/nodes/493');
      expect(r.status()).toBe(404);
    } finally {
      await api.dispose();
    }
  });

  test('delete crow storage service removes server but keeps node', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 494, name: 'Rack 494' });
    await createNode(baseURL!, { id: 494, rack_id: 494 });
    await deployNodeServer(baseURL!, 494, freePort(), freePort());

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await expect(aside.getByText('KV-494')).toBeVisible({ timeout: 10_000 });

    // Right-click server → Delete Crow Storage.
    await aside.getByText('KV-494', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /delete crow storage/i }).click();

    // Confirm.
    const deleteDialog = page.getByRole('dialog', { name: /delete crow storage/i });
    await expect(deleteDialog).toBeVisible();
    const confirmBtn = deleteDialog.getByRole('button', { name: /delete crow storage/i });
    await confirmBtn.evaluate((el) => (el as HTMLElement).click());

    // Server disappears from tree, node remains.
    await expect(aside.getByText('KV-494', { exact: true })).toHaveCount(0, { timeout: 10_000 });
    await expect(aside.getByText('N-494', { exact: true })).toBeVisible();

    // Verify via API: node still exists, server is gone.
    const api = await apiContext(baseURL!);
    try {
      const nodeResp = await api.get('/api/nodes/494');
      expect(nodeResp.ok()).toBeTruthy();
      const serverResp = await api.get('/api/nodes/494/server');
      expect(serverResp.status()).toBe(404);
    } finally {
      await api.dispose();
    }
  });
});
