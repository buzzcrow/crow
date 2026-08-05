// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack, createNode, deployNodeServer, stopNodeServer, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-27 server lifecycle via context menu', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('ping, restart, and stop server via context menu', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r27', name: 'r27' });
    await createNode(baseURL!, { id: 'n27', rack_id: 'r27' });
    await deployNodeServer(baseURL!, 'n27', 9927, 9937);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-n27' });
      await expect(nodeItem).toBeVisible({ timeout: 3_000 });

      // Ping
      await nodeItem.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /ping/i }).click();

      // Restart
      await nodeItem.click({ button: 'right' });
      const restartResponse = page.waitForResponse((r: any) => r.url().includes('/server/restart'));
      await page.getByRole('menuitem', { name: /restart Crow Storage/i }).click();
      await restartResponse;

      // Stop
      await nodeItem.click({ button: 'right' });
      const stopResponse = page.waitForResponse((r: any) => r.url().includes('/server/stop'));
      await page.getByRole('menuitem', { name: /stop Crow Storage/i }).click();
      await stopResponse;

      // After stop, verify server is no longer running via API
      const api = await apiContext(baseURL!);
      try {
        const resp = await api.get('/api/nodes/n27');
        expect(resp.ok()).toBeTruthy();
        const node = await resp.json();
        const serverState = node.server?.state ?? node.server?.status ?? 'unknown';
        expect(serverState).not.toBe('running');
      } finally {
        await api.dispose();
      }
    } finally {
      await stopNodeServer(baseURL!, 'n27');
    }
  });
});
