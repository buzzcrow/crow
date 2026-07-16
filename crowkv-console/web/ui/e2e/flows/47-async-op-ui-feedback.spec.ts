// Copyright 2026-present buzzcrow <buzzcrow@126.com>

import { test, expect } from '../fixtures/realBackend';
import { createRack, createNode, deployNodeServer, stopNodeServer, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-47 async operation UI feedback', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('ping and stop show success toast, activity log records both', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r47', name: 'r47' });
    await createNode(baseURL!, { id: 'n47', rack_id: 'r47' });
    await deployNodeServer(baseURL!, 'n47', 9947, 9957);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-n47' });
      await expect(nodeItem).toBeVisible({ timeout: 3_000 });

      // Ping — should show a success toast
      await nodeItem.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /ping/i }).click();

      const pingToast = page.getByRole('alert').filter({ hasText: /ping/i });
      await expect(pingToast).toBeVisible({ timeout: 3_000 });

      // Restart — should show a success toast
      await nodeItem.click({ button: 'right' });
      const restartResponse = page.waitForResponse((r: any) => r.url().includes('/server/restart'));
      await page.getByRole('menuitem', { name: /restart crowkv/i }).click();
      await restartResponse;

      const restartToast = page.getByRole('alert').filter({ hasText: /restart/i });
      await expect(restartToast).toBeVisible({ timeout: 3_000 });

      // Stop — should show a success toast
      await nodeItem.click({ button: 'right' });
      const stopResponse = page.waitForResponse((r: any) => r.url().includes('/server/stop'));
      await page.getByRole('menuitem', { name: /stop crowkv/i }).click();
      await stopResponse;

      const stopToast = page.getByRole('alert').filter({ hasText: /stop/i });
      await expect(stopToast).toBeVisible({ timeout: 3_000 });

      // Verify all three operations appear in the activity log
      await nodeItem.getByRole('button', { name: 'N-n47' }).click();
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });
      await inspector.getByRole('tab', { name: 'Activity' }).click();

      await expect(inspector.getByText(/ping node/i)).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText(/restart crowkv/i)).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText(/stop crowkv/i)).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n47');
    }
  });
});

