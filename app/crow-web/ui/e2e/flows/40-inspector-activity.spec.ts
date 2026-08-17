// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 5.5s (2026-08-16)

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
  waitForLeader,
} from '../fixtures/consoleSetup';

async function openKvPanel(page: any, storeId: string, groupId: string) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV', exact: true }).click();
  await page.getByLabel('Store').selectOption(storeId);
  await page.getByLabel('Group').selectOption(groupId);
}

async function putKey(page: any, key: string, value: string) {
  await page.getByLabel('Put key').fill(key);
  await page.getByLabel('Put value').fill(value);
  const responsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
  await page.getByRole('button', { name: /^Put$/ }).click();
  const response = await responsePromise;
  expect(response.ok(), await response.text()).toBeTruthy();
}

test.describe('inspector · activity log', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('records mutations and async operations, and clear empties the log', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    // --- KV mutation appears in the activity log and clear works ---
    await seedRackAndNode(baseURL!, 32, 32);
    await deployNodeServer(baseURL!, 32, freePort(), freePort());
    await createStore(baseURL!, 320, [32]);
    await addGroup(baseURL!, 320, 3200, 32000, [32]);
    await waitForLeader(baseURL!, 320, 3200);

    try {
      await openKvPanel(page, '320', '3200');
      await putKey(page, 'activity-key', 'activity-val');

      // Select a node in the tree to make the inspector visible
      await page.getByRole('button', { name: 'Physical' }).click();

      // Try to find and click the node — rack may already be expanded
      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-32' });
      // If rack is collapsed, expand it first
      const expandBtn = page.getByRole('treeitem').filter({ hasText: 'R-32' }).locator('button[aria-label="Expand"]');
      if (await expandBtn.count() > 0) {
        await expandBtn.click();
      }
      await nodeItem.getByRole('button', { name: 'N-32' }).click();

      // Open inspector activity tab
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });
      await inspector.getByRole('tab', { name: 'Activity' }).click();

      // Verify an entry appears (the KV Put should be logged)
      await expect(inspector.getByText(/KV Put/i)).toBeVisible({ timeout: 3_000 });

      // Click Clear log — wait for the button to be enabled, then click.
      // force:true bypasses actionability checks (toast overlays, etc.)
      // while still dispatching a real pointer event through React's
      // synthetic event system.
      const clearBtn = inspector.getByRole('button', { name: /clear log/i });
      await expect(clearBtn).toBeEnabled({ timeout: 3_000 });
      await clearBtn.click({ force: true });

      // Verify entries are removed
      await expect(inspector.getByText('No activity yet.')).toBeVisible({ timeout: 5_000 });
    } finally {
      await stopNodeServer(baseURL!, 32);
    }

    // --- async op feedback: ping / restart / stop toasts + activity entries ---
    await resetAll(baseURL!);
    await createRack(baseURL!, { id: 47, name: 'r47' });
    await createNode(baseURL!, { id: 47, rack_id: 47 });
    await deployNodeServer(baseURL!, 47, freePort(), freePort());

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-47' });
      await expect(nodeItem).toBeVisible({ timeout: 3_000 });

      // Ping — on the node context menu.
      await nodeItem.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /ping/i }).click();

      const pingToast = page.getByRole('alert').filter({ hasText: /ping/i });
      await expect(pingToast).toBeVisible({ timeout: 3_000 });

      // Restart and Stop are on the server (KV) context menu.
      const serverItem = page.getByRole('treeitem').filter({ hasText: 'KV-47' });
      await expect(serverItem).toBeVisible({ timeout: 5_000 });

      // Restart — should show a success toast
      await serverItem.click({ button: 'right' });
      const restartResponse = page.waitForResponse((r: any) => r.url().includes('/server/restart'));
      await page.getByRole('menuitem', { name: /restart Crow Storage/i }).click();
      await restartResponse;

      const restartToast = page.getByRole('alert').filter({ hasText: /restart/i });
      await expect(restartToast).toBeVisible({ timeout: 3_000 });

      // Stop — should show a success toast
      await serverItem.click({ button: 'right' });
      const stopResponse = page.waitForResponse((r: any) => r.url().includes('/server/stop'));
      await page.getByRole('menuitem', { name: /stop Crow Storage/i }).click();
      await stopResponse;

      const stopToast = page.getByRole('alert').filter({ hasText: /stop/i });
      await expect(stopToast).toBeVisible({ timeout: 3_000 });

      // Verify all three operations appear in the activity log
      await nodeItem.getByRole('button', { name: 'N-47' }).click();
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });
      await inspector.getByRole('tab', { name: 'Activity' }).click();

      await expect(inspector.getByText(/ping node/i)).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText(/restart Crow Storage/i)).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText(/stop Crow Storage/i)).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 47);
    }
  });
});
