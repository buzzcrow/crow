// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

async function openKvPanel(page: any, storeId: string, groupId: string) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV' }).click();
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

test.describe('E2E-32 inspector activity tab', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('mutation appears in activity log and clear works', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r32', 'n32');
    await deployNodeServer(baseURL!, 'n32', 9932, 9942);
    await createStore(baseURL!, 320, ['n32']);
    await addGroup(baseURL!, 320, 3200, 32000, ['n32']);
    await waitForLeader(baseURL!, 320, 3200);

    try {
      await openKvPanel(page, '320', '3200');
      await putKey(page, 'activity-key', 'activity-val');

      // Select a node in the tree to make the inspector visible
      await page.getByRole('button', { name: 'Physical' }).click();

      // Try to find and click the node — rack may already be expanded
      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-n32' });
      // If rack is collapsed, expand it first
      const expandBtn = page.getByRole('treeitem').filter({ hasText: 'R-r32' }).locator('button[aria-label="Expand"]');
      if (await expandBtn.count() > 0) {
        await expandBtn.click();
      }
      await nodeItem.getByRole('button', { name: 'N-n32' }).click();

      // Open inspector activity tab
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });
      await inspector.getByRole('tab', { name: 'Activity' }).click();

      // Verify an entry appears (the KV Put should be logged)
      await expect(inspector.getByText(/KV Put/i)).toBeVisible({ timeout: 3_000 });

      // Click Clear log
      await inspector.getByRole('button', { name: /clear log/i }).evaluate((el: HTMLElement) => el.click());

      // Verify entries are removed
      await expect(inspector.getByText('No activity yet.')).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n32');
    }
  });
});
