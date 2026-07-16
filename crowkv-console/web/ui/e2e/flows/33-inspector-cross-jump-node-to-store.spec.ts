// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-33 inspector cross-jump node to store', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('physical node with store shows cross-jump to logical store', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r33', 'n33');
    await deployNodeServer(baseURL!, 'n33', 9933, 9943);
    await createStore(baseURL!, 330, ['n33']);
    await addGroup(baseURL!, 330, 3300, 33000, ['n33']);
    await waitForLeader(baseURL!, 330, 3300);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // Select the physical node
      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-n33' });
      await expect(nodeItem).toBeVisible({ timeout: 3_000 });
      await nodeItem.getByRole('button', { name: 'N-n33' }).click();

      // Inspector should show Details tab with cross-jump button
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });

      // Verify cross-jump button exists and click it
      const crossJumpButton = inspector.getByRole('button', { name: /Show store 330 in cluster/i });
      await expect(crossJumpButton).toBeVisible({ timeout: 3_000 });
      await crossJumpButton.click();

      // View should switch to Logical
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 3_000 });

      // Store should be selected in the logical tree
      await expect(aside.getByText('S-330')).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n33');
    }
  });
});
