// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.4s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-06 cross jump', () => {
  test('jumps from logical replica details to the hosting physical node', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 6, 6);
    await deployNodeServer(baseURL!, 6, freePort(), freePort());
    await createStore(baseURL!, 66, [6]);
    await addGroup(baseURL!, 66, 660, 6600, [6]);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await expect(aside.getByText('LR-6600')).toBeVisible({ timeout: 3_000 });
      await aside.getByText('LR-6600').click();

      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });

      // Single cross-jump button: logical Replica -> hosting physical Node.
      await inspector.getByRole('button', { name: /Show on node 6\b/ }).click();

      await expect(page.getByRole('heading', { name: 'Infrastructure' })).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText('N-6', { exact: true })).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 6);
    }
  });

  test('jumps from physical node details to the hosting logical store', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 62, 62);
    await deployNodeServer(baseURL!, 62, freePort(), freePort());
    await createStore(baseURL!, 67, [62]);
    await addGroup(baseURL!, 67, 670, 6700, [62]);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();

      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-62' });
      await expect(nodeItem).toBeVisible({ timeout: 3_000 });
      await nodeItem.getByRole('button', { name: 'N-62' }).click();

      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });

      // Cross-jump button: physical Node -> logical Store.
      await inspector.getByRole('button', { name: /Show store 67 in cluster/i }).click();

      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText('S-67', { exact: true }).first()).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 62);
    }
  });
});
