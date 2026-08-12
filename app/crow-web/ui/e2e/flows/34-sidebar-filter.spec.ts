// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.3s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { createRack, createNode, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-34 sidebar filter', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('filter narrows tree and clearing restores all items', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 341, name: 'Alpha' });
    await createRack(baseURL!, { id: 342, name: 'Beta' });
    await createRack(baseURL!, { id: 343, name: 'Gamma' });
    await createNode(baseURL!, { id: 341, rack_id: 341 });
    await createNode(baseURL!, { id: 342, rack_id: 342 });
    await createNode(baseURL!, { id: 343, rack_id: 343 });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const rackA = page.getByRole('treeitem').filter({ hasText: 'R-341' });
    const rackB = page.getByRole('treeitem').filter({ hasText: 'R-342' });
    const rackC = page.getByRole('treeitem').filter({ hasText: 'R-343' });

    // All visible initially
    await expect(rackA).toBeVisible({ timeout: 3_000 });
    await expect(rackB).toBeVisible();
    await expect(rackC).toBeVisible();

    // Type filter "alpha"
    await aside.getByPlaceholder('Filter...').fill('alpha');
    await expect(rackA).toBeVisible({ timeout: 3_000 });
    await expect(rackB).toHaveCount(0);
    await expect(rackC).toHaveCount(0);

    // Clear filter
    await aside.getByPlaceholder('Filter...').fill('');
    await expect(rackA).toBeVisible({ timeout: 3_000 });
    await expect(rackB).toBeVisible();
    await expect(rackC).toBeVisible();
  });
});
