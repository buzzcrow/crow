// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { createRack, createNode, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-34 sidebar filter', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('filter narrows tree and clearing restores all items', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r34a', name: 'Alpha' });
    await createRack(baseURL!, { id: 'r34b', name: 'Beta' });
    await createRack(baseURL!, { id: 'r34c', name: 'Gamma' });
    await createNode(baseURL!, { id: 'n34a', rack_id: 'r34a' });
    await createNode(baseURL!, { id: 'n34b', rack_id: 'r34b' });
    await createNode(baseURL!, { id: 'n34c', rack_id: 'r34c' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const rackA = page.getByRole('treeitem').filter({ hasText: 'R-r34a' });
    const rackB = page.getByRole('treeitem').filter({ hasText: 'R-r34b' });
    const rackC = page.getByRole('treeitem').filter({ hasText: 'R-r34c' });

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
