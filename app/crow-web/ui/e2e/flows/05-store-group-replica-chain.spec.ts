// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.1s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, clusterInit, deployNodeServer, freePort, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-05 store group replica chain', () => {
  test('creates store and group through the UI against a real deployed server', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 5, 5);
    await deployNodeServer(baseURL!, 5, freePort(), freePort());

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      await clusterInit(baseURL!, [5]);
      await aside.getByRole('button', { name: 'Add Store' }).click();
      await expect(page.getByRole('dialog', { name: 'Add KV Store' })).toBeVisible();
      await page.getByLabel('KV Store ID (numeric)').fill('57');
      await page.getByLabel(/^5\b/).check();
      await page.getByRole('button', { name: /create kv store/i }).click();

      await expect(aside.getByText('S-57')).toBeVisible({ timeout: 3_000 });

      // Add the first group via the store row context menu.
      await aside.getByText('S-57').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add group/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
      await page.getByLabel('Group ID (numeric)').fill('570');
      await page.getByLabel('Starting Replica ID (numeric)').fill('5700');
      await page.getByLabel(/^5\b/).check();
      await page.getByRole('button', { name: /create group/i }).click();

      // Expand the freshly-created store row (created after tree mount, so it
      // is collapsed by default) to reveal its groups.
      const store57 = page.getByRole('treeitem').filter({ hasText: 'S-57' });
      const expandStore57 = store57.getByRole('button', { name: 'Expand' });
      if (await expandStore57.count()) await expandStore57.click();
      await expect(aside.getByText('G-570')).toBeVisible({ timeout: 3_000 });

      // Verify parent-child: S-57 is expanded and G-570 is visible in the tree
      const store57Item = page.getByRole('treeitem').filter({ hasText: 'S-57' });
      await expect(store57Item).toHaveAttribute('aria-expanded', 'true');
      await expect(aside.getByText('G-570')).toBeVisible({ timeout: 3_000 });

      // Add a second group via the store row context menu.
      await aside.getByText('S-57').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add group/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
      await page.getByLabel('Group ID (numeric)').fill('580');
      await page.getByLabel('Starting Replica ID (numeric)').fill('5800');
      await page.getByLabel(/^5\b/).check();
      await page.getByRole('button', { name: /create group/i }).click();

      await expect(aside.getByText('G-580')).toBeVisible({ timeout: 3_000 });

      const stores = await api.get('/api/stores');
      expect(stores.ok(), await stores.text()).toBeTruthy();
      expect(await stores.json()).toEqual(expect.arrayContaining([expect.objectContaining({ store_id: 57 })]));

      const groups = await api.get('/api/stores/57/groups');
      expect(groups.ok(), await groups.text()).toBeTruthy();
      expect(await groups.json()).toEqual(expect.arrayContaining([expect.objectContaining({ group_id: 570 }), expect.objectContaining({ group_id: 580 })]));
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 5);
    }
  });
});
