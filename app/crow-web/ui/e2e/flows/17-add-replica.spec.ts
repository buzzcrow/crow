// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.8s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-17 add replica', () => {
  test('adds a replica to an existing group through the UI', async ({ page, baseURL }) => {
    // Setup: two racks/nodes with deployed servers.
    await seedRackAndNode(baseURL!, 171, 171);
    await seedRackAndNode(baseURL!, 172, 172);
    await Promise.all([
      deployNodeServer(baseURL!, 171, freePort(), freePort()),
      deployNodeServer(baseURL!, 172, freePort(), freePort()),
    ]);

    // Seed a store with an initial group on n17a.
    await createStore(baseURL!, 177, [171]);
    await addGroup(baseURL!, 177, 1770, 17700, [171]);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'KV Cluster' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await expect(aside.getByText('G-1770')).toBeVisible({ timeout: 3_000 });

      // Right-click selects + targets the group (without toggling its expand,
      // so the existing replica row stays visible).
      await aside.getByText('G-1770').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();

      await expect(page.getByRole('dialog', { name: 'Add Replica' })).toBeVisible();
      await page.getByLabel('Node', { exact: true }).selectOption('172');
      await page.getByRole('button', { name: /add replica/i }).click();

      // Verify the new replica appears in the logical tree.
      await expect(aside.getByText('LR-17701')).toBeVisible({ timeout: 3_000 });

      // Verify backend: two replicas in the group.
      const response = await api.get('/api/stores/177/groups/1770/replicas');
      expect(response.ok(), await response.text()).toBeTruthy();
      const replicas = await response.json();
      expect(replicas).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ replica_id: 17700, node_id: 171 }),
          expect.objectContaining({ replica_id: 17701, node_id: 172 }),
        ]),
      );
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 171);
      await stopNodeServer(baseURL!, 172);
    }
  });
});
