// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.6s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-07 delete replica', () => {
  test('deletes a replica through the UI and verifies the real backend', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 7, 7);
    await deployNodeServer(baseURL!, 7, 9937, 9947);
    await createStore(baseURL!, 77, [7]);
    await addGroup(baseURL!, 77, 770, 7700, [7]);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await expect(aside.getByText('LR-7700')).toBeVisible({ timeout: 3_000 });

      await aside.getByText('LR-7700').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete replica/i }).click();
      await expect(page.getByRole('dialog', { name: 'Delete Replica' })).toBeVisible();
      await page.getByRole('button', { name: /delete replica/i }).click();

      await expect(aside.getByText('LR-7700')).toHaveCount(0, { timeout: 3_000 });

      const response = await api.get('/api/stores/77/groups/770/replicas');
      if (response.status() === 404) {
        expect(await response.text()).toContain('group 770 in store 77 not found');
      } else {
        expect(response.ok(), await response.text()).toBeTruthy();
        expect(await response.json()).not.toEqual(expect.arrayContaining([expect.objectContaining({ replica_id: 7700 })]));
      }
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 7);
    }
  });
});
