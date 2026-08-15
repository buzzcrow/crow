// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.6s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-08 delete group', () => {
  test('deletes a group through the UI and verifies the real backend', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 8, 8);
    await deployNodeServer(baseURL!, 8, freePort(), freePort());
    await createStore(baseURL!, 88, [8]);
    await addGroup(baseURL!, 88, 880, 8800, [8]);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'KV Cluster' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await expect(aside.getByText('G-880')).toBeVisible({ timeout: 3_000 });

      await aside.getByText('G-880').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete group/i }).click();
      await expect(page.getByRole('dialog', { name: 'Delete Group' })).toBeVisible();
      await page.getByRole('button', { name: /delete group/i }).click();

      await expect(aside.getByText('G-880')).toHaveCount(0, { timeout: 3_000 });

      const response = await api.get('/api/stores/88/groups');
      expect(response.ok(), await response.text()).toBeTruthy();
      expect(await response.json()).not.toEqual(expect.arrayContaining([expect.objectContaining({ group_id: 880 })]));
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 8);
    }
  });
});
