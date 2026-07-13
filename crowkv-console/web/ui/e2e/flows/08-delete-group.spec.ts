// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-08 delete group', () => {
  test('deletes a group through the UI and verifies the real backend', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r8', 'n8');
    await deployNodeServer(baseURL!, 'n8', 9918, 9928);
    await createStore(baseURL!, 88, 880, 8800, ['n8']);
    await addGroup(baseURL!, 88, 880, 8800, ['n8']);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.locator('aside').first();
      await expect(aside.getByText('G-880')).toBeVisible({ timeout: 15_000 });

      await aside.getByText('G-880').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete group/i }).click();
      await expect(page.getByRole('dialog', { name: 'Delete Group' })).toBeVisible();
      await page.getByRole('button', { name: /delete group/i }).click();

      await expect(page.getByText(/Group "880" deleted successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(aside.getByText('G-880')).toHaveCount(0, { timeout: 15_000 });

      const response = await api.get('/api/stores/88/groups');
      expect(response.ok(), await response.text()).toBeTruthy();
      expect(await response.json()).not.toEqual(expect.arrayContaining([expect.objectContaining({ group_id: 880 })]));
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 'n8');
    }
  });
});
