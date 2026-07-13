// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack } from '../fixtures/consoleSetup';

test.describe('E2E-03 add node', () => {
  test('creates a node through the rack context menu and verifies the real backend', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r3', name: 'Rack Three' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    const aside = page.locator('aside').first();
    await expect(aside.getByText('R-r3 (Rack Three)')).toBeVisible({ timeout: 15_000 });

    // Right-click the rack row: the context menu pre-selects the rack in the
    // Add Node dialog (defaultRackId), so no manual rack selection is needed.
    await aside.getByText('R-r3 (Rack Three)').click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();

    await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
    await page.getByLabel('Node ID').fill('n3');
    await page.getByLabel('Host').fill('127.0.0.1');
    await page.getByLabel('Enable CrowKV on this node').uncheck();
    await expect(page.getByRole('button', { name: /create node/i })).toBeEnabled();
    await page.getByRole('button', { name: /create node/i }).click();

    await expect(page.getByText(/Node "n3" created successfully/)).toBeVisible({ timeout: 15_000 });
    await expect(aside.getByText('N-n3', { exact: true })).toBeVisible({ timeout: 15_000 });

    const api = await apiContext(baseURL!);
    try {
      const response = await api.get('/api/nodes');
      expect(response.ok(), await response.text()).toBeTruthy();
      const nodes = await response.json();
      expect(nodes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 'n3', rack_id: 'r3', host: '127.0.0.1' }),
        ]),
      );
    } finally {
      await api.dispose();
    }
  });
});
