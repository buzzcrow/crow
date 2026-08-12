// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack } from '../fixtures/consoleSetup';

test.describe('E2E-03 add node', () => {
  test('creates a node through the rack context menu and verifies the real backend', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 3, name: 'Rack Three' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await expect(aside.getByText('R-3 (Rack Three)')).toBeVisible({ timeout: 3_000 });

    // Right-click the rack row: the context menu pre-selects the rack in the
    // Add Node dialog (defaultRackId), so no manual rack selection is needed.
    await aside.getByText('R-3 (Rack Three)').click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();

    await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
    await page.getByLabel('Node ID').fill('3');
    await page.getByLabel('Host').fill('127.0.0.1');
    await page.getByLabel('Enable Crow Storage on this node').uncheck();
    await expect(page.getByRole('button', { name: /create node/i })).toBeEnabled();
    await page.getByRole('button', { name: /create node/i }).click();

    await expect(aside.getByText('N-3', { exact: true })).toBeVisible({ timeout: 3_000 });

    const api = await apiContext(baseURL!);
    try {
      const response = await api.get('/api/nodes');
      expect(response.ok(), await response.text()).toBeTruthy();
      const nodes = await response.json();
      expect(nodes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 3, rack_id: 3, host: '127.0.0.1' }),
        ]),
      );
    } finally {
      await api.dispose();
    }
  });
});
