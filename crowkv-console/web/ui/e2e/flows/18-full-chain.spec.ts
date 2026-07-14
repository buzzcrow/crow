// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { apiContext, DEFAULT_SERVER_BINARY, deployNodeServer, stopNodeServer, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-18 full chain', () => {
  test('creates rack, node, server, store, group, and replica entirely through the UI', async ({ page, baseURL }) => {
    const api = await apiContext(baseURL!);
    try {
      await resetAll(baseURL!);
      await page.goto('/');
      await expect(page.getByRole('button', { name: 'Physical' })).toBeVisible({ timeout: 15_000 });
      await page.getByRole('button', { name: 'Physical' }).click();
      await expect(page.getByRole('heading', { name: 'Infrastructure' })).toBeVisible({ timeout: 15_000 });
      const aside = page.locator('aside').first();

      // 1. Add rack r18.
      await page.locator('aside').getByRole('button', { name: 'Add Rack' }).click();
      await expect(page.getByRole('dialog', { name: 'Add Rack' })).toBeVisible();
      await page.getByLabel('Rack ID').fill('r18');
      await page.getByLabel('Name (optional)').fill('Rack Eighteen');
      await page.getByRole('button', { name: /create rack/i }).click();
      await expect(page.getByText(/Rack "r18" created successfully/)).toBeVisible({ timeout: 15_000 });

      // 2. Add node n18a to r18 via rack context menu.
      await page.getByRole('treeitem').filter({ hasText: 'Rack Eighteen' }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add node/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
      await page.getByLabel('Rack', { exact: true }).selectOption('r18');
      await page.getByLabel('Node ID').fill('n18a');
      await page.getByLabel('Host').fill('127.0.0.1');
      await page.getByLabel('Enable CrowKV on this node').uncheck();
      await page.getByRole('button', { name: /create node/i }).click();
      await expect(page.getByText(/Node "n18a" created successfully/)).toBeVisible({ timeout: 15_000 });

      // 3. Add node n18b to r18 via rack context menu.
      await page.getByRole('treeitem').filter({ hasText: 'Rack Eighteen' }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add node/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
      await page.getByLabel('Rack', { exact: true }).selectOption('r18');
      await page.getByLabel('Node ID').fill('n18b');
      await page.getByLabel('Host').fill('127.0.0.1');
      await page.getByLabel('Enable CrowKV on this node').uncheck();
      await page.getByRole('button', { name: /create node/i }).click();
      await expect(page.getByText(/Node "n18b" created successfully/)).toBeVisible({ timeout: 15_000 });

      // Ensure rack r18 is expanded so its nodes are visible. The tree may
      // have mounted with racks from earlier specs (shared test-mode backend),
      // leaving the freshly-added r18 collapsed.
      const rack18 = page.getByRole('treeitem', { name: /R-r18 \(Rack Eighteen\)/ });
      const expandRack18 = rack18.getByRole('button', { name: 'Expand' });
      if (await expandRack18.count()) await expandRack18.click();

      // 4. Deploy CrowKV Server on n18a.
      await page.getByRole('treeitem').filter({ hasText: 'N-n18a' }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /deploy crowkv/i }).click();
      await expect(page.getByRole('dialog', { name: /deploy crowkv on n18a/i })).toBeVisible();
      await page.getByLabel('Management Port').fill('9933');
      await page.getByLabel('gRPC Port').fill('9943');
      await page.getByLabel('Binary Path (optional)').fill(DEFAULT_SERVER_BINARY);
      await page.getByRole('button', { name: 'Deploy' }).click();
      await expect(page.getByText(/CrowKV deployed on n18a/)).toBeVisible({ timeout: 30_000 });

      // 5. Deploy CrowKV Server on n18b.
      await page.getByRole('treeitem').filter({ hasText: 'N-n18b' }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /deploy crowkv/i }).click();
      await expect(page.getByRole('dialog', { name: /deploy crowkv on n18b/i })).toBeVisible();
      await page.getByLabel('Management Port').fill('9934');
      await page.getByLabel('gRPC Port').fill('9944');
      await page.getByLabel('Binary Path (optional)').fill(DEFAULT_SERVER_BINARY);
      await page.getByRole('button', { name: 'Deploy' }).click();
      await expect(page.getByText(/CrowKV deployed on n18b/)).toBeVisible({ timeout: 30_000 });

      // Switch to Cluster (Logical) view.
      await page.getByRole('button', { name: 'Logical' }).click();
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });

      // 6. Create empty store 188 on n18a.
      await page.locator('aside').getByRole('button', { name: 'Add Store' }).click();
      await expect(page.getByRole('dialog', { name: 'Add KV Store' })).toBeVisible();
      await page.getByLabel('KV Store ID (numeric)').fill('188');
      await page.getByLabel(/^n18a/).check();
      await page.getByRole('button', { name: /create kv store/i }).click();
      await expect(page.getByText(/KV Store 188 created successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(aside.getByText('S-188')).toBeVisible({ timeout: 15_000 });

      await aside.getByText('S-188').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add group/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
      await page.getByLabel('Group ID (numeric)').fill('1880');
      await page.getByLabel('Starting Replica ID (numeric)').fill('18800');
      await page.getByLabel(/^n18b/).uncheck();
      await page.getByLabel(/^n18a/).check();
      await page.getByRole('button', { name: /create group/i }).click();
      await expect(page.getByText(/Group 1880 created successfully/)).toBeVisible({ timeout: 30_000 });

      // Store created after tree mount -> expand it to reveal its group.
      const store188 = page.getByRole('treeitem').filter({ hasText: 'S-188' });
      const expandStore188 = store188.getByRole('button', { name: 'Expand' });
      if (await expandStore188.count()) await expandStore188.click();

      // 7. Add replica to group 1880 on n18b via UI.
      await expect(aside.getByText('G-1880')).toBeVisible({ timeout: 15_000 });
      const group1880 = page.getByRole('treeitem').filter({ hasText: 'G-1880' });
      const expandGroup1880 = group1880.getByRole('button', { name: 'Expand' });
      if (await expandGroup1880.count()) await expandGroup1880.click();
      await aside.getByText('G-1880').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Replica' })).toBeVisible();
      await page.getByLabel('Node', { exact: true }).selectOption('n18b');
      await page.getByRole('button', { name: /add replica/i }).click();
      await expect(page.getByText(/Replica added to node "n18b" successfully/)).toBeVisible({ timeout: 30_000 });

      // Verify both replicas exist in the tree.
      await expect(aside.getByText('LR-18800')).toBeVisible({ timeout: 15_000 });
      await expect(aside.getByText('LR-18801')).toBeVisible({ timeout: 15_000 });

      const response = await api.get('/api/stores/188/groups/1880/replicas');
      expect(response.ok(), await response.text()).toBeTruthy();
      const replicas = await response.json();
      expect(replicas).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ replica_id: 18800, node_id: 'n18a' }),
          expect.objectContaining({ replica_id: 18801, node_id: 'n18b' }),
        ]),
      );
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 'n18a');
      await stopNodeServer(baseURL!, 'n18b');
    }
  });
});
