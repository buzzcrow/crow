// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 5s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack, freePort, stopNodeServer, stopDiskdb } from '../fixtures/consoleSetup';

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
    await page.getByLabel('Enable DiskDB on this node').uncheck();
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

  test('creates a node with both Crow Storage and DiskDB services enabled', async ({ page, baseURL }) => {
    const rackId = 31;
    const nodeId = 310;
    const restPort = freePort();
    const rpcPort = freePort();
    const diskdbRpcPort = freePort();
    await createRack(baseURL!, { id: rackId, name: 'Rack Thirty-One' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await expect(aside.getByText(`R-${rackId} (Rack Thirty-One)`)).toBeVisible({ timeout: 3_000 });

    // Right-click the rack → Add Node. Both "Enable Crow Storage" and
    // "Enable DiskDB" checkboxes default to checked.
    await aside.getByText(`R-${rackId} (Rack Thirty-One)`).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();

    await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
    await page.getByLabel('Node ID').fill(String(nodeId));
    await page.getByLabel('Host').fill('127.0.0.1');

    // Both service checkboxes should be checked by default.
    await expect(page.getByLabel('Enable Crow Storage on this node')).toBeChecked();
    await expect(page.getByLabel('Enable DiskDB on this node')).toBeChecked();

    // Fill in unique ports for KV (REST + RPC) and DiskDB (RPC).
    await page.getByLabel('REST Port').fill(String(restPort));
    await page.getByLabel('RPC Port', { exact: true }).fill(String(rpcPort));
    await page.getByLabel('DiskDB RPC Port').fill(String(diskdbRpcPort));

    await expect(page.getByRole('button', { name: /create node/i })).toBeEnabled();
    await page.getByRole('button', { name: /create node/i }).click();

    // The node should appear in the sidebar.
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 3_000 });

    const api = await apiContext(baseURL!);
    try {
      // Verify the node exists via API.
      const nodesResp = await api.get('/api/nodes');
      expect(nodesResp.ok(), await nodesResp.text()).toBeTruthy();
      const nodes = await nodesResp.json();
      expect(nodes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: nodeId, rack_id: rackId, host: '127.0.0.1' }),
        ]),
      );

      // Verify the Crow Storage server was deployed (has a live pid).
      await expect.poll(async () => {
        const r = await api.get(`/api/nodes/${nodeId}/server`);
        if (!r.ok()) return 0;
        return (await r.json()).pid ?? 0;
      }, { timeout: 10_000, intervals: [100] }).toBeGreaterThan(0);

      // Verify the DiskDB instance was deployed — check /api/servers for
      // a diskdb entry with this node_id.
      await expect.poll(async () => {
        const r = await api.get('/api/servers');
        if (!r.ok()) return false;
        const servers = await r.json();
        return servers.some((s: { node_id?: number; service_type: string }) =>
          s.node_id === nodeId && s.service_type === 'diskdb');
      }, { timeout: 10_000, intervals: [100] }).toBe(true);
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, nodeId);
      await stopDiskdb(baseURL!, nodeId);
    }
  });
});
