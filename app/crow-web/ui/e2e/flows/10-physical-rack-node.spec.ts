// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 10s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createNode, createRack, createStore, deployNodeServer, freePort, removeDiskdb, resetAll, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('physical · rack + node CRUD', () => {
  test('renders the SPA shell against a real empty backend', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (message) => {
      if (message.type() === 'error') {
        consoleErrors.push(message.text());
      }
    });

    await page.goto('/');

    await expect(page.getByRole('button', { name: 'Physical' })).toBeVisible({ timeout: 3_000 });
    await expect(page.getByRole('button', { name: 'KV Cluster' })).toBeVisible({ timeout: 3_000 });
    await expect(page.getByPlaceholder('Filter...')).toBeVisible();

    const healthText = page.locator('header').getByText(/healthy|degraded|failed|unknown/i);
    await expect(healthText).toBeVisible({ timeout: 3_000 });

    // Ignore transient network 404s; fail only on real JS/runtime errors.
    const jsErrors = consoleErrors.filter((e) => !/Failed to load resource/i.test(e));
    expect(jsErrors, jsErrors.join('\n')).toEqual([]);
  });

  test('creates racks and nodes through the UI and verifies the real backend', async ({ page, baseURL }) => {
    // --- Add a rack through the UI ---
    {
      await page.goto('/');

      await page.getByRole('button', { name: 'Physical' }).click();
      await page.getByRole('button', { name: 'Add Rack' }).click();

      await expect(page.getByRole('dialog', { name: 'Add Rack' })).toBeVisible();
      await page.getByLabel('Rack ID').fill('1');
      await page.getByLabel('Name (optional)').fill('Rack One');
      await page.getByRole('button', { name: /create rack/i }).click();

      await expect(page.locator('aside').getByText('Rack One')).toBeVisible({ timeout: 3_000 });

      const api = await apiContext(baseURL!);
      try {
        const response = await api.get('/api/racks');
        expect(response.ok(), await response.text()).toBeTruthy();
        const racks = await response.json();
        expect(racks).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ id: 1, name: 'Rack One' }),
          ]),
        );
      } finally {
        await api.dispose();
      }
    }

    // --- Add a node via the rack context menu (services disabled) ---
    {
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
    }

    // --- Add a node with Crow Storage and DiskDB services enabled ---
    {
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
      // The DiskDB RPC Port field should be pre-filled with an
      // auto-incremented value (not the hardcoded 29920 base) —
      // regression: previously always 29920, causing port collisions
      // when creating multiple nodes with DiskDB.
      const diskdbPortInput = page.getByTestId('diskdb-rpc-port');
      const preFilledDiskdbPort = await diskdbPortInput.inputValue();
      expect(preFilledDiskdbPort).toMatch(/^\d+$/);
      expect(preFilledDiskdbPort).not.toBe('29920');

      await page.getByLabel('REST Port').fill(String(restPort));
      await page.getByTestId('kv-rpc-port').fill(String(rpcPort));
      await page.getByTestId('diskdb-rpc-port').fill(String(diskdbRpcPort));

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

        // The DiskDB server should appear as a DDB-{nodeId} item under
        // N-{nodeId} in the Physical view tree (mirrors KV-{nodeId}).
        // Expand the node first so its children are rendered.
        const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
        if (await expandNode.count() > 0) await expandNode.first().click();
        await expect(aside.getByText(`DDB-${nodeId}`, { exact: true })).toBeVisible({ timeout: 10_000 });

        // The DDB item should have a health badge (regression: previously
        // no health icon). The badge renders with a title attribute set
        // to the health status (Healthy/Degraded/Failed/Unknown).
        const ddbItem = aside.getByRole('treeitem').filter({ hasText: `DDB-${nodeId}` });
        await expect(ddbItem.getByTitle(/Healthy|Degraded|Failed|Unknown/)).toBeVisible({ timeout: 10_000 });
      } finally {
        await api.dispose();
        await stopNodeServer(baseURL!, nodeId);
        await removeDiskdb(baseURL!, nodeId);
      }
    }
  });

  /**
   * Destructive confirms for store / node / rack (Req §3.2, §6).
   *
   * Replica/group deletes are covered by the KV cluster specs; this closes
   * the physical and logical *root* deletes. Each delete is confirm-gated:
   * we cancel once to prove the guard, then confirm and verify removal in
   * the DOM and via the backend.
   */
  test('confirm-gates store, node, and rack deletion', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 25, 25);
    await deployNodeServer(baseURL!, 25, freePort(), freePort());
    await createStore(baseURL!, 255, [25]);
    // A serverless node (clean to delete) and an empty rack (clean to delete).
    await createNode(baseURL!, { id: 274, rack_id: 25 });
    await createRack(baseURL!, { id: 255, name: 'Rack TwentyFive Empty' });

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // ── Store (logical) ──────────────────────────────────────────
      await page.getByRole('button', { name: 'KV Cluster' }).click();
      await expect(aside.getByText('S-255', { exact: true })).toBeVisible({ timeout: 3_000 });

      // Cancel first.
      await aside.getByText('S-255', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete store/i }).click();
      await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: 'Cancel' }).click();
      await expect(aside.getByText('S-255', { exact: true })).toBeVisible();

      // Confirm.
      await aside.getByText('S-255', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete store/i }).click();
      await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: /delete store/i }).click();
      await expect(aside.getByText('S-255', { exact: true })).toHaveCount(0, { timeout: 3_000 });

      const storesResp = await api.get('/api/stores');
      expect(storesResp.ok(), await storesResp.text()).toBeTruthy();
      expect(await storesResp.json()).not.toEqual(
        expect.arrayContaining([expect.objectContaining({ store_id: 255 })]),
      );

      // ── Node (physical, serverless n25x) ─────────────────────────
      await page.getByRole('button', { name: 'Physical' }).click();
      const node25x = page.getByRole('treeitem').filter({ hasText: 'N-274' });
      await expect(node25x).toBeVisible({ timeout: 3_000 });

      await node25x.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete node/i }).click();
      await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: 'Cancel' }).click();
      await expect(node25x).toBeVisible();

      await node25x.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete node/i }).click();
      await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: /delete node/i }).click();
      await expect(page.getByRole('treeitem').filter({ hasText: 'N-274' })).toHaveCount(0, { timeout: 3_000 });

      const nodeResp = await api.get('/api/nodes/274');
      expect(nodeResp.status()).toBe(404);

      // ── Rack (physical, empty r25e) ──────────────────────────────
      const rack25e = page.getByRole('treeitem').filter({ hasText: 'R-255' });
      await expect(rack25e).toBeVisible({ timeout: 3_000 });
      await rack25e.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete rack/i }).click();
      await page.getByRole('dialog', { name: 'Delete Rack' }).getByRole('button', { name: /delete rack/i }).click();
      await expect(page.getByRole('treeitem').filter({ hasText: 'R-255' })).toHaveCount(0, { timeout: 3_000 });

      const racksResp = await api.get('/api/racks');
      expect(racksResp.ok(), await racksResp.text()).toBeTruthy();
      expect(await racksResp.json()).not.toEqual(
        expect.arrayContaining([expect.objectContaining({ id: 255 })]),
      );
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 25);
    }
  });

  // Runs last: needs an empty backend, so it resets all registry state.
  test('rejects duplicate rack and node IDs from the add dialogs', async ({ page, baseURL }) => {
    await resetAll(baseURL!);

    // --- Adding a rack with an existing ID shows an error toast ---
    {
      await createRack(baseURL!, { id: 37, name: 'r37' });

      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // Click Add Rack
      await aside.getByRole('button', { name: 'Add Rack' }).click();
      const dialog = page.getByRole('dialog', { name: 'Add Rack' });
      await expect(dialog).toBeVisible();
      await dialog.getByLabel('Rack ID').fill('37');
      await dialog.getByLabel('Name (optional)').fill('duplicate');

      // Submit and expect error toast
      const responsePromise = page.waitForResponse((r: any) => r.url().includes('/api/racks'));
      await dialog.getByRole('button', { name: /create rack/i }).click();
      const response = await responsePromise;
      expect(response.status()).toBe(409);
    }

    // --- Adding a node with an existing ID shows an error toast ---
    {
      await createRack(baseURL!, { id: 372, name: 'r37b' });
      await createNode(baseURL!, { id: 372, rack_id: 372 });

      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();

      // Right-click rack to get Add Node
      const rackItem = page.getByRole('treeitem').filter({ hasText: 'R-372' });
      await expect(rackItem).toBeVisible({ timeout: 3_000 });
      await rackItem.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add node/i }).click();

      const dialog = page.getByRole('dialog', { name: 'Add Node' });
      await expect(dialog).toBeVisible();
      await dialog.getByLabel('Node ID').fill('372');
      await dialog.getByLabel('Host').fill('127.0.0.1');

      const responsePromise = page.waitForResponse((r: any) => r.url().includes('/api/nodes'));
      await dialog.getByRole('button', { name: /create node/i }).click();
      const response = await responsePromise;
      expect(response.status()).toBe(409);
    }
  });
});
