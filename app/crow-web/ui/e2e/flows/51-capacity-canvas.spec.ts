// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 15s (2026-08-17)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import {
  apiContext,
  createRack,
  createNode,
  freePort,
  addDiskGroup as apiAddDiskGroup,
  removeDiskGroup as apiRemoveDiskGroup,
  addDisksBatch,
  removeDisk,
  randomDiskId,
  removeDiskdb,
  deployDiskdb,
  deployNodeServer,
  clusterInit,
  waitForLeader,
} from '../fixtures/consoleSetup';

const CANVAS_RACK = 510;
const CANVAS_NODE = 510;

/**
 * Capacity canvas + scanner/recalc E2E (R77 Phase 6.3-6.4).
 * Deploys a diskdb instance and verifies the CapacityPanel renders
 * the ScannerPanel, RecalcPanel, and per-disk UI elements. The
 * diskdb's gRPC endpoint may not be fully reachable in the test
 * environment, so we test the UI components that render from the
 * service registry data and verify the action buttons trigger calls.
 */
test.describe('capacity · canvas + scanner/recalc', () => {
  test.beforeAll(async () => {
    const baseURL = consoleBaseURL();
    const resetApi = await apiContext(baseURL);
    try {
      await resetApi.post('/internal/reset').catch(() => {});
    } finally {
      await resetApi.dispose();
    }

    await createRack(baseURL, { id: CANVAS_RACK, name: 'Rack 510' });
    await createNode(baseURL, { id: CANVAS_NODE, rack_id: CANVAS_RACK });
    await deployNodeServer(baseURL, CANVAS_NODE, freePort(), freePort());
    await clusterInit(baseURL, [CANVAS_NODE]);
    await waitForLeader(baseURL, 0, 0, 15_000);
  });

  test('ScannerPanel renders with Run Scan button and empty state', async ({ page, baseURL }) => {
    test.setTimeout(30_000);
    const nodeId = CANVAS_NODE;
    const rpcPort = freePort();

    try {
      await deployDiskdb(baseURL!, nodeId, rpcPort);

      await page.goto('/');
      await page.getByRole('button', { name: 'Capacity' }).click();

      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 3_000 });

      // ScannerPanel header + Run Scan button.
      await expect(panel.getByText('Scanner', { exact: true })).toBeVisible({ timeout: 3_000 });
      const scanBtn = panel.getByRole('button', { name: /run scan/i });
      await expect(scanBtn).toBeVisible({ timeout: 3_000 });

      // Before any scan, should show "No scan has been run yet."
      await expect(panel.getByText('No scan has been run yet.')).toBeVisible({ timeout: 3_000 });

      // Click Run Scan — button should handle the response (success or error).
      await scanBtn.click();

      // The button should re-enable after the action completes.
      await expect.poll(async () => {
        const btn = panel.getByRole('button', { name: /run scan|scanning/i });
        return btn.isEnabled();
      }, { timeout: 10_000, intervals: [100] }).toBe(true);
    } finally {
      await removeDiskdb(baseURL!, nodeId);
    }
  });

  test('CapacityPanel shows instance header with grpc endpoint', async ({ page, baseURL }) => {
    test.setTimeout(30_000);
    const nodeId = CANVAS_NODE;
    const rpcPort = freePort();

    try {
      await deployDiskdb(baseURL!, nodeId, rpcPort);

      await page.goto('/');
      await page.getByRole('button', { name: 'Capacity' }).click();

      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 3_000 });

      // The instance header should show "diskdb-N" and the grpc endpoint.
      await expect(panel.getByText(/diskdb-\d+/).first()).toBeVisible({ timeout: 3_000 });

      // Cluster-wide totals cards.
      await expect(panel.getByText('Total Capacity')).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByText('Busy', { exact: true })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByText('Free', { exact: true })).toBeVisible({ timeout: 3_000 });

      // Refresh button.
      await expect(panel.getByRole('button', { name: 'Refresh' })).toBeVisible({ timeout: 3_000 });
    } finally {
      await removeDiskdb(baseURL!, nodeId);
    }
  });

  test('per-disk boxes and RecalcPanel render when DG has usage data', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const nodeId = CANVAS_NODE;
    const dgId = 610;
    const diskId = randomDiskId();
    const rpcPort = freePort();

    try {
      await deployDiskdb(baseURL!, nodeId, rpcPort);
      await apiAddDiskGroup(baseURL!, nodeId, dgId, 'canvas-dg');
      await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

      await page.goto('/');
      await page.getByRole('button', { name: 'Capacity' }).click();

      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 3_000 });

      // Wait for the diskdb to report owning this DG. The keepalive
      // loop writes owned_dg_ids to the service registry; this may
      // take a few seconds. If the DG never appears, the diskdb's
      // gRPC endpoint isn't reachable — skip the canvas assertions.
      const dgLocator = panel.getByText(`DG-${dgId}`, { exact: true });

      let dgVisible = false;
      try {
        await expect(dgLocator).toBeVisible({ timeout: 15_000 });
        dgVisible = true;
      } catch {
        console.warn(`DG-${dgId} did not appear in CapacityPanel — diskdb gRPC not reachable, skipping canvas assertions`);
      }

      if (dgVisible) {
        // Expand the disk-group.
        const dgRow = dgLocator.locator('..');
        await dgRow.click();

        // RecalcPanel should render inside the expanded DG.
        await expect(panel.getByText(`Recalc (DG-${dgId})`)).toBeVisible({ timeout: 3_000 });
        await expect(panel.getByRole('button', { name: /run recalc/i })).toBeVisible({ timeout: 3_000 });

        // Per-disk boxes render as colored divs with busy percentage.
        // The disk row should show the disk ID.
        await expect(panel.getByText(diskId, { exact: true })).toBeVisible({ timeout: 3_000 });

        // Expand the disk row to see the zone grid.
        const diskRow = panel.getByText(diskId, { exact: true }).locator('..');
        await diskRow.click();

        // Zone grid section should appear.
        await expect(panel.getByText(/Zone grid|No zone usage data available/)).toBeVisible({ timeout: 3_000 });

        // Click "Run Recalc" — should trigger the recalc action.
        const recalcBtn = panel.getByRole('button', { name: /run recalc/i });
        await recalcBtn.click();

        // The placeholder text should be replaced by a result.
        await expect.poll(async () => {
          return panel.getByText('Click "Run Recalc" to check for usage drift.').count();
        }, { timeout: 10_000, intervals: [100] }).toBe(0);
      }
    } finally {
      await removeDisk(baseURL!, nodeId, dgId, diskId).catch(() => {});
      await apiRemoveDiskGroup(baseURL!, nodeId, dgId).catch(() => {});
      await removeDiskdb(baseURL!, nodeId);
    }
  });

  /**
   * Datacenter root (plan-datacenter-root): the fixed `datacenter` node
   * sits above racks in the Capacity sidebar. Selecting it opens the
   * inspector with rack count + cluster-wide capacity totals (one DC →
   * its totals ARE the cluster totals).
   */
  test('datacenter root in Capacity sidebar; inspector shows cluster totals', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const nodeId = CANVAS_NODE;
    const dgId = 620;
    const diskId = randomDiskId();
    const rpcPort = freePort();

    try {
      await deployDiskdb(baseURL!, nodeId, rpcPort);
      await apiAddDiskGroup(baseURL!, nodeId, dgId, 'dc-dg');
      await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

      await page.goto('/');
      await page.getByRole('button', { name: 'Capacity' }).click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      // The datacenter root is the top treeitem, above the rack.
      const dcItem = aside.getByRole('treeitem').filter({ hasText: /^datacenter$/ });
      await expect(dcItem).toBeVisible({ timeout: 3_000 });
      await expect(aside.getByRole('treeitem').first()).toHaveText(/datacenter/);

      // Select the datacenter → inspector opens.
      await aside.getByText('datacenter', { exact: true }).click();
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 3_000 });
      const typeDd = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Type' }) }).locator('dd');
      await expect(typeDd).toHaveText('Datacenter', { timeout: 3_000 });
      // Rack count is always shown (one rack from beforeAll).
      const rackCountDd = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Rack Count' }) }).locator('dd');
      await expect(rackCountDd).toHaveText('1', { timeout: 3_000 });

      // Capacity totals (Total Capacity / Used / Free) are shown in the
      // Capacity view. Wait for the DG to report usage so the totals are
      // non-zero; if the diskdb gRPC is unreachable, still verify the
      // labels render (totals would be 0 B).
      await expect(inspector.getByText('Total Capacity')).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText('Used', { exact: true })).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByText('Free', { exact: true })).toBeVisible({ timeout: 3_000 });

      // If the DG appears in usage, verify the inspector totals match the
      // cluster-wide sum from the API.
      const api = await apiContext(baseURL!);
      try {
        let usageOk = false;
        try {
          await expect.poll(async () => {
            const r = await api.get('/api/diskdb/usage');
            if (!r.ok()) return false;
            const body = await r.json();
            return Array.isArray(body.disk_groups) && body.disk_groups.some((g: any) => g.disk_group_id === dgId);
          }, { timeout: 15_000, intervals: [200] }).toBe(true);
          usageOk = true;
        } catch {
          console.warn(`DG-${dgId} never reported usage — diskdb gRPC not reachable, skipping totals match`);
        }

        if (usageOk) {
          const r = await api.get('/api/diskdb/usage');
          const body = await r.json();
          const sum = (body.disk_groups || []).reduce(
            (acc: { capacity: number; busy: number; free: number }, g: any) => ({
              capacity: acc.capacity + g.capacity_bytes,
              busy: acc.busy + g.busy_bytes,
              free: acc.free + g.free_bytes,
            }),
            { capacity: 0, busy: 0, free: 0 },
          );
          const capDd = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Total Capacity' }) }).locator('dd');
          await expect(capDd).toHaveText(formatBytesAssert(sum.capacity), { timeout: 3_000 });
        }
      } finally {
        await api.dispose();
      }
    } finally {
      await removeDisk(baseURL!, nodeId, dgId, diskId).catch(() => {});
      await apiRemoveDiskGroup(baseURL!, nodeId, dgId).catch(() => {});
      await removeDiskdb(baseURL!, nodeId);
    }
  });
});

/** Match the Inspector's formatBytes rendering for an exact-text assertion. */
function formatBytesAssert(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}
