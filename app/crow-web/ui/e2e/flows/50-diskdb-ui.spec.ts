// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 45s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
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
  deployDiskdb,
  stopDiskdb,
} from '../fixtures/consoleSetup';

const DISKDB_RACK = 501;
const DISKDB_NODE = 501;
const DISKDB_DG = 501;

async function cleanupDiskdb(baseURL: string) {
  const api = await apiContext(baseURL);
  try {
    await api.delete(`/api/nodes/${DISKDB_NODE}/diskdb`);
  } catch {
    // best-effort
  }
  await api.dispose();
  await stopDiskdb(baseURL, DISKDB_NODE);
  await apiRemoveDiskGroup(baseURL, DISKDB_NODE, DISKDB_DG);
}

test.describe('E2E-50 diskdb UI flows', () => {
  test.afterEach(async ({ baseURL }) => {
    await cleanupDiskdb(baseURL!);
  });

  test('capacity view shows rack → node hierarchy (no + button)', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: DISKDB_RACK, name: 'Rack 501' });
    await createNode(baseURL!, { id: DISKDB_NODE, rack_id: DISKDB_RACK });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

    // The + button should NOT be visible in Capacity view (racks are
    // created in the Physical view only).
    await expect(aside.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);

    // The rack should appear in the tree.
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${DISKDB_RACK}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${DISKDB_NODE}`, { exact: true })).toBeVisible({ timeout: 5_000 });
  });

  test('node context menu shows Add Disk Group + Deploy DiskDB', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: DISKDB_RACK + 10, name: 'Rack 511' });
    await createNode(baseURL!, { id: DISKDB_NODE + 10, rack_id: DISKDB_RACK + 10 });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${DISKDB_RACK + 10}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${DISKDB_NODE + 10}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click the node.
    await aside.getByText(`N-${DISKDB_NODE + 10}`, { exact: true }).click({ button: 'right' });

    await expect(page.getByRole('menuitem', { name: /add disk group/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toBeVisible();
    await page.keyboard.press('Escape');
  });

  test('Add Disk Group dialog creates a disk-group via UI', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 20;
    const nodeId = DISKDB_NODE + 20;
    const dgId = 520;
    await createRack(baseURL!, { id: rackId, name: 'Rack 520' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click node → Add Disk Group.
    await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add disk group/i }).click();

    const dialog = page.getByRole('dialog', { name: /add disk group/i });
    await expect(dialog).toBeVisible();
    // The dialog should have a Disk Group ID field and a Name field.
    await expect(dialog.getByLabel('Disk Group ID (numeric)')).toBeVisible();
    await expect(dialog.getByLabel('Name (optional)')).toBeVisible();

    // Set the disk-group ID and submit.
    await dialog.getByLabel('Disk Group ID (numeric)').fill(String(dgId));
    await dialog.getByLabel('Name (optional)').fill('test-dg');
    const confirmBtn = dialog.getByRole('button', { name: /create disk group/i });
    await confirmBtn.evaluate((el) => (el as HTMLElement).click());

    // The disk-group should appear in the sidebar.
    await expect(aside.getByText(/test-dg.*DG-520|DG-520.*test-dg/, { exact: true })).toBeVisible({ timeout: 10_000 });

    // Verify via API.
    const api = await apiContext(baseURL!);
    try {
      const r = await api.get(`/api/nodes/${nodeId}/disk-groups`);
      expect(r.ok()).toBeTruthy();
      const dgs = await r.json();
      expect(dgs.some((dg: any) => dg.id === dgId && dg.node_id === nodeId)).toBeTruthy();
    } finally {
      await api.dispose();
    }

    // Cleanup.
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('disk-group context menu shows Add Disk + set-status + delete (no operations)', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 30;
    const nodeId = DISKDB_NODE + 30;
    const dgId = 530;
    await createRack(baseURL!, { id: rackId, name: 'Rack 530' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-530');

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Expand the node to see the disk-group.
    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();

    // Right-click the disk-group.
    await expect(aside.getByText(/DG-530/, { exact: true })).toBeVisible({ timeout: 5_000 });
    await aside.getByText(/DG-530/, { exact: true }).click({ button: 'right' });

    await expect(page.getByRole('menuitem', { name: /add disk/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /set disk group up/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /set disk group down/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /delete disk group/i })).toBeVisible();
    // Operations belong on disk, not disk-group.
    await expect(page.getByRole('menuitem', { name: /trigger.*scan/i })).toHaveCount(0);
    await expect(page.getByRole('menuitem', { name: /recalc usage/i })).toHaveCount(0);
    await page.keyboard.press('Escape');

    // Cleanup.
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Add Disk dialog adds disks via UI', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 40;
    const nodeId = DISKDB_NODE + 40;
    const dgId = 540;
    await createRack(baseURL!, { id: rackId, name: 'Rack 540' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-540');

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Expand the node.
    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    await expect(aside.getByText(/DG-540/, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click disk-group → Add Disk.
    await aside.getByText(/DG-540/, { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add disk/i }).click();

    const dialog = page.getByRole('dialog', { name: /add disks/i });
    await expect(dialog).toBeVisible();

    // The dialog should have a Disk ID field and a Type selector.
    const diskIdInput = dialog.getByLabel('Disk ID (UUID)');
    await expect(diskIdInput).toBeVisible();

    // Set a known disk ID.
    const testDiskId = randomDiskId();
    await diskIdInput.fill(testDiskId);

    const confirmBtn = dialog.getByRole('button', { name: /add disks/i });
    await confirmBtn.evaluate((el) => (el as HTMLElement).click());

    // Wait for the dialog to close and refresh to complete.
    await expect(dialog).toHaveCount(0, { timeout: 10_000 });

    // Expand the disk-group to see the disk.
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-540/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // The disk should appear in the sidebar (truncated to 12 chars + …).
    await expect(aside.getByText(testDiskId.slice(0, 12), { exact: false })).toBeVisible({ timeout: 10_000 });

    // Verify via API.
    const api = await apiContext(baseURL!);
    try {
      const r = await api.get(`/api/nodes/${nodeId}/disk-groups/${dgId}/disks`);
      expect(r.ok()).toBeTruthy();
      const disks = await r.json();
      expect(disks.some((d: any) => d.disk_id === testDiskId)).toBeTruthy();
    } finally {
      await api.dispose();
    }

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, testDiskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Compact Zones opens zone select dialog with range input', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 45;
    const nodeId = DISKDB_NODE + 45;
    const dgId = 545;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 545' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-545');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-545/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // Right-click the disk → Compact Zones.
    const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
    await expect(diskLabel).toBeVisible({ timeout: 5_000 });
    await diskLabel.first().click({ button: 'right' });
    await page.getByRole('menuitem', { name: /compact zones/i }).click();

    const dialog = page.getByRole('dialog', { name: /compact zones/i });
    await expect(dialog).toBeVisible();
    // Should have a Zones input field.
    await expect(dialog.getByLabel(/zones/i)).toBeVisible();
    // Default value should be "all".
    await expect(dialog.getByLabel(/zones/i)).toHaveValue('all');
    // Should have a Compact button.
    await expect(dialog.getByRole('button', { name: /compact/i })).toBeVisible();
    await page.keyboard.press('Escape');

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Rebuild Bitmap opens zone select dialog', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 46;
    const nodeId = DISKDB_NODE + 46;
    const dgId = 546;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 546' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-546');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-546/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // Right-click the disk → Rebuild Bitmap.
    const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
    await expect(diskLabel).toBeVisible({ timeout: 5_000 });
    await diskLabel.first().click({ button: 'right' });
    await page.getByRole('menuitem', { name: /rebuild bitmap/i }).click();

    const dialog = page.getByRole('dialog', { name: /rebuild bitmap/i });
    await expect(dialog).toBeVisible();
    await expect(dialog.getByLabel(/zones/i)).toBeVisible();
    await expect(dialog.getByLabel(/zones/i)).toHaveValue('all');
    await expect(dialog.getByRole('button', { name: /rebuild/i })).toBeVisible();
    await page.keyboard.press('Escape');

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Compact Zones dialog validates zone input (invalid disables button)', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 47;
    const nodeId = DISKDB_NODE + 47;
    const dgId = 547;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 547' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-547');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-547/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // Right-click the disk → Compact Zones.
    const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
    await expect(diskLabel).toBeVisible({ timeout: 5_000 });
    await diskLabel.first().click({ button: 'right' });
    await page.getByRole('menuitem', { name: /compact zones/i }).click();

    const dialog = page.getByRole('dialog', { name: /compact zones/i });
    await expect(dialog).toBeVisible();
    const zoneInput = dialog.getByLabel(/zones/i);
    const compactBtn = dialog.getByRole('button', { name: /compact/i });

    // Default "all" → button enabled.
    await expect(zoneInput).toHaveValue('all');
    await expect(compactBtn).toBeEnabled();

    // Valid range "1-5,10" → button enabled.
    await zoneInput.fill('1-5,10');
    await expect(compactBtn).toBeEnabled();

    // Single zone "3" → button enabled.
    await zoneInput.fill('3');
    await expect(compactBtn).toBeEnabled();

    // Invalid "abc" → button disabled.
    await zoneInput.fill('abc');
    await expect(compactBtn).toBeDisabled();

    // Invalid "1-5,abc" → button disabled.
    await zoneInput.fill('1-5,abc');
    await expect(compactBtn).toBeDisabled();

    // Empty → button enabled (means all zones).
    await zoneInput.fill('');
    await expect(compactBtn).toBeEnabled();

    await page.keyboard.press('Escape');

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Set Disk Group Down via context menu calls the status API', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 48;
    const nodeId = DISKDB_NODE + 48;
    const dgId = 548;
    await createRack(baseURL!, { id: rackId, name: 'Rack 548' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-548');

    // Intercept the status PUT and fulfill with 204.
    const statusRequest = page.waitForRequest(
      (req) => req.method() === 'PUT' && req.url().includes(`/api/disk-groups/${rackId}/${nodeId}/${dgId}/status`),
      { timeout: 10_000 },
    ).then(async (req) => {
      expect(req.postDataJSON()).toEqual({ status: 'Down' });
    });
    await page.route(`**/api/disk-groups/${rackId}/${nodeId}/${dgId}/status`, (route) => {
      route.fulfill({ status: 204 });
    });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();

    // Right-click disk-group → Set Disk Group Down.
    await expect(aside.getByText(/DG-548/, { exact: true })).toBeVisible({ timeout: 5_000 });
    await aside.getByText(/DG-548/, { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /set disk group down/i }).click();

    // Verify the API call was made with correct payload.
    await statusRequest;

    // Cleanup.
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Set Disk Down via context menu calls the status API', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 49;
    const nodeId = DISKDB_NODE + 49;
    const dgId = 549;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 549' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-549');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    // Intercept the status PUT.
    const statusRequest = page.waitForRequest(
      (req) => req.method() === 'PUT' && req.url().includes(`/api/disks/${encodeURIComponent(diskId)}/status`),
      { timeout: 10_000 },
    ).then(async (req) => {
      expect(req.postDataJSON()).toEqual({ status: 'Down' });
    });
    await page.route(`**/api/disks/${encodeURIComponent(diskId)}/status`, (route) => {
      route.fulfill({ status: 204 });
    });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-549/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // Right-click disk → Set Disk Down.
    const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
    await expect(diskLabel).toBeVisible({ timeout: 5_000 });
    await diskLabel.first().click({ button: 'right' });
    await page.getByRole('menuitem', { name: /set disk down/i }).click();

    // Verify the API call was made with correct payload.
    await statusRequest;

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Recalc Usage via disk context menu calls the recalc API', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 51;
    const nodeId = DISKDB_NODE + 51;
    const dgId = 551;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 551' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-551');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    // Intercept the recalc POST.
    const recalcRequest = page.waitForRequest(
      (req) => req.method() === 'POST' && req.url().includes('/api/diskdb/recalc'),
      { timeout: 10_000 },
    ).then(async (req) => {
      expect(req.postDataJSON()).toEqual({ dg: dgId });
    });
    await page.route('**/api/diskdb/recalc', (route) => {
      route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ results: [] }) });
    });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-551/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // Right-click disk → Recalc Usage.
    const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
    await expect(diskLabel).toBeVisible({ timeout: 5_000 });
    await diskLabel.first().click({ button: 'right' });
    await page.getByRole('menuitem', { name: /recalc usage/i }).click();

    // Verify the API call was made with the disk's dg_id.
    await recalcRequest;

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Trigger Consistency Scan via disk context menu calls the scan API', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 52;
    const nodeId = DISKDB_NODE + 52;
    const dgId = 552;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 552' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-552');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    // Intercept the scan POST.
    const scanRequest = page.waitForRequest(
      (req) => req.method() === 'POST' && req.url().includes('/api/diskdb/scan'),
      { timeout: 10_000 },
    ).then(async (req) => {
      expect(req.postDataJSON()).toEqual({ dg: dgId });
    });
    await page.route('**/api/diskdb/scan', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ summary: null, has_run: false, scan_in_progress: true }),
      });
    });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-552/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // Right-click disk → Trigger Consistency Scan.
    const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
    await expect(diskLabel).toBeVisible({ timeout: 5_000 });
    await diskLabel.first().click({ button: 'right' });
    await page.getByRole('menuitem', { name: /trigger consistency scan/i }).click();

    // Verify the API call was made with the disk's dg_id.
    await scanRequest;

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('sidebar shows health badges for disk-group and disk when usage data is available', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 53;
    const nodeId = DISKDB_NODE + 53;
    const dgId = 553;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 553' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-553');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    // Mock the usage API to return status data.
    await page.route('**/api/diskdb/usage', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          disk_groups: [{
            rack_id: rackId,
            node_id: nodeId,
            disk_group_id: dgId,
            status: 1, // HW_STATUS_UP → Healthy
            disk_ids: [diskId],
            disks: [{
              rack_id: rackId,
              node_id: nodeId,
              disk_group_id: dgId,
              disk_id: diskId,
              disk_type: 1,
              capacity_units: 1000,
              zone_size_units: 100,
              unit_size_bytes: 4096,
              zone_count: 10,
              status: 1, // HW_STATUS_UP → Healthy
              busy_units: 100,
              free_units: 900,
              capacity_bytes: 4096000,
              busy_bytes: 409600,
              free_bytes: 3686400,
              active_zone_count: 5,
              zone_usages: [],
            }],
            capacity_bytes: 4096000,
            busy_bytes: 409600,
            free_bytes: 3686400,
            allocatable_disk_count: 1,
          }],
        }),
      });
    });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();

    // The disk-group should show a "Healthy" badge (compact mode:
    // icon with title attribute, no text).
    const dgTreeitem = aside.getByRole('treeitem').filter({ hasText: /DG-553/ });
    await expect(dgTreeitem).toBeVisible({ timeout: 5_000 });
    await expect(dgTreeitem.getByTitle('Healthy')).toBeVisible();

    // Expand disk-group to see the disk.
    const expandDg = dgTreeitem.locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // The disk should also show a "Healthy" badge.
    const diskTreeitem = aside.getByRole('treeitem').filter({ hasText: diskId.slice(0, 12) });
    await expect(diskTreeitem).toBeVisible({ timeout: 5_000 });
    await expect(diskTreeitem.getByTitle('Healthy')).toBeVisible();

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Deploy DiskDB dialog only has RPC port (no REST/binary/listen/http/config)', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 50;
    const nodeId = DISKDB_NODE + 50;
    await createRack(baseURL!, { id: rackId, name: 'Rack 550' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click node → Deploy DiskDB.
    await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /deploy diskdb/i }).click();

    const dialog = page.getByRole('dialog', { name: /deploy diskdb/i });
    await expect(dialog).toBeVisible();

    // Should have RPC Port field.
    await expect(dialog.getByLabel('RPC Port (gRPC)')).toBeVisible();

    // Should NOT have REST Port, Binary Path, Listen Address, HTTP Address, Config Path.
    await expect(dialog.getByLabel('REST Port')).toHaveCount(0);
    await expect(dialog.getByLabel(/binary path/i)).toHaveCount(0);
    await expect(dialog.getByLabel(/listen address/i)).toHaveCount(0);
    await expect(dialog.getByLabel(/http address/i)).toHaveCount(0);
    await expect(dialog.getByLabel(/config path/i)).toHaveCount(0);

    await page.keyboard.press('Escape');
  });

  test('disk context menu shows compact/rebuild/scan/recalc/set-status/delete', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 60;
    const nodeId = DISKDB_NODE + 60;
    const dgId = 560;
    const diskId = randomDiskId();
    await createRack(baseURL!, { id: rackId, name: 'Rack 560' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-560');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Expand node → disk-group to see the disk.
    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-560/ }).locator('button[aria-label="Expand"]');
    if (await expandDg.count() > 0) await expandDg.click();

    // Right-click the disk.
    const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
    await expect(diskLabel).toBeVisible({ timeout: 5_000 });
    await diskLabel.first().click({ button: 'right' });

    await expect(page.getByRole('menuitem', { name: /compact zones/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /rebuild bitmap/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /trigger consistency scan/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /recalc usage/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /set disk down/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /set disk up/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /delete disk/i })).toBeVisible();
    await page.keyboard.press('Escape');

    // Cleanup.
    await removeDisk(baseURL!, nodeId, dgId, diskId);
    await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
  });

  test('Delete Disk Group via context menu removes it', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 70;
    const nodeId = DISKDB_NODE + 70;
    const dgId = 570;
    await createRack(baseURL!, { id: rackId, name: 'Rack 570' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });
    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-570');

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
    if (await expandNode.count() > 0) await expandNode.click();
    await expect(aside.getByText(/DG-570/, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click disk-group → Delete Disk Group.
    await aside.getByText(/DG-570/, { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /delete disk group/i }).click();

    // Confirm delete dialog.
    const deleteDialog = page.getByRole('dialog', { name: /delete disk group/i });
    await expect(deleteDialog).toBeVisible();
    const confirmBtn = deleteDialog.getByRole('button', { name: /delete disk group/i });
    await confirmBtn.evaluate((el) => (el as HTMLElement).click());

    // The disk-group should disappear from the tree.
    await expect(aside.getByText(/DG-570/, { exact: true })).toHaveCount(0, { timeout: 10_000 });

    // Verify via API.
    const api = await apiContext(baseURL!);
    try {
      const r = await api.get(`/api/nodes/${nodeId}/disk-groups`);
      expect(r.ok()).toBeTruthy();
      const dgs = await r.json();
      expect(dgs.some((dg: any) => dg.id === dgId)).toBeFalsy();
    } finally {
      await api.dispose();
    }
  });

  test('deploy diskdb via node context menu (UI Deploy button)', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 701;
    const nodeId = DISKDB_NODE + 701;
    const rpcPort = freePort();
    await createRack(baseURL!, { id: rackId, name: 'Rack 570' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click the node → Deploy DiskDB.
    await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /deploy diskdb/i }).click();

    const dialog = page.getByRole('dialog', { name: /deploy diskdb/i });
    await expect(dialog).toBeVisible();

    // Fill in the RPC port and click Deploy.
    await dialog.getByLabel('RPC Port (gRPC)').fill(String(rpcPort));
    await dialog.getByRole('button', { name: /deploy/i }).click();

    // The dialog should close.
    await expect(dialog).toHaveCount(0, { timeout: 5_000 });

    // Verify via API that a diskdb server entry exists for this node.
    const api = await apiContext(baseURL!);
    try {
      await expect.poll(async () => {
        const r = await api.get('/api/servers');
        if (!r.ok()) return false;
        const servers = await r.json();
        return servers.some((s: { node_id?: number; service_type: string }) =>
          s.node_id === nodeId && s.service_type === 'diskdb');
      }, { timeout: 10_000, intervals: [100] }).toBe(true);
    } finally {
      await api.dispose();
      await stopDiskdb(baseURL!, nodeId);
    }
  });

  test('full deploy flow: deploy diskdb, restart, stop via context menu', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK + 80;
    const nodeId = DISKDB_NODE + 80;
    const rpcPort = freePort();
    await createRack(baseURL!, { id: rackId, name: 'Rack 580' });
    await createNode(baseURL!, { id: nodeId, rack_id: rackId });

    // Deploy via API (the UI deploy requires the binary to be staged
    // in the workspace; the API path uses the same handler which
    // falls back to crow_diskdb_bin()).
    await deployDiskdb(baseURL!, nodeId, rpcPort);

    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // Right-click the node — should now show Restart/Stop DiskDB (not Deploy).
    await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
    await expect(page.getByRole('menuitem', { name: /restart diskdb/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /stop diskdb/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toHaveCount(0);
    await page.keyboard.press('Escape');

    // Stop via context menu.
    await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /stop diskdb/i }).click();

    // After stop, the node should show Deploy DiskDB again (not Restart/Stop).
    await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
    await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toBeVisible();
    await page.keyboard.press('Escape');
  });
});
