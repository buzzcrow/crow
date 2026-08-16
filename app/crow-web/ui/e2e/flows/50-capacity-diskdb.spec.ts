// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 40s (2026-08-16)

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

/**
 * All Capacity / DiskDB flows share ONE rack + node (and, for the final
 * lifecycle test, ONE diskdb deploy). diskdb deploy is the dominant setup
 * cost, so the deploy → restart → stop lifecycle runs last.
 */
test.describe('capacity · diskdb', () => {
  test.beforeAll(async () => {
    const baseURL = consoleBaseURL();
    await createRack(baseURL, { id: DISKDB_RACK, name: 'Rack 501' });
    await createNode(baseURL, { id: DISKDB_NODE, rack_id: DISKDB_RACK });
  });

  test.afterAll(async () => {
    await cleanupDiskdb(consoleBaseURL());
  });

  test('capacity tree, node context menu, and Deploy DiskDB dialog', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

    // --- capacity view shows rack → node hierarchy (no + button) ---

    // The + button should NOT be visible in Capacity view (racks are
    // created in the Physical view only).
    await expect(aside.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);

    // The rack should appear in the tree.
    const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${DISKDB_RACK}` }).locator('button[aria-label="Expand"]');
    if (await expandRack.count() > 0) await expandRack.click();
    await expect(aside.getByText(`N-${DISKDB_NODE}`, { exact: true })).toBeVisible({ timeout: 5_000 });

    // --- node context menu shows Add Disk Group + Deploy DiskDB ---

    // Right-click the node.
    await aside.getByText(`N-${DISKDB_NODE}`, { exact: true }).click({ button: 'right' });

    await expect(page.getByRole('menuitem', { name: /add disk group/i })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toBeVisible();
    await page.keyboard.press('Escape');

    // --- Deploy DiskDB dialog only has RPC port (no REST/binary/listen/http/config) ---

    // Right-click node → Deploy DiskDB.
    await aside.getByText(`N-${DISKDB_NODE}`, { exact: true }).click({ button: 'right' });
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

  test('disk-group and disk CRUD via the UI', async ({ page, baseURL }) => {
    const nodeId = DISKDB_NODE;
    const dg520 = 520;
    const dg530 = 530;
    const dg540 = 540;
    const dg560 = 560;
    const dg570 = 570;
    const disk540 = randomDiskId();
    const disk560 = randomDiskId();

    // Pre-create the disk-groups that are not created through the UI, so
    // the tree already holds them when the page mounts.
    await apiAddDiskGroup(baseURL!, nodeId, dg530, 'test-dg-530');
    await apiAddDiskGroup(baseURL!, nodeId, dg540, 'test-dg-540');
    await apiAddDiskGroup(baseURL!, nodeId, dg560, 'test-dg-560');
    await addDisksBatch(baseURL!, nodeId, dg560, [{ disk_id: disk560 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg570, 'test-dg-570');

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Capacity' }).click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${DISKDB_RACK}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      // --- Add Disk Group dialog creates a disk-group via UI ---

      // Right-click node → Add Disk Group.
      await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add disk group/i }).click();

      const dgDialog = page.getByRole('dialog', { name: /add disk group/i });
      await expect(dgDialog).toBeVisible();
      // The dialog should have a Disk Group ID field and a Name field.
      await expect(dgDialog.getByLabel('Disk Group ID (numeric)')).toBeVisible();
      await expect(dgDialog.getByLabel('Name (optional)')).toBeVisible();

      // Set the disk-group ID and submit.
      await dgDialog.getByLabel('Disk Group ID (numeric)').fill(String(dg520));
      await dgDialog.getByLabel('Name (optional)').fill('test-dg');
      const createDgBtn = dgDialog.getByRole('button', { name: /create disk group/i });
      await createDgBtn.evaluate((el) => (el as HTMLElement).click());

      // The disk-group should appear in the sidebar.
      await expect(aside.getByText(/test-dg.*DG-520|DG-520.*test-dg/, { exact: true })).toBeVisible({ timeout: 10_000 });

      // Verify via API.
      const dgApi = await apiContext(baseURL!);
      try {
        const r = await dgApi.get(`/api/nodes/${nodeId}/disk-groups`);
        expect(r.ok()).toBeTruthy();
        const dgs = await r.json();
        expect(dgs.some((dg: any) => dg.id === dg520 && dg.node_id === nodeId)).toBeTruthy();
      } finally {
        await dgApi.dispose();
      }

      // --- disk-group context menu shows Add Disk + set-status + delete (no operations) ---

      // Expand the node to see the disk-groups.
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

      // --- Add Disk dialog adds disks via UI ---

      await expect(aside.getByText(/DG-540/, { exact: true })).toBeVisible({ timeout: 5_000 });

      // Right-click disk-group → Add Disk.
      await aside.getByText(/DG-540/, { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add disk/i }).click();

      const diskDialog = page.getByRole('dialog', { name: /add disks/i });
      await expect(diskDialog).toBeVisible();

      // The dialog should have a Disk ID field and a Type selector.
      const diskIdInput = diskDialog.getByLabel('Disk ID (UUID)');
      await expect(diskIdInput).toBeVisible();

      // Set a known disk ID.
      await diskIdInput.fill(disk540);

      const addDisksBtn = diskDialog.getByRole('button', { name: /add disks/i });
      await addDisksBtn.evaluate((el) => (el as HTMLElement).click());

      // Wait for the dialog to close and refresh to complete.
      await expect(diskDialog).toHaveCount(0, { timeout: 10_000 });

      // Expand the disk-group to see the disk.
      const expandDg540 = aside.getByRole('treeitem').filter({ hasText: /DG-540/ }).locator('button[aria-label="Expand"]');
      if (await expandDg540.count() > 0) await expandDg540.click();

      // The disk should appear in the sidebar (truncated to 12 chars + …).
      await expect(aside.getByText(disk540.slice(0, 12), { exact: false })).toBeVisible({ timeout: 10_000 });

      // Verify via API.
      const diskApi = await apiContext(baseURL!);
      try {
        const r = await diskApi.get(`/api/nodes/${nodeId}/disk-groups/${dg540}/disks`);
        expect(r.ok()).toBeTruthy();
        const disks = await r.json();
        expect(disks.some((d: any) => d.disk_id === disk540)).toBeTruthy();
      } finally {
        await diskApi.dispose();
      }

      // --- disk context menu shows compact/rebuild/scan/recalc/set-status/delete ---

      // Expand disk-group 560 to see its disk.
      const expandDg560 = aside.getByRole('treeitem').filter({ hasText: /DG-560/ }).locator('button[aria-label="Expand"]');
      if (await expandDg560.count() > 0) await expandDg560.click();

      // Right-click the disk.
      const disk560Label = aside.getByText(disk560.slice(0, 12), { exact: false });
      await expect(disk560Label).toBeVisible({ timeout: 5_000 });
      await disk560Label.first().click({ button: 'right' });

      await expect(page.getByRole('menuitem', { name: /compact zones/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /rebuild bitmap/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /trigger consistency scan/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /recalc usage/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /set disk down/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /set disk up/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /delete disk/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Delete Disk Group via context menu removes it (destructive, last) ---

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
      const delApi = await apiContext(baseURL!);
      try {
        const r = await delApi.get(`/api/nodes/${nodeId}/disk-groups`);
        expect(r.ok()).toBeTruthy();
        const dgs = await r.json();
        expect(dgs.some((dg: any) => dg.id === dg570)).toBeFalsy();
      } finally {
        await delApi.dispose();
      }
    } finally {
      // Cleanup.
      await removeDisk(baseURL!, nodeId, dg540, disk540);
      await removeDisk(baseURL!, nodeId, dg560, disk560);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg520);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg530);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg540);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg560);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg570);
    }
  });

  test('disk maintenance operations, set-status, and health badges', async ({ page, baseURL }) => {
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dg545 = 545;
    const dg546 = 546;
    const dg547 = 547;
    const dg548 = 548;
    const dg549 = 549;
    const dg551 = 551;
    const dg552 = 552;
    const dg553 = 553;
    const disk545 = randomDiskId();
    const disk546 = randomDiskId();
    const disk547 = randomDiskId();
    const disk549 = randomDiskId();
    const disk551 = randomDiskId();
    const disk552 = randomDiskId();
    const disk553 = randomDiskId();

    await apiAddDiskGroup(baseURL!, nodeId, dg545, 'test-dg-545');
    await addDisksBatch(baseURL!, nodeId, dg545, [{ disk_id: disk545 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg546, 'test-dg-546');
    await addDisksBatch(baseURL!, nodeId, dg546, [{ disk_id: disk546 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg547, 'test-dg-547');
    await addDisksBatch(baseURL!, nodeId, dg547, [{ disk_id: disk547 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg548, 'test-dg-548');
    await apiAddDiskGroup(baseURL!, nodeId, dg549, 'test-dg-549');
    await addDisksBatch(baseURL!, nodeId, dg549, [{ disk_id: disk549 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg551, 'test-dg-551');
    await addDisksBatch(baseURL!, nodeId, dg551, [{ disk_id: disk551 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg552, 'test-dg-552');
    await addDisksBatch(baseURL!, nodeId, dg552, [{ disk_id: disk552 }]);

    try {
      // --- Mock-based section: the status/recalc/scan/usage endpoints
      // below are intercepted with page.route and fulfilled with canned
      // responses. This verifies the UI handles those response shapes
      // correctly, NOT that the diskdb backend performs the operations.
      // Real-backend coverage of compact/rebuild/scan is a known gap. ---
      // Intercept the disk-group status PUT and fulfill with 204.
      const dgStatusRequest = page.waitForRequest(
        (req) => req.method() === 'PUT' && req.url().includes(`/api/disk-groups/${rackId}/${nodeId}/${dg548}/status`),
        { timeout: 10_000 },
      ).then(async (req) => {
        expect(req.postDataJSON()).toEqual({ status: 'Down' });
      });
      await page.route(`**/api/disk-groups/${rackId}/${nodeId}/${dg548}/status`, (route) => {
        route.fulfill({ status: 204 });
      });

      // Intercept the disk status PUT.
      const diskStatusRequest = page.waitForRequest(
        (req) => req.method() === 'PUT' && req.url().includes(`/api/disks/${encodeURIComponent(disk549)}/status`),
        { timeout: 10_000 },
      ).then(async (req) => {
        expect(req.postDataJSON()).toEqual({ status: 'Down' });
      });
      await page.route(`**/api/disks/${encodeURIComponent(disk549)}/status`, (route) => {
        route.fulfill({ status: 204 });
      });

      // Intercept the recalc POST.
      const recalcRequest = page.waitForRequest(
        (req) => req.method() === 'POST' && req.url().includes('/api/diskdb/recalc'),
        { timeout: 10_000 },
      ).then(async (req) => {
        expect(req.postDataJSON()).toEqual({ dg: dg551 });
      });
      await page.route('**/api/diskdb/recalc', (route) => {
        route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ results: [] }) });
      });

      // Intercept the scan POST.
      const scanRequest = page.waitForRequest(
        (req) => req.method() === 'POST' && req.url().includes('/api/diskdb/scan'),
        { timeout: 10_000 },
      ).then(async (req) => {
        expect(req.postDataJSON()).toEqual({ dg: dg552 });
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

      // --- Compact Zones opens zone select dialog with range input ---

      const expandDg545 = aside.getByRole('treeitem').filter({ hasText: /DG-545/ }).locator('button[aria-label="Expand"]');
      if (await expandDg545.count() > 0) await expandDg545.click();

      // Right-click the disk → Compact Zones.
      const disk545Label = aside.getByText(disk545.slice(0, 12), { exact: false });
      await expect(disk545Label).toBeVisible({ timeout: 5_000 });
      await disk545Label.first().click({ button: 'right' });
      await page.getByRole('menuitem', { name: /compact zones/i }).click();

      const compactDialog = page.getByRole('dialog', { name: /compact zones/i });
      await expect(compactDialog).toBeVisible();
      // Should have a Zones input field.
      await expect(compactDialog.getByLabel(/zones/i)).toBeVisible();
      // Default value should be "all".
      await expect(compactDialog.getByLabel(/zones/i)).toHaveValue('all');
      // Should have a Compact button.
      await expect(compactDialog.getByRole('button', { name: /compact/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Rebuild Bitmap opens zone select dialog ---

      const expandDg546 = aside.getByRole('treeitem').filter({ hasText: /DG-546/ }).locator('button[aria-label="Expand"]');
      if (await expandDg546.count() > 0) await expandDg546.click();

      // Right-click the disk → Rebuild Bitmap.
      const disk546Label = aside.getByText(disk546.slice(0, 12), { exact: false });
      await expect(disk546Label).toBeVisible({ timeout: 5_000 });
      await disk546Label.first().click({ button: 'right' });
      await page.getByRole('menuitem', { name: /rebuild bitmap/i }).click();

      const rebuildDialog = page.getByRole('dialog', { name: /rebuild bitmap/i });
      await expect(rebuildDialog).toBeVisible();
      await expect(rebuildDialog.getByLabel(/zones/i)).toBeVisible();
      await expect(rebuildDialog.getByLabel(/zones/i)).toHaveValue('all');
      await expect(rebuildDialog.getByRole('button', { name: /rebuild/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Compact Zones dialog validates zone input (invalid disables button) ---

      const expandDg547 = aside.getByRole('treeitem').filter({ hasText: /DG-547/ }).locator('button[aria-label="Expand"]');
      if (await expandDg547.count() > 0) await expandDg547.click();

      // Right-click the disk → Compact Zones.
      const disk547Label = aside.getByText(disk547.slice(0, 12), { exact: false });
      await expect(disk547Label).toBeVisible({ timeout: 5_000 });
      await disk547Label.first().click({ button: 'right' });
      await page.getByRole('menuitem', { name: /compact zones/i }).click();

      const validateDialog = page.getByRole('dialog', { name: /compact zones/i });
      await expect(validateDialog).toBeVisible();
      const zoneInput = validateDialog.getByLabel(/zones/i);
      const compactBtn = validateDialog.getByRole('button', { name: /compact/i });

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

      // --- Set Disk Group Down via context menu calls the status API ---

      // Right-click disk-group → Set Disk Group Down.
      await expect(aside.getByText(/DG-548/, { exact: true })).toBeVisible({ timeout: 5_000 });
      await aside.getByText(/DG-548/, { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /set disk group down/i }).click();

      // Verify the API call was made with correct payload.
      await dgStatusRequest;

      // --- Set Disk Down via context menu calls the status API ---

      const expandDg549 = aside.getByRole('treeitem').filter({ hasText: /DG-549/ }).locator('button[aria-label="Expand"]');
      if (await expandDg549.count() > 0) await expandDg549.click();

      // Right-click disk → Set Disk Down.
      const disk549Label = aside.getByText(disk549.slice(0, 12), { exact: false });
      await expect(disk549Label).toBeVisible({ timeout: 5_000 });
      await disk549Label.first().click({ button: 'right' });
      await page.getByRole('menuitem', { name: /set disk down/i }).click();

      // Verify the API call was made with correct payload.
      await diskStatusRequest;

      // --- Recalc Usage via disk context menu calls the recalc API ---

      const expandDg551 = aside.getByRole('treeitem').filter({ hasText: /DG-551/ }).locator('button[aria-label="Expand"]');
      if (await expandDg551.count() > 0) await expandDg551.click();

      // Right-click disk → Recalc Usage.
      const disk551Label = aside.getByText(disk551.slice(0, 12), { exact: false });
      await expect(disk551Label).toBeVisible({ timeout: 5_000 });
      await disk551Label.first().click({ button: 'right' });
      await page.getByRole('menuitem', { name: /recalc usage/i }).click();

      // Verify the API call was made with the disk's dg_id.
      await recalcRequest;

      // --- Trigger Consistency Scan via disk context menu calls the scan API ---

      const expandDg552 = aside.getByRole('treeitem').filter({ hasText: /DG-552/ }).locator('button[aria-label="Expand"]');
      if (await expandDg552.count() > 0) await expandDg552.click();

      // Right-click disk → Trigger Consistency Scan.
      const disk552Label = aside.getByText(disk552.slice(0, 12), { exact: false });
      await expect(disk552Label).toBeVisible({ timeout: 5_000 });
      await disk552Label.first().click({ button: 'right' });
      await page.getByRole('menuitem', { name: /trigger consistency scan/i }).click();

      // Verify the API call was made with the disk's dg_id.
      await scanRequest;

      // --- sidebar shows health badges for disk-group and disk when usage data is available ---

      // The usage API is mocked from here on (page.route below), so this
      // section runs last and reloads the page to pick the mock up.
      await apiAddDiskGroup(baseURL!, nodeId, dg553, 'test-dg-553');
      await addDisksBatch(baseURL!, nodeId, dg553, [{ disk_id: disk553 }]);

      // Mock the usage API to return status data.
      await page.route('**/api/diskdb/usage', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            disk_groups: [{
              rack_id: rackId,
              node_id: nodeId,
              disk_group_id: dg553,
              status: 1, // HW_STATUS_UP → Healthy
              disk_ids: [disk553],
              disks: [{
                rack_id: rackId,
                node_id: nodeId,
                disk_group_id: dg553,
                disk_id: disk553,
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

      const expandRackAgain = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRackAgain.count() > 0) await expandRackAgain.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      const expandNodeAgain = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNodeAgain.count() > 0) await expandNodeAgain.click();

      // The disk-group should show a "Healthy" badge (compact mode:
      // icon with title attribute, no text).
      const dgTreeitem = aside.getByRole('treeitem').filter({ hasText: /DG-553/ });
      await expect(dgTreeitem).toBeVisible({ timeout: 5_000 });
      await expect(dgTreeitem.getByTitle('Healthy')).toBeVisible();

      // Expand disk-group to see the disk.
      const expandDg553 = dgTreeitem.locator('button[aria-label="Expand"]');
      if (await expandDg553.count() > 0) await expandDg553.click();

      // The disk should also show a "Healthy" badge.
      const diskTreeitem = aside.getByRole('treeitem').filter({ hasText: disk553.slice(0, 12) });
      await expect(diskTreeitem).toBeVisible({ timeout: 5_000 });
      await expect(diskTreeitem.getByTitle('Healthy')).toBeVisible();
    } finally {
      // Cleanup.
      await removeDisk(baseURL!, nodeId, dg545, disk545);
      await removeDisk(baseURL!, nodeId, dg546, disk546);
      await removeDisk(baseURL!, nodeId, dg547, disk547);
      await removeDisk(baseURL!, nodeId, dg549, disk549);
      await removeDisk(baseURL!, nodeId, dg551, disk551);
      await removeDisk(baseURL!, nodeId, dg552, disk552);
      await removeDisk(baseURL!, nodeId, dg553, disk553);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg545);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg546);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg547);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg548);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg549);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg551);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg552);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg553);
    }
  });

  test('full deploy flow: deploy diskdb via UI, restart, stop via context menu', async ({ page, baseURL }) => {
    const nodeId = DISKDB_NODE;
    const rpcPort = freePort();

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Capacity' }).click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${DISKDB_RACK}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      // --- deploy diskdb via node context menu (UI Deploy button) ---

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
      }

      // --- restart/stop menu items, then stop via context menu ---

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
    } finally {
      await stopDiskdb(baseURL!, nodeId);
    }
  });
});
