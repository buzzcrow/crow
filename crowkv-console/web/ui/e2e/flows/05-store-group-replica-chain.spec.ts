import { test, expect } from '../fixtures/realBackend';
import { apiContext, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-05 store group replica chain', () => {
  test('creates store and group through the UI against a real deployed server', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r5', 'n5');
    await deployNodeServer(baseURL!, 'n5', 9912, 9922);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      if (await page.getByRole('heading', { name: 'Infrastructure' }).isVisible()) {
        await page.getByRole('button', { name: 'Cluster' }).click();
      }
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });

      await page.locator('aside').getByRole('button', { name: 'Add Store' }).click();
      await expect(page.getByRole('dialog', { name: 'Add Store' })).toBeVisible();
      await page.getByLabel('Store ID (numeric)').fill('57');
      await page.getByLabel('Initial Group ID (numeric)').fill('570');
      await page.getByLabel('First Replica ID (numeric)').fill('5700');
      await page.getByLabel(/^n5/).check();
      await page.getByRole('button', { name: /create store/i }).click();

      await expect(page.getByText(/Store 57 created successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole('treeitem', { name: /Collapse 57 Healthy Open/ })).toBeVisible({ timeout: 15_000 });

      await page.getByRole('treeitem', { name: /Collapse 57 Healthy Open/ }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add group/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
      await page.getByLabel('Group ID (numeric)').fill('580');
      await page.getByLabel('Starting Replica ID (numeric)').fill('5800');
      await page.getByLabel(/^n5/).check();
      await page.getByRole('button', { name: /create group/i }).click();

      await expect(page.getByText(/Group 580 created successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole('treeitem', { name: /580/ }).first()).toBeVisible({ timeout: 15_000 });

      const stores = await api.get('/api/stores');
      expect(stores.ok(), await stores.text()).toBeTruthy();
      expect(await stores.json()).toEqual(expect.arrayContaining([expect.objectContaining({ store_id: 57 })]));

      const groups = await api.get('/api/stores/57/groups');
      expect(groups.ok(), await groups.text()).toBeTruthy();
      expect(await groups.json()).toEqual(expect.arrayContaining([expect.objectContaining({ group_id: 570 }), expect.objectContaining({ group_id: 580 })]));
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 'n5');
    }
  });
});
