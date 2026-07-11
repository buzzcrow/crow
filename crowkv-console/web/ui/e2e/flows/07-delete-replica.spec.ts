import { test, expect } from '../fixtures/realBackend';
import { apiContext, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-07 delete replica', () => {
  test('deletes a replica through the UI and verifies the real backend', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r7', 'n7');
    await deployNodeServer(baseURL!, 'n7', 9917, 9927);
    await createStore(baseURL!, 77, 770, 7700, ['n7']);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });
      await expect(page.getByRole('treeitem', { name: /7700/ })).toBeVisible({ timeout: 15_000 });

      await page.getByRole('treeitem', { name: /7700/ }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete replica/i }).click();
      await expect(page.getByRole('dialog', { name: 'Delete Replica' })).toBeVisible();
      await page.getByRole('button', { name: /delete replica/i }).click();

      await expect(page.getByText(/Replica "7700" deleted successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole('treeitem', { name: /7700/ })).toHaveCount(0, { timeout: 15_000 });

      const response = await api.get('/api/stores/77/groups/770/replicas');
      if (response.status() === 404) {
        expect(await response.text()).toContain('group 770 in store 77 not found');
      } else {
        expect(response.ok(), await response.text()).toBeTruthy();
        expect(await response.json()).not.toEqual(expect.arrayContaining([expect.objectContaining({ replica_id: 7700 })]));
      }
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 'n7');
    }
  });
});
