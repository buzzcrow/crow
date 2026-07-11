import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-17 add replica', () => {
  test('adds a replica to an existing group through the UI', async ({ page, baseURL }) => {
    // Setup: two racks/nodes with deployed servers.
    await seedRackAndNode(baseURL!, 'r17a', 'n17a');
    await seedRackAndNode(baseURL!, 'r17b', 'n17b');
    await deployNodeServer(baseURL!, 'n17a', 9948, 9958);
    await deployNodeServer(baseURL!, 'n17b', 9949, 9959);

    // Seed a store with an initial group on n17a.
    await createStore(baseURL!, 177, 1770, 17700, ['n17a']);
    await addGroup(baseURL!, 177, 1770, 17700, ['n17a']);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.locator('aside').first();
      await expect(aside.getByText('G-1770')).toBeVisible({ timeout: 15_000 });

      // Right-click selects + targets the group (without toggling its expand,
      // so the existing replica row stays visible).
      await aside.getByText('G-1770').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();

      await expect(page.getByRole('dialog', { name: 'Add Replica' })).toBeVisible();
      await page.getByLabel('Node', { exact: true }).selectOption('n17b');
      await page.getByRole('button', { name: /add replica/i }).click();

      await expect(page.getByText(/Replica added to node "n17b" successfully/)).toBeVisible({ timeout: 30_000 });

      // Verify the new replica appears in the logical tree.
      await expect(aside.getByText('LR-17701')).toBeVisible({ timeout: 15_000 });

      // Verify backend: two replicas in the group.
      const response = await api.get('/api/stores/177/groups/1770/replicas');
      expect(response.ok(), await response.text()).toBeTruthy();
      const replicas = await response.json();
      expect(replicas).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ replica_id: 17700, node_id: 'n17a' }),
          expect.objectContaining({ replica_id: 17701, node_id: 'n17b' }),
        ]),
      );
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 'n17a');
      await stopNodeServer(baseURL!, 'n17b');
    }
  });
});
