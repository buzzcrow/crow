import { test, expect } from '../fixtures/realBackend';
import { apiContext, DEFAULT_SERVER_BINARY, deployNodeServer, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-18 full chain', () => {
  test('creates rack, node, server, store, group, and replica entirely through the UI', async ({ page, baseURL }) => {
    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await expect(page.getByRole('button', { name: 'Infrastructure' })).toBeVisible({ timeout: 15_000 });
      await page.getByRole('button', { name: 'Infrastructure' }).click();
      await expect(page.getByRole('heading', { name: 'Infrastructure' })).toBeVisible({ timeout: 15_000 });

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
      await page.getByRole('button', { name: /create node/i }).click();
      await expect(page.getByText(/Node "n18a" created successfully/)).toBeVisible({ timeout: 15_000 });

      // 3. Add node n18b to r18 via rack context menu.
      await page.getByRole('treeitem').filter({ hasText: 'Rack Eighteen' }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add node/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
      await page.getByLabel('Rack', { exact: true }).selectOption('r18');
      await page.getByLabel('Node ID').fill('n18b');
      await page.getByLabel('Host').fill('127.0.0.1');
      await page.getByRole('button', { name: /create node/i }).click();
      await expect(page.getByText(/Node "n18b" created successfully/)).toBeVisible({ timeout: 15_000 });

      // 4. Deploy server on n18a.
      await page.getByRole('treeitem').filter({ hasText: 'n18a' }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /deploy server/i }).click();
      await expect(page.getByRole('dialog', { name: /deploy server on n18a/i })).toBeVisible();
      await page.getByLabel('Management Port').fill('9933');
      await page.getByLabel('gRPC Port').fill('9943');
      await page.getByLabel('Binary Path (optional)').fill(DEFAULT_SERVER_BINARY);
      await page.getByRole('button', { name: 'Deploy' }).click();
      await expect(page.getByText(/Server deployed on n18a/)).toBeVisible({ timeout: 30_000 });

      // 5. Deploy server on n18b.
      await page.getByRole('treeitem').filter({ hasText: 'n18b' }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /deploy server/i }).click();
      await expect(page.getByRole('dialog', { name: /deploy server on n18b/i })).toBeVisible();
      await page.getByLabel('Management Port').fill('9934');
      await page.getByLabel('gRPC Port').fill('9944');
      await page.getByLabel('Binary Path (optional)').fill(DEFAULT_SERVER_BINARY);
      await page.getByRole('button', { name: 'Deploy' }).click();
      await expect(page.getByText(/Server deployed on n18b/)).toBeVisible({ timeout: 30_000 });

      // Switch to Cluster view.
      if (await page.getByRole('heading', { name: 'Infrastructure' }).isVisible()) {
        await page.getByRole('button', { name: 'Cluster' }).click();
      }
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });

      // 6. Create store 188 with initial group 1880 on n18a.
      await page.locator('aside').getByRole('button', { name: 'Add Store' }).click();
      await expect(page.getByRole('dialog', { name: 'Add Store' })).toBeVisible();
      await page.getByLabel('Store ID (numeric)').fill('188');
      await page.getByLabel('Initial Group ID (numeric)').fill('1880');
      await page.getByLabel('First Replica ID (numeric)').fill('18800');
      await page.getByLabel(/^n18a/).check();
      await page.getByRole('button', { name: /create store/i }).click();
      await expect(page.getByText(/Store 188 created successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole('treeitem', { name: /Collapse 188 Healthy Open/ })).toBeVisible({ timeout: 15_000 });

      // 7. Add replica to group 1880 on n18b via UI.
      const group = page.getByRole('treeitem', { name: /Collapse 1880/ });
      await expect(group).toBeVisible({ timeout: 15_000 });
      await group.click();
      await group.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Replica' })).toBeVisible();
      await page.getByLabel('Node', { exact: true }).selectOption('n18b');
      await page.getByRole('button', { name: /add replica/i }).click();
      await expect(page.getByText(/Replica added to node "n18b" successfully/)).toBeVisible({ timeout: 30_000 });

      // Verify both replicas exist.
      await expect(page.getByRole('treeitem', { name: /18800/ }).first()).toBeVisible({ timeout: 15_000 });
      await expect(page.getByRole('treeitem', { name: /18801/ }).first()).toBeVisible({ timeout: 15_000 });

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
