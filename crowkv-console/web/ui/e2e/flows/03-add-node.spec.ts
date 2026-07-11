import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack } from '../fixtures/consoleSetup';

test.describe('E2E-03 add node', () => {
  test('creates a node through the rack context menu and verifies the real backend', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r3', name: 'Rack Three' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Infrastructure' }).click();
    await expect(page.getByText('Rack Three').first()).toBeVisible({ timeout: 15_000 });

    await page.getByRole('treeitem').filter({ hasText: 'Rack Three' }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();

    await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
    await expect(page.getByRole('button', { name: /create node/i })).toBeDisabled();
    await page.getByLabel('Rack', { exact: true }).selectOption('r3');
    await page.getByLabel('Node ID').fill('n3');
    await page.getByLabel('Host').fill('127.0.0.1');
    await expect(page.getByRole('button', { name: /create node/i })).toBeEnabled();
    await page.getByRole('button', { name: /create node/i }).click();

    await expect(page.getByText(/Node "n3" created successfully/)).toBeVisible({ timeout: 15_000 });
    await expect(page.getByRole('treeitem').filter({ hasText: 'n3' })).toBeVisible({ timeout: 15_000 });

    const api = await apiContext(baseURL!);
    try {
      const response = await api.get('/api/nodes');
      expect(response.ok(), await response.text()).toBeTruthy();
      const nodes = await response.json();
      expect(nodes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 'n3', rack_id: 'r3', host: '127.0.0.1' }),
        ]),
      );
    } finally {
      await api.dispose();
    }
  });
});
