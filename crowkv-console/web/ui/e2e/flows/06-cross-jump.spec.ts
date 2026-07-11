import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-06 cross jump', () => {
  test('jumps from logical replica details to the hosting physical node', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r6', 'n6');
    await deployNodeServer(baseURL!, 'n6', 9916, 9926);
    await createStore(baseURL!, 66, 660, 6600, ['n6']);
    await addGroup(baseURL!, 66, 660, 6600, ['n6']);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.locator('aside').first();
      await expect(aside.getByText('LR-6600')).toBeVisible({ timeout: 15_000 });
      await aside.getByText('LR-6600').click();

      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 15_000 });

      // Single cross-jump button: logical Replica -> hosting physical Node.
      await inspector.getByRole('button', { name: /Show on node n6/ }).click();

      await expect(page.getByRole('heading', { name: 'Infrastructure' })).toBeVisible({ timeout: 15_000 });
      await expect(inspector.getByText('N-n6', { exact: true })).toBeVisible({ timeout: 15_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n6');
    }
  });
});
