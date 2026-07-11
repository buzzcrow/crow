import { test, expect } from '../fixtures/realBackend';
import { createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-06 cross jump', () => {
  test('jumps from logical replica details to the hosting physical node', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r6', 'n6');
    await deployNodeServer(baseURL!, 'n6', 9916, 9926);
    await createStore(baseURL!, 66, 660, 6600, ['n6']);

    try {
      await page.goto('/');
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });
      await page.getByRole('treeitem', { name: /6600/ }).click();

      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 15_000 });
      await expect(inspector.locator('div').filter({ hasText: /^Replica$/ })).toBeVisible();

      await inspector.locator('div').filter({ hasText: /^Parent: node_id/ }).getByText('n6').click();

      await expect(page.getByRole('heading', { name: 'Infrastructure' })).toBeVisible({ timeout: 15_000 });
      await expect(page.locator('aside[aria-label="Entity inspector"]').locator('div').filter({ hasText: /^Node$/ })).toBeVisible({ timeout: 15_000 });
      await expect(page.locator('aside[aria-label="Entity inspector"]').getByText('n6')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n6');
    }
  });
});
