import { test, expect } from '../fixtures/realBackend';
import { deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-14 breadcrumb', () => {
  test('updates breadcrumbs for a selected logical replica', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r14', 'n14');
    await deployNodeServer(baseURL!, 'n14', 9934, 9944);

    try {
      await page.goto('/');
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });
      await page.getByRole('treeitem', { name: /1 running/ }).click();

      const breadcrumb = page.locator('header nav ol');
      await expect(breadcrumb.getByText('Cluster')).toBeVisible();
      await expect(breadcrumb.getByText('1').first()).toBeVisible();
      await expect(breadcrumb.getByText('Replica')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n14');
    }
  });
});
