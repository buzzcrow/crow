import { test, expect } from '../fixtures/realBackend';
import { seedRackAndNode } from '../fixtures/consoleSetup';

test.describe('E2E-15 favorites and recent', () => {
  test('adds a selected node to favorites and records it as recent', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r15', 'n15');

    await page.goto('/');
    await page.getByRole('button', { name: 'Infrastructure' }).click();
    await expect(page.getByRole('treeitem').filter({ hasText: 'n15' })).toBeVisible({ timeout: 15_000 });
    await page.getByRole('treeitem').filter({ hasText: 'n15' }).click();

    const inspector = page.locator('aside[aria-label="Entity inspector"]');
    await expect(inspector.locator('div').filter({ hasText: /^Node$/ })).toBeVisible({ timeout: 15_000 });
    await inspector.getByRole('button', { name: 'Add to favorites' }).click();

    await expect(page.locator('aside').getByText('Favorites')).toBeVisible();
    await expect(page.locator('aside').getByText('Recent')).toBeVisible();
    await expect(page.locator('aside').getByText('n15').first()).toBeVisible();
    await expect(page.locator('aside').getByText('No favorites yet')).toHaveCount(0);
    await expect(page.locator('aside').getByText('No recent items')).toHaveCount(0);
  });
});
