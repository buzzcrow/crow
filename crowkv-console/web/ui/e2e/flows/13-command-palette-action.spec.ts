import { test, expect } from '../fixtures/realBackend';

test.describe('E2E-13 command palette action', () => {
  test('runs the toggle-view action from the command palette', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });

    await page.getByRole('button', { name: 'Open command palette' }).click();
    const palette = page.getByRole('dialog', { name: 'Command palette' });
    await expect(palette).toBeVisible();
    await page.getByLabel('Command palette search').fill('toggle view');
    await page.getByRole('option', { name: /Toggle view/ }).click();

    await expect(page.getByRole('heading', { name: 'Infrastructure' })).toBeVisible({ timeout: 15_000 });
    await expect(palette).toHaveCount(0);
  });
});
