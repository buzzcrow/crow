import { test, expect } from '../fixtures/realBackend';

test.describe('E2E-01 fresh registry', () => {
  test('renders the SPA shell against a real empty backend', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (message) => {
      if (message.type() === 'error') {
        consoleErrors.push(message.text());
      }
    });

    await page.goto('/');

    await expect(page.getByRole('button', { name: 'Infrastructure' })).toBeVisible({ timeout: 15_000 });
    await expect(page.getByRole('button', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });
    await expect(page.getByPlaceholder('Search...')).toBeVisible();
    await expect(page.getByLabel('Search topology')).toBeVisible();
    await expect(page.locator('.react-flow')).toBeVisible();

    const healthText = page.getByText(/healthy|degraded|failed|unknown/i).first();
    await expect(healthText).toBeVisible({ timeout: 15_000 });

    expect(consoleErrors).toEqual([]);
  });
});
