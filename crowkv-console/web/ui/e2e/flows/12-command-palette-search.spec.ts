import { test, expect } from '../fixtures/realBackend';
import { seedRackAndNode } from '../fixtures/consoleSetup';

test.describe('E2E-12 command palette search', () => {
  test('searches real entities in the command palette', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r12', 'n12');

    await page.goto('/');
    await expect(page.getByRole('button', { name: 'Open command palette' })).toBeVisible({ timeout: 15_000 });
    await page.getByRole('button', { name: 'Open command palette' }).click();

    const palette = page.getByRole('dialog', { name: 'Command palette' });
    await expect(palette).toBeVisible();
    await page.getByLabel('Command palette search').fill('n12');

    const results = page.getByRole('listbox', { name: 'Command palette results' });
    await expect(results.getByRole('option', { name: /n12/ })).toBeVisible({ timeout: 15_000 });
  });
});
