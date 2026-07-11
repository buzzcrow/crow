import { test, expect } from '../fixtures/realBackend';
import { deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-09 KV put/get', () => {
  test('puts and gets a key through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r9', 'n9');
    await deployNodeServer(baseURL!, 'n9', 9919, 9929);

    try {
      await page.goto('/');
      await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });
      await page.getByRole('treeitem', { name: /Collapse 1/ }).nth(1).click();

      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector.locator('div').filter({ hasText: /^Group$/ })).toBeVisible({ timeout: 15_000 });
      await inspector.getByRole('tab', { name: 'KV' }).click();

      await inspector.getByRole('button', { name: 'Put' }).click();
      await inspector.getByPlaceholder('Key').fill('e2e-key-9');
      await inspector.getByPlaceholder('Value').fill('e2e-value-9');
      const putResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/put'));
      await inspector.getByRole('button', { name: 'Put' }).last().click();
      const putResponse = await putResponsePromise;
      expect(putResponse.ok(), await putResponse.text()).toBeTruthy();
      await expect(page.getByText(/Set value for "e2e-key-9"/)).toBeVisible({ timeout: 30_000 });

      await inspector.getByRole('button', { name: 'Get' }).click();
      await inspector.getByPlaceholder('Key to get').fill('e2e-key-9');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await inspector.getByRole('button', { name: 'Get' }).last().click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(inspector.getByText('Key found')).toBeVisible({ timeout: 30_000 });
      await expect(inspector.getByText('e2e-value-9')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n9');
    }
  });
});
