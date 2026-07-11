import { test, expect } from '../fixtures/realBackend';
import { deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

async function openKvTab(page: any) {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 15_000 });
  await page.getByRole('treeitem', { name: /Collapse 1/ }).nth(1).click();
  const inspector = page.locator('aside[aria-label="Entity inspector"]');
  await expect(inspector.locator('div').filter({ hasText: /^Group$/ })).toBeVisible({ timeout: 15_000 });
  await inspector.getByRole('tab', { name: 'KV' }).click();
  return inspector;
}

async function putKey(inspector: any, page: any, key: string, value: string) {
  await inspector.getByRole('button', { name: 'Put' }).first().click();
  await inspector.getByPlaceholder('Key').fill(key);
  await inspector.getByPlaceholder('Value').fill(value);
  const responsePromise = page.waitForResponse((response: any) => response.url().includes('/kv/put'));
  await inspector.getByRole('button', { name: 'Put' }).last().click();
  const response = await responsePromise;
  expect(response.ok(), await response.text()).toBeTruthy();
}

test.describe('E2E-11 KV delete', () => {
  test('deletes a key through the real KV UI and verifies it is gone', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r11', 'n11');
    await deployNodeServer(baseURL!, 'n11', 9931, 9941);

    try {
      const inspector = await openKvTab(page);
      await putKey(inspector, page, 'delete-11-key', 'delete-11-value');

      await inspector.getByRole('button', { name: 'Delete' }).first().click();
      await inspector.getByPlaceholder('Key to delete').fill('delete-11-key');
      const deleteResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/delete'));
      await inspector.getByRole('button', { name: 'Delete' }).last().click();
      const deleteResponse = await deleteResponsePromise;
      expect(deleteResponse.ok(), await deleteResponse.text()).toBeTruthy();
      await expect(page.getByText(/Deleted "delete-11-key"/)).toBeVisible({ timeout: 30_000 });

      await inspector.getByRole('button', { name: 'Get' }).first().click();
      await inspector.getByPlaceholder('Key to get').fill('delete-11-key');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await inspector.getByRole('button', { name: 'Get' }).last().click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(inspector.getByText('Key not found')).toBeVisible({ timeout: 30_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n11');
    }
  });
});
