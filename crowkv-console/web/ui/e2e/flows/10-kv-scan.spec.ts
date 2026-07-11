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

test.describe('E2E-10 KV scan', () => {
  test('scans keys through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r10', 'n10');
    await deployNodeServer(baseURL!, 'n10', 9930, 9940);

    try {
      const inspector = await openKvTab(page);
      await putKey(inspector, page, 'scan-10-a', 'value-a');
      await putKey(inspector, page, 'scan-10-b', 'value-b');

      await inspector.getByRole('button', { name: 'Scan' }).first().click();
      await inspector.getByPlaceholder('Key prefix (leave empty for all keys)').fill('scan-10-');
      const responsePromise = page.waitForResponse((response) => response.url().includes('/kv/scan'));
      await inspector.getByRole('button', { name: 'Scan' }).last().click();
      const response = await responsePromise;
      expect(response.ok(), await response.text()).toBeTruthy();

      await expect(inspector.getByText('scan-10-a')).toBeVisible({ timeout: 30_000 });
      await expect(inspector.getByText('value-a')).toBeVisible();
      await expect(inspector.getByText('scan-10-b')).toBeVisible();
      await expect(inspector.getByText('value-b')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n10');
    }
  });
});
