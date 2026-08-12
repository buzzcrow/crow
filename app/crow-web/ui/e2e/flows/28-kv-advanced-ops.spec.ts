// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.4s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

async function openKvPanel(page: any, storeId: string, groupId: string) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV' }).click();
  await page.getByLabel('Store').selectOption(storeId);
  await page.getByLabel('Group').selectOption(groupId);
}

async function putKey(page: any, key: string, value: string) {
  await page.getByLabel('Put key').fill(key);
  await page.getByLabel('Put value').fill(value);
  const responsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
  await page.getByRole('button', { name: /^Put$/ }).click();
  const response = await responsePromise;
  expect(response.ok(), await response.text()).toBeTruthy();
}

async function scanAndRefresh(page: any) {
  const scanResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
  await page.getByRole('button', { name: /^Scan$/ }).click();
  await scanResponse;
  await expect(page.getByTestId('kv-scan-table')).toBeVisible({ timeout: 3_000 });
}

test.describe('E2E-28 KV advanced operations', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('delete prefix, delete selected, inline delete, copy', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 28, 28);
    await deployNodeServer(baseURL!, 28, 9928, 9938);
    await createStore(baseURL!, 280, [28]);
    await addGroup(baseURL!, 280, 2800, 28000, [28]);
    await waitForLeader(baseURL!, 280, 2800);

    try {
      await openKvPanel(page, '280', '2800');

      // Put keys with a common prefix
      await putKey(page, 'adv-a-1', 'val1');
      await putKey(page, 'adv-a-2', 'val2');
      await putKey(page, 'adv-b-1', 'val3');

      // Scan to see all keys
      await scanAndRefresh(page);
      await expect(page.getByTestId('kv-scan-table').getByText('adv-a-1')).toBeVisible({ timeout: 3_000 });

      // Delete Prefix: delete all keys starting with "adv-a-"
      await page.getByLabel('Delete key').fill('adv-a-');
      await page.getByRole('button', { name: /delete prefix/i }).click();
      // Confirm dialog appears
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible({ timeout: 3_000 });
      const deletePrefixResponse = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      await deletePrefixResponse;
      // Wait for the component's automatic re-scan (setTimeout 100ms) to settle
      await page.waitForTimeout(500);

      // Scan again — adv-a-* keys should be gone
      await scanAndRefresh(page);
      await expect(page.getByTestId('kv-scan-table').getByText('adv-b-1')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('adv-a-1')).toHaveCount(0);
      await expect(page.getByTestId('kv-scan-table').getByText('adv-a-2')).toHaveCount(0);

      // Delete Selected: check the checkbox for adv-b-1, then delete
      const row = page.getByTestId('kv-scan-table').locator('tr').filter({ hasText: 'adv-b-1' });
      await row.locator('input[type="checkbox"]').check();
      await expect(page.getByRole('button', { name: /delete selected/i })).toBeEnabled({ timeout: 3_000 });
      await page.getByRole('button', { name: /delete selected/i }).click();
      const confirmDialog = page.getByRole('dialog');
      await expect(confirmDialog).toBeVisible({ timeout: 3_000 });
      const deleteSelectedResponse = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await confirmDialog.getByRole('button', { name: 'Delete' }).click();
      await deleteSelectedResponse;

      // Scan — adv-b-1 should be gone
      await scanAndRefresh(page);
      await expect(page.getByTestId('kv-scan-table').getByText('adv-b-1')).toHaveCount(0);

      // Inline delete: put a new key, click inline delete, confirm dialog
      await putKey(page, 'adv-inline', 'val-inline');
      await scanAndRefresh(page);
      await expect(page.getByTestId('kv-scan-table').getByText('adv-inline')).toBeVisible({ timeout: 3_000 });
      await page.getByTestId('inline-delete-adv-inline').click();
      const inlineDialog = page.getByRole('dialog');
      await expect(inlineDialog).toBeVisible({ timeout: 3_000 });
      const inlineDeleteResponse = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await inlineDialog.getByRole('button', { name: 'Delete' }).click();
      await inlineDeleteResponse;

      // Copy: put a key, get it, verify copy button exists
      await putKey(page, 'adv-copy', 'copy-val');
      await page.getByLabel('Get key').fill('adv-copy');
      const getResponse = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      await getResponse;
      await expect(page.getByTestId('kv-get-result')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-copy-value')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 28);
    }
  });
});
