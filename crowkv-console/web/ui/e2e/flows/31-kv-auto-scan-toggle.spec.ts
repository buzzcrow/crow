// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.7s (2026-07-16)

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

test.describe('E2E-31 KV auto-scan toggle', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('auto-scan off does not refresh, on does', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r31', 'n31');
    await deployNodeServer(baseURL!, 'n31', 9931, 9941);
    await createStore(baseURL!, 310, ['n31']);
    await addGroup(baseURL!, 310, 3100, 31000, ['n31']);
    await waitForLeader(baseURL!, 310, 3100);

    try {
      await openKvPanel(page, '310', '3100');

      // Put an initial key and scan
      await putKey(page, 'auto-key-1', 'val-1');
      const scanResp = page.waitForResponse((r) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /scan/i }).click();
      await scanResp;
      await expect(page.getByTestId('kv-scan-table').getByText('auto-key-1')).toBeVisible({ timeout: 3_000 });

      // Turn auto-scan off
      await page.getByLabel('auto-scan').uncheck();

      // Put another key — scan table should NOT auto-refresh
      await putKey(page, 'auto-key-2', 'val-2');
      // auto-scan is off, so auto-key-2 should never appear in the table.
      await expect(page.getByTestId('kv-scan-table').getByText('auto-key-2')).toHaveCount(0, { timeout: 1_000 });

      // Turn auto-scan back on
      await page.getByLabel('auto-scan').check();

      // Put another key — scan table should auto-refresh
      await putKey(page, 'auto-key-3', 'val-3');
      await expect(page.getByTestId('kv-scan-table').getByText('auto-key-3')).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n31');
    }
  });
});
