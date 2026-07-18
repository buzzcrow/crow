// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

async function openKvPanel(page: any, storeId: string) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV' }).click();
  await page.getByLabel('Store').selectOption(storeId);
}

async function putKey(page: any, key: string, value: string) {
  await page.getByLabel('Put key').fill(key);
  await page.getByLabel('Put value').fill(value);
  const responsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
  await page.getByRole('button', { name: /^Put$/ }).click();
  const response = await responsePromise;
  expect(response.ok(), await response.text()).toBeTruthy();
}

test.describe('E2E-30 KV all groups mode', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('all groups mode aggregates scan and disables get', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r30', 'n30');
    await deployNodeServer(baseURL!, 'n30', 9930, 9940);
    await createStore(baseURL!, 300, ['n30']);
    await addGroup(baseURL!, 300, 3000, 30000, ['n30']);
    await waitForLeader(baseURL!, 300, 3000);
    await addGroup(baseURL!, 300, 3001, 30010, ['n30']);
    await waitForLeader(baseURL!, 300, 3001);

    try {
      await openKvPanel(page, '300');

      // Put keys in each group
      await page.getByLabel('Group').selectOption('3000');
      await putKey(page, 'all-groups-key-0', 'val-0');

      await page.getByLabel('Group').selectOption('3001');
      await putKey(page, 'all-groups-key-1', 'val-1');

      // Switch to All Groups
      await page.getByLabel('Group').selectOption('All Groups');

      // Get should be disabled in All Groups mode
      await expect(page.getByRole('button', { name: /^Get$/ })).toBeDisabled();

      // Scan should aggregate keys from both groups
      const scanResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /scan/i }).click();
      await scanResponse;
      await expect(page.getByTestId('kv-scan-table')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('all-groups-key-0')).toBeVisible({ timeout: 10_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('all-groups-key-1')).toBeVisible({ timeout: 10_000 });

      // Group column should be visible in All Groups mode
      await expect(page.getByTestId('kv-scan-table').locator('th').filter({ hasText: 'Group' })).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n30');
    }
  });
});
