// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

async function openKvPanel(page: any) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV' }).click();
  await page.getByLabel('Store').selectOption('110');
  await page.getByLabel('Group').selectOption('1100');
}

async function putKey(page: any, key: string, value: string) {
  await page.getByLabel('Put key').fill(key);
  await page.getByLabel('Put value').fill(value);
  const responsePromise = page.waitForResponse((response: any) => response.url().includes('/kv/put'));
  await page.getByRole('button', { name: /^Put$/ }).click();
  const response = await responsePromise;
  expect(response.ok(), await response.text()).toBeTruthy();
}

test.describe('E2E-10 KV scan', () => {
  test('scans keys through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r10', 'n10');
    await deployNodeServer(baseURL!, 'n10', 9930, 9940);
    await createStore(baseURL!, 110, ['n10']);
    await addGroup(baseURL!, 110, 1100, 11000, ['n10']);
    await waitForLeader(baseURL!, 110, 1100);

    try {
      await openKvPanel(page);

      // Turn off auto-scan first to prevent stale auto-scan from overriding prefix scan results
      await page.getByLabel('auto-scan').uncheck();

      await putKey(page, 'scan-10-a', 'value-a');
      await putKey(page, 'scan-10-b', 'value-b');
      await putKey(page, 'other-10-c', 'value-c');

      // Scan with prefix "scan-10-" — should only return matching keys
      await page.getByLabel('Scan prefix').fill('scan-10-');
      const responsePromise = page.waitForResponse((response) => response.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /^Scan$/ }).click();
      const response = await responsePromise;
      expect(response.ok(), await response.text()).toBeTruthy();

      const scanTable = page.getByTestId('kv-scan-table');
      await expect(scanTable.getByText('scan-10-a')).toBeVisible({ timeout: 3_000 });
      await expect(scanTable.getByText('value-a')).toBeVisible();
      await expect(scanTable.getByText('scan-10-b')).toBeVisible();
      await expect(scanTable.getByText('value-b')).toBeVisible();

      // Prefix filter: "other-" keys should NOT appear
      await expect(scanTable.getByText('other-10-c')).toHaveCount(0, { timeout: 3_000 });
      await expect(scanTable.getByText('value-c')).toHaveCount(0, { timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n10');
    }
  });
});
