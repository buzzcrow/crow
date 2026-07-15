// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

async function openKvPanel(page: any) {
  await page.goto('/');
  await page.getByRole('button', { name: 'KV' }).click();
  await page.getByLabel('Store').selectOption('110');
  await page.getByLabel('Group').selectOption('1100');
}

async function putKey(page: any, key: string, value: string) {
  await page.getByPlaceholder('Key').fill(key);
  await page.getByPlaceholder('Value').fill(value);
  const responsePromise = page.waitForResponse((response: any) => response.url().includes('/kv/put'));
  await page.getByRole('button', { name: /^Put$/ }).click();
  const response = await responsePromise;
  expect(response.ok(), await response.text()).toBeTruthy();
}

test.describe('E2E-10 KV scan', () => {
  test('scans keys through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r10', 'n10');
    await deployNodeServer(baseURL!, 'n10', 9930, 9940);
    await createStore(baseURL!, 110, 1100, 11000, ['n10']);
    await addGroup(baseURL!, 110, 1100, 11000, ['n10']);
    await waitForLeader(baseURL!, 110, 1100);

    try {
      await openKvPanel(page);
      await putKey(page, 'scan-10-a', 'value-a');
      await putKey(page, 'scan-10-b', 'value-b');

      await page.getByPlaceholder('Key prefix (empty = all)').fill('scan-10-');
      const responsePromise = page.waitForResponse((response) => response.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /^Scan$/ }).click();
      const response = await responsePromise;
      expect(response.ok(), await response.text()).toBeTruthy();

      await expect(page.getByText('scan-10-a')).toBeVisible({ timeout: 30_000 });
      await expect(page.getByText('value-a')).toBeVisible();
      await expect(page.getByText('scan-10-b')).toBeVisible();
      await expect(page.getByText('value-b')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n10');
    }
  });
});
