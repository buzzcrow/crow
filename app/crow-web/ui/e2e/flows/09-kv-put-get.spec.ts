// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

test.describe('E2E-09 KV put/get', () => {
  test('puts and gets a key through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 9, 9);
    await deployNodeServer(baseURL!, 9, freePort(), freePort());
    await createStore(baseURL!, 99, [9]);
    await addGroup(baseURL!, 99, 990, 9900, [9]);
    await waitForLeader(baseURL!, 99, 990);

    try {
      await page.goto('/');
      await page.locator('header').getByRole('button', { name: 'KV' }).click();

      // Select store 99 and group 990
      await page.getByLabel('Store').selectOption('99');
      await page.getByLabel('Group').selectOption('990');

      // Put
      await page.getByLabel('Put key').fill('e2e-key-9');
      await page.getByLabel('Put value').fill('e2e-value-9');
      const putResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/put'));
      await page.getByRole('button', { name: /^Put$/ }).click();
      const putResponse = await putResponsePromise;
      expect(putResponse.ok(), await putResponse.text()).toBeTruthy();

      // Get
      await page.getByLabel('Get key').fill('e2e-key-9');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(page.getByTestId('kv-get-result')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-get-result')).toHaveText('e2e-value-9');

      // Overwrite: put same key with new value
      await page.getByLabel('Put key').fill('e2e-key-9');
      await page.getByLabel('Put value').fill('e2e-value-9-v2');
      const overwriteResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/put'));
      await page.getByRole('button', { name: /^Put$/ }).click();
      const overwriteResponse = await overwriteResponsePromise;
      expect(overwriteResponse.ok(), await overwriteResponse.text()).toBeTruthy();

      // Get again — should return new value
      await page.getByLabel('Get key').fill('e2e-key-9');
      const getResponse2Promise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse2 = await getResponse2Promise;
      expect(getResponse2.ok(), await getResponse2.text()).toBeTruthy();
      await expect(page.getByTestId('kv-get-result')).toHaveText('e2e-value-9-v2', { timeout: 3_000 });

      // Verify revision incremented (rev: 2 should be visible)
      await expect(page.getByText(/rev: 2/)).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 9);
    }
  });
});
