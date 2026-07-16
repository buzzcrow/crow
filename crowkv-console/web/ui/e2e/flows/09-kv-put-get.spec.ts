// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

test.describe('E2E-09 KV put/get', () => {
  test('puts and gets a key through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r9', 'n9');
    await deployNodeServer(baseURL!, 'n9', 9919, 9929);
    await createStore(baseURL!, 99, ['n9']);
    await addGroup(baseURL!, 99, 990, 9900, ['n9']);
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
      await expect(page.getByRole('alert').getByText(/Key written: "e2e-key-9"/)).toBeVisible({ timeout: 3_000 });

      // Get
      await page.getByLabel('Get key').fill('e2e-key-9');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(page.getByTestId('kv-get-result')).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n9');
    }
  });
});
