// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

test.describe('E2E-09 KV put/get', () => {
  test('puts and gets a key through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r9', 'n9');
    await deployNodeServer(baseURL!, 'n9', 9919, 9929);
    await createStore(baseURL!, 99, 990, 9900, ['n9']);
    await addGroup(baseURL!, 99, 990, 9900, ['n9']);
    await waitForLeader(baseURL!, 99, 990);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.locator('aside').first();
      await expect(aside.getByText('G-990', { exact: true })).toBeVisible({ timeout: 20_000 });
      await aside.getByText('G-990', { exact: true }).click();

      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await inspector.getByRole('tab', { name: 'KV' }).click();

      await inspector.getByRole('button', { name: 'Put' }).first().click();
      await inspector.getByPlaceholder('Key').fill('e2e-key-9');
      await inspector.getByPlaceholder('Value').fill('e2e-value-9');
      const putResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/put'));
      await inspector.getByRole('button', { name: 'Put' }).last().click();
      const putResponse = await putResponsePromise;
      expect(putResponse.ok(), await putResponse.text()).toBeTruthy();
      await expect(page.getByText(/Key written: "e2e-key-9"/)).toBeVisible({ timeout: 30_000 });

      await inspector.getByRole('button', { name: 'Get' }).first().click();
      await inspector.getByPlaceholder('Key to get').fill('e2e-key-9');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await inspector.getByRole('button', { name: 'Get' }).last().click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(inspector.getByText('e2e-value-9')).toBeVisible({ timeout: 30_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n9');
    }
  });
});
