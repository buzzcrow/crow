// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

async function openKvPanel(page: any) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV' }).click();
  await page.getByLabel('Store').selectOption('111');
  await page.getByLabel('Group').selectOption('1110');
}

async function putKey(page: any, key: string, value: string) {
  await page.getByLabel('Put key').fill(key);
  await page.getByLabel('Put value').fill(value);
  const responsePromise = page.waitForResponse((response: any) => response.url().includes('/kv/put'));
  await page.getByRole('button', { name: /^Put$/ }).click();
  const response = await responsePromise;
  expect(response.ok(), await response.text()).toBeTruthy();
}

test.describe('E2E-11 KV delete', () => {
  test('deletes a key through the real KV UI and verifies it is gone', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r11', 'n11');
    await deployNodeServer(baseURL!, 'n11', 9913, 9923);
    await createStore(baseURL!, 111, ['n11']);
    await addGroup(baseURL!, 111, 1110, 11100, ['n11']);
    await waitForLeader(baseURL!, 111, 1110);

    try {
      await openKvPanel(page);
      await putKey(page, 'delete-11-key', 'delete-11-value');

      await page.getByLabel('Delete key').fill('delete-11-key');
      await page.getByRole('button', { name: /Delete$/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      const deleteResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      const deleteResponse = await deleteResponsePromise;
      expect(deleteResponse.ok(), await deleteResponse.text()).toBeTruthy();
      await expect(page.getByRole('alert').getByText(/Key deleted: "delete-11-key"/)).toBeVisible({ timeout: 3_000 });

      // Verify key is gone via Get
      await page.getByLabel('Get key').fill('delete-11-key');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(page.getByTestId('kv-not-found')).toBeVisible({ timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n11');
    }
  });
});
