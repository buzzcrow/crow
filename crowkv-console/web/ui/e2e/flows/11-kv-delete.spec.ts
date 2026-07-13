// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

async function openKvTab(page: any) {
  await page.goto('/');
  await page.getByRole('button', { name: 'Logical' }).click();
  const aside = page.locator('aside').first();
  await expect(aside.getByText('G-1110', { exact: true })).toBeVisible({ timeout: 20_000 });
  await aside.getByText('G-1110', { exact: true }).click();
  const inspector = page.locator('aside[aria-label="Entity inspector"]');
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

test.describe('E2E-11 KV delete', () => {
  test('deletes a key through the real KV UI and verifies it is gone', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r11', 'n11');
    await deployNodeServer(baseURL!, 'n11', 9913, 9923);
    await createStore(baseURL!, 111, 1110, 11100, ['n11']);
    await addGroup(baseURL!, 111, 1110, 11100, ['n11']);
    await waitForLeader(baseURL!, 111, 1110);

    try {
      const inspector = await openKvTab(page);
      await putKey(inspector, page, 'delete-11-key', 'delete-11-value');

      await inspector.getByRole('button', { name: 'Delete' }).first().click();
      await inspector.getByPlaceholder('Key to delete').fill('delete-11-key');
      // The action button opens a confirmation dialog; the request only
      // fires after confirming.
      await inspector.getByRole('button', { name: 'Delete' }).last().click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      const deleteResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      const deleteResponse = await deleteResponsePromise;
      expect(deleteResponse.ok(), await deleteResponse.text()).toBeTruthy();
      await expect(page.getByText(/Key deleted: "delete-11-key"/)).toBeVisible({ timeout: 30_000 });

      await inspector.getByRole('button', { name: 'Get' }).first().click();
      await inspector.getByPlaceholder('Key to get').fill('delete-11-key');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await inspector.getByRole('button', { name: 'Get' }).last().click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(inspector.getByText('Key not found')).toBeVisible({ timeout: 30_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n11');
    }
  });
});
