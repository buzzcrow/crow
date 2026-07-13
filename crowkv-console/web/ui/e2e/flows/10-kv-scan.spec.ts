// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

async function openKvTab(page: any) {
  await page.goto('/');
  await page.getByRole('button', { name: 'Logical' }).click();
  const aside = page.locator('aside').first();
  await expect(aside.getByText('G-1100', { exact: true })).toBeVisible({ timeout: 20_000 });
  await aside.getByText('G-1100', { exact: true }).click();
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

test.describe('E2E-10 KV scan', () => {
  test('scans keys through the real KV UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r10', 'n10');
    await deployNodeServer(baseURL!, 'n10', 9930, 9940);
    await createStore(baseURL!, 110, 1100, 11000, ['n10']);
    await addGroup(baseURL!, 110, 1100, 11000, ['n10']);
    await waitForLeader(baseURL!, 110, 1100);

    try {
      const inspector = await openKvTab(page);
      await putKey(inspector, page, 'scan-10-a', 'value-a');
      await putKey(inspector, page, 'scan-10-b', 'value-b');

      await inspector.getByRole('button', { name: 'Scan' }).first().click();
      await inspector.getByPlaceholder('Key prefix (empty = all)').fill('scan-10-');
      const responsePromise = page.waitForResponse((response) => response.url().includes('/kv/scan'));
      await inspector.getByRole('button', { name: 'Scan' }).last().click();
      const response = await responsePromise;
      expect(response.ok(), await response.text()).toBeTruthy();

      await expect(inspector.getByText('scan-10-a')).toBeVisible({ timeout: 30_000 });
      await expect(inspector.getByText('value-a')).toBeVisible();
      await expect(inspector.getByText('scan-10-b')).toBeVisible();
      await expect(inspector.getByText('value-b')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n10');
    }
  });
});
