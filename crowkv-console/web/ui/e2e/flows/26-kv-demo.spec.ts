// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

async function openKvPanel(page: any, storeId: string, groupId: string) {
  await page.goto('/');
  await page.getByRole('button', { name: 'KV' }).click();
  await page.getByLabel('Store').selectOption(storeId);
  await page.getByLabel('Group').selectOption(groupId);
}

async function scanAllDemoKeys(baseURL: string, storeId: number, groupId: number): Promise<string[]> {
  const keys: string[] = [];
  let startAfter = '';
  for (;;) {
    const url = `/api/stores/${storeId}/groups/${groupId}/kv/scan?prefix=demo_&limit=500${startAfter ? `&start_after=${encodeURIComponent(startAfter)}` : ''}`;
    const resp = await fetch(`${baseURL}${url}`);
    const body = await resp.json();
    keys.push(...body.items.map((i: any) => i.key_utf8));
    if (!body.truncated || body.items.length === 0) break;
    startAfter = body.items[body.items.length - 1].key_utf8;
  }
  return keys;
}

test.describe('E2E-26 KV demo inject + delete', () => {
  test('inject into single group then delete all', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r26', 'n26');
    await deployNodeServer(baseURL!, 'n26', 9960, 9970);
    await createStore(baseURL!, 260, 2600, 26000, ['n26']);
    await waitForLeader(baseURL!, 260, 2600);

    try {
      await openKvPanel(page, '260', '2600');

      // Inject 5 demo keys (default is 20, we use a smaller count for speed)
      await page.getByLabel('Inject').locator('..').locator('input[type="number"]').fill('5');
      const injectResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await injectResponsePromise;

      // Wait for scan to auto-trigger and show demo keys
      await expect(page.getByText(/demo_key_/)).toBeVisible({ timeout: 15_000 });

      // Verify via API that 5 demo keys exist in group 2600
      const keys = await scanAllDemoKeys(baseURL!, 260, 2600);
      expect(keys.length).toBe(5);
      expect(keys.every((k) => k.startsWith('demo_key_'))).toBe(true);

      // Delete all demo keys
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      const deleteResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      await deleteResponsePromise;

      // Wait for the toast
      await expect(page.getByText(/Deleted 5 demo keys/)).toBeVisible({ timeout: 15_000 });

      // Verify via API that no demo keys remain
      const remaining = await scanAllDemoKeys(baseURL!, 260, 2600);
      expect(remaining.length).toBe(0);
    } finally {
      await stopNodeServer(baseURL!, 'n26');
    }
  });

  test('inject into All Groups mode distributes across groups', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r26b', 'n26b');
    await deployNodeServer(baseURL!, 'n26b', 9961, 9971);
    await createStore(baseURL!, 261, 2610, 26100, ['n26b']);
    await addGroup(baseURL!, 261, 2611, 26110, ['n26b']);
    await waitForLeader(baseURL!, 261, 2610);
    await waitForLeader(baseURL!, 261, 2611);

    try {
      await openKvPanel(page, '261', 'All Groups');

      // Inject 20 demo keys in All Groups mode — should randomly distribute
      await page.getByLabel('Inject').locator('..').locator('input[type="number"]').fill('20');
      const injectResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await injectResponsePromise;

      // Wait for scan to show demo keys
      await expect(page.getByText(/demo_key_/)).toBeVisible({ timeout: 15_000 });

      // Verify total across both groups is 20
      const keys0 = await scanAllDemoKeys(baseURL!, 261, 2610);
      const keys1 = await scanAllDemoKeys(baseURL!, 261, 2611);
      expect(keys0.length + keys1.length).toBe(20);

      // Both groups should have at least some keys (probabilistic, but
      // with 20 keys across 2 groups the chance of all-20-in-one is
      // 2 * (1/2)^20 ≈ 0.0002, safe to assert)
      expect(keys0.length).toBeGreaterThan(0);
      expect(keys1.length).toBeGreaterThan(0);

      // Delete all demo keys in All Groups mode
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      const deleteResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      await deleteResponsePromise;

      await expect(page.getByText(/Deleted 20 demo keys/)).toBeVisible({ timeout: 15_000 });

      // Verify no demo keys remain in either group
      const remaining0 = await scanAllDemoKeys(baseURL!, 261, 2610);
      const remaining1 = await scanAllDemoKeys(baseURL!, 261, 2611);
      expect(remaining0.length).toBe(0);
      expect(remaining1.length).toBe(0);
    } finally {
      await stopNodeServer(baseURL!, 'n26b');
    }
  });

  test('inject into specific second group only targets that group', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r26c', 'n26c');
    await deployNodeServer(baseURL!, 'n26c', 9962, 9972);
    await createStore(baseURL!, 262, 2620, 26200, ['n26c']);
    await addGroup(baseURL!, 262, 2621, 26210, ['n26c']);
    await waitForLeader(baseURL!, 262, 2620);
    await waitForLeader(baseURL!, 262, 2621);

    try {
      // Select the second group specifically
      await openKvPanel(page, '262', '2621');

      // Inject 10 demo keys into group 2621 only
      await page.getByLabel('Inject').locator('..').locator('input[type="number"]').fill('10');
      const injectResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await injectResponsePromise;

      await expect(page.getByText(/demo_key_/)).toBeVisible({ timeout: 15_000 });

      // All 10 keys should be in group 2621, none in 2620
      const keys0 = await scanAllDemoKeys(baseURL!, 262, 2620);
      const keys1 = await scanAllDemoKeys(baseURL!, 262, 2621);
      expect(keys0.length).toBe(0);
      expect(keys1.length).toBe(10);

      // Delete all demo keys (still in group 2621 context)
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      const deleteResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      await deleteResponsePromise;

      await expect(page.getByText(/Deleted 10 demo keys/)).toBeVisible({ timeout: 15_000 });

      // Verify cleanup
      const remaining1 = await scanAllDemoKeys(baseURL!, 262, 2621);
      expect(remaining1.length).toBe(0);
    } finally {
      await stopNodeServer(baseURL!, 'n26c');
    }
  });
});
