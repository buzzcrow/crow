// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.1s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, resetAll, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

async function openKvPanel(page: any, storeId: string, groupId: string) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV' }).click();
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
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });
  test('inject into single group then delete all', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 26, 26);
    await deployNodeServer(baseURL!, 26, 9960, 9970);
    await createStore(baseURL!, 260, [26]);
    await addGroup(baseURL!, 260, 2600, 26000, [26]);
    await waitForLeader(baseURL!, 260, 2600);

    try {
      await openKvPanel(page, '260', '2600');

      // Inject 5 demo keys (default is 20, we use a smaller count for speed)
      await page.getByLabel('Demo key count').fill('5');
      const injectResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await injectResponsePromise;

      // Wait for scan to auto-trigger and show demo keys
      await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/).first()).toBeVisible({ timeout: 3_000 });
      const keys = await scanAllDemoKeys(baseURL!, 260, 2600);
      expect(keys.length).toBe(5);
      expect(keys.every((k) => k.startsWith('demo_key_'))).toBe(true);

      // Delete all demo keys — wait for all delete responses to settle
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      await dialog.getByRole('button', { name: 'Delete' }).click();
      // Poll until no demo keys remain (delete-all sends multiple requests)
      await expect.poll(async () => {
        const remaining = await scanAllDemoKeys(baseURL!, 260, 2600);
        return remaining.length;
      }, { timeout: 5_000, intervals: [100] }).toBe(0);

      // Verify scan table no longer shows demo keys
      await page.getByRole('button', { name: /scan/i }).click();
      await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/)).toHaveCount(0, { timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 26);
    }
  });

  test('inject into All Groups mode distributes across groups', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 262, 262);
    await deployNodeServer(baseURL!, 262, 9961, 9971);
    await createStore(baseURL!, 261, [262]);
    await addGroup(baseURL!, 261, 2610, 26100, [262]);
    await addGroup(baseURL!, 261, 2611, 26110, [262]);
    await waitForLeader(baseURL!, 261, 2610);
    await waitForLeader(baseURL!, 261, 2611);

    try {
      await openKvPanel(page, '261', 'All Groups');

      // Inject 20 demo keys in All Groups mode — should randomly distribute
      await page.getByLabel('Demo key count').fill('20');
      const injectResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await injectResponsePromise;

      // Wait for scan to show demo keys
      await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/).first()).toBeVisible({ timeout: 3_000 });
      const keys0 = await scanAllDemoKeys(baseURL!, 261, 2610);
      const keys1 = await scanAllDemoKeys(baseURL!, 261, 2611);
      expect(keys0.length + keys1.length).toBe(20);

      // Both groups should have at least some keys (probabilistic, but
      // with 20 keys across 2 groups the chance of all-20-in-one is
      // 2 * (1/2)^20 ≈ 0.0002, safe to assert)
      expect(keys0.length).toBeGreaterThan(0);
      expect(keys1.length).toBeGreaterThan(0);

      // Delete all demo keys in All Groups mode — poll until clean
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      await dialog.getByRole('button', { name: 'Delete' }).click();
      // Poll until no demo keys remain in either group
      await expect.poll(async () => {
        const r0 = await scanAllDemoKeys(baseURL!, 261, 2610);
        const r1 = await scanAllDemoKeys(baseURL!, 261, 2611);
        return r0.length + r1.length;
      }, { timeout: 5_000, intervals: [100] }).toBe(0);
    } finally {
      await stopNodeServer(baseURL!, 262);
    }
  });

  test('inject into specific second group only targets that group', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 263, 263);
    await deployNodeServer(baseURL!, 263, 9962, 9972);
    await createStore(baseURL!, 262, [263]);
    await addGroup(baseURL!, 262, 2620, 26200, [263]);
    await addGroup(baseURL!, 262, 2621, 26210, [263]);
    await waitForLeader(baseURL!, 262, 2620);
    await waitForLeader(baseURL!, 262, 2621);

    try {
      // Select the second group specifically
      await openKvPanel(page, '262', '2621');

      // Inject 10 demo keys into group 2621 only
      await page.getByLabel('Demo key count').fill('10');
      const injectResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await injectResponsePromise;

      await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/).first()).toBeVisible({ timeout: 3_000 });

      // All 10 keys should be in group 2621, none in 2620
      const keys0 = await scanAllDemoKeys(baseURL!, 262, 2620);
      const keys1 = await scanAllDemoKeys(baseURL!, 262, 2621);
      expect(keys0.length).toBe(0);
      expect(keys1.length).toBe(10);

      // Delete all demo keys (still in group 2621 context) — poll until clean
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      await dialog.getByRole('button', { name: 'Delete' }).click();
      await expect.poll(async () => {
        const remaining = await scanAllDemoKeys(baseURL!, 262, 2621);
        return remaining.length;
      }, { timeout: 5_000, intervals: [100] }).toBe(0);
    } finally {
      await stopNodeServer(baseURL!, 263);
    }
  });
});
