// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.1s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

async function kvPut(baseURL: string, storeId: number, groupId: number, key: string, value: string) {
  const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/put`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key, value }),
  });
  expect(resp.ok).toBeTruthy();
}

async function kvScanAll(baseURL: string, storeId: number, groupId: number): Promise<string[]> {
  const keys: string[] = [];
  let startAfter = '';
  for (;;) {
    const url = `/api/stores/${storeId}/groups/${groupId}/kv/scan?limit=500${startAfter ? `&start_after=${encodeURIComponent(startAfter)}` : ''}`;
    const resp = await fetch(`${baseURL}${url}`);
    expect(resp.ok).toBeTruthy();
    const body = await resp.json();
    keys.push(...body.items.map((i: any) => i.key_utf8));
    if (!body.truncated) break;
    startAfter = body.items[body.items.length - 1]?.key_utf8 ?? '';
    if (!startAfter) break;
  }
  return keys;
}

test.describe('E2E-38 multi-store isolation', () => {
  test('put/get/delete on store A does not affect store B', async ({ page, baseURL }) => {
    await resetAll(baseURL!);

    // Store A: nodes n38a,b,c. Store B: nodes n38d,e,f. Separate node sets.
    await seedRackAndNode(baseURL!, 'r38a', 'n38a');
    await seedRackAndNode(baseURL!, 'r38b', 'n38b');
    await seedRackAndNode(baseURL!, 'r38c', 'n38c');
    await seedRackAndNode(baseURL!, 'r38d', 'n38d');
    await seedRackAndNode(baseURL!, 'r38e', 'n38e');
    await seedRackAndNode(baseURL!, 'r38f', 'n38f');
    await deployNodeServer(baseURL!, 'n38a', 9938, 9939);
    await deployNodeServer(baseURL!, 'n38b', 9940, 9941);
    await deployNodeServer(baseURL!, 'n38c', 9942, 9943);
    await deployNodeServer(baseURL!, 'n38d', 9944, 9945);
    await deployNodeServer(baseURL!, 'n38e', 9946, 9947);
    await deployNodeServer(baseURL!, 'n38f', 9948, 9949);

    // Store A: 380, group 3800 on n38a,b,c. Store B: 381, group 3810 on n38d,e,f.
    await createStore(baseURL!, 380, ['n38a', 'n38b', 'n38c']);
    await createStore(baseURL!, 381, ['n38d', 'n38e', 'n38f']);
    await addGroup(baseURL!, 380, 3800, 38000, ['n38a', 'n38b', 'n38c']);
    await addGroup(baseURL!, 381, 3810, 38100, ['n38d', 'n38e', 'n38f']);
    await waitForLeader(baseURL!, 380, 3800);
    await waitForLeader(baseURL!, 381, 3810);

    try {
      // Put keys in store A only
      await kvPut(baseURL!, 380, 3800, 'iso-a-key1', 'val-a1');
      await kvPut(baseURL!, 380, 3800, 'iso-a-key2', 'val-a2');

      // Put keys in store B only
      await kvPut(baseURL!, 381, 3810, 'iso-b-key1', 'val-b1');
      await kvPut(baseURL!, 381, 3810, 'iso-b-key2', 'val-b2');

      // Verify store A has only A keys
      const scanA = await kvScanAll(baseURL!, 380, 3800);
      expect(scanA).toEqual(expect.arrayContaining(['iso-a-key1', 'iso-a-key2']));
      expect(scanA).not.toEqual(expect.arrayContaining(['iso-b-key1', 'iso-b-key2']));

      // Verify store B has only B keys
      const scanB = await kvScanAll(baseURL!, 381, 3810);
      expect(scanB).toEqual(expect.arrayContaining(['iso-b-key1', 'iso-b-key2']));
      expect(scanB).not.toEqual(expect.arrayContaining(['iso-a-key1', 'iso-a-key2']));

      // Verify via UI: open KV panel, select store A, scan, see only A keys
      await page.goto('/');
      await page.locator('header').getByRole('button', { name: 'KV' }).click();
      await page.getByLabel('Store').selectOption('380');
      await page.getByLabel('Group').selectOption('3800');

      // Scan and verify store A keys appear
      const scanResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /^Scan$/ }).evaluate((el: HTMLElement) => el.click());
      await scanResponse;
      await expect(page.getByTestId('kv-scan-table').getByText('iso-a-key1')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('iso-b-key1')).toHaveCount(0);

      // Switch to store B, scan, see only B keys
      await page.getByLabel('Store').selectOption('381');
      await page.getByLabel('Group').selectOption('3810');
      const scanResponse2 = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /^Scan$/ }).evaluate((el: HTMLElement) => el.click());
      await scanResponse2;
      await expect(page.getByTestId('kv-scan-table').getByText('iso-b-key1')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('iso-a-key1')).toHaveCount(0);
    } finally {
      await stopNodeServer(baseURL!, 'n38a');
      await stopNodeServer(baseURL!, 'n38b');
      await stopNodeServer(baseURL!, 'n38c');
      await stopNodeServer(baseURL!, 'n38d');
      await stopNodeServer(baseURL!, 'n38e');
      await stopNodeServer(baseURL!, 'n38f');
    }
  });
});
