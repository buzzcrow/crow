// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.0s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { consoleBaseURL } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

async function openKvPanel(page: any, storeId: string, groupId: string) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV' }).click();
  await page.getByLabel('Store').selectOption(storeId);
  await page.getByLabel('Group').selectOption(groupId);
}

test.describe('E2E-29 KV load more', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('scan with >100 keys shows truncated indicator and Load More button', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r29', 'n29');
    await deployNodeServer(baseURL!, 'n29', 9929, 9939);
    await createStore(baseURL!, 290, ['n29']);
    await addGroup(baseURL!, 290, 2900, 29000, ['n29']);
    await waitForLeader(baseURL!, 290, 2900);

    try {
      // Bulk-insert 120 keys via API (much faster than UI one-by-one)
      const apiBase = consoleBaseURL();
      for (let i = 0; i < 120; i++) {
        const key = `load-key-${String(i).padStart(3, '0')}`;
        const resp = await fetch(`${apiBase}/api/stores/290/groups/2900/kv/put`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key, value: `val-${i}` }),
        });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(`KV put failed for key ${key}: ${resp.status} ${text}`);
        }
      }

      await openKvPanel(page, '290', '2900');

      // Scan
      await page.getByRole('button', { name: /scan/i }).click();
      await expect(page.getByTestId('kv-scan-table')).toBeVisible({ timeout: 3_000 });

      // Verify truncated indicator
      await expect(page.getByText(/truncated/i)).toBeVisible({ timeout: 3_000 });

      // Verify Load More button is visible
      await expect(page.getByRole('button', { name: /load more/i })).toBeVisible();

      // Count rows in table (should be 100)
      const initialRowCount = await page.getByTestId('kv-scan-table').locator('tbody tr').count();
      expect(initialRowCount).toBe(100);

      // Click Load More
      const loadMoreResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /load more/i }).click();
      await loadMoreResponse;

      // Verify additional rows appear
      await expect(page.getByTestId('kv-scan-table').locator('tbody tr')).toHaveCount(120, { timeout: 3_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n29');
    }
  });
});
