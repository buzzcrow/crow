// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

/**
 * E2E-24 · KV get not-found + error surfacing (Req §3.4, §7).
 *
 * Complements 09/10/11 (happy-path put/get/scan/delete) by exercising the
 * graceful not-found path: a missing key must render an inline "not found"
 * state and a non-error toast, never an uncaught exception. The KV
 * panel exposes UTF-8 key/value inputs only, so binary/hex round-trips are
 * out of scope here (a V2 follow-up). Seeds its own store/group and
 * waits for a leader since `POST /api/stores` does not create a group.
 */
test.describe('E2E-24 KV not-found', () => {
  test('renders a graceful not-found for a missing key', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 24, 24);
    await deployNodeServer(baseURL!, 24, freePort(), freePort());
    await createStore(baseURL!, 244, [24]);
    await addGroup(baseURL!, 244, 2440, 24400, [24]);
    await waitForLeader(baseURL!, 244, 2440);

    const errors: string[] = [];
    page.on('pageerror', (err) => errors.push(String(err)));

    try {
      await page.goto('/');
      await page.locator('header').getByRole('button', { name: 'KV' }).click();
      await page.getByLabel('Store').selectOption('244');
      await page.getByLabel('Group').selectOption('2440');

      await page.getByLabel('Get key').fill('missing-key-24');
      const getResponsePromise = page.waitForResponse((r) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();

      await expect(page.getByTestId('kv-not-found')).toBeVisible({ timeout: 3_000 });
      expect(errors, errors.join('\n')).toHaveLength(0);
    } finally {
      await stopNodeServer(baseURL!, 24);
    }
  });
});
