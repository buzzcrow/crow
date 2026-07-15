// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';

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
    await seedRackAndNode(baseURL!, 'r24', 'n24');
    await deployNodeServer(baseURL!, 'n24', 9956, 9966);
    await createStore(baseURL!, 244, 2440, 24400, ['n24']);
    await addGroup(baseURL!, 244, 2440, 24400, ['n24']);
    await waitForLeader(baseURL!, 244, 2440);

    const errors: string[] = [];
    page.on('pageerror', (err) => errors.push(String(err)));

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'KV' }).click();
      await page.getByLabel('Store').selectOption('244');
      await page.getByLabel('Group').selectOption('2440');

      await page.getByPlaceholder('Key').fill('missing-key-24');
      const getResponsePromise = page.waitForResponse((r) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();

      await expect(page.getByText('not found')).toBeVisible({ timeout: 15_000 });
      await expect(page.getByText(/Key "missing-key-24" not found/)).toBeVisible({ timeout: 15_000 });
      expect(errors, errors.join('\n')).toHaveLength(0);
    } finally {
      await stopNodeServer(baseURL!, 'n24');
    }
  });
});
