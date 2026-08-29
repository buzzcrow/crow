// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.2s (2026-08-16)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

// One rack/node/server/store/group shared by every test in this file
// (IDs reused from the former 09-kv-put-get spec so they stay unique).
const apiBase = consoleBaseURL();

async function openKvPanel(page: any) {
  await step('kv: goto', () => page.goto('/'));
  await page.locator('header').getByRole('button', { name: 'KV', exact: true }).click();
  await page.getByLabel('Store').selectOption('99');
  await page.getByLabel('Group').selectOption('990');
}

async function putKey(page: any, key: string, value: string) {
  await step('kv: put', async () => {
    await page.getByLabel('Put key').fill(key);
    await page.getByLabel('Put value').fill(value);
    const responsePromise = page.waitForResponse((response: any) => response.url().includes('/kv/put'));
    await page.getByRole('button', { name: /^Put$/ }).click();
    const response = await responsePromise;
    expect(response.ok(), await response.text()).toBeTruthy();
  });
}

test.describe('kv ops · put/get/scan/delete', () => {
  test.beforeAll(async () => {
    try {
      await step('kv: seed rack/node', () => seedRackAndNode(apiBase, 9, 9));
      await step('kv: deploy server', () => deployNodeServer(apiBase, 9, freePort(), freePort()));
      await step('kv: create store', () => createStore(apiBase, 99, [9]));
      await step('kv: add group', () => addGroup(apiBase, 99, 990, 9900, [9]));
      await step('kv: wait for leader', () => waitForLeader(apiBase, 99, 990));
    } catch (err) {
      await stopNodeServer(apiBase, 9);
      throw err;
    }
  });

  test.afterAll(async () => {
    await step('kv: stop server', () => stopNodeServer(apiBase, 9));
  });

  test('put/get/overwrite, prefix scan, and graceful not-found', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (err) => errors.push(String(err)));

    await openKvPanel(page);

    // --- put / get / overwrite / revision (former 09-kv-put-get) ---
    // Put
    await step('kv: put', async () => {
      await page.getByLabel('Put key').fill('e2e-key-9');
      await page.getByLabel('Put value').fill('e2e-value-9');
      const putResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/put'));
      await page.getByRole('button', { name: /^Put$/ }).click();
      const putResponse = await putResponsePromise;
      expect(putResponse.ok(), await putResponse.text()).toBeTruthy();
    });

    // Get
    await step('kv: get', async () => {
      await page.getByLabel('Get key').fill('e2e-key-9');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(page.getByTestId('kv-get-result')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-get-result')).toHaveText('e2e-value-9');
    });

    // Overwrite: put same key with new value
    await step('kv: overwrite', async () => {
      await page.getByLabel('Put key').fill('e2e-key-9');
      await page.getByLabel('Put value').fill('e2e-value-9-v2');
      const overwriteResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/put'));
      await page.getByRole('button', { name: /^Put$/ }).click();
      const overwriteResponse = await overwriteResponsePromise;
      expect(overwriteResponse.ok(), await overwriteResponse.text()).toBeTruthy();
    });

    // Get again — should return new value
    await step('kv: get v2', async () => {
      await page.getByLabel('Get key').fill('e2e-key-9');
      const getResponse2Promise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse2 = await getResponse2Promise;
      expect(getResponse2.ok(), await getResponse2.text()).toBeTruthy();
      await expect(page.getByTestId('kv-get-result')).toHaveText('e2e-value-9-v2', { timeout: 3_000 });
    });

    // Verify revision incremented (rev: 2 should be visible)
    await expect(page.getByText(/rev: 2/)).toBeVisible({ timeout: 3_000 });

    // --- prefix scan (former 10-kv-scan) ---
    // Turn off auto-scan first to prevent stale auto-scan from overriding prefix scan results
    await page.getByLabel('auto-scan').uncheck();

    await putKey(page, 'scan-10-a', 'value-a');
    await putKey(page, 'scan-10-b', 'value-b');
    await putKey(page, 'other-10-c', 'value-c');

    // Scan with prefix "scan-10-" — should only return matching keys
    await page.getByLabel('Scan prefix').fill('scan-10-');
    await step('kv: prefix scan', () => expect.poll(async () => {
      const responsePromise = page.waitForResponse((response) => response.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /^Scan$/ }).evaluate((el: HTMLElement) => el.click());
      const response = await responsePromise;
      return response.ok();
    }, { timeout: 5_000, intervals: [100] }).toBe(true));

    const scanTable = page.getByTestId('kv-scan-table');
    await expect(scanTable.getByText('scan-10-a')).toBeVisible({ timeout: 3_000 });
    await expect(scanTable.getByText('value-a')).toBeVisible();
    await expect(scanTable.getByText('scan-10-b')).toBeVisible();
    await expect(scanTable.getByText('value-b')).toBeVisible();

    // Prefix filter: "other-" keys should NOT appear
    await expect(scanTable.getByText('other-10-c')).toHaveCount(0, { timeout: 3_000 });
    await expect(scanTable.getByText('value-c')).toHaveCount(0, { timeout: 3_000 });

    // --- graceful not-found for a missing key (former 24-kv-not-found) ---
    await step('kv: get missing', async () => {
      await page.getByLabel('Get key').fill('missing-key-24');
      const missingResponsePromise = page.waitForResponse((r) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const missingResponse = await missingResponsePromise;
      expect(missingResponse.ok(), await missingResponse.text()).toBeTruthy();
    });

    await expect(page.getByTestId('kv-not-found')).toBeVisible({ timeout: 3_000 });
    expect(errors, errors.join('\n')).toHaveLength(0);
  });

  test('deletes a key through the real KV UI and verifies it is gone', async ({ page }) => {
    await openKvPanel(page);

    // --- delete a key, then confirm Get reports not-found (former 11-kv-delete) ---
    await putKey(page, 'delete-11-key', 'delete-11-value');

    await step('kv: delete dialog', async () => {
      await page.getByLabel('Delete key').fill('delete-11-key');
      await page.getByRole('button', { name: /Delete$/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      const deleteResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      const deleteResponse = await deleteResponsePromise;
      expect(deleteResponse.ok(), await deleteResponse.text()).toBeTruthy();
    });

    // Verify key is gone via Get
    await step('kv: get deleted', async () => {
      await page.getByLabel('Get key').fill('delete-11-key');
      const getResponsePromise = page.waitForResponse((response) => response.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      const getResponse = await getResponsePromise;
      expect(getResponse.ok(), await getResponse.text()).toBeTruthy();
      await expect(page.getByTestId('kv-not-found')).toBeVisible({ timeout: 3_000 });
    });
  });
});
