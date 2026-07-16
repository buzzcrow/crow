// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { apiContext, DEFAULT_SERVER_BINARY, stopNodeServer } from '../fixtures/consoleSetup';

/**
 * End-to-end smoke for the rewritten (v1 lean) console UI. Drives the full
 * real operation through the SPA against a live crowkv-web + crowkv-server:
 * rack -> node -> deploy server -> store -> group -> replica -> KV put/get,
 * in both Physical and Logical views. Uses the new DOM (Physical/Logical
 * toggle, Filter input, right-click context menus, inspector KV tab).
 */
test.describe('E2E-00 full real operation (rewritten UI)', () => {
  test('rack -> node -> server -> store -> group -> kv, both views', async ({ page, baseURL }) => {
    const consoleErrors: string[] = [];
    page.on('console', (m) => {
      if (m.type() === 'error') consoleErrors.push(m.text());
    });

    await page.goto('/');

    // --- Shell renders ---
    await expect(page.getByRole('button', { name: 'Physical' })).toBeVisible({ timeout: 3_000 });
    await expect(page.getByRole('button', { name: 'Logical' })).toBeVisible();
    await expect(page.getByPlaceholder('Filter...')).toBeVisible();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

    try {
    // --- Physical: add rack ---
    await page.getByRole('button', { name: 'Physical' }).click();
    await page.getByRole('button', { name: 'Add Rack' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await page.getByLabel('Rack ID').fill('rsm');
    await page.getByLabel('Name (optional)').fill('Rack Smoke');
    await page.getByRole('button', { name: /create rack/i }).click();
    await expect(aside.getByText('R-rsm (Rack Smoke)')).toBeVisible({ timeout: 3_000 });

    // --- Physical: add node via context menu ---
    await aside.getByText('R-rsm (Rack Smoke)').click({ button: 'right' });
    await page.getByRole('menuitem', { name: 'Add Node' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await page.getByLabel('Node ID').fill('nsm');
    await page.getByLabel('Host').fill('127.0.0.1');
    await page.getByLabel('Enable CrowKV on this node').uncheck();
    await page.getByRole('button', { name: /create node/i }).click();
    await expect(aside.getByText('N-nsm', { exact: true })).toBeVisible({ timeout: 3_000 });

    // --- Physical: deploy CrowKV Server via context menu ---
    await aside.getByText('N-nsm', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /Deploy CrowKV/i }).click();
    await expect(page.getByRole('dialog', { name: /Deploy CrowKV on nsm/ })).toBeVisible();
    await page.getByLabel('Management Port').fill('9901');
    await page.getByLabel('gRPC Port').fill('9902');
    await page.getByLabel(/Binary Path/).fill(DEFAULT_SERVER_BINARY);
    await page.getByRole('button', { name: /^Deploy$/ }).click();

    // Backend confirms the server is running.
    await expect.poll(async () => {
      const api = await apiContext(baseURL!);
      try {
        const r = await api.get('/api/nodes/nsm/server');
        if (!r.ok()) return 0;
        const body = await r.json();
        return body.pid ?? 0;
      } finally {
        await api.dispose();
      }
    }, { timeout: 3_000 }).toBeGreaterThan(0);

    // --- Logical: add empty KV store on n1 ---
    await page.getByRole('button', { name: 'Logical' }).click();
    await page.getByRole('button', { name: 'Add Store' }).click();
    await expect(page.getByRole('dialog', { name: 'Add KV Store' })).toBeVisible();
    await page.getByLabel('KV Store ID (numeric)').fill('7');
    await page.getByLabel(/^nsm/).check();
    await page.getByRole('button', { name: /create kv store/i }).click();
    await expect(aside.getByText('S-7')).toBeVisible({ timeout: 3_000 });

    // --- Logical: create first group in store 7 ---
    await aside.getByText('S-7').click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add group/i }).click();
    await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
    await page.getByLabel('Group ID (numeric)').fill('70');
    await page.getByLabel('Starting Replica ID (numeric)').fill('700');
    await page.getByLabel(/^nsm/).check();
    await page.getByRole('button', { name: /create group/i }).click();

    // --- Logical: expand store, see group + replica ---
    const store7 = page.getByRole('treeitem').filter({ hasText: 'S-7' });
    const expandStore7 = store7.getByRole('button', { name: 'Expand' });
    if (await expandStore7.count()) await expandStore7.click();
    await expect(aside.getByText('G-70')).toBeVisible({ timeout: 3_000 });

    // Wait for a leader to be elected before KV operations. GroupView has
    // no top-level leader field — the leader is the replica self-reporting
    // role "leader" (snake_case on the wire).
    await expect.poll(async () => {
      const api = await apiContext(baseURL!);
      try {
        const r = await api.get('/api/stores/7/groups/70');
        if (!r.ok()) return false;
        const body = await r.json();
        const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
        return replicas.some((rep) => String(rep.role).toLowerCase() === 'leader');
      } finally {
        await api.dispose();
      }
    }, { timeout: 10_000 }).toBe(true);

    // --- KV via KV Operator panel ---
    await page.locator('header').getByRole('button', { name: 'KV' }).click();

    // Put
    await page.getByLabel('Put key').fill('smoke-key');
    await page.getByLabel('Put value').fill('smoke-value');
    const putResponsePromise = page.waitForResponse((r) => r.url().includes('/kv/put'));
    await page.getByRole('button', { name: /^Put$/ }).click();
    await putResponsePromise;

    // Get
    await page.getByLabel('Get key').fill('smoke-key');
    await page.getByRole('button', { name: /^Get$/ }).click();
    await expect(page.getByTestId('kv-get-result')).toBeVisible({ timeout: 3_000 });

    // --- Backend verifies the full chain ---
    const api = await apiContext(baseURL!);
    try {
      const replicas = await api.get('/api/stores/7/groups/70/replicas');
      expect(replicas.ok(), await replicas.text()).toBeTruthy();
      const list = await replicas.json();
      expect(Array.isArray(list) ? list.length : 0).toBeGreaterThanOrEqual(1);
    } finally {
      await api.dispose();
    }

    // Ignore transient network 404s (e.g. a logical poll racing store
    // creation); fail only on real JS/runtime errors.
    const jsErrors = consoleErrors.filter((e) => !/Failed to load resource/i.test(e));
    expect(jsErrors, jsErrors.join('\n')).toEqual([]);
    } finally {
      // Stop the smoke server so it does not pollute later specs (its
      // bootstrap store 1 would otherwise aggregate into their views).
      await stopNodeServer(baseURL!, 'nsm');
    }
  });
});
