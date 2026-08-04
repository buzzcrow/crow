// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { createRack, createNode, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-37 dialog duplicate ID rejection', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('adding rack with existing ID shows error toast', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r37', name: 'r37' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

    // Click Add Rack
    await aside.getByRole('button', { name: 'Add Rack' }).click();
    const dialog = page.getByRole('dialog', { name: 'Add Rack' });
    await expect(dialog).toBeVisible();
    await dialog.getByLabel('Rack ID').fill('r37');
    await dialog.getByLabel('Name (optional)').fill('duplicate');

    // Submit and expect error toast
    const responsePromise = page.waitForResponse((r: any) => r.url().includes('/api/racks'));
    await dialog.getByRole('button', { name: /create rack/i }).click();
    const response = await responsePromise;
    expect(response.status()).toBe(409);
  });

  test('adding node with existing ID shows error toast', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r37b', name: 'r37b' });
    await createNode(baseURL!, { id: 'n37b', rack_id: 'r37b' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    // Right-click rack to get Add Node
    const rackItem = page.getByRole('treeitem').filter({ hasText: 'R-r37b' });
    await expect(rackItem).toBeVisible({ timeout: 3_000 });
    await rackItem.click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();

    const dialog = page.getByRole('dialog', { name: 'Add Node' });
    await expect(dialog).toBeVisible();
    await dialog.getByLabel('Node ID').fill('n37b');
    await dialog.getByLabel('Host').fill('127.0.0.1');

    const responsePromise = page.waitForResponse((r: any) => r.url().includes('/api/nodes'));
    await dialog.getByRole('button', { name: /create node/i }).click();
    const response = await responsePromise;
    expect(response.status()).toBe(409);
  });
});
