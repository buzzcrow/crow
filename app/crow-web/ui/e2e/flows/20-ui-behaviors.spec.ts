// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.6s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack, createStore, createNode, deployNodeServer, freePort, stopNodeServer } from '../fixtures/consoleSetup';

function nextNumericId(values: Array<string | number>): string {
  const max = values.reduce<number>((acc, value) => {
    const raw = String(value).trim();
    if (!/^\d+$/.test(raw)) return acc;
    return Math.max(acc, Number(raw));
  }, 0);
  return String(max + 1);
}

test.describe('E2E-20 UI behaviors', () => {
  test('covers create dialog defaults and eligible candidate lists', async ({ page, baseURL }) => {
    // Batch independent API calls to reduce total round-trip time under load.
    await Promise.all([
      createRack(baseURL!, { id: 201, name: 'Rack Twenty A' }),
      createRack(baseURL!, { id: 202, name: 'Rack Twenty B' }),
      createRack(baseURL!, { id: 203, name: 'Rack Twenty C' }),
      createRack(baseURL!, { id: 204, name: 'Rack Twenty D' }),
    ]);
    await Promise.all([
      createNode(baseURL!, { id: 201, rack_id: 201 }),
      createNode(baseURL!, { id: 202, rack_id: 202 }),
      createNode(baseURL!, { id: 203, rack_id: 203 }),
      createNode(baseURL!, { id: 204, rack_id: 204 }),
    ]);
    await Promise.all([
      deployNodeServer(baseURL!, 201, freePort(), freePort()),
      deployNodeServer(baseURL!, 202, freePort(), freePort()),
      deployNodeServer(baseURL!, 203, freePort(), freePort()),
    ]);
    await createStore(baseURL!, 207, [201, 202]);

    const api = await apiContext(baseURL!);
    try {
      const storesResponse = await api.get('/api/stores');
      expect(storesResponse.ok(), await storesResponse.text()).toBeTruthy();
      const stores = await storesResponse.json();
      const expectedStoreId = nextNumericId((Array.isArray(stores) ? stores : []).map((store: any) => store.store_id));
      const groupsResponse = await api.get('/api/stores/207/groups');
      expect(groupsResponse.ok(), await groupsResponse.text()).toBeTruthy();
      const groups = await groupsResponse.json();
      const expectedGroupId = nextNumericId((Array.isArray(groups) ? groups : []).map((group: any) => group.group_id));
      const expectedReplicaId = '1';

      await page.goto('/');
      await page.getByRole('button', { name: 'KV Cluster' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      await aside.getByRole('button', { name: 'Add Store' }).click();
      const addStoreDialog = page.getByRole('dialog', { name: 'Add KV Store' });
      await expect(addStoreDialog).toBeVisible();
      await expect(addStoreDialog.getByLabel('KV Store ID (numeric)')).toHaveValue(expectedStoreId);
      await expect(addStoreDialog.getByLabel(/^201\b/)).toBeVisible();
      await expect(addStoreDialog.getByLabel(/^202\b/)).toBeVisible();
      await expect(addStoreDialog.getByLabel(/^203\b/)).toBeVisible();
      await expect(addStoreDialog.getByLabel(/^204\b/)).toHaveCount(0);
      await addStoreDialog.getByRole('button', { name: 'Cancel' }).click();

      await expect(aside.getByText('S-207')).toBeVisible({ timeout: 3_000 });
      await aside.getByText('S-207').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add group/i }).click();
      const addGroupDialog = page.getByRole('dialog', { name: 'Add Group' });
      await expect(addGroupDialog).toBeVisible();
      await expect(addGroupDialog.getByLabel('KV Store')).toHaveValue('207');
      await expect(addGroupDialog.getByLabel('Group ID (numeric)')).toHaveValue(expectedGroupId);
      await expect(addGroupDialog.getByLabel('Starting Replica ID (numeric)')).toHaveValue(expectedReplicaId);
      await expect(addGroupDialog.getByLabel(/^201\b/)).toBeVisible();
      await expect(addGroupDialog.getByLabel(/^202\b/)).toBeVisible();
      await expect(addGroupDialog.getByLabel(/^203\b/)).toBeVisible();
      await expect(addGroupDialog.getByLabel(/^204\b/)).toHaveCount(0);
      await addGroupDialog.getByLabel(/^201\b/).check();
      await addGroupDialog.getByLabel(/^202\b/).check();
      const n20cInput = addGroupDialog.getByLabel(/^203\b/);
      if (await n20cInput.isChecked()) await n20cInput.uncheck();
      await addGroupDialog.getByRole('button', { name: /create group/i }).click();

      const expectedReplicaAfterGroup = String(Number(expectedReplicaId) + 2);
      await expect(aside.getByText(`G-${expectedGroupId}`)).toBeVisible({ timeout: 3_000 });
      await aside.getByText(`G-${expectedGroupId}`).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();
      const addReplicaDialog = page.getByRole('dialog', { name: 'Add Replica' });
      await expect(addReplicaDialog).toBeVisible();
      await expect(addReplicaDialog.getByLabel('Replica ID (optional)')).toHaveValue(expectedReplicaAfterGroup);
      const nodeOptions = await addReplicaDialog.getByLabel('Node', { exact: true }).locator('option').evaluateAll((options) =>
        options.map((option) => ({ value: (option as HTMLOptionElement).value, disabled: (option as HTMLOptionElement).disabled })),
      );
      const optionValues = nodeOptions.filter((option) => !option.disabled).map((option) => option.value);
      expect(optionValues).toEqual(expect.arrayContaining(['203', '204']));
      expect(optionValues).not.toEqual(expect.arrayContaining(['201', '202']));
      await addReplicaDialog.getByLabel('Node', { exact: true }).selectOption('203');
      await addReplicaDialog.getByRole('button', { name: /add replica/i }).click();

      await aside.getByText(`G-${expectedGroupId}`).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();
      const remainingReplicaDialog = page.getByRole('dialog', { name: 'Add Replica' });
      await expect(remainingReplicaDialog.getByLabel('Replica ID (optional)')).toHaveValue(String(Number(expectedReplicaAfterGroup) + 1));
      const remainingOptions = await remainingReplicaDialog.getByLabel('Node', { exact: true }).locator('option').evaluateAll((options) =>
        options.map((option) => ({ value: (option as HTMLOptionElement).value, disabled: (option as HTMLOptionElement).disabled })),
      );
      const remainingValues = remainingOptions.filter((option) => !option.disabled).map((option) => option.value);
      expect(remainingValues).toEqual(expect.arrayContaining(['204']));
      expect(remainingValues).not.toEqual(expect.arrayContaining(['201', '202', '203']));
      await remainingReplicaDialog.getByRole('button', { name: 'Cancel' }).click();
    } finally {
      await api.dispose();
      await Promise.all([
        stopNodeServer(baseURL!, 201),
        stopNodeServer(baseURL!, 202),
        stopNodeServer(baseURL!, 203),
      ]);
    }
  });

  test('dialog cancel does not create entity', async ({ page, baseURL }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    await page.getByRole('button', { name: 'Add Rack' }).click();
    const dialog = page.getByRole('dialog', { name: 'Add Rack' });
    await expect(dialog).toBeVisible();
    await dialog.getByLabel('Rack ID').fill('r20cancel');
    await dialog.getByLabel('Name (optional)').fill('Should Not Exist');
    await dialog.getByRole('button', { name: 'Cancel' }).click();

    await expect(dialog).toHaveCount(0);
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await expect(aside.getByText('r20cancel')).toHaveCount(0);

    const api = await apiContext(baseURL!);
    try {
      const resp = await api.get('/api/racks');
      expect(resp.ok()).toBeTruthy();
      const racks = await resp.json();
      expect(racks).not.toEqual(expect.arrayContaining([expect.objectContaining({ id: 'r20cancel' })]));
    } finally {
      await api.dispose();
    }
  });

  test('covers tree chevron vs text click behavior', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 211, name: 'Rack Twenty One A' });
    await createRack(baseURL!, { id: 212, name: 'Rack Twenty One B' });
    await createRack(baseURL!, { id: 213, name: 'Rack Twenty One C' });
    await createNode(baseURL!, { id: 211, rack_id: 211 });
    await createNode(baseURL!, { id: 212, rack_id: 212 });
    await createNode(baseURL!, { id: 213, rack_id: 213 });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    const rack21a = page.getByRole('treeitem').filter({ hasText: 'R-211 (Rack Twenty One A)' });
    const node21c = page.getByRole('treeitem').filter({ hasText: 'N-213' });
    await expect(rack21a).toBeVisible({ timeout: 3_000 });
    await expect(node21c).toBeVisible({ timeout: 3_000 });

    // Chevron click collapses/expands without selecting
    await rack21a.getByRole('button', { name: 'Collapse' }).click();
    await expect(rack21a).toHaveAttribute('aria-expanded', 'false');
    await rack21a.getByRole('button', { name: 'Expand' }).click();
    await expect(rack21a).toHaveAttribute('aria-expanded', 'true');

    // Text click selects the node
    await node21c.getByRole('button', { name: 'N-213' }).click();
    await expect(node21c).toHaveAttribute('aria-selected', 'true');
  });
});
