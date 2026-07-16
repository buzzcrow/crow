// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack, createStore, createNode, deployNodeServer, stopNodeServer } from '../fixtures/consoleSetup';

async function currentViewportTransform(page: any) {
  await page.locator('.react-flow__viewport').waitFor();
  return page.locator('.react-flow__viewport').evaluate((el: Element) => (el as HTMLElement).style.transform);
}

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
    await createRack(baseURL!, { id: 'r20a', name: 'Rack Twenty A' });
    await createRack(baseURL!, { id: 'r20b', name: 'Rack Twenty B' });
    await createRack(baseURL!, { id: 'r20c', name: 'Rack Twenty C' });
    await createRack(baseURL!, { id: 'r20d', name: 'Rack Twenty D' });
    await createNode(baseURL!, { id: 'n20a', rack_id: 'r20a' });
    await createNode(baseURL!, { id: 'n20b', rack_id: 'r20b' });
    await createNode(baseURL!, { id: 'n20c', rack_id: 'r20c' });
    await createNode(baseURL!, { id: 'n20d', rack_id: 'r20d' });
    await deployNodeServer(baseURL!, 'n20a', 9960, 9970);
    await deployNodeServer(baseURL!, 'n20b', 9961, 9971);
    await deployNodeServer(baseURL!, 'n20c', 9962, 9972);
    await createStore(baseURL!, 207, ['n20a', 'n20b']);

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
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      await aside.getByRole('button', { name: 'Add Store' }).click();
      const addStoreDialog = page.getByRole('dialog', { name: 'Add KV Store' });
      await expect(addStoreDialog).toBeVisible();
      await expect(addStoreDialog.getByLabel('KV Store ID (numeric)')).toHaveValue(expectedStoreId);
      await expect(addStoreDialog.getByLabel(/^n20a/)).toBeVisible();
      await expect(addStoreDialog.getByLabel(/^n20b/)).toBeVisible();
      await expect(addStoreDialog.getByLabel(/^n20c/)).toBeVisible();
      await expect(addStoreDialog.getByLabel(/^n20d/)).toHaveCount(0);
      await addStoreDialog.getByRole('button', { name: 'Cancel' }).click();

      await expect(aside.getByText('S-207')).toBeVisible({ timeout: 3_000 });
      await aside.getByText('S-207').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add group/i }).click();
      const addGroupDialog = page.getByRole('dialog', { name: 'Add Group' });
      await expect(addGroupDialog).toBeVisible();
      await expect(addGroupDialog.getByLabel('KV Store')).toHaveValue('207');
      await expect(addGroupDialog.getByLabel('Group ID (numeric)')).toHaveValue(expectedGroupId);
      await expect(addGroupDialog.getByLabel('Starting Replica ID (numeric)')).toHaveValue(expectedReplicaId);
      await expect(addGroupDialog.getByLabel(/^n20a/)).toBeVisible();
      await expect(addGroupDialog.getByLabel(/^n20b/)).toBeVisible();
      await expect(addGroupDialog.getByLabel(/^n20c/)).toBeVisible();
      await expect(addGroupDialog.getByLabel(/^n20d/)).toHaveCount(0);
      await addGroupDialog.getByLabel(/^n20a/).check();
      await addGroupDialog.getByLabel(/^n20b/).check();
      const n20cInput = addGroupDialog.getByLabel(/^n20c/);
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
      expect(optionValues).toEqual(expect.arrayContaining(['n20c', 'n20d']));
      expect(optionValues).not.toEqual(expect.arrayContaining(['n20a', 'n20b']));
      await addReplicaDialog.getByLabel('Node', { exact: true }).selectOption('n20c');
      await addReplicaDialog.getByRole('button', { name: /add replica/i }).click();

      await aside.getByText(`G-${expectedGroupId}`).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();
      const remainingReplicaDialog = page.getByRole('dialog', { name: 'Add Replica' });
      await expect(remainingReplicaDialog.getByLabel('Replica ID (optional)')).toHaveValue(String(Number(expectedReplicaAfterGroup) + 1));
      const remainingOptions = await remainingReplicaDialog.getByLabel('Node', { exact: true }).locator('option').evaluateAll((options) =>
        options.map((option) => ({ value: (option as HTMLOptionElement).value, disabled: (option as HTMLOptionElement).disabled })),
      );
      const remainingValues = remainingOptions.filter((option) => !option.disabled).map((option) => option.value);
      expect(remainingValues).toEqual(expect.arrayContaining(['n20d']));
      expect(remainingValues).not.toEqual(expect.arrayContaining(['n20a', 'n20b', 'n20c']));
      await remainingReplicaDialog.getByRole('button', { name: 'Cancel' }).click();
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 'n20a');
      await stopNodeServer(baseURL!, 'n20b');
      await stopNodeServer(baseURL!, 'n20c');
    }
  });

  test('covers tree chevron vs text click behavior', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r21a', name: 'Rack Twenty One A' });
    await createRack(baseURL!, { id: 'r21b', name: 'Rack Twenty One B' });
    await createRack(baseURL!, { id: 'r21c', name: 'Rack Twenty One C' });
    await createNode(baseURL!, { id: 'n21a', rack_id: 'r21a' });
    await createNode(baseURL!, { id: 'n21b', rack_id: 'r21b' });
    await createNode(baseURL!, { id: 'n21c', rack_id: 'r21c' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    const rack21a = page.getByRole('treeitem').filter({ hasText: 'R-r21a (Rack Twenty One A)' });
    const node21c = page.getByRole('treeitem').filter({ hasText: 'N-n21c' });
    await expect(rack21a).toBeVisible({ timeout: 3_000 });
    await expect(node21c).toBeVisible({ timeout: 3_000 });

    // Chevron click collapses/expands without selecting
    await rack21a.getByRole('button', { name: 'Collapse' }).click();
    await expect(rack21a).toHaveAttribute('aria-expanded', 'false');
    await rack21a.getByRole('button', { name: 'Expand' }).click();
    await expect(rack21a).toHaveAttribute('aria-expanded', 'true');

    // Text click selects the node
    await node21c.getByRole('button', { name: 'N-n21c' }).click();
    await expect(node21c).toHaveAttribute('aria-selected', 'true');
  });
});
