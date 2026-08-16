// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 28s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer, waitForLeader, resetAll, apiContext } from '../fixtures/consoleSetup';

// API helpers for setup only (cluster bring-up + initial data + leader
// discovery + recovery polling). The reconfiguration actions and post-action
// KV verification go through the UI — context menus, dialogs, tree health,
// and the KV panel — so the test exercises the same path an operator does.

async function kvPut(baseURL: string, storeId: number, groupId: number, key: string, value: string) {
  const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/put`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key, value }),
  });
  expect(resp.ok).toBeTruthy();
}

async function getGroupStatus(baseURL: string, storeId: number, groupId: number) {
  const api = await apiContext(baseURL);
  try {
    const r = await api.get(`/api/stores/${storeId}/groups/${groupId}`);
    expect(r.ok(), await r.text()).toBeTruthy();
    return await r.json();
  } finally {
    await api.dispose();
  }
}

async function findLeaderNode(baseURL: string, storeId: number, groupId: number): Promise<number | null> {
  const body = await getGroupStatus(baseURL, storeId, groupId);
  const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
  const leader = replicas.find((r) => String(r.role).toLowerCase() === 'leader');
  return leader?.node_id ?? null;
}

/** Poll the group status until `count` replicas report state = running. */
async function waitForReachableReplicas(baseURL: string, storeId: number, groupId: number, count: number, timeoutMs = 15_000) {
  await expect.poll(async () => {
    const body = await getGroupStatus(baseURL, storeId, groupId);
    const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
    return replicas.filter((r) => String(r.state).toLowerCase() === 'running').length;
  }, { timeout: timeoutMs, intervals: [200] }).toBeGreaterThanOrEqual(count);
}

/** Poll the group status until a leader is elected. */
async function waitForReelection(baseURL: string, storeId: number, groupId: number, timeoutMs = 15_000) {
  await expect.poll(async () => {
    const body = await getGroupStatus(baseURL, storeId, groupId);
    const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
    return replicas.some((r) => String(r.role).toLowerCase() === 'leader');
  }, { timeout: timeoutMs, intervals: [200] }).toBe(true);
}

/** Poll the group status until a specific replica reports state = running. */
async function waitForReplicaRunning(baseURL: string, storeId: number, groupId: number, replicaId: number, timeoutMs = 20_000) {
  await expect.poll(async () => {
    const body = await getGroupStatus(baseURL, storeId, groupId);
    const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
    const r = replicas.find((r) => r.replica_id === replicaId);
    return r ? String(r.state).toLowerCase() : 'absent';
  }, { timeout: timeoutMs, intervals: [200] }).toBe('running');
}

// ── UI helpers ───────────────────────────────────────────────────────

async function openPhysical(page: import('@playwright/test').Page) {
  await page.goto('/');
  await page.getByRole('button', { name: 'Physical' }).click();
}

async function openKvCluster(page: import('@playwright/test').Page) {
  await page.goto('/');
  await page.getByRole('button', { name: 'KV Cluster' }).click();
}

async function openKvPanel(page: import('@playwright/test').Page, storeId: number, groupId: number) {
  await page.goto('/');
  await page.locator('header').getByRole('button', { name: 'KV', exact: true }).click();
  await page.getByLabel('Store').selectOption(String(storeId));
  await page.getByLabel('Group').selectOption(String(groupId));
}

/** The health badge inside a server tree item (Physical view). */
function serverHealthBadge(page: import('@playwright/test').Page, nodeId: number) {
  return page
    .getByRole('treeitem')
    .filter({ hasText: `KV-${nodeId}` })
    .locator('[title]')
    .filter({ hasText: /^(Healthy|Failed|Unknown|Degraded)$/ });
}

async function stopServerViaMenu(page: import('@playwright/test').Page, nodeId: number) {
  const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(aside.getByText(`KV-${nodeId}`, { exact: true })).toBeVisible({ timeout: 10_000 });
  await aside.getByText(`KV-${nodeId}`, { exact: true }).click({ button: 'right' });
  const stop = page.waitForResponse((r: any) => r.url().includes('/server/stop'));
  await page.getByRole('menuitem', { name: /stop Crow Storage/i }).click();
  await stop;
}

async function restartServerViaMenu(page: import('@playwright/test').Page, nodeId: number) {
  const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(aside.getByText(`KV-${nodeId}`, { exact: true })).toBeVisible({ timeout: 10_000 });
  await aside.getByText(`KV-${nodeId}`, { exact: true }).click({ button: 'right' });
  const restart = page.waitForResponse((r: any) => r.url().includes('/server/restart'));
  await page.getByRole('menuitem', { name: /restart Crow Storage/i }).click();
  await restart;
}

async function deleteNodeViaMenu(page: import('@playwright/test').Page, nodeId: number) {
  const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 10_000 });
  await aside.getByText(`N-${nodeId}`, { exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: /delete node/i }).click();
  const dialog = page.getByRole('dialog', { name: /delete node/i });
  await expect(dialog).toBeVisible();
  await dialog.getByRole('button', { name: /delete node/i }).evaluate((el: any) => (el as HTMLElement).click());
  // Wait for the node to disappear from the tree (cascade stops + removes
  // the server, then removes the node).
  await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toHaveCount(0, { timeout: 10_000 });
}

async function putKeyUi(page: import('@playwright/test').Page, key: string, value: string) {
  await page.getByLabel('Put key').fill(key);
  await page.getByLabel('Put value').fill(value);
  const put = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
  await page.getByRole('button', { name: /^Put$/ }).click();
  const resp = await put;
  expect(resp.ok(), await resp.text()).toBeTruthy();
}

/** UI get via the KV panel; returns the value or null when not found. */
async function getKeyUi(page: import('@playwright/test').Page, key: string): Promise<string | null> {
  await page.getByLabel('Get key').fill(key);
  const get = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
  await page.getByRole('button', { name: /^Get$/ }).click();
  const resp = await get;
  expect(resp.ok(), await resp.text()).toBeTruthy();
  const body = await resp.json();
  return body.found ? (body.value_utf8 ?? '') : null;
}

test.describe('kv cluster · reconfiguration', () => {
  // These tests drive multi-step UI flows (context menus, dialogs, tree
  // health polling, KV panel ops) across view switches — heavier than the
  // 30s default.
  test.describe.configure({ timeout: 120_000 });

  test('stopping a non-leader keeps quorum, stopping the leader triggers reelection', async ({ page, baseURL }) => {
    // --- stopping a non-leader node preserves quorum and KV ops (store 420) ---
    await resetAll(baseURL!);

    for (const r of [421, 422, 423]) {
      await seedRackAndNode(baseURL!, r, r);
    }
    await Promise.all([
      deployNodeServer(baseURL!, 421, freePort(), freePort()),
      deployNodeServer(baseURL!, 422, freePort(), freePort()),
      deployNodeServer(baseURL!, 423, freePort(), freePort()),
    ]);

    await createStore(baseURL!, 420, [421, 422, 423]);
    await addGroup(baseURL!, 420, 4200, 42000, [421, 422, 423]);
    await waitForLeader(baseURL!, 420, 4200);

    try {
      await kvPut(baseURL!, 420, 4200, 'stop42-key', 'val42');

      const leaderNode = await findLeaderNode(baseURL!, 420, 4200);
      expect(leaderNode).not.toBeNull();
      const stopNode = [421, 422, 423].find((n) => n !== leaderNode)!;

      // Stop the non-leader server via the Physical-view context menu.
      await openPhysical(page);
      await stopServerViaMenu(page, stopNode);

      // Tree health: the stopped server's badge drops from Healthy.
      await expect(serverHealthBadge(page, stopNode).filter({ hasText: 'Healthy' })).toHaveCount(0, { timeout: 10_000 });

      // Quorum intact: KV ops through the UI panel still succeed.
      await openKvPanel(page, 420, 4200);
      await putKeyUi(page, 'stop42-key2', 'val42b');
      expect(await getKeyUi(page, 'stop42-key2')).toBe('val42b');
      expect(await getKeyUi(page, 'stop42-key')).toBe('val42');

      // Restart via the context menu; poll the API until the node rejoins.
      await openPhysical(page);
      await restartServerViaMenu(page, stopNode);
      await waitForReachableReplicas(baseURL!, 420, 4200, 3);
    } finally {
      await stopNodeServer(baseURL!, 421);
      await stopNodeServer(baseURL!, 422);
      await stopNodeServer(baseURL!, 423);
    }

    // --- stopping the leader triggers reelection and KV ops continue (store 430) ---
    await resetAll(baseURL!);

    for (const r of [431, 432, 433]) {
      await seedRackAndNode(baseURL!, r, r);
    }
    await Promise.all([
      deployNodeServer(baseURL!, 431, freePort(), freePort()),
      deployNodeServer(baseURL!, 432, freePort(), freePort()),
      deployNodeServer(baseURL!, 433, freePort(), freePort()),
    ]);

    await createStore(baseURL!, 430, [431, 432, 433]);
    await addGroup(baseURL!, 430, 4300, 43000, [431, 432, 433]);
    await waitForLeader(baseURL!, 430, 4300);

    try {
      await kvPut(baseURL!, 430, 4300, 'reelect43-key', 'val43');

      const leaderNode = await findLeaderNode(baseURL!, 430, 4300);
      expect(leaderNode, 'leader should be elected').not.toBeNull();

      // Stop the leader via the context menu.
      await openPhysical(page);
      await stopServerViaMenu(page, leaderNode!);
      await expect(serverHealthBadge(page, leaderNode!).filter({ hasText: 'Healthy' })).toHaveCount(0, { timeout: 10_000 });

      // A new leader is elected (API poll) and KV ops through the UI still succeed.
      await waitForReelection(baseURL!, 430, 4300);
      await openKvPanel(page, 430, 4300);
      await putKeyUi(page, 'reelect43-key2', 'val43b');
      expect(await getKeyUi(page, 'reelect43-key2')).toBe('val43b');
      expect(await getKeyUi(page, 'reelect43-key')).toBe('val43');

      // Restart the old leader; poll the API until all 3 replicas rejoin.
      await openPhysical(page);
      await restartServerViaMenu(page, leaderNode!);
      await waitForReachableReplicas(baseURL!, 430, 4300, 3);
    } finally {
      await stopNodeServer(baseURL!, 431);
      await stopNodeServer(baseURL!, 432);
      await stopNodeServer(baseURL!, 433);
    }
  });

  test('deleting non-leader nodes preserves quorum down to majority', async ({ page, baseURL }) => {
    await resetAll(baseURL!);

    for (const r of [441, 442, 443, 444, 445]) {
      await seedRackAndNode(baseURL!, r, r);
    }
    await Promise.all([
      deployNodeServer(baseURL!, 441, freePort(), freePort()),
      deployNodeServer(baseURL!, 442, freePort(), freePort()),
      deployNodeServer(baseURL!, 443, freePort(), freePort()),
      deployNodeServer(baseURL!, 444, freePort(), freePort()),
      deployNodeServer(baseURL!, 445, freePort(), freePort()),
    ]);

    await createStore(baseURL!, 440, [441, 442, 443, 444, 445]);
    await addGroup(baseURL!, 440, 4400, 44000, [441, 442, 443, 444, 445]);
    await waitForLeader(baseURL!, 440, 4400);

    try {
      await kvPut(baseURL!, 440, 4400, 'del44-key', 'val44');

      const leaderNode = await findLeaderNode(baseURL!, 440, 4400);
      expect(leaderNode).not.toBeNull();
      const nonLeader1 = [441, 442, 443, 444, 445].find((n) => n !== leaderNode)!;

      // Delete the first non-leader node via the context menu + confirm dialog.
      await openPhysical(page);
      await deleteNodeViaMenu(page, nonLeader1);

      // Quorum intact (4-of-5): KV ops through the UI still succeed.
      await openKvPanel(page, 440, 4400);
      await putKeyUi(page, 'del44-key2', 'val44b');
      expect(await getKeyUi(page, 'del44-key2')).toBe('val44b');

      // Delete a second non-leader node (down to 3 replicas — exactly majority).
      const nonLeader2 = [441, 442, 443, 444, 445].find((n) => n !== leaderNode && n !== nonLeader1)!;
      await openPhysical(page);
      await deleteNodeViaMenu(page, nonLeader2);

      await openKvPanel(page, 440, 4400);
      await putKeyUi(page, 'del44-key3', 'val44c');
      expect(await getKeyUi(page, 'del44-key3')).toBe('val44c');
      expect(await getKeyUi(page, 'del44-key')).toBe('val44');
    } finally {
      for (const n of [441, 442, 443, 444, 445]) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });

  test('4th replica catches up and group still accepts writes', async ({ page, baseURL }) => {
    await resetAll(baseURL!);

    for (const r of [451, 452, 453, 454]) {
      await seedRackAndNode(baseURL!, r, r);
    }
    await Promise.all([
      deployNodeServer(baseURL!, 451, freePort(), freePort()),
      deployNodeServer(baseURL!, 452, freePort(), freePort()),
      deployNodeServer(baseURL!, 453, freePort(), freePort()),
      deployNodeServer(baseURL!, 454, freePort(), freePort()),
    ]);

    await createStore(baseURL!, 450, [451, 452, 453, 454]);
    await addGroup(baseURL!, 450, 4500, 45000, [451, 452, 453]);
    await waitForLeader(baseURL!, 450, 4500);

    try {
      // Put data before adding the replica.
      await kvPut(baseURL!, 450, 4500, 'add45-key1', 'val1');
      await kvPut(baseURL!, 450, 4500, 'add45-key2', 'val2');

      // Add the 4th replica via the KV-Cluster Add Replica dialog.
      await openKvCluster(page);
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await expect(aside.getByText('G-4500')).toBeVisible({ timeout: 5_000 });
      await aside.getByText('G-4500').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add replica/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Replica' })).toBeVisible();
      await page.getByLabel('Node', { exact: true }).selectOption(String(454));
      const addResp = page.waitForResponse((r: any) => r.url().includes('/replicas'));
      await page.getByRole('button', { name: /add replica/i }).click();
      await addResp;

      // The new replica (LR-45003) appears in the logical tree.
      await expect(aside.getByText('LR-45003')).toBeVisible({ timeout: 10_000 });

      // Catch-up: poll the API until the new replica reports state = running,
      // proving it replicated the existing data — not just that it appeared
      // in the group status.
      await waitForReplicaRunning(baseURL!, 450, 4500, 45003);

      // Group still accepts writes through the UI.
      await openKvPanel(page, 450, 4500);
      await putKeyUi(page, 'add45-key3', 'val3');
      expect(await getKeyUi(page, 'add45-key3')).toBe('val3');

      // Pre-existing keys are readable (replicated to the new replica).
      expect(await getKeyUi(page, 'add45-key1')).toBe('val1');
      expect(await getKeyUi(page, 'add45-key2')).toBe('val2');
    } finally {
      for (const n of [451, 452, 453, 454]) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });

  test('stopping shared node degrades both stores, restart recovers', async ({ page, baseURL }) => {
    await resetAll(baseURL!);

    for (const r of [461, 462, 463, 464, 465]) {
      await seedRackAndNode(baseURL!, r, r);
    }
    await Promise.all([
      deployNodeServer(baseURL!, 461, freePort(), freePort()),
      deployNodeServer(baseURL!, 462, freePort(), freePort()),
      deployNodeServer(baseURL!, 463, freePort(), freePort()),
      deployNodeServer(baseURL!, 464, freePort(), freePort()),
      deployNodeServer(baseURL!, 465, freePort(), freePort()),
    ]);

    await createStore(baseURL!, 460, [461, 462, 463]);
    await addGroup(baseURL!, 460, 4600, 46000, [461, 462, 463]);
    await waitForLeader(baseURL!, 460, 4600);

    await createStore(baseURL!, 461, [463, 464, 465]);
    await addGroup(baseURL!, 461, 4610, 46100, [463, 464, 465]);
    await waitForLeader(baseURL!, 461, 4610);

    try {
      await kvPut(baseURL!, 460, 4600, 'ms46-a-key', 'val-a');
      await kvPut(baseURL!, 461, 4610, 'ms46-b-key', 'val-b');

      const leaderA = await findLeaderNode(baseURL!, 460, 4600);
      const leaderB = await findLeaderNode(baseURL!, 461, 4610);

      // Prefer the overlap node (463); if it leads either store, stop a
      // non-leader from store A instead.
      const stopNode = leaderA !== 463 && leaderB !== 463 ? 463 : leaderA === 461 ? 462 : 461;

      // Stop via the Physical-view context menu.
      await openPhysical(page);
      await stopServerViaMenu(page, stopNode);
      await expect(serverHealthBadge(page, stopNode).filter({ hasText: 'Healthy' })).toHaveCount(0, { timeout: 10_000 });

      // Store A still accepts writes through the UI (quorum 2-of-3).
      await openKvPanel(page, 460, 4600);
      await putKeyUi(page, 'ms46-a-key2', 'val-a2');
      expect(await getKeyUi(page, 'ms46-a-key2')).toBe('val-a2');

      // If the overlap node was stopped, store B also lost a replica but
      // keeps quorum 2-of-3.
      if (stopNode === 463) {
        await openKvPanel(page, 461, 4610);
        await putKeyUi(page, 'ms46-b-key2', 'val-b2');
        expect(await getKeyUi(page, 'ms46-b-key2')).toBe('val-b2');
      }

      // Restart; poll the API until store A has all replicas reachable.
      await openPhysical(page);
      await restartServerViaMenu(page, stopNode);
      await waitForReachableReplicas(baseURL!, 460, 4600, 3);

      // Original keys still readable after recovery.
      await openKvPanel(page, 460, 4600);
      expect(await getKeyUi(page, 'ms46-a-key')).toBe('val-a');
      await openKvPanel(page, 461, 4610);
      expect(await getKeyUi(page, 'ms46-b-key')).toBe('val-b');
    } finally {
      for (const n of [461, 462, 463, 464, 465]) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });
});
