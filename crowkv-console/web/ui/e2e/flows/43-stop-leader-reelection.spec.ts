// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 8.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll, apiContext } from '../fixtures/consoleSetup';

async function kvPut(baseURL: string, storeId: number, groupId: number, key: string, value: string) {
  const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/put`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key, value }),
  });
  expect(resp.ok).toBeTruthy();
}

async function kvGet(baseURL: string, storeId: number, groupId: number, key: string): Promise<string | null> {
  const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/get?key=${encodeURIComponent(key)}`);
  expect(resp.ok).toBeTruthy();
  const body = await resp.json();
  return body.found ? body.value_utf8 : null;
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

async function findLeaderNode(baseURL: string, storeId: number, groupId: number): Promise<string | null> {
  const body = await getGroupStatus(baseURL, storeId, groupId);
  const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
  const leader = replicas.find((r) => String(r.role).toLowerCase() === 'leader');
  return leader?.node_id ?? null;
}

test.describe('E2E-43 stop leader reelection', () => {
  test('stopping leader triggers reelection and KV ops continue', async ({ baseURL }) => {
    await resetAll(baseURL!);

    for (const r of ['r43a', 'r43b', 'r43c']) {
      await seedRackAndNode(baseURL!, r, r.replace('r', 'n'));
    }
    await deployNodeServer(baseURL!, 'n43a', 9980, 9981);
    await deployNodeServer(baseURL!, 'n43b', 9982, 9983);
    await deployNodeServer(baseURL!, 'n43c', 9984, 9985);

    await createStore(baseURL!, 430, ['n43a', 'n43b', 'n43c']);
    await addGroup(baseURL!, 430, 4300, 43000, ['n43a', 'n43b', 'n43c']);
    await waitForLeader(baseURL!, 430, 4300);

    try {
      // Put a key before stopping leader
      await kvPut(baseURL!, 430, 4300, 'reelect43-key', 'val43');
      expect(await kvGet(baseURL!, 430, 4300, 'reelect43-key')).toBe('val43');

      // Find and stop the leader
      const leaderNode = await findLeaderNode(baseURL!, 430, 4300);
      expect(leaderNode, 'leader should be elected').not.toBeNull();

      const api = await apiContext(baseURL!);
      const stopResp = await api.post(`/api/nodes/${leaderNode}/server/stop`);
      expect(stopResp.ok(), await stopResp.text()).toBeTruthy();
      await api.dispose();

      // A new leader should be elected within 10s (quorum 2-of-3)
      await expect.poll(async () => {
        const body = await getGroupStatus(baseURL!, 430, 4300);
        const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
        return replicas.some((r) => String(r.role).toLowerCase() === 'leader');
      }, { timeout: 10_000 }).toBe(true);

      // KV put/get still works after reelection
      await kvPut(baseURL!, 430, 4300, 'reelect43-key2', 'val43b');
      expect(await kvGet(baseURL!, 430, 4300, 'reelect43-key2')).toBe('val43b');
      // Original key still readable
      expect(await kvGet(baseURL!, 430, 4300, 'reelect43-key')).toBe('val43');

      // Restart the old leader, verify it rejoins
      const api2 = await apiContext(baseURL!);
      const restartResp = await api2.post(`/api/nodes/${leaderNode}/server/restart`);
      expect(restartResp.ok(), await restartResp.text()).toBeTruthy();
      await api2.dispose();

      // Wait for all 3 replicas to be reachable again
      await expect.poll(async () => {
        const body = await getGroupStatus(baseURL!, 430, 4300);
        const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
        return replicas.filter((r) => r.status !== 'unhealthy').length;
      }, { timeout: 10_000 }).toBeGreaterThanOrEqual(3);
    } finally {
      await stopNodeServer(baseURL!, 'n43a');
      await stopNodeServer(baseURL!, 'n43b');
      await stopNodeServer(baseURL!, 'n43c');
    }
  });
});
