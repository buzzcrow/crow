// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

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

test.describe('E2E-42 stop server keeps group', () => {
  test('stopping non-leader node preserves quorum and KV ops', async ({ baseURL }) => {
    await resetAll(baseURL!);

    // 3-node group on n42a, n42b, n42c
    for (const r of ['r42a', 'r42b', 'r42c']) {
      await seedRackAndNode(baseURL!, r, r.replace('r', 'n'));
    }
    await deployNodeServer(baseURL!, 'n42a', 9970, 9971);
    await deployNodeServer(baseURL!, 'n42b', 9972, 9973);
    await deployNodeServer(baseURL!, 'n42c', 9974, 9975);

    await createStore(baseURL!, 420, ['n42a', 'n42b', 'n42c']);
    await addGroup(baseURL!, 420, 4200, 42000, ['n42a', 'n42b', 'n42c']);
    await waitForLeader(baseURL!, 420, 4200);

    try {
      // Put a key before stopping
      await kvPut(baseURL!, 420, 4200, 'stop42-key', 'val42');
      expect(await kvGet(baseURL!, 420, 4200, 'stop42-key')).toBe('val42');

      // Find the leader, then stop a non-leader node
      const leaderNode = await findLeaderNode(baseURL!, 420, 4200);
      expect(leaderNode).not.toBeNull();
      const nonLeaderNodes = ['n42a', 'n42b', 'n42c'].filter((n) => n !== leaderNode);
      const stopNode = nonLeaderNodes[0];

      // Stop the non-leader server via console API
      const api = await apiContext(baseURL!);
      const stopResp = await api.post(`/api/nodes/${stopNode}/server/stop`);
      expect(stopResp.ok(), await stopResp.text()).toBeTruthy();
      await api.dispose();

      // Group should still accept puts/gets (quorum 2-of-3 intact)
      await kvPut(baseURL!, 420, 4200, 'stop42-key2', 'val42b');
      expect(await kvGet(baseURL!, 420, 4200, 'stop42-key2')).toBe('val42b');
      // Original key still readable
      expect(await kvGet(baseURL!, 420, 4200, 'stop42-key')).toBe('val42');

      // Restart the stopped server
      const api2 = await apiContext(baseURL!);
      const restartResp = await api2.post(`/api/nodes/${stopNode}/server/restart`);
      expect(restartResp.ok(), await restartResp.text()).toBeTruthy();
      await api2.dispose();

      // Wait for the restarted node to rejoin — poll group status until 3 reachable replicas
      await expect.poll(async () => {
        const body = await getGroupStatus(baseURL!, 420, 4200);
        const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
        return replicas.filter((r) => r.status !== 'unhealthy').length;
      }, { timeout: 10_000 }).toBeGreaterThanOrEqual(3);
    } finally {
      await stopNodeServer(baseURL!, 'n42a');
      await stopNodeServer(baseURL!, 'n42b');
      await stopNodeServer(baseURL!, 'n42c');
    }
  });
});
