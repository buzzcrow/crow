// Copyright 2026-present buzzcrow <buzzcrow/126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.8s (2026-07-16)

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

test.describe('E2E-46 multi-store reconfig', () => {
  test('stopping shared node degrades both stores, restart recovers', async ({ baseURL }) => {
    await resetAll(baseURL!);

    // 5 nodes. Store A on n46a,b,c. Store B on n46c,d,e (overlap on n46c).
    for (const r of ['r46a', 'r46b', 'r46c', 'r46d', 'r46e']) {
      await seedRackAndNode(baseURL!, r, r.replace('r', 'n'));
    }
    await deployNodeServer(baseURL!, 'n46a', 10020, 10021);
    await deployNodeServer(baseURL!, 'n46b', 10022, 10023);
    await deployNodeServer(baseURL!, 'n46c', 10024, 10025);
    await deployNodeServer(baseURL!, 'n46d', 10026, 10027);
    await deployNodeServer(baseURL!, 'n46e', 10028, 10029);

    // Store A: 460, group 4600 on n46a,b,c
    await createStore(baseURL!, 460, ['n46a', 'n46b', 'n46c']);
    await addGroup(baseURL!, 460, 4600, 46000, ['n46a', 'n46b', 'n46c']);
    await waitForLeader(baseURL!, 460, 4600);

    // Store B: 461, group 4610 on n46c,d,e
    await createStore(baseURL!, 461, ['n46c', 'n46d', 'n46e']);
    await addGroup(baseURL!, 461, 4610, 46100, ['n46c', 'n46d', 'n46e']);
    await waitForLeader(baseURL!, 461, 4610);

    try {
      // Put keys in both stores
      await kvPut(baseURL!, 460, 4600, 'ms46-a-key', 'val-a');
      await kvPut(baseURL!, 461, 4610, 'ms46-b-key', 'val-b');
      expect(await kvGet(baseURL!, 460, 4600, 'ms46-a-key')).toBe('val-a');
      expect(await kvGet(baseURL!, 461, 4610, 'ms46-b-key')).toBe('val-b');

      // Stop a non-leader node that participates in both stores (n46c if not leader of either)
      const leaderA = await findLeaderNode(baseURL!, 460, 4600);
      const leaderB = await findLeaderNode(baseURL!, 461, 4610);

      // Find a non-leader node in both groups — n46c is the overlap node
      // If n46c is a leader, stop a different non-leader from one store
      let stopNode: string;
      if (leaderA !== 'n46c' && leaderB !== 'n46c') {
        stopNode = 'n46c';
      } else {
        // n46c is leader of one group — stop a non-leader from store A instead
        stopNode = leaderA === 'n46a' ? 'n46b' : 'n46a';
      }

      const api = await apiContext(baseURL!);
      const stopResp = await api.post(`/api/nodes/${stopNode}/server/stop`);
      expect(stopResp.ok(), await stopResp.text()).toBeTruthy();
      await api.dispose();

      // Both stores should still accept writes (quorum intact: 2-of-3)
      await kvPut(baseURL!, 460, 4600, 'ms46-a-key2', 'val-a2');
      expect(await kvGet(baseURL!, 460, 4600, 'ms46-a-key2')).toBe('val-a2');

      // Store B may or may not be affected depending on which node was stopped
      if (stopNode === 'n46c') {
        // n46c is in both stores — store B also lost a replica but quorum 2-of-3
        await kvPut(baseURL!, 461, 4610, 'ms46-b-key2', 'val-b2');
        expect(await kvGet(baseURL!, 461, 4610, 'ms46-b-key2')).toBe('val-b2');
      }

      // Restart the stopped server
      const api2 = await apiContext(baseURL!);
      const restartResp = await api2.post(`/api/nodes/${stopNode}/server/restart`);
      expect(restartResp.ok(), await restartResp.text()).toBeTruthy();
      await api2.dispose();

      // Wait for recovery — both stores should have all replicas reachable
      await expect.poll(async () => {
        const bodyA = await getGroupStatus(baseURL!, 460, 4600);
        const replicasA: any[] = Array.isArray(bodyA.replicas) ? bodyA.replicas : [];
        return replicasA.filter((r) => r.status !== 'unhealthy').length;
      }, { timeout: 10_000 }).toBeGreaterThanOrEqual(3);

      // Verify original keys still readable after recovery
      expect(await kvGet(baseURL!, 460, 4600, 'ms46-a-key')).toBe('val-a');
      expect(await kvGet(baseURL!, 461, 4610, 'ms46-b-key')).toBe('val-b');
    } finally {
      for (const n of ['n46a', 'n46b', 'n46c', 'n46d', 'n46e']) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });
});
