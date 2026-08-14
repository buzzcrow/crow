// Copyright 2026-present buzzcrow <buzzcrow/126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.7s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, seedRackAndNode, stopNodeServer, waitForLeader, resetAll, apiContext } from '../fixtures/consoleSetup';

async function kvPut(baseURL: string, storeId: number, groupId: number, key: string, value: string) {
  const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/put`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key, value }),
  });
  expect(resp.ok).toBeTruthy();
}

async function kvGet(baseURL: string, storeId: number, groupId: number, key: string): Promise<number | null> {
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

async function findLeaderNode(baseURL: string, storeId: number, groupId: number): Promise<number | null> {
  const body = await getGroupStatus(baseURL, storeId, groupId);
  const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
  const leader = replicas.find((r) => String(r.role).toLowerCase() === 'leader');
  return leader?.node_id ?? null;
}

test.describe('E2E-44 delete node after group', () => {
  test('deleting non-leader nodes preserves quorum down to majority', async ({ baseURL }) => {
    await resetAll(baseURL!);

    // 5-node group
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
      // Put initial key
      await kvPut(baseURL!, 440, 4400, 'del44-key', 'val44');
      expect(await kvGet(baseURL!, 440, 4400, 'del44-key')).toBe('val44');

      const leaderNode = await findLeaderNode(baseURL!, 440, 4400);
      expect(leaderNode).not.toBeNull();

      // Delete first non-leader node (quorum 4-of-5)
      const nonLeader1 = [441, 442, 443, 444, 445].filter((n) => n !== leaderNode)[0];
      const api1 = await apiContext(baseURL!);
      // Stop server, then delete deployment record, then delete node
      await api1.post(`/api/nodes/${nonLeader1}/server/stop`);
      await api1.delete(`/api/nodes/${nonLeader1}/server`);
      const del1 = await api1.delete(`/api/nodes/${nonLeader1}`);
      expect(del1.ok(), await del1.text()).toBeTruthy();
      await api1.dispose();

      // Group still operates (quorum 4-of-5, need 3)
      await kvPut(baseURL!, 440, 4400, 'del44-key2', 'val44b');
      expect(await kvGet(baseURL!, 440, 4400, 'del44-key2')).toBe('val44b');

      // Delete second non-leader node (quorum 3-of-5, need 3 — still majority)
      const remainingNodes = [441, 442, 443, 444, 445].filter((n) => n !== leaderNode && n !== nonLeader1);
      const nonLeader2 = remainingNodes[0];
      const api2 = await apiContext(baseURL!);
      await api2.post(`/api/nodes/${nonLeader2}/server/stop`);
      await api2.delete(`/api/nodes/${nonLeader2}/server`);
      const del2 = await api2.delete(`/api/nodes/${nonLeader2}`);
      expect(del2.ok(), await del2.text()).toBeTruthy();
      await api2.dispose();

      // Group still operates (quorum 3-of-5, need 3 — exactly majority)
      await kvPut(baseURL!, 440, 4400, 'del44-key3', 'val44c');
      expect(await kvGet(baseURL!, 440, 4400, 'del44-key3')).toBe('val44c');
      // Original key still readable
      expect(await kvGet(baseURL!, 440, 4400, 'del44-key')).toBe('val44');
    } finally {
      // Stop all remaining servers
      for (const n of [441, 442, 443, 444, 445]) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });
});
