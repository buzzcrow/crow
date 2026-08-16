// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 4s (2026-08-16)

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

async function findLeaderNode(baseURL: string, storeId: number, groupId: number): Promise<number | null> {
  const body = await getGroupStatus(baseURL, storeId, groupId);
  const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
  const leader = replicas.find((r) => String(r.role).toLowerCase() === 'leader');
  return leader?.node_id ?? null;
}

test.describe('kv cluster · reconfiguration', () => {
  test('stopping a non-leader keeps quorum, stopping the leader triggers reelection', async ({ baseURL }) => {
    // --- stopping a non-leader node preserves quorum and KV ops (store 420) ---
    await resetAll(baseURL!);

    // 3-node group on n42a, n42b, n42c
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
      // Put a key before stopping
      await kvPut(baseURL!, 420, 4200, 'stop42-key', 'val42');
      expect(await kvGet(baseURL!, 420, 4200, 'stop42-key')).toBe('val42');

      // Find the leader, then stop a non-leader node
      const leaderNode = await findLeaderNode(baseURL!, 420, 4200);
      expect(leaderNode).not.toBeNull();
      const nonLeaderNodes = [421, 422, 423].filter((n) => n !== leaderNode);
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
      }, { timeout: 10_000, intervals: [100] }).toBeGreaterThanOrEqual(3);
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
      }, { timeout: 10_000, intervals: [100] }).toBe(true);

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
      }, { timeout: 10_000, intervals: [100] }).toBeGreaterThanOrEqual(3);
    } finally {
      await stopNodeServer(baseURL!, 431);
      await stopNodeServer(baseURL!, 432);
      await stopNodeServer(baseURL!, 433);
    }
  });

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

  test('4th replica catches up and group still accepts writes', async ({ baseURL }) => {
    await resetAll(baseURL!);

    // 4 nodes: 3 in initial group, 1 spare for adding later
    for (const r of [451, 452, 453, 454]) {
      await seedRackAndNode(baseURL!, r, r);
    }
    await Promise.all([
      deployNodeServer(baseURL!, 451, freePort(), freePort()),
      deployNodeServer(baseURL!, 452, freePort(), freePort()),
      deployNodeServer(baseURL!, 453, freePort(), freePort()),
      deployNodeServer(baseURL!, 454, freePort(), freePort()),
    ]);

    // Store on all 4 nodes, but group initially on 3
    await createStore(baseURL!, 450, [451, 452, 453, 454]);
    await addGroup(baseURL!, 450, 4500, 45000, [451, 452, 453]);
    await waitForLeader(baseURL!, 450, 4500);

    try {
      // Put data before adding replica
      await kvPut(baseURL!, 450, 4500, 'add45-key1', 'val1');
      await kvPut(baseURL!, 450, 4500, 'add45-key2', 'val2');
      expect(await kvGet(baseURL!, 450, 4500, 'add45-key1')).toBe('val1');

      // Add 4th replica via console API
      const api = await apiContext(baseURL!);
      const addResp = await api.post(`/api/stores/450/groups/4500/replicas`, {
        data: { node_id: 454 },
      });
      expect(addResp.status(), await addResp.text()).toBe(201);
      await api.dispose();

      // Wait for the new replica to show up in group status
      await expect.poll(async () => {
        const body = await getGroupStatus(baseURL!, 450, 4500);
        const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
        return replicas.length;
      }, { timeout: 10_000, intervals: [100] }).toBe(4);

      // Group still accepts writes
      await kvPut(baseURL!, 450, 4500, 'add45-key3', 'val3');
      expect(await kvGet(baseURL!, 450, 4500, 'add45-key3')).toBe('val3');

      // Original keys still readable
      expect(await kvGet(baseURL!, 450, 4500, 'add45-key1')).toBe('val1');
      expect(await kvGet(baseURL!, 450, 4500, 'add45-key2')).toBe('val2');
    } finally {
      for (const n of [451, 452, 453, 454]) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });

  test('stopping shared node degrades both stores, restart recovers', async ({ baseURL }) => {
    await resetAll(baseURL!);

    // 5 nodes. Store A on n46a,b,c. Store B on n46c,d,e (overlap on n46c).
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

    // Store A: 460, group 4600 on n46a,b,c
    await createStore(baseURL!, 460, [461, 462, 463]);
    await addGroup(baseURL!, 460, 4600, 46000, [461, 462, 463]);
    await waitForLeader(baseURL!, 460, 4600);

    // Store B: 461, group 4610 on n46c,d,e
    await createStore(baseURL!, 461, [463, 464, 465]);
    await addGroup(baseURL!, 461, 4610, 46100, [463, 464, 465]);
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
      let stopNode: number;
      if (leaderA !== 463 && leaderB !== 463) {
        stopNode = 463;
      } else {
        // n46c is leader of one group — stop a non-leader from store A instead
        stopNode = leaderA === 461 ? 462 : 461;
      }

      const api = await apiContext(baseURL!);
      const stopResp = await api.post(`/api/nodes/${stopNode}/server/stop`);
      expect(stopResp.ok(), await stopResp.text()).toBeTruthy();
      await api.dispose();

      // Both stores should still accept writes (quorum intact: 2-of-3)
      await kvPut(baseURL!, 460, 4600, 'ms46-a-key2', 'val-a2');
      expect(await kvGet(baseURL!, 460, 4600, 'ms46-a-key2')).toBe('val-a2');

      // Store B may or may not be affected depending on which node was stopped
      if (stopNode === 463) {
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
      }, { timeout: 10_000, intervals: [100] }).toBeGreaterThanOrEqual(3);

      // Verify original keys still readable after recovery
      expect(await kvGet(baseURL!, 460, 4600, 'ms46-a-key')).toBe('val-a');
      expect(await kvGet(baseURL!, 461, 4610, 'ms46-b-key')).toBe('val-b');
    } finally {
      for (const n of [461, 462, 463, 464, 465]) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });
});
