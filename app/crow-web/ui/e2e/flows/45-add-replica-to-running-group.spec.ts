// Copyright 2026-present buzzcrow <buzzcrow/126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.6s (2026-07-16)

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

test.describe('E2E-45 add replica to running group', () => {
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
      for (const n of ['n45a', 'n45b', 'n45c', 'n45d']) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });
});
