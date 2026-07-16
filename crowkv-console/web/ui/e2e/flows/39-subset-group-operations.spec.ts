// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.7s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

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

async function kvDelete(baseURL: string, storeId: number, groupId: number, key: string) {
  const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/delete`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key }),
  });
  expect(resp.ok).toBeTruthy();
}

test.describe('E2E-39 subset group operations', () => {
  test('two groups on overlapping 3-node subsets operate independently', async ({ baseURL }) => {
    await resetAll(baseURL!);

    // 5 nodes total. Group A on n39a,b,c. Group B on n39c,d,e (overlap on n39c).
    for (const r of ['r39a', 'r39b', 'r39c', 'r39d', 'r39e']) {
      await seedRackAndNode(baseURL!, r, r.replace('r', 'n'));
    }
    await deployNodeServer(baseURL!, 'n39a', 9950, 9951);
    await deployNodeServer(baseURL!, 'n39b', 9952, 9953);
    await deployNodeServer(baseURL!, 'n39c', 9954, 9955);
    await deployNodeServer(baseURL!, 'n39d', 9956, 9957);
    await deployNodeServer(baseURL!, 'n39e', 9958, 9959);

    // Single store, two groups on different node subsets.
    await createStore(baseURL!, 390, ['n39a', 'n39b', 'n39c', 'n39d', 'n39e']);
    await addGroup(baseURL!, 390, 3900, 39000, ['n39a', 'n39b', 'n39c']);
    await addGroup(baseURL!, 390, 3901, 39010, ['n39c', 'n39d', 'n39e']);
    await waitForLeader(baseURL!, 390, 3900);
    await waitForLeader(baseURL!, 390, 3901);

    try {
      // Group A: put, get, delete
      await kvPut(baseURL!, 390, 3900, 'g39a-key', 'val-a');
      expect(await kvGet(baseURL!, 390, 3900, 'g39a-key')).toBe('val-a');

      // Group B: put, get, delete
      await kvPut(baseURL!, 390, 3901, 'g39b-key', 'val-b');
      expect(await kvGet(baseURL!, 390, 3901, 'g39b-key')).toBe('val-b');

      // Cross-group get: key from group A should not be visible in group B
      expect(await kvGet(baseURL!, 390, 3901, 'g39a-key')).toBeNull();

      // Delete in group A, verify gone from A but B still has its key
      await kvDelete(baseURL!, 390, 3900, 'g39a-key');
      expect(await kvGet(baseURL!, 390, 3900, 'g39a-key')).toBeNull();
      expect(await kvGet(baseURL!, 390, 3901, 'g39b-key')).toBe('val-b');
    } finally {
      for (const n of ['n39a', 'n39b', 'n39c', 'n39d', 'n39e']) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });
});
