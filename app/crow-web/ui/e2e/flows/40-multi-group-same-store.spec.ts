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

async function kvScanAll(baseURL: string, storeId: number, groupId: number): Promise<string[]> {
  const keys: string[] = [];
  let startAfter = '';
  for (;;) {
    const url = `/api/stores/${storeId}/groups/${groupId}/kv/scan?limit=500${startAfter ? `&start_after=${encodeURIComponent(startAfter)}` : ''}`;
    const resp = await fetch(`${baseURL}${url}`);
    expect(resp.ok).toBeTruthy();
    const body = await resp.json();
    keys.push(...body.items.map((i: any) => i.key_utf8));
    if (!body.truncated) break;
    startAfter = body.items[body.items.length - 1]?.key_utf8 ?? '';
    if (!startAfter) break;
  }
  return keys;
}

test.describe('E2E-40 multi-group same store', () => {
  test('3 groups on different node subsets operate independently', async ({ baseURL }) => {
    await resetAll(baseURL!);

    // 5 nodes, 1 store, 3 groups on different 3-node subsets.
    for (const r of [401, 402, 403, 404, 405]) {

      await seedRackAndNode(baseURL!, r, r);
    }
    await deployNodeServer(baseURL!, 401, 9960, 9961);
    await deployNodeServer(baseURL!, 402, 9962, 9963);
    await deployNodeServer(baseURL!, 403, 9964, 9965);
    await deployNodeServer(baseURL!, 404, 9966, 9967);
    await deployNodeServer(baseURL!, 405, 9968, 9969);

    await createStore(baseURL!, 400, [401, 402, 403, 404, 405]);

    // Group 4000: n40a,b,c. Group 4001: n40b,c,d. Group 4002: n40c,d,e.
    await addGroup(baseURL!, 400, 4000, 40000, [401, 402, 403]);
    await addGroup(baseURL!, 400, 4001, 40010, [402, 403, 404]);
    await addGroup(baseURL!, 400, 4002, 40020, [403, 404, 405]);
    await waitForLeader(baseURL!, 400, 4000);
    await waitForLeader(baseURL!, 400, 4001);
    await waitForLeader(baseURL!, 400, 4002);

    try {
      // Each group gets its own keys
      await kvPut(baseURL!, 400, 4000, 'mg40-key0', 'val0');
      await kvPut(baseURL!, 400, 4001, 'mg40-key1', 'val1');
      await kvPut(baseURL!, 400, 4002, 'mg40-key2', 'val2');

      // Verify per-group get
      expect(await kvGet(baseURL!, 400, 4000, 'mg40-key0')).toBe('val0');
      expect(await kvGet(baseURL!, 400, 4001, 'mg40-key1')).toBe('val1');
      expect(await kvGet(baseURL!, 400, 4002, 'mg40-key2')).toBe('val2');

      // Cross-group isolation: key0 not visible in group 1 or 2
      expect(await kvGet(baseURL!, 400, 4001, 'mg40-key0')).toBeNull();
      expect(await kvGet(baseURL!, 400, 4002, 'mg40-key0')).toBeNull();

      // Per-group scan: each group only has its own keys
      const scan0 = await kvScanAll(baseURL!, 400, 4000);
      expect(scan0).toContain('mg40-key0');
      expect(scan0).not.toContain('mg40-key1');
      expect(scan0).not.toContain('mg40-key2');

      const scan1 = await kvScanAll(baseURL!, 400, 4001);
      expect(scan1).toContain('mg40-key1');
      expect(scan1).not.toContain('mg40-key0');
      expect(scan1).not.toContain('mg40-key2');

      const scan2 = await kvScanAll(baseURL!, 400, 4002);
      expect(scan2).toContain('mg40-key2');
      expect(scan2).not.toContain('mg40-key0');
      expect(scan2).not.toContain('mg40-key1');
    } finally {
      for (const n of ['n40a', 'n40b', 'n40c', 'n40d', 'n40e']) {
        await stopNodeServer(baseURL!, n);
      }
    }
  });
});
