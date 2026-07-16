// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { setupCluster, teardownCluster, SIMPLE, COMPLEX, resetAll, apiContext } from '../fixtures/consoleSetup';

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

async function runSmokeSuite(baseURL: string, topo: typeof SIMPLE, label: string) {
  await resetAll(baseURL);
  const cluster = await setupCluster(baseURL, topo);

  try {
    // Verify all stores and groups have leaders
    for (const g of cluster.groups) {
      const api = await apiContext(baseURL);
      try {
        const r = await api.get(`/api/stores/${g.storeId}/groups/${g.groupId}`);
        expect(r.ok(), await r.text()).toBeTruthy();
        const body = await r.json();
        const hasLeader =
          (Array.isArray(body.replicas) && body.replicas.some((x: any) => String(x.role).toLowerCase() === 'leader')) ||
          (typeof body.leader_id === 'number' && body.leader_id > 0);
        expect(hasLeader, `${label}: no leader for store ${g.storeId} group ${g.groupId}`).toBe(true);
      } finally {
        await api.dispose();
      }
    }

    // KV put/get on first group
    const firstGroup = cluster.groups[0];
    const testKey = `cmp-${label}-key`;
    const testValue = `cmp-${label}-value`;
    await kvPut(baseURL, firstGroup.storeId, firstGroup.groupId, testKey, testValue);
    expect(await kvGet(baseURL, firstGroup.storeId, firstGroup.groupId, testKey)).toBe(testValue);

    // Verify stores exist via API
    const api = await apiContext(baseURL);
    try {
      const storesResp = await api.get('/api/stores');
      expect(storesResp.ok(), await storesResp.text()).toBeTruthy();
      const stores = await storesResp.json();
      for (const sid of cluster.stores) {
        expect(stores).toEqual(expect.arrayContaining([expect.objectContaining({ store_id: sid })]));
      }
    } finally {
      await api.dispose();
    }
  } finally {
    await teardownCluster(baseURL, cluster);
  }
}

test.describe('E2E-41 comparative standard suite', () => {
  test('smoke suite passes on SIMPLE topology (3 nodes, 1 store, 1 group)', async ({ baseURL }) => {
    await runSmokeSuite(baseURL!, SIMPLE, 'simple');
  });

  test('smoke suite passes on COMPLEX topology (8 nodes, 2 stores, 4 groups)', async ({ baseURL }) => {
    await runSmokeSuite(baseURL!, COMPLEX, 'complex');
  });
});
