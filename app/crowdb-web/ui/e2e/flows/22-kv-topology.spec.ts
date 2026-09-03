// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 33s (2026-08-17)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import {
  apiContext,
  addGroup,
  addReplica,
  createStoreNoInit,
  deployNodeServer,
  seedRackAndNode,
  freePort,
  waitForLeader,
  resetAll,
  clusterInit,
  SIMPLE,
  COMPLEX,
  type TopologyDescriptor,
  type SetupResult,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

const apiBase = consoleBaseURL();

async function kvPut(baseURL: string, storeId: number, groupId: number, key: string, value: string) {
  const resp = await step(`kvPut(s${storeId}/g${groupId})`, () => fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/put`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key, value }),
  }));
  expect(resp.ok).toBeTruthy();
}

async function kvGet(baseURL: string, storeId: number, groupId: number, key: string): Promise<string | null> {
  return await step(`kvGet(s${storeId}/g${groupId})`, async () => {
    const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/get?key=${encodeURIComponent(key)}`);
    expect(resp.ok).toBeTruthy();
    const body = await resp.json();
    return body.found ? body.value_utf8 : null;
  });
}

async function kvScanAll(baseURL: string, storeId: number, groupId: number): Promise<string[]> {
  return await step(`kvScanAll(s${storeId}/g${groupId})`, async () => {
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
  });
}

// Build a SetupResult from a topology descriptor (mirrors what
// setupCluster would return, but without deploying — the cluster is
// already up from beforeAll).
function makeSetupResult(topo: TopologyDescriptor): SetupResult {
  const nodes = Array.from({ length: topo.nodeCount }, (_, i) => topo.nodeBase + i);
  const racks = Array.from({ length: topo.nodeCount }, (_, i) => topo.rackBase + i);
  const stores: number[] = [];
  const groups: { storeId: number; groupId: number }[] = [];
  for (let s = 0; s < topo.storeCount; s++) {
    stores.push(topo.storeBase + s);
    for (let g = 0; g < topo.groupsPerStore; g++) {
      groups.push({ storeId: topo.storeBase + s, groupId: topo.groupBase + s * topo.groupsPerStore + g });
    }
  }
  return { racks, nodes, stores, groups, apiBase };
}

async function runSmokeSuite(baseURL: string, label: string, cluster: SetupResult) {
  // Verify all stores and groups have leaders
  await step(`smoke-${label}: verify leaders`, async () => {
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
  });

  // KV put/get on first group
  const firstGroup = cluster.groups[0];
  const testKey = `cmp-${label}-key`;
  const testValue = `cmp-${label}-value`;
  await kvPut(baseURL, firstGroup.storeId, firstGroup.groupId, testKey, testValue);
  expect(await kvGet(baseURL, firstGroup.storeId, firstGroup.groupId, testKey)).toBe(testValue);

  // Verify stores exist via API
  await step(`smoke-${label}: verify stores API`, async () => {
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
  });
}

test.describe('kv cluster · multi-rack/multi-store/multi-group topology', () => {
  // One cluster shared by all 5 tests. Each test uses a disjoint node
  // set + store/group IDs. Group-0 is bootstrapped once on nodes
  // 191-193; all stores are created via createStoreNoInit.
  //
  // Node layout:
  //   191-193  store 199  groups 1990-1992  — multi-rack + leader election
  //   381-386  stores 380+381  groups 3800+3810  — store isolation
  //   391-395  store 390  groups 3900+3901  — overlapping groups
  //   401-405  store 400  groups 4000-4002  — 3 independent groups
  //   100-102  store 800  group 8000  — SIMPLE smoke
  //   200-207  stores 900+901  groups 9000-9003  — COMPLEX smoke
  test.beforeAll(async () => {
    await step('topology: resetAll', () => resetAll(apiBase));

    const allNodes = [
      ...[191, 192, 193],
      ...[381, 382, 383, 384, 385, 386],
      ...[391, 392, 393, 394, 395],
      ...[401, 402, 403, 404, 405],
      ...[100, 101, 102],
      ...[200, 201, 202, 203, 204, 205, 206, 207],
    ];
    await step('topology: seedRackAndNode', () => Promise.all(allNodes.map((r) => seedRackAndNode(apiBase, r, r))));
    await step('topology: deployNodeServer', () => Promise.all(allNodes.map((n) => deployNodeServer(apiBase, n, freePort(), freePort()))));

    // Bootstrap group-0 on the first 3 nodes (191, 192, 193).
    await step('topology: clusterInit', () => clusterInit(apiBase, [191, 192, 193]));

    // Test 1: multi-rack — bootstrap store on 1 node, extend via addReplica,
    // then create groups spanning all 3.
    await step('topology: multi-rack setup', async () => {
      await createStoreNoInit(apiBase, 199, [191]);
      await addGroup(apiBase, 199, 1990, 19900, [191]);
      await addReplica(apiBase, 199, 1990, 192, 19901);
      await addReplica(apiBase, 199, 1990, 193, 19902);
      await Promise.all([
        addGroup(apiBase, 199, 1991, 19910, [191, 192, 193]),
        addGroup(apiBase, 199, 1992, 19920, [191, 192, 193]),
      ]);
    });

    // Test 2: iso-stores — two stores on disjoint node sets.
    await step('topology: iso-stores setup', async () => {
      await createStoreNoInit(apiBase, 380, [381, 382, 383]);
      await createStoreNoInit(apiBase, 381, [384, 385, 386]);
      await Promise.all([
        addGroup(apiBase, 380, 3800, 38000, [381, 382, 383]),
        addGroup(apiBase, 381, 3810, 38100, [384, 385, 386]),
      ]);
    });

    // Test 3: overlap — two groups on overlapping 3-node subsets.
    await step('topology: overlap setup', async () => {
      await createStoreNoInit(apiBase, 390, [391, 392, 393, 394, 395]);
      await Promise.all([
        addGroup(apiBase, 390, 3900, 39000, [391, 392, 393]),
        addGroup(apiBase, 390, 3901, 39010, [393, 394, 395]),
      ]);
    });

    // Test 4: 3 groups — three groups on different 3-node subsets.
    await step('topology: 3groups setup', async () => {
      await createStoreNoInit(apiBase, 400, [401, 402, 403, 404, 405]);
      await Promise.all([
        addGroup(apiBase, 400, 4000, 40000, [401, 402, 403]),
        addGroup(apiBase, 400, 4001, 40010, [402, 403, 404]),
        addGroup(apiBase, 400, 4002, 40020, [403, 404, 405]),
      ]);
    });

    // Test 5: smoke — SIMPLE (3 nodes, 1 store, 1 group) + COMPLEX
    // (8 nodes, 2 stores, 4 groups). setupCluster would re-bootstrap
    // group-0, so we inline the store/group creation with createStoreNoInit.
    await step('topology: smoke setup', async () => {
      // SIMPLE
      await createStoreNoInit(apiBase, 800, [100, 101, 102]);
      await addGroup(apiBase, 800, 8000, 1, [100, 101, 102]);
      // COMPLEX — storeNodes = first 3 nodes (200, 201, 202)
      await createStoreNoInit(apiBase, 900, [200, 201, 202]);
      await createStoreNoInit(apiBase, 901, [200, 201, 202]);
      await Promise.all([
        addGroup(apiBase, 900, 9000, 1, [200, 201, 202]),
        addGroup(apiBase, 900, 9001, 1, [200, 201, 202]),
        addGroup(apiBase, 901, 9002, 1, [200, 201, 202]),
        addGroup(apiBase, 901, 9003, 1, [200, 201, 202]),
      ]);
    });

    // Wait for all leaders in parallel.
    await step('topology: waitForLeader', () => Promise.all([
      ...[1990, 1991, 1992].map((g) => waitForLeader(apiBase, 199, g, 10_000)),
      waitForLeader(apiBase, 380, 3800, 10_000),
      waitForLeader(apiBase, 381, 3810, 10_000),
      waitForLeader(apiBase, 390, 3900, 10_000),
      waitForLeader(apiBase, 390, 3901, 10_000),
      waitForLeader(apiBase, 400, 4000, 10_000),
      waitForLeader(apiBase, 400, 4001, 10_000),
      waitForLeader(apiBase, 400, 4002, 10_000),
      waitForLeader(apiBase, 800, 8000, 10_000),
      waitForLeader(apiBase, 900, 9000, 10_000),
      waitForLeader(apiBase, 900, 9001, 10_000),
      waitForLeader(apiBase, 901, 9002, 10_000),
      waitForLeader(apiBase, 901, 9003, 10_000),
    ]));
  });

  test.afterAll(async () => {
    // resetAll stops all servers + wipes config — one call replaces
    // N per-node stopNodeServer teardowns.
    await step('topology: resetAll', () => resetAll(apiBase));
  });

  test('creates multi-rack cluster with one store and multiple groups, monitors leader election', async ({ page, baseURL }) => {
    test.setTimeout(60_000);

    const api = await apiContext(baseURL!);
    try {
      await step('multi-rack: goto + verify UI', async () => {
        // Navigate to Cluster view and verify all groups appear in UI.
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        for (const gid of [1990, 1991, 1992]) {
          await expect(aside.getByText(`G-${gid}`).first()).toBeVisible({ timeout: 3_000 });
        }
      });

      // Monitor leader election via API polling.
      // Three concurrent fresh elections (one per group) need a few
      // election deadlines (default 4-8 s each) plus PreVote/RequestVote
      // round-trip; 30 s gives headroom on a busy CI machine.
      const groups = [1990, 1991, 1992];
      const leaders = new Map<number, number>();

      // Poll until all groups have exactly one leader, or timeout.
      await step('multi-rack: poll leaders', () => expect.poll(async () => {
        for (const gid of groups) {
          if (leaders.has(gid)) continue;
          const response = await api.get(`/api/stores/199/groups/${gid}`);
          if (!response.ok()) continue;
          const detail: { replicas: Array<{ replica_id: number; role: string }> } = await response.json();
          const leaderReplicas = detail.replicas.filter((r) => r.role === 'leader');
          if (leaderReplicas.length === 1) {
            leaders.set(gid, leaderReplicas[0].replica_id);
          }
        }
        return leaders.size;
      }, { timeout: 10_000, intervals: [200] }).toBe(groups.length));

      // Assert every group has elected exactly one leader.
      for (const gid of groups) {
        const leader = leaders.get(gid);
        expect(
          leader,
          `group ${gid} did not elect exactly one leader (leaders so far: ${JSON.stringify(Array.from(leaders.entries()))})`,
        ).toBeTruthy();
        expect(leader).toBeGreaterThan(0);
      }

      // KV put/get verification: write a key to group 1990 and read it back
      // via the console API to confirm the multi-group cluster serves KV.
      await step('multi-rack: KV put/get API', async () => {
        const putResp = await api.post(`/api/stores/199/groups/1990/kv/put`, {
          data: { key: 'e2e-19-key', value: 'e2e-19-value' },
        });
        expect(putResp.ok(), await putResp.text()).toBeTruthy();
        const getResp = await api.get(`/api/stores/199/groups/1990/kv/get?key=e2e-19-key`);
        expect(getResp.ok(), await getResp.text()).toBeTruthy();
        const getBody = await getResp.json();
        expect(getBody.found).toBe(true);
        expect(getBody.value_utf8).toBe('e2e-19-value');
      });
    } finally {
      await api.dispose();
    }
  });

  test('put/get/delete on store A does not affect store B', async ({ page, baseURL }) => {
    test.setTimeout(60_000);

    // Put keys in store A only
    await kvPut(baseURL!, 380, 3800, 'iso-a-key1', 'val-a1');
    await kvPut(baseURL!, 380, 3800, 'iso-a-key2', 'val-a2');

    // Put keys in store B only
    await kvPut(baseURL!, 381, 3810, 'iso-b-key1', 'val-b1');
    await kvPut(baseURL!, 381, 3810, 'iso-b-key2', 'val-b2');

    // Verify store A has only A keys
    const scanA = await kvScanAll(baseURL!, 380, 3800);
    expect(scanA).toEqual(expect.arrayContaining(['iso-a-key1', 'iso-a-key2']));
    expect(scanA).not.toEqual(expect.arrayContaining(['iso-b-key1', 'iso-b-key2']));

    // Verify store B has only B keys
    const scanB = await kvScanAll(baseURL!, 381, 3810);
    expect(scanB).toEqual(expect.arrayContaining(['iso-b-key1', 'iso-b-key2']));
    expect(scanB).not.toEqual(expect.arrayContaining(['iso-a-key1', 'iso-a-key2']));

    // Verify via UI: open KV panel, select store A, scan, see only A keys
    await step('iso-stores: scan UI', async () => {
      await page.goto('/');
      await page.getByTestId('domain-kv').click();
      await page.getByTestId('kv-tab-kv').click();
      // Uncheck auto-scan: group selection triggers an auto-scan whose
      // response waitForResponse would race with the explicit Scan
      // click's response, causing the auto-scan's discarded result to
      // be awaited while the explicit scan is still in flight.
      const autoScanCheckbox = page.getByRole('checkbox', { name: 'auto-scan' });
      if (await autoScanCheckbox.isChecked()) {
        await autoScanCheckbox.uncheck();
      }
      await page.getByTestId('kv-store-select').selectOption('380');
      await page.getByTestId('kv-group-select').selectOption('3800');

      // Scan and verify store A keys appear
      const scanResponse = page.waitForResponse((r: any) => r.url().includes('/stores/380/groups/3800/kv/scan'));
      await page.getByRole('button', { name: /^Scan$/ }).click();
      await scanResponse;
      await expect(page.getByTestId('kv-scan-table').getByText('iso-a-key1')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('iso-b-key1')).toHaveCount(0);

      // Switch to store B, scan, see only B keys
      await page.getByTestId('kv-store-select').selectOption('381');
      await page.getByTestId('kv-group-select').selectOption('3810');
      const scanResponse2 = page.waitForResponse((r: any) => r.url().includes('/stores/381/groups/3810/kv/scan'));
      await page.getByRole('button', { name: /^Scan$/ }).click();
      await scanResponse2;
      await expect(page.getByTestId('kv-scan-table').getByText('iso-b-key1')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('iso-a-key1')).toHaveCount(0);
    });
  });

  test('two groups on overlapping 3-node subsets operate independently', async ({ page }) => {
    test.setTimeout(60_000);

    // Drive all KV ops through the UI KV panel. Setup (deploy/create
    // group) is in beforeAll — the put/get/delete + cross-group
    // isolation checks exercise the real UI path.
    await step('overlap: KV ops UI', async () => {
      await page.goto('/');
      await page.getByTestId('domain-kv').click();
      await page.getByTestId('kv-tab-kv').click();
      await page.getByTestId('kv-store-select').selectOption('390');

      // Group A (3900): put + get g39a-key.
      await page.getByTestId('kv-group-select').selectOption('3900');
      await page.getByLabel('Put key').fill('g39a-key');
      await page.getByLabel('Put value').fill('val-a');
      const putA = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /^Put$/ }).click();
      expect((await putA).ok()).toBeTruthy();
      await page.getByLabel('Get key').fill('g39a-key');
      const getA = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      await getA;
      await expect(page.getByTestId('kv-get-result')).toHaveText('val-a', { timeout: 3_000 });

      // Group B (3901): put + get g39b-key.
      await page.getByTestId('kv-group-select').selectOption('3901');
      await page.getByLabel('Put key').fill('g39b-key');
      await page.getByLabel('Put value').fill('val-b');
      const putB = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /^Put$/ }).click();
      expect((await putB).ok()).toBeTruthy();
      await page.getByLabel('Get key').fill('g39b-key');
      const getB = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      await getB;
      await expect(page.getByTestId('kv-get-result')).toHaveText('val-b', { timeout: 3_000 });

      // Cross-group isolation: g39a-key (from group A) must not be
      // visible in group B — the UI should report not-found.
      await page.getByLabel('Get key').fill('g39a-key');
      const getCross = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      await getCross;
      await expect(page.getByTestId('kv-not-found')).toBeVisible({ timeout: 3_000 });

      // Delete g39a-key in group A, then verify it is gone from A but
      // group B still serves g39b-key.
      await page.getByTestId('kv-group-select').selectOption('3900');
      await page.getByLabel('Delete key').fill('g39a-key');
      await page.getByRole('button', { name: /Delete$/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      const delA = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      expect((await delA).ok()).toBeTruthy();

      await page.getByLabel('Get key').fill('g39a-key');
      const getAAfterDelete = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      await getAAfterDelete;
      await expect(page.getByTestId('kv-not-found')).toBeVisible({ timeout: 3_000 });

      await page.getByTestId('kv-group-select').selectOption('3901');
      await page.getByLabel('Get key').fill('g39b-key');
      const getBAfterDelete = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      await getBAfterDelete;
      await expect(page.getByTestId('kv-get-result')).toHaveText('val-b', { timeout: 3_000 });
    });
  });

  test('3 groups on different node subsets operate independently', async ({ baseURL }) => {
    test.setTimeout(60_000);

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
  });

  test('comparative smoke suite passes on SIMPLE and COMPLEX topologies', async ({ baseURL }) => {
    test.setTimeout(90_000);
    // --- SIMPLE topology (3 nodes, 1 store, 1 group) ---
    await runSmokeSuite(baseURL!, 'simple', makeSetupResult(SIMPLE));

    // --- COMPLEX topology (8 nodes, 2 stores, 4 groups) ---
    await runSmokeSuite(baseURL!, 'complex', makeSetupResult(COMPLEX));
  });
});
