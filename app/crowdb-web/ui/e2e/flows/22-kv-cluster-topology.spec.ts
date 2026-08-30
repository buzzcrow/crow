// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 33s (2026-08-17)

import { test, expect } from '../fixtures/realBackend';
import {
  apiContext,
  addGroup,
  addReplica,
  createStore,
  deployNodeServer,
  seedRackAndNode,
  stopNodeServer,
  freePort,
  waitForLeader,
  resetAll,
  setupCluster,
  teardownCluster,
  SIMPLE,
  COMPLEX,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

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

async function runSmokeSuite(baseURL: string, topo: typeof SIMPLE, label: string) {
  await step(`smoke-${label}: resetAll`, () => resetAll(baseURL));
  const cluster = await step(`smoke-${label}: setupCluster`, () => setupCluster(baseURL, topo));

  try {
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
  } finally {
    await step(`smoke-${label}: teardown`, () => teardownCluster(baseURL, cluster));
  }
}

test.describe('kv cluster · multi-rack/multi-store/multi-group topology', () => {
  // Each test below builds its own topology: they either need a distinct
  // node set or call resetAll, so setup cannot be shared via beforeAll.

  test('creates multi-rack cluster with one store and multiple groups, monitors leader election', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    // Setup: 3 racks, 3 nodes, 3 deployed servers.
    const racks = [
      { rack: 191, node: 191, restPort: freePort(), rpcPort: freePort() },
      { rack: 192, node: 192, restPort: freePort(), rpcPort: freePort() },
      { rack: 193, node: 193, restPort: freePort(), rpcPort: freePort() },
    ];

    await step('multi-rack: setup', async () => {
      await Promise.all(racks.map((r) => seedRackAndNode(baseURL!, r.rack, r.node)));
      await Promise.all(
        racks.map((r) => deployNodeServer(baseURL!, r.node, r.restPort, r.rpcPort)),
      );

      // Bootstrap store 199 with group 1990 (replica 19900) on n19a only.
      // http_add_store reuses the same replica_id across nodes and does not
      // wire remotes, so we extend the group via addReplica below which
      // auto-creates the store on each peer node and wires remotes.
      await createStore(baseURL!, 199, [191]);
      await addGroup(baseURL!, 199, 1990, 19900, [191]);
      // addReplica adds a remote replica to an existing group on a new node;
      // it ensures the target node hosts the store (creating it if needed)
      // and wires remotes on every existing peer.
      await addReplica(baseURL!, 199, 1990, 192, 19901);
      await addReplica(baseURL!, 199, 1990, 193, 19902);

      // Now all 3 nodes host store 199, so addGroup can create new groups
      // spanning all 3. Leader election must converge via Paxos.
      await addGroup(baseURL!, 199, 1991, 19910, [191, 192, 193]);
      await addGroup(baseURL!, 199, 1992, 19920, [191, 192, 193]);
    });

    const api = await apiContext(baseURL!);
    try {
      await step('multi-rack: goto + verify UI', async () => {
        // Navigate to Cluster view and verify all groups appear in UI.
        await page.goto('/');
        await page.getByRole('button', { name: 'KV' }).click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        for (const gid of [1990, 1991, 1992]) {
          await expect(aside.getByText(`G-${gid}`)).toBeVisible({ timeout: 3_000 });
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
      await step('multi-rack: teardown', () => Promise.all(racks.map((r) => stopNodeServer(baseURL!, r.node))));
    }
  });

  test('put/get/delete on store A does not affect store B', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    await step('iso-stores: resetAll', () => resetAll(baseURL!));

    // Store A: nodes n38a,b,c. Store B: nodes n38d,e,f. Separate node sets.
    await step('iso-stores: setup', async () => {
      await Promise.all([381, 382, 383, 384, 385, 386].map((n) => seedRackAndNode(baseURL!, n, n)));
      await Promise.all([
        deployNodeServer(baseURL!, 381, freePort(), freePort()),
        deployNodeServer(baseURL!, 382, freePort(), freePort()),
        deployNodeServer(baseURL!, 383, freePort(), freePort()),
        deployNodeServer(baseURL!, 384, freePort(), freePort()),
        deployNodeServer(baseURL!, 385, freePort(), freePort()),
        deployNodeServer(baseURL!, 386, freePort(), freePort()),
      ]);

      // Store A: 380, group 3800 on n38a,b,c. Store B: 381, group 3810 on n38d,e,f.
      await createStore(baseURL!, 380, [381, 382, 383]);
      await createStore(baseURL!, 381, [384, 385, 386]);
      await addGroup(baseURL!, 380, 3800, 38000, [381, 382, 383]);
      await addGroup(baseURL!, 381, 3810, 38100, [384, 385, 386]);
      await waitForLeader(baseURL!, 380, 3800);
      await waitForLeader(baseURL!, 381, 3810);
    });

    try {
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
        await page.locator('header').getByRole('button', { name: 'KV', exact: true }).click();
        // Uncheck auto-scan: group selection triggers an auto-scan whose
        // response waitForResponse would race with the explicit Scan
        // click's response, causing the auto-scan's discarded result to
        // be awaited while the explicit scan is still in flight.
        const autoScanCheckbox = page.getByRole('checkbox', { name: 'auto-scan' });
        if (await autoScanCheckbox.isChecked()) {
          await autoScanCheckbox.uncheck();
        }
        await page.getByLabel('Store').selectOption('380');
        await page.getByLabel('Group').selectOption('3800');

        // Scan and verify store A keys appear
        const scanResponse = page.waitForResponse((r: any) => r.url().includes('/stores/380/groups/3800/kv/scan'));
        await page.getByRole('button', { name: /^Scan$/ }).click();
        await scanResponse;
        await expect(page.getByTestId('kv-scan-table').getByText('iso-a-key1')).toBeVisible({ timeout: 3_000 });
        await expect(page.getByTestId('kv-scan-table').getByText('iso-b-key1')).toHaveCount(0);

        // Switch to store B, scan, see only B keys
        await page.getByLabel('Store').selectOption('381');
        await page.getByLabel('Group').selectOption('3810');
        const scanResponse2 = page.waitForResponse((r: any) => r.url().includes('/stores/381/groups/3810/kv/scan'));
        await page.getByRole('button', { name: /^Scan$/ }).click();
        await scanResponse2;
        await expect(page.getByTestId('kv-scan-table').getByText('iso-b-key1')).toBeVisible({ timeout: 3_000 });
        await expect(page.getByTestId('kv-scan-table').getByText('iso-a-key1')).toHaveCount(0);
      });
    } finally {
      await step('iso-stores: teardown', () => Promise.all([381, 382, 383, 384, 385, 386].map((n) => stopNodeServer(baseURL!, n))));
    }
  });

  test('two groups on overlapping 3-node subsets operate independently', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    await step('overlap: resetAll', () => resetAll(baseURL!));

    // 5 nodes total. Group A on n39a,b,c. Group B on n39c,d,e (overlap on n39c).
    await step('overlap: setup', async () => {
      await Promise.all([391, 392, 393, 394, 395].map((r) => seedRackAndNode(baseURL!, r, r)));
      await Promise.all([
        deployNodeServer(baseURL!, 391, freePort(), freePort()),
        deployNodeServer(baseURL!, 392, freePort(), freePort()),
        deployNodeServer(baseURL!, 393, freePort(), freePort()),
        deployNodeServer(baseURL!, 394, freePort(), freePort()),
        deployNodeServer(baseURL!, 395, freePort(), freePort()),
      ]);

      // Single store, two groups on different node subsets.
      await createStore(baseURL!, 390, [391, 392, 393, 394, 395]);
      await addGroup(baseURL!, 390, 3900, 39000, [391, 392, 393]);
      await addGroup(baseURL!, 390, 3901, 39010, [393, 394, 395]);
      await waitForLeader(baseURL!, 390, 3900);
      await waitForLeader(baseURL!, 390, 3901);
    });

    try {
      // Drive all KV ops through the UI KV panel. Setup (deploy/create
      // group) stays via API — same pattern as the other tests in this
      // file — but the put/get/delete + cross-group isolation checks
      // exercise the real UI path.
      await step('overlap: KV ops UI', async () => {
        await page.goto('/');
        await page.locator('header').getByRole('button', { name: 'KV', exact: true }).click();
        await page.getByLabel('Store').selectOption('390');

        // Group A (3900): put + get g39a-key.
        await page.getByLabel('Group').selectOption('3900');
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
        await page.getByLabel('Group').selectOption('3901');
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
        await page.getByLabel('Group').selectOption('3900');
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

        await page.getByLabel('Group').selectOption('3901');
        await page.getByLabel('Get key').fill('g39b-key');
        const getBAfterDelete = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
        await page.getByRole('button', { name: /^Get$/ }).click();
        await getBAfterDelete;
        await expect(page.getByTestId('kv-get-result')).toHaveText('val-b', { timeout: 3_000 });
      });
    } finally {
      await step('overlap: teardown', () => Promise.all([391, 392, 393, 394, 395].map((n) => stopNodeServer(baseURL!, n))));
    }
  });

  test('3 groups on different node subsets operate independently', async ({ baseURL }) => {
    test.setTimeout(60_000);
    await step('3groups: resetAll', () => resetAll(baseURL!));

    // 5 nodes, 1 store, 3 groups on different 3-node subsets.
    await step('3groups: setup', async () => {
      await Promise.all([401, 402, 403, 404, 405].map((r) => seedRackAndNode(baseURL!, r, r)));
      await Promise.all([
        deployNodeServer(baseURL!, 401, freePort(), freePort()),
        deployNodeServer(baseURL!, 402, freePort(), freePort()),
        deployNodeServer(baseURL!, 403, freePort(), freePort()),
        deployNodeServer(baseURL!, 404, freePort(), freePort()),
        deployNodeServer(baseURL!, 405, freePort(), freePort()),
      ]);

      await createStore(baseURL!, 400, [401, 402, 403, 404, 405]);

      // Group 4000: n40a,b,c. Group 4001: n40b,c,d. Group 4002: n40c,d,e.
      await addGroup(baseURL!, 400, 4000, 40000, [401, 402, 403]);
      await addGroup(baseURL!, 400, 4001, 40010, [402, 403, 404]);
      await addGroup(baseURL!, 400, 4002, 40020, [403, 404, 405]);
      await waitForLeader(baseURL!, 400, 4000);
      await waitForLeader(baseURL!, 400, 4001);
      await waitForLeader(baseURL!, 400, 4002);
    });

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
      await step('3groups: teardown', () => Promise.all([401, 402, 403, 404, 405].map((n) => stopNodeServer(baseURL!, n))));
    }
  });

  test('comparative smoke suite passes on SIMPLE and COMPLEX topologies', async ({ baseURL }) => {
    test.setTimeout(90_000);
    // --- SIMPLE topology (3 nodes, 1 store, 1 group) ---
    await runSmokeSuite(baseURL!, SIMPLE, 'simple');

    // --- COMPLEX topology (8 nodes, 2 stores, 4 groups) ---
    await runSmokeSuite(baseURL!, COMPLEX, 'complex');
  });
});
