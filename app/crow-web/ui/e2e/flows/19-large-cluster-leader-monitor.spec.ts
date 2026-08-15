// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.8s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, addReplica, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, freePort } from '../fixtures/consoleSetup';

test.describe('E2E-19 large cluster leader monitor', () => {
  test('creates multi-rack cluster with one store and multiple groups, monitors leader election', async ({ page, baseURL }) => {
    test.setTimeout(30_000);
    // Setup: 3 racks, 3 nodes, 3 deployed servers.
    const racks = [
      { rack: 191, node: 191, restPort: freePort(), rpcPort: freePort() },
      { rack: 192, node: 192, restPort: freePort(), rpcPort: freePort() },
      { rack: 193, node: 193, restPort: freePort(), rpcPort: freePort() },
    ];

    for (const r of racks) {
      await seedRackAndNode(baseURL!, r.rack, r.node);
    }
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

    const api = await apiContext(baseURL!);
    try {
      // Navigate to Cluster view and verify all groups appear in UI.
      await page.goto('/');
      await page.getByRole('button', { name: 'KV Cluster' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      for (const gid of [1990, 1991, 1992]) {
        await expect(aside.getByText(`G-${gid}`)).toBeVisible({ timeout: 3_000 });
      }

      // Monitor leader election via API polling.
      // Three concurrent fresh elections (one per group) need a few
      // election deadlines (default 4-8 s each) plus PreVote/RequestVote
      // round-trip; 30 s gives headroom on a busy CI machine.
      const groups = [1990, 1991, 1992];
      const leaders = new Map<number, number>();

      // Poll until all groups have exactly one leader, or timeout.
      await expect.poll(async () => {
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
      }, { timeout: 10_000, intervals: [200] }).toBe(groups.length);

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
      const putResp = await api.post(`/api/stores/199/groups/1990/kv/put`, {
        data: { key: 'e2e-19-key', value: 'e2e-19-value' },
      });
      expect(putResp.ok(), await putResp.text()).toBeTruthy();
      const getResp = await api.get(`/api/stores/199/groups/1990/kv/get?key=e2e-19-key`);
      expect(getResp.ok(), await getResp.text()).toBeTruthy();
      const getBody = await getResp.json();
      expect(getBody.found).toBe(true);
      expect(getBody.value_utf8).toBe('e2e-19-value');
    } finally {
      await api.dispose();
      for (const r of racks) {
        await stopNodeServer(baseURL!, r.node);
      }
    }
  });
});
