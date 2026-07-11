import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, addReplica, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-19 large cluster leader monitor', () => {
  test('creates multi-rack cluster with one store and multiple groups, monitors leader election', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    // Setup: 3 racks, 3 nodes, 3 deployed servers.
    const racks = [
      { rack: 'r19a', node: 'n19a', mgmtPort: 9935, grpcPort: 9945 },
      { rack: 'r19b', node: 'n19b', mgmtPort: 9936, grpcPort: 9946 },
      { rack: 'r19c', node: 'n19c', mgmtPort: 9937, grpcPort: 9947 },
    ];

    for (const r of racks) {
      await seedRackAndNode(baseURL!, r.rack, r.node);
      await deployNodeServer(baseURL!, r.node, r.mgmtPort, r.grpcPort);
    }

    // Bootstrap store 199 with group 1990 (replica 19900) on n19a only.
    // http_add_store reuses the same replica_id across nodes and does not
    // wire remotes, so we extend the group via addReplica below which
    // auto-creates the store on each peer node and wires remotes.
    await createStore(baseURL!, 199, 1990, 19900, ['n19a']);
    await addGroup(baseURL!, 199, 1990, 19900, ['n19a']);
    // addReplica adds a remote replica to an existing group on a new node;
    // it ensures the target node hosts the store (creating it if needed)
    // and wires remotes on every existing peer.
    await addReplica(baseURL!, 199, 1990, 'n19b', 19901);
    await addReplica(baseURL!, 199, 1990, 'n19c', 19902);

    // Now all 3 nodes host store 199, so addGroup can create new groups
    // spanning all 3. Leader election must converge via Paxos.
    await addGroup(baseURL!, 199, 1991, 19910, ['n19a', 'n19b', 'n19c']);
    await addGroup(baseURL!, 199, 1992, 19920, ['n19a', 'n19b', 'n19c']);

    const api = await apiContext(baseURL!);
    try {
      // Navigate to Cluster view and verify all groups appear in UI.
      await page.goto('/');
      await page.getByRole('button', { name: 'Logical' }).click();
      const aside = page.locator('aside').first();

      for (const gid of [1990, 1991, 1992]) {
        await expect(aside.getByText(`G-${gid}`)).toBeVisible({ timeout: 15_000 });
      }

      // Monitor leader election via API polling (max 30 s).
      // Three concurrent fresh elections (one per group) need a few
      // election deadlines (default 4-8 s each) plus PreVote/RequestVote
      // round-trip; 30 s gives headroom on a busy CI machine.
      // Use the per-group endpoint and check role=Leader directly.
      const groups = [1990, 1991, 1992];
      const deadline = Date.now() + 30_000;
      const leaders = new Map<number, number>();

      while (Date.now() < deadline && leaders.size < groups.length) {
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
        if (leaders.size < groups.length) {
          await page.waitForTimeout(500);
        }
      }

      // Assert every group has elected exactly one leader within 30 s.
      for (const gid of groups) {
        const leader = leaders.get(gid);
        expect(
          leader,
          `group ${gid} did not elect exactly one leader within 30 seconds (leaders so far: ${JSON.stringify(Array.from(leaders.entries()))})`,
        ).toBeTruthy();
        expect(leader).toBeGreaterThan(0);
      }
    } finally {
      await api.dispose();
      for (const r of racks) {
        await stopNodeServer(baseURL!, r.node);
      }
    }
  });
});
