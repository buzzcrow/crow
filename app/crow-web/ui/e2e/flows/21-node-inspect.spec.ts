// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 1.5s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import {
  apiContext,
  addGroup,
  addReplica,
  createNode,
  createRack,
  createStore,
  deployNodeServer,
  stopNodeServer,
} from '../fixtures/consoleSetup';

/**
 * E2E-21 · Physical node inspect — local + remote wiring (Req §3.3).
 *
 * The physical/debugging view is the only place a node's remote-replica
 * proxies are surfaced. Seeds a two-node group, asserts both the local
 * (`LR-*`) and remote (`RR-*`) rows render under the owning node, then
 * deletes one remote out-of-band and asserts the dashed peer row vanishes
 * after a poll — the mis-wiring this view exists to catch.
 */
test.describe('E2E-21 physical node inspect', () => {
  test('shows local + remote replicas and reflects a removed remote', async ({ page, baseURL }) => {
    // Unique ids/ports: 20-ui-behaviors already uses r21*/n21*.
    await createRack(baseURL!, { id: 26, name: 'Rack TwentySix' });
    await createNode(baseURL!, { id: 261, rack_id: 26 });
    await createNode(baseURL!, { id: 262, rack_id: 26 });
    await deployNodeServer(baseURL!, 261, 9980, 9990);
    await deployNodeServer(baseURL!, 262, 9981, 9991);
    // store 266, then group 2660 / replica 26600 on n26a, then a peer on n26b.
    await createStore(baseURL!, 266, [261]);
    await addGroup(baseURL!, 266, 2660, 26600, [261]);
    await addReplica(baseURL!, 266, 2660, 262, 26601);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // Per-node store detail loads after the tree mounts, so rows added by
      // polling stay collapsed. Wait for the stores, then expand everything.
      await expect(aside.getByText('S-266', { exact: true }).first()).toBeVisible({ timeout: 3_000 });
      for (let i = 0; i < 24; i++) {
        const expander = aside.getByRole('button', { name: 'Expand' }).first();
        if (!(await expander.count())) break;
        await expander.click();
      }

      // Local replica on n26a and the remote proxy pointing at n26b's 26601.
      await expect(aside.getByText('LR-26600', { exact: true })).toBeVisible({ timeout: 3_000 });
      await expect(aside.getByText('RR-26601', { exact: true })).toBeVisible({ timeout: 3_000 });
      // The mirror side: n26b hosts 26601 locally and a remote proxy for 26600.
      await expect(aside.getByText('LR-26601', { exact: true })).toBeVisible({ timeout: 3_000 });
      await expect(aside.getByText('RR-26600', { exact: true })).toBeVisible({ timeout: 3_000 });

      // Remove the remote on n26a out-of-band (simulated mis-wiring).
      const del = await api.delete('/api/nodes/261/stores/266/groups/2660/remotes/26601');
      expect(del.ok(), await del.text()).toBeTruthy();

      // After a poll the dashed peer row under n26a is gone; n26b's mirror
      // remote (RR-26600) is untouched.
      await expect(aside.getByText('RR-26601', { exact: true })).toHaveCount(0, { timeout: 3_000 });
      await expect(aside.getByText('RR-26600', { exact: true })).toBeVisible();
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 261);
      await stopNodeServer(baseURL!, 262);
    }
  });
});
