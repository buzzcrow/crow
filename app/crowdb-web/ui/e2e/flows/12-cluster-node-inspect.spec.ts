// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 2.3s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import {
  apiContext,
  addGroup,
  addReplica,
  createNode,
  createRack,
  createStore,
  deployNodeServer,
  freePort,
  resetAll,
  seedRackAndNode,
  stopNodeServer,
  waitForLeader,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

/**
 * Cluster node inspect — cross-jump + local/remote replica wiring.
 *
 * The cluster/debugging view is the only place a node's remote-replica
 * proxies are surfaced. The inspector also offers cross-jump buttons that
 * move between the KV and cluster topologies.
 */
test.describe('cluster · node inspect & cross-jump', () => {
  test('cross-jumps between views and shows local + remote replicas', async ({ page, baseURL }) => {
    // --- logical replica details -> hosting physical node ---
    await step('xjump: setup 6', async () => {
      await stopNodeServer(baseURL!, 6);
      try { await seedRackAndNode(baseURL!, 6, 6); } catch (err) { if (!String(err).includes('already exists')) throw err; }
      await deployNodeServer(baseURL!, 6, freePort(), freePort());
      await createStore(baseURL!, 66, [6]);
      await addGroup(baseURL!, 66, 660, 6600, [6]);
    });

    try {
      await step('xjump: logical→physical UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('LR-6600')).toBeVisible({ timeout: 3_000 });
        await aside.getByText('LR-6600').click();

        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible({ timeout: 3_000 });

        // Single cross-jump button: logical Replica -> hosting physical Node.
        await inspector.getByRole('button', { name: /Show on node 6\b/ }).click();

        await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 3_000 });
        await expect(inspector.getByText('N-6', { exact: true })).toBeVisible({ timeout: 3_000 });
      });
    } finally {
      await step('xjump: teardown 6', () => stopNodeServer(baseURL!, 6));
    }

    // --- physical node details -> hosting logical store ---
    await step('xjump: setup 62', async () => {
      await stopNodeServer(baseURL!, 62);
      try { await seedRackAndNode(baseURL!, 62, 62); } catch (err) { if (!String(err).includes('already exists')) throw err; }
      await deployNodeServer(baseURL!, 62, freePort(), freePort());
      await createStore(baseURL!, 67, [62]);
      await addGroup(baseURL!, 67, 670, 6700, [62]);
    });

    try {
      await step('xjump: physical→logical UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-62' });
        await expect(nodeItem).toBeVisible({ timeout: 3_000 });
        await nodeItem.getByRole('button', { name: 'N-62' }).click();

        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible({ timeout: 3_000 });

        // Cross-jump button: physical Node -> logical Store.
        await inspector.getByRole('button', { name: /Show store 67 in cluster/i }).click();

        await expect(page.getByRole('heading', { name: 'KV' })).toBeVisible({ timeout: 3_000 });
        await expect(inspector.getByText('S-67', { exact: true }).first()).toBeVisible({ timeout: 3_000 });
      });
    } finally {
      await step('xjump: teardown 62', () => stopNodeServer(baseURL!, 62));
    }

    // --- node inspect: local + remote replicas, removed remote disappears ---
    // Unique ids/ports: 20-ui-behaviors already uses r21*/n21*.
    await step('xjump: setup replicas', async () => {
      await stopNodeServer(baseURL!, 261);
      await stopNodeServer(baseURL!, 262);
      try { await createRack(baseURL!, { id: 26, name: 'Rack TwentySix' }); } catch (err) { if (!String(err).includes('already exists')) throw err; }
      try { await createNode(baseURL!, { id: 261, rack_id: 26 }); } catch (err) { if (!String(err).includes('already exists')) throw err; }
      try { await createNode(baseURL!, { id: 262, rack_id: 26 }); } catch (err) { if (!String(err).includes('already exists')) throw err; }
      await Promise.all([
        deployNodeServer(baseURL!, 261, freePort(), freePort()),
        deployNodeServer(baseURL!, 262, freePort(), freePort()),
      ]);
      // store 266, then group 2660 / replica 26600 on n26a, then a peer on n26b.
      await createStore(baseURL!, 266, [261]);
      await addGroup(baseURL!, 266, 2660, 26600, [261]);
      await addReplica(baseURL!, 266, 2660, 262, 26601);
    });

    const api = await apiContext(baseURL!);
    try {
      await step('xjump: replicas UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
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
      });

      // Remove the remote on n26a out-of-band (simulated mis-wiring).
      await step('xjump: delete remote API', async () => {
        const del = await api.delete('/api/nodes/261/stores/266/groups/2660/remotes/26601');
        expect(del.ok(), await del.text()).toBeTruthy();
      });

      // After a poll the dashed peer row under n26a is gone; n26b's mirror
      // remote (RR-26600) is untouched.
      await step('xjump: verify remote gone', async () => {
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('RR-26601', { exact: true })).toHaveCount(0, { timeout: 3_000 });
        await expect(aside.getByText('RR-26600', { exact: true })).toBeVisible();
      });
    } finally {
      await api.dispose();
      await step('xjump: teardown replicas', () => Promise.all([
        stopNodeServer(baseURL!, 261),
        stopNodeServer(baseURL!, 262),
      ]));
    }
  });

  // Needs a truly empty backend so the store cross-jump target is unambiguous.
  test('physical node with store shows cross-jump to logical store', async ({ page, baseURL }) => {
    await step('xjump2: resetAll', () => resetAll(baseURL!));
    await step('xjump2: setup', async () => {
      await seedRackAndNode(baseURL!, 33, 33);
      await deployNodeServer(baseURL!, 33, freePort(), freePort());
      await createStore(baseURL!, 330, [33]);
      await addGroup(baseURL!, 330, 3300, 33000, [33]);
      await waitForLeader(baseURL!, 330, 3300);
    });

    try {
      await step('xjump2: cross-jump UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        // Select the physical node
        const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-33' });
        await expect(nodeItem).toBeVisible({ timeout: 3_000 });
        await nodeItem.getByRole('button', { name: 'N-33' }).click();

        // Inspector should show Details tab with cross-jump button
        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible({ timeout: 3_000 });

        // Verify cross-jump button exists and click it
        const crossJumpButton = inspector.getByRole('button', { name: /Show store 330 in cluster/i });
        await expect(crossJumpButton).toBeVisible({ timeout: 3_000 });
        await crossJumpButton.click();

        // View should switch to KV
        await expect(page.getByRole('heading', { name: 'KV' })).toBeVisible({ timeout: 3_000 });

        // Store should be selected in the logical tree
        await expect(aside.getByText('S-330')).toBeVisible({ timeout: 3_000 });
      });
    } finally {
      await step('xjump2: teardown', () => stopNodeServer(baseURL!, 33));
    }
  });
});
