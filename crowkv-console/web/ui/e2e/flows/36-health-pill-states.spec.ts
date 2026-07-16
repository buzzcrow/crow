// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, seedRackAndNode, stopNodeServer, waitForLeader, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-36 health pill states', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('health pill shows Unknown initially and Healthy after group creation', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r36', 'n36');
    await deployNodeServer(baseURL!, 'n36', 9936, 9946);

    try {
      await page.goto('/');

      // With no stores/groups, health should be Unknown
      const healthPill = page.locator('header').getByText(/Unknown|Healthy|Degraded|Failed/);
      await expect(healthPill).toContainText('Unknown', { timeout: 3_000 });

      // Create store + group with leader
      await createStore(baseURL!, 360, ['n36']);
      await addGroup(baseURL!, 360, 3600, 36000, ['n36']);
      await waitForLeader(baseURL!, 360, 3600);

      // Click refresh to pick up the new state
      await page.getByRole('button', { name: 'Refresh' }).click();

      // Health should now be Healthy
      await expect(healthPill).toContainText('Healthy', { timeout: 10_000 });
    } finally {
      await stopNodeServer(baseURL!, 'n36');
    }
  });
});
