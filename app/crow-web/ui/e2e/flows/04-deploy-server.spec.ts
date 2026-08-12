// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.7s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, DEFAULT_SERVER_BINARY, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

test.describe('E2E-04 deploy server', () => {
  test('deploys and stops a real crow-kv-server through the UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 4, 4);

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await expect(aside.getByText('N-4', { exact: true })).toBeVisible({ timeout: 3_000 });

      await aside.getByText('N-4', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /deploy Crow Storage/i }).click();

      await expect(page.getByRole('dialog', { name: /deploy Crow Storage on 4/i })).toBeVisible();
      await page.getByLabel('Management Port').fill('9911');
      await page.getByLabel('gRPC Port').fill('9921');
      await page.getByLabel('Binary Path (optional)').fill(DEFAULT_SERVER_BINARY);
      await page.getByRole('button', { name: 'Deploy' }).click();

      await expect.poll(async () => {
        const server = await api.get('/api/nodes/4/server');
        if (!server.ok()) return null;
        return await server.json();
      }, { timeout: 5_000 }).toEqual(
        expect.objectContaining({
          node_id: 4,
          url: 'http://127.0.0.1:9911',
          grpc_url: 'http://127.0.0.1:9921',
          pid: expect.any(Number),
        }),
      );
    } finally {
      await stopNodeServer(baseURL!, 4);
      await api.dispose();
    }
  });
});
