// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { apiContext, DEFAULT_SERVER_BINARY, seedRackAndNode } from '../fixtures/consoleSetup';

test.describe('E2E-04 deploy server', () => {
  test('deploys and stops a real crowkv-server through the UI', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r4', 'n4');

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const aside = page.locator('aside').first();
      await expect(aside.getByText('N-n4', { exact: true })).toBeVisible({ timeout: 15_000 });

      await aside.getByText('N-n4', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /deploy crowkv/i }).click();

      await expect(page.getByRole('dialog', { name: /deploy crowkv on n4/i })).toBeVisible();
      await page.getByLabel('Management Port').fill('9911');
      await page.getByLabel('gRPC Port').fill('9921');
      await page.getByLabel('Binary Path (optional)').fill(DEFAULT_SERVER_BINARY);
      await page.getByRole('button', { name: 'Deploy' }).click();

      await expect(page.getByText(/CrowKV deployed on n4/)).toBeVisible({ timeout: 30_000 });

      const server = await api.get('/api/nodes/n4/server');
      expect(server.ok(), await server.text()).toBeTruthy();
      const body = await server.json();
      expect(body).toEqual(
        expect.objectContaining({
          id: 'n4',
          node_id: 'n4',
          url: 'http://127.0.0.1:9911',
          grpc_url: 'http://127.0.0.1:9921',
        }),
      );
      expect(body.pid).toEqual(expect.any(Number));
    } finally {
      await api.post('/api/nodes/n4/server/stop').catch(() => undefined);
      await api.dispose();
    }
  });
});
