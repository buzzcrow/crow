// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.9s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { createNode, createRack, deployNodeServer, stopNodeServer } from '../fixtures/consoleSetup';

/**
 * E2E-22 · Embedded Swagger panel (Req §3.5).
 *
 * Opening the API panel must render inline (no new tab / no full-page
 * navigation) and re-target the OpenAPI doc when the selected node changes.
 * We assert on the iframe element + its `src` rather than the rendered
 * Swagger bundle so the test does not depend on the offline asset set.
 */
test.describe('E2E-22 swagger panel', () => {
  test('renders inline and re-targets the node selection', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r22', name: 'Rack TwentyTwo' });
    await createNode(baseURL!, { id: 'n22a', rack_id: 'r22' });
    await createNode(baseURL!, { id: 'n22b', rack_id: 'r22' });
    await deployNodeServer(baseURL!, 'n22a', 9953, 9963);
    await deployNodeServer(baseURL!, 'n22b', 9954, 9964);

    try {
      await page.goto('/');
      await page.getByRole('button', { name: 'Physical' }).click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // Target n22a, then open the API panel.
      await expect(aside.getByText('N-n22a', { exact: true })).toBeVisible({ timeout: 3_000 });
      await aside.getByRole('button', { name: 'N-n22a' }).click();
      await page.getByRole('button', { name: 'API' }).click();

      const frameA = page.locator('iframe[title="Swagger UI for n22a"]');
      await expect(frameA).toBeVisible({ timeout: 3_000 });
      expect(decodeURIComponent((await frameA.getAttribute('src')) ?? '')).toContain('/nodes/n22a/openapi.json');
      // Shell stays mounted (no full-page navigation).
      await expect(page.locator('header').getByText('CrowKV Console')).toBeVisible();

      // Switch the selection to n22b -> the iframe re-targets inline.
      await aside.getByRole('button', { name: 'N-n22b' }).click();
      const frameB = page.locator('iframe[title="Swagger UI for n22b"]');
      await expect(frameB).toBeVisible({ timeout: 3_000 });
      expect(decodeURIComponent((await frameB.getAttribute('src')) ?? '')).toContain('/nodes/n22b/openapi.json');
      await expect(page.locator('header').getByText('CrowKV Console')).toBeVisible();
    } finally {
      await stopNodeServer(baseURL!, 'n22a');
      await stopNodeServer(baseURL!, 'n22b');
    }
  });
});
