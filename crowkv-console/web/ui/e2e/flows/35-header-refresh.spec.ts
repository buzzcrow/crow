// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.3s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';
import { createRack, resetAll } from '../fixtures/consoleSetup';

test.describe('E2E-35 header refresh', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test('refresh button picks up backend changes without page reload', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 'r35a', name: 'r35a' });

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

    // Verify initial rack
    await expect(aside.getByText('R-r35a')).toBeVisible({ timeout: 3_000 });

    // Add a new rack via API (backend change)
    await createRack(baseURL!, { id: 'r35b', name: 'r35b' });

    // Click Refresh button
    await page.getByRole('button', { name: 'Refresh' }).click();

    // New rack should appear without page reload
    await expect(aside.getByText('R-r35b')).toBeVisible({ timeout: 3_000 });
    await expect(aside.getByText('R-r35a')).toBeVisible();
  });
});
