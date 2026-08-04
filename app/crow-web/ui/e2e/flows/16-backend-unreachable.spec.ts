// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 0.2s (2026-07-16)

import { test, expect } from '../fixtures/realBackend';

test.describe('E2E-16 backend unreachable', () => {
  test('shows an alert when backend API requests fail', async ({ page }) => {
    await page.route('**/api/**', route => route.abort('failed'));

    await page.goto('/');

    await expect(page.getByRole('alert')).toContainText('Backend unreachable', { timeout: 3_000 });
  });
});
