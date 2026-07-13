// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';

test.describe('E2E-01 fresh registry', () => {
  test('renders the SPA shell against a real empty backend', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (message) => {
      if (message.type() === 'error') {
        consoleErrors.push(message.text());
      }
    });

    await page.goto('/');

    await expect(page.getByRole('button', { name: 'Physical' })).toBeVisible({ timeout: 15_000 });
    await expect(page.getByRole('button', { name: 'Logical' })).toBeVisible({ timeout: 15_000 });
    await expect(page.getByPlaceholder('Filter...')).toBeVisible();

    const healthText = page.getByText(/healthy|degraded|failed|unknown/i).first();
    await expect(healthText).toBeVisible({ timeout: 15_000 });

    // Ignore transient network 404s; fail only on real JS/runtime errors.
    const jsErrors = consoleErrors.filter((e) => !/Failed to load resource/i.test(e));
    expect(jsErrors, jsErrors.join('\n')).toEqual([]);
  });
});
