// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { apiContext } from '../fixtures/consoleSetup';

test.describe('E2E-02 add rack', () => {
  test('creates a rack through the UI and verifies the real backend', async ({ page, baseURL }) => {
    await page.goto('/');

    await page.getByRole('button', { name: 'Physical' }).click();
    await page.getByRole('button', { name: 'Add Rack' }).click();

    await expect(page.getByRole('dialog', { name: 'Add Rack' })).toBeVisible();
    await page.getByLabel('Rack ID').fill('r1');
    await page.getByLabel('Name (optional)').fill('Rack One');
    await page.getByRole('button', { name: /create rack/i }).click();

    await expect(page.getByRole('alert').getByText(/Rack "r1" created successfully/)).toBeVisible({ timeout: 3_000 });
    await expect(page.locator('aside').getByText('Rack One')).toBeVisible({ timeout: 3_000 });

    const api = await apiContext(baseURL!);
    try {
      const response = await api.get('/api/racks');
      expect(response.ok(), await response.text()).toBeTruthy();
      const racks = await response.json();
      expect(racks).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 'r1', name: 'Rack One' }),
        ]),
      );
    } finally {
      await api.dispose();
    }
  });
});
