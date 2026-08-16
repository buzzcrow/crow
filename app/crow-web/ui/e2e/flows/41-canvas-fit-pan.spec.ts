// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 2s (2026-08-16)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import { createRack, createNode, deployNodeServer, stopNodeServer, freePort } from '../fixtures/consoleSetup';

// One rack/node/server shared by every test in this file so the canvas
// has content in Physical and Capacity views (IDs reused from the former
// 48-canvas-fit-pan spec so they stay unique).
const apiBase = consoleBaseURL();

test.describe('canvas · fit + pan', () => {
  test.beforeAll(async () => {
    try {
      await createRack(apiBase, { id: 480, name: 'Rack 480' });
      await createNode(apiBase, { id: 480, rack_id: 480 });
      await deployNodeServer(apiBase, 480, freePort(), freePort());
    } catch (err) {
      await stopNodeServer(apiBase, 480);
      throw err;
    }
  });

  test.afterAll(async () => {
    await stopNodeServer(apiBase, 480);
  });

  test('Fit All is available in every view and autofit centers nodes on load', async ({ page }) => {
    // --- Fit All button visible/enabled in Physical view with content ---
    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    const fitBtn = page.getByTestId('fit-all-btn');
    await expect(fitBtn).toBeVisible();
    await expect(fitBtn).toBeEnabled();

    // Wait for the canvas to render at least one react-flow node.
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });
    await expect(fitBtn).toBeVisible();

    // --- Fit All button visible in KV Cluster view (empty canvas) ---
    await page.getByRole('button', { name: 'KV Cluster' }).click();
    await expect(page.getByTestId('fit-all-btn')).toBeVisible();

    // --- Fit All button visible in Capacity view ---
    await page.getByRole('button', { name: 'Capacity' }).click();
    await expect(page.getByTestId('fit-all-btn')).toBeVisible();

    // --- autofit centers nodes on initial load ---
    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    // Wait for nodes and the auto-fit to settle. The canvas should
    // center the nodes in the viewport — verify a node is within the
    // visible area (not scrolled off-screen).
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    // Verify at least one node is within the viewport bounds.
    const nodeBox = await page.locator('.react-flow__node').first().boundingBox();
    const canvasBox = await page.locator('.react-flow').boundingBox();
    expect(nodeBox).not.toBeNull();
    expect(canvasBox).not.toBeNull();
    if (nodeBox && canvasBox) {
      // Node center should be within the canvas bounds (with some margin).
      const nodeCenterX = nodeBox.x + nodeBox.width / 2;
      const nodeCenterY = nodeBox.y + nodeBox.height / 2;
      expect(nodeCenterX).toBeGreaterThan(canvasBox.x);
      expect(nodeCenterX).toBeLessThan(canvasBox.x + canvasBox.width);
      expect(nodeCenterY).toBeGreaterThan(canvasBox.y);
      expect(nodeCenterY).toBeLessThan(canvasBox.y + canvasBox.height);
    }
  });

  test('clicking Fit All resets the viewport after panning', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    // Wait for nodes to render.
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    // Pan the canvas by dragging the background.
    const viewport = page.locator('.react-flow__viewport');
    const canvas = page.locator('.react-flow');
    await canvas.hover({ position: { x: 200, y: 200 } });
    await page.mouse.down();
    await page.mouse.move(400, 400);
    await page.mouse.up();

    // After panning, the viewport transform should have changed.
    const transformAfterPan = await viewport.evaluate((el) => (el as HTMLElement).style.transform);

    // Click Fit All to reset the view.
    await page.getByTestId('fit-all-btn').click();

    // Wait for the fit animation (250ms) to complete and verify the
    // transform changed from the panned state.
    await expect.poll(async () => {
      return viewport.evaluate((el) => (el as HTMLElement).style.transform);
    }, { timeout: 3_000, intervals: [100] }).not.toEqual(transformAfterPan);
  });

  test('switching views always fits to window, not the stale viewport', async ({ page }) => {
    // --- Physical -> KV Cluster -> Physical ---
    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    const viewport = page.locator('.react-flow__viewport');
    const canvas = page.locator('.react-flow');

    // Wait for the initial auto-fit to settle, capturing the fitted transform.
    await page.waitForTimeout(500);
    const fittedTransform = await viewport.evaluate((el) => (el as HTMLElement).style.transform);
    expect(fittedTransform).toBeTruthy();

    // Pan the canvas away from the fitted position.
    await canvas.hover({ position: { x: 200, y: 200 } });
    await page.mouse.down();
    await page.mouse.move(400, 400);
    await page.mouse.up();

    // Confirm the transform changed after panning.
    const pannedTransform = await viewport.evaluate((el) => (el as HTMLElement).style.transform);
    expect(pannedTransform).not.toEqual(fittedTransform);

    // Switch to KV Cluster view — canvas should fit to window (no nodes
    // in KV Cluster since no stores, but the button must be visible).
    await page.getByRole('button', { name: 'KV Cluster' }).click();
    await expect(page.getByTestId('fit-all-btn')).toBeVisible();

    // Switch back to Physical — should fit to window, NOT restore the
    // panned viewport. The transform should match the fitted state
    // (translate ~0, scale ~1), not the panned offset.
    await page.getByRole('button', { name: 'Physical' }).click();
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    // After switching back, the viewport should be re-fitted, not the
    // stale panned position. Give the fit animation time to complete.
    await expect.poll(async () => {
      return viewport.evaluate((el) => (el as HTMLElement).style.transform);
    }, { timeout: 3_000, intervals: [100] }).not.toEqual(pannedTransform);

    // --- Physical -> Capacity -> Physical ---
    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    // Pan in Physical view.
    await canvas.hover({ position: { x: 200, y: 200 } });
    await page.mouse.down();
    await page.mouse.move(400, 400);
    await page.mouse.up();
    const physicalPanned = await viewport.evaluate((el) => (el as HTMLElement).style.transform);

    // Switch to Capacity — should fit (rack-node hierarchy).
    await page.getByRole('button', { name: 'Capacity' }).click();
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(500);
    const capacityFit = await viewport.evaluate((el) => (el as HTMLElement).style.transform);

    // Pan in Capacity view.
    await canvas.hover({ position: { x: 200, y: 200 } });
    await page.mouse.down();
    await page.mouse.move(400, 400);
    await page.mouse.up();
    const capacityPanned = await viewport.evaluate((el) => (el as HTMLElement).style.transform);
    expect(capacityPanned).not.toEqual(capacityFit);

    // Switch back to Physical — should fit to window, NOT restore the
    // panned Physical viewport from the first visit.
    await page.getByRole('button', { name: 'Physical' }).click();
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    await expect.poll(async () => {
      return viewport.evaluate((el) => (el as HTMLElement).style.transform);
    }, { timeout: 3_000, intervals: [100] }).not.toEqual(physicalPanned);
  });
});
