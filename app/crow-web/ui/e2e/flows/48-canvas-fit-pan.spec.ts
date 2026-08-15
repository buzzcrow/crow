// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 2s (2026-08-15)

import { test, expect } from '../fixtures/realBackend';
import { createRack, createNode, deployNodeServer, stopNodeServer, freePort } from '../fixtures/consoleSetup';

test.describe('E2E-48 canvas fit & pan', () => {
  test('Fit All button is visible in empty state and after data loads', async ({ page, baseURL }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();

    // Fit All button must be visible even when the canvas is empty.
    const fitBtn = page.getByTestId('fit-all-btn');
    await expect(fitBtn).toBeVisible();
    await expect(fitBtn).toBeEnabled();

    // Create a rack + node + deploy server so the canvas has content.
    await createRack(baseURL!, { id: 480, name: 'Rack 480' });
    await createNode(baseURL!, { id: 480, rack_id: 480 });
    await deployNodeServer(baseURL!, 480, freePort(), freePort());

    // Wait for the canvas to render at least one react-flow node.
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });
    await expect(fitBtn).toBeVisible();

    await stopNodeServer(baseURL!, 480);
  });

  test('clicking Fit All resets the viewport', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 481, name: 'Rack 481' });
    await createNode(baseURL!, { id: 481, rack_id: 481 });
    await deployNodeServer(baseURL!, 481, freePort(), freePort());

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

    await stopNodeServer(baseURL!, 481);
  });

  test('autofit centers nodes on initial load', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 482, name: 'Rack 482' });
    await createNode(baseURL!, { id: 482, rack_id: 482 });
    await deployNodeServer(baseURL!, 482, freePort(), freePort());

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

    await stopNodeServer(baseURL!, 482);
  });

  test('Fit All button visible in KV Cluster view', async ({ page, baseURL }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'KV Cluster' }).click();
    const fitBtn = page.getByTestId('fit-all-btn');
    await expect(fitBtn).toBeVisible();
  });

  test('Fit All button visible in Capacity view', async ({ page, baseURL }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Capacity' }).click();
    const fitBtn = page.getByTestId('fit-all-btn');
    await expect(fitBtn).toBeVisible();
  });

  test('switching views always fits to window, not stale viewport', async ({ page, baseURL }) => {
    // Set up topology with a rack + node + deployed server so Physical
    // and Capacity views have nodes to render.
    await createRack(baseURL!, { id: 483, name: 'Rack 483' });
    await createNode(baseURL!, { id: 483, rack_id: 483 });
    await deployNodeServer(baseURL!, 483, freePort(), freePort());

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    const viewport = page.locator('.react-flow__viewport');

    // Wait for the initial auto-fit to settle, capturing the fitted transform.
    await page.waitForTimeout(500);
    const fittedTransform = await viewport.evaluate((el) => (el as HTMLElement).style.transform);
    expect(fittedTransform).toBeTruthy();

    // Pan the canvas away from the fitted position.
    const canvas = page.locator('.react-flow');
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

    await stopNodeServer(baseURL!, 483);
  });

  test('switching Physical -> Capacity -> Physical fits each time', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 484, name: 'Rack 484' });
    await createNode(baseURL!, { id: 484, rack_id: 484 });
    await deployNodeServer(baseURL!, 484, freePort(), freePort());

    await page.goto('/');
    await page.getByRole('button', { name: 'Physical' }).click();
    await expect(page.locator('.react-flow__node').first()).toBeVisible({ timeout: 5_000 });

    const viewport = page.locator('.react-flow__viewport');
    const canvas = page.locator('.react-flow');

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

    await stopNodeServer(baseURL!, 484);
  });
});
