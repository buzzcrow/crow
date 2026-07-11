import { test, expect } from '../fixtures/realBackend';
import { apiContext, createNode, createRack, createStore, deployNodeServer, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';

/**
 * E2E-25 · Destructive confirms for store / node / rack (Req §3.2, §6).
 *
 * Replica/group deletes are covered by 07/08; this closes the physical and
 * logical *root* deletes. Each delete is confirm-gated: we cancel once to
 * prove the guard, then confirm and verify removal in the DOM and via the
 * backend.
 */
test.describe('E2E-25 root deletes', () => {
  test('confirm-gates store, node, and rack deletion', async ({ page, baseURL }) => {
    await seedRackAndNode(baseURL!, 'r25', 'n25');
    await deployNodeServer(baseURL!, 'n25', 9957, 9967);
    await createStore(baseURL!, 255, 2550, 25500, ['n25']);
    // A serverless node (clean to delete) and an empty rack (clean to delete).
    await createNode(baseURL!, { id: 'n25x', rack_id: 'r25' });
    await createRack(baseURL!, { id: 'r25e', name: 'Rack TwentyFive Empty' });

    const api = await apiContext(baseURL!);
    try {
      await page.goto('/');
      const aside = page.locator('aside').first();

      // ── Store (logical) ──────────────────────────────────────────
      await page.getByRole('button', { name: 'Logical' }).click();
      await expect(aside.getByText('S-255', { exact: true })).toBeVisible({ timeout: 20_000 });

      // Cancel first.
      await aside.getByText('S-255', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete store/i }).click();
      await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: 'Cancel' }).click();
      await expect(aside.getByText('S-255', { exact: true })).toBeVisible();

      // Confirm.
      await aside.getByText('S-255', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete store/i }).click();
      await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: /delete store/i }).click();
      await expect(page.getByText(/Store "255" deleted successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(aside.getByText('S-255', { exact: true })).toHaveCount(0, { timeout: 15_000 });

      const storesResp = await api.get('/api/stores');
      expect(storesResp.ok(), await storesResp.text()).toBeTruthy();
      expect(await storesResp.json()).not.toEqual(
        expect.arrayContaining([expect.objectContaining({ store_id: 255 })]),
      );

      // ── Node (physical, serverless n25x) ─────────────────────────
      await page.getByRole('button', { name: 'Physical' }).click();
      const node25x = page.getByRole('treeitem').filter({ hasText: 'N-n25x' });
      await expect(node25x).toBeVisible({ timeout: 20_000 });

      await node25x.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete node/i }).click();
      await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: 'Cancel' }).click();
      await expect(node25x).toBeVisible();

      await node25x.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete node/i }).click();
      await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: /delete node/i }).click();
      await expect(page.getByText(/Node "n25x" deleted successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole('treeitem').filter({ hasText: 'N-n25x' })).toHaveCount(0, { timeout: 15_000 });

      const nodeResp = await api.get('/api/nodes/n25x');
      expect(nodeResp.status()).toBe(404);

      // ── Rack (physical, empty r25e) ──────────────────────────────
      const rack25e = page.getByRole('treeitem').filter({ hasText: 'R-r25e' });
      await expect(rack25e).toBeVisible({ timeout: 15_000 });
      await rack25e.click({ button: 'right' });
      await page.getByRole('menuitem', { name: /delete rack/i }).click();
      await page.getByRole('dialog', { name: 'Delete Rack' }).getByRole('button', { name: /delete rack/i }).click();
      await expect(page.getByText(/Rack "r25e" deleted successfully/)).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole('treeitem').filter({ hasText: 'R-r25e' })).toHaveCount(0, { timeout: 15_000 });

      const racksResp = await api.get('/api/racks');
      expect(racksResp.ok(), await racksResp.text()).toBeTruthy();
      expect(await racksResp.json()).not.toEqual(
        expect.arrayContaining([expect.objectContaining({ id: 'r25e' })]),
      );
    } finally {
      await api.dispose();
      await stopNodeServer(baseURL!, 'n25');
    }
  });
});
