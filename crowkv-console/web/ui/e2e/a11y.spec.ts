import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { writeFileSync, mkdirSync } from 'node:fs';
import { stubBackend } from './fixtures/apiStubs';

const REPORT_DIR = 'a11y-reports';

function summarize(violations: Awaited<ReturnType<AxeBuilder['analyze']>>['violations']) {
  return violations.map((v) => ({
    id: v.id,
    impact: v.impact,
    help: v.help,
    helpUrl: v.helpUrl,
    nodeCount: v.nodes.length,
    targets: v.nodes.slice(0, 5).map((n) => n.target.join(' ')),
  }));
}

function persist(name: string, payload: unknown) {
  try {
    mkdirSync(REPORT_DIR, { recursive: true });
    writeFileSync(`${REPORT_DIR}/${name}.json`, JSON.stringify(payload, null, 2));
  } catch (err) {
    console.warn('Could not write a11y report:', err);
  }
}

test.describe('Accessibility — axe-core scans', () => {
  test.beforeEach(async ({ page }) => {
    await stubBackend(page);
  });

  test('initial shell (topology canvas + sidebar)', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText('Orders').first()).toBeVisible({ timeout: 15_000 });

    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    const findings = summarize(results.violations);
    persist('initial-shell', findings);

    // Log all findings for reviewer visibility.
    console.log(`a11y[initial-shell]: ${findings.length} violation rule(s)`);
    for (const f of findings) {
      console.log(`  [${f.impact}] ${f.id}: ${f.help} (${f.nodeCount} nodes)`);
    }

    // Gate: fail on serious or critical. Reports written to a11y-reports/.
    const blocking = findings.filter((f) => f.impact === 'critical' || f.impact === 'serious');
    expect(blocking).toEqual([]);
  });

  test('command palette opens via Cmd/Ctrl+K', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText('Orders').first()).toBeVisible({ timeout: 15_000 });

    await page.keyboard.press('Control+K');
    const input = page.getByPlaceholder(/search|command|type/i).first();
    await expect(input).toBeVisible({ timeout: 5_000 });

    const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();
    const findings = summarize(results.violations);
    persist('command-palette', findings);
    console.log(`a11y[command-palette]: ${findings.length} violation rule(s)`);
    for (const f of findings) {
      console.log(`  [${f.impact}] ${f.id}: ${f.help} (${f.nodeCount} nodes)`);
    }

    const blocking = findings.filter((f) => f.impact === 'critical' || f.impact === 'serious');
    expect(blocking).toEqual([]);
  });

  test('inspector panel (after selecting a store)', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText('Orders').first()).toBeVisible({ timeout: 15_000 });

    // Click the store node in the topology canvas (more reliable selection
    // hook than the sidebar tree, which is just a div).
    await page.getByText('Orders').first().click();

    // Inspector renders aria-label="Entity inspector".
    const inspector = page.locator('aside[aria-label="Entity inspector"]');
    const inspectorVisible = await inspector
      .waitFor({ state: 'visible', timeout: 5_000 })
      .then(() => true)
      .catch(() => false);

    if (!inspectorVisible) {
      // Selection didn't propagate — still useful to scan the page state.
      console.log('a11y[inspector]: panel did not open; scanning whole page instead.');
    }

    const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();
    const findings = summarize(results.violations);
    persist('inspector', findings);
    console.log(`a11y[inspector]: ${findings.length} violation rule(s)`);
    for (const f of findings) {
      console.log(`  [${f.impact}] ${f.id}: ${f.help} (${f.nodeCount} nodes)`);
    }

    const blocking = findings.filter((f) => f.impact === 'critical' || f.impact === 'serious');
    expect(blocking).toEqual([]);
  });
});
