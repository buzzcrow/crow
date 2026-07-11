import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright config for in-browser audits (axe-core + interaction smoke).
 * Boots `vite preview` against the production bundle so we audit what
 * users actually receive.
 */
export default defineConfig({
  testDir: './e2e',
  testIgnore: ['**/fixtures/**'],
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: [['list']],
  use: {
    baseURL: 'http://127.0.0.1:4173',
    trace: 'retain-on-failure',
    headless: true,
  },
  projects: [
    {
      name: 'chromium',
      // If Playwright's bundled Chromium download is unavailable, point
      // PLAYWRIGHT_CHANNEL=msedge to use the system Microsoft Edge install
      // (which is also Chromium-based) without any download.
      use: process.env.PLAYWRIGHT_CHANNEL
        ? { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL }
        : { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npm run build && npm run preview -- --port 4173 --strictPort',
    url: 'http://127.0.0.1:4173',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    stdout: 'ignore',
    stderr: 'pipe',
  },
});
