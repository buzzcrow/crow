import { existsSync } from 'node:fs';
import { defineConfig, devices } from '@playwright/test';

// Browser selection, in priority order:
//   1. PLAYWRIGHT_CHANNEL (e.g. "chrome"/"msedge") — use Playwright's channel support.
//   2. PLAYWRIGHT_CHROMIUM_EXECUTABLE — explicit binary path override.
//   3. Local Chromium (Linux snap, Linux apt, macOS app).
//   4. Local Microsoft Edge (Linux /usr/bin, macOS app).
//   5. macOS Google Chrome (common dev install; Safari is the macOS default
//      but Playwright cannot drive it directly — no CDP support).
//   6. Playwright's bundled Chromium (CI after `npx playwright install`).
const explicitExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
const localBrowsers = [
  '/snap/bin/chromium',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
  '/usr/bin/google-chrome',
  '/usr/bin/google-chrome-stable',
  '/Applications/Chromium.app/Contents/MacOS/Chromium',
  '/usr/bin/microsoft-edge',
  '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
];
const executablePath = explicitExecutable
  ?? localBrowsers.find((p) => existsSync(p));

const chromiumUse = process.env.PLAYWRIGHT_CHANNEL
  ? { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL }
  : executablePath
    ? { ...devices['Desktop Chrome'], launchOptions: { executablePath } }
    : { ...devices['Desktop Chrome'] };

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
      use: chromiumUse,
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
