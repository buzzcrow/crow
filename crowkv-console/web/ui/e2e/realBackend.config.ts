import { existsSync } from 'node:fs';
import { defineConfig, devices } from '@playwright/test';

const port = Number(process.env.CROWKV_WEB_E2E_PORT ?? 4193);
const baseURL = `http://127.0.0.1:${port}`;

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

export default defineConfig({
  testDir: './flows',
  testIgnore: ['**/fixtures/**'],
  globalSetup: './globalSetup.ts',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: [['list']],
  use: {
    baseURL,
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
    command: `npm run build && cargo run -p crowkv-web -- --bind 127.0.0.1 --port ${port} --test-mode`,
    url: `${baseURL}/healthz`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    stdout: 'pipe',
    stderr: 'pipe',
    env: { ...process.env, CROWKV_SERVER_ELECTION_PROFILE: 'e2e' },
  },
});
