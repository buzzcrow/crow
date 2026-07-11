import { existsSync } from 'node:fs';
import { defineConfig, devices } from '@playwright/test';

const port = Number(process.env.CROWKV_WEB_E2E_PORT ?? 4193);
const baseURL = `http://127.0.0.1:${port}`;

// Chromium selection, in priority order:
//   1. PLAYWRIGHT_CHANNEL (e.g. "chrome"/"msedge") — handled in the project.
//   2. PLAYWRIGHT_CHROMIUM_EXECUTABLE — explicit binary path.
//   3. A local /snap/bin/chromium if present (dev convenience).
//   4. Playwright's bundled Chromium (CI after `npx playwright install`).
const explicitExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
const snapChromium = '/snap/bin/chromium';
const executablePath = explicitExecutable ?? (existsSync(snapChromium) ? snapChromium : undefined);

const chromiumUse = process.env.PLAYWRIGHT_CHANNEL
  ? { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL }
  : executablePath
    ? { ...devices['Desktop Chrome'], launchOptions: { executablePath } }
    : { ...devices['Desktop Chrome'] };

export default defineConfig({
  testDir: './flows',
  testIgnore: ['**/fixtures/**'],
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
  },
});
