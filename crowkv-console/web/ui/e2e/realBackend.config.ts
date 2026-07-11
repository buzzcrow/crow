import { defineConfig, devices } from '@playwright/test';

const port = Number(process.env.CROWKV_WEB_E2E_PORT ?? 4193);
const baseURL = `http://127.0.0.1:${port}`;
const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE ?? '/snap/bin/chromium';

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
      use: process.env.PLAYWRIGHT_CHANNEL
        ? { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL }
        : { ...devices['Desktop Chrome'], launchOptions: { executablePath } },
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
