import { expect, test as base } from '@playwright/test';

export const test = base.extend({});
export { expect };

export function consoleBaseURL(): string {
  const port = Number(process.env.CROWKV_WEB_E2E_PORT ?? 4193);
  return process.env.CROWKV_WEB_E2E_BASE_URL ?? `http://127.0.0.1:${port}`;
}
