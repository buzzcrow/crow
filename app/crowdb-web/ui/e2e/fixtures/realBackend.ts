// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect, test as base } from '@playwright/test';

export const test = base.extend({});
export { expect };

export function consoleBaseURL(): string {
  const port = Number(process.env.CROWDB_WEB_E2E_PORT ?? 4193);
  return process.env.CROWDB_WEB_E2E_BASE_URL ?? `http://127.0.0.1:${port}`;
}
