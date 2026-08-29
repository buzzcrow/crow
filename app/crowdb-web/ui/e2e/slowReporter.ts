// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Playwright reporter that logs [TEST] lines for slow tests (>= 10 s)
// and [TEST] VERY_SLOW for tests >= 30 s. Composes with the default
// 'list' reporter — see realBackend.config.ts.

import type { Reporter, TestCase, TestResult } from '@playwright/test/reporter';

const SLOW_MS = 10_000;
const VERY_SLOW_MS = 30_000;

export default class SlowReporter implements Reporter {
  onTestEnd(test: TestCase, result: TestResult) {
    const ms = result.duration;
    const tag = ms >= VERY_SLOW_MS ? 'VERY_SLOW' : ms >= SLOW_MS ? 'SLOW' : null;
    if (tag) {
      const title = test.titlePath().slice(1).join(' › ');
      console.log(`[TEST] ${title}: ${ms}ms (${tag})`);
    }
  }
  printsToConsole() { return true; }
}
