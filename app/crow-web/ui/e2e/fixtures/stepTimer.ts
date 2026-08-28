// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// E2E step timing instrumentation. Import `step` from any test to
// measure async operations — only slow steps (>= 2 s) are logged.

const SLOW_MS = 2_000;
const VERY_SLOW_MS = 5_000;

/** Wrap an async step; log only if it takes >= 2 s. Returns the result. */
export async function step<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const start = Date.now();
  try {
    return await fn();
  } finally {
    const ms = Date.now() - start;
    const tag = ms >= VERY_SLOW_MS ? 'VERY_SLOW' : ms >= SLOW_MS ? 'SLOW' : null;
    if (tag) console.log(`[STEP] ${label}: ${ms}ms (${tag})`);
  }
}
