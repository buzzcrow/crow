// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { listRacks } from '../api';

/**
 * Regression test for the "click Create Rack does nothing" bug
 * (doc/todo_ui2.md §5.5): the backend returns a flat array at
 * `recursive=0` but switches to `{ items, truncated_at }` at
 * `recursive>=1` (see `crowkv-console/web/src/lifecycle.rs`
 * `http_list_racks`). The SPA must unwrap both shapes so a newly
 * created rack actually shows up in the sidebar after the post-create
 * refresh.
 */
describe('listRacks envelope handling', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const url = typeof input === 'string' ? input : input.toString();
        if (url.includes('recursive=2')) {
          return new Response(
            JSON.stringify({
              items: [{ id: 'r1', name: '', nodes: [] }],
              truncated_at: [],
            }),
            { status: 200, headers: { 'content-type': 'application/json' } },
          );
        }
        return new Response(JSON.stringify([{ id: 'r0', name: '', nodes: [] }]), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }),
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('returns the items array when the backend wraps with recursive>=1', async () => {
    const racks = await listRacks(2);
    expect(Array.isArray(racks)).toBe(true);
    expect(racks.map((r) => r.id)).toEqual(['r1']);
  });

  it('passes through a flat array (recursive omitted)', async () => {
    const racks = await listRacks();
    expect(racks.map((r) => r.id)).toEqual(['r0']);
  });
});
