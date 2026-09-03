// Copyright 2026-present Gian <crow.db@outlook.com>.
// Licensed under the Apache License, Version 2.0.

import { describe, it, expect } from 'vitest';
import type { CrowdbConsoleProps } from '../App';
import { Domain } from '../types';

/**
 * Embedding contract: `CrowdbConsoleProps` is the public API surface
 * that host applications use to embed the console. These tests assert
 * the shape is stable and that removed fields (`initialViewMode`,
 * `initialNodeId`, `'swagger'` module) are not accepted.
 */
describe('CrowdbConsoleProps embedding contract', () => {
  it('accepts apiPrefix', () => {
    const props: CrowdbConsoleProps = { apiPrefix: '/proxy/api' };
    expect(props.apiPrefix).toBe('/proxy/api');
  });

  it('accepts basePath', () => {
    const props: CrowdbConsoleProps = { basePath: '/storage/console' };
    expect(props.basePath).toBe('/storage/console');
  });

  it('accepts readonly', () => {
    const props: CrowdbConsoleProps = { readonly: true };
    expect(props.readonly).toBe(true);
  });

  it('accepts modules with known feature keys', () => {
    const props: CrowdbConsoleProps = {
      modules: {
        racks: true,
        nodes: true,
        stores: true,
        groups: true,
        replicas: true,
        kv: true,
        activity: true,
      },
    };
    expect(props.modules?.kv).toBe(true);
    expect(props.modules?.activity).toBe(true);
  });

  it('accepts initialDomain with all Domain values', () => {
    const cluster: CrowdbConsoleProps = { initialDomain: Domain.Cluster };
    const kv: CrowdbConsoleProps = { initialDomain: Domain.KV };
    const chunk: CrowdbConsoleProps = { initialDomain: Domain.Chunk };
    expect(cluster.initialDomain).toBe(Domain.Cluster);
    expect(kv.initialDomain).toBe(Domain.KV);
    expect(chunk.initialDomain).toBe(Domain.Chunk);
  });

  it('accepts onEvent callback', () => {
    const events: unknown[] = [];
    const props: CrowdbConsoleProps = {
      onEvent: (event) => events.push(event),
    };
    props.onEvent?.({ type: 'test', payload: { foo: 1 } });
    expect(events).toHaveLength(1);
    expect(events[0]).toEqual({ type: 'test', payload: { foo: 1 } });
  });

  it('allows empty props (all fields optional)', () => {
    const props: CrowdbConsoleProps = {};
    expect(props.apiPrefix).toBeUndefined();
    expect(props.basePath).toBeUndefined();
    expect(props.readonly).toBeUndefined();
    expect(props.modules).toBeUndefined();
    expect(props.initialDomain).toBeUndefined();
    expect(props.onEvent).toBeUndefined();
  });

  // Compile-time assertion: removed fields must not be accepted.
  // If `initialViewMode`, `initialNodeId`, or `'swagger'` are re-added
  // to the interface, these type checks will fail to compile.
  it('does not accept removed initialViewMode field', () => {
    // @ts-expect-error — initialViewMode was removed
    const _props: CrowdbConsoleProps = { initialViewMode: 'physical' };
    void _props;
  });

  it('does not accept removed initialNodeId field', () => {
    // @ts-expect-error — initialNodeId was removed
    const _props: CrowdbConsoleProps = { initialNodeId: 1 };
    void _props;
  });

  it('does not accept swagger as a module key', () => {
    // @ts-expect-error — 'swagger' module was removed
    const _props: CrowdbConsoleProps = { modules: { swagger: true } };
    void _props;
  });
});
