// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { DomainProvider } from '../contexts/DomainContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import { Sidebar } from './Sidebar';
import {
  Domain,
  NodeHealth,
  ProcState,
  Rack,
  CrowdbKVServerView,
  EnrichedStoreView,
} from '../types';
import type { NodeDiskGroups } from '../data/useCapacityTree';

const rack: Rack = {
  id: 1,
  name: 'r1',
  nodes: [
    { id: 10, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' }, has_server: true, kv_server: { mgmt_url: 'http://127.0.0.1:19910', rpc_url: 'http://127.0.0.1:19920', state: ProcState.Running, health: NodeHealth.Up, last_seen_ms: 0 } },
    { id: 11, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
  ],
};

const servers: CrowdbKVServerView[] = [
  {
    id: 'n10-kv',
    node_id: 10,
    rack_id: 1,
    host: '127.0.0.1',
    process: { mgmt_url: 'http://127.0.0.1:19910', rpc_url: 'http://127.0.0.1:19920', state: ProcState.Running, health: NodeHealth.Up, last_seen_ms: 0 },
    rest_port: 19910,
    rpc_port: 19920,
  },
];

const stores: EnrichedStoreView[] = [
  {
    store_id: '7',
    nodes: [10, 11],
    groups: [
      {
        store_id: '7',
        group_id: '70',
        replicas: [
          { replica_id: '700', node_id: 10, store_id: '7', group_id: '70', role: 'leader' as any, state: 'running' as any, engine_healthy: true },
          { replica_id: '701', node_id: 11, store_id: '7', group_id: '70', role: 'follower' as any, state: 'running' as any, engine_healthy: true },
        ],
        state: 'healthy' as any,
      },
    ],
  },
];

const diskGroups: Record<number, NodeDiskGroups> = {
  10: {
    diskGroups: [{ id: 100, rack_id: 1, node_id: 10, name: 'Physical Group' }],
    disksByDg: {
      100: [{ disk_id: '0123456789abcdef-0123456789abcdef', disk_group_id: 100, rack_id: 1, node_id: 10, disk_type: 'Hdd', capacity_bytes: 0, zone_size_bytes: 0, unit_size_bytes: 0 }],
    },
  },
};

function renderSidebar(domain: Domain, props: Record<string, unknown> = {}) {
  return render(
    <DomainProvider initialDomain={domain}>
      <SelectionProvider>
        <Sidebar
          racks={[rack]}
          servers={servers}
          stores={stores}
          nodeHealthById={{ '10': NodeHealth.Up, '11': NodeHealth.Unknown }}
          nodeDiskGroups={diskGroups}
          diskdbNodeIds={new Set([10])}
          diskdbHealthById={new Map([[10, 'up']])}
          {...props}
        />
      </SelectionProvider>
    </DomainProvider>,
  );
}

describe('Sidebar · Cluster tree projection', () => {
  it('renders rack → node → KV server under the node', () => {
    const { getByText, queryByText } = renderSidebar(Domain.Cluster);
    expect(getByText(/R-1/)).toBeTruthy();
    expect(getByText('N-10', { exact: true })).toBeTruthy();
    // KV server appears as a child of node 10, not as a top-level item.
    expect(getByText('KV-10', { exact: true })).toBeTruthy();
    // Node 11 has no KV server — no KV-11 item.
    expect(queryByText('KV-11')).toBeNull();
  });

  it('renders disk groups and disks under the node alongside the KV server', () => {
    const { getByText } = renderSidebar(Domain.Cluster);
    expect(getByText(/Physical Group.*DG-100/)).toBeTruthy();
    // Disk ID is truncated to 12 chars + ellipsis.
    expect(getByText('0123456789ab…')).toBeTruthy();
  });
});

describe('Sidebar · KV logical projection', () => {
  it('renders datacenter → store → group → replica without KV-server parents', () => {
    const { getByText, queryByText } = renderSidebar(Domain.KV);
    // Logical tree: datacenter → store → group → replica.
    expect(getByText('S-7', { exact: true })).toBeTruthy();
    expect(getByText('G-70', { exact: true })).toBeTruthy();
    expect(getByText('LR-700', { exact: true })).toBeTruthy();
    expect(getByText('LR-701', { exact: true })).toBeTruthy();
    // No physical KV-server or node items in the KV tree.
    expect(queryByText('KV-10')).toBeNull();
    expect(queryByText('N-10')).toBeNull();
  });
});

describe('Sidebar · Chunk hierarchy', () => {
  it('renders node → disk group → disk with DiskDB as a sibling, no owned DG subtree', () => {
    const { getByText, queryByText } = renderSidebar(Domain.Chunk);
    expect(getByText('N-10', { exact: true })).toBeTruthy();
    // Physical disk group is under the node.
    expect(getByText(/Physical Group.*DG-100/)).toBeTruthy();
    expect(getByText('0123456789ab…')).toBeTruthy();
    // DiskDB is a sibling item under the node.
    expect(getByText('DDB-10', { exact: true })).toBeTruthy();
    // No KV server item in the Chunk tree.
    expect(queryByText('KV-10')).toBeNull();
  });
});
