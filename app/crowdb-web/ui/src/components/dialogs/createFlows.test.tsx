// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ReactNode } from 'react';
import { ToastProvider } from '../../contexts/ToastContext';
import { AddRackDialog } from './AddRackDialog';
import { AddNodeDialog } from './AddNodeDialog';
import { AddStoreDialog } from './AddStoreDialog';
import { AddGroupDialog } from './AddGroupDialog';
import { AddReplicaDialog } from './AddReplicaDialog';
import { DeployServerDialog } from './DeployServerDialog';
import { NodeHealth, ProcState } from '../../types';
import type { Node, Rack, CrowdbKVServerView, EnrichedStoreView } from '../../types';
import { deployPortDefaultsForNode, diskdbPortDefaultsForNode, minUnusedId } from './defaults';

/**
 * These tests pin down the exact request bodies the SPA must send for the
 * end-to-end flow described in `doc/todo_ui2.md` §5.4. They are the
 * frontend counterpart to the Rust integration tests in
 * `crowdb-console/web/tests/{lifecycle,mgmt,replica}_routes.rs`.
 */

const wrapper = ({ children }: { children: ReactNode }) => (
  <ToastProvider>{children}</ToastProvider>
);

interface CapturedRequest {
  url: string;
  method: string;
  body: any;
}

let captured: CapturedRequest[] = [];

function installFetchMock(response: any = {}, status = 200) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    const method = (init?.method || 'GET').toUpperCase();
    const bodyText = typeof init?.body === 'string' ? init.body : '';
    captured.push({
      url,
      method,
      body: bodyText ? JSON.parse(bodyText) : null,
    });
    return new Response(JSON.stringify(response), {
      status,
      headers: { 'content-type': 'application/json' },
    });
  });
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

beforeEach(() => {
  captured = [];
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const mockRack: Rack = { id: 1, name: 'r1', nodes: [] };
const mockNodes: Node[] = [
  { id: 1, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
  { id: 2, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
];
const mockServers: CrowdbKVServerView[] = [
  {
    id: 'n1-kv',
    node_id: 1,
    rack_id: 1,
    host: '127.0.0.1',
    process: {
      mgmt_url: 'http://127.0.0.1:19910',
      rpc_url: 'http://127.0.0.1:19920',
      state: ProcState.Running,
      health: NodeHealth.Up,
      last_seen_ms: Date.now(),
    },
    rest_port: 19910,
    rpc_port: 19920,
  },
];
const mockStores: EnrichedStoreView[] = [
  { store_id: '7', nodes: [1, 2], groups: [] },
];

describe('Add Rack dialog', () => {
  it('POSTs { id, name? } to /api/racks', async () => {
    installFetchMock({ id: 1, name: 'Rack 1', nodes: [] });
    render(<AddRackDialog isOpen onClose={() => {}} />, { wrapper });

    fireEvent.change(screen.getByLabelText('Rack ID'), { target: { value: '1' } });
    fireEvent.change(screen.getByLabelText('Name (optional)'), { target: { value: 'Rack 1' } });
    fireEvent.click(screen.getByRole('button', { name: /create rack/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/racks',
      method: 'POST',
      body: { id: 1, name: 'Rack 1' },
    });
  });

  it('auto-fills the next rack id from existing racks', () => {
    render(
      <AddRackDialog isOpen onClose={() => {}} existingRackIds={['rack1', 'rack2']} />,
      { wrapper },
    );

    expect((screen.getByLabelText('Rack ID') as HTMLInputElement).value).toBe('3');
  });
});

describe('Add Node dialog', () => {
  it('POSTs flat NodeEntry to /api/nodes', async () => {
    installFetchMock({ id: 1 });
    render(
      <AddNodeDialog isOpen onClose={() => {}} racks={[mockRack]} defaultRackId="1" />,
      { wrapper },
    );

    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: '1' } });
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: '127.0.0.1' } });
    fireEvent.click(screen.getByLabelText('Enable CrowDB Storage on this node'));
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].url).toBe('/api/nodes');
    expect(captured[0].method).toBe('POST');
    // Backend `NodeEntry` shape — flat fields, NO nested `ssh` object.
    expect(captured[0].body).toEqual({
      id: 1,
      rack_id: 1,
      host: '127.0.0.1',
      ssh_port: 22,
      ssh_user: '',
    });
    expect(captured[0].body.ssh).toBeUndefined();
  });

  it('includes ssh_user + ssh_key when provided', async () => {
    installFetchMock({ id: 1 });
    render(
      <AddNodeDialog isOpen onClose={() => {}} racks={[mockRack]} defaultRackId="1" />,
      { wrapper },
    );

    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: '1' } });
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: '10.0.0.1' } });
    fireEvent.change(screen.getByLabelText('SSH User (optional)'), { target: { value: 'crowdb-kv' } });
    fireEvent.change(screen.getByLabelText('SSH Key Path (optional)'), {
      target: { value: '/keys/id_rsa' },
    });
    fireEvent.click(screen.getByLabelText('Enable CrowDB Storage on this node'));
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({
      id: 1,
      rack_id: 1,
      host: '10.0.0.1',
      ssh_port: 22,
      ssh_user: 'crowdb-kv',
      ssh_key: '/keys/id_rsa',
    });
  });

  it('uses default rack/node/host for one-click create', async () => {
    installFetchMock({ id: 'node2' });
    render(
      <AddNodeDialog
        isOpen
        onClose={() => {}}
        racks={[mockRack]}
        defaultRackId="1"
        existingNodeIds={['node1']}
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByLabelText('Enable CrowDB Storage on this node'));
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({
      id: 2,
      rack_id: 1,
      host: '127.0.0.1',
      ssh_port: 22,
      ssh_user: '',
    });
  });

  it('enables CrowDB Storage by default and deploys immediately after node creation', async () => {
    installFetchMock({ id: 1 });
    render(
      <AddNodeDialog
        isOpen
        onClose={() => {}}
        racks={[mockRack]}
        defaultRackId="1"
        defaultRestPort="19911"
        defaultRpcPort="19921"
      />,
      { wrapper },
    );

    expect((screen.getByLabelText('Enable CrowDB Storage on this node') as HTMLInputElement).checked).toBe(true);
    // Disable DiskDB — this test focuses on the CrowDB Storage deploy flow.
    fireEvent.click(screen.getByLabelText('Enable DiskDB on this node'));
    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: '1' } });
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: '127.0.0.1' } });
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(2));
    expect(captured[0]).toMatchObject({
      url: '/api/nodes',
      method: 'POST',
      body: {
        id: 1,
        rack_id: 1,
        host: '127.0.0.1',
        ssh_port: 22,
        ssh_user: '',
      },
    });
    expect(captured[1]).toMatchObject({
      url: '/api/nodes/1/server/deploy',
      method: 'POST',
      body: { rest_port: 19911, rpc_port: 19921 },
    });
  });
});

describe('Deploy Server dialog', () => {
  it('POSTs DeployNodeServerBody to /api/nodes/:id/server/deploy', async () => {
    installFetchMock({ node_id: 1, pid: 1234, mgmt_url: 'x', rpc_url: 'y' });
    render(<DeployServerDialog isOpen onClose={() => {}} nodeId={1} />, { wrapper });

    fireEvent.change(screen.getByLabelText('REST Port'), { target: { value: '19911' } });
    fireEvent.change(screen.getByLabelText('RPC Port'), { target: { value: '19921' } });
    fireEvent.click(screen.getByRole('button', { name: /deploy/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/nodes/1/server/deploy',
      method: 'POST',
      body: { rest_port: 19911, rpc_port: 19921 },
    });
    expect(captured[0].body.binary).toBeUndefined();
  });

  it('submits immediately with provided default ports', async () => {
    installFetchMock({ node_id: 1, pid: 1234, mgmt_url: 'x', rpc_url: 'y' });
    render(
      <DeployServerDialog
        isOpen
        onClose={() => {}}
        nodeId={1}
        defaultRestPort="19915"
        defaultRpcPort="19925"
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /deploy/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ rest_port: 19915, rpc_port: 19925 });
  });

  it('increments ports only when the same node already uses them', () => {
    const defaults = deployPortDefaultsForNode(
      [
        {
          id: 1,
          server: { mgmt_url: 'http://127.0.0.1:19910', rpc_url: 'http://127.0.0.1:19920' },
        },
        {
          id: 2,
          server: { mgmt_url: 'http://127.0.0.1:19910', rpc_url: 'http://127.0.0.1:19920' },
        },
      ],
      1,
    );

    expect(defaults).toEqual({ defaultRestPort: '19911', defaultRpcPort: '19921' });
  });

  it('increments globally when a different node already uses the base ports', () => {
    const defaults = deployPortDefaultsForNode(
      [
        {
          id: 2,
          server: { mgmt_url: 'http://127.0.0.1:19910', rpc_url: 'http://127.0.0.1:19920' },
        },
      ],
      1,
    );

    expect(defaults).toEqual({ defaultRestPort: '19911', defaultRpcPort: '19921' });
  });

  it('derives defaults from the node id suffix before checking collisions', () => {
    const defaults = deployPortDefaultsForNode([], 2, 19910, 19920);

    expect(defaults).toEqual({ defaultRestPort: '19912', defaultRpcPort: '19922' });
  });

  it('can increment from remembered same-node ports even after the server is gone', () => {
    const defaults = deployPortDefaultsForNode([], 1, 19910, 19920, [19910], [19920]);

    expect(defaults).toEqual({ defaultRestPort: '19911', defaultRpcPort: '19921' });
  });
});

describe('diskdbPortDefaultsForNode', () => {
  it('returns the base port when no instances or remembered ports collide', () => {
    expect(diskdbPortDefaultsForNode([], 1)).toBe('29921');
  });

  it('derives defaults from the node id suffix before checking collisions', () => {
    expect(diskdbPortDefaultsForNode([], 2)).toBe('29922');
  });

  it('increments past ports already assigned to other diskdb instances', () => {
    const instances = [{ rpc_endpoint: 'http://127.0.0.1:29921' }];
    expect(diskdbPortDefaultsForNode(instances, 1)).toBe('29922');
  });

  it('increments past remembered ports even when no instances exist', () => {
    expect(diskdbPortDefaultsForNode([], 1, undefined, [29921])).toBe('29922');
  });
});

describe('minUnusedId', () => {
  it('returns min when no ids exist', () => {
    expect(minUnusedId([], 1)).toBe('1');
  });

  it('returns min when ids do not conflict', () => {
    expect(minUnusedId([5, 6, 7], 1)).toBe('1');
  });

  it('fills the first gap', () => {
    expect(minUnusedId([1, 3, 4], 1)).toBe('2');
  });

  it('returns max+1 when no gaps exist', () => {
    expect(minUnusedId([1, 2, 3], 1)).toBe('4');
  });

  it('ignores non-numeric ids', () => {
    expect(minUnusedId(['abc', '1', '3'], 1)).toBe('2');
  });
});

describe('Add Store dialog', () => {
  it('POSTs numeric store_id + nodes', async () => {
    installFetchMock({ store_id: 7 }, 201);
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={mockServers} />, { wrapper });

    fireEvent.change(screen.getByLabelText('KV Store ID (numeric)'), { target: { value: '7' } });
    // Tick n1.
    fireEvent.click(screen.getByLabelText(/^1\b/));
    fireEvent.click(screen.getByRole('button', { name: /create kv store/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/stores',
      method: 'POST',
      body: { store_id: 7, nodes: [1] },
    });
  });

  it('keeps the Create button disabled until id is numeric and a deployed CrowDB Storage node is picked', () => {
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={mockServers} />, { wrapper });
    const btn = screen.getByRole('button', { name: /create kv store/i });
    expect(btn).toBeDisabled();

    fireEvent.change(screen.getByLabelText('KV Store ID (numeric)'), { target: { value: 'abc' } });
    fireEvent.click(screen.getByLabelText(/^1\b/));
    expect(btn).toBeDisabled(); // store_id not numeric

    fireEvent.change(screen.getByLabelText('KV Store ID (numeric)'), { target: { value: '7' } });
    expect(btn).toBeEnabled();
  });

  it('can submit with prefilled store defaults', async () => {
    installFetchMock({ store_id: 9 }, 201);
    render(
      <AddStoreDialog
        isOpen
        onClose={() => {}}
        nodes={mockNodes}
        servers={mockServers}
        defaultStoreId="9"
        defaultNodeIds={[1]}
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /create kv store/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ store_id: 9, nodes: [1] });
  });

  it('only lists nodes that already run CrowDB Storage', () => {
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={mockServers} />, { wrapper });

    expect(screen.getByLabelText(/^1\b/)).toBeInTheDocument();
    expect(screen.queryByLabelText(/^n2/)).toBeNull();
  });

  it('excludes configured but unavailable CrowDB Storage nodes', () => {
    const unavailableServers: CrowdbKVServerView[] = [
      ...mockServers,
      {
        id: 'n2-kv',
        node_id: 2,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          rpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Stopped,
          health: NodeHealth.Down,
          last_seen_ms: Date.now(),
        },
        rest_port: 29910,
        rpc_port: 29920,
      },
    ];

    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={unavailableServers} />, { wrapper });

    expect(screen.getByLabelText(/^1\b/)).toBeInTheDocument();
    expect(screen.queryByLabelText(/^n2/)).toBeNull();
  });
});

describe('Add Group dialog', () => {
  it('POSTs CreateGroupBody to /api/stores/:sid/groups', async () => {
    installFetchMock({ group_id: 80 }, 201);
    const bothServers: CrowdbKVServerView[] = [
      ...mockServers,
      {
        id: 'KV-n2',
        node_id: 2,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          rpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        rest_port: 29910,
        rpc_port: 29920,
      },
    ];
    render(
      <AddGroupDialog isOpen onClose={() => {}} storeId="7" stores={mockStores} nodes={mockNodes} servers={bothServers} />,
      { wrapper },
    );

    fireEvent.change(screen.getByLabelText('Group ID (numeric)'), { target: { value: '80' } });
    fireEvent.change(screen.getByLabelText('Starting Replica ID (numeric)'), {
      target: { value: '800' },
    });
    expect(screen.getByLabelText(/^1\b/) as HTMLInputElement).toBeChecked();
    expect(screen.getByLabelText(/^2\b/) as HTMLInputElement).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: /create group/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/stores/7/groups',
      method: 'POST',
      body: { group_id: 80, replica_id: 800, nodes: [1, 2] },
    });
  });

  it('can submit with prefilled group defaults', async () => {
    installFetchMock({ group_id: 81 }, 201);
    render(
      <AddGroupDialog
        isOpen
        onClose={() => {}}
        storeId="7"
        stores={mockStores}
        nodes={mockNodes}
        servers={mockServers}
        defaultGroupId="81"
        defaultReplicaId="801"
        defaultNodeIds={[1]}
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /create group/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ group_id: 81, replica_id: 801, nodes: [1] });
  });

  it('defaults to all active store nodes when the selected store already has active nodes', () => {
    const bothServers: CrowdbKVServerView[] = [
      ...mockServers,
      {
        id: 'KV-n2',
        node_id: 2,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          rpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        rest_port: 29910,
        rpc_port: 29920,
      },
    ];

    render(
      <AddGroupDialog isOpen onClose={() => {}} storeId="7" stores={mockStores} nodes={mockNodes} servers={bothServers} />,
      { wrapper },
    );

    expect(screen.getByLabelText(/^1\b/) as HTMLInputElement).toBeChecked();
    expect(screen.getByLabelText(/^2\b/) as HTMLInputElement).toBeChecked();
  });

  it('defaults to the first three active nodes when the selected store has no active nodes yet', () => {
    const fourNodes: Node[] = [
      { id: 1, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
      { id: 2, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
      { id: 3, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
      { id: 4, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
    ];
    const fourServers: CrowdbKVServerView[] = [
      {
        id: 'KV-n1',
        node_id: 1,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:19910',
          rpc_url: 'http://127.0.0.1:19920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        rest_port: 19910,
        rpc_port: 19920,
      },
      {
        id: 'KV-n2',
        node_id: 2,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          rpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        rest_port: 29910,
        rpc_port: 29920,
      },
      {
        id: 'KV-n3',
        node_id: 3,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:39910',
          rpc_url: 'http://127.0.0.1:39920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        rest_port: 39910,
        rpc_port: 39920,
      },
      {
        id: 'KV-n4',
        node_id: 4,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:49910',
          rpc_url: 'http://127.0.0.1:49920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        rest_port: 49910,
        rpc_port: 49920,
      },
    ];

    render(
      <AddGroupDialog
        isOpen
        onClose={() => {}}
        storeId="9"
        stores={[{ store_id: '9', nodes: [], groups: [] }]}
        nodes={fourNodes}
        servers={fourServers}
      />,
      { wrapper },
    );

    expect(screen.getByLabelText(/^1\b/) as HTMLInputElement).toBeChecked();
    expect(screen.getByLabelText(/^2\b/) as HTMLInputElement).toBeChecked();
    expect(screen.getByLabelText(/^3\b/) as HTMLInputElement).toBeChecked();
    expect(screen.getByLabelText(/^4\b/) as HTMLInputElement).not.toBeChecked();
  });

  it('filters selectable nodes to the selected store owners', () => {
    const bothServers: CrowdbKVServerView[] = [
      ...mockServers,
      {
        id: 'KV-n2',
        node_id: 2,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          rpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        rest_port: 29910,
        rpc_port: 29920,
      },
    ];

    render(
      <AddGroupDialog
        isOpen
        onClose={() => {}}
        stores={[{ store_id: '7', nodes: [1], groups: [] }, { store_id: '8', nodes: [2], groups: [] }]}
        nodes={mockNodes}
        servers={bothServers}
      />,
      { wrapper },
    );

    expect(screen.getByLabelText(/^1\b/)).toBeInTheDocument();
    expect(screen.getByLabelText(/^2\b/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('KV Store'), { target: { value: '8' } });
    expect(screen.getByLabelText(/^2\b/)).toBeInTheDocument();
    expect(screen.getByLabelText(/^1\b/)).toBeInTheDocument();
  });

  it('excludes unavailable store owners', () => {
    const allStores: EnrichedStoreView[] = [{ store_id: '7', nodes: [1, 2], groups: [] }];
    const unavailableN2: CrowdbKVServerView[] = [
      ...mockServers,
      {
        id: 'KV-n2',
        node_id: 2,
        rack_id: 1,
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          rpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Stopped,
          health: NodeHealth.Down,
          last_seen_ms: Date.now(),
        },
        rest_port: 29910,
        rpc_port: 29920,
      },
    ];

    render(
      <AddGroupDialog isOpen onClose={() => {}} stores={allStores} nodes={mockNodes} servers={unavailableN2} />,
      { wrapper },
    );

    expect(screen.getByLabelText(/^1\b/)).toBeInTheDocument();
    expect(screen.queryByLabelText(/^n2/)).toBeNull();
  });
});

describe('Add Replica dialog', () => {
  it('POSTs { node_id, replica_id } when replica id supplied', async () => {
    installFetchMock({ replica_id: 2 }, 201);
    render(
      <AddReplicaDialog
        isOpen
        onClose={() => {}}
        storeId="7"
        groupId="70"
        nodes={mockNodes}
      />,
      { wrapper },
    );

    fireEvent.change(screen.getByLabelText('Node'), { target: { value: '2' } });
    fireEvent.change(screen.getByLabelText('Replica ID (optional)'), { target: { value: '2' } });
    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/stores/7/groups/70/replicas',
      method: 'POST',
      body: { node_id: 2, replica_id: 2 },
    });
  });

  it('omits replica_id when blank, letting the backend auto-assign', async () => {
    installFetchMock({ replica_id: 3 }, 201);
    render(
      <AddReplicaDialog
        isOpen
        onClose={() => {}}
        storeId="7"
        groupId="70"
        nodes={mockNodes}
      />,
      { wrapper },
    );

    fireEvent.change(screen.getByLabelText('Node'), { target: { value: '2' } });
    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ node_id: 2 });
  });

  it('can submit with prefilled replica defaults', async () => {
    installFetchMock({ replica_id: 4 }, 201);
    render(
      <AddReplicaDialog
        isOpen
        onClose={() => {}}
        storeId="7"
        groupId="70"
        nodes={mockNodes}
        defaultNodeId={1}
        defaultReplicaId="4"
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ node_id: 1, replica_id: 4 });
  });
});

describe('end-to-end create flow', () => {
  /**
   * Drives the documented flow in `doc/todo_ui2.md` §5.4 entirely through
   * the dialogs and asserts the resulting HTTP transcript matches the
   * backend's normative contract (`mgmt_routes.rs`, `replica_routes.rs`).
   */
  it('rack → node(with CrowDB Storage) → store → group → replica posts the right bodies', async () => {
    installFetchMock({}, 201);

    // Rack.
    const rack = render(<AddRackDialog isOpen onClose={() => {}} />, { wrapper });
    fireEvent.change(screen.getByLabelText('Rack ID'), { target: { value: '1' } });
    fireEvent.click(screen.getByRole('button', { name: /create rack/i }));
    await waitFor(() => expect(captured.length).toBe(1));
    rack.unmount();

    // Node.
    const node = render(
      <AddNodeDialog isOpen onClose={() => {}} racks={[mockRack]} defaultRackId="1" />,
      { wrapper },
    );
    // Disable DiskDB — this flow tests CrowDB Storage, not DiskDB deploy.
    fireEvent.click(screen.getByLabelText('Enable DiskDB on this node'));
    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: '1' } });
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: '127.0.0.1' } });
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));
    await waitFor(() => expect(captured.length).toBe(3));
    node.unmount();

    // Store.
    const store = render(
      <AddStoreDialog isOpen onClose={() => {}} nodes={[mockNodes[0]]} servers={mockServers} />,
      { wrapper },
    );
    fireEvent.change(screen.getByLabelText('KV Store ID (numeric)'), { target: { value: '7' } });
    fireEvent.click(screen.getByLabelText(/^1\b/));
    fireEvent.click(screen.getByRole('button', { name: /create kv store/i }));
    await waitFor(() => expect(captured.length).toBe(4));
    store.unmount();

    // Group.
    const group = render(
      <AddGroupDialog isOpen onClose={() => {}} storeId="7" stores={mockStores} nodes={[mockNodes[0]]} servers={mockServers} />,
      { wrapper },
    );
    fireEvent.change(screen.getByLabelText('Group ID (numeric)'), { target: { value: '80' } });
    fireEvent.change(screen.getByLabelText('Starting Replica ID (numeric)'), {
      target: { value: '800' },
    });
    expect(screen.getByLabelText(/^1\b/) as HTMLInputElement).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: /create group/i }));
    await waitFor(() => expect(captured.length).toBe(5));
    group.unmount();

    // Replica.
    const replica = render(
      <AddReplicaDialog
        isOpen
        onClose={() => {}}
        storeId="7"
        groupId="70"
        nodes={mockNodes}
      />,
      { wrapper },
    );
    fireEvent.change(screen.getByLabelText('Node'), { target: { value: '2' } });
    fireEvent.change(screen.getByLabelText('Replica ID (optional)'), { target: { value: '701' } });
    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));
    await waitFor(() => expect(captured.length).toBe(6));
    replica.unmount();

    expect(captured.map((r) => `${r.method} ${r.url}`)).toEqual([
      'POST /api/racks',
      'POST /api/nodes',
      'POST /api/nodes/1/server/deploy',
      'POST /api/stores',
      'POST /api/stores/7/groups',
      'POST /api/stores/7/groups/70/replicas',
    ]);
    expect(captured[3].body).toEqual({
      store_id: 7,
      nodes: [1],
    });
    expect(captured[4].body).toEqual({ group_id: 80, replica_id: 800, nodes: [1] });
    expect(captured[5].body).toEqual({ node_id: 2, replica_id: 701 });
  });
});
