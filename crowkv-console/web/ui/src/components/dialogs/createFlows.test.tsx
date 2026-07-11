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
import type { Node, Rack, CrowKVServerView, StoreView } from '../../types';
import { deployPortDefaultsForNode } from './defaults';

/**
 * These tests pin down the exact request bodies the SPA must send for the
 * end-to-end flow described in `doc/todo_ui2.md` §5.4. They are the
 * frontend counterpart to the Rust integration tests in
 * `crowkv-console/web/tests/{lifecycle,mgmt,replica}_routes.rs`.
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

const mockRack: Rack = { id: 'r1', name: 'r1', nodes: [] };
const mockNodes: Node[] = [
  { id: 'n1', rack_id: 'r1', host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
  { id: 'n2', rack_id: 'r1', host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
];
const mockServers: CrowKVServerView[] = [
  {
    id: 'n1-kv',
    node_id: 'n1',
    rack_id: 'r1',
    host: '127.0.0.1',
    process: {
      mgmt_url: 'http://127.0.0.1:19910',
      grpc_url: 'http://127.0.0.1:19920',
      state: ProcState.Running,
      health: NodeHealth.Up,
      last_seen_ms: Date.now(),
    },
    mgmt_port: 19910,
    grpc_port: 19920,
  },
];
const mockStores: StoreView[] = [
  { store_id: '7', nodes: ['n1', 'n2'], groups: [] },
];

describe('Add Rack dialog', () => {
  it('POSTs { id, name? } to /api/racks', async () => {
    installFetchMock({ id: 'r1', name: 'Rack 1', nodes: [] });
    render(<AddRackDialog isOpen onClose={() => {}} />, { wrapper });

    fireEvent.change(screen.getByLabelText('Rack ID'), { target: { value: 'r1' } });
    fireEvent.change(screen.getByLabelText('Name (optional)'), { target: { value: 'Rack 1' } });
    fireEvent.click(screen.getByRole('button', { name: /create rack/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/racks',
      method: 'POST',
      body: { id: 'r1', name: 'Rack 1' },
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
    installFetchMock({ id: 'n1' });
    render(
      <AddNodeDialog isOpen onClose={() => {}} racks={[mockRack]} defaultRackId="r1" />,
      { wrapper },
    );

    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: 'n1' } });
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: '127.0.0.1' } });
    fireEvent.click(screen.getByLabelText('Enable CrowKV on this node'));
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].url).toBe('/api/nodes');
    expect(captured[0].method).toBe('POST');
    // Backend `NodeEntry` shape — flat fields, NO nested `ssh` object.
    expect(captured[0].body).toEqual({
      id: 'n1',
      rack_id: 'r1',
      host: '127.0.0.1',
      ssh_port: 22,
      ssh_user: '',
    });
    expect(captured[0].body.ssh).toBeUndefined();
  });

  it('includes ssh_user + ssh_key when provided', async () => {
    installFetchMock({ id: 'n1' });
    render(
      <AddNodeDialog isOpen onClose={() => {}} racks={[mockRack]} defaultRackId="r1" />,
      { wrapper },
    );

    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: 'n1' } });
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: '10.0.0.1' } });
    fireEvent.change(screen.getByLabelText('SSH User (optional)'), { target: { value: 'crowkv' } });
    fireEvent.change(screen.getByLabelText('SSH Key Path (optional)'), {
      target: { value: '/keys/id_rsa' },
    });
    fireEvent.click(screen.getByLabelText('Enable CrowKV on this node'));
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({
      id: 'n1',
      rack_id: 'r1',
      host: '10.0.0.1',
      ssh_port: 22,
      ssh_user: 'crowkv',
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
        defaultRackId="r1"
        existingNodeIds={['node1']}
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByLabelText('Enable CrowKV on this node'));
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({
      id: '2',
      rack_id: 'r1',
      host: '127.0.0.1',
      ssh_port: 22,
      ssh_user: '',
    });
  });

  it('enables CrowKV by default and deploys immediately after node creation', async () => {
    installFetchMock({ id: 'n1' });
    render(
      <AddNodeDialog
        isOpen
        onClose={() => {}}
        racks={[mockRack]}
        defaultRackId="r1"
        defaultMgmtPort="19911"
        defaultGrpcPort="19921"
      />,
      { wrapper },
    );

    expect((screen.getByLabelText('Enable CrowKV on this node') as HTMLInputElement).checked).toBe(true);
    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: 'n1' } });
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: '127.0.0.1' } });
    fireEvent.click(screen.getByRole('button', { name: /create node/i }));

    await waitFor(() => expect(captured.length).toBe(2));
    expect(captured[0]).toMatchObject({
      url: '/api/nodes',
      method: 'POST',
      body: {
        id: 'n1',
        rack_id: 'r1',
        host: '127.0.0.1',
        ssh_port: 22,
        ssh_user: '',
      },
    });
    expect(captured[1]).toMatchObject({
      url: '/api/nodes/n1/server/deploy',
      method: 'POST',
      body: { mgmt_port: 19911, grpc_port: 19921 },
    });
  });
});

describe('Deploy Server dialog', () => {
  it('POSTs DeployNodeServerBody to /api/nodes/:id/server/deploy', async () => {
    installFetchMock({ node_id: 'n1', pid: 1234, mgmt_url: 'x', grpc_url: 'y' });
    render(<DeployServerDialog isOpen onClose={() => {}} nodeId="n1" />, { wrapper });

    fireEvent.change(screen.getByLabelText('Management Port'), { target: { value: '19911' } });
    fireEvent.change(screen.getByLabelText('gRPC Port'), { target: { value: '19921' } });
    fireEvent.click(screen.getByRole('button', { name: /deploy/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/nodes/n1/server/deploy',
      method: 'POST',
      body: { mgmt_port: 19911, grpc_port: 19921 },
    });
    expect(captured[0].body.binary).toBeUndefined();
  });

  it('submits immediately with provided default ports', async () => {
    installFetchMock({ node_id: 'n1', pid: 1234, mgmt_url: 'x', grpc_url: 'y' });
    render(
      <DeployServerDialog
        isOpen
        onClose={() => {}}
        nodeId="n1"
        defaultMgmtPort="19915"
        defaultGrpcPort="19925"
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /deploy/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ mgmt_port: 19915, grpc_port: 19925 });
  });

  it('increments ports only when the same node already uses them', () => {
    const defaults = deployPortDefaultsForNode(
      [
        {
          id: 'n1',
          server: { mgmt_url: 'http://127.0.0.1:19910', grpc_url: 'http://127.0.0.1:19920' },
        },
        {
          id: 'n2',
          server: { mgmt_url: 'http://127.0.0.1:19910', grpc_url: 'http://127.0.0.1:19920' },
        },
      ],
      'n1',
    );

    expect(defaults).toEqual({ defaultMgmtPort: '19911', defaultGrpcPort: '19921' });
  });

  it('increments globally when a different node already uses the base ports', () => {
    const defaults = deployPortDefaultsForNode(
      [
        {
          id: 'n2',
          server: { mgmt_url: 'http://127.0.0.1:19910', grpc_url: 'http://127.0.0.1:19920' },
        },
      ],
      'n1',
    );

    expect(defaults).toEqual({ defaultMgmtPort: '19911', defaultGrpcPort: '19921' });
  });

  it('derives defaults from the node id suffix before checking collisions', () => {
    const defaults = deployPortDefaultsForNode([], 'n2', 19910, 19920);

    expect(defaults).toEqual({ defaultMgmtPort: '19912', defaultGrpcPort: '19922' });
  });

  it('can increment from remembered same-node ports even after the server is gone', () => {
    const defaults = deployPortDefaultsForNode([], 'n1', 19910, 19920, [19910], [19920]);

    expect(defaults).toEqual({ defaultMgmtPort: '19911', defaultGrpcPort: '19921' });
  });
});

describe('Add Store dialog', () => {
  it('POSTs numeric store_id + nodes', async () => {
    installFetchMock({ store_id: 7 }, 201);
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={mockServers} />, { wrapper });

    fireEvent.change(screen.getByLabelText('KV Store ID (numeric)'), { target: { value: '7' } });
    // Tick n1.
    fireEvent.click(screen.getByLabelText(/^n1/));
    fireEvent.click(screen.getByRole('button', { name: /create kv store/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/stores',
      method: 'POST',
      body: { store_id: 7, nodes: ['n1'] },
    });
  });

  it('keeps the Create button disabled until id is numeric and a deployed CrowKV node is picked', () => {
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={mockServers} />, { wrapper });
    const btn = screen.getByRole('button', { name: /create kv store/i });
    expect(btn).toBeDisabled();

    fireEvent.change(screen.getByLabelText('KV Store ID (numeric)'), { target: { value: 'abc' } });
    fireEvent.click(screen.getByLabelText(/^n1/));
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
        defaultNodeIds={['n1']}
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /create kv store/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ store_id: 9, nodes: ['n1'] });
  });

  it('only lists nodes that already run CrowKV', () => {
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={mockServers} />, { wrapper });

    expect(screen.getByLabelText(/^n1/)).toBeInTheDocument();
    expect(screen.queryByLabelText(/^n2/)).toBeNull();
  });

  it('excludes configured but unavailable CrowKV nodes', () => {
    const unavailableServers: CrowKVServerView[] = [
      ...mockServers,
      {
        id: 'n2-kv',
        node_id: 'n2',
        rack_id: 'r1',
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          grpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Stopped,
          health: NodeHealth.Down,
          last_seen_ms: Date.now(),
        },
        mgmt_port: 29910,
        grpc_port: 29920,
      },
    ];

    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} servers={unavailableServers} />, { wrapper });

    expect(screen.getByLabelText(/^n1/)).toBeInTheDocument();
    expect(screen.queryByLabelText(/^n2/)).toBeNull();
  });
});

describe('Add Group dialog', () => {
  it('POSTs CreateGroupBody to /api/stores/:sid/groups', async () => {
    installFetchMock({ group_id: 80 }, 201);
    const bothServers: CrowKVServerView[] = [
      ...mockServers,
      {
        id: 'KV-n2',
        node_id: 'n2',
        rack_id: 'r1',
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          grpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        mgmt_port: 29910,
        grpc_port: 29920,
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
    fireEvent.click(screen.getByLabelText(/^n1/));
    fireEvent.click(screen.getByLabelText(/^n2/));
    fireEvent.click(screen.getByRole('button', { name: /create group/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/stores/7/groups',
      method: 'POST',
      body: { group_id: 80, replica_id: 800, nodes: ['n1', 'n2'] },
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
        defaultNodeIds={['n1']}
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /create group/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ group_id: 81, replica_id: 801, nodes: ['n1'] });
  });

  it('filters selectable nodes to the selected store owners', () => {
    const bothServers: CrowKVServerView[] = [
      ...mockServers,
      {
        id: 'KV-n2',
        node_id: 'n2',
        rack_id: 'r1',
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          grpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Running,
          health: NodeHealth.Up,
          last_seen_ms: Date.now(),
        },
        mgmt_port: 29910,
        grpc_port: 29920,
      },
    ];

    render(
      <AddGroupDialog
        isOpen
        onClose={() => {}}
        stores={[{ store_id: '7', nodes: ['n1'], groups: [] }, { store_id: '8', nodes: ['n2'], groups: [] }]}
        nodes={mockNodes}
        servers={bothServers}
      />,
      { wrapper },
    );

    expect(screen.getByLabelText(/^n1/)).toBeInTheDocument();
    expect(screen.getByLabelText(/^n2/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('KV Store'), { target: { value: '8' } });
    expect(screen.getByLabelText(/^n2/)).toBeInTheDocument();
    expect(screen.getByLabelText(/^n1/)).toBeInTheDocument();
  });

  it('excludes unavailable store owners', () => {
    const allStores: StoreView[] = [{ store_id: '7', nodes: ['n1', 'n2'], groups: [] }];
    const unavailableN2: CrowKVServerView[] = [
      ...mockServers,
      {
        id: 'KV-n2',
        node_id: 'n2',
        rack_id: 'r1',
        host: '127.0.0.1',
        process: {
          mgmt_url: 'http://127.0.0.1:29910',
          grpc_url: 'http://127.0.0.1:29920',
          state: ProcState.Stopped,
          health: NodeHealth.Down,
          last_seen_ms: Date.now(),
        },
        mgmt_port: 29910,
        grpc_port: 29920,
      },
    ];

    render(
      <AddGroupDialog isOpen onClose={() => {}} stores={allStores} nodes={mockNodes} servers={unavailableN2} />,
      { wrapper },
    );

    expect(screen.getByLabelText(/^n1/)).toBeInTheDocument();
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

    fireEvent.change(screen.getByLabelText('Node'), { target: { value: 'n2' } });
    fireEvent.change(screen.getByLabelText('Replica ID (optional)'), { target: { value: '2' } });
    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/stores/7/groups/70/replicas',
      method: 'POST',
      body: { node_id: 'n2', replica_id: 2 },
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

    fireEvent.change(screen.getByLabelText('Node'), { target: { value: 'n2' } });
    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ node_id: 'n2' });
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
        defaultNodeId="n1"
        defaultReplicaId="4"
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0].body).toEqual({ node_id: 'n1', replica_id: 4 });
  });
});

describe('end-to-end create flow', () => {
  /**
   * Drives the documented flow in `doc/todo_ui2.md` §5.4 entirely through
   * the dialogs and asserts the resulting HTTP transcript matches the
   * backend's normative contract (`mgmt_routes.rs`, `replica_routes.rs`).
   */
  it('rack → node(with CrowKV) → store → group → replica posts the right bodies', async () => {
    installFetchMock({}, 201);

    // Rack.
    const rack = render(<AddRackDialog isOpen onClose={() => {}} />, { wrapper });
    fireEvent.change(screen.getByLabelText('Rack ID'), { target: { value: 'r1' } });
    fireEvent.click(screen.getByRole('button', { name: /create rack/i }));
    await waitFor(() => expect(captured.length).toBe(1));
    rack.unmount();

    // Node.
    const node = render(
      <AddNodeDialog isOpen onClose={() => {}} racks={[mockRack]} defaultRackId="r1" />,
      { wrapper },
    );
    fireEvent.change(screen.getByLabelText('Node ID'), { target: { value: 'n1' } });
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
    fireEvent.click(screen.getByLabelText(/^n1/));
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
    fireEvent.click(screen.getByLabelText(/^n1/));
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
    fireEvent.change(screen.getByLabelText('Node'), { target: { value: 'n2' } });
    fireEvent.change(screen.getByLabelText('Replica ID (optional)'), { target: { value: '701' } });
    fireEvent.click(screen.getByRole('button', { name: /add replica/i }));
    await waitFor(() => expect(captured.length).toBe(6));
    replica.unmount();

    expect(captured.map((r) => `${r.method} ${r.url}`)).toEqual([
      'POST /api/racks',
      'POST /api/nodes',
      'POST /api/nodes/n1/server/deploy',
      'POST /api/stores',
      'POST /api/stores/7/groups',
      'POST /api/stores/7/groups/70/replicas',
    ]);
    expect(captured[3].body).toEqual({
      store_id: 7,
      nodes: ['n1'],
    });
    expect(captured[4].body).toEqual({ group_id: 80, replica_id: 800, nodes: ['n1'] });
    expect(captured[5].body).toEqual({ node_id: 'n2', replica_id: 701 });
  });
});
