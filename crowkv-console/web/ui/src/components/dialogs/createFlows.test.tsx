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
import type { Node, Rack } from '../../types';

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
});

describe('Deploy Server dialog', () => {
  it('POSTs DeployNodeServerBody to /api/nodes/:id/server/deploy', async () => {
    installFetchMock({ node_id: 'n1', pid: 1234, mgmt_url: 'x', grpc_url: 'y' });
    render(<DeployServerDialog isOpen onClose={() => {}} nodeId="n1" />, { wrapper });

    fireEvent.change(screen.getByLabelText('Management Port'), { target: { value: '9911' } });
    fireEvent.change(screen.getByLabelText('gRPC Port'), { target: { value: '9921' } });
    fireEvent.click(screen.getByRole('button', { name: /deploy/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/nodes/n1/server/deploy',
      method: 'POST',
      body: { mgmt_port: 9911, grpc_port: 9921 },
    });
    expect(captured[0].body.binary).toBeUndefined();
  });
});

describe('Add Store dialog', () => {
  it('POSTs numeric store_id/group_id/replica_id + nodes', async () => {
    installFetchMock({ store_id: 7 }, 201);
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} />, { wrapper });

    fireEvent.change(screen.getByLabelText('Store ID (numeric)'), { target: { value: '7' } });
    fireEvent.change(screen.getByLabelText('Initial Group ID (numeric)'), {
      target: { value: '70' },
    });
    fireEvent.change(screen.getByLabelText('First Replica ID (numeric)'), {
      target: { value: '700' },
    });
    // Tick n1.
    fireEvent.click(screen.getByLabelText(/^n1/));
    fireEvent.click(screen.getByRole('button', { name: /create store/i }));

    await waitFor(() => expect(captured.length).toBe(1));
    expect(captured[0]).toMatchObject({
      url: '/api/stores',
      method: 'POST',
      body: { store_id: 7, group_id: 70, replica_id: 700, nodes: ['n1'] },
    });
  });

  it('keeps the Create button disabled until ids are numeric and a node is picked', () => {
    render(<AddStoreDialog isOpen onClose={() => {}} nodes={mockNodes} />, { wrapper });
    const btn = screen.getByRole('button', { name: /create store/i });
    expect(btn).toBeDisabled();

    fireEvent.change(screen.getByLabelText('Store ID (numeric)'), { target: { value: 'abc' } });
    fireEvent.change(screen.getByLabelText('Initial Group ID (numeric)'), { target: { value: '70' } });
    fireEvent.change(screen.getByLabelText('First Replica ID (numeric)'), { target: { value: '700' } });
    fireEvent.click(screen.getByLabelText(/^n1/));
    expect(btn).toBeDisabled(); // store_id not numeric

    fireEvent.change(screen.getByLabelText('Store ID (numeric)'), { target: { value: '7' } });
    expect(btn).toBeEnabled();
  });
});

describe('Add Group dialog', () => {
  it('POSTs CreateGroupBody to /api/stores/:sid/groups', async () => {
    installFetchMock({ group_id: 80 }, 201);
    render(
      <AddGroupDialog isOpen onClose={() => {}} storeId="7" nodes={mockNodes} />,
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
});

describe('end-to-end create flow', () => {
  /**
   * Drives the documented flow in `doc/todo_ui2.md` §5.4 entirely through
   * the dialogs and asserts the resulting HTTP transcript matches the
   * backend's normative contract (`mgmt_routes.rs`, `replica_routes.rs`).
   */
  it('rack → node → deploy → store → group → replica posts the right bodies', async () => {
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
    await waitFor(() => expect(captured.length).toBe(2));
    node.unmount();

    // Deploy.
    const deploy = render(
      <DeployServerDialog isOpen onClose={() => {}} nodeId="n1" />,
      { wrapper },
    );
    fireEvent.click(screen.getByRole('button', { name: /deploy/i }));
    await waitFor(() => expect(captured.length).toBe(3));
    deploy.unmount();

    // Store.
    const store = render(
      <AddStoreDialog isOpen onClose={() => {}} nodes={[mockNodes[0]]} />,
      { wrapper },
    );
    fireEvent.change(screen.getByLabelText('Store ID (numeric)'), { target: { value: '7' } });
    fireEvent.change(screen.getByLabelText('Initial Group ID (numeric)'), {
      target: { value: '70' },
    });
    fireEvent.change(screen.getByLabelText('First Replica ID (numeric)'), {
      target: { value: '700' },
    });
    fireEvent.click(screen.getByLabelText(/^n1/));
    fireEvent.click(screen.getByRole('button', { name: /create store/i }));
    await waitFor(() => expect(captured.length).toBe(4));
    store.unmount();

    // Group.
    const group = render(
      <AddGroupDialog isOpen onClose={() => {}} storeId="7" nodes={[mockNodes[0]]} />,
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
      group_id: 70,
      replica_id: 700,
      nodes: ['n1'],
    });
    expect(captured[4].body).toEqual({ group_id: 80, replica_id: 800, nodes: ['n1'] });
    expect(captured[5].body).toEqual({ node_id: 'n2', replica_id: 701 });
  });
});
