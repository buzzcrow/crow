import { expect, request } from '@playwright/test';

export const DEFAULT_SERVER_BINARY =
  process.env.CROWKV_SERVER_BINARY ?? '/cjdata/cpp/crowkv/target/debug/crowkv-server';

export interface TestRack {
  id: string;
  name?: string;
}

export interface TestNode {
  id: string;
  rack_id: string;
  host?: string;
  ssh_port?: number;
  ssh_user?: string;
}

export async function apiContext(baseURL: string) {
  return request.newContext({ baseURL });
}

export async function createRack(baseURL: string, rack: TestRack) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/racks', {
      data: {
        id: rack.id,
        name: rack.name ?? rack.id,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function createNode(baseURL: string, node: TestNode) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/nodes', {
      data: {
        id: node.id,
        rack_id: node.rack_id,
        host: node.host ?? '127.0.0.1',
        ssh_port: node.ssh_port ?? 22,
        ssh_user: node.ssh_user ?? '',
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function seedRackAndNode(baseURL: string, rackId = 'r1', nodeId = 'n1') {
  await createRack(baseURL, { id: rackId, name: rackId });
  await createNode(baseURL, { id: nodeId, rack_id: rackId });
}

export async function deployNodeServer(baseURL: string, nodeId: string, mgmtPort: number, grpcPort: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/server/deploy`, {
      data: {
        mgmt_port: mgmtPort,
        grpc_port: grpcPort,
        binary: DEFAULT_SERVER_BINARY,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function stopNodeServer(baseURL: string, nodeId: string) {
  const api = await apiContext(baseURL);
  try {
    await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/server/stop`).catch(() => undefined);
  } finally {
    await api.dispose();
  }
}

export async function addGroup(baseURL: string, storeId: number, groupId: number, replicaId: number, nodeIds: string[]) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/stores/${storeId}/groups`, {
      data: {
        group_id: groupId,
        replica_id: replicaId,
        nodes: nodeIds,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function addReplica(baseURL: string, storeId: number, groupId: number, nodeId: string, replicaId?: number) {
  const api = await apiContext(baseURL);
  try {
    const body: Record<string, unknown> = { node_id: nodeId };
    if (replicaId !== undefined) body.replica_id = replicaId;
    const response = await api.post(`/api/stores/${storeId}/groups/${groupId}/replicas`, { data: body });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function createStore(baseURL: string, storeId: number, groupId: number, replicaId: number, nodeIds: string[]) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/stores', {
      data: {
        store_id: storeId,
        group_id: groupId,
        replica_id: replicaId,
        nodes: nodeIds,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}
