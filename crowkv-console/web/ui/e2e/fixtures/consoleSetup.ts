// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { expect, request } from '@playwright/test';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

export const DEFAULT_SERVER_BINARY =
  process.env.CROWKV_SERVER_BINARY ?? resolve(__dirname, '../../../../../target/debug/crowkv-server');

export interface TestRack {
  id: string;
  name?: string;
}

export interface TestNode {
  id: string;
  rack_id: string;
  host?: string;
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
    const response = await api.post(`/api/racks/${encodeURIComponent(node.rack_id)}/nodes`, {
      data: {
        id: node.id,
        rack_id: node.rack_id,
        host: node.host ?? '127.0.0.1',
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
        election_profile: 'e2e',
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
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/server/stop`);
    if (!response.ok() && response.status() !== 400 && response.status() !== 404 && response.status() !== 409) {
      console.warn(`stopNodeServer(${nodeId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`stopNodeServer(${nodeId}) failed:`, err);
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

/**
 * Poll until the group reports an elected leader. `POST /api/stores` only
 * creates the store; a group must be added separately (`addGroup`) and then
 * needs a moment to elect before KV ops can resolve a leader.
 */
export async function waitForLeader(baseURL: string, storeId: number, groupId: number, timeoutMs = 10_000) {
  const api = await apiContext(baseURL);
  try {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const r = await api.get(`/api/stores/${storeId}/groups/${groupId}`);
      if (r.ok()) {
        const v = await r.json();
        const hasLeader =
          (Array.isArray(v.replicas) && v.replicas.some((x: any) => String(x.role).toLowerCase() === 'leader')) ||
          (typeof v.leader_id === 'number' && v.leader_id > 0);
        if (hasLeader) return;
      }
      await new Promise((res) => setTimeout(res, 100));
    }
    throw new Error(`leader not elected for store ${storeId} group ${groupId} within ${timeoutMs}ms`);
  } finally {
    await api.dispose();
  }
}

export async function createStore(baseURL: string, storeId: number, nodeIds: string[]) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/stores', {
      data: {
        store_id: storeId,
        nodes: nodeIds,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function resetAll(baseURL: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/internal/reset');
    expect(response.status(), await response.text()).toBe(200);
  } finally {
    await api.dispose();
  }
}

// ── setupCluster helper + topology presets ──────────────────────────

export interface TopologyDescriptor {
  nodeCount: number;
  storeCount: number;
  groupsPerStore: number;
  replicasPerGroup: number;
  rackPrefix: string;
  nodePrefix: string;
  portBase: number;
  storeBase: number;
  groupBase: number;
}

export interface SetupResult {
  racks: string[];
  nodes: string[];
  stores: number[];
  groups: { storeId: number; groupId: number }[];
  apiBase: string;
}

export const SIMPLE: TopologyDescriptor = {
  nodeCount: 3,
  storeCount: 1,
  groupsPerStore: 1,
  replicasPerGroup: 3,
  rackPrefix: 'sr',
  nodePrefix: 'sn',
  portBase: 9800,
  storeBase: 800,
  groupBase: 8000,
};

export const COMPLEX: TopologyDescriptor = {
  nodeCount: 8,
  storeCount: 2,
  groupsPerStore: 2,
  replicasPerGroup: 3,
  rackPrefix: 'cr',
  nodePrefix: 'cn',
  portBase: 9900,
  storeBase: 900,
  groupBase: 9000,
};

/**
 * Create a full cluster topology via API calls: racks → nodes → deploy →
 * stores → groups → wait for leaders. Returns the created entity IDs.
 * Each node gets its own rack (1:1 mapping) for simplicity.
 */
export async function setupCluster(baseURL: string, topo: TopologyDescriptor): Promise<SetupResult> {
  const racks: string[] = [];
  const nodes: string[] = [];

  for (let i = 0; i < topo.nodeCount; i++) {
    const rackId = `${topo.rackPrefix}${i}`;
    const nodeId = `${topo.nodePrefix}${i}`;
    await createRack(baseURL, { id: rackId, name: rackId });
    await createNode(baseURL, { id: nodeId, rack_id: rackId });
    await deployNodeServer(baseURL, nodeId, topo.portBase + i * 2, topo.portBase + i * 2 + 1);
    racks.push(rackId);
    nodes.push(nodeId);
  }

  const stores: number[] = [];
  const groups: { storeId: number; groupId: number }[] = [];

  for (let s = 0; s < topo.storeCount; s++) {
    const storeId = topo.storeBase + s;
    const storeNodes = nodes.slice(0, Math.min(topo.replicasPerGroup, nodes.length));
    await createStore(baseURL, storeId, storeNodes);
    stores.push(storeId);

    for (let g = 0; g < topo.groupsPerStore; g++) {
      const groupId = topo.groupBase + s * topo.groupsPerStore + g;
      const groupNodes = nodes.slice(0, topo.replicasPerGroup);
      await addGroup(baseURL, storeId, groupId, 1, groupNodes);
      await waitForLeader(baseURL, storeId, groupId);
      groups.push({ storeId, groupId });
    }
  }

  return { racks, nodes, stores, groups, apiBase: baseURL };
}

/**
 * Stop all deployed servers from a setupCluster call.
 */
export async function teardownCluster(baseURL: string, result: SetupResult) {
  for (const nodeId of result.nodes) {
    await stopNodeServer(baseURL, nodeId);
  }
}
